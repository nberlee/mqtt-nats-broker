//go:build mqttport_fulltests

package mqtt

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestMQTTSharedFailedFilterReleaseRunsFromTheResolver drives F-082's release through the
// production route rather than by calling it: a real live SUBSCRIBE whose shared filter fails
// AFTER its group join, whose rollback save goes ambiguous, and whose readLoop resolver then
// proves that write durable and runs the landed continuation.
//
// The unit half is TestSharedFailedFilterReleaseDischargesOwnStateUnderARefusal and friends.
// This one exists because a fixture that calls releaseFailedSharedFilter directly cannot show
// that anything still REGISTERS it, or that the resolver still runs it — and the registration
// is exactly what F-082 changed.
//
// The shape, and every step of it is load-bearing:
//
//   - A MULTI-LEVEL shared filter, so the packet makes two group joins. The first succeeds.
//   - Between them, the messages stream is given a consumer InactiveThreshold limit, which is
//     a real production refusal: processSharedJSConsumer will not create a reapable group
//     consumer, so the level-up join fails closed.
//   - Store suppression is armed at the same boundary, NOT before the packet: the per-filter
//     membership save comes first and has to SUCCEED, or the packet aborts before the join and
//     nothing is ever owed. The rollback's removal save is the suppressed one.
//   - Suppression is lifted from the store-result seam, which fires after the suppressed
//     return, so the resolver's re-attempt lands and settleLandedAttempt runs the continuation.
//
// The oracle is what the continuation is for: both group consumers gone, and the session's
// MaxAckPending budget back where it started. The budget half is the one that fails against
// the pre-F-082 release, which re-derived its work from sess.cons and found nothing there.
func TestMQTTSharedFailedFilterReleaseRunsFromTheResolver(t *testing.T) {
	const clientID = "failedfilterroute"
	const group = "grp"
	const topic = "foo/#"

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   clientID,
		cleanStart: true,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	// A plain subscription first, so the session is bound and its record settled before the
	// packet under test: every budget assertion below is then a delta of THIS packet.
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "settled/plain", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK %v for the settling filter, want a single granted QoS 1", codes)
	}
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTGetClient(t, s, clientID).mqtt.sess
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	sess.mu.Lock()
	budgetBefore := sess.tmaxack
	sess.mu.Unlock()

	// The two group consumers the failing packet will join: "foo.>" and its level-up "foo".
	mainDur := mqttSharedNamesDurable(group, "foo.>", 1)
	levelDur := mqttSharedNamesDurable(group, "foo", 1)

	// Fired after the FIRST group create, which is the only window in which the membership
	// save is already durable and the level-up join has not run yet.
	// Atomic: the seam fires on the readLoop while the test goroutine reads it, and a plain
	// bool here is a data race the -race suite would rightly fail.
	var staged atomic.Bool
	h := func(boundary string) {
		if boundary != "after-group-create:"+string(mqttSharedSubPrefix)+group+"/foo.>" {
			return
		}
		if !staged.CompareAndSwap(false, true) {
			return
		}
		si, err := asm.jsa.lookupStream(mqttStreamName)
		if err != nil || si == nil {
			t.Errorf("looking up %q to impose a consumer limit: %v", mqttStreamName, err)
			return
		}
		cfg := si.Config
		cfg.ConsumerLimits.InactiveThreshold = time.Minute
		if _, err := asm.jsa.updateStream(&cfg); err != nil {
			t.Errorf("imposing the consumer inactivity limit: %v", err)
			return
		}
		// From here the level-up join refuses, its filter fails, and the rollback's removal
		// save is the next session-record store — the one that must go ambiguous.
		mqttTestSessStoreSuppressV.Store(true)
	}
	mqttTestSharedArbitratePauseV.Store(&h)
	t.Cleanup(func() { mqttTestSharedArbitratePauseV.Store(nil) })

	// Lifted at the store boundary itself: the seam runs AFTER the suppressed return, so the
	// suppressed write is the rollback's and the resolver's re-attempt of those same bytes
	// lands. Suppression is process-global, so this test does not run in parallel.
	// It also CAPTURES the ambiguity, which is the only place it can be captured: the readLoop
	// calls mqttResolveAfterPacket immediately after the packet, so the session is resolved
	// again before the test goroutine could ever observe isSeqUncertain. A fixture asserting
	// that flag afterwards fails against a correct broker.
	var suppressed atomic.Bool
	restore := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sp == sess {
			if mqttTestSessStoreSuppressV.Load() {
				suppressed.Store(true)
			}
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	t.Cleanup(func() {
		restore()
		mqttTestSessStoreSuppressV.Store(false)
	})

	// The packet fails as a whole — the rollback could not confirm its write — so the
	// connection goes, without a SUBACK. That is the route: mqttResolveAfterPacket runs
	// BEFORE the error closes it.
	testSendSubV5NoReply(t, c, 2, mqttV5SubFilter{topic: "$share/" + group + "/" + topic, opts: 1})

	// TWO arming confirmations, and neither is optional. Without them this test passes for the
	// wrong reason in two different ways: a filter that failed BEFORE its first group join
	// leaves no consumer to find gone and no charge to find refunded, and a rollback whose save
	// failed DEFINITELY registers no continuation at all — yet on this route the packet returns
	// an error either way, so no wire observation distinguishes them.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !staged.Load() {
			return fmt.Errorf("the after-group-create seam has not fired: the packet has not made " +
				"its first group join")
		}
		return nil
	})
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if !suppressed.Load() {
			return fmt.Errorf("no session-record store was suppressed, so the rollback save never " +
				"went AMBIGUOUS and no landed continuation was registered: this fixture is not " +
				"exercising the release")
		}
		return nil
	})

	// The continuation runs from the resolver. It is driven rather than waited on, exactly as
	// the affine ambiguity fixtures do, because the connection that issued the write is gone.
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		asm.reconcileResidentRecordState(s)
		if sess.isSeqUncertain() {
			return fmt.Errorf("the rollback write is still unresolved")
		}
		return nil
	})

	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		cons, err := asm.jsa.consumerList(mqttStreamName)
		if err != nil {
			return fmt.Errorf("listing consumers: %w", err)
		}
		for _, ci := range cons {
			if ci == nil {
				continue
			}
			if ci.Name == mainDur || ci.Name == levelDur {
				return fmt.Errorf("the group consumer %q of the failed filter is still present", ci.Name)
			}
		}
		return nil
	})

	// The half the pre-F-082 release could not do at all: the failing attempt has not written
	// its join to cons, so a cons-keyed refund refunded nothing and this budget stayed raised
	// for the life of the resident session.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		sess.mu.Lock()
		after := sess.tmaxack
		charged := len(sess.sharedCharged)
		sess.mu.Unlock()
		if after != budgetBefore {
			return fmt.Errorf("tmaxack = %d, want the pre-packet %d: the failed filter's group "+
				"charge outlived its membership", after, budgetBefore)
		}
		if charged != 0 {
			return fmt.Errorf("%d group charge(s) still recorded for a session holding no shared membership", charged)
		}
		return nil
	})
}

// testSendSubV5NoReply writes a SUBSCRIBE and does NOT read a response.
//
// The route under test fails the packet as a whole, so there is no SUBACK to read: the
// connection is closed by the packet's own error. Every shared helper here reads one, and a
// helper that fataled on EOF would report the behaviour under test as a failure.
func testSendSubV5NoReply(t *testing.T, c net.Conn, pi uint16, filters ...mqttV5SubFilter) {
	t.Helper()
	vh := newMQTTWriter(0)
	vh.WriteUint16(pi)
	vh.WriteVarInt(0) // empty properties
	for _, f := range filters {
		vh.WriteBytes([]byte(f.topic))
		vh.WriteByte(f.opts)
	}
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(vh.Len())
	w.Write(vh.Bytes())
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("writing the SUBSCRIBE under test: %v", err)
	}
}
