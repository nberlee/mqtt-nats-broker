//go:build mqtt_fulltests

package mqtt

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMQTTUnsubCommitResolverTokenRealRoute pins the record token commitUnsub hands both
// release arms when it runs as a LANDED CONTINUATION: the resolver's, which is open, and never
// the packet's, which is closed by then and refuses every write at entry.
//
// A real QoS 1 delivery has its send section open when the UNSUBSCRIBE stages, so the staging
// treats it as possibly attempted and leaves its register unvoided. The section then ends with
// nothing written, which makes the delivery proven unsent, and the release's purge collects its
// register. With both register-stream rungs failing, the NEW never-transmitted verdict that
// purge stages is in no record yet; only the session record can make it durable, under the
// token the release was handed. A closed token refuses that save while the consumer release
// proceeds, so until a later save lands the surviving register has no durable verdict, and a
// crash in that interval leaves its identifier reserved for the session's lifetime (see
// deleteInflightRegisters).
//
// The plain arm imposes a consumer inactivity limit on the messages stream after the delivery
// parks, as a deployment would out of band: that refuses draining, so the consumer reaches the
// deferred delete instead of being converted into a drain.
//
// The oracle is the RESOLVER's transaction release, sampled under sess.mu before its fence
// closes: the verdict revision must be credited as saved by then. That release follows the
// continuation's save and precedes the packet tail's verdict flush, which would otherwise save
// the verdict after the fact and hide a refused one. It checks durability credit at that
// release; it does not crash and recover, and it does not time the physical consumer delete.
//
// Mutation M1: run the continuation as commitUnsub(packetTok) and both arms fail.
// Mutation M2: hand releaseUnsubSharedTargets the packet's token and the shared arm fails.
// Mutation M3: hand execDeferredConsumerDelete the packet's token and the plain arm fails.
//
// NOT covered: the token commitUnsub hands secureRegCancellations for the STAGED
// cancellations. Where that read is reachable (one staged tombstone succeeding beside one
// failing) every verdict the save would carry is already durable: the failed one in the landed
// packet record, the forgotten one as a tombstone.
func TestMQTTUnsubCommitResolverTokenRealRoute(t *testing.T) {
	for _, tc := range []struct {
		name, filter, topic string
		shared              bool
	}{
		{name: "shared", filter: "$share/realgrp/real/shared", topic: "real/shared", shared: true},
		{name: "plain", filter: "real/plain", topic: "real/plain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testUnsubCommitResolverTokenRealRoute(t, tc.filter, tc.topic, tc.shared)
		})
	}
}

func testUnsubCommitResolverTokenRealRoute(t *testing.T, filter, topic string, shared bool) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "unsubrealroute"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: mqttV5ConnPropsSessionExpiry(30)},
		o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK reason = %v", reason)
	}
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v", codes)
	}
	testMQTTFlush(t, c, nil, r)
	sess := testUnsubAckSession(t, s, clientID)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)

	var dur string
	sess.mu.Lock()
	for _, cc := range sess.cons {
		if cc != nil && (cc.DeliverGroup != _EMPTY_) == shared {
			dur = cc.Durable
		}
	}
	sess.mu.Unlock()
	if dur == _EMPTY_ {
		t.Fatalf("the subscription to %q must have created its durable", filter)
	}

	// Park the delivery's send section at RESERVED (the pair confirmed, nothing written) until
	// the UNSUBSCRIBE has staged, then abort it: the section ends with nothing transmitted.
	parked := make(chan uint16, 1)
	release := make(chan struct{})
	var parkOnce sync.Once
	restoreWindow := setMQTTTestMarkerSendWindowHook(func(sp *mqttSession, pi uint16, window string) bool {
		if sp != sess || window != mqttMarkerSendReserved {
			return true
		}
		first := false
		parkOnce.Do(func() { first = true })
		if !first {
			return false
		}
		parked <- pi
		<-release
		return false
	})
	defer restoreWindow()
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	defer unpark()

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "unsubrealpub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 1, false, 1, topic, []byte("payload"), nil)
	var pi uint16
	select {
	case pi = <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the delivery's send section never opened")
	}
	sess.mu.Lock()
	var nonce string
	if ack := sess.pendingPublish[pi]; ack != nil {
		nonce = ack.regNonce
	}
	sess.mu.Unlock()
	if nonce == _EMPTY_ {
		t.Fatalf("the parked delivery %d has no register nonce", pi)
	}

	// Imposed only now, with the delivery parked: the limit is read by the UNSUBSCRIBE's
	// drain decision, and imposing it earlier would gate the delivery itself.
	if !shared {
		si, lerr := sess.jsa.lookupStream(mqttStreamName)
		if lerr != nil || si == nil {
			t.Fatalf("lookupStream: %v", lerr)
		}
		cfg := si.Config
		cfg.ConsumerLimits.InactiveThreshold = time.Hour
		if _, uerr := sess.jsa.updateStream(&cfg); uerr != nil {
			t.Fatalf("could not impose a consumer inactivity limit on %q: %v", mqttStreamName, uerr)
		}
	}

	// Both register-stream rungs fail from here on (the delivery's own pair is already
	// confirmed), so the release can make the verdict durable only in the session record.
	asm.mu.Lock()
	asm.testInflightDelete = func(uint64) error { return errors.New("test: register delete unavailable") }
	asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, errors.New("test: register stream unavailable")
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testInflightDelete, asm.testInflightStore = nil, nil
		asm.mu.Unlock()
	}()

	// The packet's write: staged, so the barrier is down and the open section was left
	// unvoided. Abort the section and wait for it to END before the write goes out, then
	// suppress the write so commitUnsub runs only from the resolution.
	var armed, disarmed atomicOnce
	var stagedVoided, sectionEnded atomic.Bool
	callbackDone := make(chan struct{}, 1)
	mqttTestDeliveryCallbackDoneHook.Set(func(sub *subscription, _ *mqttDeliveryCollector) {
		if sub.client != nil && sub.client.mqtt != nil && sub.client.mqtt.sess == sess {
			select {
			case callbackDone <- struct{}{}:
			default:
			}
		}
	})
	defer mqttTestDeliveryCallbackDoneHook.Set(nil)
	// Closed when the ARMED invocation returns. Cleanup must not clear the suppression while
	// that invocation can still set it: restoring the hook pointer stops new invocations,
	// not one already running.
	saveHookReturned := make(chan struct{})
	restoreSave := setMQTTTestSessSaveHook(func(sp *mqttSession) error {
		if sp != sess || !armed.fire() {
			return nil
		}
		defer close(saveHookReturned)
		sp.mu.Lock()
		_, voided := sp.voidedRegNonces[nonce]
		sp.mu.Unlock()
		stagedVoided.Store(voided)
		unpark()
		// The parked callback returns only after its section has ended.
		select {
		case <-callbackDone:
			sectionEnded.Store(true)
		case <-time.After(10 * time.Second):
		}
		mqttTestSessStoreSuppressV.Store(true)
		return nil
	})
	defer restoreSave()
	restoreResult := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sp == sess && armed.fired() && disarmed.fire() {
			mqttTestSessStoreSuppressV.Store(false)
		}
		return resp, err
	})
	defer restoreResult()
	defer func() {
		// Either this claims the arming, so no invocation can set the suppression any more, or
		// an invocation already holds it, and cleanup waits for that one to return first.
		if !armed.fire() {
			unpark()
			<-saveHookReturned
		}
		mqttTestSessStoreSuppressV.Store(false)
	}()

	var oracleMu sync.Mutex
	var oracleFired, durableAtRelease bool
	restoreRelease := setMQTTTestRecordTxReleaseHook(func(sp *mqttSession) {
		if sp != sess {
			return
		}
		if _, staged := sp.voidedRegNonces[nonce]; !staged {
			return
		}
		oracleMu.Lock()
		defer oracleMu.Unlock()
		if !oracleFired {
			oracleFired = true
			durableAtRelease = sp.voidedRegsRev == sp.voidedRegsSavedRev
		}
	})
	defer restoreRelease()

	codes, _ := testMQTTUnsubV5Ex(t, c, r, 2, []string{filter})
	restoreRelease()
	if len(codes) != 1 {
		t.Fatalf("UNSUBACK = %v", codes)
	}
	if !armed.fired() {
		t.Fatal("the packet's record write was never suppressed")
	}
	sess.mu.Lock()
	drains := len(sess.drains)
	sess.mu.Unlock()
	if drains != 0 {
		t.Fatal("the UNSUBSCRIBE converted the consumer into a drain; the deferred delete was never reached")
	}
	if !sectionEnded.Load() {
		t.Fatal("the parked delivery's callback never returned after its section was aborted")
	}
	if stagedVoided.Load() {
		t.Fatal("the staging voided the open section's register; the route under test was not reached")
	}
	oracleMu.Lock()
	fired, durable := oracleFired, durableAtRelease
	oracleMu.Unlock()
	if !fired {
		t.Fatal("the release never staged the aborted delivery's verdict")
	}
	if !durable {
		t.Fatalf("the verdict for the aborted delivery on %q was not credited when the resolver's transaction "+
			"closed: the release saved under a CLOSED record token", dur)
	}
}
