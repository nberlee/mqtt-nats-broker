//go:build mqtt_fulltests

// End-to-end proofs that the success CONNACK follows a COMPLETE restoration, or
// the CONNECT is refused (F-050, sess_restore_gate.go).
//
// The defect these are written against: a resuming CONNECT acknowledged the
// session with Session Present = 1 and only THEN restored its subscriptions, so
// every per-filter restore failure was dropped in silence — the record kept the
// membership, no queue subscription and no group consumer existed, and the client,
// which is not required to re-subscribe, waited forever. There is no SUBACK on
// that path to carry the failure and no channel to report it, so the only truthful
// disposition is not to acknowledge at all.
//
// Each fixture names the EXIT it fails. F-050 has five reachable exits and a
// remedy demonstrated on one says nothing about the others, so the injection is
// per exit (asm.testRestoreFilterFail) and runs AFTER the real call, which is what
// makes each test evidence that the production route was reached.

package mqtt

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

// testRestoreFailExit installs a one-exit failure for the rest of the test. The
// closure touches no *testing.T, so it needs no hook gate; the counter is what a
// test reads to prove the route was taken.
func testRestoreFailExit(t *testing.T, asm *mqttAccountSessionManager, exit string, err error) *atomic.Int32 {
	t.Helper()
	var hits atomic.Int32
	asm.mu.Lock()
	prev := asm.testRestoreFilterFail
	asm.testRestoreFilterFail = func(gotExit, _ string) error {
		if gotExit != exit {
			return nil
		}
		hits.Add(1)
		return err
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testRestoreFilterFail = prev
		asm.mu.Unlock()
	})
	return &hits
}

// testRestoreGateBlockCounter counts, per message class, the deliveries the
// restore verdict blocked. Tests assert the block POSITIVELY: "the client received
// nothing" also passes when nothing was ever delivered, which is exactly how a
// mutation that deletes a gate consultation survives.
type testRestoreGateBlockCounter struct {
	mu   sync.Mutex
	seen map[string]int
}

func testWatchRestoreGateBlocks(t *testing.T) *testRestoreGateBlockCounter {
	t.Helper()
	ctr := &testRestoreGateBlockCounter{seen: map[string]int{}}
	restore := setMQTTTestRestoreGateBlocked(func(kind string) {
		ctr.mu.Lock()
		ctr.seen[kind]++
		ctr.mu.Unlock()
	})
	t.Cleanup(restore)
	return ctr
}

func (c *testRestoreGateBlockCounter) count(kind string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen[kind]
}

// testPauseRestoreAt runs body while the CONNECT is held at one named boundary of
// the restore. The gate is registered BEFORE the hook restore so a call already
// inside the closure is waited out rather than raced — cleanups run
// last-registered-first, which is the ordering hook_gate_test.go documents.
// The returned counter is how a test proves the pause FIRED. A seam that never
// fires makes every assertion downstream of it vacuous, which is the way a fixture
// passes without reaching the code it names.
func testPauseRestoreAt(t *testing.T, at string, body func()) *atomic.Int32 {
	t.Helper()
	g := newTestHookGate(t)
	var fired atomic.Int32
	var once sync.Once
	restore := setMQTTTestRestorePause(func(gotAt string) {
		if gotAt != at {
			return
		}
		if !g.enter() {
			return
		}
		defer g.leave()
		once.Do(func() {
			fired.Add(1)
			body()
		})
	})
	t.Cleanup(restore)
	return &fired
}

// ---------------------------------------------------------------------------
// T1-T3: a failed exit refuses the CONNECT, and a retry restores completely
// ---------------------------------------------------------------------------

// The three exits a saved SHARED QoS 2 filter reaches, each proved to refuse. The
// assertion set is the same for all three because the disposition is: the reason
// is told to the client, the record still names the membership, and a retry with
// the injection cleared both restores and DELIVERS — which is the property the old
// behavior violated while reporting Session Present = 1.
func TestRestoreRefusesConnectWhenAnExitFails(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	for _, exit := range []string{"affine", "pubrel", "consumer"} {
		t.Run(exit, func(t *testing.T) {
			b := testQ2AffSetup(t, "f050"+exit)
			id := "f050-" + exit

			// Establish the saved shared membership on a first, healthy connection.
			c, _ := b.member(t, id, 300)
			b.bind(t, id)
			c.Close()

			hits := testRestoreFailExit(t, b.asm, exit, errRestoreExitInjected)

			// The resume must be REFUSED, and the client told why.
			cc, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
				clientID:   id,
				cleanStart: false,
				props:      mqttV5ConnPropsSessionExpiry(300),
			}, b.o.MQTT.Host, b.o.MQTT.Port)
			sessp, reason, _ := testMQTTReadConnAckV5(t, rr)
			if reason != mqttReasonImplementationSpecificError {
				t.Fatalf("CONNACK reason = 0x%x, want 0x%x (the refusal)", reason, mqttReasonImplementationSpecificError)
			}
			if sessp {
				t.Fatal("a refused resume reported Session Present")
			}
			if got := hits.Load(); got == 0 {
				t.Fatalf("the %q exit was never reached, so this fixture proves nothing", exit)
			}
			cc.Close()

			// The membership is still the record's, so a healthy retry restores it.
			b.asm.mu.Lock()
			b.asm.testRestoreFilterFail = nil
			b.asm.mu.Unlock()

			c2, r2 := b.reconnect(t, id, 300)
			defer c2.Close()

			// And the restored subscription DELIVERS, which is what the silent drop
			// took away: the old behavior left this client acknowledged with a durable
			// membership and no delivery path at all.
			b.publish(t, 1, "after-refusal")
			pi := testRestoreAwaitPublish(t, r2, "after-refusal")
			b.completeExchange(t, c2, r2, pi)
		})
	}
}

// ---------------------------------------------------------------------------
// T14: nothing is transmitted before the CONNACK
// ---------------------------------------------------------------------------

// A QoS 0 message published while the restore is still working must not reach the
// client, because [MQTT-3.2.0-1] makes the CONNACK the first packet it may see and
// restoration installs live delivery interest before it. The block is asserted
// POSITIVELY through the counter: "the client got nothing yet" would also pass if
// the message had simply not been routed.
func TestNoQoS0PublishPrecedesTheConnAck(t *testing.T) {
	b := testQ2AffSetup(t, "f050q0")
	id := "f050-qos0"
	plainTopic, plainSid := "f050q0/plain", "f050q0.plain"

	// A saved PLAIN QoS 0 subscription: the raw-subject subscription is what the
	// QoS 0 delivery path uses, and it is installed by restoration.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: plainTopic, opts: 0}}); len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK = %v, want [0]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	c.Close()

	ctr := testWatchRestoreGateBlocks(t)

	// The publisher connects BEFORE the pause. It cannot connect during one: the
	// restore holds the subscriptions barrier for the whole of processSubs, and a
	// CONNECT that arrives inside that window blocks until it is released — so a
	// fixture that dialled from inside the pause would deadlock itself rather than
	// test anything.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f050q0-pub", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)

	// Publish while the resuming CONNECT is held after its restore and before its
	// CONNACK. This is the interval a mutation that opens the verdict early, or
	// drops the QoS 0 consultation, leaves open.
	// Paused at the PER-FILTER boundary — after this filter's installs and before
	// the rest of the restore — not merely before the CONNACK. That is the interval
	// a mutation which arms the verdict late (after processSubs) leaves unguarded;
	// at the end-of-restore pause such a mutation has already armed it and would
	// survive. The sid is the NATS-converted subject, which is how the session keys
	// its subscriptions.
	fired := testPauseRestoreAt(t, "filter:"+plainSid, func() {
		testMQTTPublishV5(t, cp, 0, 0, plainTopic, []byte("pre-connack"))
		// Wait for the delivery to reach the callback and be refused. The bound is
		// generous because the publish is asynchronous to the callback.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && ctr.count("qos0") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	sessp, reason, _ := testMQTTReadConnAckV5(t, r2)
	if reason != 0 || !sessp {
		t.Fatalf("resume CONNACK reason=0x%x sessionPresent=%v, want 0 and true", reason, sessp)
	}
	if fired.Load() == 0 {
		t.Fatalf("the restore never paused at %q, so nothing downstream of it was exercised", "filter:"+plainSid)
	}
	if got := ctr.count("qos0"); got == 0 {
		t.Fatal("no QoS 0 delivery was blocked, so this fixture never reached the drop")
	}
	// The CONNACK was the first packet: a PUBLISH ahead of it would have been read
	// as the CONNACK above and failed the packet-type check inside the helper.
	if _, _, ready := testMQTTReadPacketReady(r2, 300*time.Millisecond); ready {
		t.Fatal("an unexpected packet followed the CONNACK; the pre-CONNACK publish leaked")
	}
}

// ---------------------------------------------------------------------------
// T15: a 3.1.1 resume still force-leaves, and restores nothing shared
// ---------------------------------------------------------------------------

// Restoration now runs BEFORE the CONNACK while the forced leave still runs after
// it, so the exclusion of shared filters for a non-v5 connection had to become
// explicit. Without it the CONNECT would provision an affine consumer, a PUBREL
// consumer and a group join for memberships the forced leave then removes.
func TestThreeOneOneResumeRestoresNoSharedFilter(t *testing.T) {
	b := testQ2AffSetup(t, "f050v3")
	id := "f050-v311"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	filter := "$share/" + b.group + "/" + b.topic
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, id)
	c.Close()

	// Any shared exit failing would refuse the CONNECT if the filter were restored.
	hits := testRestoreFailExit(t, b.asm, "consumer", errRestoreExitInjected)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	if got := hits.Load(); got != 0 {
		t.Fatalf("a 3.1.1 resume reached the shared-consumer exit %d time(s); shared filters must not be restored for it", got)
	}
}

var errRestoreExitInjected = &testRestoreExitError{}

type testRestoreExitError struct{}

func (*testRestoreExitError) Error() string { return "injected restore-exit failure" }

// testRestoreAwaitPublish reads until a PUBLISH carrying want arrives and returns
// its packet identifier, tolerating duplicate redeliveries (AckWait is short in
// these fixtures, so a copy this test has not answered yet is legitimately
// re-offered).
func testRestoreAwaitPublish(t *testing.T, r *mqttReader, want string) uint16 {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, time.Second)
		if !ready {
			continue
		}
		start := r.pos
		if pb&mqttPacketMask != mqttPacketPub {
			r.pos = start + pl
			continue
		}
		topic, err := r.readString("topic")
		if err != nil {
			t.Fatalf("reading the delivered topic: %v", err)
		}
		qos := mqttGetQoS(pb)
		var pi uint16
		if qos > 0 {
			if pi, err = r.readUint16("pi"); err != nil {
				t.Fatalf("reading the delivered packet identifier: %v", err)
			}
		}
		// A v5 PUBLISH carries a property block between the identifier and the
		// payload; without consuming it the payload comparison reads its length
		// byte and never matches.
		if _, err := r.readProperties(mqttPacketPub); err != nil {
			t.Fatalf("reading the delivered properties: %v", err)
		}
		payload := make([]byte, start+pl-r.pos)
		copy(payload, r.buf[r.pos:start+pl])
		r.pos = start + pl
		if string(payload) == want {
			return pi
		}
		t.Logf("skipping delivery of %q on %q while awaiting %q", payload, topic, want)
	}
	t.Fatalf("no PUBLISH carrying %q arrived", want)
	return 0
}

// ---------------------------------------------------------------------------
// T6/T7: what the refusal must NOT do
// ---------------------------------------------------------------------------

// The two consequences that made "fail the CONNECT" the wrong answer when the
// F-064 review looked at it, and that refusing BEFORE the CONNACK avoids.
//
// A refusal after the CONNACK would go through ordinary close bookkeeping, which
// marks this connection's Will owed and publishes it; and the requested Session
// Expiry Interval would already have been granted and persisted, so a requested 0
// would have the close path discard a persistent session outright — the client's
// retry then finds Session Present 0 with its state gone. Both are asserted here
// because both are the reason the reorder exists rather than a smaller patch.
func TestRefusedRestorePublishesNoWillAndKeepsTheSession(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	b := testQ2AffSetup(t, "f050keep")
	id := "f050-keep"
	willTopic := "f050keep/will"

	// A watcher for the Will, on its own long-lived connection.
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f050keep-watch", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cw.Close()
	testMQTTReadConnAckV5(t, rw)
	if codes := testMQTTSubV5(t, cw, rw, 1, []mqttV5SubFilter{{topic: willTopic, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("watcher SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, cw, nil, rw)

	// Establish the saved shared membership.
	c, _ := b.member(t, id, 300)
	b.bind(t, id)
	c.Close()

	hits := testRestoreFailExit(t, b.asm, "affine", errRestoreExitInjected)

	// The refused resume asks for expiry 0 AND carries a Will. Neither may take
	// effect: the session must survive and the Will must not be published.
	cc, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(0),
		will:       &mqttWill{topic: []byte(willTopic), message: []byte("must-not-fire"), qos: 1},
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, rr); reason != mqttReasonImplementationSpecificError {
		t.Fatalf("CONNACK reason = 0x%x, want 0x%x", reason, mqttReasonImplementationSpecificError)
	}
	if hits.Load() == 0 {
		t.Fatal("the affine exit was never reached, so this fixture proves nothing")
	}
	cc.Close()

	// No Will. The watcher must see nothing at all on the Will topic.
	testMQTTExpectNothingFor(t, rw, 2*time.Second)

	// And the session survived its own requested expiry of 0, because that
	// interval was never granted: the grant point is after everything that can
	// refuse.
	b.asm.mu.Lock()
	b.asm.testRestoreFilterFail = nil
	b.asm.mu.Unlock()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	sessp, reason, _ := testMQTTReadConnAckV5(t, r2)
	if reason != 0 {
		t.Fatalf("the retry's CONNACK reason = 0x%x, want 0", reason)
	}
	if !sessp {
		t.Fatal("the refused resume destroyed the session: its retry got Session Present 0")
	}
}

// ---------------------------------------------------------------------------
// T17: the happy path still works after the reorder
// ---------------------------------------------------------------------------

// The regression guard for the reorder itself. Restoration now runs before the
// CONNACK with every delivery withheld behind the connection's verdict, so the
// ordinary resume — the one every client does — has to still restore its shared
// QoS 2 filter and deliver through it, with the backlog arriving after the CONNACK
// rather than being dropped or stalled to AckWait.
func TestSharedQoS2ResumeStillDeliversAfterTheReorder(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	b := testQ2AffSetup(t, "f050happy")
	id := "f050-happy"

	c, _ := b.member(t, id, 300)
	b.bind(t, id)
	c.Close()

	// Published while the client is away, so the resume owes it.
	b.publish(t, 1, "offline-backlog")

	c2, r2 := b.reconnect(t, id, 300)
	defer c2.Close()
	pi := testRestoreAwaitPublish(t, r2, "offline-backlog")
	b.completeExchange(t, c2, r2, pi)

	// And a message published after the resume is delivered too, which proves the
	// verdict was opened rather than merely never consulted.
	b.publish(t, 2, "after-resume")
	pi2 := testRestoreAwaitPublish(t, r2, "after-resume")
	b.completeExchange(t, c2, r2, pi2)
}

// ---------------------------------------------------------------------------
// T9: no stored PUBREL precedes the CONNACK
// ---------------------------------------------------------------------------

// The PUBREL callback honoured nothing before this change — not `closed`, not any
// gate — and it writes a PUBREL directly. It is also the callback whose
// subscription is installed by a bare c.processSub, whose NATS handler goes live
// BEFORE processSub returns, so a stored PUBREL can enter it with no
// per-subscription metadata at all. That pair is why the verdict is
// connection-scoped and armed before the first subscribe, and this is the fixture
// that holds it.
//
// The exchange is deliberately left half-finished: the client PUBRECs and
// disconnects without answering the PUBREL, so the PUBREL is stored and redelivered
// on the resume — into the pre-CONNACK window.
func TestNoStoredPubRelPrecedesTheConnAck(t *testing.T) {
	b := testQ2AffSetup(t, "f050rel")
	id := "f050-pubrel"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: b.topic, opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK = %v, want [2]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, id)

	b.publish(t, 1, "half-finished")
	pi := testRestoreAwaitPublish(t, r, "half-finished")
	// PUBREC only: the broker stores the PUBREL, and this client goes away without
	// completing the handshake.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	// No flush: the broker answers a PUBREC with the PUBREL immediately, so a
	// PINGREQ round trip would meet that PUBREL instead of a PINGRESP. Closing here
	// leaves the exchange unfinished, which is the point — the stored PUBREL is
	// redelivered on the resume.
	c.Close()

	ctr := testWatchRestoreGateBlocks(t)
	fired := testPauseRestoreAt(t, "before-connack", func() {
		// The stored PUBREL is redelivered on this resume. Wait for its callback to
		// be refused by the verdict; nothing may be on the wire yet.
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) && ctr.count("pubrel") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	// The CONNACK must be the FIRST packet. A PUBREL ahead of it would be read here
	// and fail the packet-type check inside the helper — which is the assertion.
	sessp, reason, _ := testMQTTReadConnAckV5(t, r2)
	if reason != 0 || !sessp {
		t.Fatalf("resume CONNACK reason=0x%x sessionPresent=%v, want 0 and true", reason, sessp)
	}
	if fired.Load() == 0 {
		t.Fatal("the restore never paused before the CONNACK, so nothing downstream was exercised")
	}
	if got := ctr.count("pubrel"); got == 0 {
		t.Fatal("no PUBREL delivery was blocked by the verdict; this fixture never reached the PUBREL gate")
	}
	// And after the CONNACK it does arrive, so the verdict withheld it rather than
	// losing it.
	testRestoreAwaitPubRel(t, r2, pi)
}

// testRestoreAwaitPubRel reads until the PUBREL for pi arrives, skipping duplicate
// PUBLISH redeliveries (AckWait is short in these fixtures).
func testRestoreAwaitPubRel(t *testing.T, r *mqttReader, want uint16) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, time.Second)
		if !ready {
			continue
		}
		start := r.pos
		if pb&mqttPacketMask != mqttPacketPubRel {
			r.pos = start + pl
			continue
		}
		got, err := r.readUint16("pi")
		if err != nil {
			t.Fatalf("reading the PUBREL packet identifier: %v", err)
		}
		r.pos = start + pl
		if got == want {
			return
		}
	}
	t.Fatalf("no PUBREL for packet identifier %d arrived after the CONNACK", want)
}

// ---------------------------------------------------------------------------
// The open is strictly AFTER the CONNACK
// ---------------------------------------------------------------------------

// The ordering the whole design rests on, tested at the one boundary that
// discriminates it. Pausing before the CONNACK cannot: a mutation that opens the
// verdict immediately BEFORE sendConnAck still has it closed at that point, so both
// the correct code and the mutant block a delivery there.
//
// This pause fires after the CONNACK is written and before the open. Correct code
// still has the verdict closed, so a QoS 0 delivery arriving here is dropped and
// counted; a mutant that opened it earlier transmits instead — and on the wire that
// PUBLISH can overtake the CONNACK, because closing the verdict's channel wakes the
// delivery goroutine to race the CONNACK write.
//
// The window this pins is microseconds wide in production. It is still the
// invariant: docs/CONFORMANCE.md states the no-transmission window as ending at the
// OPEN rather than at the CONNACK for exactly this reason.
func TestTheRestoreVerdictOpensAfterTheConnAck(t *testing.T) {
	b := testQ2AffSetup(t, "f050order")
	id := "f050-order"
	plainTopic := "f050order/plain"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: plainTopic, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	c.Close()

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f050order-pub", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)

	ctr := testWatchRestoreGateBlocks(t)
	fired := testPauseRestoreAt(t, "before-open", func() {
		testMQTTPublishV5(t, cp, 0, 0, plainTopic, []byte("between-connack-and-open"))
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && ctr.count("qos0") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("resume CONNACK reason = 0x%x, want 0", reason)
	}
	// The CONNACK is written BEFORE this pause, so reading it above can return while
	// the pause body is still running. Both assertions therefore WAIT rather than
	// sampling once — sampling is a race that reports the correct code as broken.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		if fired.Load() == 0 {
			return fmt.Errorf("the restore has not paused between the CONNACK and the open yet")
		}
		if got := ctr.count("qos0"); got == 0 {
			return fmt.Errorf("no QoS 0 delivery was withheld between the CONNACK and the open")
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// T8: a refused restore must not delete a durable the record still names
// ---------------------------------------------------------------------------

// The reachable destructive shape, and the reason the restore path's per-filter
// cleanup is narrowed. A multi-level filter is TWO consumers — the filter itself
// and its level-up companion — and if the companion fails after the main half
// succeeded, the cleanup arrives holding the main half's live config. It used to
// delete that durable, which was survivable while the CONNECT was accepted anyway
// (the next resume's reconciliation repaired it) and is the wrong disposition for a
// refused one: the record still names it and its backlog is retained by its
// interest.
//
// Mutation check: drop the `fromSubProto` guard on the cleanup's consumer disposal
// and the held message below is gone, because its durable was deleted with the
// interest that retained it.
func TestRefusedRestoreKeepsADurableTheRecordNames(t *testing.T) {
	b := testQ2AffSetup(t, "f050keepdur")
	id := "f050-keepdur"
	// A multi-level filter, so restoration builds a level-up companion beside it.
	const filter = "f050keepdur/a/#"
	const topic = "f050keepdur/a/x"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, id)
	c.Close()

	// Held by the main half's durable while the client is away.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f050keepdur-pub", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, topic, []byte("held-by-the-durable"))
	testMQTTReadPubAckV5(t, rp, 1)
	cp.Close()

	// Fail ONLY the companion, after the main half has succeeded and its config is
	// in the cleanup's hands.
	hits := testRestoreFailExit(t, b.asm, "consumer-fwc", errRestoreExitInjected)

	cc, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, rr); reason != mqttReasonImplementationSpecificError {
		t.Fatalf("CONNACK reason = 0x%x, want 0x%x (the refusal)", reason, mqttReasonImplementationSpecificError)
	}
	if hits.Load() == 0 {
		t.Fatal("the companion exit was never reached, so this fixture proves nothing")
	}
	cc.Close()

	// The held message survives the refusal, which is what proves the main half's
	// durable — and the interest retaining its backlog — was not deleted.
	b.asm.mu.Lock()
	b.asm.testRestoreFilterFail = nil
	b.asm.mu.Unlock()

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("the retry's CONNACK reason = 0x%x, want 0", reason)
	}
	pi := testRestoreAwaitPublish(t, r2, "held-by-the-durable")
	if pi == 0 {
		t.Fatal("the held delivery carried no packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
}

// ---------------------------------------------------------------------------
// A refusal records its withheld delivery, so an unraced retry sees DUP = 0
// ---------------------------------------------------------------------------

// **This test replaces one whose claim was WITHDRAWN.** Its predecessor,
// TestRebindRacingARefusalKeepsTheWithheldMarker, asserted that a rebind RACING the
// refusal still keeps the marker. Three holistic rounds established that no
// mechanism this item justifies can deliver that: draining the woken callbacks is
// not an admission barrier, and deciding on the durable instead of the client
// removes the race but not the lateness. HEAD already treats the boundary as
// best-effort and names the same consequence. So the racing claim is withdrawn, the
// residual is stated in docs/CONFORMANCE.md, and what remains asserted here is the
// narrower property that IS delivered — deliberately renamed rather than left under
// the old name with a weakened interleaving.
//
// What is still proved: a refusal wakes its withheld QoS 1/2 delivery, that delivery
// records itself as never transmitted, and the retry that follows receives the
// message as a FIRST transmission with DUP = 0. No rebind races the callback here.
//
// Mutation check: drop the withheld recording on the verdict's arm and the retry's
// delivery arrives DUP = 1.
func TestRefusedRestoreRecordsItsWithheldDelivery(t *testing.T) {
	b := testQ2AffSetup(t, "f050drain")
	id := "f050-drain"
	// The session keys its consumer entries by the NATS-converted subject, not by
	// the MQTT filter, so the two spellings are both needed.
	const topic, sid = "f050drain/plain", "f050drain.plain"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, id)
	sess := testMQTTResidentSession(t, b.s, id)
	sess.mu.Lock()
	cc0 := sess.cons[sid]
	if cc0 == nil {
		sess.mu.Unlock()
		t.Fatalf("no consumer entry for %q; cons has %d entries", sid, len(sess.cons))
	}
	dur := cc0.Durable
	sess.mu.Unlock()
	c.Close()

	// A QoS 1 message the session is owed, held by its durable while it is away.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f050drain-pub", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, topic, []byte("owed"))
	testMQTTReadPubAckV5(t, rp, 1)

	blocked := make(chan struct{}, 8)
	restore := setMQTTTestRestoreGateBlocked(func(kind string) {
		if kind != "qos12" {
			return
		}
		select {
		case blocked <- struct{}{}:
		default:
		}
	})
	defer restore()

	// THE OWED DELIVERY IS PARKED AT THE RESTORE GATE BEFORE THE RESTORE FAILS. The durable pushes
	// it once the restored subscription is installed, and the refusal's teardown removes that
	// subscription; a push that loses that race (a loaded machine) is never withheld at all. So the
	// injected failure waits, bounded, until a delivery of this session is about to wait on a gate.
	parked := make(chan struct{})
	var parkOnce sync.Once
	aboutToWait := func(s *mqttSession) {
		if s.id == id {
			parkOnce.Do(func() { close(parked) })
		}
	}
	mqttTestGateAboutToWaitV.Store(&aboutToWait)
	defer mqttTestGateAboutToWaitV.Store(nil)
	var hits atomic.Int32
	var sawParked atomic.Bool
	b.asm.mu.Lock()
	prevFail := b.asm.testRestoreFilterFail
	b.asm.testRestoreFilterFail = func(gotExit, _ string) error {
		if gotExit != "consumer" {
			return nil
		}
		hits.Add(1)
		select {
		case <-parked:
			sawParked.Store(true)
		case <-time.After(10 * time.Second):
		}
		return errRestoreExitInjected
	}
	b.asm.mu.Unlock()
	defer func() {
		b.asm.mu.Lock()
		b.asm.testRestoreFilterFail = prevFail
		b.asm.mu.Unlock()
	}()

	cc, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, rr); reason != mqttReasonImplementationSpecificError {
		t.Fatalf("CONNACK reason = 0x%x, want 0x%x (the refusal)", reason, mqttReasonImplementationSpecificError)
	}
	if hits.Load() == 0 {
		t.Fatal("the consumer exit was never reached, so this fixture proves nothing")
	}
	if !sawParked.Load() {
		t.Fatal("no delivery was parked at the restore gate before the restore failed")
	}
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("no QoS 1/2 delivery was withheld by the verdict; this fixture never reached the recording")
	}
	cc.Close()

	// The marker, waited for rather than sampled: nothing here races it, so it must
	// appear, and the retry below is what it protects.
	checkFor(t, 15*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		for k := range sess.deferred {
			if k.jsDur == dur {
				return nil
			}
		}
		return fmt.Errorf("the refusal did not record its withheld delivery for %q", dur)
	})

	b.asm.mu.Lock()
	b.asm.testRestoreFilterFail = nil
	b.asm.mu.Unlock()
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("the retry's CONNACK reason = 0x%x, want 0", reason)
	}

	// The owed message is the client's FIRST sight of it, which is the whole point
	// of recording the withholding.
	pi := testRestoreAwaitPublishDUP(t, r2, "owed", false)
	if pi == 0 {
		t.Fatal("the owed delivery carried no packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
}

// testRestoreAwaitPublishDUP is testRestoreAwaitPublish with an assertion on the
// DUP flag of the matching delivery, which is the whole point wherever a
// never-transmitted marker is under test.
func testRestoreAwaitPublishDUP(t *testing.T, r *mqttReader, want string, wantDUP bool) uint16 {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r, time.Second)
		if !ready {
			continue
		}
		start := r.pos
		if pb&mqttPacketMask != mqttPacketPub {
			r.pos = start + pl
			continue
		}
		if _, err := r.readString("topic"); err != nil {
			t.Fatalf("reading the delivered topic: %v", err)
		}
		var pi uint16
		if mqttGetQoS(pb) > 0 {
			var err error
			if pi, err = r.readUint16("pi"); err != nil {
				t.Fatalf("reading the delivered packet identifier: %v", err)
			}
		}
		if _, err := r.readProperties(mqttPacketPub); err != nil {
			t.Fatalf("reading the delivered properties: %v", err)
		}
		payload := make([]byte, start+pl-r.pos)
		copy(payload, r.buf[r.pos:start+pl])
		r.pos = start + pl
		if string(payload) != want {
			continue
		}
		if got := pb&mqttPubFlagDup != 0; got != wantDUP {
			t.Fatalf("delivery of %q carried DUP=%v, want %v", want, got, wantDUP)
		}
		return pi
	}
	t.Fatalf("no PUBLISH carrying %q arrived", want)
	return 0
}

// ---------------------------------------------------------------------------
// The arm is unconditional, so a reused session object is never stranded
// ---------------------------------------------------------------------------

// A refused resume must not strand the connection that follows it. The abandoned
// verdict is deliberately left installed, so this proves the next CONNECT's own arm
// replaces it and its subscriptions deliver.
//
// What this test does NOT prove, stated because an earlier revision claimed it did:
// that the arm has to be UNCONDITIONAL. A mutation making it conditional on the
// record naming at least one filter survives this fixture, and the argument in
// armRestoreGate says why — a refusal leaves the record's filters alone, so the
// following CONNECT always has some, and the no-filter case is unreachable. The
// unconditional arm is defence in depth.
func TestRefusedRestoreDoesNotStrandTheNextConnection(t *testing.T) {
	b := testQ2AffSetup(t, "f050zero")
	id := "f050-zero"
	const topic = "f050zero/late"

	// A session with a saved filter, so the first resume has something to refuse on.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "f050zero/saved", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, id)
	c.Close()

	// Refuse a resume, which leaves this session object holding an abandoned verdict.
	hits := testRestoreFailExit(t, b.asm, "consumer", errRestoreExitInjected)
	cc, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, rr); reason != mqttReasonImplementationSpecificError {
		t.Fatalf("CONNACK reason = 0x%x, want 0x%x", reason, mqttReasonImplementationSpecificError)
	}
	if hits.Load() == 0 {
		t.Fatal("the exit was never reached, so no abandoned verdict was left behind")
	}
	cc.Close()
	b.asm.mu.Lock()
	b.asm.testRestoreFilterFail = nil
	b.asm.mu.Unlock()

	// Now a CLEAN START on the same client ID. It clears the record, so its restore
	// has no filters at all — the case a conditional arm would skip — and it lands
	// on the SAME resident session object.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: true,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("clean-start CONNACK reason = 0x%x, want 0", reason)
	}
	if codes := testMQTTSubV5(t, c2, r2, 1, []mqttV5SubFilter{{topic: topic, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("post-refusal SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c2, nil, r2)

	// The live subscription must DELIVER. Behind a stranded verdict it would not.
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f050zero-pub", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, topic, []byte("after-clean-start"))
	testMQTTReadPubAckV5(t, rp, 1)

	pi := testRestoreAwaitPublish(t, r2, "after-clean-start")
	if pi == 0 {
		t.Fatal("the post-refusal delivery carried no packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
}

// ---------------------------------------------------------------------------
// The shared charge a refused restore added is refunded at its call site
// ---------------------------------------------------------------------------

// A shared group join charges MaxAckPending against the session's total, and the
// config it returns says nothing about whether THIS call charged it. On a refused
// restore the durable is deliberately left alone, so the charge has to be given
// back by the amount the join reported — and it has to be given back at all, or it
// outlives its config: `addJSConsToSess` runs only once the whole filter succeeds,
// so the next resume sees no entry, charges again, and double-counts for the life
// of the session.
//
// Asserting on `tmaxack` directly, because the cost is invisible on the wire until
// the account's consumer budget is exhausted.
//
// Mutation check: drop the refund call and tmaxack is higher after the refusal.
func TestRefusedRestoreRefundsTheSharedChargeItAdded(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	b := testQ2AffSetup(t, "f050charge")
	id := "f050-charge"

	c, _ := b.member(t, id, 300)
	b.bind(t, id)
	sess := testMQTTResidentSession(t, b.s, id)
	c.Close()

	// The session object survives the disconnect, so this is the baseline the
	// refusal must return to.
	sess.mu.Lock()
	before := sess.tmaxack
	sess.mu.Unlock()

	// Fail the filter AFTER its group join has charged: the raw-subscription exit
	// runs later than the consumer join, which is what makes the charge outstanding
	// at the moment of failure.
	hits := testRestoreFailExit(t, b.asm, "consumer-fwc", errRestoreExitInjected)
	_ = hits

	// A multi-level shared filter, so the companion exit exists to fail.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id + "-ml",
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r2)
	if codes := testMQTTSubV5(t, c2, r2, 1, []mqttV5SubFilter{{topic: "$share/" + b.group + "/f050charge/a/#", opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK = %v, want [2]", codes)
	}
	testMQTTFlush(t, c2, nil, r2)
	sessML := testMQTTResidentSession(t, b.s, id+"-ml")
	c2.Close()

	// Shared consumer configurations are deliberately NOT persisted, so a
	// record-sourced resume finds no entry and its group join CHARGES. A resident
	// object keeps its entries and takes the no-charge branch instead — which is
	// exactly the shape that makes this test vacuous — so the resident shared
	// entries and their charges are dropped here to reach the record-sourced case.
	sessML.mu.Lock()
	for sid, cc := range sessML.cons {
		if cc != nil && cc.DeliverGroup != _EMPTY_ {
			delete(sessML.cons, sid)
			if amt, ok := sessML.sharedCharged[cc.Durable]; ok {
				sessML.tmaxack -= amt
				delete(sessML.sharedCharged, cc.Durable)
			}
		}
	}
	beforeML := sessML.tmaxack
	sessML.mu.Unlock()

	// Refuse the resume at the companion exit, after the main half charged.
	c3, r3 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id + "-ml",
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	if _, reason, _ := testMQTTReadConnAckV5(t, r3); reason != mqttReasonImplementationSpecificError {
		t.Fatalf("CONNACK reason = 0x%x, want 0x%x (the refusal)", reason, mqttReasonImplementationSpecificError)
	}
	c3.Close()

	// The refusal must leave the accounting exactly where it found it. A charge the
	// refused restore added and did not give back would show here.
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		sessML.mu.Lock()
		after := sessML.tmaxack
		sessML.mu.Unlock()
		if after != beforeML {
			return fmt.Errorf("tmaxack = %d after a refused restore, want %d: a charge was not refunded", after, beforeML)
		}
		return nil
	})
	_ = before
	_ = sess
}

// ---------------------------------------------------------------------------
// No affine QoS 2 copy precedes the CONNACK
// ---------------------------------------------------------------------------

// The fourth delivery callback. It already honoured `closed`, so without the
// verdict a pre-CONNACK affine copy is not transmitted — it is REFUSED, returns
// unacknowledged, and only reappears on an AckWait redelivery. That is why this
// fixture asserts two things rather than one: that the copy is withheld by the
// VERDICT (positively, through the block counter) and that it then arrives promptly
// after the open rather than after an AckWait stall.
//
// Given F-072 — the affine path derives DUP from the JetStream delivery count — the
// DUP expectation on the post-open delivery is deliberately not asserted here; a
// copy that was offered and refused before trackPublish is exactly that finding's
// population.
//
// Mutation check: remove the gate consultation from mqttDeliverQ2AffineCb and the
// block counter stays at zero.
func TestNoAffineCopyPrecedesTheConnAck(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	b := testQ2AffSetup(t, "f050aff")
	id := "f050-affine"

	c, r := b.member(t, id, 300)
	b.bind(t, id)

	// Start a QoS 2 exchange and leave it unanswered, so the copy is owed to this
	// session and is re-offered on the resume — into the pre-CONNACK window.
	b.publish(t, 1, "affine-owed")
	if pi := testRestoreAwaitPublish(t, r, "affine-owed"); pi == 0 {
		t.Fatal("the affine delivery carried no packet identifier")
	}
	c.Close()

	ctr := testWatchRestoreGateBlocks(t)
	fired := testPauseRestoreAt(t, "before-connack", func() {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) && ctr.count("affine") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer c2.Close()
	// CONNACK first: an affine PUBLISH ahead of it would be read here and fail the
	// packet-type check inside the helper.
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("resume CONNACK reason = 0x%x, want 0", reason)
	}
	if fired.Load() == 0 {
		t.Fatal("the restore never paused before the CONNACK")
	}
	if got := ctr.count("affine"); got == 0 {
		t.Fatal("no affine copy was withheld by the verdict; this fixture never reached the affine gate")
	}

	// It arrives after the open. AckWait in this fixture is 400ms, so a bound well
	// under a second would be indistinguishable from a redelivery; the assertion
	// that matters here is that it arrives at all and that the counter above proves
	// the verdict, not the `closed` check, is what held it.
	pi := testRestoreAwaitPublish(t, r2, "affine-owed")
	if pi == 0 {
		t.Fatal("the affine copy never arrived after the open")
	}
	b.completeExchange(t, c2, r2, pi)
}

// ---------------------------------------------------------------------------
// A malformed persisted shared key refuses, rather than being skipped
// ---------------------------------------------------------------------------

// Found by holistic round 4, and it is F-050's own defect in miniature. The restore
// counted every SharedSubs entry but appended a filter only for keys
// mqttParseSharedKey accepted, so a malformed one produced no failed filter — the
// aggregation saw nothing wrong and the client was told Session Present for a
// membership the record still names and nothing restored.
//
// The way in is corruption or schema drift: mqttReservedMembershipInRecord
// deliberately SKIPS malformed shared keys rather than refusing the record, so
// nothing upstream rejects one.
//
// The membership is injected into the resident session's own map, which is what the
// restore reads.
//
// Mutation check: skip a malformed key instead of failing it, and the resume is
// accepted with Session Present.
func TestMalformedSharedKeyRefusesTheResume(t *testing.T) {
	b := testQ2AffSetup(t, "f050bad")
	id := "f050-badkey"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	filter := "$share/" + b.group + "/" + b.topic
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, id)
	sess := testMQTTResidentSession(t, b.s, id)

	// "$share/" has no group/topic split, so mqttParseSharedKey rejects it. It has
	// to reach the RECORD, not just memory: the resume rebuilds sharedSubs from the
	// persisted body, so an injection into the live map alone is discarded. Injecting
	// and then subscribing again persists the whole session — including this entry —
	// because sess.update writes the record from the in-memory maps.
	sess.mu.Lock()
	if sess.sharedSubs == nil {
		sess.sharedSubs = map[string]mqttSharedMembership{}
	}
	sess.sharedSubs[string(mqttSharedSubPrefix)] = testSharedMembership(string(mqttSharedSubPrefix), 1)
	sess.mu.Unlock()
	if codes := testMQTTSubV5(t, c, r, 9, []mqttV5SubFilter{{topic: "f050bad/persistme", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	c.Close()

	// Prove the record really carries it, or the assertion below would pass for the
	// wrong reason on a resume that never saw a malformed key.
	testReconcileClientGone(t, b.s, id)

	cc, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cc.Close()
	sessp, reason, _ := testMQTTReadConnAckV5(t, rr)
	if reason != mqttReasonImplementationSpecificError {
		t.Fatalf("CONNACK reason = 0x%x, want 0x%x: a malformed shared key was skipped instead of refusing", reason, mqttReasonImplementationSpecificError)
	}
	if sessp {
		t.Fatal("a resume carrying a malformed shared membership reported Session Present")
	}
}

// ---------------------------------------------------------------------------
// The RAW subject exits, and the plain (non-shared) path
// ---------------------------------------------------------------------------

// Added after holistic round 5 found the claim "each of the five exits has its own
// fixture" was FALSE: nothing injected the raw `sess.processSub` exit, on either the
// main subject or its level-up companion, so two of the five were covered by
// assertion only. They are the exits that fail on a revoked subscribe permission,
// which is the reachable cause behind the "refuses every resume" residual — so
// leaving them unexercised was the weakest part of the evidence.
//
// Both are asserted on a PLAIN filter, which also closes the other half of that
// finding: every other refusal fixture here uses a shared QoS 2 filter, so the
// non-shared path — which F-050 covers by the same mechanism — had no refusal test
// at all.
//
// Mutation check: M25 removes the raw exit consultation and the resume is accepted.
func TestRestoreRefusesConnectWhenARawSubscriptionFails(t *testing.T) {
	for _, tc := range []struct {
		name, exit, filter, sid string
	}{
		{"main subject", "raw", "f050raw/main", "f050raw.main"},
		{"level-up companion", "raw-fwc", "f050raw/ml/#", "f050raw.ml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQ2AffSetup(t, "f050raw"+tc.exit)
			id := "f050-raw-" + tc.exit

			c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
				clientID:   id,
				cleanStart: false,
				props:      mqttV5ConnPropsSessionExpiry(300),
			}, b.o.MQTT.Host, b.o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			// QoS 1, plain: this is the non-shared path, and a plain filter is what
			// makes the raw exit the only one that can fail here.
			if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: tc.filter, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
				t.Fatalf("SUBACK = %v, want [1]", codes)
			}
			testMQTTFlush(t, c, nil, r)
			b.bind(t, id)
			c.Close()

			hits := testRestoreFailExit(t, b.asm, tc.exit, errRestoreExitInjected)

			cc, rr := testMQTTConnectV5(t, &mqttV5ConnInfo{
				clientID:   id,
				cleanStart: false,
				props:      mqttV5ConnPropsSessionExpiry(300),
			}, b.o.MQTT.Host, b.o.MQTT.Port)
			sessp, reason, _ := testMQTTReadConnAckV5(t, rr)
			if reason != mqttReasonImplementationSpecificError {
				t.Fatalf("CONNACK reason = 0x%x, want 0x%x (the refusal)", reason, mqttReasonImplementationSpecificError)
			}
			if sessp {
				t.Fatal("a refused resume reported Session Present")
			}
			if hits.Load() == 0 {
				t.Fatalf("the %q exit was never reached, so this fixture proves nothing", tc.exit)
			}
			cc.Close()

			// The membership survives the refusal and a healthy retry restores it.
			b.asm.mu.Lock()
			b.asm.testRestoreFilterFail = nil
			b.asm.mu.Unlock()

			c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
				clientID:   id,
				cleanStart: false,
				props:      mqttV5ConnPropsSessionExpiry(300),
			}, b.o.MQTT.Host, b.o.MQTT.Port)
			defer c2.Close()
			gotSessp, reason, _ := testMQTTReadConnAckV5(t, r2)
			if reason != 0 || !gotSessp {
				t.Fatalf("the retry's CONNACK reason=0x%x sessionPresent=%v, want 0 and true", reason, gotSessp)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// A gate wait that times out on a SUCCEEDING resume must not stall the message
// ---------------------------------------------------------------------------

// Found by holistic round 6, on the successful-resume path rather than the refusal
// one. The wait's bound is min(AckWait, mqttPhaseGateWaitMax), and the code used to
// treat expiry as harmless "because JetStream re-pushes at exactly that point" —
// which holds only while AckWait is inside the cap. With AckWait ABOVE it, the wait
// ends long before any redelivery, and the two classes that carry no withheld
// marker — a stored PUBREL and an affine copy — returned unacknowledged with nothing
// to re-drive them. So a resume that then SUCCEEDED left an owed packet unsent for
// the rest of that AckWait, with the client holding Session Present.
//
// The fixture makes the cap small and AckWait large, and holds the restore past the
// cap but well below AckWait. The message must still arrive promptly after the
// CONNACK — which it can only do if the timed-out delivery was NAKed.
//
// Mutation check: treat a timed-out wait as an ordinary withheld verdict (drop the
// NAK) and the assertions below time out.
func TestGateTimeoutOnASucceedingResumeDoesNotStall(t *testing.T) {
	// AckWait far above the cap, so a redelivery cannot rescue the test and mask
	// the defect.
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 90 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	prevMax := mqttPhaseGateWaitMax.Get()
	mqttPhaseGateWaitMax.Set(300 * time.Millisecond)
	defer func() { mqttPhaseGateWaitMax.Set(prevMax) }()

	id := "f050-timeout"
	const topic, sid = "f050to/plain", "f050to.plain"

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: topic, opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK = %v, want [1]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	c.Close()

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f050to-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPublishV5(t, cp, 1, 1, topic, []byte("owed-past-the-cap"))
	testMQTTReadPubAckV5(t, rp, 1)

	// Hold the restore for longer than the cap, so the delivery's wait provably
	// expires, and far less than AckWait, so a redelivery cannot be what saves it.
	ctr := testWatchRestoreGateBlocks(t)
	fired := testPauseRestoreAt(t, "before-connack", func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && ctr.count("qos12") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	sessp, reason, _ := testMQTTReadConnAckV5(t, r2)
	if reason != 0 || !sessp {
		t.Fatalf("resume CONNACK reason=0x%x sessionPresent=%v, want 0 and true", reason, sessp)
	}
	if fired.Load() == 0 {
		t.Fatal("the restore never paused, so the wait never had a chance to expire")
	}
	if got := ctr.count("qos12"); got == 0 {
		t.Fatal("no QoS 1/2 delivery was held by the verdict; the wait never expired")
	}

	// The resume SUCCEEDED, so the owed message must arrive promptly — well inside
	// the 90s AckWait that would otherwise be the only thing re-driving it.
	pi := testRestoreAwaitPublish(t, r2, "owed-past-the-cap")
	if pi == 0 {
		t.Fatal("the owed delivery carried no packet identifier")
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
}

// ---------------------------------------------------------------------------
// The two timeout NAKs, each reached and each pinned separately
// ---------------------------------------------------------------------------

// Holistic round 7 caught the preceding fixture claiming more than it reached: it
// uses a plain QoS 1 subscription, so it only ever runs mqttDeliverMsgCbQoS12 —
// which records any non-Open verdict in sess.deferred and is re-driven by the resume
// independently of any NAK. Reverting either timeout NAK would reintroduce the
// AckWait-length stall while that test still passed.
//
// These reach the NAK branches. Each NAK is mutated on its own: M27 for the stored
// PUBREL against the fixture below, and M28 for the affine copy against
// TestGateTimeoutNaksAnUndeliveredAffineCopy — the affine NAK's population needs a
// copy that was materialized and never delivered, which the redelivery-based fixture
// below cannot construct.
//
// The shape both share: shorten the wait cap, leave AckWait far above it, and hold
// restoration past the cap but well below AckWait. A packet that arrives promptly
// after the CONNACK can only have been re-driven by the NAK, because a redelivery
// would not be due for another 90 seconds.

func TestGateTimeoutNaksAStoredPubRel(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 90 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	b := &testQ2AffBroker{s: s, o: o, topic: "f050relto/topic", group: "f050reltogrp"}

	prevMax := mqttPhaseGateWaitMax.Get()
	mqttPhaseGateWaitMax.Set(300 * time.Millisecond)
	defer func() { mqttPhaseGateWaitMax.Set(prevMax) }()

	id := "f050-relto"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: b.topic, opts: 2}}); len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK = %v, want [2]", codes)
	}
	testMQTTFlush(t, c, nil, r)
	b.bind(t, id)

	// The PUBREL has to be STORED but never DELIVERED, or it is pending-unacked and
	// JetStream will not re-push it until AckWait — which is 90s here, so it would
	// never reach the pre-CONNACK window and this fixture would prove nothing (its
	// first version did exactly that). Stalling delivery is what leaves it
	// undelivered, so the resume's fresh subscription is pushed it immediately.
	stalled := make(chan struct{})
	var stallOnce sync.Once
	restoreDelay := setMQTTTestPubRelDeliveryDelay(func() {
		stallOnce.Do(func() { close(stalled) })
		time.Sleep(2 * time.Second)
	})

	b.publish(t, 1, "relto")
	pi := testRestoreAwaitPublish(t, r, "relto")
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	select {
	case <-stalled:
	case <-time.After(10 * time.Second):
		restoreDelay()
		t.Fatal("the PUBREL delivery was never reached, so it cannot be left undelivered")
	}
	c.Close()
	restoreDelay()

	ctr := testWatchRestoreGateBlocks(t)
	fired := testPauseRestoreAt(t, "before-connack", func() {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) && ctr.count("pubrel") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		// Past the cap, so the wait provably expires while restoration is held.
		time.Sleep(500 * time.Millisecond)
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("resume CONNACK reason = 0x%x, want 0", reason)
	}
	if fired.Load() == 0 {
		t.Fatal("the restore never paused, so the wait never had a chance to expire")
	}
	if got := ctr.count("pubrel"); got == 0 {
		t.Fatal("no PUBREL delivery met the verdict; this fixture never reached the PUBREL timeout arm")
	}

	// Promptly, i.e. long before the 90s AckWait that is the only other thing that
	// could re-drive it.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		pb, pl, ready := testMQTTReadPacketReady(r2, time.Second)
		if !ready {
			continue
		}
		start := r2.pos
		if pb&mqttPacketMask != mqttPacketPubRel {
			r2.pos = start + pl
			continue
		}
		got, err := r2.readUint16("pi")
		if err != nil {
			t.Fatalf("reading the PUBREL packet identifier: %v", err)
		}
		r2.pos = start + pl
		if got == pi {
			testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
			return
		}
	}
	t.Fatal("the stored PUBREL was not re-driven after the gate timed out; it is waiting out AckWait")
}

func TestGateTimeoutNaksAnAffineCopy(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	// AckWait ABOVE the cap is the whole condition, and it has to be short enough
	// that a REDELIVERY lands inside the held window: an affine copy cannot be
	// materialized during that window at all, because the group delivery that would
	// move it meets the verdict first. So the copy is delivered once on the first
	// connection, and its redelivery is what the timeout arm sees.
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 2 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	b := &testQ2AffBroker{s: s, o: o, topic: "f050affto/topic", group: "f050afftogrp"}

	prevMax := mqttPhaseGateWaitMax.Get()
	mqttPhaseGateWaitMax.Set(100 * time.Millisecond)
	defer func() { mqttPhaseGateWaitMax.Set(prevMax) }()

	id := "f050-affto"
	c, r := b.member(t, id, 300)
	b.bind(t, id)

	// Delivered once and left unanswered, so it is pending and redelivered at
	// AckWait — which the pause below is held long enough to cover.
	b.publish(t, 1, "affto")
	if pi := testRestoreAwaitPublish(t, r, "affto"); pi == 0 {
		t.Fatal("the affine delivery carried no packet identifier")
	}
	c.Close()

	ctr := testWatchRestoreGateBlocks(t)
	fired := testPauseRestoreAt(t, "before-connack", func() {
		// Held past AckWait, so the redelivery arrives while the verdict is still
		// closed, and far past the 100ms cap, so its wait provably expires.
		deadline := time.Now().Add(12 * time.Second)
		for time.Now().Before(deadline) && ctr.count("affine") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(300 * time.Millisecond)
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("resume CONNACK reason = 0x%x, want 0", reason)
	}
	if fired.Load() == 0 {
		t.Fatal("the restore never paused, so the wait never had a chance to expire")
	}
	if got := ctr.count("affine"); got == 0 {
		t.Fatal("no affine copy met the verdict; this fixture never reached the affine timeout arm")
	}

	// **What this test does not pin, and where that is pinned instead.** The copy
	// here was delivered once on the first connection, so it is in the session's
	// pending state and `resendPendingRedelivery` replays it on resume whether or not
	// the NAK fired — which is why removing the affine NAK leaves this fixture
	// passing. The population the NAK actually covers is a copy materialized and
	// NEVER delivered, absent from that snapshot; it is pinned by
	// TestGateTimeoutNaksAnUndeliveredAffineCopy, which mutation M28 kills. What THIS
	// fixture establishes is that the affine timeout arm is reached at all (the
	// blocked counter above) and that an already-tracked copy owed across a gate
	// timeout still arrives.
	//
	// The DUP flag is deliberately not asserted: the affine path derives it from the
	// JetStream delivery count, which is F-072's open defect, and a NAK increments
	// that count.
	deadline := time.Now().Add(1200 * time.Millisecond)
	pi := testRestoreAwaitPublish(t, r2, "affto")
	if pi == 0 {
		t.Fatal("the affine copy was never re-driven after the gate timed out")
	}
	if time.Now().After(deadline) {
		t.Fatal("the affine copy took longer than a NAK-driven re-push: it waited out AckWait")
	}
	b.completeExchange(t, c2, r2, pi)
}

// ---------------------------------------------------------------------------
// The affine timeout NAK, on the population it actually covers
// ---------------------------------------------------------------------------

// Holistic round 8 was right that stating this gap was not proportionate: the
// population IS constructible with the helpers this suite already has, and it is a
// real QoS 2 liveness stall rather than redundancy with `resendPendingRedelivery`.
//
// The distinction is what the preceding fixture could not reach. A copy that was
// already DELIVERED once sits in the session's pending state, so a resume replays it
// whether or not anything NAKed. A copy that was MATERIALIZED and never delivered is
// absent from that snapshot: it is pushed when affine interest is restored, meets the
// closed verdict before `trackPublish` ever sees it, and — without the NAK — stays
// unacknowledged until AckWait even though the resume SUCCEEDED.
//
// So this fixture seeds exactly that: a materialized claim and a retained copy on the
// disconnected session's own live affine generation, with the copy asserted absent
// from `pendingPublish` before the resume begins.
//
// Mutation check: M28 removes the affine timeout NAK and this fixture fails.
func TestGateTimeoutNaksAnUndeliveredAffineCopy(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	// AckWait above the cap is the condition; short enough that the "without the
	// NAK" arm is observably slower rather than hanging the suite.
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 8 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	b := &testQ2AffBroker{s: s, o: o, topic: "f050undel/topic", group: "f050undelgrp"}

	prevMax := mqttPhaseGateWaitMax.Get()
	mqttPhaseGateWaitMax.Set(100 * time.Millisecond)
	defer func() { mqttPhaseGateWaitMax.Set(prevMax) }()

	id := "f050-undel"
	c, _ := b.member(t, id, 300)
	b.bind(t, id)
	sess := testMQTTResidentSession(t, b.s, id)

	// The session's own live affine generation and consumer, as provisioned by its
	// shared QoS 2 subscription — not a synthetic one, so the restore path reads
	// exactly what it would in production.
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.q2AffGen
	affDur := ""
	if sess.q2AffConsumer != nil {
		affDur = sess.q2AffConsumer.Durable
	}
	sess.mu.Unlock()
	if gen == _EMPTY_ || affDur == _EMPTY_ {
		t.Fatalf("the member has no affine generation/consumer (gen=%q dur=%q)", gen, affDur)
	}
	c.Close()
	testReconcileClientGone(t, b.s, id)

	// A materialized claim on that generation, and its copy stored where only that
	// generation can read it. Never delivered, because nothing is subscribed to the
	// affine deliver subject while the member is away.
	groupDurable := b.groupDurable(t)
	const sourceSeq = uint64(4242)
	claimHash := mqttQ2AffClaimHash(groupDurable, sourceSeq)
	claim := &mqttQ2AffClaim{
		Winner:       idHash,
		Gen:          gen,
		GroupDurable: groupDurable,
		SourceSeq:    sourceSeq,
		ClaimHash:    claimHash,
		Nonce:        testQ2AffFixtureNonce,
		CopySubject:  mqttQ2AffCopySubject(idHash, gen, claimHash, testQ2AffFixtureNonce),
		AffDurable:   affDur,
	}
	claimSubject := mqttQ2AffClaimSubject(groupDurable, sourceSeq)
	// The CLAIM record as well as the copy. The delivery gate fails closed on a copy
	// whose claim it cannot read — deliberately, since an absence is not a fact — so
	// seeding the copy alone reaches the timeout arm and then has the copy disposed
	// instead of delivered, which is how this fixture failed on its first run.
	claim.Phase = mqttQ2AffMaterialized
	if _, err := b.asm.jsa.storeQ2AffClaimCAS(claimSubject, 0, claim); err != nil {
		t.Fatalf("seeding the materialized claim on %q: %v", claimSubject, err)
	}
	testQ2AffStoreCopyFor(t, b, claimSubject, claim, "undelivered-copy")

	// The premise, asserted rather than assumed: this copy is NOT in the pending
	// snapshot, so the resume's own replay cannot be what delivers it. Without this
	// the fixture would pass for the same wrong reason its predecessor did.
	sess.mu.Lock()
	pending := len(sess.pendingPublish)
	sess.mu.Unlock()
	if pending != 0 {
		t.Fatalf("the session already has %d pending publish entries; the copy would be replayed rather than re-driven", pending)
	}

	ctr := testWatchRestoreGateBlocks(t)
	fired := testPauseRestoreAt(t, "before-connack", func() {
		// Long enough for the restored affine interest to be pushed the copy and for
		// its wait to expire against the 100ms cap; far short of the 8s AckWait.
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) && ctr.count("affine") == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(300 * time.Millisecond)
	})

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   id,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r2); reason != 0 {
		t.Fatalf("resume CONNACK reason = 0x%x, want 0", reason)
	}
	if fired.Load() == 0 {
		t.Fatal("the restore never paused, so the wait never had a chance to expire")
	}
	if got := ctr.count("affine"); got == 0 {
		t.Fatal("the undelivered copy never met the verdict; this fixture did not reach the affine timeout arm")
	}

	// Promptly after the open. Only the NAK can achieve that: the copy is not in the
	// pending snapshot, so nothing replays it, and its own redelivery is 8s away.
	deadline := time.Now().Add(3 * time.Second)
	pi := testRestoreAwaitPublish(t, r2, "undelivered-copy")
	if pi == 0 {
		t.Fatal("the undelivered affine copy never arrived after a successful resume")
	}
	if time.Now().After(deadline) {
		t.Fatal("the undelivered affine copy waited out AckWait instead of being re-driven")
	}
}
