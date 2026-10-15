// Copyright 2025 The mqtt-nats-bridge Authors
//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// End-to-end tests for outbound in-flight identifier continuity
// (inflight_register.go): after a broker restart, a started QoS 1/2 delivery is
// re-offered to the same session under its ORIGINAL packet identifier with DUP set,
// rather than under a freshly allocated one.
//
// These are the tests the roadmap item exists for. Each is mutation-checked: the
// mutation named in its comment must make it fail, or the test is not evidence.

// testInflightRestart shuts the broker down while leaving its embedded
// nats-server (and so all JetStream state) running, then brings a fresh broker up
// against the same store — the state a client sees across a broker restart or a
// failover onto a peer that has none of the first one's memory.
func testInflightRestart(t *testing.T, s *Server, o *Options) *Server {
	t.Helper()
	s.Shutdown()
	s = testMQTTRestartServerSameStore(t, s, o)
	if err := s.Start(); err != nil {
		t.Fatalf("restart the broker: %v", err)
	}
	return s
}

// testInflightWaitNoRegisters waits until one incarnation holds no in-flight registers.
//
// Polled rather than asserted once, because a register delete is deliberately
// ASYNCHRONOUS (R5: a failed delete costs a stale record, never a wedged identifier), so
// every path that removes one issues it fire-and-forget. Sampling straight afterwards
// tests send-queue timing.
func testInflightWaitNoRegisters(t *testing.T, asm *mqttAccountSessionManager, idHash, gen string) {
	t.Helper()
	filter := mqttInflightWalkFilter(idHash, gen)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttInflightStreamName, filter, 1)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return fmt.Errorf("walk the registers: %w", err)
			}
			return nil
		}
		return fmt.Errorf("a register still occupies %q", smsg.Subject)
	})
}

// testInflightSubscriber connects a persistent session and subscribes at qos.
func testInflightSubscriber(t *testing.T, o *Options, clientID string, qos byte) (net.Conn, *mqttReader) {
	t.Helper()
	ci := &mqttConnInfo{clientID: clientID, cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos}}, []byte{qos})
	testMQTTFlush(t, c, nil, r)
	return c, r
}

// testInflightReconnect reattaches to a persistent session, expecting Session
// Present.
func testInflightReconnect(t *testing.T, o *Options, clientID string) (net.Conn, *mqttReader) {
	t.Helper()
	ci := &mqttConnInfo{clientID: clientID, cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	return c, r
}

// A QoS 1 delivery whose PUBACK never arrives is redelivered after a broker restart
// under its original packet identifier.
//
// The identifier is deliberately NOT the first one this session ever allocated. A
// fresh allocation on a restarted broker starts from last_pi = 0 and produces 1, so a
// test whose in-flight delivery happens to hold identifier 1 passes with the whole
// recovery path deleted — it asserts nothing. Two acknowledged deliveries first push
// the interesting one to 3.
//
// Mutation check: skip the recoverInflightRegisters call in mqttProcessConnect, or
// have it reserve nothing, and the redelivery arrives under identifier 1.
func TestMQTTInflightQoS1IdentifierSurvivesRestart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	// A short AckWait: after a restart the redelivery is driven by AckWait rather
	// than by a NAK, because a recovered entry has no per-attempt ack subject to NAK
	// (see resendPendingRedelivery).
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-qos1", 1)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-qos1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)

	// Burn two identifiers on completed deliveries, so the one under test is not the
	// value a fresh allocator would hand out anyway.
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte(payload))
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
	}

	// Now the delivery under test: take the PUBLISH and never PUBACK it.
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	firstPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if firstPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce; the test would prove nothing", firstPI)
	}
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testInflightReconnect(t, o, "inflight-qos1")
	defer sub.Close()

	// The redelivery must be the SAME message under the SAME identifier, with DUP set.
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if gotPI != firstPI {
		t.Fatalf("redelivery after a restart used packet identifier %d, want the original %d", gotPI, firstPI)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// The same for QoS 2, stopped before PUBREC: the PUBLISH phase is the half the
// register owns, and the whole exchange then completes under that one identifier.
//
// Mutation check: as above; also, removing the register write from
// mqttDeliverMsgCbQoS12 leaves nothing to recover and the identifier changes.
func TestMQTTInflightQoS2IdentifierSurvivesRestartBeforePubRec(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-qos2", 2)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-qos2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)

	// Two completed exchanges first, so the identifier under test is not the one a
	// fresh allocator would produce anyway (see the QoS 1 test).
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte(payload))
		testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
		testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
	}

	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("payload"))
	firstPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("payload"))
	if firstPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce; the test would prove nothing", firstPI)
	}
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testInflightReconnect(t, o, "inflight-qos2")
	defer sub.Close()

	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2|mqttPubFlagDup, []byte("payload"))
	if gotPI != firstPI {
		t.Fatalf("redelivery after a restart used packet identifier %d, want the original %d", gotPI, firstPI)
	}

	// And the exchange completes under it, on the broker that never saw it start.
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, gotPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, gotPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// Past PUBREC the identifier's authority is the stored PUBREL, which already
// survived restarts. This pins that behavior alongside the new one — and pins the
// LATENT COLLISION the barrier closes: pendingPubRel used to be empty until the
// PUBREL callback replayed a record, so a fresh delivery arriving first could be
// allocated the very identifier the unreplayed PUBREL owned.
//
// Mutation check: drop the pendingPubRel reservation loop from
// recoverInflightRegisters and the fresh QoS 1 delivery below collides with the
// PUBREL's identifier.
func TestMQTTInflightPubRelIdentifierReservedAcrossRestart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-pubrel", 2)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-pubrel", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("first"))

	relPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("first"))
	// PUBREC it, take the PUBREL, and never PUBCOMP: the exchange is now past PUBREC
	// with a durable PUBREL record owning relPI.
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, relPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, relPI)
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testInflightReconnect(t, o, "inflight-pubrel")
	defer sub.Close()

	// A brand new message on the same filter. Its identifier must not be relPI,
	// which the client is still holding for the unfinished exchange
	// ([MQTT-2.3.1-4]).
	pub2, pub2r := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-pubrel-2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub2.Close()
	testMQTTCheckConnAck(t, pub2r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub2, pub2r, 2, false, false, "foo", 1, []byte("second"))

	deadline := time.Now().Add(testMQTTTimeout)
	var sawPubRel, sawSecond bool
	for (!sawPubRel || !sawSecond) && time.Now().Before(deadline) {
		b, pl := testMQTTReadPacket(t, subr)
		switch b & mqttPacketMask {
		case mqttPacketPubRel:
			pi, err := subr.readUint16("packet identifier")
			if err != nil {
				t.Fatalf("read the redelivered PUBREL identifier: %v", err)
			}
			if pi != relPI {
				t.Fatalf("the redelivered PUBREL used identifier %d, want the original %d", pi, relPI)
			}
			sawPubRel = true
		case mqttPacketPub:
			_, pi, _, payload := testMQTTGetPubMsgExEx(t, nil, subr, b, pl, _EMPTY_, nil)
			if string(payload) != "second" {
				t.Fatalf("unexpected payload %q", payload)
			}
			if pi == relPI {
				t.Fatalf("a NEW delivery was allocated packet identifier %d, which an unreplayed PUBREL still owns", pi)
			}
			sawSecond = true
		default:
			t.Fatalf("unexpected packet type %x", b&mqttPacketMask)
		}
	}
	if !sawPubRel || !sawSecond {
		t.Fatalf("timed out (pubrel=%v second=%v)", sawPubRel, sawSecond)
	}
}

// A register that outlived its exchange — the delete was lost — must not survive the
// next resume, and must not leave a phantom in-flight delivery behind.
//
// The phantom is the real damage, and it is what this test asserts. A completed
// exchange cannot be RE-DELIVERED whatever the register says, because the source
// message was acked and $MQTT_msgs is interest-retained, so it is simply gone; what a
// retained register would cost instead is a permanently reserved packet identifier
// and a permanently consumed Receive Maximum slot, for the life of the session. The
// second half is observable on the wire: with a Receive Maximum of 1, one leaked slot
// stops the session receiving anything at all.
//
// Mutation check: remove the source-completeness classification from
// recoverInflightRegisters (trust the register alone) and the follow-up message never
// arrives, because the phantom holds the session's only in-flight slot.
func TestMQTTInflightStaleRegisterLeavesNoPhantomSlot(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Receive Maximum 1 makes a leaked in-flight slot fatal to the session, and a
	// Session Expiry Interval is what keeps a v5 session alive across the restart.
	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(3600)...)
	ci := &mqttV5ConnInfo{clientID: "inflight-complete", props: props}
	sub, subr := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})

	// Suppress every register delete, so the register outlives the exchange exactly
	// as a lost delete would leave it.
	asm := testMQTTGetAccountSessionManager(t, s, "inflight-complete")
	asm.mu.Lock()
	asm.testInflightDelete = func(uint64) error { return nil }
	asm.mu.Unlock()

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pub-complete", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("first"), nil)

	_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("first"))
	// Complete the exchange in full.
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	// The register really did survive, or the test proves nothing below.
	asm.mu.RLock()
	sess := asm.sessions["inflight-complete"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	if _, err := asm.jsa.loadNextMsgFromSeq(mqttInflightStreamName,
		mqttInflightWalkFilter(idHash, gen), 1); err != nil {
		t.Fatalf("the suppressed delete should have left a stale register behind: %v", err)
	}
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, subr); !present {
		t.Fatalf("the session must be resumed")
	}

	// Nothing is owed from the completed exchange, and the session's single in-flight
	// slot is free: a fresh message gets through. The publisher reconnects, since its
	// connection did not survive the restart.
	pub.Close()
	pub, pubr = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pub-complete-2", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("second"), nil)
	_, got := testMQTTReadPublishV5(t, subr, "foo", []byte("second"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, got)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, got)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, got)
	testMQTTFlush(t, sub, nil, subr)

	// And the stale register is gone rather than accumulating.
	testInflightWaitNoRegisters(t, testMQTTGetAccountSessionManager(t, s, "inflight-complete"), idHash, gen)
}

// A QoS 2 source acknowledgement that cannot be CONFIRMED must not produce a second
// PUBLISH for the same message.
//
// At PUBREC the source's PUBLISH-phase entry — and its cpending link — are removed. If
// the ack is then unproven the source may still be pending, so JetStream re-offers it;
// with the link gone that redelivery misses the usedForPubRel guard, allocates a fresh
// identifier and re-sends the payload of an exchange already past PUBREC. The client
// would then hold the same message under two identifiers, and complete only one.
//
// Mutation check: remove the recorrelateSourceLocked call from the unproven-ack branch
// of mqttProcessPublishReceived and a second PUBLISH arrives below.
func TestMQTTInflightUnprovenSourceAckSendsNoSecondPublish(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	// The DEFAULT AckWait, deliberately long: the retirement is transient by design —
	// the settling redelivery removes it — so a short AckWait would race every assertion
	// below against that redelivery. The redelivery itself, and the removal it causes,
	// are covered by TestMQTTInflightRetiredSourceSurvivesRestart.
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-unproven", 2)
	defer sub.Close()

	// Fail the confirmation of the source ack, and suppress the real ack with it: this
	// is the state where the ack genuinely did not land.
	asm := testMQTTGetAccountSessionManager(t, s, "inflight-unproven")
	// Atomic because the hook fires on the broker's delivery goroutine while the
	// assertion below reads it from the test's: a plain counter is a data race, and the
	// read raced the increment.
	var confirms atomic.Int64
	asm.mu.Lock()
	asm.testInflightSourceAck = func(string) error {
		confirms.Add(1)
		return errors.New("test: source acknowledgement not confirmed")
	}
	asm.mu.Unlock()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-unproven", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("payload"))

	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("payload"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	// The exchange still advances: the PUBREL is stored and delivered.
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	// POLLED, not read once. The confirmation runs on the broker's delivery goroutine and
	// nothing orders it against the PUBREL this test just read, so a single read asserts a
	// cross-goroutine ordering the code does not establish — it merely usually wins. A
	// paired soak caught it failing under load while the log showed the hook had fired.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if confirms.Load() == 0 {
			return fmt.Errorf("the confirmed source acknowledgement was never attempted")
		}
		return nil
	})

	// The source is unacknowledged, so JetStream will re-offer it. What must stop that
	// re-offer becoming a second PUBLISH is the source being RETIRED — a fact recorded
	// against the source, not against the identifier, so that it outlives the
	// identifier's bookkeeping.
	//
	// Asserted on the state rather than by watching the wire: PUBREL redeliveries
	// legitimately arrive here too (the client has not sent PUBCOMP, so [MQTT-4.4.0-1]
	// has the server re-send them), so a "nothing arrives" window would be both wrong
	// and timing-dependent.
	asm.mu.RLock()
	sess := asm.sessions["inflight-unproven"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	// Polled, not sampled: the PUBREL read above is satisfied by an ASYNCHRONOUS
	// delivery callback, so it does not order this connection against the readLoop frame
	// that retires the source.
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if len(sess.srcAckOwed) != 1 {
			return fmt.Errorf("the source of identifier %d must be retired (%d retired): otherwise its redelivery "+
				"re-sends the payload of an exchange already past PUBREC", pi, len(sess.srcAckOwed))
		}
		return nil
	})
	sess.mu.Lock()
	_, owesPubRel := sess.pendingPubRel[pi]
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	if !owesPubRel {
		t.Fatalf("identifier %d must still owe a PUBREL", pi)
	}

	// The exchange completes under its original identifier...
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	// ...and the source stays retired ACROSS the PUBCOMP that removed the identifier's
	// bookkeeping, which is the half an identifier-keyed suppression would lose.
	sess.mu.Lock()
	stillRetired := len(sess.srcAckOwed)
	_, stillPubRel := sess.pendingPubRel[pi]
	sess.mu.Unlock()
	if stillPubRel {
		t.Fatalf("PUBCOMP must clear the pending PUBREL for identifier %d", pi)
	}
	if stillRetired != 1 {
		t.Fatalf("the source must stay retired after PUBCOMP (%d retired)", stillRetired)
	}

	// And the durable half is on the register, which is what carries the retirement
	// across a restart.
	rec, _, err := asm.jsa.loadInflightRecord(mqttInflightSubject(idHash, gen, pi))
	if err != nil {
		t.Fatalf("load the register: %v", err)
	}
	if rec == nil || !rec.Retired {
		t.Fatalf("the register must be marked retired, got %+v", rec)
	}
}

// A retired source stays retired ACROSS A RESTART: the completed payload is never
// re-sent, and the source is settled on the new broker.
//
// This is the case a memory-only retirement loses. Leaving the source deliberately
// unacknowledged is what makes the window reachable at all, so the retirement has to be
// on the register: without it the resume sees an ordinary PUBLISH-phase register over a
// live source — indistinguishable from a delivery that never reached PUBREC — and
// re-sends a payload the client has already completed.
//
// Mutation check: drop the Retired mark (make markInflightRegisterRetired a no-op, or
// stop routing retired registers in recoverInflightRegisters) and the PUBLISH assertion
// below fires.
func TestMQTTInflightRetiredSourceSurvivesRestart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-retired-restart", 2)

	asm := testMQTTGetAccountSessionManager(t, s, "inflight-retired-restart")
	asm.mu.Lock()
	asm.testInflightSourceAck = func(string) error {
		return errors.New("test: source acknowledgement not confirmed")
	}
	asm.mu.Unlock()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-retired-restart", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("payload"))

	// Complete the exchange in full, with the source acknowledgement unproven.
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("payload"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	// The retirement must be durable, or the restart below proves nothing.
	asm.mu.RLock()
	sess := asm.sessions["inflight-retired-restart"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	rec, _, err := asm.jsa.loadInflightRecord(mqttInflightSubject(idHash, gen, pi))
	if err != nil {
		t.Fatalf("load the register: %v", err)
	}
	if rec == nil || !rec.Retired {
		t.Fatalf("the register must be marked retired before the restart, got %+v", rec)
	}
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testInflightReconnect(t, o, "inflight-retired-restart")
	defer sub.Close()

	// The source is still unacknowledged, so the new broker is offered it. It must
	// discard and settle it, never deliver it: the client completed this exchange.
	testMQTTExpectNothingFor(t, subr, 3*o.MQTT.AckWait)

	// And the register is gone, because the source has been settled.
	testInflightWaitNoRegisters(t, testMQTTGetAccountSessionManager(t, s, "inflight-retired-restart"), idHash, gen)
}

// A retirement whose DURABLE mark cannot be written does not become a residual: its
// exchange's PUBREL is not acknowledged away while it is the only other evidence, so a
// restart still suppresses the completed payload.
//
// This is the hole a best-effort mark leaves. With the mark failing and the PUBREL acked
// away regardless, a restart sees an unmarked register over a live source —
// indistinguishable from a delivery that never reached PUBREC — and re-sends it.
// Withholding that acknowledgement keeps the PUBREL, which recovery reads as proof the
// exchange is past PUBREC, and which it then converts into the mark it could not write
// earlier.
//
// Mutation check: ack the PUBREL unconditionally in mqttProcessPubComp (drop the
// insecure-retirement guard) and the PUBLISH assertion below fires.
func TestMQTTInflightUnmarkableRetirementKeepsPubRelEvidence(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-unmarkable", 2)

	asm := testMQTTGetAccountSessionManager(t, s, "inflight-unmarkable")
	asm.mu.Lock()
	// The source acknowledgement cannot be confirmed, AND the register cannot be marked.
	asm.testInflightSourceAck = func(string) error {
		return errors.New("test: source acknowledgement not confirmed")
	}
	// Only the RETIREMENT write fails; the delivery's own register must still be
	// written, or nothing is ever sent and the test proves nothing.
	asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
		if r != nil && r.Retired {
			return 0, errors.New("test: retirement store refused")
		}
		return asm.jsa.storeInflightCAS(subject, expectSeq, r)
	}
	asm.mu.Unlock()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-unmarkable", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("payload"))

	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("payload"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	// The client completes the exchange. Its PUBREL must NOT be acknowledged away, because
	// the retirement is not durable. No flush here: a withheld acknowledgement means the
	// PUBREL is redelivered, so the next packet on the wire is not a PINGRESP.
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)

	asm.mu.RLock()
	sess := asm.sessions["inflight-unmarkable"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	pubRelSubject := sess.pubRelSubject
	sess.mu.Unlock()

	// Wait for the PUBCOMP to have been processed, observed as the REDELIVERED PUBREL
	// arriving on the wire. That is the direct evidence the acknowledgement was withheld,
	// and it is why it is not observed as the pending entry going away: a withheld
	// exchange deliberately KEEPS its entry, so the identifier and the Receive Maximum
	// slot stay reserved. Freeing them and then withholding would let a wrap-around
	// re-mint track a different PUBREL under this identifier, and mqttDeliverPubRelCb's
	// duplicate branch would then acknowledge the original — the very record this test is
	// about — away.
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	sess.mu.Lock()
	_, stillReserved := sess.pendingPubRel[pi]
	sess.mu.Unlock()
	if !stillReserved {
		t.Fatal("the withheld exchange released its packet identifier; a redelivery of this same PUBREL " +
			"can now be acknowledged away as a duplicate of whatever re-minted it")
	}
	if _, err := asm.jsa.loadNextMsgFromSeq(mqttOutStreamName, pubRelSubject, 1); err != nil {
		t.Fatalf("the PUBREL record must be kept while the retirement is not durable: %v", err)
	}
	sub.Close()

	// Let the register store work again, so the restart can convert the evidence.
	asm.mu.Lock()
	asm.testInflightStore = nil
	asm.mu.Unlock()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testInflightReconnect(t, o, "inflight-unmarkable")
	defer sub.Close()

	// The completed payload must not come back. A PUBREL redelivery may (the new broker
	// re-establishes the PUBREL path), so only a PUBLISH is a failure.
	deadline := time.Now().Add(3 * o.MQTT.AckWait)
	for time.Now().Before(deadline) {
		if err := sub.SetReadDeadline(time.Now().Add(400 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		var buf [256]byte
		n, rerr := subr.reader.Read(buf[:])
		for i := 0; i < n; i++ {
			if buf[i]&mqttPacketMask == mqttPacketPub && i == 0 {
				t.Fatalf("a completed QoS 2 payload was re-sent after the restart: %v", buf[:n])
			}
		}
		if rerr != nil {
			var netErr net.Error
			if errors.As(rerr, &netErr) && netErr.Timeout() {
				continue
			}
			t.Fatalf("connection ended while watching for a duplicate: %v", rerr)
		}
	}
	if err := sub.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}
}

// An acknowledgement that LANDED but could not be confirmed leaves nothing behind.
//
// This is the likeliest shape of an unproven acknowledgement, and treating it as "may
// still be re-offered" forever is what makes retired state accumulate: nothing would ever
// re-offer that source, so nothing would ever settle its retirement, and every later
// resume would re-establish it from its own register. Reading the source resolves the
// ambiguity instead.
//
// "Landed" is ESTABLISHED, not hoped for: the hook makes the real confirmed submission
// (which bypasses the hook) before reporting the confirmation lost, so the source's ack
// floor has moved when resolveRetiredSource reads it. A fire-and-forget ack here left
// that a race against the read.
//
// The retirement is asserted BEFORE the PUBCOMP: once the exchange completes, terminal
// cleanup clears the retirement on its own (clearTerminalRetirementLocked_), which would
// let the assertion pass with the resolution removed.
//
// Mutation check: remove the resolveRetiredSource call from the unproven-acknowledgement
// branch and the retirement assertion fails.
func TestMQTTInflightLandedButUnconfirmedAckLeavesNothing(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-ambiguous", 2)
	defer sub.Close()

	asm := testMQTTGetAccountSessionManager(t, s, "inflight-ambiguous")
	var hookCalls atomic.Int32
	var landingErr atomic.Pointer[error]
	asm.mu.Lock()
	// The acknowledgement LANDS; only its confirmation is lost. That is the ambiguity.
	asm.testInflightSourceAck = func(ackSubject string) error {
		hookCalls.Add(1)
		asm.mu.RLock()
		sess := asm.sessions["inflight-ambiguous"]
		asm.mu.RUnlock()
		var lerr error
		if sess == nil {
			lerr = errors.New("no resident session to land the acknowledgement through")
		} else if r, err := sess.submitSourceAck(ackSubject, true, nil); err != nil {
			lerr = err
		} else if !r.positive() {
			lerr = fmt.Errorf("the acknowledgement was not landed (result %d)", r)
		}
		if lerr != nil {
			landingErr.Store(&lerr)
		}
		return errors.New("test: confirmation lost, but the acknowledgement landed")
	}
	asm.mu.Unlock()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-ambiguous", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 2, false, false, "foo", 1, []byte("payload"))

	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQoS2, []byte("payload"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	// PINGREQ/PINGRESP orders the read loop: the PUBREC handler has returned.
	testMQTTFlush(t, sub, nil, subr)
	if hookCalls.Load() == 0 {
		t.Fatalf("fixture: the PUBREC never reached the source acknowledgement")
	}
	if p := landingErr.Load(); p != nil {
		t.Fatalf("fixture: the acknowledgement must really land before its confirmation is lost: %v", *p)
	}

	asm.mu.RLock()
	sess := asm.sessions["inflight-ambiguous"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()

	// No retirement is left holding state, before the exchange completes...
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if n := len(sess.srcAckOwed); n != 0 {
			return fmt.Errorf("%d retirement(s) left behind for a source that was acknowledged", n)
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
	// ...and no register either: both would otherwise persist for the life of the session
	// and be re-established by every resume.
	testInflightWaitNoRegisters(t, asm, idHash, gen)
}

// A DRAINED QoS 2 exchange releases its register when it completes, so the drain's
// consumer does not outlive it and leave a phantom behind.
//
// The drained branch of the PUBREC path deliberately does not delete the register — the
// source claim is held until the PUBREL is proven — so the drain owns it and releases it in
// its finalizer. Left behind, the register outlives the consumer that finishDrainIfEmpty
// deletes, and the next resume reserves its identifier because a consumer-gone source is
// deliberately not read as complete. Under a Receive Maximum of 1 that stops the session
// receiving anything at all, which is what this test measures.
//
// Mutation check: remove the register release from finalizeDrainDelivery and the follow-up
// message never arrives.
func TestMQTTInflightDrainedQoS2ReleasesRegister(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Receive Maximum 1 makes a leaked in-flight slot fatal, and a Session Expiry keeps
	// the session across the restart.
	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(3600)...)
	ci := &mqttV5ConnInfo{clientID: "inflight-drained", props: props}
	sub, subr := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pub-drained", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("first"), nil)

	// Take the PUBLISH, then UNSUBSCRIBE: the started delivery converts the consumer into
	// a drain ([MQTT-3.10.4-3]).
	_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("first"))
	testMQTTUnsubV5(t, sub, subr, 2, []string{"foo"})

	// Complete the exchange from the drain.
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)

	asm := testMQTTGetAccountSessionManager(t, s, "inflight-drained")
	asm.mu.RLock()
	sess := asm.sessions["inflight-drained"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()

	// The drain released the register when the exchange completed.
	testInflightWaitNoRegisters(t, asm, idHash, gen)
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, subr); !present {
		t.Fatalf("the session must be resumed")
	}
	// Nothing was left reserving the session's only in-flight slot, so a fresh
	// subscription delivers.
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})

	pub.Close()
	pub, pubr = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pub-drained-2", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("second"), nil)

	_, got := testMQTTReadPublishV5(t, subr, "foo", []byte("second"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, got)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, got)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, got)
	testMQTTFlush(t, sub, nil, subr)
}

// A retained-message delivery has no JetStream backing and therefore no register,
// which is R6's second half. It must not leave one behind, and must not be recovered.
// TestMQTTInflightRetainedSendIsRegistered REPLACES TestMQTTInflightRetainedSendNotRegistered by
// name, with its assertion reversed (F-105 7c): a retained QoS 1 send is no longer an unregistered
// write from the live retained slot. Its delivery stands as a register-backed exchange — the
// retained register (payload arm, no source coordinates), the exchange at Publish, and a READY copy
// naming that exchange as admitted — before and while the PUBLISH is unacknowledged. The PI and
// frame assertions are kept.
func TestMQTTInflightRetainedSendIsRegistered(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-retained", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, true, "foo", 1, []byte("retained"))
	testMQTTFlush(t, pub, nil, pubr)

	// Subscribed without the helper's trailing flush: the retained delivery is the
	// next packet on the wire, and it is deliberately left unacknowledged.
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: "inflight-retained", cleanSess: false},
		o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1|mqttPubFlagRetain, []byte("retained"))
	if pi == 0 {
		t.Fatalf("a QoS 1 retained delivery must carry a packet identifier")
	}

	asm := testMQTTGetAccountSessionManager(t, s, "inflight-retained")
	asm.mu.RLock()
	sess := asm.sessions["inflight-retained"]
	epoch := asm.sessStreamEpoch
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()

	reg, smsg, err := asm.loadInflightRecord(mqttInflightSubject(idHash, gen, pi))
	if err != nil || smsg == nil || reg == nil {
		t.Fatalf("a retained send must stand as a register (%v)", err)
	}
	if !reg.retainedSource() || reg.Dur != _EMPTY_ || reg.Seq != 0 || reg.QoS != 1 {
		t.Fatalf("the retained register is %+v, want the retained arm at QoS 1 with no source coordinates", reg)
	}
	x, _, xerr := asm.jsa.loadOutboundExchange("inflight-retained", epoch, gen, reg.Lin)
	if xerr != nil || x == nil || x.Phase != mqttExchangePublish || x.PI != pi {
		t.Fatalf("the retained exchange is %+v (%v), want Publish under identifier %d", x, xerr, pi)
	}
	m, _, merr := asm.jsa.loadRetainedManifest("inflight-retained", epoch, gen, reg.Payload)
	if merr != nil || m.State != mqttManifestReady || m.Admitted != reg.Lin || m.Topic != "foo" {
		t.Fatalf("the retained copy is %+v (%v), want READY admitted to %q", m, merr, reg.Lin)
	}
}

// A reconnect's ACCEPT SAVE both carries an inherited void-register verdict and CREDITS it.
//
// This is the rebind strand of the deferred-close work, end to end through a real CONNECT. The
// surviving close before it gives up its SHARED group delivery and stages a verdict it cannot
// persist — both register-stream rungs and the session store are failing — so the session is
// left with an unsaved revision and a reservation parked on the identifier, and no handoff can
// have written it because every attempt the close made failed too. The next CONNECT is then the
// first landed write, and it is not a verdict path at all: it inherits the nonce in the record
// it marshals. If crediting lived only in the paths that set out to persist a verdict, the fact
// would be durable while the identifier and its Receive Maximum slot stayed reserved in memory
// for the life of the session.
//
// SINCE F-101 the verdict has to come from a PROVEN-UNSENT delivery, and the fixture says so
// positively. The real group delivery below is TRANSMITTED — the client received it — so the
// close reserves its identifier and stages NO verdict for it, which this test now asserts
// rather than assuming the old behaviour. A second entry is injected for the credit subject:
// a confirmed register whose PUBLISH has not been sent, its durable pair still holding the
// unsent marker, which is the population a teardown may legitimately give up. Both halves are
// asserted, so neither the inversion nor the credit can pass on the other's account.
//
// Mutation checks: drop the marshal-revision credit in saveWithDetail's success arm and the
// credit assertion fails; skip the shared purge at the close and nothing is staged to inherit;
// remove the void staging's proven-unsent gate and the transmitted delivery's no-verdict
// assertion fails.
func TestMQTTInflightReconnectAcceptSaveCreditsInheritedVerdict(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "inflight-inherit-credit"
	// A Session Expiry Interval is what makes the close a SURVIVING one, which is the close
	// that purges its shared pending deliveries.
	props := mqttV5ConnPropsSessionExpiry(3600)
	sub, subr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: props}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "$share/g/foo", opts: 1}})

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pub-inherit", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)

	// One group delivery in flight, never acknowledged: that is the register the close gives
	// up, freeing its identifier without acking.
	testMQTTPubV5Props(t, pub, pubr, 1, false, 1, "foo", []byte("payload"), nil)
	testMQTTReadPublishV5(t, subr, "foo", []byte("payload"))

	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("the subscriber's session must be resident")
	}

	// The delivery above is TRANSMITTED, so since F-101 the close stages no verdict for it and
	// keeps its identifier reserved. Capture its identity now, to assert exactly that below.
	// Then inject the PROVEN-UNSENT entry the credit subject needs, on the SAME group durable
	// so the same purge reaches it: a confirmed register whose PUBLISH has not gone out, which
	// is the shape storeInflightRegister leaves before the first send (its marker sequence is
	// what says the unsent marker is still there).
	const unsentPI = uint16(0xF01)
	var sentNonce, groupDur string
	sess.mu.Lock()
	for dur, seqPis := range sess.cpending {
		groupDur = dur
		for _, pi := range seqPis {
			if ack := sess.pendingPublish[pi]; ack != nil {
				sentNonce = ack.regNonce
			}
		}
	}
	if groupDur != _EMPTY_ {
		sess.cpending[groupDur][0xF0100] = unsentPI
		sess.pendingPublish[unsentPI] = &mqttPending{
			jsDur: groupDur, sseq: 0xF0100, jsAckSubject: "ack.unsent",
			regNonce: mqttNewInflightNonce(), regSeq: 4241, markerSeq: 4242,
		}
	}
	unsentNonce := _EMPTY_
	if ack := sess.pendingPublish[unsentPI]; ack != nil {
		unsentNonce = ack.regNonce
	}
	sess.mu.Unlock()
	if groupDur == _EMPTY_ || sentNonce == _EMPTY_ || unsentNonce == _EMPTY_ {
		t.Fatalf("the fixture must hold one transmitted and one proven-unsent delivery on the group durable, got dur=%q sent=%q unsent=%q",
			groupDur, sentNonce, unsentNonce)
	}

	// Both register-stream rungs fail, so the close's cleanup has only the session record
	// left; and the record store fails DEFINITELY, so it cannot get there either. What
	// remains is a staged verdict with an unsaved revision.
	asm.mu.Lock()
	asm.testInflightDelete = func(uint64) error { return errors.New("test: register delete unavailable") }
	asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, errors.New("test: register stream unavailable")
	}
	asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, errors.New("test: register stream unavailable")
	}
	asm.mu.Unlock()
	// The record store fails for as long as the session is UNBOUND, which is what makes the
	// attribution positive: every write the close makes — its disconnect save, the purge's
	// fallback, the gate-release handoff — is refused, so the FIRST landed write on this
	// session can only be the reconnect's accept save (CONNECT binds before it saves).
	restoreSave := setMQTTTestSessSaveHook(func(cur *mqttSession) error {
		if cur != sess {
			return nil
		}
		cur.mu.Lock()
		unbound := cur.c == nil
		cur.mu.Unlock()
		if unbound {
			return errors.New("test: the session store is unavailable while unbound")
		}
		return nil
	})
	defer restoreSave()
	landed := 0
	// The record AS THE ACCEPT SAVE LEFT IT, captured the moment that write lands.
	//
	// It has to be captured rather than loaded afterwards. A resumed SHARED membership is
	// arbitrated by a further record write before the CONNACK (F-077), and `$MQTT_sess`
	// keeps one message per subject — so that second write DELETES the accept save's
	// record, and "load the record after the reconnect" cannot see it at all. Between the
	// two writes is the only moment it exists, which is exactly where this hook runs.
	var acceptRecord *mqttPersistedSession
	restoreCount := setMQTTTestSessStoreResultHook(func(cur *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if cur == sess && err == nil {
			landed++
			if acceptRecord == nil {
				if got, _, lerr := asm.loadFenceSession(sess.idHash); lerr == nil && got != nil {
					acceptRecord = got
				}
			}
		}
		return resp, err
	})
	defer restoreCount()

	sub.Close()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("the close has not detached the session yet")
		}
		if sess.voidedRegsRev <= sess.voidedRegsSavedRev {
			return fmt.Errorf("no unsaved void-register revision staged")
		}
		return nil
	})

	sess.mu.Lock()
	rev := sess.voidedRegsRev
	nonces := mqttSortedNonces(sess.cancelledRegNonces)
	sess.mu.Unlock()
	if len(nonces) == 0 {
		t.Fatal("the close must have staged at least one void-register verdict")
	}
	// The two halves, asserted separately. The proven-unsent delivery's verdict is what the
	// credit machinery below inherits; the TRANSMITTED delivery must have none, because a
	// teardown never establishes that the client completed and recovery reads a verdict as
	// licence to free the identifier (F-101).
	sess.mu.Lock()
	_, sentVerdicted := sess.cancelledRegNonces[sentNonce]
	_, unsentVerdicted := sess.cancelledRegNonces[unsentNonce]
	_, sentStillReserved := sess.pendingPublish[1]
	sess.mu.Unlock()
	if !unsentVerdicted {
		t.Fatalf("the proven-unsent delivery's verdict must be staged, so there is something to inherit; got %v", nonces)
	}
	if sentVerdicted {
		t.Fatalf("no never-transmitted verdict may name the TRANSMITTED delivery %q: the client received it", sentNonce)
	}
	if !sentStillReserved {
		t.Fatal("the transmitted delivery's identifier must stay reserved across the surviving close")
	}

	// Nothing landed during the whole close, so nothing there can have credited.
	if landed != 0 {
		t.Fatalf("no close-path write may have landed, got %d", landed)
	}

	// The register stream recovers so the resume can run; the record store stays refused
	// until the reconnect BINDS, so the accept save is the first landed write.
	asm.mu.Lock()
	asm.testInflightDelete, asm.testInflightStore, asm.testInflightLoad = nil, nil, nil
	asm.mu.Unlock()

	sub2, sub2r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, props: props}, o.MQTT.Host, o.MQTT.Port)
	defer sub2.Close()
	testMQTTReadConnAckV5(t, sub2r)

	if landed == 0 {
		t.Fatal("the reconnect's accept save must have landed")
	}

	// The record IT wrote carries the inherited verdict.
	ps := acceptRecord
	if ps == nil {
		t.Fatal("the accept save's own record was never captured, so what it carried is unknown")
	}
	for _, n := range nonces {
		found := false
		for _, got := range ps.CancelledRegs {
			if got == n {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("the accept save must carry the inherited verdict %q, got %v", n, ps.CancelledRegs)
		}
	}
	// ...and it credited what it marshalled, releasing the reservation that was waiting on
	// exactly that proof.
	asm.mu.RLock()
	cur := asm.sessions[clientID]
	asm.mu.RUnlock()
	if cur == nil {
		t.Fatal("the resumed session must be resident")
	}
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		cur.mu.Lock()
		defer cur.mu.Unlock()
		if cur.voidedRegsSavedRev < rev {
			return fmt.Errorf("the accept save did not credit the revision it marshalled: rev=%d saved=%d",
				rev, cur.voidedRegsSavedRev)
		}
		if len(cur.settledVoidReservations) != 0 {
			return fmt.Errorf("%d reservation(s) still parked on a proven verdict", len(cur.settledVoidReservations))
		}
		return nil
	})
}

// TestMQTTQoS2ExchangeRecoversFromForeignCustodyOnTheWire is F-092's open item #2 as a
// client sees it, over a real connection, a real PUBREL consumer and real AckWait.
//
// The stall: a successor resumes while a predecessor holds a live action on the slot, so
// recovery correctly withholds the custody transfer. When the predecessor's action later
// drains it clears the action fields and LEAVES the predecessor's claim behind. Nothing
// re-runs custody for a connection that is already up, so admission refuses the exchange
// on every redelivery and the client never receives its PUBREL. With Receive Maximum 1
// that stalls the session completely.
//
// The stalled state is INSTALLED on the durable slot rather than produced by a real
// takeover: that is exactly the shape a predecessor's drain leaves — a quiescent slot
// under another incarnation's claim — and reproducing it through two live brokers would
// exercise the takeover machinery rather than this repair. The cost of that choice is
// stated plainly: the recovery half of the schedule is covered by unit tests, and what
// this test establishes is the repair and the liveness that follows it, on the wire.
// Everything after that point is real: the record is re-offered by the consumer's own
// AckWait, the PUBREL arrives on the unchanged connection, the client completes it with
// PUBCOMP, and the identifier's Receive Maximum slot is released for the next message.
//
// Mutation check (F-105 7b, M16): re-add claim equality to the ordinary PUBREL admission and the
// PUBREL never arrives — the read below times out, which is precisely the stall. (Before 7b this
// was the custody repair; an ordinary slot's stamp now decides nothing and needs no repair.)
func TestMQTTQoS2ExchangeRecoversFromForeignCustodyOnTheWire(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	// A short AckWait is what makes the redelivery observable within the test's patience;
	// the repair itself is driven by that re-offer and nothing else.
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Receive Maximum 1: one stalled exchange stops the session receiving anything, so
	// the follow-up message at the end is a real liveness assertion rather than a nicety.
	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(3600)...)
	ci := &mqttV5ConnInfo{clientID: "f092-foreign-custody-wire", props: props}
	sub, subr := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})
	testMQTTFlush(t, sub, nil, subr)

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f092-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)

	// Take the exchange to PUBREC, which is where the owner slot is past PUBREC and its
	// PUBREL record exists — the state a takeover can withhold custody on.
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("stalled"), nil)
	_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("stalled"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)

	asm := testMQTTGetAccountSessionManager(t, s, "f092-foreign-custody-wire")
	asm.mu.RLock()
	sess := asm.sessions["f092-foreign-custody-wire"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen, ourClaim := sess.idHash, sess.gen, sess.regClaim
	sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)

	// INSTALL THE STALL: the slot keeps its exchange identity and its canonical PUBREL,
	// and only its custody becomes another incarnation's — with a strictly older
	// acceptance sequence, which is what a predecessor's claim carries.
	// WAIT FOR THE ACTION TO DRAIN FIRST. Immediately after the PUBREL write the slot is
	// still in `pubrel_sending` holding its action, and that is not the state this test is
	// about: the stall appears only once the holder's action has drained and left its
	// claim behind. It is also the state the shape validator refuses to rewrite, correctly.
	var rec *mqttInflightRecord
	var smsg *StoredMsg
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		var lerr error
		rec, smsg, lerr = asm.jsa.loadInflightRecord(subject)
		switch {
		case lerr != nil:
			return fmt.Errorf("loading the owner slot for identifier %d: %w", pi, lerr)
		case rec == nil || smsg == nil:
			return fmt.Errorf("no owner slot for identifier %d", pi)
		case rec.ActionToken != _EMPTY_:
			return fmt.Errorf("the holder's action has not drained yet (state %q)", rec.State)
		}
		return nil
	})
	if rec.Claim != ourClaim {
		t.Fatalf("the slot is not under this incarnation's custody to begin with (%+v)", rec)
	}

	// FIRST, THE WITHHOLDING RULE ITSELF: while the slot is ACTIVE (a live action), a
	// custody transfer must be refused. That refusal is the step which leaves the
	// predecessor's claim behind and creates the stall this test then exercises.
	//
	// STATED PRECISELY, because it is narrower than it looks: this drives the transfer
	// helper directly on an active slot. It does NOT resume a second session object, does
	// not install a successor's withheld reservation through recovery, and does not
	// witness a consumer delivery refused while the predecessor is still active. Those
	// belong to the recovery path and are covered by the unit tests over
	// claimRecoveredInflight; what is real here is everything after the drain — the
	// consumer's own re-offer, the repair, the client's PUBCOMP and the released
	// Receive Maximum slot.
	activeProbe := *rec
	// An action is the TOKEN, not a phase: the slot stays in the phase it reached and
	// gains a token. Staging a phase here would stage nothing.
	activeProbe.ActionToken = mqttNewInflightNonce()
	activeProbe.ActionClaim, activeProbe.ActionClaimSeq = activeProbe.Claim, activeProbe.ClaimSeq
	withheldSeq, err := asm.jsa.storeInflightCAS(subject, smsg.Sequence, &activeProbe)
	if err != nil {
		t.Fatalf("installing the active slot: %v", err)
	}
	sess.mu.Lock()
	beforeClaim := sess.regClaim
	sess.mu.Unlock()
	if ok := sess.transferCustodyForAdmission(sess.c, subject, withheldSeq, &activeProbe); ok {
		t.Fatal("custody was transferred out from under a LIVE action: the action is its " +
			"holder's capability and the transfer must be withheld until it drains")
	}
	activeNow, activeMsg, err := asm.jsa.loadInflightRecord(subject)
	if err != nil || activeNow == nil {
		t.Fatalf("reloading the active slot: %v", err)
	}
	if activeNow.Claim == beforeClaim && activeNow.ActionToken == _EMPTY_ {
		t.Fatal("the fixture did not actually hold an active slot; the withholding assertion " +
			"above proves nothing")
	}

	// NOW THE DRAIN: the action goes away and the predecessor's claim stays, which is
	// exactly what startActionDrain leaves behind and exactly where the stall begins.
	drained := *activeNow
	drained.State = mqttInflightPubRelReady
	drained.ActionToken, drained.ActionClaim, drained.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	if _, err := asm.jsa.storeInflightCAS(subject, activeMsg.Sequence, &drained); err != nil {
		t.Fatalf("draining the action: %v", err)
	}
	rec, smsg, err = asm.jsa.loadInflightRecord(subject)
	if err != nil || rec == nil || smsg == nil {
		t.Fatalf("reloading the drained slot: %v", err)
	}

	stalled := *rec
	stalled.Claim = mqttNewInflightNonce()
	if stalled.ClaimSeq > 0 {
		stalled.ClaimSeq--
	}
	if _, err := asm.jsa.storeInflightCAS(subject, smsg.Sequence, &stalled); err != nil {
		t.Fatalf("installing the foreign-claimed slot: %v", err)
	}

	// The client has not sent PUBCOMP, so the server re-sends the PUBREL
	// ([MQTT-4.4.0-1]) and the consumer re-offers the record. Under the stall every one
	// of those is refused; with the repair the first re-offer takes custody and the
	// PUBREL reaches the client on this same connection.
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)

	// The durable proof, since F-105 7b: the foreign stamp DECIDES NOTHING on an ordinary slot
	// (ownership is the connection's accepted entitlement), so the exchange was served under it
	// and nothing had to rewrite custody. The slot still names this exchange, or it completed.
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		cur, _, lerr := asm.jsa.loadInflightRecord(subject)
		switch {
		case lerr != nil:
			return fmt.Errorf("loading the slot: %w", lerr)
		case cur == nil:
			return nil // the exchange completed and cleaned up, which is stronger
		case cur.Nonce != stalled.Nonce:
			return fmt.Errorf("the slot names another exchange %q", cur.Nonce)
		}
		return nil
	})
	_ = ourClaim

	// The client completes it, and the identifier's Receive Maximum slot is released:
	// the next message arrives, which a stalled session could not deliver.
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	testMQTTPubV5Props(t, pub, pubr, 2, false, 2, "foo", []byte("after"), nil)
	_, nextPI := testMQTTReadPublishV5(t, subr, "foo", []byte("after"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, nextPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, nextPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, nextPI)
	testMQTTFlush(t, sub, nil, subr)
}

// ---------------------------------------------------------------------------
// F-072 acceptance tests: DUP is derived from the durable pair, never from the
// JetStream delivery count. Both are deterministic and not load-sensitive (N-1.3, N-1.4).
// ---------------------------------------------------------------------------

// T1 (N-1.3): an offer DECLINED before trackPublish raises the JetStream delivery count,
// yet the first client-visible PUBLISH carries DUP=0. The count is not where DUP comes
// from; a refusal before tracking writes no register and records no transmission.
//
// Mutation M1: reinstate dup from dcount > 1 and consume it at the sender — the first
// PUBLISH then arrives with DUP set.
func TestMQTTAffineRefusalBeforeTrackingSendsFirstPublishWithoutDup(t *testing.T) {
	// NOT parallel: the pre-send hook is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-t1", 1)
	defer sub.Close()

	// Decline the first two offers of this delivery before trackPublish — the shape of a
	// pre-tracking refusal — so AckWait re-offers it with a rising delivery count. The
	// admitting offer positively carries dcount > 1.
	// Admission is keyed on the delivery COUNT, not a call counter: silent drops are
	// re-offered by AckWait with a rising count, and keying on it directly makes the
	// admitted delivery's dcount deterministic rather than racing overlapping attempts.
	var admittedDcount uint64
	restore := setMQTTTestInflightPreSendHook(func(jsDur, reply string) bool {
		_, _, dc, _, _ := ackReplyInfo(reply)
		if dc < 3 {
			return false
		}
		admittedDcount = dc
		return true
	})
	defer restore()

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-t1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))

	// The FIRST client-visible PUBLISH: DUP=0, although the count that admitted it was > 1.
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if admittedDcount < 3 {
		t.Fatalf("the fixture must admit the delivery only after its count rose (dcount >= 3), got %d", admittedDcount)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// T2 (N-1.4): a broker that dies AFTER the pair is confirmed and BEFORE the socket write
// leaves the unsent marker intact, so on resume the delivery is re-offered under the
// register's own identifier with DUP=0 — a first transmission, not a redelivery.
//
// Mutation M2: omit the marker from the batch, or M3: seed transmitted regardless of the
// marker — the redelivery then arrives with DUP set.
func TestMQTTInflightUnsentMarkerRedeliversWithoutDup(t *testing.T) {
	// NOT parallel: the send-window hook is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-t2", 1)

	// The delivery under test is not identifier 1: two completed exchanges first.
	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-t2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte(payload))
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
	}

	// The seam: the FIRST send section that opens after the pair is confirmed aborts
	// before any byte reaches the socket. That is the crash-between-confirm-and-send
	// window; the pair stands, and its marker records that nothing was transmitted.
	confirmed := make(chan uint16, 1)
	var once sync.Once
	restore := setMQTTTestMarkerSendWindowHook(func(sess *mqttSession, pi uint16, window string) bool {
		if window != mqttMarkerSendReserved {
			return true
		}
		// Abort EVERY send section for this delivery, not just the first: AckWait would
		// otherwise re-offer it within this broker and slip a PUBLISH out before the
		// restart. The pair is confirmed by the time a section opens, so its marker
		// records "never transmitted" throughout.
		once.Do(func() { confirmed <- pi })
		return false
	})

	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	var firstPI uint16
	select {
	case firstPI = <-confirmed:
	case <-time.After(5 * time.Second):
		t.Fatal("the send section never opened after the pair was confirmed")
	}
	if firstPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce; the test would prove nothing", firstPI)
	}
	// The client saw nothing, and the marker beside the register is still present.
	testMQTTExpectNothing(t, subr)
	asm := testMQTTGetAccountSessionManager(t, s, "inflight-t2")
	asm.mu.RLock()
	sess := asm.sessions["inflight-t2"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	markerSubj := mqttInflightSubject(idHash, gen, firstPI) + "." + mqttInflightMarkerSuffix
	if smsg, err := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markerSubj); err != nil || smsg == nil {
		t.Fatalf("the unsent marker must survive an aborted send: msg=%v err=%v", smsg, err)
	}
	// Close the subscriber BEFORE restoring the hook: once the connection is gone the
	// send section is refused (sess.c != cc), so no AckWait re-offer can slip a PUBLISH
	// out and delete the marker in the window between restore and close. The hook is
	// still restored before the restart, so the post-resume redelivery proceeds.
	sub.Close()
	restore()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testInflightReconnect(t, o, "inflight-t2")
	defer sub.Close()

	// The redelivery is a FIRST transmission: the same identifier, DUP=0.
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if gotPI != firstPI {
		t.Fatalf("the resumed delivery used packet identifier %d, want the register's %d", gotPI, firstPI)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// T4 / T10: once the PUBLISH has actually been transmitted, its marker is gone and every
// later delivery of that identifier — an in-process AckWait redelivery, then one across a
// restart — carries DUP=1.
//
// Mutation M4: drop the sender's marker delete and the redelivery arrives DUP=0.
//
// AckWait is a fixture BUDGET here, not a claim: the warm-up exchanges must settle before it
// elapses, or a re-offer JetStream dispatches before it processes their acknowledgement
// reaches admission after their suppression lifts and is delivered again — the QoS 1
// residual docs/CONFORMANCE.md states. Under CPU starvation a 500 ms budget did not hold
// (a 590-680 ms release; 8 of 8 at the baseline, 7 of 8 after the parked-release handoff),
// so it is 2 s, as F-115 set for its own fixture; every assertion is unchanged.
// TestMQTTInflightParkedAckReleaseLeavesReOffersSuppressed keeps 500 ms and its controlled
// overlap for the mechanism the broker does close.
func TestMQTTInflightTransmittedRedeliversWithDup(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 2 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-t4", 1)

	pub, pubr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub-t4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte(payload))
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
	}

	// Transmit and never PUBACK: the first delivery is DUP=0.
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	firstPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if firstPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce; the test would prove nothing", firstPI)
	}

	// The in-process AckWait redelivery: DUP=1, same identifier, no restart involved.
	inProc := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if inProc != firstPI {
		t.Fatalf("the in-process redelivery used %d, want %d", inProc, firstPI)
	}
	sub.Close()

	// And across a restart: the marker was deleted at the first transmission, so recovery
	// installs the entry transmitted and the redelivery is still DUP=1.
	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)
	sub, subr = testInflightReconnect(t, o, "inflight-t4")
	defer sub.Close()
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if gotPI != firstPI {
		t.Fatalf("the redelivery after a restart used %d, want %d", gotPI, firstPI)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// ---------------------------------------------------------------------------
// The crash matrix around durable send intent (F-072 redesign, contract S-5). Intent is the
// marker's CONFIRMED delete, issued after admission and before the socket write. The two
// windows it splits have different, deliberate answers on recovery: before intent the pair
// is intact and the re-offer is a first transmission (DUP=0, T2 = X1); after it the marker is
// gone and recovery cannot know whether the write happened, so it answers DUP=1 and holds
// the identifier — the conservative reading the user accepted in place of an attribution no
// protocol can make. Every fixture here runs on real persisted pairs and a real restart.
// ---------------------------------------------------------------------------

// testInflightWarmAndTarget connects a subscriber and publisher, completes two exchanges so
// the delivery under test is not identifier 1, and returns everything the fixture needs.
func testInflightWarmAndTarget(t *testing.T, o *Options, subID, pubID string) (sub net.Conn, subr *mqttReader, pub net.Conn, pubr *mqttReader) {
	t.Helper()
	sub, subr = testInflightSubscriber(t, o, subID, 1)
	pub, pubr = testMQTTConnect(t, &mqttConnInfo{clientID: pubID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	for _, payload := range []string{"one", "two"} {
		testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte(payload))
		pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte(payload))
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
		testMQTTFlush(t, sub, nil, subr)
	}
	return sub, subr, pub, pubr
}

// testMQTTWaitSendSectionClosed blocks until the send section this identifier is under has
// been RELEASED, which is the only barrier that orders a reader against the delivery
// callback.
//
// Receiving the PUBLISH proves the bytes left the write path; it proves nothing about the
// section. `endPublishSend` still has to drain the durable action — a JetStream round trip —
// before `endSendWith` applies the outcome to the entry and discharges any settlement the
// acknowledgement parked. A PINGRESP flush does not order it either: that flush orders the
// MQTT read loop, and the delivery callback runs on its own goroutine.
//
// The marker is the SECTION, not the fields a caller is about to assert. `endSendWith`
// clears the hold's token, applies `onEntry` and untracks a settled entry inside one locked
// region, so a cleared token proves the outcome is already visible — while leaving every
// assertion about WHAT that outcome is to the caller. Waiting on the asserted fields
// themselves would turn a broker defect into a timeout with no state to read.
// testInflightSession returns the resident session for clientID.
func testInflightSession(t *testing.T, s *Server, clientID string) *mqttSession {
	t.Helper()
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for %q", clientID)
	}
	return sess
}

func testMQTTWaitSendSectionClosed(t *testing.T, sess *mqttSession, pi uint16) {
	t.Helper()
	if sess == nil || pi == 0 {
		t.Fatalf("no session or identifier to wait on (pi=%d)", pi)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		sess.mu.Lock()
		h := sess.sendsInFlight[pi]
		closed := h == nil || h.token == _EMPTY_
		var held, entry string
		if !closed {
			held = fmt.Sprintf("%+v", *h)
			if ack := sess.pendingPublish[pi]; ack != nil {
				entry = fmt.Sprintf("%+v", *ack)
			}
		}
		sess.mu.Unlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			// NOT a timeout to widen. A section that never closes is a drain that never
			// landed, which is the broker holding the identifier — report the state that
			// proves it.
			t.Fatalf("the send section for packet identifier %d never closed: hold %s, entry %s", pi, held, entry)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// testInflightEntry reads the live entry for pi on the named session, under its lock.
func testInflightEntry(t *testing.T, s *Server, clientID string, pi uint16) (ack mqttPending, tracked bool) {
	t.Helper()
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for %q", clientID)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	cur := sess.pendingPublish[pi]
	if cur == nil {
		return mqttPending{}, false
	}
	return *cur, true
}

// X2: a crash AFTER intent is confirmed and BEFORE the write. The marker is gone, so recovery
// installs the entry possibly-attempted: same identifier, DUP=1, identifier held.
// Mutation M59: record intent fire-and-forget and the restart may find the marker present;
// M61: seed DUP from the marker's presence at recovery inverted.
func TestMQTTInflightCrashAfterIntentRedeliversWithDup(t *testing.T) {
	// NOT parallel: the send-window hook is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub, subr, pub, pubr := testInflightWarmAndTarget(t, o, "inflight-x2", "pub-x2")
	defer pub.Close()

	// Abort EVERY section for this delivery at the INTENT window — intent has been
	// confirmed, nothing has been written — until restore().
	intentSeen := make(chan uint16, 1)
	var once sync.Once
	restore := setMQTTTestMarkerSendWindowHook(func(sess *mqttSession, pi uint16, window string) bool {
		if window != mqttMarkerSendIntent {
			return true
		}
		once.Do(func() { intentSeen <- pi })
		return false
	})

	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	var firstPI uint16
	select {
	case firstPI = <-intentSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("the section never reached the intent window")
	}
	if firstPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce", firstPI)
	}
	testMQTTExpectNothing(t, subr)
	// Intent LANDED before the abort: the marker is gone durably, the entry is retained.
	asm := testMQTTGetAccountSessionManager(t, s, "inflight-x2")
	asm.mu.RLock()
	sess := asm.sessions["inflight-x2"]
	asm.mu.RUnlock()
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	markerSubj := mqttInflightSubject(idHash, gen, firstPI) + "." + mqttInflightMarkerSuffix
	if smsg, err := asm.jsa.loadLastMsgFor(mqttInflightStreamName, markerSubj); err == nil && smsg != nil {
		t.Fatalf("the intent delete must have landed before the write window: marker still at %d", smsg.Sequence)
	}
	// The window hook fires INSIDE the section; the outcome it produces is applied by
	// endSendWith when that section closes, so the entry is read after the section, never
	// after the signal.
	testMQTTWaitSendSectionClosed(t, sess, firstPI)
	if cur, tracked := testInflightEntry(t, s, "inflight-x2", firstPI); !tracked || !cur.transmitted || cur.markerSeq != 0 {
		t.Fatalf("after confirmed intent the live entry is possibly-attempted and retained, got tracked=%v %+v", tracked, cur)
	}
	sub.Close()
	restore()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)
	sub, subr = testInflightReconnect(t, o, "inflight-x2")
	defer sub.Close()

	// Recovery cannot tell this from a write that happened: DUP=1, the SAME identifier.
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if gotPI != firstPI {
		t.Fatalf("the resumed delivery used packet identifier %d, want the held %d", gotPI, firstPI)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// X2b: the intent delete's response is lost but the delete itself NEVER landed, then a crash.
// The pair is intact, so recovery answers proven-unsent: same identifier, DUP=0. Together
// with X2 this pins that recovery reads the DURABLE state and nothing else.
func TestMQTTInflightCrashDuringUnlandedIntentRedeliversWithoutDup(t *testing.T) {
	// NOT parallel: the marker delete hook is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub, subr, pub, pubr := testInflightWarmAndTarget(t, o, "inflight-x2b", "pub-x2b")
	defer pub.Close()

	// Every SENDER delete answers ambiguously and is never forwarded to the stream.
	attempted := make(chan uint16, 8)
	restore := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller != mqttMarkerCallerSender {
			return nil
		}
		select {
		case attempted <- 1:
		default:
		}
		return errors.New("test: the intent delete's response was lost")
	})
	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("no intent delete was attempted")
	}
	testMQTTExpectNothing(t, subr)
	sub.Close()
	restore()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)
	sub, subr = testInflightReconnect(t, o, "inflight-x2b")
	defer sub.Close()
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if gotPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce", gotPI)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// X3: a crash AFTER the write and before the PUBACK, with persistence STOPPED after the
// write. Intent was recorded before the write, so recovery answers DUP=1 under the same
// identifier. This is the regression for holistic Finding 1: under mutation M1 — record the
// transmission AFTER the write, fire-and-forget — the suppressed post-write delete leaves the
// marker in place and the redelivery arrives DUP=0. (T4 alone cannot catch M1: its
// uncontrolled post-write delete may land before the restart.)
func TestMQTTInflightCrashAfterWriteRedeliversWithDup(t *testing.T) {
	// NOT parallel: the hooks are process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub, subr, pub, pubr := testInflightWarmAndTarget(t, o, "inflight-x3", "pub-x3")
	defer pub.Close()

	// Persistence stops the instant the write returns: any SENDER delete issued after the
	// WROTE window is refused. Under the real protocol there is none (intent preceded the
	// write), so this is inert; under M1 it is the crash that loses the record.
	var wrote atomic.Bool
	r1 := setMQTTTestMarkerSendWindowHook(func(sess *mqttSession, pi uint16, window string) bool {
		if window == mqttMarkerSendWrote {
			wrote.Store(true)
		}
		return true
	})
	defer r1()
	r2 := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller == mqttMarkerCallerSender && wrote.Load() {
			return errors.New("test: persistence stopped after the write")
		}
		return nil
	})
	defer r2()

	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	firstPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if firstPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce", firstPI)
	}
	sub.Close()
	r2()
	r1()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)
	sub, subr = testInflightReconnect(t, o, "inflight-x3")
	defer sub.Close()
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if gotPI != firstPI {
		t.Fatalf("the redelivery after a restart used %d, want %d", gotPI, firstPI)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// X4: the intent delete answers AMBIGUOUSLY and no crash follows. Nothing is written, the
// identifier is held unresolved, and the next offer RE-ATTEMPTS the same exact delete — a
// delete, never a readback — which lands, so the PUBLISH goes out DUP=0 under the same
// identifier. Mutation M62: treat an ambiguous outcome as confirmed and write.
func TestMQTTInflightAmbiguousIntentWritesNothingAndReattempts(t *testing.T) {
	// NOT parallel: the hooks are process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub, subr, pub, pubr := testInflightWarmAndTarget(t, o, "inflight-x4", "pub-x4")
	defer pub.Close()
	defer sub.Close()

	var mu sync.Mutex
	var deletes []uint64
	var reloads int
	firstSection := make(chan struct{}, 1)
	r1 := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller != mqttMarkerCallerSender {
			return nil
		}
		mu.Lock()
		deletes = append(deletes, seq)
		first := len(deletes) <= mqttInflightStoreAttempts // the whole first confirmed attempt, retries included
		mu.Unlock()
		if first {
			select {
			case firstSection <- struct{}{}:
			default:
			}
			return errors.New("test: no response")
		}
		return nil
	})
	defer r1()
	r2 := setMQTTTestMarkerReloadHook(func(caller, subject string) {
		if caller == mqttMarkerCallerSender {
			mu.Lock()
			reloads++
			mu.Unlock()
		}
	})
	defer r2()

	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	select {
	case <-firstSection:
	case <-time.After(5 * time.Second):
		t.Fatal("no intent delete was attempted")
	}
	// The first section: no PUBLISH, and the entry is held UNRESOLVED with its marker.
	testMQTTExpectNothing(t, subr)
	// The AckWait re-offer re-attempts the delete, which now lands: DUP=0, same identifier.
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if gotPI <= 1 {
		t.Fatalf("packet identifier %d is what a fresh allocator would produce", gotPI)
	}
	mu.Lock()
	dels, rl := append([]uint64(nil), deletes...), reloads
	mu.Unlock()
	if len(dels) < mqttInflightStoreAttempts+1 {
		t.Fatalf("the re-offer must RE-ATTEMPT the intent delete, saw %d sender delete(s): %v", len(dels), dels)
	}
	for _, seq := range dels[1:] {
		if seq != dels[0] {
			t.Fatalf("every attempt targets the same captured sequence %d, got %v", dels[0], dels)
		}
	}
	if rl != 0 {
		t.Fatalf("an unresolved intent is settled by re-attempting, never by a readback; saw %d sender reload(s)", rl)
	}
	// The three fields below are written by endSendWith, which runs after the durable
	// action is drained — receiving the PUBLISH does not order them.
	testMQTTWaitSendSectionClosed(t, testInflightSession(t, s, "inflight-x4"), gotPI)
	if cur, tracked := testInflightEntry(t, s, "inflight-x4", gotPI); !tracked || cur.intentUnresolved || cur.markerSeq != 0 || !cur.transmitted {
		t.Fatalf("after the landed re-attempt the entry is possibly-attempted, got tracked=%v %+v", tracked, cur)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// X5: the intent delete is DEFINITELY refused (the server answered). Nothing is written and
// the entry stays exactly proven-unsent — marker held, not unresolved — so the next offer,
// once the refusal clears, is a first transmission with DUP=0. Mutation M63: treat a definite
// refusal as confirmed and write.
func TestMQTTInflightRefusedIntentWritesNothingAndStaysProvenUnsent(t *testing.T) {
	// NOT parallel: the hooks are process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub, subr, pub, pubr := testInflightWarmAndTarget(t, o, "inflight-x5", "pub-x5")
	defer pub.Close()
	defer sub.Close()

	var refusing atomic.Bool
	refusing.Store(true)
	refused := make(chan uint64, 8)
	restore := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller != mqttMarkerCallerSender || !refusing.Load() {
			return nil
		}
		select {
		case refused <- seq:
		default:
		}
		// An explicit PRE-MUTATION refusal: the one shape that proves nothing landed. (A
		// generic delete failure is ambiguous — the store may have recorded the deletion
		// before failing — and is X7's subject.)
		return &ApiError{Code: 400, ErrCode: uint16(JSBadRequestErr), Description: "test: refused before any mutation"}
	})
	defer restore()

	testMQTTPublish(t, pub, pubr, 1, false, false, "foo", 1, []byte("payload"))
	var markerSeq uint64
	select {
	case markerSeq = <-refused:
	case <-time.After(5 * time.Second):
		t.Fatal("no intent delete was attempted")
	}
	testMQTTExpectNothing(t, subr)
	// Find the identifier through the session: the entry must still hold its marker.
	asm := testMQTTGetAccountSessionManager(t, s, "inflight-x5")
	asm.mu.RLock()
	sess := asm.sessions["inflight-x5"]
	asm.mu.RUnlock()
	var pi uint16
	sess.mu.Lock()
	for p, ack := range sess.pendingPublish {
		if ack.markerSeq == markerSeq {
			pi = p
			if ack.intentUnresolved || ack.transmitted {
				sess.mu.Unlock()
				t.Fatalf("a definite refusal leaves the entry exactly proven-unsent, got %+v", ack)
			}
		}
	}
	sess.mu.Unlock()
	if pi == 0 {
		t.Fatalf("no live entry holds the refused marker %d", markerSeq)
	}
	// The refusal clears; the next offer is a FIRST transmission, DUP=0, same identifier.
	refusing.Store(false)
	gotPI := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo", mqttPubQos1, []byte("payload"))
	if gotPI != pi {
		t.Fatalf("the re-offer used packet identifier %d, want the held %d", gotPI, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, gotPI)
	testMQTTFlush(t, sub, nil, subr)
}

// testInflightPIFromMarkerSubject reads the packet identifier out of an unsent marker's
// subject, so a fixture can name the exchange under test from INSIDE the send window
// rather than searching the session afterwards for the property it is about to assert.
func testInflightPIFromMarkerSubject(t *testing.T, subject string) uint16 {
	t.Helper()
	parts := strings.Split(subject, ".")
	if len(parts) < 2 || parts[len(parts)-1] != mqttInflightMarkerSuffix {
		t.Fatalf("%q is not an unsent marker subject", subject)
	}
	pi, err := strconv.ParseUint(parts[len(parts)-2], 10, 16)
	if err != nil || pi == 0 {
		t.Fatalf("no packet identifier in marker subject %q: %v", subject, err)
	}
	return uint16(pi)
}

// testInflightRequireReoffers waits until the QoS 1/2 delivery callback has run want more
// times than it had at base.
//
// This is what a fixed sleep never established. The property under test is that an elapsed
// Message Expiry Interval does not release an entry whose intent is UNRESOLVED, and it is
// only tested when a re-offer actually MEETS that entry with the interval elapsed. On a
// slow machine the sleep could pass with no re-offer at all, and the fixture would then be
// asserting that an entry nothing had examined was still held.
func testInflightRequireReoffers(t *testing.T, runs *atomic.Int32, base, want int32) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for runs.Load()-base < want {
		if time.Now().After(deadline) {
			t.Fatalf("the delivery callback ran %d times with the interval elapsed, want %d: no "+
				"re-offer ever met the held entry, so nothing here tests the gate",
				runs.Load()-base, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// X6 (holistic round 2, finding 1, plain path): Message Expiry elapses while an exchange's
// intent is UNRESOLVED. The expiry gate must not drop it — its marker may already be gone,
// so it may have reached the client — and once the intent settles the PUBLISH goes out
// under the held identifier. Mutation M64 (gate on !transmitted alone) drops it.
func TestMQTTInflightExpiryDoesNotReleaseAnUnresolvedIntent(t *testing.T) {
	// NOT parallel: the marker delete hook is process-global.
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = 400 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "x6-sub", cleanStart: false,
		props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "x6/topic", opts: 1}})
	testMQTTFlush(t, cs, nil, rs)

	// THE IDENTIFIER IS CAPTURED FROM THE HOOK, not scanned for by the property under
	// test. Scanning for the entry that carries `intentUnresolved` conflates FINDING the
	// exchange with ASSERTING its outcome: the scan fails when the bit is missing, so the
	// assertion below can never fail, and the fixture's only barrier was a quiet socket.
	//
	// The hook fires INSIDE the send section, so a section provably exists from this point
	// and the section waiter below is meaningful — calling it without that proof would
	// return at once on an absent hold.
	var ambiguous atomic.Bool
	ambiguous.Store(true)
	sectionPI := make(chan uint16, 1)
	// THE RE-OFFER COUNTER LIVES HERE, on the hook that is already load-bearing. Every
	// AckWait re-offer that gets PAST the expiry gate opens a send section and attempts
	// this same intent delete, so a rising count is proof of exactly the event the
	// property needs: a re-offer that MET the held entry with the interval elapsed. A
	// delivery-callback counter would not do for the affine path, which has its own
	// callback, and a sleep proves nothing on either.
	var reoffers atomic.Int32
	restore := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller == mqttMarkerCallerSender && ambiguous.Load() {
			reoffers.Add(1)
			select {
			case sectionPI <- testInflightPIFromMarkerSubject(t, subject):
			default:
			}
			return errors.New("test: the intent delete's response was lost")
		}
		return nil
	})
	defer restore()

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "x6-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	// A 30 s interval: the stored message's JetStream TTL (real time) never fires in this
	// test, while the delivery gate's clock is jumped past the interval below.
	testMQTTPubV5Props(t, cp, rp, 1, false, 30, "x6/topic", []byte("held"), mqttV5PropsWithExpiry(30))

	// The first offer records an AMBIGUOUS intent and writes nothing.
	testMQTTExpectNothing(t, rs)
	var pi uint16
	select {
	case pi = <-sectionPI:
	case <-time.After(5 * time.Second):
		t.Fatal("no send section reached the intent window")
	}
	// The outcome is applied by endSendWith when that section CLOSES; receiving nothing
	// does not order it.
	testMQTTWaitSendSectionClosed(t, testInflightSession(t, s, "x6-sub"), pi)
	ack, tracked := testInflightEntry(t, s, "x6-sub", pi)
	if !tracked || !ack.intentUnresolved {
		t.Fatalf("the ambiguous intent must leave THIS entry held unresolved, got tracked=%v %+v",
			tracked, ack)
	}
	// Now the interval ELAPSES for the gate. The clock IS the instrument: every call is
	// the gate deciding on a re-offer, so waiting for two proves the elapsed interval was
	// actually met by the entry twice — which a fixed sleep never established.
	// TWO INSTRUMENTS, and they measure different halves of one property.
	//
	// The re-offer counter proves re-offers actually MET the held entry while the interval
	// was elapsed — without it the assertions below are about an entry nothing examined.
	// The clock counter proves WHERE the gate stopped: only an entry classified
	// proven-unsent reaches the interval arithmetic, so an unresolved one must never read
	// this clock at all. Mutation M64 (gate on !transmitted alone) turns that zero into a
	// read, an expiry and a released identifier.
	var clockReads atomic.Int32
	base := reoffers.Load()
	unclock := setMQTTTestExpiryNow(func() time.Time {
		clockReads.Add(1)
		return time.Now().Add(60 * time.Second)
	})
	defer unclock()
	testInflightRequireReoffers(t, &reoffers, base, 2)
	if n := clockReads.Load(); n != 0 {
		t.Fatalf("an entry held with an UNRESOLVED intent must never reach the expiry "+
			"arithmetic — its marker may already be gone, so it may have reached the client — "+
			"but the gate's clock was read %d times", n)
	}
	ack2, tracked2 := testInflightEntry(t, s, "x6-sub", pi)
	if !tracked2 {
		t.Fatalf("the held identifier %d was released by an elapsed expiry", pi)
	}
	ack = ack2
	if ack.transmitted || ack.markerSeq == 0 || !ack.intentUnresolved {
		t.Fatalf("the unresolved entry keeps its captured marker and is not transmitted, got %+v", ack)
	}
	asm := testMQTTGetAccountSessionManager(t, s, "x6-sub")
	if smsg, err := asm.jsa.loadLastMsgFor(mqttInflightStreamName, ack.slot.name.subject); err != nil || smsg == nil {
		t.Fatalf("the register must survive an elapsed expiry while the intent is unresolved: msg=%v err=%v", smsg, err)
	}

	// The intent settles: the PUBLISH goes out under the HELD identifier as a first
	// transmission. Its arrival is also the proof that the expiry gate acked nothing away.
	ambiguous.Store(false)
	flags, gotPI := testMQTTReadPublishV5(t, rs, "x6/topic", []byte("held"))
	if gotPI != pi || flags&mqttPubFlagDup != 0 {
		t.Fatalf("want the held identifier %d with DUP=0, got pi=%d flags=%x", pi, gotPI, flags)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, cs, gotPI)
	testMQTTFlush(t, cs, nil, rs)
}

// X6a (holistic round 2, finding 1, AFFINE path): the same property on the shared QoS 2
// affine callback, which had its own expiry gate. Mutation M67: gate that callback on
// !transmitted alone.
func TestQ2AffExpiryDoesNotReleaseAnUnresolvedIntent(t *testing.T) {
	// NOT parallel: the marker delete hook is process-global.
	b := testQ2AffSetup(t, "x6aff")
	c, r := b.member(t, "x6affA", 300)
	defer c.Close()

	var ambiguous atomic.Bool
	ambiguous.Store(true)
	sectionPI := make(chan uint16, 1)
	// THE RE-OFFER COUNTER LIVES HERE, on the hook that is already load-bearing. Every
	// AckWait re-offer that gets PAST the expiry gate opens a send section and attempts
	// this same intent delete, so a rising count is proof of exactly the event the
	// property needs: a re-offer that MET the held entry with the interval elapsed. A
	// delivery-callback counter would not do for the affine path, which has its own
	// callback, and a sleep proves nothing on either.
	var reoffers atomic.Int32
	restore := setMQTTTestMarkerDeleteHook(func(caller, subject string, seq uint64) error {
		if caller == mqttMarkerCallerSender && ambiguous.Load() {
			reoffers.Add(1)
			select {
			case sectionPI <- testInflightPIFromMarkerSubject(t, subject):
			default:
			}
			return errors.New("test: the intent delete's response was lost")
		}
		return nil
	})
	defer restore()

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "x6aff-pub", cleanStart: true}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 2, false, 1, b.topic, []byte("held"), mqttV5PropsWithExpiry(30))

	if _, pl, _, _, got := testMQTTTryReadPublishV5(r, 800*time.Millisecond); got {
		t.Fatalf("nothing may be written while the intent is unresolved, got %q", pl)
	}
	var pi uint16
	select {
	case pi = <-sectionPI:
	case <-time.After(5 * time.Second):
		t.Fatal("no send section reached the intent window")
	}
	testMQTTWaitSendSectionClosed(t, testInflightSession(t, b.s, "x6affA"), pi)
	ack, tracked := testInflightEntry(t, b.s, "x6affA", pi)
	if !tracked || !ack.intentUnresolved {
		t.Fatalf("the ambiguous intent must leave THIS entry held unresolved, got tracked=%v %+v",
			tracked, ack)
	}
	// TWO INSTRUMENTS, and they measure different halves of one property.
	//
	// The re-offer counter proves re-offers actually MET the held entry while the interval
	// was elapsed — without it the assertions below are about an entry nothing examined.
	// The clock counter proves WHERE the gate stopped: only an entry classified
	// proven-unsent reaches the interval arithmetic, so an unresolved one must never read
	// this clock at all. Mutation M64 (gate on !transmitted alone) turns that zero into a
	// read, an expiry and a released identifier.
	var clockReads atomic.Int32
	base := reoffers.Load()
	unclock := setMQTTTestExpiryNow(func() time.Time {
		clockReads.Add(1)
		return time.Now().Add(60 * time.Second)
	})
	defer unclock()
	testInflightRequireReoffers(t, &reoffers, base, 2)
	if n := clockReads.Load(); n != 0 {
		t.Fatalf("an entry held with an UNRESOLVED intent must never reach the expiry "+
			"arithmetic — its marker may already be gone, so it may have reached the client — "+
			"but the gate's clock was read %d times", n)
	}
	ack2, tracked2 := testInflightEntry(t, b.s, "x6affA", pi)
	if !tracked2 {
		t.Fatalf("the held identifier %d was released by an elapsed expiry", pi)
	}
	ack = ack2
	if ack.transmitted || ack.markerSeq == 0 || !ack.intentUnresolved {
		t.Fatalf("the unresolved entry keeps its captured marker and is not transmitted, got %+v", ack)
	}
	asm := testMQTTGetAccountSessionManager(t, b.s, "x6affA")
	if smsg, err := asm.jsa.loadLastMsgFor(mqttInflightStreamName, ack.slot.name.subject); err != nil || smsg == nil {
		t.Fatalf("the register must survive an elapsed expiry while the intent is unresolved: msg=%v err=%v", smsg, err)
	}

	ambiguous.Store(false)
	flags, gotPI := testMQTTReadPublishV5(t, r, b.topic, []byte("held"))
	if gotPI != pi || flags&mqttPubFlagDup != 0 {
		t.Fatalf("want the held identifier %d with DUP=0, got pi=%d flags=%x", pi, gotPI, flags)
	}
	b.completeExchange(t, c, r, gotPI)
}

// T2 (F-099, arm A, on the wire). The message must not be lost.
//
// The unit test proves the durable slot survives; only this proves the client is still
// served. A peer stores the PUBREL and re-phases the slot between the resumed pass's
// register load and its PUBREL scan, and under the HEAD ordering recovery read its earlier
// scan, declared the exchange finished, advanced the slot to terminal and deleted the
// record — the client never received the PUBREL and never sent PUBCOMP.
//
// The staging is scoped to the RESUMED pass and returns the REAL retained record at its
// actual sequence: a hook armed for the whole test also perturbs the pre-restart recovery,
// and a synthetic record would not be the one admission delivers.
func TestMQTTInflightStalePubRelScanDoesNotLoseTheClientsPubRel(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(3600)...)
	ci := &mqttV5ConnInfo{clientID: "f099-wire", props: props}
	sub, subr := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f099-wire-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("owed"), nil)

	_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("owed"))
	// PUBREC only. The exchange is now past PUBREC with its PUBREL stored, and the client
	// is owed that PUBREL. Read it once so the record is certainly durable, then drop the
	// connection without ever answering PUBCOMP.
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	// Arm the staging on the RESUMED pass only. The first call answers as the peer's store
	// had not yet happened; every later call returns the real scan. That is precisely the
	// window a peer's PUBREC store lands in.
	// The session manager is per ACCOUNT, so a throwaway client on the restarted broker
	// reaches it before the subscriber resumes. The lookup needs some connected client, and
	// the subscriber cannot be it: arming the hook after the subscriber connects would be
	// after the very pass this test stages.
	arm, armr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f099-wire-arm", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer arm.Close()
	testMQTTReadConnAckV5(t, armr)
	asm := testMQTTGetAccountSessionManager(t, s, "f099-wire-arm")
	// THE TRIGGER IS THE REGISTER ENUMERATION, not a call count, and the distinction is the
	// whole test. Keyed on "the first scan returns empty", the staging fires on the pass's
	// only scan under BOTH orderings and would fail the repaired broker too. Keyed on the
	// enumeration, it models what actually happens: the peer's store lands after this pass
	// has looked at the registers, so a scan taken before them misses it and a scan taken
	// after them does not.
	//
	// The enumeration hook is a PASS-THROUGH — it answers exactly as the unhooked path does
	// and only records that it ran — so nothing about the pass changes except what the scan
	// can see.
	var enumerated atomic.Bool
	asm.mu.Lock()
	asm.testInflightEnumerate = func(filter string) (map[string]uint64, error) {
		subjects, _, err := asm.jsa.streamSubjects(mqttInflightStreamName, filter)
		enumerated.Store(true)
		return subjects, err
	}
	asm.testInflightPubRelScan = func(subject string) ([]mqttStoredPubRel, int, bool) {
		if !enumerated.Load() {
			return nil, 0, true // read before the registers: the peer's store is not visible
		}
		return f099RealPubRelScan(asm, subject)
	}
	asm.mu.Unlock()

	sub, subr = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, subr); !present {
		t.Fatalf("the session must be resumed")
	}

	// THE ORACLE. The client is still owed its PUBREL, and completing the exchange with
	// PUBCOMP is what proves the record was not disposed of behind its back.
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
}

// f099RealPubRelScan is the unhooked scan, so a staged hook can answer "not yet" once and
// then get out of the way. Duplicated rather than delegated because scanPubRelRecords
// consults the hook at its entry, so calling it from inside the hook would recurse.
func f099RealPubRelScan(asm *mqttAccountSessionManager, subject string) ([]mqttStoredPubRel, int, bool) {
	var records []mqttStoredPubRel
	refused := 0
	for seq := uint64(1); ; {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttOutStreamName, subject, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return records, refused, false
			}
			return records, refused, true
		}
		seq = smsg.Sequence + 1
		shape, derr := mqttDecodeStoredPubRel(smsg.Header)
		if derr != nil {
			refused++
		}
		records = append(records, mqttStoredPubRel{
			seq: smsg.Sequence, pi: shape.pi, srcDur: shape.srcDur, srcSeq: shape.srcSeq,
			affRef: shape.affRef, owner: shape.owner, shapeErr: derr,
		})
	}
}

// T3 (F-099), re-expressed for the F-105 PUBCOMP arm (consult 25). The protection is the same —
// a PUBCOMP whose durable half fails must not cost a Receive Maximum 1 client its only in-flight
// slot for the life of the session — and the contract that delivers it has changed.
//
// It used to be delivered by an INFERENCE. The handler acked the canonical PUBREL whatever the
// terminal step concluded, so a failed PUBCOMP left "pubrel_ready + no record + a complete
// source", and recovery reaped that on absence alone. Now the PUBCOMP is recorded on the exchange
// and the slot in one batch, and the canonical PUBREL's ack is WITHHELD until that completion is
// durable. So the record survives the failure, the restart redelivers it, the client answers it
// with another PUBCOMP, and that PUBCOMP ends the exchange — no inference needed, and none
// permitted: for an ordinary slot "no record" is no proof of a PUBCOMP.
//
// The failure is held through the shutdown so the process-local completion worker cannot win a
// race with the restart and complete the exchange before it.
func TestMQTTInflightFailedPubCompKeepsItsPubRelAndCompletesAfterRestart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(3600)...)
	ci := &mqttV5ConnInfo{clientID: "f099-phantom", props: props}
	sub, subr := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, subr)
	testMQTTSubV5(t, sub, subr, 1, []mqttV5SubFilter{{topic: "foo", opts: 2}})

	asm := testMQTTGetAccountSessionManager(t, s, "f099-phantom")
	asm.mu.RLock()
	sess := asm.sessions["f099-phantom"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatal("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	// The STORED subject is the first return; the second is the delivery subject, where
	// nothing is ever stored (the F-099 original read that one, so its "the ack removed the
	// record" check held vacuously).
	pubRelSubject, _, _ := mqttPubRelNames(idHash, gen)

	pub, pubr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f099-phantom-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("phantom"), nil)

	_, pi := testMQTTReadPublishV5(t, subr, "foo", []byte("phantom"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)

	// The PUBCOMP needs a pubrel_ready slot with no action live: the send action drains
	// asynchronously, so wait for the durable state rather than inferring it from the wire.
	slotSubject := mqttInflightSubject(idHash, gen, pi)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		cur, _, err := asm.loadInflightRecord(slotSubject)
		if err != nil || cur == nil {
			return fmt.Errorf("the owner slot is not readable yet: %v", err)
		}
		if cur.State != mqttInflightPubRelReady || cur.ActionToken != _EMPTY_ {
			return fmt.Errorf("slot state = %q token %q, want %q with no action", cur.State, cur.ActionToken,
				mqttInflightPubRelReady)
		}
		return nil
	})

	// Refuse THIS exchange's PUBCOMP batch for as long as the broker runs, and observe every
	// canonical-PUBREL ack the PUBCOMP path sends.
	var refusals, acks atomic.Int64
	asm.mu.Lock()
	asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.Register != nil && req.Register.PI == pi && req.Register.State == mqttInflightPubRelReady {
			refusals.Add(1)
			return mqttOpOutcome{}, errors.New("test: the PUBCOMP completion cannot be persisted")
		}
		return write()
	}
	asm.testPubCompAck = func(string, bool) { acks.Add(1) }
	asm.mu.Unlock()

	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if refusals.Load() == 0 {
			return fmt.Errorf("the PUBCOMP completion was never attempted")
		}
		return nil
	})
	// THE ACK IS WITHHELD and the record survives: the evidence a retry needs is still there.
	if n := acks.Load(); n != 0 {
		t.Fatalf("the PUBCOMP path acknowledged the canonical PUBREL %d times over an unconfirmed completion", n)
	}
	if _, err := asm.jsa.loadNextMsgFromSeq(mqttOutStreamName, pubRelSubject, 1); err != nil {
		t.Fatalf("the canonical PUBREL was removed although its completion was never recorded: %v", err)
	}
	cur, _, err := asm.loadInflightRecord(slotSubject)
	if err != nil || cur == nil || cur.State != mqttInflightPubRelReady {
		t.Fatalf("the slot moved although its completion was refused: %+v (%v)", cur, err)
	}
	lin := cur.Lin
	sub.Close()

	s = testInflightRestart(t, s, o)
	defer testMQTTShutdownServer(s)

	sub, subr = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, subr); !present {
		t.Fatalf("the session must be resumed")
	}
	// The surviving PUBREL is redelivered, and the client's PUBCOMP now completes the exchange.
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)
	asm2 := testMQTTGetAccountSessionManager(t, s, "f099-phantom")
	asm2.mu.RLock()
	sess2 := asm2.sessions["f099-phantom"]
	asm2.mu.RUnlock()
	if sess2 == nil {
		t.Fatal("no resident session after the resume")
	}
	// The completion is DURABLE now: the exchange itself says so.
	testF105ExchangePhaseIs(t, sess2, lin, mqttExchangeTerminal)

	// THE ORACLE. The single Receive Maximum slot is free again, so a fresh message gets
	// through.
	pub.Close()
	pub, pubr = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "f099-phantom-pub2", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTReadConnAckV5(t, pubr)
	testMQTTPubV5Props(t, pub, pubr, 2, false, 1, "foo", []byte("after"), nil)
	_, got := testMQTTReadPublishV5(t, subr, "foo", []byte("after"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, got)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, got)
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, got)
	testMQTTFlush(t, sub, nil, subr)
}
