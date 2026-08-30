// Copyright 2025 The mqtt-nats-bridge Authors
//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
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
// Mutation check: remove the resolveRetiredSource call from the unproven-acknowledgement
// branch and both assertions below fail.
func TestMQTTInflightLandedButUnconfirmedAckLeavesNothing(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr := testInflightSubscriber(t, o, "inflight-ambiguous", 2)
	defer sub.Close()

	asm := testMQTTGetAccountSessionManager(t, s, "inflight-ambiguous")
	asm.mu.Lock()
	// The acknowledgement LANDS; only its confirmation is lost. That is the ambiguity.
	asm.testInflightSourceAck = func(ackSubject string) error {
		asm.jsa.sendAck(ackSubject)
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
	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	asm.mu.RLock()
	sess := asm.sessions["inflight-ambiguous"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()

	// No retirement is left holding state...
	checkFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if n := len(sess.srcAckOwed); n != 0 {
			return fmt.Errorf("%d retirement(s) left behind for a source that was acknowledged", n)
		}
		return nil
	})
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
func TestMQTTInflightRetainedSendNotRegistered(t *testing.T) {
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
	if pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "foo",
		mqttPubQos1|mqttPubFlagRetain, []byte("retained")); pi == 0 {
		t.Fatalf("a QoS 1 retained delivery must carry a packet identifier")
	}

	asm := testMQTTGetAccountSessionManager(t, s, "inflight-retained")
	asm.mu.RLock()
	sess := asm.sessions["inflight-retained"]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for the subscriber")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()

	filter := mqttInflightWalkFilter(idHash, gen)
	if _, err := asm.jsa.loadNextMsgFromSeq(mqttInflightStreamName, filter, 1); err == nil {
		t.Fatalf("a retained send must not write an in-flight register")
	} else if isErrorOtherThan(err, JSNoMessageFoundErr) {
		t.Fatalf("walk the registers: %v", err)
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
// Mutation checks: drop the marshal-revision credit in saveWithDetail's success arm and the
// credit assertion fails; skip the shared purge at the close and nothing is staged to inherit.
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
