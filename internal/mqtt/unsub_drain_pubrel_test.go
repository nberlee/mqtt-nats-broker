//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testDrainQoS2Restart shuts the broker down and brings it back on the same store,
// which is what clears every in-memory shortcut and forces the resume path to
// rebuild from the record alone.
func testDrainQoS2Restart(t *testing.T, s *Server, o *Options) *Server {
	t.Helper()
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	o.MQTT.Port = -1
	testSetStoreDir(o, dir)
	return testMQTTRunServer(t, o)
}

// TestMQTTDrainQoS2SurvivesRestartWithStoredPubRel is plan test 16 in its exact
// combined shape: after the restart the session restores NO subscription, one QoS 2
// drain still claiming a PUBLISH-phase delivery, AND a stored PUBREL for a
// different exchange that is already past PUBREC.
//
// The two halves travel different paths — the PUBREL replays from $MQTT_out, the
// claim is redelivered from $MQTT_msgs — and both have to work with nothing in
// memory to help. This is also the case that catches a resume which establishes
// PUBREL delivery only when it finds a drain, or source interest only when it finds
// a subscription.
func TestMQTTDrainQoS2SurvivesRestartWithStoredPubRel(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "q2restart"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "q2restart/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "q2restartpub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	for i, payload := range []string{"past-pubrec", "still-publish"} {
		testMQTTSendPublishPacket(t, pc, 2, false, false, "q2restart/topic", uint16(i+1), []byte(payload))
		testMQTTReadPIPacket(mqttPacketPubRec, t, pr, uint16(i+1))
		testQoS2SendPubRel(t, pc, uint16(i+1))
		testMQTTReadPIPacket(mqttPacketPubComp, t, pr, uint16(i+1))
	}

	// Both are delivered and unacknowledged.
	piA := testMQTTCheckPubMsgNoAck(t, c, r, "q2restart/topic", mqttPubQoS2, []byte("past-pubrec"))
	piB := testMQTTCheckPubMsgNoAck(t, c, r, "q2restart/topic", mqttPubQoS2, []byte("still-publish"))

	// Take A past PUBREC — its PUBREL is now stored and delivered, and deliberately
	// not PUBCOMP'd — while B stays in the PUBLISH phase.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, piA)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, piA)

	// UNSUBSCRIBE now: only B is still started, so the drain claims exactly it.
	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: "q2restart/topic"}})
	testMQTTFlush(t, c, nil, r)

	sess := testMQTTResidentSession(t, s, "q2restart")
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected exactly the PUBLISH-phase delivery to be claimed, got %d claims", n)
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()

	s = testDrainQoS2Restart(t, s, o)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "q2restart", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Both obligations are honored, and the order between the two paths is not fixed —
	// which applies to B's OWN PUBREL as much as to the two exchanges. Once B is PUBREC'd
	// there are THREE packets outstanding across two exchanges and nothing orders them, so
	// every one of them is dispatched through this loop.
	//
	// An inline ordered read used to sit in the PUBLISH branch below, waiting for B's
	// PUBREL immediately after PUBREC'ing it. That reintroduced exactly the assumption
	// this loop exists to avoid, and a paired soak caught it under load: "Expected PI 2
	// got: 1" — A's replayed PUBREL arriving while B's was being waited for. The broker
	// promises no order between two stored PUBRELs, so the test may not require one.
	var gotPubRel, gotPublish, gotPublishPubRel bool
	pendingB := uint16(0)
	for reads := 0; reads < 8 && !(gotPubRel && gotPublish && gotPublishPubRel); reads++ {
		b, pl := testMQTTReadPacket(t, r2)
		switch b & mqttPacketMask {
		case mqttPacketPubRel:
			pi, err := r2.readUint16("packet identifier")
			if err != nil {
				t.Fatalf("reading the PUBREL identifier: %v", err)
			}
			switch {
			case pi == piA:
				gotPubRel = true
			case pendingB != 0 && pi == pendingB:
				gotPublishPubRel = true
			default:
				t.Fatalf("PUBREL for identifier %v, want the replayed %v or the PUBREC'd %v",
					pi, piA, pendingB)
			}
			// Idempotent: a redelivered PUBREL is answered again rather than failing.
			testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
		case mqttPacketPub:
			flags, pi, topic, payload := testMQTTGetPubMsgExEx(t, nil, r2, b, pl, "q2restart/topic", nil)
			if string(payload) != "still-publish" {
				t.Fatalf("only the PUBLISH-phase claim may be resent, got %q", payload)
			}
			if flags&mqttPubFlagDup == 0 {
				t.Errorf("a delivery transmitted before the restart must carry DUP, flags=%x", flags)
			}
			if pi != piB {
				t.Fatalf("the resent PUBLISH-phase claim used identifier %v, want the original %v: the in-flight register did not restore it",
					pi, piB)
			}
			_ = topic
			testMQTTSendPIPacket(mqttPacketPubRec, t, c2, pi)
			pendingB = pi
			gotPublish = true
		default:
			t.Fatalf("unexpected packet type %x", b&mqttPacketMask)
		}
	}
	if !gotPubRel {
		t.Fatal("the stored PUBREL was never replayed: the client cannot complete that exchange")
	}
	if !gotPublish {
		t.Fatal("the drained PUBLISH-phase claim was never redelivered")
	}
	if !gotPublishPubRel {
		t.Fatal("the resent PUBLISH-phase claim never reached its PUBREL, so that exchange did not complete")
	}

	sess2 := testMQTTResidentSession(t, s, "q2restart")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
	testMQTTExpectNothing(t, r2)
}

// TestMQTTDrainPubRelRecreatedFromIntent is plan test 17(d): the intent is
// persisted BEFORE the PUBREL is stored, so a store that never lands leaves a
// positive record of an exchange that is past PUBREC.
//
// Recovery must act on that fact rather than infer from the record's absence that
// nothing is owed: it recreates the PUBREL under the ORIGINAL packet identifier,
// so the client completes the exchange it is actually holding open. Reading the
// absence the other way would either strand the client forever or resend a payload
// whose exchange has moved on.
func TestMQTTDrainPubRelRecreatedFromIntent(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "intentonly", "intentonly/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "intentonly/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	// Fail only the PUBREL store. The intent is written first, so it lands; the
	// record does not.
	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			return errors.New("test: PUBREL store refused")
		}
		return nil
	})
	// The store error closes the connection, as any storage failure does; the
	// session stays resident, so the intent is observed there.
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	// Synchronise on the CONNECTION CLOSE, not on the in-memory intent flag: that
	// flag is set inside the record transaction, so observing it can race the save
	// and a restart would then lose the very intent this test needs. The store
	// failure closes the connection, and that happens only after the save.
	testDrainWaitConnClosed(t, conn.r)
	restore()
	if !testDrainIntentRecorded(sess) {
		t.Fatal("the intent must have been persisted before the failing store")
	}

	conn.c.Close()
	s = testDrainQoS2Restart(t, s, o)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "intentonly", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The PUBREL is recreated under the ORIGINAL identifier, not a fresh exchange.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)

	// The payload is never resent: its exchange is past PUBREC. Wait past AckWait,
	// since a resend would only appear then.
	testMQTTExpectNothingFor(t, r2, 3*time.Second)

	sess2 := testMQTTResidentSession(t, s, "intentonly")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// testDrainIntentRecorded reports whether any drained sequence carries a persisted
// PUBREL intent.
func testDrainIntentRecorded(sess *mqttSession) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, d := range sess.drainByDur {
		for _, st := range d.seqs {
			if st.intent {
				return true
			}
		}
	}
	return false
}

// TestMQTTDrainBarrierFinalizesAStoredPubRel is plan test 17(a): the PUBREL record
// exists but its delivery callback never ran, so nothing finalized the claim
// before the broker went away — the crash point between the store and its
// callback.
//
// On resume the barrier must reconstruct that exchange's phase from intent PLUS
// record, BEFORE any source interest exists. Without it, creating the delivery
// subscription redelivers the pending PUBLISH immediately and resends the payload
// of an exchange the client has already PUBREC'd: a duplicate exactly-once
// delivery. The claim is finalized here rather than waiting on the asynchronous
// callback, which is why the barrier — not the callback — is what makes the
// restart safe.
func TestMQTTDrainBarrierFinalizesAStoredPubRel(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "ambig", "ambig/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "ambig/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	// Suppress the store, so the intent lands and the claim survives — the callback
	// never runs and nothing is finalized.
	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			return errors.New("test: PUBREL store suppressed")
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	// Synchronise on the CONNECTION CLOSE, not on the in-memory intent flag: that
	// flag is set inside the record transaction, so observing it can race the save
	// and a restart would then lose the very intent this test needs. The store
	// failure closes the connection, and that happens only after the save.
	testDrainWaitConnClosed(t, conn.r)
	restore()
	if !testDrainIntentRecorded(sess) {
		t.Fatal("the intent must have been persisted before the failing store")
	}

	// Now put the record where a landed store would have left it, so the resume
	// faces intent AND record with the claim still held: the state a crash between
	// the store and its callback produces.
	_ = testDrainStorePubRelRecord(t, sess)

	conn.c.Close()
	s = testDrainQoS2Restart(t, s, o)

	// Stall PUBREL delivery for longer than an AckWait, so the SOURCE redelivery
	// gets its chance first. Without this the callback always wins the race and the
	// barrier looks redundant — which is exactly the failure mode the barrier
	// exists for, since that callback is asynchronous and nothing orders it.
	restoreDelay := setMQTTTestPubRelDeliveryDelay(func() { time.Sleep(3 * time.Second) })
	defer restoreDelay()

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "ambig", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// While the PUBREL is stalled, the source interest is live and its AckWait
	// elapses: nothing may be resent, because the barrier already removed the claim.
	testMQTTExpectNothingFor(t, r2, 2*time.Second)

	// Then the PUBREL arrives and completes the original exchange.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)
	testMQTTExpectNothingFor(t, r2, 2*time.Second)

	sess2 := testMQTTResidentSession(t, s, "ambig")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainBarrierFailsClosed is plan test 17(g): an unreadable scan is never
// evidence of absence.
//
// If the walk over the session's PUBREL records cannot be completed, the broker
// cannot know which claims are past PUBREC — so it must install NO source interest
// for those drains and keep every claim, rather than redelivering payloads whose
// exchanges may already have moved on. Denying the whole CONNECT would turn a
// JetStream hiccup into an outage, so interest is withheld instead and the next
// resume retries.
func TestMQTTDrainBarrierFailsClosed(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "failclosed", "failclosed/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "failclosed/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			return errors.New("test: PUBREL store suppressed")
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	// Synchronise on the CONNECTION CLOSE, not on the in-memory intent flag: that
	// flag is set inside the record transaction, so observing it can race the save
	// and a restart would then lose the very intent this test needs. The store
	// failure closes the connection, and that happens only after the save.
	testDrainWaitConnClosed(t, conn.r)
	restore()
	if !testDrainIntentRecorded(sess) {
		t.Fatal("the intent must have been persisted before the failing store")
	}
	conn.c.Close()

	// THE STORED-RECORD CASE, made DETERMINISTIC. The intent enrolled L1's
	// materialization, which rebuilds the owed PUBREL record from the slot once the store
	// works again. Whether it lands before or after the restart used to be a race this test
	// never named; it is now REQUIRED to have landed, and the no-record twin below covers
	// the other side (TestMQTTDrainBarrierFailsClosedWithNoStoredRecord).
	testDrainWaitOwedPubRelRecord(t, sess, pi)

	s = testDrainQoS2Restart(t, s, o)

	// Make the barrier's scan unreadable for this connection only.
	var scanned atomic.Bool
	restoreScan := setMQTTTestLoadNextMsgHook(func(stream, subject string, _ uint64) error {
		if stream == mqttOutStreamName && strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			scanned.Store(true)
			return errors.New("test: PUBREL scan unavailable")
		}
		return nil
	})

	// Deferred as well as lifted explicitly below, so an early failure cannot leave
	// the scan broken for every test that follows in this package.
	defer restoreScan()
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "failclosed", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The resume work runs asynchronously with respect to the CONNACK, so wait for
	// the scan rather than racing it.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if !scanned.Load() {
			return errors.New("the resume has not attempted the PUBREL scan yet")
		}
		return nil
	})

	// WHAT THE BARRIER OWNS, and only that (C11). The barrier withholds SOURCE interest, so
	// no PUBLISH may be resent for an exchange whose PUBREC state it cannot establish;
	// incomplete identifier recovery withholds NEW allocation. Neither owns the PUBREL
	// path: a stored record that matches the durable slot, passes admission's contextual
	// checks and acquires an action is an independently PROVEN continuation of an exchange
	// the client is holding open. So the PUBREL is not merely permitted here — it is
	// REQUIRED, because a broker that gated admission on the scan would pass a mere
	// "permitted" and strand the client. The old blanket silence assertion forbade it, and
	// was over-reaching into a mechanism this change introduced. What the observation shows is
	// no PUBLISH within its window, not "never".
	//
	// PUBCOMP is WITHHELD on this connection. The claim nonetheless goes to ZERO, and that is
	// I10 rather than a barrier failure: the drained-PUBREL path removes and persists the source
	// claim and acknowledges the source BEFORE it forwards, which is what stops a crash after
	// PUBCOMP from re-sending the payload. A claim still standing here would mean the PUBREL
	// was forwarded without settling its source — a real defect. The source subscription is
	// deliberately NOT asserted in this case; see testDrainRequireNoSourceInterest.
	testDrainObservePubRelOnly(t, r2, pi, 6*time.Second, true)

	sess2 := testMQTTResidentSession(t, s, "failclosed")
	if n := testDrainClaims(sess2); n != 0 {
		t.Fatalf("the forwarded PUBREL must have settled its source claim first (I10), got %d "+
			"still claimed", n)
	}
	testDrainRequireAllocationRefused(t, sess2)
	restoreScan()

	// The next resume, with the scan readable again, completes the exchange.
	testMQTTDisconnect(t, c2, nil)
	c2.Close()
	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: "failclosed", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r3, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c3, pi)
	// A PUBREL already travelling when the PUBCOMP is processed is the SAME exchange, and
	// sending PUBCOMP does not retract bytes already on the wire (C16). The repeat is
	// ANSWERED, as a client must; anything else fails.
	testDrainFlushToleratingPubRelRepeat(t, c3, r3, pi)
	// AND THE DURABLE HALF, so tolerance cannot hide a stuck exchange: the record goes, and
	// the identifier it reserved is free. An unreadable scan is an ERROR here, never an absence.
	sessDone := testMQTTResidentSession(t, s, "failclosed")
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		sessDone.mu.Lock()
		subject := sessDone.pubRelSubject
		sessDone.mu.Unlock()
		recs, _, ok := sessDone.scanPubRelRecords(subject)
		if !ok {
			return errors.New("the PUBREL scan did not complete: an unreadable scan is not an absence")
		}
		for _, rec := range recs {
			if rec.shapeErr == nil && rec.pi == pi {
				return fmt.Errorf("the completed exchange's PUBREL record on %q is still stored", subject)
			}
		}
		sessDone.mu.Lock()
		defer sessDone.mu.Unlock()
		if _, held := sessDone.pendingPubRel[pi]; held {
			return fmt.Errorf("packet identifier %d is still held by a PUBREL entry", pi)
		}
		if sessDone.sendReservedLocked(pi) {
			return fmt.Errorf("packet identifier %d is still reserved by a send hold", pi)
		}
		return nil
	})
}

// testDrainObservePubRelOnly reads r for the WHOLE window and fails on any PUBLISH inside it —
// the barrier's own guarantee, as far as one bounded observation can show it — while treating
// a PUBREL under pi as the authorized continuation it is. requirePubRel decides whether that
// PUBREL must arrive (a stored record exists and admission proves it) or must not (no record
// exists, so nothing can authorize one).
//
// NO EARLY EXIT once the PUBREL is seen: a PUBLISH arriving after the continuation is exactly as
// much a barrier violation as one arriving before it, and a shortcut would stop looking at the
// moment the interesting schedule begins. PUBRELs may repeat: PUBCOMP is withheld, so the
// record's own AckWait re-offers it, and a repeat under the same identifier is the same exchange.
func testDrainObservePubRelOnly(t *testing.T, r *mqttReader, pi uint16, window time.Duration, requirePubRel bool) {
	t.Helper()
	deadline := time.Now().Add(window)
	seen := false
	for {
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		b, _, ok := testMQTTReadPacketReady(r, left)
		if !ok {
			break
		}
		switch b & mqttPacketMask {
		case mqttPacketPub:
			t.Fatalf("a PUBLISH arrived within the %v observation window while the barrier's "+
				"PUBREL scan was unreadable: source interest was installed for an exchange "+
				"whose PUBREC state is unknown", window)
		case mqttPacketPubRel:
			got, err := r.readUint16("packet identifier")
			if err != nil || got != pi {
				t.Fatalf("a PUBREL arrived under packet identifier %d (err=%v), want only the "+
					"held %d", got, err, pi)
			}
			if !requirePubRel {
				t.Fatalf("a PUBREL arrived under %d although no stored record exists: nothing "+
					"can have authorized it", got)
			}
			seen = true
		default:
			t.Fatalf("unexpected packet %#x while the barrier's scan was unreadable", b)
		}
	}
	if requirePubRel && !seen {
		t.Fatalf("no PUBREL arrived under %d: the stored record matches the durable slot and "+
			"admission proves it independently of the barrier, so the client was stranded", pi)
	}
}

// testDrainRequireAllocationRefused asserts that incomplete identifier recovery refuses NEW
// allocation, asking the allocator itself: the flag alone would not show the refusal is
// enforced where identifiers are minted. With the space incomplete bumpPI returns 0 and
// mutates nothing.
func testDrainRequireAllocationRefused(t *testing.T, sess *mqttSession) {
	t.Helper()
	sess.mu.Lock()
	incomplete := sess.piSpaceIncomplete
	minted := sess.bumpPI()
	sess.mu.Unlock()
	if !incomplete || minted != 0 {
		t.Fatalf("new allocation must be refused while identifier recovery is incomplete: "+
			"piSpaceIncomplete=%v, bumpPI minted %d", incomplete, minted)
	}
}

// testDrainRequireNoSourceInterest asserts that no restored drain has a delivery subscription.
//
// SOURCE INTEREST IS THE DELIVERY SUBSCRIPTION, read where production reads it
// (c.subs[DeliverSubject]). The reverse index is NOT evidence: restoreDrainLocked puts every
// restored drain into drainByDur whether or not a subscription follows, and a first version
// that read the index failed a correct broker.
//
// Asserted ONLY where the barrier alone decides. Where a stored PUBREL exists, its delivery
// settles the claim and can do so BEFORE the barrier classifies — the PUBREL path is readied
// first — and the barrier then correctly installs interest for a drain with nothing left
// unresolved. That ordering is a race, so the subscription is unassertable in either direction
// there (C12).
func testDrainRequireNoSourceInterest(t *testing.T, s *Server, clientID string, sess *mqttSession) {
	t.Helper()
	sess.mu.Lock()
	var delivers []string
	for _, d := range sess.drains {
		if d.cc != nil && d.cc.DeliverSubject != _EMPTY_ {
			delivers = append(delivers, d.cc.DeliverSubject)
		}
	}
	sess.mu.Unlock()
	if len(delivers) == 0 {
		t.Fatal("no restored drain to check: the fixture never reached the barrier's population")
	}
	c := testMQTTGetClient(t, s, clientID)
	c.mu.Lock()
	var installed []string
	for _, subj := range delivers {
		if c.subs[subj] != nil {
			installed = append(installed, subj)
		}
	}
	c.mu.Unlock()
	if len(installed) != 0 {
		t.Fatalf("source interest was installed for an unresolved drain: %v", installed)
	}
}

// testDrainWaitOwedPubRelRecord waits until L1's materialization has stored the owed PUBREL
// record for pi, so a test about the stored-record case is not silently the no-record one.
func testDrainWaitOwedPubRelRecord(t *testing.T, sess *mqttSession, pi uint16) {
	t.Helper()
	sess.mu.Lock()
	subject := sess.pubRelSubject
	sess.mu.Unlock()
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		recs, _, ok := sess.scanPubRelRecords(subject)
		if !ok {
			return errors.New("the PUBREL scan did not complete")
		}
		for _, rec := range recs {
			if rec.shapeErr == nil && rec.pi == pi {
				return nil
			}
		}
		return errors.New("L1 has not materialized the owed PUBREL record yet")
	})
}

// TestMQTTDrainBarrierFailsClosedWithNoStoredRecord is the other half of C11's pairing: the
// same unreadable barrier, but NO PUBREL record exists anywhere, so nothing can authorize a
// transmission and the wire must stay silent. The store stays refused through the first
// server's shutdown, so L1's materialization cannot land one before the restart; after the
// restart its own scan is unreadable too, so it cannot land one then either.
//
// Together with the stored-record case this separates the three mechanisms: a PUBLISH here
// is source interest leaking, a PUBREL here is something transmitting without a record, and
// the barrier assertions pin claims, source interest and allocation independently.
func TestMQTTDrainBarrierFailsClosedWithNoStoredRecord(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "failclosednorec", "failclosednorec/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "failclosednorec/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	restoreStore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			return errors.New("test: PUBREL store suppressed")
		}
		return nil
	})
	t.Cleanup(restoreStore)
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	testDrainWaitConnClosed(t, conn.r)
	if !testDrainIntentRecorded(sess) {
		t.Fatal("the intent must have been persisted before the failing store")
	}
	conn.c.Close()
	sess.mu.Lock()
	pubRelSubject := sess.pubRelSubject
	sess.mu.Unlock()

	// Still refused ACROSS the shutdown, so L1's materialization — whose timer the shutdown
	// joins — cannot land a record before the restart.
	s = testDrainQoS2Restart(t, s, o)
	restoreStore()
	asm := testMQTTGetGlobalASM(t, s)
	if smsg, err := asm.jsa.loadLastMsgFor(mqttOutStreamName, pubRelSubject); err == nil && smsg != nil {
		t.Fatalf("the fixture must have NO stored PUBREL record, found one at %d: this would be "+
			"the stored-record case under another name", smsg.Sequence)
	}

	var scanned atomic.Bool
	restoreScan := setMQTTTestLoadNextMsgHook(func(stream, subject string, _ uint64) error {
		if stream == mqttOutStreamName && strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			scanned.Store(true)
			return errors.New("test: PUBREL scan unavailable")
		}
		return nil
	})
	defer restoreScan()
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "failclosednorec", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if !scanned.Load() {
			return errors.New("the resume has not attempted the PUBREL scan yet")
		}
		return nil
	})

	// SILENCE, for longer than L1's reconcile delay, so a materialization that stored a
	// record despite its own unreadable scan would have transmitted inside the window.
	testDrainObservePubRelOnly(t, r2, pi, mqttActionReconcileDelay+time.Second, false)
	sess2 := testMQTTResidentSession(t, s, "failclosednorec")
	// Here the barrier ALONE decides, so all three of its guarantees are asserted.
	if n := testDrainClaims(sess2); n != 1 {
		t.Fatalf("an unresolved barrier must keep every claim, got %d", n)
	}
	testDrainRequireNoSourceInterest(t, s, "failclosednorec", sess2)
	testDrainRequireAllocationRefused(t, sess2)
	restoreScan()

	// The next resume, with the scan readable, recreates the record and completes.
	testMQTTDisconnect(t, c2, nil)
	c2.Close()
	c3, r3 := testMQTTConnect(t, &mqttConnInfo{clientID: "failclosednorec", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r3, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c3, pi)
	// A PUBREL already travelling when the PUBCOMP is processed is the SAME exchange, and
	// sending PUBCOMP does not retract bytes already on the wire (C16). The repeat is
	// ANSWERED, as a client must; anything else fails.
	testDrainFlushToleratingPubRelRepeat(t, c3, r3, pi)
	// AND THE DURABLE HALF, so tolerance cannot hide a stuck exchange: the record goes, and
	// the identifier it reserved is free. An unreadable scan is an ERROR here, never an absence.
	sessDone := testMQTTResidentSession(t, s, "failclosednorec")
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		sessDone.mu.Lock()
		subject := sessDone.pubRelSubject
		sessDone.mu.Unlock()
		recs, _, ok := sessDone.scanPubRelRecords(subject)
		if !ok {
			return errors.New("the PUBREL scan did not complete: an unreadable scan is not an absence")
		}
		for _, rec := range recs {
			if rec.shapeErr == nil && rec.pi == pi {
				return fmt.Errorf("the completed exchange's PUBREL record on %q is still stored", subject)
			}
		}
		sessDone.mu.Lock()
		defer sessDone.mu.Unlock()
		if _, held := sessDone.pendingPubRel[pi]; held {
			return fmt.Errorf("packet identifier %d is still held by a PUBREL entry", pi)
		}
		if sessDone.sendReservedLocked(pi) {
			return fmt.Errorf("packet identifier %d is still reserved by a send hold", pi)
		}
		return nil
	})
}

// testDrainStorePubRelRecord writes the correlated PUBREL record for the session's
// single intent-bearing drained sequence, exactly as a landed store would have.
func testDrainStorePubRelRecord(t *testing.T, sess *mqttSession) uint64 {
	t.Helper()
	sess.mu.Lock()
	var (
		dur     string
		sseq    uint64
		pi      uint16
		attempt uint64
		owner   string
	)
	for _, d := range sess.drainByDur {
		for seq, st := range d.seqs {
			if st.intent {
				dur, sseq, pi, attempt, owner = d.cc.Durable, seq, st.pi, st.attempt, st.ownerNonce
			}
		}
	}
	idHash, subject := sess.idHash, sess.pubRelSubject
	sess.mu.Unlock()
	if dur == _EMPTY_ {
		t.Fatal("no intent-bearing drained sequence to write a PUBREL for")
	}
	// The record carries the exchange's REAL owner nonce, which the intent persisted and
	// the owner slot holds — in production all three are the register's own regNonce, so a
	// synthetic nonce that differs from the slot is contradictory and admission refuses it
	// (Decision 5). This stands in for the record a real broker would have stored.
	msgId := mqttPubRelMsgId(idHash, dur, sseq, attempt)
	natsMsg, headerLen := mqttNewCorrelatedPubRel(pi, owner, dur, sseq, msgId, _EMPTY_, _EMPTY_)
	resp, err := sess.jsa.storeMsg(subject, headerLen, natsMsg)
	if err != nil {
		t.Fatalf("storing the correlated PUBREL record: %v", err)
	}
	return resp.Sequence
}

// TestMQTTDrainPubRelRecreateEscapesDeduplication is plan test 17(d2): a store
// response is not a record.
//
// A JetStream deduplication entry OUTLIVES the message it was recorded for, so
// re-storing a PUBREL under the same Nats-Msg-Id is accepted as a duplicate while
// storing nothing at all. Trusting that response would finalize the claim and ack
// the source for a PUBREL that does not exist — the client waits forever for a
// PUBCOMP exchange nothing can advance. Presence at the returned sequence is the
// only proof, and escaping the poisoned identity needs a fresh attempt generation.
func TestMQTTDrainPubRelRecreateEscapesDeduplication(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "dedupe", "dedupe/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "dedupe/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	restore := setMQTTTestStoreMsgHook(func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, mqttPubRelSubjectPrefix) {
			return errors.New("test: PUBREL store suppressed")
		}
		return nil
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	// Synchronise on the CONNECTION CLOSE, not on the in-memory intent flag: that
	// flag is set inside the record transaction, so observing it can race the save
	// and a restart would then lose the very intent this test needs. The store
	// failure closes the connection, and that happens only after the save.
	testDrainWaitConnClosed(t, conn.r)
	restore()
	if !testDrainIntentRecorded(sess) {
		t.Fatal("the intent must have been persisted before the failing store")
	}
	conn.c.Close()

	// Store the record the intent names — which also records its deduplication
	// identity — then delete the record. The dedupe entry survives, so the identity
	// is now poisoned: a recreate under it stores nothing and reports success.
	seq := testDrainStorePubRelRecord(t, sess)
	if err := sess.jsa.deleteMsg(mqttOutStreamName, seq, true); err != nil {
		t.Fatalf("deleting the stored PUBREL: %v", err)
	}

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "dedupe", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The recreate must notice it stored nothing, bump its attempt, and try again
	// under a fresh identity — so the client still gets its PUBREL.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)

	sess2 := testMQTTResidentSession(t, s, "dedupe")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainDefiniteIntentFailureUnwedgesIdentifier is plan test 17(e): an
// intent write that DEFINITELY did not land must leave the session exactly as it
// was before the PUBREC.
//
// The in-memory state moves first — the PUBLISH tracking is dropped and a PUBREL
// placeholder installed — so a failure that is not rolled back wedges that packet
// identifier for the life of the session. A same-process reconnect reuses the very
// same session object, which is what makes the in-memory rollback load-bearing
// rather than cosmetic: nothing rereads the record to correct it.
func TestMQTTDrainDefiniteIntentFailureUnwedgesIdentifier(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "definite", "definite/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "definite/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// A definite failure: the record write is refused outright.
	restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errDrainTestSaveFailed })
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	// A storage failure terminates the connection, and that close is the signal
	// that the PUBREC really was processed. Waiting on "no intent recorded" would
	// be satisfied by the state we started in, and would lift the hook before the
	// PUBREC ever reached the broker.
	testDrainWaitConnClosed(t, conn.r)
	restore()
	conn.c.Close()

	if testDrainIntentRecorded(sess) {
		t.Fatal("an intent write that was refused must leave no intent behind")
	}

	sess.mu.Lock()
	_, wedged := sess.pendingPubRel[pi]
	claims := 0
	for _, d := range sess.drainByDur {
		claims += d.claimedCount()
	}
	sess.mu.Unlock()
	if wedged {
		t.Fatalf("identifier %v is wedged in pendingPubRel: nothing will ever fill it, so it can never be reused", pi)
	}
	if claims != 1 {
		t.Fatalf("the source claim must survive an intent that did not land, got %d claims", claims)
	}

	// The same session object is reused by the reconnect, so a wedged identifier or
	// a lost claim would show up right here.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "definite", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	flags, pi2, topic, payload := testMQTTReadPubPacket(t, r2)
	if topic != "definite/topic" || string(payload) != "payload" {
		t.Fatalf("expected the payload to be redelivered, got %q %q", topic, payload)
	}
	if flags&mqttPubFlagDup == 0 {
		t.Errorf("the redelivery must carry DUP, flags=%x", flags)
	}
	// The whole exchange completes this time.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c2, pi2)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi2)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi2)
	testMQTTFlush(t, c2, nil, r2)

	sess2 := testMQTTResidentSession(t, s, "definite")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// testDrainWaitConnClosed blocks until the broker closes the connection, which is
// how a storage failure surfaces to the client.
func testDrainWaitConnClosed(t *testing.T, r *mqttReader) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var buf [256]byte
	for time.Now().Before(deadline) {
		if err := r.reader.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		_, err := r.reader.Read(buf[:])
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			r.reader.SetReadDeadline(time.Time{}) //nolint:errcheck
			return
		}
	}
	t.Fatal("the broker never closed the connection, so the failing write was never exercised")
}

// TestMQTTDrainClaimSurvivesMessageExpiry is plan test 17's expiry case: a
// restored claim carrying a v5 Message Expiry Interval must be REDELIVERED with
// DUP, not discarded by the `!started` expiry gate.
//
// Expiry may drop a message whose onward delivery has not begun; a delivery
// already put on the wire must be completed ([MQTT-3.10.4-3], Spec5 [3.3.2.3.3]).
// After a restart the in-memory tracking is empty, so the delivery path would
// infer `started=false` and discard it — the record is what says otherwise.
//
// The interval normally drives BOTH the delivery-side check and the JetStream
// per-message TTL, so "expired for delivery" and "still in the stream" coexist
// only during the reaper's lag. The TTL is therefore suppressed here (see
// mqttStoredMsgTTL) so the invariant is tested below that boundary rather than
// against a race.
func TestMQTTDrainClaimSurvivesMessageExpiry(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	mqttTestSuppressMsgTTLV.Store(true)
	defer mqttTestSuppressMsgTTLV.Store(false)

	const topic = "expiry/topic"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "expiry"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	// A v5 publisher, so the stored copy carries a Message Expiry Interval; two
	// seconds, short enough to elapse across the restart below.
	pc, pr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "expirypub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, pr); reason != mqttReasonSuccess {
		t.Fatalf("CONNACK for the publisher: reason=%v", reason)
	}
	props := testDrainMessageExpiryProps(2)
	testMQTTPublishV5Props(t, pc, 1, 1, topic, props, []byte("payload"))
	testMQTTReadPubAck(t, pr, 1)

	pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, mqttPubQos1, []byte("payload"))
	_ = pi

	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, c, nil, r)
	sess := testMQTTResidentSession(t, s, "expiry")
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}
	testMQTTDisconnect(t, c, nil)
	c.Close()

	s = testDrainQoS2Restart(t, s, o)

	// Let the interval elapse, so the delivery-side check really does see an
	// expired message — the discard this test must NOT observe.
	time.Sleep(2500 * time.Millisecond)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "expiry", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	flags, pi2, gotTopic, payload := testMQTTReadPubPacket(t, r2)
	if gotTopic != topic || string(payload) != "payload" {
		t.Fatalf("expected the started delivery to be redelivered, got %q %q", gotTopic, payload)
	}
	if flags&mqttPubFlagDup == 0 {
		t.Errorf("a delivery transmitted before the restart must carry DUP, flags=%x", flags)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi2)

	sess2 := testMQTTResidentSession(t, s, "expiry")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// TestMQTTDrainNoResendAfterPubCompThenCrash is plan test 17(b): persist before
// forward.
//
// The client PUBCOMPs, which acks the PUBREL record away, and only then does the
// broker die. If the claim removal had been persisted AFTER forwarding the PUBREL,
// the record would still claim a sequence whose exchange is complete — and with
// the PUBREL gone there would be nothing left to correlate it to, so the resume
// would redeliver the PAYLOAD and the client would receive the message twice. The
// ordering is what makes that unreachable rather than unlikely.
func TestMQTTDrainNoResendAfterPubCompThenCrash(t *testing.T) {
	o := testMQTTDefaultOptions()
	// A LONG AckWait deliberately: with a short one the source redelivery also
	// finalizes the claim (the confirmed-phase branch of the delivery path), which
	// masks the callback and makes this test unable to tell the two orderings
	// apart. Here the callback is the only thing that can have removed the claim
	// before the crash.
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "pubcomp", "pubcomp/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "pubcomp/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}

	// The ordering is checked IN the window it exists in: the hook fires after the
	// claim removal is persisted and before the PUBREL goes out, so the assertion
	// reads the PERSISTED RECORD at the one instant where a forward-first
	// implementation would still be claiming the sequence. Waiting for the
	// in-memory claim to clear afterwards would not distinguish the two orderings —
	// both reach that state.
	var persistedClaims int
	var checked bool
	restoreHook := setMQTTTestPubRelPreForwardHook(func() {
		if checked {
			return
		}
		checked = true
		persistedClaims = testPersistedDrainClaims(t, sess)
	})
	defer restoreHook()

	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, conn.r, pi)
	if !checked {
		t.Fatal("the pre-forward hook never fired, so this test proves nothing about the ordering")
	}
	if persistedClaims != 0 {
		t.Fatalf("the session record still claimed %d source(s) when the PUBREL was forwarded: "+
			"a client PUBCOMP can now ack the PUBREL away before that removal is durable, and the resume resends the payload",
			persistedClaims)
	}
	restoreHook()

	testMQTTSendPIPacket(mqttPacketPubComp, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
	conn.c.Close()

	s = testDrainQoS2Restart(t, s, o)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "pubcomp", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Nothing at all: no payload, no PUBREL. Waited past AckWait, since a resend
	// would only appear then.
	testMQTTExpectNothingFor(t, r2, 3*time.Second)
}

// TestMQTTDrainRecreateEscapesASkippedStore is plan test 17(d2) shape (ii): the
// ORIGINAL store was skipped for lack of interest, so only a deduplication entry
// exists — no record ever did.
//
// $MQTT_out is interest-retained: with no consumer, a store succeeds, keeps
// nothing, and still records its Nats-Msg-Id. That is the case a successful PubAck
// most obviously misleads, because nothing was deleted and nothing failed. The
// recreate must therefore verify presence and escape the poisoned identity, or the
// source is acked for a PUBREL that never existed.
func TestMQTTDrainRecreateEscapesASkippedStore(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "skipped", "skipped/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "skipped/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %v", n)
	}

	// Remove the PUBREL consumer, so the interest-retained stream keeps nothing the
	// PUBREC is about to store.
	sess.mu.Lock()
	pubRelDur := ""
	if sess.pubRelConsumer != nil {
		pubRelDur = sess.pubRelConsumer.Durable
	}
	sess.mu.Unlock()
	if pubRelDur == "" {
		t.Fatal("a QoS 2 subscription should have created the PUBREL consumer")
	}
	if _, err := sess.jsa.deleteConsumer(mqttOutStreamName, pubRelDur, true); err != nil {
		t.Fatalf("deleting the PUBREL consumer: %v", err)
	}
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if ci, err := sess.jsa.lookupConsumer(mqttOutStreamName, pubRelDur); err == nil && ci != nil {
			return errors.New("the PUBREL consumer is still present")
		}
		return nil
	})

	// The store SUCCEEDS and keeps nothing. No error, no rollback, no flag.
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if !testDrainIntentRecorded(sess) {
			return errors.New("the intent has not been persisted yet")
		}
		return nil
	})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("a skipped store must not finalize the claim, got %d claims", n)
	}
	conn.c.Close()

	// Resume: the PUBREL path is verified and recreated first, then the barrier has
	// to notice its recreate stored nothing and retry under a fresh identity.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "skipped", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)

	sess2 := testMQTTResidentSession(t, s, "skipped")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// testDrainMessageExpiryProps builds a v5 PUBLISH properties body carrying only a
// Message Expiry Interval.
func testDrainMessageExpiryProps(secs uint32) []byte {
	body := newMQTTWriter(0)
	body.WriteByte(mqttPropMessageExpiry)
	body.WriteUint32(secs)
	return body.Bytes()
}

// TestMQTTDrainAmbiguousIntentSaveAdoptsItsOwnRecord is plan test 17(e2): an
// intent write that LANDED with its response lost.
//
// sess.seq is now stale, and a same-process reconnect reuses this very session
// object — so its CONNECT accept save would CAS against a sequence the record has
// already moved past and the reconnect would be rejected, before any recovery
// could run. The readback has to classify the write: same base sequence means it
// did not land, a newer record whose bytes match ours means it did (adopt it), and
// anything else belongs to somebody else. This exercises the middle case, which is
// the one that must not be mistaken for either neighbour.
func TestMQTTDrainAmbiguousIntentSaveAdoptsItsOwnRecord(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "ambigsave", "ambigsave/topic", 2)

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "ambigsave/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}

	sess.mu.Lock()
	seqBefore := sess.seq
	sess.mu.Unlock()

	// The record write LANDS; only its response is replaced by a non-CAS error, so
	// this broker cannot tell whether it did.
	var once atomicBoolOnce
	restore := setMQTTTestSessStoreResultHook(func(_ *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err == nil && once.set() {
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	testDrainWaitConnClosed(t, conn.r)
	restore()
	conn.c.Close()

	sess.mu.Lock()
	uncertain := sess.seqUncertain
	sess.mu.Unlock()
	if !uncertain {
		t.Fatal("an ambiguous record write must leave the session unresolved, or nothing drives the readback")
	}

	// The FIRST same-broker reconnect must succeed: the readback recognises our own
	// bytes at a newer sequence and adopts it. A stale CAS would reject this.
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "ambigsave", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	sess.mu.Lock()
	stillUncertain, seqAfter := sess.seqUncertain, sess.seq
	sess.mu.Unlock()
	if stillUncertain {
		t.Fatal("the reconnect must resolve the uncertain write before it accepts the session")
	}
	if seqAfter <= seqBefore {
		t.Fatalf("the readback must adopt the record's newer sequence, got %v (was %v)", seqAfter, seqBefore)
	}

	// And the intent that landed is honored: the exchange completes under its
	// original identifier, with the payload never resent.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r2, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c2, pi)
	testMQTTFlush(t, c2, nil, r2)

	sess2 := testMQTTResidentSession(t, s, "ambigsave")
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		if n := testDrainCount(sess2); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		return nil
	})
}

// atomicBoolOnce fires exactly once, so a hook can corrupt a single write.
type atomicBoolOnce struct{ done atomic.Bool }

func (o *atomicBoolOnce) set() bool { return o.done.CompareAndSwap(false, true) }

// testPersistedDrainClaims counts the claims the STORED session record carries —
// the only view that can prove an ordering against a durable write.
func testPersistedDrainClaims(t *testing.T, sess *mqttSession) int {
	t.Helper()
	subject := mqttSessStreamSubjectPrefix + sess.domainTk + sess.idHash
	smsg, err := sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil || smsg == nil {
		t.Fatalf("loading the persisted session record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decoding the persisted session record: %v", err)
	}
	n := 0
	for _, pd := range ps.Drains {
		if pd != nil {
			n += len(pd.Started)
		}
	}
	return n
}

// TestMQTTDrainAmbiguousCompletionThroughTheRealCallbacks drives the REAL delivery
// and PUBREL callbacks, not the helpers they call.
//
// A unit test that calls finalizeDrainDelivery and confirmDrainPubRel directly
// proves only that those helpers behave. Each callback then makes its own decision
// afterwards, and either can act on in-memory "not claimed": the delivery path by
// ACKING a redelivery, the PUBREL path by FORWARDING. Both are irreversible —
// acking destroys a message the stored record may still claim (I1), and forwarding
// lets the client PUBCOMP an exchange the record still owes (I10).
//
// The exchange is taken past PUBREC normally, so the completion write is the one
// the PUBREL callback performs. Its record write is then suppressed AND its readback
// made unavailable, holding the removal unconfirmed while the assertions run.
func TestMQTTDrainAmbiguousCompletionThroughTheRealCallbacks(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "ambreal", "ambreal/topic", 2)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "ambreal/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}

	// Target the PUBREL callback's COMPLETION write, and only it. The intent write
	// must succeed — an ambiguous intent closes the connection, which is a different
	// path — and the two are distinguishable by what they are about to store: the
	// intent write still carries the claim, the completion write no longer does.
	// The readback is disabled from the same moment, so the removal stays
	// unconfirmed while the assertions run.
	var suppressing atomic.Bool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressing.Load() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	restoreStore := setMQTTTestSessStoreResultHook(func(sp *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err != nil {
			return resp, err
		}
		// SCOPED TO THIS SESSION. `testDrainClaims(sp) == 0` is true of every session
		// that has no drains at all, so an unrelated one — this fixture's own publisher,
		// for instance — matched it and had its record write suppressed. A hook that
		// fires for somebody else's session is a leak with extra steps.
		if sp == sess && testDrainClaims(sp) == 0 {
			suppressing.Store(true)
			return nil, errors.New("test: session record response lost")
		}
		return resp, err
	})
	// BOTH REGISTERED IMMEDIATELY: the assertions below run inside both injected
	// windows, and a t.Fatal in any of them would leave a session-readback failure and a
	// session-store failure armed for every test that follows. That is the second stage
	// of the contamination chain, and it is the one that produced "loading session
	// record: test: readback unavailable" for the rest of the run.
	t.Cleanup(restoreRead)
	t.Cleanup(restoreStore)

	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)
	checkFor(t, 10*time.Second, 20*time.Millisecond, func() error {
		if !suppressing.Load() {
			return errors.New("the completion write has not been attempted yet")
		}
		return nil
	})

	// The PUBREL callback now runs: its completion write is ambiguous, so it must
	// forward NOTHING. A forwarded PUBREL here would let the client PUBCOMP an
	// exchange the stored record still claims.
	testMQTTExpectNothingFor(t, conn.r, 2*time.Second)

	sess.mu.Lock()
	var unconfirmed, claimed bool
	for _, d := range sess.drainByDur {
		for _, st := range d.seqs {
			unconfirmed = unconfirmed || st.unclaimUnconfirmed
			claimed = claimed || st.claimed
		}
	}
	sess.mu.Unlock()
	if !unconfirmed && !claimed {
		t.Fatal("the source removal is neither claimed nor marked unconfirmed, so the callbacks are free to ack and forward it " +
			"even though no record is known to carry the removal")
	}

	// Let it resolve: the write never landed, so the claim comes back and the
	// exchange completes through the ordinary path. Nothing was lost.
	restoreStore()
	suppressing.Store(false)
	restoreRead()
	// Deliberately NOT resolved by the test: production has to do it, or a client
	// that stays connected leaves the drain stuck forever. The redelivery path is
	// the retry clock.
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if sess.isSeqUncertain() {
			return errors.New("still unresolved: nothing in production is retrying the readback")
		}
		return nil
	})

	testMQTTReadPIPacket(mqttPacketPubRel, t, conn.r, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, conn.c, pi)
	testMQTTFlush(t, conn.c, nil, conn.r)
}

// TestMQTTDrainUnconfirmedRemovalIsNotAckedByTheDeliveryCallback covers the QoS 1
// delivery callback, which is the other place an unconfirmed removal can be acted
// on irreversibly.
//
// The callback's own "fresh or already finalized" branch acks the message. For a
// sequence whose removal is UNCONFIRMED that ack would destroy a message the stored
// record may still claim (I1) — and unlike a forward, there is nothing left
// afterwards to recover it from. The gate comes off only when the removal is PROVEN
// durable, which is what the resolver's re-attempt establishes.
func TestMQTTDrainUnconfirmedRemovalIsNotAckedByTheDeliveryCallback(t *testing.T) {
	o := testMQTTDefaultOptions()
	// Short, so the redelivery that exercises the branch arrives inside the test.
	o.MQTT.AckWait = 500 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	conn, sess, pi := testDrainSetup(t, s, o, "ackgate", "ackgate/topic", 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: "ackgate/topic"}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainClaims(sess); n != 1 {
		t.Fatalf("expected the started delivery to be claimed, got %d", n)
	}

	// The PUBACK's completion write lands nowhere and stays unresolved, so the
	// removal is held unconfirmed while redeliveries keep arriving.
	var suppressing atomic.Bool
	restoreRead := setMQTTTestLoadLastMsgHook(func(stream, _ string) error {
		if stream == mqttSessStreamName && suppressing.Load() {
			return errors.New("test: readback unavailable")
		}
		return nil
	})
	defer restoreRead()
	defer func() {
		suppressing.Store(false)
		mqttTestSessStoreSuppressV.Store(false)
	}()
	suppressing.Store(true)
	mqttTestSessStoreSuppressV.Store(true)
	testMQTTSendPIPacket(mqttPacketPubAck, t, conn.c, pi)

	// Give AckWait several windows: each redelivery reaches the drain branch with
	// claimed == false and unclaimUnconfirmed == true, and must NOT be acked.
	time.Sleep(2 * time.Second)
	mqttTestSessStoreSuppressV.Store(false)

	ci, err := sess.jsa.lookupConsumer(mqttStreamName, testDrainOnlyDurable(t, sess))
	if err != nil || ci == nil {
		t.Fatalf("lookupConsumer: %v", err)
	}
	if ci.NumAckPending == 0 && ci.AckFloor.Stream >= 1 {
		t.Fatal("the source was acked while its removal was unconfirmed: the stored record may still claim it, " +
			"and there is nothing left to recover the message from (I1)")
	}

	// Settling the write is what discharges the ack: the resolver re-attempts the
	// completion's own bytes, so the removal becomes durable and the claim is
	// released for real — rather than the source being acked on an unproven one.
	suppressing.Store(false)
	restoreRead()
	checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
		if sess.isSeqUncertain() {
			return errors.New("still unresolved: nothing in production is retrying the resolution")
		}
		return nil
	})
	if n := testDrainClaims(sess); n != 0 {
		t.Fatalf("a settled removal releases the claim, got %d still claimed", n)
	}
}

// testDrainOnlyDurable returns the single draining durable's name.
func testDrainOnlyDurable(t *testing.T, sess *mqttSession) string {
	t.Helper()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if len(sess.drainByDur) != 1 {
		t.Fatalf("expected exactly one draining durable, got %d", len(sess.drainByDur))
	}
	for dur := range sess.drainByDur {
		return dur
	}
	return _EMPTY_
}

// TestMQTTLivePubRecAdvancesOwnerSlotToPubRelReady discriminates the LIVE PUBREC handoff
// (phaseSlotToPubRelReady), which converges to the same end state as the direct arrow and so
// evaded the prior census test. PUBREL delivery is STALLED, so the direct arrow cannot fire;
// only the synchronous PUBREC handoff can advance the owner slot. It must reach pubrel_ready
// with the canonical tuple. A broken readback (e.g. a QoS mismatch in the shape check) leaves
// the slot in publish and fails this test.
func TestMQTTLivePubRecAdvancesOwnerSlotToPubRelReady(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()

	conn, sess, pi := testDrainSetup(t, s, o, "live-rephase", "live/topic", 2)
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)

	// Stall PUBREL delivery so the direct `publish -> pubrel_sending` arrow cannot fire; the
	// only path left to advance the slot is the synchronous live handoff in the PUBREC
	// handler.
	restoreDelay := setMQTTTestPubRelDeliveryDelay(func() { time.Sleep(5 * time.Second) })
	defer restoreDelay()

	testMQTTSendPIPacket(mqttPacketPubRec, t, conn.c, pi)

	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		cur, _, err := sess.asm.loadInflightRecord(subject)
		if err != nil {
			return err
		}
		if cur == nil {
			return fmt.Errorf("the owner slot is gone")
		}
		if cur.State != mqttInflightPubRelReady {
			return fmt.Errorf("the live PUBREC handoff did not advance the slot to pubrel_ready: %+v", cur)
		}
		if cur.PubRelStreamSeq == 0 || cur.PubRelDurable == _EMPTY_ {
			return fmt.Errorf("the slot reached pubrel_ready without a canonical tuple: %+v", cur)
		}
		return nil
	})
	// The discriminator is the durable advance above. The exchange is left for teardown to
	// clean up: completing it here would race the client read against the still-stalled
	// PUBREL delivery, which is not what this test is about.
}

// testDrainFlushToleratingPubRelRepeat is testMQTTFlush for a fixture whose exchange can have a
// PUBREL in flight at the moment its PUBCOMP is processed: the record's own AckWait re-offers it
// while PUBCOMP is withheld earlier in the test, and the last re-offer can already be on the
// wire. It is deliberately NOT a relaxation of testMQTTFlush, which every other test still uses:
// only a well-formed PUBREL under THIS identifier is tolerated, and it is ANSWERED rather than
// discarded (C16, [MQTT-3.6.4]). One deadline covers the whole wait.
func testDrainFlushToleratingPubRelRepeat(t *testing.T, c net.Conn, r *mqttReader, pi uint16) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPing)
	w.WriteByte(0)
	if _, err := c.Write(w.Bytes()); err != nil {
		t.Fatalf("sending PINGREQ: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatal("no PINGRESP within the window after the exchange was completed")
		}
		b, l, ok := testMQTTReadPacketReady(r, left)
		if !ok {
			t.Fatal("the connection produced no further packet while a PINGRESP was owed")
		}
		switch b & mqttPacketMask {
		case mqttPacketPingResp:
			if l != 0 {
				t.Fatalf("expected PINGRESP length 0, got %v", l)
			}
			return
		case mqttPacketPubRel:
			// WELL-FORMED, not merely typed: PUBREL carries flags 0b0010 ([MQTT-3.6.1-1]) and a
			// remaining length of exactly its two identifier bytes. A malformed packet is a defect
			// this reader must not absorb as "the same exchange's repeat".
			if b&0x0f != 0x02 {
				t.Fatalf("a PUBREL arrived with flags %#x, want 0x2 per [MQTT-3.6.1-1]", b&0x0f)
			}
			if l != 2 {
				t.Fatalf("a PUBREL arrived with remaining length %d, want 2", l)
			}
			got, err := r.readUint16("packet identifier")
			if err != nil || got != pi {
				t.Fatalf("a PUBREL arrived under packet identifier %d (err=%v), want only the "+
					"completed exchange's %d", got, err, pi)
			}
			testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
		default:
			t.Fatalf("unexpected packet %x while a PINGRESP was owed", b&mqttPacketMask)
		}
	}
}
