// Copyright 2025 The mqtt-nats-bridge Authors
//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// End-to-end tests for the late-uncertainty recovery of clear()'s reachable callers, over
// real client connections: the ordinary clean close, CONNECT's clean start, and the v5
// expiry-override close whose protocol outcome the recovery is what fixes.
//
// The race is driven POSITIVELY through clear()'s pre-fence seam — the window an
// off-readLoop register store settling would land in — so nothing here sleeps or races the
// scheduler.

// testResidentSession returns the account session manager and the resident session object
// for a client ID, which the seams below key on so one test's ambiguity cannot arm on
// another session.
//
// The manager is captured while the client is still CONNECTED and then threaded through the
// assertions: testMQTTGetAccountSessionManager finds it via a live client, and every
// interesting state here is observed after that client has gone.
func testResidentSession(t *testing.T, s *Server, clientID string) (*mqttAccountSessionManager, *mqttSession) {
	t.Helper()
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	sess := asm.sessions[clientID]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("the session for %q must be resident", clientID)
	}
	return asm, sess
}

// testArmAmbiguityAtClearPreFence arms ONE ambiguous write on sess at clear()'s pre-fence
// window: after the entry check the discard already passed, and before the fence that would
// serialize it. Only a check made under the fence can still see it.
func testArmAmbiguityAtClearPreFence(t *testing.T, sess *mqttSession) func() bool {
	t.Helper()
	armed := false
	restore := setMQTTTestClearWindowHook(func(cur *mqttSession, window string) {
		if cur != sess || window != mqttClearWindowPreFence || armed {
			return
		}
		armed = true
		cur.mu.Lock()
		cur.markWriteUncertainLocked(cur.seq, []byte(`{"competing":"ambiguous"}`), 0)
		cur.mu.Unlock()
	})
	t.Cleanup(restore)
	return func() bool { return armed }
}

func testSessionResident(asm *mqttAccountSessionManager, sess *mqttSession) bool {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	return asm.sessions[sess.id] == sess
}

func testParkedAction(sess *mqttSession) *mqttDeferredDisconnect {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.deferredDisconnect
}

// testSessionDurables returns the durable names this session's consumers hold, captured
// before a close so a later assertion can name what the discard owed.
func testSessionDurables(sess *mqttSession) []string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	durs := make([]string, 0, len(sess.cons))
	for _, cc := range sess.cons {
		durs = append(durs, cc.Durable)
	}
	return durs
}

// T4: the ordinary CLEAN CLOSE, with the ambiguity armed under the fence.
//
// The clean close is the caller whose failure leaks most: removeSession has already run, so
// on a bare log the record, the consumers and this generation's registers survive with no
// resident object — and for an expiry-0 session nothing reaps them, because
// retryUncertainSessions and CONNECT both scan as.sessions and sweepSessionExpiries skips
// an ExpiryInterval == 0 record.
//
// The WILL is the other half. It must still be published by this close, immediately, as it
// is today after a failed clear(): a parked Will is memory-only and a takeover drops it.
//
// Mutation MU-B: log on the sentinel instead of parking. It is killed by the RESIDENCY and
// parked-obligation assertions, NOT by the reconnect's durable deletion at the end — that
// deletion happens either way, because the surviving record is Clean=true, so a reconnect
// restores it and performs its own clean-start discard. The final durable check is therefore
// a successful-recovery check, not this mutation's discriminator.
// Mutation MU-B2: park but leave the session evicted, and nothing discharges it.
// Mutation MU-B3: park and return early, skipping mqttHandleWill, and the Will is never
// delivered. (The at-most-once half is already guaranteed by mqttTakeWillLocked consuming
// under c.mu, so it is kept as a check, not counted as this test's evidence.)
func TestMQTTCleanCloseReparksTheRefusedDiscard(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "clean-close-repark"
	// The Will's audience, on its own session.
	cs, rs := testMQTTConnect(t, &mqttConnInfo{clientID: "clean-close-will-sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, cs, rs, []*mqttFilter{{filter: "will/topic", qos: 0}}, []byte{0})
	testMQTTFlush(t, cs, nil, rs)

	// A clean session: Session Expiry Interval 0, so its close is a discard.
	will := &mqttWill{topic: []byte("will/topic"), message: []byte("bye"), qos: 0}
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true, will: will}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	asm, sess := testResidentSession(t, s, clientID)
	durs := testSessionDurables(sess)
	if len(durs) == 0 {
		t.Fatal("the fixture must own at least one consumer for the discard to release")
	}
	opened := testArmAmbiguityAtClearPreFence(t, sess)

	c.Close()

	// The Will is published by this close, exactly as it is today after a failed clear().
	testMQTTCheckPubMsg(t, cs, rs, "will/topic", 0, []byte("bye"))
	testMQTTExpectNothing(t, rs)

	if !opened() {
		t.Fatal("the pre-fence window never opened, so the race was not exercised")
	}
	// Nothing was discarded, and the session is resident again holding the obligation.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if !testSessionResident(asm, sess) {
			return errors.New("the session must be resident again: it holds the parked discard")
		}
		return nil
	})
	action := testParkedAction(sess)
	if action == nil {
		t.Fatal("the refused discard must be parked as an obligation")
	}
	if !action.endsAtDisconnect() {
		t.Fatalf("the parked action must end at disconnect, got expiryInterval=%d", action.expiryInterval)
	}
	if action.will != nil {
		t.Fatal("the recovery must not park the Will: this close published it")
	}
	if !testMQTTSessionRecordExists(t, s, "clean-close-will-sub", clientID) {
		t.Fatal("a refused discard must delete no record")
	}
	for _, dur := range durs {
		if _, err := asm.jsa.lookupConsumer(mqttStreamName, dur); err != nil {
			t.Fatalf("a refused discard must delete no durable (%q): %v", dur, err)
		}
	}

	// A reconnect resolves the write and discharges the parked discard, so the previous
	// incarnation's durables go. (Its FRESH state is not evidence here: Clean=true on the
	// surviving record already guarantees that.)
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	for _, dur := range durs {
		dur := dur
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if _, err := asm.jsa.lookupConsumer(mqttStreamName, dur); err == nil {
				return fmt.Errorf("the discharged discard must delete the durable %q", dur)
			}
			return nil
		})
	}
}

// T5: CONNECT's clean start rejects WITHOUT evicting.
//
// The early case a few lines above already does this, with the reasoning spelled out;
// the late case used to do the opposite. Evicting drops the retained continuations — so a
// mutation the ambiguous write persisted is never committed and one that did not land is
// never undone — and the gate release reads asm.sessions to decide the void-verdict
// handoff, which an evicted object never gets.
//
// Mutation MU-C: evict on the sentinel, and the second attempt below finds no session to
// resolve.
func TestMQTTConnectCleanStartRejectsWithoutEvicting(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "connect-clean-start-reject"
	// A persistent session that survives its close, so a later CONNECT meets it resident.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})
	asm, sess := testResidentSession(t, s, clientID)
	c.Close()
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sess.mu.Lock()
		bound := sess.c != nil
		sess.mu.Unlock()
		if bound {
			return errors.New("the session is still bound")
		}
		return nil
	})

	opened := testArmAmbiguityAtClearPreFence(t, sess)

	// Clean Start against the resident session: its discard is refused under the fence.
	cc, _ := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cc.Close()
	testMQTTExpectDisconnect(t, cc)
	if !opened() {
		t.Fatal("the pre-fence window never opened, so the race was not exercised")
	}
	if !testSessionResident(asm, sess) {
		t.Fatal("a refused CONNECT must not evict the session: its continuations and staged verdicts go with it")
	}
	if !sess.isSeqUncertain() {
		t.Fatal("the fixture must leave the session unresolved")
	}
	if a := testParkedAction(sess); a != nil {
		t.Fatalf("CONNECT must park nothing: it has a client waiting for a CONNACK, not an obligation, got %+v", a)
	}

	// The retry resolves the session itself and the clean start proceeds.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
}

// T7: the v5 expiry-override discard, which is the protocol outcome this recovery fixes.
//
// A v5 DISCONNECT carrying Session Expiry Interval 0 ends a session that was persistent —
// and it does so IN MEMORY ONLY: the record still says ExpiryInterval > 0 and Clean =
// false. So while the discard is owed, the parked action is the only representation of
// "this session must end", and losing it leaves a record a non-clean reconnect resumes,
// reporting Session Present true for a session the DISCONNECT ended (Spec5 [3.1.2.11]).
//
// What is asserted here is the common case: the discard is retried and completed at the
// next resolution, so the reconnect gets Session Present FALSE. The crash and
// remote-takeover windows are NOT asserted — they are a separate §0 roadmap item, and this
// change narrows them rather than closing them.
//
// Mutation MU-B: log instead of parking, and the reconnect resumes the ended session.
// Mutation MU-H: condition the recovery on sess.clean rather than on the sentinel — the
// override leaves sess.clean false, so that mutant skips exactly this case.
func TestMQTTExpiryOverrideCloseDiscardsAfterRefusal(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "expiry-override-discard"
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}})

	asm, sess := testResidentSession(t, s, clientID)
	sess.mu.Lock()
	persistent := sess.expiryInterval > 0 && !sess.clean
	sess.mu.Unlock()
	if !persistent {
		t.Fatal("the fixture must be a persistent session for the override to mean anything")
	}
	opened := testArmAmbiguityAtClearPreFence(t, sess)

	// Session Expiry Interval 0 at DISCONNECT: the session ends at this close.
	if _, err := testMQTTWrite(c, mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
		t.Fatalf("write DISCONNECT: %v", err)
	}
	c.Close()

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if !opened() {
			return errors.New("the pre-fence window has not opened yet")
		}
		if a := testParkedAction(sess); a == nil {
			return errors.New("the refused discard is not parked yet")
		}
		return nil
	})
	if !testSessionResident(asm, sess) {
		t.Fatal("the session must be resident again: it holds the parked discard")
	}

	// The next resolution completes the discard, so the record no longer describes a
	// resumable session and the reconnect starts fresh.
	asm.retryUncertainSessions(s)

	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID,
		props: mqttV5ConnPropsSessionExpiry(3600)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if present, _, _ := testMQTTReadConnAckV5(t, r2); present {
		t.Fatal("Session Present must be FALSE: the DISCONNECT ended this session, so the reconnect must not resume it")
	}
}

// T8: the call-graph invariant the clean close's Will fallthrough rests on.
//
// The recovery does not park the Will because mqttHandleWill publishes it immediately for
// an expiry-0 session. That is a property of the CALL GRAPH, not of mqttHandleWill: given
// an existing mqttWillOwedDelayed disposition it does not re-apply the zero-expiry rule,
// and would enqueue the Will into the outbox with its delay merely capped — building the
// takeover fence from the very sequence this item is about. The chain that makes that
// unreachable is that a v5 DISCONNECT updates c.mqtt.sessionExpiry and sess.expiryInterval
// together, and mqttOweWill classifies a zero interval as owed-NOW.
//
// Reason code 0x04 is load-bearing: a NORMAL DISCONNECT discards the Will outright, so
// without it "nothing was enqueued" would be trivially true and the test would prove
// nothing.
//
// This is a characterization test with no mutation of its own. Its value is that it fails
// if a later change breaks the chain the fallthrough depends on.
func TestMQTTDisconnectWithWillAndZeroExpiryPublishesImmediately(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "will-now-sub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/topic", opts: 0}})

	// A Will Delay Interval long enough that an enqueued Will would not fire during the
	// test, and a non-zero CONNECT Session Expiry Interval so the delay is live to begin
	// with.
	will := &mqttWill{topic: []byte("will/topic"), message: []byte("bye"), qos: 0}
	cw, rw := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "will-now-der", will: will,
		willProps: mqttV5WillDelayProps(30), props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rw)

	// Reason 0x04 KEEPS the Will; Session Expiry Interval 0 ends the session at this close.
	if _, err := testMQTTWrite(cw, mqttV5DisconnectSessionExpiry(mqttReasonDisconnectWithWill, 0)); err != nil {
		t.Fatalf("write DISCONNECT: %v", err)
	}
	cw.Close()

	testMQTTReadPublishV5(t, rs, "will/topic", []byte("bye"))
	if n := testMQTTNumPendingWills(t, s, "will-now-sub"); n != 0 {
		t.Fatalf("an owed-now Will must not be enqueued into the outbox, got %d pending", n)
	}
}
