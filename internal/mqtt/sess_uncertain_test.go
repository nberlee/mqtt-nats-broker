package mqtt

import (
	"encoding/json"
	"testing"
	"time"
)

// testMarkUncertain puts a session into the unresolved state as an ambiguous write
// would, against the given CAS base and record body.
func testMarkUncertain(sess *mqttSession, baseSeq uint64, body []byte) {
	sess.mu.Lock()
	sess.markWriteUncertainLocked(baseSeq, body, 0)
	sess.mu.Unlock()
}

// testRecordBody returns the exact bytes a save of this session's current state
// would store — the identity an ambiguous write is later classified against.
func testRecordBody(t testing.TB, sess *mqttSession) []byte {
	t.Helper()
	sess.mu.Lock()
	ps := sess.buildPersistedSessionLocked()
	sess.mu.Unlock()
	b, err := json.Marshal(&ps)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return b
}

// TestUncertainWriteThreeWayReadback is test 5: the classification itself, in all
// three outcomes plus the unreadable case.
//
// The middle case is the one that matters most in both directions. "Adopt the
// latest sequence" would resolve it — and would also turn a stale local snapshot
// into a VALID CAS over another broker's newer record, silently overwriting its
// subscriptions, drains, claims and expiry state. Only a byte-identical record may
// be adopted.
func TestUncertainWriteThreeWayReadback(t *testing.T) {
	a, b := testNewTeardownEnv(t)

	t.Run("did not land", func(t *testing.T) {
		sess := testProvisionSession(t, a, "notland")
		_, seq := testLoadSessRecord(t, a, sess.idHash)
		// An attempted write whose body never reached the stream.
		sess.subs["extra"] = 1
		testMarkUncertain(sess, seq, testRecordBody(t, sess))

		if res, err := sess.resolveUncertainWrite(); res != mqttUncertainOurs {
			t.Fatalf("resolution = %v (err=%v), want ours", res, err)
		}
		if sess.isSeqUncertain() {
			t.Error("the session should be resolved")
		}
		sess.mu.Lock()
		got := sess.seq
		sess.mu.Unlock()
		if got != seq {
			t.Errorf("sess.seq = %v, want the unchanged base %v", got, seq)
		}
	})

	t.Run("landed with the response lost", func(t *testing.T) {
		sess := testProvisionSession(t, a, "landed")
		_, baseSeq := testLoadSessRecord(t, a, sess.idHash)
		sess.subs["extra"] = 1
		body := testRecordBody(t, sess)
		// The write really lands, but the caller never learns it did.
		landedSeq, err := mqttStoreSessionRecordCASBody(&a.asm.jsa, a.asm.domainTk, body, sess.idHash, baseSeq)
		if err != nil {
			t.Fatalf("simulate the landed write: %v", err)
		}
		testMarkUncertain(sess, baseSeq, body)

		if res, err := sess.resolveUncertainWrite(); res != mqttUncertainOurs {
			t.Fatalf("resolution = %v (err=%v), want ours", res, err)
		}
		sess.mu.Lock()
		got := sess.seq
		sess.mu.Unlock()
		if got != landedSeq {
			t.Errorf("sess.seq = %v, want the sequence the write landed at (%v)", got, landedSeq)
		}
	})

	t.Run("a foreign record must not be adopted", func(t *testing.T) {
		sess := testProvisionSession(t, a, "foreign")
		_, baseSeq := testLoadSessRecord(t, a, sess.idHash)
		sess.subs["ours"] = 1
		body := testRecordBody(t, sess)

		// Another broker takes over and persists DIFFERENT content at a newer
		// sequence — the exact shape "adopt the latest sequence" would swallow.
		ps, _ := testLoadSessRecord(t, b, sess.idHash)
		ps.Subs = map[string]byte{"theirs": 2}
		ps.Origin = b.asm.jsa.id
		foreignBody, err := json.Marshal(ps)
		if err != nil {
			t.Fatalf("marshal foreign record: %v", err)
		}
		foreignSeq, err := mqttStoreSessionRecordCASBody(&b.asm.jsa, b.asm.domainTk, foreignBody, sess.idHash, baseSeq)
		if err != nil {
			t.Fatalf("foreign takeover write: %v", err)
		}
		testMarkUncertain(sess, baseSeq, body)

		if res, err := sess.resolveUncertainWrite(); res != mqttUncertainForeign {
			t.Fatalf("resolution = %v (err=%v), want foreign", res, err)
		}
		sess.mu.Lock()
		adopted := sess.seq
		stillUncertain := sess.seqUncertain
		sess.mu.Unlock()
		if adopted == foreignSeq {
			t.Error("the foreign sequence was adopted; the next save would overwrite another broker's record")
		}
		if !stillUncertain {
			t.Error("a foreign record must leave the local write unresolved, not resolved as ours")
		}
		// And the foreign record is untouched.
		got, gotSeq := testLoadSessRecord(t, b, sess.idHash)
		if got == nil || gotSeq != foreignSeq || len(got.Subs) != 1 {
			t.Fatalf("the foreign record was disturbed: %+v at %v", got, gotSeq)
		}
	})

	t.Run("an absent record is not ours", func(t *testing.T) {
		sess := testProvisionSession(t, a, "absent")
		_, baseSeq := testLoadSessRecord(t, a, sess.idHash)
		body := testRecordBody(t, sess)
		if err := a.asm.jsa.deleteMsg(mqttSessStreamName, baseSeq, true); err != nil {
			t.Fatalf("delete the record: %v", err)
		}
		testMarkUncertain(sess, baseSeq, body)

		if res, err := sess.resolveUncertainWrite(); res != mqttUncertainForeign {
			t.Fatalf("resolution = %v (err=%v), want foreign: a released record is not ours to rebuild", res, err)
		}
	})
}

// TestUncertainSessionRefusesWritesAndTeardown is test 6's core guarantee: while a
// session is unresolved, nothing may be persisted and — the part that matters most
// — nothing may be DELETED. clear() would otherwise remove the shared durables and
// the record before its stale CAS could ever fail.
func TestUncertainSessionRefusesWritesAndTeardown(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, "refuse")
	_, seq := testLoadSessRecord(t, a, sess.idHash)
	_, _, pubRelDur := mqttPubRelNames(sess.idHash, sess.gen)
	hold := mqttQoS2HoldSubject(sess.idHash, sess.gen, 1)

	testMarkUncertain(sess, seq, testRecordBody(t, sess))

	if err := testSessSave(t, sess); err == nil {
		t.Error("a save must be refused while an earlier write is unresolved")
	}
	if err := sess.clear(false); err == nil {
		t.Fatal("a discard must be refused while an earlier write is unresolved")
	}

	// Refusal has to happen BEFORE anything is destroyed, not partway through.
	if ps, gotSeq := testLoadSessRecord(t, a, sess.idHash); ps == nil || gotSeq != seq {
		t.Errorf("the record was removed by a refused discard: %+v at %v", ps, gotSeq)
	}
	if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
		t.Error("the PUBREL consumer was deleted by a refused discard")
	}
	if _, err := a.asm.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, hold); err != nil {
		t.Errorf("the QoS2 hold was purged by a refused discard: %v", err)
	}

	// Once resolved, both work again.
	if res, err := sess.resolveUncertainWrite(); res != mqttUncertainOurs {
		t.Fatalf("resolution = %v (err=%v), want ours", res, err)
	}
	if err := testSessSave(t, sess); err != nil {
		t.Errorf("a resolved session must be able to save: %v", err)
	}
	if err := sess.clear(false); err != nil {
		t.Errorf("a resolved session must be able to discard: %v", err)
	}
}

// TestDeferredDisconnectPreservesOriginalDeadlines pins the rule that makes
// deferral safe: an outage must not silently extend either the session lifetime or
// the Will Delay Interval. Both deadlines are absolute and measured from the
// ORIGINAL disconnect.
func TestDeferredDisconnectPreservesOriginalDeadlines(t *testing.T) {
	disconnected := time.Now().Add(-2 * time.Hour)

	finite := &mqttDeferredDisconnect{
		disconnectedAt: disconnected.Unix(),
		expiryInterval: 60, // one minute, long since past
	}
	deadline, ok := finite.expiryDeadline()
	if !ok {
		t.Fatal("a finite interval must have a deadline")
	}
	if want := disconnected.Unix() + 60; deadline != want {
		t.Errorf("expiry deadline = %v, want %v (measured from the ORIGINAL disconnect)", deadline, want)
	}
	if !finite.expiredBy(time.Now()) {
		t.Error("a session whose deadline passed during the outage must expire immediately, not get a fresh interval")
	}

	live := &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Unix(),
		expiryInterval: 3600,
	}
	if live.expiredBy(time.Now()) {
		t.Error("a session still inside its interval must not be expired")
	}
	if live.endsAtDisconnect() {
		t.Error("a finite interval does not end at connection close")
	}

	clean := &mqttDeferredDisconnect{disconnectedAt: disconnected.Unix()}
	if !clean.endsAtDisconnect() {
		t.Error("a zero interval must be a deferred discard")
	}
	if _, ok := clean.expiryDeadline(); ok {
		t.Error("a zero interval has no deadline to arm")
	}

	never := &mqttDeferredDisconnect{disconnectedAt: disconnected.Unix(), expiryInterval: mqttSessionNeverExpire}
	if _, ok := never.expiryDeadline(); ok {
		t.Error("a never-expiring session has no deadline to arm")
	}
	if never.expiredBy(time.Now()) {
		t.Error("a never-expiring session must never be expired by the deferred path")
	}

	// The Will: due strictly by its own absolute deadline, so a reconnect after it
	// cannot suppress a Will that was already owed.
	pastWill := &mqttDeferredDisconnect{willDeadline: time.Now().Add(-time.Minute).UnixNano()}
	if !pastWill.willDue(time.Now()) {
		t.Error("a Will whose delay elapsed during the outage is owed, not restarted")
	}
	futureWill := &mqttDeferredDisconnect{willDeadline: time.Now().Add(time.Hour).UnixNano()}
	if futureWill.willDue(time.Now()) {
		t.Error("a Will still inside its delay is not yet due")
	}
	noWill := &mqttDeferredDisconnect{}
	if noWill.willDue(time.Now()) {
		t.Error("a session with no Will has nothing due")
	}
}

// TestRetryUncertainSessionsIsSerializedAgainstConnect pins that the periodic pass
// yields to a CONNECT: both would otherwise resolve the same resident session at
// once, one running the deferred clear() while the other computes Session Present.
//
// It also covers why this pass exists at all rather than riding on
// sweepSessionExpiries, which walks persisted RECORDS and skips ExpiryInterval 0
// and never-expire — exactly the two cases that park a deferred discard or a
// deferred Will.
func TestRetryUncertainSessionsIsSerializedAgainstConnect(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, "retrygate")
	a.asm.addSession(sess, true)
	_, seq := testLoadSessRecord(t, a, sess.idHash)

	// A never-expiring session: the expiry sweep would never look at it.
	sess.mu.Lock()
	sess.expiryInterval = mqttSessionNeverExpire
	sess.mu.Unlock()
	testMarkUncertain(sess, seq, testRecordBody(t, sess))

	// While a CONNECT holds the client ID, the pass must not touch the session.
	a.asm.mu.Lock()
	a.asm.sessLocked[sess.id] = struct{}{}
	a.asm.mu.Unlock()
	a.asm.retryUncertainSessions(a.s)
	if !sess.isSeqUncertain() {
		t.Fatal("the retry pass resolved a session a CONNECT was holding")
	}
	a.asm.mu.Lock()
	delete(a.asm.sessLocked, sess.id)
	a.asm.mu.Unlock()

	// Released, it resolves — proving the pass does reach a never-expiring session.
	a.asm.retryUncertainSessions(a.s)
	if sess.isSeqUncertain() {
		t.Fatal("the retry pass did not resolve an unbound, unresolved session")
	}
}
