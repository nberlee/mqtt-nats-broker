package mqtt

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The deferred close's shared purge, as ONE classified record write.
//
// A close that happens while the session's record write is unresolved is PARKED and applied
// later by applyDeferredDisconnect. Its surviving-session branch owes everything the ordinary
// surviving close owes, and the shared purge is the half it used to skip: the session kept its
// group packet identifiers and Receive Maximum slots for the rest of its life, and a resume
// could NAK-steal group messages AckWait would have redelivered to other members.
//
// The tests below are built around why a bare purge call is wrong. The purge can fall back to
// the session record, so a purge that saved for ITSELF could land ambiguously and leave the
// branch's own disconnect-time save refused — with the parked action already consumed and
// nothing left to retry it.

// testDeferredEnv is an inflight env with one shared-group delivery in flight and its session
// record persisted, which is the state a surviving close purges.
type testDeferredEnv struct {
	*testInflightEnv
	group  string
	filter string
	dur    string
	sseq   uint64
	pi     uint16
	nonce  string
}

func testNewDeferredEnv(t *testing.T, clientID, filter string) *testDeferredEnv {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	const group = "g"
	_, _, dur := mqttSharedNames(group, filter)
	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	pi := e.track(t, dur, sseq, 1)
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the session record: %v", err)
	}
	// RESIDENT, as a session that has been through a CONNECT is: the gate-release handoff
	// deliberately skips an object no map points at, because an evicted session has no
	// record of its own to write.
	e.asm.addSession(e.sess, true)
	return &testDeferredEnv{
		testInflightEnv: e, group: group, filter: filter,
		dur: dur, sseq: sseq, pi: pi, nonce: e.nonceOf(pi),
	}
}

// storeSource publishes another source message for this env's shared filter, for a second
// allocation on the same group durable.
func (e *testDeferredEnv) storeSource(t *testing.T) uint64 {
	t.Helper()
	resp, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+e.filter, -1, []byte("payload"))
	if err != nil {
		t.Fatalf("store a source message: %v", err)
	}
	return resp.Sequence
}

// failRegisterStream makes both register-stream rungs — the confirmed delete and the
// cancellation tombstone — fail, so only the session record can carry a verdict.
func (e *testDeferredEnv) failRegisterStream(t *testing.T) {
	t.Helper()
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return errors.New("test: register delete unavailable") }
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
		return 0, errors.New("test: register stream unavailable")
	}
	e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, errors.New("test: register stream unavailable")
	}
	e.asm.mu.Unlock()
}

// gate takes the ownership gate for an unbound session, which is the state BOTH callers of
// applyDeferredDisconnect are in: resolveUncertainSession's caller holds it, and
// resolveUncertainNow takes it itself. Every convergence claim in these tests depends on it,
// because the gate is what suppresses the discharge's own fence-release handoff.
func (e *testDeferredEnv) gate(t *testing.T) {
	t.Helper()
	e.sess.mu.Lock()
	bound := e.sess.c != nil
	e.sess.mu.Unlock()
	if bound {
		t.Fatal("a deferred close is discharged only for an UNBOUND session")
	}
	if err := e.asm.lockSession(e.sess, nil); err != nil {
		t.Fatalf("take the ownership gate: %v", err)
	}
}

func (e *testDeferredEnv) release() { e.asm.unlockSession(e.sess) }

// testDeferredExpiry is long enough that no armed timer fires during a test.
const testDeferredExpiry = 3600

// testSurvivingAction is a parked close whose session SURVIVES, carrying an original
// disconnect time that is neither zero nor "now" — the two values a branch that persisted the
// wrong one would produce.
func testSurvivingAction() *mqttDeferredDisconnect {
	return &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Unix() - 600,
		expiryInterval: testDeferredExpiry,
	}
}

// testSessRecordStoreCounter counts SESSION RECORD writes that LANDED for one session, so a
// test can state "a second write happened" rather than inferring it from a state both
// orderings reach. It counts on the incoming outcome, before any rewrite of its own: the
// result seam also fires for a write the PRE-store seam suppressed, which never reached the
// stream at all.
func testSessRecordStoreCounter(sess *mqttSession) (*int, func()) {
	n := 0
	restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s == sess && err == nil {
			n++
		}
		return resp, err
	})
	return &n, restore
}

func (e *testDeferredEnv) loadRecord(t *testing.T) *mqttPersistedSession {
	t.Helper()
	ps, _, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	if ps == nil {
		t.Fatal("the surviving session must still have a record")
	}
	return ps
}

func (e *testDeferredEnv) voidRevs(t *testing.T) (rev, saved, handoff uint64) {
	t.Helper()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.voidedRegsRev, e.sess.voidedRegsSavedRev, e.sess.voidedRegsHandoffRev
}

func testRecordHasNonce(ps *mqttPersistedSession, nonce string) bool {
	for _, n := range ps.CancelledRegs {
		if n == nonce {
			return true
		}
	}
	return false
}

// T1: the deferred close PURGES.
//
// Mutation MU2: skip the purge in the deferred branch (today's behavior) and the identifier,
// the pending maps and the register all survive the close.
// Mutation MU3: set disconnectedAt after the save and the record carries 0.
func TestMQTTDeferredCloseRunsSharedPurge(t *testing.T) {
	t.Parallel()
	e := testNewDeferredEnv(t, "client-deferred-purge", "deferred.purge")
	action := testSurvivingAction()

	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()

	// The OUTER bucket, not just the inner entries: a purge that emptied cpending[dur]
	// without deleting the key leaves the derivation reading a live durable forever.
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[e.pi]
	_, bucket := e.sess.cpending[e.dur]
	verdicts := len(e.sess.cancelledRegNonces)
	e.sess.mu.Unlock()
	if held || bucket {
		t.Fatalf("the deferred close must free the shared identifier: held=%v cpending bucket=%v", held, bucket)
	}
	if verdicts != 0 {
		t.Fatalf("a proven delete must forget its verdict, %d left", verdicts)
	}
	e.waitRegisterCount(t, 0)

	// The group durable survives: other members may hold it, and the membership does.
	if _, err := e.asm.jsa.lookupConsumer(mqttStreamName, e.dur); err != nil {
		t.Fatalf("the purge must never delete the group consumer: %v", err)
	}

	// The record carries the ORIGINAL disconnect time, so the session gets the lifetime it
	// earned rather than one restarted by the outage.
	ps := e.loadRecord(t)
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Fatalf("record DisconnectedAt=%d, want the original %d", ps.DisconnectedAt, action.disconnectedAt)
	}
	e.asm.mu.Lock()
	_, armed := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if !armed {
		t.Fatal("the deferred close must arm the remaining expiry")
	}
}

// T2: the fallback verdict and the deadline reach the record in ONE write, and that write's
// response being lost does not change the outcome.
//
// This is the bullet's own requirement. Both register-stream rungs fail, so the purge has
// only the session record left; the record write LANDS and its response is lost, which is the
// case the naive fix could not survive — an independent purge save would have left the
// branch's disconnect save refused with the parked action already consumed.
//
// Mutation MU1: revert to a bare purgeSharedPending() call before the transaction and the
// record keeps DisconnectedAt=0.
// Mutation MU3: set disconnectedAt after the save and the stored body carries 0.
func TestMQTTDeferredCloseWritesVerdictAndDeadlineTogether(t *testing.T) {
	e := testNewDeferredEnv(t, "client-deferred-one-write", "deferred.onewrite")
	e.failRegisterStream(t)

	stores := 0
	restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s != e.sess {
			return resp, err
		}
		stores++
		if stores == 1 && err == nil {
			// Landed, response lost: sess.seq is now stale and the readback owes the
			// classification.
			return nil, errors.New("test: the response to a landed record write was lost")
		}
		return resp, err
	})
	defer restore()

	action := testSurvivingAction()
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)

	if !e.sess.isSeqUncertain() {
		t.Fatal("an ambiguous record write must leave the session unresolved")
	}
	rev, saved, _ := e.voidRevs(t)
	if rev == 0 || saved >= rev {
		t.Fatalf("the purge must have staged an unproven verdict: rev=%d saved=%d", rev, saved)
	}
	// The verdict travels with the ATTEMPT's identity, which is what makes the credit
	// below exact: the resolver credits the revision those bytes marshalled, not
	// whatever has been staged since.
	e.sess.mu.Lock()
	pw := e.sess.pendingWrite
	e.sess.mu.Unlock()
	if pw == nil || pw.voidedRegsRev != rev {
		t.Fatalf("the pending write must carry the marshalled void revision %d, got %+v", rev, pw)
	}

	res, err := e.sess.resolveUncertainWrite()
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v (err=%v), want ours: the same bytes are on the stream", res, err)
	}
	e.release()

	if stores != 1 {
		t.Fatalf("the deferred close must make exactly ONE record write, got %d", stores)
	}
	ps := e.loadRecord(t)
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Fatalf("record DisconnectedAt=%d, want the original %d", ps.DisconnectedAt, action.disconnectedAt)
	}
	if !testRecordHasNonce(ps, e.nonce) {
		t.Fatalf("the same record must carry the fallback verdict %q, got %v", e.nonce, ps.CancelledRegs)
	}
	if _, saved, _ := e.voidRevs(t); saved < rev {
		t.Fatalf("a proven landed write must credit the verdict: rev=%d saved=%d", rev, saved)
	}

	// Restart survival, from the record alone: a peer restoring it sees the earned deadline
	// and the verdict, not a connected-looking session.
	_, sessB, _ := e.peer(t, "node2", e.sess.id)
	sessB.mu.Lock()
	restoredAt, restoredVerdicts := sessB.disconnectedAt, len(sessB.cancelledRegNonces)
	sessB.mu.Unlock()
	if restoredAt != action.disconnectedAt {
		t.Fatalf("a restart must read the earned deadline: DisconnectedAt=%d, want %d", restoredAt, action.disconnectedAt)
	}
	if restoredVerdicts != 1 {
		t.Fatalf("a restart must read the verdict, got %d", restoredVerdicts)
	}
}

// T3: an ambiguous write that never reached the stream is settled by the RE-ATTEMPT, deadline
// included.
//
// Driven through the real resolution path: the suppressed store is the one shape that is
// genuinely "issued and never landed", and what makes the deadline durable anyway is the
// resolver re-sending the attempt's exact bytes rather than reading the base sequence and
// calling the write dead.
//
// Mutation M1: revert the at-base arm to clearing the uncertainty and the record keeps
// DisconnectedAt=0 — nothing ever writes it.
func TestMQTTDeferredCloseUnlandedWriteIsRetried(t *testing.T) {
	e := testNewDeferredEnv(t, "client-deferred-unlanded", "deferred.unlanded")
	e.failRegisterStream(t)
	stores, restore := testSessRecordStoreCounter(e.sess)
	defer restore()

	// The pre-store seam is the only one that produces "issued but never landed": the result
	// hook runs after the store, and the save hook exits before the write.
	mqttTestSessStoreSuppressV.Store(true)
	action := testSurvivingAction()
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	mqttTestSessStoreSuppressV.Store(false)

	if *stores != 0 {
		t.Fatalf("the suppressed write must not have reached the stream, %d store(s)", *stores)
	}
	rev, savedBefore, _ := e.voidRevs(t)
	if savedBefore >= rev {
		t.Fatalf("nothing landed, so nothing may be credited: rev=%d saved=%d", rev, savedBefore)
	}

	res, err := e.sess.resolveUncertainWrite()
	if res != mqttUncertainOurs {
		t.Fatalf("resolution = %v (err=%v), want ours", res, err)
	}
	e.release()

	if *stores != 1 {
		t.Fatalf("the resolver's re-attempt must make the record write, %d store(s)", *stores)
	}
	ps := e.loadRecord(t)
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Fatalf("the re-attempt must carry the original deadline: DisconnectedAt=%d, want %d",
			ps.DisconnectedAt, action.disconnectedAt)
	}
	if !testRecordHasNonce(ps, e.nonce) {
		t.Fatalf("the re-attempt must carry the verdict %q, got %v", e.nonce, ps.CancelledRegs)
	}
	if _, saved, _ := e.voidRevs(t); saved < rev {
		t.Fatalf("the credit must follow the landed re-attempt: rev=%d saved=%d", rev, saved)
	}
}

// T3b: an ambiguous RE-ATTEMPT keeps the attempt's identity and reports UNRESOLVED, and the
// next pass settles it.
//
// The re-attempt is the same content at the same CAS base, so at most one of it and the
// original can commit and the retained identity names whichever did. Dropping that identity
// on an ambiguous re-attempt would be the destructive move: the session would be reported
// resolved (or left with nothing to classify), a CONNECT would bind, its accept save would be
// refused, and the object holding the deferred deadline and the verdicts would be evicted with
// no crash anywhere.
//
// Mutation M6: clear pendingWrite on the re-attempt's ambiguous outcome and the second
// resolution has no identity to match the record against.
func TestMQTTDeferredCloseSecondAmbiguityReArms(t *testing.T) {
	e := testNewDeferredEnv(t, "client-deferred-rearm", "deferred.rearm")
	e.failRegisterStream(t)

	mqttTestSessStoreSuppressV.Store(true)
	action := testSurvivingAction()
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	mqttTestSessStoreSuppressV.Store(false)

	e.sess.mu.Lock()
	pw := e.sess.pendingWrite
	e.sess.mu.Unlock()
	if pw == nil {
		t.Fatal("the suppressed write must have left an attempt identity")
	}

	// The resolver's re-attempt lands, and its response is lost.
	stores := 0
	restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s != e.sess {
			return resp, err
		}
		stores++
		if stores == 1 && err == nil {
			return nil, errors.New("test: the response to a landed re-save was lost")
		}
		return resp, err
	})
	defer restore()

	res, err := e.sess.resolveUncertainWrite()
	if res != mqttUncertainUnresolved {
		t.Fatalf("resolution = %v (err=%v), want unresolved: the re-attempt's own outcome is unknown", res, err)
	}
	if !e.sess.isSeqUncertain() {
		t.Fatal("the unsettled uncertainty must stand")
	}
	e.sess.mu.Lock()
	same := e.sess.pendingWrite == pw
	e.sess.mu.Unlock()
	if !same {
		t.Fatal("the identity must survive an ambiguous re-attempt: it names whichever proposal lands")
	}
	e.asm.mu.RLock()
	resident := e.asm.sessByHash[e.sess.idHash] == e.sess
	e.asm.mu.RUnlock()
	if !resident {
		t.Fatal("the session holding the unresolved attempt must stay resident")
	}

	// The following resolution finally settles it: those bytes are on the stream.
	res, err = e.sess.resolveUncertainWrite()
	if res != mqttUncertainOurs {
		t.Fatalf("second resolution = %v (err=%v), want ours", res, err)
	}
	e.release()
	ps := e.loadRecord(t)
	if ps.DisconnectedAt != action.disconnectedAt || !testRecordHasNonce(ps, e.nonce) {
		t.Fatalf("the settled record must carry deadline and verdict: DisconnectedAt=%d verdicts=%v",
			ps.DisconnectedAt, ps.CancelledRegs)
	}
}

// T4: a DEFINITE save failure keeps the purge, and production's own gate release converges the
// verdict once the store recovers.
//
// Mutation MU11a: drop the gate-release trigger at the unlockSession form and the verdict
// stays memory-only — the branch's own fence release is gate-suppressed, so nothing else
// converges it.
func TestMQTTDeferredCloseDefiniteFailureConvergesAtGateRelease(t *testing.T) {
	e := testNewDeferredEnv(t, "client-deferred-definite", "deferred.definite")
	e.failRegisterStream(t)
	restoreSave := setMQTTTestSessSaveHook(func(s *mqttSession) error {
		if s != e.sess {
			return nil
		}
		return errors.New("test: the session store is unavailable")
	})

	action := testSurvivingAction()
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)

	if e.sess.isSeqUncertain() {
		t.Fatal("a definite failure is not uncertainty")
	}
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[e.pi]
	_, bucket := e.sess.cpending[e.dur]
	verdicts := len(e.sess.cancelledRegNonces)
	disconnectedAt := e.sess.disconnectedAt
	e.sess.mu.Unlock()
	if held || bucket {
		t.Fatalf("the in-memory purge stands: held=%v bucket=%v", held, bucket)
	}
	if verdicts != 1 {
		t.Fatalf("the verdict must be retained for the next convergence point, got %d", verdicts)
	}
	if disconnectedAt != action.disconnectedAt {
		t.Fatalf("in-memory disconnectedAt=%d, want %d", disconnectedAt, action.disconnectedAt)
	}
	e.asm.mu.Lock()
	_, armed := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if !armed {
		t.Fatal("the timer is armed whatever the save did")
	}

	// The store recovers. Nothing calls a persistence helper by hand: the gate release IS
	// the mechanism.
	restoreSave()
	rev, saved, _ := e.voidRevs(t)
	if saved >= rev {
		t.Fatalf("the verdict must still be uncredited before the release: rev=%d saved=%d", rev, saved)
	}
	e.release()

	ps := e.loadRecord(t)
	if !testRecordHasNonce(ps, e.nonce) {
		t.Fatalf("the gate release must persist the verdict %q, got %v", e.nonce, ps.CancelledRegs)
	}
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Fatalf("the same write carries the deadline: DisconnectedAt=%d, want %d",
			ps.DisconnectedAt, action.disconnectedAt)
	}
	if _, saved, _ := e.voidRevs(t); saved < rev {
		t.Fatalf("the landed handoff must credit: rev=%d saved=%d", rev, saved)
	}
}

// T5: a deferred close on a STALE object cannot destroy a register a COMPETING WRITER has
// claimed,
// and cannot record a verdict about it either.
//
// The register-claims arbitration is what covers the deletes: a claim rewrite moves the
// sequence, so an exact-sequence delete removes nothing. The delete seam here returns a
// GENERIC failure on purpose — an old-sequence delete reads "already gone" as success and
// never reaches the tombstone rung, which is the rung that must abandon on a foreign claim.
func TestMQTTDeferredCloseFencedByRegisterClaim(t *testing.T) {
	t.Parallel()
	const clientID = "client-deferred-stale-claim"
	e := testNewDeferredEnv(t, clientID, "deferred.claim")
	_, before := e.loadRegister(t, e.pi)
	if before == nil {
		t.Fatal("the stale owner must know the original register sequence")
	}

	asmB, sessB, cB := e.peer(t, "node2", clientID)
	if err := testSessSave(t, sessB); err != nil {
		t.Fatalf("persist the takeover: %v", err)
	}
	if _, err := sessB.recoverInflightRegisters(cB); err != nil {
		t.Fatalf("takeover recovery: %v", err)
	}
	subject := mqttInflightSubject(sessB.idHash, sessB.gen, e.pi)
	claimed, after, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || claimed == nil || after == nil {
		t.Fatalf("load the claimed register: rec=%+v msg=%+v err=%v", claimed, after, err)
	}
	if after.Sequence == before.Sequence || claimed.Claim != sessB.regClaim {
		t.Fatalf("the takeover did not move and claim the register: before=%d after=%d claim=%q want=%q",
			before.Sequence, after.Sequence, claimed.Claim, sessB.regClaim)
	}

	// Force the tombstone rung: without this the stale delete succeeds vacuously.
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(uint64) error { return errors.New("test: register delete unavailable") }
	e.asm.mu.Unlock()

	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, testSurvivingAction())
	e.release()

	survivor, survived, err := asmB.jsa.loadInflightRecord(subject)
	if err != nil || survivor == nil || survived == nil {
		t.Fatalf("the stale close removed the claimed register: rec=%+v msg=%+v err=%v", survivor, survived, err)
	}
	if survived.Sequence != after.Sequence || survivor.Claim != sessB.regClaim ||
		survivor.Cancelled || survivor.Nonce != claimed.Nonce {
		t.Fatalf("the stale close changed the competing writer's register: seq=%d claim=%q cancelled=%v nonce=%q, want %d/%q/false/%q",
			survived.Sequence, survivor.Claim, survivor.Cancelled, survivor.Nonce,
			after.Sequence, sessB.regClaim, claimed.Nonce)
	}
	// And no verdict about it lands: the stale record save is CAS-refused.
	ps, _, err := asmB.loadFenceSession(e.sess.idHash)
	if err != nil || ps == nil {
		t.Fatalf("load the authoritative record: ps=%+v err=%v", ps, err)
	}
	if testRecordHasNonce(ps, e.nonce) {
		t.Fatalf("a stale object must not record a verdict about a claimed register: %v", ps.CancelledRegs)
	}
}

// armLateSettle arms an allocation whose register CAS is still IN FLIGHT when the close runs.
// The purge cancels it and its tombstone LANDS, which RETIRES its nonce from the verdict set —
// so the branch's save marshals without it. The store then settles later with the register
// stream unreachable, and the settle stages the nonce again, this time BEHIND the marshal:
// exactly the late staging no write in flight can carry, and the reason a lock release has to
// re-check.
//
// The settle is the production entry point (confirmInflightRegister's cancelled ladder), not a
// hand-built state, and the register stream is failed only from inside the window so the
// purge's own rung behaves normally.
func (e *testDeferredEnv) armLateSettle(t *testing.T) (settle func(), nonce func() string) {
	t.Helper()
	sseq2 := e.storeSource(t)
	e.sess.mu.Lock()
	pi2, isNew := e.sess.trackPublish(e.dur, testInflightAckSubject(e.dur, sseq2, 1))
	var n2 string
	if ack := e.sess.pendingPublish[pi2]; ack != nil {
		n2 = ack.regNonce
	}
	e.sess.mu.Unlock()
	if pi2 == 0 || !isNew || n2 == _EMPTY_ {
		t.Fatalf("the second allocation was not armed: pi=%d isNew=%v nonce=%q", pi2, isNew, n2)
	}

	return func() {
		// From here the register stream is unreachable, so this settle has only the nonce
		// verdict left: the delete fails, and the tombstone can neither be written nor be
		// read back as already present.
		e.asm.mu.Lock()
		e.asm.testInflightDelete = func(uint64) error { return errors.New("test: register delete unavailable") }
		e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) {
			return 0, errors.New("test: register stream unavailable")
		}
		e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
			return nil, nil, errors.New("test: register stream unavailable")
		}
		e.asm.mu.Unlock()
		// A landed CAS whose confirmation runs now: the entry was cancelled under it, so it
		// walks the cancelled ladder — delete, tombstone, then the nonce verdict — and its
		// own persistence attempt is refused by whatever fence is open. The sequence is one
		// the delete seam refuses, which is what drives it down the ladder.
		e.sess.confirmInflightRegister(e.c, pi2, e.dur, sseq2, testLateRegisterSeq, n2, 1, 0, false)
	}, func() string { return n2 }
}

// testLateRegisterSeq is the sequence a late settle names: the seam refuses to delete it, so
// the settle walks the whole ladder down to the nonce verdict.
const testLateRegisterSeq = uint64(424242)

// T6: a verdict staged BEHIND the branch's marshal is converged by the ownership-gate
// release, with no over-credit in between.
//
// Mutation MU11a: drop the gate-release trigger and the late nonce stays memory-only.
// Mutation MU15c: credit the set's CURRENT revision instead of the marshalled one and the
// late nonce is claimed durable before anything wrote it.
func TestMQTTDeferredCloseLateSettleConvergesAtGateRelease(t *testing.T) {
	e := testNewDeferredEnv(t, "client-deferred-late-settle", "deferred.late")
	settle, lateNonce := e.armLateSettle(t)

	settled := false
	restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s == e.sess && !settled {
			// Inside the branch's save, with its transaction still open: the settle stages
			// its nonce and finds its own persistence refused.
			settled = true
			settle()
		}
		return resp, err
	})
	defer restore()

	action := testSurvivingAction()
	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)

	if !settled {
		t.Fatal("the late settle never ran inside the branch's write window")
	}
	late := lateNonce()
	// The save landed and credited what it marshalled, which is strictly less than what is
	// now staged — and the reservation the settle parked is still held.
	rev, saved, handoff := e.voidRevs(t)
	if saved >= rev {
		t.Fatalf("the late nonce must not be credited by a write that never carried it: rev=%d saved=%d", rev, saved)
	}
	if handoff != 0 {
		t.Fatalf("the branch's own fence release is gate-suppressed, so no attempt may have run: handoff=%d", handoff)
	}
	ps := e.loadRecord(t)
	if testRecordHasNonce(ps, late) {
		t.Fatalf("the record cannot yet carry a nonce staged after its marshal: %v", ps.CancelledRegs)
	}
	e.sess.mu.Lock()
	_, parked := e.sess.settledVoidReservations[late]
	e.sess.mu.Unlock()
	if !parked {
		t.Fatal("a settled-but-unproven reservation must be parked for the proof edge")
	}

	e.release()

	ps = e.loadRecord(t)
	if !testRecordHasNonce(ps, late) {
		t.Fatalf("the gate release must persist the late verdict %q, got %v", late, ps.CancelledRegs)
	}
	if ps.DisconnectedAt != action.disconnectedAt {
		t.Fatalf("the handoff's write must not lose the deadline: DisconnectedAt=%d, want %d",
			ps.DisconnectedAt, action.disconnectedAt)
	}
	rev, saved, handoff = e.voidRevs(t)
	if saved < rev || handoff != rev {
		t.Fatalf("the attempt must credit and watermark at the revision it acquired: rev=%d saved=%d handoff=%d",
			rev, saved, handoff)
	}
	e.sess.mu.Lock()
	_, stillParked := e.sess.settledVoidReservations[late]
	e.sess.mu.Unlock()
	if stillParked {
		t.Fatal("the proven verdict must release its reservation")
	}
}

// T6b: a primary write that is AMBIGUOUS plus a late staging — ONE owner, whichever way the
// ambiguity settles.
//
// A nonce staged after the attempt's marshal is not in the attempted bytes, and the resolver
// settles an ambiguity by re-attempting exactly those bytes — so neither arm can carry it, and
// the residual gap belongs to the release handoff in both. That is the point: the late nonce
// has one owner rather than two mechanisms racing to claim it.
func TestMQTTDeferredCloseAmbiguousWriteWithLateStaging(t *testing.T) {
	t.Run("never reached the stream: the re-attempt carries the deadline, the handoff the late nonce", func(t *testing.T) {
		e := testNewDeferredEnv(t, "client-deferred-amb-unlanded", "deferred.amb.unlanded")
		settle, lateNonce := e.armLateSettle(t)

		mqttTestSessStoreSuppressV.Store(true)
		action := testSurvivingAction()
		e.gate(t)
		e.s.applyDeferredDisconnect(e.asm, e.sess, action)
		mqttTestSessStoreSuppressV.Store(false)
		if !e.sess.isSeqUncertain() {
			t.Fatal("the suppressed write must leave the session unresolved")
		}
		// Staged after the attempt, while the session is uncertain: no save is possible
		// until the resolution runs.
		settle()
		late := lateNonce()

		res, err := e.sess.resolveUncertainWrite()
		if res != mqttUncertainOurs {
			t.Fatalf("resolution = %v (err=%v), want ours", res, err)
		}
		// The re-attempt is the ATTEMPT's bytes, so the deadline is durable and the late
		// nonce — staged after their marshal — is not.
		ps := e.loadRecord(t)
		if ps.DisconnectedAt != action.disconnectedAt {
			t.Fatalf("the re-attempt must carry the deadline: DisconnectedAt=%d, want %d",
				ps.DisconnectedAt, action.disconnectedAt)
		}
		if testRecordHasNonce(ps, late) {
			t.Fatalf("the re-attempted bytes never carried the late nonce: %v", ps.CancelledRegs)
		}
		e.release()
		ps = e.loadRecord(t)
		if !testRecordHasNonce(ps, late) {
			t.Fatalf("the release handoff must close the residual gap, got %v", ps.CancelledRegs)
		}
	})

	t.Run("landed: the release handoff closes the gap", func(t *testing.T) {
		e := testNewDeferredEnv(t, "client-deferred-amb-landed", "deferred.amb.landed")
		settle, lateNonce := e.armLateSettle(t)

		stores := 0
		restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if s != e.sess {
				return resp, err
			}
			stores++
			if stores == 1 && err == nil {
				return nil, errors.New("test: the response to a landed record write was lost")
			}
			return resp, err
		})
		defer restore()

		action := testSurvivingAction()
		e.gate(t)
		e.s.applyDeferredDisconnect(e.asm, e.sess, action)
		settle()
		late := lateNonce()

		res, err := e.sess.resolveUncertainWrite()
		if res != mqttUncertainOurs {
			t.Fatalf("resolution = %v (err=%v), want ours", res, err)
		}
		// The resolution credited the marshalled revision only, so the late nonce is
		// still owed and the record does not carry it.
		ps := e.loadRecord(t)
		if testRecordHasNonce(ps, late) {
			t.Fatalf("the proven bytes never carried the late nonce: %v", ps.CancelledRegs)
		}
		e.release()
		ps = e.loadRecord(t)
		if !testRecordHasNonce(ps, late) {
			t.Fatalf("the release handoff must close the residual gap, got %v", ps.CancelledRegs)
		}
	})
}

// ---------------------------------------------------------------------------
// The release handoff
// ---------------------------------------------------------------------------

// testStageVerdict stages a void-register verdict and parks a settled reservation for it, the
// state an in-flight register store leaves behind when it settles with every register-stream
// rung failed. Returns the reservation's identifier.
func (e *testDeferredEnv) testStageVerdict(t *testing.T, nonce string, pi uint16) uint16 {
	t.Helper()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	e.sess.voidRegNonceLocked(nonce)
	if e.sess.pendingPublish == nil {
		e.sess.pendingPublish = make(map[uint16]*mqttPending, 1)
	}
	e.sess.pendingPublish[pi] = &mqttPending{regCancelled: true, regNonce: nonce}
	e.sess.parkSettledVoidReservationLocked(nonce, pi)
	return pi
}

// releaseFence opens and closes a record transaction, which is the trigger every off-readLoop
// stager depends on: it could not persist its own verdict while this fence was held.
func (e *testDeferredEnv) releaseFence(t *testing.T) {
	t.Helper()
	tok := e.sess.beginRecordTx()
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("release the record fence: %v", err)
	}
}

// testVoidHandoffFrames counts how many attemptVoidVerdictHandoff frames are on the stack,
// which is what tells a trampoline apart from recursion: the loop keeps this at 1 however
// many revisions are staged, while re-entering through the attempt's own releases adds a
// frame — and a JetStream round trip — per staging. It is that frame rather than the
// persistence one because the re-entry happens from the attempt's DEFERRED gate release,
// after the persistence call has already returned.
func testVoidHandoffFrames() int {
	pcs := make([]uintptr, 256)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	depth := 0
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, "attemptVoidVerdictHandoff") {
			depth++
		}
		if !more {
			break
		}
	}
	return depth
}

// T6c: verdicts staged across SUCCESSIVE release-triggered attempts all converge, and the
// recursion terminates on the revision rather than on a fixed number of passes.
//
// One late insertion cannot tell a real handoff from a hard-coded second pass, so two
// successive attempts stage more while they are marshalling.
//
// It also pins the TRAMPOLINE: successive attempts run on one frame, so the depth probe stays
// at 1 however many revisions arrive. Recursion here would grow the stack once per late
// staging, each frame holding a JetStream round trip.
//
// Mutation MU7: drop the fence-release trigger and nothing converges.
// Mutation MU23: let the attempt recurse through its own releases and the depth grows.
// (Dropping the watermark conjunct is NOT observable here — every staged revision ends up
// credited, so the loop stops at three either way. The definite-failure test below is what
// catches that one.)
func TestMQTTVoidVerdictHandoffConvergesSuccessiveStaging(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-successive", "handoff.successive")
	attempts, maxDepth := 0, 0
	restoreDepth := setMQTTTestVoidHandoffPreAttemptHook(func(sess *mqttSession) {
		if sess != e.sess {
			return
		}
		if d := testVoidHandoffFrames(); d > maxDepth {
			maxDepth = d
		}
	})
	defer restoreDepth()
	restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s != e.sess {
			return resp, err
		}
		attempts++
		if attempts <= 2 {
			// Staged while THIS attempt was already marshalled, so only a later release can
			// carry it. Exactly what a register store settling on its own goroutine does.
			e.testStageVerdict(t, fmt.Sprintf("nonce-late-%d", attempts), uint16(100+attempts))
		}
		return resp, err
	})
	defer restore()

	e.testStageVerdict(t, "nonce-late-0", 100)
	e.releaseFence(t)

	if attempts != 3 {
		t.Fatalf("one attempt per newly staged revision: got %d, want 3 (two that staged more, one that found nothing new)", attempts)
	}
	if maxDepth != 1 {
		t.Fatalf("successive attempts must run on ONE frame, observed nesting depth %d", maxDepth)
	}
	rev, saved, handoff := e.voidRevs(t)
	if saved != rev || handoff != rev {
		t.Fatalf("every staged revision must be credited and watermarked: rev=%d saved=%d handoff=%d", rev, saved, handoff)
	}
	ps := e.loadRecord(t)
	for i := 0; i <= 2; i++ {
		if !testRecordHasNonce(ps, fmt.Sprintf("nonce-late-%d", i)) {
			t.Fatalf("verdict nonce-late-%d never reached the record: %v", i, ps.CancelledRegs)
		}
	}
	e.sess.mu.Lock()
	parked := len(e.sess.settledVoidReservations)
	var stillHeld []uint16
	for i := 0; i <= 2; i++ {
		if pi := uint16(100 + i); e.sess.pendingPublish[pi] != nil {
			stillHeld = append(stillHeld, pi)
		}
	}
	e.sess.mu.Unlock()
	if parked != 0 || len(stillHeld) != 0 {
		t.Fatalf("proven verdicts must release their reservations: parked=%d still held=%v", parked, stillHeld)
	}
}

// A DEFINITE store failure keeps its posture — the periodic backstop is what converges it,
// not a self-retrigger from the triggered call's own release.
//
// Mutation MU10 (inverse): make the re-trigger unconditional and, with the gap left open by a
// failing store, the attempts run away.
func TestMQTTVoidVerdictHandoffDefiniteFailureDoesNotRetrigger(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-definite", "handoff.definite")
	attempts := 0
	restoreSave := setMQTTTestSessSaveHook(func(s *mqttSession) error {
		if s != e.sess {
			return nil
		}
		attempts++
		return errors.New("test: the session store is unavailable")
	})
	defer restoreSave()

	e.testStageVerdict(t, "nonce-definite", 101)
	e.releaseFence(t)

	if attempts != 1 {
		t.Fatalf("a definite failure must not re-trigger: %d attempt(s)", attempts)
	}
	rev, saved, handoff := e.voidRevs(t)
	if saved >= rev {
		t.Fatalf("nothing landed, so nothing may be credited: rev=%d saved=%d", rev, saved)
	}
	if handoff != rev {
		t.Fatalf("the attempt acquired the fence, so the watermark must stand at it: rev=%d handoff=%d", rev, handoff)
	}
}

// T6d: a trigger that LOSES the fence is not an attempt, so it must not burn the watermark —
// and the fence winner's own release is what retries.
//
// Mutation MU10: advance the watermark at decision time and the nonce strands: the refused
// trigger stored nothing, and no later release considers it again.
func TestMQTTVoidVerdictHandoffWatermarkFollowsTheFence(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-fencerace", "handoff.fencerace")

	stolen := recordTx{}
	stole := false
	restoreHook := setMQTTTestVoidHandoffPreAttemptHook(func(sess *mqttSession) {
		if stole || sess != e.sess {
			return
		}
		// The window the acquired-only watermark exists for: the releaser has unlocked and
		// signalled its waiters, and one of them takes the fence before the triggered
		// attempt can.
		stole = true
		stolen = sess.beginRecordTx()
	})
	defer restoreHook()

	e.testStageVerdict(t, "nonce-fence-race", 102)
	e.releaseFence(t)

	if !stole {
		t.Fatal("the pre-attempt window never opened")
	}
	rev, saved, handoff := e.voidRevs(t)
	if handoff != 0 {
		t.Fatalf("a trigger refused the fence is not an attempt: handoff=%d", handoff)
	}
	if saved >= rev {
		t.Fatalf("nothing was stored: rev=%d saved=%d", rev, saved)
	}

	// The fence winner's release re-checks, and this time the attempt gets the fence.
	if err := e.sess.endRecordTx(stolen); err != nil {
		t.Fatalf("release the stolen fence: %v", err)
	}
	rev, saved, handoff = e.voidRevs(t)
	if saved != rev || handoff != rev {
		t.Fatalf("the fence winner's release must converge it: rev=%d saved=%d handoff=%d", rev, saved, handoff)
	}
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-fence-race") {
		t.Fatalf("the verdict never reached the record: %v", ps.CancelledRegs)
	}
}

// T9: a BOUND session is not the handoff's business — it converges through its own packet
// tails — and firing there could refuse the resume work behind an accepted connection.
//
// Mutation MU13c (the unbound rule removed from BOTH its homes — the shared predicate and the
// gate claim): a release fires an attempt on a bound session and it writes. Removing only one
// home is not observable here either, for the reason T18 states.
func TestMQTTVoidVerdictHandoffSkipsBoundSession(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-bound", "handoff.bound")
	stores, restore := testSessRecordStoreCounter(e.sess)
	defer restore()

	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	e.testStageVerdict(t, "nonce-bound", 103)

	// Both release forms, with a gap wide open.
	if err := e.asm.lockSession(e.sess, e.c); err != nil {
		t.Fatalf("take the ownership gate as the bound client: %v", err)
	}
	e.asm.unlockSession(e.sess)
	e.releaseFence(t)

	if *stores != 0 {
		t.Fatalf("no handoff may fire for a bound session, got %d record write(s)", *stores)
	}
	if _, _, handoff := e.voidRevs(t); handoff != 0 {
		t.Fatalf("no attempt may have run: handoff=%d", handoff)
	}
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()
}

// T9b: the watermark is GENERATION-local. A session object handed back for reuse must not
// inherit the old generation's watermark, or its new verdicts are suppressed until the
// revisions catch up.
//
// Mutation MU14: skip the watermark reset in clear() and the reused object's handoff never
// fires.
func TestMQTTVoidVerdictHandoffAfterGenerationReuse(t *testing.T) {
	t.Parallel()
	e := testNewDeferredEnv(t, "client-handoff-reuse", "handoff.reuse")

	// Burn the watermark in the OLD generation.
	e.testStageVerdict(t, "nonce-old-gen", 104)
	e.releaseFence(t)
	if _, _, handoff := e.voidRevs(t); handoff == 0 {
		t.Fatal("the old generation must leave a watermark behind for this test to mean anything")
	}

	// clear() ends the generation and hands the object back to be reused as a fresh session.
	if err := e.sess.clear(false); err != nil {
		t.Fatalf("clear the session: %v", err)
	}
	if rev, saved, handoff := e.voidRevs(t); rev != 0 || saved != 0 || handoff != 0 {
		t.Fatalf("a discarded generation leaves no verdict accounting behind: rev=%d saved=%d handoff=%d", rev, saved, handoff)
	}
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the reused session: %v", err)
	}

	e.testStageVerdict(t, "nonce-new-gen", 105)
	e.releaseFence(t)
	rev, saved, handoff := e.voidRevs(t)
	if saved != rev || handoff != rev {
		t.Fatalf("the new generation's verdict must converge: rev=%d saved=%d handoff=%d", rev, saved, handoff)
	}
	ps := e.loadRecord(t)
	if !testRecordHasNonce(ps, "nonce-new-gen") || testRecordHasNonce(ps, "nonce-old-gen") {
		t.Fatalf("the new generation carries its own verdicts only: %v", ps.CancelledRegs)
	}
}

// T12: the release-triggered attempt runs UNDER the ownership gate, and skips on contention.
//
// Ungated, an attempt whose store went ambiguous just after a CONNECT passed its single
// uncertainty check would let that CONNECT bind, have its accept save refused, and evict the
// very session object holding the continuation. Gated, uncertainty from an attempt can only
// exist while the gate is held, so a CONNECT that ever holds the gate sees it and resolves it.
//
// Mutation MU17: run the attempt ungated and it writes — and can go ambiguous — while the gate
// is held.
func TestMQTTVoidVerdictHandoffYieldsToTheOwnershipGate(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-gated", "handoff.gated")
	stores, restore := testSessRecordStoreCounter(e.sess)
	defer restore()

	// A CONNECT (or a resolver, or an expiry) holds the gate.
	e.gate(t)
	e.testStageVerdict(t, "nonce-gated", 106)
	e.releaseFence(t)

	if *stores != 0 {
		t.Fatalf("an attempt must not write while the ownership gate is held, got %d write(s)", *stores)
	}
	if e.sess.isSeqUncertain() {
		t.Fatal("an ungated attempt could inject uncertainty a gate holder has already passed its check on")
	}
	if _, _, handoff := e.voidRevs(t); handoff != 0 {
		t.Fatalf("a skipped attempt must not advance the watermark: handoff=%d", handoff)
	}
	e.sess.mu.Lock()
	active := e.sess.voidHandoffActive
	e.sess.mu.Unlock()
	if active {
		t.Fatal("a refused attempt must hand its loop ownership back, or the gate release suppresses itself")
	}

	// The gate holder's own release owns the re-check.
	e.release()
	if *stores != 1 {
		t.Fatalf("the gate release must converge it, got %d write(s)", *stores)
	}
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-gated") {
		t.Fatalf("the verdict never reached the record: %v", ps.CancelledRegs)
	}
}

// T14: the shared release helper, which BOTH gate-release forms call — unlockSession and
// CONNECT's own inline delete.
//
// The behavioral scenario at the inline site is unreachable today: an aborted CONNECT keeps
// its binding for the close bookkeeping to detach, and the paths that DO unbind mid-CONNECT
// (the accept rollback, a foreign disposition) evict the object as well. So the inline call is
// defensive wiring, and what is testable is the helper's own contract: it triggers for a
// resident unbound session with a gap, and skips an object no map points at.
func TestMQTTVoidVerdictHandoffHelperSkipsNonresident(t *testing.T) {
	t.Parallel()
	e := testNewDeferredEnv(t, "client-handoff-resident", "handoff.resident")

	e.testStageVerdict(t, "nonce-resident", 107)
	e.asm.handoffVoidVerdictsOnGateRelease(e.sess)
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-resident") {
		t.Fatalf("a resident unbound session with a gap must converge: %v", ps.CancelledRegs)
	}

	// Evicted: this object can no longer speak for the record, so nothing here may write it.
	e.asm.removeSession(e.sess, true)
	e.testStageVerdict(t, "nonce-nonresident", 108)
	e.asm.handoffVoidVerdictsOnGateRelease(e.sess)
	if ps := e.loadRecord(t); testRecordHasNonce(ps, "nonce-nonresident") {
		t.Fatalf("a nonresident object must be skipped: %v", ps.CancelledRegs)
	}
}

// ---------------------------------------------------------------------------
// The resolver's outer re-checks
// ---------------------------------------------------------------------------

// testParkDeferredAfterUnlandedWrite leaves the session unresolved by a write that did NOT
// land, with a surviving close parked — the state CONNECT and the retry pass find.
func (e *testDeferredEnv) testParkDeferredAfterUnlandedWrite(t *testing.T) *mqttDeferredDisconnect {
	t.Helper()
	mqttTestSessStoreSuppressV.Store(true)
	tok := e.sess.beginRecordTx()
	if err := e.sess.save(tok); err == nil {
		t.Fatal("the suppressed write must fail")
	}
	if err := e.sess.endRecordTx(tok); err != nil {
		t.Fatalf("release the fence: %v", err)
	}
	mqttTestSessStoreSuppressV.Store(false)
	if !e.sess.isSeqUncertain() {
		t.Fatal("an ambiguous write must leave the session unresolved")
	}
	action := testSurvivingAction()
	if !e.sess.deferDisconnect(action) {
		t.Fatal("the close must park its obligations")
	}
	return action
}

// testAmbiguousLandedOnce makes the NEXT record write of this session land with its response
// lost, once.
func testAmbiguousLandedOnce(sess *mqttSession) func() {
	return testAmbiguousLandedOnNth(sess, 1)
}

// testAmbiguousLandedOnNth is testAmbiguousLandedOnce aimed at a LATER write, for a fixture
// whose earlier writes belong to another step — a resolver's re-attempt, say, which must be
// allowed to settle before the write under test goes ambiguous.
func testAmbiguousLandedOnNth(sess *mqttSession, nth int) func() {
	n := 0
	return setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s != sess || err != nil {
			return resp, err
		}
		if n++; n == nth {
			return nil, errors.New("test: the response to a landed record write was lost")
		}
		return resp, err
	})
}

// T7: when the DISCHARGE re-arms uncertainty, the resolver CONNECT calls must report
// UNRESOLVED — not "ours".
//
// The discharge's classified save happens outside resolveUncertainWriteInTx, so the inner
// re-check cannot see it. Reported as ours, CONNECT would bind, find its accept save refused
// and evict the only object holding the continuation: the deferred deadline and the purge's
// verdicts, lost with no crash anywhere.
//
// Mutation MU8: return the original verdict after a discharge that re-armed uncertainty.
func TestMQTTDeferredDischargeReArmReportsUnresolved(t *testing.T) {
	e := testNewDeferredEnv(t, "client-discharge-rearm", "discharge.rearm")
	e.failRegisterStream(t)
	action := e.testParkDeferredAfterUnlandedWrite(t)
	// The SECOND write, because the first is the resolver's own re-attempt: it settles
	// the parked ambiguity, which is what authorizes the discharge whose write is the
	// one under test here.
	restore := testAmbiguousLandedOnNth(e.sess, 2)
	defer restore()

	e.gate(t)
	res := e.s.resolveUncertainSession(e.asm, e.sess)
	if res != mqttUncertainUnresolved {
		t.Fatalf("resolution = %v, want unresolved: the discharge's own write is outstanding", res)
	}
	if !e.sess.isSeqUncertain() {
		t.Fatal("the discharge's ambiguity must stand")
	}
	e.asm.mu.RLock()
	resident := e.asm.sessByHash[e.sess.idHash] == e.sess
	e.asm.mu.RUnlock()
	if !resident {
		t.Fatal("the session must stay resident: it holds the outstanding attempt")
	}
	e.sess.mu.Lock()
	pw, parked := e.sess.pendingWrite, e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if pw == nil {
		t.Fatal("the discharge's own write must have left an attempt identity")
	}
	if parked != nil {
		t.Fatal("the action was consumed by the discharge; its deadline rides the attempted bytes now")
	}

	// A later resolution finds those exact bytes on the stream and settles it.
	if res = e.s.resolveUncertainSession(e.asm, e.sess); res != mqttUncertainOurs {
		t.Fatalf("second resolution = %v, want ours", res)
	}
	e.release()
	ps := e.loadRecord(t)
	if ps.DisconnectedAt != action.disconnectedAt || !testRecordHasNonce(ps, e.nonce) {
		t.Fatalf("the settled record must carry deadline and verdict: DisconnectedAt=%d verdicts=%v",
			ps.DisconnectedAt, ps.CancelledRegs)
	}
}

// T8: resolveUncertainNow owes the same answer, and only a direct test discriminates it —
// its production callers ignore the return value.
//
// Mutation MU9: drop only this site's re-check. T7 must still pass, which is what makes the
// two sites independently pinned.
func TestMQTTResolveUncertainNowReportsDischargeReArm(t *testing.T) {
	e := testNewDeferredEnv(t, "client-resolvenow-rearm", "resolvenow.rearm")
	e.failRegisterStream(t)
	action := e.testParkDeferredAfterUnlandedWrite(t)
	// The second write: the first is the resolver's re-attempt, which settles the parked
	// ambiguity and authorizes the discharge under test.
	restore := testAmbiguousLandedOnNth(e.sess, 2)
	defer restore()

	// Ungated: resolveUncertainNow takes the ownership gate itself, which is what makes the
	// resolution and the discharge one step.
	if res := e.sess.resolveUncertainNow(e.c); res != mqttUncertainUnresolved {
		t.Fatalf("resolveUncertainNow = %v, want unresolved", res)
	}
	if !e.sess.isSeqUncertain() {
		t.Fatal("the discharge's ambiguity must stand")
	}
	e.sess.mu.Lock()
	pw := e.sess.pendingWrite
	e.sess.mu.Unlock()
	if pw == nil {
		t.Fatal("the discharge's outstanding attempt must be intact")
	}
	if res := e.sess.resolveUncertainNow(e.c); res != mqttUncertainOurs {
		t.Fatalf("second resolveUncertainNow = %v, want ours", res)
	}
	ps := e.loadRecord(t)
	if ps.DisconnectedAt != action.disconnectedAt || !testRecordHasNonce(ps, e.nonce) {
		t.Fatalf("the settled record must carry deadline and verdict: DisconnectedAt=%d verdicts=%v",
			ps.DisconnectedAt, ps.CancelledRegs)
	}
}

// ---------------------------------------------------------------------------
// Every landed write credits the verdicts it marshalled
// ---------------------------------------------------------------------------

// T10: an ORDINARY save credits the verdicts it inherited, which is what closes the rebind
// race: a reconnect's accept save carries a nonce staged before it and is not a verdict path
// at all, so a mechanism that only credited at the paths trying to persist a verdict would
// leave that reservation parked with the fact already durable.
//
// The session is BOUND here, so no release handoff can be what converges it. What this test
// pins is the ORDINARY save's credit, which is the mechanism a reconnect's accept save uses;
// the CONNECT-specific choreography — a real reconnect whose accept save is the first landed
// write after a close that could persist nothing — is pinned end to end by
// TestMQTTInflightReconnectAcceptSaveCreditsInheritedVerdict in the ported suite.
//
// Mutation MU15: drop the marshal-rev credit in saveWithDetail's success arm.
func TestMQTTLandedSaveCreditsInheritedVerdicts(t *testing.T) {
	t.Parallel()
	e := testNewDeferredEnv(t, "client-credit-inherited", "credit.inherited")
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	pi := e.testStageVerdict(t, "nonce-inherited", 110)
	rev, saved, _ := e.voidRevs(t)
	if saved >= rev {
		t.Fatalf("the verdict must start uncredited: rev=%d saved=%d", rev, saved)
	}

	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("the inheriting save: %v", err)
	}

	rev, saved, _ = e.voidRevs(t)
	if saved < rev {
		t.Fatalf("a landed save must credit what it marshalled: rev=%d saved=%d", rev, saved)
	}
	e.sess.mu.Lock()
	_, parked := e.sess.settledVoidReservations["nonce-inherited"]
	_, held := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if parked || held {
		t.Fatalf("the credited verdict must release its reservation: parked=%v held=%v", parked, held)
	}
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-inherited") {
		t.Fatalf("the save carried it: %v", ps.CancelledRegs)
	}
	e.sess.mu.Lock()
	e.sess.c = nil
	e.sess.mu.Unlock()
}

// testContendedAttempt builds the identity of a record write that did NOT land, for driving
// the contention classifier directly. The classifier's real callers reach it on a DEFINITE
// 10164 rejection, where save() deliberately records no uncertainty — so the suppressed
// attempt's uncertainty is cleared at the base sequence it CAS'd against.
func (e *testDeferredEnv) testContendedAttempt(t *testing.T) mqttSessSaveDetail {
	t.Helper()
	mqttTestSessStoreSuppressV.Store(true)
	tok := e.sess.beginRecordTx()
	d, err := e.sess.saveWithDetail(tok)
	if err == nil {
		t.Fatal("the suppressed write must fail")
	}
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("release the fence: %v", terr)
	}
	mqttTestSessStoreSuppressV.Store(false)
	// Back to the pre-attempt state: the base sequence AND the lineage the session was
	// acknowledged for, which a failed save never moved. The grant pair is captured and put
	// back verbatim rather than handed to the rotation, for the same reason: a failed save
	// moves neither body, and rotating the current one into itself would make the session its
	// own parent and leave the next fork with an empty delta.
	e.sess.mu.Lock()
	ackWID, ackPWID := e.sess.wid, e.sess.pwid
	body, bodyParent := e.sess.grantBody, e.sess.grantBodyParent
	e.sess.mu.Unlock()
	e.sess.clearUncertaintyLocked(d.baseSeq, ackWID, ackPWID, body)
	e.sess.mu.Lock()
	e.sess.grantBody, e.sess.grantBodyParent = body, bodyParent
	e.sess.mu.Unlock()
	if d.voidedRegsRev == 0 {
		t.Fatal("the attempt must carry the revision it marshalled")
	}
	return d
}

// T11: the contention classifier's landed verdicts credit too.
//
// resolveContendedRecordSave proves a 10164-rejected write durable without passing through
// saveWithDetail's success arm or the generic resolver, in three distinct ways. Left
// uncredited there, a quiet bound connection could park the reservation indefinitely.
//
// Mutation MU16: drop the mqttRepairSaveLanded credit and all three variants leave the
// reservation parked.
func TestMQTTContendedSaveCreditsMarshalledVerdicts(t *testing.T) {
	// The classifier's callers run inside CONNECT, which holds the ownership gate; gating
	// here is also what keeps the release handoff from converging the gap behind the test.
	setup := func(t *testing.T, name string) (*testDeferredEnv, mqttSessSaveDetail, uint16) {
		e := testNewDeferredEnv(t, "client-contended-"+name, "contended."+name)
		e.gate(t)
		t.Cleanup(e.release)
		pi := e.testStageVerdict(t, "nonce-contended-"+name, 111)
		return e, e.testContendedAttempt(t), pi
	}
	assertCredited := func(t *testing.T, e *testDeferredEnv, name string, pi uint16, d mqttSessSaveDetail) {
		t.Helper()
		rev, saved, _ := e.voidRevs(t)
		if saved < d.voidedRegsRev || saved > rev {
			t.Fatalf("the landed classification must credit the marshalled revision: marshalled=%d saved=%d rev=%d",
				d.voidedRegsRev, saved, rev)
		}
		e.sess.mu.Lock()
		_, parked := e.sess.settledVoidReservations["nonce-contended-"+name]
		_, held := e.sess.pendingPublish[pi]
		e.sess.mu.Unlock()
		if parked || held {
			t.Fatalf("the credited verdict must release its reservation: parked=%v held=%v", parked, held)
		}
	}

	t.Run("the exact-byte retry lands", func(t *testing.T) {
		e, d, pi := setup(t, "retry")
		if v := e.sess.resolveContendedRecordSave(&d); v != mqttRepairSaveLanded {
			t.Fatalf("verdict = %v, want landed: the record is still at the base sequence", v)
		}
		assertCredited(t, e, "retry", pi, d)
	})

	t.Run("the in-loop digest match", func(t *testing.T) {
		e, d, pi := setup(t, "digest")
		// Byte-identical at a newer sequence: the rejected write had landed all along.
		if _, err := e.asm.jsa.storeSessionMsg(d.domainTk, d.cidHash, d.hdr, d.framed); err != nil {
			t.Fatalf("land the contended bytes: %v", err)
		}
		if v := e.sess.resolveContendedRecordSave(&d); v != mqttRepairSaveLanded {
			t.Fatalf("verdict = %v, want landed", v)
		}
		assertCredited(t, e, "digest", pi, d)
	})

	t.Run("the post-exhaustion digest match", func(t *testing.T) {
		e, d, pi := setup(t, "exhausted")
		// Every retry is rejected as contention, so the loop runs out; the last one lands
		// its bytes, which the final re-read classifies.
		retries := 0
		mqttTestSessStoreSuppressV.Store(true)
		restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if s != e.sess {
				return resp, err
			}
			if retries++; retries == mqttRepairSaveAttempts {
				mqttTestSessStoreSuppressV.Store(false)
				if _, serr := e.asm.jsa.storeSessionMsg(d.domainTk, d.cidHash, d.hdr, d.framed); serr != nil {
					t.Errorf("land the contended bytes on the last attempt: %v", serr)
				}
			}
			return nil, ApiErrors[JSStreamWrongLastSequenceConstantErr]
		})
		defer func() {
			restore()
			mqttTestSessStoreSuppressV.Store(false)
		}()

		if v := e.sess.resolveContendedRecordSave(&d); v != mqttRepairSaveLanded {
			t.Fatalf("verdict = %v, want landed after exhaustion (%d retries)", v, retries)
		}
		if retries != mqttRepairSaveAttempts {
			t.Fatalf("the loop must have been exhausted: %d retries", retries)
		}
		assertCredited(t, e, "exhausted", pi, d)
	})

	t.Run("an unresolved attempt hands the revision to the readback", func(t *testing.T) {
		e, d, pi := setup(t, "unresolved")
		restoreLoad := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
			if stream == mqttSessStreamName {
				return errors.New("test: the session record is unreadable")
			}
			return nil
		})
		if v := e.sess.resolveContendedRecordSave(&d); v != mqttRepairSaveUnresolved {
			restoreLoad()
			t.Fatalf("verdict = %v, want unresolved: the re-read is unavailable", v)
		}
		restoreLoad()

		e.sess.mu.Lock()
		pw := e.sess.pendingWrite
		e.sess.mu.Unlock()
		if pw == nil || pw.voidedRegsRev != d.voidedRegsRev {
			t.Fatalf("the deferred readback must carry the marshalled revision: pendingWrite=%+v want rev %d",
				pw, d.voidedRegsRev)
		}
		// The write had in fact landed, so the readback credits what those bytes carried.
		if _, err := e.asm.jsa.storeSessionMsg(d.domainTk, d.cidHash, d.hdr, d.framed); err != nil {
			t.Fatalf("land the contended bytes: %v", err)
		}
		if res, err := e.sess.resolveUncertainWrite(); res != mqttUncertainOurs {
			t.Fatalf("resolution = %v (err=%v), want ours", res, err)
		}
		assertCredited(t, e, "unresolved", pi, d)
	})
}

// T15: a discharge that finds the record state UNRESOLVED again re-parks the action instead
// of applying a close whose every write is refused.
//
// The action is consumed BEFORE this function runs, and holding the ownership gate does not
// exclude an off-readLoop writer: a register store settling between the resolution and the
// fence acquisition takes the fence with tryBeginRecordTx and can leave its own write
// ambiguous. Applied anyway, the branch's save is refused at entry, classified definitely
// failed, and registers no continuation — so if the competing write is later settled as OURS
// (its own bytes carry no disconnect time) and the purge's register-stream rungs succeeded
// (no revision gap for the release handoff), nothing ever re-saves the disconnect time. A
// restart then reads a connected-looking record and drops the expiry: the exact failure this
// item exists to close.
//
// Mutation MU19: drop the unresolved guard (both checks) and the deadline never reaches the
// record.
func TestMQTTDeferredDischargeReparksWhenUnresolvedAgain(t *testing.T) {
	e := testNewDeferredEnv(t, "client-discharge-repark", "discharge.repark")
	action := testSurvivingAction()
	e.gate(t)

	// The competing writer: it took the fence after the resolution released it, and its own
	// write is ambiguous. Its bytes LAND, so a later resolution classifies it ours and
	// nothing it carries mentions the disconnect time.
	restore := testAmbiguousLandedOnce(e.sess)
	tok := e.sess.beginRecordTx()
	if err := e.sess.save(tok); err == nil {
		t.Fatal("the competing write must be reported as failed")
	}
	if terr := e.sess.endRecordTx(tok); terr != nil {
		t.Fatalf("release the competing writer's fence: %v", terr)
	}
	restore()
	if !e.sess.isSeqUncertain() {
		t.Fatal("the competing write must leave the session unresolved")
	}

	e.s.applyDeferredDisconnect(e.asm, e.sess, action)

	// Nothing was applied and the action is back, with its original deadlines.
	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect
	disconnectedAt := e.sess.disconnectedAt
	_, held := e.sess.pendingPublish[e.pi]
	_, bucket := e.sess.cpending[e.dur]
	e.sess.mu.Unlock()
	if parked != action {
		t.Fatalf("the consumed action must be re-parked untouched, got %+v", parked)
	}
	if disconnectedAt != 0 {
		t.Fatalf("no mutation may have happened: disconnectedAt=%d", disconnectedAt)
	}
	if !held || !bucket {
		t.Fatalf("the purge must not have run: held=%v bucket=%v", held, bucket)
	}
	e.asm.mu.Lock()
	_, armed := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if armed {
		t.Fatal("no timer may be armed for a close that was not applied")
	}

	// The resolution that follows discharges it for real, and the deadline and the purge
	// land together.
	if res := e.s.resolveUncertainSession(e.asm, e.sess); res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours: the competing bytes are on the stream", res)
	}
	e.release()
	e.sess.mu.Lock()
	parked, disconnectedAt = e.sess.deferredDisconnect, e.sess.disconnectedAt
	_, held = e.sess.pendingPublish[e.pi]
	e.sess.mu.Unlock()
	if parked != nil || disconnectedAt != action.disconnectedAt || held {
		t.Fatalf("the re-discharge must apply the close: parked=%+v disconnectedAt=%d held=%v",
			parked, disconnectedAt, held)
	}
	if ps := e.loadRecord(t); ps.DisconnectedAt != action.disconnectedAt {
		t.Fatalf("the record must carry the earned deadline: DisconnectedAt=%d, want %d",
			ps.DisconnectedAt, action.disconnectedAt)
	}
}

// T16: the DISCARD branch of a deferred close, when the record state becomes unresolved
// AFTER the entry check.
//
// `clear()` cannot run under this branch's fence — it ends the record generation — so the
// discard's safety rests on the in-fence refusal `clear` performs, and on this branch
// putting the consumed action (its Will included) back when the sentinel comes out. Without the in-fence check clear would delete the record and the
// durables on a sequence nobody has classified; without the re-park the discard obligation
// and the Will would be gone with the object.
//
// The interleaving is POSITIVE, not timed: the pre-fence seam fires exactly in the window
// the check exists for — after the entry check, before the fence — so the ambiguity is
// armed there, on this goroutine, with no sleep and no second thread. A test that slept
// instead could pass against the broken code by arming the ambiguity early enough for the
// discharge's own entry check to catch it.
//
// Mutation MU20: drop the in-fence re-check and the record is deleted anyway.
// Mutation MU21: drop the branch's re-park recovery and the action is lost.
func TestMQTTDeferredDiscardReparksWhenUnresolvedUnderTheFence(t *testing.T) {
	e := testNewDeferredEnv(t, "client-discard-repark", "discard.repark")
	// A discard: the session ended at connection close, and it owes a Will.
	// The Will is DUE (no delay), so the eventual discharge publishes it through the
	// outbox's client-free publisher rather than arming a timer this minimal test Server
	// does not wire up. What matters here is that a re-park preserves it, which the action
	// IDENTITY below proves: an action rebuilt rather than put back could lose it.
	action := &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Unix() - 600,
		will:           &mqttWill{topic: []byte("will/topic"), message: []byte("bye"), qos: 0},
		willDeadline:   time.Now().Add(-time.Minute).UnixNano(),
	}
	if !action.endsAtDisconnect() {
		t.Fatal("the fixture must be a discard")
	}
	e.gate(t)

	armed := false
	restoreHook := setMQTTTestClearWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttClearWindowPreFence || armed {
			return
		}
		armed = true
		// An off-readLoop register store settled here and its save went ambiguous: after
		// the discharge and clear() both passed their entry checks, and before the fence
		// is taken. Only a check made under that fence can still see it.
		sess.mu.Lock()
		testMarkUncertainLocked(sess, sess.seq, testAmbiguousBodyLocked(sess, "ambiguous"))
		sess.mu.Unlock()
	})
	defer restoreHook()

	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	if !armed {
		t.Fatal("the pre-fence window never opened, so the race was not exercised")
	}

	// Nothing was discarded, and the obligation is back — the same action, with its Will.
	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if parked != action {
		t.Fatalf("the consumed discard action must be re-parked untouched, got %+v", parked)
	}
	if parked != nil && parked.will == nil {
		t.Fatal("the re-parked action must still own its Will")
	}
	e.asm.mu.RLock()
	resident := e.asm.sessByHash[e.sess.idHash] == e.sess
	e.asm.mu.RUnlock()
	if !resident {
		t.Fatal("the session must stay resident: it holds the parked action")
	}
	if ps := e.loadRecord(t); ps == nil {
		t.Fatal("clear() must have refused before deleting the record")
	}
	if _, err := e.asm.jsa.lookupConsumer(mqttStreamName, e.dur); err != nil {
		t.Fatalf("a refused discard must delete no durable: %v", err)
	}

	// The next resolution discharges it for real: the record goes and the session with it.
	if res := e.s.resolveUncertainSession(e.asm, e.sess); res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours: the competing write did not land", res)
	}
	e.release()
	e.sess.mu.Lock()
	stillParked := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if stillParked != nil {
		t.Fatalf("the re-discharge must consume the action, got %+v", stillParked)
	}
	ps, _, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil {
		t.Fatalf("load the record after the discard: %v", err)
	}
	if ps != nil {
		t.Fatalf("the discharged discard must delete the record, got %+v", ps)
	}
	e.asm.mu.RLock()
	stillResident := e.asm.sessByHash[e.sess.idHash] == e.sess
	e.asm.mu.RUnlock()
	if stillResident {
		t.Fatal("the discharged discard must remove the session")
	}
}

// T15b: the window BETWEEN the discharge's own uncertainty check and clear()'s entry check.
//
// The two checks are not under one fence, so an off-readLoop register store can go ambiguous
// in between — and that arrives at clear()'s ENTRY refusal, not its in-fence one. It is
// exactly as recoverable (nothing has been deleted), so it must carry the same sentinel: a
// plain error there would have the branch read it as a partly-done teardown, remove the
// session, and lose the consumed deadline and Will with nothing resident to retry.
//
// Mutation MU22: return a plain error from clear()'s entry refusal and the action is dropped.
func TestMQTTDeferredDiscardReparksWhenUnresolvedAtClearEntry(t *testing.T) {
	e := testNewDeferredEnv(t, "client-discard-entry", "discard.entry")
	action := &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Unix() - 600,
		will:           &mqttWill{topic: []byte("will/topic"), message: []byte("bye"), qos: 0},
		willDeadline:   time.Now().Add(-time.Minute).UnixNano(),
	}
	if !action.endsAtDisconnect() {
		t.Fatal("the fixture must be a discard")
	}
	e.gate(t)

	armed := false
	restoreHook := setMQTTTestClearWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttClearWindowPreEntry || armed {
			return
		}
		armed = true
		// The competing writer lands here: after the discharge's own check, before
		// clear()'s. This is the window the entry refusal — and its sentinel — owns.
		sess.mu.Lock()
		testMarkUncertainLocked(sess, sess.seq, testAmbiguousBodyLocked(sess, "ambiguous"))
		sess.mu.Unlock()
	})
	defer restoreHook()

	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	if !armed {
		t.Fatal("the pre-entry window never opened, so the race was not exercised")
	}

	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if parked != action {
		t.Fatalf("the consumed discard action must be re-parked untouched, got %+v", parked)
	}
	if parked.will == nil {
		t.Fatal("the re-parked action must still own its Will")
	}
	e.asm.mu.RLock()
	resident := e.asm.sessByHash[e.sess.idHash] == e.sess
	e.asm.mu.RUnlock()
	if !resident {
		t.Fatal("the session must stay resident: it holds the parked action")
	}
	if ps := e.loadRecord(t); ps == nil {
		t.Fatal("nothing may have been deleted")
	}

	// And it is discharged exactly once afterwards.
	if res := e.s.resolveUncertainSession(e.asm, e.sess); res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours", res)
	}
	e.release()
	e.sess.mu.Lock()
	stillParked := e.sess.deferredDisconnect
	e.sess.mu.Unlock()
	if stillParked != nil {
		t.Fatalf("the re-discharge must consume the action exactly once, got %+v", stillParked)
	}
	ps, _, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil {
		t.Fatalf("load the record after the discard: %v", err)
	}
	if ps != nil {
		t.Fatalf("the discharged discard must delete the record, got %+v", ps)
	}
}

// T12b: a gate owner releasing WHILE a handoff attempt is being refused must not suppress
// itself against a loop that is already leaving.
//
// This is the lost wakeup the ownership hand-back closes: the owner deletes its gate entry
// under asm.mu and only then asks whether a loop is running, so a hand-back made after that
// lock was released could be seen as still running — the owner suppresses, the loop exits, and
// the staged revision has no successor at all.
//
// Driven DETERMINISTICALLY through the contention seam, which fires exactly where a two-step
// hand-back would still be holding the flag: the gate release runs from there, so in the fixed
// form it observes a flag already given back inside the gate's own critical section and
// converges the verdict, while a hand-back moved outside that section is observed as a running
// loop and suppresses itself.
//
// Mutation MU24: move the hand-back outside the gate's critical section — the release
// suppresses itself and the verdict strands.
// Mutation MU24b: never hand the ownership back at all — same outcome, one round.
func TestMQTTVoidVerdictHandoffContentionHandsBackOwnership(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-contend", "handoff.contend")

	// A CONNECT, a resolver or an expiry holds the gate, which is what refuses the attempt.
	if err := e.asm.lockSession(e.sess, nil); err != nil {
		t.Fatalf("take the ownership gate: %v", err)
	}
	released := false
	restoreHook := setMQTTTestVoidHandoffWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttHandoffWindowGateRefused || released {
			return
		}
		released = true
		// The owner releases HERE: after the attempt was refused, before a two-step
		// hand-back would have run. Its own re-check must therefore see the ownership
		// already given back.
		e.asm.unlockSession(e.sess)
	})
	defer restoreHook()

	e.testStageVerdict(t, "nonce-contended", 200)
	e.releaseFence(t)

	if !released {
		t.Fatal("the contention window never opened, so the race was not exercised")
	}
	rev, saved, _ := e.voidRevs(t)
	if saved < rev {
		t.Fatalf("the verdict was left with no successor: rev=%d saved=%d", rev, saved)
	}
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-contended") {
		t.Fatalf("the gate release must converge it: %v", ps.CancelledRegs)
	}
	e.sess.mu.Lock()
	active := e.sess.voidHandoffActive
	e.sess.mu.Unlock()
	if active {
		t.Fatal("no loop may still claim ownership")
	}
}

// T17: the periodic BACKSTOP converges a verdict whose handoff store failed definitely.
//
// One attempt per newly staged revision is what keeps the handoff from retrying in a storm,
// and the same watermark means a definite store failure ends the sequence: no release
// triggers again. An unbound, never-expiring session has no traffic and no timer, so without
// a backstop the verdict is memory-only until something happens to stage another nonce —
// possibly never, and a crash then restores the register as an ordinary live delivery.
//
// Nothing is called by hand except the periodic pass itself, which is production's own
// convergence point of last resort.
//
// Mutation MU25: drop the backstop call from the periodic pass and the verdict never
// reaches the record.
// Mutation MU25b: leave the watermark where the failed attempt left it and the re-armed
// attempt is suppressed.
// Mutation MU26: drop the residency check from the handoff's gate acquisition and the
// evicted-snapshot arm below writes for a session the manager no longer maps.
func TestMQTTVoidVerdictBackstopConvergesAfterDefiniteFailure(t *testing.T) {
	e := testNewDeferredEnv(t, "client-backstop", "backstop.definite")
	// Never-expiring and unbound: the class with neither traffic nor a timer.
	e.sess.mu.Lock()
	e.sess.expiryInterval = mqttSessionNeverExpire
	e.sess.mu.Unlock()

	restoreSave := setMQTTTestSessSaveHook(func(s *mqttSession) error {
		if s != e.sess {
			return nil
		}
		return errors.New("test: the session store is unavailable")
	})
	e.testStageVerdict(t, "nonce-backstop", 210)
	e.releaseFence(t)

	rev, saved, handoff := e.voidRevs(t)
	if saved >= rev {
		t.Fatalf("the store failed, so nothing may be credited: rev=%d saved=%d", rev, saved)
	}
	if handoff != rev {
		t.Fatalf("the attempt acquired the fence, so the watermark stands at it: rev=%d handoff=%d", rev, handoff)
	}
	// Proof that the release handoff alone is finished with it: another release changes
	// nothing.
	e.releaseFence(t)
	if ps := e.loadRecord(t); testRecordHasNonce(ps, "nonce-backstop") {
		t.Fatalf("no release may have persisted it while the store was failing: %v", ps.CancelledRegs)
	}

	// The store recovers, and the periodic pass is what notices.
	restoreSave()
	e.asm.reconcileResidentRecordState(e.s)

	rev, saved, handoff = e.voidRevs(t)
	if saved < rev {
		t.Fatalf("the backstop must converge it: rev=%d saved=%d", rev, saved)
	}
	if handoff != rev {
		t.Fatalf("the backstop's attempt must watermark at the revision it rode: rev=%d handoff=%d", rev, handoff)
	}
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-backstop") {
		t.Fatalf("the verdict never reached the record: %v", ps.CancelledRegs)
	}
	e.sess.mu.Lock()
	_, parked := e.sess.settledVoidReservations["nonce-backstop"]
	e.sess.mu.Unlock()
	if parked {
		t.Fatal("the proven verdict must release its reservation")
	}

	// And an EVICTED snapshot is not the backstop's business. The periodic pass holds raw
	// pointers, so a session the manager no longer maps to this object — a remote persist, an
	// accept rollback, an expiry, a foreign disposition — must not have its gate claimed for
	// a JetStream store: that object can no longer speak for the record, and holding the
	// shared client-ID gate would keep a replacement CONNECT out meanwhile. The per-session STEP is
	// what must refuse, because the eviction window is between the pass's snapshot and this
	// session's turn in it — by which time the map no longer lists it at all.
	stores, restoreCount := testSessRecordStoreCounter(e.sess)
	defer restoreCount()
	e.testStageVerdict(t, "nonce-evicted", 211)
	e.asm.removeSession(e.sess, true)
	e.asm.flushVoidVerdictGap(e.sess)
	if *stores != 0 {
		t.Fatalf("the backstop must not write for an evicted session, %d store(s)", *stores)
	}
	e.asm.mu.RLock()
	_, gated := e.asm.sessLocked[e.sess.id]
	e.asm.mu.RUnlock()
	if gated {
		t.Fatal("the backstop must not leave the ownership gate claimed for an evicted session")
	}
	if ps := e.loadRecord(t); testRecordHasNonce(ps, "nonce-evicted") {
		t.Fatalf("an evicted object's verdict is not ours to persist: %v", ps.CancelledRegs)
	}
}

// T17c: the release handoff DEFERS to a blocking writer queued for the fence, and the
// deferral is a handoff rather than a strand.
//
// The record fence prefers an announced blocking writer over every non-blocking acquisition
// (see sess_recordtx.go), and this attempt is one: it opens the fence inline at a lock
// release. Taking it while a waiter is announced would be exactly the barge waiter priority
// exists to refuse — and would hold the ownership gate through a JetStream store while the
// waiter, which usually holds the client-ID gate, waits behind it.
//
// Every "an attempt fired" statement here is made through the PRE-GATE window counter, never
// through the handoff watermark: an attempt that bails at its in-fence pending recheck leaves
// the watermark untouched too, so the watermark cannot tell a suppressed trigger from a
// deferred one.
//
// Mutation M2: drop the waiter conjunct from voidVerdictHandoffPendingLocked — the predicate
// half fails outright, and the release half sees the holder's own release start an attempt
// while the waiter is parked on the fence.
// Mutation M5: add the waiter conjunct to flushVoidVerdictGap's `owed` — the backstop half's
// re-arm never happens, so a definitely-failed verdict waits a whole further sweep interval
// with the crash window open.
func TestMQTTVoidVerdictHandoffDefersToQueuedWaiter(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-waiter", "handoff.waiter")

	var attempts atomic.Int64
	restoreWindow := setMQTTTestVoidHandoffWindowHook(func(sess *mqttSession, window string) {
		if sess == e.sess && window == mqttHandoffWindowPreGate {
			attempts.Add(1)
		}
	})
	defer restoreWindow()

	// The predicate itself: an announced waiter suppresses the trigger, exactly as an open
	// transaction does.
	e.testStageVerdict(t, "nonce-waiter", 220)
	if !e.handoffPending(t) {
		t.Fatal("a staged verdict on a resident, unbound, resolved session with a free fence must be pending")
	}
	e.sess.recordTxWaiters.Store(1)
	pendingWithWaiter := e.handoffPending(t)
	e.sess.recordTxWaiters.Store(0)
	if pendingWithWaiter {
		t.Fatal("a blocking writer announced for the fence must suppress the handoff: the fence is reserved for it, " +
			"and its own release re-checks")
	}

	// And at a real release, with the waiter's transaction PARKED so the not-fired assertion
	// is not racing its immediate release.
	holder := e.sess.beginRecordTx()
	parked := make(chan struct{}, 1)
	restoreWait := setMQTTTestRecordTxWaitHook(func(sess *mqttSession) {
		if sess != e.sess {
			return
		}
		select { // under sess.mu: never blocks
		case parked <- struct{}{}:
		default:
		}
	})
	defer restoreWait()

	waiterAcquired := make(chan struct{})
	waiterDone := make(chan struct{})
	unparkCh := make(chan struct{})
	unpark := sync.OnceFunc(func() { close(unparkCh) })
	go func() {
		defer close(waiterDone)
		tok := e.sess.beginRecordTx()
		close(waiterAcquired)
		<-unparkCh
		if err := e.sess.endRecordTx(tok); err != nil {
			t.Errorf("endRecordTx (waiter): %v", err)
		}
	}()
	releaseHolder := sync.OnceFunc(func() {
		if err := e.sess.endRecordTx(holder); err != nil {
			t.Errorf("endRecordTx (holder): %v", err)
		}
	})
	// Installed before the first assertion, and in this order: unwinding runs the HOLDER
	// release first, then the unpark, then the join. Any other order lets a failed
	// assertion made before the release leave the waiter blocked on a fence nobody
	// releases — the test would then hang to the suite timeout instead of reporting, which
	// is how an announcement regression (M3, M4) disappears into a harness timeout.
	defer func() { <-waiterDone }()
	defer unpark()
	defer releaseHolder()

	<-parked
	// Causally established BEFORE the holder releases. Merely having spawned the waiter
	// would let a fast release legitimately observe no waiter and start a handoff, which is
	// a false failure on correct code.
	if n := e.sess.recordTxWaiters.Load(); n != 1 {
		t.Fatalf("the waiter must be announced before the holder releases: count = %d, want 1", n)
	}
	before := attempts.Load()
	releaseHolder()
	<-waiterAcquired
	if n := attempts.Load(); n != before {
		t.Fatalf("the release must not start a handoff while a blocking writer is announced, %d attempt(s) fired", n-before)
	}
	if ps := e.loadRecord(t); testRecordHasNonce(ps, "nonce-waiter") {
		t.Fatalf("nothing may have persisted the verdict yet: %v", ps.CancelledRegs)
	}

	unpark()
	<-waiterDone
	if n := attempts.Load(); n != before+1 {
		t.Fatalf("the deferral is a handoff: the waiter's own release must fire exactly one attempt, got %d", n-before)
	}
	if rev, saved, _ := e.voidRevs(t); saved < rev {
		t.Fatalf("the waiter's release must converge the verdict: rev=%d saved=%d", rev, saved)
	}
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-waiter") {
		t.Fatalf("the verdict never reached the record: %v", ps.CancelledRegs)
	}

	// The BACKSTOP, from the state it exists for: a definite store failure has burned the
	// watermark, so no release triggers again on its own.
	restoreSave := setMQTTTestSessSaveHook(func(sess *mqttSession) error {
		if sess != e.sess {
			return nil
		}
		return errors.New("test: the session store is unavailable")
	})
	e.testStageVerdict(t, "nonce-waiter-backstop", 221)
	e.releaseFence(t)
	restoreSave()
	rev, saved, handoff := e.voidRevs(t)
	if saved >= rev || handoff != rev {
		t.Fatalf("the attempt must have acquired the fence and failed definitely: rev=%d saved=%d handoff=%d", rev, saved, handoff)
	}

	// The announced state is constructed WHITE-BOX here: a real waiter on a FREE fence
	// competes for sess.mu immediately, with no seam between its announce and its Lock(), so
	// the backstop could not be made to run first.
	e.sess.mu.Lock()
	seqBefore := e.sess.recordTxSeqno
	e.sess.mu.Unlock()
	e.sess.recordTxWaiters.Store(1)
	e.asm.flushVoidVerdictGap(e.sess)
	rev, saved, handoff = e.voidRevs(t)
	if handoff != saved || rev <= saved {
		t.Fatalf("the backstop must re-arm the watermark even while a waiter is announced — re-arming is what lets "+
			"the waiter's own release trigger the attempt: rev=%d saved=%d handoff=%d", rev, saved, handoff)
	}
	e.sess.mu.Lock()
	seqAfter := e.sess.recordTxSeqno
	e.sess.mu.Unlock()
	if seqAfter != seqBefore {
		t.Fatalf("the re-armed attempt must not take the fence ahead of an announced waiter: transaction seqno %d -> %d",
			seqBefore, seqAfter)
	}
	if ps := e.loadRecord(t); testRecordHasNonce(ps, "nonce-waiter-backstop") {
		t.Fatalf("the deferred attempt may not have written: %v", ps.CancelledRegs)
	}

	// A phase-local DELTA, never a cumulative count: the backstop's own deferred attempt
	// already fired the window hook once above before bailing.
	e.sess.recordTxWaiters.Store(0)
	snapshot := attempts.Load()
	e.releaseFence(t)
	if n := attempts.Load(); n != snapshot+1 {
		t.Fatalf("the next release must fire exactly one attempt for the re-armed revision, got %d", n-snapshot)
	}
	if rev, saved, _ = e.voidRevs(t); saved < rev {
		t.Fatalf("the re-armed verdict must converge: rev=%d saved=%d", rev, saved)
	}
	if ps := e.loadRecord(t); !testRecordHasNonce(ps, "nonce-waiter-backstop") {
		t.Fatalf("the re-armed verdict never reached the record: %v", ps.CancelledRegs)
	}
}

// handoffPending asks the release handoff's predicate under sess.mu, since every field it
// reads but the waiter count is guarded by it.
func (e *testDeferredEnv) handoffPending(t *testing.T) bool {
	t.Helper()
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.voidVerdictHandoffPendingLocked()
}

// T15c: the SURVIVING branch's in-fence re-check, in the window its entry check cannot see.
//
// The entry check is a check-then-act — the fence is what serializes writers, and an
// off-readLoop one can go ambiguous while this discharge is still waiting for it — so the
// re-check under the fence is the sound one. Without it the branch consumes the action,
// stamps the disconnect time, purges, and then has its save refused at entry and classified
// definitely failed: no continuation, no re-park, and if the competing write landed the
// record stays connected-looking with the expiry obligation gone.
//
// Driven POSITIVELY through the discharge-window seam, which fires exactly between the two
// checks; a test that armed the ambiguity earlier would be caught by the entry check and
// would prove nothing about this one.
//
// Mutation MU19b: drop only the in-fence re-check and the action is consumed and lost.
func TestMQTTDeferredDischargeReparksWhenUnresolvedUnderTheFence(t *testing.T) {
	e := testNewDeferredEnv(t, "client-discharge-fence", "discharge.fence")
	action := testSurvivingAction()
	e.gate(t)

	armed := false
	restoreHook := setMQTTTestDischargeWindowHook(func(sess *mqttSession) {
		if sess != e.sess || armed {
			return
		}
		armed = true
		// A register store settled here and its save went ambiguous: after the discharge's
		// entry check, before it takes the fence.
		sess.mu.Lock()
		testMarkUncertainLocked(sess, sess.seq, testAmbiguousBodyLocked(sess, "ambiguous"))
		sess.mu.Unlock()
	})
	defer restoreHook()

	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	if !armed {
		t.Fatal("the discharge window never opened, so the race was not exercised")
	}

	// Nothing was applied and the action is back, untouched.
	e.sess.mu.Lock()
	parked := e.sess.deferredDisconnect
	disconnectedAt := e.sess.disconnectedAt
	_, held := e.sess.pendingPublish[e.pi]
	_, bucket := e.sess.cpending[e.dur]
	e.sess.mu.Unlock()
	if parked != action {
		t.Fatalf("the consumed action must be re-parked untouched, got %+v", parked)
	}
	if disconnectedAt != 0 {
		t.Fatalf("no mutation may have happened: disconnectedAt=%d", disconnectedAt)
	}
	if !held || !bucket {
		t.Fatalf("the purge must not have run: held=%v bucket=%v", held, bucket)
	}
	e.asm.mu.Lock()
	_, armedTimer := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if armedTimer {
		t.Fatal("no timer may be armed for a close that was not applied")
	}

	// The resolution that follows discharges it for real, deadline and purge together.
	if res := e.s.resolveUncertainSession(e.asm, e.sess); res != mqttUncertainOurs {
		t.Fatalf("resolution = %v, want ours: the competing write did not land", res)
	}
	e.release()
	e.sess.mu.Lock()
	parked, disconnectedAt = e.sess.deferredDisconnect, e.sess.disconnectedAt
	_, held = e.sess.pendingPublish[e.pi]
	e.sess.mu.Unlock()
	if parked != nil || disconnectedAt != action.disconnectedAt || held {
		t.Fatalf("the re-discharge must apply the close: parked=%+v disconnectedAt=%d held=%v",
			parked, disconnectedAt, held)
	}
	if ps := e.loadRecord(t); ps.DisconnectedAt != action.disconnectedAt {
		t.Fatalf("the record must carry the earned deadline: DisconnectedAt=%d, want %d",
			ps.DisconnectedAt, action.disconnectedAt)
	}
}

// T18: a REBIND between the pending sample and the gate claim is what the claim-as-NOBODY
// guard is for.
//
// The pending condition is sampled before the gate is taken, so `sess.c == nil` there is a
// pre-gate observation: a CONNECT can acquire the gate, bind the session and release it again
// in that window. A sample cannot see that, so the rule has to be enforced against FRESH
// state, and it is enforced twice over — by the gate CLAIM, taken as NOBODY so it refuses for
// a session that has since rebound, and by the attempt's own re-validation of the same
// pending condition once the gate is held.
//
// What this test pins is therefore the pair, in the window a single pre-trigger check cannot
// cover: no record write, no injected uncertainty, ownership handed back. T9 covers the other
// window — a session already bound when the trigger fires.
//
// Mutation MU13d (both homes of the unbound rule removed): the attempt writes against a
// bound session. Removing only ONE home is not observable here and is not claimed to be:
// MU13b (the claim alone) is caught by the re-validation and MU13 (the predicate alone) by
// the claim, both recorded as surviving in docs/ROADMAP.md with that reasoning.
func TestMQTTVoidVerdictHandoffYieldsToARebindInTheGateWindow(t *testing.T) {
	e := testNewDeferredEnv(t, "client-handoff-rebind", "handoff.rebind")
	stores, restoreCount := testSessRecordStoreCounter(e.sess)
	defer restoreCount()

	rebound := false
	restoreHook := setMQTTTestVoidHandoffWindowHook(func(sess *mqttSession, window string) {
		if sess != e.sess || window != mqttHandoffWindowPreGate || rebound {
			return
		}
		rebound = true
		// A CONNECT won the gate, bound the session and released the gate again — all in
		// the window between this attempt's sample and its claim.
		sess.mu.Lock()
		sess.c = e.c
		sess.mu.Unlock()
	})
	defer restoreHook()

	e.testStageVerdict(t, "nonce-rebind", 212)
	e.releaseFence(t)

	if !rebound {
		t.Fatal("the pre-gate window never opened, so the race was not exercised")
	}
	if *stores != 0 {
		t.Fatalf("a rebound session's record is not the handoff's to write, got %d store(s)", *stores)
	}
	if e.sess.isSeqUncertain() {
		t.Fatal("the handoff must not be able to inject uncertainty behind a CONNECT that has bound")
	}
	if ps := e.loadRecord(t); testRecordHasNonce(ps, "nonce-rebind") {
		t.Fatalf("nothing may have been written for the rebound session: %v", ps.CancelledRegs)
	}
	e.sess.mu.Lock()
	active := e.sess.voidHandoffActive
	e.sess.c = nil
	e.sess.mu.Unlock()
	if active {
		t.Fatal("the refused attempt must hand its loop ownership back")
	}
}

// T19: a Will deadline that passes WHILE the branch is doing its JetStream work is published
// directly, not enqueued.
//
// The purge's register-stream rungs and the classified save are round trips, so a deadline
// sampled at entry can be stale by the time the Will is dispatched. Enqueuing an already-due
// Will is not a harmless approximation: an outbox record is cancellable, so a reconnect
// arriving AFTER the deadline could suppress a Will that was already owed — the reconnect rule
// the direct-publication branch exists to hold.
//
// Mutation MU27: decide the Will's fate from the entry sample and the overdue Will is enqueued
// where a reconnect can still cancel it.
func TestMQTTDeferredCloseDueWillIsPublishedNotEnqueued(t *testing.T) {
	e := testNewDeferredEnv(t, "client-due-will", "due.will")
	// A delayed Will whose deadline falls DURING the branch's work.
	action := &mqttDeferredDisconnect{
		disconnectedAt: time.Now().Unix() - 600,
		expiryInterval: testDeferredExpiry,
		will:           &mqttWill{topic: []byte("will/due"), message: []byte("bye"), qos: 0},
		willDeadline:   time.Now().Add(150 * time.Millisecond).UnixNano(),
		willDelayed:    true,
		willGen:        e.sess.gen,
	}

	// Hold the branch's own record write long enough for the deadline to pass, which is the
	// interleaving: at entry the Will was still in the future, at dispatch it is not.
	held := false
	restore := setMQTTTestSessStoreResultHook(func(s *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if s == e.sess && !held {
			held = true
			time.Sleep(250 * time.Millisecond)
		}
		return resp, err
	})
	defer restore()

	e.gate(t)
	e.s.applyDeferredDisconnect(e.asm, e.sess, action)
	e.release()
	if !held {
		t.Fatal("the branch's write was never held, so the deadline did not pass during it")
	}

	// Nothing may be sitting in the outbox for this client: a due Will goes out directly,
	// where no reconnect can cancel it.
	filter := mqttWillQClientFilter(e.asm.domainTk, e.sess.idHash)
	if smsg, err := e.asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, filter, 1); err == nil {
		t.Fatalf("an already-due Will must not be enqueued; found an outbox record at sequence %d", smsg.Sequence)
	} else if isErrorOtherThan(err, JSNoMessageFoundErr) {
		t.Fatalf("scan the Will outbox: %v", err)
	}
}
