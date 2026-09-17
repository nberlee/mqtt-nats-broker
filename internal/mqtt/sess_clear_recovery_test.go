package mqtt

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// Late-uncertainty refusal and recovery for clear()'s reachable callers.
//
// clear() refuses — with errMQTTClearRefusedUnresolved — while a record write is unresolved,
// at entry and again with the record fence held. The refusal deletes nothing, which is
// what makes it recoverable; but a caller that has already evicted the session and only
// logs turns "do not tear down on an unclassified sequence" into "strand the object while
// its record, its consumers and its shared durables survive". Residency is what every
// resolver needs: reconcileResidentRecordState and CONNECT both scan as.sessions, and for an
// expiry-0 record sweepSessionExpiries looks at nothing.
//
// The tests below drive the race POSITIVELY through the clear-window seams — on the same
// goroutine, at the named window, with no sleeps. A test that slept could pass against
// broken code by arming the ambiguity early enough for an earlier check to catch it.

// testExpiringEnv is a deferred env whose session carries an earned finite deadline, which
// is the state an expiry timer fires against.
//
// It also records the shared-group MEMBERSHIP, which testNewDeferredEnv does not: that env
// creates the group consumer and tracks a delivery on it, but a discard derives the shared
// durables it owes a release from sharedSubs — the persisted authority — so without the
// membership the release these tests assert would have nothing to release. A live group
// consumer that no session's sharedSubs names is a separate, documented gap
// (docs/ROADMAP.md), not what this recovery is about.
func testExpiringEnv(t *testing.T, clientID, filter string) (*testDeferredEnv, int64) {
	t.Helper()
	e := testNewDeferredEnv(t, clientID, filter)
	disconnectedAt := time.Now().Unix() - 600
	key := string(mqttSharedSubPrefix) + e.group + "/" + e.filter
	if !mqttSharedKeyOwnsDurable(key, e.dur) {
		t.Fatalf("the session key %q must own the group durable %q the fixture created", key, e.dur)
	}
	e.sess.mu.Lock()
	e.sess.disconnectedAt = disconnectedAt
	e.sess.expiryInterval = 60
	if e.sess.sharedSubs == nil {
		e.sess.sharedSubs = make(map[string]byte)
	}
	e.sess.sharedSubs[key] = 1
	e.sess.mu.Unlock()
	// Persist the membership, so the record and the in-memory view agree — a transient
	// object rebuilt from this record derives the same durables.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the shared membership: %v", err)
	}
	return e, disconnectedAt
}

// fireExpiry arms the timer entry expireSession consumes and then runs it on this
// goroutine. The entry must exist: without it expireSession returns before doing anything,
// and a test asserting "nothing was discarded" would pass vacuously.
func testFireExpiry(t *testing.T, e *testDeferredEnv) {
	t.Helper()
	e.asm.scheduleSessionExpiry(e.s, e.sess.idHash, time.Hour)
	e.asm.mu.Lock()
	_, armed := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if !armed {
		t.Fatal("the expiry timer entry was not armed, so expireSession would return at once")
	}
	e.asm.expireSession(e.s, e.sess.idHash)
}

// armAtClearWindow arms an ambiguous write at one of clear()'s two decision points, as an
// off-readLoop register store settling there would. Reports whether the window opened.
func testArmAtClearWindow(t *testing.T, e *testDeferredEnv, window string) func() bool {
	t.Helper()
	armed := false
	restore := setMQTTTestClearWindowHook(func(sess *mqttSession, w string) {
		if sess != e.sess || w != window || armed {
			return
		}
		armed = true
		sess.mu.Lock()
		testMarkUncertainLocked(sess, sess.seq, testAmbiguousBodyLocked(sess, "ambiguous"))
		sess.mu.Unlock()
	})
	t.Cleanup(restore)
	return func() bool { return armed }
}

func (e *testDeferredEnv) resident() bool {
	e.asm.mu.RLock()
	defer e.asm.mu.RUnlock()
	return e.asm.sessByHash[e.sess.idHash] == e.sess
}

func (e *testDeferredEnv) parkedAction() *mqttDeferredDisconnect {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.deferredDisconnect
}

func (e *testDeferredEnv) recordPresent(t *testing.T) bool {
	t.Helper()
	ps, _, err := e.asm.loadFenceSession(e.sess.idHash)
	if err != nil {
		t.Fatalf("load the session record: %v", err)
	}
	return ps != nil
}

func (e *testDeferredEnv) durablePresent() bool {
	_, err := e.asm.jsa.lookupConsumer(mqttStreamName, e.dur)
	return err == nil
}

// assertRefusedDiscardParked is the state a recovered refusal leaves: nothing deleted, the
// session RESIDENT, and a discard-only obligation parked against the earned deadline.
func (e *testDeferredEnv) assertRefusedDiscardParked(t *testing.T, disconnectedAt int64) *mqttDeferredDisconnect {
	t.Helper()
	if !e.recordPresent(t) {
		t.Fatal("a refused discard must delete no record")
	}
	if !e.durablePresent() {
		t.Fatal("a refused discard must delete no durable")
	}
	if !e.resident() {
		t.Fatal("the session must be resident again: it holds the parked discard, and only a resident session is resolved")
	}
	action := e.parkedAction()
	if action == nil {
		t.Fatal("the refused discard must be parked as an obligation")
	}
	if !action.endsAtDisconnect() {
		t.Fatalf("the parked action must end at disconnect (expiryInterval 0), got %d", action.expiryInterval)
	}
	if action.will != nil {
		t.Fatal("the recovery must not park a Will: a memory-only Will is dropped by a crash")
	}
	if action.disconnectedAt != disconnectedAt {
		t.Fatalf("the parked action must carry the earned disconnect time: got %d, want %d",
			action.disconnectedAt, disconnectedAt)
	}
	return action
}

// T1: session expiry, ambiguity armed with the fence still to be taken.
//
// expireSession has no early uncertainty check at all, so this is the recovery's main
// route: removeSession has already run and the timer entry has already been consumed when
// clear() refuses. Evicting anyway loses the unresolved write's pendingWrite, its retained
// continuations and its staged void-register verdicts — nothing else can classify that
// write, so they are lost outright, not merely delayed.
//
// Mutation MU-A: proceed on the sentinel (evict and warn) and nothing is parked.
// Mutation MU-A2: park but do not re-add, and nothing ever discharges it.
// Mutation MU-D: drop clear()'s in-fence re-check and the record is deleted on a sequence
// nobody has classified.
func TestMQTTExpiryReparksTheRefusedDiscardAtTheFence(t *testing.T) {
	e, disconnectedAt := testExpiringEnv(t, "client-expiry-prefence", "expiry.prefence")
	opened := testArmAtClearWindow(t, e, mqttClearWindowPreFence)

	testFireExpiry(t, e)
	if !opened() {
		t.Fatal("the pre-fence window never opened, so the race was not exercised")
	}
	e.assertRefusedDiscardParked(t, disconnectedAt)

	// And the obligation is real: the periodic pass over resident unresolved sessions
	// classifies the write and completes the discard the timer could not.
	e.asm.reconcileResidentRecordState(e.s)
	if e.recordPresent(t) {
		t.Fatal("the discharged discard must delete the record")
	}
	if e.durablePresent() {
		t.Fatal("the discharged discard must release the shared group durable this member held")
	}
	if e.resident() {
		t.Fatal("the discharged discard must remove the session")
	}
	if a := e.parkedAction(); a != nil {
		t.Fatalf("the discharge must consume the action, got %+v", a)
	}
}

// T2: session expiry, ambiguity already armed at entry.
//
// Not a race at all — the write was unresolved before the timer fired — and the reason it
// needs its own test is that expireSession has no early check, so this state reaches
// clear()'s ENTRY refusal rather than its in-fence one. Both must carry the sentinel.
//
// Mutation MU-E: return a plain error from the entry refusal and the recovery is skipped.
func TestMQTTExpiryReparksTheRefusedDiscardAtClearEntry(t *testing.T) {
	t.Parallel()
	e, disconnectedAt := testExpiringEnv(t, "client-expiry-entry", "expiry.entry")
	testMarkUncertain(e.sess, e.sess.seq, testAmbiguousBody(e.sess, "ambiguous"))

	testFireExpiry(t, e)
	e.assertRefusedDiscardParked(t, disconnectedAt)
}

// T3: a clear() failure that is NOT the sentinel keeps the historical outcome.
//
// The seam is the session-record delete, which makes this a POST-CLAIM failure: the
// teardown marker has landed, releaseTeardown retries and returns mqttTeardownRetry with
// the underlying error, and a CONNECT or the orphan sweep finishes the teardown from the
// marker. Recovering here would re-park an obligation that is already half discharged.
//
// Mutation MU-A3: re-add on ANY clear() error rather than on the sentinel.
func TestMQTTExpiryKeepsHistoricalOutcomeOnANonSentinelFailure(t *testing.T) {
	e, _ := testExpiringEnv(t, "client-expiry-nonsentinel", "expiry.nonsentinel")
	if a := e.parkedAction(); a != nil {
		t.Fatalf("the fixture must start with no parked action, got %+v", a)
	}

	// Scoped to the session stream, so unrelated deletes (the in-flight registers) still
	// work, and NOT a not-found error, which the release would read as already gone.
	failed := false
	restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
		if stream != mqttSessStreamName {
			return nil
		}
		failed = true
		return errors.New("test: the session record delete is unavailable")
	})
	defer restore()

	testFireExpiry(t, e)
	if !failed {
		t.Fatal("the session-record delete was never attempted, so no post-claim failure was produced")
	}
	if e.resident() {
		t.Fatal("a post-claim failure must keep the historical outcome: the session stays evicted")
	}
	if a := e.parkedAction(); a != nil {
		t.Fatalf("a post-claim failure must park nothing, got %+v", a)
	}
}

// T3b: a post-claim failure on a session that IS unresolved still keeps the historical
// outcome.
//
// This is a SYNTHETIC DEFENSIVE-STATE test, and it must not be read as more: the state it
// builds is NOT production-reachable, and it does not demonstrate a runtime interleaving.
// clear() holds its record transaction from before the in-fence uncertainty re-check
// through claimTeardown and releaseTeardown to the release, and a production writer needs
// that same fence to arm uncertainty — tryBeginRecordTx fails while it is held. So no
// off-readLoop writer can go ambiguous after the claim has landed. The test reaches the
// state only by calling markWriteUncertainLocked directly from the delete hook, bypassing
// the fence.
//
// What it legitimately proves is that the caller's SENTINEL check is not the same check as
// the helper's uncertainty gate, which is why T3 alone cannot discriminate MU-A3: with the
// session resolved, `deferDisconnectIfUncertain(requireUncertain: true)` refuses to park
// anyway, so dropping the sentinel test changes nothing there. The gate asks "is this
// session unresolved"; the sentinel asks "was this failure recoverable". Separating them
// needs a non-sentinel failure on an unresolved session, and the fence makes that
// unreachable in production — so it is constructed here to pin the defensive branch, which
// matters because recovering on a post-claim failure would re-park a discard a CONNECT or
// the orphan sweep is already positioned to finish from the marker.
//
// The error is provably not the sentinel: that can only come from the two pre-deletion
// checks, and this failure is the record delete.
//
// Mutation MU-A3 (re-add on ANY clear() error rather than on the sentinel) is killed as a
// DEFENSIVE-branch mutation, not a production-shaped one.
func TestMQTTExpiryKeepsHistoricalOutcomeWhenAPostClaimFailureLeavesItUnresolved(t *testing.T) {
	e, _ := testExpiringEnv(t, "client-expiry-postclaim-uncertain", "expiry.postclaim.uncertain")

	failed := false
	restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
		if stream != mqttSessStreamName {
			return nil
		}
		if !failed {
			failed = true
			// Synthesized, not production-reachable: clear() holds the fence across the
			// claim and this delete, so a real writer could not arm uncertainty here.
			e.sess.mu.Lock()
			testMarkUncertainLocked(e.sess, e.sess.seq, testAmbiguousBodyLocked(e.sess, "ambiguous"))
			e.sess.mu.Unlock()
		}
		return errors.New("test: the session record delete is unavailable")
	})
	defer restore()

	testFireExpiry(t, e)
	if !failed {
		t.Fatal("the session-record delete was never attempted, so no post-claim failure was produced")
	}
	if !e.sess.isSeqUncertain() {
		t.Fatal("the fixture must leave the session unresolved, or it does not separate the two checks")
	}
	if e.resident() {
		t.Fatal("a post-claim failure must keep the historical outcome even when the session is unresolved: the claim landed, so the teardown is finished from the marker, not re-parked")
	}
	if a := e.parkedAction(); a != nil {
		t.Fatalf("a post-claim failure must park nothing, got %+v", a)
	}
}

// T6: clear() refuses UNCONDITIONALLY, at both windows, and releases the fence WITHOUT
// ending the record generation.
//
// The epoch is the assertion that discriminates: sess.gen is re-minted by the discard
// itself, so an assertion on it would pass against the mutant, while recordEpoch is what
// endRecordTxNewGeneration bumps — and bumping it for a discard that is not happening
// kills every token minted before it.
//
// Mutation MU-F: release the in-fence refusal with endRecordTxNewGeneration.
func TestMQTTClearRefusesLateUncertaintyUnconditionally(t *testing.T) {
	for _, window := range []string{mqttClearWindowPreEntry, mqttClearWindowPreFence} {
		t.Run(window, func(t *testing.T) {
			e := testNewDeferredEnv(t, "client-clear-refuses-"+window, "clear.refuses."+window)
			opened := testArmAtClearWindow(t, e, window)

			e.sess.mu.Lock()
			epochBefore := e.sess.recordEpoch
			e.sess.mu.Unlock()

			err := e.sess.clear(true)
			if !opened() {
				t.Fatalf("the %s window never opened", window)
			}
			if !errors.Is(err, errMQTTClearRefusedUnresolved) {
				t.Fatalf("clear() = %v, want the late-uncertainty sentinel", err)
			}
			if !e.recordPresent(t) {
				t.Fatal("a refused discard must delete no record")
			}
			e.sess.mu.Lock()
			epochAfter := e.sess.recordEpoch
			txOpen := e.sess.recordTxOpen
			e.sess.mu.Unlock()
			if epochAfter != epochBefore {
				t.Fatalf("recordEpoch = %d, want the unchanged %d: a refusal mutates nothing and must not end the generation",
					epochAfter, epochBefore)
			}
			if txOpen {
				t.Fatal("the refusal must release the record fence")
			}
		})
	}
}

// T9: safe RE-ENTRY, with the SWEEP as the link that produces the second entry.
//
// The route: a finite close persists DisconnectedAt > 0; the timer fires and clear() is
// refused; the recovery parks an expiry-0 action and re-adds; the retry does not resolve;
// and because sweepSessionExpiries' residency guard sits only inside its
// DisconnectedAt == 0 branch, it arms ANOTHER timer for this still-resident session, so
// expireSession is entered a second time with the action already parked.
//
// The sweep step is driven here rather than assumed, because that is the load-bearing link
// and an earlier version of this test skipped it: it scheduled both entries by hand, so a
// regression that stopped the sweep re-arming a resident session would have passed. What is
// still CONSTRUCTED rather than driven is the finite close itself — the fixture sets and
// persists the earned deadline directly instead of running mqttHandleClosedClient — so this
// test is evidence about the sweep-to-expiry link and the helper's idempotence, not about
// the close that precedes them.
//
// The helper arbitrates nothing on re-entry: the parked action is already this recovery's
// own obligation, so it is neither mutated nor replaced.
//
// Mutation MU-G: park a second action, or replace the existing one, instead of
// recognising the obligation already recorded.
func TestMQTTExpiryReEntryLeavesTheParkedDiscardUntouched(t *testing.T) {
	e, _ := testExpiringEnv(t, "client-expiry-reentry", "expiry.reentry")

	// TWO deadlines, and the split is deliberate.
	//
	// The RECORD gets a few seconds of remaining, because the sweep arms
	// `time.AfterFunc(remaining, ...)` from the record and a past deadline clamps that
	// to zero — the timer would fire at once and its own expiry would consume the
	// entry whose re-arm this test exists to observe.
	//
	// The IN-MEMORY copy stays ELAPSED, because an expiry fire means the session's own
	// copy says it is due, and the in-memory path now RE-READS that deadline under the
	// gate (F-036 unit 1): a callback delayed past a reconnect and a later disconnect
	// would otherwise destroy a session whose lifetime had not run out. An earlier
	// version of this fixture instead relied on that path never re-checking.
	//
	// The divergence is a state the broker reaches on its own — a peer's disconnect
	// save is newer than this copy — and both halves stay legitimate under it. The
	// record write happens HERE, while the session is still resolved; once the refusal
	// below arms uncertainty, nothing may persist through it.
	e.sess.mu.Lock()
	disconnectedAt := e.sess.disconnectedAt
	e.sess.disconnectedAt = time.Now().Unix() - 55
	e.sess.mu.Unlock()
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the record deadline the sweep will read: %v", err)
	}
	e.sess.mu.Lock()
	e.sess.disconnectedAt = disconnectedAt
	e.sess.mu.Unlock()

	opened := testArmAtClearWindow(t, e, mqttClearWindowPreFence)

	testFireExpiry(t, e)
	if !opened() {
		t.Fatal("the pre-fence window never opened, so the race was not exercised")
	}
	first := e.assertRefusedDiscardParked(t, disconnectedAt)

	// The timer entry was consumed by the entry above, and the record still carries the
	// earned deadline, so the SWEEP is what re-arms this resident session. Assert it does:
	// that is the production link from one refusal to the next.
	e.asm.mu.Lock()
	_, armedBefore := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if armedBefore {
		t.Fatal("the first entry must have consumed the timer entry, or the sweep's re-arm proves nothing")
	}
	e.asm.sweepSessionExpiries(e.s)
	e.asm.mu.Lock()
	_, armedBySweep := e.asm.pendingExpiries[e.sess.idHash]
	e.asm.mu.Unlock()
	if !armedBySweep {
		t.Fatal("the sweep must re-arm an expiry for a RESIDENT session whose record carries DisconnectedAt > 0: that is what re-enters the recovery")
	}

	// Second entry, consuming the timer entry the sweep just armed. The session is still
	// unresolved, so this one refuses at clear()'s ENTRY check and arrives at the recovery
	// with an action already parked.
	e.asm.expireSession(e.s, e.sess.idHash)
	again := e.assertRefusedDiscardParked(t, disconnectedAt)
	if again != first {
		t.Fatalf("the parked action must be the SAME one, neither replaced nor rebuilt: got %p, want %p", again, first)
	}
	if again.expiryInterval != 0 || again.willDeadline != 0 || again.will != nil {
		t.Fatalf("the parked action's terms must be untouched, got %+v", again)
	}

	// And the discard still completes once the write is classified.
	e.asm.reconcileResidentRecordState(e.s)
	if e.recordPresent(t) {
		t.Fatal("the discharged discard must delete the record")
	}
	if e.resident() {
		t.Fatal("the discharged discard must remove the session")
	}
}

// T9b: every OTHER parked shape declines, keeping today's evict-and-log.
//
// The helper recognises only the shape it would have written itself — ends at disconnect
// AND no Will. A parked action carries no owner identity, so nothing here can prove whose
// deadline or whose Will a retarget would overwrite. No existing layer enforces that
// distinction: deferDisconnectIfUncertain distinguishes only "an action exists".
//
// Mutation MU-G2: accept ANY existing action (drop the endsAtDisconnect() && no-Will test).
// The mutant does not rewrite the parked action — it treats an unrelated obligation as
// though it already satisfied this refused discard, reporting success and restoring
// residency while the discard the caller actually owed is silently dropped. The residency
// assertion is what discriminates it.
func TestMQTTExpiryDeclinesRecoveryForAnyOtherParkedShape(t *testing.T) {
	t.Parallel()
	shapes := map[string]*mqttDeferredDisconnect{
		"a surviving lifetime": {
			disconnectedAt: time.Now().Unix() - 600,
			expiryInterval: testDeferredExpiry,
		},
		"a discard owing a Will": {
			disconnectedAt: time.Now().Unix() - 600,
			will:           &mqttWill{topic: []byte("will/topic"), message: []byte("bye"), qos: 0},
			willDeadline:   time.Now().Add(-time.Minute).UnixNano(),
		},
	}
	for name, action := range shapes {
		t.Run(name, func(t *testing.T) {
			e, _ := testExpiringEnv(t, "client-expiry-decline-"+name, "expiry.decline")
			testMarkUncertain(e.sess, e.sess.seq, testAmbiguousBody(e.sess, "ambiguous"))
			if !e.sess.deferDisconnect(action) {
				t.Fatal("park the pre-existing action")
			}

			testFireExpiry(t, e)

			if got := e.parkedAction(); got != action {
				t.Fatalf("the pre-existing action must be untouched, got %+v", got)
			}
			if action.expiryInterval == 0 && action.will == nil {
				t.Fatal("the fixture is the discard-only shape, so it proves nothing")
			}
			if e.resident() {
				t.Fatal("a declined recovery keeps today's outcome: the session stays evicted")
			}
		})
	}
}

// T10: the superseded shortcut must not fire while the session is unresolved.
//
// expireSession reads "the record's sequence is higher than ours" as a remote owner. That
// is ALSO true for our own ambiguous write that landed with its response lost, so the
// shortcut evicted the continuation-bearing object and ran the teardown from a transient
// object with no pendingWrite and no uncertainReverts — losing exactly what the recovery
// exists to keep. Sequence is not identity; only the digest classifier can tell the two
// apart, so the decision is deferred to it.
//
// T10's own evidence is narrow and deliberately so: that expireSession PRESERVES the
// object and reaches the recovery. Landed-sequence adoption is already pinned by
// TestUncertainWriteThreeWayReadback and landed-continuation execution by the drain
// tests, so those are kept below as checks, not claimed as this test's evidence.
//
// Mutation MU-I: remove the isSeqUncertain() guard from the superseded decision.
func TestMQTTExpiryDoesNotEvictOnALandedAmbiguousWrite(t *testing.T) {
	t.Parallel()
	e, disconnectedAt := testExpiringEnv(t, "client-expiry-landed", "expiry.landed")

	e.sess.mu.Lock()
	baseSeq := e.sess.seq
	if e.sess.subs == nil {
		e.sess.subs = make(map[string]byte)
	}
	e.sess.subs["extra"] = 1
	e.sess.mu.Unlock()
	body := testRecordBody(t, e.sess)
	// The write really lands; the caller never learns it did, so sess.seq stays at the
	// base and the record's sequence is genuinely higher.
	landedSeq, err := mqttStoreSessionRecordCASBody(&e.asm.jsa, e.asm.domainTk, body, e.sess.idHash, baseSeq)
	if err != nil {
		t.Fatalf("simulate the landed write: %v", err)
	}
	if landedSeq <= baseSeq {
		t.Fatalf("the fixture needs a record sequence above ours: landed=%d base=%d", landedSeq, baseSeq)
	}
	testMarkUncertain(e.sess, baseSeq, body)

	testFireExpiry(t, e)
	e.assertRefusedDiscardParked(t, disconnectedAt)

	// Classification is what settles ownership: ours, so the landed sequence is adopted
	// and the parked discard CASes against the right one.
	e.asm.reconcileResidentRecordState(e.s)
	if e.recordPresent(t) {
		t.Fatal("the discharged discard must delete the record it now knows is ours")
	}
	if e.resident() {
		t.Fatal("the discharged discard must remove the session")
	}
}

// T10b: the record fence is HELD across the load.
//
// Asserting the invariant itself, not a downstream symptom: an earlier form of this test
// asserted "resident and parked", which the correct code and the unfenced mutant both
// produce by different routes. From inside the load hook — which runs before the actual
// JetStream read — a tryBeginRecordTx probe must be REFUSED. Under the correct code
// nothing can arm uncertainty during the load, so the expected outcome is the ordinary
// one: the decision is made against fence-protected state and clear() succeeds.
//
// The probe must be tryBeginRecordTx, never the blocking beginRecordTx: same goroutine, so
// waiting would deadlock against the very fence under test. And an unexpectedly successful
// probe is released immediately, or it holds the fence open and deadlocks the clear().
//
// Mutation MU-I2: make the decision with no transaction open at all — sampled before or
// after the load — and the probe succeeds.
func TestMQTTExpiryHoldsTheRecordFenceAcrossTheLoad(t *testing.T) {
	e, _ := testExpiringEnv(t, "client-expiry-fenced-load", "expiry.fenced.load")

	watching := false
	probed := false
	probeRefused := false
	restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if !watching || probed || stream != mqttSessStreamName || subject != mqttSessStreamSubjectPrefix+e.asm.domainTk+e.sess.idHash {
			return nil
		}
		probed = true
		tok, ok := e.sess.tryBeginRecordTx()
		probeRefused = !ok
		if ok {
			// Never leave the fence open: the clear() that follows would block forever.
			if terr := e.sess.endRecordTx(tok); terr != nil {
				t.Errorf("release the unexpected probe transaction: %v", terr)
			}
		}
		return nil
	})
	defer restore()

	watching = true
	testFireExpiry(t, e)
	watching = false

	if !probed {
		t.Fatal("expireSession never loaded the session record, so the fence was never observed")
	}
	if !probeRefused {
		t.Fatal("the record fence was NOT held across the load: the superseded decision is being made on an unfenced sample")
	}
	// The ordinary outcome: nothing was armed, so the discard completes.
	if a := e.parkedAction(); a != nil {
		t.Fatalf("nothing was unresolved, so nothing may be parked, got %+v", a)
	}
	if e.recordPresent(t) {
		t.Fatal("the expiry must delete the record")
	}
	if e.resident() {
		t.Fatal("the expiry must remove the session")
	}
}

// T10c: ONE uninterrupted transaction spans the load AND the decision — pinned by
// transaction IDENTITY, not merely by "some transaction is open".
//
// T10b proves only that the fence is held when the load BEGINS, and a fence-refusal probe
// at the decision does not close that gap: tryBeginRecordTx reports only that the
// acquisition failed — never which transaction is open, and (since waiter priority) not
// even that one is, since an announced blocking writer refuses it on a free fence. So an
// implementation that released the load's transaction and opened a fresh one before
// deciding would pass both. So this reads sess.recordTxID at each
// seam and requires them EQUAL and non-zero. That identity changes on every open
// (recordTxSeqno increments in openRecordTxLocked), which is what makes a release-and-reopen
// between the two seams detectable.
//
// This states the branch's CONTRACT. It is not a claim that the state is production
// reachable — a genuinely superseded record coinciding with local uncertainty is what the
// guard is for, and its reachability is argued in the plan, not here.
//
// Mutation MU-I2a: the fence covers only the load; the decision is made after releasing it.
//
// Reported SURVIVOR, MU-I2b: leaving the sess.seq sample outside the fence, where it sat
// BEFORE this change, is not deterministically killable — the pre-fence sample normally
// equals the later value, so no assertion here can discriminate it. This change moves the
// read inside the fence on the argument that the window is real, not on this test.
func TestMQTTExpiryDecidesSupersededUnderTheSameTransaction(t *testing.T) {
	e, _ := testExpiringEnv(t, "client-expiry-fenced-decision", "expiry.fenced.decision")

	// THE OBSERVATIONS ARE GUARDED, because the load hook is GLOBAL.
	//
	// setMQTTTestLoadLastMsgHook installs one hook for the whole process, so EVERY goroutine
	// that calls loadLastMsgFor enters this closure — including a drain-reconcile timer
	// belonging to some other test running beside this one. The captured variables below were
	// plain, so that timer read and wrote them concurrently with this test's own expiry
	// goroutine, and `-race` reported it:
	//
	//	Read at ... schedulePublishDrainReconcile.func1 -> drainPublishAction ->
	//	              loadInflightRecord -> loadLastMsgFor -> this closure
	//	Previous write at ... testFireExpiry -> expireSession -> expireSessionFired ->
	//	              loadSessionMsg -> loadLastMsgFor -> this closure
	//
	// The mutex changes NO assertion. It makes the fixture's own bookkeeping safe against the
	// concurrency its hook is exposed to, which is a property the assertions always assumed and
	// nothing established.
	var obsMu sync.Mutex
	txAtLoad := uint64(0)
	loadSeen := false
	restoreLoad := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if stream != mqttSessStreamName ||
			subject != mqttSessStreamSubjectPrefix+e.asm.domainTk+e.sess.idHash {
			return nil
		}
		obsMu.Lock()
		if loadSeen {
			obsMu.Unlock()
			return nil
		}
		loadSeen = true
		obsMu.Unlock()
		e.sess.mu.Lock()
		tx := e.sess.recordTxID
		e.sess.mu.Unlock()
		obsMu.Lock()
		txAtLoad = tx
		obsMu.Unlock()
		return nil
	})
	defer restoreLoad()

	fired := false
	txAtDecision := uint64(0)
	loadDone := false
	restore := setMQTTTestExpireDecisionHook(func(sess *mqttSession, loadedSeq, seq uint64, superseded bool) {
		if sess != e.sess {
			return
		}
		obsMu.Lock()
		if fired {
			obsMu.Unlock()
			return
		}
		fired = true
		loadDone = loadedSeq != 0
		obsMu.Unlock()
		sess.mu.Lock()
		tx := sess.recordTxID
		sess.mu.Unlock()
		obsMu.Lock()
		txAtDecision = tx
		obsMu.Unlock()
		if superseded {
			t.Errorf("the fixture must not be superseded: loaded=%d ours=%d", loadedSeq, seq)
		}
	})
	defer restore()

	testFireExpiry(t, e)

	obsMu.Lock()
	defer obsMu.Unlock()
	if !loadSeen {
		t.Fatal("expireSession never loaded the session record, so the load's transaction was never observed")
	}
	if !fired {
		t.Fatal("the superseded decision point was never reached")
	}
	if !loadDone {
		t.Fatal("the decision was made before the record was loaded")
	}
	if txAtLoad == 0 {
		t.Fatal("no record transaction was open at the load: the superseded decision reads an unfenced sample")
	}
	if txAtDecision == 0 {
		t.Fatal("no record transaction was open at the decision: the fence was released before it")
	}
	if txAtLoad != txAtDecision {
		t.Fatalf("the load and the decision ran in DIFFERENT transactions (%d then %d): the fence was released and retaken, so the decision's inputs were not protected as one",
			txAtLoad, txAtDecision)
	}
}
