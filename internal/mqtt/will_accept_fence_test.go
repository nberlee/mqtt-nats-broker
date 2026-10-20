// The claimant side of acceptance facts: what a delayed Will's claimant does with
// them, and what it persists about the decision it took.
//
// Every test here drives tryFireWill (or the enqueue's early cancel) DIRECTLY on a
// planted record rather than waiting for the claim loop. FireAt is far in the future
// throughout, which keeps the background loop out of the record: tryFireWill itself
// does not test FireAt — dueness is the scan's question — so driving it directly is
// the same code path the loop reaches, minus the timing.

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// mqttWillQClientFilter matches every outbox record for one client hash. Production
// no longer needs it: the reconnect-time cancellation walk that used it is gone, and
// suppression is now decided by the claimant's acceptance-fact scan. It survives as a
// TEST helper, because "nothing is enqueued for this client" is still a thing tests
// need to assert. It lives in an untagged file so both the tagged and untagged Will
// suites can see it.
func mqttWillQClientFilter(domainTk, cidHash string) string {
	return mqttWillQSubjectPrefix + domainTk + cidHash + ".*"
}

// testWillFor builds a valid delayed-Will outbox record for a client.
func testWillFor(asm *mqttAccountSessionManager, clientID, willID string, fireAt int64, originSeq uint64) *mqttWillOutbox {
	return &mqttWillOutbox{
		WillID:           willID,
		ClientIDHash:     getHash(clientID),
		ClientID:         clientID,
		Gen:              mqttNewSessGen(),
		Topic:            "will/fence",
		Msg:              []byte(willID),
		FireAt:           fireAt,
		State:            mqttWillPending,
		OriginSessionSeq: originSeq,
		OriginSessEpoch:  asm.sessStreamEpoch,
	}
}

// testSeedWill stores a record on its own subject and returns both.
func testSeedWill(t testing.TB, asm *mqttAccountSessionManager, rec *mqttWillOutbox) (string, uint64) {
	t.Helper()
	subject := mqttWillQSubject(asm.domainTk, rec.ClientIDHash, rec.WillID)
	seq, err := asm.jsa.storeWillOutboxCAS(subject, 0, rec)
	if err != nil {
		t.Fatalf("seed the outbox record: %v", err)
	}
	return subject, seq
}

// testCapturePublishes replaces the delayed-Will publication with a counter, so a
// test asserts on the EXTERNAL act rather than on whatever the record ended up
// saying. Returns the counter.
func testCapturePublishes(t testing.TB, asm *mqttAccountSessionManager) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	asm.mu.Lock()
	asm.testPublishWill = func(*mqttWillOutbox) error {
		n.Add(1)
		return nil
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testPublishWill = nil
		asm.mu.Unlock()
	})
	return &n
}

// testReloadWill returns the current record on a subject.
func testReloadWill(t testing.TB, asm *mqttAccountSessionManager, subject string) (*mqttWillOutbox, uint64) {
	t.Helper()
	rec, seq, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
	if err != nil {
		t.Fatalf("reload the outbox record on %q: %v", subject, err)
	}
	return rec, seq
}

// testLicenseWill puts a record into a LICENSED shape: the decision and the cutoff
// it was taken under, alongside the state. The legal matrix refuses a terminal
// record that carries no decision, so a fixture that only set the state would be
// refused at decode rather than acted on.
func testLicenseWill(rec *mqttWillOutbox, state mqttWillState, dec mqttWillDecision, cutoff uint64) {
	rec.State = state
	rec.Decision = dec
	c := cutoff
	rec.DecisionCutoff = &c
}

// testQuietClaimLoop stops this account's BACKGROUND claim loop and lets the claimant
// the test drives directly keep running.
//
// Every budget-exhaustion scenario leaves a reclaimable record by construction — the
// budget is two leases, so the lease always expires first — and a live loop on the
// same account will reclaim it, license it and reap it while the test is still
// asserting. Quieting the loop is what makes the assertion about the claimant under
// test rather than about a race with its own broker.
// EVERY broker sharing the account has to be quieted, not just the one under test:
// they all scan the same $MQTT_willq filter, so a peer's loop reclaims an
// expired-lease record just as readily.
func testQuietClaimLoop(t testing.TB, envs ...*testTeardownEnv) {
	t.Helper()
	none := []<-chan struct{}{}
	for _, e := range envs {
		close(e.s.willClaimStopCh)
		e.asm.mu.Lock()
		e.asm.testClaimStop = &none
		e.asm.mu.Unlock()
		t.Cleanup(func() {
			e.asm.mu.Lock()
			e.asm.testClaimStop = nil
			e.asm.mu.Unlock()
		})
	}
}

// testFarFuture is a FireAt no scan will ever call due.
func testFarFuture() int64 { return time.Now().Add(time.Hour).UnixNano() }

// testSessStreamFloor stores two throwaway acceptance facts for unrelated clients
// and returns the FIRST one's sequence, to be used as a Will's origin.
//
// That is the production shape, and getting it wrong makes a fixture prove nothing:
// a Will's origin IS the session record of the connection that enqueued it, so every
// acceptance able to supersede it necessarily commits later. A Will whose origin sat
// at or above the facts under test would be non-qualifying for a reason that has
// nothing to do with what the test is about. The SECOND filler guarantees the stream
// has a sequence strictly above the origin, so the scan issues at least one read.
//
// They belong to other clients, so they also prove the walk's filter is
// client-scoped: neither ever qualifies.
func testSessStreamFloor(t testing.TB, asm *mqttAccountSessionManager) uint64 {
	t.Helper()
	_, first := testPlantAcceptFact(t, asm, "unrelated-a-"+nuid.Next(), 1)
	testPlantAcceptFact(t, asm, "unrelated-b-"+nuid.Next(), 1)
	return first
}

// testWaitFor polls fn until it stops erroring or the budget runs out. The publisher
// worker is dispatched to its own goroutine, so every assertion about an EXTERNAL
// publication has to wait for it.
func testWaitFor(t testing.TB, total, step time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(total)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(step)
	}
	t.Fatalf("condition never held within %v: %v", total, err)
}

// ---------------------------------------------------------------------------
// T1 / T3: the predicate, through the claimant
// ---------------------------------------------------------------------------

// TestWillClaimantSuppressesOnlyForAnAcceptanceBeforeItWasDue is T1 and T3 together,
// because they are two columns of one table and separating them would let a fixture
// carry either result.
//
// The dated term is the reason the previous design had to be replaced. A reconnect
// DURING the Will Delay suppresses ([MQTT-3.1.3-9]); once the delay has elapsed the
// Will is owed and must be published ([MQTT-3.1.2-8]). A watermark, and the direct
// reconnect cancel, could only ever answer the first.
func TestWillClaimantSuppressesOnlyForAnAcceptanceBeforeItWasDue(t *testing.T) {
	for _, tc := range []struct {
		name       string
		plant      bool
		factOffset time.Duration // relative to FireAt
		wantCancel bool
	}{
		{"no acceptance at all publishes", false, 0, false},
		{"an acceptance before the Will was due suppresses", true, -time.Minute, true},
		{"an acceptance exactly when it fell due does not", true, 0, false},
		{"an acceptance after it fell due does not", true, time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := testNewTeardownEnv(t)
			pubs := testCapturePublishes(t, a.asm)
			cid := "due-" + t.Name()
			fireAt := testFarFuture()
			origin := testSessStreamFloor(t, a.asm)
			if tc.plant {
				testPlantAcceptFact(t, a.asm, cid, fireAt+int64(tc.factOffset))
			}
			rec := testWillFor(a.asm, cid, "due-w", fireAt, origin)
			subject, seq := testSeedWill(t, a.asm, rec)

			a.asm.tryFireWill(a.s, subject, rec, seq)

			cur, _ := testReloadWill(t, a.asm, subject)
			if cur == nil {
				t.Fatal("the record must still exist")
			}
			if tc.wantCancel {
				if cur.State != mqttWillCancelled || cur.Decision != mqttWillDecisionCancel || cur.DecisionCutoff == nil {
					t.Fatalf("want a LICENSED cancel, got state=%q decision=%q cutoff=%v",
						cur.State, cur.Decision, cur.DecisionCutoff)
				}
				if pubs.Load() != 0 {
					t.Fatalf("a suppressed Will must not be published, got %d publications", pubs.Load())
				}
				return
			}
			if !cur.licensedToPublish() && cur.State != mqttWillDone {
				t.Fatalf("want a LICENSED publish, got state=%q decision=%q cutoff=%v",
					cur.State, cur.Decision, cur.DecisionCutoff)
			}
		})
	}
}

// TestWillEarlyCancelUsesTheSamePredicate is T1's second claim route: the enqueue's
// best-effort early cancel.
//
// It licenses a cancel from PENDING with a single CAS and takes no claim, which is
// not an omission. A claim exists so a claimant that may publish cannot have its
// decision re-taken after the irreversible act; this path never publishes. Claiming
// here would leave a publishing record behind whenever no fact qualified — and the
// recovery arm fires a publishing record regardless of FireAt, so a best-effort
// optimization would have become a way to publish a Will before it was due. That
// last part is what the second subtest pins.
func TestWillEarlyCancelUsesTheSamePredicate(t *testing.T) {
	t.Run("a qualifying acceptance licenses the cancel", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const cid = "early-cancel"
		fireAt := testFarFuture()
		origin := testSessStreamFloor(t, a.asm)
		testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))
		rec := testWillFor(a.asm, cid, "early-w", fireAt, origin)
		subject, seq := testSeedWill(t, a.asm, rec)

		a.asm.earlyCancelIfSuperseded(a.s, subject, rec, seq)

		cur, _ := testReloadWill(t, a.asm, subject)
		if cur == nil || cur.State != mqttWillCancelled || cur.Decision != mqttWillDecisionCancel || cur.DecisionCutoff == nil {
			t.Fatalf("want a LICENSED cancel straight from pending, got %+v", cur)
		}
	})

	t.Run("no acceptance leaves the record PENDING, never publishing", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const cid = "early-nocancel"
		origin := testSessStreamFloor(t, a.asm)
		rec := testWillFor(a.asm, cid, "early-w2", testFarFuture(), origin)
		subject, seq := testSeedWill(t, a.asm, rec)

		a.asm.earlyCancelIfSuperseded(a.s, subject, rec, seq)

		cur, curSeq := testReloadWill(t, a.asm, subject)
		if cur == nil || cur.State != mqttWillPending {
			t.Fatalf("the early cancel must leave an unsuppressed record PENDING, got %+v: a publishing "+
				"record is recovered on lease expiry with no FireAt test at all", cur)
		}
		if curSeq != seq {
			t.Fatalf("the early cancel must have written nothing: sequence moved %d -> %d", seq, curSeq)
		}
	})
}

// TestWillClaimantSeesAnEarlierAcceptanceAfterALaterOne is T4.
//
// This is F-031 itself. A single-slot watermark is overwritten by the next
// acceptance, so an earlier entitled one is erased and unrecoverable. Facts have
// unique subjects, so the set only grows — and the SCAN must therefore keep looking
// past a non-qualifying fact rather than stopping at the newest.
func TestWillClaimantSeesAnEarlierAcceptanceAfterALaterOne(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "two-acceptances"
	fireAt := testFarFuture()

	// The entitled acceptance first, then a later one that is NOT entitled (it
	// happened after the Will fell due). A slot would now hold only the second.
	origin := testSessStreamFloor(t, a.asm)
	testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))
	testPlantAcceptFact(t, a.asm, cid, fireAt+int64(time.Minute))

	rec := testWillFor(a.asm, cid, "two-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)
	a.asm.tryFireWill(a.s, subject, rec, seq)

	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.State != mqttWillCancelled {
		t.Fatalf("the EARLIER entitled acceptance must still suppress, got %+v", cur)
	}
	if pubs.Load() != 0 {
		t.Fatalf("the Will must not have been published, got %d", pubs.Load())
	}
}

// TestWillClaimantCrossEpoch is T11.
func TestWillClaimantCrossEpoch(t *testing.T) {
	t.Run("a current-epoch acceptance supersedes a Will from another incarnation", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const cid = "epoch-super"
		fireAt := testFarFuture()
		testSessStreamFloor(t, a.asm)
		testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))

		rec := testWillFor(a.asm, cid, "epoch-w", fireAt, 1)
		// Another incarnation, and an origin sequence ABOVE anything this stream has:
		// an unconditional OriginSessionSeq+1 scan start would find nothing at all.
		rec.OriginSessEpoch = mqttGenSessEpoch()
		rec.OriginSessionSeq = 1 << 40
		subject, seq := testSeedWill(t, a.asm, rec)
		a.asm.tryFireWill(a.s, subject, rec, seq)

		cur, _ := testReloadWill(t, a.asm, subject)
		if cur == nil || cur.State != mqttWillCancelled {
			t.Fatalf("a current-incarnation acceptance supersedes a Will of another incarnation "+
				"outright, and the scan must start at the beginning of the stream rather than at an "+
				"origin sequence that names a stream that no longer exists: got %+v", cur)
		}
	})

	t.Run("a stale-epoch acceptance says nothing", func(t *testing.T) {
		a, _ := testNewTeardownEnv(t)
		const cid = "epoch-stale"
		fireAt := testFarFuture()
		// Planted under a DIFFERENT incarnation, so it is not even in the filter.
		stale := &mqttAcceptFact{ClientID: cid, SessEpoch: mqttGenSessEpoch(), AcceptID: mqttNewAcceptID(), At: fireAt - 1}
		if !mqttValidSessEpoch(stale.SessEpoch) {
			t.Fatal("fixture epoch must be well-formed")
		}
		rec := testWillFor(a.asm, cid, "epoch-w2", fireAt, 1)
		// The sequence must sit ABOVE the Will's origin, or the origin term rejects the
		// fact on its own and the epoch term is never what decides — the fixture then
		// passes against a predicate carrying no epoch term at all. Every other term is
		// deliberately satisfied here so the epoch is the only thing left to reject it.
		if mqttAcceptFactQualifies(stale, rec.OriginSessionSeq+98, a.asm.sessStreamEpoch, rec) {
			t.Fatal("an acceptance from another incarnation must never qualify, and the predicate must " +
				"re-test the epoch itself rather than trusting the walk's filter to have excluded it")
		}
	})
}

// TestWillRenewalPersistsTheFloorWithItsIncarnation pins the WRITE side of the
// floor/epoch pair, which nothing else covered.
//
// TestWillScanFloorAndEpochAreOneFact hand-builds records and calls the validator, so it
// proves the illegal shape is refused — not that the renewal never produces it. A
// renewal writing ScanFrom alone yields a floor no reader can use: usableScanFloor
// discards a floor whose epoch does not match, so progress silently stops persisting and
// every pass re-reads the same prefix. That is the wedge, reached from the write side.
func TestWillRenewalPersistsTheFloorWithItsIncarnation(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	testCapturePublishes(t, a.asm)
	testQuietClaimLoop(t, a, b)
	const cid = "floor-write"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	for range 6 {
		testPlantAcceptFact(t, a.asm, cid, fireAt+int64(time.Hour)) // none can qualify
	}
	rec := testWillFor(a.asm, cid, "floorw-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	restore := testShortenWillLease(t, a.asm, 30*time.Millisecond)
	defer restore()

	var renewals atomic.Int64
	var badFloor atomic.Int64
	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		time.Sleep(6 * time.Millisecond) // let the 10ms cadence fall due inside the walk
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.testClaimStore = func(subj string, expect uint64, r *mqttWillOutbox) (uint64, error) {
		if r.State == mqttWillPublishing && r.Decision == mqttWillDecisionUnset && r.ScanFrom != 0 {
			renewals.Add(1)
			// The pair, checked as a pair: a floor without its incarnation is unusable,
			// and validateWillOutboxBody refuses the shape outright.
			if r.ScanEpoch != a.asm.sessStreamEpoch {
				badFloor.Add(1)
			}
			if err := validateWillOutboxBody(r); err != nil {
				badFloor.Add(1)
			}
		}
		return a.asm.jsa.storeWillOutboxCAS(subj, expect, r)
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if renewals.Load() == 0 {
		t.Fatal("fixture: no renewal persisted a floor, so the write side was never exercised")
	}
	if n := badFloor.Load(); n != 0 {
		t.Fatalf("%d renewal(s) persisted a scan floor without its incarnation: usableScanFloor "+
			"discards such a floor, so the scan restarts at the same place on every pass and an owed "+
			"Will is never decided", n)
	}
}

// ---------------------------------------------------------------------------
// T7 / T16: what a failure to read the evidence costs
// ---------------------------------------------------------------------------

// TestWillClaimantAbortsThePassOnATransientFactOutage is T7's transient half and T16.
//
// TWO expired records with the failure injected on the FIRST. A single-record test
// false-passes against an implementation that discards the abort result: the
// difference only shows up in whether the pass touches the SECOND record. That
// discarded return was the defect — an outage cost one JetStream timeout per expired
// record instead of one per pass.
func TestWillClaimantAbortsThePassOnATransientFactOutage(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "abort-pass"
	origin := testSessStreamFloor(t, a.asm)

	// Two records whose leases have already expired, so the scan's RECOVERY arm is
	// what reaches them.
	var subjects []string
	for _, wid := range []string{"abort-w1", "abort-w2"} {
		rec := testWillFor(a.asm, cid, wid, testFarFuture(), origin)
		rec.State = mqttWillPublishing
		rec.ClaimOwner = "a-dead-broker"
		rec.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
		subject, _ := testSeedWill(t, a.asm, rec)
		subjects = append(subjects, subject)
	}

	var loads atomic.Int64
	a.asm.mu.Lock()
	a.asm.testFactLoad = func(string, uint64) (*StoredMsg, error) {
		loads.Add(1)
		return nil, errors.New("mqtt test: acceptance-fact read unavailable")
	}
	a.asm.mu.Unlock()

	a.asm.scanWillOutbox(a.s)

	if got := loads.Load(); got != 1 {
		t.Fatalf("a transient fact-read failure must stop the PASS: %d fact reads were issued, want 1 "+
			"(one per record means an outage costs one timeout per record)", got)
	}
	// Neither record may have been published or cancelled: nothing was decidable.
	for _, subject := range subjects {
		cur, _ := testReloadWill(t, a.asm, subject)
		if cur == nil || cur.State != mqttWillPublishing {
			t.Fatalf("an undecidable record must be left publishing for lease-expiry recovery, got %+v", cur)
		}
		if cur.Decision != mqttWillDecisionUnset {
			t.Fatalf("nothing may be licensed without reading the evidence, got decision %q", cur.Decision)
		}
	}
}

// TestWillPendingArmAbortsThePassOnATransientFactOutage is the sibling of the test above,
// on the arm it does NOT reach.
//
// The two arms are separate `== willScanAbort` tests on separate lines, and the recovery
// test uses expired PUBLISHING records, so a mutation discarding the PENDING arm's result
// survived it untouched. Two DUE PENDING records are what distinguish them.
func TestWillPendingArmAbortsThePassOnATransientFactOutage(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "abort-pending"
	origin := testSessStreamFloor(t, a.asm)

	// Due, and PENDING: the first arm of the walk's switch.
	var subjects []string
	for _, wid := range []string{"abortp-w1", "abortp-w2"} {
		rec := testWillFor(a.asm, cid, wid, time.Now().Add(-time.Millisecond).UnixNano(), origin)
		subject, _ := testSeedWill(t, a.asm, rec)
		subjects = append(subjects, subject)
	}

	var loads atomic.Int64
	a.asm.mu.Lock()
	a.asm.testFactLoad = func(string, uint64) (*StoredMsg, error) {
		loads.Add(1)
		return nil, errors.New("mqtt test: acceptance-fact read unavailable")
	}
	a.asm.mu.Unlock()

	a.asm.scanWillOutbox(a.s)

	if got := loads.Load(); got != 1 {
		t.Fatalf("a transient fact-read failure must stop the PASS from the pending arm too: %d fact "+
			"reads were issued, want 1 — one per due record turns a brief outage into a scan that "+
			"never finishes", got)
	}
	for _, subject := range subjects {
		cur, _ := testReloadWill(t, a.asm, subject)
		if cur == nil {
			t.Fatalf("an undecidable record must survive the pass, got nil for %q", subject)
		}
		if cur.Decision != mqttWillDecisionUnset {
			t.Fatalf("nothing may be licensed without reading the evidence, got decision %q", cur.Decision)
		}
	}
}

// ---------------------------------------------------------------------------
// T12a / T12b: the renewal
// ---------------------------------------------------------------------------

// TestWillClaimantRenewalLoss is T12a.
//
// The discriminating assertions are the CALL COUNTS. "A neither published nor
// cancelled" alone false-passes: once recovery has advanced the record, A's final
// licensing CAS is stale and fails anyway, so that outcome is produced by both the
// correct implementation and one that ignores the renewal failure entirely.
func TestWillClaimantRenewalLoss(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	// The background claim loops invoke the SAME instance seams from their own
	// goroutines, so without this the counters below are shared mutable state and the
	// assertions are about a race rather than about this claimant.
	testQuietClaimLoop(t, a, b)
	const cid = "renewal-loss"

	// Enough facts that the scan takes more than one read, so a renewal falls due
	// inside it.
	origin := testSessStreamFloor(t, a.asm)
	for range 4 {
		testPlantAcceptFact(t, a.asm, cid, testFarFuture()+int64(time.Hour))
	}
	rec := testWillFor(a.asm, cid, "renewal-w", testFarFuture(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	// Short enough that the renewal cadence (lease/3) falls due inside the scan, but
	// not so short that the budget (2 x lease) can expire while merely sampling the
	// cutoff — that would defer for the wrong reason and prove nothing about renewal.
	// The renewal itself then fails, standing in for a record a recovery claimant has
	// advanced underneath us.
	restore := testShortenWillLease(t, a.asm, 60*time.Millisecond)
	defer restore()

	var licensing, factLoads atomic.Int64
	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		factLoads.Add(1)
		time.Sleep(25 * time.Millisecond) // let the renewal cadence fall due
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	var claims atomic.Int64
	a.asm.testClaimStore = func(subj string, expect uint64, r *mqttWillOutbox) (uint64, error) {
		if claims.Add(1) == 1 {
			return a.asm.jsa.storeWillOutboxCAS(subj, expect, r) // the initial claim
		}
		if r.State != mqttWillPublishing || r.Decision != mqttWillDecisionUnset {
			licensing.Add(1) // a licensing CAS
			return a.asm.jsa.storeWillOutboxCAS(subj, expect, r)
		}
		if r.Decision != mqttWillDecisionUnset {
			licensing.Add(1)
		}
		return 0, errors.New("mqtt test: the renewal CAS is rejected")
	}
	a.asm.mu.Unlock()

	loadsBeforeRenewal := int64(0)
	_ = loadsBeforeRenewal
	a.asm.tryFireWill(a.s, subject, rec, seq)

	if pubs.Load() != 0 {
		t.Fatalf("a claimant that lost its renewal must not publish, got %d", pubs.Load())
	}
	if licensing.Load() != 0 {
		t.Fatalf("a claimant that lost its renewal must issue NO licensing CAS, got %d: "+
			"\"it did not publish\" alone is produced by a stale final CAS too", licensing.Load())
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.Decision != mqttWillDecisionUnset {
		t.Fatalf("nothing may be licensed after an unconfirmed renewal, got %+v", cur)
	}
}

// TestWillClaimantRenewsWhileScanning is T12b.
//
// T12a cannot kill "omit renewal entirely": with the record advanced, the final
// licensing CAS is stale either way. So this asserts the renewal LANDED and that it
// pushed the lease out — the only observation that distinguishes a claimant holding
// its claim from one that has silently let it lapse mid-scan.
func TestWillClaimantRenewsWhileScanning(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	testCapturePublishes(t, a.asm)
	testQuietClaimLoop(t, a, b)
	const cid = "renewal-happens"
	origin := testSessStreamFloor(t, a.asm)
	for range 4 {
		testPlantAcceptFact(t, a.asm, cid, testFarFuture()+int64(time.Hour))
	}
	rec := testWillFor(a.asm, cid, "renew-w", testFarFuture(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	restore := testShortenWillLease(t, a.asm, 30*time.Millisecond)
	defer restore()

	var renewals atomic.Int64
	var firstLease, lastLease atomic.Int64
	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		time.Sleep(6 * time.Millisecond) // let the cadence fall due inside the scan
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.testClaimStore = func(subj string, expect uint64, r *mqttWillOutbox) (uint64, error) {
		if r.State == mqttWillPublishing && r.Decision == mqttWillDecisionUnset {
			if firstLease.Load() == 0 {
				firstLease.Store(r.ClaimUntil)
			} else {
				renewals.Add(1)
				lastLease.Store(r.ClaimUntil)
			}
		}
		return a.asm.jsa.storeWillOutboxCAS(subj, expect, r)
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if renewals.Load() == 0 {
		t.Fatal("a scan longer than the renewal cadence must renew its claim, or a peer reclaims the " +
			"record underneath the claimant that is still deciding it")
	}
	if lastLease.Load() <= firstLease.Load() {
		t.Fatalf("a renewal must push the lease out: first=%d last=%d", firstLease.Load(), lastLease.Load())
	}
}

// testShortenWillLease shortens ONE account manager's claim lease (and with it the
// derived renewal cadence and scan budget).
//
// Instance-scoped deliberately: the package variable is read by every live claim
// loop on its own goroutine, so a test that assigned it would be racing every other
// test's broker rather than configuring its own.
func testShortenWillLease(t testing.TB, asm *mqttAccountSessionManager, lease time.Duration) func() {
	t.Helper()
	asm.mu.Lock()
	prev := asm.testClaimLease
	asm.testClaimLease = lease
	asm.mu.Unlock()
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		asm.mu.Lock()
		asm.testClaimLease = prev
		asm.mu.Unlock()
	}
	t.Cleanup(restore)
	return restore
}

// TestWillClaimantBudgetExhaustionLicensesNothing is T13.
//
// A renewed lease is not permission to act. The budget bounds how STALE the cutoff a
// decision was taken under may be, and the check immediately before the licensing
// CAS is what enforces it — a claimant that only checked the budget while scanning
// would renew its way past the bound and then license anyway.
func TestWillClaimantBudgetExhaustionLicensesNothing(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	testQuietClaimLoop(t, a, b)
	const cid = "budget"
	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "budget-w", testFarFuture(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	// A lease short enough that the budget (2x) expires while the single fact read
	// is in progress, but long enough that the claim itself lands first.
	restore := testShortenWillLease(t, a.asm, 20*time.Millisecond)
	defer restore()

	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		time.Sleep(60 * time.Millisecond) // past 2 x lease
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if pubs.Load() != 0 {
		t.Fatalf("a claimant past its scan budget must license nothing, got %d publications", pubs.Load())
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.State != mqttWillPublishing || cur.Decision != mqttWillDecisionUnset {
		t.Fatalf("an exhausted budget defers: the record stays an UNLICENSED claim for lease-expiry "+
			"recovery, got %+v", cur)
	}
}

// ---------------------------------------------------------------------------
// T15: the decision survives recovery, and is not re-taken
// ---------------------------------------------------------------------------

// TestWillRecoveryReusesThePersistedDecision is T15.
//
// Staged so that RE-DECIDING would differ: A licenses a publish under a cutoff, and
// a qualifying acceptance then lands ABOVE that cutoff. A recovering claimant that
// sampled a fresh cutoff would see it and cancel — the same Will reinterpreted from
// published to suppressed after the irreversible act.
//
// The discriminating assertion is the FACT-LOAD COUNT, not the publication: a
// recovery that rescanned and happened to find nothing publishes too. Here it would
// find something, and the count proves it never looked.
func TestWillRecoveryReusesThePersistedDecision(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "decision-reuse"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "reuse-w", fireAt, origin)
	subject, _ := testSeedWill(t, a.asm, rec)

	// A's licensed publish, under a cutoff taken before any acceptance existed, with
	// an EXPIRED lease: exactly what a claimant that crashed after publishing and
	// before the terminal commit leaves behind.
	licensed := *rec
	licensed.ClaimOwner = "a-dead-broker"
	licensed.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
	testLicenseWill(&licensed, mqttWillPublishing, mqttWillDecisionPublish, 1)
	_, curSeq := testReloadWill(t, a.asm, subject)
	licSeq, err := a.asm.jsa.storeWillOutboxCAS(subject, curSeq, &licensed)
	if err != nil {
		t.Fatalf("stage A's licensed publish: %v", err)
	}

	// The acceptance lands AFTER the cutoff: a fresh scan would cancel.
	testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))

	loads := testCountFactLoads(t, a.asm)
	reload, _ := testReloadWill(t, a.asm, subject)
	a.asm.tryFireWill(a.s, subject, reload, licSeq)

	if got := loads.Load(); got != 0 {
		t.Fatalf("a recovering claimant must reuse the persisted decision and read NO facts, got %d "+
			"reads: republishing alone would also be produced by a rescan that found nothing", got)
	}
	testWaitFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		if n := pubs.Load(); n != 1 {
			return fmt.Errorf("the persisted publish decision must be EXECUTED, got %d publications", n)
		}
		return nil
	})
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur != nil && cur.State == mqttWillCancelled {
		t.Fatal("a recovering claimant must never re-decide a Will another claimant may already have published")
	}
}

// TestWillFinalizeRequiresTheSameLicence pins the finalizer's half of the same
// invariant. The previous form retried against any current publishing record with
// the same WillID, which becomes unsafe once a claim can be UNLICENSED: it would
// have committed done over a record another claimant was still scanning under.
func TestWillFinalizeRequiresTheSameLicence(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "finalize-licence"
	rec := testWillFor(a.asm, cid, "fin-w", testFarFuture(), 1)
	subject, seq := testSeedWill(t, a.asm, rec)

	t.Run("an unlicensed record is not finalizable", func(t *testing.T) {
		unlicensed := *rec
		unlicensed.State = mqttWillPublishing
		if _, ok := a.asm.finalizeWillPublish(a.s, subject, &unlicensed, seq); ok {
			t.Fatal("a claim carrying no publish licence must never be committed terminal")
		}
	})

	t.Run("a differently licensed record is abandoned", func(t *testing.T) {
		mine, theirs := uint64(5), uint64(9)
		licensed := *rec
		licensed.State = mqttWillPublishing
		licensed.Decision = mqttWillDecisionPublish
		licensed.DecisionCutoff = &mine

		other := licensed
		other.DecisionCutoff = &theirs
		cur, curSeq := testReloadWill(t, a.asm, subject)
		_ = cur
		newSeq, err := a.asm.jsa.storeWillOutboxCAS(subject, curSeq, &other)
		if err != nil {
			t.Fatalf("stage the other claimant's licence: %v", err)
		}
		if _, ok := a.asm.finalizeWillPublish(a.s, subject, &licensed, newSeq-1); ok {
			t.Fatal("the same act under a DIFFERENT cutoff is a different licensing and must not be finalized")
		}
	})
}

// ---------------------------------------------------------------------------
// T14: shutdown between the two paired acceptance reads
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// The two NEGATIVE DEPENDENCY tests
// ---------------------------------------------------------------------------
//
// Both stage their state with an UNCONDITIONAL store, which bypasses the per-subject
// expectation every production write on `$MQTT_willq` carries. That is the whole
// point of them: they say what the claim protocol would do if the server's CAS
// contract did not hold, and they are the reason the protocol depends on it rather
// than on a re-check of its own.
//
// They are NOT evidence that the supported server admits either state. It does not: the
// routes by which a write conditioned on a superseded base could reach the stream across
// a leader transition are enumerated, fixtured and mutation-checked in `test/depproof/`,
// and every run measured refused the write. F-030 is closed by that evidence together
// with the pinned source mechanisms and the maintainer-confirmed leader-only contract;
// neither synthetic test reproduces an election window, and neither is cited as though
// it independently proves the server property.

// TestWillUnconditionalWriteDivergesClaimants: with the CAS contract bypassed, two
// claimants taken from one base diverge — A publishes externally while B durably
// cancels the same Will generation.
func TestWillUnconditionalWriteDivergesClaimants(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "cross-term"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "xterm-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	// A: no acceptance is visible under its cutoff, so it licenses a publish and
	// publishes.
	a.asm.tryFireWill(a.s, subject, rec, seq)
	testWaitFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		if n := pubs.Load(); n != 1 {
			return fmt.Errorf("A must publish externally, got %d", n)
		}
		return nil
	})

	// B: an acceptance lands, and B's licensing CAS is applied from the OLDER body —
	// the interleaving a leader transition permits. Written unconditionally, because
	// a same-base CAS cannot be made to succeed twice in-term.
	testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))
	bCancel := *rec
	testLicenseWill(&bCancel, mqttWillCancelled, mqttWillDecisionCancel, 1<<40)
	body, err := json.Marshal(&bCancel)
	if err != nil {
		t.Fatalf("marshal B's cancel: %v", err)
	}
	if _, err := a.asm.jsa.storeMsg(subject, -1, body); err != nil {
		t.Fatalf("stage B's cancel: %v", err)
	}

	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.State != mqttWillCancelled {
		t.Fatalf("B must have durably cancelled the same Will generation, got %+v", cur)
	}
	if pubs.Load() != 1 {
		t.Fatalf("and A's external publication stands: got %d", pubs.Load())
	}
}

// TestWillUnconditionalWriteRefiresAPublishedWill: with the CAS contract bypassed, a
// committed `done` taken back to `publishing` makes the record re-fireable, and the
// persisted decision makes the republication DETERMINISTIC rather than accidental.
//
// The zero-fact-load assertion is what makes "deterministic" mechanically explicit.
// A qualifying acceptance is staged ABOVE the persisted cutoff, so any illicit fresh
// scan would CANCEL rather than republish; without that guard a republication could
// false-pass through a rescan that happened to find nothing.
//
// This is why the terminal state's durability is a release-blocking property and not
// a nicety: the whole distance between "published once" and "published twice" is the
// server refusing this write. `TestMQTTWillOutboxReplicatedTerminalDoneStands` is the
// same shape against the real server, where the write IS refused.
func TestWillUnconditionalWriteRefiresAPublishedWill(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "terminal-regression"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "term-w", fireAt, origin)
	subject, _ := testSeedWill(t, a.asm, rec)

	licensed := *rec
	licensed.ClaimOwner = "broker-a"
	licensed.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
	testLicenseWill(&licensed, mqttWillPublishing, mqttWillDecisionPublish, 1)

	// `done` COMMITS first — the state that is supposed to retire the record.
	done := licensed
	done.State = mqttWillDone
	_, curSeq := testReloadWill(t, a.asm, subject)
	if _, err := a.asm.jsa.storeWillOutboxCAS(subject, curSeq, &done); err != nil {
		t.Fatalf("commit done: %v", err)
	}

	// A qualifying acceptance, above the persisted cutoff: an illicit rescan would
	// cancel rather than republish.
	testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))

	// The write applies AFTER it, from the older publishing body, preserving the
	// licence. Unconditional, which is what the supported server refuses.
	body, err := json.Marshal(&licensed)
	if err != nil {
		t.Fatalf("marshal the stale reclaim: %v", err)
	}
	if _, err := a.asm.jsa.storeMsg(subject, -1, body); err != nil {
		t.Fatalf("stage the stale reclaim: %v", err)
	}

	loads := testCountFactLoads(t, a.asm)
	regressed, regSeq := testReloadWill(t, a.asm, subject)
	if regressed == nil || regressed.State != mqttWillPublishing {
		t.Fatalf("the reclaim must have regressed the terminal state, got %+v", regressed)
	}
	a.asm.tryFireWill(a.s, subject, regressed, regSeq)

	if got := loads.Load(); got != 0 {
		t.Fatalf("the recovering claimant must execute the persisted decision without reading any "+
			"fact, got %d reads", got)
	}
	testWaitFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		if n := pubs.Load(); n != 1 {
			return fmt.Errorf("bypassing the CAS contract must give a DETERMINISTIC "+
				"republication after a committed done: got %d publications, want 1", n)
		}
		return nil
	})
}

// TestWillTimingInvariantHolds asserts the relationships the claim protocol depends
// on, rather than leaving them as properties of whatever the constants happen to be.
func TestWillTimingInvariantHolds(t *testing.T) {
	t.Parallel()
	// The PRODUCTION defaults, read as constants rather than through the package
	// variables: other tests shorten those deliberately, and a two-second lease
	// against a five-second request timeout is their point, not a defect.
	if err := mqttWillTimingInvariant(
		mqttWillDefaultClaimLease, mqttWillDefaultClaimInterval, mqttDefaultJSAPITimeout); err != nil {
		t.Fatalf("the production constants violate the claim protocol's timing invariant: %v", err)
	}
	// The derived quantities are ratios of whatever lease the manager resolves, so
	// these hold at the default AND at any override a test installs.
	as := &mqttAccountSessionManager{}
	// The contract is "0 resolves the PACKAGE VALUE", not "0 resolves 30s": other
	// tests shorten that value deliberately, so asserting the constant here would be
	// asserting their absence.
	if as.claimLease() != mqttWillClaimLease {
		t.Fatalf("an unconfigured manager must resolve the package value %v, got %v",
			mqttWillClaimLease, as.claimLease())
	}
	for _, lease := range []time.Duration{0, 90 * time.Millisecond} {
		as.testClaimLease = lease
		if as.renewCadence() >= as.claimLease() {
			t.Fatalf("the renewal cadence must sit strictly below the lease (lease override %v)", lease)
		}
		if as.scanBudget() <= as.claimLease() {
			t.Fatalf("the scan budget must exceed one lease, or a claimant can never renew inside it "+
				"(lease override %v)", lease)
		}
	}
	for _, tc := range []struct {
		name                     string
		lease, interval, timeout time.Duration
	}{
		{"a request timeout at the lease", time.Second, time.Millisecond, time.Second},
		{"a claim interval at the lease", time.Second, time.Second, time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := mqttWillTimingInvariant(tc.lease, tc.interval, tc.timeout); err == nil {
				t.Fatal("this relationship must be reported: a publish could outlive the lease it was " +
					"licensed under, or a live publisher be reclaimed by the very next scan")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Round-1 review findings
// ---------------------------------------------------------------------------

// TestWillClaimMintsAFreshLease pins that a claim's lease is minted from the clock at
// CLAIM time, not from one sampled at the top of the pass.
//
// The walk used to hand a single timestamp down to every record. One record's
// acceptance-fact scan can consume the whole scan budget, so a later record's claim
// was minted with a ClaimUntil ALREADY in the past — reclaimable by a peer the
// instant it landed, which puts two brokers on one licensed publish. That is not the
// sanctioned duplicate, and the difference is not whether anything crashed: a lease
// already in the past was never an ownership window at all, whereas the sanctioned
// duplicate needs one minted correctly and then EXPIRED.
func TestWillClaimMintsAFreshLease(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	testCapturePublishes(t, a.asm)
	const cid = "fresh-lease"
	origin := testSessStreamFloor(t, a.asm)

	// A lease far shorter than the delay the first record's scan will impose.
	restore := testShortenWillLease(t, a.asm, 150*time.Millisecond)
	defer restore()

	var subjects []string
	for _, wid := range []string{"lease-w1", "lease-w2"} {
		rec := testWillFor(a.asm, cid, wid, time.Now().Add(-time.Millisecond).UnixNano(), origin)
		subject, _ := testSeedWill(t, a.asm, rec)
		subjects = append(subjects, subject)
	}

	var reads atomic.Int64
	// Observed AT the claim, not by reloading afterwards. Reloading cannot see this: a
	// claim minted with an expired lease still lands, so the record goes on to be
	// licensed, published and reaped, and every reload-based assertion then escapes
	// through its own "already terminal" arm. The mutation that mints a past lease
	// survived exactly that way.
	var claims, expired atomic.Int64
	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		// Only the FIRST record's scan is slowed, so the second record's claim is the
		// one the assertion is about.
		if reads.Add(1) == 1 {
			time.Sleep(400 * time.Millisecond) // > 2 leases
		}
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.testClaimStore = func(subj string, expect uint64, r *mqttWillOutbox) (uint64, error) {
		if r.State == mqttWillPublishing && r.ClaimOwner != _EMPTY_ {
			claims.Add(1)
			if r.ClaimUntil <= time.Now().UnixNano() {
				expired.Add(1)
			}
		}
		return a.asm.jsa.storeWillOutboxCAS(subj, expect, r)
	}
	a.asm.mu.Unlock()

	a.asm.scanWillOutbox(a.s)

	if claims.Load() < 2 {
		t.Fatalf("fixture: both records must be claimed for the assertion to mean anything, got %d",
			claims.Load())
	}
	if n := expired.Load(); n != 0 {
		t.Fatalf("%d record(s) were claimed with a lease that had ALREADY expired: a peer can reclaim "+
			"such a record the instant it lands, putting two brokers on one licensed publish", n)
	}
	for _, subject := range subjects {
		cur, _ := testReloadWill(t, a.asm, subject)
		if cur == nil || cur.ClaimOwner == _EMPTY_ {
			continue // terminal and reaped, or never claimed
		}
		if cur.ClaimUntil <= time.Now().UnixNano() {
			t.Fatalf("record %q still holds a lease that expired (until=%d, now=%d)",
				subject, cur.ClaimUntil, time.Now().UnixNano())
		}
	}
}

// TestWillAheadPhaseIsBoundedByItsCaller pins that the early-cancellation phase's
// budget bounds the work each iteration STARTS, not merely the loop.
//
// The nested fact scan carries its own two-lease budget. A budget that bounds only
// the loop is not a budget: one client with a long acceptance history would hold the
// claim loop for a full scan budget, and every Will falling due meanwhile would fire
// late on every replica without a local timer.
func TestWillAheadPhaseIsBoundedByItsCaller(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "ahead-bound"
	origin := testSessStreamFloor(t, a.asm)
	testPlantAcceptFact(t, a.asm, cid, time.Now().UnixNano())

	rec := testWillFor(a.asm, cid, "bound-w", testFarFuture(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	loads := testCountFactLoads(t, a.asm)
	// A bound already in the past: nothing may be started at all.
	a.asm.cancelIfSuperseded(a.s, subject, rec, seq, time.Now().Add(-time.Second))
	if n := loads.Load(); n != 0 {
		t.Fatalf("a caller bound already in the past must start no work, got %d fact read(s)", n)
	}
	cur, curSeq := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.State != mqttWillPending || curSeq != seq {
		t.Fatalf("nothing may have been written, got %+v at %d", cur, curSeq)
	}

	// The control: with no caller bound the SAME call suppresses, so the row above is
	// about the bound and not about the fixture.
	a.asm.cancelIfSuperseded(a.s, subject, rec, seq, time.Time{})
	if cur, _ = testReloadWill(t, a.asm, subject); cur == nil || cur.State != mqttWillCancelled {
		t.Fatalf("unbounded, the same call must license a cancel, got %+v", cur)
	}
}

// TestWillAheadPhaseStopsOnShutdown pins the labelled break. An unlabelled break
// inside a select leaves the SELECT, not the loop, so the phase carries on right
// through shutdown.
//
// The assertion is the CUTOFF SAMPLE COUNT, and choosing it took a mutation to find.
// "No fact was read" and "nothing was written" both hold either way: the nested scan
// has its own stop check and defers before its first read. What the loop-level break
// actually prevents is one JetStream round trip PER RECORD to sample a cutoff that
// will immediately be thrown away — which on a large backlog is the difference
// between leaving promptly and walking the whole account first.
func TestWillAheadPhaseStopsOnShutdown(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "ahead-shutdown"
	origin := testSessStreamFloor(t, a.asm)
	testPlantAcceptFact(t, a.asm, cid, time.Now().UnixNano())

	var future []futureWill
	for _, wid := range []string{"sd-w1", "sd-w2", "sd-w3"} {
		rec := testWillFor(a.asm, cid, wid, testFarFuture(), origin)
		subject, seq := testSeedWill(t, a.asm, rec)
		future = append(future, futureWill{subject: subject, rec: rec, seq: seq})
	}

	close(a.s.willClaimStopCh)

	loads := testCountFactLoads(t, a.asm)
	var cutoffs atomic.Int64
	a.asm.mu.Lock()
	a.asm.testSessLastSeq = func() (uint64, error) {
		cutoffs.Add(1)
		return 1 << 40, nil
	}
	a.asm.mu.Unlock()

	a.asm.cancelSupersededAhead(a.s, future)

	if n := cutoffs.Load(); n != 0 {
		t.Fatalf("a shut-down claim loop must leave the phase at once, got %d cutoff sample(s): "+
			"an unlabelled break leaves the select, not the loop, so every record still costs a "+
			"round trip", n)
	}
	if n := loads.Load(); n != 0 {
		t.Fatalf("a shut-down claim loop must start no early-cancellation work, got %d fact read(s)", n)
	}
	for _, f := range future {
		cur, _ := testReloadWill(t, a.asm, f.subject)
		if cur == nil || cur.State != mqttWillPending {
			t.Fatalf("nothing may have been written during shutdown, got %+v on %q", cur, f.subject)
		}
	}
}

// TestWillBudgetedScanMakesProgressAcrossPasses is the wedge, and it is a LIVENESS
// test rather than a correctness one.
//
// A bounded scan that restarts at the same sequence every time cannot finish an
// unbounded history. A client can accumulate a non-qualifying suffix — acceptances
// dated at or after the Will fell due, which never suppress — long enough to exhaust
// the scan budget, and the owed Will is then neither published nor cancelled: every
// lease-expiry recovery re-reads the identical prefix and stops in the same place.
// The fix is that the renewal persists the scan's PROGRESS, so a later claimant
// resumes above what an earlier one already rejected.
//
// The discriminating assertion is that the record's persisted floor ADVANCES between
// passes and the Will eventually publishes. "It published" alone would pass against a
// budget generous enough to finish in one go, which is why the budget here is forced
// to expire mid-history.
func TestWillBudgetedScanMakesProgressAcrossPasses(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "scan-progress"
	origin := testSessStreamFloor(t, a.asm)
	fireAt := time.Now().Add(-time.Millisecond).UnixNano()

	// A finite NON-qualifying history: every acceptance is dated after the Will fell
	// due, so none of them ever suppresses it. The Will is owed and must publish.
	const history = 12
	for range history {
		testPlantAcceptFact(t, a.asm, cid, fireAt+int64(time.Hour))
	}

	rec := testWillFor(a.asm, cid, "progress-w", fireAt, origin)
	subject, _ := testSeedWill(t, a.asm, rec)

	// A lease short enough that the budget (2x) expires after a few reads, and a
	// cadence (lease/3) short enough that a renewal lands first and carries progress.
	restore := testShortenWillLease(t, a.asm, 90*time.Millisecond)
	defer restore()

	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		time.Sleep(25 * time.Millisecond) // ~7 reads per budget, renewals every 30ms
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.mu.Unlock()

	var floors []uint64
	for pass := 0; pass < 12; pass++ {
		cur, curSeq := testReloadWill(t, a.asm, subject)
		if cur == nil {
			break // published and reaped
		}
		if cur.State == mqttWillDone || cur.licensedToPublish() {
			floors = append(floors, cur.ScanFrom)
			break
		}
		// Expire the claim so this pass is a recovery, exactly as lease expiry makes it.
		cur.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
		_, _ = a.asm.jsa.storeWillOutboxCAS(subject, curSeq, cur)
		reload, seq := testReloadWill(t, a.asm, subject)
		if reload == nil {
			break
		}
		floors = append(floors, reload.ScanFrom)
		a.asm.tryFireWill(a.s, subject, reload, seq)
	}

	if len(floors) < 2 {
		t.Fatalf("the fixture must have taken more than one pass, got floors %v", floors)
	}
	advanced := false
	for i := 1; i < len(floors); i++ {
		if floors[i] > floors[i-1] {
			advanced = true
		}
		if floors[i] < floors[i-1] {
			t.Fatalf("the scan floor must never go backwards: %v", floors)
		}
	}
	if !advanced {
		t.Fatalf("the persisted scan floor never advanced across passes (%v): every recovery "+
			"re-reads the same rejected prefix, so a long enough non-qualifying history wedges "+
			"the owed Will permanently", floors)
	}
	testWaitFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if pubs.Load() == 0 {
			cur, seq := testReloadWill(t, a.asm, subject)
			if cur != nil && cur.State == mqttWillPublishing && !cur.licensedToPublish() {
				cur.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
				if _, err := a.asm.jsa.storeWillOutboxCAS(subject, seq, cur); err == nil {
					if r2, s2 := testReloadWill(t, a.asm, subject); r2 != nil {
						a.asm.tryFireWill(a.s, subject, r2, s2)
					}
				}
			}
			return fmt.Errorf("the owed Will has not been published yet (floors %v)", floors)
		}
		return nil
	})
}

// TestWillScanRefusesToActOnAnAnswerThatArrivedTooLate pins the recheck AFTER a
// blocking read.
//
// A JetStream request runs to its own timeout and cannot be cancelled from here, so
// shutdown or an exhausted caller bound can both arrive while one is in flight. A
// qualifying answer arriving then would otherwise license a durable cancel on a
// broker that is shutting down. TestWillAheadPhaseStopsOnShutdown cannot cover this:
// it shuts down BEFORE any work starts, so the pre-read check catches it there.
func TestWillScanRefusesToActOnAnAnswerThatArrivedTooLate(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const cid = "late-answer"
	origin := testSessStreamFloor(t, a.asm)
	fireAt := testFarFuture()
	testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute)) // QUALIFYING

	rec := testWillFor(a.asm, cid, "late-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	// The stop fires DURING the read, and the read then returns the qualifying fact.
	stop := make(chan struct{})
	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		smsg, err := a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
		close(stop) // shutdown arrives while this request is in flight
		return smsg, err
	}
	a.asm.mu.Unlock()

	sc := &mqttFactScan{
		upTo:       1 << 40,
		deadline:   time.Now().Add(time.Minute),
		renewEvery: time.Minute,
		stop:       []<-chan struct{}{stop},
	}
	if v := a.asm.scanAcceptFacts(rec, sc); v != mqttFactDefer {
		t.Fatalf("a scan whose stop fired while its read was in flight must DEFER, got %v: "+
			"acting on that answer writes durable state after shutdown began", v)
	}
	cur, curSeq := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.State != mqttWillPending || curSeq != seq {
		t.Fatalf("nothing may have been written, got %+v at %d", cur, curSeq)
	}
}

// TestWillLicensingIsRefusedPastTheBudget pins the budget check that sits immediately
// BEFORE the licensing CAS, which no other check covers.
//
// The scan's own checks bracket its reads; this one covers the case where the scan
// returns "nothing qualifies" without performing a read at all — a cutoff at or below
// the scan's start — while the budget was consumed getting that cutoff. A renewed
// lease is not permission to act once the budget is gone, and without this check the
// claimant would license a publish under a cutoff it is no longer entitled to use.
func TestWillLicensingIsRefusedPastTheBudget(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	testQuietClaimLoop(t, a, b)
	const cid = "late-licence"
	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "licence-w", time.Now().Add(-time.Millisecond).UnixNano(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	restore := testShortenWillLease(t, a.asm, 40*time.Millisecond)
	defer restore()

	loads := testCountFactLoads(t, a.asm)
	var samples atomic.Int64
	a.asm.mu.Lock()
	// Every cutoff is BELOW the scan's start, so the walk performs no read at all and
	// reports "nothing qualifies". The budget is consumed by the CHASE's re-sample,
	// after the chase's own pre-sample check has passed and returning an unmoved cutoff
	// so the chase concludes — so the only remaining place to notice it is the guard
	// before the licensing CAS. Spending the budget on the FIRST sample instead would be
	// caught by the chase's check and prove nothing about this guard.
	a.asm.testSessLastSeq = func() (uint64, error) {
		if samples.Add(1) == 2 {
			time.Sleep(120 * time.Millisecond) // > 2 x lease
		}
		return origin, nil
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if n := loads.Load(); n != 0 {
		t.Fatalf("the fixture must reach the licensing CAS without any fact read, got %d", n)
	}
	if pubs.Load() != 0 {
		t.Fatalf("a claimant past its budget must license nothing, got %d publication(s)", pubs.Load())
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.Decision != mqttWillDecisionUnset {
		t.Fatalf("nothing may be licensed past the budget, got %+v", cur)
	}
}

// ---------------------------------------------------------------------------
// Round-3 review findings
// ---------------------------------------------------------------------------

// TestWillScanFloorIsIgnoredAcrossIncarnations pins that a persisted scan floor is a
// $MQTT_sess SEQUENCE and therefore meaningless outside the incarnation that assigned
// it.
//
// The state: $MQTT_sess is replaced, so every fact is gone and sequences restart at 1,
// while a pending Will survives carrying a floor from the old stream. Honouring it
// skips the new stream's early facts, which is exactly what a cross-incarnation Will
// qualifies on. Worse than skipping: with the floor above the new cutoff the walk runs
// ZERO times and reports "nothing qualifies", licensing a publication having read
// nothing.
//
// This USED TO be reachable through an operation the documentation supported, because
// $MQTT_willq sat outside the coupled recreation unit. F-066 made all ten streams one
// recreation unit, so it is now reachable only through administrative deletion or a
// partial restore — unsupported, not impossible. The behaviour is still pinned: a test
// is not weakened because the route to its state got narrower.
func TestWillScanFloorIsIgnoredAcrossIncarnations(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	testQuietClaimLoop(t, a, b)
	const cid = "floor-epoch"

	// A qualifying acceptance at a LOW sequence, as the first CONNECT after a
	// recreation would leave.
	fireAt := time.Now().Add(-time.Millisecond).UnixNano()
	_, factSeq := testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))

	// A surviving Will from ANOTHER incarnation — so the predicate qualifies it on any
	// current-epoch fact dated before FireAt, without a sequence comparison — carrying
	// a floor from that other incarnation, above the fact.
	rec := testWillFor(a.asm, cid, "floor-w", fireAt, 1)
	rec.OriginSessEpoch = mqttGenSessEpoch()
	rec.State = mqttWillPublishing
	rec.ClaimOwner = "a-dead-broker"
	rec.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
	rec.ScanFrom = factSeq + 1000
	rec.ScanEpoch = mqttGenSessEpoch() // a DIFFERENT incarnation
	subject, seq := testSeedWill(t, a.asm, rec)

	loads := testCountFactLoads(t, a.asm)
	a.asm.tryFireWill(a.s, subject, rec, seq)

	if loads.Load() == 0 {
		t.Fatal("a floor from another incarnation must be discarded and the walk restarted; " +
			"honouring it makes the claimant license an act having read no evidence at all")
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.State != mqttWillCancelled {
		t.Fatalf("the qualifying acceptance below the stale floor must suppress the Will, got %+v", cur)
	}
	if pubs.Load() != 0 {
		t.Fatalf("the Will must not have been published, got %d", pubs.Load())
	}
}

// TestWillScanFloorAndEpochAreOneFact pins the shape rules that keep the pair
// meaningful: a bare sequence is uncomparable, a bare epoch says nothing, and a
// pending record has had no claimant to record either.
func TestWillScanFloorAndEpochAreOneFact(t *testing.T) {
	t.Parallel()
	epoch := mqttGenSessEpoch()
	base := func() *mqttWillOutbox {
		r := testWillFor(&mqttAccountSessionManager{sessStreamEpoch: epoch}, "floor-shape", "shape-w", 1, 1)
		r.State = mqttWillPublishing
		return r
	}
	for _, tc := range []struct {
		name    string
		mutate  func(*mqttWillOutbox)
		errWant string
	}{
		{"a floor with no epoch", func(r *mqttWillOutbox) { r.ScanFrom = 5 }, "disagree about presence"},
		{"an epoch with no floor", func(r *mqttWillOutbox) { r.ScanEpoch = epoch }, "disagree about presence"},
		{"a malformed epoch", func(r *mqttWillOutbox) { r.ScanFrom, r.ScanEpoch = 5, "nuid:short" }, "not well-formed"},
		{"a pending record with a floor", func(r *mqttWillOutbox) {
			r.State, r.ScanFrom, r.ScanEpoch = mqttWillPending, 5, epoch
		}, "carries a scan floor"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base()
			tc.mutate(r)
			err := validateWillOutboxBody(r)
			if err == nil {
				t.Fatal("this shape must be refused")
			}
			if !strings.Contains(err.Error(), tc.errWant) {
				t.Fatalf("the refusal must say %q, got: %v", tc.errWant, err)
			}
		})
	}
	// The control: the pair set together, well-formed, on a claimed record.
	ok := base()
	ok.ScanFrom, ok.ScanEpoch = 5, epoch
	if err := validateWillOutboxBody(ok); err != nil {
		t.Fatalf("a floor recorded with its incarnation is legal: %v", err)
	}
	if got := ok.usableScanFloor(epoch); got != 5 {
		t.Fatalf("a floor from THIS incarnation must be usable, got %d", got)
	}
	if got := ok.usableScanFloor(mqttGenSessEpoch()); got != 0 {
		t.Fatalf("a floor from another incarnation must be discarded, got %d", got)
	}
}

// TestWillClaimantRefusesToLicenseAfterShutdown pins the stop check in the FINAL
// pre-license guard, and the one after the cutoff read.
//
// Both were gaps. A scan that returns a verdict WITHOUT entering its loop — a cutoff at
// or below the start — never checks a stop signal at all, and the final guard checked
// only the deadline. "The worker gate refuses the dispatch" is not protection: the
// durable decision would still have been written, and recovery executes it later.
func TestWillClaimantRefusesToLicenseAfterShutdown(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "shutdown-licence"
	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "sdlic-w", time.Now().Add(-time.Millisecond).UnixNano(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)
	_ = b

	loads := testCountFactLoads(t, a.asm)
	var samples atomic.Int64
	a.asm.mu.Lock()
	// Shutdown arrives while the CHASE's re-sample is in flight, and every cutoff comes
	// back below the scan's start so the walk performs no iteration and checks nothing.
	//
	// The chase's own pre-sample check has therefore already passed, and the re-sample
	// returns an unmoved cutoff so the chase concludes — which leaves exactly one place
	// this shutdown can still be noticed. Staging it on the FIRST sample instead would
	// be caught by the chase's check and prove nothing about this guard.
	a.asm.testSessLastSeq = func() (uint64, error) {
		if samples.Add(1) == 2 {
			close(a.s.willClaimStopCh)
		}
		return origin, nil
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if n := loads.Load(); n != 0 {
		t.Fatalf("the fixture must reach the licensing decision without any fact read, got %d", n)
	}
	if pubs.Load() != 0 {
		t.Fatalf("a claimant must not publish after shutdown began, got %d", pubs.Load())
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.Decision != mqttWillDecisionUnset {
		t.Fatalf("no decision may be LICENSED after shutdown began — recovery would execute it "+
			"later, so refusing the dispatch is not enough: got %+v", cur)
	}
}

// TestWillClaimantStopsBeforeScanningAfterShutdown pins the scan's own PRE-READ stop
// check, on the read count.
//
// A shutdown that arrives while the claimant is sampling its cutoff must not then cost
// a whole budget's worth of acceptance-fact reads for a decision that cannot be
// licensed. This is the check that delivers that; a separate check between the cutoff
// read and the scan was written and deleted, because it caught the same condition one
// step earlier and nothing could tell the two apart.
func TestWillClaimantStopsBeforeScanningAfterShutdown(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "shutdown-prescan"
	origin := testSessStreamFloor(t, a.asm)
	// Facts of THIS client above the origin, so a scan that ran would read.
	for range 3 {
		testPlantAcceptFact(t, a.asm, cid, testFarFuture()+int64(time.Hour))
	}
	rec := testWillFor(a.asm, cid, "sdpre-w", time.Now().Add(-time.Millisecond).UnixNano(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)
	_ = b

	loads := testCountFactLoads(t, a.asm)
	a.asm.mu.Lock()
	// A cutoff well ABOVE the scan's start, so the walk would issue reads — and
	// shutdown arrives while that cutoff read is in flight.
	a.asm.testSessLastSeq = func() (uint64, error) {
		close(a.s.willClaimStopCh)
		return 1 << 40, nil
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if n := loads.Load(); n != 0 {
		t.Fatalf("a claimant that learns of shutdown while sampling its cutoff must not go on to "+
			"scan: %d fact read(s) issued, and a full budget of them is the cost", n)
	}
	if pubs.Load() != 0 {
		t.Fatalf("and nothing may be published, got %d", pubs.Load())
	}
}

// testNowSessLastSeq reads the real $MQTT_sess cutoff, so a test can capture the
// value a claimant WOULD have sampled before planting a fact above it.
func testNowSessLastSeq(t testing.TB, asm *mqttAccountSessionManager) uint64 {
	t.Helper()
	u, err := asm.sessLastSeq()
	if err != nil {
		t.Fatalf("sample the acceptance cutoff: %v", err)
	}
	return u
}

// testStagedCutoffs replaces the cutoff sample with a fixed script, and reports how
// many samples were taken. Each call returns the next entry, and the last entry is
// repeated once the script runs out — so a script of one value is a claimant whose
// cutoff never moves.
func testStagedCutoffs(t testing.TB, asm *mqttAccountSessionManager, seqs ...uint64) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	asm.mu.Lock()
	asm.testSessLastSeq = func() (uint64, error) {
		i := int(n.Add(1)) - 1
		if i >= len(seqs) {
			i = len(seqs) - 1
		}
		return seqs[i], nil
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testSessLastSeq = nil
		asm.mu.Unlock()
	})
	return &n
}

// TestWillClaimantChasesTheCutoffForALateAcceptance is the round-4 finding: an
// acceptance can commit AFTER the cutoff was sampled and still qualify on every
// documented term, and the persisted decision means no later claimant re-reads it.
//
// The first sample deliberately misses the fact. The chase must find it, suppress the
// Will, and persist the cutoff it actually verified through — not the sample that
// missed it, which would tell a recovery claimant this decision rested on less
// evidence than it did.
func TestWillClaimantChasesTheCutoffForALateAcceptance(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	testQuietClaimLoop(t, a)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "cutoff-chase"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	// The cutoff a claimant sampling BEFORE the acceptance would have got.
	missed := testNowSessLastSeq(t, a.asm)
	_, factSeq := testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))
	if factSeq <= missed {
		t.Fatalf("the fixture needs the fact ABOVE the missed cutoff, got fact=%d cutoff=%d", factSeq, missed)
	}

	rec := testWillFor(a.asm, cid, "chase-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)
	samples := testStagedCutoffs(t, a.asm, missed, factSeq)

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if n := samples.Load(); n < 2 {
		t.Fatalf("the cutoff must be RE-SAMPLED after the walk, got %d sample(s)", n)
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil {
		t.Fatal("the record must still exist")
	}
	if cur.State != mqttWillCancelled || cur.Decision != mqttWillDecisionCancel {
		t.Fatalf("an acceptance that committed while the walk ran must still suppress the Will, "+
			"got state=%q decision=%q", cur.State, cur.Decision)
	}
	if cur.DecisionCutoff == nil || *cur.DecisionCutoff != factSeq {
		t.Fatalf("the persisted cutoff must be the one VERIFIED through (%d), not the sample that "+
			"missed the fact (%d), got %v", factSeq, missed, cur.DecisionCutoff)
	}
	if pubs.Load() != 0 {
		t.Fatalf("a suppressed Will must not be published, got %d publications", pubs.Load())
	}
}

// TestWillCutoffChaseIsBoundedAndStillDecides pins the chase's stopping rule, which
// is the one place it could have become a wedge.
//
// An empty delta is NOT required to decide. A client reconnecting steadily after its
// Will fell due produces facts forever, none of which can qualify, so a chase that
// insisted on an empty delta would defer an OWED Will on every pass and never fire
// it — the same wedge shape the persisted scan floor exists to prevent. Exhausting
// the limit must therefore LICENSE under the cutoff verified so far.
func TestWillCutoffChaseIsBoundedAndStillDecides(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	testQuietClaimLoop(t, a)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "cutoff-churn"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	// One NON-qualifying fact per chase, each above the last, so every delta the
	// chase samples has something in it and none of it can ever suppress.
	cutoffs := make([]uint64, 0, mqttWillCutoffChases+1)
	for i := 0; i <= mqttWillCutoffChases; i++ {
		_, s := testPlantAcceptFact(t, a.asm, cid, fireAt+int64(time.Minute))
		cutoffs = append(cutoffs, s)
	}
	rec := testWillFor(a.asm, cid, "churn-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)
	samples := testStagedCutoffs(t, a.asm, cutoffs...)

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if got, want := samples.Load(), int64(1+mqttWillCutoffChases); got != want {
		t.Fatalf("the chase must be BOUNDED: want %d cutoff samples, got %d", want, got)
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil {
		t.Fatal("the record must still exist")
	}
	if !cur.licensedToPublish() && cur.State != mqttWillDone {
		t.Fatalf("an owed Will must be DECIDED when the chase limit runs out, not deferred: "+
			"got state=%q decision=%q cutoff=%v", cur.State, cur.Decision, cur.DecisionCutoff)
	}
	testWaitFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		if n := pubs.Load(); n != 1 {
			return fmt.Errorf("the owed Will must be published, got %d publications", n)
		}
		return nil
	})
}

// TestWillAcceptanceAfterTheFinalCutoffIsMissed pins the DIRECTION of the residual
// the chase narrows but cannot close, so nobody reads the chase as a fence.
//
// The cutoff here never moves, which is what a claimant sees when the acceptance is
// still uncommitted at every sample it takes. The fact qualifies on every term and
// the Will is published anyway. That outcome is deliberate: re-deciding after an
// irreversible publication may have occurred is the one thing the persisted decision
// exists to forbid, and TestWillRecoveryReusesThePersistedDecision pins that half.
func TestWillAcceptanceAfterTheFinalCutoffIsMissed(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	testQuietClaimLoop(t, a)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "cutoff-residual"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	final := testNowSessLastSeq(t, a.asm)
	// Qualifying on every term — exact client, current epoch, dated before the Will
	// fell due, above the origin — and above every cutoff this claimant will sample.
	_, factSeq := testPlantAcceptFact(t, a.asm, cid, fireAt-int64(time.Minute))
	if factSeq <= final {
		t.Fatalf("the fixture needs the fact above the final cutoff, got fact=%d cutoff=%d", factSeq, final)
	}

	rec := testWillFor(a.asm, cid, "residual-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)
	samples := testStagedCutoffs(t, a.asm, final)

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if n := samples.Load(); n < 2 {
		t.Fatalf("the claimant must still have CHASED before concluding, got %d sample(s)", n)
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil {
		t.Fatal("the record must still exist")
	}
	if cur.State == mqttWillCancelled {
		t.Fatal("a fact above the final cutoff cannot be read, so it cannot suppress: if this now " +
			"passes, the residual is closed and both this test and its CONFORMANCE entry are stale")
	}
	if !cur.licensedToPublish() && cur.State != mqttWillDone {
		t.Fatalf("the Will must be licensed to publish, got state=%q decision=%q", cur.State, cur.Decision)
	}
	if cur.DecisionCutoff == nil || *cur.DecisionCutoff != final {
		t.Fatalf("the decision must record the cutoff it was taken under (%d), got %v", final, cur.DecisionCutoff)
	}
	testWaitFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		if n := pubs.Load(); n != 1 {
			return fmt.Errorf("want the residual's publication, got %d", n)
		}
		return nil
	})
}

// TestWillCutoffChaseStopsBeforeResamplingAfterShutdown pins the chase's own
// pre-sample check, on the sample COUNT.
//
// It needs the one entry shape that reaches the chase after shutdown has begun: a
// cutoff below the scan's start, so the walk returns "nothing qualifies" without
// executing an iteration and therefore without consulting a stop at all. A shutdown
// noticed anywhere inside the walk returns DEFER instead, which never enters the chase
// — which is why the sibling guard tests stage their shutdown on the SECOND sample and
// this one stages it on the first.
func TestWillCutoffChaseStopsBeforeResamplingAfterShutdown(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "chase-shutdown"
	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "chasesd-w", time.Now().Add(-time.Millisecond).UnixNano(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	loads := testCountFactLoads(t, a.asm)
	var samples atomic.Int64
	a.asm.mu.Lock()
	a.asm.testSessLastSeq = func() (uint64, error) {
		if samples.Add(1) == 1 {
			close(a.s.willClaimStopCh)
		}
		return origin, nil
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if n := samples.Load(); n != 1 {
		t.Fatalf("a claimant that learns of shutdown must not CHASE the cutoff further: want 1 "+
			"sample, got %d — each one is a request issued by a broker that is leaving", n)
	}
	if n := loads.Load(); n != 0 {
		t.Fatalf("and no fact may be read, got %d", n)
	}
	if pubs.Load() != 0 {
		t.Fatalf("and nothing may be published, got %d", pubs.Load())
	}
	cur, _ := testReloadWill(t, a.asm, subject)
	if cur == nil || cur.Decision != mqttWillDecisionUnset {
		t.Fatalf("no decision may be licensed after shutdown began, got %+v", cur)
	}
}

// TestWillRenewalCadenceSpansTheWholeChase is the round-11 finding, and it is the
// chase's second-order cost: the cutoff chase re-enters the walk, so anything the walk
// kept as a LOCAL was silently being restarted per pass.
//
// The renewal deadline was one. Each delta here is deliberately shorter than one
// cadence while the passes together outlast it, which is the interleaving a per-pass
// deadline cannot see: every pass finishes "early", so a claimant re-arming on entry
// renews NEVER and is reclaimed underneath itself while still deciding. The cadence
// bounds the claim, and the claim spans the whole chase, not one pass of it.
//
// The assertion is the renewal COUNT. Neither the record's final state nor the
// publication distinguishes the two implementations: the claimant here holds its lease
// long enough to license either way, and the damage a lapsed lease does needs a PEER to
// reclaim — which is exactly what a unit test of one claimant cannot stage.
func TestWillRenewalCadenceSpansTheWholeChase(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	testCapturePublishes(t, a.asm)
	testQuietClaimLoop(t, a, b)
	const cid = "chase-renewal"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	// One NON-qualifying fact per pass, and a cutoff sitting exactly on each, so every
	// delta is a single fact load and no pass can renew on its own.
	cutoffs := make([]uint64, 0, 5)
	for range 5 {
		_, fseq := testPlantAcceptFact(t, a.asm, cid, fireAt+int64(time.Hour))
		cutoffs = append(cutoffs, fseq)
	}
	rec := testWillFor(a.asm, cid, "chaseren-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	restore := testShortenWillLease(t, a.asm, 30*time.Millisecond)
	defer restore()
	testStagedCutoffs(t, a.asm, cutoffs...)

	var renewals, claims atomic.Int64
	a.asm.mu.Lock()
	// 6ms against a 10ms cadence: a single pass never falls due, three of them do.
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		time.Sleep(6 * time.Millisecond)
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.testClaimStore = func(subj string, expect uint64, r *mqttWillOutbox) (uint64, error) {
		if r.State == mqttWillPublishing && r.Decision == mqttWillDecisionUnset {
			if claims.Add(1) > 1 {
				renewals.Add(1)
			}
		}
		return a.asm.jsa.storeWillOutboxCAS(subj, expect, r)
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if renewals.Load() == 0 {
		t.Fatal("a CHASE longer than the renewal cadence must renew, even though no single pass of " +
			"it is: the lease runs from the claim, so a deadline re-armed per pass renews never and " +
			"a peer reclaims the record from a claimant still deciding it")
	}
}

// TestWillScanStopsWhenShutdownArrivesDuringARenewal is the round-11 finding's second
// half. A renewal is itself a JetStream request that runs to its own timeout, so
// shutdown can arrive while it is in flight — the same reason the walk re-checks after
// a fact LOAD, applied to the other request it issues.
//
// The assertion is that no fact was read AFTER the renewal returned. "It licensed
// nothing" false-passes: the check at the top of the next iteration catches it anyway,
// one whole request later. What this pins is that the request is never issued.
func TestWillScanStopsWhenShutdownArrivesDuringARenewal(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	testCapturePublishes(t, a.asm)
	testQuietClaimLoop(t, a, b)
	const cid = "renew-shutdown"
	fireAt := testFarFuture()

	origin := testSessStreamFloor(t, a.asm)
	// One pass, several facts: the cadence has to fall due INSIDE the walk, so the
	// renewal is reached before the facts run out.
	var last uint64
	for range 6 {
		_, last = testPlantAcceptFact(t, a.asm, cid, fireAt+int64(time.Hour))
	}
	rec := testWillFor(a.asm, cid, "renewsd-w", fireAt, origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	restore := testShortenWillLease(t, a.asm, 30*time.Millisecond)
	defer restore()
	testStagedCutoffs(t, a.asm, last)

	var renewed atomic.Bool
	var loadsAfter, claims atomic.Int64
	// The claimant's OWN stop channel, not the server's: testQuietClaimLoop has already
	// closed that one to silence the background loops, and this test needs a shutdown the
	// claimant under test observes at a moment the test chooses.
	shutdown := make(chan struct{})
	stop := []<-chan struct{}{shutdown}
	a.asm.mu.Lock()
	a.asm.testClaimStop = &stop
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		if renewed.Load() {
			loadsAfter.Add(1)
		}
		time.Sleep(6 * time.Millisecond)
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.testClaimStore = func(subj string, expect uint64, r *mqttWillOutbox) (uint64, error) {
		res, err := a.asm.jsa.storeWillOutboxCAS(subj, expect, r)
		// Shutdown lands while the renewal is in flight: the claimant's next observation
		// of the world is the renewal's own return.
		if r.State == mqttWillPublishing && r.Decision == mqttWillDecisionUnset && claims.Add(1) > 1 {
			if !renewed.Swap(true) {
				close(shutdown)
			}
		}
		return res, err
	}
	a.asm.mu.Unlock()

	a.asm.tryFireWill(a.s, subject, rec, seq)

	if !renewed.Load() {
		t.Fatal("fixture: the scan never renewed, so the window under test was never entered")
	}
	if n := loadsAfter.Load(); n != 0 {
		t.Fatalf("a claimant that learns of shutdown while renewing must not read another fact: "+
			"got %d, each one a full request timeout charged to a broker that is leaving", n)
	}
}

// TestWillScanProgressPersistsDespiteAStaleForeignFloor is the round-5 finding, and the
// THIRD appearance of one defect class: a $MQTT_sess sequence treated as meaningful
// outside the incarnation that issued it.
//
// The read side was already scoped — a foreign floor is ignored, so the scan restarts at
// the beginning. The WRITE side was not: progress was compared against the stored
// number, so a numerically larger stale floor from a previous incarnation kept every
// renewal from persisting anything. The scan then re-read the same prefix on every pass
// and re-exhausted the same budget, wedging an owed Will permanently — the exact failure
// the floor exists to prevent, reached by scoping only one of the two sides.
//
// The quantity asserted is the USABLE floor, not the raw field: the fix necessarily moves
// the stored number DOWNWARDS (a real current-epoch sequence replacing a large foreign
// one), so a raw-value monotonicity check would be measuring the bug.
func TestWillScanProgressPersistsDespiteAStaleForeignFloor(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	pubs := testCapturePublishes(t, a.asm)
	const cid = "stale-floor-progress"
	origin := testSessStreamFloor(t, a.asm)
	fireAt := time.Now().Add(-time.Millisecond).UnixNano()

	const history = 12
	for range history {
		testPlantAcceptFact(t, a.asm, cid, fireAt+int64(time.Hour)) // never qualifies
	}

	rec := testWillFor(a.asm, cid, "stalefloor-w", fireAt, origin)
	// A floor from an incarnation that no longer exists, numerically far above anything
	// the current stream will reach: what a surviving $MQTT_willq record carries when
	// $MQTT_sess has been replaced under it. No SUPPORTED operation produces that any
	// more (F-066 made the ten streams one recreation unit); an administrative
	// deletion or a partial restore still can.
	//
	// Seeded PUBLISHING with an expired lease, because that is the only shape that can
	// carry a floor at all: a floor is written by a claimant renewal, and a `pending`
	// record holding one is a refused shape. So the survivor is the record a claimant
	// was mid-scan on when the old incarnation went away.
	rec.State = mqttWillPublishing
	rec.ClaimOwner = "a-broker-from-the-old-incarnation"
	rec.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
	rec.ScanFrom, rec.ScanEpoch = 1_000_000, mqttGenSessEpoch()
	if rec.ScanEpoch == a.asm.sessStreamEpoch {
		t.Fatal("the fixture needs a FOREIGN epoch")
	}
	subject, _ := testSeedWill(t, a.asm, rec)

	restore := testShortenWillLease(t, a.asm, 90*time.Millisecond)
	defer restore()

	a.asm.mu.Lock()
	a.asm.testFactLoad = func(filter string, s uint64) (*StoredMsg, error) {
		time.Sleep(25 * time.Millisecond)
		return a.asm.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, s)
	}
	a.asm.mu.Unlock()

	epoch := a.asm.sessStreamEpoch
	var usable []uint64
	for pass := 0; pass < 12; pass++ {
		cur, curSeq := testReloadWill(t, a.asm, subject)
		if cur == nil {
			break // published and reaped
		}
		if cur.State == mqttWillDone || cur.licensedToPublish() {
			usable = append(usable, cur.usableScanFloor(epoch))
			break
		}
		cur.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
		_, _ = a.asm.jsa.storeWillOutboxCAS(subject, curSeq, cur)
		reload, seq := testReloadWill(t, a.asm, subject)
		if reload == nil {
			break
		}
		usable = append(usable, reload.usableScanFloor(epoch))
		a.asm.tryFireWill(a.s, subject, reload, seq)
	}

	advanced := false
	for _, v := range usable {
		if v > 0 {
			advanced = true
		}
	}
	if !advanced {
		t.Fatalf("no scan progress was ever persisted under the CURRENT incarnation (usable floors "+
			"%v): the stale foreign floor outranks every real sequence, so each pass re-reads the "+
			"same prefix and the owed Will wedges forever", usable)
	}
	testWaitFor(t, 20*time.Second, 25*time.Millisecond, func() error {
		if pubs.Load() == 0 {
			cur, seq := testReloadWill(t, a.asm, subject)
			if cur != nil && cur.State == mqttWillPublishing && !cur.licensedToPublish() {
				cur.ClaimUntil = time.Now().Add(-time.Hour).UnixNano()
				if _, err := a.asm.jsa.storeWillOutboxCAS(subject, seq, cur); err == nil {
					if r2, s2 := testReloadWill(t, a.asm, subject); r2 != nil {
						a.asm.tryFireWill(a.s, subject, r2, s2)
					}
				}
			}
			return fmt.Errorf("the owed Will has not been published yet (usable floors %v)", usable)
		}
		return nil
	})
}

// TestWillUnsafeJSTimeoutRefusesTheAccount pins the timing invariant as a PRODUCTION
// check rather than a claim a unit test makes about constants.
//
// The publish worker deliberately does not renew its lease, and the least this needs is
// that no single JetStream request can consume a whole ownership window on its own. The
// request timeout is an OPTION an operator can set; the lease is a constant. At or above
// the lease, a licensing acknowledgement can arrive after a peer has already reclaimed
// the expired record and published it — a duplicate Will with no crash anywhere. So an
// unsafe timeout must fail account-manager initialization, exactly as a bad stream shape
// does, rather than being asserted only over the defaults.
//
// "Safe" here is only the absence of that misconfiguration. It is NOT the property that a
// licensed publish fits its lease: licensing stamps a fresh lease and is then acknowledged
// over up to a whole timeout, so `below the lease` can still leave the worker almost no
// residue — and the barrier it waits on is unbounded regardless. That overrun is a
// disclosed residual, pinned separately by TestWillLiveWorkerCanOutliveItsLease.
func TestWillUnsafeJSTimeoutRefusesTheAccount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		wantErr bool
	}{
		{"the default is safe", 0, false},
		{"below the lease is safe", mqttWillDefaultClaimLease - time.Second, false},
		{"equal to the lease is refused", mqttWillDefaultClaimLease, true},
		{"above the lease is refused", mqttWillDefaultClaimLease + time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, nc := runNATS(t)
			s := &Server{
				nrc:             nc,
				opts:            &Options{MQTT: MQTTOpts{JSAPITimeout: tc.timeout}},
				quitCh:          make(chan struct{}),
				nodeName:        "timing-" + nuid.Next(),
				willClaimStopCh: make(chan struct{}),
				willTimers:      make(map[string]*time.Timer),
			}
			s.running.Store(true)
			s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
			t.Cleanup(func() {
				select {
				case <-s.quitCh:
				default:
					close(s.quitCh)
				}
			})

			_, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
			if tc.wantErr {
				if err == nil {
					t.Fatal("a JetStream request timeout at or above the claim lease must REFUSE the " +
						"account: the publish worker does not renew, so an ack arriving after the lease " +
						"expired lets a peer republish the same Will")
				}
				if !strings.Contains(err.Error(), "delayed-Will timing") {
					t.Fatalf("the refusal must name what is unsafe, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("a safe timeout must be accepted, got %v", err)
			}
		})
	}
}

// TestWillLiveWorkerCanOutliveItsLease pins the DIRECTION of the sanctioned duplicate's
// real boundary: lease EXPIRY, not a crash.
//
// The construction check (TestWillUnsafeJSTimeoutRefusesTheAccount) bounds one JetStream
// request below the lease. It does not bound the worker's licensed lifetime, and the
// difference is reachable rather than theoretical: mqttInitiateMsgDeliveryJSA fans a Will
// out to subscribers FIRST and then waits on mqttSubsBarrier before storing the QoS 1/2
// copy — a wait bounded by SHUTDOWN, not by the lease. The barrier is held for a whole
// processSubs, which creates a JetStream consumer per filter, so a CONNECT session
// restore with a few dozen subscriptions on a cluster timing requests out holds it past
// the lease.
//
// The stall is modelled here with a blocking publish hook rather than by driving a real
// SUBSCRIBE: what the invariant turns on is that a LIVE worker inside its licensed
// publish can lose its lease, and the hook reproduces exactly that without a cluster.
// If this ever starts failing because the worker keeps its claim alive, the residual is
// closed and both this test and its CONFORMANCE entry are stale.
func TestWillLiveWorkerCanOutliveItsLease(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	testQuietClaimLoop(t, a, b)
	const cid = "live-worker-lease"

	restoreA := testShortenWillLease(t, a.asm, 150*time.Millisecond)
	defer restoreA()
	restoreB := testShortenWillLease(t, b.asm, 150*time.Millisecond)
	defer restoreB()

	origin := testSessStreamFloor(t, a.asm)
	rec := testWillFor(a.asm, cid, "livelease-w", time.Now().Add(-time.Millisecond).UnixNano(), origin)
	subject, seq := testSeedWill(t, a.asm, rec)

	// A's worker enters its licensed publish and stays there — the barrier wait, in
	// miniature. It never returns during the test, which is the point: A is ALIVE.
	started, release := make(chan struct{}), make(chan struct{})
	var aPubs atomic.Int64
	a.asm.mu.Lock()
	a.asm.testPublishWill = func(*mqttWillOutbox) error {
		aPubs.Add(1)
		close(started)
		<-release
		return nil
	}
	a.asm.mu.Unlock()
	defer close(release)

	bPubs := testCapturePublishes(t, b.asm)

	a.asm.tryFireWill(a.s, subject, rec, seq)
	select {
	case <-started:
	case <-time.After(4 * time.Second):
		t.Fatal("A's worker never entered its licensed publish")
	}
	if aPubs.Load() != 1 {
		t.Fatalf("A must have published exactly once, got %d", aPubs.Load())
	}

	// A's lease expires while A is still inside that publish. Nothing crashed.
	testWaitFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		cur, _ := testReloadWill(t, b.asm, subject)
		if cur == nil {
			return fmt.Errorf("the record vanished")
		}
		if time.Now().UnixNano() <= cur.ClaimUntil {
			return fmt.Errorf("A's lease has not expired yet")
		}
		return nil
	})

	// B reclaims the still-publishing record and executes the SAME persisted decision.
	reload, rseq := testReloadWill(t, b.asm, subject)
	if reload == nil {
		t.Fatal("the record must still exist for B to reclaim")
	}
	if !reload.licensedToPublish() {
		t.Fatalf("the record must still carry A's publish licence, got state=%q decision=%q",
			reload.State, reload.Decision)
	}
	b.asm.tryFireWill(b.s, subject, reload, rseq)

	testWaitFor(t, 4*time.Second, 10*time.Millisecond, func() error {
		if n := bPubs.Load(); n != 1 {
			return fmt.Errorf("B must republish the Will A is still publishing, got %d", n)
		}
		return nil
	})

	// The duplicate, with both brokers alive and nothing crashed anywhere.
	select {
	case <-release:
		t.Fatal("A's worker must still be inside its publish: the whole point is that A is LIVE")
	default:
	}
	if aPubs.Load() != 1 || bPubs.Load() != 1 {
		t.Fatalf("want exactly one publication from each live broker, got A=%d B=%d",
			aPubs.Load(), bPubs.Load())
	}
}
