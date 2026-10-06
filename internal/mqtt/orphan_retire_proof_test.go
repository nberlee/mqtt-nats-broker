package mqtt

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// Coverage for the orphan sweep's retirement proof (orphan_retire_proof.go), F-036
// site 3.
//
// The property under test is one rule, not three site behaviors: any arm that can still
// destroy live session-generation state requires a valid retirement watermark whose Gen
// EQUALS the candidate generation, and without that proof it RETAINS. The negatives below
// therefore assert on the EXACT retained state — the payload, the register, the consumer —
// because "the pass did not crash" and "the record is still there for some other reason"
// are both things a weaker assertion would accept.
//
// The PUBREL consumer arm is the exception, and the tests over it say so: since F-105 7c-lc its
// licence is the consumer name's RETIRED ownership alone (Codex consult 14 finding 2), written
// by opAuthorizeDeletion in the batch that authorizes the resource's deletion. The watermark
// licenses nothing there, so those negatives plant one beside a missing ownership record and
// still expect retention, and their controls retire the name through opAuthorizeDeletion.
//
// Each negative is paired with a POSITIVE CONTROL. Without those, every mutation that
// bypasses the gate could be "killed" by a pass that simply retains everything, and the
// gate would be indistinguishable from disabling reclamation altogether.
//
// A structural inventory guard over the gated arms was designed and DELETED after three
// review rounds: a primitive-only AST inventory cannot catch a new unsafe CALLER of an
// already-inventoried destructive helper, and completing it needs the bounded SSA
// reachability this repository has ruled disproportionate. What protects the property is
// the census in orphan_retire_proof.go's header, these behavioral tests, and their
// controls.

// forgetWithoutLicence drops the record and the resident object WITHOUT planting a
// retirement watermark — the state a session live on ANOTHER broker leaves behind. It is
// the whole point of the negatives: absence of a record and of a local resident is
// precisely what F-036 says does not prove a generation dead.
func (e *testOrphanEnv) forgetWithoutLicence(t *testing.T) {
	t.Helper()
	e.dropLocalSession()
	e.dropSessionRecord(t)
}

// TestF036OrphanSweepRetainsRemoteLiveGenerationWithoutMatchingRetirement is the
// negative, over the inbound hold class, with the payload asserted exactly.
func TestF036OrphanSweepRetainsRemoteLiveGenerationWithoutMatchingRetirement(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-retain-hold")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "live-remote-payload")
	e.forgetWithoutLicence(t)

	// Several passes: the grace is zero, so a pass that was going to destroy would have
	// done so by the second one — that is exactly what the licensed fixtures assert.
	e.settle(t)
	e.settle(t)

	if !e.holdPresent(t, holdSubject) {
		t.Fatal("a hold whose generation is merely ABSENT was destroyed: absence is not proof that the session ended")
	}
	// The EXACT payload, not merely a record: a pass that replaced the hold with a
	// tombstone would satisfy holdPresent and still have destroyed the message.
	rec, _, err := e.asm.loadQoS2Record(holdSubject)
	if err != nil {
		t.Fatalf("load the retained hold: %v", err)
	}
	if rec == nil || rec.state != mqttQoS2Held {
		t.Fatalf("the retained record must still be HELD, got %+v", rec)
	}
	if got := string(rec.stored.Data); !strings.Contains(got, "live-remote-payload") {
		t.Fatalf("the retained hold no longer carries its payload, got %q", got)
	}
	// And no fence was written either. Creating the `discarded` marker is itself
	// destructive — it is what a later releaser must lose to — so an unlicensed pass
	// must not write one.
	if e.claimPresent(t, holdSubject) {
		t.Fatal("an unlicensed pass wrote a discard marker: the marker is a fence a live releaser would then lose to")
	}
}

// TestF036OrphanSweepReclaimsWithMatchingRetirement is the POSITIVE CONTROL for the hold
// class: with the licence present the pass reclaims exactly as before, so the gate is a
// gate rather than a switch that turned reclamation off.
func TestF036OrphanSweepReclaimsWithMatchingRetirement(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-reclaim-hold")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "retired-payload")
	e.forget(t) // drops the record AND plants the licence

	e.settle(t)
	if e.holdPresent(t, holdSubject) {
		t.Fatal("a hold whose generation is PROVABLY retired was not reclaimed")
	}
}

// TestF036OrphanSweepDifferentGenerationWatermarkAuthorizesNothing is the arm that makes
// the proof exact-match rather than merely "a watermark exists".
//
// A valid watermark naming a DIFFERENT generation is the ordinary steady state once a
// client has been torn down more than once: the single slot names the latest retirement.
// It says nothing about the candidate generation, and reading it as a licence would
// destroy the state of every generation whose slot has since been replaced.
func TestF036OrphanSweepDifferentGenerationWatermarkAuthorizesNothing(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-othergen-hold")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "other-generation-payload")
	e.forgetWithoutLicence(t)
	// A LATER generation's retirement occupies the slot.
	e.retireClient(t, e.sess.id, mqttNewSessGen())

	e.settle(t)
	e.settle(t)

	if !e.holdPresent(t, holdSubject) {
		t.Fatal("a watermark naming a DIFFERENT generation was read as a licence to destroy this one's hold")
	}
	if e.claimPresent(t, holdSubject) {
		t.Fatal("a watermark naming a DIFFERENT generation authorized writing a discard fence")
	}
}

// TestF036OrphanSweepCorruptWatermarkAuthorizesNothing covers the two remaining negative
// statuses. Neither is absence and neither is proof: a watermark this broker cannot
// interpret may name any retirement at all, and an unreadable stream says nothing.
func TestF036OrphanSweepCorruptWatermarkAuthorizesNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status mqttRetirementStatus
	}{
		{"corrupt", mqttRetirementCorrupt},
		{"indeterminate", mqttRetirementIndeterminate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewOrphanEnv(t, "f036-"+tc.name+"-hold")
			e.setGraces(t, 0, 0)
			holdSubject := e.hold(t, 1, nuid.Next(), tc.name+"-payload")
			e.forgetWithoutLicence(t)
			// The hook is process-GLOBAL and Go resumes paused parallel tests between
			// sequential ones, so an unscoped hook answers for every client in every
			// concurrently running test — which is how this one made
			// TestMQTTInflightOrphanReconcile flake. It therefore answers only for THIS
			// client and DELEGATES every other hash to the real read.
			ourHash := e.sess.idHash
			asm := e.asm
			restore := setMQTTTestRetirementLoadHook(
				func(idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
					if idHash != ourHash {
						return asm.jsa.loadGenRetirementRecord(asm.domainTk, idHash)
					}
					return nil, 3, tc.status
				})
			defer restore()

			e.settle(t)
			e.settle(t)

			if !e.holdPresent(t, holdSubject) {
				t.Fatalf("a %s watermark was read as a licence to destroy a hold", tc.name)
			}
			if e.claimPresent(t, holdSubject) {
				t.Fatalf("a %s watermark authorized writing a discard fence", tc.name)
			}
		})
	}
}

// TestF036OrphanSweepRereadsANegativeRetirementObservation pins the cache boundary from
// the sweep's side: only the monotone matching observation is remembered.
//
// A pass that observed no licence must not have cached that, or the state would be
// retained forever once a retirement finally lands.
func TestF036OrphanSweepRereadsANegativeRetirementObservation(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-reread-hold")
	e.setGraces(t, 0, 0)
	holdSubject := e.hold(t, 1, nuid.Next(), "reread-payload")
	e.forgetWithoutLicence(t)

	// Passes with NO licence: the hold is retained, and the pass has now observed an
	// absent watermark for this (client, generation).
	e.settle(t)
	e.settle(t)
	if !e.holdPresent(t, holdSubject) {
		t.Fatal("the unlicensed passes destroyed the hold, so this test cannot show a re-read")
	}

	// The retirement lands afterwards. A cached negative would make every later pass
	// skip the read and retain forever.
	e.retireClient(t, e.sess.id, e.sess.gen)
	e.settle(t)
	if e.holdPresent(t, holdSubject) {
		t.Fatal("a NEGATIVE retirement observation was cached: the hold survived a retirement that landed afterwards")
	}
}

// TestF036OrphanSweepRetainsAnUnlicensedRegister and its control cover the second gated
// arm. A register belonging to a live remote generation still holds that client's packet
// identifier and its Receive Maximum slot; deleting it restores the exchange, on a crash,
// as an ordinary live delivery.
func TestF036OrphanSweepRetainsAnUnlicensedRegister(t *testing.T) {
	e := testNewInflightEnv(t, "f036-retain-register")
	subject := e.seedOrphanRegisterWithoutLicence(t, 21)
	zero := time.Duration(0)
	e.asm.mu.Lock()
	e.asm.testOrphanGrace = &zero
	e.asm.mu.Unlock()

	for i := 0; i < 4; i++ {
		e.asm.sweepOrphanedQoS2State(e.s)
	}
	if !e.registerPresent(t, subject) {
		t.Fatal("an in-flight register whose generation is merely ABSENT was deleted")
	}
}

// TestF036OrphanSweepRetainsAnUnlicensedPubRelConsumer and its control cover the third
// gated arm — the one keyed differently from the other two, by refs.pubRelDurables and
// its own record re-read rather than by refs.hasInbound. Deleting the consumer drops
// every interest-retained PUBREL it holds.
//
// This arm's licence is no longer the watermark: since F-105 7c-lc it is the name's RETIRED
// ownership alone (Codex consult 14 finding 2), which opAuthorizeDeletion writes in the batch
// that authorizes the resource's deletion. So the negative is a consumer whose name no
// ownership record claims, and it CARRIES a matching watermark: that is what separates the
// landed gate from re-licensing the delete by the watermark (consult 14's rejected option C).
// The control runs in the SAME passes, so a pass that retains everything fails it.
func TestF036OrphanSweepRetainsAnUnlicensedPubRelConsumer(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-retain-pubrel")
	e.setGraces(t, 0, 0)
	unowned := e.unownedPubRelConsumer(t, "f036-pubrel-live-remote")
	retired, _, _ := e.retiredPubRelConsumer(t, "f036-pubrel-retired")

	e.sweepPubRelConsumers(t)
	e.sweepPubRelConsumers(t)
	e.sweepPubRelConsumers(t)

	if !e.consumerExists(t, unowned) {
		t.Fatal("a PUBREL consumer whose name no ownership record claims was deleted on its generation's " +
			"watermark, dropping the PUBRELs it held: only a RETIRED name licenses the delete")
	}
	// The control: a RETIRED name is reclaimed by the same passes, so the gate is not simply
	// switching this class off.
	if e.consumerExists(t, retired) {
		t.Fatal("a PUBREL consumer whose name is RETIRED was not reclaimed")
	}
}

// TestF036OrphanSweepRotatesPubRelRevalidationWithoutSkipping is holistic review round
// 2's second finding: the accepted retention leak is unbounded in cardinality, so a pass
// that revalidates EVERY aged candidate pays a session-record read, a consumer lookup and
// an ownership read per candidate forever, and its JetStream traffic grows without bound.
//
// The bound is a ROTATION, not a truncation, and that distinction is the whole assertion
// here: a truncating bound would revisit the same prefix on every pass and leave the tail
// unreclaimed permanently, which would turn a bounded-cost fix into a second leak. So the
// test creates more licensed (RETIRED-name) candidates than one pass may revalidate and
// asserts that REPEATED passes eventually reclaim ALL of them, while the unowned candidates
// revalidated beside them are reclaimed by none.
//
// Mutations: removing the cursor advance (so every pass starts at 0), or sorting
// non-deterministically, must fail this test.
func TestF036OrphanSweepRotatesPubRelRevalidationWithoutSkipping(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-rotate")
	e.setGraces(t, 0, 0)

	// More than one pass's budget, so the rotation is actually exercised. Each candidate's
	// name is RETIRED through opAuthorizeDeletion, which is what licenses its delete.
	n := mqttPubRelRevalidateBudget + 5
	durables := make([]string, 0, n)
	for i := 0; i < n; i++ {
		d, _, _ := e.retiredPubRelConsumer(t, fmt.Sprintf("f036-rotate-gone-%d", i))
		durables = append(durables, d)
	}
	// The missing-ownership arm, in the same passes: these stay in the candidate set and
	// are revalidated beside the RETIRED ones, and must never be reclaimed.
	unowned := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		unowned = append(unowned, e.unownedPubRelConsumer(t, fmt.Sprintf("f036-rotate-unowned-%d", i)))
	}

	// Enough passes for the rotation to cover the whole set several times over. Each
	// pass revalidates at most the budget, so a truncating bound would leave the tail
	// forever and this loop would time out.
	for i := 0; i < 12; i++ {
		e.sweepPubRelConsumers(t)
	}

	var survivors []string
	for _, d := range durables {
		if e.consumerExists(t, d) {
			survivors = append(survivors, d)
		}
	}
	if len(survivors) > 0 {
		t.Fatalf("the bounded revalidation never reached %d of %d licensed candidates: %v",
			len(survivors), n, survivors[:min(len(survivors), 5)])
	}
	for _, d := range unowned {
		if !e.consumerExists(t, d) {
			t.Fatalf("PUBREL consumer %q, whose name no ownership record claims, was reclaimed: "+
				"a missing record is withheld, never read as permission", d)
		}
	}
}

// TestF036OrphanSweepAdvancesItsPubRelCursor is the DETERMINISTIC half of the rotation,
// and it exists because the coverage test above cannot supply it.
//
// Coverage alone cannot detect a stuck cursor: every reclaimed candidate leaves the
// candidate set, so after one pass the remainder falls under the budget and even a
// truncating bound finishes the job. The population that actually exposes the difference
// is the one this change deliberately RETAINS — unlicensed candidates are never
// reclaimed, so they stay above the budget pass after pass, and a cursor that does not
// advance revalidates the same prefix forever while the tail is never revisited. That
// would turn a bounded-cost fix into a second leak.
//
// So this asserts the mechanism directly: with a retained backlog larger than one pass's
// budget, consecutive passes must start at DIFFERENT offsets.
//
// Mutation: not advancing the cursor must fail this test.
func TestF036OrphanSweepAdvancesItsPubRelCursor(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-cursor")
	e.setGraces(t, 0, 0)

	// A RETAINED backlog: no licence for any of them, so none is ever reclaimed and the
	// candidate count stays above the budget for every pass.
	n := mqttPubRelRevalidateBudget + 10
	for i := 0; i < n; i++ {
		testCreatePubRelConsumer(t, e, getHash(fmt.Sprintf("f036-cursor-live-%d", i)), mqttNewSessGen())
	}

	cursorAfter := func() string {
		e.asm.mu.Lock()
		defer e.asm.mu.Unlock()
		return e.asm.pubRelSweepAfter
	}
	// Two passes to qualify the candidates (nothing is acted on at a first sighting),
	// then read the offset each subsequent pass starts from.
	e.sweepPubRelConsumers(t)
	e.sweepPubRelConsumers(t)
	first := cursorAfter()
	e.sweepPubRelConsumers(t)
	second := cursorAfter()

	if first == second {
		t.Fatalf("the PUBREL revalidation cursor did not advance across passes (stayed at %q): "+
			"a retained backlog larger than one pass's budget would revalidate the same prefix "+
			"forever and never revisit the tail", first)
	}
}

// TestF036OrphanSweepRevalidatesAtMostTheBudget is the COUNT assertion, and it is a
// separate test from the rotation ones deliberately: they prove nothing is starved, and
// this proves the bound is actually applied. Either alone would pass a broken
// implementation — a pass with no bound satisfies coverage, and a bound with no rotation
// satisfies the count.
//
// The observable is the name-ownership read, which is the per-candidate cost the budget exists
// to bound: exactly one per revalidated candidate, on this arm. (It was the retirement-watermark
// read until F-105 7c-lc made a RETIRED ownership this arm's whole licence and deleted the
// watermark gate it shadowed; the bound is unchanged.)
func TestF036OrphanSweepRevalidatesAtMostTheBudget(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-budget")
	e.setGraces(t, 0, 0)

	n := mqttPubRelRevalidateBudget + 40
	for i := 0; i < n; i++ {
		testCreatePubRelConsumer(t, e, getHash(fmt.Sprintf("f036-budget-live-%d", i)), mqttNewSessGen())
	}
	// Two passes to qualify the candidates; nothing is acted on at a first sighting.
	e.sweepPubRelConsumers(t)
	e.sweepPubRelConsumers(t)

	// Count the PUBREL name-ownership reads of the NEXT pass only. The hook observes and
	// passes every read through, so the pass behaves exactly as in production.
	var reads atomic.Int64
	restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if stream == mqttInflightStreamName && mqttIsNameOwnershipSubject(subject) &&
			strings.Contains(subject, "."+string(mqttOwnedStreamOut)+".") {
			reads.Add(1)
		}
		return nil
	})
	e.sweepPubRelConsumers(t)
	restore()

	if got := reads.Load(); got > int64(mqttPubRelRevalidateBudget) {
		t.Fatalf("one pass performed %d PUBREL ownership reads for %d candidates, above the budget of %d: "+
			"the accepted retention leak is unbounded, so an unbounded per-pass cost grows with it",
			got, n, mqttPubRelRevalidateBudget)
	}
	if reads.Load() == 0 {
		t.Fatal("the pass performed no ownership reads at all, so this test proves nothing about the bound")
	}
}

// TestF036OrphanSweepCursorSurvivesChurn is the CHURN case, and it is why the cursor is a
// durable NAME rather than a slice index.
//
// An index modulo the current length is shifted by every insertion and deletion before
// it, so a sustained arrival-and-departure rate can keep one candidate outside every
// selected window forever — starvation, which would turn a bounded-cost fix into a second
// leak. A name is stable under both operations.
//
// The fixture places a LICENSED sentinel (a RETIRED name) inside a large retained backlog
// whose names no ownership record claims, then churns the set between passes by deleting
// and adding names on both sides of the cursor. The sentinel must still be reached and
// reclaimed, and every backlog name the churn left standing must still stand.
//
// Mutations that must fail this: restoring the numeric modulo cursor, resetting the key
// cursor each pass, or failing to advance it.
func TestF036OrphanSweepCursorSurvivesChurn(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-churn")
	e.setGraces(t, 0, 0)

	// The retained backlog: no ownership record claims any of these names, so it never
	// leaves the candidate set and the count stays above the budget for every pass. It is
	// also this test's missing-ownership arm, asserted at the end.
	backlog := mqttPubRelRevalidateBudget + 40
	live := make([]string, 0, backlog)
	for i := 0; i < backlog; i++ {
		live = append(live, testCreatePubRelConsumer(t, e,
			getHash(fmt.Sprintf("f036-churn-live-%d", i)), mqttNewSessGen()))
	}
	// The LICENSED sentinel — its name RETIRED through opAuthorizeDeletion — which the
	// rotation must eventually reach.
	sentinel, _, _ := e.retiredPubRelConsumer(t, "f036-churn-sentinel")

	// The churn deletes from the FRONT of the sorted-by-name order and adds fresh names
	// that land wherever their hashes put them, so candidates disappear and appear on
	// both sides of the cursor — which is exactly what shifts an index-based cursor's
	// meaning while leaving a name-based one intact.
	sort.Strings(live)
	next := 0
	var added []string
	for pass := 0; pass < 24 && e.consumerExists(t, sentinel); pass++ {
		e.sweepPubRelConsumers(t)
		for k := 0; k < 3 && next < len(live); k++ {
			added = append(added, testCreatePubRelConsumer(t, e,
				getHash(fmt.Sprintf("f036-churn-add-%d-%d", pass, k)), mqttNewSessGen()))
			if _, derr := e.asm.jsa.deleteConsumer(mqttOutStreamName, live[next], false); derr != nil &&
				isErrorOtherThan(derr, JSConsumerNotFoundErr) {
				t.Fatalf("churn delete of %q: %v", live[next], derr)
			}
			next++
		}
	}
	if e.consumerExists(t, sentinel) {
		t.Fatal("the rotation never reached the licensed sentinel under churn: a cursor whose " +
			"meaning shifts with insertions and deletions can starve a candidate forever")
	}
	// The missing-ownership arm: every unowned candidate the churn did not itself delete was
	// revalidated beside the sentinel and withheld.
	for _, d := range append(append([]string(nil), live[next:]...), added...) {
		if !e.consumerExists(t, d) {
			t.Fatalf("PUBREL consumer %q, whose name no ownership record claims, was reclaimed: "+
				"a missing record is withheld, never read as permission", d)
		}
	}
}

// TestF036OrphanSweepReapsATerminalTombstoneWithoutProof is the other half of the safe
// tail, and it is the half whose ORDER matters.
//
// A terminal tombstone needs no retirement proof: the exchange it belongs to already
// delivered onward and committed, so removing it destroys nothing — and the PUBREL branch
// that answers from it answers identically from its absence.
//
// The fixture deliberately STARTS WITH THE TOMBSTONE PRESENT. A fixture that started with
// the hold already absent could not exercise this branch at all, and so could not
// distinguish the correct ORDER — delete, then prove absent, then reap the claim — from
// its reverse.
//
// WHAT THIS TEST DOES AND DOES NOT ENFORCE, stated because an earlier version of it
// claimed more than it proves. It enforces the ORDER: reversing the absence read and the
// delete fails it, because the read then finds the record still present and the pass
// defers forever. It does NOT enforce that the delete is CONFIRMED rather than no-wait —
// two complete passes let a queued delete land in between, and even a one-pass fixture
// could not separate "confirmed" from "applied quickly" without a delayed-application
// seam. The confirmation is kept in production as defence in depth: on clustered
// JetStream a no-wait delete returns once the operation is proposed, so the pair stays
// fail-safe either way (an unapplied delete makes the read defer), but the confirmed form
// reports the failure AT ITS OWN SITE instead of turning it into a silent extra pass.
// That diagnostic is not a behavioural difference a test can assert, and it is recorded
// here rather than asserted falsely.
//
// Mutation: reordering the absence read before the delete must fail this test.
func TestF036OrphanSweepReapsATerminalTombstoneWithoutProof(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-tombstone")
	e.setGraces(t, 0, 0)
	gen := nuid.Next()
	holdSubject := e.hold(t, 1, gen, "committed-payload")
	_, holdSeq, lerr := e.asm.jsa.loadQoS2Record(holdSubject)
	if lerr != nil || holdSeq == 0 {
		t.Fatalf("read the hold's sequence: seq=%d err=%v", holdSeq, lerr)
	}
	// Commit the terminal state at the held record's own sequence: this is what a
	// completed release leaves behind, and the claim beside it is its own.
	if _, err := e.asm.jsa.storeQoS2DoneCAS(holdSubject, gen, holdSeq); err != nil {
		t.Fatalf("commit the terminal tombstone: %v", err)
	}
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "node2", 0))
	// NO licence: the point is that a tombstone needs none.
	e.forgetWithoutLicence(t)

	e.settle(t)
	e.settle(t)

	if e.holdPresent(t, holdSubject) {
		t.Fatal("a terminal tombstone needs no retirement proof: it delivered and committed, so removing it destroys nothing")
	}
	if e.claimPresent(t, holdSubject) {
		t.Fatal("the claim behind a removed tombstone was never reaped: the gate has become a blanket retain")
	}
}

// TestF036OrphanSweepRetiresClaimAfterHoldAlreadyGone is the SAFE TAIL: the arm that
// deliberately runs WITHOUT a retirement proof.
//
// Once the payload is authoritatively absent, removing the fence that protected it
// destroys nothing. The defensible claim is exactly that and no more — it is NOT that a
// paused releaser "can only win a new claim", since one may already hold the claim and
// already have passed its own proof; deleting the claim does not revoke that releaser's
// in-memory progress, and the pre-existing post-proof residual stays.
//
// Without this arm the gate would be a blanket retain: every claim whose generation can
// no longer be proven retired would accumulate beside a hold that is already gone.
func TestF036OrphanSweepRetiresClaimAfterHoldAlreadyGone(t *testing.T) {
	e := testNewOrphanEnv(t, "f036-safetail")
	e.setGraces(t, 0, 0)
	gen := nuid.Next()
	holdSubject := e.hold(t, 1, gen, "safetail-payload")
	e.putClaim(t, holdSubject, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, gen, "node2", 0))
	// The payload is destroyed by its own releaser or by an earlier licensed pass, so
	// only the fence is left. NO licence is planted: this is the point.
	_, holdSeq, lerr := e.asm.jsa.loadQoS2Record(holdSubject)
	if lerr != nil || holdSeq == 0 {
		t.Fatalf("read the hold's sequence on %q: seq=%d err=%v", holdSubject, holdSeq, lerr)
	}
	if st, err := e.asm.releaseQoS2Record(holdSeq); st != mqttQoS2Released {
		t.Fatalf("removing the hold for the safe-tail setup: st=%v err=%v", st, err)
	}
	e.forgetWithoutLicence(t)

	e.settle(t)
	e.settle(t)

	if e.claimPresent(t, holdSubject) {
		t.Fatal("the safe tail did not retire an aged claim whose hold is already absent: without it the gate is a blanket retain")
	}
}
