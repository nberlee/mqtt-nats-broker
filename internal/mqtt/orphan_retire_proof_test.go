package mqtt

import (
	"strings"
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
// gated arm — the one keyed differently from the other two, by the name's ownership record
// rather than by refs.hasInbound. Deleting the consumer drops every interest-retained PUBREL
// it holds.
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
