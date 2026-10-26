//go:build mqtt_fulltests

// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

// The traversal's arithmetic, pinned where a test can see it.
//
// The cursor arithmetic is separated from the pass that reads records for the same reason the
// sweep cursor is: it is the part an earlier design got wrong, and the part a test can pin
// exactly. Everything here is a pure function over a header.

// TestF105WrappingIsACASOnTheCycle walks every row of the cycle table, including the one round 5
// found unexercised: a walker whose own walk is complete while the HEADER's position is not.
func TestF105WrappingIsACASOnTheCycle(t *testing.T) {
	const shard = 7

	t.Run("same cycle, merged as a maximum", func(t *testing.T) {
		stored := &mqttDirShard{Shard: shard, Hi: 9, Next: 3, Cycle: 2, Horizon: 5}
		next, err := mqttAdvanceShard(stored, 11, 2, 5)
		if err != nil || next == nil {
			t.Fatalf("a forward position in the stored cycle was not written: %v %+v", err, next)
		}
		if next.Next != 5 || next.Cycle != 2 || next.Horizon != 5 || next.Hi != 9 {
			t.Fatalf("the merge moved something it should not: %+v", next)
		}
	})

	t.Run("a stale position is never rolled back", func(t *testing.T) {
		stored := &mqttDirShard{Shard: shard, Hi: 9, Next: 5, Cycle: 2, Horizon: 5}
		next, err := mqttAdvanceShard(stored, 11, 2, 3)
		if err != nil {
			t.Fatalf("a stale position errored instead of being discarded: %v", err)
		}
		if next != nil {
			t.Fatalf("a walker behind the stored position rolled it back to %+v", next)
		}
	})

	t.Run("the cycle ends by the HEADER's progress", func(t *testing.T) {
		// The walker finished its own visit at the horizon, but the header has not: another
		// walker is still short of it. Merging is all this may do — a cycle ends when the
		// header's progress reaches the horizon, not when one worker personally visited it.
		stored := &mqttDirShard{Shard: shard, Hi: 9, Next: 3, Cycle: 2, Horizon: 5}
		next, err := mqttAdvanceShard(stored, 11, 2, 6)
		if err != nil || next == nil {
			t.Fatalf("the walker's own completion was not merged: %v %+v", err, next)
		}
		if next.Cycle != 2 {
			t.Fatalf("one walker's completed visit wrapped the shard's cycle: %+v", next)
		}
		if next.Next != 6 || next.Horizon != 5 {
			t.Fatalf("the merge did not record the position, or moved the horizon: %+v", next)
		}
		// AND THE WRAP THEN SUCCEEDS from the header that records it.
		wrapped, werr := mqttAdvanceShard(next, 12, 2, 6)
		if werr != nil || wrapped == nil {
			t.Fatalf("the completed cycle did not wrap: %v %+v", werr, wrapped)
		}
		if wrapped.Cycle != 3 || wrapped.Next != 1 || wrapped.Horizon != 9 {
			t.Fatalf("the wrap did not open a cycle over the current high-water: %+v", wrapped)
		}
	})

	t.Run("a stale cycle is discarded entirely", func(t *testing.T) {
		// Suspended across TWO wraps. The position means nothing in a cycle this walker did not
		// walk, so it is discarded rather than merged — merging it would drag a newer cycle's
		// traversal back to an older cycle's position.
		stored := &mqttDirShard{Shard: shard, Hi: 20, Next: 2, Cycle: 4, Horizon: 20}
		next, err := mqttAdvanceShard(stored, 11, 2, 19)
		if err != nil {
			t.Fatalf("a stale cycle errored instead of being discarded: %v", err)
		}
		if next != nil {
			t.Fatalf("a position from cycle 2 was written into cycle 4: %+v", next)
		}
	})

	t.Run("a cycle no committed header carried is refused", func(t *testing.T) {
		stored := &mqttDirShard{Shard: shard, Hi: 9, Next: 3, Cycle: 2, Horizon: 5}
		if _, err := mqttAdvanceShard(stored, 11, 3, 1); err == nil {
			t.Fatal("a walker offered a position in a cycle ahead of the header and it was taken")
		}
	})

	t.Run("allocation during the cycle does not move its horizon", func(t *testing.T) {
		// An allocation raises Hi; the horizon is what the cycle began at, so the walk still ends
		// where it was going to end. That is what stops continuous allocation from postponing an
		// older root for ever.
		stored := &mqttDirShard{Shard: shard, Hi: 40, Next: 4, Cycle: 2, Horizon: 5}
		next, err := mqttAdvanceShard(stored, 11, 2, 5)
		if err != nil || next == nil {
			t.Fatalf("advancing inside the cycle failed: %v", err)
		}
		if next.Horizon != 5 {
			t.Fatalf("an allocation moved the running cycle's horizon to %d", next.Horizon)
		}
		if next.Hi != 40 {
			t.Fatalf("the merge did not take the high-water from the stored header: %+v", next)
		}
	})
}

// TestF105OneReadinessWalkPerPass pins the COUNT that replaces the budget for the account-wide
// readiness walk.
//
// The walk cannot be charged per record without starving it, so what stops a single pass from
// performing an unbounded number of them is this cap and nothing else. It is the whole bound, so
// it is asserted on its own rather than only through a pass that happens to find one generation.
func TestF105OneReadinessWalkPerPass(t *testing.T) {
	b := newMqttRecoveryBudget()
	if !b.takeReadinessAttempt() {
		t.Fatal("a fresh pass cannot take its own readiness attempt")
	}
	if b.takeReadinessAttempt() {
		t.Fatal("one pass took a SECOND account-wide readiness walk")
	}

	// AND THE CAP IS PER PASS, not per process: the next sweep builds its own budget, which is
	// what makes the walk repeatable rather than once-ever.
	if !newMqttRecoveryBudget().takeReadinessAttempt() {
		t.Fatal("the cap outlived the pass that set it")
	}

	// SPENDING DOES NOT BUY ONE. The two bounds are separate, and an unspent budget is not a
	// second attempt.
	c := newMqttRecoveryBudget()
	if !c.takeReadinessAttempt() || !c.spend(1) || c.takeReadinessAttempt() {
		t.Fatal("ordinary budget left over was mistaken for a second readiness attempt")
	}
}

// TestF105ProgressCapacityIsReserved pins the budget's three states and its reserve.
func TestF105ProgressCapacityIsReserved(t *testing.T) {
	b := newMqttRecoveryBudget()
	if !b.fitsAUnit() {
		t.Fatal("a fresh budget does not fit one complete unit")
	}

	// ORDINARY WORK STOPS AT THE RESERVE, so the progress write can never be the thing that does
	// not fit.
	spent := 0
	for b.spend(1) {
		spent++
	}
	if b.left != b.reserve {
		t.Fatalf("ordinary work ran the budget to %d with a reserve of %d", b.left, b.reserve)
	}
	if spent != mqttRecoveryOpBudget-mqttRecoveryOpReserve {
		t.Fatalf("ordinary work took %d operations, want %d",
			spent, mqttRecoveryOpBudget-mqttRecoveryOpReserve)
	}
	if b.fitsAUnit() {
		t.Fatal("an exhausted budget still claims to fit a complete unit")
	}

	// AND THE RESERVE IS STILL THERE FOR PROGRESS.
	if !b.spendProgress(1) {
		t.Fatal("the reserve did not cover a progress write")
	}
	for b.spendProgress(1) {
	}
	if b.left != 0 {
		t.Fatalf("the reserve did not drain to zero: %d", b.left)
	}
	if b.progress == 0 {
		t.Fatal("no progress write was accounted for")
	}
}

// TestF105TheRecoveryPassStopsWithItsAccountManager establishes that a pass ends when its manager
// says stop, between store operations rather than between sweeps.
func TestF105TheRecoveryPassStopsWithItsAccountManager(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-pass-stops")
	defer cleanup()

	before := mqttRecoveryPassRuns.Load()
	// A stop that is true from the outset: the pass must complete — and count — without opening
	// a single shard.
	e.asm.sweepOutboundRecovery(e.asm.server(), func() bool { return true })
	if mqttRecoveryPassRuns.Load() != before+1 {
		t.Fatal("a stopped pass did not complete")
	}
}

// TestF105ThePassVisitsAnEnrolledRootAndRetiresIt is the driver's end-to-end arithmetic: a
// generation closed with an empty inventory is born finished, so the first visit retires it —
// certificate first, then the root.
func TestF105ThePassVisitsAnEnrolledRootAndRetiresIt(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-pass-retires")
	defer cleanup()
	auth, _ := e.bootstrap(t, "txn-boot")

	if _, err := e.asm.jsa.opCloseGeneration(e.id, e.epoch, mqttNewInflightNonce(), auth.Gen, auth.Tenure,
		auth.OwnerTxn); err != nil {
		t.Fatalf("closing the generation: %v", err)
	}
	closed, _, _ := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen)
	if closed == nil {
		t.Fatal("the closure does not stand")
	}

	// The shard walk resumes round the fixed shard count, so one pass per shard is enough to
	// reach this client's.
	for i := 0; i < mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		if root, _, _ := e.asm.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd); root == nil {
			break
		}
	}

	cert, cerr := e.asm.jsa.loadGenCompletionCert(e.id, e.epoch, testF105Gen)
	if cerr != nil {
		t.Fatalf("reading the completion certificate: %v", cerr)
	}
	if cert == nil {
		t.Fatal("an empty generation was never certified: its root is born finished and retires " +
			"on the first visit")
	}
	if cert.SealedHi != 0 {
		t.Fatalf("the certificate covers bound %d, and an empty inventory seals at 0", cert.SealedHi)
	}
	root, _, rerr := e.asm.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd)
	if rerr != nil {
		t.Fatalf("reading the root back: %v", rerr)
	}
	if root != nil {
		t.Fatal("the root outlived its certificate, so retirement did not complete")
	}

	// AND THE GATE STILL CLEARS THE GENERATION, on the certificate rather than on the root. That
	// is the whole reason the certificate stands on its own: recovery may finish BEFORE the
	// teardown gets there, and requiring an open cursor would deadlock the release for ever.
	f, ferr := e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen)
	if ferr != nil {
		t.Fatalf("gathering release facts after retirement: %v", ferr)
	}
	if f.RootEnrolled {
		t.Fatal("a retired root was still reported as enrolled")
	}
	if f.Cert == nil {
		t.Fatal("the gate did not read the completion certificate back")
	}
	if err := mqttAdoptedReleaseAuthorized(e.id, testF105Gen, f); err != nil {
		t.Fatalf("a certified generation was not cleared for release: %v", err)
	}
}

// TestF105AHoleIsAnEmptySlotNotARetirement establishes that a missing root claims nothing about
// any generation, and does not end the shard walk either.
func TestF105AHoleIsAnEmptySlotNotARetirement(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-hole")
	defer cleanup()

	shard := mqttDirShardFor(getHash(e.id))
	// A header covering three ordinals with NOTHING at any of them: three holes.
	testF105PlantShard(t, e, shard, 3)

	before := mqttRecoveryPassRuns.Load()
	for i := 0; i < mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}
	if mqttRecoveryPassRuns.Load() == before {
		t.Fatal("no pass ran")
	}

	// NOTHING WAS CERTIFIED. A hole names no generation, so there is nothing a certificate could
	// be about — and a pass that treated one as a retirement would be writing a claim out of an
	// absence.
	if cert, cerr := e.asm.jsa.loadGenCompletionCert(e.id, e.epoch, testF105Gen); cerr != nil || cert != nil {
		t.Fatalf("a hole produced a completion certificate: %+v (%v)", cert, cerr)
	}
	// AND THE WALK MOVED PAST THEM: the header's position advanced beyond the holes rather than
	// stopping at the first one.
	hdr, _, herr := e.asm.jsa.loadDirShardAt(shard)
	if herr != nil || hdr == nil {
		t.Fatalf("the header is gone: %v", herr)
	}
	if hdr.Next == 1 && hdr.Cycle == 1 {
		t.Fatalf("the walk never advanced past the holes: %+v", hdr)
	}
}

// TestF105TheSealRequiresAWellFormedReadinessRecord is the affine fence, stated against arbitrary
// batches rather than through the composer that respects it.
func TestF105TheSealRequiresAWellFormedReadinessRecord(t *testing.T) {
	// The affine enrolment rides a DISCARDING FINALIZATION, because that is the only closure
	// that may carry one: `close-admit` refuses a discard outright — ending a generation is not
	// authority to abandon its obligations — and a closure that preserves them leaves its affine
	// consumer to the incarnation that continues. So the fixture takes the production route:
	// prepare a handoff carrying the retirement's targets, commit its metadata half, finalize.
	// The affine enrolment rides a DISCARDING FINALIZATION, because that is the only closure that
	// may carry one: `close-admit` refuses a discard outright — ending a generation is not
	// authority to abandon its obligations — and a closure that preserves them leaves its affine
	// consumer to the incarnation that continues.
	plant := func(t *testing.T, e *testF105Env) *mqttOutboundResource {
		t.Helper()
		const affGen = "0123456789abcdefghijkq"
		_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
		_, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
		return r
	}

	t.Run("no readiness at all", func(t *testing.T) {
		e, cleanup := testF105NewEnv(t, "client-f105-seal-noready")
		defer cleanup()
		r := plant(t, e)
		_, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, r.Inc)
		root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}
		err := e.asm.jsa.opSealAffineProvisioning(root, r, rseq, mqttNewInflightNonce())
		if err == nil {
			t.Fatal("an affine resource was sealed with no readiness assertion standing")
		}
		if !strings.Contains(err.Error(), "no readiness assertion stands") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	t.Run("readiness about another consumer", func(t *testing.T) {
		e, cleanup := testF105NewEnv(t, "client-f105-seal-wrongready")
		defer cleanup()
		r := plant(t, e)
		// A WELL-FORMED record for ANOTHER affine generation: its durable is DERIVED, so the
		// writer accepts it and the seal's own comparison is what has to refuse. A made-up name
		// would now be refused at the write, which tests the validator rather than the seal.
		const other = "0123456789abcdefghijkz"
		_, _, otherDur := mqttQ2AffineNames(getHash(e.id), other)
		if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
			AffGen: other, Durable: otherDur, PubRelDur: r.PubRelDur,
		}); err != nil {
			t.Fatalf("publishing the readiness: %v", err)
		}
		_, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, r.Inc)
		root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}
		err := e.asm.jsa.opSealAffineProvisioning(root, r, rseq, mqttNewInflightNonce())
		if err == nil {
			t.Fatal("a readiness assertion about another consumer sealed this one")
		}
		if !strings.Contains(err.Error(), "asserts nothing about this consumer") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	t.Run("readiness naming another proof source", func(t *testing.T) {
		// HOLISTIC ROUND 12. A record could keep the client, epoch, generation, affine generation
		// and durable and substitute another non-empty PubRelDur — and both readers compared only
		// the affine pair, so the seal rested on an assertion naming a DIFFERENT proof source than
		// the resource whose consumer it licenses deleting.
		//
		// Planted RAW, because the validator now refuses this at the write too: the threat is a
		// record already standing on the stream, so the READ-side refusal is what must hold.
		e, cleanup := testF105NewEnv(t, "client-f105-seal-wrongproofs")
		defer cleanup()
		r := plant(t, e)
		_, _, otherPubRel := mqttPubRelNames(getHash(e.id), testF105Gen2)
		if otherPubRel == r.PubRelDur {
			t.Fatal("the fixture's two PUBREL names coincide, so nothing is substituted")
		}
		body, merr := json.Marshal(&mqttAffineReadiness{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
			AffGen: r.AffGen, Durable: r.Name, PubRelDur: otherPubRel,
		})
		if merr != nil {
			t.Fatalf("marshalling the substituted readiness: %v", merr)
		}
		msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
		if _, serr := e.asm.jsa.storeMsg(mqttReadinessSubject(getHash(e.id), testF105Gen),
			hdr, msg); serr != nil {
			t.Fatalf("planting the substituted readiness: %v", serr)
		}
		_, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, r.Inc)
		root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}
		if err := e.asm.jsa.opSealAffineProvisioning(root, r, rseq,
			mqttNewInflightNonce()); err == nil {
			t.Fatal("a readiness assertion naming another PUBREL consumer sealed this resource, " +
				"so the seal rests on proofs secured somewhere else")
		}
	})

	t.Run("the matching assertion seals", func(t *testing.T) {
		e, cleanup := testF105NewEnv(t, "client-f105-seal-ok")
		defer cleanup()
		r := plant(t, e)
		if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
			AffGen: r.AffGen, Durable: r.Name, PubRelDur: r.PubRelDur,
		}); err != nil {
			t.Fatalf("publishing the readiness: %v", err)
		}
		_, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, r.Inc)
		root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}
		if err := e.asm.jsa.opSealAffineProvisioning(root, r, rseq, mqttNewInflightNonce()); err != nil {
			t.Fatalf("a matching readiness assertion did not seal: %v", err)
		}
		after, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, r.Inc)
		if after == nil || after.Prov != mqttProvisioningSealed {
			t.Fatalf("the resource did not reach SEALED: %+v", after)
		}
	})

	t.Run("a matching but PREMATURE assertion also seals, and that is the trust boundary", func(t *testing.T) {
		// The boundary establishes WHOM the assertion is about. It cannot establish that the
		// proofs were secured — matching identifiers are not evidence of work — so a producer
		// that skipped the acts satisfies this rule. That limit is asserted here rather than
		// left to be discovered: the producers are enumerated and audited instead.
		e, cleanup := testF105NewEnv(t, "client-f105-seal-premature")
		defer cleanup()
		r := plant(t, e)
		if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
			AffGen: r.AffGen, Durable: r.Name, PubRelDur: r.PubRelDur,
		}); err != nil {
			t.Fatalf("publishing the readiness: %v", err)
		}
		_, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, r.Inc)
		root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen}
		if err := e.asm.jsa.opSealAffineProvisioning(root, r, rseq, mqttNewInflightNonce()); err != nil {
			t.Fatalf("the trust boundary is narrower than stated: %v", err)
		}
	})
}

// TestF105ReadinessIsIdempotentAndConflictsAreErrors: two producers may reach it, and they write
// the same facts.
func TestF105ReadinessIsIdempotentAndConflictsAreErrors(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-ready-idem")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)
	ready := &mqttAffineReadiness{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
		AffGen: affGen, Durable: durable, PubRelDur: pubRelDur,
	}
	if err := e.asm.jsa.publishAffineReadiness(ready); err != nil {
		t.Fatalf("the first producer failed: %v", err)
	}
	// THE SECOND PRODUCER FINDS THE SAME FACT and is done, rather than competing for the subject.
	if err := e.asm.jsa.publishAffineReadiness(ready); err != nil {
		t.Fatalf("an identical second assertion was refused: %v", err)
	}
	// A CONFLICTING ONE IS AN ERROR: one generation has one affine consumer.
	conflicting := *ready
	conflicting.Durable = "$MQTT_Q2AFF_b"
	if err := e.asm.jsa.publishAffineReadiness(&conflicting); err == nil {
		t.Fatal("two different affine consumers were asserted for one generation")
	}
}

// TestF105ACertificateMustStateItsCoverage exercises the READ path, which is where the required
// member is enforced.
//
// A zero bound is LEGAL — an empty inventory seals at zero — so the value alone can no longer say
// whether the certificate stated its coverage. What says it is the member being PRESENT and
// non-null in the body: an omitted one would decode into that legal zero, and a generation sealed
// higher would then be released on a certificate that never said what it covered.
func TestF105ACertificateMustStateItsCoverage(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-cert-members")
	defer cleanup()

	store := func(t *testing.T, body string) error {
		t.Helper()
		subj := mqttGenCertSubject(getHash(e.id), testF105Gen)
		var expect uint64
		if cur, cerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj); cerr == nil && cur != nil {
			expect = cur.Sequence
		}
		msg, hdr := mqttFrameBatchWrite(expect, []byte(body), _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(subj, hdr, msg); err != nil {
			t.Fatalf("planting the certificate: %v", err)
		}
		_, lerr := e.asm.jsa.loadGenCompletionCert(e.id, e.epoch, testF105Gen)
		return lerr
	}

	// OMITTED: refused, rather than read as a certificate about an empty inventory.
	body := `{"id":"` + e.id + `","epoch":"` + e.epoch + `","gen":"` + testF105Gen + `"}`
	if err := store(t, body); err == nil {
		t.Fatal("a certificate that never stated its coverage was accepted")
	}
	// NULL: the same, because a null member is not a stated one either.
	body = `{"id":"` + e.id + `","epoch":"` + e.epoch + `","gen":"` + testF105Gen +
		`","sealed_hi":null}`
	if err := store(t, body); err == nil {
		t.Fatal("a certificate with a null bound was accepted")
	}
	// AN EXPLICIT ZERO IS ACCEPTED, which is what the two refusals above must not be confused
	// with: a generation that admitted nothing seals at zero and its completion covers zero.
	body = `{"id":"` + e.id + `","epoch":"` + e.epoch + `","gen":"` + testF105Gen +
		`","sealed_hi":0}`
	if err := store(t, body); err != nil {
		t.Fatalf("a certificate covering an empty inventory was refused: %v", err)
	}
}

// TestF105TheSweepIsChargedPerEntry: the budget bounds STORE OPERATIONS, not invocations.
//
// One call to the sweep reads up to 128 entries. Charging the call rather than the reads bought
// 128 round trips for one unit, so the advertised limit — and the shutdown latency that rests on
// it — meant nothing under a large inventory. This pins the charge at the entry.
func TestF105TheSweepIsChargedPerEntry(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-sweep-charge")
	defer cleanup()
	e.bootstrap(t, "txn-boot")

	// An inventory of five duties, sealed, with a root over it.
	members := []mqttTransitionMember{testF105LedgerMember(t, e, 0, &mqttGenLedger{
		Lo: 1, Del: 1,
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, State: mqttGenLedgerOpen, Hi: 5,
	})}
	for ord := uint64(1); ord <= 5; ord++ {
		members = append(members, testF105DutyMember(t, e, 0,
			testF105Duty(e, ord, mqttDutyCleanup, "inc-"+string(rune('a'+ord)), mqttDutyNotYetActionable)))
	}
	if err := testF105SubmitInventory(t, e, "txn-alloc", members...); err != nil {
		t.Fatalf("planting the inventory: %v", err)
	}
	// start 1, resume 1, bound 5, nothing unresolved: a sweep with five entries ahead of it.
	root := testF105EnvRoot(e, 1, 1, 5, 0)
	testF105PlantRoot(t, e, root)

	// A CHARGE THAT ALLOWS EXACTLY TWO READS. A per-call charge would read all five.
	allowed := 2
	seen := 0
	charge := func() bool {
		if allowed == 0 {
			return false
		}
		allowed--
		seen++
		return true
	}
	_, out, err := e.asm.jsa.sweepOneBatchCharged(root, charge, false)
	if err != nil {
		t.Fatalf("sweeping under a charge: %v", err)
	}
	if seen != 2 {
		t.Fatalf("the charge was consulted %d times for a five-entry inventory", seen)
	}
	if out.Examined != 2 {
		t.Fatalf("the batch reported %d entries examined against a budget of 2: the cursor would "+
			"advance over ordinals nothing read", out.Examined)
	}

	// AND THE CURSOR ADVANCES OVER EXACTLY WHAT WAS EXAMINED.
	next, aerr := mqttAdvanceSweep(root, out)
	if aerr != nil {
		t.Fatalf("advancing the cursor: %v", aerr)
	}
	if next.Resume != root.Resume+2 {
		t.Fatalf("the cursor advanced to %d from %d after two entries", next.Resume, root.Resume)
	}
}

// TestF105AnAffineResourceNamesItsOwnConsumer: the name the executor DELETES is derived, never
// merely carried.
//
// Trusting the closing caller for the COMPLETENESS of a target set never licensed trusting it for
// the IDENTITY of one target. A record whose name is any string would point the deletion at
// another live consumer on `$MQTT_out` — and the pass would discard the stated generation's
// claims, publish a matching readiness and remove a consumer belonging to somebody else. Both
// names are derived from an identity the record itself carries, so a mismatch is a record this
// broker refuses to read rather than a deletion it performs.
func TestF105AnAffineResourceNamesItsOwnConsumer(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-affine-name")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	base := &mqttOutboundResource{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: mqttNewInflightNonce(),
		Kind: mqttResourceAffineConsumer, Name: durable, State: mqttResourceLive,
		Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{},
		AffGen: affGen, PubRelDur: pubRelDur,
	}
	if err := mqttValidateOutboundResource(base, e.id, e.epoch, testF105Gen, base.Inc); err != nil {
		t.Fatalf("a correctly derived affine resource was refused: %v", err)
	}

	// ANOTHER LIVE CONSUMER'S NAME. Exactly the shape that would aim the executor's delete at a
	// consumer this generation does not own.
	foreign := *base
	foreign.Name = "$MQTT_Q2AFF_someone_else"
	if err := mqttValidateOutboundResource(&foreign, e.id, e.epoch, testF105Gen, foreign.Inc); err == nil {
		t.Fatal("an affine resource naming another consumer was accepted")
	} else if !strings.Contains(err.Error(), "affine generation") ||
		!strings.Contains(err.Error(), "owns") {
		// The wording is the SHARED descriptor definition's since round 14 consolidated the three
		// copies into one; what must still hold is that the refusal names the DERIVATION — the
		// generation and the durable it owns — rather than some unrelated rule.
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// AND ANOTHER GENERATION'S PUBREL CONSUMER, which would read the proofs it owes from records
	// belonging to a different incarnation.
	otherGen := *base
	otherGen.PubRelDur = "$MQTT_PUBREL_someone_else"
	if err := mqttValidateOutboundResource(&otherGen, e.id, e.epoch, testF105Gen, otherGen.Inc); err == nil {
		t.Fatal("an affine resource naming another generation's PUBREL consumer was accepted")
	} else if !strings.Contains(err.Error(), "PUBREL consumer") ||
		!strings.Contains(err.Error(), "owns") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// AND A NAME DERIVED FROM A DIFFERENT AFFINE GENERATION is refused for the same reason: the
	// pair has to agree, because the durable IS the affine generation's name.
	mixed := *base
	_, _, mixed.Name = mqttQ2AffineNames(getHash(e.id), "0123456789abcdefghijkz")
	if err := mqttValidateOutboundResource(&mixed, e.id, e.epoch, testF105Gen, mixed.Inc); err == nil {
		t.Fatal("an affine resource whose name belongs to another affine generation was accepted")
	}
}
