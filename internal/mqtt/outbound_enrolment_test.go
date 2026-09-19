//go:build mqttport_fulltests

// Copyright 2026 The mqtt-nats-bridge Authors
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

// A closing batch enrols its generation for recovery, and the rules are stated FROM THE CLOSURE.
//
// Every fixture here goes through the write boundary rather than through a composer, because the
// bypasses these rules exist to close are OMISSIONS — a closure that carries no ledger member, a
// first enrolment that seals directly and allocates no root — and a composer never produces one.
// A rule proven only through the composer is a rule the boundary never had to make.

// testF105ClosingBatch submits a closing transition with the given extra members, under a
// closure the caller shapes.
func testF105ClosingBatch(t *testing.T, e *testF105Env, prev *mqttOutboundAuthority, prevSeq uint64,
	txn string, closure *mqttGenClosure, members ...mqttTransitionMember) error {
	t.Helper()
	_, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionCloseAdmit, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityClosed, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		Closure: closure,
		Extra:   members,
	})
	return err
}

// testF105Closure builds a closure naming a root address.
func testF105ClosureRecord(e *testF105Env, txn string, discard bool, shard, ord uint64) *mqttGenClosure {
	return &mqttGenClosure{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Txn: txn, Discard: discard,
		RootShard: shard, RootOrd: ord,
	}
}

func testF105Member(t *testing.T, subj string, expect uint64, v any) mqttTransitionMember {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshalling %q: %v", subj, err)
	}
	return mqttTransitionMember{subj: subj, expect: expect, body: body}
}

// TestF105ClosureAllocatesAReachableRoot is the positive baseline: one batch leaves the closure,
// a sealed ledger, the shard header and a root the closure's own address names.
func TestF105ClosureAllocatesAReachableRoot(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-enrol-happy")
	defer cleanup()
	auth, _ := e.bootstrap(t, "txn-boot")

	if err := e.asm.jsa.closeGenerationEnrolled(e.id, e.epoch, auth.Gen, auth.Tenure,
		auth.OwnerTxn, false, mqttClosingTargets{}); err != nil {
		t.Fatalf("closing the generation: %v", err)
	}

	closed, _, status := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen)
	if status != mqttOutboundValid {
		t.Fatalf("the closure does not stand: status %v", status)
	}
	if closed.RootOrd == 0 {
		t.Fatalf("the closure names no recovery ordinal")
	}
	hdr, _, herr := e.asm.jsa.loadDirShardAt(closed.RootShard)
	if herr != nil || hdr == nil {
		t.Fatalf("the directory shard does not stand: %v", herr)
	}
	if closed.RootOrd > hdr.Hi {
		t.Fatalf("the closure names ordinal %d outside the shard's high-water %d",
			closed.RootOrd, hdr.Hi)
	}
	root, _, rerr := e.asm.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd)
	if rerr != nil || root == nil {
		t.Fatalf("the recovery root does not stand: %v", rerr)
	}
	if root.Gen != testF105Gen || root.ID != e.id || root.Epoch != e.epoch {
		t.Fatalf("the root names %q/%q/%q", root.ID, root.Epoch, root.Gen)
	}
	led, _, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, testF105Gen)
	if lerr != nil || led == nil || led.State != mqttGenLedgerSealed {
		t.Fatalf("the ledger is not sealed: %v %+v", lerr, led)
	}
	if root.Bound != led.SealedHi {
		t.Fatalf("the root is bounded at %d and the ledger sealed at %d", root.Bound, led.SealedHi)
	}
	// AND THE GATE FINDS IT BY ADDRESS.
	reach, gerr := e.asm.recoveryRootReachable(e.id, e.epoch, testF105Gen, closed)
	if gerr != nil || !reach {
		t.Fatalf("the gate does not reach the root it was handed the address of: %v %v", reach, gerr)
	}
}

// TestF105EveryClosureSealsAndRoots pins the two arms that ONLY boundary rule 1 can reject: a
// closure carrying no ledger, header or root member at all, over a generation whose ledger stands
// OPEN, and the same over a generation with no ledger yet. Both satisfy every other rule
// vacuously, so nothing else is able to refuse them.
func TestF105EveryClosureSealsAndRoots(t *testing.T) {
	t.Run("open ledger, no members", func(t *testing.T) {
		e, cleanup := testF105NewEnv(t, "client-f105-enrol-open")
		defer cleanup()
		auth, seq := e.bootstrap(t, "txn-boot")
		// An OPEN ledger with one duty, so the generation genuinely has an inventory.
		if err := testF105SubmitInventory(t, e, "txn-alloc",
			testF105LedgerMember(t, e, 0, &mqttGenLedger{
				ID: e.id, Epoch: e.epoch, Gen: testF105Gen, State: mqttGenLedgerOpen, Hi: 1,
			}),
			testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyCleanup, "inc-a", mqttDutyNotYetActionable)),
		); err != nil {
			t.Fatalf("planting an open inventory: %v", err)
		}
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		err := testF105ClosingBatch(t, e, al.rec, al.seq, "txn-close",
			testF105ClosureRecord(e, "txn-close", false, mqttDirShardFor(getHash(e.id)), 1))
		if err == nil {
			t.Fatal("a closure that seals nothing was accepted")
		}
		if !strings.Contains(err.Error(), "without sealing its inventory") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
		_ = auth
		_ = seq
	})

	t.Run("absent ledger, no members", func(t *testing.T) {
		e, cleanup := testF105NewEnv(t, "client-f105-enrol-absent")
		defer cleanup()
		auth, seq := e.bootstrap(t, "txn-boot")
		err := testF105ClosingBatch(t, e, auth, seq, "txn-close",
			testF105ClosureRecord(e, "txn-close", false, mqttDirShardFor(getHash(e.id)), 1))
		if err == nil {
			t.Fatal("a closure over a generation with no ledger sealed nothing and was accepted")
		}
		if !strings.Contains(err.Error(), "without sealing its inventory") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	t.Run("sealed ledger, no root", func(t *testing.T) {
		e, cleanup := testF105NewEnv(t, "client-f105-enrol-noroot")
		defer cleanup()
		auth, seq := e.bootstrap(t, "txn-boot")
		err := testF105ClosingBatch(t, e, auth, seq, "txn-close",
			testF105ClosureRecord(e, "txn-close", false, mqttDirShardFor(getHash(e.id)), 1),
			testF105LedgerMember(t, e, 0, &mqttGenLedger{
				ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
				State: mqttGenLedgerSealed, Hi: 0, SealedHi: 0,
			}),
		)
		if err == nil {
			t.Fatal("a closure sealed its inventory and allocated no root, and was accepted")
		}
		if !strings.Contains(err.Error(), "allocates no recovery root") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})
}

// TestF105RootMembersAreRefusedOutsideAClosingTransition establishes that a transition writing no
// closure may not carry a directory member at all.
func TestF105RootMembersAreRefusedOutsideAClosingTransition(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-root-outside")
	defer cleanup()
	e.bootstrap(t, "txn-boot")

	shard := mqttDirShardFor(getHash(e.id))
	root := &mqttRecoveryRoot{
		Shard: shard, Ord: 1, ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
		SweepStart: 1, Resume: 1, Bound: 0,
	}
	err := testF105SubmitInventory(t, e, "txn-smuggle",
		testF105Member(t, mqttRootSubject(shard, 1), 0, root))
	if err == nil {
		t.Fatal("a dispose-resource transition carried a recovery root and was accepted")
	}
	// TWO guards stand in front of this, and the fixture names both rather than assuming which
	// one fires: the per-kind member list of a dispose-resource transition, and the member
	// classifier's rule that a directory member belongs only to a closing transition. A mutation
	// that opens this route has to remove BOTH, which is what the paired mutation does.
	if !strings.Contains(err.Error(), "only a closing transition may carry") &&
		!strings.Contains(err.Error(), "neither a resource nor its inventory") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// TestF105RootBoundEqualsTheSealTheBatchInstalls makes the pre-batch and post-batch bounds DIFFER,
// so a root bounded at the earlier one is refused rather than accidentally equal.
func TestF105RootBoundEqualsTheSealTheBatchInstalls(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-root-bound")
	defer cleanup()
	e.bootstrap(t, "txn-boot")

	// One duty already allocated, so the ledger stands OPEN at 1; the closing batch seals at 2.
	if err := testF105SubmitInventory(t, e, "txn-alloc",
		testF105LedgerMember(t, e, 0, &mqttGenLedger{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, State: mqttGenLedgerOpen, Hi: 1,
		}),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyCleanup, "inc-a", mqttDutyNotYetActionable)),
	); err != nil {
		t.Fatalf("planting an open inventory: %v", err)
	}
	led, ledSeq, _ := e.asm.jsa.loadGenLedger(e.id, e.epoch, testF105Gen)
	if led == nil || led.Hi != 1 {
		t.Fatalf("the planted ledger is not open at 1: %+v", led)
	}

	shard := mqttDirShardFor(getHash(e.id))
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	// The batch seals at 1 and the root is bounded at 0 — the pre-inventory bound, which is the
	// mutation this test exists to kill. Nothing else differs, so only the bound rule can refuse.
	err := testF105ClosingBatch(t, e, al.rec, al.seq, "txn-close",
		testF105ClosureRecord(e, "txn-close", false, shard, 1),
		testF105LedgerMember(t, e, ledSeq, &mqttGenLedger{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
			State: mqttGenLedgerSealed, Hi: 1, SealedHi: 1,
		}),
		testF105Member(t, mqttShardSubject(shard), 0,
			&mqttDirShard{Shard: shard, Hi: 1, Next: 1, Cycle: 1, Horizon: 1}),
		testF105Member(t, mqttRootSubject(shard, 1), 0, &mqttRecoveryRoot{
			Shard: shard, Ord: 1, ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
			SweepStart: 1, Resume: 1, Bound: 0,
		}),
	)
	if err == nil {
		t.Fatal("a root bounded at the pre-batch high-water was accepted")
	}
	if !strings.Contains(err.Error(), "a root's bound is the inventory this batch fixes") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// TestF105HeaderAllocationPreservesTraversalState starts from distinct non-default traversal
// state and asserts all three fields survive an allocation untouched.
func TestF105HeaderAllocationPreservesTraversalState(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-header-preserve")
	defer cleanup()
	auth, _ := e.bootstrap(t, "txn-boot")

	shard := mqttDirShardFor(getHash(e.id))
	// A header with a walk already in progress: cycle 4, horizon 3, position 3.
	planted := &mqttDirShard{Shard: shard, Hi: 3, Next: 3, Cycle: 4, Horizon: 3}
	body, _ := json.Marshal(planted)
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttShardSubject(shard), hdr, msg); err != nil {
		t.Fatalf("planting the shard header: %v", err)
	}

	if err := e.asm.jsa.closeGenerationEnrolled(e.id, e.epoch, auth.Gen, auth.Tenure,
		auth.OwnerTxn, false, mqttClosingTargets{}); err != nil {
		t.Fatalf("closing the generation: %v", err)
	}
	got, _, gerr := e.asm.jsa.loadDirShardAt(shard)
	if gerr != nil || got == nil {
		t.Fatalf("the shard header does not stand: %v", gerr)
	}
	if got.Hi != 4 {
		t.Fatalf("the high-water moved from 3 to %d, and an allocation takes exactly one", got.Hi)
	}
	if got.Next != 3 || got.Cycle != 4 || got.Horizon != 3 {
		t.Fatalf("allocation moved a walker: next %d, cycle %d, horizon %d (want 3, 4, 3)",
			got.Next, got.Cycle, got.Horizon)
	}
}

// TestF105AnArbitraryBatchCannotForgeAnEnrolment walks the structural refusals, each arm built so
// only the rule under test can reject it.
func TestF105AnArbitraryBatchCannotForgeAnEnrolment(t *testing.T) {
	shardOf := func(e *testF105Env) uint64 { return mqttDirShardFor(getHash(e.id)) }

	// A helper that closes with a caller-shaped header and root.
	try := func(t *testing.T, name string, mutate func(e *testF105Env, hdr *mqttDirShard,
		root *mqttRecoveryRoot, closure *mqttGenClosure), want string) {

		t.Helper()
		e, cleanup := testF105NewEnv(t, "client-f105-forge-"+name)
		defer cleanup()
		auth, seq := e.bootstrap(t, "txn-boot")
		shard := shardOf(e)
		hdr := &mqttDirShard{Shard: shard, Hi: 1, Next: 1, Cycle: 1, Horizon: 1}
		root := &mqttRecoveryRoot{
			Shard: shard, Ord: 1, ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
			SweepStart: 1, Resume: 1, Bound: 0,
		}
		closure := testF105ClosureRecord(e, "txn-close", false, shard, 1)
		mutate(e, hdr, root, closure)
		err := testF105ClosingBatch(t, e, auth, seq, "txn-close", closure,
			testF105LedgerMember(t, e, 0, &mqttGenLedger{
				ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
				State: mqttGenLedgerSealed, Hi: 0, SealedHi: 0,
			}),
			testF105Member(t, mqttShardSubject(shard), 0, hdr),
			testF105Member(t, mqttRootSubject(root.Shard, root.Ord), 0, root),
		)
		if err == nil {
			t.Fatalf("%s: the batch was accepted", name)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: refused for the wrong reason: %v", name, err)
		}
	}

	t.Run("header moves Hi by two", func(t *testing.T) {
		try(t, "hi-by-two", func(_ *testF105Env, hdr *mqttDirShard, root *mqttRecoveryRoot, _ *mqttGenClosure) {
			hdr.Hi, root.Ord = 2, 2
		}, "a first allocation creates ordinal 1")
	})
	t.Run("header rewrites the traversal position", func(t *testing.T) {
		try(t, "rewrites-next", func(_ *testF105Env, hdr *mqttDirShard, _ *mqttRecoveryRoot, _ *mqttGenClosure) {
			hdr.Next, hdr.Horizon, hdr.Cycle = 1, 1, 7
		}, "a first allocation creates ordinal 1 and a first cycle over it")
	})
	t.Run("closure names another address", func(t *testing.T) {
		try(t, "wrong-address", func(_ *testF105Env, _ *mqttDirShard, _ *mqttRecoveryRoot, c *mqttGenClosure) {
			c.RootOrd = 2
		}, "and allocates")
	})
	t.Run("root names another generation", func(t *testing.T) {
		try(t, "wrong-gen", func(_ *testF105Env, _ *mqttDirShard, root *mqttRecoveryRoot, _ *mqttGenClosure) {
			root.Gen = "0000000000000000000000"
		}, "allocates a root naming")
	})
	t.Run("root cursor is not at birth", func(t *testing.T) {
		try(t, "cursor", func(_ *testF105Env, _ *mqttDirShard, root *mqttRecoveryRoot, _ *mqttGenClosure) {
			root.Bound, root.Resume = 3, 2
		}, "a cursor that is not at birth")
	})
}

// testF105EnrolClosing completes a hand-built closing transition so it satisfies the enrolment
// rules, deriving the same members the production composer derives.
//
// Every closure seals and roots (F-105, row 6), so a fixture that hand-wrote only the closure is
// no longer building a legal batch. This fills in the address and appends the enrolment rather
// than having each fixture restate it: a test that spelled the members out would be asserting the
// shape of the enrolment while trying to test something else, and would drift from the composer
// the moment either changed.
//
// It preserves whatever the fixture already put in Extra, because several of them carry members
// of their own and the enrolment is additive.
func testF105EnrolClosing(t *testing.T, e *testF105Env, tr *mqttAuthorityTransition) *mqttAuthorityTransition {
	t.Helper()
	if tr.Closure == nil {
		return tr
	}
	gen := tr.Closure.Gen
	// IDEMPOTENT. Several fixtures already compose their own enrolment — the shared-durable ones
	// deliberately do — and appending a second ledger member would make the batch name one
	// subject twice, which the boundary refuses for a reason that has nothing to do with the rule
	// under test.
	for _, m := range tr.Extra {
		if mqttIsRootSubject(m.subj) {
			return tr
		}
	}
	for _, m := range tr.Extra {
		if mqttIsLedgerSubject(m.subj) {
			// A fixture that seals its own ledger needs only the directory half, derived against
			// that member's bound rather than against the store.
			sealedHi, _, serr := mqttSealedHiOf(tr.Extra, e.id, e.epoch, gen)
			if serr != nil {
				// A fixture whose ledger member is deliberately NOT sealed is testing the seal
				// rule itself; it needs no directory half, because rule 1 refuses it before the
				// address is ever consulted.
				return tr
			}
			hdr, hdrSeq, _ := e.asm.jsa.loadDirShardAt(mqttDirShardFor(getHash(e.id)))
			sm, rm, ord, aerr := mqttAllocateRecoveryRoot(e.id, e.epoch, gen, hdr, hdrSeq, sealedHi)
			if aerr != nil {
				t.Fatalf("allocating the recovery root: %v", aerr)
			}
			tr.Closure.RootShard, tr.Closure.RootOrd = sm.shardOf(), ord
			tr.Extra = append(tr.Extra, sm, rm)
			return tr
		}
	}
	cur, curSeq, _ := e.asm.jsa.loadGenLedger(e.id, e.epoch, gen)
	hdr, hdrSeq, _ := e.asm.jsa.loadDirShardAt(mqttDirShardFor(getHash(e.id)))
	enr, err := mqttComposeClosingEnrolment(e.id, e.epoch, gen, tr.Closure.Discard,
		mqttClosingTargets{}, cur, curSeq, hdr, hdrSeq)
	if err != nil {
		t.Fatalf("composing the closing enrolment for generation %q: %v", gen, err)
	}
	tr.Closure.RootShard, tr.Closure.RootOrd = enr.RootShard, enr.RootOrd
	tr.Extra = append(tr.Extra, enr.Members...)
	return tr
}

// TestF105ALedgerIsSealedOnlyInAClosingBatch is holistic round 10's finding, and the worst of the
// no-closure bypasses because its consequence is PERMANENT.
//
// Sealing is the closing act: the enrolment rules derive the root's bound from the seal the batch
// installs, so a seal with no closure is a seal no root was allocated against. And it wedges the
// generation — mqttEnrolSharedDurables refuses an already-sealed ledger, so the normal composers
// can never close it, the inventory cannot be reopened, and no root will ever exist for the
// recovery pass to find. A generation that can neither close nor be recovered is exactly what the
// enrolment exists to prevent.
func TestF105ALedgerIsSealedOnlyInAClosingBatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expect uint64
	}{
		{"created sealed", 0},
		{"open moved to sealed", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, cleanup := testF105NewEnv(t, "client-f105-seal-"+strings.ReplaceAll(tc.name, " ", "-"))
			defer cleanup()
			auth, seq := e.bootstrap(t, "txn-boot")

			expect := tc.expect
			if expect != 0 {
				// Give the generation a real OPEN ledger first, so the second arm is a genuine
				// OPEN-to-SEALED move rather than another create.
				if err := testF105SubmitInventory(t, e, "txn-open",
					testF105LedgerMember(t, e, 0, &mqttGenLedger{
						ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
						State: mqttGenLedgerOpen, Hi: 0,
					})); err != nil {
					t.Fatalf("opening the ledger: %v", err)
				}
				_, lseq := testF105LoadLedger(t, e)
				expect = lseq
				auth2 := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
				auth, seq = auth2.rec, auth2.seq
			}

			_, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
				Attempt: &mqttTransitionAttempt{
					ID: e.id, Epoch: e.epoch, Txn: "txn-seal-noclose", Op: "op-seal-noclose",
					Kind: mqttTransitionDisposeResource, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
					Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
				},
				Prev: auth, PrevSeq: seq,
				Auth: &mqttOutboundAuthority{
					ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Tenure: auth.Tenure,
					State: auth.State, Rev: auth.Rev + 1, Txn: "txn-seal-noclose",
					OwnerTxn: auth.OwnerTxn,
				},
				Extra: []mqttTransitionMember{
					testF105LedgerMember(t, e, expect, &mqttGenLedger{
						ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
						State: mqttGenLedgerSealed, Hi: 0, SealedHi: 0,
					}),
				},
			})
			if err == nil {
				t.Fatal("a ledger was SEALED by a batch that writes no closure, leaving a " +
					"generation that can neither close nor be recovered")
			}
			// PIN THE REASON: another rule's refusal proves nothing about this one.
			if !strings.Contains(err.Error(), "writes no closure") {
				t.Fatalf("refused for the wrong reason, so this fixture does not reach the rule "+
					"under test: %v", err)
			}
		})
	}
}

// TestF105AnAffineResourceIsBornOnlyInAClosingBatch is holistic round 9's finding.
//
// The born-PENDING rule and the discarding-disposition rule are both keyed on the batch's
// CLOSURE, so a batch that writes no closure was never asked either question. That let a
// caller-built provision-resource batch create a LIVE, SEALED affine resource with an open
// ledger and a cleanup duty; a later NON-discarding close would then root that inventory, and
// the recovery pass would authorize and delete the consumer with no readiness and no discard
// licence behind it — defeating the structural exclusion of affine cleanup from preserving
// closures.
func TestF105AnAffineResourceIsBornOnlyInAClosingBatch(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-affine-noclosure")
	defer cleanup()
	auth, seq := e.bootstrap(t, "txn-boot")

	_, _, durable := mqttQ2AffineNames(getHash(e.id), testF105Gen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	for _, tc := range []struct {
		name string
		prov mqttProvisioningState
	}{
		{"sealed", mqttProvisioningSealed},
		{"pending", mqttProvisioningPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &mqttOutboundResource{
				ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: "inc-aff-" + tc.name,
				Kind: mqttResourceAffineConsumer, Name: durable, AffGen: testF105Gen,
				PubRelDur: pubRelDur, State: mqttResourceLive, Prov: tc.prov,
				Deps: []mqttResourceEdge{},
			}
			body, merr := json.Marshal(r)
			if merr != nil {
				t.Fatalf("marshalling the resource: %v", merr)
			}
			_, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
				Attempt: &mqttTransitionAttempt{
					ID: e.id, Epoch: e.epoch, Txn: "txn-aff-" + tc.name,
					Op: "op-aff-" + tc.name, Kind: mqttTransitionProvisionResource,
					PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
					Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
				},
				Prev: auth, PrevSeq: seq,
				Auth: &mqttOutboundAuthority{
					ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Tenure: auth.Tenure,
					State: auth.State, Rev: auth.Rev + 1, Txn: "txn-aff-" + tc.name,
					OwnerTxn: auth.OwnerTxn,
				},
				Extra: []mqttTransitionMember{
					{
						subj:   mqttResourceSubject(getHash(e.id), testF105Gen, r.Inc),
						expect: 0, body: body,
					},
					// The cleanup duty and an OPEN ledger ride along, because the affine
					// coverage rule refuses a resource with no duty naming it — without them
					// this batch is refused for a different reason and never reaches the rule
					// under test.
					testF105DutyMember(t, e, 0, &mqttDutyEntry{
						ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
						Kind: mqttDutyCleanup, Ref: r.Inc, State: mqttDutyPending,
					}),
					testF105LedgerMember(t, e, 0, &mqttGenLedger{
						ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
						State: mqttGenLedgerOpen, Hi: 1,
					}),
				},
			})
			if err == nil {
				t.Fatalf("an affine consumer resource was created by a batch that writes no "+
					"closure (provisioning %v), so the born-PENDING and discard-disposition "+
					"rules were both skipped", tc.prov)
			}
			// THE REFUSAL MUST BE THIS RULE'S, not another's. Without this the fixture passes
			// on the affine-coverage rule's refusal and says nothing about the closure rule.
			if !strings.Contains(err.Error(), "writes no closure") {
				t.Fatalf("refused for the wrong reason, so this fixture does not reach the rule "+
					"under test: %v", err)
			}
		})
	}
}

// TestF105AnAffineEnrolmentIsBoundToTheStandingRetirement is holistic round 11's finding.
//
// The discard disposition says the generation's obligations are abandoned. It does NOT say which
// consumer that licenses deleting — and everything the executor acts on is derived from the
// resource's own AffGen. So a finalization that keeps the closing generation, the PENDING state,
// the cleanup duty and the root, and substitutes another correctly DERIVED affine target, would
// send the pass to discard that generation's assignments and delete its consumer: possibly the
// SUCCESSOR'S. Trusting the acceptance's choice of target is not a licence to change it.
func TestF105AnAffineEnrolmentIsBoundToTheStandingRetirement(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-affine-binding")
	defer cleanup()

	const mine, theirs = "0123456789abcdefghijkq", "0123456789abcdefghijkz"
	_, _, mineDur := mqttQ2AffineNames(getHash(e.id), mine)
	_, _, theirsDur := mqttQ2AffineNames(getHash(e.id), theirs)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	retire := &mqttRetirementRequest{
		Gen: testF105Gen, Discard: true,
		AffineDurable: mineDur, AffineGen: mine, PubRelDurable: pubRelDur,
	}
	attempt := &mqttTransitionAttempt{
		ID: e.id, Epoch: e.epoch, Txn: "txn-fin", Op: "op-fin", Kind: mqttTransitionFinalize,
	}
	member := func(t *testing.T, r *mqttOutboundResource) mqttTransitionMember {
		t.Helper()
		body, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshalling the resource: %v", err)
		}
		return mqttTransitionMember{
			subj:   mqttResourceSubject(getHash(e.id), testF105Gen, r.Inc),
			expect: 0, body: body,
		}
	}
	base := func(affGen, name string) *mqttOutboundResource {
		return &mqttOutboundResource{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: "inc-aff",
			Kind: mqttResourceAffineConsumer, Name: name, AffGen: affGen,
			PubRelDur: pubRelDur, State: mqttResourceLive, Prov: mqttProvisioningPending,
			Deps: []mqttResourceEdge{},
		}
	}
	tr := func(members ...mqttTransitionMember) *mqttAuthorityTransition {
		return &mqttAuthorityTransition{
			Prev: &mqttOutboundAuthority{
				ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
				Intent: &mqttHandoffIntent{Gen: testF105Gen2, Retire: retire},
			},
			Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Discard: true},
			Extra:   members,
		}
	}

	// THE TARGET THE RETIREMENT NAMED is accepted, so the refusals below are about substitution
	// and not about the rule refusing everything.
	if err := mqttCheckClosingEnrolment(attempt, tr(member(t, base(mine, mineDur)))); err != nil {
		t.Fatalf("the enrolment naming the retirement's own target was refused: %v", err)
	}

	// A SUBSTITUTED TARGET, correctly derived for a DIFFERENT affine generation.
	sub := base(theirs, theirsDur)
	err := mqttCheckClosingEnrolment(attempt, tr(member(t, sub)))
	if err == nil {
		t.Fatal("an enrolment named an affine consumer no standing retirement selected, so the " +
			"pass would discard another generation's assignments and delete its consumer")
	}
	if !strings.Contains(err.Error(), "bound to the target") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// TWO AFFINE TARGETS: a generation holds one.
	second := base(mine, mineDur)
	second.Inc = "inc-aff-2"
	err = mqttCheckClosingEnrolment(attempt, tr(member(t, base(mine, mineDur)), member(t, second)))
	if err == nil {
		t.Fatal("an enrolment carried two affine consumers")
	}
	if !strings.Contains(err.Error(), "two affine consumers") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// AND NO STANDING RETIREMENT AT ALL is refused rather than treated as permission.
	noIntent := tr(member(t, base(mine, mineDur)))
	noIntent.Prev.Intent = nil
	if err := mqttCheckClosingEnrolment(attempt, noIntent); err == nil {
		t.Fatal("an affine consumer was enrolled with no standing retirement to have selected it")
	}

	// THE SAME BINDING ON THE SHARED KIND, which was an EMPTY case until this round's family
	// sweep. Its limit is stated rather than papered over: a close-admission closure enrols its
	// generation's durables with no handoff intent at all, and target-set completeness there is
	// the closing caller's audited assertion — so the binding applies where a retirement STANDS,
	// which is the finalization route a caller can forge a substitution on.
	retire.SharedDurables = []string{"$MQTT_shared_grp_named"}
	shared := func(name string) mqttTransitionMember {
		return member(t, &mqttOutboundResource{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: "inc-shared",
			Kind: mqttResourceSharedGroupDurable, Name: name,
			State: mqttResourceLive, Prov: mqttProvisioningSealed,
			Deps: []mqttResourceEdge{},
		})
	}
	if err := mqttCheckClosingEnrolment(attempt, tr(shared("$MQTT_shared_grp_named"))); err != nil {
		t.Fatalf("the shared durable the retirement names was refused: %v", err)
	}
	err = mqttCheckClosingEnrolment(attempt, tr(shared("$MQTT_shared_grp_other")))
	if err == nil {
		t.Fatal("a shared group durable the standing retirement does not name was enrolled, so " +
			"the transfer that lands later would authorize deleting a durable other sessions hold")
	}
	if !strings.Contains(err.Error(), "does not name") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	// WITH NO RETIREMENT the shared kind is ALLOWED, and that is the stated limit, not an
	// oversight: close-admission has no intent and its completeness is trusted by audit.
	noRet := tr(shared("$MQTT_shared_grp_other"))
	noRet.Prev.Intent = nil
	if err := mqttCheckClosingEnrolment(attempt, noRet); err != nil {
		t.Fatalf("a close-admission closure could not enrol its own shared durable: %v", err)
	}
}

// TestF105AMalformedAffineGenerationIsRefusedAtEveryRecord is holistic round 13's finding, and it
// is a consistency defect the readiness derivation CREATED.
//
// A derived name is derived from whatever string the field holds, so `AffGen: "x"` with a name
// derived from "x" satisfies every derivation rule. Nothing would delete the wrong consumer — the
// name matches the field — but the enrolment would be UNFINISHABLE: the readiness record this
// resource requires names the same affine generation, and its validator refuses a malformed one, so
// the resource stays PENDING and its cleanup duty never completes. An enrolment whose required
// evidence can never be written strands the generation exactly as an unsupported shape does, so it
// is refused at the RESOURCE, at the RETIREMENT REQUEST that selects the target, and on the
// READINESS record itself.
func TestF105AMalformedAffineGenerationIsRefusedAtEveryRecord(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-malformed-affgen")
	defer cleanup()

	const bad = "x"
	_, _, badDur := mqttQ2AffineNames(getHash(e.id), bad)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	// (1) THE RESOURCE. Its name is correctly DERIVED from the malformed generation, so only a
	// well-formedness rule refuses it.
	badRes := &mqttOutboundResource{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: "inc-aff",
		Kind: mqttResourceAffineConsumer, Name: badDur, AffGen: bad,
		PubRelDur: pubRelDur, State: mqttResourceLive, Prov: mqttProvisioningPending,
		Deps: []mqttResourceEdge{},
	}
	if err := mqttValidateOutboundResource(badRes, e.id, e.epoch, testF105Gen, "inc-aff"); err == nil {
		t.Fatal("an affine resource carrying a malformed affine generation was accepted, so its " +
			"cleanup duty could never be finished")
	} else if !strings.Contains(err.Error(), "malformed affine generation") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// (2) THE RETIREMENT REQUEST that selects the target, which is the earliest record carrying it.
	intent := &mqttHandoffIntent{
		ID: "intent-bad", PrevGen: testF105Gen, PrevTenure: "ten", PrevOwnerTxn: "own",
		PrevSessWID: "wid", PrevSessSeq: 1, Gen: testF105Gen2, Tenure: "ten2", CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: testF105Gen, Tenure: "ten", OwnerTxn: "own", SessWID: "wid", SessSeq: 1,
			Reason: mqttRetireCleanStart, Discard: true,
			AffineGen: bad, AffineDurable: badDur, PubRelDurable: pubRelDur,
		},
	}
	if err := mqttValidateHandoffIntentFor(getHash(e.id), intent); err == nil {
		t.Fatal("a retirement request named a malformed affine generation, so it could persist a " +
			"target no readiness assertion could ever name")
	} else if !strings.Contains(err.Error(), "malformed affine generation") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// (3a) A MISSING OR WRONG PUBREL CONSUMER — round 14's finding. The affine half can be
	// entirely valid while the proof source is empty or names another generation, and the resource
	// the finalization composes refuses exactly that; once the metadata half has committed the
	// descriptor is immutable, so the handoff would fail forever on a record already accepted.
	for _, tc := range []struct {
		name   string
		pubRel string
	}{
		{"missing", _EMPTY_},
		{"another generation's", func() string {
			_, _, d := mqttPubRelNames(getHash(e.id), testF105Gen2)
			return d
		}()},
	} {
		bad := *intent.Retire
		bad.AffineGen = testF105Gen2
		_, _, bad.AffineDurable = func() (string, string, string) {
			return mqttQ2AffineNames(getHash(e.id), testF105Gen2)
		}()
		bad.PubRelDurable = tc.pubRel
		badIntent := *intent
		badIntent.Retire = &bad
		if err := mqttValidateHandoffIntentFor(getHash(e.id), &badIntent); err == nil {
			t.Fatalf("a retirement named a %s PUBREL consumer, so finalization would refuse a "+
				"descriptor that can no longer be changed", tc.name)
		}
	}

	// (3b) AND A WRONG AFFINE DURABLE, derived for no generation at all.
	wrongDur := *intent.Retire
	wrongDur.AffineGen = testF105Gen2
	wrongDur.AffineDurable = "$MQTT_Q2AFF_not_derived"
	wrongIntent := *intent
	wrongIntent.Retire = &wrongDur
	if err := mqttValidateHandoffIntentFor(getHash(e.id), &wrongIntent); err == nil {
		t.Fatal("a retirement named an affine durable no generation of this client owns")
	}

	// (3) A HALF-NAMED TARGET: a generation with no durable, or the reverse, names nothing usable.
	half := *intent.Retire
	half.AffineDurable = _EMPTY_
	half.AffineGen = testF105Gen2
	halfIntent := *intent
	halfIntent.Retire = &half
	if err := mqttValidateHandoffIntentFor(getHash(e.id), &halfIntent); err == nil {
		t.Fatal("a retirement named an affine generation with no durable")
	}

	// (3c) AN UNNAMED SHARED DURABLE — holistic round 15's finding, the same class one field over.
	// mqttEnrolSharedDurables refuses an empty entry, so a list carrying one is accepted here and
	// refused at the finalization that must consume it, with the list immutable by then.
	emptyEntry := *intent.Retire
	emptyEntry.AffineGen, emptyEntry.AffineDurable, emptyEntry.PubRelDurable = _EMPTY_, _EMPTY_, _EMPTY_
	emptyEntry.SharedDurables = []string{"$MQTT_shared_grp_a", _EMPTY_}
	emptyIntent := *intent
	emptyIntent.Retire = &emptyEntry
	if err := mqttValidateHandoffIntentFor(getHash(e.id), &emptyIntent); err == nil {
		t.Fatal("a retirement named an unnamed shared durable, so finalization would refuse an " +
			"immutable list and the handoff could never complete")
	} else if !strings.Contains(err.Error(), "unnamed shared durable") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// AND THE SHAPES THAT MUST STAY LEGAL: a shared-only retirement, an empty list, and
	// duplicates (enrolment dedupes them).
	for _, tc := range []struct {
		name string
		list []string
	}{
		{"shared only", []string{"$MQTT_shared_grp_a"}},
		{"no list at all", nil},
		{"empty list", []string{}},
		{"duplicates", []string{"$MQTT_shared_grp_a", "$MQTT_shared_grp_a"}},
	} {
		okShared := *intent.Retire
		okShared.AffineGen, okShared.AffineDurable, okShared.PubRelDurable = _EMPTY_, _EMPTY_, _EMPTY_
		okShared.SharedDurables = tc.list
		okIntentShared := *intent
		okIntentShared.Retire = &okShared
		if err := mqttValidateHandoffIntentFor(getHash(e.id), &okIntentShared); err != nil {
			t.Fatalf("a legitimate %s retirement was refused: %v", tc.name, err)
		}
	}

	// (4) AND THE WELL-FORMED DESCRIPTOR IS ACCEPTED at both, so the rules above refuse a shape
	// rather than the feature.
	good := *intent.Retire
	good.AffineGen = testF105Gen2
	_, _, goodDur := mqttQ2AffineNames(getHash(e.id), testF105Gen2)
	good.AffineDurable = goodDur
	_, _, good.PubRelDurable = mqttPubRelNames(getHash(e.id), good.Gen)
	okIntent := *intent
	okIntent.Retire = &good
	if err := mqttValidateHandoffIntentFor(getHash(e.id), &okIntent); err != nil {
		t.Fatalf("a well-formed affine descriptor was refused: %v", err)
	}
	okRes := *badRes
	okRes.AffGen = testF105Gen2
	okRes.Name = goodDur
	if err := mqttValidateOutboundResource(&okRes, e.id, e.epoch, testF105Gen, "inc-aff"); err != nil {
		t.Fatalf("a well-formed affine resource was refused: %v", err)
	}
}
