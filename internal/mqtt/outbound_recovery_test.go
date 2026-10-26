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

import "testing"

// Recovering another claimant's abandoned proposal, end to end against the store.
//
// The decision tests establish WHICH continuation each settled verdict licenses. These
// establish that the continuations can actually RUN — the defect this closes twice over is an
// action returned by a decision function for which no transition kind exists, which is a
// route that was decided and not built.

// testF105ReserveCreation assembles a reservation over the given predecessor (nil for a
// virgin client, or a CLOSED authority).
func testF105ReserveCreation(e *testF105Env, prev *mqttOutboundAuthority, prevSeq uint64,
	txn, gen, tenure, intentID string) *mqttAuthorityTransition {

	rev := uint64(1)
	if prev != nil {
		rev = prev.Rev + 1
	}
	prevTxn := _EMPTY_
	if prev != nil {
		prevTxn = prev.OwnerTxn
	}
	c := &mqttCreationIntent{ID: intentID, Gen: gen, Tenure: tenure}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionReserveCreation, PrevTxn: prevTxn, Rev: rev,
			Gen: gen, Tenure: tenure, OwnerTxn: txn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: gen, Tenure: tenure,
			State: mqttAuthorityReserving, Rev: rev, Txn: txn, OwnerTxn: txn, Creation: c,
		},
	}
}

// testF105FinalizeCreation turns a standing reservation into a serving authority. txn is the
// FINALIZING transition's identity, which may belong to a broker other than the one that
// reserved — that is the whole point of the on-behalf path.
func testF105FinalizeCreation(e *testF105Env, prev *mqttOutboundAuthority, prevSeq uint64,
	txn string) *mqttAuthorityTransition {

	c := prev.Creation
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionFinalizeCreation, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: c.Gen, Tenure: c.Tenure, OwnerTxn: prev.OwnerTxn, Intent: c.ID,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: c.Gen, Tenure: c.Tenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
	}
}

// TestF105AbandonedForeignReservationIsRecoverable walks the whole RESERVING/ABORTED
// continuation against the store.
//
// Broker A claims a virgin client and dies before resolving its metadata half. Without a
// recovery the client is unservable forever: the reservation is create-only and is never
// deleted, so no later create-only write can succeed against it. Broker B settles the
// proposal, closes the reserved generation, and claims a fresh one BESIDE the closure —
// still as a reservation with its own metadata half, never as a serving authority, because
// installing one outright is the bootstrap gap reappearing on the second attempt.
func TestF105AbandonedForeignReservationIsRecoverable(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-recover-reservation")
	defer done()

	// (1) BROKER A reserves, and then stops.
	a := testF105ReserveCreation(e, nil, 0, "txn-a", testF105Gen, "tenure-a", "intent-a")
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, a)); err != nil {
		t.Fatalf("broker A's reservation was refused: %v", err)
	}

	// (2) BROKER B finds it. It is not B's, so B settles rather than adopts.
	load := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if load.status != mqttOutboundValid {
		t.Fatalf("the reservation does not load back: status %v", load.status)
	}
	d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{Load: load, IntentID: "intent-b"})
	if d.Action != mqttOwnershipRecoverForeign || d.Recovery == nil {
		t.Fatalf("broker B got action %d (%q), want RECOVER FOREIGN with a target", d.Action, d.Reason)
	}
	if d.Recovery.Proposal.ID != "intent-a" || d.Recovery.AuthSeq != load.seq {
		t.Fatalf("broker B's recovery target is %+v, want A's proposal at sequence %d",
			d.Recovery, load.seq)
	}

	v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, d.Recovery.Proposal)
	if v != mqttMetadataAborted {
		t.Fatalf("settling A's stalled proposal returned %v, want ABORTED", v)
	}
	next, why := mqttForeignRecoveryContinuation(d.Recovery, v)
	if next != mqttOwnershipCloseAbandoned {
		t.Fatalf("an aborted foreign reservation licensed action %d (%q), want CLOSE ABANDONED",
			next, why)
	}

	// (3) CLOSE the reserved generation. The reservation itself is never deleted.
	closing := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-b-close", Op: "op-b-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: load.rec.OwnerTxn, Rev: load.rec.Rev + 1,
			Gen: load.rec.Gen, Tenure: load.rec.Tenure, OwnerTxn: load.rec.OwnerTxn,
		},
		Prev: load.rec, PrevSeq: load.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: load.rec.Gen, Tenure: load.rec.Tenure,
			State: mqttAuthorityClosed, Rev: load.rec.Rev + 1, Txn: "txn-b-close",
			OwnerTxn: load.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: load.rec.Gen, Txn: "txn-b-close"},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, closing)); err != nil {
		t.Fatalf("closing the abandoned reservation was refused: %v", err)
	}
	if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttOutboundAuthoritySubject(getHash(e.id))); err != nil {
		t.Fatalf("the authority record disappeared: %v", err)
	}

	// (4) OVER THE CLOSURE, a FRESH reservation — and the decision says so.
	load = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if load.status != mqttOutboundValid || load.rec.State != mqttAuthorityClosed {
		t.Fatalf("the closed authority does not load back (status %v)", load.status)
	}
	d = mqttDecideConnectOwnership(mqttConnectOwnershipInput{Load: load, IntentID: "intent-b"})
	if d.Action != mqttOwnershipReserveOverClosed {
		t.Fatalf("a closed authority licensed action %d (%q), want RESERVE OVER CLOSED",
			d.Action, d.Reason)
	}

	// REUSING THE CLOSED GENERATION IS NOT A FRESH PROPOSAL, and is refused. Attempted
	// FIRST, at the sequence just read: run after a successful claim it would be refused by
	// the stale CAS instead, and a fixture refused by the sequence never reaches the rule.
	reuse := testF105ReserveCreation(e, load.rec, load.seq, "txn-b-reuse", testF105Gen,
		"tenure-b2", "intent-b2")
	if _, rerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, reuse)); rerr == nil {
		t.Fatalf("a reservation reused the closed generation")
	}
	if back := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); back.seq != load.seq {
		t.Fatalf("the refused reuse moved the authority record")
	}

	// THIS IS THE ROUTE THAT DID NOT EXIST. reserve-creation refused any predecessor, so
	// the action was decided and unrunnable.
	res := testF105ReserveCreation(e, load.rec, load.seq, "txn-b", testF105Gen2, "tenure-b", "intent-b")
	resSeq, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, res))
	if err != nil {
		t.Fatalf("reserving over the closure was refused: %v", err)
	}

	// (5) B's own metadata half commits, and B finalizes ITS OWN reservation.
	p, perr := mqttProposalFromCreation(res.Auth.Creation)
	if perr != nil {
		t.Fatalf("viewing B's reservation as a proposal failed: %v", perr)
	}
	f := testF105FreezeProposal(t, e, p, testF105RecordSeq(t, e))
	if _, cerr := e.asm.jsa.commitMetadataAttempt(f); cerr != nil {
		t.Fatalf("B's metadata commit was refused: %v", cerr)
	}
	load = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	d = mqttDecideConnectOwnership(mqttConnectOwnershipInput{
		Load: load, IntentID: "intent-b",
		Metadata: e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, p),
	})
	if d.Action != mqttOwnershipFinalizeCreation {
		t.Fatalf("B's committed reservation licensed action %d (%q), want FINALIZE CREATION",
			d.Action, d.Reason)
	}
	if _, ferr := e.asm.jsa.commitAuthorityTransition(
		testF105FinalizeCreation(e, load.rec, load.seq, "txn-b-fin")); ferr != nil {
		t.Fatalf("B's finalization was refused: %v", ferr)
	}
	end := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if end.status != mqttOutboundValid || end.rec.State != mqttAuthorityServing ||
		end.rec.Gen != testF105Gen2 || end.rec.Tenure != "tenure-b" {
		t.Fatalf("after the recovery the client is not served by B: %+v", end.rec)
	}
	_ = resSeq
}

// TestF105OnBehalfFinalizationGrantsTheHelperNothing is Codex's Q4, end to end.
//
// Broker A's reservation COMMITTED its metadata half and A then stopped. The proposal won, so
// settling it must not abort it — and B, finding a committed proposal it did not make,
// finalizes it AS PROPOSED. What B must not get out of that is the tenure: A's generation and
// A's tenure are installed, and B then takes an ordinary handoff like any other acceptance.
// Nothing about having pushed a stalled proposal over the line entitles B to serve under it.
func TestF105OnBehalfFinalizationGrantsTheHelperNothing(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-onbehalf")
	defer done()

	a := testF105ReserveCreation(e, nil, 0, "txn-a", testF105Gen, "tenure-a", "intent-a")
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, a)); err != nil {
		t.Fatalf("broker A's reservation was refused: %v", err)
	}
	pa, perr := mqttProposalFromCreation(a.Auth.Creation)
	if perr != nil {
		t.Fatalf("viewing A's reservation as a proposal failed: %v", perr)
	}
	fa := testF105FreezeProposal(t, e, pa, testF105RecordSeq(t, e))
	if _, cerr := e.asm.jsa.commitMetadataAttempt(fa); cerr != nil {
		t.Fatalf("A's metadata commit was refused: %v", cerr)
	}
	// ...and A stops here, without finalizing.

	load := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	d := mqttDecideConnectOwnership(mqttConnectOwnershipInput{Load: load, IntentID: "intent-b"})
	if d.Action != mqttOwnershipRecoverForeign || d.Recovery == nil {
		t.Fatalf("broker B got action %d (%q), want RECOVER FOREIGN", d.Action, d.Reason)
	}
	// SETTLING A WON PROPOSAL REPORTS THE WIN. It does not abort it: the slot is taken, and
	// resolving before aborting is what makes that so.
	v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, d.Recovery.Proposal)
	if v != mqttMetadataCommitted {
		t.Fatalf("settling A's committed proposal returned %v, want COMMITTED", v)
	}
	next, why := mqttForeignRecoveryContinuation(d.Recovery, v)
	if next != mqttOwnershipFinalizeCreation {
		t.Fatalf("a committed foreign reservation licensed action %d (%q), want FINALIZE CREATION",
			next, why)
	}

	// B finalizes under ITS OWN transition identity. The ownership token stays A's, because
	// the transition that CLAIMED the client is the one that owns it.
	fin := testF105FinalizeCreation(e, load.rec, load.seq, "txn-b-onbehalf")
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, fin)); err != nil {
		t.Fatalf("B's on-behalf finalization was refused: %v", err)
	}

	end := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if end.status != mqttOutboundValid || end.rec.State != mqttAuthorityServing {
		t.Fatalf("the on-behalf finalization did not install a serving authority: %+v", end.rec)
	}
	// EXACTLY WHAT A PROPOSED, and nothing of B's.
	if end.rec.Gen != testF105Gen || end.rec.Tenure != "tenure-a" {
		t.Fatalf("the on-behalf finalization installed %q/%q, want A's proposal %q/%q",
			end.rec.Gen, end.rec.Tenure, testF105Gen, "tenure-a")
	}
	if end.rec.OwnerTxn != "txn-a" {
		t.Fatalf("the on-behalf finalization moved ownership to %q; the transition that "+
			"claimed the client is the one that owns it", end.rec.OwnerTxn)
	}

	// AND B IS NOT SERVING. Its next step is an ordinary handoff, exactly like an acceptance
	// that had done no recovery at all.
	d = mqttDecideConnectOwnership(mqttConnectOwnershipInput{
		Load: end, IntentID: "intent-b", Recovering: d.Recovery,
	})
	if d.Action != mqttOwnershipPrepareHandoff {
		t.Fatalf("after finalizing on A's behalf, B got action %d (%q), want PREPARE HANDOFF",
			d.Action, d.Reason)
	}
}

// TestF105OnlyAnAbsentOrClosedAuthorityIsClaimable pins the predecessor rule the widened
// claim rests on.
//
// reserve-creation gained a CLOSED predecessor so the post-closure claim would have a route.
// What it must NOT have gained is the ability to claim over a LIVE authority: a serving
// tenure would be displaced with no handoff and no metadata half, and a preparing or
// reserving one would have its unresolved proposal silently abandoned — the authority would
// still be in that state with nothing left to finalize.
func TestF105OnlyAnAbsentOrClosedAuthorityIsClaimable(t *testing.T) {
	for name, build := range map[string]func(e *testF105Env) *mqttAuthorityTransition{
		"a serving tenure": func(e *testF105Env) *mqttAuthorityTransition {
			return e.firstReservation("txn-live")
		},
		"a standing reservation": func(e *testF105Env) *mqttAuthorityTransition {
			return testF105ReserveCreation(e, nil, 0, "txn-res", testF105Gen, "tenure-res", "intent-res")
		},
	} {
		func() {
			e, done := testF105NewEnv(t, "f105-claimable")
			defer done()

			if _, err := e.asm.jsa.commitAuthorityTransition(build(e)); err != nil {
				t.Fatalf("establishing %s failed: %v", name, err)
			}
			load := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
			if load.status != mqttOutboundValid {
				t.Fatalf("%s does not load back: status %v", name, load.status)
			}
			claim := testF105ReserveCreation(e, load.rec, load.seq, "txn-claim", testF105Gen2,
				"tenure-claim", "intent-claim")
			if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, claim)); err == nil {
				t.Fatalf("a reservation was claimed over %s", name)
			}
			after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
			if after.seq != load.seq || after.rec.State != load.rec.State {
				t.Fatalf("the refused claim moved %s", name)
			}
		}()
	}

	// A PREPARING authority is the third live state, and reaching it takes a preparation.
	e, done := testF105NewEnv(t, "f105-claimable-prep")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	intent := &mqttHandoffIntent{
		ID: "intent-1", PrevGen: auth.Gen, PrevTenure: auth.Tenure,
		PrevOwnerTxn: auth.OwnerTxn, PrevSessWID: "wid-0", PrevSessSeq: 12,
		Tenure: "tenure-next", Gen: testF105Gen2, CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
			SessWID: "wid-0", SessSeq: 12, Reason: mqttRetireCleanStart, Discard: true,
		},
	}
	prep := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-prep", Op: "op-prep",
			Kind: mqttTransitionPrepareIntent, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: mqttAuthorityPreparing, Rev: auth.Rev + 1, Txn: "txn-prep",
			OwnerTxn: auth.OwnerTxn, Intent: intent,
		},
	}
	if _, perr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, prep)); perr != nil {
		t.Fatalf("the preparation was refused: %v", perr)
	}
	load := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if load.status != mqttOutboundValid || load.rec.State != mqttAuthorityPreparing {
		t.Fatalf("the preparing authority does not load back (status %v)", load.status)
	}
	claim := testF105ReserveCreation(e, load.rec, load.seq, "txn-claim", testF105Gen2,
		"tenure-claim", "intent-claim")
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, claim)); cerr == nil {
		t.Fatalf("a reservation was claimed over a pending handoff, abandoning it unresolved")
	}
	if after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); after.rec.Intent == nil ||
		after.rec.Intent.ID != intent.ID {
		t.Fatalf("the refused claim disturbed the pending handoff")
	}
}
