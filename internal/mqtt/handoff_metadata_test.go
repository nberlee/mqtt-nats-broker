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
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testF105StandingSession reads the session record the environment's own CONNECT wrote, with
// its sequence.
//
// A HANDOFF NAMES THE PREDECESSOR IT ACTUALLY DISPLACES. Hard-coding a write identity and a
// sequence made the fixture describe a session that was never there, which the write boundary
// now refuses — correctly, since a receipt claiming to have displaced a record it did not
// touch is the defect, not the test's convenience.
func testF105StandingSession(t *testing.T, e *testF105Env) (*mqttPersistedSession, uint64) {
	t.Helper()
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(_EMPTY_, getHash(e.id)))
	if err != nil {
		t.Fatalf("the environment has no session record: %v", err)
	}
	ps := &mqttPersistedSession{}
	if derr := json.Unmarshal(smsg.Data, ps); derr != nil {
		t.Fatalf("decoding the standing session record: %v", derr)
	}
	return ps, smsg.Sequence
}

// testF105Intent builds a clean-start handoff intent naming the environment's STANDING
// session record as its predecessor.
func testF105Intent(t *testing.T, e *testF105Env, idSuffix string) *mqttHandoffIntent {
	t.Helper()
	// THE PREDECESSOR IS A REAL ONE. A handoff's metadata half may only commit for a
	// proposal the client's own authority carries, so these fixtures need an ownership to
	// hand off FROM — and its generation, tenure and ownership are what the intent names,
	// rather than invented tokens no authority ever stood at.
	prev := testF105PredecessorOwnership(t, e)
	ps, seq := testF105StandingSession(t, e)
	// A CLEAN START REPLACES THE GENERATION WITH A FRESH ONE, minted per intent. Alternating
	// between two literals was enough while nothing remembered the old ones; it is not now,
	// because a generation that has ever been closed is closed permanently and may never be
	// claimed again — so the second clean start in a test would propose the generation the
	// first one closed.
	next := mqttNewSessGen()
	return &mqttHandoffIntent{
		ID: "intent-" + idSuffix, PrevGen: prev.Gen, PrevTenure: prev.Tenure,
		PrevOwnerTxn: prev.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Tenure: "tenure-1", Gen: next, CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
			SessWID: ps.WID, SessSeq: seq, Reason: mqttRetireCleanStart, Discard: true,
		},
	}
}

// testF105ResumeHandoff is testF105Intent's RESUME counterpart, carried to a serving authority
// through the live ops an accepting CONNECT's executor composes: prepare, commit the metadata
// half, finalize. A RESUME PRESERVES THE GENERATION and installs a fresh tenure, so the handoff
// proposes the standing serving authority's own generation and requests no retirement — the
// predecessor's obligations stay the successor's to complete. The metadata half names record
// at recordSeq as what it displaces and installs a fresh write identity, as an accepted CONNECT
// does. The serving generation and tenure are READ BACK from the authority, never assumed from
// what was proposed.
func testF105ResumeHandoff(t *testing.T, jsa *mqttJSA, domainTk, id, epoch string,
	record *mqttPersistedSession, recordSeq uint64) (gen, tenure string) {
	t.Helper()
	serving, _, err := jsa.servingAuthority(id, epoch)
	if err != nil {
		t.Fatalf("a resume displaces a SERVING authority, and none stands: %v", err)
	}
	intent := &mqttHandoffIntent{
		ID: mqttNewInflightNonce(), Gen: serving.Gen, Tenure: mqttNewInflightNonce(),
		PrevSessWID: record.WID, PrevSessSeq: recordSeq,
	}
	if _, err := jsa.opPrepareHandoff(id, epoch, mqttNewInflightNonce(), intent); err != nil {
		t.Fatalf("preparing the resume handoff failed: %v", err)
	}
	// THE PROPOSAL IS THE STANDING PREPARATION'S, which carries the predecessor ownership the
	// prepare stamped from the serving authority; the local intent does not.
	al := jsa.loadOutboundAuthority(id, epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityPreparing ||
		al.rec.Intent == nil || al.rec.Intent.ID != intent.ID {
		t.Fatalf("the resume handoff's preparation does not stand: %v %+v", al.status, al.rec)
	}
	p, err := mqttProposalFromHandoff(al.rec.Intent)
	if err != nil {
		t.Fatalf("the standing preparation is uninterpretable: %v", err)
	}
	wid := mqttNewInflightNonce()
	rec := *record
	rec.ID, rec.Gen, rec.WID = id, p.Gen, wid
	recBody, err := json.Marshal(&rec)
	if err != nil {
		t.Fatalf("marshalling the session record: %v", err)
	}
	fact := &mqttAcceptFact{ClientID: id, SessEpoch: epoch, AcceptID: mqttNewAcceptID(), At: time.Now().UnixNano()}
	factBody, err := json.Marshal(fact)
	if err != nil {
		t.Fatalf("marshalling the acceptance fact: %v", err)
	}
	f, err := mqttFreezeMetadataAttempt(id, epoch, domainTk, p, wid,
		mqttAcceptSubject(domainTk, getHash(id), epoch, fact.AcceptID), factBody, recordSeq, recBody)
	if err != nil {
		t.Fatalf("freezing the resume's metadata half: %v", err)
	}
	if _, err := jsa.commitMetadataAttempt(f); err != nil {
		t.Fatalf("committing the resume's metadata half failed: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(id, epoch, mqttNewInflightNonce(), intent.ID); err != nil {
		t.Fatalf("finalizing the resume handoff failed: %v", err)
	}
	after, _, err := jsa.servingAuthority(id, epoch)
	if err != nil || after.Gen != intent.Gen || after.Tenure != intent.Tenure {
		t.Fatalf("the resume handoff did not leave the client serving at its own generation and "+
			"tenure: err=%v authority=%+v", err, after)
	}
	return after.Gen, after.Tenure
}

// testF105PredecessorOwnership is the ownership a handoff built now would displace: the
// standing authority's, or — while a preparation stands — the one a cancellation would
// restore, since these fixtures cancel before they prepare again.
//
// It CLAIMS the client if nothing stands, through the ordinary three steps, because there is
// no other way to reach a serving authority and a fixture that installed one directly would
// be asserting a route the broker does not have.
func testF105PredecessorOwnership(t *testing.T, e *testF105Env) *mqttOutboundAuthority {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	switch {
	case al.status == mqttOutboundAbsent:
		auth, _ := e.claim(t, nil, 0, "txn-own", testF105Gen, "tenure-own")
		return auth
	case al.status != mqttOutboundValid:
		t.Fatalf("the authority reads %v, so no predecessor can be named", al.status)
	}
	if al.rec.State == mqttAuthorityPreparing && al.rec.Intent != nil {
		// A STANDING PREPARATION IS RESOLVED BEFORE ANOTHER IS BUILT, so the predecessor an
		// intent names is the one that will still be standing when it is prepared. Resolved
		// by its own permanent verdict: a COMMITTED proposal is finalized, because
		// cancelling it would abandon a handoff the metadata half already granted.
		testF105ResolveStanding(t, e, al.rec.Intent)
		al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		if al.status != mqttOutboundValid {
			t.Fatalf("the authority reads %v after resolving its preparation", al.status)
		}
	}
	return al.rec
}

// testF105ResolveStanding settles a standing preparation and either finalizes or cancels it,
// which is the route a broker itself takes: a preparation is never overwritten.
func testF105ResolveStanding(t *testing.T, e *testF105Env, standing *mqttHandoffIntent) {
	t.Helper()
	p, perr := mqttProposalFromHandoff(standing)
	if perr != nil {
		t.Fatalf("the standing preparation is uninterpretable: %v", perr)
	}
	switch v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, p); v {
	case mqttMetadataCommitted:
		if _, err := e.asm.jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin-"+standing.ID, standing.ID); err != nil {
			t.Fatalf("finalizing the standing preparation failed: %v", err)
		}
	case mqttMetadataAborted:
		if _, err := e.asm.jsa.opCancelHandoff(e.id, e.epoch, "txn-cancel-"+standing.ID, standing.ID); err != nil {
			t.Fatalf("cancelling the standing preparation failed: %v", err)
		}
	default:
		t.Fatalf("settling the standing preparation returned %v", v)
	}
}

// testF105StandCreation makes a creation proposal the client's standing reservation, which is
// the provenance a metadata commit for it requires.
func testF105StandCreation(t *testing.T, e *testF105Env, p mqttOwnershipProposal) {
	t.Helper()
	// A RESERVATION ALREADY STANDING IS ABANDONED FIRST, the way a broker abandons one: its
	// proposal is settled and its generation is CLOSED. It is never deleted, because the
	// next claimant's create-only write must fail against it rather than succeed against an
	// absence.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status == mqttOutboundValid && al.rec.State == mqttAuthorityReserving &&
		al.rec.Creation != nil && al.rec.Creation.ID != p.ID {
		standing, perr := mqttProposalFromCreation(al.rec.Creation)
		if perr != nil {
			t.Fatalf("the standing reservation is uninterpretable: %v", perr)
		}
		if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, standing); v != mqttMetadataAborted {
			t.Fatalf("settling the standing reservation returned %v, want ABORTED", v)
		}
		if _, err := e.asm.jsa.opCloseGeneration(e.id, e.epoch, "txn-abandon-"+standing.ID, al.rec.Gen, al.rec.Tenure, al.rec.OwnerTxn); err != nil {
			t.Fatalf("abandoning the standing reservation failed: %v", err)
		}
	}
	if _, err := e.asm.jsa.opClaimClient(e.id, e.epoch, "txn-claim-"+p.ID, p.Gen, p.Tenure, p.ID); err != nil {
		t.Fatalf("standing the creation proposal %q failed: %v", p.ID, err)
	}
}

// testF105Prepare makes i the client's STANDING proposal.
//
// A metadata half may only commit for a proposal the authority carries: the claim is written
// first and serves nothing, and the metadata commit is what makes it servable. Where a
// preparation already stands it is settled and cancelled first, which is the route a broker
// itself takes — a preparation is never simply overwritten.
func testF105Prepare(t *testing.T, e *testF105Env, i *mqttHandoffIntent) {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status == mqttOutboundValid && al.rec.State == mqttAuthorityPreparing && al.rec.Intent != nil &&
		al.rec.Intent.ID != i.ID {
		testF105ResolveStanding(t, e, al.rec.Intent)
	}
	if _, err := e.asm.jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-"+i.ID, i); err != nil {
		t.Fatalf("standing the handoff proposal %q failed: %v", i.ID, err)
	}
}

// testF105RecordSeq is the session record's standing sequence, which every metadata attempt
// must CAS against. The environment's own CONNECT already wrote a record, so an attempt
// frozen at base zero is a create against an occupied subject — a fixture error, not a
// mechanism one.
// testF105Prop views a handoff intent as the proposal the metadata half commits. The two
// shapes share ONE protocol, so every call goes through the constructor rather than filling
// the view in by hand — the kind is the constructor's to set.
func testF105Prop(t *testing.T, i *mqttHandoffIntent) mqttOwnershipProposal {
	t.Helper()
	p, err := mqttProposalFromHandoff(i)
	if err != nil {
		t.Fatalf("the fixture handoff intent was refused: %v", err)
	}
	return p
}

func testF105RecordSeq(t *testing.T, e *testF105Env) uint64 {
	t.Helper()
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(_EMPTY_, getHash(e.id)))
	if err != nil {
		return 0
	}
	return smsg.Sequence
}

func testF105FreezeAttempt(t *testing.T, e *testF105Env, i *mqttHandoffIntent, baseSeq uint64) *mqttFrozenMetadataAttempt {
	t.Helper()
	fact := &mqttAcceptFact{
		ClientID: e.id, SessEpoch: e.epoch, AcceptID: mqttNewAcceptID(),
		At: time.Now().UnixNano(),
	}
	fb, err := json.Marshal(fact)
	if err != nil {
		t.Fatalf("marshalling the acceptance fact: %v", err)
	}
	factSubj := mqttAcceptSubject(_EMPTY_, getHash(e.id), e.epoch, fact.AcceptID)
	// A FRESH WRITE IDENTITY PER ATTEMPT. Installing the predecessor's own identity is
	// refused, and reusing a literal made the fixture do exactly that once an earlier
	// handoff in the same environment had committed.
	wid := nuid.Next()
	rec, _ := json.Marshal(&mqttPersistedSession{ID: e.id, Gen: i.Gen, WID: wid, Origin: "o"})
	f, ferr := mqttFreezeMetadataAttempt(e.id, e.epoch, _EMPTY_, testF105Prop(t, i), wid, factSubj, fb, baseSeq, rec)
	if ferr != nil {
		t.Fatalf("freezing the attempt: %v", ferr)
	}
	return f
}

// TestF105MetadataCommitCarriesAllThreeMembers pins that the accepted CONNECT's batch gains
// the handoff receipt without losing either property the two-member batch established.
//
// Indivisibility means extending the transaction, not keeping its member count at two: the
// fact and the record still commit together, the fact's sequence is still below the record's,
// and the acknowledgement still names the record whose sequence the session adopts.
func TestF105MetadataCommitCarriesAllThreeMembers(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-meta")
	defer done()

	i := testF105Intent(t, e, "1")
	testF105Prepare(t, e, i)
	f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))

	recSeq, err := e.asm.jsa.commitMetadataAttempt(f)
	if err != nil {
		t.Fatalf("the metadata commit was refused: %v", err)
	}
	if recSeq == 0 {
		t.Fatalf("the metadata commit returned no session-record sequence")
	}

	// The receipt stands, and answers this intent.
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataCommitted {
		t.Fatalf("the committed metadata half resolved %v, want COMMITTED", v)
	}

	// The acceptance fact landed BELOW the record: a delayed Will reading the fact must
	// never see a record older than the acceptance that permitted it.
	fmsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, f.FactSubj)
	if err != nil {
		t.Fatalf("the acceptance fact does not load back: %v", err)
	}
	if fmsg.Sequence >= recSeq {
		t.Fatalf("the acceptance fact landed at %d, at or above the record's %d", fmsg.Sequence, recSeq)
	}

	// And the acknowledgement named the RECORD, which is the sequence a session adopts.
	rmsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(_EMPTY_, getHash(e.id)))
	if err != nil {
		t.Fatalf("the session record does not load back: %v", err)
	}
	if rmsg.Sequence != recSeq {
		t.Fatalf("the commit returned %d but the record stands at %d", recSeq, rmsg.Sequence)
	}
}

// TestF105AbortFencesADelayedMetadataCommit is the delayed-write fence.
//
// A resolver that gave up on an attempt takes its permanent slot. The frozen attempt's own
// create-only member must then be unable to land — which is what stops a batch that was
// still in flight from committing a handoff somebody already settled, and it must leave the
// SESSION RECORD untouched, since the whole batch is refused.
func TestF105AbortFencesADelayedMetadataCommit(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-meta-fence")
	defer done()

	i := testF105Intent(t, e, "2")
	testF105Prepare(t, e, i)
	f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))

	before, berr := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(_EMPTY_, getHash(e.id)))
	var beforeSeq uint64
	if berr == nil {
		beforeSeq = before.Sequence
	}

	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); err != nil {
		t.Fatalf("the abort was refused: %v", err)
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataAborted {
		t.Fatalf("the aborted metadata half resolved %v, want ABORTED", v)
	}

	if _, err := e.asm.jsa.commitMetadataAttempt(f); err == nil {
		t.Fatalf("a metadata commit landed after its intent was positively aborted")
	}
	after, aerr := e.asm.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(_EMPTY_, getHash(e.id)))
	var afterSeq uint64
	if aerr == nil {
		afterSeq = after.Sequence
	}
	if afterSeq != beforeSeq {
		t.Fatalf("the refused batch still moved the session record from %d to %d: the losing "+
			"member must reject the WHOLE batch", beforeSeq, afterSeq)
	}

	// And a commit-first attempt makes the abort fail, so exactly one verdict can stand.
	j := testF105Intent(t, e, "3")
	testF105Prepare(t, e, j)
	g := testF105FreezeAttempt(t, e, j, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(g); err != nil {
		t.Fatalf("the second metadata commit was refused: %v", err)
	}
	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, j)); err == nil {
		t.Fatalf("an intent that had already committed was aborted afterwards")
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, j)); v != mqttMetadataCommitted {
		t.Fatalf("the committed intent resolved %v after a failed abort, want COMMITTED", v)
	}
}

// TestF105RetryResendsTheFrozenAttempt pins the correction to how the existing acceptance
// loop retries.
//
// Recomposing is right when a retry means "write this again" and WRONG once the batch
// carries a create-only member: a recomposed attempt mints a new intent, and the permanent
// slot of the old one is left for nobody to resolve. A retry therefore resends the same
// frozen attempt — and because the transport batch id is minted inside compose, resending is
// safe rather than an abandoned staged batch.
func TestF105RetryResendsTheFrozenAttempt(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-meta-retry")
	defer done()

	i := testF105Intent(t, e, "4")
	testF105Prepare(t, e, i)
	f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))

	// Two composes of ONE attempt differ only in their transport batch id.
	m1, b1, err := f.compose()
	if err != nil {
		t.Fatalf("composing: %v", err)
	}
	m2, b2, err := f.compose()
	if err != nil {
		t.Fatalf("recomposing: %v", err)
	}
	if b1 == b2 {
		t.Fatalf("two attempts shared a transport batch id: reusing one while a batch is " +
			"staged abandons it")
	}
	if len(m1) != len(m2) || len(m1) != 3 {
		t.Fatalf("the frozen attempt composed %d and %d members, want 3 each", len(m1), len(m2))
	}
	for k := range m1 {
		if m1[k].subj != m2[k].subj {
			t.Fatalf("member %d moved subject between attempts: %q then %q", k, m1[k].subj, m2[k].subj)
		}
	}
	// The semantic identity is unchanged: same intent, same receipt, same write identity.
	// The identity is read back from the attempt rather than compared with a literal — what
	// is being pinned is that it does not MOVE between composes, not what it happens to be.
	wid := f.Outcome.SessWID
	if wid == _EMPTY_ {
		t.Fatalf("the frozen receipt installs no write identity")
	}
	if _, _, rerr := f.compose(); rerr != nil {
		t.Fatalf("recomposing again: %v", rerr)
	}
	if f.Outcome.Intent != i.ID || f.Outcome.SessWID != wid {
		t.Fatalf("the frozen receipt changed between attempts")
	}

	// The real retry: the first send lands, and a second send of the SAME attempt is
	// refused by its own create-only members rather than duplicating anything.
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err != nil {
		t.Fatalf("the metadata commit was refused: %v", err)
	}
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err == nil {
		t.Fatalf("resending a landed attempt committed a second time")
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataCommitted {
		t.Fatalf("after a refused resend the intent resolved %v, want COMMITTED", v)
	}
}

// TestF105MetadataReceiptMustAnswerItsIntent pins that a receipt sharing a slot is not
// thereby an answer: one conditioned on a different predecessor describes a different
// handoff, and finalizing on it would displace ownership nobody checked.
func TestF105MetadataReceiptMustAnswerItsIntent(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-meta-foreign")
	defer done()

	i := testF105Intent(t, e, "5")
	testF105Prepare(t, e, i)
	f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err != nil {
		t.Fatalf("the metadata commit was refused: %v", err)
	}

	// The same intent id, a different predecessor.
	other := testF105Intent(t, e, "5")
	other.PrevSessWID = "wid-elsewhere"
	other.Retire.SessWID = "wid-elsewhere"
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, other)); v != mqttMetadataForeign {
		t.Fatalf("a receipt conditioned on another predecessor resolved %v, want FOREIGN", v)
	}

	// An intent nobody wrote for is unresolved, never aborted: the batch may still be in
	// flight.
	absent := testF105Intent(t, e, "never")
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, absent)); v != mqttMetadataUnresolved {
		t.Fatalf("an unwritten intent resolved %v, want UNRESOLVED", v)
	}
}

// TestF105CommitFirstLostAcknowledgementIsNotARetry pins the case a caller is most likely to
// get wrong: the batch LANDED and the acknowledgement did not arrive.
//
// A caller that treats that as "retry" would resend, be refused by its own create-only
// members, and — if it read that refusal as failure — abort a handoff that committed. The
// refusal must lead to a matching committed readback, never to a new acceptance.
func TestF105CommitFirstLostAcknowledgementIsNotARetry(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-lostack")
	defer done()

	i := testF105Intent(t, e, "lost")
	testF105Prepare(t, e, i)
	f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err != nil {
		t.Fatalf("the metadata commit was refused: %v", err)
	}

	// The caller never saw the acknowledgement. Its only honest move is to resolve, and
	// resolution says COMMITTED — so the resend below is what a WRONG caller would do.
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataCommitted {
		t.Fatalf("a landed attempt resolved %v, want COMMITTED", v)
	}
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err == nil {
		t.Fatalf("a resend of a landed attempt committed again")
	}
	// And after that refusal the verdict is unchanged: the refusal is evidence the work is
	// done, not evidence it failed.
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataCommitted {
		t.Fatalf("after a refused resend the intent resolved %v, want COMMITTED", v)
	}
	// Aborting now must fail: a committed handoff cannot be retracted by a caller that
	// merely lost its acknowledgement.
	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); err == nil {
		t.Fatalf("a committed handoff was aborted by a caller that lost its acknowledgement")
	}
}

// TestF105OutcomeSlotContendsWithoutDisplacingTheRecord pins that losing the outcome slot
// costs the session record NOTHING.
//
// The two live on one stream and in one batch, so a naive implementation could let a
// contested receipt take the record with it — which would turn another resolver's abort into
// a rollback of an unrelated session write.
func TestF105OutcomeSlotContendsWithoutDisplacingTheRecord(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-slotcontend")
	defer done()

	// Land a first handoff so there is a record whose sequence we can watch.
	first := testF105Intent(t, e, "c1")
	testF105Prepare(t, e, first)
	f1 := testF105FreezeAttempt(t, e, first, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(f1); err != nil {
		t.Fatalf("the first metadata commit was refused: %v", err)
	}
	recSeq := testF105RecordSeq(t, e)

	// A second handoff whose slot another resolver takes first.
	second := testF105Intent(t, e, "c2")
	f2 := testF105FreezeAttempt(t, e, second, recSeq)
	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, second)); err != nil {
		t.Fatalf("the competing abort was refused: %v", err)
	}
	if _, err := e.asm.jsa.commitMetadataAttempt(f2); err == nil {
		t.Fatalf("a metadata commit won a slot another resolver had aborted")
	}

	if got := testF105RecordSeq(t, e); got != recSeq {
		t.Fatalf("losing the outcome slot moved the session record from %d to %d: another "+
			"resolver's abort must not roll back an unrelated session write", recSeq, got)
	}
	// The first handoff is untouched by the second's failure.
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, first)); v != mqttMetadataCommitted {
		t.Fatalf("the first handoff resolved %v after the second lost its slot, want COMMITTED", v)
	}
}

// TestF105MetadataEvidenceSurvivesRetryExhaustion pins what a caller owes when it gives up.
//
// Exhausting a retry budget does not settle anything by itself: the batch may still be in
// flight. The caller settles it by taking the slot, and only a MATCHING positive abort lets
// it treat the handoff as never committed. Absence at exhaustion is still UNRESOLVED.
func TestF105MetadataEvidenceSurvivesRetryExhaustion(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-exhaust")
	defer done()

	i := testF105Intent(t, e, "ex")
	// Nothing was sent, or everything sent failed: the honest state is unresolved, NOT
	// aborted, because a delayed batch can still arrive.
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataUnresolved {
		t.Fatalf("an exhausted attempt with no receipt resolved %v, want UNRESOLVED", v)
	}
	// Settling it is a positive act.
	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); err != nil {
		t.Fatalf("settling the exhausted attempt failed: %v", err)
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataAborted {
		t.Fatalf("a settled attempt resolved %v, want ABORTED", v)
	}
	// And the fence holds afterwards: a batch that was in flight all along is refused.
	testF105Prepare(t, e, i)
	f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err == nil {
		t.Fatalf("a delayed batch committed after its intent was settled as aborted")
	}
}

// TestF105PostCommitRefusalDoesNotRetractTheHandoff pins the disposition for every failure
// AFTER the acceptance batch commits — a restore refusal, a failed CONNACK write, a lost
// ownership proof.
//
// None of them may abort or cancel: the handoff committed, and what fails is the connection.
// Retracting it would discard evidence the cluster has already acted on.
func TestF105PostCommitRefusalDoesNotRetractTheHandoff(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-postcommit")
	defer done()

	i := testF105Intent(t, e, "pc")
	testF105Prepare(t, e, i)
	f := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err != nil {
		t.Fatalf("the metadata commit was refused: %v", err)
	}

	// Whatever fails next, the receipt stands and cannot be withdrawn.
	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); err == nil {
		t.Fatalf("a committed handoff was retracted after a post-commit refusal")
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataCommitted {
		t.Fatalf("the handoff resolved %v after a post-commit refusal, want COMMITTED", v)
	}

	// The connection's own disposition is separate, and sealing its transport does not
	// touch the handoff: a closure certificate says the connection can emit nothing more,
	// never that an obligation was abandoned.
	bind := mqttBindConnection(1234)
	cert := mqttCertifyClosed(nil, bind, "post-commit refusal")
	if cert == nil || !mqttBindingCertifies(bind, cert) {
		t.Fatalf("certifying the refused connection failed")
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataCommitted {
		t.Fatalf("closing the connection changed the handoff verdict to %v", v)
	}
}

// testF105CreationProp views a first-creation intent as a proposal.
func testF105CreationProp(t *testing.T, idSuffix string) mqttOwnershipProposal {
	t.Helper()
	return testF105CreationPropAt(t, idSuffix, testF105Gen2)
}

// testF105CreationPropAt names the generation, for fixtures that claim more than once: a
// claim over a CLOSED predecessor must propose a FRESH generation, so a second proposal
// reusing the first's is refused by the kind rule rather than by anything being tested.
func testF105CreationPropAt(t *testing.T, idSuffix, gen string) mqttOwnershipProposal {
	t.Helper()
	p, err := mqttProposalFromCreation(&mqttCreationIntent{
		ID: "intent-" + idSuffix, Gen: gen, Tenure: "tenure-1",
	})
	if err != nil {
		t.Fatalf("the fixture creation intent was refused: %v", err)
	}
	return p
}

// testF105FreezeProposal freezes an attempt for EITHER proposal shape.
func testF105FreezeProposal(t *testing.T, e *testF105Env, p mqttOwnershipProposal, baseSeq uint64) *mqttFrozenMetadataAttempt {
	t.Helper()
	fact := &mqttAcceptFact{ClientID: e.id, SessEpoch: e.epoch, AcceptID: mqttNewAcceptID(),
		At: time.Now().UnixNano()}
	fb, err := json.Marshal(fact)
	if err != nil {
		t.Fatalf("marshalling the acceptance fact: %v", err)
	}
	factSubj := mqttAcceptSubject(_EMPTY_, getHash(e.id), e.epoch, fact.AcceptID)
	wid := nuid.Next()
	rec, _ := json.Marshal(&mqttPersistedSession{ID: e.id, Gen: p.Gen, WID: wid, Origin: "o"})
	f, ferr := mqttFreezeMetadataAttempt(e.id, e.epoch, _EMPTY_, p, wid, factSubj, fb, baseSeq, rec)
	if ferr != nil {
		t.Fatalf("freezing the attempt: %v", ferr)
	}
	return f
}

// TestF105FirstCreationHasAnExecutableMetadataProtocol is the round trip that makes the
// bootstrap real rather than described.
//
// The reservation that opens a virgin client's ownership is not a handoff — it displaces
// nothing — and before this the frozen attempt, the resolver and the aborter took a handoff
// intent and nothing else, so a first creation had a state machine and no way to commit. The
// same create-only slot arbitrates it: one verdict per proposal, commit and abort racing for
// it, which is the whole fence.
func TestF105FirstCreationHasAnExecutableMetadataProtocol(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-creation")
	defer done()

	p := testF105CreationProp(t, "c1")
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataUnresolved {
		t.Fatalf("a creation with no receipt resolved %v, want UNRESOLVED", v)
	}

	testF105StandCreation(t, e, p)
	f := testF105FreezeProposal(t, e, p, testF105RecordSeq(t, e))
	recSeq, err := e.asm.jsa.commitMetadataAttempt(f)
	if err != nil {
		t.Fatalf("the creation's metadata commit was refused: %v", err)
	}
	if recSeq == 0 {
		t.Fatalf("the creation's metadata commit returned no session-record sequence")
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataCommitted {
		t.Fatalf("the committed creation resolved %v, want COMMITTED", v)
	}
	// The slot is permanent for a creation exactly as for a handoff.
	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, p); err == nil {
		t.Fatalf("a committed creation was retracted")
	}

	// A HANDOFF NAMING THE SAME ID IS NOT ANSWERED BY IT. The kind is part of the answer,
	// not decoration: finalizing a handoff on a creation's receipt would install ownership
	// with no predecessor displaced.
	h := testF105Prop(t, &mqttHandoffIntent{
		ID: p.ID, PrevGen: testF105Gen, PrevTenure: "tenure-0", PrevOwnerTxn: "txn-0",
		PrevSessWID: "wid-0", PrevSessSeq: 3, Tenure: "tenure-1", Gen: testF105Gen2,
		CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: testF105Gen, Tenure: "tenure-0", OwnerTxn: "txn-0",
			SessWID: "wid-0", SessSeq: 3, Reason: mqttRetireCleanStart, Discard: true,
		},
	})
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, h); v != mqttMetadataForeign {
		t.Fatalf("a handoff read a creation's receipt as %v, want FOREIGN", v)
	}
}

// TestF105AbandonedCreationSettlesBeforeItsBatchArrives pins the abort half for a creation:
// the slot is taken positively, and a batch that was in flight all along is then refused in
// its entirety.
func TestF105AbandonedCreationSettlesBeforeItsBatchArrives(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-creation-abort")
	defer done()

	p := testF105CreationProp(t, "c2")
	if err := e.asm.jsa.abortMetadataAttempt(_EMPTY_, e.id, e.epoch, p); err != nil {
		t.Fatalf("settling the abandoned creation failed: %v", err)
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataAborted {
		t.Fatalf("a settled creation resolved %v, want ABORTED", v)
	}

	// The reservation stands, so the delayed batch is refused by the SETTLED VERDICT and
	// not for want of a claim — which is the fence being tested.
	testF105StandCreation(t, e, p)
	f := testF105FreezeProposal(t, e, p, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err == nil {
		t.Fatalf("a delayed creation batch committed after its proposal was settled as aborted")
	}
	// And the refusal took the WHOLE batch: the session record did not move.
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataAborted {
		t.Fatalf("the delayed batch changed the settled verdict to %v", v)
	}
}

// TestF105SettlementResolvesBeforeItAborts pins the order the one settlement executor runs
// in, which is the whole of its correctness.
//
// A settlement that aborted first would destroy a verdict that had already been reached; one
// that returned the abort's own error as the verdict would report "aborted" for a slot a
// commit already held, because a failed abort establishes only that the slot is OCCUPIED and
// says nothing about by what.
func TestF105SettlementResolvesBeforeItAborts(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-settle")
	defer done()

	// (1) A COMMITTED slot is reported as committed and is NOT aborted by the settlement.
	wonIntent := testF105Intent(t, e, "won")
	testF105Prepare(t, e, wonIntent)
	won := testF105Prop(t, wonIntent)
	f := testF105FreezeProposal(t, e, won, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err != nil {
		t.Fatalf("the metadata commit was refused: %v", err)
	}
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, won); v != mqttMetadataCommitted {
		t.Fatalf("settling a committed proposal returned %v, want COMMITTED", v)
	}
	// Still committed: the settlement did not overwrite the verdict it found.
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, won); v != mqttMetadataCommitted {
		t.Fatalf("settling a committed proposal changed its verdict to %v", v)
	}

	// (2) AN UNRESOLVED slot is taken, and the verdict comes from the READBACK.
	stalledIntent := testF105Intent(t, e, "stalled")
	stalled := testF105Prop(t, stalledIntent)
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, stalled); v != mqttMetadataUnresolved {
		t.Fatalf("a proposal with no receipt resolved %v, want UNRESOLVED", v)
	}
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, stalled); v != mqttMetadataAborted {
		t.Fatalf("settling an unresolved proposal returned %v, want ABORTED", v)
	}
	// AND THE FENCE HOLDS: the claimant's batch, still in flight all along, is refused in
	// its entirety. This is what makes the settlement a decision rather than a guess.
	testF105Prepare(t, e, stalledIntent)
	late := testF105FreezeProposal(t, e, stalled, testF105RecordSeq(t, e))
	if _, err := e.asm.jsa.commitMetadataAttempt(late); err == nil {
		t.Fatalf("a delayed batch committed after its proposal had been settled as aborted")
	}

	// (3) SETTLING IS IDEMPOTENT. A second round — the retry a CAS loop performs — returns
	// the same permanent verdict and writes nothing new.
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, stalled); v != mqttMetadataAborted {
		t.Fatalf("re-settling an aborted proposal returned %v, want ABORTED", v)
	}

	// (4) A CREATION SETTLES THROUGH THE SAME EXECUTOR. The slot does not know who is
	// writing, and neither shape gets a protocol of its own.
	virgin := testF105CreationProp(t, "c-settle")
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, virgin); v != mqttMetadataAborted {
		t.Fatalf("settling an unresolved creation returned %v, want ABORTED", v)
	}

	// (5) A RECEIPT THAT DOES NOT ANSWER THIS PROPOSAL IS FOREIGN, never a settlement. The
	// handoff below shares the creation's id and nothing else, and reading its receipt as an
	// answer would finalize a displacement that never happened.
	impostor := testF105Prop(t, &mqttHandoffIntent{
		ID: virgin.ID, PrevGen: testF105Gen, PrevTenure: "tenure-0", PrevOwnerTxn: "txn-0",
		PrevSessWID: "wid-0", PrevSessSeq: 3, Tenure: "tenure-1", Gen: testF105Gen2,
		CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: testF105Gen, Tenure: "tenure-0", OwnerTxn: "txn-0",
			SessWID: "wid-0", SessSeq: 3, Reason: mqttRetireCleanStart, Discard: true,
		},
	})
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, impostor); v != mqttMetadataForeign {
		t.Fatalf("a handoff settling against a creation's receipt returned %v, want FOREIGN", v)
	}
}

// TestF105HandoffCommitsAgainstThePredecessorItNames pins that the receipt and the write
// name ONE record.
//
// The receipt copies the proposal's PrevSessSeq and PrevSessWID; the batch writes the session
// record under its own expectation. If those two could differ, a delayed handoff naming
// session A could commit against whatever stands now — session B — overwrite it, and publish
// a permanent receipt claiming it displaced A. resolveMetadataAttempt would accept that
// receipt, because it compares the PROPOSAL's fields and not the record actually replaced.
//
// A sequence is a POSITION, not an identity, so the stored write identity and generation are
// checked too: the session record is a single-slot register every confirmed save advances.
func TestF105HandoffCommitsAgainstThePredecessorItNames(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-cas-binding")
	defer done()

	i := testF105Intent(t, e, "bind")

	// (1) THE EXPECTATION MUST BE THE PREDECESSOR'S OWN. A handoff frozen against any other
	// sequence is refused at composition, before anything reaches the store.
	for name, base := range map[string]uint64{
		"a successor's sequence":  i.PrevSessSeq + 1,
		"a predecessor's":         i.PrevSessSeq - 1,
		"create-only, as a first": 0,
	} {
		if _, err := testF105TryFreeze(t, e, i, base); err == nil {
			t.Fatalf("a handoff naming session record %d was frozen against %s (%d)",
				i.PrevSessSeq, name, base)
		}
	}
	// The matching one is accepted.
	if _, err := testF105TryFreeze(t, e, i, i.PrevSessSeq); err != nil {
		t.Fatalf("a handoff frozen against the record it names was refused: %v", err)
	}

	// (2) AND THE RECORD AT THAT SEQUENCE MUST BE THE ONE IT NAMES. A proposal whose
	// predecessor identity does not match the standing record is refused at commit, even
	// though its expectation is correct — the position is right and the session is not.
	ps, seq := testF105StandingSession(t, e)
	for name, mutate := range map[string]func(*mqttHandoffIntent){
		"another write identity": func(m *mqttHandoffIntent) { m.PrevSessWID = "wid-someone-else" },
		"another generation": func(m *mqttHandoffIntent) {
			if m.PrevGen == testF105Gen {
				m.PrevGen, m.Gen = testF105Gen2, testF105Gen
			} else {
				m.PrevGen, m.Gen = testF105Gen, testF105Gen2
			}
			m.Retire.Gen = m.PrevGen
		},
	} {
		m := *i
		retire := *i.Retire
		m.Retire = &retire
		m.ID = "intent-" + name[:4]
		mutate(&m)
		m.PrevSessSeq, m.Retire.SessSeq = seq, seq
		m.Retire.SessWID = m.PrevSessWID
		f, ferr := testF105TryFreeze(t, e, &m, seq)
		if ferr != nil {
			t.Fatalf("freezing the %s attempt failed: %v", name, ferr)
		}
		if _, cerr := e.asm.jsa.commitMetadataAttempt(f); cerr == nil {
			t.Fatalf("a handoff naming %s committed against the standing session record", name)
		}
		// Nothing moved: the refusal is before the batch.
		if _, after := testF105StandingSession(t, e); after != seq {
			t.Fatalf("a refused handoff moved the session record from %d to %d", seq, after)
		}
	}
	if ps.WID == _EMPTY_ {
		t.Fatalf("the environment's session record carries no write identity")
	}

	// (3) The legitimate handoff still commits, and its receipt answers it.
	good := testF105Intent(t, e, "bind-ok")
	testF105Prepare(t, e, good)
	f := testF105FreezeAttempt(t, e, good, good.PrevSessSeq)
	if _, cerr := e.asm.jsa.commitMetadataAttempt(f); cerr != nil {
		t.Fatalf("a handoff naming the record it displaces was refused: %v", cerr)
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, good)); v != mqttMetadataCommitted {
		t.Fatalf("the committed handoff resolved %v, want COMMITTED", v)
	}
}

// testF105TryFreeze composes an attempt and RETURNS the error rather than failing, so a
// refusal can be the assertion.
func testF105TryFreeze(t *testing.T, e *testF105Env, i *mqttHandoffIntent, baseSeq uint64) (*mqttFrozenMetadataAttempt, error) {
	t.Helper()
	fact := &mqttAcceptFact{ClientID: e.id, SessEpoch: e.epoch, AcceptID: mqttNewAcceptID(),
		At: time.Now().UnixNano()}
	fb, err := json.Marshal(fact)
	if err != nil {
		t.Fatalf("marshalling the acceptance fact: %v", err)
	}
	factSubj := mqttAcceptSubject(_EMPTY_, getHash(e.id), e.epoch, fact.AcceptID)
	wid := nuid.Next()
	rec, _ := json.Marshal(&mqttPersistedSession{ID: e.id, Gen: i.Gen, WID: wid, Origin: "o"})
	return mqttFreezeMetadataAttempt(e.id, e.epoch, _EMPTY_, testF105Prop(t, i), wid,
		factSubj, fb, baseSeq, rec)
}
