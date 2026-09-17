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

// The F-105 arbitration against a real JetStream.
//
// Everything below drives the actual stream, because the properties being established are
// properties of the STORE and not of the Go code: that a create-only outcome member and a
// create-only abort genuinely contend for one subject, that a losing member rejects its
// whole batch, and that a committed receipt survives being superseded. A fixture that
// mocked the store would be asserting its own mock.

// testF105Env is a live account session manager plus the exact identities its records are
// attributed by.
type testF105Env struct {
	asm   *mqttAccountSessionManager
	id    string
	epoch string
}

func testF105NewEnv(t *testing.T, clientID string) (*testF105Env, func()) {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	epoch := asm.sessStreamEpoch
	asm.mu.RUnlock()
	if epoch == _EMPTY_ {
		c.Close()
		testMQTTShutdownServer(s)
		t.Fatal("the account has no recovery epoch, so nothing here can be attributed")
	}
	// PIN THE SWEEP START. inflightSweepStart seeds itself at a RANDOM point in the stream,
	// so any assertion about what a bounded pass reaches is otherwise a coin toss — this
	// test file's boundedness oracle failed roughly one run in three before this, which is
	// a flaky oracle rather than a real signal. The repository provides this seam for
	// exactly that reason.
	asm.mu.Lock()
	asm.testQoS2SweepSeed = func(firstSeq, _ uint64) uint64 { return firstSeq }
	asm.mu.Unlock()

	return &testF105Env{asm: asm, id: clientID, epoch: epoch}, func() {
		c.Close()
		testMQTTShutdownServer(s)
	}
}

// firstReservation builds this environment's FIRST CLAIM on its client.
//
// It is a reservation and not a serving authority, because there is no longer a kind that
// installs one outright: a claim serves nothing until its metadata half commits. Its
// OWNERSHIP IS ITS OWN IDENTITY — there is no predecessor whose ownership it could inherit,
// so the transition that claims the client is the transition that owns it.
func (e *testF105Env) firstReservation(txn string) *mqttAuthorityTransition {
	return testF105ReserveCreation(e, nil, 0, txn, testF105Gen, "tenure-"+txn, "intent-"+txn)
}

// bootstrap installs a SERVING authority the only way one can now be installed: a
// reservation, the metadata commit that answers its proposal, and a finalization that reads
// that permanent verdict.
//
// Every fixture that used to open with a one-shot first admission goes through this, which
// is the point of deleting that kind: the bootstrap a test performs is the bootstrap the
// broker performs, so a test can no longer establish ownership by a route production has no
// way to take.
//
// It returns the serving authority and the sequence it stands at, which is what a follow-on
// transition CASes against.
func (e *testF105Env) bootstrap(t *testing.T, txn string) (*mqttOutboundAuthority, uint64) {
	t.Helper()
	tr, seq := e.bootstrapTr(t, txn)
	return tr.Auth, seq
}

// bootstrapTr is bootstrap for a fixture that needs the FINALIZING TRANSITION itself — to
// resolve it, to freeze a handle over it, or to name it as a predecessor's attempt.
//
// It composes at the TRANSITION layer rather than through the operation surface, because
// those fixtures are testing the transition layer: an operation returns a sequence, which is
// all a converted caller needs and not enough to ask what an attempt resolved to.
func (e *testF105Env) bootstrapTr(t *testing.T, txn string) (*mqttAuthorityTransition, uint64) {
	t.Helper()
	return e.claimTr(t, nil, 0, txn, testF105Gen, "tenure-"+txn)
}

// claim is the whole route to a serving authority, over an absent predecessor or a CLOSED
// one. The post-closure case is the SAME three steps as the first claim, which is what
// deleting the readmission kind established: there is no cheaper way in on a second attempt
// than on the first.
func (e *testF105Env) claim(t *testing.T, prev *mqttOutboundAuthority, prevSeq uint64,
	txn, gen, tenure string) (*mqttOutboundAuthority, uint64) {

	t.Helper()
	// THROUGH THE OPERATION SURFACE, which is what a converted caller will use: it loads the
	// predecessor itself, so prev/prevSeq are the fixture's statement of what it EXPECTS to
	// be there rather than what the operation acts on.
	if _, err := e.asm.jsa.opClaimClient(e.id, e.epoch, txn, gen, tenure, "intent-"+txn); err != nil {
		t.Fatalf("the reservation %q was refused: %v", txn, err)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.Creation == nil {
		t.Fatalf("the reservation %q does not load back (status %v)", txn, al.status)
	}
	if prev != nil && al.rec.Rev != prev.Rev+1 {
		t.Fatalf("the reservation %q landed at revision %d over a predecessor at %d",
			txn, al.rec.Rev, prev.Rev)
	}
	e.commitCreation(t, al.rec.Creation)
	fin, ferr := e.asm.jsa.opFinalizeClaim(e.id, e.epoch, txn+"-fin", "intent-"+txn)
	if ferr != nil {
		t.Fatalf("finalizing the reservation %q was refused: %v", txn, ferr)
	}
	fl := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if fl.status != mqttOutboundValid || fl.rec.State != mqttAuthorityServing {
		t.Fatalf("the finalized authority does not load back (status %v)", fl.status)
	}
	return fl.rec, fin.Seq
}

func (e *testF105Env) claimTr(t *testing.T, prev *mqttOutboundAuthority, prevSeq uint64,
	txn, gen, tenure string) (*mqttAuthorityTransition, uint64) {

	t.Helper()
	res := testF105ReserveCreation(e, prev, prevSeq, txn, gen, tenure, "intent-"+txn)
	resSeq, err := e.asm.jsa.commitAuthorityTransition(res)
	if err != nil {
		t.Fatalf("the reservation %q was refused: %v", txn, err)
	}
	e.commitCreation(t, res.Auth.Creation)
	fin := testF105FinalizeCreation(e, res.Auth, resSeq, txn+"-fin")
	finSeq, ferr := e.asm.jsa.commitAuthorityTransition(fin)
	if ferr != nil {
		t.Fatalf("finalizing the reservation %q was refused: %v", txn, ferr)
	}
	return fin, finSeq
}

// testF105AdmitNothing is an admission that carries no obligation of its own.
//
// It exists because several fixtures need a transition that ADVANCES the authority's
// revision under the standing tenure and nothing else — to be resolved, to be superseded, or
// to carry one hand-built member. The kind is the admission kind because that is what a
// revision-advancing transition under a serving tenure IS; the obligations it would normally
// carry are the caller's to add.
func testF105AdmitNothing(e *testF105Env, prev *mqttOutboundAuthority, prevSeq uint64,
	txn string) *mqttAuthorityTransition {

	rev := prev.Rev + 1
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionAdmitExchange, PrevTxn: prev.OwnerTxn, Rev: rev,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityServing, Rev: rev, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
	}
}

// testF105Closure closes the generation a serving authority stands at, permanently.
//
// The revision is derived from the predecessor rather than written as a literal, because the
// route to a serving authority is three transitions rather than one and a fixture that
// counted them would break every time the bootstrap gained a step.
func testF105Closure(e *testF105Env, prev *mqttOutboundAuthority, prevSeq uint64,
	txn string) *mqttAuthorityTransition {

	rev := prev.Rev + 1
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionCloseAdmit, PrevTxn: prev.OwnerTxn, Rev: rev,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityClosed, Rev: rev, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: prev.Gen, Txn: txn},
	}
}

// commitCreation commits the metadata half of a creation proposal, which is what licenses
// the finalization that follows it.
func (e *testF105Env) commitCreation(t *testing.T, c *mqttCreationIntent) {
	t.Helper()
	p, err := mqttProposalFromCreation(c)
	if err != nil {
		t.Fatalf("the reservation's proposal is uninterpretable: %v", err)
	}
	f := testF105FreezeProposal(t, e, p, testF105RecordSeq(t, e))
	if _, cerr := e.asm.jsa.commitMetadataAttempt(f); cerr != nil {
		t.Fatalf("the creation's metadata half was refused: %v", cerr)
	}
}

// TestF105AbortAndCommitContendForOneSubject is the ambiguity protocol, against the store.
//
// The whole design rests on a claim about JetStream: a create-only member inside an atomic
// batch and a standalone create-only publish to the same subject are mutually exclusive, and
// the loser's batch is rejected in its entirety. If that were false, an abort could stand
// beside a committed transition and "never landed" would be a lie.
func TestF105AbortAndCommitContendForOneSubject(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-contend")
	defer done()

	// ABORT FIRST: the commit that follows must be refused, and nothing it carried may
	// stand — including the authority, which is the member that matters.
	tr := e.firstReservation("txn-abort-first")
	if err := e.asm.jsa.abortTransition(tr.Attempt); err != nil {
		t.Fatalf("the abort was refused: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a transition committed after its own attempt was positively aborted")
	}
	if al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); al.status != mqttOutboundAbsent {
		t.Fatalf("the refused batch left an authority behind (status %v): the losing member "+
			"must reject the whole batch", al.status)
	}
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionNeverLanded {
		t.Fatalf("an aborted attempt resolved %v, want NEVER LANDED", v)
	}

	// COMMIT FIRST: the abort that follows must fail, and the receipt must still say
	// COMMITTED.
	tr2 := e.firstReservation("txn-commit-first")
	seq, err := e.asm.jsa.commitAuthorityTransition(tr2)
	if err != nil {
		t.Fatalf("the transition was refused: %v", err)
	}
	if seq == 0 {
		t.Fatalf("the commit returned no authority sequence")
	}
	if err := e.asm.jsa.abortTransition(tr2.Attempt); err == nil {
		t.Fatalf("an attempt that had already committed was aborted afterwards")
	}
	if v := e.asm.jsa.resolveTransition(tr2); v != mqttTransitionLanded {
		t.Fatalf("a committed attempt resolved %v, want LANDED", v)
	}

	// The returned sequence must be the AUTHORITY's, which is what a later transition
	// contends against.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the committed authority does not load back (status %v)", al.status)
	}
	if al.seq != seq {
		t.Fatalf("the commit returned sequence %d but the authority stands at %d", seq, al.seq)
	}
	if al.rec.OwnerTxn != "txn-commit-first" {
		t.Fatalf("the authority carries ownership %q, want txn-commit-first", al.rec.OwnerTxn)
	}
}

// TestF105CommittedOutcomeSurvivesSupersession pins the distinction a lost acknowledgement
// depends on: an attempt that landed and was later superseded is NOT an attempt that never
// landed. Its exchange mutations are real and its obligations exist; only the ownership it
// installed has moved on. Collapsing the two would strand everything the first transition
// created.
func TestF105CommittedOutcomeSurvivesSupersession(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-supersede")
	defer done()

	first, seq := e.bootstrapTr(t, "txn-1")
	if v := e.asm.jsa.resolveTransition(first); v != mqttTransitionLanded {
		t.Fatalf("the finalization resolved %v, want LANDED", v)
	}

	// A second transition over it: an exchange admitted under the same tenure, which
	// advances the revision without moving ownership.
	second := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-2", Op: "op-2",
			Kind: mqttTransitionAdmitExchange, PrevTxn: "txn-1", Rev: first.Auth.Rev + 1,
			Gen: testF105Gen, Tenure: first.Auth.Tenure, OwnerTxn: "txn-1",
		},
		Prev: first.Auth, PrevSeq: seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Tenure: first.Auth.Tenure,
			State: mqttAuthorityServing, Rev: first.Auth.Rev + 1, Txn: "txn-2", OwnerTxn: "txn-1",
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(second); err != nil {
		t.Fatalf("the second transition was refused: %v", err)
	}

	if v := e.asm.jsa.resolveTransition(first); v != mqttTransitionSuperseded {
		t.Fatalf("the first attempt resolved %v after a later revision landed, want SUPERSEDED", v)
	}
	if v := e.asm.jsa.resolveTransition(second); v != mqttTransitionLanded {
		t.Fatalf("the second attempt resolved %v, want LANDED", v)
	}

	// And the first attempt's own permanent receipt is still readable and still committed:
	// supersession retracts nothing.
	o, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, "txn-1")
	if st != mqttOutboundValid || o.Verdict != mqttOutcomeCommitted {
		t.Fatalf("the superseded attempt's receipt reads back status=%v verdict=%v", st, o.Verdict)
	}
}

// TestF105ResolutionRefusesAReceiptForAnotherMutation pins the digest check. A receipt found
// at a transition id proves only that SOME batch under that id committed; if the caller's
// assembled mutation differs, that is a contradiction and must not be reported as its
// answer.
func TestF105ResolutionRefusesAReceiptForAnotherMutation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-digest")
	defer done()

	auth, seq := e.bootstrap(t, "txn-1")
	tr := testF105AdmitNothing(e, auth, seq, "txn-d")
	// An invalid body would make this test pass through the shape validator rather than
	// through the digest, which is not the property being established.
	//
	// TWO VALID EXCHANGE mutations, differing only in their bodies. Neither a marker nor a
	// register can be used here any more: a marker is minted with the register it names, and
	// a register is admitted with the obligation it allocates for, so either alone is refused
	// for a reason that has nothing to do with the digest.
	xchBody := func(nonce string) []byte {
		b, merr := json.Marshal(&mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: nonce,
			PI: 7, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: "txn-d", Tenure: auth.Tenure,
		})
		if merr != nil {
			t.Fatalf("marshalling the exchange body failed: %v", merr)
		}
		return b
	}
	nonceA, nonceB := mqttNewInflightNonce(), mqttNewInflightNonce()
	markerA, markerB := xchBody(nonceA), xchBody(nonceB)
	tr.Extra = testF105CoverExchanges(t, e, testF105Gen, []mqttTransitionMember{{
		subj:   mqttExchangeSubject(getHash(e.id), testF105Gen, nonceA),
		expect: 0,
		body:   markerA,
	}})
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("the transition was refused: %v", err)
	}
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionLanded {
		t.Fatalf("the committed transition resolved %v, want LANDED", v)
	}

	// The same attempt, resolved against a DIFFERENT assembled mutation. Every field of
	// the outcome still matches; only the member's body differs.
	other := *tr
	other.Extra = []mqttTransitionMember{{
		subj:   mqttExchangeSubject(getHash(e.id), testF105Gen, nonceB),
		expect: 0,
		body:   markerB,
	}}
	if v := e.asm.jsa.resolveTransition(&other); v != mqttTransitionUnresolved {
		t.Fatalf("a receipt for a different mutation resolved %v, want UNRESOLVED", v)
	}
}

// TestF105ClosureLoadsBackAndAbsenceIsNotOpen pins both halves of the closure contract: a
// close-admission transition writes the permanent record with its authority in one batch,
// and a generation with no closure record is NOT thereby established as open — open
// admission requires the authority's positive state.
func TestF105ClosureLoadsBackAndAbsenceIsNotOpen(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closure")
	defer done()

	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen); st != mqttOutboundAbsent {
		t.Fatalf("an unclosed generation reports closure status %v, want ABSENT", st)
	}

	auth, seq := e.bootstrap(t, "txn-1")

	closing := testF105Closure(e, auth, seq, "txn-close")
	if _, err := e.asm.jsa.commitAuthorityTransition(closing); err != nil {
		t.Fatalf("the close-admission transition was refused: %v", err)
	}

	c, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen)
	if st != mqttOutboundValid {
		t.Fatalf("the permanent closure does not load back (status %v)", st)
	}
	if c.Txn != "txn-close" {
		t.Fatalf("the closure names transition %q, want txn-close", c.Txn)
	}
	// Another generation is still absent, and absence establishes nothing about it.
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen2); st != mqttOutboundAbsent {
		t.Fatalf("an unrelated generation reports closure status %v, want ABSENT", st)
	}

	// ADMISSION AFTER CLOSURE IS REFUSED, against the real standing authority.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityClosed {
		t.Fatalf("the authority did not become closed (status %v)", al.status)
	}
	admit := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-late", Op: "op-late",
			Kind: mqttTransitionAdmitExchange, PrevTxn: "txn-1", Rev: al.rec.Rev + 1,
			Gen: testF105Gen, Tenure: auth.Tenure, OwnerTxn: "txn-1",
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Tenure: auth.Tenure,
			State: mqttAuthorityServing, Rev: al.rec.Rev + 1, Txn: "txn-late", OwnerTxn: "txn-1",
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(admit); err == nil {
		t.Fatalf("an exchange was admitted over a CLOSED authority")
	}
}

// TestF105UnresolvedIsNotNeverLanded pins the honest answer. An attempt whose outcome has
// not been written is UNRESOLVED, never "never landed": the batch may still be in flight and
// may still commit, and a caller that treated absence as a negative fact would be the
// release-on-absence shape under another name.
func TestF105UnresolvedIsNotNeverLanded(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-unresolved")
	defer done()

	tr := e.firstReservation("txn-never-sent")
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionUnresolved {
		t.Fatalf("an attempt with no outcome resolved %v, want UNRESOLVED", v)
	}
	// Settling it positively is what turns it into a fact.
	if err := e.asm.jsa.abortTransition(tr.Attempt); err != nil {
		t.Fatalf("the abort was refused: %v", err)
	}
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionNeverLanded {
		t.Fatalf("a positively aborted attempt resolved %v, want NEVER LANDED", v)
	}
}

// TestF105CancelledPreparationReturnsToServing pins the exit a preparation needs when its
// metadata half is positively aborted.
//
// Without a cancellation the state machine has no move: admission and preparation both
// require SERVING, finalization requires the proposed ownership, and closing admission would
// end a session nobody displaced. A broker would have to delete the authority or wait
// forever, and deleting it is the shape this whole item exists to remove.
func TestF105CancelledPreparationReturnsToServing(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-cancel")
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
	prepSeq, err := e.asm.jsa.commitAuthorityTransition(prep)
	if err != nil {
		t.Fatalf("the preparation was refused: %v", err)
	}

	// While PREPARING, no new outbound work may be admitted: the freeze is the point.
	frozen := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-frozen", Op: "op-frozen",
			Kind: mqttTransitionAdmitExchange, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 2,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: prep.Auth, PrevSeq: prepSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: mqttAuthorityServing, Rev: auth.Rev + 2, Txn: "txn-frozen", OwnerTxn: auth.OwnerTxn,
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(frozen); err == nil {
		t.Fatalf("an exchange was admitted while the authority was preparing")
	}

	// A cancellation naming ANOTHER intent is refused. This runs BEFORE the good
	// cancellation: afterwards the authority has advanced, and the store would refuse it on
	// the CAS without the intent rule ever being reached — which is a fixture passing for
	// the wrong reason rather than a guard being proved.
	foreign := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-cancel-foreign", Op: "op-cancel-foreign",
			Kind: mqttTransitionCancelIntent, Intent: "intent-other",
			PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 2,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: prep.Auth, PrevSeq: prepSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: mqttAuthorityServing, Rev: auth.Rev + 2, Txn: "txn-cancel-foreign", OwnerTxn: auth.OwnerTxn,
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(foreign); err == nil {
		t.Fatalf("a cancellation naming another intent was sent")
	}

	// The cancellation restores the ORIGINAL ownership. It must not install the proposal.
	cancel := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-cancel", Op: "op-cancel",
			Kind: mqttTransitionCancelIntent, Intent: intent.ID,
			PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 2,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: prep.Auth, PrevSeq: prepSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: mqttAuthorityServing, Rev: auth.Rev + 2, Txn: "txn-cancel", OwnerTxn: auth.OwnerTxn,
		},
	}
	// A CANCELLATION FOLLOWS THE PERMANENT VERDICT, not the authority record's shape. The
	// proposal's metadata half is settled ABORTED first; without that the transition is
	// refused, which is the point — a preparation may not be cancelled while its batch could
	// still commit.
	hp, herr := mqttProposalFromHandoff(intent)
	if herr != nil {
		t.Fatalf("viewing the pending handoff as a proposal failed: %v", herr)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(cancel); err == nil {
		t.Fatalf("a cancellation was committed while its metadata half was unresolved")
	}
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, hp); v != mqttMetadataAborted {
		t.Fatalf("settling the pending handoff returned %v, want ABORTED", v)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(cancel); err != nil {
		t.Fatalf("the cancellation was refused: %v", err)
	}

	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the cancelled authority does not load back (status %v)", al.status)
	}
	if al.rec.State != mqttAuthorityServing {
		t.Fatalf("the authority is in state %d after cancellation, want serving", al.rec.State)
	}
	if al.rec.Gen != auth.Gen || al.rec.OwnerTxn != auth.OwnerTxn {
		t.Fatalf("the cancellation installed %q/%q rather than restoring %q/%q",
			al.rec.Gen, al.rec.OwnerTxn, auth.Gen, auth.OwnerTxn)
	}
	if al.rec.Intent != nil {
		t.Fatalf("the cancelled authority still carries a handoff intent")
	}

}

// TestF105ClosedAuthorityCanClaimAFreshGeneration pins the other missing exit. Closure is
// permanent and per GENERATION while the authority is per CLIENT, so without a route out a
// client whose authority reached CLOSED could never be served again — a first claim requires
// revision 1 and no predecessor, and finalization requires a standing proposal.
//
// The route is reserve-creation over the closure, and it is the SAME route as the first
// claim: there is no kind that installs a serving authority over a closed one, because such a
// kind would be a second bootstrap with no metadata half — reachable exactly when the first
// creation was abandoned, which is when it matters most.
func TestF105ClosedAuthorityCanClaimAFreshGeneration(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-reclaim")
	defer done()

	auth, seq := e.bootstrap(t, "txn-1")
	closing := testF105Closure(e, auth, seq, "txn-close")
	closeSeq, err := e.asm.jsa.commitAuthorityTransition(closing)
	if err != nil {
		t.Fatalf("the close-admission was refused: %v", err)
	}

	// The claim must name a DIFFERENT generation: it reserves a successor beside the
	// permanent closure and never reopens the closed one.
	reuse := testF105ReserveCreation(e, closing.Auth, closeSeq, "txn-reuse", testF105Gen,
		"tenure-reuse", "intent-reuse")
	if _, err := e.asm.jsa.commitAuthorityTransition(reuse); err == nil {
		t.Fatalf("a post-closure claim reused the closed generation")
	}

	// AND IT MAY NOT ARRIVE SERVING. The reservation is the only shape a claim takes, so a
	// caller cannot skip the metadata half by describing its successor as already servable.
	serving := testF105ReserveCreation(e, closing.Auth, closeSeq, "txn-serving", testF105Gen2,
		"tenure-serving", "intent-serving")
	serving.Auth.State, serving.Auth.Creation = mqttAuthorityServing, nil
	if _, err := e.asm.jsa.commitAuthorityTransition(serving); err == nil {
		t.Fatalf("a post-closure claim installed a SERVING authority in one step")
	}

	e.claim(t, closing.Auth, closeSeq, "txn-fresh", testF105Gen2, "tenure-fresh")

	// THE OLD CLOSURE STANDS. The claim reserves a successor; it does not reopen what was
	// closed, and a reader asking about the old generation must still be told it is closed.
	c, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen)
	if st != mqttOutboundValid || c.Txn != "txn-close" {
		t.Fatalf("the old generation's permanent closure did not survive re-admission (status %v)", st)
	}
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen2); st != mqttOutboundAbsent {
		t.Fatalf("the fresh generation reports closure status %v, want ABSENT", st)
	}
}

// TestF105SweepSkipsArbitrationRecords pins that the orphan sweep neither sweeps nor
// mis-logs F-105's permanent records, and does not spend its bounded budget on them.
//
// They share $MQTT_inflight with the registers because a batch is per stream, so the sweep
// walks straight over them. Before this they were retained as "unattributable registers":
// true in the sense that nothing could attribute them to a register, false as a description,
// and each one consumed a record from a pass whose whole point is to be bounded.
func TestF105SweepSkipsArbitrationRecords(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-sweep")
	defer done()

	// Land an authority, a permanent outcome and a permanent closure.
	auth, seq := e.bootstrap(t, "txn-1")
	closing := testF105Closure(e, auth, seq, "txn-close")
	if _, err := e.asm.jsa.commitAuthorityTransition(closing); err != nil {
		t.Fatalf("the close-admission was refused: %v", err)
	}

	// A register behind them, so the pass has something real to find.
	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 9)
	if _, err := e.asm.jsa.storeInflightCAS(regSubj, 0, testF105SweepRegister(t, e)); err != nil {
		t.Fatalf("planting the register behind the arbitration records failed: %v", err)
	}

	// THE DISCRIMINATING OBSERVABLE IS THE WARNING, and the obvious assertion is not:
	// whether or not the sweep recognises these classes it never COLLECTS one, because an
	// unrecognised record falls through to the register parse, fails it, and is retained.
	// What changes is that an operator is told a permanent arbitration record is an
	// unattributable in-flight register — a false statement about a record that is working
	// exactly as designed, and the kind an operator acts on.
	log := &DummyLogger{}
	e.asm.jsa.c.srv.SetLogger(log, false, false)

	cands, markers, err := e.asm.collectInflightCandidates(e.asm.jsa.c.srv)
	if err != nil {
		t.Fatalf("the candidate collection failed: %v", err)
	}
	for _, c := range cands {
		if mqttIsF105Subject(c.name.subject) {
			t.Fatalf("the sweep collected the arbitration record %q as a register candidate", c.name.subject)
		}
	}
	for _, m := range markers {
		if mqttIsF105Subject(m.name.subject) {
			t.Fatalf("the sweep collected the arbitration record %q as a marker candidate", m.name.subject)
		}
	}
	var found bool
	for _, c := range cands {
		if c.name.subject == regSubj {
			found = true
		}
	}
	if !found {
		t.Fatalf("the pass never reached the planted register at %q", regSubj)
	}

	log.Lock()
	msgs := append([]string(nil), log.AllMsgs...)
	log.Unlock()
	for _, m := range msgs {
		if strings.Contains(m, "unattributable in-flight register") && strings.Contains(m, mqttF105ClassTk) {
			t.Fatalf("the sweep warned that an arbitration record is an unattributable "+
				"in-flight register: %q", m)
		}
	}

	// And every record it DID land is still readable: the sweep walked over them without
	// touching them.
	if al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); al.status != mqttOutboundValid {
		t.Fatalf("the authority did not survive the sweep (status %v)", al.status)
	}
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen); st != mqttOutboundValid {
		t.Fatalf("the permanent closure did not survive the sweep (status %v)", st)
	}
	if _, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, "txn-1"); st != mqttOutboundValid {
		t.Fatalf("the permanent outcome did not survive the sweep (status %v)", st)
	}
}

// testF105SweepRegister is a minimal supported register body for the sweep fixture. It is
// built through the same shape the decode API accepts, so the plant cannot pass for a
// reason the production writer would not.
func testF105SweepRegister(t *testing.T, e *testF105Env) *mqttInflightRecord {
	t.Helper()
	return freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 9, Dur: "$MQTT_sweep", Seq: 1, QoS: 1,
		Nonce: mqttNewInflightNonce(),
	})
}

// TestF105SweepStaysBoundedOverArbitrationRecords pins that the skip charges the record
// budget.
//
// An earlier version of the skip refunded it, on the reasoning that a client's permanent
// outcomes should not crowd the pass out of its registers. That was wrong and this test
// exists because of it: the budget is the ONLY thing bounding the pass, so a record class
// that costs nothing to walk makes a stream full of permanent outcomes an unbounded scan.
// The cost is real — a client with many outcomes reaches its registers over more passes —
// and it is the correct trade, because a bounded pass that makes progress beats an
// unbounded one that must finish.
func TestF105SweepStaysBoundedOverArbitrationRecords(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-bounded")
	defer done()

	// Several transitions, each leaving a permanent outcome ahead of the register.
	prevAuth, seq := e.bootstrap(t, "txn-1")
	var err error
	for i := 2; i <= 5; i++ {
		txn := "txn-" + string(rune('0'+i))
		next := testF105AdmitNothing(e, prevAuth, seq, txn)
		if seq, err = e.asm.jsa.commitAuthorityTransition(next); err != nil {
			t.Fatalf("transition %s was refused: %v", txn, err)
		}
		prevAuth = next.Auth
	}

	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 9)
	if _, err := e.asm.jsa.storeInflightCAS(regSubj, 0, testF105SweepRegister(t, e)); err != nil {
		t.Fatalf("planting the register behind the arbitration records failed: %v", err)
	}

	limit := 2
	e.asm.mu.Lock()
	e.asm.testQoS2SweepRecordLimit = &limit
	e.asm.mu.Unlock()

	cands, _, err := e.asm.collectInflightCandidates(e.asm.jsa.c.srv)
	if err != nil {
		t.Fatalf("the candidate collection failed: %v", err)
	}
	for _, c := range cands {
		if c.name.subject == regSubj {
			t.Fatalf("a pass budgeted for %d records walked past at least 5 arbitration "+
				"records and still reached %q: the skip is not charging the budget, so a "+
				"stream of permanent outcomes would be scanned without bound", limit, regSubj)
		}
	}
}

// TestF105HandleClassifiesWithoutTheOriginalInputs pins the classification boundary Codex's
// consult 6 set.
//
// resolveTransition recomputes the digest from the assembled transition, so it serves a
// caller still holding what it tried to write. A caller that RESTARTED holds none of that.
// The handle is the form that survives: it retains the ANSWER — the immutable attempt
// identity and the expected digest — rather than the inputs, and it must still classify
// correctly after later transitions have overwritten the authority the original mutation
// touched.
//
// The test DISCARDS the original transition before resolving, which is the whole point: if
// the handle needed those bytes it would be the same API under another name.
func TestF105HandleClassifiesWithoutTheOriginalInputs(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-handle")
	defer done()

	base, baseSeq := e.bootstrap(t, "txn-1")
	first := testF105AdmitNothing(e, base, baseSeq, "txn-h")
	h, err := mqttNewTransitionHandle(first)
	if err != nil {
		t.Fatalf("freezing the handle failed: %v", err)
	}
	seq, err := e.asm.jsa.commitAuthorityTransition(first)
	if err != nil {
		t.Fatalf("the transition under the handle was refused: %v", err)
	}
	if v := e.asm.jsa.resolveTransitionHandle(h); v != mqttTransitionLanded {
		t.Fatalf("a committed attempt resolved %v from its handle, want LANDED", v)
	}

	// THE ORIGINAL INPUTS GO AWAY. Everything after this point is what a restarted caller
	// would have: the handle, and the store.
	prevAuth, prevSeq := first.Auth, seq
	first = nil

	// MULTIPLE successors, each overwriting the authority the original mutation wrote.
	for i := 2; i <= 4; i++ {
		txn := "txn-" + string(rune('0'+i))
		next := testF105AdmitNothing(e, prevAuth, prevSeq, txn)
		if prevSeq, err = e.asm.jsa.commitAuthorityTransition(next); err != nil {
			t.Fatalf("successor %s was refused: %v", txn, err)
		}
		prevAuth = next.Auth
	}

	// The historical attempt is still decidable, and correctly SUPERSEDED rather than
	// unresolved or never-landed: its mutations are real and its obligations exist.
	if v := e.asm.jsa.resolveTransitionHandle(h); v != mqttTransitionSuperseded {
		t.Fatalf("after three successors the handle resolved %v, want SUPERSEDED", v)
	}

	// A handle whose expected digest describes a different mutation is UNRESOLVED, not a
	// match. This is the assertion that would fail if a handle were ever built by reading
	// the receipt it is meant to check.
	wrong := *h
	wrong.digest = "not-the-digest-that-was-committed"
	if v := e.asm.jsa.resolveTransitionHandle(&wrong); v != mqttTransitionUnresolved {
		t.Fatalf("a handle expecting a different mutation resolved %v, want UNRESOLVED", v)
	}

	// And an attempt that was never written is unresolved from a handle too: absence is
	// never a negative fact, whichever resolver asks.
	absent := e.firstReservation("txn-never")
	ah, err := mqttNewTransitionHandle(absent)
	if err != nil {
		t.Fatalf("freezing the second handle failed: %v", err)
	}
	if v := e.asm.jsa.resolveTransitionHandle(ah); v != mqttTransitionUnresolved {
		t.Fatalf("an unwritten attempt resolved %v from its handle, want UNRESOLVED", v)
	}
}

// TestF105HandleCannotBeBuiltFromAReceipt pins the provenance rule: the expected digest must
// never come from the outcome being checked.
//
// There is deliberately no constructor from an outcome, so this asserts the property
// structurally — the only constructor takes the frozen transition, and its digest is
// computed from the mutation rather than read from the store. A handle built from a receipt
// would agree with every receipt, including one this broker never authored.
func TestF105HandleCannotBeBuiltFromAReceipt(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-provenance")
	defer done()

	tr := e.firstReservation("txn-1")
	h, err := mqttNewTransitionHandle(tr)
	if err != nil {
		t.Fatalf("freezing the handle failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("the transition was refused: %v", err)
	}

	o, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, "txn-1")
	if st != mqttOutboundValid {
		t.Fatalf("the receipt does not load back (status %v)", st)
	}
	if h.digest != o.Digest {
		t.Fatalf("the independently computed digest disagrees with the committed receipt")
	}
	// The digest is derived from the MUTATION: a handle for a materially different
	// transition must not match this receipt, which is what makes the comparison mean
	// anything at all.
	other := e.firstReservation("txn-1")
	other.Extra = []mqttTransitionMember{{
		subj:   mqttInflightMarkerSubject(getHash(e.id), testF105Gen, 4),
		expect: 0,
		body:   []byte(`{"nonce":"n-other"}`),
	}}
	oh, err := mqttNewTransitionHandle(other)
	if err != nil {
		t.Fatalf("freezing the comparison handle failed: %v", err)
	}
	if oh.digest == o.Digest {
		t.Fatalf("a handle for a different mutation carries the committed receipt's digest")
	}
	if v := e.asm.jsa.resolveTransitionHandle(oh); v != mqttTransitionUnresolved {
		t.Fatalf("a handle for a different mutation resolved %v, want UNRESOLVED", v)
	}
}

// TestF105ExchangeAdmissionIsAtomicWithItsAuthority is the admission boundary, against the
// store.
//
// An exchange installed without a winning authority transition would be an obligation
// nothing authorized; an authority advanced without its exchange landing would be an
// authority that admitted nothing. They commit together or not at all, and the test proves
// the second half by losing the race deliberately.
func TestF105ExchangeAdmissionIsAtomicWithItsAuthority(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-admit")
	defer done()

	auth, seq := e.bootstrap(t, "txn-1")

	xch := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: "xch-1",
		PI: 7, QoS: 2, Phase: mqttExchangePublish,
		AdmitTxn: "txn-x1", Tenure: auth.Tenure,
	}
	tr, err := testF105AdmitExchangeTr(t, e, auth, seq, "txn-x1", "op-x1", xch)
	if err != nil {
		t.Fatalf("assembling the admission failed: %v", err)
	}
	xseq, err := e.asm.jsa.commitAuthorityTransition(tr)
	if err != nil {
		t.Fatalf("the exchange admission was refused: %v", err)
	}

	// Both halves landed.
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttExchangeSubject(getHash(e.id), testF105Gen, "xch-1"))
	if err != nil {
		t.Fatalf("the admitted exchange does not load back: %v", err)
	}
	got, derr := mqttDecodeOutboundExchange(smsg.Data, e.id, e.epoch, testF105Gen, "xch-1")
	if derr != nil {
		t.Fatalf("the admitted exchange does not decode: %v", derr)
	}
	if got.AdmitTxn != "txn-x1" || got.PI != 7 {
		t.Fatalf("the admitted exchange carries %q/%d, want txn-x1/7", got.AdmitTxn, got.PI)
	}
	if al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); al.status != mqttOutboundValid ||
		al.rec.Rev != auth.Rev+1 {
		t.Fatalf("the authority did not advance with the admission (status %v)", al.status)
	}

	// A SECOND admission against the STALE authority must fail, and must install nothing.
	// This is the half that matters: the exchange member would succeed on its own subject,
	// so only the authority's expectation stops it.
	stale := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: "xch-stale",
		PI: 8, QoS: 1, Phase: mqttExchangePublish,
		AdmitTxn: "txn-x2", Tenure: auth.Tenure,
	}
	str, err := testF105AdmitExchangeTr(t, e, auth, seq, "txn-x2", "op-x2", stale)
	if err != nil {
		t.Fatalf("assembling the stale admission failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(str); err == nil {
		t.Fatalf("an exchange was admitted against a superseded authority")
	}
	if _, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttExchangeSubject(getHash(e.id), testF105Gen, "xch-stale")); lerr == nil {
		t.Fatalf("the refused admission still installed its exchange: the obligation exists " +
			"and no authority transition admitted it")
	}

	// An ACTION for that exchange, carrying the ORIGINAL connection binding.
	bind := mqttBindConnection(99)
	act := &mqttOutboundAction{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: "xch-1", Acq: 1,
		Kind: mqttActionPublish, PI: 7,
		Run: bind.run, Conn: bind.conn, Tenure: auth.Tenure, AcqTxn: "txn-a1",
	}
	atr, err := mqttAcquireActionTransition(tr.Auth, xseq, "txn-a1", "op-a1", act)
	if err != nil {
		t.Fatalf("assembling the acquisition failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(atr); err != nil {
		t.Fatalf("the action acquisition was refused: %v", err)
	}
	amsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttActionSubject(getHash(e.id), testF105Gen, "xch-1", 1))
	if err != nil {
		t.Fatalf("the acquired action does not load back: %v", err)
	}
	ga, derr := mqttDecodeOutboundAction(amsg.Data, e.id, e.epoch, testF105Gen, "xch-1", 1)
	if derr != nil {
		t.Fatalf("the acquired action does not decode: %v", derr)
	}
	if !mqttActionBoundTo(ga, bind) {
		t.Fatalf("the acquired action is not bound to the connection that acquired it")
	}
	if mqttActionBoundTo(ga, mqttBindConnection(100)) {
		t.Fatalf("the acquired action claims to be bound to a different connection")
	}
}

// TestF105ObligationMembersAreCheckedAgainstTheirTransition pins that carrying an obligation
// is not the same as carrying anything: the body must agree with the subject that names it,
// and both must belong to the transition doing the carrying.
func TestF105ObligationMembersAreCheckedAgainstTheirTransition(t *testing.T) {
	jsa := &mqttJSA{}
	idHash := getHash(testF105ID)

	good := func() *mqttOutboundExchange {
		return &mqttOutboundExchange{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Nonce: "xch-1",
			PI: 7, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: "txn-1", Tenure: "tenure-0",
		}
	}

	for name, mutate := range map[string]func(*mqttOutboundExchange, *mqttTransitionMember){
		"a body naming another client": func(x *mqttOutboundExchange, _ *mqttTransitionMember) {
			x.ID = "client-b"
		},
		"a body naming another generation": func(x *mqttOutboundExchange, _ *mqttTransitionMember) {
			x.Gen = testF105Gen2
		},
		"a body whose nonce disagrees with its subject": func(x *mqttOutboundExchange, _ *mqttTransitionMember) {
			x.Nonce = "xch-other"
		},
		"a subject for another client": func(_ *mqttOutboundExchange, m *mqttTransitionMember) {
			m.subj = mqttExchangeSubject("zzzz9999", testF105Gen, "xch-1")
		},
		"a subject for another generation": func(_ *mqttOutboundExchange, m *mqttTransitionMember) {
			m.subj = mqttExchangeSubject(idHash, testF105Gen2, "xch-1")
		},
	} {
		x := good()
		tr := testF105Transition()
		m := mqttTransitionMember{subj: mqttExchangeSubject(idHash, testF105Gen, "xch-1"), expect: 0}
		mutate(x, &m)
		b, _ := json.Marshal(x)
		m.body = b
		tr.Extra = []mqttTransitionMember{m}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a transition carrying %s was sent", name)
		}
	}

	// AN ACTION MUST NAME ITS ORIGINATING CONNECTION. Without it the action has no
	// completion route that survives a rebind, which is the entire reason the binding is
	// carried on the record rather than looked up from the session.
	for name, mutate := range map[string]func(*mqttOutboundAction){
		"no broker run": func(ac *mqttOutboundAction) { ac.Run = _EMPTY_ },
		"no connection": func(ac *mqttOutboundAction) { ac.Conn = _EMPTY_ },
		"neither":       func(ac *mqttOutboundAction) { ac.Run, ac.Conn = _EMPTY_, _EMPTY_ },
	} {
		ac := &mqttOutboundAction{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Nonce: "xch-1", Acq: 1,
			Kind: mqttActionPublish, PI: 7,
			Run: "run-1", Conn: "1", Tenure: "tenure-0", AcqTxn: "txn-1",
		}
		mutate(ac)
		ab, _ := json.Marshal(ac)
		atr := testF105Transition()
		atr.Extra = []mqttTransitionMember{{
			subj: mqttActionSubject(idHash, testF105Gen, "xch-1", 1), expect: 0, body: ab,
		}}
		if _, err := jsa.commitAuthorityTransition(atr); err == nil {
			t.Fatalf("an action with %s was acquired: it has no completion route that "+
				"survives a resident reconnect", name)
		}
	}

	// A QoS 1 exchange cannot stand in a PUBREL-side phase: that describes an exchange
	// which cannot exist, and admitting it would give recovery an obligation with no route.
	x := good()
	x.QoS, x.Phase = 1, mqttExchangePubRelOwed
	b, _ := json.Marshal(x)
	tr := testF105Transition()
	tr.Extra = []mqttTransitionMember{{
		subj: mqttExchangeSubject(idHash, testF105Gen, "xch-1"), expect: 0, body: b,
	}}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a QoS 1 exchange in a PUBREL phase was admitted")
	}
}

// TestF105FirstCreationReservesBeforeItServes closes the bootstrap gap Codex's consult 8
// named.
//
// Virgin creation had no representation: a first admission installs SERVING with no
// predecessor, while a prepared handoff requires a FULL one — predecessor generation, tenure,
// ownership transition, session write identity and a nonzero session sequence. So a client
// with no prior ownership could only be claimed by serving before its metadata committed, or
// by manufacturing a predecessor from session metadata. The second is forbidden: missing
// metadata is not evidence that a creation is first.
//
// The reservation is the shape that was missing. It grants NO serving permission, it is
// create-only so exactly one claimant holds it, and it becomes servable only by a
// finalization bound to its own proposal.
func TestF105FirstCreationReservesBeforeItServes(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-create")
	defer done()

	creation := &mqttCreationIntent{ID: "create-1", Gen: testF105Gen, Tenure: "tenure-new"}
	reserve := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-res", Op: "op-res",
			Kind: mqttTransitionReserveCreation, Rev: 1,
			Gen: creation.Gen, Tenure: creation.Tenure, OwnerTxn: "txn-res",
		},
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: creation.Gen, Tenure: creation.Tenure,
			State: mqttAuthorityReserving, Rev: 1, Txn: "txn-res", OwnerTxn: "txn-res",
			Creation: creation,
		},
	}
	resSeq, err := e.asm.jsa.commitAuthorityTransition(reserve)
	if err != nil {
		t.Fatalf("the creation reservation was refused: %v", err)
	}

	// A RESERVATION SERVES NOTHING. An admission under it must be refused, because the
	// metadata half has not committed and nothing may be owed to a client whose ownership
	// is not yet established.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityReserving {
		t.Fatalf("the reservation does not load back as reserving (status %v)", al.status)
	}
	admit := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-early", Op: "op-early",
			Kind: mqttTransitionAdmitExchange, PrevTxn: "txn-res", Rev: 2,
			Gen: creation.Gen, Tenure: creation.Tenure, OwnerTxn: "txn-res",
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: creation.Gen, Tenure: creation.Tenure,
			State: mqttAuthorityServing, Rev: 2, Txn: "txn-early", OwnerTxn: "txn-res",
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(admit); err == nil {
		t.Fatalf("an obligation was admitted under a RESERVATION, before its metadata half committed")
	}

	// A SECOND claimant cannot reserve the same client: the reservation is create-only.
	rival := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-rival", Op: "op-rival",
			Kind: mqttTransitionReserveCreation, Rev: 1,
			Gen: testF105Gen2, Tenure: "tenure-rival", OwnerTxn: "txn-rival",
		},
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen2, Tenure: "tenure-rival",
			State: mqttAuthorityReserving, Rev: 1, Txn: "txn-rival", OwnerTxn: "txn-rival",
			Creation: &mqttCreationIntent{ID: "create-2", Gen: testF105Gen2, Tenure: "tenure-rival"},
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(rival); err == nil {
		t.Fatalf("two claimants both reserved one client's first creation")
	}

	// Finalization installs exactly the proposal, and only under its own name.
	wrong := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-fin", Op: "op-fin",
			Kind: mqttTransitionFinalizeCreation, Intent: "create-other",
			PrevTxn: "txn-res", Rev: 2,
			Gen: creation.Gen, Tenure: creation.Tenure, OwnerTxn: "txn-res",
		},
		Prev: reserve.Auth, PrevSeq: resSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: creation.Gen, Tenure: creation.Tenure,
			State: mqttAuthorityServing, Rev: 2, Txn: "txn-fin", OwnerTxn: "txn-res",
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(wrong); err == nil {
		t.Fatalf("a finalization answering another proposal was admitted: a second claimant " +
			"could finalize somebody else's reservation into its own tenure")
	}

	fin := *wrong
	fa := *wrong.Attempt
	fa.Intent = creation.ID
	fin.Attempt = &fa
	// NOTHING IS SERVED BEFORE THE METADATA HALF COMMITS, and that is checked at the write
	// boundary rather than left to the caller's own decision function. The finalization
	// naming the right proposal is still refused until the permanent verdict stands.
	if _, err := e.asm.jsa.commitAuthorityTransition(&fin); err == nil {
		t.Fatalf("a reservation was finalized into a serving authority while its metadata " +
			"half was unresolved")
	}
	cp, cerr := mqttProposalFromCreation(creation)
	if cerr != nil {
		t.Fatalf("viewing the reservation as a proposal failed: %v", cerr)
	}
	mf := testF105FreezeProposal(t, e, cp, testF105RecordSeq(t, e))
	if _, merr := e.asm.jsa.commitMetadataAttempt(mf); merr != nil {
		t.Fatalf("the reservation's metadata commit was refused: %v", merr)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(&fin); err != nil {
		t.Fatalf("the creation finalization was refused: %v", err)
	}
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the finalized creation is not serving (status %v)", al.status)
	}
	if al.rec.Gen != creation.Gen || al.rec.Tenure != creation.Tenure {
		t.Fatalf("the finalization installed %q/%q rather than the proposed %q/%q",
			al.rec.Gen, al.rec.Tenure, creation.Gen, creation.Tenure)
	}
	if al.rec.Creation != nil {
		t.Fatalf("the finalized authority still carries its creation proposal")
	}
}

// TestF105AbandonedReservationIsClosedNotDeleted pins the recoverable abort.
//
// No arbitration record is ever deleted, so an abandoned reservation is CLOSED: its
// generation never served and never can, and a later attempt reserves a FRESH generation
// beside it. Deleting it instead would make the next claimant's create-only reservation
// succeed against an absence, which is the inference this mechanism exists to remove.
func TestF105AbandonedReservationIsClosedNotDeleted(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-create-abort")
	defer done()

	creation := &mqttCreationIntent{ID: "create-1", Gen: testF105Gen, Tenure: "tenure-new"}
	reserve := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-res", Op: "op-res",
			Kind: mqttTransitionReserveCreation, Rev: 1,
			Gen: creation.Gen, Tenure: creation.Tenure, OwnerTxn: "txn-res",
		},
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: creation.Gen, Tenure: creation.Tenure,
			State: mqttAuthorityReserving, Rev: 1, Txn: "txn-res", OwnerTxn: "txn-res",
			Creation: creation,
		},
	}
	resSeq, err := e.asm.jsa.commitAuthorityTransition(reserve)
	if err != nil {
		t.Fatalf("the creation reservation was refused: %v", err)
	}

	abandon := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-abandon", Op: "op-abandon",
			Kind: mqttTransitionCloseAdmit, PrevTxn: "txn-res", Rev: 2,
			Gen: creation.Gen, Tenure: creation.Tenure, OwnerTxn: "txn-res",
		},
		Prev: reserve.Auth, PrevSeq: resSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: creation.Gen, Tenure: creation.Tenure,
			State: mqttAuthorityClosed, Rev: 2, Txn: "txn-abandon", OwnerTxn: "txn-res",
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: creation.Gen, Txn: "txn-abandon",
		},
	}
	// ABANDONMENT ALSO FOLLOWS THE PERMANENT VERDICT. A reservation whose metadata half is
	// merely unresolved may not be closed: its batch can still commit, and closing it would
	// end a generation that is about to become the serving one.
	if _, err := e.asm.jsa.commitAuthorityTransition(abandon); err == nil {
		t.Fatalf("a reservation was abandoned while its metadata half was unresolved")
	}
	cp, cerr := mqttProposalFromCreation(creation)
	if cerr != nil {
		t.Fatalf("viewing the reservation as a proposal failed: %v", cerr)
	}
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, cp); v != mqttMetadataAborted {
		t.Fatalf("settling the abandoned reservation returned %v, want ABORTED", v)
	}
	closeSeq, err := e.asm.jsa.commitAuthorityTransition(abandon)
	if err != nil {
		t.Fatalf("abandoning the reservation was refused: %v", err)
	}
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, creation.Gen); st != mqttOutboundValid {
		t.Fatalf("the abandoned generation has no permanent closure (status %v)", st)
	}

	// A later attempt claims a FRESH generation beside it, by reserving again — the same
	// route as the first claim, never a shortcut that installs serving over the closure. It
	// may not reuse the abandoned generation, which is closed permanently.
	reuse := testF105ReserveCreation(e, abandon.Auth, closeSeq, "txn-reuse", creation.Gen,
		"tenure-2", "intent-reuse")
	if _, err := e.asm.jsa.commitAuthorityTransition(reuse); err == nil {
		t.Fatalf("a later attempt reused the abandoned generation")
	}
	e.claim(t, abandon.Auth, closeSeq, "txn-fresh", testF105Gen2, "tenure-2")
	// The abandoned generation's closure still stands.
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, creation.Gen); st != mqttOutboundValid {
		t.Fatalf("the abandoned generation's closure did not survive (status %v)", st)
	}
}

// TestF105ReservationCannotServeOrMisdescribeItself covers the two ways a reservation could
// be subverted into serving before its metadata half commits.
//
// Both survived their first mutation run because nothing exercised them: the existing tests
// build a well-formed reservation and never try to install a serving authority under the
// reservation kind, nor a reservation whose body disagrees with the proposal it carries.
func TestF105ReservationCannotServeOrMisdescribeItself(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-create-subvert")
	defer done()

	creation := &mqttCreationIntent{ID: "create-1", Gen: testF105Gen, Tenure: "tenure-new"}

	// A reserve-creation that installs SERVING outright. This is the claim-and-serve shape
	// the reservation exists to replace: it would own the client and be immediately
	// servable, before any metadata committed. It carries no creation intent, because a
	// serving authority may not — which is exactly why the KIND must refuse it rather than
	// leaving the shape check to do the work.
	serveNow := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-serve", Op: "op-serve",
			Kind: mqttTransitionReserveCreation, Rev: 1,
			Gen: creation.Gen, Tenure: creation.Tenure, OwnerTxn: "txn-serve",
		},
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: creation.Gen, Tenure: creation.Tenure,
			State: mqttAuthorityServing, Rev: 1, Txn: "txn-serve", OwnerTxn: "txn-serve",
		},
	}
	if err := mqttCheckTransitionKindRules(serveNow); err == nil {
		t.Fatalf("a reserve-creation installed a SERVING authority: it would own the client " +
			"and be immediately servable before any metadata committed")
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(serveNow); err == nil {
		t.Fatalf("a reserve-creation installing SERVING reached the store")
	}

	// A reservation whose BODY disagrees with the proposal it carries. Finalization binds
	// to the proposal, so a body naming something else would make that binding meaningless:
	// the finalization would check one thing and install another.
	for name, mutate := range map[string]func(*mqttOutboundAuthority){
		"generation": func(a *mqttOutboundAuthority) { a.Gen = testF105Gen2 },
		"tenure":     func(a *mqttOutboundAuthority) { a.Tenure = "tenure-elsewhere" },
	} {
		c := *creation
		auth := &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: c.Gen, Tenure: c.Tenure,
			State: mqttAuthorityReserving, Rev: 1, Txn: "txn-res", OwnerTxn: "txn-res",
			Creation: &c,
		}
		mutate(auth)
		if err := mqttValidateOutboundAuthority(auth, e.id, e.epoch); err == nil {
			t.Fatalf("a reservation whose %s disagrees with its own proposal was admitted: "+
				"finalization binds to the proposal, so the binding would check one thing "+
				"and install another", name)
		}
	}

	// And a creation intent may not ride on any other state.
	for name, state := range map[string]mqttAuthorityState{
		"serving": mqttAuthorityServing,
		"closed":  mqttAuthorityClosed,
	} {
		c := *creation
		auth := &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: c.Gen, Tenure: c.Tenure,
			State: state, Rev: 1, Txn: "txn-res", OwnerTxn: "txn-res", Creation: &c,
		}
		if err := mqttValidateOutboundAuthority(auth, e.id, e.epoch); err == nil {
			t.Fatalf("a %s authority carrying a creation proposal was admitted", name)
		}
	}
}
