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
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The issue #107 arbitration against a real JetStream.
//
// Everything below drives the actual stream, because the properties being established are
// properties of the STORE and not of the Go code: that a create-only outcome member and a
// create-only abort genuinely contend for one subject, that a losing member rejects its
// whole batch, and that a committed receipt survives being superseded. A fixture that
// mocked the store would be asserting its own mock.

// testOutboundEnv is a live account session manager plus the exact identities its records are
// attributed by.
type testOutboundEnv struct {
	asm   *mqttAccountSessionManager
	id    string
	epoch string
}

func testNewEnv(t *testing.T, clientID string) (*testOutboundEnv, func()) {
	t.Helper()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	// THE RECOVERY UNIT'S ENV INSPECTS WHAT CERTIFICATION LEAVES, so the generation reclamation that
	// would purge it is held (retirement is exercised on a real server, retirement_e2e_test.go).
	asm.testHoldGenerationRetirement.Store(true)
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

	testClearMintedAuthority(t, asm, clientID, func() {
		c.Close()
		testMQTTShutdownServer(s)
	})

	return &testOutboundEnv{asm: asm, id: clientID, epoch: epoch}, func() {
		c.Close()
		testMQTTShutdownServer(s)
	}
}

// testClearMintedAuthority returns a freshly-connected client to the PRE-AUTHORITY
// substrate.
//
// A production CONNECT now mints a SERVING outbound authority (issue #107 rows 1-9), but the
// substrate tests each build their OWN authority state from a clean client — a first
// reservation, a bootstrapped serving tenure, a standing predecessor. They cannot
// opClaimClient over a subject the harness already minted. The mint's OTHER batch members —
// the acceptance fact, the COMMITTED receipt, the session record — predate the flip and are
// the state these tests have always started from, so only the authority subject is purged.
// Purge and not deleteMsg because a transition may leave more than one revision on the
// subject (a RESERVING superseded by a SERVING), and reading back the latest after deleting
// it would resurrect the predecessor.
//
// onFatal runs the caller's own teardown before t.Fatalf, since a leaked server outlives the
// test binary otherwise.
func testClearMintedAuthority(t *testing.T, asm *mqttAccountSessionManager, clientID string, onFatal func()) {
	t.Helper()
	// AND ITS LIFECYCLE IS RETIRED in the registry (issue #133): the minting CONNECT allocated the
	// client's first lifecycle, and an authority purged beside a standing active entry is a state
	// the broker refuses. Retired rather than purged, so the allocator moves on and a fixture's
	// generations (testGen, testGen2: lifecycle 2) never collide with the mint's records.
	shard := mqttLifecycleRegistryShardFor(getHash(clientID))
	if rl := asm.jsa.loadLifecycleRegistryShard(asm.sessStreamEpoch, shard); rl.status == mqttOutboundValid {
		delete(rl.rec.Active, getHash(clientID))
		body, _ := json.Marshal(rl.rec)
		msg, hdr := mqttFrameBatchWrite(rl.seq, body, _EMPTY_, 0, false)
		if _, err := asm.jsa.storeMsg(mqttLifecycleRegistrySubject(shard), hdr, msg); err != nil {
			if onFatal != nil {
				onFatal()
			}
			t.Fatalf("retiring the minted lifecycle of %q: %v", clientID, err)
		}
	}
	if err := asm.jsa.purgeStream(mqttSessStreamName, mqttOutboundAuthoritySubject(getHash(clientID)), 0, false); err != nil {
		if onFatal != nil {
			onFatal()
		}
		t.Fatalf("could not return %q to the pre-authority substrate: %v", clientID, err)
	}
}

// firstReservation builds this environment's FIRST CLAIM on its client.
//
// It is a reservation and not a serving authority, because there is no longer a kind that
// installs one outright: a claim serves nothing until its metadata half commits. Its
// OWNERSHIP IS ITS OWN IDENTITY — there is no predecessor whose ownership it could inherit,
// so the transition that claims the client is the transition that owns it.
func (e *testOutboundEnv) firstReservation(txn string) *mqttAuthorityTransition {
	// A FIRST ACCEPTANCE, composed and not sent (issue #133): the revision-1 transition a fresh
	// client's ownership starts from, record member included.
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	reg := jsa.loadLifecycleRegistryShard(e.epoch, mqttLifecycleRegistryShardFor(getHash(e.id)))
	smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(e.asm.domainTk, getHash(e.id)))
	recordExists := lerr == nil && smsg != nil
	ca, err := jsa.composeAcceptance(mqttAcceptRequest{
		ID: e.id, Epoch: e.epoch, Txn: txn, Tenure: "tenure-" + txn, Nonce: "intent-" + txn,
		At: time.Now().UnixNano(), RecordExists: recordExists,
	}, al, reg)
	if err != nil {
		panic("firstReservation: " + err.Error())
	}
	var recSeq uint64
	if recordExists {
		recSeq = smsg.Sequence
	}
	ca.T.Record = &mqttTransitionMember{
		subj: mqttSessSubject(e.asm.domainTk, getHash(e.id)), expect: recSeq,
		body: []byte(`{"id":"` + e.id + `","gen":"` + ca.Gen + `"}`),
	}
	return ca.T
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
// lifeScope is the lifecycle outcome scope of the client's standing authority: where the
// outcome of an acceptance, a bind or a teardown transition lives.
func (e *testOutboundEnv) lifeScope(t *testing.T) string {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("fixture: the authority reads %v, so it names no lifecycle scope", al.status)
	}
	return mqttLifeScope(al.rec.Life)
}

func (e *testOutboundEnv) bootstrap(t *testing.T, txn string) (*mqttOutboundAuthority, uint64) {
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
func (e *testOutboundEnv) bootstrapTr(t *testing.T, txn string) (*mqttAuthorityTransition, uint64) {
	t.Helper()
	return e.claimTr(t, nil, 0, txn, testGen, "tenure-"+txn)
}

// claim is the whole route to a serving authority, over an absent predecessor or a CLOSED
// one. The post-closure case is the SAME three steps as the first claim, which is what
// deleting the readmission kind established: there is no cheaper way in on a second attempt
// than on the first.
func (e *testOutboundEnv) claim(t *testing.T, prev *mqttOutboundAuthority, prevSeq uint64,
	txn, gen, tenure string) (*mqttOutboundAuthority, uint64) {

	t.Helper()
	tr, seq := e.claimTr(t, prev, prevSeq, txn, gen, tenure)
	return tr.Auth, seq
}

// claimTr installs a SERVING authority the way an accepted CONNECT does (issue #133): one
// acceptance batch — a creation over an absent authority, a reopen over a closed one — and the
// bind that clears its pending acceptance. It returns the BIND, the transition standing at the
// authority, so a fixture that resolves or supersedes it acts on the current revision.
//
// gen is the fixture's statement of the generation it expects: the lifecycle allocates it, and
// testGen / testGen2 are a fresh client's first two (lifecycle 1, ordinals 0 and 1).
func (e *testOutboundEnv) claimTr(t *testing.T, prev *mqttOutboundAuthority, prevSeq uint64,
	txn, gen, tenure string) (*mqttAuthorityTransition, uint64) {

	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if prev != nil && (al.status != mqttOutboundValid || al.rec.Rev != prev.Rev) {
		t.Fatalf("the claim %q expects a predecessor at revision %d, but the authority reads %v", txn, prev.Rev, al.status)
	}
	return e.accept(t, txn, gen, tenure, true, mqttClosingTargets{})
}

// accept is one acceptance and its bind, of whatever shape the standing authority calls for: a
// creation, a reopen, a resume (clean false over a serving authority) or a Clean Start handoff,
// whose closing enrols the retire targets. It returns the bind and the authority's sequence.
func (e *testOutboundEnv) accept(t *testing.T, txn, gen, tenure string, clean bool,
	retire mqttClosingTargets) (*mqttAuthorityTransition, uint64) {

	t.Helper()
	return testAcceptAndBind(t, &e.asm.jsa, e.asm.domainTk, e.id, e.epoch, txn, gen, tenure, clean, retire)
}

// testAdmitNothing is an admission that carries no obligation of its own.
//
// It exists because several fixtures need a transition that ADVANCES the authority's
// revision under the standing tenure and nothing else — to be resolved, to be superseded, or
// to carry one hand-built member. The kind is the admission kind because that is what a
// revision-advancing transition under a serving tenure IS; the obligations it would normally
// carry are the caller's to add.
func testAdmitNothing(e *testOutboundEnv, prev *mqttOutboundAuthority, prevSeq uint64,
	txn string) *mqttAuthorityTransition {

	rev := prev.Rev + 1
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Scope: prev.Gen, Op: "op-" + txn,
			Kind: mqttTransitionAdmitExchange, PrevTxn: prev.OwnerTxn, Rev: rev,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityServing, Rev: rev, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Life: mqttLifeOf(prev),
		},
	}
}

// testClosure composes the close of the generation a serving authority stands at, permanently:
// the teardown claim batch over the client's standing record, which is the one act that closes
// a generation (issue #133). Composed, not sent, so a fixture can resolve it, freeze a handle
// over it or race it.
func testClosure(e *testOutboundEnv, prev *mqttOutboundAuthority, prevSeq uint64,
	txn string) *mqttAuthorityTransition {

	t, err := testComposeTeardownClaim(e.asm, e.id, prev, prevSeq, txn,
		mqttRetirementRequest{Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn}, nil)
	if err != nil {
		// A fixture that cannot compose its own close would otherwise fail later, at the
		// boundary, with a message about the rule rather than about the fixture.
		panic("testClosure: " + err.Error())
	}
	return t
}

// TestAbortAndCommitContendForOneSubject is the ambiguity protocol, against the store.
//
// The whole design rests on a claim about JetStream: a create-only member inside an atomic
// batch and a standalone create-only publish to the same subject are mutually exclusive, and
// the loser's batch is rejected in its entirety. If that were false, an abort could stand
// beside a committed transition and "never landed" would be a lie.
func TestAbortAndCommitContendForOneSubject(t *testing.T) {
	e, done := testNewEnv(t, "f105-contend")
	defer done()
	e.bootstrap(t, "txn-1")
	current := func() (*mqttOutboundAuthority, uint64) {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		if al.status != mqttOutboundValid {
			t.Fatalf("the authority reads %v", al.status)
		}
		return al.rec, al.seq
	}

	// ABORT FIRST: the commit that follows must be refused, and nothing it carried may
	// stand — including the authority, which is the member that matters. The attempt is
	// composed over the authority as the abort left it, so only the outcome subject contends.
	prev, prevSeq := current()
	if err := e.asm.jsa.abortTransition(testAdmitNothing(e, prev, prevSeq, "txn-abort-first").Attempt, nil); err != nil {
		t.Fatalf("the abort was refused: %v", err)
	}
	prev, prevSeq = current()
	tr := testAdmitNothing(e, prev, prevSeq, "txn-abort-first")
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a transition committed after its own attempt was positively aborted")
	}
	if after, _ := current(); after.Rev != prev.Rev || after.Txn != prev.Txn {
		t.Fatalf("the refused batch moved the authority to %d/%q: the losing member must reject "+
			"the whole batch", after.Rev, after.Txn)
	}
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionNeverLanded {
		t.Fatalf("an aborted attempt resolved %v, want NEVER LANDED", v)
	}

	// COMMIT FIRST: the abort that follows must fail, and the receipt must still say
	// COMMITTED.
	prev, prevSeq = current()
	tr2 := testAdmitNothing(e, prev, prevSeq, "txn-commit-first")
	seq, err := e.asm.jsa.commitAuthorityTransition(tr2)
	if err != nil {
		t.Fatalf("the transition was refused: %v", err)
	}
	if seq == 0 {
		t.Fatalf("the commit returned no authority sequence")
	}
	if err := e.asm.jsa.abortTransition(tr2.Attempt, nil); err == nil {
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
	if al.rec.Txn != "txn-commit-first" {
		t.Fatalf("the authority names transition %q, want txn-commit-first", al.rec.Txn)
	}
}

// TestCommittedOutcomeSurvivesSupersession pins the distinction a lost acknowledgement
// depends on: an attempt that landed and was later superseded is NOT an attempt that never
// landed. Its exchange mutations are real and its obligations exist; only the ownership it
// installed has moved on. Collapsing the two would strand everything the first transition
// created.
func TestCommittedOutcomeSurvivesSupersession(t *testing.T) {
	e, done := testNewEnv(t, "f105-supersede")
	defer done()

	first, seq := e.bootstrapTr(t, "txn-1")
	if v := e.asm.jsa.resolveTransition(first); v != mqttTransitionLanded {
		t.Fatalf("the finalization resolved %v, want LANDED", v)
	}

	// A second transition over it: an exchange admitted under the same tenure, which
	// advances the revision without moving ownership.
	second := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-2", Scope: testGen, Op: "op-2",
			Kind: mqttTransitionAdmitExchange, PrevTxn: "txn-1", Rev: first.Auth.Rev + 1,
			Gen: testGen, Tenure: first.Auth.Tenure, OwnerTxn: "txn-1",
		},
		Prev: first.Auth, PrevSeq: seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testGen, Tenure: first.Auth.Tenure,
			State: mqttAuthorityServing, Rev: first.Auth.Rev + 1, Txn: "txn-2", OwnerTxn: "txn-1",
			Life: mqttLifeOf(first.Auth),
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, second)); err != nil {
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
	o, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, e.lifeScope(t), "txn-1")
	if st != mqttOutboundValid || o.Verdict != mqttOutcomeCommitted {
		t.Fatalf("the superseded attempt's receipt reads back status=%v verdict=%v", st, o.Verdict)
	}
}

// TestResolutionRefusesAReceiptForAnotherMutation pins the digest check. A receipt found
// at a transition id proves only that SOME batch under that id committed; if the caller's
// assembled mutation differs, that is a contradiction and must not be reported as its
// answer.
func TestResolutionRefusesAReceiptForAnotherMutation(t *testing.T) {
	e, done := testNewEnv(t, "f105-digest")
	defer done()

	auth, seq := e.bootstrap(t, "txn-1")
	tr := testAdmitNothing(e, auth, seq, "txn-d")
	// An invalid body would make this test pass through the shape validator rather than
	// through the digest, which is not the property being established.
	//
	// TWO VALID ADMISSIONS, differing only in their exchange bodies. Each carries its register
	// and marker, because an exchange is admitted only with its register and a marker or a
	// register alone is refused; all of those are reasons that have nothing to do with the
	// digest.
	xchBody := func(nonce string) []byte {
		b, merr := json.Marshal(&mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: nonce,
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
	slots := func(nonce string) []mqttTransitionMember {
		return testSlotMembersFor(t, &e.asm.jsa, auth, seq, &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: nonce,
			PI: 7, QoS: 2, Phase: mqttExchangePublish, AdmitTxn: "txn-d", Tenure: auth.Tenure,
		})
	}
	tr.Extra = append(testCoverExchanges(t, e, testGen, []mqttTransitionMember{{
		subj:   mqttExchangeSubject(getHash(e.id), testGen, nonceA),
		expect: 0,
		body:   markerA,
	}}), slots(nonceA)...)
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, tr)); err != nil {
		t.Fatalf("the transition was refused: %v", err)
	}
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionLanded {
		t.Fatalf("the committed transition resolved %v, want LANDED", v)
	}

	// The same attempt, resolved against a DIFFERENT assembled mutation. Every field of
	// the outcome still matches; only the member's body differs.
	other := *tr
	other.Extra = append([]mqttTransitionMember{{
		subj:   mqttExchangeSubject(getHash(e.id), testGen, nonceB),
		expect: 0,
		body:   markerB,
	}}, slots(nonceB)...)
	if v := e.asm.jsa.resolveTransition(&other); v != mqttTransitionUnresolved {
		t.Fatalf("a receipt for a different mutation resolved %v, want UNRESOLVED", v)
	}
}

// TestClosureLoadsBackAndAbsenceIsNotOpen pins both halves of the closure contract: a
// close-admission transition writes the permanent record with its authority in one batch,
// and a generation with no closure record is NOT thereby established as open — open
// admission requires the authority's positive state.
func TestClosureLoadsBackAndAbsenceIsNotOpen(t *testing.T) {
	e, done := testNewEnv(t, "f105-closure")
	defer done()

	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testGen); st != mqttOutboundAbsent {
		t.Fatalf("an unclosed generation reports closure status %v, want ABSENT", st)
	}

	auth, seq := e.bootstrap(t, "txn-1")

	closing := testClosure(e, auth, seq, "txn-close")
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, closing)); err != nil {
		t.Fatalf("the close-admission transition was refused: %v", err)
	}

	c, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testGen)
	if st != mqttOutboundValid {
		t.Fatalf("the permanent closure does not load back (status %v)", st)
	}
	if c.Txn != "txn-close" {
		t.Fatalf("the closure names transition %q, want txn-close", c.Txn)
	}
	// Another generation is still absent, and absence establishes nothing about it.
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testGen2); st != mqttOutboundAbsent {
		t.Fatalf("an unrelated generation reports closure status %v, want ABSENT", st)
	}

	// ADMISSION AFTER CLOSURE IS REFUSED, against the real standing authority.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityClosed {
		t.Fatalf("the authority did not become closed (status %v)", al.status)
	}
	admit := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-late", Scope: testGen, Op: "op-late",
			Kind: mqttTransitionAdmitExchange, PrevTxn: "txn-1", Rev: al.rec.Rev + 1,
			Gen: testGen, Tenure: auth.Tenure, OwnerTxn: "txn-1",
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testGen, Tenure: auth.Tenure,
			State: mqttAuthorityServing, Rev: al.rec.Rev + 1, Txn: "txn-late", OwnerTxn: "txn-1",
			Life: mqttLifeOf(al.rec),
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, admit)); err == nil {
		t.Fatalf("an exchange was admitted over a CLOSED authority")
	}
}

// TestUnresolvedIsNotNeverLanded pins the honest answer. An attempt whose outcome has
// not been written is UNRESOLVED, never "never landed": the batch may still be in flight and
// may still commit, and a caller that treated absence as a negative fact would be the
// release-on-absence shape under another name.
func TestUnresolvedIsNotNeverLanded(t *testing.T) {
	e, done := testNewEnv(t, "f105-unresolved")
	defer done()

	base, baseSeq := e.bootstrap(t, "txn-1")
	tr := testAdmitNothing(e, base, baseSeq, "txn-never-sent")
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionUnresolved {
		t.Fatalf("an attempt with no outcome resolved %v, want UNRESOLVED", v)
	}
	// Settling it positively is what turns it into a fact.
	if err := e.asm.jsa.abortTransition(tr.Attempt, nil); err != nil {
		t.Fatalf("the abort was refused: %v", err)
	}
	if v := e.asm.jsa.resolveTransition(tr); v != mqttTransitionNeverLanded {
		t.Fatalf("a positively aborted attempt resolved %v, want NEVER LANDED", v)
	}
}

// TestAnUnreadableOutcomeIsNotNeverLanded is the INDETERMINATE half of the pin above
// (issue #107 WP-F W2, row M10). A transition X that COMMITTED, and was then superseded by two later
// revisions, has an outcome that cannot be read for a reason other than its absence. The read
// failed, so nothing was established: X is UNRESOLVED from its handle, never "never landed" -
// a caller that took the unreadable receipt as an abort would re-issue an obligation X already
// created. Mutation (M10): the classifier answers NeverLanded for an indeterminate outcome read.
func TestAnUnreadableOutcomeIsNotNeverLanded(t *testing.T) {
	e, done := testNewEnv(t, "f105-unreadable-outcome")
	defer done()

	base, baseSeq := e.bootstrap(t, "txn-1")
	x := testAdmitNothing(e, base, baseSeq, "txn-x")
	h, err := mqttNewTransitionHandle(x)
	if err != nil {
		t.Fatalf("freezing X's handle failed: %v", err)
	}
	prevSeq, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, x))
	if err != nil {
		t.Fatalf("X was refused: %v", err)
	}
	prevAuth := x.Auth
	// TWO MORE AUTHORITY REVISIONS land over X.
	for _, txn := range []string{"txn-x2", "txn-x3"} {
		next := testAdmitNothing(e, prevAuth, prevSeq, txn)
		if prevSeq, err = e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, next)); err != nil {
			t.Fatalf("successor %s was refused: %v", txn, err)
		}
		prevAuth = next.Auth
	}
	// Control: readable, X is SUPERSEDED.
	if v := e.asm.jsa.resolveTransitionHandle(h); v != mqttTransitionSuperseded {
		t.Fatalf("X resolved %v after two successors, want SUPERSEDED", v)
	}

	// X's outcome subject - and only it - fails to read with an error that is not "no message".
	hidden := mqttTransitionOutcomeSubject(getHash(e.id), x.Attempt.Scope, "txn-x")
	var hits atomic.Int64
	restore := setMQTTTestLoadLastMsgHook(func(_, subject string) error {
		if subject == hidden {
			hits.Add(1)
			return errors.New("test: the outcome read failed")
		}
		return nil
	})
	t.Cleanup(restore)
	// ROUTE WITNESS: the read the classifier makes is INDETERMINATE, not absent.
	if _, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, x.Attempt.Scope, "txn-x"); st != mqttOutboundIndeterminate {
		t.Fatalf("the hidden outcome reads back status %v, want INDETERMINATE", st)
	}
	before := hits.Load()
	// TARGET.
	if v := e.asm.jsa.resolveTransitionHandle(h); v != mqttTransitionUnresolved {
		t.Fatalf("a committed X whose outcome could not be read resolved %v, want UNRESOLVED", v)
	}
	if hits.Load() == before {
		t.Fatal("fixture: the classifier never read X's outcome subject")
	}
	restore()
	if v := e.asm.jsa.resolveTransitionHandle(h); v != mqttTransitionSuperseded {
		t.Fatalf("X resolved %v once its outcome was readable again, want SUPERSEDED", v)
	}
}

// TestSweepSkipsArbitrationRecords pins that the orphan sweep neither sweeps nor
// mis-logs issue #107's permanent records, and does not spend its bounded budget on them.
//
// They share a stream with the registers because a batch is per stream, so the sweep
// walks straight over them. Before this they were retained as "unattributable registers":
// true in the sense that nothing could attribute them to a register, false as a description,
// and each one consumed a record from a pass whose whole point is to be bounded.
func TestSweepSkipsArbitrationRecords(t *testing.T) {
	e, done := testNewEnv(t, "f105-sweep")
	defer done()

	// Land an authority, a permanent outcome and a permanent closure.
	auth, seq := e.bootstrap(t, "txn-1")
	closing := testClosure(e, auth, seq, "txn-close")
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, closing)); err != nil {
		t.Fatalf("the close-admission was refused: %v", err)
	}

	// A register behind them, so the pass has something real to find.
	regSubj := mqttInflightSubject(getHash(e.id), testGen, 9)
	if _, err := e.asm.jsa.storeInflightCAS(regSubj, 0, testSweepRegister(t, e)); err != nil {
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

	cands, markers, _, err := e.asm.collectInflightCandidates(e.asm.jsa.c.srv)
	if err != nil {
		t.Fatalf("the candidate collection failed: %v", err)
	}
	for _, c := range cands {
		if mqttIsOutboundRecordSubject(c.name.subject) {
			t.Fatalf("the sweep collected the arbitration record %q as a register candidate", c.name.subject)
		}
	}
	for _, m := range markers {
		if mqttIsOutboundRecordSubject(m.name.subject) {
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
		if strings.Contains(m, "unattributable in-flight register") && strings.Contains(m, mqttOutboundRecordClassTk) {
			t.Fatalf("the sweep warned that an arbitration record is an unattributable "+
				"in-flight register: %q", m)
		}
	}

	// And every record it DID land is still readable: the sweep walked over them without
	// touching them.
	if al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); al.status != mqttOutboundValid {
		t.Fatalf("the authority did not survive the sweep (status %v)", al.status)
	}
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testGen); st != mqttOutboundValid {
		t.Fatalf("the permanent closure did not survive the sweep (status %v)", st)
	}
	if _, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, e.lifeScope(t), "txn-1"); st != mqttOutboundValid {
		t.Fatalf("the permanent outcome did not survive the sweep (status %v)", st)
	}
}

// testSweepRegister is a minimal supported register body for the sweep fixture. It is
// built through the same shape the decode API accepts, so the plant cannot pass for a
// reason the production writer would not.
func testSweepRegister(t *testing.T, e *testOutboundEnv) *mqttInflightRecord {
	t.Helper()
	return freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 9, Dur: "$MQTT_sweep", Seq: 1, QoS: 1,
		Nonce: mqttNewInflightNonce(),
	})
}

// TestSweepStaysBoundedOverArbitrationRecords pins that the skip charges the record
// budget.
//
// An earlier version of the skip refunded it, on the reasoning that a client's permanent
// outcomes should not crowd the pass out of its registers. That was wrong and this test
// exists because of it: the budget is the ONLY thing bounding the pass, so a record class
// that costs nothing to walk makes a stream full of permanent outcomes an unbounded scan.
// The cost is real — a client with many outcomes reaches its registers over more passes —
// and it is the correct trade, because a bounded pass that makes progress beats an
// unbounded one that must finish.
func TestSweepStaysBoundedOverArbitrationRecords(t *testing.T) {
	e, done := testNewEnv(t, "f105-bounded")
	defer done()

	// Several transitions, each leaving a permanent outcome ahead of the register.
	prevAuth, seq := e.bootstrap(t, "txn-1")
	var err error
	for i := 2; i <= 5; i++ {
		txn := "txn-" + string(rune('0'+i))
		next := testAdmitNothing(e, prevAuth, seq, txn)
		if seq, err = e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, next)); err != nil {
			t.Fatalf("transition %s was refused: %v", txn, err)
		}
		prevAuth = next.Auth
	}

	regSubj := mqttInflightSubject(getHash(e.id), testGen, 9)
	if _, err := e.asm.jsa.storeInflightCAS(regSubj, 0, testSweepRegister(t, e)); err != nil {
		t.Fatalf("planting the register behind the arbitration records failed: %v", err)
	}

	limit := 2
	e.asm.mu.Lock()
	e.asm.testQoS2SweepRecordLimit = &limit
	e.asm.mu.Unlock()

	cands, _, _, err := e.asm.collectInflightCandidates(e.asm.jsa.c.srv)
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

// TestHandleClassifiesWithoutTheOriginalInputs pins the classification boundary Codex's
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
func TestHandleClassifiesWithoutTheOriginalInputs(t *testing.T) {
	e, done := testNewEnv(t, "f105-handle")
	defer done()

	base, baseSeq := e.bootstrap(t, "txn-1")
	first := testAdmitNothing(e, base, baseSeq, "txn-h")
	h, err := mqttNewTransitionHandle(first)
	if err != nil {
		t.Fatalf("freezing the handle failed: %v", err)
	}
	seq, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, first))
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
		next := testAdmitNothing(e, prevAuth, prevSeq, txn)
		if prevSeq, err = e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, next)); err != nil {
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

// TestHandleCannotBeBuiltFromAReceipt pins the provenance rule: the expected digest must
// never come from the outcome being checked.
//
// There is deliberately no constructor from an outcome, so this asserts the property
// structurally — the only constructor takes the frozen transition, and its digest is
// computed from the mutation rather than read from the store. A handle built from a receipt
// would agree with every receipt, including one this broker never authored.
func TestHandleCannotBeBuiltFromAReceipt(t *testing.T) {
	e, done := testNewEnv(t, "f105-provenance")
	defer done()

	tr := e.firstReservation("txn-1")
	h, err := mqttNewTransitionHandle(tr)
	if err != nil {
		t.Fatalf("freezing the handle failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, tr)); err != nil {
		t.Fatalf("the transition was refused: %v", err)
	}

	o, _, st := e.asm.jsa.loadTransitionOutcome(e.id, e.epoch, tr.Attempt.Scope, "txn-1")
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
		subj:   mqttInflightMarkerSubject(getHash(e.id), testGen, 4),
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

// TestExchangeAdmissionIsAtomicWithItsAuthority is the admission boundary, against the
// store.
//
// An exchange installed without a winning authority transition would be an obligation
// nothing authorized; an authority advanced without its exchange landing would be an
// authority that admitted nothing. They commit together or not at all, and the test proves
// the second half by losing the race deliberately.
func TestExchangeAdmissionIsAtomicWithItsAuthority(t *testing.T) {
	e, done := testNewEnv(t, "f105-admit")
	defer done()
	// A well-formed lineage: the acquisition installs its token on a register naming it (issue #107 row 2).
	xchNonce := mqttNewInflightNonce()

	auth, seq := e.bootstrap(t, "txn-1")

	xch := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: xchNonce,
		PI: 7, QoS: 2, Phase: mqttExchangePublish,
		AdmitTxn: "txn-x1", Tenure: auth.Tenure,
	}
	tr, err := testAdmitExchangeTr(t, e, auth, seq, "txn-x1", "op-x1", xch)
	if err != nil {
		t.Fatalf("assembling the admission failed: %v", err)
	}
	xseq, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, tr))
	if err != nil {
		t.Fatalf("the exchange admission was refused: %v", err)
	}

	// Both halves landed.
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName,
		mqttExchangeSubject(getHash(e.id), testGen, xchNonce))
	if err != nil {
		t.Fatalf("the admitted exchange does not load back: %v", err)
	}
	got, derr := mqttDecodeOutboundExchange(smsg.Data, e.id, e.epoch, testGen, xchNonce)
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
	staleNonce := mqttNewInflightNonce()
	stale := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: staleNonce,
		PI: 8, QoS: 1, Phase: mqttExchangePublish,
		AdmitTxn: "txn-x2", Tenure: auth.Tenure,
	}
	str, err := testAdmitExchangeTr(t, e, auth, seq, "txn-x2", "op-x2", stale)
	if err != nil {
		t.Fatalf("assembling the stale admission failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, str)); err == nil {
		t.Fatalf("an exchange was admitted against a superseded authority")
	}
	if _, lerr := e.asm.jsa.loadLastMsgFor(mqttSessStreamName,
		mqttExchangeSubject(getHash(e.id), testGen, staleNonce)); lerr == nil {
		t.Fatalf("the refused admission still installed its exchange: the obligation exists " +
			"and no authority transition admitted it")
	}

	// An ACTION for that exchange, carrying the ORIGINAL connection binding.
	bind := mqttBindConnection(99)
	act := &mqttOutboundAction{
		ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: xchNonce, Acq: 1,
		Kind: mqttActionPublish, PI: 7,
		Run: bind.run, Conn: bind.conn, Tenure: auth.Tenure, AcqTxn: "txn-a1",
	}
	atr, err := mqttAcquireActionsTransition(tr.Auth, xseq, "txn-a1", "op-a1", []*mqttOutboundAction{act})
	if err != nil {
		t.Fatalf("assembling the acquisition failed: %v", err)
	}
	atr.Extra = append(atr.Extra, testTokenMember(t, e, act.Gen, act))
	if _, err := e.asm.jsa.commitAuthorityTransition(testEnrolClosing(t, e, atr)); err != nil {
		t.Fatalf("the action acquisition was refused: %v", err)
	}
	amsg, err := e.asm.jsa.loadLastMsgFor(mqttSessStreamName,
		mqttActionSubject(getHash(e.id), testGen, xchNonce, 1))
	if err != nil {
		t.Fatalf("the acquired action does not load back: %v", err)
	}
	ga, derr := mqttDecodeOutboundAction(amsg.Data, e.id, e.epoch, testGen, xchNonce, 1)
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

// TestObligationMembersAreCheckedAgainstTheirTransition pins that carrying an obligation
// is not the same as carrying anything: the body must agree with the subject that names it,
// and both must belong to the transition doing the carrying.
func TestObligationMembersAreCheckedAgainstTheirTransition(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}
	idHash := getHash(testID)

	good := func() *mqttOutboundExchange {
		return &mqttOutboundExchange{
			ID: testID, Epoch: testEpoch, Gen: testGen, Nonce: "xch-1",
			PI: 7, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: "txn-1", Tenure: "tenure-0",
		}
	}

	for name, mutate := range map[string]func(*mqttOutboundExchange, *mqttTransitionMember){
		"a body naming another client": func(x *mqttOutboundExchange, _ *mqttTransitionMember) {
			x.ID = "client-b"
		},
		"a body naming another generation": func(x *mqttOutboundExchange, _ *mqttTransitionMember) {
			x.Gen = testGen2
		},
		"a body whose nonce disagrees with its subject": func(x *mqttOutboundExchange, _ *mqttTransitionMember) {
			x.Nonce = "xch-other"
		},
		"a subject for another client": func(_ *mqttOutboundExchange, m *mqttTransitionMember) {
			m.subj = mqttExchangeSubject("zzzz9999", testGen, "xch-1")
		},
		"a subject for another generation": func(_ *mqttOutboundExchange, m *mqttTransitionMember) {
			m.subj = mqttExchangeSubject(idHash, testGen2, "xch-1")
		},
	} {
		x := good()
		tr := testTransition()
		m := mqttTransitionMember{subj: mqttExchangeSubject(idHash, testGen, "xch-1"), expect: 0}
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
			ID: testID, Epoch: testEpoch, Gen: testGen, Nonce: "xch-1", Acq: 1,
			Kind: mqttActionPublish, PI: 7,
			Run: "run-1", Conn: "1", Tenure: "tenure-0", AcqTxn: "txn-1",
		}
		mutate(ac)
		ab, _ := json.Marshal(ac)
		atr := testTransition()
		atr.Extra = []mqttTransitionMember{{
			subj: mqttActionSubject(idHash, testGen, "xch-1", 1), expect: 0, body: ab,
		}}
		if _, err := jsa.commitAuthorityTransition(atr); err == nil {
			t.Fatalf("an action with %s was acquired: it has no completion route that "+
				"survives a resident reconnect", name)
		}
	}

	// A QoS 1 exchange cannot stand in a PUBREL-side phase: that describes an exchange
	// which cannot exist, and admitting it would give recovery an obligation with no route.
	x := good()
	x.QoS, x.Phase = 1, mqttExchangePubRecObserved
	b, _ := json.Marshal(x)
	tr := testTransition()
	tr.Extra = []mqttTransitionMember{{
		subj: mqttExchangeSubject(idHash, testGen, "xch-1"), expect: 0, body: b,
	}}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a QoS 1 exchange in a PUBREL phase was admitted")
	}
}
