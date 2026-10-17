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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// F-105 8b, the unit-level half: the one creator of a ledger, the installation order at both
// layers, the closing driver's licence, and the close-optimization audit. The CONNECT-level half
// is outbound_tenure_close_8b_e2e_test.go. Mutation ledger: mutation-ledger-8b.md.

// T4u: the composer refuses to install an OLDER tenure over a newer one. Mutation M-comp-order.
func TestF105_8bComposerRefusesAnOlderInstallation(t *testing.T) {
	x := testNewBoundaryEnv(t)
	x.install(t, testTenure(10))
	b, err := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(7), x.ledger(t))
	if !errors.Is(err, errMQTTSharedTenureSuperseded) {
		t.Fatalf("installing tenure 7 over 10 composed %+v, %v: want errMQTTSharedTenureSuperseded", b, err)
	}
}

// T4v: the write boundary refuses an installation that does not ADVANCE the tenure, isolated on
// EQUAL sequences (the general decreasing-sequence rule cannot fire) with a different nonce, so
// only the install-specific ordering rule can refuse it. Mutation M-val-order.
func TestF105_8bValidatorRefusesANonAdvancingInstallation(t *testing.T) {
	x := testNewBoundaryEnv(t)
	x.install(t, testTenure(10))
	b, err := mqttComposeSharedInstall(x.epoch, x.id, x.gen, testTenure(11), x.ledger(t))
	if err != nil || b == nil {
		t.Fatalf("composing a legal installation to mutate: %+v, %v", b, err)
	}
	same := mqttSharedTenure{Seq: 10, ID: mqttNewInflightNonce()}
	b.Tenure, b.LedgerNext.Tenure = same, same
	x.refused(t, "an installation at the standing sequence under another nonce", b)
	if l := x.ledger(t); l.status != mqttOutboundValid || l.rec.Tenure.Seq != 10 || l.rec.Tenure.ID == same.ID {
		t.Fatalf("the refused installation changed the ledger: %+v", l.rec)
	}
}

// T5c: a join never creates a ledger — the composer refuses one over absence. Mutation
// M-join-comp.
func TestF105_8bComposerRefusesAJoinOverAbsence(t *testing.T) {
	x := testNewBoundaryEnv(t)
	b, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, mqttNewInflightNonce(), testTenure(3),
		mqttSharedGroupLoad{status: mqttOutboundAbsent}, mqttSharedLedgerLoad{status: mqttOutboundAbsent},
		mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending))
	if !errors.Is(err, errMQTTSharedJoinWithoutLedger) {
		t.Fatalf("a join over an absent ledger composed %+v, %v: want errMQTTSharedJoinWithoutLedger", b, err)
	}
}

// T5v: the write boundary refuses a join whose ledger prior is absent, whatever composed it:
// a join legal against generation G's installed ledger is re-targeted at a generation with no
// ledger and handed in directly. Mutation M-join-val.
func TestF105_8bValidatorRefusesAJoinOverAbsence(t *testing.T) {
	x := testNewBoundaryEnv(t)
	tenure := testTenure(3)
	x.install(t, tenure)
	b, err := mqttComposeSharedJoin(x.epoch, x.id, x.gen, x.qkey, mqttNewInflightNonce(), tenure,
		x.group(t), x.ledger(t), mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending))
	if err != nil {
		t.Fatalf("composing the legal join to re-target: %v", err)
	}
	other := mqttNewSessGen()
	b.Gen = other
	b.LedgerPrior, b.LedgerSeq, b.ledgerExpect = nil, 0, 0
	b.LedgerNext = &mqttSharedLedger{Epoch: x.epoch, ID: x.id, Gen: other, State: mqttSharedLedgerOpen,
		Tenure: tenure, Acqs: []mqttSharedAcqRef{b.Acq}, Closed: []string{}}
	x.refused(t, "a join creating a ledger", b)
	if l := x.e.asm.jsa.loadSharedLedger(x.id, x.epoch, other); l.status != mqttOutboundAbsent {
		t.Fatalf("a refused join left generation %q's ledger reading %v", other, l.status)
	}
}

// T5a: an exhausted acquisition meeting an absent ledger is UNRESOLVED and writes nothing — the
// abort never recreates a ledger. Mutation M-abort-init.
func TestF105_8bAbortOverAbsenceWritesNothing(t *testing.T) {
	e := testNewInflightEnv(t, "f105-8b-abort-absent")
	e.sess.mu.Lock()
	id, gen, epoch := e.sess.id, e.sess.gen, e.sess.sharedEpoch()
	e.sess.mu.Unlock()
	tenure := testTenure(4)
	_, out, err := e.sess.abortSharedAcquisition(id, gen, epoch, tenure, mqttNewInflightNonce(), 0)
	if out != mqttSharedAcquireFailed || err == nil {
		t.Fatalf("the abort over absence reported %v, %v", out, err)
	}
	if l := e.sess.jsa.loadSharedLedger(id, epoch, gen); l.status != mqttOutboundAbsent {
		t.Fatalf("the abort over absence wrote the ledger: it reads %v", l.status)
	}
	_, out, err = e.sess.abortSharedAcquisition(id, gen, epoch, tenure, mqttNewInflightNonce(), 7)
	if out != mqttSharedAcquireFailed || !errors.Is(err, errMQTTSharedAcquireExhausted) {
		t.Fatalf("an abort of attempts composed against a sequence, meeting absence, reported %v, %v: want exhausted (unresolved)", out, err)
	}
	if l := e.sess.jsa.loadSharedLedger(id, epoch, gen); l.status != mqttOutboundAbsent {
		t.Fatalf("the abort recreated an absent ledger: it reads %v", l.status)
	}
}

// testF105_8bPreserve closes the bootstrapped SERVING generation with a PRESERVING closure.
func testF105_8bPreserve(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) {
	t.Helper()
	if err := e.asm.jsa.closeGenerationEnrolled(e.id, e.epoch, auth.Gen, auth.Tenure, auth.OwnerTxn,
		false, mqttClosingTargets{}); err != nil {
		t.Fatalf("closing the generation preserving: %v", err)
	}
}

// testF105_8bInstallEmpty creates the generation's ledger the one legal way, empty.
func testF105_8bInstallEmpty(t *testing.T, e *testF105Env, gen string) {
	t.Helper()
	if _, err := e.asm.jsa.installSharedTenure(e.id, e.epoch, gen, testTenure(2)); err != nil {
		t.Fatalf("installing an empty ledger: %v", err)
	}
}

func testF105_8bLedgerState(e *testF105Env, gen string) (mqttOutboundLoadStatus, bool, int) {
	l := e.asm.jsa.loadSharedLedger(e.id, e.epoch, gen)
	if l.status != mqttOutboundValid {
		return l.status, false, 0
	}
	return l.status, l.rec.State == mqttSharedLedgerSealed, len(l.rec.Acqs)
}

// T13: the closing driver's licence, row by row (consult 122). Mutations M-preserve-licence (the
// emptiness requirement dropped for an unsealed non-discarding case) and M-sealed-resume.
func TestF105_8bClosingLicenceMatrix(t *testing.T) {
	type row struct {
		name     string
		setup    func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool // returns authAbsent
		licensed bool
	}
	rows := []row{
		{"discarding closure over a listed acquisition", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			testPlantSharedAcq(t, e, auth.Gen, "g8b-disc")
			testDiscardClose(t, e)
			return false
		}, true},
		{"preserving closure over an absent ledger", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			testF105_8bPreserve(t, e, auth)
			return false
		}, true},
		{"preserving closure over an empty ledger", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			testF105_8bInstallEmpty(t, e, auth.Gen)
			testF105_8bPreserve(t, e, auth)
			return false
		}, true},
		{"preserving closure over a listed acquisition", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			testPlantSharedAcq(t, e, auth.Gen, "g8b-pres")
			testF105_8bPreserve(t, e, auth)
			return false
		}, false},
		{"serving with no closure, empty ledger", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			testF105_8bInstallEmpty(t, e, auth.Gen)
			return false
		}, false},
		{"authority read absent, absent ledger", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			return true
		}, true},
		{"authority read absent, listed acquisition", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			testPlantSharedAcq(t, e, auth.Gen, "g8b-abs")
			return true
		}, false},
		{"corrupt ledger behind a discarding closure", func(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) bool {
			testDiscardClose(t, e)
			if _, err := e.asm.jsa.storeMsg(mqttSharedLedgerSubject(getHash(e.id), auth.Gen), 0, []byte(`{"epoch":`)); err != nil {
				t.Fatalf("planting a corrupt ledger: %v", err)
			}
			return false
		}, false},
	}
	for i, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			e, done := testF105NewEnv(t, "f105-8b-licence-"+string(rune('a'+i)))
			defer done()
			auth, _ := e.bootstrap(t, "txn-1")
			gen := auth.Gen
			absent := r.setup(t, e, auth)
			_, sealedBefore, _ := testF105_8bLedgerState(e, gen)
			err := e.asm.jsa.closeRetiringGeneration(e.id, e.epoch, gen, absent)
			st, sealed, n := testF105_8bLedgerState(e, gen)
			if r.licensed {
				if err != nil {
					t.Fatalf("a licensed closing withheld: %v", err)
				}
				if st != mqttOutboundValid || !sealed || n != 0 {
					t.Fatalf("a licensed closing left the ledger %v sealed=%v listing %d", st, sealed, n)
				}
				return
			}
			if !errors.Is(err, errMQTTClosingUnlicensed) {
				t.Fatalf("an unlicensed closing returned %v: want errMQTTClosingUnlicensed", err)
			}
			if sealed && !sealedBefore {
				t.Fatalf("an unlicensed closing SEALED the ledger")
			}
		})
	}
}

// T13c + T13d: a join landing after the driver's empty observation is preserved by the seal and
// released from the SEALED set; and a closing that fails after sealing that late join is finished
// by the next driver, which resumes from the sealed ledger. Mutations S2b (release from the
// pre-seal load) and M-sealed-resume.
func TestF105_8bLateJoinIsReleasedFromTheSealedSet(t *testing.T) {
	for _, crash := range []bool{false, true} {
		name := "completes"
		if crash {
			name = "fails after the seal, then resumes"
		}
		t.Run(name, func(t *testing.T) {
			e, done := testF105NewEnv(t, "f105-8b-latejoin")
			defer done()
			auth, _ := e.bootstrap(t, "txn-1")
			gen := auth.Gen
			testF105_8bInstallEmpty(t, e, gen)
			testF105_8bPreserve(t, e, auth)
			var joined atomic.Bool
			hook := func(g string) {
				if g == gen && joined.CompareAndSwap(false, true) {
					testPlantSharedAcq(t, e, gen, "g8b-late")
				}
			}
			mqttTestClosingFenceHookV.Store(&hook)
			defer mqttTestClosingFenceHookV.Store(nil)
			if crash {
				// The state a driver leaves when it crashes after its seal: the late join landed after
				// its empty observation, and its seal preserved it. The next driver resumes from it.
				mqttTestClosingFenceHookV.Store(nil)
				testPlantSharedAcq(t, e, gen, "g8b-late")
				if _, err := e.asm.jsa.sealSharedLedger(e.id, e.epoch, gen); err != nil {
					t.Fatalf("sealing as the crashed driver did: %v", err)
				}
				if _, sealed, n := testF105_8bLedgerState(e, gen); !sealed || n != 1 {
					t.Fatalf("the crashed driver's ledger must be sealed listing the late join: sealed=%v n=%d", sealed, n)
				}
			}
			if err := e.asm.jsa.closeRetiringGeneration(e.id, e.epoch, gen, false); err != nil {
				t.Fatalf("the closing withheld: %v", err)
			}
			if err := testSharedLedgerClosedEmpty(e, gen); err != nil {
				t.Fatalf("the late join was not released: %v", err)
			}
		})
	}
}

// T13e: the stated residue. A join that lands after the empty observation but BEFORE the seal,
// with the closing failing in between, leaves an open listed ledger behind a preserving closure;
// the next driver withholds on it (docs/CONFORMANCE.md).
func TestF105_8bLateJoinBeforeTheSealWithholds(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-8b-residue")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	gen := auth.Gen
	testF105_8bInstallEmpty(t, e, gen)
	testF105_8bPreserve(t, e, auth)
	testPlantSharedAcq(t, e, gen, "g8b-residue")
	err := e.asm.jsa.closeRetiringGeneration(e.id, e.epoch, gen, false)
	if !errors.Is(err, errMQTTClosingUnlicensed) {
		t.Fatalf("a preserving closure over an open listed ledger returned %v: want the withhold", err)
	}
	if _, sealed, _ := testF105_8bLedgerState(e, gen); sealed {
		t.Fatal("the withheld closing sealed the ledger")
	}
}

// A1a: the fast path's receipt binding refuses a carried authority whose tenure is not the one the
// receipt authorizes, before anything is sent. Mutation A-fast-bind.
func TestF105_8bFastCloseRefusesAMismatchedReceipt(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-8b-fastbind")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	ret := &mqttRetirementRequest{
		Gen: auth.Gen, Tenure: "tenure-other", OwnerTxn: auth.OwnerTxn,
		SessWID: "wid-x", SessSeq: 1, Reason: mqttRetireSessionEnd, Discard: true,
	}
	if _, err := e.asm.jsa.opCloseTeardownGenerationWith(e.id, e.epoch, "txn-fast", auth.Gen, auth, seq, ret); err == nil {
		t.Fatal("a fast close under a receipt naming another tenure was accepted")
	}
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, auth.Gen); st != mqttOutboundAbsent {
		t.Fatalf("the refused fast close left a closure reading %v", st)
	}
}

// A1b: the transition boundary refuses a close-teardown whose closure does not discard, on an
// otherwise-valid transition (a discarding retirement standing, no affine member). Mutation
// A-fast-validate.
func TestF105_8bCloseTeardownMustDiscard(t *testing.T) {
	mk := func(discard bool) *mqttAuthorityTransition {
		tr := testF105Transition()
		tr.Attempt.Kind = mqttTransitionCloseTeardown
		tr.Auth.State = mqttAuthorityClosed
		tr.Closure = &mqttGenClosure{
			ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: tr.Attempt.Txn, Discard: discard,
		}
		tr.Retire = &mqttRetirementRequest{
			Gen: testF105Gen, Tenure: tr.Prev.Tenure, OwnerTxn: tr.Prev.OwnerTxn,
			SessWID: "wid-x", SessSeq: 1, Reason: mqttRetireSessionEnd, Discard: true,
		}
		return tr
	}
	if err := mqttCheckTransitionKindRules(mk(true)); err != nil {
		t.Fatalf("the discarding baseline was refused: %v", err)
	}
	if err := mqttCheckTransitionKindRules(mk(false)); err == nil {
		t.Fatal("a close-teardown writing a NON-discarding closure passed the kind rules")
	}
}

// testF105_8bDropFirstCloseOutcome arms the request seam so that the FIRST close-teardown
// attempt's outcome subject can never be written or read: its batch cannot commit, its
// resolution stays unresolved, and its abort fails. Later attempts are untouched. It returns a
// counter of close-teardown compositions that reached the send.
func testF105_8bDropFirstCloseOutcome(t *testing.T, e *testF105Env) *atomic.Int32 {
	t.Helper()
	var first atomic.Pointer[string]
	var sent atomic.Int32
	pre := func(tr *mqttAuthorityTransition) {
		if tr.Attempt == nil || tr.Attempt.Kind != mqttTransitionCloseTeardown {
			return
		}
		sent.Add(1)
		subj := mqttTransitionOutcomeSubject(getHash(e.id), tr.Attempt.Txn)
		first.CompareAndSwap(nil, &subj)
	}
	e.asm.jsa.testTransitionPresend.Store(&pre)
	drop := func(subject string, body []byte) bool {
		f := first.Load()
		return f != nil && (subject == *f || strings.Contains(string(body), *f))
	}
	e.asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() {
		e.asm.jsa.testTransitionPresend.Store(nil)
		e.asm.jsa.testDropRequest.Store(nil)
	})
	return &sent
}

// A2: the fast path never composes a second close while its first attempt is unresolved — the
// abort failed and the resolution stays unresolved — it returns the error. Mutation
// A-fast-recompose (restore the fall-through into the full episode).
func TestF105_8bFastCloseDoesNotRecomposeWhileUnresolved(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-8b-fastunres")
	defer done()
	auth, seq := e.bootstrap(t, "txn-1")
	ret := &mqttRetirementRequest{
		Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		SessWID: "wid-x", SessSeq: 1, Reason: mqttRetireSessionEnd, Discard: true,
	}
	// The durable receipt stands, as it does beside the owner's claim, so a fall-through into the
	// full episode WOULD compose a second close: that is what this test must be able to see.
	rcpt, rerr := mqttComposeTeardownReceipt(auth.Gen, "nonce-x", "marker-wid-x", *ret)
	if rerr != nil {
		t.Fatal(rerr)
	}
	testF105_8bPlantReceipt(t, e, auth.Gen, rcpt)
	sent := testF105_8bDropFirstCloseOutcome(t, e)
	err := e.asm.jsa.closeTeardownGenerationFast(e.id, e.epoch, e.asm.domainTk, auth.Gen, auth, seq, ret)
	if err == nil {
		t.Fatal("an unresolved fast close reported success")
	}
	if n := sent.Load(); n != 1 {
		t.Fatalf("%d close-teardown attempts were sent while the first stayed unresolved: want exactly 1", n)
	}
}

// A2b: the full episode does not recompose either. Mutation A-ep-recompose.
func TestF105_8bCloseEpisodeDoesNotRecomposeWhileUnresolved(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-8b-epunres")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	ret := mqttRetirementRequest{
		Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		SessWID: "wid-x", SessSeq: 1, Reason: mqttRetireSessionEnd, Discard: true,
	}
	rcpt, err := mqttComposeTeardownReceipt(auth.Gen, "nonce-x", "marker-wid-x", ret)
	if err != nil {
		t.Fatal(err)
	}
	testF105_8bPlantReceipt(t, e, auth.Gen, rcpt)
	sent := testF105_8bDropFirstCloseOutcome(t, e)
	if err := e.asm.jsa.closeTeardownGeneration(e.id, e.epoch, e.asm.domainTk, auth.Gen); err == nil {
		t.Fatal("an unresolved close episode reported success")
	}
	if n := sent.Load(); n != 1 {
		t.Fatalf("%d close-teardown attempts were sent while the first stayed unresolved: want exactly 1", n)
	}
}

// A3: a teardown close meeting a PRESERVING closure of its generation refuses rather than
// reporting the discard done. Mutation A-nondiscard.
func TestF105_8bCloseEpisodeRefusesAPreservingClosure(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-8b-nondiscard")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105_8bPreserve(t, e, auth)
	if err := e.asm.jsa.closeTeardownGeneration(e.id, e.epoch, e.asm.domainTk, auth.Gen); err == nil {
		t.Fatal("a teardown close reported a preserving closure as its discard")
	}
}

// testF105_8bPlantReceipt stores a teardown receipt for gen directly, create-only.
func testF105_8bPlantReceipt(t *testing.T, e *testF105Env, gen string, r *mqttSessTeardownReceipt) {
	t.Helper()
	body, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.asm.jsa.storeMsg(mqttTeardownReceiptSubject(e.asm.domainTk, getHash(e.id), gen), 0, body); err != nil {
		t.Fatalf("planting the receipt: %v", err)
	}
}

// T13f: what proves a generation NEVER SERVED. A determinate absence, or an authority serving or
// closed at ANOTHER generation; never a standing proposal (it may still finalize or cancel back
// to this generation), never this generation, never an indeterminate or corrupt read. Mutation
// M-never-served (an indeterminate read, or a proposal, taken as proof).
func TestF105_8bNeverServedEvidence(t *testing.T) {
	const gen = "gen-retiring"
	at := func(st mqttAuthorityState, g string) mqttAuthorityLoad {
		return mqttAuthorityLoad{status: mqttOutboundValid, rec: &mqttOutboundAuthority{State: st, Gen: g}}
	}
	for name, c := range map[string]struct {
		al   mqttAuthorityLoad
		want bool
	}{
		"absent":               {mqttAuthorityLoad{status: mqttOutboundAbsent}, true},
		"serving another":      {at(mqttAuthorityServing, "gen-other"), true},
		"closed at another":    {at(mqttAuthorityClosed, "gen-other"), true},
		"serving this":         {at(mqttAuthorityServing, gen), false},
		"closed at this":       {at(mqttAuthorityClosed, gen), false},
		"preparing at another": {at(mqttAuthorityPreparing, "gen-other"), false},
		"reserving at another": {at(mqttAuthorityReserving, "gen-other"), false},
		"indeterminate":        {mqttAuthorityLoad{status: mqttOutboundIndeterminate}, false},
		"corrupt":              {mqttAuthorityLoad{status: mqttOutboundCorrupt}, false},
	} {
		if got := mqttAuthorityNeverServed(c.al, gen); got != c.want {
			t.Errorf("%s: never served = %v, want %v", name, got, c.want)
		}
	}
}
