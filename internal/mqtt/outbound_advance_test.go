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
	"strconv"
	"testing"
)

// TestF105PhaseRelationIsTheProtocolNotAnOrdering pins that the successor relation is
// enumerated per QoS rather than derived from the phase numbers.
//
// An ordering check would admit every forward move, and two of those are protocol
// violations: a QoS 2 exchange going from its publish straight to terminal discards the
// release the client is owed, and a QoS 1 exchange reaching PUBREC-observed records an
// ownership transfer that has no packet behind it.
func TestF105PhaseRelationIsTheProtocolNotAnOrdering(t *testing.T) {
	for _, ok := range []struct {
		qos      byte
		from, to mqttExchangePhase
	}{
		{1, mqttExchangePublish, mqttExchangeTerminal},
		{1, mqttExchangePublish, mqttExchangeTerminalPendingDrain},
		{1, mqttExchangeTerminalPendingDrain, mqttExchangeTerminal},
		{2, mqttExchangePublish, mqttExchangePubRecObserved},
		{2, mqttExchangePubRecObserved, mqttExchangeTerminal},
		{2, mqttExchangePubRecObserved, mqttExchangeTerminalPendingDrain},
		{2, mqttExchangeTerminalPendingDrain, mqttExchangeTerminal},
	} {
		if err := mqttExchangePhaseSucceeds(ok.qos, ok.from, ok.to); err != nil {
			t.Fatalf("a QoS %d exchange was refused the legitimate move %d->%d: %v",
				ok.qos, ok.from, ok.to, err)
		}
	}

	for name, bad := range map[string]struct {
		qos      byte
		from, to mqttExchangePhase
	}{
		// The two an ordering check would wrongly admit.
		"a QoS 2 publish going straight to terminal": {2, mqttExchangePublish, mqttExchangeTerminal},
		"a QoS 2 publish skipping the release":       {2, mqttExchangePublish, mqttExchangeTerminalPendingDrain},
		"a QoS 1 exchange observing a PUBREC":        {1, mqttExchangePublish, mqttExchangePubRecObserved},
		"a QoS 1 exchange completing a PUBREC":       {1, mqttExchangePubRecObserved, mqttExchangeTerminal},
		// The retired PUBREL-owed value (3) is no phase at all, in either direction.
		"a PUBREC advancing to the retired phase": {2, mqttExchangePubRecObserved, mqttExchangePhase(3)},
		"the retired phase advancing to terminal": {2, mqttExchangePhase(3), mqttExchangeTerminal},
		// Terminal is absorbing: a resurrected obligation would re-charge Receive Maximum
		// and re-authorize output for a message the client already finished with.
		"a terminal exchange returning to publish":  {2, mqttExchangeTerminal, mqttExchangePublish},
		"a terminal exchange returning to a PUBREC": {2, mqttExchangeTerminal, mqttExchangePubRecObserved},
		"a terminal exchange awaiting a drain":      {2, mqttExchangeTerminal, mqttExchangeTerminalPendingDrain},
		// And the drain is the only way out of pending-drain.
		"a pending drain reopening its release": {2, mqttExchangeTerminalPendingDrain, mqttExchangePubRecObserved},
		// A move to where it already stands records nothing.
		"a move to the standing phase": {2, mqttExchangePubRecObserved, mqttExchangePubRecObserved},
		// Unsupported phases and QoS have no relation at all.
		"an unsupported source phase": {2, mqttExchangePhase(0), mqttExchangeTerminal},
		"an unsupported target phase": {2, mqttExchangePublish, mqttExchangePhase(9)},
		"QoS 0, which owes nothing":   {0, mqttExchangePublish, mqttExchangeTerminal},
	} {
		if err := mqttExchangePhaseSucceeds(bad.qos, bad.from, bad.to); err == nil {
			t.Fatalf("%s was admitted as a legal phase move", name)
		}
	}
}

// TestF105AdvanceCarriesNothingButThePhase pins that a completion cannot ride an unrelated
// rewrite into the store.
//
// The advance is built by COPYING the record as read, so an identifier reallocation, a
// tenure or an admitting transition cannot travel with it. Each of those is a different
// operation with a different authorization, and an advance that accepted a whole replacement
// body would be the one kind that needs no serving authority rewriting an obligation's
// identity under a closed generation.
func TestF105AdvanceCarriesNothingButThePhase(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-shape")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, err := mqttAdmitPublishTransition(adm)
	if err != nil {
		t.Fatalf("assembling the admission failed: %v", err)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr)); cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}

	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-adv", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if aerr != nil {
		t.Fatalf("assembling the advance failed: %v", aerr)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, adv)); cerr != nil {
		t.Fatalf("the advance was refused: %v", cerr)
	}

	got, _ := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	if got.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the exchange stands at phase %d after PUBREC, want %d",
			got.Phase, mqttExchangePubRecObserved)
	}
	if got.PI != cur.PI || got.QoS != cur.QoS || got.Nonce != cur.Nonce ||
		got.AdmitTxn != cur.AdmitTxn || got.Tenure != cur.Tenure || got.Gen != cur.Gen {
		t.Fatalf("the advance rewrote the obligation's identity: %+v became %+v", cur, got)
	}
	// AND THE AUTHORITY DID NOT MOVE. An advance is not an ownership event.
	after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.rec.Gen != al.rec.Gen || after.rec.Tenure != al.rec.Tenure ||
		after.rec.OwnerTxn != al.rec.OwnerTxn || after.rec.State != al.rec.State {
		t.Fatalf("the advance moved the authority from %+v to %+v", al.rec, after.rec)
	}

	// A STALE READ LOSES. Re-advancing at the sequence held before the first advance is the
	// second broker observing the same acknowledgement, and it must not overwrite what
	// happened in between.
	stale, serr := mqttAdvanceExchangeTransition(after.rec, after.seq, "txn-adv-stale", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if serr != nil {
		t.Fatalf("assembling the stale advance failed: %v", serr)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, stale)); cerr == nil {
		t.Fatalf("an advance conditioned on a superseded read was accepted")
	}

	// An advance with no read sequence would frame the member create-only, which for an
	// exchange that already exists is not an advance at all.
	if _, zerr := mqttAdvanceExchangeTransition(after.rec, after.seq, "txn-adv-zero", "op-adv",
		got, 0, mqttExchangeTerminal); zerr == nil {
		t.Fatalf("an advance with no read sequence was assembled")
	}
}

// TestF105AdvanceAdmitsNothing pins the confinement that makes the serving exemption safe.
//
// advance-exchange and dispose-resource are the only kinds that run under a closed
// authority. If an advance could carry a register, an action or a dependency count, it would
// be a route for admitting outbound work into a generation that is permanently closed —
// reached not by racing the closure but by naming a different transition kind.
func TestF105AdvanceAdmitsNothing(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-confine")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, _ := mqttAdmitPublishTransition(adm)
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr)); cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}
	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)

	base, berr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-adv", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if berr != nil {
		t.Fatalf("assembling the advance failed: %v", berr)
	}

	regBody, merr := json.Marshal(adm.Register)
	if merr != nil {
		t.Fatalf("marshalling the register: %v", merr)
	}
	// A REGISTER SMUGGLED IN BESIDE THE PHASE, at the sequence the admission left it — so
	// the store would accept this member happily and the only thing that can refuse it is
	// the kind's own confinement. A member the store would reject anyway proves nothing
	// about the rule.
	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 7)
	regMsg, rerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj)
	if rerr != nil {
		t.Fatalf("the admission's register is not in the store: %v", rerr)
	}
	withReg := *base
	withReg.Extra = append(append([]mqttTransitionMember{}, base.Extra...),
		mqttTransitionMember{subj: regSubj, expect: regMsg.Sequence, body: regBody})
	if _, cerr := e.asm.jsa.commitAuthorityTransition(&withReg); cerr == nil {
		t.Fatalf("an advance-exchange transition carried a register into the store")
	}

	// AN EXCHANGE THE STORE HAS NEVER SEEN, create-only: that would INSTALL an obligation
	// nothing admitted, through the one kind that needs no serving authority. The nonce is
	// fresh precisely so the store's own create-only CAS would otherwise succeed, which is
	// what leaves the rule as the only thing refusing it.
	freshNonce := mqttNewInflightNonce()
	freshBody, ferr := json.Marshal(&mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: freshNonce,
		PI: 11, QoS: 2, Phase: mqttExchangePublish,
		AdmitTxn: "txn-adm", Tenure: cur.Tenure,
	})
	if ferr != nil {
		t.Fatalf("marshalling the smuggled exchange: %v", ferr)
	}
	creating := *base
	creating.Extra = append(append([]mqttTransitionMember{}, base.Extra...),
		mqttTransitionMember{
			subj:   mqttExchangeSubject(getHash(e.id), testF105Gen, freshNonce),
			expect: 0,
			body:   freshBody,
		})
	if _, cerr := e.asm.jsa.commitAuthorityTransition(&creating); cerr == nil {
		t.Fatalf("an advance-exchange transition installed an exchange create-only")
	}
	if _, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttExchangeSubject(getHash(e.id), testF105Gen, freshNonce)); lerr == nil {
		t.Fatalf("the smuggled exchange landed in the store")
	}

	// And an advance carrying nothing at all is not an advance.
	empty := *base
	empty.Extra = nil
	if _, cerr := e.asm.jsa.commitAuthorityTransition(&empty); cerr == nil {
		t.Fatalf("an advance-exchange transition carrying no exchange was accepted")
	}

	// The unmutated advance still commits, so the refusals above are the members' doing and
	// not a broken fixture.
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, base)); cerr != nil {
		t.Fatalf("the well-formed advance was refused: %v", cerr)
	}
}

// TestF105ClosedGenerationsObligationStillEnds is the reason advance-exchange does not
// require a serving authority.
//
// G1 is closed and G2 serves. G1's outstanding exchange must still be able to reach its end:
// the client's PUBCOMP arrives on G1's connection, and the only authority whose CAS can
// succeed is G2's. A kind that demanded SERVING would leave that obligation charged forever —
// its Receive Maximum slot held, its identifier unreclaimable — which is precisely the
// unfinishable generation the retirement protocol exists to avoid.
func TestF105ClosedGenerationsObligationStillEnds(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-crossgen")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, _ := mqttAdmitPublishTransition(adm)
	admSeq, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr))
	if cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}

	// Close G1 permanently, then claim a FRESH generation beside it.
	closing := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: auth.OwnerTxn, Rev: atr.Auth.Rev + 1,
			Gen: testF105Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: atr.Auth, PrevSeq: admSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Tenure: auth.Tenure,
			State: mqttAuthorityClosed, Rev: atr.Auth.Rev + 1, Txn: "txn-close",
			OwnerTxn: auth.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Txn: "txn-close"},
	}
	closeSeq, clerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, closing))
	if clerr != nil {
		t.Fatalf("the closure was refused: %v", clerr)
	}
	// The successor generation is CLAIMED beside the closure, by the same three steps as the
	// first claim: there is no kind that installs a serving authority over a closure.
	e.claim(t, closing.Auth, closeSeq, "txn-g2", testF105Gen2, "tenure-g2")

	// G1's exchange advances under G2's serving authority, all the way to terminal.
	for _, to := range []mqttExchangePhase{
		mqttExchangePubRecObserved, mqttExchangeTerminal,
	} {
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		if al.rec.Gen != testF105Gen2 {
			t.Fatalf("the serving authority is at generation %q, want the successor", al.rec.Gen)
		}
		adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-x-"+string(rune('a'+to)),
			"op-x", cur, curSeq, to)
		if aerr != nil {
			t.Fatalf("assembling the cross-generation advance to %d failed: %v", to, aerr)
		}
		// The ending releases G1's source hold in the same batch, under G2's authority: the
		// resource member names the obligation's own, earlier, generation (F-105 7c-lc, P-4).
		adv = testF105WithHoldReleases(t, e, adv, cur, to)
		if _, ccerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, adv)); ccerr != nil {
			t.Fatalf("a closed generation's obligation could not advance to %d: %v", to, ccerr)
		}
	}
	testF105RequireDeps(t, e, "inc-src")
	end, _ := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	if end.Phase != mqttExchangeTerminal {
		t.Fatalf("the closed generation's obligation stands at phase %d, want terminal", end.Phase)
	}
	// It is still G1's obligation: the advance moved the phase and nothing else.
	if end.Gen != testF105Gen || end.Tenure != auth.Tenure {
		t.Fatalf("advancing under the successor re-attributed the obligation to %q/%q",
			end.Gen, end.Tenure)
	}

	// AND THE EXEMPTION IS NOT A GENERAL ONE. An ADMISSION naming the closed generation is
	// still refused, so the relaxed member check did not open admission-after-closure.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	late := testF105Admission(e, al.rec, al.seq, "txn-late", 9, nil)
	late.Exchange.Gen = testF105Gen
	if _, lerr := mqttAdmitPublishTransition(late); lerr == nil {
		t.Fatalf("an admission into the closed generation was assembled under the successor")
	}
}

// TestF105AdvanceIsNotAnOwnershipEvent pins that the transition exempt from the serving
// requirement cannot touch the authority itself.
//
// Every other check in the kind's rule guards what it CARRIES. These guard what it
// INSTALLS: an advance that could move the authority's state would be a route from closed
// back to serving — reopening a permanently closed generation without a readmission — and
// one that could move the generation, the tenure or the ownership token would be a takeover
// executed by a completion handler.
//
// EACH MUTATION IS STATED CONSISTENTLY IN BOTH THE ATTEMPT AND THE AUTHORITY. Stating it in
// the authority alone is caught by the generic agreement check between them, which ties the
// installed record to the attempt that describes it — a different fact from the one here,
// which ties the installed record to its PREDECESSOR. An oracle that left them disagreeing
// would be testing the generic check and reporting it as this one.
func TestF105AdvanceIsNotAnOwnershipEvent(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-ownership")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, _ := mqttAdmitPublishTransition(adm)
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr)); cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}
	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)

	// The keys are transition-id tokens, because they are used as one: an id carrying a
	// space is refused by the attempt's own shape check, and a fixture refused there never
	// reaches the rule it was written for.
	for name, mutate := range map[string]func(*mqttTransitionAttempt, *mqttOutboundAuthority){
		"moving-the-generation": func(a *mqttTransitionAttempt, o *mqttOutboundAuthority) {
			a.Gen, o.Gen = testF105Gen2, testF105Gen2
		},
		"moving-the-tenure": func(a *mqttTransitionAttempt, o *mqttOutboundAuthority) {
			a.Tenure, o.Tenure = "tenure-other", "tenure-other"
		},
		"taking-ownership": func(a *mqttTransitionAttempt, o *mqttOutboundAuthority) {
			a.OwnerTxn, o.OwnerTxn = a.Txn, a.Txn
		},
	} {
		adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-adv-"+name, "op-adv",
			cur, curSeq, mqttExchangePubRecObserved)
		if aerr != nil {
			t.Fatalf("assembling the advance for %q failed: %v", name, aerr)
		}
		attempt, auth := *adv.Attempt, *adv.Auth
		mutate(&attempt, &auth)
		adv.Attempt, adv.Auth = &attempt, &auth
		if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, adv)); cerr == nil {
			t.Fatalf("an advance-exchange transition %s was accepted", name)
		}
	}

	// The authority is where it was: none of the refusals above left a partial write.
	after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.status != mqttOutboundValid || after.rec.State != mqttAuthorityServing ||
		after.rec.Gen != al.rec.Gen || after.rec.Tenure != al.rec.Tenure ||
		after.rec.OwnerTxn != al.rec.OwnerTxn {
		t.Fatalf("a refused advance moved the authority from %+v to %+v", al.rec, after.rec)
	}
	// And the well-formed advance still commits.
	ok, oerr := mqttAdvanceExchangeTransition(after.rec, after.seq, "txn-adv-ok", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if oerr != nil {
		t.Fatalf("assembling the well-formed advance failed: %v", oerr)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, ok)); cerr != nil {
		t.Fatalf("the well-formed advance was refused: %v", cerr)
	}
}

// TestF105AdvanceCannotReopenAClosedGeneration is the state half of the same rule, and the
// one that matters most.
//
// advance-exchange is exempt from the serving requirement, so it is the kind a closed
// authority can actually run. If it could also INSTALL a serving state, a completion handler
// would be able to reopen a permanently closed generation to new admission without a fresh
// claim and without a fresh generation — the whole closure would be undone by the
// transition that was allowed because it does not admit anything.
func TestF105AdvanceCannotReopenAClosedGeneration(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-reopen")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, _ := mqttAdmitPublishTransition(adm)
	admSeq, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr))
	if cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}
	closing := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: auth.OwnerTxn, Rev: atr.Auth.Rev + 1,
			Gen: testF105Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: atr.Auth, PrevSeq: admSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Tenure: auth.Tenure,
			State: mqttAuthorityClosed, Rev: atr.Auth.Rev + 1, Txn: "txn-close",
			OwnerTxn: auth.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Txn: "txn-close"},
	}
	if _, clerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, closing)); clerr != nil {
		t.Fatalf("the closure was refused: %v", clerr)
	}

	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.rec.State != mqttAuthorityClosed {
		t.Fatalf("the authority is at state %d, want closed", al.rec.State)
	}

	reopen, rerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-adv-reopen", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if rerr != nil {
		t.Fatalf("assembling the advance failed: %v", rerr)
	}
	mutated := *reopen.Auth
	mutated.State = mqttAuthorityServing
	reopen.Auth = &mutated
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, reopen)); err == nil {
		t.Fatalf("an advance-exchange transition reopened a permanently closed generation")
	}
	if back := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); back.rec.State != mqttAuthorityClosed {
		t.Fatalf("the authority is at state %d after the refused reopen, want closed", back.rec.State)
	}

	// The unmutated advance commits, and the generation stays closed.
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	good, gerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-adv-good", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if gerr != nil {
		t.Fatalf("assembling the well-formed advance failed: %v", gerr)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, good)); err != nil {
		t.Fatalf("the well-formed advance was refused under the closed authority: %v", err)
	}
	if back := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); back.rec.State != mqttAuthorityClosed {
		t.Fatalf("a well-formed advance moved the authority to state %d", back.rec.State)
	}
}

// TestF105AdvancePreservesAPendingProposal pins that cleanup running beside an unresolved
// proposal cannot quietly replace it.
//
// An advance runs while a handoff is PREPARING — completion work does not wait for an
// ownership decision — and the authority record is rewritten by every transition. If the
// advance could substitute a different intent, the proposal a CONNECT is waiting to finalize
// would be swapped for one nobody proposed, and that CONNECT would finalize into ownership
// it never asked for. DROPPING it is refused by the authority's own shape; REPLACING it with
// a well-formed intent is refused only here.
func TestF105AdvancePreservesAPendingProposal(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-pending")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, _ := mqttAdmitPublishTransition(adm)
	admSeq, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr))
	if cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}

	mkIntent := func(id, tenure string) *mqttHandoffIntent {
		return &mqttHandoffIntent{
			ID: id, PrevGen: auth.Gen, PrevTenure: auth.Tenure,
			PrevOwnerTxn: auth.OwnerTxn, PrevSessWID: "wid-0", PrevSessSeq: 12,
			Tenure: tenure, Gen: testF105Gen2, CleanReq: true,
			Retire: &mqttRetirementRequest{
				Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
				SessWID: "wid-0", SessSeq: 12, Reason: mqttRetireCleanStart, Discard: true,
			},
		}
	}
	standing := mkIntent("intent-1", "tenure-next")
	prep := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-prep", Op: "op-prep",
			Kind: mqttTransitionPrepareIntent, PrevTxn: auth.OwnerTxn, Rev: atr.Auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: atr.Auth, PrevSeq: admSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: mqttAuthorityPreparing, Rev: atr.Auth.Rev + 1, Txn: "txn-prep",
			OwnerTxn: auth.OwnerTxn, Intent: standing,
		},
	}
	if _, perr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, prep)); perr != nil {
		t.Fatalf("the preparation was refused: %v", perr)
	}

	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	// THE PREPARING AUTHORITY MUST LOAD BACK. A record that can be written and not read is
	// a corrupt authority the moment a broker restarts, and the closed schema will report it
	// as exactly that.
	if al.status != mqttOutboundValid {
		t.Fatalf("the preparing authority does not load back: status %v", al.status)
	}
	if al.rec.State != mqttAuthorityPreparing || al.rec.Intent == nil {
		t.Fatalf("the authority is not preparing with an intent (state %d)", al.rec.State)
	}

	swapped, serr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-adv-swap", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if serr != nil {
		t.Fatalf("assembling the advance failed: %v", serr)
	}
	mutated := *swapped.Auth
	mutated.Intent = mkIntent("intent-9", "tenure-usurper")
	swapped.Auth = &mutated
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, swapped)); err == nil {
		t.Fatalf("an advance-exchange transition replaced the pending handoff intent")
	}

	// The well-formed advance commits and carries the standing proposal through untouched.
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	good, gerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-adv-good", "op-adv",
		cur, curSeq, mqttExchangePubRecObserved)
	if gerr != nil {
		t.Fatalf("assembling the well-formed advance failed: %v", gerr)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, good)); err != nil {
		t.Fatalf("the well-formed advance was refused while preparing: %v", err)
	}
	back := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if back.rec.State != mqttAuthorityPreparing || back.rec.Intent == nil ||
		back.rec.Intent.ID != standing.ID || back.rec.Intent.Tenure != standing.Tenure {
		t.Fatalf("the advance did not carry the pending proposal through: %+v", back.rec.Intent)
	}
}

// TestF105AdvanceIsCheckedAgainstTheStoreNotItsBuilder is the adversarial half of the
// phase-only guarantee, and the one that matters: the guarantee has to hold at the WRITE
// BOUNDARY, not in the convenience that composed the transition.
//
// A builder-only guarantee is a caller convention. Every check on a transition's own shape
// passes for a body substituted after it was built — the member's schema is valid, the class
// is right, the expected sequence is right — so the CAS succeeds and a completed obligation
// is resurrected, or a QoS 2 release the client is owed simply disappears. Under a CLOSED
// authority, which is the one state `advance-exchange` is exempt for.
func TestF105AdvanceIsCheckedAgainstTheStoreNotItsBuilder(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-boundary")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, _ := mqttAdmitPublishTransition(adm)
	admSeq, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr))
	if cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}

	// Close the generation, so every attempt below runs under the authority state the kind
	// is exempt for.
	closing := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: auth.OwnerTxn, Rev: atr.Auth.Rev + 1,
			Gen: testF105Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: atr.Auth, PrevSeq: admSeq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Tenure: auth.Tenure,
			State: mqttAuthorityClosed, Rev: atr.Auth.Rev + 1, Txn: "txn-close",
			OwnerTxn: auth.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Txn: "txn-close"},
	}
	if _, clerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, closing)); clerr != nil {
		t.Fatalf("the closure was refused: %v", clerr)
	}

	// Walk the exchange past PUBREC durably, so there is a real mid-protocol record to
	// attack — and so the regression cases below have somewhere to regress FROM.
	for _, to := range []mqttExchangePhase{mqttExchangePubRecObserved} {
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-step-"+strconv.Itoa(int(to)),
			"op-step", cur, curSeq, to)
		if aerr != nil {
			t.Fatalf("assembling the advance to %d failed: %v", to, aerr)
		}
		if _, serr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, adv)); serr != nil {
			t.Fatalf("the advance to %d was refused: %v", to, serr)
		}
	}

	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	if cur.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the exchange stands at phase %d, want the release owed (PUBREC observed)", cur.Phase)
	}

	// Each case builds a LEGITIMATE advance and then substitutes the body. Everything the
	// transition can check about itself still passes.
	for name, substitute := range map[string]func(x *mqttOutboundExchange){
		"regressing to its publish phase": func(x *mqttOutboundExchange) { x.Phase = mqttExchangePublish },
		"regressing to the observed PUBREC": func(x *mqttOutboundExchange) {
			x.Phase = mqttExchangePubRecObserved
		},
		"reallocating the identifier":        func(x *mqttOutboundExchange) { x.PI = 9 },
		"changing the QoS":                   func(x *mqttOutboundExchange) { x.QoS = 1 },
		"re-attributing the tenure":          func(x *mqttOutboundExchange) { x.Tenure = "tenure-other" },
		"rewriting the admitting transition": func(x *mqttOutboundExchange) { x.AdmitTxn = "txn-other" },
	} {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-sub", "op-sub",
			cur, curSeq, mqttExchangeTerminal)
		if aerr != nil {
			t.Fatalf("assembling the advance for %q failed: %v", name, aerr)
		}
		x := *cur
		x.Phase = mqttExchangeTerminal
		substitute(&x)
		body, merr := json.Marshal(&x)
		if merr != nil {
			t.Fatalf("marshalling the substituted body for %q: %v", name, merr)
		}
		adv.Extra = []mqttTransitionMember{{subj: adv.Extra[0].subj, expect: curSeq, body: body}}
		if _, serr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, adv)); serr == nil {
			t.Fatalf("an advance %s was committed: the phase-only guarantee lives in the "+
				"builder and not at the write boundary", name)
		}
		got, _ := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
		if got.Phase != cur.Phase || got.PI != cur.PI || got.QoS != cur.QoS ||
			got.Tenure != cur.Tenure || got.AdmitTxn != cur.AdmitTxn {
			t.Fatalf("a refused advance %s changed the exchange: %+v became %+v", name, cur, got)
		}
	}

	// A QoS 2 PUBLISH SKIPPING STRAIGHT TO TERMINAL is the same bypass in its other form:
	// the body is untouched apart from the phase, and the move is still not the protocol's.
	// It needs an exchange still at its publish phase, so admit a second obligation.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	_ = al
	fresh, freshSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	skip := *fresh
	skip.Phase = mqttExchangeTerminal
	skipBody, _ := json.Marshal(&skip)
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	legit, lerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-skip", "op-skip",
		fresh, freshSeq, mqttExchangeTerminal)
	if lerr != nil {
		t.Fatalf("assembling the legitimate terminal advance failed: %v", lerr)
	}
	legit.Extra = []mqttTransitionMember{{subj: legit.Extra[0].subj, expect: freshSeq, body: skipBody}}
	legit = testF105WithHoldReleases(t, e, legit, fresh, mqttExchangeTerminal)
	// This one IS legal from pubrel-owed, so it must COMMIT: the boundary check refuses
	// illegal moves, not every substituted body, and a check that refused this would be
	// refusing the mechanism's ordinary use.
	if _, serr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, legit)); serr != nil {
		t.Fatalf("a legitimate terminal advance was refused by the boundary check: %v", serr)
	}
	end, _ := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	if end.Phase != mqttExchangeTerminal {
		t.Fatalf("the exchange stands at phase %d after its PUBCOMP, want terminal", end.Phase)
	}

	// And a STALE expectation is refused with the reason, rather than left to the CAS: a
	// comparison against a record this transition is not conditioned on proves nothing.
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	stale, serr2 := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-stale", "op-stale",
		fresh, freshSeq, mqttExchangeTerminal)
	if serr2 != nil {
		t.Fatalf("assembling the stale advance failed: %v", serr2)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, stale)); cerr == nil {
		t.Fatalf("an advance conditioned on a superseded read was committed")
	}
}

// TestF105NoKindMayRegressAnExchangeOrRewriteAnAction is the FAMILY tripwire, not the
// instance.
//
// The commit-boundary check was written for advance-exchange, which is where the bypass was
// found. Scoping it to that kind would have left the identical bypass one kind name away:
// admit-exchange and acquire-action confine their members by nothing at all, so either could
// carry an exchange at a nonzero expectation and regress a phase under a SERVING authority.
// The rule is therefore keyed on the MEMBER — an exchange member that overwrites IS an
// advance, whoever carries it.
//
// The action half is the same shape. An action's Run, Conn and Tenure are the ORIGINAL
// binding a captured completion handler resolves against, so an overwrite would redirect an
// acquired operation through a rebound connection. A retransmission is a new acquisition
// counter, never a replacement of the attempt that may still be executing.
func TestF105NoKindMayRegressAnExchangeOrRewriteAnAction(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-family")
	defer done()

	auth, authSeq := e.bootstrap(t, "txn-1")
	src, srcSeq := testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	adm := testF105Admission(e, auth, authSeq, "txn-adm", 7,
		[]mqttResourceDependency{{Resource: src, Seq: srcSeq, Role: mqttRoleSourceReplay}})
	atr, _ := mqttAdmitPublishTransition(adm)
	admSeq, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, atr))
	if cerr != nil {
		t.Fatalf("the admission was refused: %v", cerr)
	}

	// An action to attack, acquired the ordinary way.
	bind := mqttBindConnection(4242)
	act := &mqttOutboundAction{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: adm.Exchange.Nonce, Acq: 1,
		Kind: mqttActionPublish, PI: 7,
		Run: bind.run, Conn: bind.conn, Tenure: auth.Tenure, AcqTxn: "txn-acq",
	}
	acq, aerr := mqttAcquireActionTransition(atr.Auth, admSeq, "txn-acq", "op-acq", act)
	if aerr != nil {
		t.Fatalf("assembling the acquisition failed: %v", aerr)
	}
	acq.Extra = append(acq.Extra, testF105TokenMember(t, e, act.Gen, act))
	if _, serr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, acq)); serr != nil {
		t.Fatalf("the acquisition was refused: %v", serr)
	}

	// Walk the exchange forward so there is something to regress FROM.
	for _, to := range []mqttExchangePhase{mqttExchangePubRecObserved} {
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		adv, berr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-fam-"+strconv.Itoa(int(to)),
			"op-fam", cur, curSeq, to)
		if berr != nil {
			t.Fatalf("assembling the advance to %d failed: %v", to, berr)
		}
		if _, serr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, adv)); serr != nil {
			t.Fatalf("the advance to %d was refused: %v", to, serr)
		}
	}
	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)

	// The regression body, identical to the standing record apart from an illegal phase.
	regressed := *cur
	regressed.Phase = mqttExchangePublish
	regBody, merr := json.Marshal(&regressed)
	if merr != nil {
		t.Fatalf("marshalling the regressed exchange: %v", merr)
	}
	xchSubj := mqttExchangeSubject(getHash(e.id), testF105Gen, adm.Exchange.Nonce)

	// EVERY KIND THAT MAY CARRY MEMBERS UNDER A SERVING AUTHORITY is tried, not just the
	// one the check was written for.
	for name, kind := range map[string]mqttTransitionKind{
		"admit-exchange": mqttTransitionAdmitExchange,
		"acquire-action": mqttTransitionAcquireAction,
	} {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		txn := "txn-via-" + name
		smuggle := &mqttAuthorityTransition{
			Attempt: &mqttTransitionAttempt{
				ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
				Kind: kind, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
				Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
			},
			Prev: al.rec, PrevSeq: al.seq,
			Auth: &mqttOutboundAuthority{
				ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
				State: mqttAuthorityServing, Rev: al.rec.Rev + 1, Txn: txn,
				OwnerTxn: al.rec.OwnerTxn,
			},
			Extra: []mqttTransitionMember{{subj: xchSubj, expect: curSeq, body: regBody}},
		}
		if _, serr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, smuggle)); serr == nil {
			t.Fatalf("a %s transition regressed an exchange's phase: the boundary check is "+
				"scoped to a kind rather than to the member", name)
		}
		got, _ := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
		if got.Phase != cur.Phase {
			t.Fatalf("a refused %s transition moved the exchange to phase %d", name, got.Phase)
		}
	}

	// AN ACTION OVERWRITE IS REFUSED IN EVERY KIND, and does not need a store comparison to
	// be: the record is never rewritten at all, so a nonzero expectation is the whole
	// defect.
	actSubj := mqttActionSubject(getHash(e.id), testF105Gen, adm.Exchange.Nonce, 1)
	amsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, actSubj)
	if lerr != nil {
		t.Fatalf("the acquired action is not in the store: %v", lerr)
	}
	rebound := *act
	rebound.Conn = "some-other-connection"
	actBody, aerr2 := json.Marshal(&rebound)
	if aerr2 != nil {
		t.Fatalf("marshalling the rebound action: %v", aerr2)
	}
	for name, kind := range map[string]mqttTransitionKind{
		"acquire-action":   mqttTransitionAcquireAction,
		"admit-exchange":   mqttTransitionAdmitExchange,
		"advance-exchange": mqttTransitionAdvanceExchange,
	} {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		txn := "txn-rebind-" + name
		reb := &mqttAuthorityTransition{
			Attempt: &mqttTransitionAttempt{
				ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
				Kind: kind, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
				Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
			},
			Prev: al.rec, PrevSeq: al.seq,
			Auth: &mqttOutboundAuthority{
				ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
				State: al.rec.State, Rev: al.rec.Rev + 1, Txn: txn, OwnerTxn: al.rec.OwnerTxn,
			},
			Extra: []mqttTransitionMember{{subj: actSubj, expect: amsg.Sequence, body: actBody}},
		}
		if _, serr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, reb)); serr == nil {
			t.Fatalf("a %s transition rewrote an acquired action's connection binding", name)
		}
	}
	back, derr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, actSubj)
	if derr != nil {
		t.Fatalf("the action disappeared: %v", derr)
	}
	still, derr2 := mqttDecodeOutboundAction(back.Data, e.id, e.epoch, testF105Gen,
		adm.Exchange.Nonce, 1)
	if derr2 != nil {
		t.Fatalf("decoding the action back: %v", derr2)
	}
	if still.Conn != act.Conn || still.Run != act.Run || still.Tenure != act.Tenure {
		t.Fatalf("the action's original binding was rewritten: %+v", still)
	}
}
