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
	"fmt"
	"strings"
	"testing"
)

// Replacing a register inside an authority transition (F-105, row 1).
//
// Every fixture here composes its members BY HAND and submits them at the occupant's real
// sequence — the sequence the store itself would accept. That is deliberate: the store's CAS
// is not the property under test, and a fixture that let the CAS do the refusing would pass
// against a boundary that had stopped checking. What is under test is that the WRITE BOUNDARY
// refuses a batch whose members are each individually legal but which, taken together with
// the state they land in, do not implement the disposition the replacement selected.

// testF105ReplEnv is one bootstrapped authority with an admitted obligation standing on an
// identifier, which is what every replacement below displaces.
type testF105ReplEnv struct {
	*testF105Env
	auth    *mqttOutboundAuthority
	authSeq uint64
	pi      uint16
	regSubj string
	occ     *mqttInflightRecord
	occSeq  uint64
	occLin  string
}

func testF105NewReplEnv(t *testing.T, clientID string) (*testF105ReplEnv, func()) {
	t.Helper()
	e, done := testF105NewEnv(t, clientID)
	auth, authSeq := e.bootstrap(t, "txn-boot")

	const pi = uint16(7)
	adm := testF105Admission(e, auth, authSeq, "txn-occupant", pi, nil)
	tr, err := mqttAdmitPublishTransition(adm)
	if err != nil {
		done()
		t.Fatalf("assembling the occupant's admission failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(tr); err != nil {
		done()
		t.Fatalf("the occupant's admission was refused: %v", err)
	}
	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, pi)
	occ, smsg, lerr := e.asm.jsa.loadInflightRecord(regSubj)
	if lerr != nil || smsg == nil {
		done()
		t.Fatalf("the occupant does not load back: %v", lerr)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	return &testF105ReplEnv{
		testF105Env: e, auth: al.rec, authSeq: al.seq, pi: pi,
		regSubj: regSubj, occ: occ, occSeq: smsg.Sequence, occLin: occ.Lin,
	}, done
}

// markerSeq returns the occupant's standing unsent-marker sequence, or 0 when it is absent.
func (r *testF105ReplEnv) markerSeq(t *testing.T) uint64 {
	t.Helper()
	smsg, err := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName, r.regSubj+"."+mqttInflightMarkerSuffix)
	if err != nil {
		return 0
	}
	return smsg.Sequence
}

// deleteMarker removes the occupant's marker, which is what a transmission does — the send
// intent deletes it BEFORE the bytes leave, so its absence means POSSIBLY ATTEMPTED.
func (r *testF105ReplEnv) deleteMarker(t *testing.T) {
	t.Helper()
	seq := r.markerSeq(t)
	if seq == 0 {
		t.Fatalf("the fixture has no marker to delete")
	}
	if err := r.asm.deleteInflightMarker(mqttMarkerCallerSender, r.regSubj, seq, true); err != nil {
		t.Fatalf("deleting the occupant's marker failed: %v", err)
	}
}

// replacingRegister builds the member that displaces the occupant. lin is the lineage it
// claims, qos the QoS it writes, and both are parameters because both are refusal oracles.
func (r *testF105ReplEnv) replacingRegister(t *testing.T, lin string, qos byte) mqttTransitionMember {
	t.Helper()
	rec := &mqttInflightRecord{
		State: mqttInflightPublish, PI: r.pi, Dur: r.occ.Dur, Seq: r.occ.Seq, QoS: qos,
		Nonce: mqttNewInflightNonce(), Claim: r.occ.Claim, ClaimSeq: r.occ.ClaimSeq,
		SourceAckReply: r.occ.SourceAckReply,
	}
	if lin == _EMPTY_ {
		rec.Lin = rec.Nonce
	} else {
		rec.Lin = lin
	}
	body, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshalling the replacing register failed: %v", err)
	}
	return mqttTransitionMember{subj: r.regSubj, expect: r.occSeq, body: body}
}

// freshMarkerFor is the marker a PAIR replacement mints for its own allocation, replacing the
// occupant's at that marker's observed sequence.
func (r *testF105ReplEnv) freshMarkerFor(t *testing.T, reg mqttTransitionMember, markerSeq uint64) mqttTransitionMember {
	t.Helper()
	var rec mqttInflightRecord
	if err := json.Unmarshal(reg.body, &rec); err != nil {
		t.Fatalf("reading back the replacing register failed: %v", err)
	}
	body, err := json.Marshal(&mqttInflightMarker{Nonce: rec.Nonce})
	if err != nil {
		t.Fatalf("marshalling the replacement marker failed: %v", err)
	}
	return mqttTransitionMember{
		subj: r.regSubj + "." + mqttInflightMarkerSuffix, expect: markerSeq, body: body,
	}
}

// exchangeMember builds an exchange member: create-only when expect is 0, an advance to the
// given phase otherwise.
func (r *testF105ReplEnv) exchangeMember(t *testing.T, nonce string, phase mqttExchangePhase,
	expect uint64, admitTxn string) mqttTransitionMember {
	t.Helper()
	// AN ADMITTED EXCHANGE NAMES THE TRANSITION ADMITTING IT. An advance keeps whatever its
	// admission wrote, which is why the caller supplies this rather than the fixture assuming
	// one: an obligation nothing authorized is an obligation nobody owes.
	x := &mqttOutboundExchange{
		ID: r.id, Epoch: r.epoch, Gen: testF105Gen, Nonce: nonce,
		PI: r.pi, QoS: r.occ.QoS, Phase: phase,
		AdmitTxn: admitTxn, Tenure: r.auth.Tenure,
	}
	body, err := json.Marshal(x)
	if err != nil {
		t.Fatalf("marshalling the exchange failed: %v", err)
	}
	return mqttTransitionMember{subj: mqttExchangeSubject(getHash(r.id), testF105Gen, nonce), expect: expect, body: body}
}

// submit composes a transition carrying exactly these members and returns the boundary's answer.
func (r *testF105ReplEnv) submit(t *testing.T, txn string, members ...mqttTransitionMember) error {
	t.Helper()
	al := r.asm.jsa.loadOutboundAuthority(r.id, r.epoch)
	tr := testF105AdmitNothing(r.testF105Env, al.rec, al.seq, txn)
	// COVERED, because rule 3 makes an admitted obligation owe a duty entry and these fixtures
	// build their exchange members by hand — they are about replacement, not about discovery.
	// A test specifically about coverage omits it on purpose and asserts the refusal.
	tr.Extra = testF105CoverExchanges(t, r.testF105Env, testF105Gen, members)
	_, err := r.asm.jsa.commitAuthorityTransition(tr)
	return err
}

// TestF105RegisterOnlyReplacementContinuesTheOccupantsObligation is the POSSIBLY-ATTEMPTED
// mode, and it is the one that must SUCCEED.
//
// The send intent deletes the unsent marker before the bytes leave, so an authoritatively
// absent marker does not say "transmitted" — it says the client MAY already hold this
// exchange. The obligation therefore continues: the new register inherits the occupant's
// lineage and its QoS, and the batch admits no exchange, because admitting one would put a
// second obligation and a second charge on an identifier the client is already holding.
func TestF105RegisterOnlyReplacementContinuesTheOccupantsObligation(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-repl-continue")
	defer done()

	r.deleteMarker(t)

	reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
	if err := r.submit(t, "txn-inherit", reg); err != nil {
		t.Fatalf("a register-only replacement inheriting its occupant's obligation was refused: %v", err)
	}

	// The register moved, and it carries the occupant's lineage.
	cur, smsg, lerr := r.asm.jsa.loadInflightRecord(r.regSubj)
	if lerr != nil || smsg == nil {
		t.Fatalf("the replacing register does not load back: %v", lerr)
	}
	if smsg.Sequence == r.occSeq {
		t.Fatalf("the replacement did not move the register")
	}
	if cur.Lin != r.occLin {
		t.Fatalf("the replacing register carries lineage %q rather than the occupant's %q", cur.Lin, r.occLin)
	}
	if cur.QoS != r.occ.QoS {
		t.Fatalf("the replacing register carries QoS %d rather than the occupant's %d", cur.QoS, r.occ.QoS)
	}
	// AND NO MARKER WAS MINTED. A marker here would describe an allocation the client may
	// already be holding as unsent, which is [MQTT-3.3.1-1] lying in the forbidden direction.
	if seq := r.markerSeq(t); seq != 0 {
		t.Fatalf("the register-only replacement minted an unsent marker at sequence %d", seq)
	}
}

// TestF105PairReplacementEndsThePredecessor is the PROVEN-UNSENT mode.
//
// A marker naming the occupant is proof nothing ever went out under it, so there is no client
// obligation to inherit: the identifier goes to a FRESH exchange.
//
// What the batch owes is that the predecessor is VALIDLY ENDED IN THE RESULTING STATE —
// already ended, or ended by this transaction — not that an ending member appears. That
// distinction is not pedantic: mqttExchangePhaseEnded (outbound_exchange.go:130) is true only
// for Terminal and Rejected, so BOTH pending-drain phases still HOLD the identifier, and
// checkIdentifierExclusivity refuses a replacement that leaves one standing. This test ends it
// here; TestF105PairReplacementPredecessorDispositions walks the other cases.
func TestF105PairReplacementEndsThePredecessor(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-repl-pair")
	defer done()

	mseq := r.markerSeq(t)
	if mseq == 0 {
		t.Fatalf("the fixture's occupant has no unsent marker")
	}

	reg := r.replacingRegister(t, _EMPTY_, r.occ.QoS)
	var rec mqttInflightRecord
	if err := json.Unmarshal(reg.body, &rec); err != nil {
		t.Fatalf("reading back the replacing register failed: %v", err)
	}
	mk := r.freshMarkerFor(t, reg, mseq)
	admit := r.exchangeMember(t, rec.Lin, mqttExchangePublish, 0, "txn-pair")
	end := r.exchangeMember(t, r.occLin, mqttExchangeRejected, r.exchangeSeq(t, r.occLin), "txn-occupant")

	if err := r.submit(t, "txn-pair", reg, mk, admit, end); err != nil {
		t.Fatalf("a pair replacement ending its predecessor was refused: %v", err)
	}
	cur, _, lerr := r.asm.jsa.loadInflightRecord(r.regSubj)
	if lerr != nil || cur == nil {
		t.Fatalf("the replacing register does not load back: %v", lerr)
	}
	if cur.Lin != rec.Lin {
		t.Fatalf("the pair replacement did not install its fresh lineage")
	}
}

// TestF105PairReplacementPredecessorDispositions walks what the predecessor's own phase does
// to a pair replacement, because "ended" is a narrower set than it reads as.
//
// mqttExchangePhaseEnded is true for Terminal and Rejected ONLY. Both pending-drain phases
// still hold the identifier — an action acquired earlier may still emit under it — so a
// replacement that leaves one standing is refused even though the phase sounds final.
func TestF105PairReplacementPredecessorDispositions(t *testing.T) {
	// pairOver composes a legal pair replacement whose predecessor is first advanced to the
	// given phase, and returns the boundary's answer.
	pairOver := func(t *testing.T, clientID string, phase mqttExchangePhase, endInBatch bool) error {
		t.Helper()
		r, done := testF105NewReplEnv(t, clientID)
		defer done()
		mseq := r.markerSeq(t)
		// WALKED, NOT JUMPED. A QoS 2 exchange has a protocol: PUBLISH reaches a terminal
		// phase only through PUBREC and the owed release, because there is no acknowledgement
		// that ends it sooner and admitting one would discard a release the client is owed. A
		// fixture that jumped straight to the target phase was refused by that rule and
		// tested nothing about replacement.
		var path []mqttExchangePhase
		switch phase {
		case mqttExchangePublish:
		case mqttExchangeRejected, mqttExchangeRejectedPendingDrain:
			path = []mqttExchangePhase{phase}
		case mqttExchangeTerminal, mqttExchangeTerminalPendingDrain:
			path = []mqttExchangePhase{mqttExchangePubRecObserved, phase}
		default:
			t.Fatalf("the fixture has no path to phase %d", int(phase))
		}
		for i, step := range path {
			if _, err := r.asm.jsa.opAdvanceExchange(r.id, r.epoch,
				fmt.Sprintf("txn-pre-%s-%d", clientID, i), "op-pre", testF105Gen, r.occLin, step); err != nil {
				t.Fatalf("advancing the predecessor to phase %d failed: %v", int(step), err)
			}
		}
		reg := r.replacingRegister(t, _EMPTY_, r.occ.QoS)
		var rec mqttInflightRecord
		if err := json.Unmarshal(reg.body, &rec); err != nil {
			t.Fatalf("reading back the replacing register failed: %v", err)
		}
		members := []mqttTransitionMember{
			reg, r.freshMarkerFor(t, reg, mseq),
			r.exchangeMember(t, rec.Lin, mqttExchangePublish, 0, "txn-disp"),
		}
		if endInBatch {
			members = append(members,
				r.exchangeMember(t, r.occLin, mqttExchangeRejected, r.exchangeSeq(t, r.occLin), "txn-occupant"))
		}
		return r.submit(t, "txn-disp", members...)
	}

	// A predecessor ALREADY ended needs no ending member: it holds the identifier no longer.
	t.Run("already rejected, nothing ends it in the batch", func(t *testing.T) {
		if err := pairOver(t, "f105-pred-rejected", mqttExchangeRejected, false); err != nil {
			t.Fatalf("a replacement over an already-ended predecessor was refused: %v", err)
		}
	})
	t.Run("already terminal, nothing ends it in the batch", func(t *testing.T) {
		if err := pairOver(t, "f105-pred-terminal", mqttExchangeTerminal, false); err != nil {
			t.Fatalf("a replacement over an already-ended predecessor was refused: %v", err)
		}
	})

	// PENDING DRAIN IS NOT ENDED. Output acquired before may still execute under this
	// identifier, so the obligation still holds it.
	t.Run("terminal pending drain still holds the identifier", func(t *testing.T) {
		if err := pairOver(t, "f105-pred-tpd", mqttExchangeTerminalPendingDrain, false); err == nil {
			t.Fatalf("a replacement landed while a terminal-pending-drain predecessor held the identifier")
		}
	})
	t.Run("rejected pending drain still holds the identifier", func(t *testing.T) {
		if err := pairOver(t, "f105-pred-rpd", mqttExchangeRejectedPendingDrain, false); err == nil {
			t.Fatalf("a replacement landed while a rejected-pending-drain predecessor held the identifier")
		}
	})

	// And a LIVE predecessor ended BY THIS BATCH is the ordinary shape, already covered by
	// TestF105PairReplacementEndsThePredecessor; asserted here too so the matrix is whole.
	t.Run("live, ended by this batch", func(t *testing.T) {
		if err := pairOver(t, "f105-pred-batchend", mqttExchangePublish, true); err != nil {
			t.Fatalf("a replacement ending its live predecessor in the batch was refused: %v", err)
		}
	})
}

// TestF105ReplacementRequiresARealObligation is Codex consult-2 item 6: the replacement path
// must not rest on a lineage STRING.
//
// checkPairBindings delegates "every allocation has an obligation" to the replacement
// validator for a replacing register, so the validator owes the proof: the occupant's lineage
// must name a readable exchange that agrees with the occupant. A readable ORPHAN register
// whose exchange is gone answers to nobody and excludes nobody.
func TestF105ReplacementRequiresARealObligation(t *testing.T) {
	t.Run("the occupant's obligation does not exist", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-orphanreg")
		defer done()
		r.deleteMarker(t)
		// Delete the obligation, leaving a readable register naming a lineage nothing backs.
		xseq := r.exchangeSeq(t, r.occLin)
		if err := r.asm.jsa.deleteMsg(mqttInflightStreamName, xseq, true); err != nil {
			t.Fatalf("deleting the predecessor obligation failed: %v", err)
		}
		if err := r.submit(t, "txn-orphanreg", r.replacingRegister(t, r.occLin, r.occ.QoS)); err == nil {
			t.Fatalf("a register with no obligation behind it was replaced")
		}
	})

	t.Run("a continued obligation that has already ended", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-endedcont")
		defer done()
		r.deleteMarker(t)
		if _, err := r.asm.jsa.opAdvanceExchange(r.id, r.epoch, "txn-end", "op-end",
			testF105Gen, r.occLin, mqttExchangeRejected); err != nil {
			t.Fatalf("ending the obligation failed: %v", err)
		}
		// Rewriting the register of an exchange that ended re-opens a completed obligation
		// under a new allocation.
		if err := r.submit(t, "txn-endedcont", r.replacingRegister(t, r.occLin, r.occ.QoS)); err == nil {
			t.Fatalf("an already-ended obligation was continued by rewriting its register")
		}
	})

	t.Run("a continuation that advances the slot", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-phasejump")
		defer done()
		r.deleteMarker(t)
		reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
		var rec mqttInflightRecord
		if err := json.Unmarshal(reg.body, &rec); err != nil {
			t.Fatalf("reading back the replacing register failed: %v", err)
		}
		// mqttInflightPubRelIntended, NOT pubrel_ready: the shape validator refuses a
		// past-PUBREC slot naming no canonical PUBREL record, and a fixture using one is
		// refused for that unrelated reason and never reaches the rule under test. This is
		// the one past-PUBREC phase that legally names none.
		rec.State = mqttInflightPubRelIntended
		body, merr := json.Marshal(&rec)
		if merr != nil {
			t.Fatalf("marshalling failed: %v", merr)
		}
		reg.body = body
		// A replacement RE-ALLOCATES a PUBLISH. Moving the slot past PUBREC is the business
		// of the transition that authorizes the advance, and checkPairBindings' PUBLISH rule
		// does not reach here because no exchange is admitted beside it.
		if err := r.submit(t, "txn-phasejump", reg); err == nil {
			t.Fatalf("a register-only replacement advanced the slot with no authorizing transition")
		}
	})
}

// TestF105ReplacementRefusesAPastPubRecOccupant is consult-2 item 5: the disposition gate.
//
// mqttSlotAdmitsDisposition is an acceptance boundary this conversion RELAXES — the boundary
// now permits replacement at all — so the gate needs its own fixture rather than a note in a
// ledger. A past-PUBREC slot is the identifier's continuing authority: replacing it with a
// fresh PUBLISH record destroys its canonical PUBREL identity and makes an answered payload
// eligible for retransmission.
//
// FOCUSED, and deliberately not end-to-end. Other validators independently refuse this batch,
// so this test drives checkRegisterReplacements DIRECTLY and asserts the gate's own refusal;
// claiming an end-to-end mutation kill here would be claiming a kill another guard masks.
func TestF105ReplacementRefusesAPastPubRecOccupant(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-repl-pastpubrec")
	defer done()
	r.deleteMarker(t)

	// Walk the obligation and its slot past PUBREC together, so the occupant is the shape
	// the gate exists for and its exchange agrees with it.
	for i, step := range []mqttExchangePhase{mqttExchangePubRecObserved} {
		if _, err := r.asm.jsa.opAdvanceExchange(r.id, r.epoch,
			fmt.Sprintf("txn-pastpubrec-%d", i), "op-pre", testF105Gen, r.occLin, step); err != nil {
			t.Fatalf("advancing the obligation to phase %d failed: %v", int(step), err)
		}
	}
	occ := *r.occ
	// pubrel_intended is the ONE past-PUBREC phase that legally names no canonical PUBREL
	// record — it is written BEFORE the store, so a crash between them leaves a slot saying
	// "past PUBREC, canonical record OWED". Using it keeps the fixture to a shape the writer
	// actually accepts, rather than one the shape validator refuses for an unrelated reason.
	occ.State = mqttInflightPubRelIntended
	newSeq, serr := r.asm.jsa.storeInflightCAS(r.regSubj, r.occSeq, &occ)
	if serr != nil {
		t.Fatalf("moving the occupant past PUBREC failed: %v", serr)
	}
	r.occ, r.occSeq = &occ, newSeq

	reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
	err := r.asm.jsa.checkRegisterReplacements(r.attempt("txn-pastpubrec"), []mqttTransitionMember{reg})
	if err == nil {
		t.Fatalf("the disposition gate admitted a replacement over a past-PUBREC occupant")
	}
	if !strings.Contains(err.Error(), "admits no replacement") {
		t.Fatalf("the refusal did not come from the disposition gate: %v", err)
	}
	// AND THE OCCUPANT IS UNTOUCHED. A refusal that moved durable state would be a different
	// defect wearing a passing test.
	cur, smsg, lerr := r.asm.jsa.loadInflightRecord(r.regSubj)
	if lerr != nil || smsg == nil || smsg.Sequence != newSeq || cur.State != mqttInflightPubRelIntended {
		t.Fatalf("the refused replacement moved the standing occupant")
	}
}

// TestF105ReplacementExpectationNamesTheInspectedMarker is consult-2 item 7.
//
// The store's CAS establishes "the marker at commit matches the supplied expectation". This
// establishes "the supplied expectation names the marker whose body this validator inspected".
// Those are different facts, and the difference is the read-to-commit interval: a create-only
// marker expectation is invalid while the inspected marker stands, and becomes acceptable to
// the store once that marker is deleted — so without this comparison a transaction can
// validate PRESENCE and commit against ABSENCE.
//
// Driven DIRECTLY at the validator, so the store CAS cannot supply the refusal. A first
// mutation run read the overlap on a stationary store as redundancy; it is not.
func TestF105ReplacementExpectationNamesTheInspectedMarker(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-repl-expectbind")
	defer done()
	mseq := r.markerSeq(t)
	if mseq == 0 {
		t.Fatalf("the fixture's occupant has no unsent marker")
	}
	reg := r.replacingRegister(t, _EMPTY_, r.occ.QoS)
	var rec mqttInflightRecord
	if err := json.Unmarshal(reg.body, &rec); err != nil {
		t.Fatalf("reading back the replacing register failed: %v", err)
	}
	admit := r.exchangeMember(t, rec.Lin, mqttExchangePublish, 0, "txn-bind")

	// THE POSITIVE CONTROL FIRST, so the illegal variation below differs in ONE thing. Without
	// it the variation would be refused by whichever other rule the control also violates, and
	// would prove nothing about the binding.
	if err := r.asm.jsa.checkRegisterReplacements(r.attempt("txn-bind"),
		[]mqttTransitionMember{reg, r.freshMarkerFor(t, reg, mseq), admit}); err != nil {
		t.Fatalf("a pair replacement naming the standing marker was refused: %v", err)
	}

	// The same batch with a CREATE-ONLY marker expectation: legal as a member, accepted by
	// the store the moment the inspected marker is deleted, and refused here because it does
	// not name the marker this validation decided on.
	if err := r.asm.jsa.checkRegisterReplacements(r.attempt("txn-bind"),
		[]mqttTransitionMember{reg, r.freshMarkerFor(t, reg, 0), admit}); err == nil {
		t.Fatalf("a replacement validated against a standing marker while expecting none")
	}
}

// attempt is the transition description the boundary checks are driven with directly, for the
// tests that must not let the store's CAS supply their refusal.
func (r *testF105ReplEnv) attempt(txn string) *mqttTransitionAttempt {
	al := r.asm.jsa.loadOutboundAuthority(r.id, r.epoch)
	return &mqttTransitionAttempt{
		ID: r.id, Epoch: r.epoch, Txn: txn, Op: "op-" + txn,
		Kind: mqttTransitionAdmitExchange, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
		Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
	}
}

// occExchange is the occupant's own obligation, as it durably stands.
func (r *testF105ReplEnv) occExchange(t *testing.T) *mqttOutboundExchange {
	t.Helper()
	x, _, err := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, testF105Gen, r.occLin)
	if err != nil || x == nil {
		t.Fatalf("the occupant's obligation does not load back: %v", err)
	}
	return x
}

// exchangeSeq is the sequence an exchange currently stands at, which an advance must CAS.
func (r *testF105ReplEnv) exchangeSeq(t *testing.T, nonce string) uint64 {
	t.Helper()
	smsg, err := r.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttExchangeSubject(getHash(r.id), testF105Gen, nonce))
	if err != nil {
		t.Fatalf("the exchange %q does not load back: %v", nonce, err)
	}
	return smsg.Sequence
}

// TestF105ReplacementRefusalsAreTheBoundarys walks every way a replacement can be composed
// wrongly while every one of its members is individually legal and lands at a sequence the
// STORE would accept.
//
// Each arm is a mutation oracle, and they are separate sub-tests because a single fixture
// carrying several defects would be killed by whichever check fires first — which is how a
// mutation that removes a later check survives.
func TestF105ReplacementRefusalsAreTheBoundarys(t *testing.T) {
	t.Run("register-only with a fresh lineage", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-freshlin")
		defer done()
		r.deleteMarker(t)
		// An absent marker means POSSIBLY ATTEMPTED. Minting a fresh lineage here abandons
		// an obligation the client may be holding and starts a second one on its identifier.
		if err := r.submit(t, "txn-freshlin", r.replacingRegister(t, _EMPTY_, r.occ.QoS)); err == nil {
			t.Fatalf("a register-only replacement minted a fresh lineage over a possibly-attempted occupant")
		}
	})

	t.Run("register-only that also admits an exchange", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-dupadmit")
		defer done()
		r.deleteMarker(t)
		reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
		admit := r.exchangeMember(t, r.occLin, mqttExchangePublish, 0, "txn-dupadmit")
		// The occupant's obligation is being CONTINUED. Admitting one for the same lineage
		// would duplicate its charge and its dependency edges.
		if err := r.submit(t, "txn-dupadmit", reg, admit); err == nil {
			t.Fatalf("a continued obligation was admitted a second time")
		}
	})

	t.Run("pair replacement leaving its predecessor standing", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-orphan")
		defer done()
		mseq := r.markerSeq(t)
		reg := r.replacingRegister(t, _EMPTY_, r.occ.QoS)
		var rec mqttInflightRecord
		if err := json.Unmarshal(reg.body, &rec); err != nil {
			t.Fatalf("reading back the replacing register failed: %v", err)
		}
		// Register, marker and a fresh admission — all legal, and the predecessor exchange
		// is simply never mentioned. THIS is the two-obligations-on-one-identifier state,
		// and it is invisible to any rule about the batch's own members.
		if err := r.submit(t, "txn-orphan", reg, r.freshMarkerFor(t, reg, mseq),
			r.exchangeMember(t, rec.Lin, mqttExchangePublish, 0, "txn-orphan")); err == nil {
			t.Fatalf("a pair replacement left its predecessor's obligation standing")
		}
	})

	t.Run("pair replacement inheriting a lineage", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-pairinherit")
		defer done()
		mseq := r.markerSeq(t)
		reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
		if err := r.submit(t, "txn-pairinherit", reg, r.freshMarkerFor(t, reg, mseq)); err == nil {
			t.Fatalf("a proven-unsent replacement inherited an obligation that was never transmitted")
		}
	})

	t.Run("a QoS the occupant does not hold", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-qos")
		defer done()
		r.deleteMarker(t)
		// The occupant's QoS is the only durable trace of what the client may be holding,
		// and for 2 -> 1 taking ours licenses the slot deletion that abandons the handshake.
		if err := r.submit(t, "txn-qos", r.replacingRegister(t, r.occLin, 1)); err == nil {
			t.Fatalf("a replacement overwrote its occupant's QoS")
		}
	})

	t.Run("a marker overwrite with no register replacing its slot", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-lonemarker")
		defer done()
		mseq := r.markerSeq(t)
		reg := r.replacingRegister(t, _EMPTY_, r.occ.QoS)
		// The marker alone, at the occupant marker's real sequence. Standing alone it
		// describes a transmitted allocation as unsent again.
		if err := r.submit(t, "txn-lonemarker", r.freshMarkerFor(t, reg, mseq)); err == nil {
			t.Fatalf("an unsent marker was rewritten with no register replacing its slot")
		}
	})

	t.Run("an occupant naming another delivery", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-otherdelivery")
		defer done()
		r.deleteMarker(t)
		reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
		var rec mqttInflightRecord
		if err := json.Unmarshal(reg.body, &rec); err != nil {
			t.Fatalf("reading back the replacing register failed: %v", err)
		}
		rec.Seq = r.occ.Seq + 99
		testRegisterSourceReply(&rec)
		body, merr := json.Marshal(&rec)
		if merr != nil {
			t.Fatalf("marshalling failed: %v", merr)
		}
		reg.body = body
		// Every replacement arm is scoped to the SAME delivery. Another delivery's occupant
		// is quarantined, reaped or abandoned, and never overwritten by an admission.
		if err := r.submit(t, "txn-otherdelivery", reg); err == nil {
			t.Fatalf("a replacement overwrote an occupant naming another delivery")
		}
	})

	t.Run("register-only while the occupant's marker still stands", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-modelies")
		defer done()
		// The marker is NOT deleted, so the occupant is PROVEN UNSENT and the disposition is
		// PAIR. A register-only batch is individually legal in every member, and landing it
		// would orphan a marker naming the occupant's nonce beside a register carrying a new
		// one. The mode is the store's answer, not the batch's shape.
		if err := r.submit(t, "txn-modelies", r.replacingRegister(t, r.occLin, r.occ.QoS)); err == nil {
			t.Fatalf("a register-only replacement landed while a marker named its occupant")
		}
	})

	t.Run("a pair replacement over a marker naming another allocation", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-strayemarker")
		defer done()
		mseq := r.markerSeq(t)
		// The marker on the subject is rewritten to name SOME OTHER allocation, at its own
		// sequence, so a pair replacement's CAS still passes on sequence alone. A marker
		// about a different exchange proves nothing about this occupant.
		stray, merr := json.Marshal(&mqttInflightMarker{Nonce: mqttNewInflightNonce()})
		if merr != nil {
			t.Fatalf("marshalling the stray marker failed: %v", merr)
		}
		_ = mseq
		if _, err := r.asm.jsa.storeMsg(r.regSubj+"."+mqttInflightMarkerSuffix, -1, stray); err != nil {
			t.Fatalf("planting the stray marker failed: %v", err)
		}
		nseq := r.markerSeq(t)
		reg := r.replacingRegister(t, _EMPTY_, r.occ.QoS)
		var rec mqttInflightRecord
		if err := json.Unmarshal(reg.body, &rec); err != nil {
			t.Fatalf("reading back the replacing register failed: %v", err)
		}
		// The obligation IS admitted and the predecessor IS ended, so every other rule this
		// batch meets is satisfied and only the marker's identity can refuse it. Without
		// those two members the admission check fires first and this arm proves nothing —
		// which is what its first mutation run showed.
		if err := r.submit(t, "txn-stray", reg, r.freshMarkerFor(t, reg, nseq),
			r.exchangeMember(t, rec.Lin, mqttExchangePublish, 0, "txn-stray"),
			r.exchangeMember(t, r.occLin, mqttExchangeRejected, r.exchangeSeq(t, r.occLin), "txn-occupant")); err == nil {
			t.Fatalf("a pair replacement took a marker naming another allocation as proof " +
				"its occupant was never transmitted")
		}
	})

	t.Run("a replacing register admitting nothing", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-noobligation")
		defer done()
		mseq := r.markerSeq(t)
		// PAIR mode with a fresh lineage and no exchange admitted. checkPairBindings exempts
		// a replacing register from its admission requirement — that exemption is what lets a
		// CONTINUED obligation through — so this arm is what keeps the exemption from being a
		// hole: an allocation with no obligation behind it answers to nothing.
		reg := r.replacingRegister(t, _EMPTY_, r.occ.QoS)
		if err := r.submit(t, "txn-noobligation", reg, r.freshMarkerFor(t, reg, mseq)); err == nil {
			t.Fatalf("a replacing register with a fresh lineage admitted no obligation")
		}
	})

	t.Run("an unreadable marker beside the occupant", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-opaque")
		defer done()
		if _, err := r.asm.jsa.storeMsg(r.regSubj+"."+mqttInflightMarkerSuffix, -1,
			[]byte("not a marker")); err != nil {
			t.Fatalf("planting the unreadable marker failed: %v", err)
		}
		// Neither disposition is established: the occupant may have been transmitted or may
		// not. The identifier is WITHHELD — it becomes a marker-opaque reservation — rather
		// than replaced under a guess.
		reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
		if err := r.submit(t, "txn-opaque", reg); err == nil {
			t.Fatalf("a replacement landed beside a marker this broker cannot read")
		}
	})

	t.Run("a stale sequence", func(t *testing.T) {
		r, done := testF105NewReplEnv(t, "f105-repl-stale")
		defer done()
		r.deleteMarker(t)
		reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
		reg.expect = r.occSeq - 1
		// A decision taken against a record that is not the one standing. The store's CAS
		// would refuse this too; the boundary refuses it FIRST, and for the reason that
		// matters — the disposition was chosen against an occupant nobody re-read.
		if err := r.submit(t, "txn-stale", reg); err == nil {
			t.Fatalf("a replacement committed at a sequence its occupant does not stand at")
		}
	})
}

// TestF105RegisterOnlyReplacementNeedsAPublishablePhase is the gap the first bounded
// implementation review found, and it is NOT the same case as
// TestF105ReplacementRefusesAPastPubRecOccupant above.
//
// That test moves the occupant's REGISTER past PUBREC as well, so mqttSlotAdmitsDisposition
// refuses it and the phase never comes into question. The hole was the batch where the two
// disagree: the occupant's register still reads PUBLISH — so every slot-shaped check passes —
// while its EXCHANGE has legitimately advanced to PubRecObserved by its own authorized
// transition. A register-only replacement then installs a fresh PUBLISH allocation for an
// obligation the client has already answered for, and the batch needs no exchange member at
// all, so nothing else in the boundary looks at the phase.
//
// Three phases were admitted by the rule this replaces, which asked only whether the exchange
// had ENDED: PubRecObserved and both pending-drain phases. All three are live and none of them
// may carry another PUBLISH.
//
// DRIVEN DIRECTLY at checkRegisterReplacements, for the same reason as the test above: other
// validators refuse neighbouring shapes, and a kill claimed end-to-end would be one another
// guard masks.
func TestF105RegisterOnlyReplacementNeedsAPublishablePhase(t *testing.T) {
	for _, tc := range []struct {
		name string
		// path is walked STEP BY STEP, because phase advancement is itself validated and a
		// jump from the publish straight to a drain is refused by the advance rather than by
		// the rule under test.
		path []mqttExchangePhase
		want bool // true: the boundary must refuse
	}{
		// THE POSITIVE CONTROL. An obligation still at its publish is exactly what a
		// possibly-attempted replacement continues, and it must pass — without this row the
		// three below would be satisfied by a validator that refused everything.
		{"still at its publish", nil, false},
		{"past PUBREC", []mqttExchangePhase{mqttExchangePubRecObserved}, true},
		{"terminal pending drain", []mqttExchangePhase{
			mqttExchangePubRecObserved,
			mqttExchangeTerminalPendingDrain}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, done := testF105NewReplEnv(t, "f105-repl-phase")
			defer done()
			// No marker: absence means POSSIBLY ATTEMPTED, which is what selects
			// register-only. The occupant's register is left in PUBLISH throughout — that is
			// the whole point, and moving it would be the other test.
			r.deleteMarker(t)

			for i, step := range tc.path {
				if _, err := r.asm.jsa.opAdvanceExchange(r.id, r.epoch,
					fmt.Sprintf("txn-phase-pre-%d", i), "op-pre", testF105Gen, r.occLin,
					step); err != nil {
					t.Fatalf("advancing the obligation to phase %d failed: %v", int(step), err)
				}
			}

			reg := r.replacingRegister(t, r.occLin, r.occ.QoS)
			err := r.asm.jsa.checkRegisterReplacements(r.attempt("txn-phase"), []mqttTransitionMember{reg})
			if !tc.want {
				if err != nil {
					t.Fatalf("the boundary refused a continuation of an obligation still at its "+
						"publish: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("the boundary admitted a fresh PUBLISH allocation for an obligation at "+
					"phase %d", int(tc.path[len(tc.path)-1]))
			}
			// THE REFUSAL MUST BE THIS RULE'S. A refusal arriving from some other validator
			// would leave the phase rule unproven while the test passed.
			if !strings.Contains(err.Error(), "no longer admits") ||
				!strings.Contains(err.Error(), "may not emit another PUBLISH") {
				t.Fatalf("the refusal did not come from the phase rule: %v", err)
			}
			// AND NOTHING DURABLE MOVED.
			cur, smsg, lerr := r.asm.jsa.loadInflightRecord(r.regSubj)
			if lerr != nil || smsg == nil || smsg.Sequence != r.occSeq || cur.Lin != r.occLin {
				t.Fatalf("the refused replacement moved the standing occupant")
			}
		})
	}
}

// TestF105PairSupersedesProvenUnsentPredecessor is the TRUTHFUL pair ending: a proven-unsent
// occupant's PUBLISH obligation is ended as mqttExchangeSupersededUnsent when its identifier is
// re-allocated, never Terminal (completion) or Rejected (client refusal), which would fabricate a
// client answer that never came for a delivery that never left the broker.
func TestF105PairSupersedesProvenUnsentPredecessor(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-superseded")
	defer done()

	adm := r.replacingFor(t, mqttReplacePair, "txn-superseded")
	adm.Exchange = &mqttOutboundExchange{
		ID: r.id, Epoch: r.epoch, Gen: testF105Gen, Nonce: adm.Register.Lin,
		PI: r.pi, QoS: r.occ.QoS, Phase: mqttExchangePublish,
		AdmitTxn: "txn-superseded", Tenure: adm.Auth.Tenure,
	}
	pred := *r.occExchange(t)
	pred.Phase = mqttExchangeSupersededUnsent
	adm.Predecessor, adm.PredecessorSeq = &pred, r.exchangeSeq(t, r.occLin)

	tr, err := mqttReplacePublishTransition(adm)
	if err != nil {
		t.Fatalf("composing the superseding pair replacement failed: %v", err)
	}
	if _, err := r.asm.jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("the superseding pair replacement was refused by the boundary: %v", err)
	}
	cur, _, lerr := r.asm.jsa.loadInflightRecord(r.regSubj)
	if lerr != nil || cur == nil || cur.Lin != adm.Register.Lin {
		t.Fatalf("the pair replacement did not install its fresh lineage")
	}
	px, _, perr := r.asm.jsa.loadOutboundExchange(r.id, r.epoch, testF105Gen, r.occLin)
	if perr != nil || px == nil {
		t.Fatalf("the superseded predecessor does not load back: %v", perr)
	}
	if px.Phase != mqttExchangeSupersededUnsent {
		t.Fatalf("the predecessor stands at phase %d, not superseded-unsent", px.Phase)
	}
}

// TestF105BoundaryRefusesALoneSupersession proves the rule is the write boundary's, not just the
// composer's convention: a mqttExchangeSupersededUnsent ending is authorized ONLY as the
// predecessor half of a pair replacement that re-allocates the identifier to a fresh obligation.
// A lone supersession would end a proven-unsent obligation with no client answer and no
// successor, silently dropping the delivery.
func TestF105BoundaryRefusesALoneSupersession(t *testing.T) {
	r, done := testF105NewReplEnv(t, "f105-lone-superseded")
	defer done()

	pred := *r.occExchange(t)
	pred.Phase = mqttExchangeSupersededUnsent
	predBody, err := json.Marshal(&pred)
	if err != nil {
		t.Fatalf("marshalling the superseded predecessor failed: %v", err)
	}
	tr := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: r.id, Epoch: r.epoch, Txn: "txn-lone", Op: "op-lone",
			Kind: mqttTransitionAdmitExchange, PrevTxn: r.auth.OwnerTxn, Rev: r.auth.Rev + 1,
			Gen: testF105Gen, Tenure: r.auth.Tenure, OwnerTxn: r.auth.OwnerTxn,
		},
		Prev: r.auth, PrevSeq: r.authSeq,
		Auth: &mqttOutboundAuthority{
			ID: r.id, Epoch: r.epoch, Gen: testF105Gen, Tenure: r.auth.Tenure,
			State: mqttAuthorityServing, Rev: r.auth.Rev + 1, Txn: "txn-lone", OwnerTxn: r.auth.OwnerTxn,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttExchangeSubject(getHash(r.id), testF105Gen, r.occLin),
			expect: r.exchangeSeq(t, r.occLin), body: predBody,
		}},
	}
	if _, err := r.asm.jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatal("a lone supersession that drops the delivery was accepted by the boundary")
	} else if !strings.Contains(err.Error(), "without admitting a fresh obligation") {
		t.Fatalf("the lone supersession was refused for the wrong reason: %v", err)
	}
}
