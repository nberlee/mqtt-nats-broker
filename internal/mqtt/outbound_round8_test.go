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
	"time"

	"github.com/nats-io/nuid"
)

// The four findings of implementation review 8, each against the store.
//
// Three of them are the same class the file beside this one exists for — a rule enforced by
// the helper that composed a write rather than by the write — and the fourth is a protocol
// case the phase relation had no legal move for.

// TestF105RefusedPublishReceivedHasAnEndingOfItsOwn is finding 4.
//
// A PUBREC carrying a reason code >= 0x80 says the client will NOT take the message
// ([MQTT-4.3.3]): the sender must not send a PUBREL, and the exchange is over. The phase
// relation had no move for that. Its only route out of a publish was PUBREC-observed, so the
// only way to end such an exchange durably was to record a successful ownership transfer
// that never happened and then owe a release nothing may emit — or to leave the obligation
// standing forever, holding its charge and its identifier.
func TestF105RefusedPublishReceivedHasAnEndingOfItsOwn(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-pubrec-refused")
	defer done()

	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	// THE REFUSAL ENDS IT, straight from the publish, through the write boundary.
	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-rej", "op-rej",
		cur, curSeq, mqttExchangeRejected)
	if aerr != nil {
		t.Fatalf("assembling the rejection failed: %v", aerr)
	}
	adv = testWithSlotMember(t, &e.asm.jsa, adv, testF105Gen, nonce, mqttExchangeRejected)
	adv = testF105WithHoldReleases(t, e, adv, cur, mqttExchangeRejected)
	if _, cerr := e.asm.jsa.commitAuthorityTransition(adv); cerr != nil {
		t.Fatalf("a refused PUBREC could not end its exchange: %v", cerr)
	}
	end, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
	if end.Phase != mqttExchangeRejected {
		t.Fatalf("the refused exchange stands at phase %d, want rejected", end.Phase)
	}

	// AND IT IS AN ENDING: the resource it depends on is released over it, exactly as over a
	// completion, because nothing further can be emitted for it (F-105 7c-lc: release-owed,
	// then released by a release-holds batch over the stored ending).
	testReleaseOwed(t, e, nonce)
	res, _ := testF105LoadResource(t, e, "inc-src")
	if len(res.Deps) != 0 {
		t.Fatalf("the resource still names %v after its only obligation was refused", res.Deps)
	}

	// AND IT CLAIMS NOTHING ABOUT OWNERSHIP. The prohibition the relation already carried is
	// untouched: a SUCCESSFUL PUBREC still owes a release, so an exchange that observed one
	// can never be talked back into a rejection, and a publish can still not skip to
	// terminal.
	for name, bad := range map[string]struct{ from, to mqttExchangePhase }{
		"an observed PUBREC becoming a refusal":  {mqttExchangePubRecObserved, mqttExchangeRejected},
		"a refusal reopening as an owed release": {mqttExchangeRejected, mqttExchangePubRecObserved},
		"a refusal becoming a completion":        {mqttExchangeRejected, mqttExchangeTerminal},
		"a publish skipping to terminal":         {mqttExchangePublish, mqttExchangeTerminal},
	} {
		if err := mqttExchangePhaseSucceeds(2, bad.from, bad.to); err == nil {
			t.Fatalf("%s was admitted as a legal phase move", name)
		}
	}
	// A QoS 1 exchange has no PUBREC to refuse, so neither rejection phase is a shape it
	// may carry.
	for _, p := range []mqttExchangePhase{mqttExchangeRejected, mqttExchangeRejectedPendingDrain} {
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Nonce: nonce,
			PI: 7, QoS: 1, Phase: p, AdmitTxn: "txn-adm", Tenure: "tenure-txn-1",
		}
		if err := mqttValidateOutboundExchange(x, e.id, e.epoch, testF105Gen, nonce); err == nil {
			t.Fatalf("a QoS 1 exchange carrying phase %d was accepted", p)
		}
	}
}

// TestF105RefusedPublishReceivedWaitsForItsDrain is the other half of finding 4: a refusal that
// arrives while this broker's transmission may still be executing is not recorded until the
// output is over.
//
// That transmission is a PUBLISH whose action token stands on the slot. No batch records a
// refusal beside it: the acknowledgement's slot arrows have no pending-drain ending for a PUBLISH
// slot, so the acknowledgement op refuses it, and an exchange-only batch recording the
// refused-pending-drain phase is refused because an exchange advances only beside its slot.
// The live route waits instead: the terminal attempt returns not-done while a holder may
// still act, and the refusal lands as Rejected beside publish_terminal once the token is gone.
// Recording it at once would let the retransmission escape its own release boundary.
func TestF105RefusedPublishReceivedWaitsForItsDrain(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-pubrec-refused-drain")
	defer done()

	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, subj, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 2, "inc-src")
	jsa := &e.asm.jsa

	// A retransmission this broker acquired is still executing.
	if _, _, err := jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq", Op: "op-acq",
		Gen: auth.Gen, Tenure: auth.Tenure, Nonce: nonce, Acq: reg.Acq + 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242), Register: reg, RegisterSeq: seq, Token: mqttNewInflightNonce(),
	}); err != nil {
		t.Fatalf("acquiring the transmission was refused: %v", err)
	}
	held, hmsg, _ := jsa.loadInflightRecord(subj)

	// The acknowledgement op has no arrow for it: a refusal over a live PUBLISH token is not
	// recorded, and nothing moves.
	for _, to := range []mqttExchangePhase{mqttExchangeRejected, mqttExchangeRejectedPendingDrain} {
		if _, err := jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-rej", Op: "op-rej",
			Gen: auth.Gen, Nonce: nonce, To: to, Acq: held.Acq, Register: held, RegisterSeq: hmsg.Sequence,
		}); err == nil {
			t.Fatalf("a refusal to phase %d was recorded over a live transmission", to)
		}
	}
	// And the batch the retired exchange-only route wrote is refused at the boundary.
	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, nonce)
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-rejd", "op-rejd",
		cur, curSeq, mqttExchangeRejectedPendingDrain)
	if aerr != nil {
		t.Fatalf("assembling the pending-drain rejection failed: %v", aerr)
	}
	if _, cerr := jsa.commitAuthorityTransition(adv); cerr == nil ||
		!strings.Contains(cerr.Error(), "with no slot member beside it") {
		t.Fatalf("a refused-pending-drain exchange recorded with no slot was not refused: %v", cerr)
	}
	if x, _ := testF105LoadExchange(t, e, testF105Gen, nonce); x.Phase != mqttExchangePublish {
		t.Fatalf("a refused refusal moved the exchange to phase %d", x.Phase)
	}
	// The dependency stays held: nothing ended.
	if err := testF105ReleaseDependency(t, e, "inc-src", nonce, "txn-early"); err == nil {
		t.Fatalf("a refusal that was never recorded released its dependency")
	}

	// The PHASE RELATION is unchanged: a refusal drains to refused and never to terminal, and a
	// refusal awaiting its drain still holds its charge. Both are properties of the vocabulary,
	// stated without a batch.
	if err := mqttExchangePhaseSucceeds(2, mqttExchangeRejectedPendingDrain, mqttExchangeTerminal); err == nil {
		t.Fatalf("a refusal drained into a successful completion")
	}
	pend := *cur
	pend.Phase = mqttExchangeRejectedPendingDrain
	if d, cerr := mqttExchangeCharge(&pend, false); cerr != nil || d != mqttChargeHeldPendingDrain {
		t.Fatalf("a refusal awaiting its drain reports charge %d (%v), want HELD PENDING DRAIN", d, cerr)
	}
}

// TestF105MetadataReceiptIsDerivedFromTheBatchItTravelsWith is finding 2.
//
// The freezer accepted any non-empty record body and the writer published whatever receipt
// the caller's struct carried. So an attempt could name proposal P, carry the PREDECESSOR's
// session body — its old write identity, its old generation — and still publish a permanent
// COMMITTED receipt saying P installed a new one. Every later ownership move reads that
// receipt, so the authority would be finalized over a session that never moved.
func TestF105MetadataReceiptIsDerivedFromTheBatchItTravelsWith(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-receipt-binding")
	defer done()

	prev, prevSeq := testF105StandingSession(t, e)

	// The substitutions, each applied to an attempt that is otherwise exactly the one the
	// freezer produced.
	for name, mutate := range map[string]func(*mqttFrozenMetadataAttempt){
		"the predecessor's session body": func(f *mqttFrozenMetadataAttempt) {
			body, err := json.Marshal(prev)
			if err != nil {
				t.Fatalf("marshalling the predecessor record: %v", err)
			}
			f.RecBody = body
		},
		// The generation alone, with the receipt's own write identity kept: otherwise the
		// derived-receipt comparison refuses it first and the generation rule never runs.
		"a session record at another generation": func(f *mqttFrozenMetadataAttempt) {
			rec := &mqttPersistedSession{}
			if err := json.Unmarshal(f.RecBody, rec); err != nil {
				t.Fatalf("reading the frozen record back: %v", err)
			}
			rec.Gen = testF105Gen
			body, err := json.Marshal(rec)
			if err != nil {
				t.Fatalf("marshalling the substitute record: %v", err)
			}
			f.RecBody = body
		},
		"another client's session record": func(f *mqttFrozenMetadataAttempt) {
			rec := &mqttPersistedSession{ID: "somebody-else", Gen: testF105Gen2, WID: nuid.Next()}
			body, err := json.Marshal(rec)
			if err != nil {
				t.Fatalf("marshalling the foreign record: %v", err)
			}
			f.RecBody = body
		},
		"a receipt naming another write identity": func(f *mqttFrozenMetadataAttempt) {
			o := *f.Outcome
			o.SessWID = nuid.Next()
			f.Outcome = &o
		},
		"a receipt naming another proposed generation": func(f *mqttFrozenMetadataAttempt) {
			o := *f.Outcome
			o.ProposedGen = testF105Gen
			f.Outcome = &o
		},
		"an acceptance fact for another client": func(f *mqttFrozenMetadataAttempt) {
			fact := &mqttAcceptFact{
				ClientID: "somebody-else", SessEpoch: e.epoch,
				AcceptID: mqttNewAcceptID(), At: time.Now().UnixNano(),
			}
			body, err := json.Marshal(fact)
			if err != nil {
				t.Fatalf("marshalling the foreign fact: %v", err)
			}
			f.FactBody = body
		},
		"an acceptance fact stored on another subject": func(f *mqttFrozenMetadataAttempt) {
			f.FactSubj = mqttAcceptSubject(_EMPTY_, getHash("somebody-else"), e.epoch, mqttNewAcceptID())
		},
	} {
		p := testF105CreationPropAt(t, "recv-"+nuid.Next(), mqttNewSessGen())
		testF105StandCreation(t, e, p)
		f := testF105FreezeProposal(t, e, p, prevSeq)
		mutate(f)
		if _, err := e.asm.jsa.commitMetadataAttempt(f); err == nil {
			t.Fatalf("a metadata attempt carrying %s committed", name)
		}
		if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataUnresolved {
			t.Fatalf("the refused attempt carrying %s left verdict %v behind", name, v)
		}
	}

	// THE UNMUTATED ATTEMPT COMMITS, so every refusal above is the substitution and not the
	// fixture.
	good := testF105CreationPropAt(t, "recv-good", mqttNewSessGen())
	testF105StandCreation(t, e, good)
	if _, err := e.asm.jsa.commitMetadataAttempt(testF105FreezeProposal(t, e, good, testF105RecordSeq(t, e))); err != nil {
		t.Fatalf("the unmutated metadata attempt was refused: %v", err)
	}
	if v := e.asm.jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, good); v != mqttMetadataCommitted {
		t.Fatalf("the committed attempt resolves %v, want COMMITTED", v)
	}
}

// TestF105HandoffCASBindingIsEnforcedAtTheWrite is the rest of finding 2: the sequence
// equality the freezer established is re-established where the batch is actually composed.
//
// A frozen attempt is a mutable struct. BaseSeq moved after freezing is a handoff committing
// against a record its own permanent receipt does not name, and the receipt is what every
// later reader believes.
func TestF105HandoffCASBindingIsEnforcedAtTheWrite(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-handoff-cas-write")
	defer done()

	i := testF105Intent(t, e, "cas-write")
	testF105Prepare(t, e, i)
	recSeq := i.PrevSessSeq
	f, ferr := testF105TryFreeze(t, e, i, recSeq)
	if ferr != nil {
		t.Fatalf("freezing the handoff failed: %v", ferr)
	}
	// THE FREEZER BOUND IT; THE CALLER UNBINDS IT AFTERWARDS — and the unbinding leaves the
	// expectation pointing at the record that really stands there, so the predecessor check
	// is satisfied and only the receipt-to-write binding can catch it. Naming a sequence no
	// record occupies would be refused by the predecessor read instead, which proves nothing
	// about this rule.
	f.Proposal.PrevSessSeq, f.Outcome.PrevSessSeq = recSeq-1, recSeq-1
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err == nil {
		t.Fatalf("a handoff committed against a record its receipt does not name")
	}
	f.Proposal.PrevSessSeq, f.Outcome.PrevSessSeq = recSeq, recSeq
	if _, err := e.asm.jsa.commitMetadataAttempt(f); err != nil {
		t.Fatalf("the rebound handoff was refused: %v", err)
	}
}

// TestF105ThereIsNoOneStepRouteToServing is finding 3.
//
// Two kinds installed a SERVING authority outright — admit-tenure at revision 1 and
// readmit-tenure over a closure — and each was the bootstrap gap itself: ownership entitled
// to serve a client with no reservation behind it and no committed metadata half. Deleting
// only one would have left the gap reachable on the second attempt, which is exactly when an
// abandoned first creation puts a broker there.
func TestF105ThereIsNoOneStepRouteToServing(t *testing.T) {
	for _, k := range []mqttTransitionKind{"admit-tenure", "readmit-tenure"} {
		if mqttValidTransitionKind(k) {
			t.Fatalf("%q is still an accepted transition kind: it installs a serving "+
				"authority with no metadata half behind it", k)
		}
	}

	// And the kinds that remain cannot be talked into it. A reservation installs RESERVING
	// and a finalization requires a reserving predecessor, so there is no pair of kinds
	// that reaches SERVING without a proposal in between.
	tr := testF105Transition()
	tr.Attempt.Kind = mqttTransitionFinalizeCreation
	tr.Attempt.Intent = "intent-1"
	tr.Prev.State = mqttAuthorityServing
	if err := mqttCheckTransitionKindRules(tr); err == nil {
		t.Fatalf("a finalization over a serving authority carrying no proposal was admitted")
	}
	tr.Prev.State = mqttAuthorityClosed
	if err := mqttCheckTransitionKindRules(tr); err == nil {
		t.Fatalf("a finalization over a closed authority was admitted")
	}
}

// TestF105MarkerMembersAreShapedAndCreateOnly is the half of finding 1 that is F-105's own.
//
// The marker member was exempt from body validation here on the reasoning that its builder
// validates it — the builder-only guarantee this boundary exists to replace — and it had no
// create-only requirement at all. An unattributed marker body makes a resumed PUBLISH
// undecidable between a first transmission and a retransmission; an overwritten marker
// describes a transmitted allocation as unsent again, which is the DUP flag lying in the
// direction [MQTT-3.3.1-1] forbids.
func TestF105MarkerMembersAreShapedAndCreateOnly(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-marker-boundary")
	defer done()

	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")

	// Every case starts from a WHOLE ADMISSION — register, marker and the obligation they
	// allocate for — because each of the three is now refused on its own: a marker is minted
	// with the register it names, and a register is admitted with the obligation it allocates
	// for. A batch missing any of them would be refused for a reason that is not the marker's
	// body.
	admit := func(txn string, pi uint16, mutate func(*mqttAuthorityTransition)) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		adm := testF105Admission(e, al.rec, al.seq, txn, pi, nil)
		tr, terr := mqttAdmitPublishTransition(adm)
		if terr != nil {
			t.Fatalf("assembling the admission failed: %v", terr)
		}
		mutate(tr)
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}
	setMarker := func(tr *mqttAuthorityTransition, body []byte, expect uint64) {
		for i, m := range tr.Extra {
			if _, merr := mqttParseInflightMarkerSubject(m.subj); merr != nil {
				continue
			}
			tr.Extra[i].body = body
			tr.Extra[i].expect = expect
			return
		}
		t.Fatalf("the assembled admission carries no marker member")
	}

	for _, c := range []struct {
		name string
		body []byte
	}{
		{"a marker naming no well-formed nonce", []byte(`{"nonce":"n-a"}`)},
		{"a marker carrying a foreign member", []byte(`{"nonce":"` + mqttNewInflightNonce() + `","pi":7}`)},
		{"a marker that is not an object", []byte(`"unsent"`)},
	} {
		if err := admit("txn-m-"+mqttNewInflightNonce(), 7, func(tr *mqttAuthorityTransition) {
			setMarker(tr, c.body, 0)
		}); err == nil {
			t.Fatalf("%s was written through the transition boundary", c.name)
		}
	}

	// A MARKER WRITTEN ALONE IS REFUSED, which is what makes its presence mean anything:
	// after a transmission deletes one, its subject is free, so a create-only write would
	// succeed and claim an allocation is unsent that has already been sent.
	lone, berr := mqttBuildInflightMarker(mqttNewInflightNonce())
	if berr != nil {
		t.Fatalf("building the marker body failed: %v", berr)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	solo := testF105AdmitNothing(e, al.rec, al.seq, "txn-m-solo")
	solo.Extra = []mqttTransitionMember{{
		subj:   mqttInflightSubject(getHash(e.id), testF105Gen, 7) + "." + mqttInflightMarkerSuffix,
		expect: 0, body: lone,
	}}
	if _, err := jsa.commitAuthorityTransition(solo); err == nil {
		t.Fatalf("a marker was written without the register it is minted with")
	}

	// THE WHOLE ADMISSION COMMITS, so every refusal above is the marker and not the fixture.
	if err := admit("txn-m-ok", 7, func(*mqttAuthorityTransition) {}); err != nil {
		t.Fatalf("a well-formed admission was refused: %v", err)
	}

	// AND THE MARKER IS NEVER REWRITTEN. The discriminating shape is the one recovery can
	// really meet: the obligation ENDED and its REGISTER deleted by a teardown, while the
	// marker survives — so a fresh admission on that identifier is legitimate and can carry
	// a marker rewrite the store itself would accept.
	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 7)
	subj := regSubj + "." + mqttInflightMarkerSuffix
	rmsg, rlerr := jsa.loadLastMsgFor(mqttInflightStreamName, regSubj)
	if rlerr != nil {
		t.Fatalf("the committed register does not load back: %v", rlerr)
	}
	standing, derr := mqttDecodeInflightRecord(rmsg.Data)
	if derr != nil {
		t.Fatalf("the committed register does not decode: %v", derr)
	}
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testF105Gen, standing.Lin, mqttExchangeRejected); err != nil {
		t.Fatalf("ending the standing obligation was refused: %v", err)
	}
	// The ending re-phased the register in its own batch: delete it at the sequence it stands at.
	if rmsg, rlerr = jsa.loadLastMsgFor(mqttInflightStreamName, regSubj); rlerr != nil {
		t.Fatalf("the ended register does not load back: %v", rlerr)
	}
	if derr := jsa.deleteMsg(mqttInflightStreamName, rmsg.Sequence, true); derr != nil {
		t.Fatalf("deleting the register failed: %v", derr)
	}
	mmsg, mlerr := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if mlerr != nil {
		t.Fatalf("the standing marker does not load back: %v", mlerr)
	}
	if err := admit("txn-m-rewrite", 7, func(tr *mqttAuthorityTransition) {
		for i, m := range tr.Extra {
			if _, merr := mqttParseInflightMarkerSubject(m.subj); merr != nil {
				continue
			}
			tr.Extra[i].expect = mmsg.Sequence
			return
		}
		t.Fatalf("the assembled admission carries no marker member")
	}); err == nil {
		t.Fatalf("a standing unsent marker was rewritten at its own sequence: a transmitted " +
			"allocation would be described as unsent again")
	}
	after, alerr := jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if alerr != nil || after.Sequence != mmsg.Sequence {
		t.Fatalf("the refused rewrite moved the standing marker")
	}
}
