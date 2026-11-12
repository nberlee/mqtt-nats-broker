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
	"strings"
	"testing"
)

// The four findings of implementation review 8, each against the store.
//
// Three of them are the same class the file beside this one exists for — a rule enforced by
// the helper that composed a write rather than by the write — and the fourth is a protocol
// case the phase relation had no legal move for.

// TestRefusedPublishReceivedHasAnEndingOfItsOwn is finding 4.
//
// A PUBREC carrying a reason code >= 0x80 says the client will NOT take the message
// ([MQTT-4.3.3]): the sender must not send a PUBREL, and the exchange is over. The phase
// relation had no move for that. Its only route out of a publish was PUBREC-observed, so the
// only way to end such an exchange durably was to record a successful ownership transfer
// that never happened and then owe a release nothing may emit — or to leave the obligation
// standing forever, holding its charge and its identifier.
func TestRefusedPublishReceivedHasAnEndingOfItsOwn(t *testing.T) {
	e, done := testNewEnv(t, "f105-pubrec-refused")
	defer done()

	e.bootstrap(t, "txn-1")
	testPlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testAdmitOn(t, e, "txn-adm", 7, "inc-src")

	// THE REFUSAL ENDS IT, straight from the publish, through the write boundary.
	cur, curSeq := testLoadExchange(t, e, testGen, nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	adv, aerr := mqttAdvanceExchangeTransition(al.rec, al.seq, "txn-rej", "op-rej",
		cur, curSeq, mqttExchangeRejected)
	if aerr != nil {
		t.Fatalf("assembling the rejection failed: %v", aerr)
	}
	adv = testWithSlotMember(t, &e.asm.jsa, adv, testGen, nonce, mqttExchangeRejected)
	adv = testWithHoldReleases(t, e, adv, cur, mqttExchangeRejected)
	if _, cerr := e.asm.jsa.commitAuthorityTransition(adv); cerr != nil {
		t.Fatalf("a refused PUBREC could not end its exchange: %v", cerr)
	}
	end, _ := testLoadExchange(t, e, testGen, nonce)
	if end.Phase != mqttExchangeRejected {
		t.Fatalf("the refused exchange stands at phase %d, want rejected", end.Phase)
	}

	// AND IT IS AN ENDING: the resource it depends on is released over it, exactly as over a
	// completion, because nothing further can be emitted for it (issue #107 7c-lc: release-owed,
	// then released by a release-holds batch over the stored ending).
	testReleaseOwed(t, e, nonce)
	res, _ := testBoundaryLoadResource(t, e, "inc-src")
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
			ID: e.id, Epoch: e.epoch, Gen: testGen, Nonce: nonce,
			PI: 7, QoS: 1, Phase: p, AdmitTxn: "txn-adm", Tenure: "tenure-txn-1",
		}
		if err := mqttValidateOutboundExchange(x, e.id, e.epoch, testGen, nonce); err == nil {
			t.Fatalf("a QoS 1 exchange carrying phase %d was accepted", p)
		}
	}
}

// TestRefusedPublishReceivedWaitsForItsDrain is the other half of finding 4: a refusal that
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
func TestRefusedPublishReceivedWaitsForItsDrain(t *testing.T) {
	e, done := testNewEnv(t, "f105-pubrec-refused-drain")
	defer done()

	auth, _ := e.bootstrap(t, "txn-1")
	testPlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, subj, reg, seq := testAdmitQoS(t, e, "txn-adm", 7, 2, "inc-src")
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
	cur, curSeq := testLoadExchange(t, e, testGen, nonce)
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
	if x, _ := testLoadExchange(t, e, testGen, nonce); x.Phase != mqttExchangePublish {
		t.Fatalf("a refused refusal moved the exchange to phase %d", x.Phase)
	}
	// The dependency stays held: nothing ended.
	if err := testReleaseDependency(t, e, "inc-src", nonce, "txn-early"); err == nil {
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

// TestMarkerMembersAreShapedAndCreateOnly is the half of finding 1 that is issue #107's own.
//
// The marker member was exempt from body validation here on the reasoning that its builder
// validates it — the builder-only guarantee this boundary exists to replace — and it had no
// create-only requirement at all. An unattributed marker body makes a resumed PUBLISH
// undecidable between a first transmission and a retransmission; an overwritten marker
// describes a transmitted allocation as unsent again, which is the DUP flag lying in the
// direction [MQTT-3.3.1-1] forbids.
func TestMarkerMembersAreShapedAndCreateOnly(t *testing.T) {
	e, done := testNewEnv(t, "f105-marker-boundary")
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
		adm := testAdmission(e, al.rec, al.seq, txn, pi, nil)
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
	solo := testAdmitNothing(e, al.rec, al.seq, "txn-m-solo")
	solo.Extra = []mqttTransitionMember{{
		subj:   mqttInflightSubject(getHash(e.id), testGen, 7) + "." + mqttInflightMarkerSuffix,
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
	regSubj := mqttInflightSubject(getHash(e.id), testGen, 7)
	subj := regSubj + "." + mqttInflightMarkerSuffix
	rmsg, rlerr := jsa.loadLastMsgFor(mqttSessStreamName, regSubj)
	if rlerr != nil {
		t.Fatalf("the committed register does not load back: %v", rlerr)
	}
	standing, derr := mqttDecodeInflightRecord(rmsg.Data)
	if derr != nil {
		t.Fatalf("the committed register does not decode: %v", derr)
	}
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, standing.Lin, mqttExchangeRejected); err != nil {
		t.Fatalf("ending the standing obligation was refused: %v", err)
	}
	// The ending re-phased the register in its own batch: delete it at the sequence it stands at.
	if rmsg, rlerr = jsa.loadLastMsgFor(mqttSessStreamName, regSubj); rlerr != nil {
		t.Fatalf("the ended register does not load back: %v", rlerr)
	}
	if derr := jsa.deleteMsg(mqttSessStreamName, rmsg.Sequence, true); derr != nil {
		t.Fatalf("deleting the register failed: %v", derr)
	}
	mmsg, mlerr := jsa.loadLastMsgFor(mqttSessStreamName, subj)
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
	after, alerr := jsa.loadLastMsgFor(mqttSessStreamName, subj)
	if alerr != nil || after.Sequence != mmsg.Sequence {
		t.Fatalf("the refused rewrite moved the standing marker")
	}
}
