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

//go:build mqttport_fulltests

package mqtt

import (
	"testing"

	"github.com/nats-io/nuid"
)

// testF105AdmitQoS admits ONE outbound PUBLISH obligation at the given QoS and returns the
// exchange nonce, the register subject, and the register record with the sequence it stands at.
// It is testF105AdmitOn with the QoS as a parameter, because the acknowledgement arrows differ
// by QoS: a PUBACK ends a QoS 1 delivery at terminal, a PUBREC a QoS 2 one at PUBREC-observed.
func testF105AdmitQoS(t *testing.T, e *testF105Env, txn string, pi uint16, qos byte,
	incs ...string) (string, string, *mqttInflightRecord, uint64) {
	t.Helper()
	reg := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: pi, Dur: "$MQTT_src", Seq: 1, QoS: qos,
		Nonce: mqttNewInflightNonce(),
	})
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load back: status %v", al.status)
	}
	deps := make([]mqttAdmitDependency, 0, len(incs))
	for _, inc := range incs {
		deps = append(deps, mqttAdmitDependency{Inc: inc, Role: testF105AdmissionRole(t, e, inc)})
	}
	if _, err := e.asm.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
		Gen: al.rec.Gen, Tenure: al.rec.Tenure,
		Register: reg, Deps: deps,
	}); err != nil {
		t.Fatalf("admission %s at QoS %d was refused: %v", txn, qos, err)
	}
	subj := mqttInflightSubject(getHash(e.id), al.rec.Gen, pi)
	cur, smsg, lerr := e.asm.jsa.loadInflightRecord(subj)
	if lerr != nil || cur == nil || smsg == nil {
		t.Fatalf("the admitted register does not load back: %v", lerr)
	}
	return reg.Lin, subj, cur, smsg.Sequence
}

// A PUBACK moves the QoS 1 exchange to terminal AND re-phases its slot to publish_terminal, in
// ONE batch. The two records state one fact — the client answered — and this is the operation
// that keeps them from ever disagreeing.
func TestF105AckAdvanceMovesExchangeAndSlotAtomically(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-atomic")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, subj, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")

	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: reg.Acq,
		Register: reg, RegisterSeq: seq,
	}); err != nil {
		t.Fatalf("the atomic acknowledgement advance was refused: %v", err)
	}

	end, _ := testF105LoadExchange(t, e, auth.Gen, nonce)
	if end.Phase != mqttExchangeTerminal {
		t.Fatalf("the exchange stands at phase %d after PUBACK, want terminal", end.Phase)
	}
	slot, _, _ := e.asm.jsa.loadInflightRecord(subj)
	if slot == nil || slot.State != mqttInflightPublishTerminal {
		t.Fatalf("the slot stands at %v after PUBACK, want publish_terminal", slot)
	}
}

// A successful PUBREC moves the QoS 2 exchange to PUBREC-observed and re-phases its slot to
// pubrel_intended, and — unlike the terminal arrow — PRESERVES a live action token and is not
// bound to the answered transmission (a PUBREC may land on a retransmission).
func TestF105AckAdvancePubRecPreservesTokenAndIsNotAcqBound(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-pubrec")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, subj, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 2, "inc-src")

	// Acquire an output-custody token, so the slot carries a LIVE action when the PUBREC lands.
	token := mqttNewInflightNonce()
	if _, err := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq", Op: "op-acq",
		Gen: auth.Gen, Tenure: auth.Tenure, Nonce: nonce, Acq: reg.Acq + 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242), Register: reg, RegisterSeq: seq, Token: token,
	}); err != nil {
		t.Fatalf("acquiring the send token was refused: %v", err)
	}
	held, hsmsg, _ := e.asm.jsa.loadInflightRecord(subj)

	// The PUBREC advance names a transmission DIFFERENT from the live one, deliberately: the
	// arrow is not acq-bound, so it must still be accepted.
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangePubRecObserved, Acq: held.Acq + 99,
		Register: held, RegisterSeq: hsmsg.Sequence,
	}); err != nil {
		t.Fatalf("the PUBREC advance over a live token was refused: %v", err)
	}

	end, _ := testF105LoadExchange(t, e, auth.Gen, nonce)
	if end.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the exchange stands at phase %d after PUBREC, want PUBREC-observed", end.Phase)
	}
	slot, _, _ := e.asm.jsa.loadInflightRecord(subj)
	if slot == nil || slot.State != mqttInflightPubRelIntended {
		t.Fatalf("the slot stands at %v after PUBREC, want pubrel_intended", slot)
	}
	if slot.ActionToken != token {
		t.Fatalf("the PUBREC arrow dropped the live action token: %+v", slot)
	}
}

// The terminal arrow refuses a live action token: its holder may still be writing the
// transmission the acknowledgement answers, and terminalising over it would license disposing
// of an exchange the client never finished with.
func TestF105AckAdvanceTerminalRefusesLiveToken(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-livetoken")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, subj, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")

	token := mqttNewInflightNonce()
	if _, err := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq", Op: "op-acq",
		Gen: auth.Gen, Tenure: auth.Tenure, Nonce: nonce, Acq: reg.Acq + 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242), Register: reg, RegisterSeq: seq, Token: token,
	}); err != nil {
		t.Fatalf("acquiring the send token was refused: %v", err)
	}
	held, hsmsg, _ := e.asm.jsa.loadInflightRecord(subj)

	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: held.Acq,
		Register: held, RegisterSeq: hsmsg.Sequence,
	}); err == nil {
		t.Fatalf("a PUBACK terminalised a slot whose action was still live")
	}
}

// The terminal arrow is bound to the ONE transmission the acknowledgement answers: a PUBACK
// naming a transmission the slot has moved past records a client answer that never happened.
func TestF105AckAdvanceTerminalIsBoundToTheAnsweredTransmission(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-acqbound")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")

	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: reg.Acq + 1,
		Register: reg, RegisterSeq: seq,
	}); err == nil {
		t.Fatalf("a PUBACK answering a different transmission than the slot's was accepted")
	}
}

// A stale register read loses: the exact-sequence CAS is what makes the re-phase and the
// exchange advance one consistent act, so a read superseded by an acquisition in between must
// not commit.
func TestF105AckAdvanceLosesAStaleRegisterRead(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-stale")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")

	// An acquisition moves the register after the read above; the stale (reg, seq) is now the
	// second broker observing the same acknowledgement.
	if _, err := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq", Op: "op-acq",
		Gen: auth.Gen, Tenure: auth.Tenure, Nonce: nonce, Acq: reg.Acq + 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242), Register: reg, RegisterSeq: seq, Token: mqttNewInflightNonce(),
	}); err != nil {
		t.Fatalf("the intervening acquisition was refused: %v", err)
	}
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: reg.Acq,
		Register: reg, RegisterSeq: seq,
	}); err == nil {
		t.Fatalf("an acknowledgement advance conditioned on a superseded register read was accepted")
	}
}

// The builder refuses a slot member that names a different obligation than the exchange it
// advances: the two are one acknowledgement, so a re-phase can never ride beside a phase move
// for another obligation.
func TestF105AckAdvanceBuilderRefusesAMismatchedSlot(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-mismatch")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")

	cur, curSeq := testF105LoadExchange(t, e, auth.Gen, nonce)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	foreign := *reg
	foreign.Lin = mqttNewInflightNonce() // a different obligation than the exchange's
	if _, err := mqttAdvanceExchangeAckTransition(al.rec, al.seq,
		&mqttAdvanceExchangeAckRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
			Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminal, Acq: foreign.Acq,
			Register: &foreign, RegisterSeq: seq,
		}, cur, curSeq); err == nil {
		t.Fatalf("a slot member naming a foreign obligation was assembled beside the advance")
	}
}

// A pending-drain ending carries the exchange ALONE: its output is still outstanding, so the
// slot cannot terminalise, and asking the acknowledgement op to re-phase it is refused.
func TestF105AckAdvancePendingDrainCarriesNoSlot(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-drain")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")

	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeTerminalPendingDrain, Acq: reg.Acq,
		Register: reg, RegisterSeq: seq,
	}); err == nil {
		t.Fatalf("a pending-drain ending re-phased the slot instead of carrying the exchange alone")
	}
}

// A FAILED PUBREC ends a QoS 2 exchange at REJECTED (not Terminal), re-phasing its slot to
// publish_terminal in the same batch. This is the arrow the (B) arm selects with `cur.QoS == 2`:
// the only acknowledgement that ends a QoS 2 delivery still at PUBLISH is a PUBREC the client
// refused, and it must leave the exchange at Rejected so recovery never resends the payload under
// a fresh identifier (consult 18, arm-level owed item #1).
func TestF105AckAdvanceFailedPubRecRejectsTheQoS2Exchange(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-rejected")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, subj, reg, seq := testF105AdmitQoS(t, e, "txn-adm", 7, 2, "inc-src")

	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: auth.Gen, Nonce: nonce, To: mqttExchangeRejected, Acq: reg.Acq,
		Register: reg, RegisterSeq: seq,
	}); err != nil {
		t.Fatalf("the failed-PUBREC advance to Rejected was refused: %v", err)
	}

	end, _ := testF105LoadExchange(t, e, auth.Gen, nonce)
	if end.Phase != mqttExchangeRejected {
		t.Fatalf("the exchange stands at phase %d after a failed PUBREC, want rejected", end.Phase)
	}
	slot, _, _ := e.asm.jsa.loadInflightRecord(subj)
	if slot == nil || slot.State != mqttInflightPublishTerminal {
		t.Fatalf("the slot stands at %v after a failed PUBREC, want publish_terminal", slot)
	}
}

// A closed generation's obligation still ends: a PUBREC on G1's connection advances the G1
// exchange and re-phases its G1 slot under G2's serving authority, with the register member
// naming G1 while the transition is G2's — the generation exemption this row relies on.
func TestF105AckAdvanceCrossGenerationEndsAClosedObligation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ack-crossgen")
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
			State: mqttAuthorityClosed, Rev: atr.Auth.Rev + 1, Txn: "txn-close", OwnerTxn: auth.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Txn: "txn-close"},
	}
	closeSeq, clerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, closing))
	if clerr != nil {
		t.Fatalf("the closure was refused: %v", clerr)
	}
	e.claim(t, closing.Auth, closeSeq, "txn-g2", testF105Gen2, "tenure-g2")

	regSubj := mqttInflightSubject(getHash(e.id), testF105Gen, 7)
	reg, smsg, lerr := e.asm.jsa.loadInflightRecord(regSubj)
	if lerr != nil || reg == nil || smsg == nil {
		t.Fatalf("G1's register does not load back: %v", lerr)
	}
	if al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch); al.rec.Gen != testF105Gen2 {
		t.Fatalf("the serving authority is at %q, want the successor", al.rec.Gen)
	}

	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-ack", Op: "op-ack",
		Gen: testF105Gen, Nonce: adm.Exchange.Nonce, To: mqttExchangePubRecObserved, Acq: reg.Acq,
		Register: reg, RegisterSeq: smsg.Sequence,
	}); err != nil {
		t.Fatalf("a closed generation's obligation could not record its PUBREC under the successor: %v", err)
	}

	end, _ := testF105LoadExchange(t, e, testF105Gen, adm.Exchange.Nonce)
	if end.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the G1 exchange stands at phase %d, want PUBREC-observed", end.Phase)
	}
	if end.Gen != testF105Gen {
		t.Fatalf("advancing under the successor re-attributed the obligation to generation %q", end.Gen)
	}
	slot, _, _ := e.asm.jsa.loadInflightRecord(regSubj)
	if slot == nil || slot.State != mqttInflightPubRelIntended {
		t.Fatalf("G1's slot stands at %v, want pubrel_intended", slot)
	}
}

// AN ORDINARY SLOT WITHOUT ITS EXCHANGE IS REFUSED BY EVERY ACKNOWLEDGEMENT ARROW (consult 20).
//
// Production admits an ordinary obligation beside its exchange in one batch, so an ordinary slot
// with no exchange is a shape this broker does not write. It is refused rather than completed
// slot-only: the slot-only arrow would leave the durable record of the obligation saying nothing
// about the answer. Both callers share one gate, so both are asserted, and neither may write.
//
// Mutation M3: restore "explicit absence means a legacy register" (slot-only), and both arms move
// the slot. Mutation M5: map the terminal arm's refusal to the slot-only path, and arm (b) does.
func TestF105AckRefusesAnOrdinarySlotWithoutItsExchange(t *testing.T) {
	t.Parallel()
	for _, arm := range []string{"successful PUBREC", "PUBACK"} {
		arm := arm
		t.Run(arm, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "f105-no-exchange")
			e.subscribePubRel(t)
			dur := e.sess.idHash + "_" + nuid.Next()
			qos := byte(2)
			if arm == "PUBACK" {
				qos = 1
			}
			pi := e.track(t, dur, 41, qos) // the exchange-less ordinary register, deliberately
			subj := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
			cur, smsg, lerr := e.asm.jsa.loadInflightRecord(subj)
			if lerr != nil || cur == nil || smsg == nil {
				t.Fatalf("the fixture needs its register: %+v %v", cur, lerr)
			}
			ref := mqttSlotRefFromRecord(subj, smsg.Sequence, cur)
			if ref == nil {
				t.Fatal("the seeded slot must yield a reference")
			}
			if arm == "successful PUBREC" {
				if got := e.sess.intendPubRelSlot(e.c, &mqttPending{slot: *ref}); got != mqttPubRelIntentNotRecorded {
					t.Fatalf("intent outcome = %d, want NotRecorded", got)
				}
			} else if e.sess.attemptTerminalRegisterRelease(e.c, pi, *ref, true) {
				t.Fatal("the terminal arm discharged an ordinary slot that has no exchange")
			}
			after, asmsg, aerr := e.asm.jsa.loadInflightRecord(subj)
			if aerr != nil || after == nil || after.State != mqttInflightPublish || asmsg.Sequence != smsg.Sequence {
				t.Fatalf("the refused acknowledgement wrote the slot: %+v (%v)", after, aerr)
			}
		})
	}
}
