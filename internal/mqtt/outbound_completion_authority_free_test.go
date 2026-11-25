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

//go:build mqtt_fulltests

package mqtt

import (
	"strings"
	"testing"
)

// A COMPLETION IS AN ACT OF THE CLIENT'S ANSWER, NOT OF THE OWNER (mqttAttemptAuthorityFree). Its
// batch carries the outcome, the exchange, the register and the resources it releases, each
// fenced by its own CAS, and no authority member: so it can never be what defeats the next
// admission's authority CAS.

// testCompletionWorld is one admitted QoS 2 obligation on a bootstrapped authority.
func testCompletionWorld(t *testing.T, client string) (*testOutboundEnv, func(), string) {
	t.Helper()
	e, done := testNewEnv(t, client)
	e.bootstrap(t, "txn-1")
	testPlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	return e, done, testAdmitOn(t, e, "txn-adm", 7, "inc-src")
}

// testComposeAck composes, without sending, the PUBREC advance of exchange nonce as the
// production operation would.
func testComposeAck(t *testing.T, e *testOutboundEnv, nonce string, to mqttExchangePhase) *mqttAuthorityTransition {
	t.Helper()
	jsa := &e.asm.jsa
	x, xSeq, xerr := jsa.loadOutboundExchange(e.id, e.epoch, testGen, nonce)
	if xerr != nil || x == nil {
		t.Fatalf("reading exchange %q: %+v %v", nonce, x, xerr)
	}
	reg, smsg, lerr := jsa.loadInflightRecord(mqttInflightSubject(getHash(e.id), testGen, x.PI))
	if lerr != nil || reg == nil || smsg == nil {
		t.Fatalf("reading the slot of exchange %q: %v", nonce, lerr)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load: status %v", al.status)
	}
	tr, err := mqttAdvanceExchangeAckTransition(al.rec, al.seq, &mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: mqttNewInflightNonce(), Op: "op-ack",
		Gen: testGen, Nonce: nonce, To: to, Acq: reg.Acq, Register: reg, RegisterSeq: smsg.Sequence,
	}, x, xSeq)
	if err != nil {
		t.Fatalf("composing the advance: %v", err)
	}
	return tr
}

// The advance batch carries no authority member, and a landed PUBREC leaves the authority's
// sequence and revision exactly where they stood.
//
// Mutation M-L1a: the composer writes the authority again (Auth and Prev restored, the kind no
// longer authority-free) -> the authority's sequence and revision move.
func TestCompletionWritesNoAuthority(t *testing.T) {
	e, done, nonce := testCompletionWorld(t, "completion-no-auth")
	defer done()
	jsa := &e.asm.jsa

	tr := testComposeAck(t, e, nonce, mqttExchangePubRecObserved)
	if tr.Auth != nil || tr.Prev != nil || tr.PrevSeq != 0 || tr.Attempt.Rev != 0 || tr.Attempt.PrevTxn != _EMPTY_ {
		t.Fatalf("the advance names an authority: auth %v, prev %v at %d, attempt revision %d over %q",
			tr.Auth != nil, tr.Prev != nil, tr.PrevSeq, tr.Attempt.Rev, tr.Attempt.PrevTxn)
	}
	authSubj := mqttOutboundAuthoritySubject(getHash(e.id))
	for _, m := range tr.Extra {
		if m.subj == authSubj {
			t.Fatal("the advance carries the authority as a member")
		}
	}

	before := jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("the PUBREC advance was refused: %v", err)
	}
	if x, _, _ := jsa.loadOutboundExchange(e.id, e.epoch, testGen, nonce); x == nil ||
		x.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the PUBREC advance did not land: %+v", x)
	}
	after := jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.status != mqttOutboundValid || after.seq != before.seq || after.rec.Rev != before.rec.Rev {
		t.Fatalf("the PUBREC advance moved the authority: sequence %d -> %d, revision %d -> %d",
			before.seq, after.seq, before.rec.Rev, after.rec.Rev)
	}
}

// An acknowledgement advance writes no outcome (issue #192): it is arbitrated by its exchange
// member's CAS, so its commit returns no handle, stores no outcome record for anyone to resolve,
// and an abort of it is refused because it has no receipt to contend for.
//
// Mutation: let the advance write its receipt again (mqttKindWritesReceipt true for it) -> the
// commit returns a handle and stores an outcome.
func TestCompletionWritesNoOutcome(t *testing.T) {
	e, done, nonce := testCompletionWorld(t, "completion-no-outcome")
	defer done()
	jsa := &e.asm.jsa

	var attempt mqttTransitionAttempt
	testArmTransitionSeam(t, jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdvanceExchange {
			attempt = *tr.Attempt
		}
		return send()
	})
	out, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, nonce, mqttExchangePubRecObserved)
	if err != nil {
		t.Fatalf("the PUBREC advance was refused: %v", err)
	}
	if attempt.Txn == _EMPTY_ {
		t.Fatal("fixture: the advance never reached the commit")
	}
	if out.Handle != nil {
		t.Fatal("the advance returned a handle, so a caller could resolve an outcome that is never written")
	}
	if _, _, st := jsa.loadTransitionOutcome(e.id, e.epoch, attempt.Scope, attempt.Txn); st != mqttOutboundAbsent {
		t.Fatalf("the advance's outcome reads %v, want absent: it writes none", st)
	}
	second := testAdmitOn(t, e, "txn-adm-2", 8, "inc-src")
	unsent := testComposeAck(t, e, second, mqttExchangePubRecObserved)
	if aerr := jsa.abortTransition(unsent.Attempt, nil); aerr == nil || !strings.Contains(aerr.Error(), "writes no receipt") {
		t.Fatalf("an abort of an advance was not refused for its missing receipt: %v", aerr)
	}
}

// An advance composed BEFORE the authority moves still lands after it: there is no predecessor
// check for it to fail. The window is staged at the transition seam, where the composed batch is
// paused while an admission commits a new authority revision.
//
// Mutation M-L1c: the composer captures Prev/PrevSeq again and the writer checks the predecessor
// for the advance -> the paused advance is refused by the pre-submit check.
func TestCompletionLandsOverAMovedAuthority(t *testing.T) {
	e, done, nonce := testCompletionWorld(t, "completion-paused")
	defer done()
	jsa := &e.asm.jsa

	paused, moved := false, false
	testArmTransitionSeam(t, jsa, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if paused || tr.Attempt.Kind != mqttTransitionAdvanceExchange {
			return send()
		}
		paused = true
		before := jsa.loadOutboundAuthority(e.id, e.epoch)
		testAdmitOn(t, e, "txn-adm-2", 8, "inc-src")
		moved = jsa.loadOutboundAuthority(e.id, e.epoch).seq != before.seq
		return send()
	})
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("the advance composed before the authority moved was refused: %v", err)
	}
	if !paused || !moved {
		t.Fatalf("fixture: the advance was paused %v and the authority moved %v, so nothing here is tested",
			paused, moved)
	}
	if x, _, _ := jsa.loadOutboundExchange(e.id, e.epoch, testGen, nonce); x == nil ||
		x.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the paused advance did not land: %+v", x)
	}
}
