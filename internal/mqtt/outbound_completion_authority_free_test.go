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
	"testing"
)

// A COMPLETION IS AN ACT OF THE CLIENT'S ANSWER, NOT OF THE OWNER (mqttAttemptAuthorityFree). Its
// batch carries the outcome, the exchange, the register and the resources it releases, each
// fenced by its own CAS, and no authority member: so it can never be what defeats the next
// admission's authority CAS.

// testCompletionWorld is one admitted QoS 2 obligation on a bootstrapped authority.
func testCompletionWorld(t *testing.T, client string) (*testF105Env, func(), string) {
	t.Helper()
	e, done := testF105NewEnv(t, client)
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	return e, done, testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
}

// testComposeAck composes, without sending, the PUBREC advance of exchange nonce as the
// production operation would.
func testComposeAck(t *testing.T, e *testF105Env, nonce string, to mqttExchangePhase) *mqttAuthorityTransition {
	t.Helper()
	jsa := &e.asm.jsa
	x, xSeq, xerr := jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, nonce)
	if xerr != nil || x == nil {
		t.Fatalf("reading exchange %q: %+v %v", nonce, x, xerr)
	}
	reg, smsg, lerr := jsa.loadInflightRecord(mqttInflightSubject(getHash(e.id), testF105Gen, x.PI))
	if lerr != nil || reg == nil || smsg == nil {
		t.Fatalf("reading the slot of exchange %q: %v", nonce, lerr)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load: status %v", al.status)
	}
	tr, err := mqttAdvanceExchangeAckTransition(al.rec, al.seq, &mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: mqttNewInflightNonce(), Op: "op-ack",
		Gen: testF105Gen, Nonce: nonce, To: to, Acq: reg.Acq, Register: reg, RegisterSeq: smsg.Sequence,
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
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("the PUBREC advance was refused: %v", err)
	}
	if x, _, _ := jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, nonce); x == nil ||
		x.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the PUBREC advance did not land: %+v", x)
	}
	after := jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.status != mqttOutboundValid || after.seq != before.seq || after.rec.Rev != before.rec.Rev {
		t.Fatalf("the PUBREC advance moved the authority: sequence %d -> %d, revision %d -> %d",
			before.seq, after.seq, before.rec.Rev, after.rec.Rev)
	}
}

// An authority-free attempt's outcome is decided by its receipt alone: committed with the
// retained digest is LANDED and stays LANDED however far the authority moves afterwards
// (nothing it installed can be superseded); an absent receipt is UNRESOLVED (the batch may yet
// commit); an aborted one is NEVER LANDED.
//
// Mutation M-L1b: classify an authority-free attempt against the authority's revision like
// every other kind -> the landed advance does not resolve LANDED once an admission moves the
// authority.
func TestCompletionOutcomeResolvesLanded(t *testing.T) {
	e, done, nonce := testCompletionWorld(t, "completion-outcome")
	defer done()
	jsa := &e.asm.jsa

	out, err := testAdvanceLive(t, jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangePubRecObserved)
	if err != nil || out.Handle == nil {
		t.Fatalf("the PUBREC advance was refused or returned no handle: %v", err)
	}
	if v := jsa.resolveTransitionHandle(out.Handle); v != mqttTransitionLanded {
		t.Fatalf("the landed advance resolves %v, want LANDED", v)
	}
	// An ADMISSION moves the authority's revision; the advance's answer does not change.
	before := jsa.loadOutboundAuthority(e.id, e.epoch)
	second := testF105AdmitOn(t, e, "txn-adm-2", 8, "inc-src")
	if after := jsa.loadOutboundAuthority(e.id, e.epoch); after.rec.Rev == before.rec.Rev {
		t.Fatal("fixture: the second admission did not move the authority, so nothing here is tested")
	}
	if v := jsa.resolveTransitionHandle(out.Handle); v != mqttTransitionLanded {
		t.Fatalf("after an admission moved the authority the advance resolves %v, want LANDED", v)
	}

	// ABSENT: composed and frozen, never sent.
	unsent := testComposeAck(t, e, second, mqttExchangePubRecObserved)
	h, herr := mqttNewTransitionHandle(unsent)
	if herr != nil {
		t.Fatalf("freezing the unsent attempt's handle: %v", herr)
	}
	if v := jsa.resolveTransitionHandle(h); v != mqttTransitionUnresolved {
		t.Fatalf("an attempt with no receipt resolves %v, want UNRESOLVED", v)
	}
	// ABORTED: the settlement a caller with an ambiguous send writes.
	if aerr := jsa.abortTransition(unsent.Attempt); aerr != nil {
		t.Fatalf("aborting the unsent attempt: %v", aerr)
	}
	if v := jsa.resolveTransitionHandle(h); v != mqttTransitionNeverLanded {
		t.Fatalf("an aborted attempt resolves %v, want NEVER LANDED", v)
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
		testF105AdmitOn(t, e, "txn-adm-2", 8, "inc-src")
		moved = jsa.loadOutboundAuthority(e.id, e.epoch).seq != before.seq
		return send()
	})
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("the advance composed before the authority moved was refused: %v", err)
	}
	if !paused || !moved {
		t.Fatalf("fixture: the advance was paused %v and the authority moved %v, so nothing here is tested",
			paused, moved)
	}
	if x, _, _ := jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, nonce); x == nil ||
		x.Phase != mqttExchangePubRecObserved {
		t.Fatalf("the paused advance did not land: %+v", x)
	}
}
