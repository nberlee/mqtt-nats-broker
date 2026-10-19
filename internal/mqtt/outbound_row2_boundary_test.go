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
	"strings"
	"sync/atomic"
	"testing"
)

// The write boundary's statement of one acquisition (F-105 row 2): the register token and the action
// record are one acquisition in one batch, an acquisition is recognised or refused, and only the
// lease's holder ends live custody. Each case is submitted directly to commitAuthorityTransition, so
// the refusal asserted is the boundary's own and not a composer's.

// testRow2Acquired admits one ordinary exchange on identifier 7 and acquires its PUBLISH through
// the production composer, returning the lineage and the register as the acquisition left it.
func testRow2Acquired(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority) (string, *mqttInflightRecord, uint64) {
	t.Helper()
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	if _, _, err := e.asm.jsa.opAcquireAction(testF105WithRegister(t, e, 7, &mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq-a", Op: "op-acq-a",
		Gen: auth.Gen, Tenure: auth.Tenure, Nonce: nonce, Acq: 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242),
	})); err != nil {
		t.Fatalf("A's acquisition was refused: %v", err)
	}
	cur, smsg, err := e.asm.jsa.loadInflightRecord(mqttInflightSubject(getHash(e.id), auth.Gen, 7))
	if err != nil || cur == nil || smsg == nil || cur.ActionToken == _EMPTY_ || cur.Acq != 1 {
		t.Fatalf("A's acquisition did not leave its token: %+v (%v)", cur, err)
	}
	return nonce, cur, smsg.Sequence
}

// testRow2AcquireBatch composes an acquire-action transition carrying act and, when reg is
// non-nil, the register member reg at expect.
func testRow2AcquireBatch(t *testing.T, e *testF105Env, txn string, act *mqttOutboundAction,
	reg *mqttInflightRecord, expect uint64) *mqttAuthorityTransition {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	tr := testF105AdmitNothing(e, al.rec, al.seq, txn)
	tr.Attempt.Kind = mqttTransitionAcquireAction
	act.AcqTxn = txn
	abody, err := json.Marshal(act)
	if err != nil {
		t.Fatalf("marshalling the action: %v", err)
	}
	tr.Extra = []mqttTransitionMember{{subj: mqttActionSubject(getHash(e.id), act.Gen, act.Nonce, act.Acq), body: abody}}
	if reg != nil {
		rbody, rerr := json.Marshal(reg)
		if rerr != nil {
			t.Fatalf("marshalling the register member: %v", rerr)
		}
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: mqttInflightSubject(getHash(e.id), act.Gen, reg.PI),
			expect: expect, body: rbody})
	}
	return tr
}

func testRow2Refused(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the batch committed; want a refusal naming %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("the batch was refused by the wrong rule: %v (want %q)", err, want)
	}
}

// T2d / T12c-i / T12c-iii: an acquisition is ONE batch of both carriers, over a token-free exact
// occupant, recognised or refused.
func TestF105Row2AnAcquisitionIsBothCarriersOverAFreeOccupant(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-row2-acq-boundary")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	nonce, afterA, seqA := testRow2Acquired(t, e, auth)
	tokenA := afterA.ActionToken
	act := func(acq uint64) *mqttOutboundAction {
		b := mqttBindConnection(99)
		return &mqttOutboundAction{ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: nonce, Acq: acq,
			Kind: mqttActionPublish, PI: 7, Run: b.run, Conn: b.conn, Tenure: auth.Tenure}
	}
	unchanged := func(t *testing.T) {
		t.Helper()
		cur, _, _ := e.asm.jsa.loadInflightRecord(mqttInflightSubject(getHash(e.id), auth.Gen, 7))
		if cur == nil || cur.ActionToken != tokenA || cur.Acq != 1 {
			t.Fatalf("a refused batch moved A's lease: %+v", cur)
		}
		if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
			mqttActionSubject(getHash(e.id), auth.Gen, nonce, 2)); err == nil {
			t.Fatalf("a refused batch installed its action record")
		}
	}

	t.Run("an action record with no token member", func(t *testing.T) {
		_, err := e.asm.jsa.commitAuthorityTransition(testRow2AcquireBatch(t, e, "txn-no-token", act(2), nil, 0))
		testRow2Refused(t, err, "no output-custody token installed")
		unchanged(t)
	})
	t.Run("a stale predecessor", func(t *testing.T) {
		// The occupant as it stood BEFORE A acquired: token-free at Acq 0, one sequence back.
		stale := *afterA
		stale.ActionToken, stale.ActionClaim, stale.ActionClaimSeq, stale.Acq = _EMPTY_, _EMPTY_, 0, 0
		next := stale
		next.ActionToken, next.ActionClaim, next.ActionClaimSeq, next.Acq = mqttNewInflightNonce(), stale.Claim, stale.ClaimSeq, 2
		_, err := e.asm.jsa.commitAuthorityTransition(testRow2AcquireBatch(t, e, "txn-stale", act(2), &next, seqA-1))
		testRow2Refused(t, err, "a replacement is decided against an exact occupant")
		unchanged(t)
	})
	t.Run("the fresh token-bearing predecessor", func(t *testing.T) {
		// Every field A's register carries, but token B and the next counter: not an acquisition
		// of a free occupant, so recognition refuses it — never the replacement rules.
		next := *afterA
		next.ActionToken, next.Acq = mqttNewInflightNonce(), 2
		_, err := e.asm.jsa.commitAuthorityTransition(testRow2AcquireBatch(t, e, "txn-fresh", act(2), &next, seqA))
		testRow2Refused(t, err, "is not a token acquisition of its exact occupant")
		unchanged(t)
	})
}

// T2c: the packet binds to the register step, at the boundary. The exchange is at PUBREC-observed,
// so a PUBREL action passes the exchange's own check and reaches the register-step binding.
func TestF105Row2ThePacketBindsToTheRegisterStep(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-row2-kind")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	if _, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, auth.Gen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("recording the PUBREC: %v", err)
	}
	// The live PUBREC re-phased the slot with it. The rule under test refuses a PUBREL action on a
	// register still at publish beside a PUBREC-observed exchange — a pair no batch writes — so
	// the fixture re-plants the register at publish to construct it.
	regSubj := mqttInflightSubject(getHash(e.id), auth.Gen, 7)
	moved, mmsg, merr := e.asm.jsa.loadInflightRecord(regSubj)
	if merr != nil || moved == nil || mmsg == nil {
		t.Fatalf("reading the re-phased register: %v", merr)
	}
	back := *moved
	back.State = mqttInflightPublish
	if _, perr := e.asm.jsa.storeInflightCAS(regSubj, mmsg.Sequence, &back); perr != nil {
		t.Fatalf("re-planting the register at publish: %v", perr)
	}
	cur, smsg, err := e.asm.jsa.loadInflightRecord(regSubj)
	if err != nil || cur == nil || cur.State != mqttInflightPublish {
		t.Fatalf("the fixture needs the register still at publish: %+v (%v)", cur, err)
	}
	b := mqttBindConnection(99)
	rel := &mqttOutboundAction{ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: nonce, Acq: 1,
		Kind: mqttActionPubRel, PI: 7, Run: b.run, Conn: b.conn, Tenure: auth.Tenure}
	next := *cur
	next.ActionToken, next.ActionClaim, next.ActionClaimSeq, next.Acq = mqttNewInflightNonce(), cur.Claim, cur.ClaimSeq, 1
	_, cerr := e.asm.jsa.commitAuthorityTransition(testRow2AcquireBatch(t, e, "txn-rel-on-publish", rel, &next, smsg.Sequence))
	testRow2Refused(t, cerr, "may not ride a register step")
}

// T2b: the direct arrow's rebind installs the canonical tuple and moves intended -> ready, and
// nothing else. A member that also rewrites a shape-valid field outside the tuple is refused.
func TestF105Row2TheRebindArmChangesOnlyTheTuple(t *testing.T) {
	cur := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPubRelIntended, PI: 7, Dur: "$MQTT_src",
		Seq: 1, QoS: 2, Nonce: mqttNewInflightNonce()})
	good := *cur
	good.ActionToken, good.ActionClaim, good.ActionClaimSeq, good.Acq = "tok", cur.Claim, cur.ClaimSeq, 1
	good.State, good.PubRelDurable, good.PubRelStreamSeq = mqttInflightPubRelReady, "$MQTT_PUBREL_x", 9
	if !mqttIsActionTokenAcquisition(cur, &good) {
		t.Fatalf("the direct arrow's own member is not recognised")
	}
	bad := good
	bad.Seq = 2
	if mqttIsActionTokenAcquisition(cur, &bad) {
		t.Fatalf("a rebind that also rewrites the source sequence was recognised as an acquisition")
	}
	named := *cur
	named.PubRelDurable, named.PubRelStreamSeq = "$MQTT_PUBREL_x", 9
	other := good
	other.PubRelStreamSeq = 10
	if mqttIsActionTokenAcquisition(&named, &other) {
		t.Fatalf("a rebind rewrote a canonical sequence the occupant already named")
	}
}

// T12c-iv: live custody is preserved at the boundary for every kind. An admit-exchange
// register-only replacement over a live PUBLISH token, clearing it or substituting another, is
// refused; the legitimate PUBREC advance over the live token still commits.
func TestF105Row2OnlyTheHolderEndsLiveCustody(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-row2-custody")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	nonce, afterA, seqA := testRow2Acquired(t, e, auth)
	subj := mqttInflightSubject(getHash(e.id), auth.Gen, 7)
	replace := func(txn string, mutate func(*mqttInflightRecord)) error {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		tr := testF105AdmitNothing(e, al.rec, al.seq, txn)
		next := *afterA
		mutate(&next)
		body, err := json.Marshal(&next)
		if err != nil {
			t.Fatalf("marshalling: %v", err)
		}
		tr.Extra = []mqttTransitionMember{{subj: subj, expect: seqA, body: body}}
		_, cerr := e.asm.jsa.commitAuthorityTransition(tr)
		return cerr
	}
	testRow2Refused(t, replace("txn-clear", func(r *mqttInflightRecord) {
		r.ActionToken, r.ActionClaim, r.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
	}), "over live output custody")
	testRow2Refused(t, replace("txn-swap", func(r *mqttInflightRecord) {
		r.ActionToken = mqttNewInflightNonce()
	}), "over live output custody")
	if cur, _, _ := e.asm.jsa.loadInflightRecord(subj); cur == nil || cur.ActionToken != afterA.ActionToken {
		t.Fatalf("a refused replacement moved the live lease: %+v", cur)
	}
	// B, entitled, still cannot acquire while A's lease stands.
	_, _, berr := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq-b", Op: "op-acq-b", Gen: auth.Gen, Tenure: auth.Tenure,
		Nonce: nonce, Acq: 2, Kind: mqttActionPublish, Binding: mqttBindConnection(77),
		Register: afterA, RegisterSeq: seqA, Token: mqttNewInflightNonce(),
	})
	testRow2Refused(t, berr, "already carries a live token")
	// The legitimate member over the live token: a successful PUBREC keeps the lease.
	to, perr := mqttPhaseForAck(mqttAckArrival{Packet: mqttPacketPubRec, Reason: mqttReasonSuccess}, 2)
	if perr != nil {
		t.Fatalf("deriving the PUBREC phase: %v", perr)
	}
	if _, err := e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-pubrec", Op: "op-ack-advance", Gen: auth.Gen, Nonce: nonce,
		To: to, Acq: afterA.Acq, Register: afterA, RegisterSeq: seqA,
	}); err != nil {
		t.Fatalf("the PUBREC advance over a live PUBLISH token was refused: %v", err)
	}
	if cur, _, _ := e.asm.jsa.loadInflightRecord(subj); cur == nil || cur.ActionToken != afterA.ActionToken ||
		cur.State != mqttInflightPubRelIntended {
		t.Fatalf("the PUBREC advance did not preserve the lease: %+v", cur)
	}
}

// T12c-ii: REAL store contention. B's batch passes every boundary check against a token-free
// occupant; before it is sent, A's acquisition lands. The store's own CAS refuses B's atomic batch,
// and none of B's members lands: the register and the action subject read back as A's.
func TestF105Row2StoreContentionRefusesTheWholeLosingBatch(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-row2-contention")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	subj := mqttInflightSubject(getHash(e.id), auth.Gen, 7)
	reqFor := func(txn string, bind uint64) *mqttAcquireActionRequest {
		return testF105WithRegister(t, e, 7, &mqttAcquireActionRequest{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn, Gen: auth.Gen, Tenure: auth.Tenure,
			Nonce: nonce, Acq: 1, Kind: mqttActionPublish, Binding: mqttBindConnection(bind),
		})
	}
	reqB, reqA := reqFor("txn-b", 88), reqFor("txn-a", 99)
	var raced, aLanded atomic.Bool
	hook := func(tr *mqttAuthorityTransition) {
		if tr.Attempt.Txn != "txn-b" || !raced.CompareAndSwap(false, true) {
			return
		}
		if _, _, err := e.asm.jsa.opAcquireAction(reqA); err != nil {
			t.Errorf("A's racing acquisition was refused: %v", err)
			return
		}
		aLanded.Store(true)
	}
	e.asm.jsa.testTransitionPresend.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionPresend.Store(nil) })

	_, _, err := e.asm.jsa.opAcquireAction(reqB)
	if !raced.Load() || !aLanded.Load() {
		t.Fatalf("B never reached the send with A landing first (raced=%v A=%v): nothing here is tested",
			raced.Load(), aLanded.Load())
	}
	if err == nil {
		t.Fatalf("B's batch committed over A's acquisition")
	}
	// Either spelling of the store's wrong-last-sequence refusal is its own CAS (10071 names the
	// sequence it found, 10164 does not); anything else was a check that ran before the send.
	if se := jsAPIError(err); se == nil || !IsNatsErr(se, JSStreamWrongLastSequenceErrF, JSStreamWrongLastSequenceConstantErr) {
		t.Fatalf("B was refused by something other than the store's CAS: %v", err)
	}
	cur, _, _ := e.asm.jsa.loadInflightRecord(subj)
	if cur == nil || cur.ActionToken != reqA.Token || cur.Acq != 1 {
		t.Fatalf("the register is not A's acquisition: %+v", cur)
	}
	smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttActionSubject(getHash(e.id), auth.Gen, nonce, 1))
	if lerr != nil || smsg == nil {
		t.Fatalf("A's action record is missing: %v", lerr)
	}
	a, derr := mqttDecodeOutboundAction(smsg.Data, e.id, e.epoch, auth.Gen, nonce, 1)
	if derr != nil || a.AcqTxn != "txn-a" {
		t.Fatalf("the action record is not A's: %+v (%v)", a, derr)
	}
}
