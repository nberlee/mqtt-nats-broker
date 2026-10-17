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
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
)

// The PUBREL side of F-105 row 2, on the real store: an ordinary PUBREL is acquired ONLY as one
// authority batch of its token and its action record, under the connection's own accepted
// entitlement, and its frame is written through the authorized boundary, consumed once and only
// for its live holder. Every test asserts it reached the route it names, so a mutation that stops
// the route cannot pass it by refusing earlier.

// testRow2Batch is the one landed acquire-action transition a test observed, split into its
// register token member and its action member.
type testRow2Batch struct {
	reg    *mqttInflightRecord
	expect uint64
	action *mqttOutboundAction
}

func testRow2Split(t *testing.T, e *testInflightEnv, tr *mqttAuthorityTransition) testRow2Batch {
	t.Helper()
	var b testRow2Batch
	for _, m := range tr.Extra {
		if cls, ok := mqttF105ObligationClass(m.subj); ok && cls == mqttActionClassTk {
			var a mqttOutboundAction
			if err := json.Unmarshal(m.body, &a); err != nil {
				t.Fatalf("the batch's action member does not decode: %v", err)
			}
			b.action = &a
			continue
		}
		if mqttIsF105Subject(m.subj) || m.expect == 0 {
			continue
		}
		if _, perr := mqttParseInflightSubject(m.subj); perr != nil {
			continue
		}
		rec, derr := mqttDecodeInflightRecord(m.body)
		if derr != nil {
			t.Fatalf("the batch's register member does not decode: %v", derr)
		}
		b.reg, b.expect = rec, m.expect
	}
	if b.reg == nil || b.action == nil {
		t.Fatalf("the acquisition batch did not carry BOTH carriers: register=%v action=%v", b.reg != nil, b.action != nil)
	}
	return b
}

// T1. The ready-slot PUBREL acquisition is ONE authority batch of the token and the action
// record, and the holder carries the exact record that landed. Kills M1, M3a.
func TestF105Row2PubRelAcquisitionIsOneAuthorityBatch(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t1")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	var landed []*mqttAuthorityTransition
	testObserveAcquisitions(t, e, func(tr *mqttAuthorityTransition) { landed = append(landed, tr) })

	ref, cur, seq := testRow2Ref(t, e, seed.subject)
	act, ended := e.sess.mqttAcquirePubRelAction(e.c, ref, cur.Lin, 0)
	if act == nil || ended {
		t.Fatalf("the ordinary ready-slot acquisition failed (ended=%v)", ended)
	}
	if len(landed) != 1 {
		t.Fatalf("%d acquire-action batches landed, want exactly the one: the acquisition did not go "+
			"through the authority", len(landed))
	}
	b := testRow2Split(t, e, landed[0])
	if b.expect != seq || b.reg.ActionToken != act.token || b.reg.Acq != cur.Acq+1 ||
		b.reg.State != mqttInflightPubRelReady {
		t.Fatalf("the batch's token member is not this acquisition over the exact occupant: %+v at %d (read %d)",
			b.reg, b.expect, seq)
	}
	if b.action.Kind != mqttActionPubRel || b.action.Nonce != cur.Lin || b.action.Acq != cur.Acq+1 || b.action.PI != seed.pi {
		t.Fatalf("the batch's action member is not this PUBREL acquisition: %+v", b.action)
	}
	after, _, _ := e.asm.loadInflightRecord(seed.subject)
	if after == nil || after.ActionToken != act.token || after.Acq != cur.Acq+1 {
		t.Fatalf("the register does not carry the landed token: %+v", after)
	}
	stored := testRow2Action(t, e, gen, cur.Lin, cur.Acq+1)
	if stored == nil {
		t.Fatalf("no action record landed for %s/%d", cur.Lin, cur.Acq+1)
	}
	if act.outAction == nil || !reflect.DeepEqual(*act.outAction, *stored) {
		t.Fatalf("the holder does not carry the exact record that landed: holder=%+v stored=%+v", act.outAction, stored)
	}
	e.sess.startActionDrain(e.c, act)
}

// T1 (delivery). A delivered ordinary PUBREL transmits its frame through the authorized
// boundary, which consumes that acquisition exactly once. Kills M1, M3a, M8's PUBREL half.
func TestF105Row2DeliveredPubRelIsTheExecutionOfItsAcquisition(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t1-delivery")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	wire := testRow2Wire(e)
	var landed []*mqttAuthorityTransition
	testObserveAcquisitions(t, e, func(tr *mqttAuthorityTransition) { landed = append(landed, tr) })

	// The holder is captured at its write, while it is still the identifier's registered one: after
	// the callback's drain the session no longer reaches it.
	var held *mqttSendAction
	writes := 0
	restore := setMQTTTestPubRelWriteBlock(func() { writes++; held = testRow2Holder(e, seed.pi) })
	testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
	restore()
	if writes != 1 || len(landed) != 1 || held == nil {
		t.Fatalf("the delivery did not acquire through the authority and enter its write "+
			"(entered %d, acquisitions landed %d)", writes, len(landed))
	}
	if !bytes.Equal(wire.written, testRow2PubRelFrame(seed.pi)) {
		t.Fatalf("the wire carries %x, want exactly one PUBREL %x", wire.written, testRow2PubRelFrame(seed.pi))
	}
	b := testRow2Split(t, e, landed[0])
	if st, ok := testRow2CellState(held.exec, gen, seed.lin, b.action.Acq); !ok || st != mqttFrameFinished {
		t.Fatalf("the acquisition %s/%d is %v (present=%v) in the connection's guard, want FINISHED: the "+
			"frame was not the execution of its acquisition", seed.lin, b.action.Acq, st, ok)
	}
	testRow2Eventually(t, testRow2Timeout, "the deferred drain to clear the token", func() bool {
		cur, _, _ := e.asm.loadInflightRecord(seed.subject)
		return cur != nil && cur.ActionToken == _EMPTY_
	})
}

// testRow2IntendedSlot admits an ordinary QoS 2 delivery, stores its canonical PUBREL, and
// records the PUBREC the stored record proves, leaving the slot at pubrel_intended with no tuple:
// the shape the PUBREL direct arrow binds.
func testRow2IntendedSlot(t *testing.T, e *testInflightEnv) (pi uint16, subject, pubRelDur string, canon uint64) {
	t.Helper()
	pi = e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 2)
	e.sess.mu.Lock()
	idHash, gen := e.sess.idHash, e.sess.gen
	e.sess.mu.Unlock()
	subject = mqttInflightSubject(idHash, gen, pi)
	_, _, pubRelDur = mqttPubRelNames(idHash, gen)
	nonce := e.nonceOf(pi)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, nonce, testRealPubRelSrcDur, testRealPubRelSrcSeq, _EMPTY_, _EMPTY_, _EMPTY_)
	stored, serr := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg)
	if serr != nil || stored == nil || stored.Sequence == 0 {
		t.Fatalf("storing the canonical PUBREL: %+v (%v)", stored, serr)
	}
	ref, _, _ := testRow2Ref(t, e, subject)
	if got := e.sess.recordRecoveredPubRec(ref); got != mqttRecoveredPubRecRecorded {
		t.Fatalf("recording the PUBREC the stored record proves: %v", got)
	}
	cur, _, _ := e.asm.loadInflightRecord(subject)
	if cur == nil || cur.State != mqttInflightPubRelIntended || cur.PubRelStreamSeq != 0 || cur.ActionToken != _EMPTY_ {
		t.Fatalf("the fixture must start from an owed pubrel_intended slot with no tuple: %+v", cur)
	}
	return pi, subject, pubRelDur, stored.Sequence
}

// T2. The direct arrow on an ordinary slot: the canonical tuple, intended -> ready, the token and
// the action record in ONE authority batch, and the PUBREL transmits. Kills M2.
func TestF105Row2DirectArrowRebindsInsideTheAcquisitionBatch(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t2")
	testReadyForRealPubRelDelivery(t, e)
	pi, subject, pubRelDur, canon := testRow2IntendedSlot(t, e)
	_, before, beforeSeq := testRow2Ref(t, e, subject)
	wire := testRow2Wire(e)
	var landed []*mqttAuthorityTransition
	testObserveAcquisitions(t, e, func(tr *mqttAuthorityTransition) { landed = append(landed, tr) })

	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject(pubRelDur, canon, 1), before.Nonce)
	})
	if len(landed) != 1 {
		t.Fatalf("%d acquire-action batches landed, want the direct arrow's one", len(landed))
	}
	b := testRow2Split(t, e, landed[0])
	if b.expect != beforeSeq || b.reg.State != mqttInflightPubRelReady || b.reg.PubRelDurable != pubRelDur ||
		b.reg.PubRelStreamSeq != canon || b.reg.ActionToken == _EMPTY_ || b.reg.Acq != before.Acq+1 {
		t.Fatalf("the batch did not rebind the canonical tuple and install the token over the intended "+
			"occupant at %d: %+v at %d", beforeSeq, b.reg, b.expect)
	}
	if b.action.Kind != mqttActionPubRel || b.action.Nonce != before.Lin || b.action.Acq != before.Acq+1 {
		t.Fatalf("the batch's action member is not this PUBREL acquisition: %+v", b.action)
	}
	if writes != 1 || !bytes.Equal(wire.written, testRow2PubRelFrame(pi)) {
		t.Fatalf("the rebound PUBREL did not transmit once (entered %d, wire %x)", writes, wire.written)
	}
}

// T3, the ready-slot site, the immediate adoption. The acquisition lands and its response is
// lost: the helper reports NOT acquired, the reread adopts the token for its drain only, nothing
// is written, and the acquisition is ABANDONED on the connection it was acquired for, at the
// counter read beside the token. Kills M10, M7c's immediate half.
func TestF105Row2LostPubRelAcquisitionIsAdoptedNeverEmitted(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t3-ready")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	wire := testRow2Wire(e)
	lost := testLoseNextAcquisitionResponse(t, e)

	ref, cur, _ := testRow2Ref(t, e, seed.subject)
	act, ended := e.sess.mqttAcquirePubRelAction(e.c, ref, cur.Lin, 0)
	if !lost.Load() {
		t.Fatal("the acquisition batch never landed-and-lost, so nothing here is tested")
	}
	if act != nil || ended {
		t.Fatalf("an acquisition whose response was lost reported acquired=%v ended=%v; it may only be "+
			"adopted for its drain", act != nil, ended)
	}
	if a := testRow2Action(t, e, gen, cur.Lin, cur.Acq+1); a == nil {
		t.Fatal("the lost acquisition's record is not on the store: the loss was not a landed batch")
	}
	testRow2Eventually(t, testRow2Timeout, "the adopted token to drain", func() bool {
		c, _, _ := e.asm.loadInflightRecord(seed.subject)
		return c != nil && c.ActionToken == _EMPTY_
	})
	// The adopted holder is never returned to the caller: read by connection (unreachable by design).
	if st, ok := testRow2GuardState(e.c, gen, cur.Lin, cur.Acq+1); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the adopted acquisition is %v (present=%v) in its connection's guard, want ABANDONED", st, ok)
	}
	if len(wire.written) != 0 {
		t.Fatalf("an adopted acquisition put %x on the wire", wire.written)
	}
}

// T3, the ready-slot site, the late discovery: the resolving read is unavailable, so the holder
// cannot be adopted at once; the reconcile that later proves the token ours abandons it then.
// Kills M7c.
func TestF105Row2LostPubRelAcquisitionIsAbandonedWhenReconcileProvesIt(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t3-reconcile")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	wire := testRow2Wire(e)
	var fail atomic.Bool
	testRow2FailLoadsWhile(e, &fail)
	var lost atomic.Bool
	testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if lost.Load() || testAcquisitionTokenOf(tr) == _EMPTY_ {
			return send()
		}
		seq, err := send()
		if err != nil {
			return seq, err
		}
		lost.Store(true)
		fail.Store(true) // every read from here is indeterminate, until the test restores them
		return 0, errors.New("test: the acquisition landed but its response was lost")
	})

	ref, cur, _ := testRow2Ref(t, e, seed.subject)
	act, _ := e.sess.mqttAcquirePubRelAction(e.c, ref, cur.Lin, 0)
	if !lost.Load() {
		t.Fatal("the acquisition batch never landed-and-lost, so nothing here is tested")
	}
	if act != nil {
		t.Fatal("an unresolved acquisition reported acquired")
	}
	// No holder was returned and none is reachable: read by connection (unreachable by design).
	if _, ok := testRow2GuardState(e.c, gen, cur.Lin, cur.Acq+1); ok {
		t.Fatal("the acquisition was recorded before any read proved it ours")
	}
	fail.Store(false)
	testRow2Eventually(t, 3*mqttActionReconcileDelay, "the reconcile to drain the token", func() bool {
		c, _, _ := e.asm.jsa.loadInflightRecord(seed.subject)
		return c != nil && c.ActionToken == _EMPTY_
	})
	// The reconcile's holder is internal to the session: read by connection (unreachable by design).
	if st, ok := testRow2GuardState(e.c, gen, cur.Lin, cur.Acq+1); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the late-proven acquisition is %v (present=%v), want ABANDONED", st, ok)
	}
	if len(wire.written) != 0 {
		t.Fatalf("an unresolved acquisition put %x on the wire", wire.written)
	}
}

// T3, the direct-arrow site: the rebinding acquisition lands and its response is lost. Kills M10.
func TestF105Row2LostDirectArrowAcquisitionIsAdoptedNeverEmitted(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t3-direct")
	testReadyForRealPubRelDelivery(t, e)
	pi, subject, pubRelDur, canon := testRow2IntendedSlot(t, e)
	gen := testRow2Gen(e)
	wire := testRow2Wire(e)
	lost := testLoseNextAcquisitionResponse(t, e)

	ref, cur, _ := testRow2Ref(t, e, subject)
	act, ended := e.sess.acquireActionFromPublish(e.c, ref, cur.Lin, pubRelDur, canon)
	if !lost.Load() {
		t.Fatal("the direct arrow's batch never landed-and-lost, so nothing here is tested")
	}
	if act != nil || ended {
		t.Fatalf("a direct-arrow acquisition whose response was lost reported acquired=%v ended=%v", act != nil, ended)
	}
	testRow2Eventually(t, testRow2Timeout, "the adopted token to drain", func() bool {
		c, _, _ := e.asm.loadInflightRecord(subject)
		return c != nil && c.ActionToken == _EMPTY_
	})
	// The adopted direct-arrow holder is never returned: read by connection (unreachable by design).
	if st, ok := testRow2GuardState(e.c, gen, cur.Lin, cur.Acq+1); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the adopted direct-arrow acquisition is %v (present=%v), want ABANDONED", st, ok)
	}
	if len(wire.written) != 0 {
		t.Fatalf("an adopted acquisition put %x on the wire", wire.written)
	}
	_ = pi
}

// T3b. A failed candidate abandons nothing: a peer acquires counter n between our batch's
// composition and its send, the store refuses ours, and our connection's guard has no entry for
// n — it is the peer's acquisition, not ours. Kills M7d.
func TestF105Row2AFailedCandidateAbandonsNothing(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t3b")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	ref, cur, curSeq := testRow2Ref(t, e, seed.subject)
	ent, _ := e.c.outboundEntitlement()
	peerTok := mqttNewInflightNonce()
	var peerLanded, oursRefused atomic.Bool
	var inPeer atomic.Bool
	testSetTransitionCommit(t, e, func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		tok := testAcquisitionTokenOf(tr)
		if tok == _EMPTY_ || inPeer.Load() || peerLanded.Load() {
			return send()
		}
		inPeer.Store(true)
		_, _, perr := e.asm.jsa.opAcquireAction(&mqttAcquireActionRequest{
			ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-peer",
			Gen: ent.Gen, Tenure: ent.Tenure, Nonce: cur.Lin, Acq: cur.Acq + 1, Kind: mqttActionPubRel,
			Binding: mqttBindConnection(990001), Register: cur, RegisterSeq: curSeq, Token: peerTok,
		})
		inPeer.Store(false)
		if perr != nil {
			t.Errorf("the peer's competing acquisition was refused: %v", perr)
			return send()
		}
		peerLanded.Store(true)
		seq, err := send()
		oursRefused.Store(err != nil)
		return seq, err
	})

	act, _ := e.sess.mqttAcquirePubRelAction(e.c, ref, cur.Lin, 0)
	if !peerLanded.Load() || !oursRefused.Load() {
		t.Fatalf("the race never happened (peer landed=%v, ours refused=%v), so nothing here is tested",
			peerLanded.Load(), oursRefused.Load())
	}
	if act != nil {
		t.Fatal("an acquisition the store refused reported acquired")
	}
	if after, _, _ := e.asm.loadInflightRecord(seed.subject); after == nil || after.ActionToken != peerTok ||
		after.Acq != cur.Acq+1 {
		t.Fatalf("the peer's acquisition is not the one standing: %+v", after)
	}
	// A refused candidate's holder is retired inside the call: read by connection (unreachable by design).
	if testRow2GuardHasLineage(e.c, gen, cur.Lin) {
		t.Fatalf("our connection's guard holds an entry for %s: a failed candidate's counter was "+
			"recorded, and it names the PEER's acquisition", cur.Lin)
	}
}

// testRow2SuccessorTenure rewrites the standing authority to a successor's tenure, as a
// takeover's accept does: this connection's accepted entitlement no longer serves.
func testRow2SuccessorTenure(t *testing.T, e *testInflightEnv) {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("no authority to displace: %+v", al)
	}
	succ := *al.rec
	succ.Tenure, succ.Rev, succ.Txn = mqttNewInflightNonce(), al.rec.Rev+1, "txn-successor"
	body, err := json.Marshal(&succ)
	if err != nil {
		t.Fatalf("marshalling the successor authority: %v", err)
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, body, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.sess.id)), hdr, msg); err != nil {
		t.Fatalf("installing the successor's tenure: %v", err)
	}
	if ent, _ := e.c.outboundEntitlement(); ent.Tenure == succ.Tenure {
		t.Fatal("the fixture connection must still carry its OWN, now displaced, entitlement")
	}
}

// T4. After a takeover, this connection's acquisitions are refused as an ENDED entitlement —
// terminal for the delivery, which does not redispatch — and nothing is acquired or written.
// Kills M9.
func TestF105Row2AnEndedEntitlementAcquiresNothingAndEndsTheDelivery(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t4")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	wire := testRow2Wire(e)
	ref, cur, _ := testRow2Ref(t, e, seed.subject)
	testRow2SuccessorTenure(t, e)

	// The reason, from the helper itself.
	act, ended := e.sess.mqttAcquirePubRelAction(e.c, ref, cur.Lin, 0)
	if act != nil || !ended {
		t.Fatalf("an acquisition under a displaced tenure reported acquired=%v ended=%v, want ended", act != nil, ended)
	}

	// And through the delivery callback: one dispatch, no redispatch. Every dispatch of
	// admitStoredPubRel reads the slot at its top; a redispatch would read it again.
	testInstallPassThroughSeams(e)
	var reads atomic.Int64
	e.asm.mu.Lock()
	load := e.asm.testInflightLoad
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if subject == seed.subject {
			reads.Add(1)
		}
		return load(subject)
	}
	e.asm.mu.Unlock()
	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
	})
	if reads.Load() == 0 {
		t.Fatal("the delivery never read the slot, so it never reached admission")
	}
	t.Logf("one dispatch read the slot %d time(s)", reads.Load())
	if n := reads.Load(); n > testRow2OneDispatchReads {
		t.Fatalf("the delivery read the slot %d times, more than one dispatch's %d: an ended entitlement "+
			"was redispatched", n, testRow2OneDispatchReads)
	}
	if writes != 0 || len(wire.written) != 0 {
		t.Fatalf("a displaced connection wrote (entered %d, wire %x)", writes, wire.written)
	}
	if after, _, _ := e.asm.jsa.loadInflightRecord(seed.subject); after == nil || after.ActionToken != _EMPTY_ {
		t.Fatalf("a displaced connection left a token: %+v", after)
	}
	if a := testRow2Action(t, e, gen, cur.Lin, cur.Acq+1); a != nil {
		t.Fatalf("a displaced connection created an action record: %+v", a)
	}
}

// testRow2OneDispatchReads is the slot reads one admitStoredPubRel dispatch of a ready slot makes
// when its acquisition ends on the entitlement: admission's own read, the acquisition's premise
// read, and the resolving read of the unacquired holder.
const testRow2OneDispatchReads = 3

// testRow2Acquired acquires the seeded ready slot's PUBREL through the production helper.
func testRow2AcquiredPubRel(t *testing.T, e *testInflightEnv, seed testRealPubRelSeed) (*mqttSendAction, *mqttInflightRecord) {
	t.Helper()
	ref, cur, _ := testRow2Ref(t, e, seed.subject)
	act, ended := e.sess.mqttAcquirePubRelAction(e.c, ref, cur.Lin, 0)
	if act == nil || ended || act.outAction == nil {
		t.Fatalf("acquiring the fixture's ordinary PUBREL failed (ended=%v)", ended)
	}
	return act, cur
}

func testRow2Enter(e *testInflightEnv, act *mqttSendAction, c *client) (*mqttWriteEntry, bool) {
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	return e.sess.enterPubRelWriteLocked(act, c)
}

// T7. A saved request for a holder that is no longer live cannot re-enter its write: entry is
// decided by the holder's state, not by the record's existence. Each variant starts from a guard
// with NO entry for the key, so only the holder can refuse it. Kills M4.
func TestF105Row2EntryRefusesAHolderThatIsNoLongerLive(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t7")
	testReadyForRealPubRelDelivery(t, e)
	gen := testRow2Gen(e)
	// Each variant seeds its OWN source delivery. The variants share one session and leave
	// their exchanges past PUBREC, and one source delivery has at most one un-ended exchange:
	// re-seeding the same source would ask the broker to admit a second exchange for a source
	// it is still holding at PUBREL, which it refuses (F-115).
	nextSeq := testRealPubRelSrcSeq
	seedFresh := func() testRealPubRelSeed {
		t.Helper()
		nextSeq++
		return testSeedRealPubRelReadyAt(t, e, nextSeq)
	}

	t.Run("released before the write", func(t *testing.T) {
		seed := seedFresh()
		act, cur := testRow2AcquiredPubRel(t, e, seed)
		// A FRESH guard for the key: the drain's abandonment is removed, so consumption would
		// admit a write and only the holder check stands between the saved request and the wire.
		e.sess.startActionDrain(e.c, act)
		testRow2ResetKey(e.c, mqttActionKey{gen: gen, nonce: cur.Lin, acq: cur.Acq + 1})
		if _, ok := testRow2Enter(e, act, e.c); ok {
			t.Fatal("a released holder re-entered its write")
		}
		if _, ok := testRow2CellState(act.exec, gen, cur.Lin, cur.Acq+1); ok {
			t.Fatal("a refused entry consumed the acquisition")
		}
	})
	t.Run("already writing", func(t *testing.T) {
		seed := seedFresh()
		act, _ := testRow2AcquiredPubRel(t, e, seed)
		ticket, ok := testRow2Enter(e, act, e.c)
		if !ok {
			t.Fatal("the positive control: a live acquired holder must enter")
		}
		if _, again := testRow2Enter(e, act, e.c); again {
			t.Fatal("a holder already writing entered a second time")
		}
		ticket.relinquish()
		e.sess.startActionDrain(e.c, act)
	})
	// A SAVED VALUE COPY of the holder carries the same token and a stale lifecycle; only pointer
	// membership in the registry refuses it (holistic review round 1). The original is live in
	// the first case and entered in the second, so the copy's own fields still say "acquired".
	t.Run("a saved copy while the original writes", func(t *testing.T) {
		seed := seedFresh()
		act, _ := testRow2AcquiredPubRel(t, e, seed)
		e.sess.mu.Lock()
		saved := *act
		e.sess.mu.Unlock()
		ticket, ok := testRow2Enter(e, act, e.c)
		if !ok {
			t.Fatal("the positive control: the registered holder must enter")
		}
		if _, again := testRow2Enter(e, &saved, e.c); again {
			t.Fatal("a saved copy of a holder that is already writing entered its own write")
		}
		ticket.relinquish()
		e.sess.startActionDrain(e.c, act)
	})
	t.Run("a saved copy after the original is released", func(t *testing.T) {
		seed := seedFresh()
		act, cur := testRow2AcquiredPubRel(t, e, seed)
		e.sess.mu.Lock()
		saved := *act
		e.sess.mu.Unlock()
		e.sess.startActionDrain(e.c, act)
		testRow2ResetKey(e.c, mqttActionKey{gen: gen, nonce: cur.Lin, acq: cur.Acq + 1})
		if _, ok := testRow2Enter(e, &saved, e.c); ok {
			t.Fatal("a saved copy of a released holder entered its write")
		}
	})
	t.Run("drained", func(t *testing.T) {
		seed := seedFresh()
		act, _ := testRow2AcquiredPubRel(t, e, seed)
		e.sess.mu.Lock()
		act.drained = true
		e.sess.mu.Unlock()
		if _, ok := testRow2Enter(e, act, e.c); ok {
			t.Fatal("a drained holder entered its write")
		}
		e.sess.mu.Lock()
		act.drained = false
		e.sess.mu.Unlock()
		e.sess.startActionDrain(e.c, act)
	})
	t.Run("replaced as the identifier's holder", func(t *testing.T) {
		seed := seedFresh()
		act, _ := testRow2AcquiredPubRel(t, e, seed)
		e.sess.mu.Lock()
		h := e.sess.sendsInFlight[seed.pi]
		h.action = &mqttSendAction{token: mqttNewInflightNonce(), state: mqttSendActionAcquired, slot: act.slot}
		e.sess.mu.Unlock()
		if _, ok := testRow2Enter(e, act, e.c); ok {
			t.Fatal("a holder the identifier no longer registers entered its write")
		}
		e.sess.mu.Lock()
		h.action = act
		e.sess.mu.Unlock()
		e.sess.startActionDrain(e.c, act)
	})
	t.Run("token cleared", func(t *testing.T) {
		seed := seedFresh()
		act, _ := testRow2AcquiredPubRel(t, e, seed)
		e.sess.mu.Lock()
		tok := act.slot.actionToken
		act.slot.actionToken = _EMPTY_
		e.sess.mu.Unlock()
		if _, ok := testRow2Enter(e, act, e.c); ok {
			t.Fatal("a holder with no live token entered its write")
		}
		e.sess.mu.Lock()
		act.slot.actionToken = tok
		e.sess.mu.Unlock()
		e.sess.startActionDrain(e.c, act)
	})
}

// T7b. Entry and the writer compare the EXACT acquisition: a holder whose record names another
// counter, identifier, packet or lineage, or was acquired for another connection, does not enter;
// a ticket used for another action, on another connection, or twice does not write — and each is
// refused before consumption. Kills M4, M4b.
func TestF105Row2EntryAndTicketCompareTheExactAcquisition(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t7b")
	testReadyForRealPubRelDelivery(t, e)
	gen := testRow2Gen(e)
	seed := testSeedRealPubRelReady(t, e)
	act, cur := testRow2AcquiredPubRel(t, e, seed)
	var other client

	for name, mutate := range map[string]func(h *mqttSendAction){
		"another counter":    func(h *mqttSendAction) { a := *h.outAction; a.Acq++; h.outAction = &a },
		"another identifier": func(h *mqttSendAction) { a := *h.outAction; a.PI++; h.outAction = &a },
		"another packet":     func(h *mqttSendAction) { a := *h.outAction; a.Kind = mqttActionPublish; h.outAction = &a },
		"another lineage":    func(h *mqttSendAction) { a := *h.outAction; a.Nonce = mqttNewInflightNonce(); h.outAction = &a },
	} {
		e.sess.mu.Lock()
		clone := *act
		e.sess.mu.Unlock()
		mutate(&clone)
		if _, ok := testRow2Enter(e, &clone, e.c); ok {
			t.Fatalf("a holder whose acquisition names %s entered the write", name)
		}
	}
	// ANOTHER CONNECTION, on the REGISTERED holder itself: a clone is refused by registry membership
	// before the connection is compared, so only mutating the live holder's cell reaches the
	// comparison. Restored before the positive control.
	e.sess.mu.Lock()
	own := act.exec
	act.exec = &mqttExecCell{conn: &other}
	e.sess.mu.Unlock()
	if _, ok := testRow2Enter(e, act, e.c); ok {
		t.Fatal("a holder whose acquisition names another connection entered the write")
	}
	e.sess.mu.Lock()
	act.exec = own
	e.sess.mu.Unlock()

	ticket, ok := testRow2Enter(e, act, e.c)
	if !ok {
		t.Fatal("the positive control: the exact live holder must enter")
	}
	defer ticket.relinquish()
	wire := testRow2Wire(e)
	frame := testRow2PubRelFrame(seed.pi)
	key := mqttActionKey{gen: gen, nonce: cur.Lin, acq: cur.Acq + 1}

	fake := *ticket.action
	if _, err := e.c.writeAuthorizedFrame(mqttFrameRequest{Action: &fake, Binding: e.c.outBind, Holder: ticket,
		Parts: [][]byte{frame}}); err == nil || !bytes.Contains([]byte(err.Error()), []byte("not held for this execution")) {
		t.Fatalf("a ticket used for another action was not refused by the holder check: %v", err)
	}
	c2wire := &testFrameConn{}
	c2 := &client{nc: c2wire, rawConn: c2wire, outBind: e.c.outBind, mqtt: e.c.mqtt}
	if _, err := c2.writeAuthorizedFrame(mqttFrameRequest{Action: ticket.action, Binding: e.c.outBind, Holder: ticket,
		Parts: [][]byte{frame}}); err == nil || !bytes.Contains([]byte(err.Error()), []byte("not held for this execution")) {
		t.Fatalf("a ticket used on another connection was not refused by the holder check: %v", err)
	}
	if len(wire.written) != 0 || len(c2wire.written) != 0 {
		t.Fatalf("a refused ticket wrote (%x / %x)", wire.written, c2wire.written)
	}
	if _, consumed := testRow2CellState(act.exec, gen, cur.Lin, cur.Acq+1); consumed {
		t.Fatal("a refused ticket consumed the acquisition")
	}
	if res, err := e.c.writeAuthorizedFrame(mqttFrameRequest{Action: ticket.action, Binding: e.c.outBind, Holder: ticket,
		Parts: [][]byte{frame}}); err != nil || res != mqttOutputCompleted || !bytes.Equal(wire.written, frame) {
		t.Fatalf("the positive control: the ticket's own execution must complete (%v, %v, %x)", res, err, wire.written)
	}
	if _, err := e.c.writeAuthorizedFrame(mqttFrameRequest{Action: ticket.action, Binding: e.c.outBind, Holder: ticket,
		Parts: [][]byte{frame}}); err == nil || !bytes.Contains([]byte(err.Error()), []byte("not held for this execution")) {
		t.Fatalf("a used ticket was not refused by the holder check before consumption: %v", err)
	}
	if !bytes.Equal(wire.written, frame) {
		t.Fatalf("a used ticket wrote again: %x", wire.written)
	}
	if st, _ := testRow2CellState(act.exec, key.gen, key.nonce, key.acq); st != mqttFrameFinished {
		t.Fatalf("the executed acquisition is %v, want FINISHED", st)
	}
	e.sess.startActionDrain(e.c, act)
}

// T10a. The entry refusal itself, observed at entry: an ordinary holder with no acquisition record
// does not enter, and stays acquired. Kills M13a.
func TestF105Row2AnOrdinaryHolderWithoutItsRecordDoesNotEnter(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t10a")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	act, _ := testRow2AcquiredPubRel(t, e, seed)
	e.sess.mu.Lock()
	saved := act.outAction
	act.outAction = nil
	e.sess.mu.Unlock()
	if _, ok := testRow2Enter(e, act, e.c); ok {
		t.Fatal("an ordinary PUBREL holder with no acquisition record entered its write")
	}
	e.sess.mu.Lock()
	state := act.state
	act.outAction = saved
	e.sess.mu.Unlock()
	if state != mqttSendActionAcquired {
		t.Fatalf("the refused entry moved the holder to %v", state)
	}
	e.sess.startActionDrain(e.c, act)
}

// T11 (PUBREL). A PUBREL given up before its write is ABANDONED in this connection's frame guard;
// one acquired and not yet written has no frame state at all. Kills M7a.
func TestF105Row2APubRelGivenUpBeforeItsWriteIsFinishedHere(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t11")
	testReadyForRealPubRelDelivery(t, e)
	gen := testRow2Gen(e)
	state := func(act *mqttSendAction, lin string, acq uint64) (mqttFrameState, bool) {
		return testRow2CellState(act.exec, gen, lin, acq)
	}

	seedA := testSeedRealPubRelReady(t, e)
	actA, curA := testRow2AcquiredPubRel(t, e, seedA)
	if st, ok := state(actA, curA.Lin, curA.Acq+1); ok {
		t.Fatalf("an acquired, unwritten PUBREL reads %v before it was given up", st)
	}
	e.sess.startActionDrain(e.c, actA)
	if st, ok := state(actA, curA.Lin, curA.Acq+1); !ok || st != mqttFrameAbandoned {
		t.Fatalf("a PUBREL given up before its write reads %v (present=%v), want ABANDONED", st, ok)
	}

	seedB := testSeedRealPubRelReadyAt(t, e, testRealPubRelSrcSeq+1)
	actB, curB := testRow2AcquiredPubRel(t, e, seedB)
	if st, ok := state(actB, curB.Lin, curB.Acq+1); ok {
		t.Fatalf("an acquired, unwritten PUBREL reads %v", st)
	}
	e.sess.startActionDrain(e.c, actB)
}

// T14. Nonce != Lin: after a register-only replacement re-mints the allocation nonce, the
// acquisition, its record, its execution and its abandonment are all keyed by the obligation's
// LINEAGE. Kills M15.
func TestF105Row2AnAcquisitionIsKeyedByItsLineageNotItsAllocation(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t14")
	testReadyForRealPubRelDelivery(t, e)
	gen := testRow2Gen(e)
	seed := testSeedRealPubRelReady(t, e)
	cur, smsg, err := e.asm.jsa.loadInflightRecord(seed.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the seeded slot: %v", err)
	}
	// THE REGISTER-ONLY REPLACEMENT'S SHAPE: a fresh allocation nonce, the lineage surviving.
	next := *cur
	next.Nonce = mqttNewInflightNonce()
	if _, err := e.asm.jsa.storeInflightCAS(seed.subject, smsg.Sequence, &next); err != nil {
		t.Fatalf("re-minting the allocation nonce: %v", err)
	}
	if next.Nonce == next.Lin {
		t.Fatal("the fixture must have Nonce != Lin")
	}

	// Abandoned before its write: under the lineage.
	act1, c1 := testRow2AcquiredPubRel(t, e, seed)
	if a := testRow2Action(t, e, gen, next.Lin, c1.Acq+1); a == nil {
		t.Fatal("the action record is not named by the lineage")
	}
	if a := testRow2Action(t, e, gen, next.Nonce, c1.Acq+1); a != nil {
		t.Fatal("an action record is named by the allocation nonce")
	}
	e.sess.startActionDrain(e.c, act1)
	if st, ok := testRow2CellState(act1.exec, gen, next.Lin, c1.Acq+1); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the given-up acquisition is %v (present=%v) under its lineage, want ABANDONED", st, ok)
	}
	if _, ok := testRow2CellState(act1.exec, gen, next.Nonce, c1.Acq+1); ok {
		t.Fatal("an acquisition was recorded under the allocation nonce")
	}

	// Executed: under the lineage too.
	act2, c2 := testRow2AcquiredPubRel(t, e, seed)
	wire := testRow2Wire(e)
	ticket, ok := testRow2Enter(e, act2, e.c)
	if !ok {
		t.Fatal("the lineage-keyed holder did not enter its write")
	}
	e.c.mqttWriteAuthorizedPubRel(seed.pi, ticket, false)
	ticket.relinquish()
	if !bytes.Equal(wire.written, testRow2PubRelFrame(seed.pi)) {
		t.Fatalf("the lineage-keyed acquisition did not write its PUBREL: %x", wire.written)
	}
	if st, ok := testRow2CellState(act2.exec, gen, next.Lin, c2.Acq+1); !ok || st != mqttFrameFinished {
		t.Fatalf("the executed acquisition is %v (present=%v) under its lineage, want FINISHED", st, ok)
	}
	e.sess.startActionDrain(e.c, act2)
}

// testRow2SlotOnlyReady seeds a slot-only (shared or affine) PUBREL-ready slot in the map fixture
// store: a class with no exchange, which keeps the slot machine's own CAS until 7a.
func testRow2SlotOnlyReady(t *testing.T, e *testInflightEnv, pi uint16, dur string) mqttInflightSlotRef {
	t.Helper()
	e.sess.mu.Lock()
	idHash, gen, claim := e.sess.idHash, e.sess.gen, e.sess.regClaim
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := freshRegisterLineage(&mqttInflightRecord{
		PI: pi, Dur: dur, Seq: 41, QoS: 2, Nonce: mqttNewInflightNonce(), State: mqttInflightPubRelReady,
		Claim: claim, ClaimSeq: 1, PubRelDurable: "PUBREL_F089", PubRelStreamSeq: testCanonicalPubRelSeq,
	})
	if !mqttDurIsSlotOnlyClass(rec.Dur) {
		t.Fatalf("the fixture must be slot-only: %q", rec.Dur)
	}
	seq, err := e.asm.testInflightStore(subject, 0, rec)
	if err != nil {
		t.Fatalf("seeding the slot-only slot: %v", err)
	}
	ref := mqttSlotRefFromRecord(subject, seq, rec)
	if ref == nil {
		t.Fatal("the seeded slot yields no reference")
	}
	return *ref
}

// T7c. A slot-only PUBREL takes the same conditional entry without an authority record: a released
// holder does not enter (so its raw emitter is never reached), a live one does and its ticket
// carries no action — the raw branch. And no action record is written for the class. Kills M4.
func TestF105Row2SlotOnlyPubRelTakesTheSameConditionalEntry(t *testing.T) {
	// The slot-only class is the experimental affine one since 7a: a shared delivery is ordinary
	// and QoS 1 (it has no PUBREL at all), so the former "shared" case describes a shape that can no
	// longer exist.
	for name, prefix := range map[string]string{
		"affine": mqttQ2AffineConsumerDurablePrefix,
	} {
		t.Run(name, func(t *testing.T) {
			e := testNewInflightEnv(t, "row2-t7c-"+name)
			testReadyForPubRelDelivery(t, e) // no authority: a slot-only class acquires without one
			gen := testRow2Gen(e)
			dur := prefix + e.sess.idHash + "_" + mqttNewInflightNonce()

			released := testRow2SlotOnlyReady(t, e, 21, dur)
			act, _ := e.sess.mqttAcquirePubRelAction(e.c, released, released.nonce, 0)
			if act == nil {
				t.Fatalf("the %s holder was not acquired by the slot machine's CAS", name)
			}
			if act.outAction != nil {
				t.Fatalf("a %s acquisition carries an authority record", name)
			}
			e.sess.startActionDrain(e.c, act)
			if _, ok := testRow2Enter(e, act, e.c); ok {
				t.Fatalf("a released %s holder entered its write", name)
			}

			live := testRow2SlotOnlyReady(t, e, 22, dur)
			act2, _ := e.sess.mqttAcquirePubRelAction(e.c, live, live.nonce, 0)
			if act2 == nil {
				t.Fatalf("the live %s holder was not acquired", name)
			}
			ticket, ok := testRow2Enter(e, act2, e.c)
			if !ok {
				t.Fatalf("the live %s holder did not enter its write", name)
			}
			if ticket.action != nil {
				t.Fatalf("a %s holder's ticket carries an authority action: its frame would take the "+
					"authorized branch", name)
			}
			ticket.relinquish()
			e.sess.startActionDrain(e.c, act2)
			for _, r := range []mqttInflightSlotRef{released, live} {
				if a := testRow2Action(t, e, gen, r.nonce, 1); a != nil {
					t.Fatalf("a %s acquisition wrote an action record: %+v", name, a)
				}
			}
		})
	}
}

// T9. A seal between write entry and the writer: nothing is written, the acquisition is
// ABANDONED by the relinquish armed at entry — not by the drain, which never abandons an entered
// holder — the holder is not restored to acquired, and the deferred drain still clears the token.
// Kills M6, M14b.
func TestF105Row2ASealAfterEntryAbandonsWithoutWriting(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t9")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	wire := testRow2Wire(e)
	var held *mqttSendAction
	reached := 0
	restore := setMQTTTestPubRelWriteBlock(func() {
		reached++
		held = testRow2Holder(e, seed.pi)
		sealOutput(e.c.outBind)
	})
	testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
	restore()
	if reached != 1 || held == nil {
		t.Fatalf("the delivery never entered its write (reached %d), so nothing here is tested", reached)
	}
	if len(wire.written) != 0 {
		t.Fatalf("a write past a sealed gate reached the wire: %x", wire.written)
	}
	acq := held.slot.acq
	if st, ok := testRow2CellState(held.exec, gen, seed.lin, acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the entered, unwritten acquisition is %v (present=%v), want ABANDONED", st, ok)
	}
	e.sess.mu.Lock()
	state := held.state
	e.sess.mu.Unlock()
	if state == mqttSendActionAcquired {
		t.Fatal("the entered holder was restored to acquired")
	}
	testRow2Eventually(t, testRow2Timeout, "the deferred drain to clear the token", func() bool {
		c, _, _ := e.asm.loadInflightRecord(seed.subject)
		return c != nil && c.ActionToken == _EMPTY_
	})
}

// T9b. An abnormal unwind after entry — a panic at the write-block point, recovered by the caller
// — writes nothing, still ABANDONS the acquisition, and the token still drains. Kills M14b, M14d.
func TestF105Row2AnUnwindAfterEntryStillAbandons(t *testing.T) {
	e := testNewInflightEnv(t, "row2-t9b")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	gen := testRow2Gen(e)
	wire := testRow2Wire(e)
	var held *mqttSendAction
	restore := setMQTTTestPubRelWriteBlock(func() {
		held = testRow2Holder(e, seed.pi)
		panic("test: an abnormal unwind after write entry")
	})
	func() {
		defer func() { _ = recover() }()
		testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
	}()
	restore()
	if held == nil {
		t.Fatal("the delivery never entered its write, so nothing here is tested")
	}
	if len(wire.written) != 0 {
		t.Fatalf("an unwound write reached the wire: %x", wire.written)
	}
	if st, ok := testRow2CellState(held.exec, gen, seed.lin, held.slot.acq); !ok || st != mqttFrameAbandoned {
		t.Fatalf("the unwound acquisition is %v (present=%v), want ABANDONED", st, ok)
	}
	testRow2Eventually(t, testRow2Timeout, "the deferred drain to clear the token", func() bool {
		c, _, _ := e.asm.loadInflightRecord(seed.subject)
		return c != nil && c.ActionToken == _EMPTY_
	})
}

// testRow2PeerRefused asserts a second broker, holding a VALID entitlement of its own, cannot
// acquire the exchange's PUBREL while A's lease stands — and that the refusal is the token's, not
// a tenure's.
func testRow2PeerRefused(t *testing.T, e *testInflightEnv, seed testRealPubRelSeed, tokenA string) {
	t.Helper()
	asmB, sessB, cB := e.peer(t, "node2", e.sess.id)
	testRow2AcceptedClient(t, e, asmB, sessB, cB)
	cur, smsg, err := asmB.loadInflightRecord(seed.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("B reading the slot: %+v (%v)", cur, err)
	}
	if cur.ActionToken != tokenA {
		t.Fatalf("A's lease is not on the register while its frame is in flight: %+v", cur)
	}
	ref := mqttSlotRefFromRecord(seed.subject, smsg.Sequence, cur)
	if actB, ended := sessB.mqttAcquirePubRelAction(cB, *ref, cur.Lin, 0); actB != nil || ended {
		t.Fatalf("B, entitled, acquired=%v ended=%v while A's lease stood", actB != nil, ended)
	}
	ent, _ := cB.outboundEntitlement()
	_, _, berr := asmB.jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: sessB.id, Epoch: asmB.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-peer",
		Gen: ent.Gen, Tenure: ent.Tenure, Nonce: cur.Lin, Acq: cur.Acq + 1, Kind: mqttActionPubRel,
		Binding: cB.outBind, Register: cur, RegisterSeq: smsg.Sequence, Token: mqttNewInflightNonce(),
	})
	if berr == nil || !bytes.Contains([]byte(berr.Error()), []byte("already carries a live token")) {
		t.Fatalf("B's acquisition was not refused on A's live token: %v", berr)
	}
}

// T12. The durable token outlives the write: while A's ordinary PUBREL frame is in flight — held
// after entry, and again with half the frame on the socket — the token stands and an entitled
// peer cannot acquire; once the write returns, the callback's deferred drain clears it. Kills
// M14, M14c.
func TestF105Row2TheLeaseOutlivesTheWrite(t *testing.T) {
	t.Run("held after entry", func(t *testing.T) {
		e := testNewInflightEnv(t, "row2-t12-entry")
		testReadyForRealPubRelDelivery(t, e)
		seed := testSeedRealPubRelReady(t, e)
		wire := testRow2Wire(e)
		reached, release, done := make(chan string, 1), make(chan struct{}), make(chan struct{})
		restore := setMQTTTestPubRelWriteBlock(func() {
			h := testRow2Holder(e, seed.pi)
			tok := _EMPTY_
			if h != nil {
				tok = h.token
			}
			reached <- tok
			<-release
		})
		defer restore()
		go func() {
			defer close(done)
			testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
		}()
		var tokenA string
		select {
		case tokenA = <-reached:
		case <-done:
			t.Fatal("A's delivery returned without reaching its write, so nothing here is tested")
		}
		testRow2PeerRefused(t, e, seed, tokenA)
		close(release)
		<-done
		if !bytes.Equal(wire.written, testRow2PubRelFrame(seed.pi)) {
			t.Fatalf("A's released write did not complete: %x", wire.written)
		}
		testRow2Eventually(t, testRow2Timeout, "the deferred drain to clear the token", func() bool {
			c, _, _ := e.asm.loadInflightRecord(seed.subject)
			return c != nil && c.ActionToken == _EMPTY_
		})
	})
	t.Run("half the frame on the socket", func(t *testing.T) {
		e := testNewInflightEnv(t, "row2-t12-partial")
		testReadyForRealPubRelDelivery(t, e)
		seed := testSeedRealPubRelReady(t, e)
		conn := newTestRow2StallConn()
		e.c.mu.Lock()
		e.c.nc, e.c.rawConn = conn, conn
		e.c.mu.Unlock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			testDeliverPubRelAs(t, e, seed.pi, seed.reply(1), seed.nonce)
		}()
		select {
		case <-conn.firstDone:
		case <-done:
			t.Fatal("A's delivery returned without starting its frame, so nothing here is tested")
		}
		if got := conn.bytes(); len(got) != 2 {
			t.Fatalf("the fixture must hold A mid-frame, with 2 bytes out: %x", got)
		}
		h := testRow2Holder(e, seed.pi)
		if h == nil {
			t.Fatal("A's holder is gone while its frame is mid-write")
		}
		testRow2PeerRefused(t, e, seed, h.token)
		close(conn.release)
		<-done
		if !bytes.Equal(conn.bytes(), testRow2PubRelFrame(seed.pi)) {
			t.Fatalf("A's frame did not complete: %x", conn.bytes())
		}
		testRow2Eventually(t, testRow2Timeout, "the deferred drain to clear the token", func() bool {
			c, _, _ := e.asm.loadInflightRecord(seed.subject)
			return c != nil && c.ActionToken == _EMPTY_
		})
	})
}
