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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// F-097 WP5 (plan rev 20): admission continuity, WITNESS FIRST. These are the W5 witnesses: each
// drives a controlled in-process schedule on the real store and checks the committed-state
// invariant "at most one un-ended exchange per source delivery (dur, seq)".

// testContinuityUnended counts the generation's registers naming (dur, seq) whose exchange is
// not positively ended (an unreadable exchange counts: nothing proves it ended).
func testContinuityUnended(t *testing.T, e *testInflightEnv, gen, dur string, seq uint64) (int, []string) {
	t.Helper()
	filter := mqttInflightWalkFilter(e.sess.idHash, gen)
	var lins []string
	n := 0
	for s := uint64(1); ; {
		smsg, err := e.asm.jsa.loadNextMsgFromSeq(mqttInflightStreamName, filter, s)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				t.Fatalf("walk registers: %v", err)
			}
			return n, lins
		}
		s = smsg.Sequence + 1
		rec, derr := mqttDecodeInflightRecord(smsg.Data)
		if derr != nil || rec.Dur != dur || rec.Seq != seq || rec.Lin == _EMPTY_ {
			continue
		}
		x, _, xerr := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, gen, rec.Lin)
		if xerr == nil && x != nil && mqttExchangePhaseEnded(x.Phase) {
			continue
		}
		n++
		lins = append(lins, rec.Lin)
	}
}

// testContinuityOffer tracks a (re-)offer of the source delivery the way the delivery callback
// does, and runs its admission. It returns the identifier (0: withheld or refused) and whether the
// admission was accepted.
func testContinuityOffer(t *testing.T, e *testInflightEnv, deliveries int) (uint16, string, bool) {
	t.Helper()
	reply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, deliveries)
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(testRealPubRelSrcDur, reply)
	var nonce string
	if p := e.sess.pendingPublish[pi]; p != nil {
		nonce = p.regNonce
	}
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		return 0, _EMPTY_, false
	}
	return pi, nonce, e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, nonce, _EMPTY_, _EMPTY_)
}

// W5a. A DEFINITE admission failure (the batch never leaves; resolution proves it never landed):
// the ladder abandons, the source is re-offered, and the re-offer is admitted ONCE. The invariant
// holds trivially - nothing of the first attempt landed.
func TestF097ContinuityW5aDefiniteFailureAdmitsOnce(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f097-w5a")
	testReadyForRealPubRelDelivery(t, e)
	e.admissionPrerequisites(t, testRealPubRelSrcDur, 1)
	e.acceptUnderServingAuthority(t)
	var fail atomic.Bool
	fail.Store(true)
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange && fail.Load() {
			return 0, errors.New("test: the admission's batch never left")
		}
		return send()
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
	if _, _, ok := testContinuityOffer(t, e, 1); ok {
		t.Fatal("fixture: the first admission must fail definitely")
	}
	fail.Store(false)
	var writes []string
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(s *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		writes = append(writes, fmt.Sprintf("registerOnly=%v err=%v", registerOnly, err))
		return out, err
	}
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	held := len(e.sess.custody.heldAdmissions)
	e.sess.mu.Unlock()
	t.Logf("before the re-offer: %d held admission(s)", held)
	pi2, _, ok := testContinuityOffer(t, e, 2)
	t.Logf("re-offer: identifier %d admitted=%v writes=%v", pi2, ok, writes)
	if !ok || pi2 == 0 {
		t.Fatal("the re-offer must be admitted after a definite failure")
	}
	if n, lins := testContinuityUnended(t, e, e.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq); n != 1 {
		t.Fatalf("committed state holds %d un-ended exchanges for the source delivery, want 1: %v", n, lins)
	}
	testDriverP2(t, e.sess, e.s)
}

// W5b. The first admission LANDS, its response is lost, every readback of its register fails and
// its custody resolution stays unresolved, so the ladder exhausts and abandons the entry and its
// source mapping; the source is re-offered. RECORDS whether the re-offer admits a SECOND lineage
// for the same source delivery while the first stays at Publish (interleaving 3).
func TestF097ContinuityW5bLandedThenUnreadable(t *testing.T) {
	t.Parallel()
	t.Run("decided-by-the-record", func(t *testing.T) { t.Parallel(); testF097W5b(t, "f097-w5b", false) })
	t.Run("same-object-resume-serves-the-restored-entry", func(t *testing.T) {
		t.Parallel()
		testF097W5b(t, "f097-w5b-res", true)
	})
}

func testF097W5b(t *testing.T, clientID string, resume bool) {
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	e.admissionPrerequisites(t, testRealPubRelSrcDur, 1)
	e.acceptUnderServingAuthority(t)
	var fault atomic.Bool
	fault.Store(true)
	var first atomic.Bool
	var firstSubject atomic.Value
	firstSubject.Store(_EMPTY_)
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(s *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if err == nil && first.CompareAndSwap(false, true) {
			return out, errors.New("test: the admission's response was lost")
		}
		return out, err
	}
	e.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if fault.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if fault.Load() && subject == firstSubject.Load().(string) {
			return nil, nil, errors.New("test: the register read failed")
		}
		return e.asm.jsa.loadInflightRecord(subject)
	}
	e.asm.mu.Unlock()
	reply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1)
	e.sess.mu.Lock()
	pi1, _ := e.sess.trackPublish(testRealPubRelSrcDur, reply)
	nonce1 := e.sess.pendingPublish[pi1].regNonce
	firstSubject.Store(mqttInflightSubject(e.sess.idHash, e.sess.gen, pi1))
	e.sess.mu.Unlock()
	admitted1 := e.sess.admitInflightPublish(e.c, pi1, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, nonce1, _EMPTY_, _EMPTY_)
	e.sess.mu.Lock()
	_, entry1 := e.sess.pendingPublish[pi1]
	e.sess.mu.Unlock()
	t.Logf("W5b: first admission returned %v; its entry stands: %v", admitted1, entry1)
	if admitted1 || entry1 {
		t.Skip("W5b UNREACHED in this construction: the ladder did not abandon the landed admission")
	}
	// WHAT THE LADDER LEFT, read with the fault still standing for the ledger but directly from
	// the store: the population the continuity item has to decide.
	{
		rec1, _, _ := e.asm.jsa.loadInflightRecord(firstSubject.Load().(string))
		ml := e.asm.jsa.loadOccupantMarkerLoad(firstSubject.Load().(string))
		var phase mqttExchangePhase = 255
		if rec1 != nil {
			if x, _, _ := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, rec1.Lin); x != nil {
				phase = x.Phase
			}
		}
		e.sess.mu.Lock()
		_, admHeld := e.sess.custody.heldAdmissions[nonce1]
		piHeld := e.sess.custody.holdsPI(pi1)
		e.sess.mu.Unlock()
		if rec1 != nil {
			t.Logf("W5b left: register state=%q acq=%d token=%q; exchange phase=%d; marker for nonce1 standing=%v; "+
				"ledger admission held=%v, pi1 held=%v", rec1.State, rec1.Acq, rec1.ActionToken, phase,
				mqttMarkerNamesAllocation(ml, nonce1), admHeld, piHeld)
		} else {
			t.Logf("W5b left: no register readable; ledger admission held=%v, pi1 held=%v", admHeld, piHeld)
		}
	}
	// WITNESS (pre-WP5): this re-offer was admitted under identifier 2 and committed state held
	// TWO un-ended exchanges for the one source delivery. With the continuity record it is WITHHELD.
	pi2, nonce2, ok2 := testContinuityOffer(t, e, 2)
	n, lins := testContinuityUnended(t, e, e.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq)
	t.Logf("W5b: re-offer -> identifier %d (nonce %q) admitted=%v; un-ended exchanges for the source: %d %v",
		pi2, nonce2, ok2, n, lins)
	if n > 1 {
		t.Fatalf("%d un-ended exchanges for ONE source delivery (interleaving 3)", n)
	}
	if pi2 != 0 || ok2 {
		t.Fatalf("the re-offer must be withheld while the first admission is undecided, got %d (admitted %v)", pi2, ok2)
	}
	// A SAME-OBJECT RESUME that restores the first allocation's own entry supersedes the record:
	// the restored entry serves the next offer (identifier pi1, no second lineage).
	if resume {
		e.sess.mu.Lock()
		rec := e.sess.admissionContinuityLocked(testRealPubRelSrcDur, testRealPubRelSrcSeq)
		e.sess.mu.Unlock()
		if rec == nil {
			t.Fatal("fixture: the continuity record must stand before the resume")
		}
		fault.Store(false)
		e.asm.mu.Lock()
		e.asm.testCustodyResolve = nil
		e.asm.mu.Unlock()
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("same-object recovery: %v", err)
		}
		e.sess.mu.Lock()
		restored := e.sess.pendingPublish[pi1]
		e.sess.mu.Unlock()
		if restored == nil || restored.regNonce != nonce1 {
			t.Skipf("fixture: recovery did not restore the first allocation's entry (%+v); nothing supersedes", restored)
		}
		e.sess.mu.Lock()
		piR, _ := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 3))
		left := e.sess.admissionContinuityLocked(testRealPubRelSrcDur, testRealPubRelSrcSeq)
		e.sess.mu.Unlock()
		if piR != pi1 || left != nil {
			t.Fatalf("the restored entry must serve the offer (got identifier %d, want %d) and retire the record (%v)",
				piR, pi1, left != nil)
		}
		if n, lins := testContinuityUnended(t, e, e.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq); n != 1 {
			t.Fatalf("exactly one un-ended exchange for the source, got %d %v", n, lins)
		}
		testDriverP2(t, e.sess, e.s)
		return
	}
	// ACROSS A COLD PEER RESUME (the continuity record is A's memory and dies with A): B's
	// recovery restores the landed obligation by its ordinary rules, so B's offer of the same
	// delivery continues it rather than admitting a second lineage.
	t.Run("cold-peer-resume", func(t *testing.T) {
		if err := testSessSave(t, e.sess); err != nil {
			t.Fatalf("persist the session record: %v", err)
		}
		_, peer, pc := e.peer(t, "node2", e.sess.id)
		if _, err := peer.recoverInflightRegisters(pc); err != nil {
			t.Fatalf("B's recovery: %v", err)
		}
		peer.mu.Lock()
		piB, newB := peer.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 3))
		peer.mu.Unlock()
		n, lins := testContinuityUnended(t, e, e.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq)
		t.Logf("B: offer -> identifier %d (new %v); un-ended exchanges for the source: %d %v", piB, newB, n, lins)
		if newB && piB != 0 && piB != pi1 {
			t.Fatalf("B allocated a fresh identifier %d for a delivery whose obligation %d it restored", piB, pi1)
		}
		if n > 1 {
			t.Fatalf("%d un-ended exchanges for one source delivery after the peer resume", n)
		}
	})
	// The first admission is decided once its store answers again: the continuity record settles the
	// attempt, withdraws the never-sent obligation on its standing marker, and re-offers the source.
	fault.Store(false)
	testCustodyKick(e.sess)
	testCustodyEventually(t, "the continuity record decides the first admission", func() bool {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		return len(e.sess.admissionContinuity) == 0
	})
	rec1, _, _ := e.asm.jsa.loadInflightRecord(firstSubject.Load().(string))
	if x, _, _ := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, nonce1); x == nil ||
		x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the never-sent first admission must be withdrawn, got %+v (register %+v)", x, rec1)
	}
	pi3, _, ok3 := testContinuityOffer(t, e, 3)
	if !ok3 || pi3 == 0 {
		t.Fatal("the re-offer must be admitted once the first admission is decided")
	}
	if n, lins := testContinuityUnended(t, e, e.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq); n != 1 {
		t.Fatalf("committed state must hold exactly one un-ended exchange for the source, got %d %v", n, lins)
	}
	testDriverP2(t, e.sess, e.s)
}
