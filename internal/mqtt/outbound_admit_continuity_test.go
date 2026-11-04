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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// issue #99 WP5 (plan rev 20): admission continuity, WITNESS FIRST. These are the W5 witnesses: each
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
	pi, isNew := e.sess.trackPublish(testRealPubRelSrcDur, reply, mqttOfferOrigin{})
	var nonce string
	if p := e.sess.pendingPublish[pi]; p != nil {
		nonce = p.regNonce
	}
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		return 0, _EMPTY_, false
	}
	return pi, nonce, e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, nonce, _EMPTY_, _EMPTY_, nil)
}

// W5a. A DEFINITE admission failure (the batch never leaves; resolution proves it never landed):
// the ladder abandons, the source is re-offered, and the re-offer is admitted ONCE. The invariant
// holds trivially - nothing of the first attempt landed.
func TestContinuityW5aDefiniteFailureAdmitsOnce(t *testing.T) {
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

// testW5bStanding is W5b's standing population: the first admission LANDED, its response was lost,
// every readback of its register fails and its custody resolution stays unresolved, so the ladder
// abandoned the entry and its source mapping and the continuity record withheld the re-offer.
type testW5bStanding struct {
	e            *testInflightEnv
	pi1          uint16
	nonce1       string
	firstSubject string
	// readFault fails the first allocation's register reads; resolveFault keeps its custody
	// resolution unresolved, which holds the continuity record in its settle step.
	readFault, resolveFault *atomic.Bool
}

// testW5bStandingPopulation drives W5b to its standing population and asserts it: register at
// publish with no action token, the unsent marker naming the first allocation, its exchange at
// Publish, the continuity record standing and the re-offer withheld with exactly one un-ended
// exchange for the source delivery.
func testW5bStandingPopulation(t *testing.T, clientID string) *testW5bStanding {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	e.admissionPrerequisites(t, testRealPubRelSrcDur, 1)
	e.acceptUnderServingAuthority(t)
	w := &testW5bStanding{e: e, readFault: &atomic.Bool{}, resolveFault: &atomic.Bool{}}
	w.readFault.Store(true)
	w.resolveFault.Store(true)
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
		if w.resolveFault.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if w.readFault.Load() && subject == firstSubject.Load().(string) {
			return nil, nil, errors.New("test: the register read failed")
		}
		return e.asm.jsa.loadInflightRecord(subject)
	}
	e.asm.mu.Unlock()
	reply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1)
	e.sess.mu.Lock()
	w.pi1, _ = e.sess.trackPublish(testRealPubRelSrcDur, reply, mqttOfferOrigin{})
	w.nonce1 = e.sess.pendingPublish[w.pi1].regNonce
	w.firstSubject = mqttInflightSubject(e.sess.idHash, e.sess.gen, w.pi1)
	firstSubject.Store(w.firstSubject)
	e.sess.mu.Unlock()
	admitted1 := e.sess.admitInflightPublish(e.c, w.pi1, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, w.nonce1, _EMPTY_, _EMPTY_, nil)
	e.sess.mu.Lock()
	_, entry1 := e.sess.pendingPublish[w.pi1]
	e.sess.mu.Unlock()
	if admitted1 || entry1 {
		t.Fatalf("fixture: the ladder must abandon the landed admission (admitted %v, entry stands %v)", admitted1, entry1)
	}
	// WHAT THE LADDER LEFT, read directly from the store: the population the record decides.
	rec1, _, err := e.asm.jsa.loadInflightRecord(w.firstSubject)
	if err != nil || rec1 == nil || rec1.State != mqttInflightPublish || rec1.ActionToken != _EMPTY_ || rec1.Nonce != w.nonce1 {
		t.Fatalf("fixture: the first allocation's register must stand at publish with no token, got %+v (%v)", rec1, err)
	}
	if ml := e.asm.jsa.loadOccupantMarkerLoad(w.firstSubject); !mqttMarkerNamesAllocation(ml, w.nonce1) {
		t.Fatalf("fixture: the unsent marker must name the first allocation, got %+v", ml)
	}
	if x, _, xerr := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, rec1.Lin); xerr != nil || x == nil ||
		x.Phase != mqttExchangePublish {
		t.Fatalf("fixture: the first admission's exchange must stand at Publish, got %+v (%v)", x, xerr)
	}
	// WITNESS (pre-WP5): this re-offer was admitted under identifier 2 and committed state held
	// TWO un-ended exchanges for the one source delivery. With the continuity record it is WITHHELD.
	pi2, nonce2, ok2 := testContinuityOffer(t, e, 2)
	if pi2 != 0 || ok2 {
		t.Fatalf("the re-offer must be withheld while the first admission is undecided, got %d (nonce %q, admitted %v)", pi2, nonce2, ok2)
	}
	if n, lins := testContinuityUnended(t, e, e.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq); n != 1 {
		t.Fatalf("exactly the first admission's exchange is un-ended for the source, got %d %v", n, lins)
	}
	e.sess.mu.Lock()
	standing := e.sess.admissionContinuityLocked(testRealPubRelSrcDur, testRealPubRelSrcSeq) != nil
	e.sess.mu.Unlock()
	if !standing {
		t.Fatal("fixture: the continuity record must stand over the withheld re-offer")
	}
	return w
}

// testMakeSourceDeliveryLive makes the source delivery (SRC, seq) one that can still be re-offered,
// through the real classification route: SRC's consumer stands, nothing acknowledges what it
// delivers (nobody subscribes to its deliver subject), and the stream holds a message at seq.
func testMakeSourceDeliveryLive(t *testing.T, e *testInflightEnv, seq uint64) {
	t.Helper()
	for {
		m, err := e.s.nrc.Request(mqttStreamSubjectPrefix+"f092.source", []byte("live"), 5*time.Second)
		if err != nil {
			t.Fatalf("publishing to the source: %v", err)
		}
		var ack struct {
			Seq uint64 `json:"seq"`
		}
		if err := json.Unmarshal(m.Data, &ack); err != nil || ack.Seq == 0 {
			t.Fatalf("publishing to the source: %q (%v)", m.Data, err)
		}
		if ack.Seq == seq {
			break
		}
		if ack.Seq > seq {
			t.Fatalf("fixture: the stream is already past sequence %d (%d)", seq, ack.Seq)
		}
	}
	if _, err := e.asm.jsa.loadMsg(mqttStreamName, seq); err != nil {
		t.Fatalf("fixture: the source message %d must stand: %v", seq, err)
	}
	ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, testRealPubRelSrcDur)
	if err != nil || ci == nil || ci.AckFloor.Stream >= seq {
		t.Fatalf("fixture: the source consumer must stand below %d, got %+v (%v)", seq, ci, err)
	}
}

// W5b. The first admission LANDS, its response is lost, every readback of its register fails and
// its custody resolution stays unresolved, so the ladder exhausts and abandons the entry and its
// source mapping; the source's re-offer is withheld by the continuity record (interleaving 3).
func TestContinuityW5bLandedThenUnreadable(t *testing.T) {
	t.Parallel()
	t.Run("decided-by-the-record", func(t *testing.T) {
		t.Parallel()
		// The first admission is decided once its store answers again: the continuity record settles
		// the attempt, withdraws the never-sent obligation on its standing marker, and re-offers the
		// source.
		w := testW5bStandingPopulation(t, "f097-w5b")
		e := w.e
		w.readFault.Store(false)
		w.resolveFault.Store(false)
		testCustodyKick(e.sess)
		testCustodyEventually(t, "the continuity record decides the first admission", func() bool {
			e.sess.mu.Lock()
			defer e.sess.mu.Unlock()
			return len(e.sess.admissionContinuity) == 0
		})
		rec1, _, _ := e.asm.jsa.loadInflightRecord(w.firstSubject)
		if x, _, _ := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, w.nonce1); x == nil ||
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
	})
	t.Run("same-object-resume-serves-the-restored-entry", func(t *testing.T) {
		t.Parallel()
		// A SAME-OBJECT RESUME that restores the first allocation's own entry supersedes the record:
		// the restored entry serves the next offer (identifier pi1, no second lineage). Recovery
		// restores it only over a source that can still re-offer it. The custody resolution stays
		// unresolved throughout, so the record can never classify or withdraw; its only exit is
		// supersession, by the dispatcher or by the offer, whichever runs first.
		w := testW5bStandingPopulation(t, "f097-w5b-res")
		e := w.e
		testMakeSourceDeliveryLive(t, e, testRealPubRelSrcSeq)
		w.readFault.Store(false)
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("same-object recovery: %v", err)
		}
		e.sess.mu.Lock()
		restored := e.sess.pendingPublish[w.pi1]
		e.sess.mu.Unlock()
		if restored == nil || restored.regNonce != w.nonce1 || !restored.acts() {
			t.Fatalf("recovery over a live source must restore the first allocation's acting entry, got %+v", restored)
		}
		e.sess.mu.Lock()
		piR, isNew := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 3), mqttOfferOrigin{})
		left := e.sess.admissionContinuityLocked(testRealPubRelSrcDur, testRealPubRelSrcSeq)
		e.sess.mu.Unlock()
		if piR != w.pi1 || isNew || left != nil {
			t.Fatalf("the restored entry must serve the offer (got identifier %d new %v, want %d) and the record be retired (%v)",
				piR, isNew, w.pi1, left != nil)
		}
		if n, lins := testContinuityUnended(t, e, e.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq); n != 1 {
			t.Fatalf("exactly one un-ended exchange for the source, got %d %v", n, lins)
		}
		w.resolveFault.Store(false)
		testDriverP2(t, e.sess, e.s)
	})
}

// ACROSS A COLD PEER RESUME the continuity record is A's memory and dies with A. B's recovery
// classifies the landed never-sent obligation by its source, as it classifies any proven-unsent
// register: over a gone source it withdraws it and an offer is a fresh allocation; over a live one
// it restores the unsent acting entry and the offer continues it under the same identifier.
func TestContinuityColdPeerResumeFollowsTheSource(t *testing.T) {
	t.Parallel()
	coldPeer := func(t *testing.T, w *testW5bStanding) *testInflightEnv {
		t.Helper()
		if err := testSessSave(t, w.e.sess); err != nil {
			t.Fatalf("persist the session record: %v", err)
		}
		return testUnitCColdPeer(t, w.e)
	}
	t.Run("source-gone", func(t *testing.T) {
		t.Parallel()
		// SRC's consumer stands with no message at the source sequence: the source reads Complete.
		w := testW5bStandingPopulation(t, "w5b-cold-gone")
		b := coldPeer(t, w)
		if _, err := b.sess.recoverInflightRegisters(b.c); err != nil {
			t.Fatalf("B's recovery: %v", err)
		}
		if x, _, err := b.asm.jsa.loadOutboundExchange(b.sess.id, b.asm.sessStreamEpoch, b.sess.gen, w.nonce1); err != nil || x == nil ||
			x.Phase != mqttExchangeWithdrawnUnsent {
			t.Fatalf("B's recovery must withdraw the never-sent obligation over a gone source, got %+v (%v)", x, err)
		}
		if n, lins := testContinuityUnended(t, b, b.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq); n != 0 {
			t.Fatalf("no un-ended exchange may remain for the source, got %d %v", n, lins)
		}
		b.sess.mu.Lock()
		for pi, p := range b.sess.pendingPublish {
			if p != nil && p.regNonce == w.nonce1 {
				b.sess.mu.Unlock()
				t.Fatalf("B restored identifier %d for the withdrawn allocation: %+v", pi, p)
			}
		}
		_, isNew := b.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 3), mqttOfferOrigin{})
		b.sess.mu.Unlock()
		if !isNew {
			t.Fatal("an offer after the withdrawal must be a fresh allocation")
		}
	})
	t.Run("source-live", func(t *testing.T) {
		t.Parallel()
		w := testW5bStandingPopulation(t, "w5b-cold-live")
		b := coldPeer(t, w)
		testMakeSourceDeliveryLive(t, b, testRealPubRelSrcSeq)
		if _, err := b.sess.recoverInflightRegisters(b.c); err != nil {
			t.Fatalf("B's recovery: %v", err)
		}
		b.sess.mu.Lock()
		restored := b.sess.pendingPublish[w.pi1]
		b.sess.mu.Unlock()
		if restored == nil || restored.regNonce != w.nonce1 || !restored.acts() || restored.transmitted {
			t.Fatalf("B's recovery must restore the never-sent obligation as an unsent acting entry, got %+v", restored)
		}
		b.sess.mu.Lock()
		piB, isNew := b.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 3), mqttOfferOrigin{})
		b.sess.mu.Unlock()
		if piB != w.pi1 || isNew {
			t.Fatalf("B's offer must continue the restored obligation (got identifier %d new %v, want %d)", piB, isNew, w.pi1)
		}
		n, lins := testContinuityUnended(t, b, b.sess.gen, testRealPubRelSrcDur, testRealPubRelSrcSeq)
		if n != 1 || lins[0] != w.nonce1 {
			t.Fatalf("exactly the restored obligation is un-ended for the source, got %d %v", n, lins)
		}
	})
}
