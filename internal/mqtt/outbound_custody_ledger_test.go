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
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"sync/atomic"
	"testing"
	"time"
)

// The custody ledger owns every ordinary exchange's Receive Maximum charge and identifier
// (F-105 cutover, row 5; plan-row5.md rev 6 §6). Unless stated, each test runs at Receive
// Maximum 1 with a large MaxAckPending, so the local scheduling budget cannot mask an RM
// defect, and "B sent" is read as capacityAvailableLocked() > 0.

const testCustodyBound = 5 * time.Second

func testCustodyRM1(t *testing.T, sess *mqttSession) {
	t.Helper()
	sess.mu.Lock()
	sess.rmax = 1
	sess.maxp = 100
	sess.mu.Unlock()
}

func testCustodyCapacity(sess *mqttSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.capacityAvailableLocked()
}

// testCustodyEventually waits for cond within the bound.
func testCustodyEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testCustodyBound)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not within %v: %s", testCustodyBound, what)
}

// testCustodyStaysFor asserts cond holds throughout a short interval.
func testCustodyStaysFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("stopped holding: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testCustodyHasExchange(sess *mqttSession, lin string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.custody.hasExchange(sess.gen, lin)
}

func testCustodyHasAdmission(sess *mqttSession, nonce string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	_, ok := sess.custody.heldAdmissions[nonce]
	return ok
}

// testCustodyMints reports whether bumpPI, offered exactly pi first, hands it out.
func testCustodyMints(sess *mqttSession, pi uint16) uint16 {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	saved := sess.last_pi
	sess.last_pi = pi - 1
	got := sess.bumpPI()
	if got != 0 {
		// Undo the side effect on the allocator cursor only; nothing was tracked.
		sess.last_pi = saved
	}
	return got
}

func testCustodyKick(sess *mqttSession) {
	sess.mu.Lock()
	sess.kickCustodyPassLocked()
	sess.mu.Unlock()
}

// T1. A metadata route removes a transmitted exchange's scheduling entry: the charge stays with
// the exchange, and comes back only once the exchange durably ends and is observed.
//
// Mutation M1: capacity counts the maps and ignores the ledger -> B admitted at once.
func TestF105CustodyMetadataRemovalReleasesNothing(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-metadata", 1)
	sess := f.e.sess
	testCustodyRM1(t, sess)
	if c := testCustodyCapacity(sess); c != 0 {
		t.Fatalf("fixture: the admitted exchange must hold the only slot, capacity %d", c)
	}
	// Park A as a settled cancelled reservation and retire its verdict — the real metadata
	// release edge.
	sess.mu.Lock()
	ack := sess.pendingPublish[f.pi]
	ack.regCancelled = true
	if sess.settledVoidReservations == nil {
		sess.settledVoidReservations = make(map[string]uint16)
	}
	sess.settledVoidReservations[f.nonce] = f.pi
	if sess.cancelledRegNonces == nil {
		sess.cancelledRegNonces = make(map[string]uint64)
	}
	sess.cancelledRegNonces[f.nonce] = 1
	sess.mu.Unlock()
	sess.forgetCancelledReg(f.nonce)
	sess.mu.Lock()
	_, still := sess.pendingPublish[f.pi]
	sess.mu.Unlock()
	if still {
		t.Fatal("fixture: the metadata route did not remove the scheduling entry, so nothing is tested")
	}
	testCustodyStaysFor(t, "B withheld while A's exchange is outstanding", 200*time.Millisecond, func() bool {
		return testCustodyCapacity(sess) == 0
	})
	// A durable ending that needs no scheduling entry: A was never transmitted, so it is
	// withdrawn. Its charge goes only when the observer reads WithdrawnUnsent.
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, nil); r != mqttWithdrawLanded {
		t.Fatalf("withdrawal: %v", r)
	}
	testCustodyEventually(t, "B admitted after the ending is observed", func() bool {
		return testCustodyCapacity(sess) == 1
	})
}

// T4a. A lost admission response whose first resolution is faulted: the abort loses to the
// committed batch and the re-resolution says Landed, so the exchange is charged.
//
// Mutation M4a: Unresolved treated as NeverLanded without the abort -> B admitted while A stands.
func TestF105CustodyLostAdmissionResponseStaysCharged(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-lost-response")
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	var faulted atomic.Bool
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if err == nil && faulted.CompareAndSwap(false, true) {
			return out, errors.New("test: the admission's response was lost")
		}
		return out, err
	}
	var resolvedOnce atomic.Bool
	e.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if step == mqttCustodyStepResolve && resolvedOnce.CompareAndSwap(false, true) {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	e.asm.mu.Unlock()
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 1)
	if !faulted.Load() || !resolvedOnce.Load() {
		t.Fatal("fixture: the lost response and the faulted first resolution never happened")
	}
	lin := e.linOf(t, pi)
	if !testCustodyHasExchange(e.sess, lin) {
		t.Fatal("the landed exchange is not charged")
	}
	if c := testCustodyCapacity(e.sess); c != 0 {
		t.Fatalf("B admitted while A stands: capacity %d", c)
	}
}

// T4b. A batch that composed (so it has a handle) and was refused before it committed, on every
// attempt: the abort wins, every attempt is NeverLanded, and when the ladder ends the
// reservation is gone.
func TestF105CustodyRefusedAdmissionReleasesAtLadderEnd(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-refused")
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange {
			return 0, errors.New("test: a concurrent authority write refused the batch")
		}
		return send()
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
	e.installServingAuthority(t)
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1))
	e.sess.mu.Unlock()
	nonce := e.nonceOf(pi)
	e.acceptUnderServingAuthority(t)
	if e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, nonce, _EMPTY_, _EMPTY_) {
		t.Fatal("fixture: an admission whose every batch was refused was admitted")
	}
	if testCustodyHasAdmission(e.sess, nonce) {
		t.Fatal("the reservation of a ladder whose every attempt never landed survived its end")
	}
	if c := testCustodyCapacity(e.sess); c != 1 {
		t.Fatalf("B withheld after nothing was admitted: capacity %d", c)
	}
}

// T4c. An earlier attempt's late NeverLanded settlement never releases the exchange a later
// attempt created under the same string.
//
// Mutation M4b: a NeverLanded settlement deletes the exchange entry of the attempt's lineage ->
// B admitted.
func TestF105CustodyLateSettlementKeepsTheLandedExchange(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-late-settlement")
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	var first atomic.Bool
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange && first.CompareAndSwap(false, true) {
			return 0, errors.New("test: the first attempt's batch never left")
		}
		return send()
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
	var hold atomic.Bool
	hold.Store(true)
	e.asm.mu.Lock()
	e.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if hold.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	e.asm.mu.Unlock()
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 1)
	nonce := e.nonceOf(pi)
	lin := e.linOf(t, pi)
	if lin != nonce {
		t.Fatalf("fixture: a create's lineage is its nonce, got %q vs %q", lin, nonce)
	}
	if !testCustodyHasAdmission(e.sess, nonce) || !testCustodyHasExchange(e.sess, lin) {
		t.Fatal("fixture: the admission and the landed exchange must coexist under one string")
	}
	hold.Store(false)
	testCustodyKick(e.sess)
	testCustodyEventually(t, "the earlier attempt settled", func() bool {
		return !testCustodyHasAdmission(e.sess, nonce)
	})
	if !testCustodyHasExchange(e.sess, lin) {
		t.Fatal("the late NeverLanded settlement released the exchange a later attempt created")
	}
	if c := testCustodyCapacity(e.sess); c != 0 {
		t.Fatalf("B admitted while A stands: capacity %d", c)
	}
}

// T4d. A committed batch whose settlement stays unresolved through the abort: the admission
// reservation holds; once the fault clears the reconciler resolves it Landed, the exchange is
// charged and the admission goes.
//
// Mutation M4c: the reconciler skips admission targets -> the admission remains past the bound.
func TestF105CustodyReconcilerSettlesAnUnresolvedAdmission(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-reconciler")
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	var lost atomic.Bool
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if err == nil && lost.CompareAndSwap(false, true) {
			return out, errors.New("test: the admission's response was lost")
		}
		return out, err
	}
	var fault atomic.Bool
	fault.Store(true)
	e.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if fault.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	e.asm.mu.Unlock()
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 1)
	nonce := e.nonceOf(pi)
	lin := e.linOf(t, pi)
	if !testCustodyHasAdmission(e.sess, nonce) {
		t.Fatal("an admission with an unresolved attempt lost its reservation")
	}
	if testCustodyHasExchange(e.sess, lin) {
		t.Fatal("fixture: nothing has settled the attempt Landed yet")
	}
	fault.Store(false)
	testCustodyKick(e.sess)
	testCustodyEventually(t, "the reconciler settled the committed attempt", func() bool {
		return !testCustodyHasAdmission(e.sess, nonce) && testCustodyHasExchange(e.sess, lin)
	})
	if c := testCustodyCapacity(e.sess); c != 0 {
		t.Fatalf("the live exchange must still hold the slot: capacity %d", c)
	}
}

// T23. After a NeverLanded attempt the ladder retries without passing the delivery gate: the
// reservation holds across the retry, and exactly one exchange is charged when it lands.
//
// Mutation M23: the removal rule ignores ladderDone -> B admitted while the retry is pending.
func TestF105CustodyReservationSpansTheLadder(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-ladder")
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	var first atomic.Bool
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange && first.CompareAndSwap(false, true) {
			return 0, errors.New("test: the first attempt's batch never left")
		}
		return send()
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
	paused := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if calls.Add(1) == 2 {
			close(paused)
			<-release
		}
		return write()
	}
	e.asm.mu.Unlock()
	done := make(chan uint16, 1)
	go func() { done <- e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 1) }()
	select {
	case <-paused:
	case <-time.After(testCustodyBound):
		t.Fatal("the ladder never reached its second attempt")
	}
	if c := testCustodyCapacity(e.sess); c != 0 {
		close(release)
		t.Fatalf("B admitted while the ladder may still land a retry: capacity %d", c)
	}
	close(release)
	pi := <-done
	e.sess.mu.Lock()
	n := len(e.sess.custody.heldExchanges)
	adm := len(e.sess.custody.heldAdmissions)
	e.sess.mu.Unlock()
	if n != 1 || adm != 0 {
		t.Fatalf("after the retry landed: %d exchanges and %d admissions charged, want 1 and 0 (pi %d)", n, adm, pi)
	}
}

// T24. Refusals before composition send nothing, and the reservation goes when the ladder ends.
// And an allocation abandoned between tracking and admission is never reserved or submitted.
//
// Mutation M24: finishAdmission not run on a refusal return -> B withheld past the bound.
// Mutation M24b: the conversion skips its nonce check -> the ladder is reached / a reservation
// exists.
func TestF105CustodyRefusalWithoutSubmissionReleases(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-no-handle")
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(*mqttSession, bool, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		return mqttOpOutcome{}, errMQTTReplacementDepsNotReady
	}
	e.asm.mu.Unlock()
	e.installServingAuthority(t)
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1))
	e.sess.mu.Unlock()
	nonce := e.nonceOf(pi)
	e.acceptUnderServingAuthority(t)
	if e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, nonce, _EMPTY_, _EMPTY_) {
		t.Fatal("fixture: an admission whose every attempt was refused was admitted")
	}
	testCustodyEventually(t, "the reservation released at the ladder's end", func() bool {
		return !testCustodyHasAdmission(e.sess, nonce) && testCustodyCapacity(e.sess) == 1
	})

	// The tracking-to-conversion window: inspected at the moment a reservation would open, not
	// by what is left afterwards (a deferred finish would erase the evidence).
	var reached, reserved atomic.Bool
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		reached.Store(true)
		return write()
	}
	e.asm.testAdmitReserved = func(*mqttSession, string) { reserved.Store(true) }
	e.asm.mu.Unlock()
	sseq := testRealPubRelSrcSeq + 1
	e.sess.mu.Lock()
	pi2, _ := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, sseq, 1))
	nonce2 := e.sess.pendingPublish[pi2].regNonce
	delete(e.sess.pendingPublish, pi2)
	e.sess.mu.Unlock()
	if e.sess.admitInflightPublish(e.c, pi2, 1, testRealPubRelSrcDur, sseq, nonce2, _EMPTY_, _EMPTY_) {
		t.Fatal("an allocation abandoned before admission was admitted")
	}
	if reached.Load() {
		t.Fatal("an abandoned allocation reached the authority write")
	}
	if reserved.Load() {
		t.Fatal("an abandoned allocation opened a custody reservation")
	}
	if testCustodyHasAdmission(e.sess, nonce2) {
		t.Fatal("an abandoned allocation holds a custody reservation")
	}
}

// T5/T6. The allocator skips an identifier the ledger holds even with no map entry and no send
// hold — and after an exclusion over it actually lifted.
//
// Mutation M5: the allocator ignores heldPIs -> both fail.
func TestF105CustodyAllocatorSkipsALedgerHeldIdentifier(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-allocator", 1)
	sess := f.e.sess
	sess.mu.Lock()
	delete(sess.pendingPublish, f.pi)
	held := sess.sendReservedLocked(f.pi)
	sess.mu.Unlock()
	if held {
		t.Fatal("fixture: a send hold would mask the ledger")
	}
	if got := testCustodyMints(sess, f.pi); got == f.pi || got == 0 {
		t.Fatalf("the allocator handed out %d with the ledger holding %d", got, f.pi)
	}
	// An exclusion over it, lifted for real.
	sess.mu.Lock()
	sess.excludeIdentifierLocked(f.pi, f.subject, f.nonce)
	excluded := sess.sendReservedLocked(f.pi)
	sess.mu.Unlock()
	if !excluded {
		t.Fatal("fixture: the exclusion did not install")
	}
	sess.mu.Lock()
	inst := uint64(0)
	if h := sess.sendsInFlight[f.pi]; h != nil && h.slotOwed != nil {
		inst = h.slotOwed.inst
	}
	sess.liftIdentifierExclusionLocked(f.pi, inst, map[string]bool{f.subject: true})
	lifted := !sess.sendReservedLocked(f.pi)
	sess.mu.Unlock()
	if !lifted {
		t.Fatal("fixture: the exclusion did not lift, so nothing about a lift is tested")
	}
	if got := testCustodyMints(sess, f.pi); got == f.pi || got == 0 {
		t.Fatalf("a lifted exclusion freed %d while the ledger holds it (got %d)", f.pi, got)
	}
}

// T7. A committed attempt whose return is delayed across a generation change installs nothing
// in the fresh ledger.
//
// Mutation M7a: the ledger is not replaced with the generation -> old charges remain.
// Mutation M7b: forGen made to accept any generation -> the late landing is charged.
func TestF105CustodyLateCallbackOfAnOldGeneration(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-old-gen")
	testReadyForRealPubRelDelivery(t, e)
	// A first admission provisions the source and the authority the delayed one needs.
	e.trackAdmitted(t, testRealPubRelSrcDur, testRealPubRelSrcSeq, 1)
	sseq := testRealPubRelSrcSeq + 1
	testCustodyRM1(t, e.sess)
	e.sess.mu.Lock()
	e.sess.rmax = 2
	e.sess.mu.Unlock()
	committed := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if err == nil && once.CompareAndSwap(false, true) {
			close(committed)
			<-release
		}
		return out, err
	}
	e.asm.mu.Unlock()
	e.sess.mu.Lock()
	pi, _ := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, sseq, 1))
	e.sess.mu.Unlock()
	nonce := e.nonceOf(pi)
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, sseq, nonce, _EMPTY_, _EMPTY_)
	}()
	select {
	case <-committed:
	case <-time.After(testCustodyBound):
		t.Fatal("the attempt never committed")
	}
	// The generation changes while the committed attempt has not returned.
	e.sess.mu.Lock()
	e.sess.setIncarnationLocked(mqttNewSessGen(), mqttRetirementPermit{tr: e.sess.leases})
	e.sess.pendingPublish, e.sess.pendingPubRel = nil, nil
	e.sess.mu.Unlock()
	close(release)
	<-done
	e.sess.mu.Lock()
	charges := e.sess.custody.charges()
	e.sess.mu.Unlock()
	if charges != 0 {
		t.Fatalf("a late callback of the old generation charged the fresh ledger: %d", charges)
	}
	if c := testCustodyCapacity(e.sess); c != 2 {
		t.Fatalf("B withheld by a retired generation: capacity %d", c)
	}
}

// T7, second leg. A reconstruction census of an old generation that completes after the
// generation changed merges nothing into the fresh ledger: its exchanges are the retired
// generation's.
//
// Mutation M7b: forGen made to accept any generation -> the late census charges the fresh ledger.
func TestF105CustodyLateCensusOfAnOldGeneration(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-old-census", 1)
	sess := f.e.sess
	started := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyCensus = func() error {
		if once.CompareAndSwap(false, true) {
			close(started)
			<-release
		}
		return nil
	}
	f.e.asm.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		sess.reconstructCustody(f.gen)
	}()
	select {
	case <-started:
	case <-time.After(testCustodyBound):
		t.Fatal("the census never started")
	}
	sess.mu.Lock()
	sess.setIncarnationLocked(mqttNewSessGen(), mqttRetirementPermit{tr: sess.leases})
	sess.pendingPublish, sess.pendingPubRel = nil, nil
	sess.mu.Unlock()
	close(release)
	<-done
	sess.mu.Lock()
	charges := sess.custody.charges()
	sess.mu.Unlock()
	if charges != 0 {
		t.Fatalf("a late census of the retired generation charged the fresh ledger: %d", charges)
	}
}

// T8 and T17. A resume rebuilds custody from the EXCHANGES: an exchange whose register the
// gated door destroyed without evidence (consult 32) keeps its charge and identifier, live and
// after a same-generation resume, before any delivery.
//
// Mutation M8/M17a: the resume does not rebuild from the exchanges -> uncharged after resume.
// Mutation M17b: the door's scheduling removal releases the ledger entry -> uncharged live.
func TestF105CustodyDestructionWithoutEvidenceKeepsItsCharge(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-no-evidence", 1)
	sess := f.e.sess
	testCustodyRM1(t, sess)
	ml := f.e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := f.e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatalf("deleting the marker: %v", err)
	}
	cur, seq := f.register(t)
	tomb := *cur
	tomb.Cancelled = true
	nseq, err := f.e.asm.storeInflightCAS(f.subject, seq, &tomb)
	if err != nil {
		t.Fatalf("writing the tombstone: %v", err)
	}
	if !sess.reapObservedRegister(f.e.c, f.subject, nseq, &tomb, mqttSlotDeleteNeverTransmitted) {
		t.Fatal("the door refused a destruction it admits on the tombstone's own proof")
	}
	if x := f.exchange(t); x.Phase != mqttExchangePublish {
		t.Fatalf("fixture: the exchange must survive at Publish, got %d", x.Phase)
	}
	sess.mu.Lock()
	sess.untrackPublish(f.pi)
	sess.mu.Unlock()
	if c := testCustodyCapacity(sess); c != 0 {
		t.Fatalf("live: B admitted while the Publish exchange stands: capacity %d", c)
	}
	if got := testCustodyMints(sess, f.pi); got == f.pi {
		t.Fatalf("live: the identifier %d was handed out", f.pi)
	}

	// A same-generation resume from the record alone.
	restored, _, rerr := f.e.asm.createOrRestoreSession(sess.id, f.e.s.getOpts())
	if rerr != nil {
		t.Fatalf("createOrRestoreSession: %v", rerr)
	}
	if restored.gen != f.gen {
		t.Fatalf("fixture: the resume must keep the generation, %q vs %q", restored.gen, f.gen)
	}
	testCustodyRM1(t, restored)
	if _, err := restored.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if !testCustodyHasExchange(restored, f.lin) {
		t.Fatal("resumed: the outstanding exchange with no register is not charged")
	}
	if c := testCustodyCapacity(restored); c != 0 {
		t.Fatalf("resumed: B admitted while the Publish exchange stands: capacity %d", c)
	}
	if got := testCustodyMints(restored, f.pi); got == f.pi {
		t.Fatalf("resumed: the identifier %d was handed out", f.pi)
	}
}

// T14. A withdrawal that found no marker evidence ends nothing and releases nothing; the hint
// after a committed one is what releases it, with no delivery offered to drive a pass.
//
// Mutation M14: the hint after the withdrawal removed -> held past the bound.
func TestF105CustodyWithdrawalHintReleases(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-withdraw-hint", 1)
	sess := f.e.sess
	testCustodyRM1(t, sess)
	if r := sess.withdrawUnsent(f.gen, f.subject, mqttNewInflightNonce(), f.lin, nil); r == mqttWithdrawLanded {
		t.Fatal("fixture: a withdrawal naming another allocation landed")
	}
	testCustodyStaysFor(t, "a refused withdrawal keeps the charge", 200*time.Millisecond, func() bool {
		return testCustodyHasExchange(sess, f.lin)
	})
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, nil); r != mqttWithdrawLanded {
		t.Fatalf("withdrawal: %v", r)
	}
	testCustodyEventually(t, "the hinted observation released the withdrawn exchange", func() bool {
		return !testCustodyHasExchange(sess, f.lin)
	})
}

// T16. A failed resume census withholds capacity and allocation; a later pass restores both.
//
// Mutation M16: incompleteness ignored by the capacity function -> B admitted.
func TestF105CustodyIncompleteCensusWithholds(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-incomplete")
	testReadyForRealPubRelDelivery(t, e)
	testCustodyRM1(t, e.sess)
	e.sess.mu.Lock()
	e.sess.rmax = 5
	e.sess.mu.Unlock()
	var fail atomic.Bool
	fail.Store(true)
	e.asm.mu.Lock()
	e.asm.testCustodyCensus = func() error {
		if fail.Load() {
			return errors.New("test: the census failed")
		}
		return nil
	}
	e.asm.mu.Unlock()
	e.sess.rebuildCustodyForResume(e.sess.gen)
	if c := testCustodyCapacity(e.sess); c != 0 {
		t.Fatalf("an incomplete census admitted new work: capacity %d", c)
	}
	if got := testCustodyMints(e.sess, 7); got != 0 {
		t.Fatalf("an incomplete census allocated identifier %d", got)
	}
	fail.Store(false)
	testCustodyKick(e.sess)
	testCustodyEventually(t, "a whole census restored capacity", func() bool {
		return testCustodyCapacity(e.sess) == 5
	})
	if got := testCustodyMints(e.sess, 7); got != 7 {
		t.Fatalf("allocation not restored: got %d", got)
	}
}

// T19. An exchange that could not be decoded is uncertain: charged, allocation withheld; once it
// reads, it is converted to an exchange entry and allocation reopens.
//
// Mutation M19: the pass does not re-read uncertain subjects -> withheld past the bound.
func TestF105CustodyUncertainSubjectRecovers(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-uncertain", 1)
	sess := f.e.sess
	testCustodyRM1(t, sess)
	sess.mu.Lock()
	sess.rmax = 5
	sess.mu.Unlock()
	xsubj := mqttExchangeSubject(sess.idHash, f.gen, f.lin)
	var fault atomic.Bool
	fault.Store(true)
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyDecodeFault = func(subject string) bool { return fault.Load() && subject == xsubj }
	f.e.asm.mu.Unlock()
	// A fresh ledger for the same generation, rebuilt as a resume would.
	sess.mu.Lock()
	sess.custody = newMQTTCustodyLedger(f.gen)
	sess.mu.Unlock()
	sess.rebuildCustodyForResume(f.gen)
	sess.mu.Lock()
	_, uncertain := sess.custody.uncertainSubjects[xsubj]
	sess.mu.Unlock()
	if !uncertain {
		t.Fatal("an undecodable exchange is not uncertain")
	}
	if got := testCustodyMints(sess, 9); got != 0 {
		t.Fatalf("allocation proceeded with an exchange's identifier unknown: %d", got)
	}
	fault.Store(false)
	testCustodyKick(sess)
	testCustodyEventually(t, "the uncertain subject re-read", func() bool {
		return testCustodyHasExchange(sess, f.lin)
	})
	if got := testCustodyMints(sess, 9); got != 9 {
		t.Fatalf("allocation not reopened: got %d", got)
	}
	if got := testCustodyMints(sess, f.pi); got == f.pi {
		t.Fatal("the recovered exchange's identifier was handed out")
	}
}

// T21. A census answering stream-not-found is incomplete; a vanished subject stays uncertain
// across a later census that no longer lists it.
//
// Mutation M21a: stream-not-found read as an empty census -> complete.
// Mutation M21b: the census recomputes uncertainty from scratch -> allocation reopens.
func TestF105CustodyCensusShapesStayConservative(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-census-shapes")
	testReadyForRealPubRelDelivery(t, e)
	gen := e.sess.gen
	ghost := mqttExchangeSubject(e.sess.idHash, gen, mqttNewInflightNonce())
	var mode atomic.Int32 // 0: stream missing, 1: lists the ghost, 2: lists nothing
	obs := func(filter string) (map[string]uint64, int, error) {
		switch mode.Load() {
		case 0:
			return nil, 0, ApiErrors[JSStreamNotFoundErr]
		case 1:
			return map[string]uint64{ghost: 1}, 1, nil
		}
		return map[string]uint64{}, 0, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&obs)
	t.Cleanup(func() { e.asm.jsa.exchangeSubjectsObs.Store(nil) })

	e.sess.rebuildCustodyForResume(gen)
	e.sess.mu.Lock()
	incomplete := e.sess.custody.withholdsNew()
	e.sess.mu.Unlock()
	if !incomplete {
		t.Fatal("a census answering stream-not-found was read as complete")
	}
	mode.Store(1)
	e.sess.rebuildCustodyForResume(gen)
	mode.Store(2)
	e.sess.rebuildCustodyForResume(gen)
	e.sess.mu.Lock()
	_, still := e.sess.custody.uncertainSubjects[ghost]
	e.sess.mu.Unlock()
	if !still {
		t.Fatal("a vanished subject was forgotten by a later census")
	}
	if got := testCustodyMints(e.sess, 11); got != 0 {
		t.Fatalf("allocation reopened with an exchange's identifier unknown: %d", got)
	}
}

// T10 and T20. The two budgets, and the resume replay's own allowance, on the computation alone.
//
// M10a: RM omits a PubRecObserved exchange -> (b). M10b: the scheduling budget counts
// pendingPubRel -> (c). M20a: replay budgeted as new work -> (a). M20b: the budget term not
// reduced by the replay -> (b'). M20c: the RM term drops PUBRELs -> (c'). M20d/e: incompleteness
// zeroes the replay / is ignored for deferred -> (d).
func TestF105CustodyCapacityComputation(t *testing.T) {
	t.Parallel()
	gen := mqttNewSessGen()
	mk := func(maxp, rmax uint16) *mqttSession {
		return &mqttSession{gen: gen, maxp: maxp, rmax: rmax, custody: newMQTTCustodyLedger(gen),
			pendingPublish: map[uint16]*mqttPending{}, pendingPubRel: map[uint16]*mqttPending{}}
	}
	// (a) maxp < rmax: the scheduling budget binds.
	s := mk(1, 10)
	s.pendingPublish[1] = &mqttPending{}
	if c := s.capacityAvailableLocked(); c != 0 {
		t.Fatalf("(a) scheduling budget: capacity %d", c)
	}
	// (b) rmax < maxp, a QoS 2 exchange past PUBREC: RM binds, the scheduling budget is free.
	s = mk(10, 1)
	testCustodyEnsureExchange(s.custody, gen, "x", 1)
	s.pendingPubRel[1] = &mqttPending{lin: "x", custody: true}
	if c := s.capacityAvailableLocked(); c != 0 {
		t.Fatalf("(b) Receive Maximum with a PubRecObserved exchange: capacity %d", c)
	}
	// (c) no Receive Maximum, maxp 1, one PUBREL outstanding and pendingPublish empty.
	s = mk(1, 0)
	s.pendingPubRel[1] = &mqttPending{}
	if c := s.capacityAvailableLocked(); c != 1 {
		t.Fatalf("(c) a PUBREL spent the scheduling budget: capacity %d", c)
	}
	// Replay (a'): RM 1, one custody PUBLISH selected for replay.
	s = mk(100, 1)
	testCustodyEnsureExchange(s.custody, gen, "r", 1)
	s.pendingPublish[1] = &mqttPending{lin: "r", custody: true}
	if a := s.replayAllowanceLocked(map[uint16]*mqttPending{1: s.pendingPublish[1]}); a < 1 {
		t.Fatalf("(a') the replay was budgeted as new work: allowance %d", a)
	}
	// (b') no RM, maxp 1, one replay.
	s = mk(1, 0)
	s.pendingPublish[1] = &mqttPending{}
	if a := s.replayAllowanceLocked(map[uint16]*mqttPending{1: s.pendingPublish[1]}); a < 1 {
		t.Fatalf("(b') the replay spent its own budget: allowance %d", a)
	}
	// (c') RM 1, a known PUBREL outstanding, one replay: no room.
	s = mk(100, 1)
	testCustodyEnsureExchange(s.custody, gen, "r", 1)
	s.pendingPublish[1] = &mqttPending{lin: "r", custody: true}
	testCustodyEnsureExchange(s.custody, gen, "q", 2)
	s.pendingPubRel[2] = &mqttPending{lin: "q", custody: true}
	if a := s.replayAllowanceLocked(map[uint16]*mqttPending{1: s.pendingPublish[1]}); a != 0 {
		t.Fatalf("(c') known occupancy dropped from the replay budget: allowance %d", a)
	}
	// (d) incomplete, RM 2: the replay allowance stands, new work is withheld.
	s = mk(100, 2)
	testCustodyEnsureExchange(s.custody, gen, "r", 1)
	s.pendingPublish[1] = &mqttPending{lin: "r", custody: true}
	s.custody.beginReconstruction(gen)
	if a := s.replayAllowanceLocked(map[uint16]*mqttPending{1: s.pendingPublish[1]}); a < 1 {
		t.Fatalf("(d) incompleteness zeroed the replay: allowance %d", a)
	}
	if c := s.capacityAvailableLocked(); c != 0 {
		t.Fatalf("(d) incompleteness admitted new work: capacity %d", c)
	}
	if d := s.resumeDeferredAllowanceLocked(2, 1); d != 0 {
		t.Fatalf("(d) incompleteness re-offered a deferred delivery: allowance %d", d)
	}
	s.custody.mergeCensus(gen, nil, nil, true)
	if d := s.resumeDeferredAllowanceLocked(2, 1); d != 1 {
		t.Fatalf("(d) a complete ledger withheld the deferred remainder: allowance %d", d)
	}
}

// T18. Fair re-observation: with a budget of one, a permanently live target never starves one
// that has ended.
//
// Mutation M18: targets ordered by key rather than lastObserved -> the later key is never taken.
func TestF105CustodyReobservationIsFair(t *testing.T) {
	t.Parallel()
	gen := mqttNewSessGen()
	l := newMQTTCustodyLedger(gen)
	testCustodyEnsureExchange(l, gen, "a-live", 1)
	testCustodyEnsureExchange(l, gen, "b-ended", 2)
	seen := map[string]bool{}
	for pass := 0; pass < 2; pass++ {
		ts := l.targets(gen, 1)
		if len(ts) != 1 {
			t.Fatalf("pass %d: %d targets", pass, len(ts))
		}
		seen[ts[0].key] = true
		at, _ := l.beginObservation(gen, ts[0].key)
		phase := mqttExchangePublish
		if ts[0].key == "b-ended" {
			phase = mqttExchangeTerminal
		}
		l.applyExchangeObservation(gen, ts[0].key, at, &mqttOutboundExchange{Nonce: ts[0].key, Phase: phase})
	}
	if !seen["b-ended"] {
		t.Fatal("the ended target was never reached within two passes of budget one")
	}
	if l.hasExchange(gen, "b-ended") {
		t.Fatal("the ended target was reached but not released")
	}
}

// T12. The ledger's state is touched only by its own methods, every method that takes a
// generation enters through forGen, and only the conversion and recovery mark an entry custody.
//
// Mutation M12: a direct delete from the ledger inserted elsewhere -> this fails.
func TestF105CustodyLedgerIsEncapsulated(t *testing.T) {
	t.Parallel()
	fields := map[string]bool{"heldAdmissions": true, "heldExchanges": true, "uncertainSubjects": true,
		"heldPIs": true, "reconstructionIncomplete": true, "observeTick": true, "ledgerGen": true}
	custodyWriters := map[string]bool{"openAdmissionCustodyLocked": true, "markRecoveredCustodyLocked": true,
		"trackAsPubRel": true}
	fset := token.NewFileSet()
	files, _ := filepath.Glob("*.go")
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			isLedgerMethod := false
			if fd.Recv != nil && len(fd.Recv.List) == 1 {
				if st, ok := fd.Recv.List[0].Type.(*ast.StarExpr); ok {
					if id, ok := st.X.(*ast.Ident); ok && id.Name == "mqttCustodyLedger" {
						isLedgerMethod = true
					}
				}
			}
			if isLedgerMethod && name != "outbound_custody_ledger.go" {
				t.Errorf("%s: a ledger method outside its file: %s", name, fd.Name.Name)
			}
			if isLedgerMethod && fd.Name.Name != "forGen" {
				takesGen := false
				for _, p := range fd.Type.Params.List {
					for _, n := range p.Names {
						if n.Name == "gen" {
							takesGen = true
						}
					}
				}
				if takesGen && !mqttTestFirstStmtCallsForGen(fd.Body) {
					t.Errorf("%s: ledger method %s takes a generation but does not enter through forGen",
						name, fd.Name.Name)
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.SelectorExpr:
					if fields[x.Sel.Name] && name != "outbound_custody_ledger.go" {
						t.Errorf("%s:%d: %s touches the ledger's %s directly", name,
							fset.Position(x.Pos()).Line, fd.Name.Name, x.Sel.Name)
					}
				case *ast.AssignStmt:
					for _, lhs := range x.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "custody" &&
							!custodyWriters[fd.Name.Name] && fd.Name.Name != "setIncarnationLocked" {
							t.Errorf("%s:%d: %s assigns %s.custody", name, fset.Position(x.Pos()).Line,
								fd.Name.Name, mqttTestExprString(sel.X))
						}
					}
				}
				return true
			})
		}
	}
}

func mqttTestFirstStmtCallsForGen(body *ast.BlockStmt) bool {
	if len(body.List) == 0 {
		return false
	}
	found := false
	ast.Inspect(body.List[0], func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "forGen" {
				found = true
			}
		}
		return !found
	})
	return found
}

func mqttTestExprString(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return mqttTestExprString(x.X) + "." + x.Sel.Name
	}
	return "?"
}

// T13. Nothing computes capacity from the scheduling maps' lengths outside the capacity
// computation; the allow-list names the non-capacity readers.
//
// Mutation M13: a len(sess.pendingPublish) comparison re-added to the delivery gate -> fails.
func TestF105CustodyNoMapLengthCapacity(t *testing.T) {
	t.Parallel()
	allowed := map[string]bool{
		"capacityAvailableLocked": true, "replayAllowanceLocked": true,
		// Allocation sizes and a presence test, not capacity.
		"resendPendingRedelivery": true,
	}
	fset := token.NewFileSet()
	files, _ := filepath.Glob("*.go")
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || allowed[fd.Name.Name] {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				c, ok := n.(*ast.CallExpr)
				if !ok || len(c.Args) != 1 {
					return true
				}
				if id, ok := c.Fun.(*ast.Ident); !ok || id.Name != "len" {
					return true
				}
				if sel, ok := c.Args[0].(*ast.SelectorExpr); ok &&
					(sel.Sel.Name == "pendingPublish" || sel.Sel.Name == "pendingPubRel") {
					t.Errorf("%s:%d: %s reads len(%s)", name, fset.Position(c.Pos()).Line, fd.Name.Name, sel.Sel.Name)
				}
				return true
			})
		}
	}
}

// linOf reads the lineage the register on pi landed holding.
func (e *testInflightEnv) linOf(t *testing.T, pi uint16) string {
	t.Helper()
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil {
		t.Fatalf("no register on %q: %v", subject, err)
	}
	return cur.Lin
}

// T2. A PUBACK frees the slot only once the exchange's advance is durable and observed: while
// the advance is held, a competing delivery is withheld.
//
// Mutation M2: the charge released with the scheduling entry -> B admitted while the advance is
// held.
func TestF105CustodyPubAckReleasesAfterTheAdvance(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-puback", 1)
	sess := f.e.sess
	testCustodyRM1(t, sess)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.Nonce == f.lin && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return write()
	}
	f.e.asm.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- f.e.c.mqttProcessPubAck(f.pi) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("the PUBACK never reached the exchange advance: %v", err)
	case <-time.After(testCustodyBound):
		t.Fatal("the PUBACK never reached the exchange advance")
	}
	testCustodyStaysFor(t, "B withheld while the advance is held", 200*time.Millisecond, func() bool {
		return testCustodyCapacity(sess) == 0
	})
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("PUBACK: %v", err)
	}
	testCustodyEventually(t, "B admitted once the advance landed and was observed", func() bool {
		return testCustodyCapacity(sess) == 1
	})
	if x := f.exchange(t); !mqttExchangePhaseEnded(x.Phase) {
		t.Fatalf("fixture: the PUBACK did not end the exchange (phase %d)", x.Phase)
	}
}

// T3. PUBCOMP over a live PUBREL action records terminal-pending-drain: the charge is held
// (pending drain), and released only when the drain completes the exchange.
//
// Mutation M3: pending-drain counted releasable -> B admitted before the drain.
func TestF105CustodyPendingDrainHoldsTheCharge(t *testing.T) {
	t.Parallel()
	w, act := testPendingDrainWorld(t, "custody-pending-drain")
	sess := w.e.sess
	testCustodyRM1(t, sess)
	testCustodyEventually(t, "the PUBCOMP's advance observed as pending drain", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		x := sess.custody.heldExchanges[w.seed.lin]
		return x != nil && x.pendingDrain
	})
	if c := testCustodyCapacity(sess); c != 0 {
		t.Fatalf("B admitted while the PUBREL output may still execute: capacity %d", c)
	}
	sess.startActionDrain(w.e.c, act)
	testCustodyEventually(t, "the drain completion released the charge", func() bool {
		return !testCustodyHasExchange(sess, w.seed.lin)
	})
}

// T11 (row 4's census). Two emissions of one canonical PUBREL are two acquisitions: distinct
// ordinals, the same canonical tuple.
//
// Mutation M11: the acquisition does not advance the ordinal -> fewer than two records land.
func TestF105CustodySuccessivePubRelEmissionsAreDistinctAcquisitions(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-pubrel-twice")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	var mu sync.Mutex
	var landed []*mqttAuthorityTransition
	testObserveAcquisitions(t, e, func(tr *mqttAuthorityTransition) {
		mu.Lock()
		landed = append(landed, tr)
		mu.Unlock()
	})
	for n := 1; n <= 2; n++ {
		testDeliverPubRelAs(t, e, seed.pi, seed.reply(n), seed.nonce)
		testRow2Eventually(t, testRow2Timeout, "the emission's drain to clear the token", func() bool {
			cur, _, _ := e.asm.loadInflightRecord(seed.subject)
			return cur != nil && cur.ActionToken == _EMPTY_
		})
	}
	mu.Lock()
	defer mu.Unlock()
	if len(landed) != 2 {
		t.Fatalf("%d acquisitions landed for two emissions, want 2", len(landed))
	}
	a, b := testRow2Split(t, e, landed[0]), testRow2Split(t, e, landed[1])
	if a.action == nil || b.action == nil || a.reg == nil || b.reg == nil {
		t.Fatal("an acquisition batch lacks its action or its slot")
	}
	if a.action.Acq == b.action.Acq {
		t.Fatalf("both emissions acquired ordinal %d", a.action.Acq)
	}
	if a.reg.PubRelStreamSeq != seed.canon || b.reg.PubRelStreamSeq != seed.canon {
		t.Fatalf("the canonical tuple moved: %d, %d, want %d", a.reg.PubRelStreamSeq, b.reg.PubRelStreamSeq, seed.canon)
	}
}

// T15. A hint never releases: a continuation that stopped (here, after PUBREC) asks for an
// observation, and the exchange still at PubRecObserved keeps its charge.
//
// Mutation M15: a hint releases the lineage directly -> the charge is gone.
func TestF105CustodyHintNeverReleases(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "custody-hint")
	testReadyForRealPubRelDelivery(t, e)
	seed := testSeedRealPubRelReady(t, e)
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	e.sess.hintCustody(gen, seed.lin)
	testCustodyEventually(t, "the hinted observation ran", func() bool {
		e.sess.mu.Lock()
		defer e.sess.mu.Unlock()
		x := e.sess.custody.heldExchanges[seed.lin]
		return x == nil || x.observedAt != 0
	})
	if !testCustodyHasExchange(e.sess, seed.lin) {
		t.Fatal("a hint released an exchange that is still PUBREC-observed")
	}
}

// T9. A pair replacement over a proven-unsent occupant on the SAME identifier starts a fresh
// lineage and supersedes the occupant's exchange in one batch: one charge before, one after, and
// the occupant's charge goes once its SupersededUnsent phase is observed.
//
// The ledger itself makes this shape unreachable by allocation — bumpPI skips an identifier the
// occupant's exchange still holds — so the fixture installs the successor's scheduling entry on
// that identifier directly, which is the state a same-delivery re-offer reaches the ladder with.
//
// Mutation M9: the superseded lineage not hinted -> its charge stays past the bound.
func TestF105CustodyPairReplacementMovesOneCharge(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-pair", 1)
	sess := f.e.sess
	sess.mu.Lock()
	sess.rmax, sess.maxp = 2, 100
	delete(sess.pendingPublish, f.pi)
	nonce := mqttNewInflightNonce()
	sess.pendingPublish[f.pi] = &mqttPending{jsDur: testRealPubRelSrcDur, sseq: f.sseq,
		jsAckSubject: testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2), regPending: true, regNonce: nonce}
	sess.mu.Unlock()
	// One charge, the occupant's: the successor's tracked entry sits on an identifier the ledger
	// holds, so it is read as the ledger's (receiveMaxOccupancyLocked) — the allocator could not
	// have handed it out.
	if c := testCustodyCapacity(sess); c != 1 {
		t.Fatalf("fixture: one charge (the occupant's) before the replacement, capacity %d", c)
	}
	if !sess.admitInflightPublish(f.e.c, f.pi, 1, testRealPubRelSrcDur, f.sseq, nonce, _EMPTY_, _EMPTY_) {
		t.Fatal("the pair replacement was refused")
	}
	heir, _ := f.register(t)
	if heir == nil || heir.Nonce != nonce || heir.Lin != nonce || heir.PI != f.pi {
		t.Fatalf("no pair replacement landed on the same identifier: %+v", heir)
	}
	if x := f.exchange(t); x.Phase != mqttExchangeSupersededUnsent {
		t.Fatalf("the occupant's exchange stands at %d, want SupersededUnsent", x.Phase)
	}
	testCustodyEventually(t, "the superseded charge released: one charge after", func() bool {
		return !testCustodyHasExchange(sess, f.lin) && testCustodyHasExchange(sess, nonce) &&
			testCustodyCapacity(sess) == 1
	})
}

// testCustodyEnsureExchange charges an exchange directly, for the ledger-level tests: the
// production paths charge only through a landed admission, a census or an uncertain re-read.
func testCustodyEnsureExchange(l *mqttCustodyLedger, gen, lin string, pi uint16) {
	if l.forGen(gen) {
		l.ensureExchangeLocked(lin, pi)
	}
}

// T47 (H-4). With no Receive Maximum the capacity gate never reaches zero on custody, so a
// ledger hold left by a failed one-shot observation of an ENDED exchange is converged only if
// allocator exhaustion itself asks for a pass: every other identifier is live, the capacity gate
// has room, the hint's observation failed, and the store has since recovered.
//
// Mutation M47: exhaustion returns 0 without requesting a pass -> the space never reopens.
func TestF105CustodyExhaustionRequestsObservation(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "custody-exhaustion", 1)
	sess := f.e.sess
	xsubj := mqttExchangeSubject(sess.idHash, f.gen, f.lin)
	var fault atomic.Bool
	fault.Store(true)
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyDecodeFault = func(subject string) bool { return fault.Load() && subject == xsubj }
	f.e.asm.mu.Unlock()
	sess.mu.Lock()
	sess.rmax = 0
	sess.maxp = 0xFFFF
	delete(sess.pendingPublish, f.pi)
	var before uint64
	if x := sess.custody.heldExchanges[f.lin]; x != nil {
		before = x.lastObserved
	}
	sess.mu.Unlock()
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, nil); r != mqttWithdrawLanded {
		t.Fatalf("withdrawal: %v", r)
	}
	testCustodyEventually(t, "the withdrawal hint's observation ran and failed", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		x := sess.custody.heldExchanges[f.lin]
		return x != nil && x.lastObserved != before && !sess.custodyObserving
	})
	if !testCustodyHasExchange(sess, f.lin) {
		t.Fatal("fixture: the failed observation released the exchange, so nothing is tested")
	}
	// Every other identifier is live in the scheduling map; the capacity gate still has room.
	filler := &mqttPending{}
	sess.mu.Lock()
	for pi := 1; pi <= 0xFFFF; pi++ {
		if uint16(pi) != f.pi {
			sess.pendingPublish[uint16(pi)] = filler
		}
	}
	sess.mu.Unlock()
	t.Cleanup(func() {
		sess.mu.Lock()
		for pi, p := range sess.pendingPublish {
			if p == filler {
				delete(sess.pendingPublish, pi)
			}
		}
		sess.mu.Unlock()
	})
	sess.mu.Lock()
	room := sess.custody.withholdsNew() || sess.custody.withholdsAllocation()
	sess.mu.Unlock()
	if room {
		t.Fatal("fixture: the ledger withholds for another reason, which kicks a pass of its own")
	}
	fault.Store(false) // the store has recovered; nothing else will re-read the exchange
	testCustodyStaysFor(t, "no pass without a request", 100*time.Millisecond, func() bool {
		return testCustodyHasExchange(sess, f.lin)
	})
	if got := testCustodyMints(sess, f.pi); got != 0 {
		t.Fatalf("fixture: allocation must be exhausted, got %d", got)
	}
	testCustodyEventually(t, "the exhausted allocator's pass observed the ended exchange", func() bool {
		return !testCustodyHasExchange(sess, f.lin)
	})
	if got := testCustodyMints(sess, f.pi); got != f.pi {
		t.Fatalf("the reopened identifier was not allocated: got %d", got)
	}
}

// testCustodyRetainedQoS reports the identifier a retained request is given now (0 = downgraded),
// untracking any entry it installs so the fixture's occupancy is unchanged.
func testCustodyRetainedQoS(sess *mqttSession) uint16 {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	pi := sess.trackPublishRetained(1)
	if pi != 0 {
		delete(sess.pendingPublish, pi)
	}
	return pi
}

// testCustodyQuiet waits until no observer runs and no hint or pass request is queued, so the
// retained request is the only thing that can start one.
func testCustodyQuiet(t *testing.T, sess *mqttSession) {
	t.Helper()
	testCustodyEventually(t, "the custody observer is idle", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving && !sess.custodyPassWanted && len(sess.custodyHints) == 0
	})
}

// T54 (H-5 f2, unit A). Retained-only traffic at the capacity gate requests the custody pass: a
// stale charge of an ended exchange whose one observation failed, and an incomplete census, both
// converge once the store answers, with retained requests the ONLY trigger. An exchange still at
// Publish stays charged: the request observes, it never releases.
//
// Mutation M54: the retained capacity-zero return requests no pass -> downgraded past the bound.
func TestF105CustodyRetainedCapacityRequestsObservation(t *testing.T) {
	t.Parallel()
	t.Run("ended-exchange-failed-observation", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "custody-retained-ended", 1)
		sess := f.e.sess
		testCustodyRM1(t, sess)
		xsubj := mqttExchangeSubject(sess.idHash, f.gen, f.lin)
		var fault atomic.Bool
		fault.Store(true)
		f.e.asm.mu.Lock()
		f.e.asm.testCustodyDecodeFault = func(subject string) bool { return fault.Load() && subject == xsubj }
		f.e.asm.mu.Unlock()
		sess.mu.Lock()
		delete(sess.pendingPublish, f.pi)
		sess.mu.Unlock()
		if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, nil); r != mqttWithdrawLanded {
			t.Fatalf("withdrawal: %v", r)
		}
		testCustodyQuiet(t, sess)
		if !testCustodyHasExchange(sess, f.lin) {
			t.Fatal("fixture: the failed observation released the exchange, so nothing is tested")
		}
		if pi := testCustodyRetainedQoS(sess); pi != 0 {
			t.Fatalf("fixture: the stale charge must downgrade the retained request, got %d", pi)
		}
		testCustodyQuiet(t, sess)
		fault.Store(false) // the store answers; only retained requests remain
		testCustodyEventually(t, "a retained request regained QoS 1/2", func() bool {
			return testCustodyRetainedQoS(sess) != 0
		})
		if testCustodyHasExchange(sess, f.lin) {
			t.Fatal("QoS 1/2 regained while the ended exchange is still charged")
		}
	})
	t.Run("incomplete-census", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "custody-retained-census")
		testReadyForRealPubRelDelivery(t, e)
		testCustodyRM1(t, e.sess)
		var fail atomic.Bool
		fail.Store(true)
		e.asm.mu.Lock()
		e.asm.testCustodyCensus = func() error {
			if fail.Load() {
				return errors.New("test: the census failed")
			}
			return nil
		}
		e.asm.mu.Unlock()
		e.sess.rebuildCustodyForResume(e.sess.gen)
		testCustodyQuiet(t, e.sess)
		if pi := testCustodyRetainedQoS(e.sess); pi != 0 {
			t.Fatalf("fixture: an incomplete census must downgrade, got %d", pi)
		}
		testCustodyQuiet(t, e.sess)
		fail.Store(false)
		testCustodyEventually(t, "a retained request regained QoS 1/2 after the census", func() bool {
			return testCustodyRetainedQoS(e.sess) != 0
		})
	})
	t.Run("publish-stays-charged", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "custody-retained-live", 1)
		sess := f.e.sess
		testCustodyRM1(t, sess)
		sess.mu.Lock()
		delete(sess.pendingPublish, f.pi)
		sess.mu.Unlock()
		testCustodyQuiet(t, sess)
		testCustodyStaysFor(t, "an exchange at Publish keeps its charge under retained requests", 300*time.Millisecond, func() bool {
			return testCustodyRetainedQoS(sess) == 0 && testCustodyHasExchange(sess, f.lin)
		})
	})
}
