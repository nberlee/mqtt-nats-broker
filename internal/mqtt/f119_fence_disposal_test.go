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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// F-119: a pending cancellation fence is disposed of only behind a confirmed, entitlement-fenced
// authority advance, and only once no ladder of its nonce can still prepare an attempt. These
// witnesses drive the production admission, cancellation and disposal routes; see
// pending_fence_disposal.go for the rule.

// f119AuthRev reads the serving authority's revision and sequence.
func f119AuthRev(t *testing.T, f *t36Fix) (rev, seq uint64) {
	t.Helper()
	al := f.e.asm.jsa.loadOutboundAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("the serving authority does not read: %+v", al)
	}
	return al.rec.Rev, al.seq
}

// f119RawDelete removes a record behind the door's back, so neither the register create-only CAS
// nor the disposal advance can be what refuses a later write.
func f119RawDelete(t *testing.T, f *t36Fix, seq uint64) {
	t.Helper()
	if err := f.e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true); err != nil {
		t.Fatalf("raw delete of sequence %d: %v", seq, err)
	}
}

// f119OursLanded reports whether a register carrying the allocation's nonce stands.
func f119OursLanded(t *testing.T, f *t36Fix) bool {
	t.Helper()
	rec, smsg := f.register(t)
	return smsg != nil && rec != nil && rec.Nonce == f.nonce && !rec.Cancelled
}

// W1: an admission batch PREPARED before the cancellation (its authority read taken, its predecessor
// validated) is refused by the authority CAS alone. The cancellation's fence is an authority
// transition, so the batch's authority sequence is stale by the time it reaches the store; the fence
// is raw-deleted before the batch is sent, so the register create-only CAS cannot be what refuses it.
//
// Mutation M1: emit the batch's authority member against the CURRENT stored sequence instead of
// PrevSeq -> the paused batch lands.
func TestF119PreparedAdmissionLosesItsAuthorityCASToTheFence(t *testing.T) {
	f := newT36Fix(t, "f119-w1")
	var paused atomic.Bool
	var revBefore, revAfter uint64
	presend := func(tr *mqttAuthorityTransition) {
		if tr.Attempt.Kind != mqttTransitionAdmitExchange || !paused.CompareAndSwap(false, true) {
			return
		}
		revBefore, _ = f119AuthRev(t, f)
		f.cancel(t)
		ok, fseq := f.fenceStands(t)
		if !ok {
			t.Error("fixture: the cancellation must land its pending fence")
			return
		}
		revAfter, _ = f119AuthRev(t, f)
		f119RawDelete(t, f, fseq)
	}
	f.e.asm.jsa.testTransitionPresend.Store(&presend)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionPresend.Store(nil) })
	var sent atomic.Bool
	var firstErr error
	commit := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind != mqttTransitionAdmitExchange {
			return send()
		}
		if sent.CompareAndSwap(false, true) {
			seq, err := send()
			firstErr = err
			return seq, err
		}
		// Only the paused batch is under test: a later attempt (the ladder retrying over the
		// subject the fixture emptied) is not sent.
		return 0, errors.New("test: later admission attempts are not sent")
	}
	f.e.asm.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })

	if f.admit() {
		t.Fatal("a cancelled allocation must not be admitted")
	}
	if !paused.Load() || !sent.Load() {
		t.Fatal("fixture: the admission's batch was never paused and sent")
	}
	if revAfter <= revBefore {
		t.Fatalf("the cancellation's fence must be an authority transition: revision %d -> %d", revBefore, revAfter)
	}
	if firstErr == nil {
		t.Fatal("the batch prepared before the fence must be refused by its authority CAS")
	}
	if f119OursLanded(t, f) || f.exchange() != nil {
		t.Fatal("the refused batch must land neither its register nor its exchange")
	}
}

// f119ServeResident binds the session to its connection and makes it the resident session for its
// client ID, the production preconditions of an exclusion-reclaim pass, and replaces the pass's
// timer with the test's explicit passes. It returns the pass's fence-step outcomes so far.
func f119ServeResident(t *testing.T, f *t36Fix) func() []mqttReclaimFenceOutcome {
	t.Helper()
	var steps []mqttReclaimFenceOutcome
	var mu sync.Mutex
	f.e.sess.mu.Lock()
	f.e.sess.c = f.e.c
	f.e.sess.testExclReclaim = &mqttExclReclaimSeams{
		fenceStep: func(_ uint16, _ string, out mqttReclaimFenceOutcome) {
			mu.Lock()
			steps = append(steps, out)
			mu.Unlock()
		},
		reschedule: func(time.Duration) bool { return false },
	}
	f.e.sess.mu.Unlock()
	f.e.asm.addSession(f.e.sess, true)
	return func() []mqttReclaimFenceOutcome {
		mu.Lock()
		defer mu.Unlock()
		return append([]mqttReclaimFenceOutcome(nil), steps...)
	}
}

// f119Advances records, per fence subject, whether each disposal decision committed an advance.
func f119Advances(t *testing.T) func() (advanced, refused int) {
	t.Helper()
	var adv, ref atomic.Int32
	h := func(_ string, advanced, ok bool) {
		switch {
		case advanced:
			adv.Add(1)
		case !ok:
			ref.Add(1)
		}
	}
	mqttTestPendingFenceDisposalHookV.Store(&h)
	t.Cleanup(func() { mqttTestPendingFenceDisposalHookV.Store(nil) })
	return func() (int, int) { return int(adv.Load()), int(ref.Load()) }
}

// W2: an admission attempt prepared AFTER the cancellation's fence (its authority read follows the
// fence) is held unsent and left unsettled — resolve and abort both suppressed — so it can still
// land. The cancelling ladder's own disposal of its fence is refused while the ladder runs; once it
// has returned, the fence is disposed of only behind a confirmed advance, and the held batch,
// released afterwards, is refused by that advance.
//
// Mutation M2: fenceBeforePendingFenceDisposal admits every disposal -> the ladder's own path deletes
// the fence with no advance and the held batch lands.
func TestF119UnsettledPostFenceAttemptCannotLandAfterDisposal(t *testing.T) {
	f := newT36Fix(t, "f119-w2")
	steps := f119ServeResident(t, f)
	advances := f119Advances(t)
	var cancelled atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testAdmitWrite = func(s *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if cancelled.CompareAndSwap(false, true) {
			f.cancel(t) // the fence lands BEFORE this attempt reads the authority
		}
		return write()
	}
	var fault atomic.Bool
	fault.Store(true)
	f.e.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if fault.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	f.e.asm.mu.Unlock()
	var held func() (uint64, error)
	var once atomic.Bool
	commit := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAdmitExchange {
			if once.CompareAndSwap(false, true) {
				held = send
				return 0, errors.New("test: the post-fence attempt's response is lost; the batch is held")
			}
			return 0, errors.New("test: later admission attempts are not sent")
		}
		return send()
	}
	f.e.asm.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })

	if f.admit() {
		t.Fatal("a cancelled allocation must not be admitted")
	}
	if !cancelled.Load() || held == nil {
		t.Fatal("fixture: the post-fence attempt was never prepared and held")
	}
	if !testCustodyHasAdmission(f.e.sess, f.nonce) {
		t.Fatal("fixture: the held attempt must stay unsettled in custody")
	}
	ok, fseq := f.fenceStands(t)
	if !ok {
		t.Fatal("the cancelling ladder's own disposal must be refused while it runs: the fence must stand")
	}
	if !t36Excluded(f.e.sess, f.pi) {
		t.Fatalf("the refused own disposal must leave identifier %d excluded for the reclaim continuation", f.pi)
	}

	// The ladder has returned: the reclaim continuation disposes of the fence, with no further
	// offer, and only behind an advance.
	f.e.sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	if got := steps(); len(got) != 1 || got[0] != mqttReclaimFenceAbsent {
		t.Fatalf("the continuation must dispose of the fence at %d and confirm its absence, steps %v", fseq, got)
	}
	if ok, _ := f.fenceStands(t); ok {
		t.Fatal("once the ladder has returned the fence must be disposed of")
	}
	if adv, _ := advances(); adv == 0 {
		// Not fatal: the consequence below is the property; this names its mechanism.
		t.Error("the fence must be disposed of only behind a committed authority advance")
	}

	// The held batch reaches the store now: its authority sequence is behind the advance.
	if _, err := held(); err == nil {
		t.Fatal("the held post-fence batch must be refused once the disposal's advance landed")
	}
	if f119OursLanded(t, f) || f.exchange() != nil {
		t.Fatal("the held batch must land neither its register nor its exchange")
	}
	fault.Store(false)
}

// f119StaleClient is a connection accepted under a PREDECESSOR tenure of the serving generation: the
// compare an authority transition makes is equality on generation and tenure, so a tenure the
// standing authority does not carry is exactly a displaced connection's, on any broker.
func f119StaleClient(t *testing.T, f *t36Fix) *client {
	t.Helper()
	stale := f.e.s.createInternalAccountClient()
	stale.mqtt = &mqtt{asm: f.e.asm, sess: f.e.sess}
	stale.setOutboundEntitlement(mqttOutboundEntitlement{Gen: f.gen, Tenure: mqttNewInflightNonce()})
	if _, ok := stale.outboundEntitlement(); !ok {
		t.Fatal("fixture: the stale connection must carry an entitlement")
	}
	return stale
}

// W3: a DISPLACED connection's ladder meets another allocation's pending fence on its identifier and
// takes the other-allocation arm, which disposes of the fence and retries. The disposal's advance is
// refused by the entitlement compare, so the fence stands and nothing is advanced.
//
// Mutation M3: drop the entitlement compare -> the displaced ladder advances and deletes the fence.
func TestF119DisplacedLadderCannotDisposeOfAServingFence(t *testing.T) {
	f := newT36Fix(t, "f119-w3")
	advances := f119Advances(t)
	other := mqttNewInflightNonce()
	_, fseq := testF088WriteFence(t, f.e, f.pi, f.dur, f.sseq+100, other)
	stale := f119StaleClient(t, f)
	if f.e.sess.admitInflightPublish(stale, f.pi, f.qos, f.dur, f.sseq, f.nonce, _EMPTY_, _EMPTY_) {
		t.Fatal("a displaced connection's admission must not succeed")
	}
	if ok, seq := f.fenceStands(t); !ok || seq != fseq {
		t.Fatalf("the displaced ladder must not dispose of the serving fence at %d: stands=%v seq=%d", fseq, ok, seq)
	}
	adv, refused := advances()
	if adv != 0 {
		t.Fatalf("a displaced connection must commit no advance, %d committed", adv)
	}
	if refused == 0 {
		t.Fatal("fixture: the displaced ladder never reached the fence's disposal")
	}
}

// W4: the OTHER door entrance (the gated one), reached by a never-transmitted request the door
// converts to the fence's own disposal, refuses a displaced licence and disposes of the fence for
// the serving one — behind an advance.
//
// Mutation M4: the helper is called only at the loaded entrance -> the converted request deletes the
// fence under the displaced licence.
func TestF119GatedEntranceConvertsAndFencesTheDisposal(t *testing.T) {
	f := newT36Fix(t, "f119-w4")
	advances := f119Advances(t)
	other := mqttNewInflightNonce()
	rec, fseq := testF088WriteFence(t, f.e, f.pi, f.dur, f.sseq+100, other)
	ref := mqttSlotRefFromRecord(f.subject, fseq, rec)
	stale := mqttLicenceOf(f119StaleClient(t, f))
	if f.e.sess.reapRegisterWithVerdictGated(fseq, other, false, ref, mqttSlotDeleteNeverTransmitted, stale) {
		t.Fatal("the gated entrance must refuse a displaced licence's converted disposal")
	}
	if ok, seq := f.fenceStands(t); !ok || seq != fseq {
		t.Fatalf("the fence must stand after the refused converted disposal: stands=%v seq=%d", ok, seq)
	}
	if adv, _ := advances(); adv != 0 {
		t.Fatalf("a displaced licence must commit no advance, %d committed", adv)
	}
	if !f.e.sess.reapRegisterWithVerdictGated(fseq, other, false, ref, mqttSlotDeleteNeverTransmitted, mqttLicenceOf(f.e.c)) {
		t.Fatal("the gated entrance must dispose of the fence for the serving licence")
	}
	if ok, _ := f.fenceStands(t); ok {
		t.Fatal("the serving licence's converted disposal must remove the fence")
	}
	if adv, _ := advances(); adv != 1 {
		t.Fatalf("the serving disposal must commit exactly one advance, %d committed", adv)
	}
}

// f119PausedLadder starts the allocation's real admission and pauses its ladder inside the attempt
// loop, before the attempt's authority read. resume lets the attempt proceed and returns whether the
// admission succeeded; attempted reports whether that attempt's batch was refused.
func f119PausedLadder(t *testing.T, f *t36Fix) (resume func() (admitted bool, refusedAttempt bool)) {
	t.Helper()
	paused, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var attemptErr atomic.Value
	f.e.asm.mu.Lock()
	f.e.asm.testAdmitWrite = func(s *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if once.CompareAndSwap(false, true) {
			close(paused)
			<-release
			out, err := write()
			attemptErr.Store(err != nil)
			return out, err
		}
		return mqttOpOutcome{}, errors.New("test: later admission attempts are not sent")
	}
	f.e.asm.mu.Unlock()
	done := make(chan bool, 1)
	go func() { done <- f.admit() }()
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: the ladder never reached its attempt")
	}
	f.e.sess.mu.Lock()
	live := f.e.sess.custody.admissionLadderLive(f.gen, f.nonce)
	f.e.sess.mu.Unlock()
	if !live {
		t.Fatal("fixture: the paused ladder's custody admission must be live")
	}
	return func() (bool, bool) {
		close(release)
		var admitted bool
		select {
		case admitted = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the released ladder never returned")
		}
		refused, _ := attemptErr.Load().(bool)
		return admitted, refused
	}
}

// W5: the exclusion-reclaim continuation reaches a pending fence whose nonce's ladder is still LIVE —
// the state a stale snapshot meets when its exclusion was lifted and the identifier re-allocated to
// that nonce (the exclusion here is installed directly as that stand-in). Q refuses: the fence
// stands, nothing is advanced, and the live ladder's next attempt meets the fence and is refused.
//
// Mutation M5: remove Q -> the continuation advances and deletes, and the live ladder's next attempt
// lands on the empty subject.
func TestF119DisposalRefusedWhileTheFenceNonceLadderRuns(t *testing.T) {
	f := newT36Fix(t, "f119-w5")
	steps := f119ServeResident(t, f)
	advances := f119Advances(t)
	resume := f119PausedLadder(t, f)
	f.cancel(t)
	ok, fseq := f.fenceStands(t)
	if !ok {
		t.Fatal("fixture: the cancellation must land its pending fence")
	}
	f.e.sess.mu.Lock()
	f.e.sess.excludeIdentifierLocked(f.pi, f.subject, f.nonce)
	f.e.sess.mu.Unlock()
	f.e.sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	if ok, seq := f.fenceStands(t); !ok || seq != fseq {
		t.Errorf("the fence of a live ladder's nonce must stand (steps %v)", steps())
	}
	if adv, _ := advances(); adv != 0 {
		t.Errorf("no advance may be committed while the ladder runs, %d committed", adv)
	}
	admitted, refused := resume()
	if admitted || !refused {
		t.Fatalf("the live ladder's next attempt must meet the fence and be refused: admitted=%v refused=%v", admitted, refused)
	}
	if f119OursLanded(t, f) {
		t.Fatal("nothing of the cancelled allocation may land")
	}
}

// W5b: the same, after a DRAIN removed the live ladder's scheduling entry while its store was in
// flight (stageDrain's production steps: verdict staged, identifier excluded, entry untracked). Q is
// keyed on the ladder's custody admission, not on that entry, so it still refuses. The fence is
// PLANTED through the production fence writer: stageDrain writes none, so this is a synthetic fence
// fixture that tests Q's predicate against a drain-removed entry, not a production producer.
//
// Mutation M5b: key Q on the scheduling entry -> the drain-removed entry passes, the fence is
// deleted, and the live ladder's next attempt lands.
func TestF119DisposalRefusedForADrainRemovedLiveLadder(t *testing.T) {
	f := newT36Fix(t, "f119-w5b")
	steps := f119ServeResident(t, f)
	advances := f119Advances(t)
	resume := f119PausedLadder(t, f)
	lic := mqttLicenceOf(f.e.c)
	f.e.sess.mu.Lock()
	ack := f.e.sess.pendingPublish[f.pi]
	if ack == nil || !ack.regPending || ack.regNonce != f.nonce {
		f.e.sess.mu.Unlock()
		t.Fatalf("fixture: the live ladder's entry must be its pending allocation, got %+v", ack)
	}
	if !f.e.sess.cancelRegPendingLocked(ack, lic) {
		f.e.sess.mu.Unlock()
		t.Fatal("fixture: the drain's cancellation must stage")
	}
	f.e.sess.excludeIdentifierLocked(f.pi, f.subject, f.nonce)
	f.e.sess.untrackPublishEntry(f.pi, mqttUntrackUnanswered)
	_, entryStands := f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if entryStands {
		t.Fatal("fixture: the drain must remove the entry")
	}
	_, fseq := testF088WriteFence(t, f.e, f.pi, f.dur, f.sseq, f.nonce)
	f.e.sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	if ok, seq := f.fenceStands(t); !ok || seq != fseq {
		t.Errorf("the fence of a drain-removed live ladder's nonce must stand (steps %v)", steps())
	}
	if adv, _ := advances(); adv != 0 {
		t.Errorf("no advance may be committed while the ladder runs, %d committed", adv)
	}
	admitted, refused := resume()
	if admitted || !refused {
		t.Fatalf("the live ladder's next attempt must meet the fence and be refused: admitted=%v refused=%v", admitted, refused)
	}
	if f119OursLanded(t, f) {
		t.Fatal("nothing of the cancelled allocation may land")
	}
}

// f119RevOf reads a session's serving authority revision.
func f119RevOf(t *testing.T, e *testInflightEnv) uint64 {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("the serving authority does not read: %+v", al)
	}
	return al.rec.Rev
}

// W6: the exclusion-reclaim pass's advance is a background authority attempt. (a) The dispatch gate
// closing after every earlier gate passed and immediately before the commit stops it: nothing is
// committed and the fence stands. (b) An announced foreground operation holds it back until released;
// it then disposes of the fence. The pass's request account (exact, row 18b) charges every request.
//
// Mutation M6: the advance runs on the ungated view -> (a), (c) and (d) send requests through a closed
// gate (a and c commit an advance).
func TestF119ReclaimAdvanceHonoursShutdownAndForeground(t *testing.T) {
	t.Run("closed-gate-before-commit", func(t *testing.T) {
		r := newT36Reclaim(t, "f119-w6a")
		const X = uint16(10)
		f, n, fseq := r.fence("f", 110)
		r.exclude(r.e.sess, X, f, n)
		rev := f119RevOf(t, r.e)
		var reached atomic.Bool
		h := func(string) {
			reached.Store(true)
			r.e.s.willDispatchMu.Lock()
			r.e.s.willDispatchClosed = true
			r.e.s.willDispatchMu.Unlock()
		}
		mqttTestPendingFenceBeforeCommitHookV.Store(&h)
		t.Cleanup(func() { mqttTestPendingFenceBeforeCommitHookV.Store(nil) })
		steps := r.pass(r.e.sess)
		if !reached.Load() {
			t.Fatal("fixture: the advance never reached its commit gate")
		}
		if got := f119RevOf(t, r.e); got != rev {
			t.Fatalf("a closed gate before the commit must commit nothing: revision %d -> %d (steps %v)", rev, got, steps)
		}
		if !r.present(f) {
			t.Fatalf("the fence at %d must stand (steps %v)", fseq, steps)
		}
	})
	// Closed INSIDE the commit, after its predecessor readback and before the batch: the batch is a
	// request of the gated view, so it is refused unsent.
	t.Run("closed-gate-inside-commit", func(t *testing.T) {
		r := newT36Reclaim(t, "f119-w6c")
		const X = uint16(10)
		f, n, _ := r.fence("f", 110)
		r.exclude(r.e.sess, X, f, n)
		rev := f119RevOf(t, r.e)
		refusedBefore := mqttTestRequestGateRefusals.Load()
		var closed atomic.Bool
		presend := func(tr *mqttAuthorityTransition) {
			if tr.Attempt.Kind != mqttTransitionFencePendingDisposal || !closed.CompareAndSwap(false, true) {
				return
			}
			r.e.s.willDispatchMu.Lock()
			r.e.s.willDispatchClosed = true
			r.e.s.willDispatchMu.Unlock()
		}
		r.e.asm.jsa.testTransitionPresend.Store(&presend)
		t.Cleanup(func() { r.e.asm.jsa.testTransitionPresend.Store(nil) })
		steps := r.pass(r.e.sess)
		if !closed.Load() {
			t.Fatal("fixture: the advance never reached its batch")
		}
		if got := f119RevOf(t, r.e); got != rev {
			t.Fatalf("a gate closed inside the commit must keep its batch unsent: revision %d -> %d (steps %v)", rev, got, steps)
		}
		if mqttTestRequestGateRefusals.Load() == refusedBefore {
			t.Fatal("the batch must have been refused BY THE GATE, not merely not reached")
		}
		if !r.present(f) {
			t.Fatal("the fence must stand")
		}
	})
	// A lost reply, then the gate closes: the resolution's reads and the abort are requests of the
	// gated view, so none of them is sent after the closure.
	t.Run("closed-gate-at-resolution", func(t *testing.T) {
		r := newT36Reclaim(t, "f119-w6d")
		const X = uint16(10)
		f, n, _ := r.fence("f", 110)
		r.exclude(r.e.sess, X, f, n)
		rev := f119RevOf(t, r.e)
		refusedBefore := mqttTestRequestGateRefusals.Load()
		commit := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.Kind != mqttTransitionFencePendingDisposal {
				return send()
			}
			r.e.s.willDispatchMu.Lock()
			r.e.s.willDispatchClosed = true
			r.e.s.willDispatchMu.Unlock()
			return 0, errors.New("test: the advance's reply is lost and the gate closes")
		}
		r.e.asm.jsa.testTransitionCommit.Store(&commit)
		t.Cleanup(func() { r.e.asm.jsa.testTransitionCommit.Store(nil) })
		r.mu.Lock()
		addedAtClose := -1
		r.mu.Unlock()
		pre := func(string) {
			r.mu.Lock()
			addedAtClose = r.added
			r.mu.Unlock()
		}
		mqttTestPendingFenceBeforeCommitHookV.Store(&pre)
		t.Cleanup(func() { mqttTestPendingFenceBeforeCommitHookV.Store(nil) })
		r.pass(r.e.sess)
		if got := f119RevOf(t, r.e); got != rev {
			t.Fatalf("nothing may be committed: revision %d -> %d", rev, got)
		}
		if mqttTestRequestGateRefusals.Load() == refusedBefore {
			t.Fatal("the resolution's first read must be refused by the gate")
		}
		r.mu.Lock()
		after := r.added
		r.mu.Unlock()
		// The predecessor readback is the commit's only request before the hook-closed batch; nothing
		// after the closure is charged, because nothing after it is sent.
		if addedAtClose < 0 || after > addedAtClose+1 {
			t.Fatalf("requests charged after the closure: %d at the commit, %d after", addedAtClose, after)
		}
		if !r.present(f) {
			t.Fatal("the fence must stand")
		}
	})
	// A commit that LANDS but whose reply is lost: the resolution reads it back as landed, every one
	// of those reads is charged, and the pass's account stays exact (spent = snapshot charge + added).
	t.Run("landed-lost-reply-is-charged", func(t *testing.T) {
		r := newT36Reclaim(t, "f119-w6e")
		const X = uint16(10)
		f, n, _ := r.fence("f", 110)
		r.exclude(r.e.sess, X, f, n)
		rev := f119RevOf(t, r.e)
		commit := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
			if tr.Attempt.Kind != mqttTransitionFencePendingDisposal {
				return send()
			}
			if _, err := send(); err != nil {
				return 0, err
			}
			return 0, errors.New("test: the advance landed but its reply is lost")
		}
		r.e.asm.jsa.testTransitionCommit.Store(&commit)
		t.Cleanup(func() { r.e.asm.jsa.testTransitionCommit.Store(nil) })
		admitsBefore := mqttTestRequestGateAdmits.Load()
		r.pass(r.e.sess)
		// The authority load, the commit's predecessor readback and batch, and the resolution's
		// outcome read all went through the gated view: at least four admitted, charged requests.
		if got := mqttTestRequestGateAdmits.Load() - admitsBefore; got < 4 {
			t.Fatalf("the advance's requests must each be admitted and charged by the gate, %d were", got)
		}
		if got := f119RevOf(t, r.e); got != rev+1 {
			t.Fatalf("the landed advance must stand exactly once: revision %d -> %d", rev, got)
		}
		if r.present(f) {
			t.Fatal("a resolution that reads the advance back as landed must dispose of the fence")
		}
		r.mu.Lock()
		spent := append([][2]int(nil), r.spent...)
		added := r.added
		r.mu.Unlock()
		if len(spent) == 0 || spent[len(spent)-1][1] != spent[len(spent)-1][0]+added {
			t.Fatalf("the pass's account must stay exact: spent %v, added %d", spent, added)
		}
	})
	t.Run("foreground-holds-it-back", func(t *testing.T) {
		r := newT36Reclaim(t, "f119-w6b")
		const X = uint16(10)
		f, n, _ := r.fence("f", 110)
		r.exclude(r.e.sess, X, f, n)
		rev := f119RevOf(t, r.e)
		release := r.e.sess.announceForegroundAuthority()
		done := make(chan []string, 1)
		go func() { done <- r.pass(r.e.sess) }()
		select {
		case steps := <-done:
			release()
			t.Fatalf("the background advance must wait for the announced foreground operation (steps %v)", steps)
		case <-time.After(300 * time.Millisecond):
		}
		if got := f119RevOf(t, r.e); got != rev {
			release()
			t.Fatalf("nothing may be committed while a foreground operation is announced: revision %d -> %d", rev, got)
		}
		release()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the pass never finished after the foreground operation ended")
		}
		if r.present(f) {
			t.Fatal("once released the pass must dispose of the fence")
		}
		if got := f119RevOf(t, r.e); got != rev+1 {
			t.Fatalf("the disposal must commit exactly one advance: revision %d -> %d", rev, got)
		}
	})
}

// f119ServeGeneration installs, as store state, a serving authority at gen with a fresh tenure,
// CAS'd over the standing one: the successor generation's completed handoff.
func f119ServeGeneration(t *testing.T, e *testInflightEnv, gen string) {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("the serving authority does not read: %+v", al)
	}
	next := *al.rec
	next.Gen, next.Tenure, next.Rev, next.Txn = gen, mqttNewInflightNonce(), al.rec.Rev+1, "txn-f119-handoff"
	b, _ := json.Marshal(&next)
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.sess.id)), hdr, msg); err != nil {
		t.Fatalf("installing the successor generation's serving authority: %v", err)
	}
}

// W7: the generation branch is decided by the STORE, fail-closed. (b) While the stored authority still
// serves the fence's generation but the local ledger is another generation's — the clean-start window,
// where resetForReuseLocked re-mints the object (deleting no record) before the authority handoff — the
// disposal is refused even under a licence of the still-serving entitlement: a local generation
// mismatch is never retirement. The window's ledger is INSTALLED directly (newMQTTCustodyLedger of a
// fresh generation): a synthetic state for Q's generation predicate, not the re-mint route. (a) Once
// the stored authority serves the successor generation, the same disposal goes through with NO
// advance — no admission of the fence's generation can land any more — so the refusal was transient.
//
// Mutations: M7b lets the ledger mismatch pass Q -> (b) deletes the fence; M7a treats another serving
// generation like the fence's -> (a) is refused by the entitlement compare and the fence never goes.
func TestF119GenerationBranchIsDecidedByTheStore(t *testing.T) {
	f := newT36Fix(t, "f119-w7")
	advances := f119Advances(t)
	f.cancel(t)
	rec, smsg := f.register(t)
	if smsg == nil || !mqttValidPendingFence(f.subject, rec) {
		t.Fatalf("fixture: the cancellation must land its pending fence, got %+v", rec)
	}
	stillServing := mqttLicenceOf(f.e.c)
	successor := mqttNewSessGen()
	f.e.sess.mu.Lock()
	f.e.sess.custody = newMQTTCustodyLedger(successor)
	f.e.sess.mu.Unlock()

	// (b) the clean-start window: the authority still serves the fence's generation.
	if out := f.e.sess.reapLoadedRegister(f.subject, smsg.Sequence, rec, mqttSlotDeletePendingFence, stillServing, nil); out == mqttReapDeleted {
		t.Fatal("a fence of the serving generation must be kept while the local ledger is another generation's")
	}
	if ok, _ := f.fenceStands(t); !ok {
		t.Fatal("the fence must stand through the clean-start window")
	}
	if adv, _ := advances(); adv != 0 {
		t.Fatalf("the clean-start window must commit no advance, %d committed", adv)
	}

	// (a) the handoff lands: the stored authority serves the successor generation.
	f119ServeGeneration(t, f.e, successor)
	if out := f.e.sess.reapLoadedRegister(f.subject, smsg.Sequence, rec, mqttSlotDeletePendingFence, stillServing, nil); out != mqttReapDeleted {
		t.Fatalf("once another generation serves, the fence must be disposed of, got %v", out)
	}
	if ok, _ := f.fenceStands(t); ok {
		t.Fatal("the fence of a generation no longer served must be gone")
	}
	if adv, _ := advances(); adv != 0 {
		t.Fatalf("a fence of a generation no longer served needs no advance, %d committed", adv)
	}
}

// f119ServeTenure installs, as store state, the serving authority at the SAME generation under a fresh
// tenure (a resident reconnect's accept), and returns that tenure.
func f119ServeTenure(t *testing.T, e *testInflightEnv) string {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("the serving authority does not read: %+v", al)
	}
	next := *al.rec
	next.Tenure, next.Rev, next.Txn = mqttNewInflightNonce(), al.rec.Rev+1, "txn-f119-rebind"
	b, _ := json.Marshal(&next)
	msg, hdr := mqttFrameBatchWrite(al.seq, b, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(e.sess.id)), hdr, msg); err != nil {
		t.Fatalf("installing the successor tenure's serving authority: %v", err)
	}
	return next.Tenure
}

// W8 (the current-binding exception's distinctive invariant, consult D2): connection A's ladder of the
// fence's nonce is still running when the SAME session object is rebound to successor B under a new
// tenure and its custody is reconstructed for the resume. Reclaim, now under B's licence, refuses: the
// reconstruction preserved A's live admission, so the quiescence proof still sees it. A's attempt is
// refused by the entitlement compare; once A's ladder has returned, the same exclusion's retry under B
// disposes of the fence behind one advance.
func TestF119ResidentRebindKeepsTheOldLadderVisible(t *testing.T) {
	f := newT36Fix(t, "f119-w8")
	steps := f119ServeResident(t, f)
	advances := f119Advances(t)
	resume := f119PausedLadder(t, f)
	f.cancel(t)
	ok, fseq := f.fenceStands(t)
	if !ok {
		t.Fatal("fixture: the cancellation must land its pending fence")
	}
	f.e.sess.mu.Lock()
	f.e.sess.excludeIdentifierLocked(f.pi, f.subject, f.nonce)
	f.e.sess.mu.Unlock()

	tenureB := f119ServeTenure(t, f.e)
	b := f.e.s.createInternalAccountClient()
	b.mqtt = &mqtt{asm: f.e.asm, sess: f.e.sess}
	b.setOutboundEntitlement(mqttOutboundEntitlement{Gen: f.gen, Tenure: tenureB})
	f.e.sess.mu.Lock()
	f.e.sess.c = b
	f.e.sess.mu.Unlock()
	f.e.sess.rebuildCustodyForResume(f.gen)
	f.e.sess.mu.Lock()
	live := f.e.sess.custody.admissionLadderLive(f.gen, f.nonce)
	f.e.sess.mu.Unlock()
	if !live {
		t.Fatal("the resume's reconstruction must preserve A's live admission")
	}

	f.e.sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	if ok, seq := f.fenceStands(t); !ok || seq != fseq {
		t.Errorf("reclaim under B must refuse while A's ladder runs (steps %v)", steps())
	}
	if adv, _ := advances(); adv != 0 {
		t.Errorf("no advance may be committed while A's ladder runs, %d committed", adv)
	}
	admitted, refused := resume()
	if admitted || !refused {
		t.Fatalf("A's attempt must be refused: admitted=%v refused=%v", admitted, refused)
	}
	if f119OursLanded(t, f) {
		t.Fatal("nothing of the cancelled allocation may land")
	}

	f.e.sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	if ok, _ := f.fenceStands(t); ok {
		t.Fatalf("once A's ladder has returned, the retry under B must dispose of the fence (steps %v)", steps())
	}
	if adv, _ := advances(); adv != 1 {
		t.Fatalf("the disposal must commit exactly one advance under B, %d committed", adv)
	}
}
