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
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A delivery callback whose connection's incarnation starts retiring stops starting work
// (mqttDeliveryLifecycle): each witness closes the connection's tracker — the retirement's own first
// step — from a seam INSIDE the boundary it tests, after every earlier check has passed, and counts
// the work started after the close. Then the tracker is reopened (an aborted takeover) and every
// message still arrives once.

// testStopRig is a batch rig whose session's authority transitions are counted per operation, with
// a hook before and after each commit, and whose transmissions are counted as they enter.
type testStopRig struct {
	*testBatchRig
	ss *mqttSession

	mu      sync.Mutex
	ops     map[string]int
	atClose map[string]int
	before  func(op string, n int, at *mqttAuthorityTransition)
	after   func(op string, n int, at *mqttAuthorityTransition, err error)
	errs    []string

	// closer, when set, replaces closeTracker as the way testStopAfterPrecedingFlush ends the
	// lifecycle.
	closer    func()
	tr        atomic.Pointer[mqttIncarnationTracker]
	transmits atomic.Int32 // sections that reached their transmission
	sentAt    atomic.Int32 // transmits when the tracker closed
}

func newTestStopRig(t *testing.T, id string, qos byte) *testStopRig {
	t.Helper()
	return newTestStopRigAckWait(t, id, qos, time.Second)
}

func newTestStopRigAckWait(t *testing.T, id string, qos byte, ackWait time.Duration) *testStopRig {
	t.Helper()
	rig := newTestBatchRig(t, id, qos, func(o *Options) { o.MQTT.AckWait = ackWait })
	return testStopRigOver(t, rig)
}

func testStopRigOver(t *testing.T, rig *testBatchRig) *testStopRig {
	r := &testStopRig{testBatchRig: rig, ss: rig.sess(), ops: map[string]int{}}
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt == nil || at.Attempt.ID != r.ss.id {
			return send()
		}
		r.mu.Lock()
		r.ops[at.Attempt.Op]++
		n := r.ops[at.Attempt.Op]
		before, after := r.before, r.after
		r.mu.Unlock()
		if before != nil {
			before(at.Attempt.Op, n, at)
		}
		seq, err := send()
		if after != nil {
			after(at.Attempt.Op, n, at, err)
		}
		return seq, err
	}
	r.ss.jsa.testTransitionCommit.Store(&commit)
	window := func(s *mqttSession, _ uint16, w string) bool {
		if s == r.ss && w == mqttMarkerSendReserved {
			r.transmits.Add(1)
		}
		return true
	}
	mqttTestMarkerSendWindowHookV.Store(&window)
	t.Cleanup(func() {
		r.ss.jsa.testTransitionCommit.Store(nil)
		mqttTestMarkerSendWindowHookV.Store(nil)
		r.reopen()
	})
	return r
}

func (r *testStopRig) hooks(before func(op string, n int, at *mqttAuthorityTransition),
	after func(op string, n int, at *mqttAuthorityTransition, err error)) {
	r.mu.Lock()
	r.before, r.after = before, after
	r.mu.Unlock()
}

func (r *testStopRig) fail(format string, args ...any) {
	r.mu.Lock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

// closeTracker starts the retirement of the subscriber connection's incarnation, once: the
// tracker's close, which is what a takeover's retirement does first. The operation counts at that
// instant are kept.
func (r *testStopRig) closeTracker() {
	c := testSessionConn(r.ss)
	if c == nil || c.mqtt == nil {
		r.fail("fixture: no connection to retire")
		return
	}
	tr := c.mqtt.tracker
	if !r.tr.CompareAndSwap(nil, tr) {
		return
	}
	r.mu.Lock()
	r.atClose = map[string]int{}
	for k, v := range r.ops {
		r.atClose[k] = v
	}
	r.mu.Unlock()
	r.sentAt.Store(r.transmits.Load())
	tr.close()
}

// reopen aborts the retirement, as a takeover that fails does.
func (r *testStopRig) reopen() {
	if tr := r.tr.Load(); tr != nil {
		tr.reopen()
	}
}

// waitClosed waits for the fixture to have closed the tracker.
func (r *testStopRig) waitClosed() {
	r.t.Helper()
	checkFor(r.t, 15*time.Second, 5*time.Millisecond, func() error {
		if r.tr.Load() == nil {
			return fmt.Errorf("the tracker was never closed")
		}
		return nil
	})
}

// startedAfterClose reports how many of the named operations were committed after the close, once
// the session has had time to start any it would.
func (r *testStopRig) startedAfterClose(ops ...string) map[string]int {
	r.t.Helper()
	time.Sleep(700 * time.Millisecond)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.errs {
		r.t.Error(e)
	}
	out := map[string]int{}
	for _, op := range ops {
		if d := r.ops[op] - r.atClose[op]; d != 0 {
			out[op] = d
		}
	}
	return out
}

func (r *testStopRig) expectNothingStarted(what string, ops ...string) {
	r.t.Helper()
	if got := r.startedAfterClose(ops...); len(got) != 0 {
		r.t.Fatalf("%s: work started after the incarnation began retiring: %v", what, got)
	}
}

func (r *testStopRig) withheld() int {
	r.ss.mu.Lock()
	defer r.ss.mu.Unlock()
	return len(r.ss.deferred)
}

// readAllFirst reads n messages after the reopen: each arrives exactly once, as a first
// transmission.
func (r *testStopRig) readAllFirst(n int) {
	r.t.Helper()
	seen := map[string]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < n {
		if time.Now().After(deadline) {
			r.t.Fatalf("only %d of %d messages arrived after the reopen", len(seen), n)
		}
		flags, pi, _, payload := testMQTTGetPubMsgEx(r.t, r.sc, r.sr, "ab/t", nil)
		if flags&mqttPubFlagDup != 0 {
			r.t.Fatalf("%s arrived with DUP set; nothing was transmitted before the stop", payload)
		}
		if seen[string(payload)] {
			r.t.Fatalf("%s arrived twice", payload)
		}
		seen[string(payload)] = true
		testMQTTSendPIPacket(mqttPacketPubAck, r.t, r.sc, pi)
	}
}

// stale makes the commit about to be sent lose to a stale authority expectation, as a sibling
// subscription's commit does.
func (r *testStopRig) stale() {
	if err := testBumpSessionAuthority(r.ss); err != nil {
		r.fail("fixture: revising the authority: %v", err)
	}
}

var testAdmitOps = []string{"op-admit-publishes", "op-admit-publish"}
var testAcquireOps = []string{"op-acquire-publishes", "op-acquire-publish"}
var testDeliveryOps = append(append([]string(nil), testAdmitOps...), testAcquireOps...)

// TestRetirementStopsABatchAdmissionAtItsNextAttempt: a batched admission that lost an attempt to
// contention while its incarnation began retiring makes no further attempt; its deliveries are
// abandoned unacknowledged and recorded withheld, and arrive once after the reopen.
func TestRetirementStopsABatchAdmissionAtItsNextAttempt(t *testing.T) {
	r := newTestStopRig(t, "rs-batch", 1)
	r.hooks(func(op string, n int, _ *mqttAuthorityTransition) {
		if op == "op-admit-publishes" && r.tr.Load() == nil {
			r.stale()
			r.closeTracker()
		}
	}, nil)
	const n = 8
	r.backlog(n)
	r.waitClosed()
	r.expectNothingStarted("batch admission", testDeliveryOps...)
	if r.withheld() == 0 {
		t.Fatalf("the abandoned deliveries were not recorded withheld")
	}
	r.reopen()
	r.readAllFirst(n)
}

// TestRetirementStopsTheSingleAdmissionLadderAtItsNextAttempt: one delivery at rest takes the single
// ladder; its first attempt loses to contention as the incarnation begins retiring, and no second
// attempt is made.
func TestRetirementStopsTheSingleAdmissionLadderAtItsNextAttempt(t *testing.T) {
	r := newTestStopRig(t, "rs-ladder", 1)
	r.hooks(func(op string, _ int, _ *mqttAuthorityTransition) {
		if op == "op-admit-publish" && r.tr.Load() == nil {
			r.stale()
			r.closeTracker()
		}
	}, nil)
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("single ladder", testDeliveryOps...)
	if r.withheld() == 0 {
		t.Fatalf("the abandoned delivery was not recorded withheld")
	}
	r.reopen()
	r.readAllFirst(1)
}

// TestRetirementStopsTheLadderAfterAFailedReread: the ladder's first attempt loses to contention and
// its re-read of the register fails as the incarnation begins retiring; the read failure's loop
// back meets the stop, and no second attempt is made.
func TestRetirementStopsTheLadderAfterAFailedReread(t *testing.T) {
	r := newTestStopRig(t, "rs-reread", 1)
	var armed atomic.Value // the register subject whose next read fails
	as := r.ss.asm
	as.mu.Lock()
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if s, _ := armed.Load().(string); s != _EMPTY_ && s == subject {
			armed.Store(_EMPTY_)
			r.closeTracker()
			return nil, nil, fmt.Errorf("fixture: the register could not be read")
		}
		return as.jsa.loadInflightRecord(subject)
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testInflightLoad = nil
		as.mu.Unlock()
	})
	var once sync.Once
	r.hooks(func(op string, _ int, _ *mqttAuthorityTransition) {
		if op == "op-admit-publish" && r.tr.Load() == nil {
			once.Do(r.stale)
		}
	}, func(op string, _ int, at *mqttAuthorityTransition, err error) {
		if op != "op-admit-publish" || err == nil || r.tr.Load() != nil {
			return
		}
		for _, m := range at.Extra {
			if name, perr := mqttParseInflightSubject(m.subj); perr == nil && name != nil && m.expect == 0 {
				armed.Store(m.subj)
				return
			}
		}
		r.fail("fixture: the attempt named no register")
	})
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("ladder after a failed re-read", testDeliveryOps...)
	r.reopen()
	r.readAllFirst(1)
}

// TestRetirementStopsTheSegmentsOfASplitBatch: a batch whose third slot is occupied fails stale as
// the incarnation begins retiring. Neither the prefix (a recursive segment starting inside the
// failed attempt), nor the occupant's single ladder, nor the suffix starts.
func TestRetirementStopsTheSegmentsOfASplitBatch(t *testing.T) {
	r := newTestStopRig(t, "rs-split", 1)
	sess := r.ss
	var singles atomic.Int32
	mqttTestAdmissionBatchSingleHook.Set(func(s *mqttSession, _ string) {
		if s == sess && r.tr.Load() != nil {
			singles.Add(1)
		}
	})
	t.Cleanup(func() { mqttTestAdmissionBatchSingleHook.Set(nil) })
	r.hooks(func(op string, _ int, at *mqttAuthorityTransition) {
		if op != "op-admit-publishes" || r.tr.Load() != nil {
			return
		}
		var regs []string
		for _, m := range at.Extra {
			if name, err := mqttParseInflightSubject(m.subj); err == nil && name != nil && m.expect == 0 {
				regs = append(regs, m.subj)
			}
		}
		if len(regs) < 5 {
			return
		}
		testOccupyRegister(r, regs[2])
		r.closeTracker()
	}, nil)
	const n = 10
	r.backlog(n)
	r.waitClosed()
	r.expectNothingStarted("split batch", testDeliveryOps...)
	if s := singles.Load(); s != 0 {
		t.Fatalf("%d single ladder(s) were handed a delivery after the incarnation began retiring", s)
	}
	r.reopen()
	// The occupied identifier's delivery may be withheld behind its occupant; the others arrive.
	seen := map[string]bool{}
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < n-1 && time.Now().Before(deadline) {
		_, pi, _, payload := testMQTTGetPubMsgEx(t, r.sc, r.sr, "ab/t", nil)
		seen[string(payload)] = true
		testMQTTSendPIPacket(mqttPacketPubAck, t, r.sc, pi)
	}
	if len(seen) < n-1 {
		t.Fatalf("only %d messages arrived after the reopen", len(seen))
	}
}

// testOccupyRegister writes, on subject, a register naming another delivery of the session's
// durable (one that does not exist), between a batch's reads and its commit.
func testOccupyRegister(r *testStopRig, subject string) {
	sess := r.ss
	name, _ := mqttParseInflightSubject(subject)
	dur := _EMPTY_
	sess.mu.Lock()
	claim, claimSeq := sess.regClaim, sess.seq
	for _, cc := range sess.cons {
		if cc != nil && cc.Durable != _EMPTY_ {
			dur = cc.Durable
		}
	}
	sess.mu.Unlock()
	const foreignSeq = 999999
	nonce := mqttNewInflightNonce()
	occ := &mqttInflightRecord{PI: name.pi, Dur: dur, Seq: foreignSeq, QoS: 1, State: mqttInflightPublish,
		Nonce: nonce, Lin: nonce, Claim: claim, ClaimSeq: claimSeq,
		SourceAckReply: fmt.Sprintf("$JS.ACK.%s.%s.1.%d.1.0.0", mqttStreamName, dur, foreignSeq)}
	if verr := mqttValidateInflightShape(occ); verr != nil {
		r.fail("fixture: the occupying register is not a supported shape: %v", verr)
		return
	}
	body, _ := json.Marshal(occ)
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, err := sess.jsa.storeMsg(subject, hdr, msg); err != nil {
		r.fail("fixture: occupying %q: %v", subject, err)
	}
}

// TestRetirementStopsABatchAcquisitionAtItsNextAttempt: a flush's batched acquisition loses an
// attempt to contention as the incarnation begins retiring; no further acquisition starts, and
// nothing is transmitted.
func TestRetirementStopsABatchAcquisitionAtItsNextAttempt(t *testing.T) {
	r := newTestStopRig(t, "rs-acq-batch", 1)
	r.hooks(func(op string, _ int, _ *mqttAuthorityTransition) {
		if op == "op-acquire-publishes" && r.tr.Load() == nil {
			r.stale()
			r.closeTracker()
		}
	}, nil)
	const n = 8
	r.backlog(n)
	r.waitClosed()
	r.expectNothingStarted("batched acquisition", testDeliveryOps...)
	if d := r.transmits.Load() - r.sentAt.Load(); d != 0 {
		t.Fatalf("%d section(s) reached transmission after the incarnation began retiring", d)
	}
	r.reopen()
	r.readAllOnce(n, 30*time.Second)
}

// TestRetirementStopsASingleAcquisitionAtItsNextAttempt: one delivery's section acquires alone; its
// first attempt loses to contention as the incarnation begins retiring, and no second attempt is
// made.
func TestRetirementStopsASingleAcquisitionAtItsNextAttempt(t *testing.T) {
	r := newTestStopRig(t, "rs-acq-single", 1)
	r.hooks(func(op string, _ int, _ *mqttAuthorityTransition) {
		if op == "op-acquire-publish" && r.tr.Load() == nil {
			r.stale()
			r.closeTracker()
		}
	}, nil)
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("single acquisition", testDeliveryOps...)
	if d := r.transmits.Load() - r.sentAt.Load(); d != 0 {
		t.Fatalf("%d section(s) reached transmission after the incarnation began retiring", d)
	}
	r.reopen()
	r.readAllOnce(1, 30*time.Second)
}

// testHeldStop observes the callback stopped by testStopAtHeldOffers: how many of ITS offers began
// processing after the close, and, as it returned, how many of its unprocessed held offers were
// recorded withheld. Callbacks dispatched later are not it, whatever they process.
type testHeldStop struct {
	after    atomic.Int32
	withheld atomic.Int32
	done     chan struct{}
}

// testStopAtHeldOffers closes the tracker as the processing callback starts the first offer of a
// run of at least three held ones; reopen, when set, reopens it at once, inside the same callback.
func testStopAtHeldOffers(t *testing.T, r *testStopRig, reopen bool) *testHeldStop {
	st := &testHeldStop{done: make(chan struct{})}
	var mu sync.Mutex
	var lastHeld, stoppedSeqs map[uint64]bool
	var stoppedCol *mqttDeliveryCollector
	ours := func(sub *subscription) bool {
		return sub.client != nil && sub.client.mqtt != nil && sub.client.mqtt.sess == r.ss
	}
	mqttTestHeldOffersHook.Set(func(sub *subscription, _ int) {
		if !ours(sub) {
			return
		}
		// On the dispatch goroutine, so sub.held is this run's.
		m := map[uint64]bool{}
		for _, h := range sub.held {
			seq, _, _, _, _ := ackReplyInfo(h.reply)
			m[seq] = true
		}
		mu.Lock()
		lastHeld = m
		mu.Unlock()
	})
	mqttTestDeliveryOfferHook.Set(func(sub *subscription, reply string, col *mqttDeliveryCollector) {
		if !ours(sub) {
			return
		}
		seq, _, _, _, _ := ackReplyInfo(reply)
		mu.Lock()
		defer mu.Unlock()
		if stoppedCol != nil {
			if col == stoppedCol {
				st.after.Add(1)
			}
			return
		}
		if len(lastHeld) >= 3 && lastHeld[seq] {
			stoppedCol = col
			stoppedSeqs = lastHeld
			delete(stoppedSeqs, seq)
			r.closeTracker()
			if reopen {
				r.reopen()
			}
		}
	})
	mqttTestDeliveryCallbackDoneHook.Set(func(sub *subscription, col *mqttDeliveryCollector) {
		mu.Lock()
		stopped := stoppedCol != nil && col == stoppedCol
		mu.Unlock()
		if !stopped {
			return
		}
		r.ss.mu.Lock()
		for k := range r.ss.deferred {
			if stoppedSeqs[k.sseq] {
				st.withheld.Add(1)
			}
		}
		r.ss.mu.Unlock()
		close(st.done)
	})
	t.Cleanup(func() {
		mqttTestHeldOffersHook.Set(nil)
		mqttTestDeliveryOfferHook.Set(nil)
		mqttTestDeliveryCallbackDoneHook.Set(nil)
	})
	return st
}

func (st *testHeldStop) wait(t *testing.T) {
	t.Helper()
	select {
	case <-st.done:
	case <-time.After(15 * time.Second):
		t.Fatalf("fixture: the stopped callback never returned")
	}
}

// TestRetirementStopsTheCallbacksRemainingOffers: the callback processing a run of held offers stops
// before the next one once its incarnation begins retiring; the rest are recorded withheld.
func TestRetirementStopsTheCallbacksRemainingOffers(t *testing.T) {
	r := newTestStopRig(t, "rs-offers", 1)
	st := testStopAtHeldOffers(t, r, false)
	const n = 12
	r.backlog(n)
	st.wait(t)
	r.expectNothingStarted("held offers", testDeliveryOps...)
	if a := st.after.Load(); a != 0 {
		t.Fatalf("%d offer(s) of the stopped callback began processing after the incarnation began retiring", a)
	}
	if st.withheld.Load() == 0 {
		t.Fatalf("the stopped callback's unprocessed offers were not recorded withheld")
	}
	r.reopen()
	r.readAllOnce(n, 30*time.Second)
}

// TestRetirementStopStandsAcrossAReopen: the tracker is closed and at once reopened inside the
// processing callback. That callback's later offers still do not start — its lifecycle captured the
// closed cycle — and each is recorded withheld although the tracker is no longer closing.
// Callbacks dispatched after the reopen serve normally.
func TestRetirementStopStandsAcrossAReopen(t *testing.T) {
	r := newTestStopRig(t, "rs-reopen", 1)
	st := testStopAtHeldOffers(t, r, true)
	const n = 12
	r.backlog(n)
	st.wait(t)
	if a := st.after.Load(); a != 0 {
		t.Fatalf("%d offer(s) of the stopped callback began processing after the reopen", a)
	}
	if st.withheld.Load() == 0 {
		t.Fatalf("the stopped callback's unprocessed offers were not recorded withheld after the reopen")
	}
	r.readAllOnce(n, 30*time.Second)
}

// TestRetirementStopsACollectedDeliveryBeforeItsAdmission: one collected delivery reaches its flush
// as the incarnation begins retiring; no admission starts and it is abandoned.
func TestRetirementStopsACollectedDeliveryBeforeItsAdmission(t *testing.T) {
	r := newTestStopRig(t, "rs-flush", 1)
	mqttTestDeliveryFlushHook.Set(func(s *mqttSession, n int) {
		if s == r.ss && n == 1 {
			r.closeTracker()
		}
	})
	t.Cleanup(func() { mqttTestDeliveryFlushHook.Set(nil) })
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("collected delivery", testDeliveryOps...)
	if r.withheld() == 0 {
		t.Fatalf("the abandoned delivery was not recorded withheld")
	}
	r.reopen()
	r.readAllFirst(1)
}

// TestRetirementStopsAnAdmittedDeliveryBeforeItsAcquisition: the incarnation begins retiring once a
// delivery is admitted and before its section acquires; no acquisition starts.
func TestRetirementStopsAnAdmittedDeliveryBeforeItsAcquisition(t *testing.T) {
	r := newTestStopRig(t, "rs-admitted", 1)
	admitted := func(s *mqttSession, _ uint16) {
		if s == r.ss {
			r.closeTracker()
		}
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("admitted delivery", testDeliveryOps...)
	r.reopen()
	r.readAllOnce(1, 30*time.Second)
}

// TestRetirementStopsASingletonSectionBeforeItsTransmission: the incarnation begins retiring once a
// singleton section's acquisition has landed; nothing is transmitted, no intent recorded.
func TestRetirementStopsASingletonSectionBeforeItsTransmission(t *testing.T) {
	r := newTestStopRig(t, "rs-transmit", 1)
	r.hooks(nil, func(op string, _ int, _ *mqttAuthorityTransition, err error) {
		if op == "op-acquire-publish" && err == nil {
			r.closeTracker()
		}
	})
	r.publish(1, 0)
	r.waitClosed()
	time.Sleep(500 * time.Millisecond)
	if d := r.transmits.Load() - r.sentAt.Load(); d != 0 {
		t.Fatalf("%d section(s) reached transmission after the incarnation began retiring", d)
	}
	if _, _, ok := testMQTTReadPacketReady(r.sr, 200*time.Millisecond); ok {
		t.Fatalf("a packet was written after the incarnation began retiring")
	}
	r.reopen()
	r.readAllOnce(1, 30*time.Second)
}

// TestRetirementStopsAFallOutSectionAfterTheBatch: a flush's batched acquisition settles with one
// participant fallen out, and the incarnation begins retiring before the flush's writes; the
// fallen-out participant's single acquisition does not start.
func TestRetirementStopsAFallOutSectionAfterTheBatch(t *testing.T) {
	r := newTestStopRig(t, "rs-fallout", 1)
	sess := r.ss
	var calls atomic.Int32
	var foreign atomic.Value // the fallen-out participant's register subject
	admitted := func(s *mqttSession, pi uint16) {
		if s != sess || calls.Add(1) != 3 {
			return
		}
		sub := testRegisterSubjects(s, []uint16{pi})
		if len(sub) != 1 {
			r.fail("fixture: no register for %d", pi)
			return
		}
		cur, smsg, err := s.asm.loadInflightRecord(sub[0])
		if err != nil || cur == nil || smsg == nil {
			r.fail("fixture: reading %q: %v", sub[0], err)
			return
		}
		cur.ActionToken = mqttNewInflightNonce()
		cur.ActionClaim, cur.ActionClaimSeq = cur.Claim, cur.ClaimSeq
		if _, err := s.jsa.storeInflightCAS(sub[0], smsg.Sequence, cur); err != nil {
			r.fail("fixture: installing the foreign token: %v", err)
			return
		}
		foreign.Store(sub[0])
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	mqttTestAcquisitionBatchPauseHook.Set(func(s *mqttSession, pis []uint16) {
		subj, _ := foreign.Load().(string)
		if s != sess || subj == _EMPTY_ || r.tr.Load() != nil {
			return
		}
		// The other holder lets go, so the fallen-out participant's single acquisition would
		// commit if it started.
		cur, smsg, err := sess.asm.loadInflightRecord(subj)
		if err != nil || cur == nil || smsg == nil || cur.ActionToken == _EMPTY_ {
			return // not this flush's participant
		}
		cur.ActionToken, cur.ActionClaim, cur.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
		if _, err := sess.jsa.storeInflightCAS(subj, smsg.Sequence, cur); err != nil {
			r.fail("fixture: draining the foreign token: %v", err)
			return
		}
		r.closeTracker()
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchPauseHook.Set(nil) })
	const n = 16
	r.backlog(n)
	r.waitClosed()
	if got := r.startedAfterClose("op-acquire-publish"); len(got) != 0 {
		t.Fatalf("a fallen-out section acquired after the incarnation began retiring: %v", got)
	}
	r.reopen()
}

// TestRetirementStopsAnExpiredDeliverysDisposal: a flush holding an expired delivery behind live
// ones stops, once its incarnation begins retiring, before disposing of it: the expired delivery is
// abandoned unacknowledged, not disposed of.
func TestRetirementStopsAnExpiredDeliverysDisposal(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	ci := &mqttV5ConnInfo{clientID: "rs-expired", cleanStart: false, props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}
	c, rd := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rd)
	testMQTTSubV5(t, c, rd, 1, []mqttV5SubFilter{{topic: "ae/t", opts: 1}})
	sess := testMQTTGetClient(t, s, "rs-expired").mqtt.sess
	c.Close()
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("still attached")
		}
		return nil
	})
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rs-expired-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { cp.Close() })
	testMQTTReadConnAckV5(t, rp)
	// Stream sequences 1..4 live, 5 expiring (2 s), 6..8 live.
	for i := 0; i < 8; i++ {
		exp := uint32(60)
		if i == 4 {
			exp = 2
		}
		testMQTTPubV5Props(t, cp, rp, 1, false, uint16(i+1), "ae/t", []byte(fmt.Sprintf("m%d", i)), mqttV5PropsWithExpiry(exp))
	}
	var tr atomic.Pointer[mqttIncarnationTracker]
	var disposedWhileClosed atomic.Bool
	mqttTestSourceAckDropHook.Set(func(reply string) bool {
		if seq, _, _, _, _ := ackReplyInfo(reply); seq == 5 && tr.Load() != nil {
			disposedWhileClosed.Store(true)
		}
		return false
	})
	t.Cleanup(func() { mqttTestSourceAckDropHook.Set(nil) })
	var mu sync.Mutex
	var until time.Time
	mqttTestDeliveryFlushHook.Set(func(_ *mqttSession, n int) {
		mu.Lock()
		if until.IsZero() {
			until = time.Now().Add(2500 * time.Millisecond)
		}
		d := time.Until(until)
		mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
	})
	t.Cleanup(func() { mqttTestDeliveryFlushHook.Set(nil) })
	// The incarnation begins retiring as the last live delivery ahead of the expired one is admitted.
	admitted := func(s *mqttSession, pi uint16) {
		if s != sess || tr.Load() != nil {
			return
		}
		s.mu.Lock()
		ack := s.pendingPublish[pi]
		cc := s.c
		s.mu.Unlock()
		if ack == nil || ack.sseq != 4 || cc == nil {
			return
		}
		if tr.CompareAndSwap(nil, cc.mqtt.tracker) {
			cc.mqtt.tracker.close()
		}
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() {
		mqttTestDeliveryAdmittedPauseV.Store(nil)
		if t := tr.Load(); t != nil {
			t.reopen()
		}
	})
	c2, r2 := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c2.Close() })
	testMQTTReadConnAckV5(t, r2)
	checkFor(t, 15*time.Second, 5*time.Millisecond, func() error {
		if tr.Load() == nil {
			return fmt.Errorf("the fixture never reached delivery 4")
		}
		return nil
	})
	time.Sleep(800 * time.Millisecond)
	if disposedWhileClosed.Load() {
		t.Fatalf("the expired delivery was disposed of after the incarnation began retiring")
	}
}

// TestRetirementEndsAnExtendedAdmissionWhoseConnectionClosed: an admission extended past its budget
// whose connection closes during the extension's wait — the tracker untouched — makes no further
// attempt: the lifecycle is decided after the wait, before the write.
func TestRetirementEndsAnExtendedAdmissionWhoseConnectionClosed(t *testing.T) {
	rig := newTestAdmissionContentionRig(t, "rs-extclose")
	seen := rig.contendAdmissions(t, 1<<30, 0)
	var at atomic.Int32
	mqttTestAdmissionExtendedHook.Set(func(sess *mqttSession, _ int) {
		if sess != rig.sess || at.Load() != 0 {
			return
		}
		if c := testSessionConn(sess); c != nil {
			at.Store(seen.Load())
			c.closeConnection(ClientClosed)
		}
	})
	rig.publish(t, 0)
	checkFor(t, 15*time.Second, 5*time.Millisecond, func() error {
		if at.Load() == 0 {
			return fmt.Errorf("the admission was never extended")
		}
		return nil
	})
	time.Sleep(700 * time.Millisecond)
	if d := seen.Load() - at.Load(); d != 0 {
		t.Fatalf("%d attempt(s) were written after the connection closed during the extension's wait", d)
	}
}

// testSharedStopRig is a persistent subscriber on "$share/g/ab/t" at qos, and a publisher.
func testSharedStopRig(t *testing.T, id string, qos byte) *testStopRig {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &testBatchRig{t: t, s: s, o: o, id: id, qos: qos}
	ci := &mqttV5ConnInfo{clientID: id, cleanStart: false, props: mqttV5PropsSessionExpiryReceiveMax(300, 0)}
	rig.sc, rig.sr = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	sc := rig.sc
	t.Cleanup(func() { sc.Close() })
	testMQTTReadConnAckV5(t, rig.sr)
	testMQTTSubV5(t, rig.sc, rig.sr, 1, []mqttV5SubFilter{{topic: "$share/g/ab/t", opts: qos}})
	rig.pc, rig.pr = testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.pc.Close() })
	testMQTTCheckConnAck(t, rig.pr, mqttConnAckRCConnectionAccepted, false)
	return testStopRigOver(t, rig)
}

// testSharedReadOnce reads one v5 PUBLISH carrying want on the shared rig, acknowledging it when
// ack is set.
func testSharedReadOnce(t *testing.T, r *testStopRig, want string, ack bool) {
	t.Helper()
	payload, pi, _ := testBatchReadPublishV5(t, r.sr)
	if string(payload) != want {
		t.Fatalf("received %q, want %q", payload, want)
	}
	if ack {
		testMQTTSendPIPacket(mqttPacketPubAck, t, r.sc, pi)
	}
}

// TestRetirementStopsASharedDeliveryBeforeItsAdmission: a shared delivery is admitted outside the
// collector; once its incarnation begins retiring as its processing starts, no admission starts.
func TestRetirementStopsASharedDeliveryBeforeItsAdmission(t *testing.T) {
	r := testSharedStopRig(t, "rs-shared-admit", 1)
	mqttTestDeliveryOfferHook.Set(func(sub *subscription, _ string, _ *mqttDeliveryCollector) {
		if sub.client != nil && sub.client.mqtt != nil && sub.client.mqtt.sess == r.ss {
			r.closeTracker()
		}
	})
	t.Cleanup(func() { mqttTestDeliveryOfferHook.Set(nil) })
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("shared admission", testDeliveryOps...)
	r.reopen()
	mqttTestDeliveryOfferHook.Set(nil)
	testSharedReadOnce(t, r, "m0", true)
}

// TestRetirementStopsASharedDeliveryBeforeItsAcquisition: a shared delivery's admission lands and
// the incarnation begins retiring before its single-route section acquires; no acquisition starts.
func TestRetirementStopsASharedDeliveryBeforeItsAcquisition(t *testing.T) {
	r := testSharedStopRig(t, "rs-shared-acq", 1)
	admitted := func(s *mqttSession, _ uint16) {
		if s == r.ss {
			r.closeTracker()
		}
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("shared acquisition", testAcquireOps...)
	r.reopen()
}

// TestRetirementStopsASharedQoS0Transmission: a shared QoS 0 member's delivery is about to be
// written when its incarnation begins retiring; nothing is written and the source is left for the
// group to re-offer.
func TestRetirementStopsASharedQoS0Transmission(t *testing.T) {
	r := testSharedStopRig(t, "rs-shared-q0", 0)
	out := func(string, string) { r.closeTracker() }
	mqttTestSharedQoS0OutputHook.Store(&out)
	t.Cleanup(func() { mqttTestSharedQoS0OutputHook.Store(nil) })
	r.publish(1, 0)
	r.waitClosed()
	if _, _, ok := testMQTTReadPacketReady(r.sr, 700*time.Millisecond); ok {
		t.Fatalf("a shared QoS 0 PUBLISH was written after the incarnation began retiring")
	}
	mqttTestSharedQoS0OutputHook.Store(nil)
	r.reopen()
	testSharedReadOnce(t, r, "m0", false)
}

// TestRetirementStopKeepsARetransmissionsEvidence: an unanswered delivery's AckWait re-offer is a
// retransmission; stopped before it is written, it records no intent, and its next transmission
// still carries DUP.
func TestRetirementStopKeepsARetransmissionsEvidence(t *testing.T) {
	r := newTestStopRig(t, "rs-retx", 1)
	r.publish(1, 0)
	testMQTTCheckPubMsgNoAck(t, r.sc, r.sr, "ab/t", 2, []byte("m0")) // left unanswered
	r.hooks(nil, func(op string, _ int, _ *mqttAuthorityTransition, err error) {
		if op == "op-acquire-publish" && err == nil {
			r.closeTracker()
		}
	})
	r.waitClosed()
	time.Sleep(500 * time.Millisecond)
	if d := r.transmits.Load() - r.sentAt.Load(); d != 0 {
		t.Fatalf("the stopped retransmission reached its transmission")
	}
	r.hooks(nil, nil)
	r.reopen()
	flags, pi, _, payload := testMQTTGetPubMsgEx(t, r.sc, r.sr, "ab/t", nil)
	if string(payload) != "m0" || flags&mqttPubFlagDup == 0 {
		t.Fatalf("the retransmission after the stop arrived as %q with flags %x, want m0 with DUP", payload, flags)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, r.sc, pi)
}

// testLoseResponse makes the first commit of op land and report its response lost, closing the
// tracker in between; with unresolved set, the custody settlement of that attempt's receipt is
// forced to stay UNRESOLVED (its resolution and its abort both), as a store that answers neither
// leaves it.
func testLoseResponse(t *testing.T, r *testStopRig, op string, unresolved bool) {
	var lost atomic.Bool
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt == nil || at.Attempt.ID != r.ss.id {
			return send()
		}
		r.mu.Lock()
		r.ops[at.Attempt.Op]++
		r.mu.Unlock()
		if at.Attempt.Op == op && lost.CompareAndSwap(false, true) {
			seq, err := send()
			r.closeTracker()
			if err != nil {
				return seq, err
			}
			return 0, errors.New("fixture: the response was lost")
		}
		return send()
	}
	r.ss.jsa.testTransitionCommit.Store(&commit)
	if !unresolved {
		return
	}
	var handle atomic.Pointer[mqttTransitionHandle]
	var forced atomic.Int32
	as := r.ss.asm
	as.mu.Lock()
	as.testCustodyResolve = func(h *mqttTransitionHandle, _ string) (mqttTransitionVerdict, bool) {
		if r.tr.Load() == nil {
			return 0, false
		}
		if handle.Load() == nil {
			handle.Store(h)
		}
		if h == handle.Load() && forced.Add(1) <= 2 {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testCustodyResolve = nil
		as.mu.Unlock()
	})
}

// TestRetirementStopLeavesAnUnresolvedBatchToItsContinuity: a batch whose receipt can be neither
// resolved nor aborted as the incarnation begins retiring concludes nothing: it is not re-attempted
// or split, its participants keep their identifiers excluded and their sources held by continuity
// records, and every message still arrives, none twice as a first transmission, once the receipt
// is decided after the reopen.
func TestRetirementStopLeavesAnUnresolvedBatchToItsContinuity(t *testing.T) {
	r := newTestStopRig(t, "rs-unresolved", 1)
	verdicts := make(chan mqttTransitionVerdict, 64)
	mqttTestAdmissionBatchSettledHook.Set(func(s *mqttSession, _ []string, v mqttTransitionVerdict) {
		if s == r.ss {
			select {
			case verdicts <- v:
			default:
			}
		}
	})
	t.Cleanup(func() { mqttTestAdmissionBatchSettledHook.Set(nil) })
	testLoseResponse(t, r, "op-admit-publishes", true)
	const n = 8
	r.backlog(n)
	r.waitClosed()
	r.expectNothingStarted("unresolved batch", testDeliveryOps...)
	sawUnresolved := false
	for len(verdicts) > 0 {
		if <-verdicts == mqttTransitionUnresolved {
			sawUnresolved = true
		}
	}
	if !sawUnresolved {
		t.Fatalf("fixture: the lost batch was not settled UNRESOLVED")
	}
	r.ss.mu.Lock()
	continuity := len(r.ss.admissionContinuity)
	r.ss.mu.Unlock()
	if continuity == 0 {
		t.Fatalf("the unresolved batch's participants hold no continuity record")
	}
	r.reopen()
	r.readAllOnce(n, 60*time.Second)
}

// TestRetirementStopLeavesAnAmbiguousAcquisitionToItsResolver: a single acquisition lands but its
// response is lost as the incarnation begins retiring; its resolver decides what landed: nothing is
// transmitted, no further acquisition starts, the landed token is drained, and the message arrives
// once after the reopen.
func TestRetirementStopLeavesAnAmbiguousAcquisitionToItsResolver(t *testing.T) {
	r := newTestStopRig(t, "rs-ambiguous", 1)
	var subj atomic.Value
	admitted := func(s *mqttSession, pi uint16) {
		if s == r.ss {
			if sub := testRegisterSubjects(s, []uint16{pi}); len(sub) == 1 {
				subj.Store(sub[0])
			}
		}
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	testLoseResponse(t, r, "op-acquire-publish", false)
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("ambiguous acquisition", testAcquireOps...)
	if d := r.transmits.Load() - r.sentAt.Load(); d != 0 {
		t.Fatalf("%d section(s) reached transmission after the incarnation began retiring", d)
	}
	s, _ := subj.Load().(string)
	if s == _EMPTY_ {
		t.Fatalf("fixture: the delivery's register was never named")
	}
	testNoTokenOn(t, r.ss, []string{s}, 10*time.Second)
	r.reopen()
	r.readAllOnce(1, 30*time.Second)
}

// TestRetirementStopLeavesACancelledAllocationReserved: an allocation cancelled while its first
// admission attempt is out, as the incarnation begins retiring, is not freed by the stop: the
// ladder's give-up meets the cancellation and keeps the identifier reserved or excluded until the
// store's truth settles it.
func TestRetirementStopLeavesACancelledAllocationReserved(t *testing.T) {
	r := newTestStopRig(t, "rs-cancelled", 1)
	var pi atomic.Int32
	r.hooks(func(op string, _ int, at *mqttAuthorityTransition) {
		if op != "op-admit-publish" || r.tr.Load() != nil {
			return
		}
		var name *mqttInflightName
		for _, m := range at.Extra {
			if nm, err := mqttParseInflightSubject(m.subj); err == nil && nm != nil && m.expect == 0 {
				name = nm
				break
			}
		}
		if name == nil {
			r.fail("fixture: the attempt named no register")
			return
		}
		pi.Store(int32(name.pi))
		r.stale()
		c := testSessionConn(r.ss)
		r.ss.mu.Lock()
		if ack := r.ss.pendingPublish[name.pi]; ack != nil {
			r.ss.cancelPendingRegistrationsLocked(ack.jsDur, mqttLicenceOf(c))
		}
		cancelled := r.ss.pendingPublish[name.pi] != nil && r.ss.pendingPublish[name.pi].regCancelled
		r.ss.mu.Unlock()
		if !cancelled {
			r.fail("fixture: the allocation of identifier %d was not cancelled", name.pi)
		}
		r.closeTracker()
	}, nil)
	r.publish(1, 0)
	r.waitClosed()
	r.expectNothingStarted("cancelled allocation", testDeliveryOps...)
	r.ss.mu.Lock()
	ack := r.ss.pendingPublish[uint16(pi.Load())]
	excluded := r.ss.sendsInFlight[uint16(pi.Load())] != nil
	r.ss.mu.Unlock()
	if ack == nil && !excluded {
		t.Fatalf("the cancelled allocation's identifier %d was freed by the stop", pi.Load())
	}
	r.reopen()
	r.readAllOnce(1, 30*time.Second)
}

// TestRetirementStopsADirectExpiryDisposal: a shared delivery whose Message Expiry has passed reaches
// its processing as the incarnation begins retiring; it is neither disposed of nor acknowledged.
func TestRetirementStopsADirectExpiryDisposal(t *testing.T) {
	r := testSharedStopRig(t, "rs-shared-expiry", 1)
	var disposed atomic.Bool
	mqttTestSourceAckDropHook.Set(func(string) bool {
		if r.tr.Load() != nil {
			disposed.Store(true)
		}
		return false
	})
	t.Cleanup(func() { mqttTestSourceAckDropHook.Set(nil) })
	mqttTestDeliveryOfferHook.Set(func(sub *subscription, _ string, _ *mqttDeliveryCollector) {
		if sub.client != nil && sub.client.mqtt != nil && sub.client.mqtt.sess == r.ss && r.tr.Load() == nil {
			time.Sleep(1500 * time.Millisecond) // the 1 s Message Expiry passes
			r.closeTracker()
		}
	})
	t.Cleanup(func() { mqttTestDeliveryOfferHook.Set(nil) })
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "rs-shared-expiry-pub5", cleanStart: true}, r.o.MQTT.Host, r.o.MQTT.Port)
	t.Cleanup(func() { cp.Close() })
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, false, 1, "ab/t", []byte("m0"), mqttV5PropsWithExpiry(1))
	r.waitClosed()
	r.expectNothingStarted("direct expiry", testDeliveryOps...)
	if disposed.Load() {
		t.Fatalf("the expired delivery was disposed of after the incarnation began retiring")
	}
	r.reopen()
}

// TestRetirementLetsALandedBatchSettle: a batch lands and its first register's read-back fails as the
// incarnation begins retiring. Settling what landed is not fresh work: the fallback ladder still
// makes its first attempt, which adopts the landed register (its entry is kept, not abandoned);
// nothing after it starts.
func TestRetirementLetsALandedBatchSettle(t *testing.T) {
	r := newTestStopRig(t, "rs-readback", 1)
	var armed, failed atomic.Value
	as := r.ss.asm
	as.mu.Lock()
	as.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		if s, _ := armed.Load().(string); s != _EMPTY_ && s == subject {
			armed.Store(_EMPTY_)
			failed.Store(subject)
			r.closeTracker()
			return nil, nil, fmt.Errorf("fixture: the read-back failed")
		}
		return as.jsa.loadInflightRecord(subject)
	}
	as.mu.Unlock()
	t.Cleanup(func() {
		as.mu.Lock()
		as.testInflightLoad = nil
		as.mu.Unlock()
	})
	var once sync.Once
	r.hooks(nil, func(op string, _ int, at *mqttAuthorityTransition, err error) {
		if op != "op-admit-publishes" || err != nil {
			return
		}
		once.Do(func() {
			for _, m := range at.Extra {
				if name, perr := mqttParseInflightSubject(m.subj); perr == nil && name != nil && m.expect == 0 {
					armed.Store(m.subj)
					return
				}
			}
			r.fail("fixture: the batch named no register")
		})
	})
	const n = 8
	r.backlog(n)
	r.waitClosed()
	if got := r.startedAfterClose(testAcquireOps...); len(got) != 0 {
		t.Fatalf("an acquisition started after the incarnation began retiring: %v", got)
	}
	subj, _ := failed.Load().(string)
	name, err := mqttParseInflightSubject(subj)
	if err != nil || name == nil {
		t.Fatalf("fixture: %q names no register: %v", subj, err)
	}
	r.ss.mu.Lock()
	ack := r.ss.pendingPublish[name.pi]
	adopted := ack != nil && !ack.regPending
	r.ss.mu.Unlock()
	if !adopted {
		t.Fatalf("the landed delivery whose read-back failed was abandoned instead of adopted (entry %+v)", ack)
	}
	r.reopen()
	r.readAllOnce(n, 30*time.Second)
}

// TestDeliveryOfferStartsNoWorkAfterAStoppingFlush is the census of mqttDeliverQoS12Offer's flushes:
// each runs what were earlier callbacks before batching, so after each one the offer either
// returns, or tests whether the callback's lifecycle still lasts before any fresh work of its own,
// or does only settlement of a decision it already made (an acknowledgement of a declined offer, a
// withheld record, an abandonment, a log line, a test seam). The collector's plain flush, which
// reports nothing, does not appear at all. The chapter P affine move is the one exception.
func TestDeliveryOfferStartsNoWorkAfterAStoppingFlush(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "publish_deliver.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "mqttDeliverQoS12Offer" && fd.Recv == nil {
			body = fd.Body
		}
	}
	if body == nil {
		t.Fatal("mqttDeliverQoS12Offer is not in publish_deliver.go")
	}
	settlement := map[string]bool{
		"submitSourceAck": true, "recordWithheldDelivery": true, "recordWithheldStopped": true,
		"abandonInflightRegister": true, "mqttTestRestoreGateBlockedHook": true,
		"Warnf": true, "Errorf": true, "Debugf": true, "Noticef": true,
	}
	callName := func(e ast.Expr) string {
		c, ok := e.(*ast.CallExpr)
		if !ok {
			return ""
		}
		switch fn := c.Fun.(type) {
		case *ast.SelectorExpr:
			return fn.Sel.Name
		case *ast.Ident:
			return fn.Name
		}
		return ""
	}
	isFlush := func(e ast.Expr) bool {
		n := callName(e)
		return n == "unlockAndFlush" || n == "flushLasts"
	}
	var settles func(s ast.Stmt) bool
	settles = func(s ast.Stmt) bool {
		switch st := s.(type) {
		case *ast.ReturnStmt:
			return true
		case *ast.ExprStmt:
			return settlement[callName(st.X)]
		case *ast.AssignStmt:
			return len(st.Rhs) == 1 && settlement[callName(st.Rhs[0])]
		case *ast.IfStmt:
			// A test seam (if h := mqttTest....Get(); h != nil { ... }) or a settlement-only branch.
			if as, ok := st.Init.(*ast.AssignStmt); ok && len(as.Rhs) == 1 {
				if sel, ok := as.Rhs[0].(*ast.CallExpr); ok {
					if s2, ok := sel.Fun.(*ast.SelectorExpr); ok {
						if x, ok := s2.X.(*ast.Ident); ok && strings.HasPrefix(x.Name, "mqttTest") {
							return true
						}
					}
				}
			}
			for _, b := range st.Body.List {
				if !settles(b) {
					return false
				}
			}
			if st.Else != nil {
				return settles(st.Else)
			}
			return true
		case *ast.BlockStmt:
			for _, b := range st.List {
				if !settles(b) {
					return false
				}
			}
			return true
		}
		return false
	}
	affine := func(rest []ast.Stmt) bool {
		found := false
		for _, s := range rest {
			ast.Inspect(s, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok && callName(c) == "moveStartedSharedQoS2" {
					found = true
				}
				return true
			})
		}
		return found
	}
	// guards reports whether cond makes its body run only when the flush reported the lifecycle
	// over: !flush(), or a conjunction with !flush() as a conjunct.
	var guards func(cond ast.Expr) bool
	guards = func(cond ast.Expr) bool {
		switch c := cond.(type) {
		case *ast.UnaryExpr:
			return c.Op == token.NOT && isFlush(c.X)
		case *ast.BinaryExpr:
			return c.Op == token.LAND && (guards(c.X) || guards(c.Y))
		case *ast.ParenExpr:
			return guards(c.X)
		}
		return false
	}
	// controls reports whether cond runs its body only while the named result says the lifecycle
	// lasts.
	controls := func(cond ast.Expr, name string) bool {
		var walk func(e ast.Expr) bool
		walk = func(e ast.Expr) bool {
			switch c := e.(type) {
			case *ast.Ident:
				return c.Name == name
			case *ast.BinaryExpr:
				return c.Op == token.LAND && (walk(c.X) || walk(c.Y))
			case *ast.ParenExpr:
				return walk(c.X)
			}
			return false
		}
		return walk(cond)
	}
	var problems []string
	sites := 0
	report := func(n ast.Node, why string) {
		problems = append(problems, fmt.Sprintf("%s: %s", fset.Position(n.Pos()), why))
	}
	var visitBlock func(list []ast.Stmt)
	visitBlock = func(list []ast.Stmt) {
		for i, s := range list {
			rest := list[i+1:]
			switch st := s.(type) {
			case *ast.CaseClause:
				visitBlock(st.Body)
				continue
			case *ast.CommClause:
				visitBlock(st.Body)
				continue
			case *ast.ExprStmt:
				if isFlush(st.X) {
					sites++
					ok := true
					for _, r := range rest {
						if !settles(r) {
							ok = false
							break
						}
						if _, isRet := r.(*ast.ReturnStmt); isRet {
							break
						}
					}
					if !ok && !affine(rest) {
						report(st, "fresh work follows a flush without a lifecycle check")
					}
				}
			case *ast.AssignStmt:
				if len(st.Rhs) == 1 && isFlush(st.Rhs[0]) && len(st.Lhs) == 1 {
					sites++
					id, _ := st.Lhs[0].(*ast.Ident)
					next, _ := func() (*ast.IfStmt, bool) {
						if len(rest) == 0 {
							return nil, false
						}
						n, ok := rest[0].(*ast.IfStmt)
						return n, ok
					}()
					if id == nil || next == nil || !controls(next.Cond, id.Name) {
						report(st, "a flush's result is not the condition of the step after it")
					} else {
						for _, r := range rest[1:] {
							if !settles(r) {
								report(r, "fresh work follows a lifecycle-controlled step without its own check")
								break
							}
							if _, isRet := r.(*ast.ReturnStmt); isRet {
								break
							}
						}
					}
				}
			case *ast.IfStmt:
				if guards(st.Cond) {
					sites++
					stmts := st.Body.List
					if len(stmts) == 0 {
						report(st, "a stopping flush's branch is empty")
					} else if _, isRet := stmts[len(stmts)-1].(*ast.ReturnStmt); !isRet {
						report(st, "a stopping flush's branch does not return")
					} else {
						for _, b := range stmts {
							if !settles(b) {
								report(b, "a stopping flush's branch starts fresh work")
							}
						}
					}
				}
			}
			// Descend into nested blocks.
			ast.Inspect(s, func(n ast.Node) bool {
				if n == s {
					return true
				}
				switch b := n.(type) {
				case *ast.BlockStmt:
					visitBlock(b.List)
					return false
				case *ast.CaseClause:
					visitBlock(b.Body)
					return false
				case *ast.FuncLit:
					return false
				}
				return true
			})
		}
	}
	visitBlock(body.List)
	// Every flush call or method value is one of the sites above, or the gate's own flush.
	calls, gateValues := 0, 0
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if isFlush(x) {
				calls++
			}
			if callName(x) == "awaitTransmissionGatesThen" {
				for _, a := range x.Args {
					if sel, ok := a.(*ast.SelectorExpr); ok && sel.Sel.Name == "flushLasts" {
						gateValues++
					}
				}
			}
		case *ast.SelectorExpr:
			if id, ok := x.X.(*ast.Ident); ok && id.Name == "col" && x.Sel.Name == "flush" {
				report(x, "the collector's plain flush reports no lifecycle")
			}
		}
		return true
	})
	if calls != sites {
		report(body, fmt.Sprintf("%d flush calls, %d classified sites", calls, sites))
	}
	if gateValues != 1 {
		report(body, fmt.Sprintf("the transmission gate is handed flushLasts %d times, want 1", gateValues))
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// testContinuationStop stages, in one processing callback, a fresh delivery A that is collected and
// then an offer B whose branch flushes A before doing work of its own; the tracker closes inside A's
// admission, which runs in B's flush. It counts the offer continuations that start after the close.
type testContinuationStop struct {
	mu       sync.Mutex
	sites    map[string]int
	bReply   string
	done     chan struct{}
	closedAt time.Time
	doneAt   time.Time
}

func (st *testContinuationStop) started(site string) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.sites[site]
}

// testStopAfterPrecedingFlush backlogs three messages and, on the second offer a callback processes,
// calls inject (sess.mu held) to put B in its branch; undo (sess.mu held) removes it once that
// callback returns.
func testStopAfterPrecedingFlush(t *testing.T, r *testStopRig, inject, undo func(sub *subscription, sseq uint64, reply string)) *testContinuationStop {
	st := &testContinuationStop{sites: map[string]int{}, done: make(chan struct{})}
	var mu sync.Mutex
	var cur, staged *mqttDeliveryCollector
	var n int
	var bSub *subscription
	var bSeq uint64
	ours := func(sub *subscription) bool {
		return sub.client != nil && sub.client.mqtt != nil && sub.client.mqtt.sess == r.ss
	}
	mqttTestDeliveryOfferHook.Set(func(sub *subscription, reply string, col *mqttDeliveryCollector) {
		if !ours(sub) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if staged != nil {
			return
		}
		if col != cur {
			cur, n = col, 0
		}
		n++
		if n != 2 {
			return
		}
		staged, bSub = col, sub
		bSeq, _, _, _, _ = ackReplyInfo(reply)
		st.mu.Lock()
		st.bReply = reply
		st.mu.Unlock()
		r.ss.mu.Lock()
		inject(sub, bSeq, reply)
		r.ss.mu.Unlock()
	})
	mqttTestOfferContinuationHook.Set(func(site string) {
		if r.tr.Load() == nil {
			return
		}
		st.mu.Lock()
		st.sites[site]++
		st.mu.Unlock()
	})
	mqttTestDeliveryCallbackDoneHook.Set(func(sub *subscription, col *mqttDeliveryCollector) {
		mu.Lock()
		mine := staged != nil && col == staged
		mu.Unlock()
		if !mine {
			return
		}
		st.mu.Lock()
		st.doneAt = time.Now()
		st.mu.Unlock()
		r.ss.mu.Lock()
		undo(bSub, bSeq, st.bReply)
		r.ss.mu.Unlock()
		close(st.done)
	})
	r.hooks(func(op string, _ int, _ *mqttAuthorityTransition) {
		mu.Lock()
		ready := staged != nil
		mu.Unlock()
		if ready && (op == "op-admit-publish" || op == "op-admit-publishes") && r.tr.Load() == nil {
			st.mu.Lock()
			st.closedAt = time.Now()
			st.mu.Unlock()
			if r.closer != nil {
				r.closer()
			} else {
				r.closeTracker()
			}
		}
	}, nil)
	t.Cleanup(func() {
		mqttTestDeliveryOfferHook.Set(nil)
		mqttTestOfferContinuationHook.Set(nil)
		mqttTestDeliveryCallbackDoneHook.Set(nil)
	})
	r.backlog(3)
	select {
	case <-st.done:
	case <-time.After(20 * time.Second):
		t.Fatalf("fixture: no callback processed a collected delivery and then a second offer")
	}
	if r.tr.Load() == nil {
		t.Fatalf("fixture: the preceding flush made no admission attempt, so the tracker never closed")
	}
	return st
}

// testStoppedContinuation runs one continuation witness: site must not start once the flush ahead
// of it ended the lifecycle, and every message arrives after the reopen.
func testStoppedContinuation(t *testing.T, id, site string, inject, undo func(r *testStopRig, sub *subscription, sseq uint64, reply string)) {
	r := newTestStopRig(t, id, 1)
	st := testStopAfterPrecedingFlush(t, r,
		func(sub *subscription, sseq uint64, reply string) { inject(r, sub, sseq, reply) },
		func(sub *subscription, sseq uint64, reply string) { undo(r, sub, sseq, reply) })
	time.Sleep(300 * time.Millisecond)
	if n := st.started(site); n != 0 {
		t.Fatalf("the offer's %s continuation started %d time(s) after the flush ahead of it ended the lifecycle", site, n)
	}
	r.reopen()
	r.readAllOnce(3, 30*time.Second)
}

// TestRetirementStopsATerminalSourcesContinuation: an offer of a source a terminal owner holds stops
// after the flush ahead of it ends the lifecycle; the owner's own continuation stands.
func TestRetirementStopsATerminalSourcesContinuation(t *testing.T) {
	testStoppedContinuation(t, "rs-terminal", "terminal",
		func(r *testStopRig, sub *subscription, sseq uint64, _ string) {
			if r.ss.terminalSources == nil {
				r.ss.terminalSources = make(map[mqttTerminalSourceKey]*mqttTerminalSource)
			}
			r.ss.terminalSources[mqttTerminalSourceKey{dur: sub.mqtt.jsDur, sseq: sseq}] = &mqttTerminalSource{
				ref: mqttInflightSlotRef{name: mqttInflightName{gen: r.ss.gen}}, epoch: r.ss.asm.sessStreamEpoch}
		},
		func(r *testStopRig, sub *subscription, sseq uint64, _ string) {
			delete(r.ss.terminalSources, mqttTerminalSourceKey{dur: sub.mqtt.jsDur, sseq: sseq})
		})
}

// TestRetirementStopsADrainResolutionRetry: an unclaimed drained offer whose removal is unconfirmed
// does not start its resolution retry after the flush ahead of it ends the lifecycle.
func TestRetirementStopsADrainResolutionRetry(t *testing.T) {
	testStoppedContinuation(t, "rs-drain-resolve", "drain-resolve",
		func(r *testStopRig, sub *subscription, sseq uint64, _ string) {
			if r.ss.drainByDur == nil {
				r.ss.drainByDur = make(map[string]*mqttDrain)
			}
			r.ss.drainByDur[sub.mqtt.jsDur] = &mqttDrain{seqs: map[uint64]*drainSeq{sseq: {unclaimUnconfirmed: true}}}
		},
		func(r *testStopRig, sub *subscription, _ uint64, _ string) {
			delete(r.ss.drainByDur, sub.mqtt.jsDur)
		})
}

// TestRetirementStopsADrainFinalization: a drained offer whose PUBREL is confirmed does not start its
// finalization after the flush ahead of it ends the lifecycle; the claim stands.
func TestRetirementStopsADrainFinalization(t *testing.T) {
	testStoppedContinuation(t, "rs-drain-finalize", "drain-finalize",
		func(r *testStopRig, sub *subscription, sseq uint64, _ string) {
			if r.ss.drainByDur == nil {
				r.ss.drainByDur = make(map[string]*mqttDrain)
			}
			r.ss.drainByDur[sub.mqtt.jsDur] = &mqttDrain{seqs: map[uint64]*drainSeq{sseq: {claimed: true, phase: drainPubRelConfirmed}}}
		},
		func(r *testStopRig, sub *subscription, _ uint64, _ string) {
			delete(r.ss.drainByDur, sub.mqtt.jsDur)
		})
}

// TestRetirementStopsADeniedReoffersRelease: an offer the connection is now denied does not start its
// release, acknowledgement or hand-back after the flush ahead of it ends the lifecycle.
func TestRetirementStopsADeniedReoffersRelease(t *testing.T) {
	var deny atomic.Bool
	mqttTestDenySubVerdictHook.Set(func(*client, string) (mqttSubDeliveryVerdict, bool) {
		if deny.Load() {
			return mqttSubDeliverDenied, true
		}
		return 0, false
	})
	t.Cleanup(func() { mqttTestDenySubVerdictHook.Set(nil) })
	testStoppedContinuation(t, "rs-denied", "denied",
		func(*testStopRig, *subscription, uint64, string) { deny.Store(true) },
		func(*testStopRig, *subscription, uint64, string) { deny.Store(false) })
}

// TestRetirementStopsARetiredSourcesSettlement: an offer of a source whose exchange already retired it
// does not start that settlement after the flush ahead of it ends the lifecycle.
func TestRetirementStopsARetiredSourcesSettlement(t *testing.T) {
	testStoppedContinuation(t, "rs-retired", "retired",
		func(r *testStopRig, sub *subscription, sseq uint64, _ string) {
			if r.ss.srcAckOwed == nil {
				r.ss.srcAckOwed = make(map[mqttDeferredKey]*mqttRetiredSource)
			}
			r.ss.srcAckOwed[mqttDeferredKey{sub.mqtt.jsDur, sseq}] = &mqttRetiredSource{}
		},
		func(r *testStopRig, sub *subscription, sseq uint64, _ string) {
			delete(r.ss.srcAckOwed, mqttDeferredKey{sub.mqtt.jsDur, sseq})
		})
}

// TestRetirementStopStillSettlesADeclinedDrainOffer: the settlement of a decision already made runs
// after a stopping flush: an unclaimed offer of a durable that is draining, not staged, is still
// acknowledged, and no resolution retry starts.
func TestRetirementStopStillSettlesADeclinedDrainOffer(t *testing.T) {
	var acked sync.Map
	mqttTestSourceAckDropHook.Set(func(reply string) bool {
		acked.Store(reply, true)
		return false
	})
	t.Cleanup(func() { mqttTestSourceAckDropHook.Set(nil) })
	r := newTestStopRig(t, "rs-drain-settle", 1)
	st := testStopAfterPrecedingFlush(t, r,
		func(sub *subscription, _ uint64, _ string) {
			if r.ss.drainByDur == nil {
				r.ss.drainByDur = make(map[string]*mqttDrain)
			}
			r.ss.drainByDur[sub.mqtt.jsDur] = &mqttDrain{seqs: map[uint64]*drainSeq{}}
		},
		func(sub *subscription, _ uint64, _ string) {
			delete(r.ss.drainByDur, sub.mqtt.jsDur)
		})
	st.mu.Lock()
	b := st.bReply
	st.mu.Unlock()
	if _, ok := acked.Load(b); !ok {
		t.Fatalf("the declined drained offer was not acknowledged after the stopping flush")
	}
	if n := st.started("drain-resolve"); n != 0 {
		t.Fatalf("a resolution retry started after the stopping flush")
	}
	r.reopen()
}

// testStopAtAPhaseGate stages B behind a phase gate that never opens, with a 10 s AckWait (the
// gate's wait cap), and ends the lifecycle inside the flush ahead of it with closer. It returns how
// long B's callback ran after the close and whether B was recorded withheld as it returned.
func testStopAtAPhaseGate(t *testing.T, id string, closer func(r *testStopRig)) (time.Duration, bool) {
	r := newTestStopRigAckWait(t, id, 1, 10*time.Second)
	r.closer = func() { closer(r) }
	var withheld bool
	st := testStopAfterPrecedingFlush(t, r,
		func(sub *subscription, _ uint64, _ string) { sub.mqtt.phaseGate = make(chan struct{}) },
		func(sub *subscription, sseq uint64, _ string) {
			sub.mqtt.phaseGate = nil
			_, withheld = r.ss.deferred[mqttDeferredKey{sub.mqtt.jsDur, sseq}]
		})
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.doneAt.Sub(st.closedAt), withheld
}

// TestRetirementStopsAPhaseGateWaitAfterAStoppingFlush: an offer about to wait on a resume phase gate
// does not wait once the flush ahead of it ends the lifecycle by closing the connection (no tracker
// close, so only the lifecycle check, not the closing channel, can stop it); it is recorded
// withheld.
func TestRetirementStopsAPhaseGateWaitAfterAStoppingFlush(t *testing.T) {
	ran, withheld := testStopAtAPhaseGate(t, "rs-phase-conn", func(r *testStopRig) {
		c := testSessionConn(r.ss)
		if c == nil {
			r.fail("fixture: no connection to close")
			return
		}
		r.tr.CompareAndSwap(nil, c.mqtt.tracker) // marks the stop; the tracker stays open
		c.closeConnection(ClientClosed)
	})
	if ran > 3*time.Second {
		t.Fatalf("the offer waited on its phase gate for %v after its connection closed", ran)
	}
	if !withheld {
		t.Fatalf("the offer stopped before its phase gate was not recorded withheld")
	}
}

// TestRetirementPhaseGateWaitUsesTheCallbacksCycle: the tracker closes and at once reopens inside the
// flush ahead of an offer that would wait on a phase gate; the offer does not wait on the new
// cycle, and is recorded withheld.
func TestRetirementPhaseGateWaitUsesTheCallbacksCycle(t *testing.T) {
	ran, withheld := testStopAtAPhaseGate(t, "rs-phase-reopen", func(r *testStopRig) {
		r.closeTracker()
		r.reopen()
	})
	if ran > 3*time.Second {
		t.Fatalf("the offer waited on its phase gate for %v after its callback's cycle closed", ran)
	}
	if !withheld {
		t.Fatalf("the offer stopped before its phase gate was not recorded withheld after the reopen")
	}
}

// TestRetirementPhaseGateWaitIsOnTheCallbacksCycle: the tracker closes and at once reopens after an
// offer's lifecycle check allowed its phase-gate wait and before the wait begins. The wait is on the
// callback's own closing channel, the closed one, so it ends at once and the offer is recorded
// withheld; a wait on the tracker's current channel would last the whole gate cap.
func TestRetirementPhaseGateWaitIsOnTheCallbacksCycle(t *testing.T) {
	r := newTestStopRigAckWait(t, "rs-phase-wait", 1, 10*time.Second)
	ours := func(sub *subscription) bool {
		return sub.client != nil && sub.client.mqtt != nil && sub.client.mqtt.sess == r.ss
	}
	var mu sync.Mutex
	var staged *mqttDeliveryCollector
	var closedAt, doneAt time.Time
	var withheld bool
	done := make(chan struct{})
	mqttTestDeliveryOfferHook.Set(func(sub *subscription, _ string, col *mqttDeliveryCollector) {
		if !ours(sub) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if staged == nil {
			staged = col
			r.ss.mu.Lock()
			sub.mqtt.phaseGate = make(chan struct{})
			r.ss.mu.Unlock()
		}
	})
	mqttTestPhaseGateWaitHook.Set(func(sub *subscription) {
		if !ours(sub) || r.tr.Load() != nil {
			return
		}
		mu.Lock()
		closedAt = time.Now()
		mu.Unlock()
		r.closeTracker()
		r.reopen()
	})
	mqttTestDeliveryCallbackDoneHook.Set(func(sub *subscription, col *mqttDeliveryCollector) {
		mu.Lock()
		mine := staged != nil && col == staged
		if mine {
			doneAt = time.Now()
		}
		mu.Unlock()
		if !mine {
			return
		}
		r.ss.mu.Lock()
		sub.mqtt.phaseGate = nil
		for k := range r.ss.deferred {
			if k.jsDur == sub.mqtt.jsDur {
				withheld = true
			}
		}
		r.ss.mu.Unlock()
		close(done)
	})
	t.Cleanup(func() {
		mqttTestDeliveryOfferHook.Set(nil)
		mqttTestPhaseGateWaitHook.Set(nil)
		mqttTestDeliveryCallbackDoneHook.Set(nil)
	})
	r.publish(1, 0)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("fixture: the offer's callback never returned")
	}
	mu.Lock()
	ran, reached := doneAt.Sub(closedAt), !closedAt.IsZero()
	mu.Unlock()
	if !reached {
		t.Fatalf("fixture: the offer never reached its phase-gate wait")
	}
	if ran > 3*time.Second {
		t.Fatalf("the phase-gate wait lasted %v after its callback's cycle closed and the tracker reopened", ran)
	}
	if !withheld {
		t.Fatalf("the offer stopped at its phase gate was not recorded withheld")
	}
}
