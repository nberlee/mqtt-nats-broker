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
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A flush's admitted deliveries are acquired in one transition, then written and ended one at a
// time in offer order.

const testAcquireOp = "op-acquire-publishes"

// testAcqAttempt is one settled batch attempt: its participants and its verdict.
type testAcqAttempt struct {
	pis []uint16
	v   mqttTransitionVerdict
}

// testAcqRecorder observes a session's batched acquisitions: every settled attempt, every flush's
// participants as the pause hook sees them, and an ordered event log tests append to.
type testAcqRecorder struct {
	mu       sync.Mutex
	attempts []testAcqAttempt
	flushes  [][]uint16
	events   []string
	errs     []string
}

func testRecordAcquisitions(t *testing.T, sess *mqttSession) *testAcqRecorder {
	r := &testAcqRecorder{}
	mqttTestAcquisitionBatchHook.Set(func(s *mqttSession, pis []uint16, v mqttTransitionVerdict) {
		if s != sess {
			return
		}
		r.mu.Lock()
		r.attempts = append(r.attempts, testAcqAttempt{pis: append([]uint16(nil), pis...), v: v})
		r.mu.Unlock()
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchHook.Set(nil) })
	return r
}

func (r *testAcqRecorder) event(e string) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *testAcqRecorder) fail(format string, args ...any) {
	r.mu.Lock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

// check reports every failure a hook recorded. Hooks run on broker goroutines, so they record
// rather than fail the test themselves.
func (r *testAcqRecorder) check(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.errs {
		t.Error(e)
	}
}

// landedMulti counts the LANDED or SUPERSEDED attempts that carried more than one participant.
func (r *testAcqRecorder) landedMulti() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, a := range r.attempts {
		if len(a.pis) > 1 && (a.v == mqttTransitionLanded || a.v == mqttTransitionSuperseded) {
			n++
		}
	}
	return n
}

// testOnAcquireBatchCommit wraps every committed batched acquisition of the session; n counts
// them from 1.
func testOnAcquireBatchCommit(t *testing.T, sess *mqttSession,
	wrap func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error)) {
	var n atomic.Int32
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt == nil || at.Attempt.ID != sess.id || at.Attempt.Op != testAcquireOp {
			return send()
		}
		return wrap(n.Add(1), at, send)
	}
	sess.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { sess.jsa.testTransitionCommit.Store(nil) })
}

// testOnAcquireBatchPause runs fn at every flush's pause (acquisition settled, nothing written) and
// records the flush's participants.
func testOnAcquireBatchPause(t *testing.T, sess *mqttSession, r *testAcqRecorder, fn func(pis []uint16)) {
	mqttTestAcquisitionBatchPauseHook.Set(func(s *mqttSession, pis []uint16) {
		if s != sess {
			return
		}
		r.mu.Lock()
		r.flushes = append(r.flushes, append([]uint16(nil), pis...))
		r.mu.Unlock()
		if fn != nil {
			fn(pis)
		}
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchPauseHook.Set(nil) })
}

// testBacklogSharingConn is rig.backlog, publishing the reconnected subscriber's connection
// through conn as soon as it is dialled, so a broker-side hook can close it without reading the
// rig's fields.
func testBacklogSharingConn(rig *testBatchRig, n int, conn *atomic.Pointer[net.Conn]) {
	rig.t.Helper()
	rig.detach()
	for i := 0; i < n; i++ {
		rig.publish(rig.qos, i)
	}
	sc, sr := testMQTTConnect(rig.t, &mqttConnInfo{clientID: rig.id, cleanSess: false}, rig.o.MQTT.Host, rig.o.MQTT.Port)
	conn.Store(&sc)
	rig.t.Cleanup(func() { sc.Close() })
	testMQTTCheckConnAck(rig.t, sr, mqttConnAckRCConnectionAccepted, true)
	rig.sc, rig.sr = sc, sr
}

// testCloseShared closes the connection published through conn, waiting a bounded time for it.
func testCloseShared(conn *atomic.Pointer[net.Conn]) {
	deadline := time.Now().Add(10 * time.Second)
	for conn.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c := conn.Load(); c != nil {
		(*c).Close()
	}
}

// testSessionConn is the session's current connection, read under its lock.
func testSessionConn(sess *mqttSession) *client {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.c
}

// testRegisterSubjects names the registers of the given identifiers, as the session's entries do.
func testRegisterSubjects(sess *mqttSession, pis []uint16) []string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	var out []string
	for _, pi := range pis {
		if ack := sess.pendingPublish[pi]; ack != nil && ack.slot.name.subject != _EMPTY_ {
			out = append(out, ack.slot.name.subject)
		}
	}
	return out
}

// testNoTokenOn waits until no named register carries an output-custody token.
func testNoTokenOn(t *testing.T, sess *mqttSession, subjects []string, within time.Duration) {
	t.Helper()
	checkFor(t, within, 20*time.Millisecond, func() error {
		for _, s := range subjects {
			cur, _, err := sess.asm.loadInflightRecord(s)
			if err != nil {
				return fmt.Errorf("register %q unreadable: %v", s, err)
			}
			if cur != nil && cur.ActionToken != _EMPTY_ {
				return fmt.Errorf("register %q still carries token %q", s, cur.ActionToken)
			}
		}
		return nil
	})
}

// testFlushWith reports the first recorded flush with more than one participant.
func (r *testAcqRecorder) multiFlush() []uint16 {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range r.flushes {
		if len(f) > 1 {
			return f
		}
	}
	return nil
}

// TestAcquisitionBatchAcquiresAFlushTogether: a backlog's admitted deliveries are acquired several
// per committed transition; each participant's entry learns its register's own sequence before
// any section writes; every message reaches the client once, in publish order; and no register
// keeps a token afterwards.
func TestAcquisitionBatchAcquiresAFlushTogether(t *testing.T) {
	rig := newTestBatchRig(t, "acq-flush", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var commits atomic.Int32
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt != nil && at.Attempt.ID == sess.id && at.Attempt.Kind == mqttTransitionAcquireAction {
			commits.Add(1)
		}
		return send()
	}
	sess.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { sess.jsa.testTransitionCommit.Store(nil) })
	var subjects sync.Map
	mqttTestAcquisitionBatchAdoptedHook.Set(func(s *mqttSession, pi uint16, tok *mqttSendToken) {
		if s != sess {
			return
		}
		subjects.Store(tok.slot.name.subject, true)
		_, smsg, err := s.asm.loadInflightRecord(tok.slot.name.subject)
		if err != nil || smsg == nil {
			rec.fail("participant %d: register unreadable at adoption: %v", pi, err)
			return
		}
		s.mu.Lock()
		ack := s.pendingPublish[pi]
		var entrySeq uint64
		if ack != nil {
			entrySeq = ack.slot.seq
		}
		s.mu.Unlock()
		if tok.slot.seq != smsg.Sequence || entrySeq != smsg.Sequence {
			rec.fail("participant %d adopted sequence %d (entry %d), its register stands at %d",
				pi, tok.slot.seq, entrySeq, smsg.Sequence)
		}
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchAdoptedHook.Set(nil) })
	const n = 40
	rig.backlog(n)
	rig.readInOrder(n)
	if _, _, ok := testMQTTReadPacketReady(rig.sr, 300*time.Millisecond); ok {
		t.Fatalf("a packet followed the %d deliveries", n)
	}
	rec.check(t)
	if rec.landedMulti() == 0 {
		t.Fatalf("no committed acquisition carried more than one delivery (%d attempts)", len(rec.attempts))
	}
	if c := commits.Load(); c >= n {
		t.Fatalf("%d acquire-action commits for %d deliveries: nothing was acquired together", c, n)
	}
	var subs []string
	subjects.Range(func(k, _ any) bool {
		subs = append(subs, k.(string))
		return true
	})
	testNoTokenOn(t, sess, subs, 10*time.Second)
}

// TestAcquisitionBatchEndsEachSectionBeforeTheNextWrite: every participant of a flush is adopted
// before its first write, and each section's drain runs after its own write and before the next
// participant's write.
func TestAcquisitionBatchEndsEachSectionBeforeTheNextWrite(t *testing.T) {
	rig := newTestBatchRig(t, "acq-order", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) { rec.event("pause") })
	mqttTestAcquisitionBatchAdoptedHook.Set(func(s *mqttSession, pi uint16, _ *mqttSendToken) {
		if s == sess {
			rec.event(fmt.Sprintf("a%d", pi))
		}
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchAdoptedHook.Set(nil) })
	wrote := func(s *mqttSession, pi uint16, window string) bool {
		if s == sess && window == mqttMarkerSendWrote {
			rec.event(fmt.Sprintf("w%d", pi))
		}
		return true
	}
	mqttTestMarkerSendWindowHookV.Store(&wrote)
	t.Cleanup(func() { mqttTestMarkerSendWindowHookV.Store(nil) })
	drained := func(pi uint16) { rec.event(fmt.Sprintf("d%d", pi)) }
	mqttTestPublishDrainedPauseV.Store(&drained)
	t.Cleanup(func() { mqttTestPublishDrainedPauseV.Store(nil) })
	const n = 24
	rig.backlog(n)
	rig.readInOrder(n)
	f := rec.multiFlush()
	if f == nil {
		t.Fatalf("fixture: no flush had more than one participant")
	}
	rec.mu.Lock()
	evs := append([]string(nil), rec.events...)
	rec.mu.Unlock()
	// The events of the first multi-participant flush: from its pause to its last drain.
	start := -1
	for i, e := range evs {
		if e == "pause" && i+1 < len(evs) {
			if evs[i+1] == fmt.Sprintf("w%d", f[0]) {
				start = i
				break
			}
		}
	}
	if start < 0 {
		t.Fatalf("the first multi-participant flush did not write its first participant right after its pause: %v", evs)
	}
	for _, pi := range f {
		found := false
		for _, e := range evs[:start] {
			if e == fmt.Sprintf("a%d", pi) {
				found = true
			}
		}
		if !found {
			t.Fatalf("participant %d was not adopted before the flush's first write: %v", pi, evs)
		}
	}
	want := make([]string, 0, 2*len(f))
	for _, pi := range f {
		want = append(want, fmt.Sprintf("w%d", pi), fmt.Sprintf("d%d", pi))
	}
	got := evs[start+1:]
	if len(got) < len(want) {
		t.Fatalf("the flush's events %v, want %v", got, want)
	}
	if strings.Join(got[:len(want)], ",") != strings.Join(want, ",") {
		t.Fatalf("the flush's events %v, want each write followed by its own drain: %v", got[:len(want)], want)
	}
}

// TestAcquisitionBatchBoundaryPairsEveryParticipant: the write boundary judges a multi-pair
// acquisition per acquisition: a batch with one token member missing, or one action record
// missing, is refused whole; the intact batch then lands.
func TestAcquisitionBatchBoundaryPairsEveryParticipant(t *testing.T) {
	rig := newTestBatchRig(t, "acq-pairs", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var checked atomic.Bool
	testOnAcquireBatchCommit(t, sess, func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if checked.Load() {
			return send()
		}
		var acts, toks []int
		for i, e := range at.Extra {
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttActionClassTk {
				acts = append(acts, i)
			} else if _, perr := mqttParseInflightSubject(e.subj); perr == nil {
				toks = append(toks, i)
			}
		}
		if len(acts) < 2 || len(toks) != len(acts) {
			rec.fail("the batch carries %d actions and %d token members", len(acts), len(toks))
			return send()
		}
		checked.Store(true)
		orig := at.Extra
		without := func(skip int) []mqttTransitionMember {
			var out []mqttTransitionMember
			for i, e := range orig {
				if i != skip {
					out = append(out, e)
				}
			}
			return out
		}
		at.Extra = without(toks[0])
		if _, err := send(); err == nil || !strings.Contains(err.Error(), "no output-custody token") {
			rec.fail("a batch missing one token member was not refused by the pairing rule: %v", err)
		}
		at.Extra = without(acts[0])
		if _, err := send(); err == nil || !strings.Contains(err.Error(), "no action record") {
			rec.fail("a batch missing one action record was not refused by the pairing rule: %v", err)
		}
		at.Extra = orig
		return send()
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	rec.check(t)
	if !checked.Load() {
		t.Fatalf("fixture: no batched acquisition was committed")
	}
}

// TestAcquisitionBatchRefusesTwoActionsForOneExchange: the boundary refuses a transition that
// acquires two actions for one exchange, judged directly on a real batch's action member.
func TestAcquisitionBatchRefusesTwoActionsForOneExchange(t *testing.T) {
	rig := newTestBatchRig(t, "acq-dup", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var checked atomic.Bool
	testOnAcquireBatchCommit(t, sess, func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if checked.Swap(true) {
			return send()
		}
		var act *mqttTransitionMember
		for i, e := range at.Extra {
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttActionClassTk {
				act = &at.Extra[i]
				break
			}
		}
		if act == nil {
			rec.fail("the batch carries no action member")
			return send()
		}
		toks := strings.Split(act.subj, ".")
		acq := toks[len(toks)-1]
		second := *act
		second.subj = strings.TrimSuffix(act.subj, acq) + acq + "0"
		err := sess.jsa.checkActionMembers(at.Attempt, []mqttTransitionMember{*act, second})
		if err == nil || !strings.Contains(err.Error(), "acquires two actions for exchange") {
			rec.fail("two actions for one exchange were not refused as such: %v", err)
		}
		return send()
	})
	const n = 8
	rig.backlog(n)
	rig.readInOrder(n)
	rec.check(t)
	if !checked.Load() {
		t.Fatalf("fixture: no batched acquisition was committed")
	}
}

// TestAcquisitionBatchComposesEveryParticipant: opAcquireActions composes one action record and
// one token member per participant, each pair naming the same acquisition, and once the batch
// lands every participant's register carries its section's token.
func TestAcquisitionBatchComposesEveryParticipant(t *testing.T) {
	rig := newTestBatchRig(t, "acq-compose", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var checked atomic.Bool
	testOnAcquireBatchCommit(t, sess, func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if checked.Swap(true) {
			return send()
		}
		acts := map[string]bool{}
		toks := map[string]bool{}
		for _, e := range at.Extra {
			if cls, ok := mqttF105ObligationClass(e.subj); ok && cls == mqttActionClassTk {
				parts := strings.Split(e.subj, ".")
				acts[parts[len(parts)-2]+"."+parts[len(parts)-1]] = true
				continue
			}
			if _, perr := mqttParseInflightSubject(e.subj); perr != nil {
				continue
			}
			r, derr := mqttDecodeInflightRecord(e.body)
			if derr != nil || r.ActionToken == _EMPTY_ {
				rec.fail("a register member of the acquisition installs no token: %v", derr)
				continue
			}
			toks[r.Lin+"."+fmt.Sprint(r.Acq)] = true
		}
		if len(acts) < 2 || len(acts) != len(toks) {
			rec.fail("the batch composes %d actions and %d token members", len(acts), len(toks))
		}
		for k := range acts {
			if !toks[k] {
				rec.fail("action %s has no token member naming the same acquisition", k)
			}
		}
		return send()
	})
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		if len(pis) < 2 {
			return
		}
		for _, s := range testRegisterSubjects(sess, pis) {
			cur, _, err := sess.asm.loadInflightRecord(s)
			if err != nil || cur == nil || cur.ActionToken == _EMPTY_ {
				rec.fail("participant register %q carries no token after the batch (%v)", s, err)
			}
		}
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	rec.check(t)
	if !checked.Load() || rec.multiFlush() == nil {
		t.Fatalf("fixture: no multi-participant batch was committed")
	}
}

// TestAcquisitionBatchLostResponseIsSettledByItsReceipt: a batch that lands and loses its response
// is settled LANDED by its receipt, and every participant is adopted and written in that flush,
// once, in order.
func TestAcquisitionBatchLostResponseIsSettledByItsReceipt(t *testing.T) {
	rig := newTestBatchRig(t, "acq-lost", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	testOnAcquireBatchCommit(t, sess, func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if n != 1 {
			return send()
		}
		if _, err := send(); err != nil {
			return 0, err
		}
		return 0, errors.New("the batch's response was lost")
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.attempts) == 0 || rec.attempts[0].v != mqttTransitionLanded || len(rec.attempts[0].pis) < 2 {
		t.Fatalf("the lost batch settled %+v, want a multi-participant landing", rec.attempts)
	}
}

// TestAcquisitionBatchUnreadableLandingIsDrainedNotSent: a landed batch whose participants'
// read-backs fail sends none of them; each one's token is drained by its section's end while the
// connection is still open; every message still arrives once.
func TestAcquisitionBatchUnreadableLandingIsDrainedNotSent(t *testing.T) {
	rig := newTestBatchRig(t, "acq-readback", 1, func(o *Options) { o.MQTT.AckWait = time.Second })
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var target sync.Map // pi -> subject, for the first multi-participant batch
	var armed atomic.Bool
	mqttTestAcquisitionBatchReadBackHook.Set(func(s *mqttSession, pi uint16) bool {
		if s != sess {
			return true
		}
		if _, ok := target.Load(pi); ok {
			return false
		}
		if armed.CompareAndSwap(false, true) {
			for _, sub := range testRegisterSubjects(s, []uint16{pi}) {
				target.Store(pi, sub)
			}
			return false
		}
		return true
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchReadBackHook.Set(nil) })
	var checkedDrain atomic.Int32
	drained := func(pi uint16) {
		v, ok := target.Load(pi)
		if !ok {
			return
		}
		if c := testSessionConn(sess); c == nil || c.isClosed() {
			rec.fail("participant %d was drained only after its connection closed", pi)
		}
		cur, _, err := sess.asm.loadInflightRecord(v.(string))
		if err != nil || (cur != nil && cur.ActionToken != _EMPTY_) {
			rec.fail("participant %d still carries a token after its section ended (%v)", pi, err)
		}
		checkedDrain.Add(1)
	}
	mqttTestPublishDrainedPauseV.Store(&drained)
	t.Cleanup(func() { mqttTestPublishDrainedPauseV.Store(nil) })
	const n = 12
	rig.backlog(n)
	rig.readAllOnce(n, 30*time.Second)
	rec.check(t)
	if checkedDrain.Load() == 0 {
		t.Fatalf("fixture: no participant's read-back was failed")
	}
}

// TestAcquisitionBatchDelayedLandingCannotOutliveTheAbort: a batch whose request is still in flight
// when its caller times out is settled by the create-only abort, so the request, arriving late,
// cannot land; the re-attempt lands and every participant is written in that flush.
func TestAcquisitionBatchDelayedLandingCannotOutliveTheAbort(t *testing.T) {
	rig := newTestBatchRig(t, "acq-delayed", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var late func() (uint64, error)
	var lateErr atomic.Value
	testOnAcquireBatchCommit(t, sess, func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		switch n {
		case 1:
			late = send // submitted, not yet applied
			return 0, errors.New("the batch request timed out")
		case 2:
			// THE LATE ARRIVAL, after the first attempt was settled and before the re-attempt.
			if _, err := late(); err == nil {
				lateErr.Store("landed")
			} else {
				lateErr.Store("refused")
			}
		}
		return send()
	})
	const n = 12
	rig.backlog(n)
	rig.readInOrder(n)
	if v, _ := lateErr.Load().(string); v != "refused" {
		t.Fatalf("the late request %q, want refused by the abort", v)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.attempts) < 2 || rec.attempts[0].v != mqttTransitionNeverLanded || rec.attempts[1].v != mqttTransitionLanded {
		t.Fatalf("the attempts settled %+v, want never landed then landed", rec.attempts)
	}
}

// TestAcquisitionBatchUnresolvedAttemptIsSettledBeforeItsTokensAreReleased: an attempt whose
// receipt can be neither resolved nor aborted sends nothing; its sections end, but the owed
// settlement keeps the attempt; when it lands afterwards, the settlement decides it and drains
// every token while the connection is still open, and every message still arrives once.
func TestAcquisitionBatchUnresolvedAttemptIsSettledBeforeItsTokensAreReleased(t *testing.T) {
	rig := newTestBatchRig(t, "acq-unres", 1, func(o *Options) { o.MQTT.AckWait = time.Second })
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var late func() (uint64, error)
	var subjects []string
	testOnAcquireBatchCommit(t, sess, func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if n == 1 {
			late = send
			for _, e := range at.Extra {
				if _, perr := mqttParseInflightSubject(e.subj); perr == nil {
					subjects = append(subjects, e.subj)
				}
			}
			return 0, errors.New("the batch request timed out")
		}
		return send()
	})
	var blocked atomic.Bool
	blocked.Store(true)
	sess.asm.mu.Lock()
	sess.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if blocked.Load() && h.attempt.Op == testAcquireOp {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	sess.asm.mu.Unlock()
	t.Cleanup(func() {
		sess.asm.mu.Lock()
		sess.asm.testCustodyResolve = nil
		sess.asm.mu.Unlock()
	})
	unresolved := make(chan struct{})
	var once sync.Once
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		rec.mu.Lock()
		last := rec.attempts[len(rec.attempts)-1]
		rec.mu.Unlock()
		if last.v == mqttTransitionUnresolved {
			once.Do(func() { close(unresolved) })
		}
	})
	// THE LATE LANDING, after the unresolved flush ended its sections and before this
	// subscription writes anything else: at the start of its next flush.
	landed := make(chan error, 1)
	var lateOnce sync.Once
	mqttTestDeliveryFlushHook.Set(func(s *mqttSession, _ int) {
		if s != sess {
			return
		}
		select {
		case <-unresolved:
		default:
			return
		}
		lateOnce.Do(func() {
			sess.mu.Lock()
			owed := len(sess.acquireSettleOwed)
			sess.mu.Unlock()
			if owed != 1 {
				landed <- fmt.Errorf("%d owed acquisition settlements when the sections ended, want 1", owed)
				return
			}
			_, err := late()
			landed <- err
		})
	})
	t.Cleanup(func() { mqttTestDeliveryFlushHook.Set(nil) })
	const n = 24
	rig.backlog(n)
	select {
	case err := <-landed:
		if err != nil {
			t.Fatalf("the late request: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("fixture: no unresolved flush was followed by another")
	}
	blocked.Store(false)
	sess.mu.Lock()
	sess.kickCustodyItemsLocked()
	sess.mu.Unlock()
	if c := testSessionConn(sess); c == nil || c.isClosed() {
		t.Fatalf("fixture: the connection closed")
	}
	testNoTokenOn(t, sess, subjects, 20*time.Second)
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if len(sess.acquireSettleOwed) != 0 {
			return fmt.Errorf("%d owed acquisition settlements remain", len(sess.acquireSettleOwed))
		}
		return nil
	})
	rig.readAllOnce(n, 60*time.Second)
}

// TestAcquisitionBatchContentionReattemptsTheRemainingParticipants: a batch refused because the
// authority moved under it, while one participant's register gained another holder's token, is
// re-attempted at once with every other participant and lands in the same flush; the participant
// held by the other holder is delivered once that holder lets go.
func TestAcquisitionBatchContentionReattemptsTheRemainingParticipants(t *testing.T) {
	rig := newTestBatchRig(t, "acq-contend", 1, func(o *Options) { o.MQTT.AckWait = time.Second })
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var foreign atomic.Value // subject the foreign token was installed on
	var foreignTok string
	var staged atomic.Bool
	testOnAcquireBatchCommit(t, sess, func(n int32, at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if staged.Load() {
			return send()
		}
		var regs []string
		for _, e := range at.Extra {
			if _, perr := mqttParseInflightSubject(e.subj); perr == nil {
				regs = append(regs, e.subj)
			}
		}
		if len(regs) < 3 {
			return send() // the contention is staged on the first batch of three or more
		}
		staged.Store(true)
		subj := regs[1]
		cur, smsg, err := sess.asm.loadInflightRecord(subj)
		if err != nil || cur == nil || smsg == nil {
			rec.fail("fixture: reading %q: %v", subj, err)
			return send()
		}
		foreignTok = mqttNewInflightNonce()
		cur.ActionToken = foreignTok
		cur.ActionClaim, cur.ActionClaimSeq = cur.Claim, cur.ClaimSeq
		if _, err := sess.jsa.storeInflightCAS(subj, smsg.Sequence, cur); err != nil {
			rec.fail("fixture: installing the foreign token: %v", err)
		}
		foreign.Store(subj)
		if err := testBumpSessionAuthority(sess); err != nil {
			rec.fail("fixture: revising the authority: %v", err)
		}
		return send()
	})
	var t0 atomic.Int64
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		subj, _ := foreign.Load().(string)
		if subj == _EMPTY_ || t0.Load() != 0 {
			return
		}
		t0.Store(time.Now().UnixNano())
		// THE OTHER HOLDER LETS GO, after this flush settled.
		cur, smsg, err := sess.asm.loadInflightRecord(subj)
		if err != nil || cur == nil || smsg == nil || cur.ActionToken != foreignTok {
			rec.fail("fixture: the foreign token is gone before its holder let go (%v)", err)
			return
		}
		cur.ActionToken, cur.ActionClaim, cur.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
		if _, err := sess.jsa.storeInflightCAS(subj, smsg.Sequence, cur); err != nil {
			rec.fail("fixture: draining the foreign token: %v", err)
		}
	})
	const n = 12
	start := time.Now()
	rig.backlog(n)
	rig.readAllOnce(n, 30*time.Second)
	rec.check(t)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	first := -1
	for i, at := range rec.attempts {
		if at.v == mqttTransitionNeverLanded {
			first = i
			break
		}
	}
	if !staged.Load() || first < 0 {
		t.Fatalf("fixture: no contended batch of three or more (attempts %+v)", rec.attempts)
	}
	if first+1 >= len(rec.attempts) {
		t.Fatalf("the contended batch was not re-attempted (attempts %+v)", rec.attempts)
	}
	a, b := rec.attempts[first], rec.attempts[first+1]
	if a.v != mqttTransitionNeverLanded || b.v != mqttTransitionLanded || len(b.pis) != len(a.pis)-1 {
		t.Fatalf("the contended batch (%d participants, %v) was followed by %d participants (%v), "+
			"want the others re-attempted and landed", len(a.pis), a.v, len(b.pis), b.v)
	}
	if at := t0.Load(); at == 0 || time.Duration(at-start.UnixNano()) > 2*time.Second+5*time.Second {
		t.Fatalf("the re-attempt did not land within the flush")
	}
}

// TestAcquisitionBatchSealedOutputEndsEveryRemainingSection: when the connection closes after a
// flush's first write, no later participant is written or records its intent, every later section
// is ended (no local hold remains), and every message arrives after the client reconnects.
func TestAcquisitionBatchSealedOutputEndsEveryRemainingSection(t *testing.T) {
	rig := newTestBatchRig(t, "acq-sealed", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var flush atomic.Value // []uint16 of the first multi-participant flush
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		if len(pis) > 1 {
			flush.CompareAndSwap(nil, pis)
		}
	})
	var closed atomic.Bool
	var intents sync.Map
	var shared atomic.Pointer[net.Conn]
	window := func(s *mqttSession, pi uint16, w string) bool {
		f, _ := flush.Load().([]uint16)
		if s != sess || len(f) == 0 {
			return true
		}
		if closed.Load() && w == mqttMarkerSendIntent {
			intents.Store(pi, true)
		}
		if w == mqttMarkerSendWrote && pi == f[0] && closed.CompareAndSwap(false, true) {
			c := testSessionConn(s)
			testCloseShared(&shared)
			deadline := time.Now().Add(10 * time.Second)
			for c != nil && c.outBind.mayWrite() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if c == nil || c.outBind.mayWrite() {
				rec.fail("fixture: the closed connection's output was never sealed")
			}
		}
		return true
	}
	mqttTestMarkerSendWindowHookV.Store(&window)
	t.Cleanup(func() { mqttTestMarkerSendWindowHookV.Store(nil) })
	const n = 16
	testBacklogSharingConn(rig, n, &shared)
	checkFor(t, 20*time.Second, 10*time.Millisecond, func() error {
		if !closed.Load() {
			return fmt.Errorf("not yet closed")
		}
		return nil
	})
	f := flush.Load().([]uint16)
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		for _, pi := range f[1:] {
			if sess.sendsInFlight[pi] != nil {
				return fmt.Errorf("participant %d still holds its section", pi)
			}
		}
		return nil
	})
	intents.Range(func(k, _ any) bool {
		t.Errorf("participant %v recorded its intent after the output was sealed", k)
		return true
	})
	rec.check(t)
	rig.connectSubscriber(true)
	rig.readAllOnce(n, 60*time.Second)
}

// TestAcquisitionBatchDisplacedBrokerRecordsNoLaterIntent: broker A's flush has acquired its
// participants when the client resumes on broker B. A records the intent of none of them and
// writes nothing; every message reaches the client on B once as a first transmission.
func TestAcquisitionBatchDisplacedBrokerRecordsNoLaterIntent(t *testing.T) {
	const n = 16
	p := newTestBatchPeers(t, "acq-peers", n)
	held, release := make(chan struct{}), make(chan struct{})
	var once, rel sync.Once
	var sessA atomic.Pointer[mqttSession]
	mqttTestAcquisitionBatchPauseHook.Set(func(s *mqttSession, pis []uint16) {
		if s.id != p.cid || len(pis) < 2 || (sessA.Load() != nil && sessA.Load() != s) {
			return
		}
		sessA.CompareAndSwap(nil, s)
		once.Do(func() {
			close(held)
			<-release
		})
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchPauseHook.Set(nil) })
	var intents atomic.Int32
	window := func(s *mqttSession, pi uint16, w string) bool {
		if s == sessA.Load() && w == mqttMarkerSendIntent {
			select {
			case <-held:
				intents.Add(1)
			default:
			}
		}
		return true
	}
	mqttTestMarkerSendWindowHookV.Store(&window)
	t.Cleanup(func() { mqttTestMarkerSendWindowHookV.Store(nil) })
	t.Cleanup(func() { rel.Do(func() { close(release) }) })
	cA, rA := testMQTTConnect(t, &mqttConnInfo{clientID: p.cid, cleanSess: false}, p.oA.MQTT.Host, p.oA.MQTT.Port)
	t.Cleanup(func() { cA.Close() })
	testMQTTCheckConnAck(t, rA, mqttConnAckRCConnectionAccepted, true)
	select {
	case <-held:
	case <-time.After(15 * time.Second):
		t.Fatal("fixture: A never acquired a multi-participant flush")
	}
	cB, rB := p.takeOver(t, func() { rel.Do(func() { close(release) }) })
	testBatchReadAllOnceOn(t, cB, rB, n, 60*time.Second)
	if k := intents.Load(); k != 0 {
		t.Fatalf("the displaced broker recorded %d intents after the takeover", k)
	}
	if _, _, ok := testMQTTReadPacketReady(rA, 500*time.Millisecond); ok {
		t.Fatalf("the displaced connection on A received a packet")
	}
}

// TestAcquisitionBatchRefusedForAnEndedEntitlement: a batch about to commit when the client resumes
// on another broker is refused and never re-composed under the successor's tenure: no batched
// acquisition of the displaced broker lands, and every message reaches the client on B once.
func TestAcquisitionBatchRefusedForAnEndedEntitlement(t *testing.T) {
	const n = 16
	p := newTestBatchPeers(t, "acq-ended", n)
	held, release := make(chan struct{}), make(chan struct{})
	var once, rel sync.Once
	var sessA atomic.Pointer[mqttSession]
	var landedA atomic.Int32
	mqttTestAcquisitionBatchHook.Set(func(s *mqttSession, pis []uint16, v mqttTransitionVerdict) {
		if s == sessA.Load() && (v == mqttTransitionLanded || v == mqttTransitionSuperseded) {
			landedA.Add(1)
		}
	})
	t.Cleanup(func() { mqttTestAcquisitionBatchHook.Set(nil) })
	t.Cleanup(func() { rel.Do(func() { close(release) }) })
	cA, rA := testMQTTConnect(t, &mqttConnInfo{clientID: p.cid, cleanSess: false}, p.oA.MQTT.Host, p.oA.MQTT.Port)
	t.Cleanup(func() { cA.Close() })
	_ = rA
	sA := testMQTTGetClient(t, p.sA, p.cid).mqtt.sess
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if at.Attempt != nil && at.Attempt.ID == p.cid && at.Attempt.Op == testAcquireOp {
			sessA.CompareAndSwap(nil, sA)
			once.Do(func() {
				close(held)
				<-release
			})
		}
		return send()
	}
	sA.jsa.testTransitionCommit.Store(&commit)
	t.Cleanup(func() { sA.jsa.testTransitionCommit.Store(nil) })
	select {
	case <-held:
	case <-time.After(15 * time.Second):
		t.Fatal("fixture: A never committed a batched acquisition")
	}
	cB, rB := p.takeOver(t, func() { rel.Do(func() { close(release) }) })
	testBatchReadAllOnceOn(t, cB, rB, n, 60*time.Second)
	time.Sleep(500 * time.Millisecond)
	if k := landedA.Load(); k != 0 {
		t.Fatalf("%d batched acquisitions of the displaced broker landed", k)
	}
}

// TestAcquisitionBatchOversizeParticipantIsDisposedInPlace: a participant exceeding the client's
// Maximum Packet Size is disposed of at its own position; the others are written in order.
func TestAcquisitionBatchOversizeParticipantIsDisposedInPlace(t *testing.T) {
	rig := newTestBatchRig(t, "acq-oversize", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	testOnAcquireBatchPause(t, sess, rec, nil)
	var once sync.Once
	admitted := func(s *mqttSession, pi uint16) {
		if s != sess {
			return
		}
		once.Do(func() {
			s.mu.Lock()
			c := s.c
			s.mu.Unlock()
			// Set on the dispatch goroutine, which is the reader of this limit.
			c.mqtt.maxPacketSize = 200
		})
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	rig.detach()
	const n = 8
	big := strings.Repeat("x", 400)
	for i := 0; i < n; i++ {
		payload := fmt.Sprintf("m%d", i)
		if i == 2 {
			payload = big
		}
		testMQTTPublish(t, rig.pc, rig.pr, 1, false, false, "ab/t", uint16(i+1), []byte(payload))
	}
	rig.connectSubscriber(true)
	for i := 0; i < n; i++ {
		if i == 2 {
			continue
		}
		testMQTTCheckPubMsg(t, rig.sc, rig.sr, "ab/t", 1<<1, []byte(fmt.Sprintf("m%d", i)))
	}
	if _, _, ok := testMQTTReadPacketReady(rig.sr, 1500*time.Millisecond); ok {
		t.Fatalf("a packet followed the deliveries: the oversize message was sent or re-offered")
	}
	if rec.multiFlush() == nil {
		t.Fatalf("fixture: no flush had more than one participant")
	}
}

// TestAcquisitionBatchFallsOutToTheSingleRoute: a participant whose register carries another
// holder's token when the batch reads it falls out of the batch; once that holder lets go it is
// acquired by the single route at its own turn and written in publish order in the same flush.
func TestAcquisitionBatchFallsOutToTheSingleRoute(t *testing.T) {
	rig := newTestBatchRig(t, "acq-fallout", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var calls atomic.Int32
	var subj atomic.Value
	var tokv atomic.Value
	admitted := func(s *mqttSession, pi uint16) {
		if s != sess || calls.Add(1) != 3 {
			return
		}
		sub := testRegisterSubjects(s, []uint16{pi})
		if len(sub) != 1 {
			rec.fail("fixture: no register for %d", pi)
			return
		}
		cur, smsg, err := s.asm.loadInflightRecord(sub[0])
		if err != nil || cur == nil || smsg == nil {
			rec.fail("fixture: reading %q: %v", sub[0], err)
			return
		}
		tok := mqttNewInflightNonce()
		cur.ActionToken = tok
		cur.ActionClaim, cur.ActionClaimSeq = cur.Claim, cur.ClaimSeq
		if _, err := s.jsa.storeInflightCAS(sub[0], smsg.Sequence, cur); err != nil {
			rec.fail("fixture: installing the foreign token: %v", err)
			return
		}
		subj.Store(sub[0])
		tokv.Store(tok)
	}
	mqttTestDeliveryAdmittedPauseV.Store(&admitted)
	t.Cleanup(func() { mqttTestDeliveryAdmittedPauseV.Store(nil) })
	var fell atomic.Bool
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		s, _ := subj.Load().(string)
		if s == _EMPTY_ || fell.Load() {
			return
		}
		cur, smsg, err := sess.asm.loadInflightRecord(s)
		if err != nil || cur == nil || smsg == nil || cur.ActionToken != tokv.Load().(string) {
			return // not this flush's participant
		}
		fell.Store(true)
		cur.ActionToken, cur.ActionClaim, cur.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
		if _, err := sess.jsa.storeInflightCAS(s, smsg.Sequence, cur); err != nil {
			rec.fail("fixture: draining the foreign token: %v", err)
		}
	})
	const n = 16
	rig.backlog(n)
	rig.readInOrder(n)
	rec.check(t)
	if !fell.Load() {
		t.Fatalf("fixture: the foreign-held participant was not in a batched flush")
	}
}

// testCompleteQoS2 reads n QoS 2 deliveries on the rig's subscriber and completes every exchange;
// it reports how many PUBRELs arrived per identifier.
func testCompleteQoS2(t *testing.T, rig *testBatchRig, n int, within time.Duration,
	onPub func(pi uint16, payload string)) map[uint16]int {
	t.Helper()
	seen := map[string]bool{}
	owed := map[uint16]bool{}
	rels := map[uint16]int{}
	deadline := time.Now().Add(within)
	for (len(seen) < n || len(owed) > 0) && time.Now().Before(deadline) {
		b, pl, ok := testMQTTReadPacketReady(rig.sr, time.Until(deadline))
		if !ok {
			continue
		}
		switch b & mqttPacketMask {
		case mqttPacketPub:
			flags, pi, _, payload := testMQTTGetPubMsgExEx(t, nil, rig.sr, b, pl, "ab/t", nil)
			if flags&mqttPubFlagDup == 0 && seen[string(payload)] {
				t.Fatalf("%s was transmitted twice as a first transmission", payload)
			}
			seen[string(payload)] = true
			owed[pi] = true
			if onPub != nil {
				onPub(pi, string(payload))
			}
			testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
		case mqttPacketPubRel:
			pi, err := rig.sr.readUint16("pi")
			if err != nil {
				t.Fatal(err)
			}
			rels[pi]++
			delete(owed, pi)
			testMQTTSendPIPacket(mqttPacketPubComp, t, rig.sc, pi)
		default:
			t.Fatalf("unexpected packet %x", b)
		}
	}
	if len(seen) < n || len(owed) > 0 {
		t.Fatalf("%d of %d delivered, %d QoS 2 exchanges unfinished", len(seen), n, len(owed))
	}
	return rels
}

// TestAcquisitionBatchQoS2BurstCompletes: a QoS 2 backlog acquired in batches completes every
// exchange once, its PUBREL acquisitions contending on the same authority.
func TestAcquisitionBatchQoS2BurstCompletes(t *testing.T) {
	rig := newTestBatchRig(t, "acq-qos2", 2, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	rig.detach()
	const n = 40
	for i := 0; i < n; i++ {
		rig.publish(2, i)
	}
	rig.connectSubscriber(true)
	rels := testCompleteQoS2(t, rig, n, 60*time.Second, nil)
	for pi, k := range rels {
		if k > 1 {
			t.Fatalf("identifier %d received %d PUBRELs", pi, k)
		}
	}
	if rec.landedMulti() == 0 {
		t.Fatalf("fixture: no multi-participant acquisition landed")
	}
}

// TestAcquisitionBatchCertifiedCloseAfterTheCommitRecoversEveryParticipant: the connection closes
// after its flush acquired and before any participant was written; on reconnect every message is
// delivered once and no register keeps a token.
func TestAcquisitionBatchCertifiedCloseAfterTheCommitRecoversEveryParticipant(t *testing.T) {
	rig := newTestBatchRig(t, "acq-close", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var shared atomic.Pointer[net.Conn]
	var subjectsMu sync.Mutex
	var subjects []string
	var once sync.Once
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		if len(pis) < 2 {
			return
		}
		once.Do(func() {
			subs := testRegisterSubjects(sess, pis)
			subjectsMu.Lock()
			subjects = subs
			subjectsMu.Unlock()
			c := testSessionConn(sess)
			testCloseShared(&shared)
			deadline := time.Now().Add(10 * time.Second)
			for c != nil && c.outBind.mayWrite() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
		})
	})
	const n = 16
	testBacklogSharingConn(rig, n, &shared)
	checkFor(t, 20*time.Second, 10*time.Millisecond, func() error {
		subjectsMu.Lock()
		defer subjectsMu.Unlock()
		if len(subjects) == 0 {
			return fmt.Errorf("no flush closed yet")
		}
		return nil
	})
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.c != nil {
			return fmt.Errorf("the session still serves the closed connection")
		}
		return nil
	})
	rig.connectSubscriber(true)
	rig.readAllOnce(n, 60*time.Second)
	rec.check(t)
	subjectsMu.Lock()
	subs := subjects
	subjectsMu.Unlock()
	testNoTokenOn(t, sess, subs, 20*time.Second)
}

// TestAcquisitionBatchEarlierAcknowledgementsWhileLaterHoldersAreOpen: the client's PUBREC for a
// flush's first participant arrives while the later participants still hold their acquisitions;
// its stored PUBREL, refused while the first section is still releasing, is re-driven and sent
// once before the second participant is written, and every exchange completes.
func TestAcquisitionBatchEarlierAcknowledgementsWhileLaterHoldersAreOpen(t *testing.T) {
	rig := newTestBatchRig(t, "acq-early-ack", 2, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	var flush atomic.Value
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		if len(pis) > 2 {
			flush.CompareAndSwap(nil, pis)
		}
	})
	pubRecSent := make(chan struct{})
	relSeen := make(chan struct{})
	var drainOnce, writeOnce sync.Once
	drained := func(pi uint16) {
		f, _ := flush.Load().([]uint16)
		if len(f) == 0 || pi != f[0] {
			return
		}
		drainOnce.Do(func() {
			select {
			case <-pubRecSent:
			case <-time.After(10 * time.Second):
				rec.fail("fixture: the PUBREC for %d was never sent", pi)
				return
			}
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				sess.mu.Lock()
				waiting := len(sess.pubRelRedrive[pi]) > 0
				sess.mu.Unlock()
				if waiting {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			rec.fail("the stored PUBREL for %d was not refused with a redrive waiter while its section held", pi)
		})
	}
	mqttTestPublishDrainedPauseV.Store(&drained)
	t.Cleanup(func() { mqttTestPublishDrainedPauseV.Store(nil) })
	window := func(s *mqttSession, pi uint16, w string) bool {
		f, _ := flush.Load().([]uint16)
		if s != sess || len(f) < 2 || pi != f[1] || w != mqttMarkerSendReserved {
			return true
		}
		writeOnce.Do(func() {
			select {
			case <-relSeen:
			case <-time.After(10 * time.Second):
				rec.fail("the PUBREL of %d was not sent while %d held its acquisition", f[0], pi)
			}
		})
		return true
	}
	mqttTestMarkerSendWindowHookV.Store(&window)
	t.Cleanup(func() { mqttTestMarkerSendWindowHookV.Store(nil) })
	rig.detach()
	const n = 24
	for i := 0; i < n; i++ {
		rig.publish(2, i)
	}
	rig.connectSubscriber(true)
	var firstPI atomic.Uint32
	var recOnce, relOnce sync.Once
	seen := map[string]bool{}
	owed := map[uint16]bool{}
	rels := map[uint16]int{}
	deadline := time.Now().Add(60 * time.Second)
	for (len(seen) < n || len(owed) > 0) && time.Now().Before(deadline) {
		b, pl, ok := testMQTTReadPacketReady(rig.sr, time.Until(deadline))
		if !ok {
			continue
		}
		switch b & mqttPacketMask {
		case mqttPacketPub:
			_, pi, _, payload := testMQTTGetPubMsgExEx(t, nil, rig.sr, b, pl, "ab/t", nil)
			seen[string(payload)] = true
			owed[pi] = true
			testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
			if f, _ := flush.Load().([]uint16); len(f) > 0 && pi == f[0] {
				firstPI.Store(uint32(pi))
				recOnce.Do(func() { close(pubRecSent) })
			}
		case mqttPacketPubRel:
			pi, err := rig.sr.readUint16("pi")
			if err != nil {
				t.Fatal(err)
			}
			rels[pi]++
			delete(owed, pi)
			if uint32(pi) == firstPI.Load() {
				relOnce.Do(func() { close(relSeen) })
			}
			testMQTTSendPIPacket(mqttPacketPubComp, t, rig.sc, pi)
		default:
			t.Fatalf("unexpected packet %x", b)
		}
	}
	rec.check(t)
	if len(seen) < n || len(owed) > 0 {
		t.Fatalf("%d of %d delivered, %d QoS 2 exchanges unfinished", len(seen), n, len(owed))
	}
	if flush.Load() == nil {
		t.Fatalf("fixture: no flush had three participants")
	}
	if k := rels[uint16(firstPI.Load())]; k != 1 {
		t.Fatalf("the first participant's PUBREL was sent %d times", k)
	}
}

// TestAcquisitionBatchShutdownWaitsForEveryOpenedSection: a server shutting down while a flush
// holds its acquisitions waits for every opened section to end.
func TestAcquisitionBatchShutdownWaitsForEveryOpenedSection(t *testing.T) {
	rig := newTestBatchRig(t, "acq-shutdown", 1, nil)
	sess := rig.sess()
	rec := testRecordAcquisitions(t, sess)
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var flush []uint16
	testOnAcquireBatchPause(t, sess, rec, func(pis []uint16) {
		if len(pis) < 2 {
			return
		}
		once.Do(func() {
			flush = pis
			close(held)
			<-release
		})
	})
	var drains sync.Map
	drained := func(pi uint16) { drains.Store(pi, true) }
	mqttTestPublishDrainedPauseV.Store(&drained)
	t.Cleanup(func() { mqttTestPublishDrainedPauseV.Store(nil) })
	const n = 16
	rig.backlog(n)
	select {
	case <-held:
	case <-time.After(15 * time.Second):
		t.Fatal("fixture: no multi-participant flush")
	}
	done := make(chan struct{})
	go func() {
		rig.s.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		close(release)
		t.Fatal("the shutdown returned while a flush held its sections")
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the shutdown never returned")
	}
	for _, pi := range flush {
		if _, ok := drains.Load(pi); !ok {
			t.Errorf("participant %d's section was not ended before the shutdown returned", pi)
		}
	}
	rec.check(t)
}
