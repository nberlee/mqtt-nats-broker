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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// T36 rows 18, 18b, 18c and 18d (F-088 plan rev 20, WP3 rule 6): the exclusion-reclaim
// continuation disposes of a PENDING FENCE that a finite failure left standing, with no further
// offer, allocation or resume, under its worker contract — every added request charged, a fair
// traversal independent of deletion success, a per-exclusion fence cursor, and the dispatch gate
// asked before every added request.
//
// Every fence here is written by the REAL provenance-enforcing producer
// (markInflightRegisterCancelledPending) on the real store; the pass is the real
// runExclusionReclaim, driven one pass at a time with its self-rescheduling switched off by the
// session's reclaim seam so a pass never runs behind the fixture's back.

// TestF088PendingFenceDisposalPreservationMatrix is T36: ONE matrix, row by row per consult C6.
// Rows are subtests; each builds its own environment.
func TestF088PendingFenceDisposalPreservationMatrix(t *testing.T) {
	t.Parallel()
	for _, row := range t36Rows() {
		row := row
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			row.fn(t)
		})
	}
}

type t36Row struct {
	name string
	fn   func(t *testing.T)
}

// t36ReclaimRows are the continuation's rows (rule 6).
func t36ReclaimRows() []t36Row {
	return []t36Row{
		{"18-finite-failure-fence-retry", t36Row18FiniteFailureFenceRetry},
		{"18b-budget-exhausted-mid-fence-fair-continuation", t36Row18bBudgetExhaustedMidFence},
		{"18c-shutdown-between-load-and-delete", t36Row18cShutdown},
		{"18d-persistent-fence-failure-does-not-starve", t36Row18dPersistentFenceFailureDoesNotStarve},
	}
}

// t36Rows is the whole matrix; the lifecycle rows are appended as they land.
func t36Rows() []t36Row {
	return append(t36LifecycleRows(), t36ReclaimRows()...)
}

// mqttExclusionReclaimFenceStepMax is one pending-fence step's request bound (F-088 WP3 rule 6
// (a)): the door's evidence marker probe, each confirmed delete attempt, the door's own
// confirmation before it retires a verdict naming the fence (only when one does), and the step's
// post-delete confirmation. The register load is the bind's own pre-charged base read. It is the
// most a pass may overrun its budget by beyond the snapshot's whole-entry overrun.
const mqttExclusionReclaimFenceStepMax = 1 + mqttInflightStoreAttempts + 1 + 1

// t36Reclaim instruments the real reclaim pass of one session over a real store.
type t36Reclaim struct {
	t    *testing.T
	e    *testInflightEnv
	dur  string
	sseq uint64

	mu       sync.Mutex
	counting bool
	// Per pass (reset by pass()).
	steps    []string
	added    int
	issued   int
	loadsOf  map[string]int
	lifts    []uint16
	first    []uint16
	spent    [][2]int
	next     []time.Duration
	failSeq  map[uint64]bool
	failOnce map[uint64]int
	// failLoad makes a subject's register load fail (an unreadable bind: never absent).
	failLoad map[string]bool
	names    map[string]string
	// fenceFound / afterDelete run inside the pass, for the shutdown subtests.
	fenceFound  func(pi uint16, subject string)
	afterDelete func(seq uint64, err error)
	budget      func(snapshotCharge int) int
}

func newT36Reclaim(t *testing.T, clientID string) *t36Reclaim {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	e.acceptUnderServingAuthority(t)
	r := &t36Reclaim{t: t, e: e, dur: e.sess.idHash + "_" + nuid.Next(), sseq: 10,
		failSeq: map[uint64]bool{}, failOnce: map[uint64]int{}, failLoad: map[string]bool{}, names: map[string]string{},
		loadsOf: map[string]int{}}
	r.installSeams(e.sess)
	// RESIDENT, as a served session is: the pass refuses to run for an object that is not the
	// live session for its client ID.
	e.asm.addSession(e.sess, true)
	e.asm.mu.Lock()
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		r.mu.Lock()
		if r.counting {
			r.issued++
			r.loadsOf[subject]++
		}
		fail := r.failLoad[subject]
		r.mu.Unlock()
		if fail {
			return nil, nil, errors.New("test: the bind's register load fails")
		}
		return e.asm.jsa.loadInflightRecord(subject)
	}
	e.asm.testInflightMarkerLoad = func(subject string) (*StoredMsg, error) {
		r.mu.Lock()
		if r.counting {
			r.issued++
		}
		r.mu.Unlock()
		return e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	}
	e.asm.testInflightDelete = func(seq uint64) error {
		r.mu.Lock()
		if r.counting {
			r.issued++
		}
		fail := r.failSeq[seq]
		if n := r.failOnce[seq]; n > 0 {
			r.failOnce[seq] = n - 1
			fail = true
		}
		after := r.afterDelete
		r.mu.Unlock()
		var err error
		if fail {
			err = errors.New("test: the fence's delete failed")
		} else {
			err = e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
		}
		if after != nil {
			after(seq, err)
		}
		return err
	}
	e.asm.mu.Unlock()
	return r
}

// installSeams installs the reclaim seams on sess and marks its continuation armed, so an
// exclusion install schedules no timer of its own: every pass is the fixture's.
func (r *t36Reclaim) installSeams(sess *mqttSession) {
	seams := &mqttExclReclaimSeams{
		budget: func(charge int) int {
			r.mu.Lock()
			fn := r.budget
			r.mu.Unlock()
			if fn != nil {
				return fn(charge)
			}
			return mqttExclusionReclaimReadBudget
		},
		passStart: func(pi uint16) {
			r.mu.Lock()
			r.first = append(r.first, pi)
			r.mu.Unlock()
		},
		fenceFound: func(pi uint16, subject string) {
			r.mu.Lock()
			fn := r.fenceFound
			r.mu.Unlock()
			if fn != nil {
				fn(pi, subject)
			}
		},
		fenceStep: func(pi uint16, subject string, out mqttReclaimFenceOutcome) {
			r.mu.Lock()
			r.steps = append(r.steps, fmt.Sprintf("%d:%s:%s", pi, r.names[subject], t36Outcome(out)))
			r.mu.Unlock()
		},
		charged: func() {
			r.mu.Lock()
			r.added++
			r.mu.Unlock()
		},
		lift: func(pi uint16) {
			r.mu.Lock()
			r.lifts = append(r.lifts, pi)
			r.mu.Unlock()
		},
		passEnd: func(charge, spent int) {
			r.mu.Lock()
			r.spent = append(r.spent, [2]int{charge, spent})
			r.mu.Unlock()
		},
		reschedule: func(next time.Duration) bool {
			r.mu.Lock()
			r.next = append(r.next, next)
			r.mu.Unlock()
			return false
		},
	}
	sess.mu.Lock()
	sess.testExclReclaim = seams
	sess.exclReclaimArmed = true
	sess.mu.Unlock()
}

func t36Outcome(o mqttReclaimFenceOutcome) string {
	switch o {
	case mqttReclaimFenceAbsent:
		return "disposed"
	case mqttReclaimFenceShutdown:
		return "shutdown"
	}
	return "kept"
}

// fence writes a pending fence on pi's subject through the real producer and names it.
func (r *t36Reclaim) fence(name string, pi uint16) (subject, nonce string, seq uint64) {
	r.t.Helper()
	r.sseq++
	nonce = mqttNewInflightNonce()
	_, seq = testF088WriteFence(r.t, r.e, pi, r.dur, r.sseq, nonce)
	subject = mqttInflightSubject(r.e.sess.idHash, r.e.sess.gen, pi)
	r.mu.Lock()
	r.names[subject] = name
	r.mu.Unlock()
	return subject, nonce, seq
}

// exclude installs (or reasserts) a bind of identifier pi through the production install.
func (r *t36Reclaim) exclude(sess *mqttSession, pi uint16, subject, nonce string) {
	sess.mu.Lock()
	sess.excludeIdentifierLocked(pi, subject, nonce)
	sess.mu.Unlock()
}

// pass runs ONE real pass and returns its fence steps.
func (r *t36Reclaim) pass(sess *mqttSession) []string {
	r.mu.Lock()
	r.counting, r.steps, r.added, r.issued, r.lifts = true, nil, 0, 0, nil
	r.loadsOf = map[string]int{}
	r.mu.Unlock()
	sess.runExclusionReclaim(mqttExclusionReclaimDelay)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counting = false
	return append([]string(nil), r.steps...)
}

func t36Excluded(sess *mqttSession, pi uint16) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return slotExclusionHeldLocked(sess.sendsInFlight[pi])
}

func t36Exclusion(sess *mqttSession, pi uint16) *mqttSlotExclusion {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if h := sess.sendsInFlight[pi]; h != nil {
		return h.slotOwed
	}
	return nil
}

func t36Cursor(sess *mqttSession) uint16 {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.exclReclaimCursor
}

func (r *t36Reclaim) present(subject string) bool {
	r.t.Helper()
	_, smsg, err := r.e.asm.jsa.loadInflightRecord(subject)
	if err != nil {
		r.t.Fatalf("reading %q: %v", subject, err)
	}
	return smsg != nil
}

// Row 18: the first exact-fence delete (recovery's drop) fails finitely; with NO further offer,
// allocation or resume, the continuation re-loads, validates and disposes of the fence, and lifts
// the exclusion only on its confirmed absence.
func t36Row18FiniteFailureFenceRetry(t *testing.T) {
	e := testNewInflightEnv(t, "f088-t36-18")
	e.acceptUnderServingAuthority(t)
	dur := e.sess.idHash + "_" + nuid.Next()
	e.sess.mu.Lock()
	pi := e.sess.bumpPI()
	e.sess.mu.Unlock()
	_, fseq := testF088WriteFence(t, e, pi, dur, 4, mqttNewInflightNonce())
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)

	var mu sync.Mutex
	failing, failed := true, 0
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(seq uint64) error {
		mu.Lock()
		f := failing && seq == fseq
		if f {
			failed++
		}
		mu.Unlock()
		if f {
			return errors.New("test: the first disposal of the fence fails finitely")
		}
		return e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true)
	}
	e.asm.mu.Unlock()

	restored := testF088ColdResume(t, e)
	mu.Lock()
	gotFailed := failed
	failing = false
	mu.Unlock()
	if gotFailed == 0 {
		t.Fatal("fixture: recovery's disposal of the fence never reached its delete")
	}
	if _, smsg, _ := e.asm.loadInflightRecord(subject); smsg == nil {
		t.Fatal("fixture: the failed disposal must leave the fence standing")
	}
	if !t36Excluded(restored, pi) {
		t.Fatalf("identifier %d must stay excluded while its fence stands", pi)
	}

	var steps []string
	var visited []uint16
	var smu sync.Mutex
	restored.mu.Lock()
	lastPI, entries := restored.last_pi, len(restored.pendingPublish)
	restored.testExclReclaim = &mqttExclReclaimSeams{
		passStart: func(p uint16) {
			smu.Lock()
			visited = append(visited, p)
			smu.Unlock()
		},
		fenceStep: func(p uint16, s string, out mqttReclaimFenceOutcome) {
			smu.Lock()
			steps = append(steps, fmt.Sprintf("%d:%s", p, t36Outcome(out)))
			smu.Unlock()
		},
		reschedule: func(time.Duration) bool { return false },
	}
	restored.mu.Unlock()

	restored.runExclusionReclaim(mqttExclusionReclaimDelay)

	smu.Lock()
	got := append([]string(nil), steps...)
	first := append([]uint16(nil), visited...)
	smu.Unlock()
	// REACH (M51's route): the continuation's pass visited the excluded identifier. This witness
	// sits ahead of the fence step, so it holds whether or not the pass has a fence step to take.
	if len(first) != 1 || first[0] != pi {
		t.Fatalf("the continuation's pass must visit identifier %d, visited %v", pi, first)
	}
	if _, smsg, _ := e.asm.loadInflightRecord(subject); smsg != nil {
		t.Fatalf("the continuation must dispose of the fence with no further offer (steps %v)", got)
	}
	// The disposal came from the continuation's own fence step (rule 6), not from another route.
	if len(got) == 0 {
		t.Fatal("the continuation's fence step was never reached")
	}
	if t36Excluded(restored, pi) {
		t.Fatalf("the exclusion must be lifted on the fence's confirmed absence (steps %v)", got)
	}
	restored.mu.Lock()
	lastAfter, entriesAfter := restored.last_pi, len(restored.pendingPublish)
	restored.mu.Unlock()
	if lastAfter != lastPI || entriesAfter != entries {
		t.Fatalf("no offer or allocation may happen in the window: last_pi %d->%d, entries %d->%d",
			lastPI, lastAfter, entries, entriesAfter)
	}
}

// Row 18b: the budget stops a pass between fence steps, fairly, and the request account is exact.
func t36Row18bBudgetExhaustedMidFence(t *testing.T) {
	check := func(t *testing.T, r *t36Reclaim) {
		t.Helper()
		r.mu.Lock()
		defer r.mu.Unlock()
		if len(r.spent) == 0 {
			t.Fatal("the pass reported no request account")
		}
		last := r.spent[len(r.spent)-1]
		if last[1] != last[0]+r.added {
			t.Errorf("spent %d != snapshot charge %d + added fence requests %d", last[1], last[0], r.added)
		}
		if base := r.issued - r.added; base > last[0] {
			t.Errorf("the pass issued %d base requests against a pre-charge of %d", base, last[0])
		}
		if last[1] > last[0]+mqttExclusionReclaimFenceStepMax {
			t.Errorf("spent %d exceeds the budget (%d) plus one fence step's bound (%d)",
				last[1], last[0], mqttExclusionReclaimFenceStepMax)
		}
	}
	t.Run("not-started", func(t *testing.T) {
		r := newT36Reclaim(t, "f088-t36-18b-ns")
		// THE BUDGET LEAVES ROOM FOR EXACTLY THE ADDED REQUESTS (M54's discriminating input): one
		// request above the snapshot's pre-charge. W's fence step adds at least one charged request
		// (its marker probe), so a pass that charges fence requests reaches the budget after W's
		// step; a pass that did not would still be under it and would start A's step.
		r.budget = func(charge int) int { return charge + 1 }
		const W, A, B = uint16(10), uint16(20), uint16(30)
		w, wn, _ := r.fence("w", 110)
		a, an, _ := r.fence("a", 120)
		b, bn, _ := r.fence("b", 130)
		// W ALSO binds an unreadable subject, so W stays excluded after its fence is disposed of:
		// with W still held, a cursor left at the snapshot's last candidate wraps to W, and only
		// the rewind to A-1 makes the next pass start at A (M55's discriminating input).
		wHeld := mqttInflightSubject(r.e.sess.idHash, r.e.sess.gen, 111)
		r.mu.Lock()
		r.failLoad[wHeld] = true
		r.mu.Unlock()
		r.exclude(r.e.sess, W, w, wn)
		r.exclude(r.e.sess, W, wHeld, mqttNewInflightNonce())
		r.exclude(r.e.sess, A, a, an)
		r.exclude(r.e.sess, B, b, bn)

		steps := r.pass(r.e.sess)
		r.mu.Lock()
		wAdded := r.added
		r.mu.Unlock()
		// Witness (M54's constraint): the request-counter seam saw W's fence requests.
		if wAdded == 0 {
			t.Fatal("fixture: the request-counter seam recorded no fence request for W's step")
		}
		if want := []string{"10:w:disposed"}; strings.Join(steps, ",") != strings.Join(want, ",") {
			t.Fatalf("pass 1 fence steps %v, want %v: W's completed step reaches the budget", steps, want)
		}
		check(t, r)
		r.mu.Lock()
		lifts, next := append([]uint16(nil), r.lifts...), r.next[len(r.next)-1]
		r.mu.Unlock()
		for _, p := range lifts {
			if p == A {
				t.Fatal("the lift was invoked for A, whose fence work never started in the stopped pass")
			}
		}
		if !t36Excluded(r.e.sess, A) || !r.present(a) {
			t.Fatal("A must stay excluded with its fence standing")
		}
		if !t36Excluded(r.e.sess, W) {
			t.Fatal("fixture: W must stay excluded on its unreadable bind")
		}
		// Supporting (non-fatal, so the next pass's target is evaluated too).
		if got := t36Cursor(r.e.sess); got != A-1 {
			t.Errorf("the identifier cursor reads %d, want A-1 = %d (the only rewind)", got, A-1)
		}
		if next != mqttExclusionReclaimDelay {
			t.Fatalf("a budget stop reschedules at the short delay, got %v", next)
		}
		r.pass(r.e.sess)
		r.mu.Lock()
		first := r.first[len(r.first)-1]
		r.mu.Unlock()
		if first != A {
			t.Fatalf("the next pass starts at %d, want A = %d", first, A)
		}
		check(t, r)
		r.mu.Lock()
		delete(r.failLoad, wHeld) // W's unreadable bind now reads absent: W can lift too
		r.mu.Unlock()
		for i := 0; i < 6 && (t36Excluded(r.e.sess, W) || t36Excluded(r.e.sess, A) || t36Excluded(r.e.sess, B)); i++ {
			r.pass(r.e.sess)
			check(t, r)
		}
		for _, p := range []uint16{W, A, B} {
			if t36Excluded(r.e.sess, p) {
				t.Errorf("identifier %d was never lifted", p)
			}
		}
	})
	t.Run("started", func(t *testing.T) {
		r := newT36Reclaim(t, "f088-t36-18b-st")
		r.budget = func(charge int) int { return charge }
		const A, B = uint16(20), uint16(30)
		f1, n1, _ := r.fence("f1", 121)
		f2, n2, _ := r.fence("f2", 122)
		f3, n3, _ := r.fence("f3", 123)
		b, bn, _ := r.fence("b", 130)
		for _, x := range [][2]string{{f1, n1}, {f2, n2}, {f3, n3}} {
			r.exclude(r.e.sess, A, x[0], x[1])
		}
		r.exclude(r.e.sess, B, b, bn)

		steps := r.pass(r.e.sess)
		if want := []string{"20:f1:disposed"}; strings.Join(steps, ",") != strings.Join(want, ",") {
			t.Fatalf("pass 1 fence steps %v, want %v", steps, want)
		}
		check(t, r)
		r.mu.Lock()
		lifts := append([]uint16(nil), r.lifts...)
		r.mu.Unlock()
		for _, p := range lifts {
			if p == A {
				t.Fatal("the lift was invoked for A with its remaining binds unread")
			}
		}
		if got := t36Cursor(r.e.sess); got != A {
			t.Fatalf("the identifier cursor reads %d, want A = %d (advanced past a started candidate)", got, A)
		}
		if x := t36Exclusion(r.e.sess, A); x == nil || x.fenceCursor != f1 {
			t.Fatalf("A's fence cursor must read f1, got %+v", x)
		}
		r.pass(r.e.sess)
		r.mu.Lock()
		first := r.first[len(r.first)-1]
		r.mu.Unlock()
		if first != B {
			t.Fatalf("the next pass starts at %d, want B = %d", first, B)
		}
		// When A is next stepped, its fence steps resume at f2.
		var aSteps []string
		for i := 0; i < 8 && t36Excluded(r.e.sess, A); i++ {
			for _, s := range r.pass(r.e.sess) {
				if strings.HasPrefix(s, "20:") {
					aSteps = append(aSteps, s)
				}
			}
			check(t, r)
		}
		if len(aSteps) == 0 || aSteps[0] != "20:f2:disposed" {
			t.Fatalf("A's fence steps after the stop must resume at f2, got %v", aSteps)
		}
		for i := 0; i < 6 && t36Excluded(r.e.sess, B); i++ {
			r.pass(r.e.sess)
		}
		for _, p := range []uint16{A, B} {
			if t36Excluded(r.e.sess, p) {
				t.Errorf("identifier %d was never lifted", p)
			}
		}
	})
}

// Row 18c: shutdown between the register load and the fence step's next request.
func t36Row18cShutdown(t *testing.T) {
	closeGate := func(e *testInflightEnv, closed bool) {
		e.s.willDispatchMu.Lock()
		e.s.willDispatchClosed = closed
		e.s.willDispatchMu.Unlock()
	}
	workersJoined := func(t *testing.T, e *testInflightEnv) {
		t.Helper()
		done := make(chan struct{})
		go func() { e.s.willWorkerWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the pass kept its worker token after shutdown began")
		}
	}
	setup := func(t *testing.T, name string) (*t36Reclaim, string, uint64) {
		r := newT36Reclaim(t, name)
		const X = uint16(10)
		f, n, seq := r.fence("f", 110)
		r.exclude(r.e.sess, X, f, n)
		return r, f, seq
	}
	const X = uint16(10)
	shutAfterLoad := func(t *testing.T, r *t36Reclaim) {
		t.Helper()
		var before int
		r.fenceFound = func(uint16, string) {
			r.mu.Lock()
			before = r.issued
			r.mu.Unlock()
			closeGate(r.e, true)
		}
		steps := r.pass(r.e.sess)
		r.mu.Lock()
		after, added := r.issued, r.added
		r.mu.Unlock()
		if strings.Join(steps, ",") != "10:f:shutdown" {
			t.Fatalf("the step must stop at the closed gate, got %v", steps)
		}
		if after != before || added != 0 {
			t.Fatalf("no request may follow the load once shutdown began: %d issued after it, %d charged", after-before, added)
		}
		if !t36Excluded(r.e.sess, X) {
			t.Fatal("the exclusion must stand")
		}
		r.e.sess.mu.Lock()
		armed := r.e.sess.exclReclaimArmed
		r.e.sess.mu.Unlock()
		if armed {
			t.Fatal("the continuation must disarm at shutdown")
		}
		workersJoined(t, r.e)
	}
	t.Run("same-server-reenrolled", func(t *testing.T) {
		r, f, _ := setup(t, "f088-t36-18c-i")
		r.fenceFound = nil
		shutAfterLoad(t, r)
		if !r.present(f) {
			t.Fatal("the fence must still stand")
		}
		r.fenceFound = nil
		closeGate(r.e, false)
		// Re-enrolment through addSession (the restoration funnel), its own timer.
		r.e.asm.removeSession(r.e.sess, true)
		r.e.asm.addSession(r.e.sess, true)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) && (r.present(f) || t36Excluded(r.e.sess, X)) {
			time.Sleep(50 * time.Millisecond)
		}
		r.mu.Lock()
		steps := append([]string(nil), r.steps...)
		r.mu.Unlock()
		if r.present(f) || t36Excluded(r.e.sess, X) {
			t.Fatalf("the re-enrolled continuation must dispose of the fence and lift (steps %v)", steps)
		}
	})
	t.Run("restart-cold-resume", func(t *testing.T) {
		r, f, _ := setup(t, "f088-t36-18c-ii")
		shutAfterLoad(t, r)
		closeGate(r.e, false)
		testF088ColdResume(t, r.e)
		if r.present(f) {
			t.Fatal("recovery's drop list must dispose of the fence after the restart")
		}
	})
	t.Run("between-delete-attempts", func(t *testing.T) {
		r, f, seq := setup(t, "f088-t36-18c-attempts")
		r.mu.Lock()
		r.failOnce[seq] = 1
		r.mu.Unlock()
		deletes := 0
		r.afterDelete = func(s uint64, err error) {
			if s == seq {
				deletes++
				closeGate(r.e, true)
			}
		}
		steps := r.pass(r.e.sess)
		if strings.Join(steps, ",") != "10:f:shutdown" {
			t.Fatalf("the step must stop at the closed gate, got %v", steps)
		}
		if deletes != 1 {
			t.Fatalf("exactly one delete attempt may be issued, got %d", deletes)
		}
		r.mu.Lock()
		confirmations := r.loadsOf[f] - 1 // the base read
		r.mu.Unlock()
		if confirmations != 0 {
			t.Fatalf("no confirmation may follow, got %d extra reads", confirmations)
		}
		if !r.present(f) || !t36Excluded(r.e.sess, X) {
			t.Fatal("the fence and the exclusion must stand")
		}
		workersJoined(t, r.e)
	})
	t.Run("between-delete-and-confirmation", func(t *testing.T) {
		r, f, seq := setup(t, "f088-t36-18c-confirm")
		r.afterDelete = func(s uint64, err error) {
			if s == seq && err == nil {
				closeGate(r.e, true)
			}
		}
		steps := r.pass(r.e.sess)
		// Non-fatal: the outcome follows from the confirmation below, which is the target (M56c).
		if strings.Join(steps, ",") != "10:f:shutdown" {
			t.Errorf("the step must stop at the closed gate, got %v", steps)
		}
		r.mu.Lock()
		confirmations := r.loadsOf[f] - 1
		r.mu.Unlock()
		if confirmations != 0 {
			t.Fatalf("no confirmation request may be issued, got %d", confirmations)
		}
		if r.present(f) {
			t.Fatal("fixture: the delete itself must have landed")
		}
		if !t36Excluded(r.e.sess, X) {
			t.Fatal("an unconfirmed delete must not lift the exclusion")
		}
		workersJoined(t, r.e)
		r.afterDelete = nil
		closeGate(r.e, false)
		r.pass(r.e.sess)
		if t36Excluded(r.e.sess, X) {
			t.Fatal("the next run's base read finds the subject absent and lifts")
		}
	})
}

// Row 18d: a persistently failing fence does not starve anything, across two exclusions, with
// exactly ONE fence step per pass.
func t36Row18dPersistentFenceFailureDoesNotStarve(t *testing.T) {
	r := newT36Reclaim(t, "f088-t36-18d")
	r.budget = func(charge int) int { return charge } // spent starts AT the budget
	const X, Y = uint16(10), uint16(20)
	f1, nf1, sf1 := r.fence("f1", 101)
	f2, nf2, _ := r.fence("f2", 102)
	f3, nf3, _ := r.fence("f3", 103)
	g1, ng1, sg1 := r.fence("g1", 201)
	g2, ng2, _ := r.fence("g2", 202)
	r.mu.Lock()
	r.failSeq[sf1], r.failSeq[sg1] = true, true
	r.mu.Unlock()
	for _, b := range [][2]string{{f1, nf1}, {f2, nf2}, {f3, nf3}} {
		r.exclude(r.e.sess, X, b[0], b[1])
	}
	for _, b := range [][2]string{{g1, ng1}, {g2, ng2}} {
		r.exclude(r.e.sess, Y, b[0], b[1])
	}

	var got []string
	var cursors []uint16
	var f2Reads []int
	run := func() {
		steps := r.pass(r.e.sess)
		if len(steps) != 1 {
			t.Fatalf("pass %d made %d fence steps %v: the schedule needs exactly one per pass", len(got)+1, len(steps), steps)
		}
		got = append(got, steps[0])
		cursors = append(cursors, t36Cursor(r.e.sess))
		r.mu.Lock()
		f2Reads = append(f2Reads, r.loadsOf[f2])
		r.mu.Unlock()
	}
	run() // 1
	run() // 2
	// TARGET (M58): the traversal is not tied to deletion success - X's kept f1 does not hold the
	// pass at X, so pass 2 attempts Y's g1.
	if got[1] != "20:g1:kept" {
		t.Fatalf("pass 2 must attempt Y:g1 after X's kept f1, got %v", got)
	}
	// Between passes 2 and 3: a NEW bind f0 < f1 (fenced) and a duplicate reassertion of f2, each
	// advancing X's instance; X's fence cursor must survive both.
	f0, nf0, _ := r.fence("f0", 100)
	instBefore := t36Exclusion(r.e.sess, X).inst
	r.exclude(r.e.sess, X, f0, nf0)
	r.exclude(r.e.sess, X, f2, nf2)
	x := t36Exclusion(r.e.sess, X)
	// Fixture witness (M60's constraint): the f0 addition and the f2 reassertion advanced X's
	// instance between passes 2 and 3.
	if x.inst <= instBefore {
		t.Fatalf("fixture: X's instance must advance (%d -> %d)", instBefore, x.inst)
	}
	// The cursor surviving the instance change is asserted here directly AND observed by pass 3's
	// target below; non-fatal so the target is evaluated too.
	if x.fenceCursor != f1 {
		t.Errorf("X's fence cursor must survive the instance change at f1, got %q", x.fenceCursor)
	}
	run() // 3
	// TARGET (M59, M60): X's fence steps resume at f2, after the cursor - not at f0 or f1.
	if got[2] != "10:f2:disposed" {
		t.Fatalf("pass 3 must dispose of X:f2 (the fence cursor resumes after f1), got %v", got)
	}
	for i := 4; i <= 9; i++ {
		run()
	}
	want := []string{
		"10:f1:kept", "20:g1:kept", "10:f2:disposed", "20:g2:disposed", "10:f3:disposed",
		"20:g1:kept", "10:f0:disposed", "20:g1:kept", "10:f1:kept",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("the fence-step schedule\n got  %v\n want %v", got, want)
	}
	wantCursor := []uint16{X, Y, X, Y, X, X - 1, X, X - 1, Y - 1}
	if fmt.Sprint(cursors) != fmt.Sprint(wantCursor) {
		t.Fatalf("identifier cursor after each pass %v, want %v: it never moves back for a failed step", cursors, wantCursor)
	}
	r.mu.Lock()
	first := append([]uint16(nil), r.first...)
	r.mu.Unlock()
	if want := []uint16{X, Y, X, Y, X, Y, X, Y, X}; fmt.Sprint(first) != fmt.Sprint(want) {
		t.Fatalf("first candidate per pass %v, want %v", first, want)
	}
	// The cursor is never evidence: after f2's deletion (pass 3) the only pass that reaches f2
	// in X's order is pass 9 (passes 5 and 7 stop before it), and it base-reads f2 before the
	// lift can count it absent.
	for _, p := range []int{9} {
		if f2Reads[p-1] == 0 {
			t.Fatalf("pass %d visited X without base-reading f2: the fence cursor was used as evidence", p)
		}
	}
	if !t36Excluded(r.e.sess, X) || !t36Excluded(r.e.sess, Y) {
		t.Fatal("X and Y must stay excluded while f1 and g1 stand")
	}
	if !r.present(f1) || !r.present(g1) {
		t.Fatal("f1 and g1 failed on every attempt and must stand")
	}
	if r.present(f0) || r.present(f2) || r.present(f3) || r.present(g2) {
		t.Fatal("every deletable fence must have been disposed of")
	}
}
