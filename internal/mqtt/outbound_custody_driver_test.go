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
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// F-097 WP4 (plan rev 20): the autonomous custody reconciliation driver. T2 - T2i.

// testDriverRec records one session's dispatcher activity through its seams.
type testDriverRec struct {
	mu         sync.Mutex
	starts     []string
	iterations int
	units      []testDriverUnit
	census     []time.Time
	startEntry func()
	iteration  func(n int)
	idle       func()
	unit       func(u mqttCustodyUnit)
}

type testDriverUnit struct {
	gen   string
	kind  mqttCustodyUnitKind
	queue mqttCustodyQueue // the queue the unit was taken from
	key   string           // a target unit's target key
	iter  int
	run   int // the number of runs started when the unit was dispatched
	at    time.Time
}

func testDriverInstall(sess *mqttSession) *testDriverRec {
	r := &testDriverRec{}
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		startEntry: func() {
			r.mu.Lock()
			fn := r.startEntry
			r.mu.Unlock()
			if fn != nil {
				fn()
			}
		},
		started: func(gen string) {
			r.mu.Lock()
			r.starts = append(r.starts, gen)
			r.mu.Unlock()
		},
		iteration: func(gen string, units []mqttCustodyUnit, census bool) {
			r.mu.Lock()
			r.iterations++
			n := r.iterations
			if census {
				r.census = append(r.census, time.Now())
			}
			fn := r.iteration
			r.mu.Unlock()
			if fn != nil {
				fn(n)
			}
		},
		unit: func(gen string, u mqttCustodyUnit) {
			r.mu.Lock()
			r.units = append(r.units, testDriverUnit{gen: gen, kind: u.kind, queue: u.queue, key: u.target.key, iter: r.iterations,
				run: len(r.starts), at: time.Now()})
			fn := r.unit
			r.mu.Unlock()
			if fn != nil {
				fn(u)
			}
		},
		idle: func(gen string) {
			r.mu.Lock()
			fn := r.idle
			r.mu.Unlock()
			if fn != nil {
				fn()
			}
		},
	}
	sess.mu.Unlock()
	return r
}

func (r *testDriverRec) startCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.starts)
}

// testDriverWithheld admits A at Receive Maximum 1 and offers B, which the delivery gate
// withholds (deferred) - the gate's kick latches the dispatcher's demand.
func testDriverWithheld(t *testing.T, clientID string, qos byte) (*testWithdrawFix, string) {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, qos)
	testCustodyRM1(t, f.e.sess)
	bReply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq+1, 1)
	f.e.sess.mu.Lock()
	bPI, _ := f.e.sess.trackPublish(testRealPubRelSrcDur, bReply)
	f.e.sess.mu.Unlock()
	if bPI != 0 {
		t.Fatalf("fixture: B must be withheld while A holds the only slot, got identifier %d", bPI)
	}
	return f, bReply
}

// testDriverTickArmed reports whether the session's current tick arm (its stored key) is
// registered.
func testDriverTickArmed(sess *mqttSession, srv *Server) bool {
	sess.mu.Lock()
	key := sess.custodyTickKey
	sess.mu.Unlock()
	if key == _EMPTY_ {
		return false
	}
	srv.willDispatchMu.Lock()
	defer srv.willDispatchMu.Unlock()
	_, ok := srv.willTimers[key]
	return ok
}

func testDriverWait(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("not within %v: %s", d, what)
}

// testDriverP1 installs PROPERTY P1 of the dispatcher lifecycle matrix on the session's seams:
// at every exit that ends a run, under the session lock, owed work that no run observes has a
// tick armed, or its incarnation is closing (the reopen re-arms it), or the dispatch gate is
// closed (shutdown), or its generation moved (discarded with the ledger). Install it AFTER any
// seam set the test builds. The seam only records, so a run outliving the test cannot fail it;
// the violations are reported when the test ends.
func testDriverP1(t *testing.T, sess *mqttSession) {
	t.Helper()
	var mu sync.Mutex
	var violations []string
	sess.mu.Lock()
	if sess.testCustodyDriver == nil {
		sess.testCustodyDriver = &mqttCustodyDriverSeams{}
	}
	sess.testCustodyDriver.exited = func(gen string) {
		if v := testDriverP1ViolationLocked(sess, gen); v != _EMPTY_ {
			mu.Lock()
			violations = append(violations, v)
			mu.Unlock()
		}
	}
	sess.mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, v := range violations {
			t.Errorf("property P1: %s", v)
		}
	})
}

// testDriverP1ViolationLocked is P1's check at one exit. Lock held on entry.
func testDriverP1ViolationLocked(sess *mqttSession, gen string) string {
	if sess.gen != gen || sess.custodyObserving {
		return _EMPTY_
	}
	var owed []string
	if len(sess.custodyHints) > 0 {
		owed = append(owed, "hints")
	}
	if sess.custodyPassWanted || len(sess.custodyPassQueue) > 0 {
		owed = append(owed, "pass")
	}
	if len(sess.custodyRetry) > 0 {
		owed = append(owed, "retries")
	}
	if len(sess.withdrawOwed) > 0 {
		owed = append(owed, "withdrawals")
	}
	if len(sess.admissionContinuity) > 0 {
		owed = append(owed, "continuity")
	}
	if sess.custodyDemand {
		owed = append(owed, "demand")
	}
	if sess.custody != nil && sess.custody.censusWanted {
		owed = append(owed, "census")
	}
	if sess.floorWanted {
		owed = append(owed, "floor")
	}
	if len(owed) == 0 || !sess.custodyTickAt.IsZero() {
		return _EMPTY_
	}
	select {
	case <-sess.leases.closingSignal():
		return _EMPTY_
	default:
	}
	if srv := sess.serverFor(nil); srv == nil || !srv.willDispatchOpen() {
		return _EMPTY_
	}
	return fmt.Sprintf("an exit of generation %q left %v owed with no run, no tick, an open incarnation and an "+
		"open dispatch gate", gen, owed)
}

// testDriverP2 asserts PROPERTY P2 (quiescence) of the dispatcher lifecycle matrix: an item whose
// subject is decided retires at its next unit. Called at the END of a test whose fixture has
// decided every subject its items name, with the store answering: within the bound no read
// retry, owed withdrawal or continuity record is left, nothing else is owed (hints, pass work, a
// latched demand, a census), no run observes, and no tick is armed (neither the session's deadline
// nor a registered timer). A test that deliberately leaves work owed does not call it.
func testDriverP2(t *testing.T, sess *mqttSession, srv *Server) {
	t.Helper()
	var left []string
	deadline := time.Now().Add(testCustodyBound)
	for {
		sess.mu.Lock()
		left = testDriverP2LeftLocked(sess)
		sess.mu.Unlock()
		if len(left) == 0 && testDriverTickArmed(sess, srv) {
			left = append(left, "a registered tick")
		}
		if len(left) == 0 || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(left) != 0 {
		t.Fatalf("property P2: not quiescent within %v with every subject decided: %v", testCustodyBound, left)
	}
}

// testDriverP2LeftLocked is what keeps the dispatcher from quiescence. Lock held on entry.
func testDriverP2LeftLocked(sess *mqttSession) []string {
	var left []string
	if n := len(sess.custodyRetry); n > 0 {
		left = append(left, fmt.Sprintf("%d read retries", n))
	}
	if n := len(sess.withdrawOwed); n > 0 {
		left = append(left, fmt.Sprintf("%d owed withdrawals", n))
	}
	if n := len(sess.admissionContinuity); n > 0 {
		left = append(left, fmt.Sprintf("%d continuity records", n))
	}
	if len(sess.custodyHints) > 0 || sess.custodyPassWanted || len(sess.custodyPassQueue) > 0 {
		left = append(left, "hints or pass work")
	}
	if sess.custodyDemand {
		left = append(left, "a latched demand")
	}
	if sess.custody != nil && sess.custody.censusWanted {
		left = append(left, "an owed census")
	}
	if sess.floorWanted {
		left = append(left, "an owed floor unit")
	}
	if sess.custodyObserving {
		left = append(left, "a run observing")
	}
	if !sess.custodyTickAt.IsZero() {
		left = append(left, "a tick deadline")
	}
	return left
}

// testDriverDispatched counts the target units dispatched for key, and those of them dispatched
// at or after since.
func (r *testDriverRec) testDriverDispatched(key string, since time.Time) (all, after int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, u := range r.units {
		if u.kind == mqttCustodyUnitTarget && u.key == key {
			all++
			if !u.at.Before(since) {
				after++
			}
		}
	}
	return all, after
}

// T2. A failed observation is RETRIED on the dispatcher's own cadence: AckWait 30 s, Receive
// Maximum 1; A's ending is observed through a read that first fails, and the withheld B is
// admitted within 5 s, with no further offer.
//
// Mutations M11a/b (never armed / tick callback a no-op): the release waits for AckWait.
func TestF097FailedObservationReleasesBeforeAckWait(t *testing.T) {
	t.Parallel()
	f, bReply := testDriverWithheld(t, "f097-t2", 1)
	sess := f.e.sess
	// AckWait is the default 30 s (mqttDefaultAckWait): the env sets none, so the dispatcher's cap
	// is min(5 s, AckWait/4) = 5 s.
	if aw := f.e.s.getOpts().MQTT.AckWait; aw != 0 && aw != mqttDefaultAckWait {
		t.Fatalf("fixture: AckWait must be the 30 s default, got %v", aw)
	}
	// GATE KICKS FROZEN AFTER THE FIRST (M11a/b's constraint): the withheld B's kick already ran
	// its pass and latched its demand. Once that run is idle, the latched demand and any tick
	// armed for it are withdrawn, and B is not offered again until the end - so after A's failed
	// read the dispatcher's own tick is the only thing that can re-read A.
	testDriverWait(t, "the gate kick's run is idle", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving && len(sess.custodyHints) == 0 && !sess.custodyPassWanted &&
			len(sess.custodyPassQueue) == 0
	})
	sess.mu.Lock()
	sess.custodyDemand, sess.custodyDemandDelay, sess.custodyDemandDueAt = false, 0, time.Time{}
	tickKey := sess.custodyTickKey
	sess.custodyTickAt, sess.custodyTickKey = time.Time{}, _EMPTY_
	sess.mu.Unlock()
	f.e.s.cancelWillTimer(tickKey)
	rec := testDriverInstall(sess)

	xsubj := mqttExchangeSubject(sess.idHash, f.gen, f.lin)
	var faults atomic.Int32
	var failedAt atomic.Int64 // starts recorded when A's read failed
	failedAt.Store(-1)
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyDecodeFault = func(subject string) bool {
		if subject == xsubj && faults.CompareAndSwap(0, 1) {
			failedAt.Store(int64(rec.startCount()))
			return true
		}
		return false
	}
	f.e.asm.mu.Unlock()
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(sess, nil), nil); r != mqttWithdrawLanded {
		t.Fatalf("fixture: A's withdrawal must land, got %v", r)
	}
	start := time.Now()
	// Evidence kept apart for the two mutations: whether the tick was ever armed after A's read
	// failed (M11a: never armed), and how many dispatcher runs STARTED after it (M11b: the tick
	// fires but its callback starts nothing).
	armedSeen := false
	released := false
	for deadline := start.Add(testCustodyBound); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if failedAt.Load() >= 0 && testDriverTickArmed(sess, f.e.s) {
			armedSeen = true
		}
		if testCustodyCapacity(sess) == 1 {
			released = true
			break
		}
	}
	startsAfter := 0
	if n := failedAt.Load(); n >= 0 {
		startsAfter = rec.startCount() - int(n)
	}
	if faults.Load() == 0 {
		t.Fatal("fixture: the first observation of A's ending never failed")
	}
	// TARGET (M11a/b): A's charge is released by the dispatcher's own retry, before AckWait.
	if !released {
		t.Fatalf("not within %v: A's charge released by the dispatcher's retry (tick armed after the failed read: %v; "+
			"runs started after it: %d)", testCustodyBound, armedSeen, startsAfter)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the release took %v, not before AckWait's share", took)
	}
	// The retry came from a run that STARTED after the failed read (the tick's), not from the run
	// that failed it.
	if startsAfter == 0 {
		t.Fatalf("the release must come from a run started after A's failed read (tick armed: %v)", armedSeen)
	}
	// Positive control: B is admitted through the gate now.
	sess.mu.Lock()
	bPI, bNew := sess.trackPublish(testRealPubRelSrcDur, bReply)
	sess.mu.Unlock()
	if bPI == 0 || !bNew {
		t.Fatalf("B must be admitted once A's charge is released, got %d (new=%v)", bPI, bNew)
	}
	testDriverP2(t, sess, f.e.s)
}

// T2b. A tick paused BEFORE the start site's admission and lease, across a clean start: no unit of
// the old generation is dispatched.
func TestF097DriverTickRefusedAcrossClear(t *testing.T) {
	t.Parallel()
	f, _ := testDriverWithheld(t, "f097-t2b", 1)
	sess := f.e.sess
	rec := testDriverInstall(sess)
	testDriverP1(t, sess)
	testDriverWait(t, "the latched demand arms the tick", 5*time.Second, func() bool {
		return testDriverTickArmed(sess, f.e.s)
	})
	oldGen := f.gen
	reached, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	rec.mu.Lock()
	rec.startEntry = func() {
		if once.CompareAndSwap(false, true) {
			close(reached)
			<-release
		}
	}
	rec.mu.Unlock()
	select {
	case <-reached:
	case <-time.After(8 * time.Second):
		t.Fatal("the tick never reached the start site")
	}
	// A clean start retires the incarnation only with no client bound (the disconnect came first).
	sess.mu.Lock()
	sess.c = nil
	sess.mu.Unlock()
	if err := sess.clear(false); err != nil {
		close(release)
		t.Fatalf("clear: %v", err)
	}
	rec.mu.Lock()
	cut := len(rec.units)
	rec.mu.Unlock()
	close(release)
	time.Sleep(300 * time.Millisecond)
	rec.mu.Lock()
	for _, u := range rec.units[cut:] {
		if u.gen == oldGen {
			rec.mu.Unlock()
			t.Fatalf("a unit of the cleared generation %q was dispatched after the clear", oldGen)
		}
	}
	rec.mu.Unlock()
	testDriverP2(t, sess, f.e.s)
}

// T2b, the RUNNING-PASS schedule (M13's disposition: UNREACHABLE, this is its witness). M13
// removes the per-unit (generation, epoch, ledger) checks; they could only matter for a unit of a
// run that STARTED under one generation and is dispatched after the generation changed. That
// schedule is forbidden: a generation (and its ledger) is re-minted only by setIncarnationLocked,
// which requires a retirement permit, which retireIncarnationLocked mints only for a CLOSED tracker
// with no unpromoted owner - and every dispatcher run holds an incarnation lease from before its
// single-flight flag until its loop exits. Witness: a run paused INSIDE a unit (lease held) keeps
// clear() from completing - the generation is unchanged while the unit is held - and the clear
// completes only after the run released its lease.
func TestF097DriverRunHoldsItsGenerationAcrossClear(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-t2b-run", 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	oldGen := f.gen
	reached, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var mu sync.Mutex
	var after []string // generations of units dispatched after the clear returned
	cleared := false
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{unit: func(gen string, u mqttCustodyUnit) {
		mu.Lock()
		if cleared {
			after = append(after, gen)
		}
		mu.Unlock()
		if once.CompareAndSwap(false, true) {
			close(reached)
			<-release
		}
	}}
	sess.mu.Unlock()
	testDriverP1(t, sess)
	sess.mu.Lock()
	sess.kickCustodyPassLocked() // A's exchange is a pass target: the run dispatches a unit for it
	sess.mu.Unlock()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture: the run never dispatched a unit")
	}
	sess.mu.Lock()
	sess.c = nil // a clean start retires the incarnation only with no client bound
	sess.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- sess.clear(false) }()
	select {
	case err := <-done:
		close(release)
		t.Fatalf("clear() completed (%v) while a dispatcher run held its lease inside a unit", err)
	case <-time.After(300 * time.Millisecond):
	}
	sess.mu.Lock()
	genHeld := sess.gen
	sess.mu.Unlock()
	if genHeld != oldGen {
		close(release)
		t.Fatalf("the generation changed (%q -> %q) while the run's unit was held", oldGen, genHeld)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clear after the run released its lease: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("clear() did not complete after the run released its lease")
	}
	mu.Lock()
	cleared = true
	mu.Unlock()
	sess.mu.Lock()
	newGen := sess.gen
	sess.mu.Unlock()
	if newGen == oldGen {
		t.Fatal("fixture: the clear must re-mint the generation")
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	for _, g := range after {
		if g == oldGen {
			mu.Unlock()
			t.Fatalf("a unit of the cleared generation %q was dispatched after the clear", oldGen)
		}
	}
	mu.Unlock()
	testDriverP2(t, sess, f.e.s)
}

// T2c. Cancelling the tick releases its timer token: shutdown's timer and worker joins complete.
func TestF097DriverStopReleasesTheTimerToken(t *testing.T) {
	t.Parallel()
	f, _ := testDriverWithheld(t, "f097-t2c", 1)
	sess := f.e.sess
	testDriverP1(t, sess)
	testDriverWait(t, "the tick is armed", 5*time.Second, func() bool { return testDriverTickArmed(sess, f.e.s) })
	sess.mu.Lock()
	key := sess.custodyTickKey
	sess.mu.Unlock()
	f.e.s.cancelWillTimer(key)
	f.e.s.closeWillDispatch()
	done := make(chan struct{})
	go func() {
		f.e.s.willTimerWG.Wait()
		f.e.s.willWorkerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown's joins did not complete after the tick was cancelled")
	}
}

// T2d. The tick fires and is cancelled at the same time: exactly one side releases its token
// (shutdown's join neither hangs nor sees a negative count).
//
// The delays are this session's own: a package-level override outlived the test in a run its
// tick had started, and raced the next test that wrote it.
func TestF097DriverStopFireRace(t *testing.T) {
	t.Parallel()
	f, _ := testDriverWithheld(t, "f097-t2d", 1)
	sess := f.e.sess
	testDriverP1(t, sess)
	sess.mu.Lock()
	sess.testCustodyDriver.delays = func() (time.Duration, time.Duration) { return time.Millisecond, 2 * time.Millisecond }
	sess.mu.Unlock()
	for i := 0; i < 20; i++ {
		var key string
		testDriverWait(t, "the tick re-arms", 5*time.Second, func() bool {
			sess.mu.Lock()
			key = sess.custodyTickKey
			sess.mu.Unlock()
			return testDriverTickArmed(sess, f.e.s)
		})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); f.e.s.cancelWillTimer(key) }()
		wg.Wait()
		// The fixture's external cancellation is no production caller (those reset the deadline
		// or re-arm themselves): forget the deadline and let the latched demand re-arm a tick.
		testDriverWait(t, "the run ends", 5*time.Second, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			if sess.custodyObserving {
				return false
			}
			sess.custodyTickAt = time.Time{}
			return true
		})
		sess.mu.Lock()
		sess.kickCustodyPassLocked()
		sess.mu.Unlock()
	}
	f.e.s.closeWillDispatch()
	done := make(chan struct{})
	go func() { f.e.s.willTimerWG.Wait(); f.e.s.willWorkerWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a token was lost between a firing tick and its cancellation")
	}
}

// T2e. A CLOSING incarnation refuses every start; its reopen re-arms the dispatcher, and the
// withheld delivery is then admitted.
func TestF097DriverCloseThenReopen(t *testing.T) {
	t.Parallel()
	f, bReply := testDriverWithheld(t, "f097-t2e", 1)
	sess := f.e.sess
	// Quiesce: let the gate kick's own run finish before the incarnation starts closing. The kick
	// starts its run on a goroutine, so an unset flag alone may precede that run: its exit is
	// what arms the latched demand's tick, and that tick must not be about to fire.
	testDriverWait(t, "the first run ends", 5*time.Second, func() bool {
		sess.mu.Lock()
		idle := !sess.custodyObserving && sess.custodyTickAt.After(time.Now().Add(30*time.Millisecond))
		sess.mu.Unlock()
		return idle && testDriverTickArmed(sess, f.e.s)
	})
	rec := testDriverInstall(sess)
	testDriverP1(t, sess)
	sess.mu.Lock()
	leases := sess.leases
	sess.mu.Unlock()
	leases.close()
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(sess, nil), nil); r != mqttWithdrawLanded {
		t.Fatalf("fixture: A's withdrawal must land, got %v", r)
	}
	testCustodyStaysFor(t, "a closing incarnation starts no dispatcher", 300*time.Millisecond, func() bool {
		return rec.startCount() == 0 && testCustodyCapacity(sess) == 0
	})
	leases.reopen()
	testCustodyEventually(t, "the reopen re-arms the dispatcher and A's charge is released", func() bool {
		return testCustodyCapacity(sess) == 1
	})
	if rec.startCount() == 0 {
		t.Fatal("the release must come from a dispatcher run started after the reopen")
	}
	sess.mu.Lock()
	bPI, _ := sess.trackPublish(testRealPubRelSrcDur, bReply)
	sess.mu.Unlock()
	if bPI == 0 {
		t.Fatal("the withheld delivery must be admitted after the reopen")
	}
	testDriverP2(t, sess, f.e.s)
}

// T2f. Under CONTINUOUS hints, an owed withdrawal item and a gate request each complete within 3
// iterations of being registered.
func TestF097DispatcherIsFairUnderContinuousHints(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-t2f", 1)
	sess := f.e.sess
	sess.mu.Lock()
	sess.maxp = 100
	sess.mu.Unlock()
	// Hinted exchanges: several admitted, transmitted-or-not obligations the ledger holds.
	var lins []string
	for i := uint64(1); i <= 4; i++ {
		g := testWithdrawAdmitMore(t, f.e, testRealPubRelSrcSeq+10+i, 1)
		lins = append(lins, g.lin)
	}
	rec := testDriverInstall(sess)
	stop := make(chan struct{})
	var hinters sync.WaitGroup
	hinters.Add(1)
	go func() {
		defer hinters.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, lin := range lins {
				sess.hintCustody(f.gen, lin)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	defer func() { close(stop); hinters.Wait() }()
	testDriverWait(t, "the hints keep the dispatcher iterating", 5*time.Second, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.iterations >= 2
	})
	// THE WINDOW IS MEASURED FROM WHAT RAN (F-123). The dispatcher is parked inside its iteration
	// seam, which it reaches after selecting that iteration's units and holding no lock; the item
	// and the gate request are registered while it is parked, so the parked iteration is the last
	// one whose units were chosen without them. Completion is then read from execution, not from
	// the queues: selection dequeues pass targets BEFORE the seam runs, so an empty pass queue at
	// iteration n's seam means its last targets run DURING n. The item is done in iteration n-1
	// when its owed withdrawal is gone at n's seam; the gate request is done in the last iteration
	// that executed a unit from the pass queue, read once a later seam proves that iteration ran.
	parked := make(chan int, 1)
	release := make(chan struct{})
	done := make(chan [2]int, 1)
	var phase atomic.Int32           // 0 idle, 1 park the next iteration, 2 measure
	itemDone, passSelectedBy := 0, 0 // touched by the dispatcher goroutine only
	rec.mu.Lock()
	rec.iteration = func(n int) {
		switch phase.Load() {
		case 1:
			if phase.CompareAndSwap(1, 2) {
				parked <- n
				<-release
			}
		case 2:
			sess.mu.Lock()
			itemGone := len(sess.withdrawOwed) == 0
			passGone := !sess.custodyPassWanted && len(sess.custodyPassQueue) == 0
			sess.mu.Unlock()
			if itemGone && itemDone == 0 {
				itemDone = n - 1
			}
			if passGone && passSelectedBy == 0 {
				passSelectedBy = n
			}
			if itemDone != 0 && passSelectedBy != 0 && n > passSelectedBy {
				select {
				case done <- [2]int{itemDone, passSelectedBy}:
				default:
				}
			}
		}
	}
	rec.mu.Unlock()
	phase.Store(1)
	var base int
	select {
	case base = <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("the dispatcher never reached an iteration to park in")
	}
	sess.recordWithdrawOwed(f.gen, f.subject, f.nonce, f.lin, f.seqOf(t), mqttTestLicence(sess, nil))
	sess.mu.Lock()
	sess.kickCustodyPassLocked()
	sess.mu.Unlock()
	close(release)
	var marks [2]int
	select {
	case marks = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the owed withdrawal and the gate request did not complete")
	}
	gateDone := base
	rec.mu.Lock()
	for _, u := range rec.units {
		if u.queue == mqttCustodyQueuePass && u.iter > base && u.iter <= marks[1] && u.iter > gateDone {
			gateDone = u.iter
		}
	}
	rec.mu.Unlock()
	if marks[0]-base > 3 || gateDone-base > 3 {
		t.Fatalf("under continuous hints the item took %d iterations and the gate request %d, want each <= 3",
			marks[0]-base, gateDone-base)
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("the owed withdrawal must land, got %+v", x)
	}
}

// T2h. Shutdown JOINS a dispatcher blocked inside a withdrawal's store request, and no unit is
// dispatched after the dispatch gate closed.
func TestF097ShutdownJoinsABlockedDispatcher(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-t2h", 1)
	sess := f.e.sess
	rec := testDriverInstall(sess)
	blocked, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if once.CompareAndSwap(false, true) {
			close(blocked)
			<-release
			return mqttOpOutcome{}, errors.New("test: the store request was released by the fixture")
		}
		return write()
	}
	f.e.asm.mu.Unlock()
	sess.recordWithdrawOwed(f.gen, f.subject, f.nonce, f.lin, f.seqOf(t), mqttTestLicence(sess, nil))
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the dispatcher never reached the withdrawal's store request")
	}
	f.e.s.closeWillDispatch()
	closedAt := time.Now()
	done := make(chan struct{})
	go func() { f.e.s.willTimerWG.Wait(); f.e.s.willWorkerWG.Wait(); close(done) }()
	select {
	case <-done:
		close(release)
		t.Fatal("shutdown completed while the dispatcher was blocked inside a store request")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not complete after the blocked request returned")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, u := range rec.units {
		if u.at.After(closedAt) {
			t.Fatalf("a unit (kind %d) was dispatched after the dispatch gate closed", u.kind)
		}
	}
}

// T2i. After dispatch closes, NO start source starts a run: a hint, a gate request, an item, and
// the tick's own call.
func TestF097EveryDispatcherStartIsAdmitted(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-t2i", 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	rec := testDriverInstall(sess)
	f.e.s.closeWillDispatch()
	sess.hintCustody(f.gen, f.lin)
	sess.mu.Lock()
	sess.kickCustodyPassLocked()
	sess.kickCustodyItemsLocked()
	sess.mu.Unlock()
	sess.recordWithdrawOwed(f.gen, f.subject, f.nonce, f.lin, f.seqOf(t), mqttTestLicence(sess, nil))
	sess.startCustodyDispatcher() // the tick's call
	testCustodyStaysFor(t, "no run starts after dispatch closed", 300*time.Millisecond, func() bool {
		return rec.startCount() == 0
	})
}

// seqOf is the register's current sequence.
func (f *testWithdrawFix) seqOf(t *testing.T) uint64 {
	t.Helper()
	_, seq := f.register(t)
	if seq == 0 {
		t.Fatal("fixture: the register must stand")
	}
	return seq
}

// T2g (consult C3, course A). The dispatcher's contract, measured at the JSA request boundary:
// each row's bound for ONE unit invoked synchronously in a quiescent session (attribution by the
// call: the single-flight flag is held so no other dispatcher run can issue requests, and the env's
// JetStream assistant is private to this test); at most eight units per iteration; the census at
// most once per cap interval across competing triggers.
func TestF097DispatcherCostBound(t *testing.T) {
	t.Parallel()
	count := func(t *testing.T, f *testWithdrawFix, fn func()) int {
		t.Helper()
		// Attributed to THIS session: a request that names neither its client hash nor its
		// identifier is an account-level background request (the Will claim loop, ...), not the
		// unit's. Each counted request is logged, so a reader sees what was attributed.
		var n atomic.Int32
		idHash := f.e.sess.idHash
		hook := func(subject string, body []byte) bool {
			if !strings.Contains(subject, idHash) && !strings.Contains(string(body), idHash) {
				return false
			}
			n.Add(1)
			t.Logf("request %d: %s %.160s", n.Load(), subject, body)
			return false
		}
		f.e.asm.jsa.testDropRequest.Store(&hook)
		defer f.e.asm.jsa.testDropRequest.Store(nil)
		fn()
		return int(n.Load())
	}
	quiesce := func(t *testing.T, sess *mqttSession) func() {
		t.Helper()
		testDriverWait(t, "quiescent", 5*time.Second, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			if sess.custodyObserving {
				return false
			}
			sess.custodyObserving = true // hold the single-flight flag: no run can start
			return true
		})
		return func() {
			sess.mu.Lock()
			sess.custodyObserving = false
			sess.mu.Unlock()
		}
	}
	unit := func(sess *mqttSession, u mqttCustodyUnit) {
		sess.mu.Lock()
		gen, epoch, ledger := sess.gen, sess.asm.sessStreamEpoch, sess.custody
		sess.mu.Unlock()
		sess.runCustodyUnit(gen, epoch, ledger, u)
	}
	t.Run("observation-row", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "f097-t2g-obs", 1)
		defer quiesce(t, f.e.sess)()
		n := count(t, f, func() {
			unit(f.e.sess, mqttCustodyUnit{kind: mqttCustodyUnitTarget,
				target: mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: f.lin}})
		})
		if n > 1 {
			t.Fatalf("an exchange observation issued %d requests, row bound 1", n)
		}
	})
	t.Run("ordinary-withdrawal-row-h2", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "f097-t2g-wd", 2) // QoS 2: source + canonical PUBREL holds
		defer quiesce(t, f.e.sess)()
		w := &mqttWithdrawOwed{gen: f.gen, epoch: f.e.asm.sessStreamEpoch, subject: f.subject, nonce: f.nonce,
			lin: f.lin, regSeq: f.seqOf(t), lic: mqttTestLicence(f.e.sess, nil)}
		n := count(t, f, func() { unit(f.e.sess, mqttCustodyUnit{kind: mqttCustodyUnitWithdraw, w: w}) })
		if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
			t.Fatalf("fixture: the unit's withdrawal must land, got %+v", x)
		}
		t.Logf("measured: one ordinary QoS 2 withdrawal unit issued %d requests (row bound 19)", n)
		if n > 19 {
			t.Fatalf("an ordinary withdrawal unit issued %d requests, row bound 15 + 2h <= 19", n)
		}
	})
	t.Run("eight-units-per-iteration", func(t *testing.T) {
		f := testWithdrawAdmitted(t, "f097-t2g-eight", 1)
		sess := f.e.sess
		sess.mu.Lock()
		sess.maxp = 100
		sess.mu.Unlock()
		release := quiesce(t, sess)
		var fixes []*testWithdrawFix
		for i := uint64(1); i <= 10; i++ {
			fixes = append(fixes, testWithdrawAdmitMore(t, f.e, testRealPubRelSrcSeq+20+i, 1))
		}
		rec := testDriverInstall(sess)
		max := 0
		rec.iteration = nil
		sess.mu.Lock()
		sess.testCustodyDriver.iteration = func(gen string, units []mqttCustodyUnit, census bool) {
			rec.mu.Lock()
			if len(units) > max {
				max = len(units)
			}
			rec.iterations++
			rec.mu.Unlock()
		}
		sess.mu.Unlock()
		for _, g := range fixes {
			sess.recordWithdrawOwed(g.gen, g.subject, g.nonce, g.lin, g.seqOf(t), mqttTestLicence(sess, nil))
		}
		release()
		sess.startCustodyDispatcher()
		testDriverWait(t, "every owed withdrawal runs", 10*time.Second, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return len(sess.withdrawOwed) == 0
		})
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if max == 0 || max > mqttCustodyDispatchUnits {
			t.Fatalf("an iteration ran %d units, want 1..%d", max, mqttCustodyDispatchUnits)
		}
		if rec.iterations < 2 {
			t.Fatalf("ten due items must take more than one iteration, took %d", rec.iterations)
		}
	})
	// MOVED-PROBE delta (WP3 edit 7; M43's target): a same-delivery, marker-absent, `Cancelled`
	// occupant that reaches BOTH the pre-classification probe site and the register-only
	// replacement site costs exactly ONE marker probe across the two, for an ordinary AND an
	// experimental affine occupant. Counted at the instance marker-load seam while the ladder runs
	// synchronously in a quiescent env; a probe is attributed to the cancelled occupant by the
	// register standing beside it at the probe (the fixture's class tag), so a probe after the
	// replacement landed is not counted against the two sites. Site witnesses: the
	// pre-classification probe ran (a tagged probe exists) and the replacement site was reached
	// (the register-only replacement landed, inheriting the occupant's lineage).
	t.Run("moved-probe", func(t *testing.T) {
		countTagged := func(events []string, tag string) int {
			n := 0
			for _, ev := range events {
				if ev == tag {
					n++
				}
			}
			return n
		}
		t.Run("ordinary-replacement", func(t *testing.T) {
			o := testF088OrdinaryCancelledOccupant(t, "f097-t2g-probe-ord", true)
			e := o.e
			if !e.sess.admitInflightPublish(e.c, o.pi, 1, o.dur, o.sseq, o.nonce, _EMPTY_, _EMPTY_) {
				t.Fatalf("fixture: the allocation over the refuted cancellation must be admitted (events %v)", o.log())
			}
			rec, _ := e.loadRegister(t, 11)
			events := o.log()
			if rec == nil || rec.Nonce != o.nonce || rec.Lin != o.occ.Lin {
				t.Fatalf("witness: the replacement site must be reached (register-only replacement inheriting %q), got %+v",
					o.occ.Lin, rec)
			}
			n := countTagged(events, "probe(ordinary cancelled)")
			if n == 0 {
				t.Fatalf("witness: the pre-classification probe never ran (events %v)", events)
			}
			if n != 1 {
				t.Fatalf("the classification and replacement sites must cost exactly ONE marker probe, got %d (events %v)",
					n, events)
			}
		})
		t.Run("affine-replacement", func(t *testing.T) {
			e := testNewInflightEnv(t, "f097-t2g-probe-aff")
			affDur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_" + nuid.Next()[:6]
			const sseq = 9
			subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, 11)
			occ := freshRegisterLineage(&mqttInflightRecord{State: mqttInflightPublish, PI: 11, Dur: affDur, Seq: sseq,
				QoS: 2, Nonce: mqttNewInflightNonce()})
			if _, err := e.asm.storeInflightPair(subject, 0, 0, occ); err != nil {
				t.Fatalf("fixture: seeding the affine occupant pair: %v", err)
			}
			e.sess.mu.Lock()
			e.sess.last_pi = 10
			e.sess.mu.Unlock()
			o := testF088CancelledOccupantOn(t, e, affDur, sseq, 2, occ, true)
			if !mqttDurIsSlotOnlyClass(o.occ.Dur) {
				t.Fatalf("fixture: the occupant must be slot-only, got %+v", o.occ)
			}
			stored := e.sess.storeInflightRegister(e.c, o.pi, 2, affDur, sseq, o.nonce, _EMPTY_, _EMPTY_)
			rec, _ := e.loadRegister(t, 11)
			events := o.log()
			if !stored || rec == nil || rec.Nonce != o.nonce || rec.Lin != o.occ.Lin {
				t.Fatalf("witness: the replacement site must be reached (register-only replacement inheriting %q), "+
					"stored %v, got %+v (events %v)", o.occ.Lin, stored, rec, events)
			}
			n := countTagged(events, "probe(slot-only cancelled)")
			if n == 0 {
				t.Fatalf("witness: the pre-classification probe never ran for the slot-only class (events %v)", events)
			}
			if n != 1 {
				t.Fatalf("the classification and replacement sites must cost exactly ONE marker probe, got %d (events %v)",
					n, events)
			}
		})
		t.Run("abandon-adds-one", func(t *testing.T) {
			// A classification that ends in an ABANDON (the marker cannot be read): the probe is the
			// ADDED read of the cost delta, exactly one.
			o := testF088OrdinaryCancelledOccupant(t, "f097-t2g-probe-abandon", false)
			e := o.e
			markerSubject := o.subject + "." + mqttInflightMarkerSuffix
			e.asm.mu.Lock()
			e.asm.testInflightMarkerLoad = func(subject string) (*StoredMsg, error) {
				if subject == markerSubject {
					o.probes.Add(1)
					return nil, errors.New("test: the marker read did not resolve")
				}
				return e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
			}
			e.asm.mu.Unlock()
			if e.sess.admitInflightPublish(e.c, o.pi, 1, o.dur, o.sseq, o.nonce, _EMPTY_, _EMPTY_) {
				t.Fatal("fixture: the allocation over an undecided cancelled occupant must abandon")
			}
			if n := o.probes.Load(); n != 1 {
				t.Fatalf("an abandoning classification must add exactly one marker probe, got %d", n)
			}
		})
	})
	// WP3 BOUNDARY-EVIDENCE delta (M42's target): the acknowledgement advance's write boundary reads
	// the unsent marker exactly ONCE when its member clears a cancellation (the qualifying
	// evaluation), and ZERO times when it clears nothing. Counted at the JSA request boundary, only
	// INSIDE the advance's write (the testAckAdvance wrapper), so the terminal cleanup's own marker
	// reap after the advance is not attributed to the boundary.
	t.Run("marker-read-delta", func(t *testing.T) {
		advanceMarkerReads := func(t *testing.T, f *testWithdrawFix, complete func()) (int, int) {
			t.Helper()
			markerSubject := f.subject + "." + mqttInflightMarkerSuffix
			var counting atomic.Bool
			var reads, advances atomic.Int32
			hook := func(subject string, body []byte) bool {
				if counting.Load() && (strings.Contains(subject, markerSubject) || strings.Contains(string(body), markerSubject)) {
					reads.Add(1)
				}
				return false
			}
			f.e.asm.jsa.testDropRequest.Store(&hook)
			defer f.e.asm.jsa.testDropRequest.Store(nil)
			f.e.asm.mu.Lock()
			f.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				advances.Add(1)
				counting.Store(true)
				defer counting.Store(false)
				return write()
			}
			f.e.asm.mu.Unlock()
			complete()
			testCustodyEventually(t, "the advance ends the exchange", func() bool {
				x := f.exchange(t)
				return x != nil && x.Phase == mqttExchangeTerminal
			})
			return int(reads.Load()), int(advances.Load())
		}
		t.Run("non-clearing-reads-none", func(t *testing.T) {
			f := testLateAckTransmitted(t, "f097-t2g-delta-plain", 1)
			if cur, _ := f.register(t); cur == nil || cur.Cancelled {
				t.Fatalf("fixture: a register with no cancellation to clear is required, got %+v", cur)
			}
			reads, advances := advanceMarkerReads(t, f.testWithdrawFix, func() {
				if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
					t.Fatalf("PUBACK: %v", err)
				}
			})
			if advances == 0 {
				t.Fatal("witness: the acknowledgement advance never ran")
			}
			if reads != 0 {
				t.Fatalf("a boundary evaluation that clears nothing must read no marker, read %d over %d advance(s)", reads, advances)
			}
		})
		t.Run("clearing-reads-one", func(t *testing.T) {
			// Positive control: T35's occupant cancellation over an admitted register whose marker an
			// intent consumed; the late PUBACK's advance clears it on the marker's authoritative
			// absence.
			f, _, _ := testF088CopiedFreshTombstone(t, "f097-t2g-delta-clear")
			f.e.sess.mu.Lock()
			ack := f.e.sess.pendingPublish[f.pi]
			f.e.sess.mu.Unlock()
			if ack == nil {
				t.Fatal("fixture: the delivery's entry must still stand")
			}
			reads, advances := advanceMarkerReads(t, f, func() { f.e.sess.completeAcknowledgedPublish(f.e.c, f.pi, ack) })
			if reads != advances || advances == 0 {
				t.Fatalf("each clearing boundary evaluation must read the marker exactly once: %d read(s) over %d advance(s)",
					reads, advances)
			}
		})
	})
	// HANDOFF BOUNDARY (M40's disposition). At most one direct handoff per unit; in this tree NO
	// dispatcher unit hands off at all: the unit bodies (observeCustodyTarget, settleCustodyAttempt,
	// runWithdrawOwedUnit -> withdrawUnsent, runAdmissionContinuityUnit) reach none of the handoff
	// entry points (scheduleTerminalRegisterRelease / armTerminalRetryLocked,
	// scheduleIntentResolution, scheduleOwedMaterialization), whose callers are the ACK handler,
	// the delivery callbacks, recovery, the send sections and the intent worker (call-graph sweep,
	// recorded in the lane). Witness, sampled: the two handoff registries a unit could grow - the
	// session's terminal retries and the server's owed intents - do not grow across an
	// observation unit and a withdrawal unit; positive control: an acknowledgement whose first
	// advance fails DOES register a terminal retry, so the registry read sees a handoff.
	t.Run("handoff-count", func(t *testing.T) {
		handoffs := func(f *testWithdrawFix) int {
			sess := f.e.sess
			sess.mu.Lock()
			n := len(sess.terminalRetries)
			sess.mu.Unlock()
			s := f.e.s
			s.owedIntentsMu.Lock()
			for k := range s.owedIntents {
				if k.sess == sess {
					n++
				}
			}
			s.owedIntentsMu.Unlock()
			return n
		}
		t.Run("positive-control", func(t *testing.T) {
			f := testLateAckTransmitted(t, "f097-t2g-handoff-ctl", 1)
			var refused atomic.Bool
			f.e.asm.mu.Lock()
			f.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				if refused.CompareAndSwap(false, true) {
					return mqttOpOutcome{}, errors.New("test: the first advance fails")
				}
				return write()
			}
			f.e.asm.mu.Unlock()
			before := handoffs(f.testWithdrawFix)
			if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
				t.Fatalf("PUBACK: %v", err)
			}
			if after := handoffs(f.testWithdrawFix); !refused.Load() || after <= before {
				t.Fatalf("control: a failed first advance must hand off to the terminal retry (refused %v, %d -> %d)",
					refused.Load(), before, after)
			}
		})
		t.Run("units", func(t *testing.T) {
			f := testWithdrawAdmitted(t, "f097-t2g-handoff", 2)
			defer quiesce(t, f.e.sess)()
			before := handoffs(f)
			unit(f.e.sess, mqttCustodyUnit{kind: mqttCustodyUnitTarget,
				target: mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: f.lin}})
			w := &mqttWithdrawOwed{gen: f.gen, epoch: f.e.asm.sessStreamEpoch, subject: f.subject, nonce: f.nonce,
				lin: f.lin, regSeq: f.seqOf(t), lic: mqttTestLicence(f.e.sess, nil)}
			unit(f.e.sess, mqttCustodyUnit{kind: mqttCustodyUnitWithdraw, w: w})
			if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
				t.Fatalf("fixture: the withdrawal unit must land, got %+v", x)
			}
			unit(f.e.sess, mqttCustodyUnit{kind: mqttCustodyUnitTarget,
				target: mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: f.lin}})
			if after := handoffs(f); after != before {
				t.Fatalf("a dispatcher unit handed off: handoff registries %d -> %d", before, after)
			}
		})
	})
	t.Run("census-once-per-cap-interval", func(t *testing.T) {
		f, _ := testDriverWithheld(t, "f097-t2g-census", 1)
		sess := f.e.sess
		// A STATE THAT WANTS A CENSUS ON EVERY PASS (M41's input): the reconstruction is
		// incomplete and every census fails, so it stays incomplete and each requested pass asks
		// for another census. Only the cap-interval throttle keeps them apart.
		var traversals atomic.Int32
		f.e.asm.mu.Lock()
		f.e.asm.testCustodyCensus = func() error {
			traversals.Add(1)
			return errors.New("test: the inventory could not be read")
		}
		f.e.asm.mu.Unlock()
		sess.mu.Lock()
		sess.custody.beginReconstruction(sess.gen)
		sess.mu.Unlock()
		rec := testDriverInstall(sess)
		// Competing triggers within one cap interval (AckWait default -> cap 5 s): gate kicks,
		// hints and the tick (the withheld delivery's latched demand).
		for i := 0; i < 5; i++ {
			sess.mu.Lock()
			sess.kickCustodyPassLocked()
			sess.mu.Unlock()
			sess.hintCustody(f.gen, f.lin)
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
		sess.mu.Lock()
		withholds, wanted := sess.custody.withholdsNew(), sess.custody.censusWanted
		sess.mu.Unlock()
		rec.mu.Lock()
		defer rec.mu.Unlock()
		// Witness: the census was wanted (it ran, failed, and is wanted again) - the throttle had
		// something to throttle.
		if len(rec.census) == 0 || !withholds || !wanted {
			t.Fatalf("fixture: an incomplete reconstruction must keep wanting the census (ran %d, withholds %v, wanted %v)",
				len(rec.census), withholds, wanted)
		}
		// ONE history traversal per admitted census, not multiplied by the queued triggers.
		if n := int(traversals.Load()); n != len(rec.census) {
			t.Fatalf("%d census admissions issued %d inventory traversals", len(rec.census), n)
		}
		for i := 1; i < len(rec.census); i++ {
			if rec.census[i].Sub(rec.census[i-1]) < mqttCustodyBaseDelay {
				t.Fatalf("the census ran twice within %v", rec.census[i].Sub(rec.census[i-1]))
			}
		}
		if len(rec.census) > 1 {
			t.Fatalf("the census ran %d times within one cap interval", len(rec.census))
		}
	})
}

// testDriverTimerRegistered reports whether a tracked timer is registered under key.
func testDriverTimerRegistered(srv *Server, key string) bool {
	srv.willDispatchMu.Lock()
	defer srv.willDispatchMu.Unlock()
	_, ok := srv.willTimers[key]
	return ok
}

// W-tick (F-097, consult C8 (2)). A run the tick started that exits while the tick's callback is
// still inside it - the firing timer still registered under its key - re-arms a tick of its own
// for its outstanding item, and that arm retries the item with no other trigger. Witness: the
// tick's callback is held after it reached the start site until the run it spawned has exited
// with a read retry due in the future; a DIFFERENT key is then registered for that deadline; once
// the callback returns and the store answers, the retry releases the exchange's charge.
//
// Mutation M-tick (every arm shares the generation's key: mqttCustodyTickKey(id, gen, 0)): the
// re-arm is refused while the firing tick's key is registered, the arm's deadline is cleared, and
// nothing is armed for the retry - "not within ...: a fresh arm is registered".
func TestF097DriverTickRearmsWhileItsCallbackRuns(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-wtick", 1)
	sess := f.e.sess
	srv := f.e.s
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	xsubj := mqttExchangeSubject(sess.idHash, f.gen, f.lin)
	var failing atomic.Bool
	failing.Store(true)
	var reads atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyDecodeFault = func(subject string) bool {
		if subject != xsubj {
			return false
		}
		reads.Add(1)
		return failing.Load()
	}
	f.e.asm.mu.Unlock()
	held := make(chan string, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHold := func() { releaseOnce.Do(func() { close(release) }) }
	// A callback parked in the hook keeps the timer's shutdown token: release it on every exit.
	t.Cleanup(releaseHold)
	var once atomic.Bool
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		// The retry's first deadline ~200 ms out, the re-armed one ~400 ms: no arm fires while the
		// fixture inspects the held state.
		delays: func() (time.Duration, time.Duration) { return 200 * time.Millisecond, 400 * time.Millisecond },
		tickStarted: func(gen, key string) {
			if once.CompareAndSwap(false, true) {
				held <- key
				<-release
			}
		},
	}
	sess.mu.Unlock()
	testDriverP1(t, sess)
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(sess, nil), nil); r != mqttWithdrawLanded {
		t.Fatalf("fixture: A's withdrawal must land, got %v", r)
	}
	var firingKey string
	select {
	case firingKey = <-held:
	case <-time.After(testCustodyBound):
		t.Fatal("fixture: the read retry's tick never fired")
	}
	// The run the held tick started (its flag was set before the hook ran) re-read A - the fault
	// holds - backed its retry off and exited. Its re-arm must be a fresh key, registered while the
	// firing key still is.
	testDriverWait(t, "a fresh arm is registered for the run's outstanding retry while the firing tick's "+
		"callback is held", testCustodyBound, func() bool {
		sess.mu.Lock()
		now := time.Now()
		r := sess.custodyRetry[f.lin]
		key, at, idle := sess.custodyTickKey, sess.custodyTickAt, !sess.custodyObserving
		sess.mu.Unlock()
		return idle && r != nil && r.nextEligibleAt.After(now) && !at.IsZero() && key != _EMPTY_ &&
			key != firingKey && testDriverTimerRegistered(srv, key)
	})
	if !testDriverTimerRegistered(srv, firingKey) {
		t.Fatal("fixture: the firing tick's key must still be registered while its callback is held")
	}
	failing.Store(false) // the store answers
	readsAtRelease := reads.Load()
	releaseHold()
	// No hint, pass or gate request follows: only the fresh arm can re-read A.
	testCustodyEventually(t, "the fresh arm's retry released A's charge", func() bool {
		return !testCustodyHasExchange(sess, f.lin)
	})
	if reads.Load() <= readsAtRelease {
		t.Fatal("A's charge was released without the retry re-reading its exchange")
	}
	testDriverP2(t, sess, srv)
}

// H-3 (F-097 holistic round 3; lifecycle matrix E14). A census that observes an exchange ended
// retires the exchange's read retry: A's failed observation installs its retry, the store
// recovers, and the resume's census observes A ended and removes its holder while the retry still
// stands, not yet due. The retry's next run finds nothing left to observe and ends it, and the
// dispatcher goes idle with no tick armed.
//
// Mutation M-retry (observeCustodyTarget's no-holder, no-binding exit keeps the retry): every run
// re-times the retry a cap interval out and re-arms the tick for it until the generation is
// discarded - "not within ...: the census-ended exchange's retry is retired and the dispatcher is
// idle with no tick armed".
func TestF097DriverCensusEndedExchangeRetiresItsRetry(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-h3-retry", 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	xsubj := mqttExchangeSubject(sess.idHash, f.gen, f.lin)
	var failing atomic.Bool
	failing.Store(true)
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyDecodeFault = func(subject string) bool { return failing.Load() && subject == xsubj }
	f.e.asm.mu.Unlock()
	var retryRuns atomic.Int32
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		// The retry's first deadline 0.8 - 1.2 s out: the census below runs before it is due.
		delays: func() (time.Duration, time.Duration) { return time.Second, time.Second },
		unit: func(gen string, u mqttCustodyUnit) {
			if u.queue == mqttCustodyQueueRetry && u.target.key == f.lin {
				retryRuns.Add(1)
			}
		},
	}
	sess.mu.Unlock()
	testDriverP1(t, sess)
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(sess, nil), nil); r != mqttWithdrawLanded {
		t.Fatalf("fixture: A's withdrawal must land, got %v", r)
	}
	testDriverWait(t, "A's failed observation installs its retry and the run ends", testCustodyBound, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving && sess.custodyRetry[f.lin] != nil && sess.custody.hasExchange(f.gen, f.lin)
	})
	failing.Store(false) // the store answers
	sess.rebuildCustodyForResume(f.gen)
	sess.mu.Lock()
	ended := !sess.custody.hasExchange(f.gen, f.lin)
	retried := sess.custodyRetry[f.lin] != nil
	sess.mu.Unlock()
	runsAtCensus := retryRuns.Load()
	if !ended || !retried {
		t.Fatalf("fixture: the census must end A while its retry stands (ended %v, retry %v)", ended, retried)
	}
	testDriverWait(t, "the census-ended exchange's retry is retired and the dispatcher is idle with no tick armed",
		testCustodyBound, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return len(sess.custodyRetry) == 0 && !sess.custodyObserving && sess.custodyTickAt.IsZero()
		})
	if retryRuns.Load() <= runsAtCensus {
		t.Fatal("the retry was retired without a retry run reaching A")
	}
	testDriverP2(t, sess, f.e.s)
}

// H-4 (F-097 holistic round 4; lifecycle matrix RETIREMENT). An owed withdrawal whose own attempt
// COMMITTED retires once its exchange reads ended. The door's withdrawal lands while its response
// is lost and its resolve-abort-resolve cannot be read (Unresolved), so the withdrawal is owed;
// the store then answers while the cancellation tombstone stands with the same allocation and
// lineage. Every later attempt finds no evidence (the exchange is no longer at Publish), and the
// register re-read alone keeps the item for as long as the tombstone stands.
//
// Mutation M-owed (the no-evidence exchange-ended arm removed): the item is re-timed and retried
// every cap interval - "not within 5s: the owed withdrawal of an ended exchange retires".
func TestF097DriverOwedWithdrawalOfAnEndedExchangeRetires(t *testing.T) {
	t.Parallel()
	testF097OwedWithdrawalLandedUnresolved(t, "f097-h4-owed", false)
}

// H-4, control. The same committed-but-unresolved withdrawal with its tombstone deleted before the
// store answers: the item retires (under M-owed, through the register re-read finding it gone).
func TestF097DriverOwedWithdrawalOfAGoneRegisterRetires(t *testing.T) {
	t.Parallel()
	testF097OwedWithdrawalLandedUnresolved(t, "f097-h4-gone", true)
}

func testF097OwedWithdrawalLandedUnresolved(t *testing.T, clientID string, deleteRegister bool) {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	var lost atomic.Bool
	lost.Store(true)
	var noEvidence atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.Subject != f.subject {
			return write()
		}
		out, err := write()
		if lost.Load() {
			// The batch was sent (the first attempt commits); its response is lost, and a handle
			// whose outcome can never be read answers UNRESOLVED and cannot be aborted.
			return mqttOpOutcome{Handle: &mqttTransitionHandle{}}, errors.New("test: the response was lost")
		}
		if errors.Is(err, errMQTTWithdrawNoEvidence) {
			noEvidence.Add(1)
		}
		return out, err
	}
	f.e.asm.mu.Unlock()
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		// A short cap: under M-owed the item recurs many times within the bound.
		delays: func() (time.Duration, time.Duration) { return 10 * time.Millisecond, 50 * time.Millisecond },
	}
	sess.mu.Unlock()
	testDriverP1(t, sess)
	cur, seq := f.register(t)
	if _, _, ok := sess.endUnsentBeforeDestroy(f.subject, seq, cur, mqttSlotDeleteNeverTransmitted,
		mqttTestLicence(sess, nil)); ok {
		t.Fatal("fixture: the door must refuse a destruction whose withdrawal is unresolved")
	}
	sess.mu.Lock()
	owed := sess.withdrawOwed[f.lin] != nil
	sess.mu.Unlock()
	if !owed {
		t.Fatal("fixture: the unresolved withdrawal must be owed")
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangeWithdrawnUnsent {
		t.Fatalf("fixture: the withdrawal must have committed, the exchange reads %+v", x)
	}
	tomb, tombSeq := f.register(t)
	if tomb == nil || !tomb.Cancelled || tomb.Nonce != f.nonce || tomb.Lin != f.lin {
		t.Fatalf("fixture: the cancellation tombstone of this allocation and lineage must stand, got %+v", tomb)
	}
	if deleteRegister {
		if err := f.e.asm.jsa.deleteMsg(mqttInflightStreamName, tombSeq, true); err != nil {
			t.Fatalf("fixture: deleting the tombstone: %v", err)
		}
	}
	lost.Store(false) // the store answers
	testDriverWait(t, "the owed withdrawal of an ended exchange retires", testCustodyBound, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.withdrawOwed[f.lin] == nil
	})
	if noEvidence.Load() == 0 {
		t.Fatal("the item retired without an attempt after the store answered")
	}
	if !deleteRegister {
		if after, _ := f.register(t); after == nil || !after.Cancelled || after.Nonce != f.nonce {
			t.Fatalf("the tombstone must still stand: the item retires on its exchange, got %+v", after)
		}
	}
	testDriverP2(t, sess, f.e.s)
}

// H-4, admission continuity (lifecycle matrix RETIREMENT). A continuity record whose exchange's
// delivery was ABANDONED retires at its next classification. The deny of a restored entry
// (releaseTransmittedIdentifier) abandons it whether or not it was ever sent, and can reach it
// before any continuity unit has seen the restored entry; the record then names an exchange at
// DeliveryAbandoned - absorbing, and not an ending - with no entry left to supersede it. That is
// the state built here: the never-sent obligation denied and abandoned, then the record.
//
// Mutation M-cont (the classification retires on mqttExchangePhaseEnded only): the record is
// retried at its backoff until the generation is discarded - "not within 5s: the continuity
// record of an abandoned delivery retires".
func TestF097ContinuityOfAnAbandonedDeliveryRetires(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-h4-cont", 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		delays: func() (time.Duration, time.Duration) { return 10 * time.Millisecond, 50 * time.Millisecond },
	}
	sess.mu.Unlock()
	testDriverP1(t, sess)
	reply := testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2)
	if r := sess.releaseTransmittedIdentifier(f.e.c, testRealPubRelSrcDur, reply); r != mqttAbandonDone {
		t.Fatalf("fixture: the deny must abandon the restored delivery, got %v", r)
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangeDeliveryAbandoned {
		t.Fatalf("fixture: the exchange must read DeliveryAbandoned, got %+v", x)
	}
	sess.mu.Lock()
	installed := sess.installAdmissionContinuityLocked(f.pi, testRealPubRelSrcDur, f.sseq, f.nonce, f.lin, false,
		_EMPTY_, mqttTestLicenceLocked(sess, nil))
	tracked := sess.pendingPublish[f.pi] != nil
	sess.mu.Unlock()
	if !installed || tracked {
		t.Fatalf("fixture: the record must stand over an untracked abandoned delivery (installed %v, tracked %v)",
			installed, tracked)
	}
	testDriverWait(t, "the continuity record of an abandoned delivery retires", testCustodyBound, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return len(sess.admissionContinuity) == 0
	})
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangeDeliveryAbandoned {
		t.Fatalf("the record must retire on the abandonment, not on a later ending: %+v", x)
	}
	testDriverP2(t, sess, f.e.s)
}

// C10 (F-097 holistic round 5, H-5; lifecycle matrix RETIREMENT, W row). An owed withdrawal whose
// exchange's delivery was ABANDONED retires at its next unit, and only the item retires. The deny
// of a restored entry (releaseTransmittedIdentifier) abandons the delivery while its register
// stands as the disposal record, with the same allocation and lineage; a withdrawal owed over it
// then finds no evidence (the register is no longer at Publish) on every attempt, and the register
// re-read alone keeps the item for as long as the disposal record stands. The abandonment is a
// decided subject: the exchange owes no further delivery. Its identifier, ACK binding and register
// are another question, and stay.
//
// Mutation M-owed-2 (the exchange arm retires on mqttExchangePhaseEnded only): the item is re-timed
// and retried every cap interval until the generation is discarded - "not within 5s: the owed
// withdrawal of an abandoned delivery retires".
func TestF097DriverOwedWithdrawalOfAnAbandonedDeliveryRetires(t *testing.T) {
	t.Parallel()
	testF097OwedWithdrawalDutyEnded(t, "f097-c10-abandoned", false)
}

// C10, the discarded generation. The same owed withdrawal over an exchange the generation discard
// moved to Discarded - by the real discard op, under the generation's discarding closure, while the
// session still stands at that generation so the map is not cleared under the item - retires at its
// next unit. Mutation M-owed-2: "not within 5s: the owed withdrawal of a discarded exchange retires".
func TestF097DriverOwedWithdrawalOfADiscardedExchangeRetires(t *testing.T) {
	t.Parallel()
	testF097OwedWithdrawalDutyEnded(t, "f097-c10-discarded", true)
}

func testF097OwedWithdrawalDutyEnded(t *testing.T, clientID string, discard bool) {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		// A short cap: under M-owed-2 the item recurs many times within the bound.
		delays: func() (time.Duration, time.Duration) { return 10 * time.Millisecond, 50 * time.Millisecond },
	}
	gen := sess.gen
	sess.mu.Unlock()
	testDriverP1(t, sess)

	// THE SUBJECT IS DECIDED FIRST, the item recorded second: an item recorded over a Publish
	// exchange with its marker standing would land a real withdrawal and never reach the arm.
	want := mqttExchangeDeliveryAbandoned
	what := "an abandoned delivery"
	var binding *mqttAllocationAckBinding
	bindingIdx := mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}
	if discard {
		want, what = mqttExchangeDiscarded, "a discarded exchange"
		jsa, id, epoch := &f.e.asm.jsa, sess.id, f.e.asm.sessStreamEpoch
		testDiscardPlantClosure(t, jsa, id, epoch, f.gen, true)
		if _, err := jsa.opDiscardExchange(id, epoch, "txn-discard", f.gen, f.lin); err != nil {
			t.Fatalf("fixture: discarding the obligation was refused: %v", err)
		}
	} else {
		reply := testInflightAckSubject(testRealPubRelSrcDur, f.sseq, 2)
		if r := sess.releaseTransmittedIdentifier(f.e.c, testRealPubRelSrcDur, reply); r != mqttAbandonDone {
			t.Fatalf("fixture: the deny must abandon the restored delivery, got %v", r)
		}
		sess.mu.Lock()
		binding = sess.abandonedAcks[bindingIdx]
		sess.mu.Unlock()
		if binding == nil {
			t.Fatal("fixture: the abandonment must install the ACK binding")
		}
	}
	if x := f.exchange(t); x == nil || x.Phase != want || x.Nonce != f.lin {
		t.Fatalf("fixture: the exchange must read phase %d, got %+v", want, x)
	}
	reg, regSeq := f.register(t)
	if reg == nil || reg.Nonce != f.nonce || reg.Lin != f.lin || (!discard && reg.State != mqttInflightDeliveryAbandoned) ||
		(discard && reg.State != mqttInflightPublish) {
		t.Fatalf("fixture: the register of this allocation and lineage must stand, got %+v", reg)
	}
	testDriverWait(t, "the decision observed", testCustodyBound, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})

	// No acknowledgement of any kind from here on. The observer is a measuring instrument: a
	// barrier through the session's own send queue bounds each reading.
	obs, err := f.e.s.nrc.SubscribeSync("$JS.ACK.>")
	if err != nil {
		t.Fatalf("subscribing to the acknowledgement subjects: %v", err)
	}
	t.Cleanup(func() { _ = obs.Unsubscribe() })
	if ferr := f.e.s.nrc.Flush(); ferr != nil {
		t.Fatalf("flush: %v", ferr)
	}
	const barrierSubj = "$JS.ACK.c10-barrier.none.1.1.1.0.0"
	naks := func() (n int) {
		t.Helper()
		sess.jsa.sendMsg(barrierSubj, []byte("c10-barrier"))
		for {
			m, nerr := obs.NextMsg(testCustodyBound)
			if nerr != nil {
				t.Fatalf("the barrier never arrived: %v", nerr)
			}
			if m.Subject == barrierSubj {
				return n
			}
			if string(m.Data) == string(AckNak) {
				n++
			}
		}
	}
	naks() // what the fixture queued

	var noEvidence atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testWithdrawUnsent = func(req *mqttWithdrawUnsentRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, werr := write()
		if req.Subject == f.subject && errors.Is(werr, errMQTTWithdrawNoEvidence) &&
			!errors.Is(werr, errMQTTWithdrawNoMarker) {
			noEvidence.Add(1)
		}
		return out, werr
	}
	f.e.asm.mu.Unlock()
	sess.recordWithdrawOwed(f.gen, f.subject, f.nonce, f.lin, regSeq, mqttTestLicence(sess, nil))
	sess.mu.Lock()
	owed := sess.withdrawOwed[f.lin] != nil
	sess.mu.Unlock()
	if !owed {
		t.Fatal("fixture: the withdrawal must be owed")
	}
	testDriverWait(t, "the owed withdrawal of "+what+" retires", testCustodyBound, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.withdrawOwed[f.lin] == nil
	})
	if noEvidence.Load() == 0 {
		t.Fatal("the item retired without an attempt finding no evidence")
	}
	testDriverP2(t, sess, f.e.s)

	// ONLY THE ITEM RETIRED.
	if x := f.exchange(t); x == nil || x.Phase != want {
		t.Fatalf("the exchange must still read phase %d: %+v", want, x)
	}
	if after, afterSeq := f.register(t); after == nil || afterSeq != regSeq || after.State != reg.State ||
		after.Nonce != f.nonce || after.Lin != f.lin {
		t.Fatalf("the register must stand unrewritten at %d, got %+v at %d", regSeq, after, afterSeq)
	}
	sess.mu.Lock()
	sameGen := sess.gen == gen
	keptBinding := sess.abandonedAcks[bindingIdx] == binding
	sess.mu.Unlock()
	if !sameGen {
		t.Fatal("the generation moved: the retirement must not discard it")
	}
	if !discard {
		if !keptBinding {
			t.Fatal("the abandoned delivery's ACK binding must stand: only the item retires")
		}
		if got := testCustodyMints(sess, f.pi); got == 0 || got == f.pi {
			t.Fatalf("the abandoned delivery's identifier %d must stay excluded while others mint, got %d", f.pi, got)
		}
	}
	if n := naks(); n != 0 {
		t.Fatalf("the retirement sent %d NAKs: only the item retires", n)
	}
}

// W-tick, generation discard (F-097, consult C8 (2)). Discarding the generation cancels the armed
// tick by the arm's STORED key: a tick armed far in the future is unregistered by the clear, not
// left to fire into a generation that no longer stands.
//
// Mutation (the discard cancels a key rebuilt without the arm): the stored key stays registered.
func TestF097DriverGenerationDiscardCancelsTheArmedTick(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-wtick-discard", 1)
	sess := f.e.sess
	srv := f.e.s
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	sess.mu.Lock()
	// A demand deadline ~30 s out: the tick cannot fire within the test.
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		delays: func() (time.Duration, time.Duration) { return 30 * time.Second, 30 * time.Second },
	}
	sess.mu.Unlock()
	testDriverP1(t, sess)
	testCustodyRM1(t, sess)
	bReply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq+1, 1)
	sess.mu.Lock()
	bPI, _ := sess.trackPublish(testRealPubRelSrcDur, bReply)
	sess.mu.Unlock()
	if bPI != 0 {
		t.Fatalf("fixture: B must be withheld while A holds the only slot, got identifier %d", bPI)
	}
	var key string
	testDriverWait(t, "the latched demand arms the tick and the run ends", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		key = sess.custodyTickKey
		return !sess.custodyObserving && key != _EMPTY_ && testDriverTimerRegistered(srv, key)
	})
	sess.mu.Lock()
	sess.c = nil // a clean start retires the incarnation only with no client bound
	sess.mu.Unlock()
	if err := sess.clear(false); err != nil {
		t.Fatalf("clear: %v", err)
	}
	testDriverWait(t, "the discarded generation's tick is unregistered by its stored key", 5*time.Second, func() bool {
		return !testDriverTimerRegistered(srv, key)
	})
	testDriverP2(t, sess, srv)
}

// W-demand (F-097, consult C8 (3)). A latched demand's deadline stands until it is consumed:
// runs that hints start and that exit before it neither move it nor back the demand off, and the
// demand's pass is requested AT that first deadline although hints keep arriving. The request is
// observed where the deadline becomes a pass (the demandDue seam), so hint processing alone
// cannot satisfy it.
//
// Mutation M-drift (the IsZero guard removed: every exit backs off and re-stamps the deadline):
// the first hint run's exit moves it - "a hint run's exit moved the latched demand's deadline".
func TestF097DriverDemandDeadlineSurvivesHintExits(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-wdemand", 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	type due struct{ dueAt, at time.Time }
	var dues []due // appended under sess.mu (the seam is called with it held)
	sess.mu.Lock()
	sess.testCustodyDriver = &mqttCustodyDriverSeams{
		// The first deadline 0.8 - 1.2 s after the first exit: room for many hint runs before it.
		delays: func() (time.Duration, time.Duration) { return time.Second, 8 * time.Second },
		demandDue: func(gen string, dueAt time.Time) {
			dues = append(dues, due{dueAt: dueAt, at: time.Now()})
		},
	}
	sess.mu.Unlock()
	testDriverP1(t, sess)
	testCustodyRM1(t, sess)
	bReply := testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq+1, 1)
	sess.mu.Lock()
	bPI, _ := sess.trackPublish(testRealPubRelSrcDur, bReply)
	sess.mu.Unlock()
	if bPI != 0 {
		t.Fatalf("fixture: B must be withheld while A holds the only slot, got identifier %d", bPI)
	}
	idle := func() bool {
		return !sess.custodyObserving && len(sess.custodyHints) == 0 && !sess.custodyPassWanted &&
			len(sess.custodyPassQueue) == 0
	}
	var due0 time.Time
	var delay0 time.Duration
	testDriverWait(t, "the gate kick's run ends with the demand's first deadline", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		due0, delay0 = sess.custodyDemandDueAt, sess.custodyDemandDelay
		return idle() && sess.custodyDemand && !due0.IsZero()
	})
	sess.mu.Lock()
	consumed := len(dues)
	sess.mu.Unlock()
	if consumed != 0 || !time.Now().Before(due0) {
		t.Fatalf("fixture: the first deadline (%v) must still be ahead, consumed %d", due0, consumed)
	}
	// Periodic hint runs for A (live at Publish: observed, never released), each waited out.
	hintExits := 0
	for time.Now().Before(due0.Add(time.Second)) {
		sess.hintCustody(f.gen, f.lin)
		testDriverWait(t, "the hint's run ends", 5*time.Second, func() bool {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			return idle()
		})
		sess.mu.Lock()
		consumed = len(dues)
		dueNow, delayNow, demand := sess.custodyDemandDueAt, sess.custodyDemandDelay, sess.custodyDemand
		sess.mu.Unlock()
		if !demand {
			t.Fatal("fixture: the latched demand was cleared while B stays withheld")
		}
		if consumed > 0 {
			break
		}
		hintExits++
		if !dueNow.Equal(due0) || delayNow != delay0 {
			t.Fatalf("a hint run's exit moved the latched demand's deadline: %v (delay %v) -> %v (delay %v) after %d exits",
				due0, delay0, dueNow, delayNow, hintExits)
		}
		time.Sleep(20 * time.Millisecond)
	}
	sess.mu.Lock()
	got := append([]due(nil), dues...)
	sess.mu.Unlock()
	if hintExits < 3 {
		t.Fatalf("fixture: only %d hint runs exited before the deadline", hintExits)
	}
	if len(got) == 0 {
		t.Fatalf("the demand's pass was never requested at its deadline %v under %d hint runs", due0, hintExits)
	}
	if !got[0].dueAt.Equal(due0) {
		t.Fatalf("the demand's pass was requested for deadline %v, want the first deadline %v", got[0].dueAt, due0)
	}
	if got[0].at.Before(due0) {
		t.Fatalf("the demand's pass was requested at %v, before its deadline %v", got[0].at, due0)
	}
	if late := got[0].at.Sub(due0); late > 500*time.Millisecond {
		t.Fatalf("the demand's pass was requested %v after its deadline", late)
	}
}

// testF097ExitTakesItem drives ONE item recorded in the exit window: the first time a run finds no
// work, before its exit takes the session lock, install records the item through its production
// recorder - which finds the single-flight flag set and starts nothing. No demand is latched, no
// tick is armed and nothing follows. The item's unit must then run in THAT run, and the item
// retire.
func testF097ExitTakesItem(t *testing.T, clientID string, kind mqttCustodyUnitKind,
	install func(f *testWithdrawFix, lic mqttDestroyLicence, seq uint64) bool, retired func(f *testWithdrawFix) bool) {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	lic, seq := mqttTestLicence(sess, nil), f.seqOf(t)
	rec := testDriverInstall(sess)
	testDriverP1(t, sess)
	type window struct {
		recorded, observing, demand, census bool
		tickKey                             string
		retries, runs                       int
	}
	var w window
	fired := make(chan struct{})
	var once atomic.Bool
	rec.mu.Lock()
	rec.idle = func() {
		if !once.CompareAndSwap(false, true) {
			return
		}
		recorded := install(f, lic, seq)
		sess.mu.Lock()
		w = window{recorded: recorded, observing: sess.custodyObserving, demand: sess.custodyDemand,
			census: sess.custody != nil && sess.custody.censusWanted, tickKey: sess.custodyTickKey,
			retries: len(sess.custodyRetry)}
		sess.mu.Unlock()
		w.runs = rec.startCount()
		close(fired)
	}
	rec.mu.Unlock()
	sess.startCustodyDispatcher()
	select {
	case <-fired:
	case <-time.After(testCustodyBound):
		t.Fatal("fixture: the run never found an empty iteration")
	}
	if !w.recorded || !w.observing || w.demand || w.census || w.tickKey != _EMPTY_ || w.retries != 0 || w.runs != 1 {
		t.Fatalf("fixture: the item must be recorded into a lone run with no demand, census, tick or retry: %+v", w)
	}
	var got testDriverUnit
	testCustodyEventually(t, "the item recorded in the exit window has its unit dispatched", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, u := range rec.units {
			if u.kind == kind {
				got = u
				return true
			}
		}
		return false
	})
	if got.run != w.runs {
		t.Fatalf("the item's unit ran in run %d, not in run %d that was live when it was recorded: the exit "+
			"left it to another trigger", got.run, w.runs)
	}
	testCustodyEventually(t, "the item retires", func() bool { return retired(f) })
	testDriverP2(t, sess, f.e.s)
}

// W-exit, owed withdrawal (F-097, H-1). An owed withdrawal recorded after a run's last look and
// before its exit is taken by that run: the recorder starts nothing while the flag stands, so the
// exit is the only thing that can.
//
// Mutations: M-exit-a (the exit's due-item condition dropped) - the unit runs only through a fresh
// tick's run: "ran in run 2, not in run 1". M-exit-ab (that and the due-is-now arming dropped) -
// nothing takes it: "not within 5s: the item recorded in the exit window has its unit dispatched".
func TestF097DriverExitTakesARecordedWithdrawal(t *testing.T) {
	t.Parallel()
	testF097ExitTakesItem(t, "f097-wexit-withdraw", mqttCustodyUnitWithdraw,
		func(f *testWithdrawFix, lic mqttDestroyLicence, seq uint64) bool {
			f.e.sess.recordWithdrawOwed(f.gen, f.subject, f.nonce, f.lin, seq, lic)
			f.e.sess.mu.Lock()
			defer f.e.sess.mu.Unlock()
			return f.e.sess.withdrawOwed[f.lin] != nil
		},
		func(f *testWithdrawFix) bool {
			f.e.sess.mu.Lock()
			defer f.e.sess.mu.Unlock()
			return f.e.sess.withdrawOwed[f.lin] == nil
		})
}

// W-exit, a run that may not continue (F-097, H-1). A run whose captured identity no longer holds
// exits without continuing; an owed withdrawal recorded while its flag stood (zero eligibility)
// must still leave a tick armed, and that tick's run withdraws it. The run's flag is held by the
// fixture and its exit is the one a nil-ledger iteration takes.
//
// Mutation M-exit-b (a zero eligibility no longer arms as "now"): nothing is armed - "not within
// 5s: the exit armed a tick for the due withdrawal, and its run withdrew it".
func TestF097DriverExitThatMayNotContinueArmsADueItem(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-wexit-nocontinue", 1)
	sess := f.e.sess
	srv := f.e.s
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	lic, seq := mqttTestLicence(sess, nil), f.seqOf(t)
	testDriverP1(t, sess)
	sess.mu.Lock()
	sess.custodyObserving = true // the run whose exit this is
	demand, key := sess.custodyDemand, sess.custodyTickKey
	sess.mu.Unlock()
	if demand || key != _EMPTY_ {
		t.Fatalf("fixture: no demand and no tick may stand (demand %v, tick %q)", demand, key)
	}
	sess.recordWithdrawOwed(f.gen, f.subject, f.nonce, f.lin, seq, lic)
	sess.mu.Lock()
	w := sess.withdrawOwed[f.lin]
	sess.mu.Unlock()
	if w == nil || !w.nextEligibleAt.IsZero() {
		t.Fatalf("fixture: the owed withdrawal must be recorded with zero eligibility: %+v", w)
	}
	if sess.exitCustodyDispatcher(srv, f.gen, false) {
		t.Fatal("an exit that may not continue reported the loop continues")
	}
	testCustodyEventually(t, "the exit armed a tick for the due withdrawal, and its run withdrew it", func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.withdrawOwed[f.lin] == nil
	})
	testDriverP2(t, sess, srv)
}

// W-exit, admission continuity (F-097, H-1). The same window for an admission continuity record.
//
// Mutations: as TestF097DriverExitTakesARecordedWithdrawal.
func TestF097DriverExitTakesARecordedContinuity(t *testing.T) {
	t.Parallel()
	testF097ExitTakesItem(t, "f097-wexit-continuity", mqttCustodyUnitContinuity,
		func(f *testWithdrawFix, lic mqttDestroyLicence, _ uint64) bool {
			f.e.sess.mu.Lock()
			defer f.e.sess.mu.Unlock()
			return f.e.sess.installAdmissionContinuityLocked(f.pi, testRealPubRelSrcDur, f.sseq, f.nonce, f.lin,
				false, _EMPTY_, lic)
		},
		func(f *testWithdrawFix) bool {
			f.e.sess.mu.Lock()
			defer f.e.sess.mu.Unlock()
			return len(f.e.sess.admissionContinuity) == 0
		})
}

// testDriverBusy is ONE run whose queue never empties: every unit of the fixture's exchange
// re-records its hint before it runs - the shape of read retries due again before the others
// finish - so no iteration of the run is ever empty and the run never exits on its own.
type testDriverBusy struct {
	f      *testWithdrawFix
	sess   *mqttSession
	leases *mqttIncarnationTracker
	rec    *testDriverRec
	refill atomic.Bool
	// closingUnits counts the units dispatched while the incarnation was closing.
	closingUnits atomic.Int32
}

func testDriverBusyRun(t *testing.T, clientID string) *testDriverBusy {
	t.Helper()
	f := testWithdrawAdmitted(t, clientID, 1)
	sess := f.e.sess
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	sess.mu.Lock()
	leases := sess.leases
	sess.mu.Unlock()
	// The drain must be decided by the dispatcher's lease alone.
	leases.mu.Lock()
	held := leases.owners - leases.promoted
	leases.mu.Unlock()
	if held != 0 {
		t.Fatalf("fixture: the idle incarnation must hold no lease, %d do", held)
	}
	b := &testDriverBusy{f: f, sess: sess, leases: leases}
	b.refill.Store(true)
	b.rec = testDriverInstall(sess)
	b.rec.mu.Lock()
	b.rec.unit = func(mqttCustodyUnit) {
		select {
		case <-leases.closingSignal():
			b.closingUnits.Add(1)
		default:
		}
		time.Sleep(2 * time.Millisecond) // a unit takes time
		if !b.refill.Load() {
			return
		}
		sess.mu.Lock()
		if sess.gen == f.gen {
			if sess.custodyHints == nil {
				sess.custodyHints = make(map[string]struct{})
			}
			sess.custodyHints[f.lin] = struct{}{}
		}
		sess.mu.Unlock()
	}
	b.rec.mu.Unlock()
	testDriverP1(t, sess)
	t.Cleanup(func() { b.refill.Store(false) })
	sess.hintCustody(f.gen, f.lin)
	testDriverWait(t, "the refilled hint keeps one run iterating", 5*time.Second, func() bool {
		b.rec.mu.Lock()
		defer b.rec.mu.Unlock()
		return b.rec.iterations >= 5
	})
	if n := b.rec.startCount(); n != 1 {
		t.Fatalf("fixture: ONE run must be iterating, %d started", n)
	}
	return b
}

// W-retire (F-097, holistic H-2). A run whose queue never empties does not hold its incarnation
// against a retirement: once the incarnation is closing, the run stops between units, releases
// its lease and leaves its work queued. Witness: the busy run's incarnation is closed; it drains
// within the bound after at most the unit in flight, the hint is still queued, and no run starts
// while the incarnation is closing (the tick the exit armed for the hint is refused at the lease).
//
// Mutation M-retire (quit ignores the closing signal): the run keeps its lease for as long as its
// queue refills - "the closing incarnation never drained within 3s".
func TestF097DriverRetirementStopsABusyRun(t *testing.T) {
	t.Parallel()
	b := testDriverBusyRun(t, "f097-wretire")
	sess, leases := b.sess, b.leases
	leases.close()
	if !leases.waitDrained(time.After(3 * time.Second)) {
		t.Fatal("the closing incarnation never drained within 3s: the busy run kept its lease")
	}
	if n := b.closingUnits.Load(); n > 1 {
		t.Fatalf("%d units were dispatched after the incarnation closed, want at most the one in flight", n)
	}
	sess.mu.Lock()
	_, queued := sess.custodyHints[b.f.lin]
	observing := sess.custodyObserving
	sess.mu.Unlock()
	if !queued || observing {
		t.Fatalf("the stopped run must end with its hint still queued (queued %v, observing %v)", queued, observing)
	}
	runs := b.rec.startCount()
	testCustodyStaysFor(t, "no run starts while the incarnation is closing", 300*time.Millisecond, func() bool {
		return b.rec.startCount() == runs
	})
	b.refill.Store(false)
	leases.reopen()
	testDriverP2(t, sess, b.f.e.s)
}

// W-retire, the clean start (F-097, holistic H-2). The retirement the finding names completes: a
// Clean Start's discard of the busy run's session drains within its ceiling and re-mints the
// generation, although the run's queue never empties.
//
// Mutation M-retire: the discard waits on the run's lease until its ceiling - "the clean start's
// discard did not complete within 5s".
func TestF097DriverCleanStartRetiresABusyRun(t *testing.T) {
	t.Parallel()
	b := testDriverBusyRun(t, "f097-wretire-clear")
	sess := b.sess
	sess.mu.Lock()
	sess.c = nil // a clean start retires the incarnation only with no client bound
	sess.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- sess.clear(false) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the clean start's discard failed with the run busy: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the clean start's discard did not complete within 5s while the run kept its queue busy")
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	if gen == b.f.gen {
		t.Fatal("the discard must re-mint the generation")
	}
	if n := b.closingUnits.Load(); n > 1 {
		t.Fatalf("%d units were dispatched after the incarnation closed, want at most the one in flight", n)
	}
	testDriverP2(t, sess, b.f.e.s)
}

// W-requeue (F-097, holistic H-2; lifecycle matrix G1). An ABORTED retirement loses nothing the
// stopped run had taken. The run's one iteration took two hints; the incarnation closed while the
// first unit was in flight, and the run stopped before the second. That hint is back in its
// queue, is not dispatched while the incarnation is closing, and is dispatched once the
// retirement aborts (the reopen re-arms the dispatcher).
//
// Mutations: M-G1 (a quit drops the unrun units) - "the unrun hint ... must be back in its queue".
// M-retire (the run does not stop) - "stopped holding: the unrun hint is not dispatched while the
// incarnation is closing".
func TestF097DriverAbortedRetirementRunsTheRequeuedUnit(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-wrequeue", 1)
	sess := f.e.sess
	sess.mu.Lock()
	sess.maxp = 100
	sess.mu.Unlock()
	g := testWithdrawAdmitMore(t, f.e, testRealPubRelSrcSeq+10, 1)
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	sess.mu.Lock()
	leases := sess.leases
	sess.mu.Unlock()
	rec := testDriverInstall(sess)
	blocked, release := make(chan string, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseHold := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHold)
	var once atomic.Bool
	rec.mu.Lock()
	rec.unit = func(u mqttCustodyUnit) {
		if once.CompareAndSwap(false, true) {
			blocked <- u.target.key
			<-release
		}
	}
	rec.mu.Unlock()
	testDriverP1(t, sess)
	sess.mu.Lock()
	sess.custodyHints = map[string]struct{}{f.lin: {}, g.lin: {}}
	sess.mu.Unlock()
	sess.startCustodyDispatcher()
	var first string
	select {
	case first = <-blocked:
	case <-time.After(testCustodyBound):
		t.Fatal("fixture: the run never dispatched a unit")
	}
	second := g.lin
	if first == g.lin {
		second = f.lin
	}
	rec.mu.Lock()
	iterations := rec.iterations
	rec.mu.Unlock()
	sess.mu.Lock()
	left := len(sess.custodyHints)
	sess.mu.Unlock()
	if iterations != 1 || left != 0 {
		releaseHold()
		t.Fatalf("fixture: one iteration must have taken both hints (iterations %d, hints left %d)", iterations, left)
	}
	leases.close()
	closedAt := time.Now()
	releaseHold()
	if !leases.waitDrained(time.After(testCustodyBound)) {
		t.Fatal("the closing incarnation never drained after the unit in flight")
	}
	testCustodyStaysFor(t, "the unrun hint is not dispatched while the incarnation is closing", 300*time.Millisecond,
		func() bool {
			_, n := rec.testDriverDispatched(second, closedAt)
			return n == 0
		})
	sess.mu.Lock()
	_, requeued := sess.custodyHints[second]
	sess.mu.Unlock()
	if !requeued {
		t.Fatalf("the unrun hint %q must be back in its queue after the run stopped", second)
	}
	reopenedAt := time.Now()
	leases.reopen()
	testCustodyEventually(t, "the aborted retirement's reopen dispatches the requeued hint", func() bool {
		_, n := rec.testDriverDispatched(second, reopenedAt)
		return n > 0
	})
	testDriverP2(t, sess, f.e.s)
}

// W-census (F-097, lifecycle matrix G2). A census owed ALONE survives an aborted retirement. Its
// only arm is the tick; that tick fires while the incarnation is closing and its start is refused
// at the lease, so the reopen must count the census as owed work.
//
// Mutation M-G2 (the reopen's owed set omits the census): nothing starts after the reopen - "not
// within 5s: the reopen re-arms the owed census and it runs". P1 cannot see it: no exit is
// involved, only the tick and the reopen.
func TestF097DriverReopenRearmsAnOwedCensus(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-wcensus", 1)
	sess := f.e.sess
	srv := f.e.s
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	rec := testDriverInstall(sess)
	sess.mu.Lock()
	// The census is throttled for 1 s after its last run: room to close the incarnation first.
	sess.testCustodyDriver.delays = func() (time.Duration, time.Duration) { return 50 * time.Millisecond, time.Second }
	leases := sess.leases
	owedElse := len(sess.custodyHints) > 0 || sess.custodyPassWanted || len(sess.custodyPassQueue) > 0 ||
		sess.custodyDemand || len(sess.custodyRetry) > 0 || len(sess.withdrawOwed) > 0 ||
		len(sess.admissionContinuity) > 0
	tick := sess.custodyTickKey
	sess.custody.censusWanted, sess.custody.lastHeavyAt = true, time.Now()
	sess.mu.Unlock()
	if owedElse || tick != _EMPTY_ {
		t.Fatalf("fixture: the census must be the only owed work, with no tick (other work %v, tick %q)", owedElse, tick)
	}
	testDriverP1(t, sess)
	sess.startCustodyDispatcher()
	var key string
	testDriverWait(t, "the run ends with the throttled census's tick armed", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		key = sess.custodyTickKey
		return !sess.custodyObserving && key != _EMPTY_
	})
	leases.close()
	rec.mu.Lock()
	ran := len(rec.census)
	rec.mu.Unlock()
	if ran != 0 {
		t.Fatalf("fixture: the census ran (%d) before the incarnation closed", ran)
	}
	runs := rec.startCount()
	testDriverWait(t, "the census's tick fires into the closing incarnation", 5*time.Second, func() bool {
		sess.mu.Lock()
		cur := sess.custodyTickKey
		sess.mu.Unlock()
		return cur != key && !testDriverTimerRegistered(srv, key)
	})
	sess.mu.Lock()
	owed := sess.custody.censusWanted
	sess.mu.Unlock()
	if !owed || rec.startCount() != runs {
		t.Fatalf("fixture: the closing incarnation must refuse the tick's start and keep the census owed "+
			"(owed %v, runs %d -> %d)", owed, runs, rec.startCount())
	}
	leases.reopen()
	testCustodyEventually(t, "the reopen re-arms the owed census and it runs", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.census) > 0
	})
	testDriverP2(t, sess, srv)
}

// W-exit, a hint (F-097, lifecycle matrix G3). A hint recorded while a run's flag stood, at an
// exit that may not continue (a nil-ledger exit: the run's epoch moved), is left armed: a hint has
// no eligibility of its own, so the exit arms a tick for now and that tick's run observes it. The
// run's flag is held by the fixture.
//
// Mutation M-G3 (the exit arms nothing for hints and pass work): nothing takes the hint - "not
// within 5s: the exit armed a tick for the recorded hint, and its run observed it" - and P1
// reports the exit.
func TestF097DriverExitThatMayNotContinueArmsAHint(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-wexit-hint", 1)
	sess := f.e.sess
	srv := f.e.s
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	rec := testDriverInstall(sess)
	testDriverP1(t, sess)
	sess.mu.Lock()
	sess.custodyObserving = true // the run whose exit this is
	demand, key := sess.custodyDemand, sess.custodyTickKey
	sess.mu.Unlock()
	if demand || key != _EMPTY_ {
		t.Fatalf("fixture: no demand and no tick may stand (demand %v, tick %q)", demand, key)
	}
	sess.hintCustody(f.gen, f.lin)
	sess.mu.Lock()
	_, hinted := sess.custodyHints[f.lin]
	sess.mu.Unlock()
	if !hinted || rec.startCount() != 0 {
		t.Fatalf("fixture: the hint must be recorded and start nothing while the flag stands (hinted %v, runs %d)",
			hinted, rec.startCount())
	}
	if sess.exitCustodyDispatcher(srv, f.gen, false) {
		t.Fatal("an exit that may not continue reported the loop continues")
	}
	testCustodyEventually(t, "the exit armed a tick for the recorded hint, and its run observed it", func() bool {
		all, _ := rec.testDriverDispatched(f.lin, time.Time{})
		return all > 0
	})
	testDriverP2(t, sess, srv)
}

// The demand latch's RETIREMENT (consult C9): a latched demand names a waiter, a withheld
// allocation. A waiter may leave without ever being allocated, and some withholdings defer
// nothing at all, so the latch retires at its own deadline once nobody is deferred, no
// identifier is unknown and no custody is held - and is served while any of those stands.

// testDriverDemandSeams installs the recorder, a small (base, cap) so the demand's deadlines come
// well inside the bound, a count of the deadlines served as passes, and property P1.
func testDriverDemandSeams(t *testing.T, sess *mqttSession) (*testDriverRec, *atomic.Int32) {
	t.Helper()
	rec := testDriverInstall(sess)
	dues := &atomic.Int32{}
	sess.mu.Lock()
	sess.testCustodyDriver.delays = func() (time.Duration, time.Duration) {
		return 10 * time.Millisecond, 100 * time.Millisecond
	}
	sess.testCustodyDriver.demandDue = func(string, time.Time) { dues.Add(1) }
	sess.mu.Unlock()
	testDriverP1(t, sess)
	return rec, dues
}

// testDriverDemandStaysServed asserts the latched demand is SERVED, not retired, while an
// allocation may still wait: two more of its deadlines become requested passes and it stays
// latched throughout. Two served deadlines make the check non-vacuous - a window that reached no
// deadline would pass under any retirement rule.
func testDriverDemandStaysServed(t *testing.T, sess *mqttSession, dues *atomic.Int32, while string) {
	t.Helper()
	latched := func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.custodyDemand
	}
	n0 := dues.Load()
	deadline := time.Now().Add(testCustodyBound)
	for dues.Load() < n0+2 {
		if !latched() {
			t.Fatalf("the latched demand retired while %s", while)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("fixture: the latched demand's deadline was served %d times within %v while %s, want 2",
				dues.Load()-n0, testCustodyBound, while)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !latched() {
		t.Fatalf("the latched demand retired while %s", while)
	}
}

// testDriverDemandRetiresQuietly waits for the latch to retire on its own, then asserts nothing
// follows it: no deadline served, no unit dispatched across several cap intervals, and P2.
func testDriverDemandRetiresQuietly(t *testing.T, f *testWithdrawFix, rec *testDriverRec, dues *atomic.Int32) {
	t.Helper()
	sess := f.e.sess
	testDriverWait(t, "the latched demand retires once nothing can be waiting", testCustodyBound, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyDemand
	})
	testDriverP2(t, sess, f.e.s)
	rec.mu.Lock()
	units := len(rec.units)
	rec.mu.Unlock()
	served := dues.Load()
	testCustodyStaysFor(t, "the retired demand stays retired and serves nothing", 400*time.Millisecond, func() bool {
		sess.mu.Lock()
		demand := sess.custodyDemand
		sess.mu.Unlock()
		rec.mu.Lock()
		n := len(rec.units)
		rec.mu.Unlock()
		return !demand && n == units && dues.Load() == served
	})
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	if gen != f.gen {
		t.Fatalf("the session's generation moved (%q -> %q): the fixture was reset, not quiesced", f.gen, gen)
	}
	testDriverP2(t, sess, f.e.s)
}

// testDriverWithdrawA ends the fixture's admitted obligation and waits for the ledger to release
// it: the remaining custody is resolved.
func testDriverWithdrawA(t *testing.T, f *testWithdrawFix) {
	t.Helper()
	sess := f.e.sess
	if r := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(sess, nil), nil); r != mqttWithdrawLanded {
		t.Fatalf("fixture: A's withdrawal must land, got %v", r)
	}
	testCustodyEventually(t, "the ledger released A's ended exchange", func() bool {
		return !testCustodyHasExchange(sess, f.lin)
	})
}

// W-D1. A withheld delivery's waiter LEAVES WITHOUT AN ALLOCATION: B, withheld at the delivery
// gate (deferred, demand latched), loses its consumer - the committed teardown drops its withheld
// marker and nothing will re-offer it. While A still holds custody the demand is served; once A
// ends and is released, the latch retires on its own, with no further offer of B and no reset.
//
// Mutation M-dem-a (the deadline always requests the pass): the latch serves forever, the
// retirement wait fails.
func TestF097DriverDemandOfADrainedWaiterRetires(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-dem-drained", 1)
	sess := f.e.sess
	rec, dues := testDriverDemandSeams(t, sess)
	testCustodyRM1(t, sess)
	// B on its OWN consumer, so the teardown drops B's deferred entry and nothing of A's.
	const bDur = "SRCB"
	bSeq := uint64(testRealPubRelSrcSeq + 1)
	sess.mu.Lock()
	bPI, _ := sess.trackPublish(bDur, testInflightAckSubject(bDur, bSeq, 1))
	_, deferred := sess.deferred[mqttDeferredKey{bDur, bSeq}]
	latched := sess.custodyDemand
	lastPI := sess.last_pi
	sess.mu.Unlock()
	if bPI != 0 || !deferred || !latched {
		t.Fatalf("fixture: B must be withheld at the delivery gate (identifier %d, deferred %v, demand %v)",
			bPI, deferred, latched)
	}
	sess.mu.Lock()
	sess.purgeConsumerPendingLocked(bDur, mqttTestLicenceLocked(sess, nil))
	left := len(sess.deferred)
	sess.mu.Unlock()
	if left != 0 {
		t.Fatalf("fixture: B's consumer teardown left %d deferred deliveries", left)
	}
	// Nobody is deferred, but A's exchange is still held: the demand is served.
	testDriverDemandStaysServed(t, sess, dues, "A's exchange is held")
	testDriverWithdrawA(t, f)
	testDriverDemandRetiresQuietly(t, f, rec, dues)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.last_pi != lastPI {
		t.Fatalf("an identifier was allocated after B was withheld (last %d -> %d): the latch was met, not retired",
			lastPI, sess.last_pi)
	}
	if pi := sess.cpending[bDur][bSeq]; pi != 0 || len(sess.deferred) != 0 {
		t.Fatalf("B was offered again after its consumer was torn down (identifier %d, deferred %d)", pi, len(sess.deferred))
	}
}

// W-D2. The retained capacity-zero downgrade defers nothing: its request latches the demand with
// nobody deferred. While A holds the only Receive Maximum slot the demand is served; once A is
// released it retires.
//
// Mutation M-dem-a: the latch serves forever.
func TestF097DriverDemandOfARetainedDowngradeRetires(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-dem-retained", 1)
	sess := f.e.sess
	rec, dues := testDriverDemandSeams(t, sess)
	testCustodyRM1(t, sess)
	sess.mu.Lock()
	pi := sess.trackPublishRetained(1)
	deferred, latched := len(sess.deferred), sess.custodyDemand
	sess.mu.Unlock()
	if pi != 0 || deferred != 0 || !latched {
		t.Fatalf("fixture: the retained request must be downgraded at capacity zero with nothing deferred "+
			"(identifier %d, deferred %d, demand %v)", pi, deferred, latched)
	}
	testDriverDemandStaysServed(t, sess, dues, "A holds the only Receive Maximum slot")
	testDriverWithdrawA(t, f)
	testDriverDemandRetiresQuietly(t, f, rec, dues)
	if c := testCustodyCapacity(sess); c != 1 {
		t.Fatalf("positive control: the released slot must be free, capacity %d", c)
	}
}

// W-D3. Allocator exhaustion with a KNOWN held identifier latches the demand with nothing
// deferred and no identifier unknown (no Receive Maximum, every other identifier live in the
// scheduling map). While the holder's exchange stands the demand is served; once it ends and is
// released, the latch retires.
//
// Mutation M-dem-b (retire on !withholdsAllocation() alone): the demand retires while A's
// exchange is still held, and the served-while-held control fails.
func TestF097DriverDemandOfAnExhaustedAllocatorRetires(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f097-dem-exhausted", 1)
	sess := f.e.sess
	rec, dues := testDriverDemandSeams(t, sess)
	filler := &mqttPending{}
	sess.mu.Lock()
	sess.rmax = 0
	sess.maxp = 0xFFFF
	delete(sess.pendingPublish, f.pi)
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
	if got := testCustodyMints(sess, f.pi); got != 0 {
		t.Fatalf("fixture: allocation must be exhausted, got %d", got)
	}
	sess.mu.Lock()
	latched, deferred := sess.custodyDemand, len(sess.deferred)
	unknown := sess.custody.withholdsAllocation()
	held := sess.custody.holdsPI(f.pi)
	sess.mu.Unlock()
	if !latched || deferred != 0 || unknown || !held {
		t.Fatalf("fixture: exhaustion over a known held identifier must latch with nothing deferred "+
			"(demand %v, deferred %d, unknown %v, held %v)", latched, deferred, unknown, held)
	}
	testDriverDemandStaysServed(t, sess, dues, "A's identifier is held by its standing exchange")
	testDriverWithdrawA(t, f)
	testDriverDemandRetiresQuietly(t, f, rec, dues)
}

// A HELD ADMISSION is custody too: an admission whose committed attempt is not yet settled holds
// its identifier and its charge with no exchange entry behind it, and a re-observation may still
// settle it. The demand is served while it is the only holder. The settlement (Landed) moves the
// custody to the exchange, which keeps it served; once that exchange ends and is released, the
// latch retires.
//
// Mutation M-dem-d (holdsCustody counts held exchanges only): the demand retires while the
// admission is the only holder, and the served-while-held assertion fails.
func TestF097DriverDemandServedWhileAnAdmissionIsHeld(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "f097-dem-admission")
	testReadyForRealPubRelDelivery(t, e)
	sess := e.sess
	rec, dues := testDriverDemandSeams(t, sess)
	testCustodyRM1(t, sess)
	// T4d's device: the admission's write lands but its response is lost, and every settlement
	// stays unresolved while held, so the admission keeps its reservation and no exchange entry is
	// charged.
	var lost atomic.Bool
	var hold atomic.Bool
	hold.Store(true)
	e.asm.mu.Lock()
	e.asm.testAdmitWrite = func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if err == nil && lost.CompareAndSwap(false, true) {
			return out, errors.New("test: the admission's response was lost")
		}
		return out, err
	}
	e.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
		if hold.Load() {
			return mqttTransitionUnresolved, true
		}
		return 0, false
	}
	e.asm.mu.Unlock()
	f := testWithdrawAdmitMore(t, e, testRealPubRelSrcSeq, 1)
	// The retained downgrade at capacity zero latches the demand and defers nothing.
	sess.mu.Lock()
	rpi := sess.trackPublishRetained(1)
	latched, deferred := sess.custodyDemand, len(sess.deferred)
	unknown := sess.custody.withholdsAllocation()
	_, admission := sess.custody.heldAdmissions[f.nonce]
	exchanges := len(sess.custody.heldExchanges)
	sess.mu.Unlock()
	if rpi != 0 || !latched || deferred != 0 || unknown || !admission || exchanges != 0 {
		t.Fatalf("fixture: only the held admission may stand (retained identifier %d, demand %v, deferred %d, "+
			"unknown %v, admission held %v, exchanges %d)", rpi, latched, deferred, unknown, admission, exchanges)
	}
	testDriverDemandStaysServed(t, sess, dues, "the admission is the only holder")
	if !testCustodyHasAdmission(sess, f.nonce) || testCustodyHasExchange(sess, f.lin) {
		t.Fatal("fixture: the admission was settled during the served window")
	}
	hold.Store(false)
	testCustodyEventually(t, "the admission's attempt settled Landed and the admission went", func() bool {
		return !testCustodyHasAdmission(sess, f.nonce) && testCustodyHasExchange(sess, f.lin)
	})
	testDriverDemandStaysServed(t, sess, dues, "the settled admission's exchange is held")
	testDriverWithdrawA(t, f)
	testDriverDemandRetiresQuietly(t, f, rec, dues)
}

// Negative controls: the demand is SERVED while an allocation may still wait - a delivery is
// deferred, or an identifier is unknown - even with no custody held. (A held exchange is W-D1..3's
// served-while-held phase.)
func TestF097DriverDemandServedWhileAnAllocationMayWait(t *testing.T) {
	t.Parallel()
	t.Run("deferred-waiter", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "f097-dem-ctl-deferred", 1)
		sess := f.e.sess
		_, dues := testDriverDemandSeams(t, sess)
		// A's custody is resolved FIRST, so no holder stands. Then the local scheduling budget (no
		// Receive Maximum) is spent by an entry the ledger does not hold, so B is withheld at the
		// delivery gate and no custody release can re-offer it.
		testDriverWithdrawA(t, f)
		filler := &mqttPending{}
		sess.mu.Lock()
		delete(sess.pendingPublish, f.pi)
		sess.pendingPublish[f.pi+1] = filler
		sess.rmax = 0
		sess.maxp = uint16(len(sess.pendingPublish))
		bPI, _ := sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq+1, 1))
		latched := sess.custodyDemand
		sess.mu.Unlock()
		t.Cleanup(func() {
			sess.mu.Lock()
			delete(sess.pendingPublish, f.pi+1)
			sess.mu.Unlock()
		})
		if bPI != 0 || !latched {
			t.Fatalf("fixture: B must be withheld at the delivery gate (identifier %d, demand %v)", bPI, latched)
		}
		sess.mu.Lock()
		deferred := len(sess.deferred)
		holds := len(sess.custody.heldAdmissions) + len(sess.custody.heldExchanges)
		unknown := sess.custody.withholdsAllocation()
		sess.mu.Unlock()
		if deferred != 1 || holds != 0 || unknown {
			t.Fatalf("fixture: only the deferred waiter may stand (deferred %d, holders %d, unknown %v)",
				deferred, holds, unknown)
		}
		testDriverDemandStaysServed(t, sess, dues, "B is deferred")
	})
	t.Run("unknown-identifier", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "f097-dem-ctl-unknown", 1)
		sess := f.e.sess
		_, dues := testDriverDemandSeams(t, sess)
		xsubj := mqttExchangeSubject(sess.idHash, f.gen, f.lin)
		f.e.asm.mu.Lock()
		f.e.asm.testCustodyDecodeFault = func(subject string) bool { return subject == xsubj }
		f.e.asm.mu.Unlock()
		// A fresh ledger for the same generation, rebuilt as a resume would: A's exchange cannot be
		// decoded, so its identifier is unknown and no holder names it.
		sess.mu.Lock()
		sess.custody = newMQTTCustodyLedger(f.gen)
		delete(sess.pendingPublish, f.pi)
		sess.mu.Unlock()
		sess.rebuildCustodyForResume(f.gen)
		if got := testCustodyMints(sess, f.pi); got != 0 {
			t.Fatalf("fixture: allocation must be withheld with an identifier unknown, got %d", got)
		}
		sess.mu.Lock()
		latched, deferred := sess.custodyDemand, len(sess.deferred)
		holds := len(sess.custody.heldAdmissions) + len(sess.custody.heldExchanges)
		unknown := sess.custody.subjectUncertain(f.gen, xsubj)
		sess.mu.Unlock()
		if !latched || deferred != 0 || holds != 0 || !unknown {
			t.Fatalf("fixture: only the unknown identifier may stand (demand %v, deferred %d, holders %d, unknown %v)",
				latched, deferred, holds, unknown)
		}
		testDriverDemandStaysServed(t, sess, dues, "A's identifier is unknown")
	})
}
