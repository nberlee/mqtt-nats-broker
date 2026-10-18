package mqtt

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testValidateRecordTx is validateRecordTxLocked with the session lock taken, so
// tests can ask "would this token authorize a write right now?" without racing
// the fence they are exercising.
func testValidateRecordTx(sess *mqttSession, tok recordTx) error {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.validateRecordTxLocked(tok)
}

// testSessSave persists a session inside its own record transaction. Tests that
// only need "make this state durable" use it instead of hand-rolling the fence.
func testSessSave(t testing.TB, sess *mqttSession) error {
	t.Helper()
	return testSessRecordTx(t, sess, sess.save)
}

// testSessRecordTx runs fn inside a record transaction, as a caller on the
// readLoop would.
func testSessRecordTx(t testing.TB, sess *mqttSession, fn func(tok recordTx) error) error {
	t.Helper()
	tok := sess.beginRecordTx()
	err := fn(tok)
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	return err
}

// TestRecordTxRequiresAnOpenTransaction is the base rule: a record write is
// authorized by an OPEN transaction, never by the caller merely holding a token.
//
// Regression: save() used to write whenever it was called, so a writer could
// persist state another transaction had staged and was about to roll back — the
// mutations happen before save(), so serializing the stores alone never fixed it.
func TestRecordTxRequiresAnOpenTransaction(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	s := testRigServer(t, nc, "node1")
	c := s.createInternalAccountClient()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	sess, _, err := asm.createOrRestoreSession("recordtx", s.getOpts())
	if err != nil {
		t.Fatalf("createOrRestoreSession: %v", err)
	}

	if err := sess.save(recordTx{}); err == nil {
		t.Fatal("save with no open transaction must be refused")
	}

	tok := sess.beginRecordTx()
	if err := sess.save(tok); err != nil {
		t.Fatalf("save inside its own transaction: %v", err)
	}
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The very same token, after its transaction closed, must not write again.
	if err := sess.save(tok); err == nil {
		t.Fatal("save with a token whose transaction has closed must be refused")
	}
}

// TestRecordTxTokenIdentity covers the identity rules a bare epoch check cannot
// give: a token copied from a COMPLETED transaction must not write or close a
// LATER one within the same generation, and a nested token must not close the
// transaction it was handed.
//
// Go has no goroutine identity, so ownership cannot be inferred from "a fence is
// open" — a callee that observed one would otherwise assume it was its own.
func TestRecordTxTokenIdentity(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "identity"}

	first := sess.beginRecordTx()
	if err := sess.endRecordTx(first); err != nil {
		t.Fatalf("endRecordTx(first): %v", err)
	}

	second := sess.beginRecordTx()
	if first.epoch != second.epoch {
		t.Fatalf("the two transactions should share a generation, got %v and %v", first.epoch, second.epoch)
	}
	if first.id == second.id {
		t.Fatal("each transaction must have its own identity")
	}
	// The stale token matches the epoch but not the open transaction.
	if err := testValidateRecordTx(sess, first); err == nil {
		t.Fatal("a token from a completed transaction must not authorize a write in a later one")
	}
	if err := sess.endRecordTx(first); err == nil {
		t.Fatal("a stale token must not be able to close a newer transaction")
	}
	// ...and the newer transaction is still open, untouched by that attempt.
	if err := testValidateRecordTx(sess, second); err != nil {
		t.Fatalf("the open transaction was disturbed by a stale token's release: %v", err)
	}
	if err := sess.endRecordTx(second); err != nil {
		t.Fatalf("the open transaction's token must be able to close it: %v", err)
	}
}

// TestRecordTxGenerationBinding is the clear() rule (I9): ending a generation
// kills every token minted before it, INCLUDING on a session object that is then
// reused as a fresh session — which is exactly what the clean-start discard and
// the overdue-record path do.
//
// Regression: clear() zeroed sess.seq and then deleted the record, so a late
// writer stored with no CAS header at all and RECREATED a terminated session's
// record.
func TestRecordTxGenerationBinding(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "generation"}

	stale := sess.beginRecordTx()
	if err := sess.endRecordTxNewGeneration(stale); err != nil {
		t.Fatalf("endRecordTxNewGeneration: %v", err)
	}

	fresh := sess.beginRecordTx()
	if fresh.epoch == stale.epoch {
		t.Fatal("ending a generation must bump the record epoch")
	}
	if err := testValidateRecordTx(sess, stale); err == nil {
		t.Fatal("a token from the terminated generation must not authorize a write")
	}
	// The REUSED object writes normally at the new epoch.
	if err := testValidateRecordTx(sess, fresh); err != nil {
		t.Fatalf("the reused session must write normally at its new generation: %v", err)
	}
	if err := sess.endRecordTx(fresh); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	// The check above is satisfied by transaction identity alone, so it does not
	// isolate the epoch. This does: the generation ends underneath a transaction
	// that is still open and still the current one, so ONLY the epoch can refuse
	// it. That is the state a discard racing a still-running writer produces —
	// exactly what used to let a late writer recreate a terminated record.
	open := sess.beginRecordTx()
	sess.mu.Lock()
	sess.recordEpoch++
	sameID := open.id == sess.recordTxID
	sess.mu.Unlock()
	if !sameID {
		t.Fatal("the transaction must still be the open one for this check to isolate the epoch")
	}
	if err := testValidateRecordTx(sess, open); err == nil {
		t.Fatal("a token whose generation ended must not authorize a write, even while its transaction is open")
	}
}

// TestRecordTxTryBeginNeverWaits pins the off-readLoop form: a writer that cannot
// get the fence is told so rather than blocking, so its work stays pending and
// retryable — and, crucially, it cannot observe or persist a half-applied
// readLoop transaction.
func TestRecordTxTryBeginNeverWaits(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "trybegin"}

	held := sess.beginRecordTx()
	if _, ok := sess.tryBeginRecordTx(); ok {
		t.Fatal("tryBeginRecordTx must fail while another writer holds the fence")
	}
	if err := sess.endRecordTx(held); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}

	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		t.Fatal("tryBeginRecordTx must succeed on a free fence")
	}
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
}

// TestRecordTxTryBeginDefersToQueuedWaiter is waiter priority: a blocking
// acquisition announces itself before it contends on sess.mu, and no non-blocking
// acquisition may take the fence ahead of it.
//
// Regression: sync.Cond gives no handoff, so a sustained stream of try-callers could
// pass the fence between themselves indefinitely while a blocking caller starved —
// and because a blocking acquisition is usually made holding the session ownership
// gate, that starvation also failed every same-client-ID CONNECT meanwhile.
//
// The parts are ordered from the claim itself to the ordering that makes it hold:
// the refusal, the barge-free window, the exactly-once withdrawal, and the three
// pre-lock/under-mutex orderings the double check consists of.
func TestRecordTxTryBeginDefersToQueuedWaiter(t *testing.T) {
	// (1) The refusal itself, stated directly. Raising the count is exactly the
	// announced-but-not-yet-locked state, since the announce precedes the mutex. Both
	// checks must reject EVERY positive count, not just one: the design admits any
	// number of blocking callers.
	t.Run("refuses on a free fence while a waiter is announced", func(t *testing.T) {
		sess := &mqttSession{id: "waiter-priority"}
		for _, count := range []int64{1, 2} {
			sess.recordTxWaiters.Store(count)
			if tok, ok := sess.tryBeginRecordTx(); ok {
				t.Fatalf("with %d waiter(s) announced, a non-blocking acquisition must be refused on a free fence (got transaction %v)",
					count, tok.id)
			}
		}
		sess.recordTxWaiters.Store(0)
		tok, ok := sess.tryBeginRecordTx()
		if !ok {
			t.Fatal("with no waiter announced, a non-blocking acquisition must succeed on a free fence")
		}
		if err := sess.endRecordTx(tok); err != nil {
			t.Fatalf("endRecordTx: %v", err)
		}
	})

	// (2) The window the defect lived in: a real waiter parked behind a held fence
	// while try-callers storm the fence. Repeated, because the barge it looks for was
	// an interleaving rather than a state.
	t.Run("no try-caller acquires ahead of a parked waiter", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			testRecordTxStormRound(t)
		}
	})

	// (3) The announce is withdrawn EXACTLY once on normal completion — the explicit
	// withdrawal under sess.mu, never also the guard. A double withdrawal drives the
	// count negative and permanently disables the priority check, a state no other
	// assertion here reaches: they read the count before withdrawal.
	t.Run("withdraws the announce exactly once", func(t *testing.T) {
		sess := &mqttSession{id: "withdraw-once"}
		uncontended := sess.beginRecordTx()
		if n := sess.recordTxWaiters.Load(); n != 0 {
			t.Fatalf("after an uncontended acquisition the announce must be withdrawn exactly once, count = %d", n)
		}

		// ...and after one that actually waited, which is the path with a wait loop
		// between the announce and the withdrawal.
		waited := make(chan int64, 1)
		acquired := make(chan struct{})
		go func() {
			tok := sess.beginRecordTx()
			waited <- sess.recordTxWaiters.Load()
			close(acquired)
			if err := sess.endRecordTx(tok); err != nil {
				t.Errorf("endRecordTx (waiter): %v", err)
			}
		}()
		if err := sess.endRecordTx(uncontended); err != nil {
			t.Fatalf("endRecordTx: %v", err)
		}
		<-acquired
		if n := <-waited; n != 0 {
			t.Fatalf("after a waiting acquisition the announce must be withdrawn exactly once, count = %d", n)
		}
	})

	// (4) The announce precedes the mutex. The raw mutex is the instrument: holding it
	// pins any goroutine at its Lock() while leaving everything before the Lock() free
	// to run, so an announce made after Lock() provably cannot move the count.
	t.Run("announces before contending on the session lock", func(t *testing.T) {
		sess := &mqttSession{id: "announce-order"}
		done := make(chan struct{})
		sess.mu.Lock()
		go func() {
			defer close(done)
			tok := sess.beginRecordTx()
			if err := sess.endRecordTx(tok); err != nil {
				t.Errorf("endRecordTx (waiter): %v", err)
			}
		}()
		announced := false
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
			if sess.recordTxWaiters.Load() == 1 {
				announced = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		sess.mu.Unlock()
		<-done
		if !announced {
			t.Fatal("a blocking acquisition must announce itself BEFORE contending on sess.mu; " +
				"an announce made under the lock cannot be seen while the lock is held, which is the window try-callers used to barge through")
		}
		if n := sess.recordTxWaiters.Load(); n != 0 {
			t.Fatalf("count = %d after the waiter completed, want 0", n)
		}
	})

	// (5) The under-mutex reload is authoritative: a try-caller that passed the
	// pre-lock fast check on a count of zero must still refuse if the announce landed
	// while it was on its way to the lock. Driven causally through the pre-lock seam,
	// with a count of TWO so the same run also refuses a check that only recognizes
	// one waiter.
	t.Run("the under-mutex reload refuses an announce that landed after the fast check", func(t *testing.T) {
		sess := &mqttSession{id: "reload-authoritative"}
		parked, unpark, restore := testParkTryCallerPreLock(t, sess)
		defer restore()
		out := testTryBeginAsync(t, sess)
		<-parked
		sess.recordTxWaiters.Store(2)
		unpark()
		if ok := <-out; ok {
			t.Fatal("a try-caller whose fast check saw no waiter must still refuse at its under-mutex reload; " +
				"acquiring on the cached zero is the race the double check exists to close")
		}
		sess.recordTxWaiters.Store(0)
	})

	// (5′) ...and that reload sits UNDER the mutex, not merely after the seam. Same
	// seam, different interleaving: the test holds sess.mu across the unpark, so a
	// reload hoisted ahead of the Lock() runs on a stale zero and the announce lands
	// while the caller is pinned.
	t.Run("the reload happens under the session lock", func(t *testing.T) {
		sess := &mqttSession{id: "reload-under-lock"}
		parked, unpark, restore := testParkTryCallerPreLock(t, sess)
		defer restore()
		out := testTryBeginAsync(t, sess)
		<-parked
		sess.mu.Lock()
		unpark()
		// Causal, not a sleep: the try-caller is pinned at its own sess.mu.Lock(), which
		// proves it has run everything BEFORE that lock and nothing after it. A reload
		// hoisted above the lock has therefore already happened, on a count of zero,
		// while the authoritative one has not happened at all.
		testWaitBlockedOnSessLock(t, "tryBeginRecordTx")
		sess.recordTxWaiters.Store(1)
		sess.mu.Unlock()
		if ok := <-out; ok {
			t.Fatal("the authoritative reload must happen UNDER sess.mu: a reload made before the lock is " +
				"arbitrarily stale by the time the fence is opened")
		}
		sess.recordTxWaiters.Store(0)
	})

	// (6) A refused try-caller never touches sess.mu. That is a liveness property, not
	// an optimization: without it, a storm of try-callers keeps contending for the
	// mutex the announced waiter needs, and sync.Mutex documents no fairness. Stated as
	// an impossibility — the call must RETURN while the test still holds the lock —
	// and run at both counts, since a fast check that only recognizes a single waiter
	// falls through to the lock at two.
	t.Run("a refused try-caller never locks the session", func(t *testing.T) {
		for _, count := range []int64{1, 2} {
			sess := &mqttSession{id: "fast-refusal"}
			sess.recordTxWaiters.Store(count)
			out := make(chan bool, 1)
			sess.mu.Lock()
			go func() {
				_, ok := sess.tryBeginRecordTx()
				out <- ok
			}()
			var ok, returned bool
			select {
			case ok = <-out:
				returned = true
			case <-time.After(2 * time.Second):
			}
			sess.mu.Unlock()
			if !returned {
				<-out // it can only be blocked on the lock we just released
				t.Fatalf("with %d waiter(s) announced, a try-caller must refuse WITHOUT taking sess.mu; "+
					"it blocked on the lock instead, which is the mutex pressure that starves the waiter", count)
			}
			if ok {
				t.Fatalf("with %d waiter(s) announced, the acquisition must be refused", count)
			}
		}
	})
}

// testRecordTxStormRound parks one real waiter behind a held fence, storms the fence
// with non-blocking acquisitions, and reports any that got in ahead of the waiter.
//
// The waiter is synchronized through the wait-loop seam rather than by polling: the
// seam fires under sess.mu inside the loop, which is the causal point at which the
// waiter is provably announced and parked. Every storm success samples the waiter's
// flag AFTER acquiring, so a stale pre-sample cannot record a barge that never
// happened — on fixed code a success is only reachable once the waiter has finished.
func testRecordTxStormRound(t *testing.T) {
	t.Helper()
	sess := &mqttSession{id: "storm"}
	parked := make(chan struct{}, 1)
	restore := setMQTTTestRecordTxWaitHook(func(s *mqttSession) {
		if s != sess {
			return
		}
		// Runs under sess.mu: never block, never touch the test's channels
		// synchronously.
		select {
		case parked <- struct{}{}:
		default:
		}
	})
	defer restore()

	var (
		waiterAcquired atomic.Bool
		barges         atomic.Int64
		stormWG        sync.WaitGroup
	)
	stop := make(chan struct{})
	waiterDone := make(chan struct{})
	holder := sess.beginRecordTx()

	// Registered before the first assertion, so a failure cannot leave the waiter
	// parked inside a fence nobody releases. Unwinds as release, stop, join.
	stopOnce := sync.OnceFunc(func() { close(stop) })
	releaseOnce := sync.OnceFunc(func() {
		if err := sess.endRecordTx(holder); err != nil {
			t.Errorf("endRecordTx (holder): %v", err)
		}
	})
	defer stormWG.Wait()
	defer func() { <-waiterDone }()
	defer stopOnce()
	defer releaseOnce()

	go func() {
		defer close(waiterDone)
		tok := sess.beginRecordTx()
		waiterAcquired.Store(true)
		if err := sess.endRecordTx(tok); err != nil {
			t.Errorf("endRecordTx (waiter): %v", err)
		}
	}()

	<-parked
	// The deterministic statement of the announce: at the seam the waiter has
	// announced itself and not yet withdrawn.
	if n := sess.recordTxWaiters.Load(); n != 1 {
		t.Fatalf("a parked waiter must be announced: count = %d, want 1", n)
	}

	for i := 0; i < 4; i++ {
		stormWG.Add(1)
		go func() {
			defer stormWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tok, ok := sess.tryBeginRecordTx()
				if !ok {
					continue
				}
				if !waiterAcquired.Load() {
					barges.Add(1)
				}
				if err := sess.endRecordTx(tok); err != nil {
					t.Errorf("endRecordTx (storm): %v", err)
					return
				}
			}
		}()
	}

	releaseOnce()
	<-waiterDone
	stopOnce()
	stormWG.Wait()
	if n := barges.Load(); n != 0 {
		t.Fatalf("%d non-blocking acquisition(s) took the fence ahead of a queued waiter", n)
	}
}

// testParkTryCallerPreLock installs the pre-lock seam so one try-caller on sess stops
// between its fast check and sess.mu.Lock(). That seam runs OUTSIDE sess.mu, which is
// what makes parking there safe and lets a test drive the double check's two halves
// causally instead of by timing.
func testParkTryCallerPreLock(t *testing.T, sess *mqttSession) (parked <-chan struct{}, unpark func(), restore func()) {
	t.Helper()
	at := make(chan struct{}, 1)
	release := make(chan struct{})
	unparkOnce := sync.OnceFunc(func() { close(release) })
	restoreHook := setMQTTTestTryBeginPreLockHook(func(s *mqttSession) {
		if s != sess {
			return
		}
		select {
		case at <- struct{}{}:
		default:
		}
		<-release
	})
	return at, unparkOnce, func() {
		// Unpark before uninstalling: a caller still parked in the hook must never be
		// left there by a failing assertion.
		unparkOnce()
		restoreHook()
	}
}

// testTryBeginAsync runs one non-blocking acquisition on its own goroutine and reports
// whether it got the fence, releasing it if it did.
func testTryBeginAsync(t *testing.T, sess *mqttSession) <-chan bool {
	t.Helper()
	out := make(chan bool, 1)
	go func() {
		tok, ok := sess.tryBeginRecordTx()
		if ok {
			if err := sess.endRecordTx(tok); err != nil {
				t.Errorf("endRecordTx (try-caller): %v", err)
			}
		}
		out <- ok
	}()
	return out
}

// testWaitBlockedOnSessLock waits until some goroutine is provably parked acquiring a
// mutex inside fn, and fails the test if none ever is.
//
// It replaces "sleep and hope it got there": a goroutine observed here has run
// everything up to its Lock() and nothing after it, which is exactly the state a test
// needs before publishing the state the code under test must observe UNDER that lock.
func testWaitBlockedOnSessLock(t *testing.T, fn string) {
	t.Helper()
	// The dump must FIT. runtime.Stack truncates silently at len(buf) and reports the
	// truncated length, so a fixed buffer turns "the process has many goroutines" into "the
	// parked goroutine is not there" — the helper then waits out its whole deadline and
	// fails a test whose subject is working correctly. Grow until the dump fits, which is
	// the standard idiom; a full suite run can carry far more than 1 MiB of stacks.
	stacks := func() string {
		for size := 1 << 20; ; size *= 2 {
			buf := make([]byte, size)
			if n := runtime.Stack(buf, true); n < size {
				return string(buf[:n])
			}
		}
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		for _, g := range strings.Split(stacks(), "\n\ngoroutine ") {
			if !strings.Contains(g, fn) || !strings.Contains(g, "sync.(*Mutex).Lock") {
				continue
			}
			// Parked, not merely inside Lock: the fast path never reaches either of
			// these.
			if strings.Contains(g, "lockSlow") || strings.Contains(g, "Semacquire") {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no goroutine ever parked acquiring the session lock inside %s", fn)
}

// TestRecordTxAbnormalWithdrawalPrecedesTheUnlock pins the ordering of the withdrawal
// guard against the unlock: a waiter that dies abnormally must withdraw its announce
// BEFORE it releases sess.mu.
//
// Unlocking first would publish a free fence with a dead waiter still announced, and
// the next goroutine to take sess.mu decides things on that count — a fence release
// suppresses the void-verdict handoff while a waiter is announced
// (voidVerdictHandoffPendingLocked), and the withdrawal behind it has no release of its
// own to re-trigger the attempt, so the staged verdict stays memory-only until some
// later event or the periodic backstop.
//
// Stated as a FACT about the ordering rather than as a race: the guard's seam fires
// immediately before the withdrawal, and TryLock there answers "is sess.mu held" —
// false while this dying goroutine still holds it, true once it has released it. The
// test contends for nothing, so neither answer depends on the scheduler. An observer
// racing for the lock instead would have to win a window of a few instructions, which
// is exactly why that shape was abandoned (it left M13 alive).
//
// Mutation M13: withdraw after the unlock (`defer withdrawAtExit()` before a plain
// `defer sess.mu.Unlock()`, which is what the first implementation did) — the seam
// finds the lock free.
func TestRecordTxAbnormalWithdrawalPrecedesTheUnlock(t *testing.T) {
	sess := &mqttSession{id: "goexit-order"}
	var fired, lockFree atomic.Bool
	restoreGuard := setMQTTTestRecordTxWithdrawGuardHook(func(s *mqttSession) {
		if s != sess {
			return
		}
		fired.Store(true)
		if sess.mu.TryLock() {
			lockFree.Store(true)
			sess.mu.Unlock()
		}
	})
	defer restoreGuard()
	restoreWait := setMQTTTestRecordTxWaitHook(func(s *mqttSession) {
		if s == sess {
			runtime.Goexit() // as t.Fatal in a hook would
		}
	})
	defer restoreWait()

	holder := sess.beginRecordTx()
	died := make(chan struct{})
	go func() {
		defer close(died)
		sess.beginRecordTx() // dies at the wait seam, announce outstanding
	}()
	<-died

	if !fired.Load() {
		t.Fatal("the withdrawal guard never ran for the dying waiter, so this test proves nothing about its ordering")
	}
	if lockFree.Load() {
		t.Fatal("a dying waiter withdrew its announce with sess.mu already released; the next holder of the lock " +
			"decides the void-verdict handoff on that count, and a withdrawal behind it has no release to re-trigger the attempt")
	}
	if n := sess.recordTxWaiters.Load(); n != 0 {
		t.Fatalf("count = %d after the waiter died, want 0", n)
	}
	if err := sess.endRecordTx(holder); err != nil {
		t.Fatalf("endRecordTx (holder): %v", err)
	}
}

// TestRecordTxBlockingAcquisitionBoundedUnderTryStorm is the positive form of the
// starvation bound: a blocking acquisition completes under a continuous storm of
// non-blocking ones. It is a scheduler stress test — the deterministic statements
// live in TestRecordTxTryBeginDefersToQueuedWaiter — and the bound is deliberately
// enormous against the holders it competes with, which do nothing but validate a
// token.
func TestRecordTxBlockingAcquisitionBoundedUnderTryStorm(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "bounded-under-storm"}
	const (
		stormers     = 8
		acquisitions = 20
		bound        = 5 * time.Second
	)

	var stormWG sync.WaitGroup
	stop := make(chan struct{})
	stopOnce := sync.OnceFunc(func() { close(stop) })
	defer stormWG.Wait()
	defer stopOnce()

	for i := 0; i < stormers; i++ {
		stormWG.Add(1)
		go func() {
			defer stormWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if tok, ok := sess.tryBeginRecordTx(); ok {
					if err := sess.endRecordTx(tok); err != nil {
						t.Errorf("endRecordTx (storm): %v", err)
						return
					}
				}
			}
		}()
	}

	acquired := make(chan struct{}, acquisitions)
	blockingDone := make(chan struct{})
	go func() {
		defer close(blockingDone)
		for k := 0; k < acquisitions; k++ {
			tok := sess.beginRecordTx()
			if err := sess.endRecordTx(tok); err != nil {
				t.Errorf("endRecordTx (blocking): %v", err)
				return
			}
			acquired <- struct{}{}
		}
	}()

	for k := 0; k < acquisitions; k++ {
		select {
		case <-acquired:
		case <-time.After(bound):
			t.Fatalf("blocking acquisition %d of %d did not complete within %v under a %d-way try-storm",
				k+1, acquisitions, bound, stormers)
		}
	}
	<-blockingDone
}

// TestRecordTxWaitHookGoexitWithdrawsAnnounce covers the withdrawal guard: an
// announce must be withdrawn on EVERY exit from beginRecordTx, including one that
// never reaches the explicit withdrawal.
//
// runtime.Goexit is what t.Fatal does, and it runs deferred functions while skipping
// the straight-line code after it — so a "handlers must not panic" contract is not
// enough. A stranded announce would refuse every later non-blocking acquisition on
// the session for as long as it lives.
func TestRecordTxWaitHookGoexitWithdrawsAnnounce(t *testing.T) {
	sess := &mqttSession{id: "goexit-withdraws"}
	holder := sess.beginRecordTx()
	restore := setMQTTTestRecordTxWaitHook(func(s *mqttSession) {
		if s != sess {
			return
		}
		runtime.Goexit()
	})
	defer restore()

	died := make(chan struct{})
	go func() {
		defer close(died)
		sess.beginRecordTx() // never returns: the seam ends this goroutine
	}()
	<-died

	if n := sess.recordTxWaiters.Load(); n != 0 {
		t.Fatalf("a waiter that died at the wait seam must withdraw its announce, count = %d", n)
	}
	if err := sess.endRecordTx(holder); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		t.Fatal("a stranded announce refuses every non-blocking acquisition for the life of the session")
	}
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
}

// TestRecordTxWithdrawGuardHookGoexitStillCleansUp is the guard's own cleanup path made
// exception-safe: a seam that dies mid-cleanup must not become another way to strand the
// announce — or, worse, the session lock.
//
// The withdrawal and the unlock are therefore deferred AROUND the seam rather than
// sequenced after it. Without that, a handler that Goexits leaves the count positive
// (refusing every later non-blocking acquisition for the life of the session) and sess.mu
// locked forever, which is the abnormal-exit class the guard was added to close.
//
// Mutation M14a: call `withdraw()` after the seam instead of deferring it — the count
// stays 1.
// Mutation M14b: unlock after `withdrawAtExit()` instead of deferring it — sess.mu is
// never released.
func TestRecordTxWithdrawGuardHookGoexitStillCleansUp(t *testing.T) {
	sess := &mqttSession{id: "guard-goexit"}
	holder := sess.beginRecordTx()
	restoreGuard := setMQTTTestRecordTxWithdrawGuardHook(func(s *mqttSession) {
		if s == sess {
			runtime.Goexit() // a t.Fatal inside the guard's own seam
		}
	})
	defer restoreGuard()
	restoreWait := setMQTTTestRecordTxWaitHook(func(s *mqttSession) {
		if s == sess {
			runtime.Goexit() // ...reached from an abnormal exit at the wait seam
		}
	})
	defer restoreWait()

	died := make(chan struct{})
	go func() {
		defer close(died)
		sess.beginRecordTx()
	}()
	<-died

	if n := sess.recordTxWaiters.Load(); n != 0 {
		t.Fatalf("a guard seam that died mid-cleanup left %d announced waiter(s); the withdrawal must be deferred around it", n)
	}
	if !sess.mu.TryLock() {
		t.Fatal("a guard seam that died mid-cleanup left sess.mu held, which wedges every operation on the session")
	}
	sess.mu.Unlock()

	// ...and the fence itself still works afterwards.
	if err := sess.endRecordTx(holder); err != nil {
		t.Fatalf("endRecordTx (holder): %v", err)
	}
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		t.Fatal("the fence must still admit a non-blocking acquisition after the guard's seam died")
	}
	if err := sess.endRecordTx(tok); err != nil {
		t.Fatalf("endRecordTx: %v", err)
	}
}

// TestRecordTxSerializesWriters runs concurrent writers through the fence under
// -race and asserts that transactions never interleave: at most one is open at a
// time, and every one of them is the transaction its own token names.
func TestRecordTxSerializesWriters(t *testing.T) {
	t.Parallel()
	sess := &mqttSession{id: "concurrent"}

	var (
		mu     sync.Mutex
		inside int
		peak   int
	)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				tok := sess.beginRecordTx()
				mu.Lock()
				inside++
				if inside > peak {
					peak = inside
				}
				mu.Unlock()

				if err := testValidateRecordTx(sess, tok); err != nil {
					t.Errorf("a held transaction must authorize its own writes: %v", err)
				}

				mu.Lock()
				inside--
				mu.Unlock()
				if err := sess.endRecordTx(tok); err != nil {
					t.Errorf("endRecordTx: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if peak != 1 {
		t.Fatalf("%d transactions were open at once; the fence must admit exactly one", peak)
	}
}
