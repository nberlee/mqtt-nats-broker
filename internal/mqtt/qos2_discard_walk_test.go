package mqtt

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// Tests for the bounded concurrent discard walk: mqttWalkBounded's admission protocol on plain
// closures, and its wiring into arbitrateQoS2ReleaseClaims on a real embedded JetStream.
//
// Where a mutant is detected because something did NOT happen within testWalkQuiet, that is
// timeout-based evidence, not a proof. The correct implementation never depends on it: every
// positive wait is a handshake under testWalkWatchdog.

const (
	testWalkWatchdog = 10 * time.Second
	testWalkQuiet    = 250 * time.Millisecond
)

func testWalkAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testWalkWatchdog):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestMQTTWalkBoundedIsConcurrentAndBounded pins both halves of the width: the walk runs width
// visits at once (a serial walk never fills the first wave), and never more (an unbounded one
// admits the next visit while the first wave is still parked).
func TestMQTTWalkBoundedIsConcurrentAndBounded(t *testing.T) {
	t.Parallel()
	const width, n = 4, 8

	var (
		mu      sync.Mutex
		visits  = make(map[int]int)
		inside  int
		release = make(chan struct{})
		relOnce sync.Once
		full    = make(chan struct{})
		fullOne sync.Once
		over    = make(chan int, n)
		exits   sync.WaitGroup
		done    = make(chan struct{})
		gotI    int
		gotErr  error
	)
	// Fixed before the walk starts: every index is visited, and counting at entry would race
	// a launched visit that has not been scheduled yet.
	exits.Add(n)
	go func() {
		defer close(done)
		gotI, gotErr = mqttWalkBounded(n, width, func(i int) error {
			defer exits.Done()
			mu.Lock()
			visits[i]++
			inside++
			now := inside
			mu.Unlock()
			if now > width {
				over <- i
			}
			if now == width {
				fullOne.Do(func() { close(full) })
			}
			<-release
			mu.Lock()
			inside--
			mu.Unlock()
			return nil
		})
	}()
	t.Cleanup(func() {
		relOnce.Do(func() { close(release) })
		<-done
		exits.Wait()
	})

	select {
	case <-full:
	case <-time.After(testWalkWatchdog):
		mu.Lock()
		got := inside
		mu.Unlock()
		t.Fatalf("only %d of %d visits ran at once: the walk is not concurrent", got, width)
	}
	select {
	case i := <-over:
		t.Fatalf("visit %d was admitted while %d were in flight: the walk is not bounded by its width", i, width)
	case <-time.After(testWalkQuiet):
	}
	relOnce.Do(func() { close(release) })
	testWalkAwait(t, done, "the walk to return")

	if gotI != -1 || gotErr != nil {
		t.Fatalf("walk = (%d, %v), want (-1, nil)", gotI, gotErr)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n; i++ {
		if visits[i] != 1 {
			t.Fatalf("index %d visited %d times, want exactly once (visits %v)", i, visits[i], visits)
		}
	}
}

// TestMQTTWalkBoundedJoinsItsWorkers pins that the walk returns only after every admitted visit
// has: a discard that returned early would let the teardown delete the session record while a
// hold arbitration is still in flight.
func TestMQTTWalkBoundedJoinsItsWorkers(t *testing.T) {
	t.Parallel()
	const width, n = 4, 5
	last := n - 1

	var (
		release = make(chan struct{})
		relOnce sync.Once
		parked  = make(chan struct{})
		exits   sync.WaitGroup
		done    = make(chan struct{})
	)
	exits.Add(n)
	go func() {
		defer close(done)
		_, _ = mqttWalkBounded(n, width, func(i int) error {
			defer exits.Done()
			if i == last {
				close(parked)
				<-release
			}
			return nil
		})
	}()
	t.Cleanup(func() {
		relOnce.Do(func() { close(release) })
		<-done
		exits.Wait()
	})

	testWalkAwait(t, parked, "the last visit to start")
	select {
	case <-done:
		t.Fatal("the walk returned while a visit it admitted was still running")
	case <-time.After(testWalkQuiet):
	}
	relOnce.Do(func() { close(release) })
	testWalkAwait(t, done, "the walk to return")
}

// TestMQTTWalkBoundedStopsAdmittingAfterAnError pins the admission stop and the returned error.
// Once a failure is published nothing further is admitted, which is what keeps a walk against
// an unavailable server from paying a request timeout for every remaining hold, and the error
// returned is the lowest failing index's whatever order the failures landed in.
func TestMQTTWalkBoundedStopsAdmittingAfterAnError(t *testing.T) {
	t.Parallel()

	t.Run("no admission after the failure is published", func(t *testing.T) {
		t.Parallel()
		const width, n = 4, 9
		errA := errors.New("visit 0 failed")

		var (
			mu        sync.Mutex
			visited   = make(map[int]bool)
			release   = make(chan struct{})
			relOnce   sync.Once
			acks      = make(chan struct{}, width)
			late      = make(chan int, n)
			published = make(chan struct{})
			pubOnce   sync.Once
			done      = make(chan struct{})
			gotI      int
			gotErr    error
		)
		go func() {
			defer close(done)
			gotI, gotErr = mqttWalkBoundedObserved(n, width, func(i int) error {
				mu.Lock()
				visited[i] = true
				mu.Unlock()
				switch {
				case i == 0:
					// Fail only once the whole first wave is in flight, so the slot this
					// failure frees is the only one the dispatcher can get.
					for k := 1; k < width; k++ {
						<-acks
					}
					return errA
				case i < width:
					acks <- struct{}{}
					<-release
					return nil
				default:
					late <- i
					return nil
				}
			}, func(i int) {
				if i == 0 {
					pubOnce.Do(func() { close(published) })
				}
			})
		}()
		// Not run under the join mutation, so the production join covers every visit.
		t.Cleanup(func() {
			relOnce.Do(func() { close(release) })
			<-done
		})

		testWalkAwait(t, published, "visit 0's failure to be published")
		select {
		case i := <-late:
			t.Fatalf("index %d was admitted after a failure was published", i)
		case <-time.After(testWalkQuiet):
		}
		relOnce.Do(func() { close(release) })
		testWalkAwait(t, done, "the walk to return")

		if gotI != 0 || !errors.Is(gotErr, errA) {
			t.Fatalf("walk = (%d, %v), want (0, %v)", gotI, gotErr, errA)
		}
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < n; i++ {
			if want := i < width; visited[i] != want {
				t.Fatalf("index %d visited = %v, want %v (visited %v)", i, visited[i], want, visited)
			}
		}
	})

	t.Run("the lowest failing index wins whatever order the failures land in", func(t *testing.T) {
		t.Parallel()
		const width, n = 4, 4
		err0, err1 := errors.New("visit 0 failed"), errors.New("visit 1 failed")

		var (
			pub1    = make(chan struct{})
			pubOnce sync.Once
			done    = make(chan struct{})
			gotI    int
			gotErr  error
		)
		go func() {
			defer close(done)
			gotI, gotErr = mqttWalkBoundedObserved(n, width, func(i int) error {
				switch i {
				case 0:
					// Fails strictly after visit 1's failure was published.
					<-pub1
					return err0
				case 1:
					return err1
				default:
					return nil
				}
			}, func(i int) {
				if i == 1 {
					pubOnce.Do(func() { close(pub1) })
				}
			})
		}()
		t.Cleanup(func() {
			pubOnce.Do(func() { close(pub1) })
			<-done
		})

		testWalkAwait(t, done, "the walk to return")
		if gotI != 0 || !errors.Is(gotErr, err0) {
			t.Fatalf("walk = (%d, %v), want (0, %v): the error must not depend on completion order", gotI, gotErr, err0)
		}
	})
}

// testWalkHolds stores k real holds for the env's session and returns their subjects in walk
// (sorted) order with their sequences.
func testWalkHolds(t *testing.T, e *testRelClaimEnv, k int) ([]string, map[string]uint64) {
	t.Helper()
	seqs := make(map[string]uint64, k)
	subjects := make([]string, 0, k)
	for pi := 1; pi <= k; pi++ {
		subject, seq := e.hold(t, uint16(pi), nuid.Next(), "payload")
		seqs[subject] = seq
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	return subjects, seqs
}

// TestQoS2DiscardArbitrationUsesTheBoundedWalk pins that arbitrateQoS2ReleaseClaims runs its
// holds through the bounded walk rather than one at a time, which is what keeps a discard of a
// full Receive Maximum of held exchanges off the next same-ID CONNECT's critical path, and that
// the walk's end state and error are those of the serial walk.
func TestQoS2DiscardArbitrationUsesTheBoundedWalk(t *testing.T) {
	t.Parallel()

	t.Run("holds are arbitrated concurrently", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "walk-concurrent")
		holds, seqs := testWalkHolds(t, e, mqttQoS2DiscardWalkWidth+2)
		if mqttQoS2DiscardWalkWidth < 2 {
			t.Fatalf("mqttQoS2DiscardWalkWidth = %d: the walk cannot be concurrent", mqttQoS2DiscardWalkWidth)
		}

		var (
			mu      sync.Mutex
			parked  int
			both    = make(chan struct{})
			release = make(chan struct{})
			relOnce sync.Once
			done    = make(chan struct{})
			walkErr error
		)
		e.asm.mu.Lock()
		e.asm.testQoS2ClaimStore = func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
			mu.Lock()
			park := parked < 2
			if park {
				parked++
				if parked == 2 {
					close(both)
				}
			}
			mu.Unlock()
			if park {
				<-release
			}
			return e.asm.jsa.storeQoS2RelClaimCAS(subject, expectSeq, c)
		}
		e.asm.mu.Unlock()

		go func() {
			defer close(done)
			walkErr = e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen)
		}()
		// Registered after the env, so it runs before the env's own shutdown.
		t.Cleanup(func() {
			relOnce.Do(func() { close(release) })
			<-done
		})

		select {
		case <-both:
		case <-time.After(testWalkWatchdog):
			t.Fatal("a second hold's claim was never attempted while the first was in flight: " +
				"the discard arbitrates its holds one at a time")
		}
		relOnce.Do(func() { close(release) })
		testWalkAwait(t, done, "the discard walk to return")

		if walkErr != nil {
			t.Fatalf("arbitrateQoS2ReleaseClaims: %v", walkErr)
		}
		var top uint64
		for _, seq := range seqs {
			top = max(top, seq)
		}
		for _, h := range holds {
			if e.holdPresent(t, h) {
				t.Fatalf("hold %q survived an uncontested discard", h)
			}
			cur, _ := e.loadClaim(t, h)
			if cur == nil || cur.State != mqttQoS2RelStateDiscarded || cur.Fence < top {
				t.Fatalf("claim on %q = %+v, want a discard marker fenced at or above the walk's watermark %d", h, cur, top)
			}
		}
	})

	t.Run("an error names its hold", func(t *testing.T) {
		t.Parallel()
		e := testNewRelClaimEnv(t, "walk-error")
		holds, _ := testWalkHolds(t, e, 2)
		failing := mqttQoS2ClaimSubjectFor(holds[0])
		errInjected := errors.New("stream unavailable")

		e.asm.mu.Lock()
		e.asm.testQoS2ClaimStore = func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error) {
			if subject == failing {
				return 0, errInjected
			}
			return e.asm.jsa.storeQoS2RelClaimCAS(subject, expectSeq, c)
		}
		e.asm.testQoS2ClaimLoad = func(subject string) (*mqttQoS2RelClaim, *StoredMsg, error) {
			if subject == failing {
				return nil, nil, errInjected
			}
			return e.asm.jsa.loadQoS2RelClaim(subject)
		}
		e.asm.mu.Unlock()

		err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen)
		if !errors.Is(err, errInjected) {
			t.Fatalf("arbitrateQoS2ReleaseClaims error = %v, want one wrapping %v", err, errInjected)
		}
		if !strings.Contains(err.Error(), holds[0]) {
			t.Fatalf("error %q does not name the failing hold %q", err, holds[0])
		}
		if !e.holdPresent(t, holds[0]) {
			t.Fatalf("the hold whose arbitration failed was destroyed")
		}
	})
}

// TestQoS2DiscardWalkUnderReleasingClaims pins that the concurrent walk keeps every per-hold
// decision: a hold under a standing `releasing` claim is never destroyed, a hold whose releaser
// finished while the walk waited is competed for again and destroyed under the walk's own
// marker, and an unreadable claim is reaped and competed for again. It asserts outcomes only,
// not how the polling budget was spread across the concurrent waits.
func TestQoS2DiscardWalkUnderReleasingClaims(t *testing.T) {
	t.Parallel()
	e := testNewRelClaimEnv(t, "walk-releasing")
	holds, _ := testWalkHolds(t, e, mqttQoS2DiscardWalkWidth+2)
	for _, h := range holds {
		e.putClaim(t, h, mqttNewQoS2RelClaim(mqttQoS2RelStateReleasing, nuid.Next(), "peer", 0))
	}
	corrupt, _ := e.hold(t, uint16(len(holds)+1), nuid.Next(), "payload")
	if _, err := e.asm.jsa.storeMsg(mqttQoS2ClaimSubjectFor(corrupt), 0, []byte("{oops")); err != nil {
		t.Fatalf("store malformed claim: %v", err)
	}

	// The releasers of the first two holds complete while the walk waits on them: their first
	// load still returns the `releasing` claim, which is then gone, so the poll finds the subject
	// empty. One-shot per subject; every other load is the real one.
	completing := map[string]bool{
		mqttQoS2ClaimSubjectFor(holds[0]): true,
		mqttQoS2ClaimSubjectFor(holds[1]): true,
	}
	var (
		mu          sync.Mutex
		intercepted = make(map[string]bool)
	)
	e.asm.mu.Lock()
	e.asm.testQoS2ClaimLoad = func(subject string) (*mqttQoS2RelClaim, *StoredMsg, error) {
		mu.Lock()
		first := completing[subject] && !intercepted[subject]
		if first {
			intercepted[subject] = true
		}
		mu.Unlock()
		cur, smsg, err := e.asm.jsa.loadQoS2RelClaim(subject)
		if !first || err != nil || smsg == nil {
			return cur, smsg, err
		}
		if st, rerr := e.asm.releaseQoS2Record(smsg.Sequence); st != mqttQoS2Released {
			t.Errorf("completing the releaser on %q: st=%v err=%v", subject, st, rerr)
		}
		return cur, smsg, nil
	}
	e.asm.mu.Unlock()

	if err := e.asm.arbitrateQoS2ReleaseClaims(e.s, e.sess.idHash, e.sess.gen); err != nil {
		t.Fatalf("arbitrateQoS2ReleaseClaims: %v", err)
	}
	mu.Lock()
	if len(intercepted) != len(completing) {
		mu.Unlock()
		t.Fatalf("only %d of %d releasers completed: the fixture staged no recompetition", len(intercepted), len(completing))
	}
	mu.Unlock()

	for _, h := range append([]string{holds[0], holds[1]}, corrupt) {
		if e.holdPresent(t, h) {
			t.Fatalf("hold %q survived although the walk could compete for it again", h)
		}
		if cur, _ := e.loadClaim(t, h); cur == nil || cur.State != mqttQoS2RelStateDiscarded {
			t.Fatalf("claim on %q = %+v, want the marker the walk won", h, cur)
		}
	}
	for _, h := range holds[2:] {
		if !e.holdPresent(t, h) {
			t.Fatalf("hold %q was destroyed under a standing releasing claim", h)
		}
		if cur, _ := e.loadClaim(t, h); cur == nil || cur.State != mqttQoS2RelStateReleasing {
			t.Fatalf("claim on %q = %+v, want the releaser's claim left standing", h, cur)
		}
	}
}
