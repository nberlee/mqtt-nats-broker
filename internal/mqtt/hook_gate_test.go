package mqtt

import (
	"sync"
	"testing"
	"time"
)

// A process-global test hook must not outlive the test that installed it.
//
// The setters return a restore, but restoring only stores the previous pointer:
// it does not stop a call already past its atomic Load, and it does not stop the
// server goroutine that is about to make one. A session expiry, a disconnect-path
// save or a watch-driven reconcile can enter a hook while the test that installed
// it is finishing. If the closure touches t at that point, the harness panics the
// whole binary — "Fail in goroutine after <test> has completed" — which aborts
// every test that had not run yet, not only the one that installed the hook.
//
// That is not hypothetical: it is what stops `make full-suite` today, from
// testTraceReDrive's rival-record closure reporting through t.Errorf after its
// subtest returned.
//
// testHookGate closes that window. enter holds the gate for the closure's whole
// body, so a cleanup blocks on a call in flight rather than racing it, and every
// call after the test is a no-op. Registering the gate BEFORE the restore matters:
// cleanups run last-registered-first, so the restore stops new calls and the gate
// then waits out the ones already inside.
type testHookGate struct {
	mu   sync.Mutex
	live bool

	// onClose, when set, runs inside close. It exists so a test can observe WHEN
	// the gate closed relative to the hook's restore, which is an ordering the
	// comment above asserts and nothing else would check.
	onClose func()
}

// newTestHookGate returns a gate that closes when t finishes. Install it before
// the hook whose closure it guards.
func newTestHookGate(t *testing.T) *testHookGate {
	t.Helper()
	g := &testHookGate{live: true}
	t.Cleanup(g.close)
	return g
}

// close shuts the gate, blocking until any closure currently inside it has left.
// Factored out of the cleanup so the waiting half is reachable from a test: it is
// the half that a plain atomic flag would silently drop.
func (g *testHookGate) close() {
	g.mu.Lock()
	g.live = false
	if g.onClose != nil {
		g.onClose()
	}
	g.mu.Unlock()
}

// enter reports whether the installing test is still running, and holds the gate
// when it returns true. A true return must be paired with leave.
func (g *testHookGate) enter() bool {
	g.mu.Lock()
	if !g.live {
		g.mu.Unlock()
		return false
	}
	return true
}

func (g *testHookGate) leave() { g.mu.Unlock() }

// TestHookGateSilencesAHookAfterItsTestCompleted is the regression test for the
// panic above. It installs a gated hook inside a subtest whose closure would fail
// that subtest, lets the subtest finish, and then fires the hook from the parent.
//
// Without the gate the closure runs against a completed *testing.T and the binary
// panics, so this test does not merely assert a boolean — removing the enter/leave
// pair takes the whole run down with it.
func TestHookGateSilencesAHookAfterItsTestCompleted(t *testing.T) {
	t.Parallel()

	var fired int
	var restore func()

	t.Run("installs a gated hook and returns", func(t *testing.T) {
		gate := newTestHookGate(t)
		restore = setMQTTTestClearWindowHook(func(sess *mqttSession, window string) {
			if !gate.enter() {
				return
			}
			defer gate.leave()
			fired++
			t.Errorf("a hook must not report against a test that has completed")
		})
		// Deliberately NOT restored here: the point is a hook that is still
		// installed when its test returns, which is the state every one of these
		// helpers is in for the window between the test body ending and its
		// cleanup running.
	})
	t.Cleanup(restore)

	// The subtest has completed. A server goroutine reaching the window now must
	// find the hook inert.
	(&mqttSession{}).clearWindowHook(mqttClearWindowPreFence)

	if fired != 0 {
		t.Fatalf("the hook body ran %d time(s) after its test completed", fired)
	}
}

// TestHookGateCloseWaitsForAnInvocationAlreadyInside covers the half the test
// above cannot reach: a closure that is already PAST enter when the test ends.
//
// That is the race the gate exists for. A restore stops nothing that is already
// running, so if close merely flipped a flag — an atomic.Bool would read the same
// at every call site — the closure would carry on and report against a completed
// test exactly as before. What makes the gate work is that close BLOCKS until the
// body leaves, and only a test that holds a body inside it can tell the two
// implementations apart.
func TestHookGateCloseWaitsForAnInvocationAlreadyInside(t *testing.T) {
	t.Parallel()

	g := &testHookGate{live: true}

	inside, finish := make(chan struct{}), make(chan struct{})
	go func() {
		if !g.enter() {
			t.Error("the gate refused a call while it was still open")
			close(inside)
			return
		}
		close(inside)
		<-finish
		g.leave()
	}()
	<-inside

	// Deterministic, and deliberately not a timed observation of the closer.
	//
	// "close has not returned yet" cannot be asserted by waiting: on a loaded
	// machine the closing goroutine may not have been scheduled at all inside any
	// window we pick, so a close mutated to return immediately would look
	// indistinguishable from one that is properly blocked, and the test would pass
	// against broken code.
	//
	// What makes close block is not a flag but the gate being HELD for the body's
	// whole run, and that is a state this goroutine can test directly. A gate
	// reduced to an atomic flag releases the lock inside enter, so TryLock succeeds
	// and this fails at once, with no scheduler assumption anywhere.
	if g.mu.TryLock() {
		g.mu.Unlock()
		t.Fatal("the gate is not held while a closure is inside it, so close cannot " +
			"wait for one: a hook body can outlive its test and report against a " +
			"completed *testing.T")
	}

	// With the gate provably held, close cannot return until leave releases it, so
	// the completion order below is fixed rather than raced.
	closed := make(chan struct{})
	go func() { g.close(); close(closed) }()
	close(finish)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("close never returned after the closure left the gate")
	}

	if g.enter() {
		g.leave()
		t.Fatal("the gate admitted a call after it closed")
	}
}

// TestHookGateClosesAfterTheHookIsRestored pins the registration order the gate
// depends on. Cleanups run last-registered-first, so a gate created BEFORE the
// restore is registered closes AFTER it runs: new calls stop first, then the gate
// waits out whatever was already inside. Registering them the other way round
// would leave a window with the hook still installed and the gate already shut.
func TestHookGateClosesAfterTheHookIsRestored(t *testing.T) {
	t.Parallel()

	var order []string
	t.Run("child", func(t *testing.T) {
		g := newTestHookGate(t)
		g.onClose = func() { order = append(order, "gate closed") }
		t.Cleanup(func() { order = append(order, "hook restored") })
	})

	if len(order) != 2 || order[0] != "hook restored" || order[1] != "gate closed" {
		t.Fatalf("the gate must close after the hook is restored, got %v", order)
	}
}
