package mqtt

import (
	"testing"
	"time"
)

// testGateWaitHook installs a gate-wait test hook for the duration of the test.
func testGateWaitHook(t *testing.T, v interface {
	Load() *func()
	Store(*func())
}, h func()) {
	t.Helper()
	prev := v.Load()
	v.Store(&h)
	t.Cleanup(func() { v.Store(prev) })
}

// testGateWaiter starts a waiter on cid's held gate the way the CONNECT does — observation and
// registration in one as.mu section — and returns its outcome channel.
func testGateWaiter(t *testing.T, as *mqttAccountSessionManager, cid string, deadline time.Time) <-chan mqttGateWaitOutcome {
	t.Helper()
	out := make(chan mqttGateWaitOutcome, 1)
	as.mu.Lock()
	if _, held := as.sessLocked[cid]; !held {
		as.mu.Unlock()
		t.Fatalf("fixture: the gate of %q is not held", cid)
	}
	released := as.sessGateWaiterLocked(cid)
	as.mu.Unlock()
	go func() { out <- mqttWaitSessGate(released, make(chan struct{}), deadline) }()
	return out
}

// Every way a gate is released wakes a CONNECT waiting on it, promptly and long before its
// deadline. Each release runs only once the waiter has registered, so the release is the one
// that must wake it, not one it could have missed.
func TestMQTTGateWaitWakesOnEveryRelease(t *testing.T) {
	releases := []struct {
		name    string
		release func(as *mqttAccountSessionManager, cid string)
	}{
		// unlockSession: every lockSession variant, the close's teardown and the resolvers.
		{"unlockSession", func(as *mqttAccountSessionManager, cid string) {
			as.unlockSession(&mqttSession{id: cid})
		}},
		// A CONNECT's own deferred release goes through the same helper under as.mu.
		{"connect release", func(as *mqttAccountSessionManager, cid string) {
			as.mu.Lock()
			as.releaseSessGateLocked(cid)
			as.mu.Unlock()
		}},
	}
	for _, rel := range releases {
		t.Run(rel.name, func(t *testing.T) {
			const cid = "gate-wake"
			as := &mqttAccountSessionManager{sessLocked: map[string]struct{}{cid: {}}}
			registered := make(chan struct{})
			testGateWaitHook(t, &mqttTestGateWaitRegisteredHookV, func() { close(registered) })
			out := testGateWaiter(t, as, cid, time.Now().Add(time.Minute))
			<-registered
			rel.release(as, cid)
			select {
			case got := <-out:
				if got != mqttGateWaitReleased {
					t.Fatalf("the waiter ended %v, want released", got)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the release did not wake the waiter")
			}
			as.mu.RLock()
			_, left := as.sessGateWaiters[cid]
			as.mu.RUnlock()
			if left {
				t.Fatal("the release left its wakeup channel registered")
			}
		})
	}
}

// A gate that is never released ends the wait at the deadline, and an aborted connection ends
// it at once.
func TestMQTTGateWaitEndsAtDeadlineOrAbort(t *testing.T) {
	const cid = "gate-bounded"
	as := &mqttAccountSessionManager{sessLocked: map[string]struct{}{cid: {}}}
	start := time.Now()
	if got := <-testGateWaiter(t, as, cid, start.Add(100*time.Millisecond)); got != mqttGateWaitExpired {
		t.Fatalf("a never-released gate ended the wait %v, want expired", got)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the expired wait took %v", took)
	}

	as.mu.Lock()
	released := as.sessGateWaiterLocked(cid)
	as.mu.Unlock()
	abort := make(chan struct{})
	close(abort)
	if got := mqttWaitSessGate(released, abort, time.Now().Add(time.Minute)); got != mqttGateWaitAborted {
		t.Fatalf("an aborted connection ended the wait %v, want aborted", got)
	}
}
