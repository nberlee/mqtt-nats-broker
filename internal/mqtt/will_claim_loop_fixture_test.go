// Keeping a broker's background Will claim loop out of a test's records.
//
// Every account manager runs willClaimLoop, which scans the whole account's outbox once
// at start and then on every mqttWillClaimInterval tick. A pass acts on any record it
// finds actionable - a due pending record, a publishing record whose lease expired, a
// terminal record to reap, a pending record an acceptance supersedes - through the
// manager's OWN seams, which a test hooking another manager does not cover. So a test
// that seeds such a record has to know that no pass is running: the initial one has
// returned (testAwaitInitialWillScan) and no tick can fall due (withLongWillScan).

package mqtt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"
)

// willInitialScansReturned records every account manager whose claim loop's initial
// pass has returned, keyed weakly so the registry never keeps a manager alive and a new
// manager at a reused address is never mistaken for an old one. willInitialScanCount
// counts them.
var (
	willInitialScansReturned sync.Map // weak.Pointer[mqttAccountSessionManager] -> struct{}
	willInitialScanCount     atomic.Int64
)

// The hook is stored ONCE, here, and never again: parallel tests start managers
// concurrently, and a hook any test re-stored or cleared would lose another test's
// signal.
func init() {
	record := func(as *mqttAccountSessionManager) {
		willInitialScansReturned.Store(weak.Make(as), struct{}{})
		willInitialScanCount.Add(1)
	}
	mqttTestWillInitialScanDoneV.Store(&record)
}

// testAwaitInitialWillScan waits until asm's claim loop has RETURNED from its initial
// pass - completed, aborted on an error, or stopped by shutdown. That is what a fixture
// needs: afterwards no pass of that loop is in flight until its first tick.
func testAwaitInitialWillScan(t testing.TB, asm *mqttAccountSessionManager) {
	t.Helper()
	key := weak.Make(asm)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := willInitialScansReturned.Load(key); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture: the claim loop's initial Will outbox pass did not return within 10s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
