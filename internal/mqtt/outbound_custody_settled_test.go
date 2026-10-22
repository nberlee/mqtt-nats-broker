package mqtt

import (
	"testing"
	"time"
)

// testCustodySettled reports whether a session's custody has nothing running and nothing owed:
// no dispatcher run, and none of the work rearmCustodyAfterReopen would restart one for. A
// dispatcher's idle point is not this — work queued while it decides to exit continues the run —
// and neither is its exit, which can leave retries or demand armed for later.
func testCustodySettled(sess *mqttSession) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	owed := len(sess.custodyHints) > 0 || sess.custodyPassWanted || len(sess.custodyPassQueue) > 0 ||
		sess.custodyDemand || len(sess.custodyRetry) > 0 || len(sess.withdrawOwed) > 0 ||
		len(sess.admissionContinuity) > 0 || (sess.custody != nil && sess.custody.censusWanted) ||
		sess.floorWanted
	return !sess.custodyObserving && !owed
}

// testWaitCustodySettled waits, bounded, for testCustodySettled.
func testWaitCustodySettled(t *testing.T, sess *mqttSession, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !testCustodySettled(sess) {
		if time.Now().After(deadline) {
			t.Fatalf("the session's custody did not settle within %v", within)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
