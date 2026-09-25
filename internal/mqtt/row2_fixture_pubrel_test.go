package mqtt

import "testing"

// Real-store PUBREL fixtures for the F-092 acquisition tests (F-105 row 2, consult 29): an
// ordinary exchange acquires its PUBREL only through the authority, so these tests seed it through
// admission, the real PUBREC and the canonical rebind (testSeedRealPubRelReady) rather than into a
// map, and observe the acquisition at the authority commit rather than at the register store.

// testRealPubRelSlot reads the slot for pi from the real store (nil when absent).
func testRealPubRelSlot(t *testing.T, e *testInflightEnv, pi uint16) *mqttInflightRecord {
	t.Helper()
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()
	rec, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil {
		t.Fatalf("reading the slot for packet identifier %d: %v", pi, err)
	}
	if smsg == nil {
		return nil
	}
	return rec
}

// testRealBaseLoad is what a load hook delegates to: any hook already installed, else the store.
func testRealBaseLoad(e *testInflightEnv) func(string) (*mqttInflightRecord, *StoredMsg, error) {
	e.asm.mu.RLock()
	hook := e.asm.testInflightLoad
	e.asm.mu.RUnlock()
	if hook != nil {
		return hook
	}
	return e.asm.jsa.loadInflightRecord
}

// testObserveAcquisitions calls landed after every acquire-action batch that the store accepted.
// It is the authority-route counterpart of a register-store hook watching a token being installed.
func testObserveAcquisitions(t *testing.T, e *testInflightEnv, landed func(*mqttAuthorityTransition)) {
	t.Helper()
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if err == nil && tr.Attempt != nil && tr.Attempt.Kind == mqttTransitionAcquireAction {
			landed(tr)
		}
		return seq, err
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
}
