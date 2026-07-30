package mqtt

import (
	"sync"
	"testing"
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
	_, nc := runNATS(t)
	s := &Server{nrc: nc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: "node1"}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	defer close(s.quitCh)
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

	// A nested token may write but not close.
	nested := second.nested()
	if err := testValidateRecordTx(sess, nested); err != nil {
		t.Fatalf("a nested token must authorize writes: %v", err)
	}
	if err := sess.endRecordTx(nested); err == nil {
		t.Fatal("a nested token must not close its caller's transaction")
	}
	if err := sess.endRecordTx(second); err != nil {
		t.Fatalf("the owner must be able to close: %v", err)
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

// TestRecordTxSerializesWriters runs concurrent writers through the fence under
// -race and asserts that transactions never interleave: at most one is open at a
// time, and every one of them is the transaction its own token names.
func TestRecordTxSerializesWriters(t *testing.T) {
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
