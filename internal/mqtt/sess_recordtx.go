// The session-record transaction fence.
//
// A session record is a single-slot register on $MQTT_sess, written with a
// per-subject CAS on the sequence the writer last observed. Two problems made
// that insufficient on its own:
//
//  1. sess.save() advances sess.seq only on ITS OWN success, so two local writers
//     never converge: one wins the CAS, the other keeps a stale sequence and fails
//     forever. Serializing the stores would not fix it either, because state is
//     mutated BEFORE save() runs — so an off-readLoop writer could persist another
//     transaction's staged (and later rolled back) state. What is needed is a
//     transaction spanning mutate → save → commit/rollback, not a mutex around the
//     store.
//
//  2. clear() zeroes sess.seq and then deletes the record, so a late writer stores
//     with no CAS header at all and RECREATES a terminated session's record.
//
// So every record write — save or delete — happens inside an open transaction,
// identified by an explicit token. Ownership is never inferred: Go has no
// goroutine identity, so a caller that merely observes an open fence must not
// assume the transaction is its own. Tokens are passed down explicitly, and a
// token is valid only for the record GENERATION it was opened against: clear()
// ends a generation by bumping the epoch as it releases the fence, so no writer
// from the old generation can land — including on the session objects that clear()
// hands back to be reused as a fresh session, which write normally at the new
// epoch.
//
// There is deliberately no "a zero token opens one for you" convenience. A
// transaction that starts after the mutation cannot make that mutation invisible
// to another writer, which is the entire point.

package mqtt

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// recordTx is the capability to write this session's record. It is worthless on
// its own: every write revalidates it against the session's live fence state.
//
// owner distinguishes the token that OPENED the transaction from a copy passed
// down to a nested callee. Only the opener may close it; otherwise a stale copy
// held past its transaction could close — and signal a waiter into — somebody
// else's newer transaction.
type recordTx struct {
	epoch uint64
	id    uint64
	owner bool
}

// recordTxCondLocked returns the fence's condition variable, creating it on first
// use so a session built as a plain struct literal (as tests do) still works.
//
// sess.mu must be held.
func (sess *mqttSession) recordTxCondLocked() *sync.Cond {
	if sess.recordTxCond == nil {
		sess.recordTxCond = sync.NewCond(&sess.mu)
	}
	return sess.recordTxCond
}

// beginRecordTx opens a record transaction, WAITING while another writer holds the
// fence. Only outermost readLoop entry points may use it: a session has one bound
// readLoop, and the non-readLoop clear() callers (session expiry) are already
// serialized against a reconnect by lockSession, so at most one blocking caller
// can exist at a time.
//
// Lock not held on entry.
func (sess *mqttSession) beginRecordTx() recordTx {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for sess.recordTxOpen {
		sess.recordTxCondLocked().Wait()
	}
	return sess.openRecordTxLocked()
}

// tryBeginRecordTx opens a record transaction without ever waiting, reporting
// false when another writer holds the fence. This is the form for off-readLoop
// writers (drain finalizers): failing to get the fence must leave their work
// pending and retryable, never blocked, and it is what makes such a writer unable
// to observe or persist a half-applied readLoop transaction.
//
// Lock not held on entry.
func (sess *mqttSession) tryBeginRecordTx() (recordTx, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.recordTxOpen {
		return recordTx{}, false
	}
	return sess.openRecordTxLocked(), true
}

// openRecordTxLocked marks the fence held and mints the transaction's identity.
//
// sess.mu must be held and sess.recordTxOpen must be false.
func (sess *mqttSession) openRecordTxLocked() recordTx {
	sess.recordTxOpen = true
	sess.recordTxSeqno++
	sess.recordTxID = sess.recordTxSeqno
	return recordTx{epoch: sess.recordEpoch, id: sess.recordTxID, owner: true}
}

// endRecordTx releases the fence held by tok.
//
// A mismatch — a stale epoch, a different transaction, or a non-owner copy —
// leaves the fence untouched and reports an error rather than releasing (and
// signalling a waiter into) a transaction that is not the caller's. Callers treat
// that as a programming error; tests assert on it.
//
// Lock not held on entry.
func (sess *mqttSession) endRecordTx(tok recordTx) error {
	return sess.endRecordTxLocked(tok, false)
}

// endRecordTxNewGeneration releases the fence AND ends the record generation, so
// every token minted before it — including ones held by writers still in flight —
// is dead. This is how clear() terminates a session: the epoch bump cannot sit
// beside clear()'s early state reset, because clear()'s own record delete must
// still validate against the epoch it opened with.
//
// Lock not held on entry.
func (sess *mqttSession) endRecordTxNewGeneration(tok recordTx) error {
	return sess.endRecordTxLocked(tok, true)
}

func (sess *mqttSession) endRecordTxLocked(tok recordTx, newGeneration bool) error {
	sess.mu.Lock()
	if err := sess.validateRecordTxLocked(tok); err != nil {
		sess.mu.Unlock()
		return err
	}
	if !tok.owner {
		sess.mu.Unlock()
		return fmt.Errorf("session %q: a nested record transaction token may not close transaction %v", sess.id, tok.id)
	}
	// Test seam, fired with sess.mu held and the fence still OPEN: it observes
	// exactly what the next holder of the fence will see. That is the only way to
	// state "a definite failure is undone before the transaction is released" as an
	// assertion rather than as a race against the scheduler.
	if h := mqttTestRecordTxReleaseHookV.Load(); h != nil {
		(*h)(sess)
	}
	sess.recordTxOpen = false
	sess.recordTxID = 0
	if newGeneration {
		sess.recordEpoch++
	}
	// Whether a void-register verdict was staged while this transaction held the fence is
	// decided INSIDE this critical section, not after the unlock. A stager takes sess.mu to
	// record its nonce and then finds the fence held; the cleared fence becomes visible to
	// it only when we unlock, so a decision sampled after the unlock can miss a nonce
	// staged in exactly that window — and that stager's own persistence attempt was
	// refused, so nothing else is left owing it. Never for a generation that has just
	// ENDED: clear() discards the session's whole state, verdicts included.
	handoff := !newGeneration && sess.voidVerdictHandoffDueLocked()
	sess.recordTxCondLocked().Broadcast()
	sess.mu.Unlock()
	if handoff {
		// After the unlock, and after the broadcast: the attempt takes the ownership gate
		// and re-acquires this fence, and it runs on this goroutine, so holding either
		// here would deadlock. No client on this path — the fence is released by paths
		// with and without one — and every log site the attempt reaches is nil-safe.
		sess.attemptVoidVerdictHandoff(nil)
	}
	return nil
}

// validateRecordTxLocked reports whether tok authorizes a write right now: the
// fence must be open, the token's epoch must be the current generation, and it
// must belong to the transaction that is actually open.
//
// The two identity checks are not redundant, and neither subsumes the other.
// Transaction identity is what refuses a token whose transaction has CLOSED — an
// epoch check alone would admit it, since a later transaction in the same
// generation shares the epoch. The epoch is what refuses a token whose generation
// has ENDED while its transaction is still open, which transaction identity alone
// would admit; that is the state a discard racing a still-running writer produces,
// and it is precisely the case that used to let a late writer recreate a
// terminated session's record.
//
// sess.mu must be held.
func (sess *mqttSession) validateRecordTxLocked(tok recordTx) error {
	switch {
	case !sess.recordTxOpen:
		return fmt.Errorf("session %q: no record transaction is open", sess.id)
	case tok.epoch != sess.recordEpoch:
		return fmt.Errorf("session %q: record transaction token is from generation %v, the current generation is %v",
			sess.id, tok.epoch, sess.recordEpoch)
	case tok.id != sess.recordTxID:
		return fmt.Errorf("session %q: record transaction token %v is not the open transaction %v",
			sess.id, tok.id, sess.recordTxID)
	}
	return nil
}

// nested returns tok as a token to hand to a callee. The callee may write under
// it but may not close the transaction.
func (tok recordTx) nested() recordTx {
	tok.owner = false
	return tok
}

// mqttTestRecordTxReleaseHookV fires while a record transaction is being released,
// under sess.mu and before the fence is marked closed. See endRecordTxLocked.
var mqttTestRecordTxReleaseHookV atomic.Pointer[func(sess *mqttSession)]

func setMQTTTestRecordTxReleaseHook(h func(sess *mqttSession)) func() {
	prev := mqttTestRecordTxReleaseHookV.Load()
	if h == nil {
		mqttTestRecordTxReleaseHookV.Store(nil)
	} else {
		mqttTestRecordTxReleaseHookV.Store(&h)
	}
	return func() { mqttTestRecordTxReleaseHookV.Store(prev) }
}

// validateSessStream verifies a $MQTT_sess stream can carry session records.
// Derived from what this owner does: a per-subject CAS store (sess.save),
// loadLastMsgFor reads, filtered forward scans, and exact-sequence CONFIRMED
// deletes (the teardown release, session expiry, the CONNECT rollback). It
// never purges and never creates a consumer, so DenyPurge, MaxConsumers and
// ConsumerLimits are audited NON-prerequisites here — a validator that listed
// them would refuse legitimate deployments for settings no operation depends
// on.
//
// DenyDelete IS fatal: the teardown release's delete is the only exit a
// record has, and since nobody resumes a marked record, a client whose
// teardown cannot delete is locked out permanently — every subsequent CONNECT
// re-runs the teardown and fails again.
//
// The incarnation epoch is validated here, on the same authoritative snapshot
// the shape is (docs/ARCHITECTURE.md, "Session incarnations and teardown"): a
// missing, empty, or malformed mqtt_sess_epoch metadata value (e.g. a
// pre-first-release development stream) is a hard error, because an exact
// design must never serve CONNECTs with an unknown local epoch and this build
// does not mutate an existing stream to install one. Unrelated metadata keys
// are accepted untouched.
func validateSessStream(si *StreamInfo, domainTk string, wantReplicas int) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, mqttSessStreamSubjectPrefix+domainTk+">", wantReplicas); err != nil {
		return err
	}
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits): nothing consumes a session record, so any other retention drops it immediately", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		return fmt.Errorf("discard is %v (want old): DiscardNew would reject the CAS replacement every session save performs", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		return fmt.Errorf("max_msgs_per_subject is %d (want 1): the record CAS and exact-sequence deletes depend on the single-slot register", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict live session records",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a session's CAS slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): a record grows with subscriptions, drains and verdicts, and a cap would reject a save the protocol cannot skip", cfg.MaxMsgSize)
	}
	if cfg.DenyDelete {
		return fmt.Errorf("deny_delete is set: it blocks the teardown release, permanently locking the client ID out")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): a marker after the teardown delete would be read back as an unreadable record", cfg.SubjectDeleteMarkerTTL)
	}
	if epoch := cfg.Metadata[mqttSessEpochMetaKey]; !mqttValidSessEpoch(epoch) {
		return fmt.Errorf("no valid %q metadata (found %q); recreate the session stream/store so a fresh incarnation epoch is generated",
			mqttSessEpochMetaKey, epoch)
	}
	return nil
}
