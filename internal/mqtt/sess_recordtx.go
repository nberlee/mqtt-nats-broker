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
//
// # Waiter priority
//
// The fence is barge-proof against the non-blocking form. A blocking acquisition
// ANNOUNCES itself (recordTxWaiters) before contending on sess.mu, and every
// non-blocking acquisition refuses while the count is nonzero. Both linearization
// points are exact: the announce is an atomic increment ordered before the mutex
// contention, and an acquirer linearizes at its successful free-fence predicate load
// under sess.mu.
//
// What that delivers: try-callers cannot add a fence hold ahead of an announced
// waiter unless already admitted at the announce — at most one such pre-admitted
// transaction exists, blocking, try, or the inline handoff, because sess.mu
// serializes the predicate loads. What it does NOT deliver: any FIFO among BLOCKING
// waiters (a newly arriving beginRecordTx can win the post-broadcast mutex race
// against an older Cond waiter, so identity churn can starve one waiter at a
// constant count), and no priority at all over ordinary sess.mu traffic, which is
// invisible to the count. A try-caller delayed past the waiter's completion succeeds
// normally, which lengthens nothing. See docs/CONFORMANCE.md residual 13 for the
// blocking-vs-blocking residual this leaves stated rather than fixed.

package mqtt

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// recordTx is the capability to write this session's record. It is worthless on
// its own: every write revalidates it against the session's live fence state.
//
// The epoch and transaction id are what refuse a STALE token: a copy held past its
// transaction cannot write into, close, or signal a waiter into somebody else's
// newer transaction, because the id no longer names the open one. Copies of the
// CURRENT token are interchangeable - production passes the opener's token down to
// its callees, and any of them may close the transaction it names (F-054).
type recordTx struct {
	epoch uint64
	id    uint64
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
// fence. It is the form for a caller that cannot proceed without the fence and is not
// running on a goroutine that might already hold one: outermost readLoop entry points,
// the CONNECT accept transaction, session expiry, teardown, the bound rejection
// settlement, the will-cancel write, the drain's recovery paths.
//
// Blocking callers are ordinarily SPARSE on one session — the readLoop entry points
// share the session's single readLoop, and the expiry-side clear() is serialized against
// a reconnect by lockSession — but MULTIPLE ones are permitted, and nothing here orders
// them. There is no FIFO among blocking waiters: a newly arriving beginRecordTx can win
// the post-broadcast mutex race against an older Cond waiter, so under a sustained stream
// of blocking arrivals one waiter can be preceded indefinitely. That residual is stated,
// not fixed (docs/CONFORMANCE.md residual 13); what this function does guarantee is
// priority over the NON-BLOCKING form, below.
//
// The wait is BARGE-PROOF against non-blocking acquisitions. sync.Cond gives no
// handoff — Broadcast only wakes a waiter, which must then re-acquire sess.mu and
// re-check — so without the announce below, a sustained stream of try-callers could
// pass the fence between themselves indefinitely while this caller starved. That
// matters because a blocking acquisition is usually made while holding the session
// OWNERSHIP GATE, so a starved waiter also fails every same-client-ID CONNECT for
// as long as the starvation lasts.
//
// Lock not held on entry.
func (sess *mqttSession) beginRecordTx() recordTx {
	// Announced BEFORE contending on sess.mu: while this announce remains
	// outstanding, no non-blocking acquisition whose predicate load follows it can
	// take the fence. At most one transaction already past its successful free-fence
	// predicate — blocking, try, or the inline handoff; sess.mu serializes them all —
	// can still be admitted after the announce, and OTHER BLOCKING callers (already
	// queued or arriving later) may acquire first, in no particular order. What the
	// announce guarantees is narrower and exact: try-callers cannot add a fence hold
	// ahead of this waiter unless already admitted at the announce. A try load
	// following the announce refuses without locking sess.mu; a caller past only the
	// fast check contends once and refuses if its authoritative reload occurs while
	// the announce remains outstanding; at most one caller already past the
	// authoritative predicate may still open and hold the fence; and a delayed caller
	// may succeed after this waiter has completed.
	sess.recordTxWaiters.Add(1)
	// The withdrawal guard: an announce must be withdrawn on EVERY exit from this
	// function, and once sess.mu is held it must be withdrawn INSIDE the critical section
	// that holds it. A panic or runtime.Goexit escaping the wait-loop test seam (t.Fatal in
	// a hook is a Goexit, which runs defers but skips straight-line code) would otherwise
	// leave the count positive forever, refusing every later non-blocking acquisition.
	//
	// The ordering against the unlock is load-bearing, not tidiness: an abnormal exit that
	// unlocked first would publish a FREE fence with a dead waiter still announced, and a
	// release acquiring sess.mu in that window would suppress the void-verdict handoff for
	// a waiter that no longer exists — with the withdrawal behind it having no release of
	// its own to re-trigger the attempt, so the staged verdict would stay memory-only until
	// some later event or the periodic backstop. Every observer of the count that matters
	// reads it under sess.mu, so withdrawing before the unlock makes the announce and the
	// waiter's existence agree at every point such an observer can look.
	announced := true
	withdraw := func() {
		if announced {
			announced = false
			sess.recordTxWaiters.Add(-1)
		}
	}
	// withdrawAtExit is withdraw for an exit that never reached the explicit withdrawal
	// below — the abnormal path, and the only one that still finds the announce
	// outstanding. Its test seam fires there and nowhere else, and it exists to make the
	// ordering above assertable rather than argued: a handler that finds sess.mu FREE has
	// caught the withdrawal happening outside the critical section, which is the state a
	// release would misread. Nothing on the normal path reaches it.
	withdrawAtExit := func() {
		if !announced {
			return
		}
		// Deferred, not called after the seam: a cleanup path must be exception-safe, or
		// the seam becomes another way to reach exactly the abnormal exit this guard
		// exists to cover — with the announce left outstanding for the life of the
		// session.
		defer withdraw()
		if h := mqttTestRecordTxWithdrawGuardHookV.Load(); h != nil {
			(*h)(sess)
		}
	}
	// Covers an exit before sess.mu is held at all. It runs last, after the locked cleanup
	// below has already withdrawn on every path that got the lock.
	defer withdrawAtExit()
	sess.mu.Lock()
	defer func() {
		// The unlock is deferred for the same reason, and it is what keeps the withdrawal
		// inside the critical section: the withdrawal runs while this frame does, the
		// unlock only as it leaves — including when the guard's seam dies mid-cleanup.
		defer sess.mu.Unlock()
		withdrawAtExit()
	}()
	for sess.recordTxOpen {
		// Test seam, fired under sess.mu each iteration before Wait — the causal point
		// at which a waiter is provably announced and parked, which is what lets a test
		// read the count without racing the scheduler. Same pattern as
		// mqttTestRecordTxReleaseHookV. A handler runs with sess.mu HELD, so one that
		// blocks pins every other user of the session lock — deliberately usable as such,
		// and it must therefore never wait on anything that itself needs sess.mu. The
		// withdrawal guard above keeps the count balanced if one panics or Goexits.
		if h := mqttTestRecordTxWaitHookV.Load(); h != nil {
			(*h)(sess)
		}
		sess.recordTxCondLocked().Wait()
	}
	// Withdrawn under sess.mu immediately before opening, so the count never reads
	// zero while this waiter is still due the fence.
	withdraw()
	return sess.openRecordTxLocked()
}

// tryBeginRecordTx opens a record transaction without ever waiting, reporting
// false when another writer holds the fence OR a blocking writer has announced
// itself for it. This is the form for off-readLoop writers (drain finalizers):
// failing to get the fence must leave their work pending and retryable, never
// blocked, and it is what makes such a writer unable to observe or persist a
// half-applied readLoop transaction.
//
// Lock not held on entry.
func (sess *mqttSession) tryBeginRecordTx() (recordTx, bool) {
	// Double-checked refusal. The pre-lock fast path is a LIVENESS guarantee, not an
	// optimization: a try-caller refused HERE never touches sess.mu, so while an
	// announce is outstanding NEW try arrivals stop contending for the mutex. One
	// already past this check contends once and refuses at the reload below if the
	// announce is still outstanding there — it may instead be the one pre-admitted
	// transaction, or succeed later once the waiter has completed. Without the fast
	// path, sync.Mutex's undocumented fairness would be the only thing keeping a storm
	// from starving the announced waiter of the mutex itself.
	//
	// Refusing on a stale positive is safe because it selects the same arm ANY refusal
	// selects — retry later for most consumers, the unordered fallback for the
	// never-expire close stamp in mqttHandleClosedClient, whose residual
	// this transient merely gives one more occasion.
	if sess.recordTxWaiters.Load() > 0 {
		return recordTx{}, false
	}
	// Test seam, PRE-LOCK, after the zero observation. Unlike the wait-loop seam this
	// one runs outside sess.mu, so a handler MAY block — which is what lets a test park
	// a try-caller here causally.
	if h := mqttTestTryBeginPreLockHookV.Load(); h != nil {
		(*h)(sess)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	// The authoritative re-check, under the mutex: acquisition linearizes HERE, and
	// this reload is what closes the race with an announce that landed after the fast
	// path's load.
	if sess.recordTxOpen || sess.recordTxWaiters.Load() > 0 {
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
	return recordTx{epoch: sess.recordEpoch, id: sess.recordTxID}
}

// endRecordTx releases the fence held by tok.
//
// A mismatch — a stale epoch or a different transaction — leaves the fence
// untouched and reports an error rather than releasing (and signalling a waiter
// into) a transaction that is not the caller's. Callers treat that as a
// programming error; tests assert on it.
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

// abandonRecordTx releases the fence held by tok WITHOUT running the void-verdict
// handoff and WITHOUT ending the record generation. It exists for exactly one
// caller shape: an abnormal-exit guard (deferred around a fence acquisition) that
// may run while the goroutine still holds the account manager's lock. The full
// release is illegal there — endRecordTx can synchronously run the void-verdict
// handoff, which takes the ownership gate and therefore as.mu
// (lockSessionForVoidHandoff) — and endRecordTxNewGeneration suppresses the
// handoff only by advancing recordEpoch, which is wrong when no discard occurred.
// This helper takes only sess.mu (as.mu → sess.mu is the established order, so it
// is legal under as.mu).
//
// The handoff it skips is not lost: it is due only for a resident, unbound,
// settled session with an unsaved void-register revision, and the periodic
// backstop (reconcileResidentRecordState → flushVoidVerdictGap) persists that gap
// once the fence reads closed. Normal paths must keep using endRecordTx, whose
// synchronous handoff is a liveness edge the backstop only backstops.
//
// Lock not held on entry.
func (sess *mqttSession) abandonRecordTx(tok recordTx) error {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if err := sess.validateRecordTxLocked(tok); err != nil {
		return err
	}
	sess.recordTxOpen = false
	sess.recordTxID = 0
	sess.recordTxCondLocked().Broadcast()
	return nil
}

func (sess *mqttSession) endRecordTxLocked(tok recordTx, newGeneration bool) error {
	sess.mu.Lock()
	if err := sess.validateRecordTxLocked(tok); err != nil {
		sess.mu.Unlock()
		return err
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
	// Retired drains whose disposal this transaction's holders could not issue, detached
	// HERE — inside the critical section, before the unlock — for the same reason the
	// handoff decision is made here: detaching after the unlock lets the NEXT
	// transaction's queue be taken by this releaser, and lets this queue be stranded by a
	// releaser that sampled it too early. Executed below, on this goroutine, holding
	// neither lock and no longer holding the fence, which is precisely what the disposal
	// needs in order to wait for an answer.
	disposals := sess.pendingDrainDisposals
	sess.pendingDrainDisposals = nil
	sess.recordTxCondLocked().Broadcast()
	sess.mu.Unlock()
	// Test seam, fired with sess.mu released, the fence CLEARED and this release's batch
	// already detached. It is the only point from which a test can let ANOTHER transaction
	// open the fence and queue its own disposal while this releaser is still between its
	// unlock and its disposals — the schedule that distinguishes detaching before the
	// unlock from reading the queue after it, where this releaser would take the next
	// transaction's work.
	if h := mqttTestRecordTxPostReleaseHookV.Load(); h != nil {
		(*h)(sess)
	}
	if handoff {
		// After the unlock, and after the broadcast: the attempt takes the ownership gate
		// and re-acquires this fence, and it runs on this goroutine, so holding either
		// here would deadlock. No client on this path — the fence is released by paths
		// with and without one — and every log site the attempt reaches is nil-safe.
		sess.attemptVoidVerdictHandoff(nil)
	}
	// AFTER the handoff, deliberately. A disposal blocks for up to jsa.timeout, and the
	// handoff previously ran immediately after the broadcast; putting the requests first
	// would delay a persistence attempt that a stager is already owed by up to one timeout
	// per queued drain. Nothing in the disposal depends on the handoff or vice versa — the
	// drain is out of the record and out of both maps before it is ever queued.
	for _, d := range disposals {
		// No client here: the fence is released by paths with and without one. The decision
		// carries the entitlement its omission was taken under, never the one serving now
		// (F-105 7c-lc).
		sess.disposeRetiredDrainConsumer(nil, d.durable, d.ent)
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

// mqttTestRecordTxReleaseHookV fires while a record transaction is being released,
// under sess.mu and before the fence is marked closed. See endRecordTxLocked.
var mqttTestRecordTxReleaseHookV atomic.Pointer[func(sess *mqttSession)]

// mqttTestRecordTxPostReleaseHookV fires AFTER the release: sess.mu is unlocked, the fence
// is clear, the waiters are broadcast, and this release's queued drain disposals are already
// detached. It runs outside every lock, so a handler MAY block — which is the point. It is
// the only seam from which a test can hold one releaser between its unlock and its disposals
// while a SECOND transaction opens the fence and queues its own, which is what tells
// "detached before the unlock" apart from "read after it". See endRecordTxLocked.
var mqttTestRecordTxPostReleaseHookV atomic.Pointer[func(sess *mqttSession)]

// mqttTestRecordTxWaitHookV fires inside beginRecordTx's wait loop, under sess.mu,
// after the announce and before each Wait. It is the causal point at which a
// blocking waiter is provably announced and parked, so a test can read
// recordTxWaiters there rather than polling against the scheduler. Handlers run with
// sess.mu HELD: one that blocks pins every other user of the session lock, which is
// how a test holds a waiter in place, and it must therefore never wait on anything
// that itself needs sess.mu. See beginRecordTx.
var mqttTestRecordTxWaitHookV atomic.Pointer[func(sess *mqttSession)]

// mqttTestTryBeginPreLockHookV fires in tryBeginRecordTx between the pre-lock fast
// refusal and sess.mu.Lock(). It runs OUTSIDE sess.mu, so unlike the wait-loop seam
// a handler MAY block — which is what lets a test park a try-caller between the two
// halves of the double check and drive the interleaving causally rather than by
// timing. See tryBeginRecordTx.
var mqttTestTryBeginPreLockHookV atomic.Pointer[func(sess *mqttSession)]

// mqttTestRecordTxWithdrawGuardHookV fires in beginRecordTx's withdrawal guard, on an
// ABNORMAL exit only (a panic or runtime.Goexit that skipped the explicit withdrawal),
// immediately before the announce is withdrawn. It is what makes the withdrawal's
// ordering against the unlock assertable: sess.mu is HELD at this point on every exit
// that got the lock, so a handler observing a free lock has caught the withdrawal
// escaping the critical section.
//
// A handler here may itself panic or Goexit — the withdrawal and the unlock are both
// deferred around it, so the count and the lock come back either way. See beginRecordTx.
var mqttTestRecordTxWithdrawGuardHookV atomic.Pointer[func(sess *mqttSession)]

// validateSessStream verifies a $MQTT_sess stream can carry session records.
// Derived from what this owner does: a per-subject CAS store (sess.save),
// loadLastMsgFor reads, filtered forward scans, and exact-sequence CONFIRMED
// deletes (the teardown release, session expiry, the CONNECT rollback). It
// never purges, so DenyPurge is an audited NON-prerequisite here — a validator
// that listed it would refuse legitimate deployments for a setting no operation
// depends on.
//
// MaxConsumers IS a prerequisite now, and it was not before: this owner creates
// a consumer. Every broker runs its own ephemeral session-record watch over this
// stream (sess_record_watch.go), which is how a resident session learns that its
// record MOVED — not who moved it, which the watch cannot say (F-084) — so a
// finite cap eventually rejects a legitimate broker
// start — the same reasoning, and the same check, as $MQTT_rmsgs. Note that a
// configured 0 means UNLIMITED once nats-server normalizes the config, so only a
// positive value is hostile.
//
// ConsumerLimits stays UNCHECKED, and the reason is enforcement rather than
// irrelevance: a stream InactiveThreshold below the watch's requested five
// minutes already fails initialization at the consumer create itself, naming the
// consumer (a consumer cannot opt out of a stream limit), and any positive
// MaxAckPending limit is copied into the zero-request consumer and then rejected
// against AckNone at that same create. A validator entry would duplicate an
// enforcement that already exists and already names the offending object.
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
	if err := validateBrokerOwnedStreamShape(&cfg, []string{
		mqttSessStreamSubjectPrefix + domainTk + ">",
		mqttAcceptSubjectPrefix + domainTk + ">",
		mqttHandoffSubjectPrefix + domainTk + ">",
		mqttTeardownReceiptSubjectPrefix + domainTk + ">",
	}, wantReplicas); err != nil {
		return err
	}
	// The permanent CONNECT acceptance facts, and F-105's permanent handoff outcomes,
	// live on this stream so that an accepted CONNECT commits its record and both
	// facts in one atomic batch. Without
	// the capability the server rejects the batch headers outright, which would
	// fail every CONNECT — and, worse, a stream that merely LOOKED right would
	// leave the split-acceptance interval the fact exists to remove. It is a
	// prerequisite of an operation this owner performs, so it belongs here rather
	// than in the shared shape: no other broker-owned stream is written in a batch.
	if !cfg.AllowAtomicPublish {
		return fmt.Errorf("allow_atomic is not set: an accepted CONNECT commits the session record and its permanent acceptance fact in one atomic batch, which the stream would reject")
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
	if cfg.MaxConsumers > 0 {
		return fmt.Errorf("max_consumers is %d (want unlimited): every broker creates its own ephemeral session-record watch consumer, so a cap eventually rejects a legitimate broker start", cfg.MaxConsumers)
	}
	if epoch := cfg.Metadata[mqttSessEpochMetaKey]; !mqttValidSessEpoch(epoch) {
		return fmt.Errorf("no valid %q metadata (found %q); stop ALL brokers for this account, delete ALL the broker-owned streams (%s) and restart, which mints a fresh recovery epoch across the whole bundle — recreating this stream alone is refused at the next startup, because they are one recreation unit",
			mqttSessEpochMetaKey, epoch, mqttBrokerStreamBundleList)
	}
	return nil
}
