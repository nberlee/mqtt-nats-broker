// Settling a DEFINITELY REJECTED session-record CAS.
//
// A session-record save is a per-subject CAS. When JetStream rejects it (10071 from the
// single-writer check, 10164 from the clustered contention branch) the write certainly
// did not land, so sess.seq is still right and the session stays usable — which is why
// saveWithDetail deliberately does not mark the session unresolved. That is correct
// about the write and silent about the other fact the rejection carries: the SUBJECT
// MOVED, so this broker's belief that it owns the session may be obsolete.
//
// Two things evict a stale local session object otherwise, and neither covers this:
// the persist observation in processSessionPersist, which is an at-most-once core-NATS
// notification with no retry and nothing durable behind it; and an uncertainty
// readback's foreign verdict, which a definite rejection never reaches because it
// creates no uncertainty. A never-expiring session — every MQTT 3.1.1 persistent
// session — has no expiry timer either, and sweepSessionExpiries skips it outright. So
// with the notification lost the object stays resident for good, holding the client
// ID's reaping duty it can no longer discharge.
//
// This file settles an UNBOUND session, which is the shape the roadmap bullet described. A
// BOUND one keeps serving a client against a session another broker owns ([MQTT-3.1.4-3]),
// and converging THAT is deferred to its own roadmap item: the ownership gate a settlement
// needs is the one a concurrent SUBSCRIBE claims, so taking it for a live connection turns
// that packet into an error and closes the connection. A bound session is settled here when
// it unbinds.
//
// WHAT SETTLES IT IS A WRITE, NOT A READ. JetStream returns 10164 from
// checkMsgHeadersPreClusteredProposal while a write for the subject is IN PROCESS
// (nats-server jetstream_batching.go), so a load that finds the record still at our
// base sequence proves nothing: the contender's proposal can commit a moment later. A
// design that cleared its marker there would delete the only reason anything ever looks
// again. The settlement is therefore a PROBE — re-attempt the save of CURRENT memory,
// so a fresh CAS arbitrates ownership — and the readback only distinguishes contention
// from a foreign owner. See docs/ARCHITECTURE.md.
//
// The probe re-marshals current memory rather than replaying the rejected bytes: a
// saveDefinitelyFailed caller has since rolled its mutation back, so replaying them
// would durably persist state the session no longer has.

package mqtt

import "sync/atomic"

// markWriteRejectedLocked records that a record CAS was DEFINITELY REJECTED at baseSeq.
//
// There is no probe rate limit beyond the periodic pass itself: the settlement runs once
// per pass over an unbound session, which IS the cadence. An earlier iteration carried a
// per-arming licence, but that existed only to stop a bound session's per-packet
// settlement from holding the ownership gate across JetStream round trips; with the bound
// half deferred to its own roadmap item, a licence would be a mechanism restating what the
// sweep interval already says.
//
// sess.mu must be held.
func (sess *mqttSession) markWriteRejectedLocked(baseSeq uint64) {
	sess.recordRejectedSeq = baseSeq
}

// recordRejectionPendingLocked reports whether a definite rejection is still
// outstanding work on this session.
//
// Validity is a predicate at the point of use rather than a field that every path must
// remember to clear, so every route that moves the sequence — a landed save, the
// resolver's adoption, a repair save's adopting arms — invalidates the marker without
// having to be enumerated. Each conjunct earns its place:
//
//   - != 0 because clear() resets sess.seq to 0, and two zeroes must not read as a
//     match;
//   - == sess.seq because a write that has since landed settled the question and moved
//     the sequence past this rejection's base;
//   - !seqUncertain because the uncertainty machinery owns such a session — and this one
//     is load-bearing, not defensive: an unresolved session's save is refused AT ENTRY,
//     before the store, so the detail comes back zero-valued and a probe that classified
//     on baseSeq == 0 would read any real record as foreign and dispose of the one object
//     holding that session's pendingWrite and continuations.
//
// sess.mu must be held.
func (sess *mqttSession) recordRejectionPendingLocked() bool {
	return sess.recordRejectedSeq != 0 && sess.recordRejectedSeq == sess.seq &&
		!sess.seqUncertain
}

// mqttTestRejectionWindowHookV fires at each point where this feature samples record
// state BEFORE taking the fence that would make the sample sound, named by the window —
// the same shape as clear's two-window hook. Both are windows in which another writer's
// landed save, armed uncertainty or open transaction is still invisible to the check that
// ran before it, and in neither case can anything outside open that window: the periodic
// pass skips a session whose fence is open, and the close's stamp sits between two
// statements. Nil in production.
var mqttTestRejectionWindowHookV atomic.Pointer[func(sess *mqttSession, window string)]

const (
	// mqttRejectionWindowSettle: between the settlement's pre-gate sample of the marker
	// and its fence acquisition.
	mqttRejectionWindowSettle = "settle-pre-fence"
	// mqttRejectionWindowStamp: between the close's entry uncertainty check and the
	// never-expire disconnect stamp's fence acquisition.
	mqttRejectionWindowStamp = "stamp-pre-fence"
)

func setMQTTTestRejectionWindowHook(h func(sess *mqttSession, window string)) func() {
	prev := mqttTestRejectionWindowHookV.Load()
	if h == nil {
		mqttTestRejectionWindowHookV.Store(nil)
	} else {
		mqttTestRejectionWindowHookV.Store(&h)
	}
	return func() { mqttTestRejectionWindowHookV.Store(prev) }
}

func (sess *mqttSession) rejectionWindowHook(window string) {
	if h := mqttTestRejectionWindowHookV.Load(); h != nil {
		(*h)(sess, window)
	}
}

// settleRecordRejection probes an outstanding record rejection and classifies what the
// probe's own CAS arbitrated. A FOREIGN verdict is disposed here; an UNRESOLVED one is the
// caller's, because resolving it needs the gate the caller already holds.
//
// It is UNBOUND-ONLY, and takes no client for that reason: the session it settles has none,
// so the disposition below disposes with nil. Converging a BOUND session is deferred to its
// own roadmap item — a settlement on the session's own readLoop has to decide whether the
// triggering packet may be dispatched and how the connection is torn down, and neither
// question arises here.
//
// The caller holds the ownership gate, claimed as nobody through
// asm.lockSessionForRecordRejection.
//
// No lock held on entry. Performs JetStream requests.
func (sess *mqttSession) settleRecordRejection(asm *mqttAccountSessionManager) mqttRejectionVerdict {
	sess.rejectionWindowHook(mqttRejectionWindowSettle)
	// NEVER the blocking form: a caller may hold a transaction on this goroutine, and a
	// refusal simply means "not now" — the marker is still armed and the next pass settles
	// it.
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		// A local writer holds the fence, so nothing is arbitrated. The marker stays armed
		// and the next pass tries again.
		return mqttRejectionKeep
	}
	verdict := sess.probeRecordRejectionInTx(tok)
	// Plain, never endRecordTxNewGeneration: on every arm but the landed one nothing was
	// mutated, and the landed one is an ordinary save.
	if terr := sess.endRecordTx(tok); terr != nil {
		sess.jsa.c.Errorf("MQTT: %v", terr)
	}
	if verdict == mqttRejectionForeign {
		// Outside the fence, as expireSession disposes: the disposal takes sess.mu, evicts
		// and closes, and holds no record state. nil, not a client: this path is
		// unbound-only, so there is no connection of ours to close.
		sess.disposeForeignSessionLocked(asm, nil)
	}
	return verdict
}

// probeRecordRejectionInTx is settleRecordRejection's body, with the fence held.
//
// No lock held on entry. Performs JetStream requests.
func (sess *mqttSession) probeRecordRejectionInTx(tok recordTx) mqttRejectionVerdict {
	// RE-READ the predicate inside the fence. The caller's pre-gate sample is a
	// check-then-act: the fence is what serializes record writers, so only a check taken
	// under it can see a save that landed, or an ambiguity that was armed, since.
	sess.mu.Lock()
	pending := sess.recordRejectionPendingLocked()
	domainTk, idHash := sess.domainTk, sess.idHash
	sess.mu.Unlock()
	if !pending {
		return mqttRejectionKeep
	}

	// THE PROBE: current memory, its own CAS at sess.seq. It can never overwrite a
	// record that moved — a foreign record rejects it — it never resurrects rolled-back
	// state, and it cannot recreate a released record either, because a rejection implies
	// a non-zero base and therefore a real expected-last-subject-sequence header.
	outcome, detail, err := sess.saveClassifiedDetail(tok)
	// A probe that is itself rejected re-arms the marker centrally, at the same base, so the
	// next pass asks again — which is the cadence this settlement wants.

	var smsg *StoredMsg
	var lerr error
	if outcome == saveDefinitelyFailed {
		// Inside the same transaction, so no other local writer can move the record
		// between the probe and the readback that classifies it.
		smsg, lerr = sess.jsa.loadSessionMsg(domainTk, idHash)
	}
	verdict := classifyRejectedProbe(outcome, detail, smsg, lerr)
	if verdict == mqttRejectionKeep && err != nil {
		sess.jsa.c.Debugf("MQTT: rejection probe for session %q established nothing: %v", sess.id, err)
	}
	return verdict
}
