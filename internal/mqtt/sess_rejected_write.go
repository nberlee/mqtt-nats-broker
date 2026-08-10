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
// This file settles BOTH shapes an armed marker can be in.
//
// An UNBOUND session is settled by the periodic pass, claiming the ownership gate as
// nobody (lockSessionForRecordRejection).
//
// A BOUND one would otherwise keep serving a client against a session another broker owns
// — PUBLISHes routed and acked, SUBSCRIBEs changing durable state — for the whole
// remaining life of the connection, which is the split-brain [MQTT-3.1.4-3] forbids. The
// gate such a settlement needs is the one a concurrent SUBSCRIBE claims, so the settlement
// is made BY that connection's own readLoop rather than against it
// (mqttGateDispatchOnRejection), BEFORE the triggering packet is dispatched: a QoS 1
// PUBLISH served from a displaced connection is acked and unretractable, so a check that
// runs after the dispatch switch has already lost.
//
// The bound half is FAIL-CLOSED, and the two questions it asks are deliberately named
// apart. recordRejectionGatesServingLocked asks whether the ownership question is still
// outstanding (may this packet be served?); recordRejectionPendingLocked asks whether a
// probe is valid right now (may we ask the stream again?). Gating service on the probe's
// licence would serve every packet between a contended probe and the next opportunity to
// re-probe, which is the same mistake as reading "no verdict" as permission: only a landed
// CAS is arbitration, and every other outcome closes the connection.
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

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// markWriteRejectedLocked records that a record CAS was DEFINITELY REJECTED at baseSeq.
//
// There is no probe rate limit on either settlement, and neither needs one. The unbound
// settlement runs once per periodic pass, which IS its cadence. The bound settlement
// performs AT MOST ONE probe per arming, because the only verdict that lets the connection
// keep running is the landed one — and a landed save moves sess.seq, retiring the marker.
// An earlier iteration carried a per-arming licence to bound the bound half's gate holds;
// with one probe per arming that licence would restate what the verdict handling already
// guarantees, and (worse) a licence read as a serve gate would license serving packets it
// never arbitrated.
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

// recordRejectionGatesServingLocked reports whether the OWNERSHIP QUESTION this rejection
// raised is still outstanding, which is what decides whether a bound client's packet may
// be served. It is deliberately not the same predicate as recordRejectionPendingLocked:
// that one is probe-VALIDITY ("may we ask the stream again right now?"), and reading a
// probe licence as permission to serve is how an earlier attempt served every packet
// between a contended probe and its next opportunity to re-probe.
//
// The two share the first two conjuncts — the marker is invalidated by any route that
// moves the sequence, because a landed CAS arbitrated the question — and differ in the
// third, which is ABSENT here on purpose. While a write of ours is unresolved, sess.seq
// still holds the base, so this predicate keeps gating: no probe is possible (an
// unresolved session's save is refused at entry) and no verdict exists, and no verdict is
// not permission. Resolution then either adopts a landed sequence, which moves sess.seq
// and retires the marker, or restores the base, which leaves this gate closed and the next
// packet probing.
//
// sess.mu must be held.
func (sess *mqttSession) recordRejectionGatesServingLocked() bool {
	return sess.recordRejectedSeq != 0 && sess.recordRejectedSeq == sess.seq
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
// so the disposition below disposes with nil. A BOUND session is settled by
// settleRecordRejectionBound on its own readLoop instead, because there the verdict also
// decides whether the triggering packet may be dispatched and how the connection is torn
// down — neither of which arises here.
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
		// The fence was unavailable — a local writer holds it, or it is free but reserved
		// for an announced blocking writer (see sess_recordtx.go) — so nothing is
		// arbitrated. The marker stays armed and the next pass tries again.
		return mqttRejectionKeep
	}
	verdict := sess.probeRecordRejectionInTx(tok)
	// Plain, never endRecordTxNewGeneration: on every arm but the landed one nothing was
	// mutated, and the landed one is an ordinary save.
	if terr := sess.endRecordTx(tok); terr != nil {
		sess.jsa.c.Errorf("MQTT: %v", terr)
	}
	if verdict == mqttRejectionForeign || verdict == mqttRejectionForked {
		// Outside the fence, as expireSession disposes: the disposal takes sess.mu, evicts
		// and closes, and holds no record state. nil, not a client: this path is
		// unbound-only, so there is no connection of ours to close — which is also why
		// the forked verdict changes nothing OBSERVABLE here. The helper returns before
		// the notification when there is no bound client, so no packet and no log line
		// differ; the arm exists so the classifier has one meaning at every call site
		// rather than a special case where the distinction happens not to be visible.
		sess.disposeForeignSessionLocked(asm, nil, mqttDisposalForRejection(verdict))
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
	// The pair this broker was ACKNOWLEDGED for, sampled under the fence alongside the
	// predicate that makes it meaningful. The probe below mints its own identity; this
	// is the base that identity descends from, and it is what a surviving sibling would
	// share a parent with. A rejected probe never moves it. See sess_fork_lineage.go.
	ourGen, ackWID, ackPWID := sess.gen, sess.wid, sess.pwid
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
	verdict := classifyRejectedProbe(outcome, detail, smsg, lerr, ourGen, ackWID, ackPWID)
	if verdict == mqttRejectionKeep && err != nil {
		sess.jsa.c.Debugf("MQTT: rejection probe for session %q established nothing: %v", sess.id, err)
	}
	return verdict
}

// errMQTTSessionOwnershipUnsettled is the sentinel the bound settlement fails closed with:
// the ownership question is outstanding and NOTHING ARBITRATED IT. It is not a takeover —
// no evidence of one exists — which is why it maps to 0x89 "Server busy, try again later"
// (mqttDisconnectReasonFromErr) rather than 0x8E: claiming a takeover the broker cannot
// demonstrate would tell a client to stop reconnecting on the strength of a guess.
//
// Returning it as a parse error is what makes the stop a real stop: the readLoop's
// disconnect() runs the ordinary close bookkeeping — the Will by its own rules, the expiry
// save, the purge — and the marker is settled after the unbind by the periodic pass. An
// earlier attempt returned nil from mid-parse instead, which closed nothing, silently
// dropped every following packet and skipped the footer that re-arms the keepalive
// deadline.
var errMQTTSessionOwnershipUnsettled = errors.New("session ownership is unsettled after a rejected record write")

// mqttBoundSettlement is what the gate-held half established, which decides both whether
// the packet may be dispatched and WHO tells the client the connection is ending.
type mqttBoundSettlement uint8

const (
	// mqttBoundServe: a fresh CAS landed (or nothing was outstanding). Dispatch.
	mqttBoundServe mqttBoundSettlement = iota
	// mqttBoundUnsettled: no verdict, with the ownership gate HELD — contended,
	// unreadable, ambiguous, or suspended before the claim. Nobody else is disposing this
	// connection, so it is ours to close and ours to explain: 0x89.
	mqttBoundUnsettled
	// mqttBoundDisplaced: the ownership claim was refused, or the disposal's identity
	// check found the session rebound. Some other operation owns this client ID right now
	// — a CONNECT taking the session over, a remote persist observation evicting it, a
	// resolver disposing it — and every one of those owns the notification as well as the
	// close. This arm therefore says NOTHING and only closes. Staying silent is what keeps
	// the REASON theirs; the claim would refuse a second attempt in any case, and their
	// attempt can itself be suppressed (CONNACK gate) or fail.
	mqttBoundDisplaced
	// mqttBoundDisposed: the probe proved the session foreign and this settlement disposed
	// it, so this connection is the one that owes the client its 0x8E.
	mqttBoundDisposed
)

// mqttGateDispatchOnRejection settles an outstanding record-CAS rejection BEFORE the
// packet that triggered the check is dispatched, and reports whether it may be dispatched
// at all.
//
// Three outcomes:
//
//   - (true, nil): a fresh CAS landed, so this broker demonstrably still owns the session
//     (or nothing was outstanding). Dispatch normally; the parse footer re-arms the
//     keepalive deadline as it does for any served packet.
//   - (false, err): NO VERDICT reached with the gate held — contended, unreadable or
//     suspended by an unresolved write of ours. Fail closed on the sentinel; the caller's
//     parse-error footer tells a v5 client 0x89 and the readLoop tears the connection down.
//   - (false, nil): the connection is already closed here — because the probe proved the
//     session FOREIGN (0x8E first, then the close) or because this connection was already
//     DISPLACED by another operation (closed silently). The caller returns nil exactly as
//     the graceful-DISCONNECT branch does.
//
// The silence on the displaced arm is load-bearing, not an omission. A refused claim means
// a CONNECT takeover, a remote persist observation or a resolver disposal is running for
// this client ID, and each of those attempts its OWN notification (0x8E, or 0x89 for a
// record its classifier proved forked) and closes the connection asynchronously — the
// persist observation's eviction worker claims no gate of its own, though it defers to one
// it observes held. Answering a refusal with 0x89 would try to append "server busy, try
// again" to another path's verdict: the connection's one DISCONNECT claim now refuses
// whichever of the two is second (mqttClaimDisconnectLocked), so the cost of speaking here
// is no longer a second packet but a WRONG one — this arm racing the displacer for the
// reason the client is left with, in an order neither path controls.
//
// CONNECT and DISCONNECT are exempt. Pre-CONNECT there is no bound session to arbitrate,
// and a second CONNECT is already a protocol error before dispatch. DISCONNECT neither
// routes nor acks anything: it carries the client's own instruction about its Will (which
// a fail-closed error-close would turn into a Will publication the client explicitly
// forbade, [MQTT-3.1.2-8] / Spec5 [3.14.4]), a session-expiry override already guarded by
// sess.c == c and by the record CAS, and the teardown that IS the [MQTT-3.1.4-3]
// convergence — refusing to process the packet that ends the split-brain would be
// self-defeating. PINGREQ is NOT exempt, and that is what bounds convergence: a conformant
// client sends one within its keepalive, so even an otherwise silent displaced connection
// closes within about 1.5 keepalive intervals.
//
// Runs on the client's readLoop, from mqttParse's per-packet loop. No lock held on entry.
// Performs JetStream requests only when a rejection is actually outstanding.
func (c *client) mqttGateDispatchOnRejection(pt byte) (bool, error) {
	if pt == mqttPacketConnect || pt == mqttPacketDisconnect {
		return true, nil
	}
	c.mu.Lock()
	var sess *mqttSession
	var asm *mqttAccountSessionManager
	if c.mqtt != nil {
		sess, asm = c.mqtt.sess, c.mqtt.asm
	}
	c.mu.Unlock()
	if sess == nil {
		return true, nil
	}

	// The fast path for every packet on every healthy connection: one sess.mu acquisition
	// that finds nothing outstanding.
	sess.mu.Lock()
	gates := sess.recordRejectionGatesServingLocked()
	suspended := sess.seqUncertain
	sess.mu.Unlock()
	if !gates {
		return true, nil
	}
	if suspended {
		// An unresolved write owns this session's record: its save is refused at entry, so
		// no probe can run and no verdict can exist. Fail closed. The close bookkeeping
		// PARKS the teardown (mqttHandleClosedClient's unresolved branch) and the existing
		// resolution machinery discharges or disposes it.
		return false, fmt.Errorf("session %q: %w", sess.id, errMQTTSessionOwnershipUnsettled)
	}
	if asm == nil {
		// No manager to claim the ownership gate through, so nothing can arbitrate. Same
		// answer as any other no-verdict outcome.
		return false, fmt.Errorf("session %q: %w", sess.id, errMQTTSessionOwnershipUnsettled)
	}

	outcome, stale := c.settleBoundRejectionGated(asm, sess)
	switch outcome {
	case mqttBoundServe:
		return true, nil
	case mqttBoundDisposed:
		// The gate is released; the tail runs HERE, on the readLoop, in the graceful
		// DISCONNECT branch's shape: notify, then close synchronously, then return so the
		// next Read fails instead of parsing a queued packet against a disposed session.
		// Doing it inline is also what keeps the ownership gate off socket I/O.
		if stale != nil {
			// Only mqttRejectionForeign reaches mqttBoundDisposed — the forked verdict
			// routes to mqttBoundUnsettled above and never disposes — so the takeover
			// vocabulary is the only one this tail can owe.
			stale.mqttNotifyForeignDisposal(sess.id, mqttDisposalTakenOver)
			stale.closeConnection(mqttDisposalTakenOver.closeReason)
		}
		return false, nil
	case mqttBoundDisplaced:
		// Whoever displaced this connection owns the notification. Close synchronously
		// anyway — that path's close is asynchronous, and until it lands this readLoop
		// would keep parsing packets against a session that is no longer ours. The Will is
		// left exactly as it is: the displacing path already decided its fate.
		c.closeConnection(DuplicateClientID)
		return false, nil
	}
	return false, fmt.Errorf("session %q: %w", sess.id, errMQTTSessionOwnershipUnsettled)
}

// settleBoundRejectionGated is mqttGateDispatchOnRejection's gate-held half: it claims the
// ownership gate, probes, and applies the verdict's STATE changes — everything that must
// not race a CONNECT. It reports what was established, and, for a foreign verdict whose
// disposal APPLIED, the stale client its caller must notify and close.
//
// The gate is claimed through lockSessionForTeardown, NOT plain lockSession, and the
// difference is a Will-correctness one. A refusal does not say which competing operation
// holds the gate: a CONNECT registers the client-ID gate long before it rebinds the session
// (mqttProcessConnect holds it for its whole duration), and a resolver can hold it for a
// session that stays OURS. Closing this connection on the bare refusal therefore ends it
// while a takeover is still mid-flight — and mqttHandleClosedClient marks the Will owed
// BEFORE it contends for the gate, so that takeover's later mqttDiscardWillOnTakeover finds
// an already-owed Will, preserves it by its own too-late-to-cancel rule, and the close
// publishes a Will the takeover was supposed to suppress.
//
// So a refusal is CLASSIFIED rather than assumed, on the same bounded spin the teardown
// path uses for exactly this problem: it retries while this connection is still the
// session's owner and gives up the moment the binding moves. Every holder is bounded (a
// CONNECT rebinds early, a resolver by its JetStream timeouts), so the wait is bounded too.
//
//   - Acquired: the competing operation finished; probe normally.
//   - REBOUND: a takeover owns this connection's fate now. Apply the takeover's own Will
//     rule here — the identical call it makes a few lines later, so whichever side notices
//     the displacement first applies the same rule and the other's is a no-op — and report
//     displacement, which closes the connection without a packet.
//   - Still ours after the spin: nothing is displacing us that we can see, so this is an
//     ordinary no-verdict outcome and says 0x89.
//
// Runs on the client's readLoop. No lock held on entry. Performs JetStream requests.
func (c *client) settleBoundRejectionGated(asm *mqttAccountSessionManager, sess *mqttSession) (mqttBoundSettlement, *client) {
	if err := asm.lockSessionForTeardown(sess, c); err != nil {
		sess.mu.Lock()
		rebound := sess.c != c
		sess.mu.Unlock()
		if !rebound {
			return mqttBoundUnsettled, nil
		}
		c.mqttDiscardWillOnTakeover()
		return mqttBoundDisplaced, nil
	}
	defer asm.unlockSession(sess)

	switch sess.settleRecordRejectionBound() {
	case mqttRejectionOurs:
		// The probe's CAS arbitrated ownership our way, and the advanced sequence retired
		// the marker. This connection is served again from the fast path.
		return mqttBoundServe, nil
	case mqttRejectionForeign:
		// Exactly what a local takeover does: an armed Will goes away with the connection
		// the displacement closes, and one already owed NOW is not the takeover's to
		// cancel. Before the disposal, so the disposal's own close cannot observe a Will
		// still armed.
		c.mqttDiscardWillOnTakeover()
		stale, applied := sess.disposeForeignStateLocked(asm, c)
		if !applied {
			// The identity check refused: the session was rebound while this verdict was
			// being reached, so the disposal — and the notification — are not ours.
			return mqttBoundDisplaced, nil
		}
		return mqttBoundDisposed, stale
	case mqttRejectionForked:
		// A PROVEN sibling of the record we were acknowledged for: our own write was
		// erased by something that is no successor of ours, and nothing established
		// another owner. That is precisely what mqttBoundUnsettled already means — the
		// ownership question is outstanding with the gate held and nobody else
		// disposing this connection — so it routes there rather than to a fifth
		// settlement, and the client is told 0x89 by the caller's sentinel.
		//
		// Deliberately NOT the foreign arm's shape. That arm disposes the session and
		// calls mqttDiscardWillOnTakeover first, because a demonstrated takeover owns
		// the Will's fate. Here nothing is demonstrated, so the connection takes the
		// ordinary close bookkeeping — the Will handled by its own rules, the expiry
		// save attempted, shared pending purged — and the session object stays
		// resident with its marker armed, settled by the next reconnect's accept save
		// or by the periodic unbound pass. Copying the foreign arm would silently
		// suppress a Will nothing arbitrated. See docs/CONFORMANCE.md.
		return mqttBoundUnsettled, nil
	case mqttRejectionKeep:
		// Nothing was arbitrated by the probe — but the fence it took may have been
		// waiting on a writer whose save LANDED, which settles the question without any
		// probe at all. Re-read the serve gate rather than the probe's licence: false
		// means the marker was retired while we waited.
		sess.mu.Lock()
		still := sess.recordRejectionGatesServingLocked()
		sess.mu.Unlock()
		if still {
			return mqttBoundUnsettled, nil
		}
		return mqttBoundServe, nil
	default: // mqttRejectionUnresolved
		// The probe's own write is ambiguous, so it established nothing either. Fail
		// closed and let the uncertainty machinery — which now owns this session — reach
		// a verdict on its own schedule.
		return mqttBoundUnsettled, nil
	}
}

// settleRecordRejectionBound is settleRecordRejection for a BOUND session, probing on that
// session's own readLoop and returning the verdict RAW.
//
// It differs from the unbound settlement in two ways, both consequences of running on the
// readLoop:
//
//   - the fence is taken in the BLOCKING form, as mqttProcessSubs does. This is an
//     outermost readLoop entry point, so no transaction can be held on this goroutine, and
//     an off-readLoop writer holding the fence is doing bounded JetStream work whose result
//     the in-fence re-read must see. The unbound settlement cannot block: its caller may
//     hold a transaction, and "not now" is a complete answer to a periodic pass.
//   - a foreign verdict is NOT disposed here. The bound caller's disposal has a tail — the
//     0x8E and the close — that must run off the ownership gate and on this readLoop, so
//     sequencing it belongs to the caller.
//
// The caller holds the ownership gate, claimed as the bound client.
//
// No lock held on entry. Performs JetStream requests.
func (sess *mqttSession) settleRecordRejectionBound() mqttRejectionVerdict {
	// The same pre-fence window the unbound settlement fires, for the same reason: the
	// caller's sample of the marker was taken without the fence that makes it sound. Fired
	// here rather than inside probeRecordRejectionInTx because settleRecordRejection fires
	// it itself, and one hook per window is what the tests key on.
	sess.rejectionWindowHook(mqttRejectionWindowSettle)
	tok := sess.beginRecordTx()
	verdict := sess.probeRecordRejectionInTx(tok)
	// Plain, never endRecordTxNewGeneration: on every arm but the landed one nothing was
	// mutated, and the landed one is an ordinary save.
	if terr := sess.endRecordTx(tok); terr != nil {
		sess.jsa.c.Errorf("MQTT: %v", terr)
	}
	return verdict
}
