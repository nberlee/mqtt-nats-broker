// Plain QoS 1/2 consumer reconciliation on session resume.
//
// The session record persists each plain QoS 1/2 subscription's JetStream
// consumer config (ps.Cons) and the CONNECT restore path used to trust it
// verbatim: a durable reaped by an inactivity threshold — the broker's own
// consumer_inactive_threshold, or a stream-forced ConsumerLimits.
// InactiveThreshold, both of which reach consumers requesting 0 — was silently
// restored, and the client got Session Present = 1 with a subscription that
// never delivered again until a clean start.
//
// This file is the fix: a dedicated resume phase, run inside the CONNECT's
// accept transaction AFTER recoverInflightRegisters (so every in-flight
// register is classified against the OLD durable names before any repair
// exists) and BEFORE the CONNACK (so a failure refuses the CONNECT and nothing
// has been transmitted when it fails). The pass is:
//
//	collect   every plain QoS 1/2 (subject, sid) pair — the level-up companion
//	          of a multi-level filter included, since it is an independent
//	          consumer — and mint an identity outright for a QoS 1/2 sid with
//	          no Cons entry at all (the restore path would otherwise create its
//	          consumer after the CONNACK and never persist it);
//	subscribe every recorded deliver subject CLOSED and flushed, BEFORE the
//	          lookup: a consumer that is present at lookup can be reaped a
//	          moment later — the lookup alone does not stop the inactivity
//	          timer — so interest must exist before the lookup result means
//	          anything;
//	verify    all collected durables in one bounded batched lookup; an
//	          indeterminate answer REFUSES the CONNECT (fail closed: a session
//	          resumed with silently dead subscriptions is the bug this phase
//	          exists to fix, so the client reconnects instead);
//	repair    each authoritatively absent durable under a FRESH identity —
//	          fresh nuid durable and deliver subject, never the recorded name:
//	          a consumer recreated under the old name starts DeliverNew with
//	          its ack floor at the stream's tail, so on the next resume every
//	          register naming old sequences would flip Lost -> false Complete
//	          and free identifiers the client may still hold; and the recorded
//	          $JS.ACK/NAK subjects embed the name with per-instance counters.
//	          Subscribe-first here too, so the replacement cannot be reaped in
//	          the create-to-subscribe gap;
//	save      the repaired identities in ONE record write, still pre-CONNACK.
//	          By construction this is the first write after the accept save:
//	          processSubs and everything it can reach runs only after the
//	          CONNACK, so no general subscription-processing write can observe
//	          a half-committed replacement.
//
// Every phase-installed subscription carries a phase-owned marker and stays
// CLOSED until subscription restoration adopts it IN PLACE (processJSConsumer's
// restore exists-branch): re-issuing it would put a no-interest gap right back
// into the flow. A marker still standing after processSubs means restoration
// skipped that filter (a PUBREL setup failure, a raw-subscription failure); the
// sweep removes it, because nothing else ever would — the connection stays
// live, a later re-SUBSCRIBE's metadata refresh does not clear `closed`, and a
// closed subscription would otherwise hold consumer interest forever. Such a
// filter stays unavailable until the next CONNECT: live re-SUBSCRIBE is NOT a
// recovery mechanism (stated in docs/CONFORMANCE.md).
//
// See docs/ARCHITECTURE.md "Plain QoS 1/2 consumer reconciliation on resume".

package mqtt

import (
	"fmt"
	"strings"

	"github.com/nats-io/nuid"
)

// mqttResumeReconcileVerdict is how the reconciliation phase ended. Anything
// but "proceed" refuses the CONNECT through the pre-CONNACK abort discipline.
type mqttResumeReconcileVerdict int

const (
	// mqttReconcileProceed: every collected durable is verified or repaired, and
	// any repair is durably named by the record. The CONNACK may be sent.
	mqttReconcileProceed mqttResumeReconcileVerdict = iota
	// mqttReconcileRefuseLocal: the phase failed definitively and undid its own
	// work. The resident session is still ours, stays mapped, and detaches
	// through ordinary close bookkeeping under the lifetime it already had — the
	// CONNECT's requested Session Expiry Interval is applied only at the grant
	// point, after this phase, so a refused resume leaves nothing to repair. The
	// client reconnects and resumes.
	mqttReconcileRefuseLocal
	// mqttReconcileRefuseForeign: the repair save proved another broker owns the
	// session record; it has been foreign-disposed (DISCONNECT suppressed, since
	// no success CONNACK was written). The phase's consumers are referenced by
	// no record and no live session, so the orphan sweep reclaims them.
	mqttReconcileRefuseForeign
	// mqttReconcileRefuseUnresolved: the repair save's outcome is unknown. The
	// pending-write state carries the attempt's identity and BOTH continuations
	// (not-landed -> the phase's undo; landed -> keep the replacements), and
	// resolution is deferred: the accept transaction is still open, so the
	// readback runs from the deferred-close path, the periodic uncertain pass,
	// or the next CONNECT. No traffic is served meanwhile — no CONNACK was sent
	// and every phase subscription is closed.
	mqttReconcileRefuseUnresolved
)

// mqttReconcileEntry is one plain QoS 1/2 delivery identity the phase owes a
// verdict: a sid (the filter itself, or its level-up companion), the subject
// its consumer filters, and the subscription options its delivery callback
// reads. cc is the recorded consumer config; nil means the record carries the
// subscription but no consumer identity at all (absent identity), which is
// repaired exactly like a reaped durable, minus the stale pieces.
type mqttReconcileEntry struct {
	sid      string
	subject  string
	qos      byte
	noLocal  bool
	rap      bool
	subID    int
	reserved bool
	cc       *ConsumerConfig
}

// mqttRepairSaveAttempts bounds the re-read/retry of a repair save rejected by
// clustered contention (10164), which is a rejection but NOT evidence that the
// record moved — the same posture as mqttTeardownClaimAttempts. Exhausting it
// with the base sequence still unchanged is a local-definite failure: the phase
// undoes its work and the client reconnects.
const mqttRepairSaveAttempts = 3

// mqttRepairSaveVerdict is how a contended repair save was finally classified.
type mqttRepairSaveVerdict int

const (
	// mqttRepairSaveLanded: the repair record is durably stored (a retry
	// succeeded, or a re-read found our exact bytes at a newer sequence — the
	// response was lost to contention). sess.seq has adopted the landed sequence.
	mqttRepairSaveLanded mqttRepairSaveVerdict = iota
	// mqttRepairSaveForeign: the record moved to content that is not our write
	// (or is gone). Another owner is authoritative.
	mqttRepairSaveForeign
	// mqttRepairSaveLocalDefinite: retries exhausted with the base sequence still
	// unchanged. Nothing was stored; undo and refuse.
	mqttRepairSaveLocalDefinite
	// mqttRepairSaveUnresolved: a retry's outcome (or the re-read itself) is
	// unknowable right now. The pending-write state has been established with the
	// attempt's base and digest; the caller registers the continuations.
	mqttRepairSaveUnresolved
)

// mqttRecordAttemptVerdict is what a readback says about the ATTEMPT it was about,
// before any digest comparison: the base/gone/moved decision alone.
type mqttRecordAttemptVerdict uint8

const (
	// mqttRecordAttemptUnreadable: the re-read itself is unavailable, so nothing can
	// be classified. Never a verdict about ownership.
	mqttRecordAttemptUnreadable mqttRecordAttemptVerdict = iota
	// mqttRecordAttemptGone: the record is absent. A write of ours cannot have landed
	// and then vanished on its own, so somebody released this session.
	mqttRecordAttemptGone
	// mqttRecordAttemptAtBase: the record is still exactly the one we CAS'd against.
	// This is CONTENTION, never ownership — JetStream returns 10164 while a write for
	// the subject is in process upstream, so the contender's proposal can commit a
	// moment later and a read that finds the base sequence proves nothing.
	mqttRecordAttemptAtBase
	// mqttRecordAttemptMoved: the record is at some other sequence. Whether that is
	// our own write landing with its response lost is a question about CONTENT, which
	// only a caller holding the attempted bytes can answer.
	mqttRecordAttemptMoved
)

// classifyRecordAgainstAttempt is the base/gone/moved decision for a readback taken
// about an attempted write at baseSeq.
//
// It exists as one function so "the base sequence is contention, not ownership" has a
// single home: every caller that reads a record back to classify its own failed write
// asks the same question, and the one that answered it by inference is the defect this
// mechanism was built for.
func classifyRecordAgainstAttempt(smsg *StoredMsg, lerr error, baseSeq uint64) mqttRecordAttemptVerdict {
	if lerr != nil {
		if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
			return mqttRecordAttemptUnreadable
		}
		return mqttRecordAttemptGone
	}
	if smsg == nil {
		// No error and no message is not a state the load boundary produces; treat it
		// as unclassifiable rather than as either owner's evidence.
		return mqttRecordAttemptUnreadable
	}
	if smsg.Sequence == baseSeq {
		return mqttRecordAttemptAtBase
	}
	return mqttRecordAttemptMoved
}

// mqttRejectionVerdict is how a REJECTION PROBE — a re-attempt of the session's
// current memory, made so a CAS arbitrates ownership rather than a read guessing at
// it — was classified.
type mqttRejectionVerdict uint8

const (
	// mqttRejectionKeep: nothing was established. The probe was refused, contended, or its
	// readback unreadable; the marker stays armed and a later pass settles it. No ownership
	// claim in either direction, so no caller may act on it.
	//
	// An earlier iteration split this three ways — contended (the record demonstrably has
	// not moved), indeterminate (nothing arbitrated at all) and nothing-owed — because a
	// BOUND session's packet tail was expected to decide whether to serve traffic on each.
	// That half now exists and does NOT need the split: only a landed CAS licenses serving,
	// so every non-landed shade is one answer, and the bound caller's own re-read of the
	// serve gate distinguishes the case it actually has to act on (a marker retired while
	// it waited for the fence) without any help from the verdict.
	mqttRejectionKeep mqttRejectionVerdict = iota
	// mqttRejectionOurs: the probe LANDED. The CAS arbitrated ownership in our
	// favour, and the advanced sequence invalidates the marker.
	mqttRejectionOurs
	// mqttRejectionForeign: the probe definitively did not land and the record is not
	// this attempt's. Another owner holds the session.
	mqttRejectionForeign
	// mqttRejectionUnresolved: the probe's own outcome is unknown. The uncertainty
	// machinery owns the session until a readback classifies it.
	mqttRejectionUnresolved
)

// classifyRejectedProbe is the probe's whole post-save decision, the CAS guard
// included.
//
// The guard is the load-bearing part. A save can fail DEFINITELY without ever
// reaching the store — a refused fence, a refusal at entry — and such a failure comes
// back with a ZERO-VALUED detail, so classifying its readback would compare a real
// record against base sequence 0 and read every session in the account as foreign.
// Only a CAS-typed store error means "this attempt was rejected by the stream", which
// is the only failure a readback can say anything about. Everything else keeps the marker.
//
// There is no digest arm, and that is deliberate rather than an omission: a rejected
// write did not land, so a later record matching our bytes cannot be this attempt.
func classifyRejectedProbe(outcome saveOutcome, detail mqttSessSaveDetail, smsg *StoredMsg, lerr error) mqttRejectionVerdict {
	switch outcome {
	case saveLanded:
		return mqttRejectionOurs
	case saveMaybeLanded:
		return mqttRejectionUnresolved
	}
	if !mqttIsRecordCASRejection(detail.rawErr) {
		return mqttRejectionKeep
	}
	switch classifyRecordAgainstAttempt(smsg, lerr, detail.baseSeq) {
	case mqttRecordAttemptGone:
		return mqttRejectionForeign
	case mqttRecordAttemptMoved:
		// The probe definitively did not land, so an accepted transition at another
		// sequence is not this attempt's — and it cannot be ours by another route: a
		// second local writer needs the fence this probe holds, an earlier ambiguous
		// write of ours would have armed seqUncertain (which the marker's predicate
		// excludes), a replacement object on this broker is excluded by the
		// identity-current ownership gate, and the expiry sweep's raw CAS skips a
		// client whose session is resident.
		return mqttRejectionForeign
	default: // at base (contention) or unreadable: nothing established either way
		return mqttRejectionKeep
	}
}

// mqttReleasePhaseGateLocked releases a phase-installed subscription's delivery
// gate, waking any QoS 1/2 delivery waiting on it. Idempotent — every release
// path (adoption, the sweep, the phase's own undo) calls it, and the release
// must happen exactly once for a channel close to be safe.
//
// sess.mu and sess.subsMu must both be held, the same pair that guards `closed`:
// a waiter re-reads that flag the moment it wakes, so releasing under anything
// weaker would let it observe a half-written decision.
func mqttReleasePhaseGateLocked(sub *subscription) {
	if sub == nil || sub.mqtt == nil || sub.mqtt.phaseGate == nil {
		return
	}
	close(sub.mqtt.phaseGate)
	sub.mqtt.phaseGate = nil
}

// disownPhaseSub clears the phase-owned marker, so a QoS 1/2 delivery that
// wakes on the gate afterwards neither counts as adoptable nor records itself as
// withheld. Used by the paths that REMOVE a phase subscription; the paths that
// keep it (an ambiguous refusal) release the gate alone.
//
// No lock held on entry.
func (sess *mqttSession) disownPhaseSub(sub *subscription) {
	if sub == nil {
		return
	}
	sess.mu.Lock()
	sess.subsMu.Lock()
	if sub.mqtt != nil {
		sub.mqtt.phaseOwned = false
	}
	sess.subsMu.Unlock()
	sess.mu.Unlock()
}

// mqttReleasePhaseGate is mqttReleasePhaseGateLocked for a caller holding
// neither lock.
func (sess *mqttSession) mqttReleasePhaseGate(sub *subscription) {
	sess.mu.Lock()
	sess.subsMu.Lock()
	mqttReleasePhaseGateLocked(sub)
	sess.subsMu.Unlock()
	sess.mu.Unlock()
}

// mqttAbortAcceptedConnect unwinds the parser-visible half of an accepted
// CONNECT that must be refused BEFORE a success CONNACK. The client is already
// bound (connectReceived set, the CONNECT — Will included — installed), so a
// bare error return would have the readLoop send a v5 DISCONNECT as the FIRST
// packet, violating [MQTT-3.2.0-1], and close handling could publish the NEW
// connection's Will for a session that was never granted.
//
// It clears exactly those two exposures — this connection's Will, and the
// parser's pre-CONNACK connected flag (whose production reads are confined to
// MQTT parsing; clearing it is what suppresses the parser's v5 DISCONNECT
// branch). The session's own lifetime is restored by the caller
// (abortAcceptedResume), which is what keeps an unacknowledged resume from
// applying its Session Expiry Interval. This helper deliberately PRESERVES
// c.mqtt.sess/c.mqtt.cp:
// mqttHandleClosedClient reads the session directly and independently of the
// flag, and for a local refusal that bookkeeping must run — it is what detaches
// the client from the resident session and persists/schedules a finite expiry.
//
// Applies to EVERY post-bind exit before a confirmed success CONNACK: the
// register-recovery error return, the reconciliation refusals, and the
// defensive CONNACK-size/write failure.
//
// No lock held on entry.
func (c *client) mqttAbortAcceptedConnect() {
	c.mu.Lock()
	c.mqttDiscardWillLocked()
	c.flags.clear(connectReceived)
	c.mu.Unlock()
}

// reconcileResumedConsumers is the resume reconciliation phase. Runs from the
// client's readLoop, inside the CONNECT's still-open accept transaction, with
// the session held in asm.sessLocked (which is the ownership gate the foreign
// disposition requires).
//
// On mqttReconcileProceed it returns the deliver subjects of every
// still-phase-owned subscription, for the post-processSubs sweep (and its
// deferred fallback) to reclaim if restoration skips a filter. On any refusal
// the caller aborts the CONNECT through mqttAbortAcceptedConnect; the phase has
// already put the session in the state its verdict names.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) reconcileResumedConsumers(sess *mqttSession, c *client, tok recordTx) ([]string, mqttResumeReconcileVerdict, error) {
	opts := c.srv.getOpts()
	ackWait := opts.MQTT.AckWait
	if ackWait == 0 {
		ackWait = mqttDefaultAckWait
	}
	baseMaxAckPending := int(opts.MQTT.MaxAckPending)
	if baseMaxAckPending == 0 {
		baseMaxAckPending = mqttDefaultMaxAckPending
	}

	// ---- Step 1: collect --------------------------------------------------
	//
	// Plain filters only: shared subscriptions live in sharedSubs and their
	// group durables have their own machinery; drained sids are not in cons
	// (stageDrain removed them) and their filters are not in subs unless a
	// re-SUBSCRIBE adopted the drain — in which case the entry is back in cons
	// and is verified here like any other. Filters in the reserved "$MQTT.>"
	// namespace are excluded outright: subscription restoration denies and
	// purges them, so the phase must never install interest for them. A stale
	// Cons entry with no matching subscription is not an active delivery entry
	// and is deliberately not verified (stated residual, docs/CONFORMANCE.md).
	var entries []*mqttReconcileEntry
	sess.mu.Lock()
	idHash := sess.idHash
	rmax := sess.rmax
	for subject, qos := range sess.subs {
		if qos == 0 || strings.HasPrefix(subject, mqttPrefix) {
			continue
		}
		reserved := isMQTTReservedSubscription(subject)
		_, noLocal := sess.noLocalSubs[subject]
		_, rap := sess.rapSubs[subject]
		subID := sess.subIDs[subject]
		entries = append(entries, &mqttReconcileEntry{
			sid: subject, subject: subject, qos: qos,
			noLocal: noLocal, rap: rap, subID: subID, reserved: reserved,
			cc: sess.cons[subject],
		})
		// The level-up companion of a multi-level filter is an independent
		// consumer (processSubs creates it through the same fwc derivation); a
		// reaped companion would otherwise stay dead while the main half
		// verified clean.
		if mqttNeedSubForLevelUp(subject) {
			fwcsid := subject[:len(subject)-2] + mqttMultiLevelSidSuffix
			entries = append(entries, &mqttReconcileEntry{
				sid: fwcsid, subject: subject[:len(subject)-2], qos: qos,
				noLocal: noLocal, rap: rap, subID: subID, reserved: reserved,
				cc: sess.cons[fwcsid],
			})
		}
	}
	sess.mu.Unlock()
	if len(entries) == 0 {
		return nil, mqttReconcileProceed, nil
	}

	// Everything the phase does is tracked so a failure undoes exactly that and
	// nothing else — the same undo-exactly-what-you-did shape as the affine
	// path. installed is keyed by deliver subject so the step-3 removal of a
	// stale step-2 subscription keeps the set identity-current.
	installed := make(map[string]struct{})
	// The subset of installed that delivers for a durable THIS phase minted, and
	// which a failure therefore deletes. See unsubscribePhase's doomed argument.
	mintedInbox := make(map[string]bool)
	var created []*ConsumerConfig
	type replacedCons struct {
		sid    string
		old    *ConsumerConfig
		hadOld bool
	}
	var replaced []replacedCons

	// releaseGates wakes every delivery parked on a phase subscription without
	// removing the subscription itself. The refusal paths that KEEP the phase's
	// state — an ambiguous save, whose outcome only a readback can settle — still
	// owe this: a parked delivery would otherwise wait out its whole bound and
	// then find the session detached, at which point it can no longer record
	// itself as never transmitted and its redelivery reaches the client marked
	// DUP.
	releaseGates := func() {
		for deliver := range installed {
			c.mu.Lock()
			sub := c.subs[deliver]
			c.mu.Unlock()
			sess.mqttReleasePhaseGate(sub)
		}
	}

	// unsubscribePhase removes one phase-installed subscription, releasing its
	// gate FIRST so a delivery parked on it wakes, re-reads `closed`, and skips
	// rather than waiting out the bound against a subscription that is going
	// away.
	// doomed says this subscription's durable is being deleted (or is already
	// authoritatively absent), so a never-transmitted marker for it could never
	// be consumed: disown before opening the gate, and a delivery that wakes
	// into the removal records nothing. A SURVIVING consumer is the opposite
	// case — its messages are still owed, so it stays phase-owned and the woken
	// delivery records itself exactly as the docs promise.
	unsubscribePhase := func(deliver string, doomed bool) {
		c.mu.Lock()
		sub := c.subs[deliver]
		c.mu.Unlock()
		if doomed {
			sess.disownPhaseSub(sub)
		}
		sess.mqttReleasePhaseGate(sub)
		if uerr := c.processUnsub([]byte(deliver)); uerr != nil {
			c.Warnf("MQTT: unable to remove a resume-reconciliation delivery subscription on %q: %v", deliver, uerr)
		}
	}

	// undo unwinds the phase's own prior work. The stale charge of an
	// authoritatively absent durable is PERMANENTLY retired and never re-added:
	// consCharged holds only what this process charged for live consumers.
	// deleteConsumer refunds each created durable exactly once. Safe after the
	// connection has closed too (the uncertain continuation runs it from a
	// resolver): processUnsub on a closed client is a no-op.
	undo := func() {
		for deliver := range installed {
			unsubscribePhase(deliver, mintedInbox[deliver])
		}
		for _, cc := range created {
			sess.deleteConsumer(cc)
		}
		sess.mu.Lock()
		// A gated delivery may already have recorded a never-transmitted marker
		// against a durable this undo just deleted. trackPublish consumes such a
		// marker only when that durable delivers again, which it never will, so
		// leaving it would strand an entry for the life of the session — and every
		// retried repair mints another identity to strand.
		for _, cc := range created {
			for key := range sess.deferred {
				if key.jsDur == cc.Durable {
					delete(sess.deferred, key)
				}
			}
		}
		for _, r := range replaced {
			if r.hadOld {
				sess.cons[r.sid] = r.old
			} else {
				delete(sess.cons, r.sid)
			}
		}
		sess.mu.Unlock()
	}

	// subscribeClosed installs a delivery subscription CLOSED, flushed, and
	// phase-owned. Through processQOS12Sub — never bare processSub — so sub.mqtt
	// is fully wired for the delivery callback from the start. processSub
	// unconditionally clears the teardown mark, so the closed flag is set inside
	// the same sess.mu critical section that installs the subscription; the
	// QoS 1/2 callback checks it at entry under the same lock, so nothing
	// reaches the client before the CONNACK. A delivery pushed into that window
	// WAITS on the phase gate instead of being dropped — see mqttSub.phaseGate.
	subscribeClosed := func(e *mqttReconcileEntry, deliver, durable string) error {
		sess.mu.Lock()
		sub, err := sess.processQOS12Sub(c, []byte(deliver), []byte(deliver), nil,
			e.reserved, e.qos, e.noLocal, e.rap, false, e.subID, durable, mqttDeliverMsgCbQoS12)
		if err == nil {
			sess.subsMu.Lock()
			if sub.mqtt != nil {
				sub.mqtt.closed = true
				sub.mqtt.phaseOwned = true
				// The gate every delivery pushed into this window waits on. It is
				// what keeps the offline backlog a set of first transmissions
				// instead of AckWait-delayed DUPs; released on adoption, and on
				// every path that reclaims this subscription instead.
				sub.mqtt.phaseGate = make(chan struct{})
			}
			sess.subsMu.Unlock()
		}
		sess.mu.Unlock()
		return err
	}

	// ---- Step 2: subscribe everything closed, then batch verify ------------
	//
	// Interest first, for present consumers too: a consumer present at lookup
	// can be reaped a moment later (the lookup does not stop the inactivity
	// timer), and its dead recorded deliver subject would then be restored — the
	// target bug, reproduced. Re-subscribing a recorded deliver subject is safe:
	// a same-broker predecessor connection closes asynchronously but its
	// callback is rejected after rebinding by the sess.c != cc gate, and
	// cross-broker overlap is the existing takeover-notification window, not a
	// new identity collision.
	for _, e := range entries {
		if e.cc == nil {
			continue // absent identity: minted fresh in step 3
		}
		if err := subscribeClosed(e, e.cc.DeliverSubject, e.cc.Durable); err != nil {
			undo()
			return nil, mqttReconcileRefuseLocal,
				fmt.Errorf("unable to install delivery interest for durable %q of filter %q: %v", e.cc.Durable, e.sid, err)
		}
		installed[e.cc.DeliverSubject] = struct{}{}
	}

	var durables []string
	seen := make(map[string]struct{})
	for _, e := range entries {
		if e.cc == nil {
			continue
		}
		if _, dup := seen[e.cc.Durable]; dup {
			continue
		}
		seen[e.cc.Durable] = struct{}{}
		durables = append(durables, e.cc.Durable)
	}

	// The lookup is resolved through an instance-scoped hook so a test can fail
	// (or delay) a chosen batch of a real CONNECT deterministically; production
	// resolves to the pipelined multi-lookup on the messages stream.
	as.mu.RLock()
	lookup := as.testResumeReconcileLookup
	as.mu.RUnlock()
	if lookup == nil {
		lookup = func(durs []string) ([]*ConsumerInfo, []bool, error) {
			return as.jsa.lookupConsumersMulti(mqttStreamName, durs)
		}
	}
	present, err := mqttVerifyConsumersBatched(durables, mqttInflightLoadBatch, lookup)
	if err != nil {
		// Indeterminate: never recreate over a consumer that may exist, and never
		// resume a session whose subscriptions may be silently dead. Fail closed;
		// the client reconnects.
		undo()
		return nil, mqttReconcileRefuseLocal,
			fmt.Errorf("unable to verify the session's QoS 1/2 consumers: %v", err)
	}

	// ---- Step 3: repair each absent durable --------------------------------
	repairedAny := false
	for _, e := range entries {
		if e.cc != nil && present[e.cc.Durable] {
			continue // verified: the closed step-2 subscription holds its interest
		}
		repairedAny = true

		// (1) Subscribe first, on a FRESH deliver inbox, closed: interest exists
		// before the consumer, so the replacement cannot be inactivity-reaped in
		// the gap. The stale step-2 subscription on the old deliver subject goes:
		// nothing will ever deliver on it, and restoration adopts the fresh one.
		inbox := mqttSubPrefix + nuid.Next()
		durName := idHash + "_" + nuid.Next()
		if err := subscribeClosed(e, inbox, durName); err != nil {
			undo()
			return nil, mqttReconcileRefuseLocal,
				fmt.Errorf("unable to install delivery interest for the replacement consumer of filter %q: %v", e.sid, err)
		}
		installed[inbox] = struct{}{}
		mintedInbox[inbox] = true
		if e.cc != nil {
			// The old durable is authoritatively ABSENT: it will never deliver
			// again, so a marker naming it could never be consumed.
			unsubscribePhase(e.cc.DeliverSubject, true)
			delete(installed, e.cc.DeliverSubject)
		}

		// (2) Retire the stale charge BEFORE the budget snapshot: a durable this
		// process created in an earlier connection keeps its consCharged entry
		// across rebind (only clear() resets it), so repairing without the refund
		// would double-charge tmaxack. A record-restored session was never
		// charged; the refund is then a no-op. The charge is permanently retired:
		// it named a consumer that no longer exists, so no failure path re-adds it.
		maxAckPending := baseMaxAckPending
		if rmax != 0 && int(rmax) < maxAckPending {
			maxAckPending = int(rmax)
		}
		sess.mu.Lock()
		if e.cc != nil {
			sess.refundConsumerChargeLocked(e.cc.Durable)
			// A delivery that ran out its gate wait before this durable was found
			// absent may have recorded a never-transmitted marker against it. That
			// consumer will never deliver again, so trackPublish can never consume
			// the marker and resendPendingRedelivery would NAK a dead ack subject
			// on every resume. Same reasoning as undo's purge of created durables.
			for key := range sess.deferred {
				if key.jsDur == e.cc.Durable {
					delete(sess.deferred, key)
				}
			}
		}
		if after := sess.tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			sess.mu.Unlock()
			undo()
			return nil, mqttReconcileRefuseLocal,
				fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v", after, mqttMaxAckTotalLimit)
		}
		sess.mu.Unlock()

		// (3) Create the replacement, DeliverNew under the fresh identity. No
		// backlog is guaranteed recoverable once the old consumer is gone: its
		// pending went with it, and no cursor proves what it was owed. The
		// pre-create seam is what lets a test pin subscribe-before-create as an
		// ordering rather than a race.
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{
				DeliverSubject: inbox,
				Durable:        durName,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  mqttStreamSubjectPrefix + e.subject,
				AckWait:        ackWait,
				MaxAckPending:  maxAckPending,
				MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			},
		}
		if opts.MQTT.ConsumerInactiveThreshold > 0 {
			ccr.Config.InactiveThreshold = opts.MQTT.ConsumerInactiveThreshold
		}
		as.mu.RLock()
		preCreate := as.testResumeReconcilePreCreate
		as.mu.RUnlock()
		var cerr error
		if preCreate != nil {
			cerr = preCreate(sess, inbox)
		}
		if cerr == nil {
			_, cerr = sess.jsa.createDurableConsumer(ccr)
		}
		if cerr != nil {
			undo()
			return nil, mqttReconcileRefuseLocal,
				fmt.Errorf("unable to recreate the QoS %d consumer for filter %q: %v", e.qos, e.sid, cerr)
		}
		newCC := &ccr.Config
		created = append(created, newCC)

		// (4) Charge the replacement and replace the session's identity for this
		// sid. The old entry's in-memory pending rows stay: client acks clear
		// them, and registers naming the old durable classify Lost with their
		// identifiers reserved — the pre-existing safe disposition.
		sess.mu.Lock()
		sess.chargeConsumerLocked(durName, maxAckPending)
		old, hadOld := sess.cons[e.sid]
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[e.sid] = newCC
		sess.mu.Unlock()
		replaced = append(replaced, replacedCons{sid: e.sid, old: old, hadOld: hadOld})

		if e.cc != nil {
			c.Warnf("MQTT: durable consumer %q for filter %q of session %q no longer exists; "+
				"recreated as %q (messages the lost consumer was owed cannot be recovered)",
				e.cc.Durable, e.sid, sess.id, durName)
		} else {
			c.Warnf("MQTT: session %q carries QoS %d filter %q with no consumer identity; minted %q",
				sess.id, e.qos, e.sid, durName)
		}
	}

	installedList := func() []string {
		l := make([]string, 0, len(installed))
		for deliver := range installed {
			l = append(l, deliver)
		}
		return l
	}

	if !repairedAny {
		return installedList(), mqttReconcileProceed, nil
	}

	// ---- Step 4: persist the repaired identity ------------------------------
	//
	// One record write, still pre-CONNACK: the FIRST write after the accept
	// save, by construction. The helper preserves the raw store error (the CAS
	// kind is destroyed by save()'s formatting) plus the exact base sequence and
	// body bytes sent, so the 10164 re-read compares against what was actually
	// stored.
	outcome, detail, serr := sess.saveClassifiedDetail(tok)
	if serr == nil {
		return installedList(), mqttReconcileProceed, nil
	}

	if outcome == saveMaybeLanded {
		// The record may already name the replacements. Which way memory
		// reconciles is the readback's to decide: not-landed runs the phase's
		// undo; landed keeps the replaced entries (the record names them, and the
		// next resume verifies them present — nothing further is owed).
		// Resolution is necessarily deferred: the accept transaction is still
		// open, so the readback runs from the deferred-close path, the periodic
		// uncertain pass, or the next CONNECT — and no traffic is served
		// meanwhile (no CONNACK, every phase subscription closed).
		sess.retainOnUncertain(func(recordTx) { undo() }, nil)
		releaseGates()
		return nil, mqttReconcileRefuseUnresolved, serr
	}

	// Definite. The CAS kinds are deliberately not equivalent: 10071 (the
	// single-writer check) means another broker wrote after our accept save —
	// the session was taken over mid-CONNECT; 10164 signals clustered
	// contention, not a lost race, and gets the bounded re-read/retry treatment.
	switch {
	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceErrF):
		releaseGates()
		sess.disposeForeignSessionLocked(as, c)
		return nil, mqttReconcileRefuseForeign, serr
	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceConstantErr):
		switch sess.resolveContendedRecordSave(&detail) {
		case mqttRepairSaveLanded:
			return installedList(), mqttReconcileProceed, nil
		case mqttRepairSaveForeign:
			releaseGates()
			sess.disposeForeignSessionLocked(as, c)
			return nil, mqttReconcileRefuseForeign, serr
		case mqttRepairSaveUnresolved:
			// The pending-write state (base + digest) is already established;
			// register the real continuations and refuse. NOT the generic ambiguous
			// handling: a CAS rejection is definite and save() deliberately did not
			// mark it uncertain, so without this the readback would classify
			// nothing.
			sess.retainOnUncertain(func(recordTx) { undo() }, nil)
			releaseGates()
			return nil, mqttReconcileRefuseUnresolved,
				fmt.Errorf("repair save contended and its resolution is unavailable: %v", serr)
		default: // mqttRepairSaveLocalDefinite
			undo()
			return nil, mqttReconcileRefuseLocal, serr
		}
	default:
		// A definite non-CAS failure (a refused fence, a refusal at entry, a test
		// seam): nothing was stored, sess.seq is right, undo and refuse.
		undo()
		return nil, mqttReconcileRefuseLocal, serr
	}
}

// mqttVerifyConsumersBatched resolves every durable to present/absent through
// lookup, at most bound per call. Any transport failure, short answer or
// unreadable entry is INDETERMINATE and fails the whole verification: "gone"
// must be authoritative before anything is recreated over it, and "present"
// must be authoritative before a session resumes trusting it.
func mqttVerifyConsumersBatched(durables []string, bound int,
	lookup func([]string) ([]*ConsumerInfo, []bool, error)) (map[string]bool, error) {

	if bound < 1 {
		bound = 1
	}
	present := make(map[string]bool, len(durables))
	for start := 0; start < len(durables); start += bound {
		end := min(start+bound, len(durables))
		batch := durables[start:end]
		infos, oks, err := lookup(batch)
		if err != nil {
			return nil, err
		}
		if len(infos) != len(batch) || len(oks) != len(batch) {
			return nil, fmt.Errorf("consumer lookup answered %d/%d of a batch of %d", len(infos), len(oks), len(batch))
		}
		for i, dur := range batch {
			if !oks[i] {
				return nil, fmt.Errorf("consumer %q could not be read", dur)
			}
			present[dur] = infos[i] != nil
		}
	}
	return present, nil
}

// resolveContendedRecordSave is the bounded re-read/retry for a record save
// rejected with 10164 (used by the repair save and by the lifetime restore): contention is a rejection, never a verdict, so the
// record is re-read and the attempt classified by its own identity — the base
// sequence it CAS'd against and the digest of the exact bytes it sent:
//
//	readSeq == base                  -> nothing moved; retry the SAME bytes
//	readSeq  > base && digest match  -> our write landed, response lost; adopt
//	anything else (incl. gone)       -> another owner is authoritative
//	the re-read/retry unclassifiable -> establish the pending-write state and
//	                                    let the readback machinery decide later
//
// The retry re-sends detail's exact framed bytes rather than re-marshalling,
// so the digest a later readback compares cannot diverge from what was stored.
//
// Every landed classification here CREDITS the void-register revision those bytes
// marshalled, because this is a durability proof that never passes through
// saveWithDetail's success arm or through the generic resolver: a 10164-rejected save
// whose exact-byte retry succeeds, or whose digest is found at a newer sequence, is
// durable and its verdicts are durable with it. Left uncredited, a quiet bound
// connection could park the reservation indefinitely. The unresolved arms hand the same
// revision to pendingWrite so the deferred readback credits it instead. See
// creditVoidedRegsLocked.
//
// No lock held on entry. Performs JetStream requests.
func (sess *mqttSession) resolveContendedRecordSave(d *mqttSessSaveDetail) mqttRepairSaveVerdict {
	want := mqttRecordDigest(d.body)
	for attempt := 0; attempt < mqttRepairSaveAttempts; attempt++ {
		smsg, lerr := sess.jsa.loadSessionMsg(d.domainTk, d.cidHash)
		switch classifyRecordAgainstAttempt(smsg, lerr, d.baseSeq) {
		case mqttRecordAttemptUnreadable:
			// The re-read itself is unavailable. A CAS rejection is definite, so
			// save() deliberately did not mark this uncertain — the phase
			// establishes the pending-write state itself, carrying the base and
			// digest, so the deferred readback classifies THIS attempt.
			sess.mu.Lock()
			sess.markWriteUncertainLocked(d.baseSeq, d.body, d.voidedRegsRev)
			sess.mu.Unlock()
			return mqttRepairSaveUnresolved
		case mqttRecordAttemptGone:
			// The record is gone: somebody released this session. Not ours.
			return mqttRepairSaveForeign
		case mqttRecordAttemptAtBase:
			// Still exactly the record we CAS'd against: the rejection was
			// transient contention. Retry the identical write.
			resp, rerr := sess.retryRepairStore(d)
			if rerr == nil {
				sess.mu.Lock()
				sess.seq = resp.Sequence
				sess.creditVoidedRegsLocked(d.voidedRegsRev)
				sess.mu.Unlock()
				return mqttRepairSaveLanded
			}
			if mqttIsRecordCASRejection(rerr) {
				// Rejected again — contention, or a write that just landed ahead of
				// us. The next re-read classifies which.
				continue
			}
			// The retry's outcome is unknown (a timeout, a lost response). Same
			// pending-write discipline as the unreadable re-read above.
			sess.mu.Lock()
			sess.markWriteUncertainLocked(d.baseSeq, d.body, d.voidedRegsRev)
			sess.mu.Unlock()
			return mqttRepairSaveUnresolved
		default: // mqttRecordAttemptMoved
			if smsg.Sequence > d.baseSeq && mqttRecordDigest(smsg.Data) == want {
				// Byte-identical at a newer sequence: our write landed and only its
				// response was lost to the contention.
				sess.mu.Lock()
				sess.seq = smsg.Sequence
				sess.creditVoidedRegsLocked(d.voidedRegsRev)
				sess.mu.Unlock()
				return mqttRepairSaveLanded
			}
			// A newer, different record (or one below our base). Another owner is
			// authoritative; adopting its sequence would turn our stale snapshot into a
			// valid CAS over it.
			return mqttRepairSaveForeign
		}
	}

	// Retries exhausted. The last rejection was never classified — a CAS
	// rejection says the subject moved, not who moved it — so read once more
	// rather than assuming the contention was ours to lose: a 10071 on the final
	// attempt means another broker took the session over, and returning
	// local-definite there would undo our work while leaving a session we no
	// longer own mapped and bound.
	smsg, lerr := sess.jsa.loadSessionMsg(d.domainTk, d.cidHash)
	switch classifyRecordAgainstAttempt(smsg, lerr, d.baseSeq) {
	case mqttRecordAttemptUnreadable:
		sess.mu.Lock()
		sess.markWriteUncertainLocked(d.baseSeq, d.body, d.voidedRegsRev)
		sess.mu.Unlock()
		return mqttRepairSaveUnresolved
	case mqttRecordAttemptGone:
		return mqttRepairSaveForeign // the record is gone: somebody released it
	case mqttRecordAttemptAtBase:
		return mqttRepairSaveLocalDefinite // still ours, still unwritten
	default: // mqttRecordAttemptMoved
		if smsg.Sequence > d.baseSeq && mqttRecordDigest(smsg.Data) == want {
			sess.mu.Lock()
			sess.seq = smsg.Sequence
			sess.creditVoidedRegsLocked(d.voidedRegsRev)
			sess.mu.Unlock()
			return mqttRepairSaveLanded
		}
		return mqttRepairSaveForeign
	}
}

// retryRepairStore re-issues a repair save's EXACT bytes — same CAS base, same
// body — through the same store boundary (the suppress and result seams
// included, so a test observes the retry exactly as it observed the original).
// The caller's accept transaction is still open on this goroutine, which is
// what makes the raw re-send safe without re-validating the fence: nothing can
// have closed a transaction this goroutine holds.
func (sess *mqttSession) retryRepairStore(d *mqttSessSaveDetail) (*JSPubAckResponse, error) {
	resp, err := sess.jsa.storeSessionMsg(d.domainTk, d.cidHash, d.hdr, d.framed)
	if h := mqttTestSessStoreResultHookV.Load(); h != nil {
		resp, err = (*h)(sess, resp, err)
	}
	return resp, err
}

// sweepUnadoptedPhaseSubs removes every reconciliation-phase subscription that
// subscription restoration did not adopt. Restoration can legitimately skip a
// collected filter after the CONNACK — a PUBREL setup failure fails a QoS 2
// filter, a raw-subscription failure continues without reaching
// processJSConsumer — and the connection stays live, so nothing else would ever
// remove that subscription: it would sit closed forever, holding consumer
// interest, and a later re-SUBSCRIBE's metadata refresh does not clear
// `closed`. Such a filter stays unavailable until the next CONNECT (stated
// residual: live re-SUBSCRIBE is not a recovery mechanism).
//
// Idempotent, and identity-current: only a subscription still carrying the
// phase-owned marker is removed, so an adopted (or already-swept) one is left
// alone. Runs both as the always-on pass after processSubs and as the deferred
// fallback for exits that never reach it.
//
// No lock held on entry.
func (sess *mqttSession) sweepUnadoptedPhaseSubs(c *client, delivers []string) {
	for _, deliver := range delivers {
		c.mu.Lock()
		sub := c.subs[deliver]
		c.mu.Unlock()
		if sub == nil {
			continue
		}
		sess.mu.Lock()
		sess.subsMu.Lock()
		owned := sub.mqtt != nil && sub.mqtt.phaseOwned
		sess.subsMu.Unlock()
		sess.mu.Unlock()
		if !owned {
			continue
		}
		c.Warnf("MQTT: subscription restoration did not adopt the verified delivery subscription on %q; "+
			"removing its interest — the filter is unavailable until the next CONNECT", deliver)
		// The gate opens, but the subscription is NOT disowned: this consumer
		// SURVIVES — only its interest is being withdrawn — so a delivery parked
		// on the gate is still owed to the session and must record itself as
		// never transmitted, or the redelivery that follows the next CONNECT
		// reaches the client marked DUP on first sight. Only a durable that is
		// being deleted is disowned (see unsubscribePhase's doomed argument).
		sess.mqttReleasePhaseGate(sub)
		if uerr := c.processUnsub([]byte(deliver)); uerr != nil {
			c.Warnf("MQTT: unable to remove the unadopted delivery subscription on %q: %v", deliver, uerr)
		}
	}
}

// grantRequestedLifetime applies this CONNECT's Session Expiry Interval and
// Clean flag to a RESUMED session and persists them, as one step, at the moment
// the resume is granted.
//
// The two are deliberately not applied at rebind: the accept save would carry
// them into the record while the CONNECT could still be refused, and a record
// describing terms the client was never told it has is not harmless — with a
// requested interval of 0 the close path would discard a persistent session
// outright. Applying them here means a refused resume changes nothing and has
// nothing to repair.
//
// Nothing is written when the terms did not change, which is the ordinary
// reconnect.
//
// The write owes the same outcome discipline as every other record write in
// this broker, and for the same reasons (see sess_uncertain.go):
//
//   - AMBIGUOUS: memory goes back to the CONFIRMED terms and the grant is
//     re-applied only if the readback proves the write landed. Keeping the
//     speculative terms instead would be actively dangerous: the CONNECT is
//     refused, the connection closes while the session is still unresolved, and
//     mqttDeferDisconnect SNAPSHOTS the lifetime into the deferred action — so a
//     requested interval of 0 that never landed would still discard a persistent
//     session, because applyDeferredDisconnect acts on that snapshot rather than
//     on whatever the readback later restores. Preferring the confirmed terms
//     costs at most one disconnect honoring the older lifetime; the alternative
//     costs the session.
//   - 10071: another broker wrote after our accept save and owns this session.
//     Restoring or re-writing anything would be a stale write, so it is handed
//     over instead (DISCONNECT suppressed: no CONNACK was written).
//   - 10164: clustered contention is a rejection, not a verdict, so the bounded
//     same-base/same-body re-read decides.
//   - Definitely failed: nothing was stored, so memory goes back and the record
//     still describes the lifetime the session had.
//
// Runs inside the CONNECT's accept transaction, whose token it must be given.
// No lock held on entry.
func (sess *mqttSession) grantRequestedLifetime(asm *mqttAccountSessionManager, c *client,
	tok recordTx, expiry uint32, clean bool) error {

	sess.mu.Lock()
	wasExpiry, wasClean := sess.expiryInterval, sess.clean
	if wasExpiry == expiry && wasClean == clean {
		sess.mu.Unlock()
		return nil
	}
	sess.expiryInterval, sess.clean = expiry, clean
	// Whatever teardown was already parked when this grant was attempted, so the
	// landed continuation can tell it apart from one THIS connection's close
	// parks. A stale action from an earlier connection is reachable and is not
	// ours to rewrite. See retargetDeferredLifetime.
	priorDeferred := sess.deferredDisconnect
	sess.mu.Unlock()

	restore := func() {
		sess.mu.Lock()
		sess.expiryInterval, sess.clean = wasExpiry, wasClean
		sess.mu.Unlock()
	}
	// Re-applied only on proof that the write landed. Until then the session runs
	// on its confirmed terms, which is what any teardown decision taken while it
	// is unresolved will snapshot.
	apply := func(recordTx) {
		sess.mu.Lock()
		sess.expiryInterval, sess.clean = expiry, clean
		sess.mu.Unlock()
		// A close that happened while this write was unresolved parked its
		// teardown against the confirmed terms. Now that the grant is proven
		// durable, that snapshot is the stale half — and it, not the session, is
		// what decides whether the session is discarded and what timer is armed.
		sess.retargetDeferredLifetime(priorDeferred, expiry)
	}

	outcome, detail, err := sess.saveClassifiedDetail(tok)
	switch {
	case err == nil:
		return nil
	case outcome == saveMaybeLanded:
		restore()
		sess.retainOnUncertain(nil, apply)
	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceErrF):
		sess.disposeForeignSessionLocked(asm, c)
	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceConstantErr):
		switch sess.resolveContendedRecordSave(&detail) {
		case mqttRepairSaveLanded:
			return nil
		case mqttRepairSaveUnresolved:
			restore()
			sess.retainOnUncertain(nil, apply)
		case mqttRepairSaveForeign:
			sess.disposeForeignSessionLocked(asm, c)
		default:
			restore()
		}
	default:
		restore()
	}
	return err
}
