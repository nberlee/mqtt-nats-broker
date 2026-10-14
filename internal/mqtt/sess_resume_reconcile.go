// Plain QoS 1/2 consumer reconciliation on session resume.
//
// The session record persists each plain QoS 1/2 subscription's JetStream
// consumer config (ps.Cons) and the CONNECT restore path used to trust it
// verbatim: a durable reaped by an inactivity threshold — a stream-forced
// ConsumerLimits.InactiveThreshold, which reaches consumers requesting 0, or
// one an administrator armed on the consumer — was silently restored, and the client got Session Present = 1 with a subscription that
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
//	          consumer later in the resume and never persist it);
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
//	          this phase runs BEFORE processSubs, so no general
//	          subscription-processing write can observe a half-committed
//	          replacement. Note what that does NOT say — processSubs itself is
//	          also pre-CONNACK (F-050 moved restoration there), so a later
//	          pre-CONNACK write can and does observe state it provisioned.
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

	"github.com/nats-io/nuid"
)

// mqttPreConnackOutcome is how a pre-CONNACK step that can refuse the CONNECT
// ended. ONE partition for the reconciliation phase and the lifetime grant,
// because they classify the same record write the same way.
//
// Anything but "proceed" refuses the CONNECT through the pre-CONNACK abort
// discipline, and the two DISPOSING outcomes additionally carry a disposal
// vocabulary. That vocabulary is what the DISPOSAL claims; what the CLIENT is
// told is preConnackRefusalFor's projection, which is deliberately a different
// thing — see mqttPreConnackRefusal in mqtt.go.
type mqttPreConnackOutcome int

const (
	// mqttPreConnackProceed: every collected durable is verified or repaired, and
	// any repair is durably named by the record. The CONNACK may be sent.
	mqttPreConnackProceed mqttPreConnackOutcome = iota
	// mqttPreConnackLocal: the step failed definitively and undid its own work.
	// The resident session is still ours, stays mapped, and detaches through
	// ordinary close bookkeeping under the lifetime it already had — the CONNECT's
	// requested Session Expiry Interval is applied only at the grant point, so a
	// refused resume leaves nothing to repair. The client reconnects and resumes.
	//
	// Reached without a save at all on five of the reconciliation phase's returns
	// (an indeterminate verification, a failed or refused create, the tmaxack
	// limit), which is why this partition is named for the STEP rather than for a
	// save: "nothing was stored" holds trivially when nothing was attempted.
	mqttPreConnackLocal
	// mqttPreConnackDisposingNonForked: the record CAS was rejected and nothing
	// narrowed it to a sibling. Deliberately NOT called "foreign": that arm is
	// also reached when the record could not be read, was absent, did not decode,
	// was released, or sat at or below our base, none of which establishes another
	// owner. The disposal's own claim is unchanged and still comes from the
	// vocabulary; the CLIENT is told only that ownership could not be confirmed.
	// See F-084 for the claim the vocabulary itself still over-states.
	mqttPreConnackDisposingNonForked
	// mqttPreConnackDisposingForked: the authoritative body has a non-empty,
	// distinct identity and claims the same parent as our acknowledged record.
	// That is a positive fact and 0x89 "try again" states exactly what it
	// licenses — not that a fork was proved, since provenance is not validated.
	// See sess_fork_lineage.go.
	mqttPreConnackDisposingForked
	// mqttPreConnackUnresolved: the record write's outcome is unknown. The
	// pending-write state carries the attempt's identity and its exact bytes, the
	// replaced entries are KEPT (the record may name them, and a resolution that
	// re-attempts those bytes makes it so), and resolution is deferred: the accept
	// transaction is still open, so it runs from the deferred-close path, the
	// periodic uncertain pass, or the next CONNECT. No traffic is served meanwhile
	// — no SUCCESS CONNACK was sent (the refusal carries 0x83 and this step's own
	// diagnostic, which opens nothing) and every phase subscription is closed.
	mqttPreConnackUnresolved
)

// disposes reports whether this outcome hands the session to the foreign
// disposition. Only these two carry a vocabulary.
func (o mqttPreConnackOutcome) disposes() bool {
	return o == mqttPreConnackDisposingNonForked || o == mqttPreConnackDisposingForked
}

// mqttResumeReconcileResult is how the reconciliation phase ended, and it keeps
// the two facts APART on purpose: outcome decides what the client is told, voc
// decides what the disposal claims, and neither is derived from the other.
//
// installed is non-nil only on Proceed; voc is the zero vocabulary unless the
// outcome disposes.
type mqttResumeReconcileResult struct {
	installed []string
	outcome   mqttPreConnackOutcome
	voc       mqttDisposalVocabulary
	err       error
}

// mqttLifetimeGrantResult is the same shape for the lifetime grant, which used to
// return a bare error and dispose on the way out — so its caller could neither
// order the refusal signal before the close it scheduled, nor tell a fork from
// anything else.
type mqttLifetimeGrantResult struct {
	outcome mqttPreConnackOutcome
	voc     mqttDisposalVocabulary
	err     error
}

// mqttClassifiedPreConnackSave is what classifyGatedRecordSave found: the outcome,
// the vocabulary the route that reached it computed, and whether that route was
// the CONTENDED one, which is the only thing that distinguishes the two
// unresolved arms from each other.
type mqttClassifiedPreConnackSave struct {
	outcome   mqttPreConnackOutcome
	voc       mqttDisposalVocabulary
	err       error
	contended bool
}

// mqttReconcileEntry is one plain QoS 1/2 delivery identity the phase owes a
// verdict: a sid (the filter itself, or its level-up companion), the subject
// its consumer filters, and the subscription options its delivery callback
// reads. cc is the recorded consumer config; nil means the record carries the
// subscription but no consumer identity at all (absent identity), which is
// repaired exactly like a reaped durable, minus the stale pieces.
type mqttReconcileEntry struct {
	sid     string
	subject string
	// filterKey is the session key of the filter, for BOTH the main entry and its
	// level-up companion, whose sid and subject are the shortened forms: the No Local
	// stamp names the key (see mqttSub.filterKey).
	filterKey string
	qos       byte
	noLocal   bool
	rap       bool
	subID     int
	reserved  bool
	cc        *ConsumerConfig
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
	// mqttRepairSaveForeign: the record moved to content that is not our write, or is
	// gone. Deliberately NOT "another owner is authoritative": that is the inference
	// F-084 rejects, and this arm is also reached by a gone, unreadable, undecodable or
	// below-base record, none of which establishes a successor. What IS established is
	// that our write did not land and this CONNECT cannot proceed.
	mqttRepairSaveForeign
	// mqttRepairSaveForked: the record moved to content provably CLAIMING the same parent, a sibling of
	// the one this broker was acknowledged for. Every consequence of foreign holds —
	// the write did not land, the sequence must not be adopted, the caller refuses —
	// and nothing is credited, because nothing landed. What differs is only what the
	// caller's disposition CLAIMS. See sess_fork_lineage.go.
	mqttRepairSaveForked
	// mqttRepairSaveLocalDefinite: retries exhausted with the base sequence still
	// unchanged. Nothing was stored; undo and refuse.
	mqttRepairSaveLocalDefinite
	// mqttRepairSaveUnresolved: a retry's outcome (or the re-read itself) is
	// unknowable right now. The pending-write state has been established with the
	// attempt's base, digest and exact bytes, so the resolver can settle it; the
	// caller keeps its mutation and registers whatever it deferred for want of
	// proof.
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
	// and then vanished on its own, so SOMETHING released this session — a teardown, an
	// expiry, an operator. Which, and whether anyone succeeded us, is exactly what an
	// absence cannot say (F-084).
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
	// this attempt's. That is ALL it establishes — the subject moved past our base, and
	// nothing about who moved it (F-084).
	mqttRejectionForeign
	// mqttRejectionUnresolved: the probe's own outcome is unknown. The uncertainty
	// machinery owns the session until a readback classifies it.
	mqttRejectionUnresolved
	// mqttRejectionForked: the probe definitively did not land, and the record that IS
	// there is provably a body CLAIMING the parent of the one this broker was acknowledged for. The
	// session is equally not ours to write, so the session is disposed exactly as a
	// foreign verdict disposes it — but no takeover was demonstrated, so none is
	// claimed and the client is told 0x89 rather than 0x8E. See sess_fork_lineage.go.
	mqttRejectionForked
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
func classifyRejectedProbe(outcome saveOutcome, detail mqttSessSaveDetail, smsg *StoredMsg, lerr error,
	ackWID, ackPWID string) mqttRejectionVerdict {

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
		// An ABSENT record carries no identity, so there is nothing to compare and the
		// conclusion stands as it is.
		return mqttRejectionForeign
	case mqttRecordAttemptMoved:
		// The probe definitively did not land, so an accepted transition at another
		// sequence is not this attempt's — and it cannot be ours by another route: a
		// second local writer needs the fence this probe holds, an earlier ambiguous
		// write of ours would have armed seqUncertain (which the marker's predicate
		// excludes), a replacement object on this broker is excluded by the
		// identity-current ownership gate, and the expiry sweep's raw CAS skips a
		// client whose session is resident.
		//
		// That reasoning establishes the record is not OURS. It does not establish
		// that it SUCCEEDS ours — and since F-084 the foreign verdict no longer claims
		// that either, which is why this test decides the MINT rather than the claim:
		// an UNCONDITIONAL foreign write can erase us off our own base
		// without succeeding us, and no CAS excludes one (the same-base boundary,
		// docs/CONFORMANCE.md). Asked here, on the branch that already concluded
		// foreign, and only above the base — a record below it stays foreign fail-safe,
		// because a lower sequence means a reset or re-provisioned store.
		if smsg.Sequence > detail.baseSeq &&
			classifyRecordLineage(mqttDecodeSessionRecord(smsg), ackWID, ackPWID) == mqttLineageForked {
			return mqttRejectionForked
		}
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
// On mqttPreConnackProceed it returns the deliver subjects of every
// still-phase-owned subscription, for the post-processSubs sweep (and its
// deferred fallback) to reclaim if restoration skips a filter.
//
// It REPORTS its refusal rather than acting on the disposing half of it: the
// caller aborts the CONNECT, signals the client, and only then hands the session
// to the foreign disposition. Disposing here instead would schedule
// closeConnection on another goroutine before the caller could enqueue a
// CONNACK, which is what kept this site silent (docs/ROADMAP.md's
// pre-CONNACK-signal item). Everything else the verdict names — the phase's own
// undo, its gate releases — has already happened on return.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) reconcileResumedConsumers(sess *mqttSession, c *client, tok recordTx) mqttResumeReconcileResult {
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
	// Plain filters only: shared subscriptions live in sharedSubs, and their group
	// durables are verified by their own machinery — the two batched passes the CONNECT
	// restore runs around processSubs (shared_group_verify.go, F-076). That comment used
	// to name a mechanism that did not exist, which is the gap F-076 closed.
	//
	// Drained sids are not in cons (stageDrain removed them). Their filters are absent
	// from subs when an UNSUBSCRIBE drained them, and present AT QoS 0 when a downgrade
	// did — which this collection skips either way, since it takes positive QoS only. A
	// re-SUBSCRIBE at QoS > 0 that adopted the drain puts the entry back in cons, and it
	// is verified here like any other. A stale Cons entry with no matching
	// subscription is not an active delivery entry and is deliberately not
	// verified (stated residual, docs/CONFORMANCE.md).
	var entries []*mqttReconcileEntry
	sess.mu.Lock()
	idHash := sess.idHash
	rmax := sess.rmax
	for subject, qos := range sess.subs {
		if qos == 0 {
			continue
		}
		reserved := isMQTTReservedSubscription(subject)
		_, noLocal := sess.noLocalSubs[subject]
		_, rap := sess.rapSubs[subject]
		subID := sess.subIDs[subject]
		entries = append(entries, &mqttReconcileEntry{
			sid: subject, subject: subject, filterKey: subject, qos: qos,
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
				sid: fwcsid, subject: subject[:len(subject)-2], filterKey: subject, qos: qos,
				noLocal: noLocal, rap: rap, subID: subID, reserved: reserved,
				cc: sess.cons[fwcsid],
			})
		}
	}
	sess.mu.Unlock()
	if len(entries) == 0 {
		return mqttResumeReconcileResult{outcome: mqttPreConnackProceed}
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
	// createdClaims names the claim behind each created durable: the undo disposes of exactly
	// that incarnation, under the entitlement it was claimed with (F-105 7c-lc).
	createdClaims := map[string]*mqttSourceClaim{}
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
	// Each created durable is refunded exactly once and its claimed incarnation disposed of
	// (F-105 7c-lc: the retirement owner deletes it, never this undo). Safe after the
	// connection has closed too (the uncertain continuation runs it from a
	// resolver): processUnsub on a closed client is a no-op.
	undo := func() {
		for deliver := range installed {
			unsubscribePhase(deliver, mintedInbox[deliver])
		}
		// A CREATED durable is not deleted raw: its claimed incarnation is disposed of and the
		// retirement owner retires it, its name with it (F-105 7c-lc).
		for _, cc := range created {
			sess.mu.Lock()
			sess.refundConsumerChargeLocked(cc.Durable)
			sess.mu.Unlock()
			if cl := createdClaims[cc.Durable]; cl != nil {
				sess.handSourceDisposal(cl.gen, cl.name, cl.inc, mqttDisposalRepairUndone, cl.ent, "repair-undo")
			}
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
		sub, err := sess.processQOS12Sub(c, []byte(deliver), []byte(deliver), nil, e.filterKey,
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
			return mqttResumeReconcileResult{outcome: mqttPreConnackLocal,
				err: fmt.Errorf("unable to install delivery interest for durable %q of filter %q: %v", e.cc.Durable, e.sid, err)}
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
		return mqttResumeReconcileResult{outcome: mqttPreConnackLocal,
			err: fmt.Errorf("unable to verify the session's QoS 1/2 consumers: %v", err)}
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
			return mqttResumeReconcileResult{outcome: mqttPreConnackLocal,
				err: fmt.Errorf("unable to install delivery interest for the replacement consumer of filter %q: %v", e.sid, err)}
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
			return mqttResumeReconcileResult{outcome: mqttPreConnackLocal,
				err: fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v", after, mqttMaxAckTotalLimit)}
		}
		sess.mu.Unlock()

		// CLAIM THE FRESH NAME BEFORE THE CREATE (F-105 §5.1, Codex consult 11/12). A repaired
		// source consumer is minted under a fresh physical name, so it is an ENROLLED resource
		// exactly like a first-subscribe source: admission validates LIVE ownership owned by the
		// serving generation, so a repaired durable created without its ownership record would be
		// un-admittable and the repaired session could never serve QoS 1/2. Claim the name's LIVE
		// ownership, its resource incarnation and its inventory duties as ONE authority batch, THEN
		// create. See outbound_ops.go opProvisionResource.
		//
		// UNDER THIS CONNECTION'S ENTITLEMENT, never under whatever authority stands when the claim
		// reads it (F-105 7f): a claim a takeover's successor would own is not this connection's to
		// make, and opProvisionResource refuses an entitlement that moved.
		repairEnt, entOK := c.outboundEntitlement()
		if !entOK {
			undo()
			return mqttResumeReconcileResult{outcome: mqttPreConnackLocal,
				err: fmt.Errorf("this connection holds no accepted entitlement, so the replacement consumer "+
					"name %q for filter %q cannot be claimed", durName, e.sid)}
		}
		repairInc := mqttNewInflightNonce()
		repairClaim := &mqttSourceClaim{gen: repairEnt.Gen, name: durName, inc: repairInc, ent: repairEnt}
		if out, perr := sess.jsa.opProvisionResource(sess.id, sess.asm.sessStreamEpoch,
			mqttNewInflightNonce(), "op-provision-source", repairEnt.Gen, repairEnt.Tenure,
			repairInc, durName, mqttResourceSourceConsumer); perr != nil {
			// The claim may have landed: its handle goes to arbitration (F-105 7c-lc).
			if out.Handle != nil {
				repairClaim.handles = []*mqttTransitionHandle{out.Handle}
			}
			sess.custodyFailedClaim(repairClaim)
			undo()
			return mqttResumeReconcileResult{outcome: mqttPreConnackLocal,
				err: fmt.Errorf("claiming replacement consumer name %q for filter %q: %v", durName, e.sid, perr)}
		}

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
		// SAFE INACTIVITY CONFIG FROM THE OUTSET (F-105 §5.1 step 3): the repaired source is
		// enrolled, so it is created with NO inactivity threshold — its lifetime is its resource's.
		// The definitive effective-config verify is at admission.
		mqttClearConsumerInactivity(&ccr.Config)
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
			// The claim landed and the create may yet: disposed of BEFORE any bookkeeping, so the
			// created list's ordering never decides it (F-105 7c-lc, P-1 f2).
			sess.handSourceDisposal(repairClaim.gen, repairClaim.name, repairClaim.inc,
				mqttDisposalRepairUndone, repairClaim.ent, "repair-create-failed")
			undo()
			return mqttResumeReconcileResult{outcome: mqttPreConnackLocal,
				err: fmt.Errorf("unable to recreate the QoS %d consumer for filter %q: %v", e.qos, e.sid, cerr)}
		}
		newCC := &ccr.Config
		created = append(created, newCC)
		createdClaims[newCC.Durable] = repairClaim

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
		return mqttResumeReconcileResult{installed: installedList(), outcome: mqttPreConnackProceed}
	}

	// ---- Step 4: persist the repaired identity ------------------------------
	//
	// One record write, still pre-CONNACK: the FIRST write after the accept
	// save, by construction. The helper preserves the raw store error (the CAS
	// kind is destroyed by save()'s formatting) plus the exact base sequence and
	// body bytes sent, so the 10164 re-read compares against what was actually
	// stored.
	cs := sess.classifyGatedRecordSave(tok)
	switch cs.outcome {
	case mqttPreConnackProceed:
		// THE REPAIR DECIDED ITS PREDECESSORS (F-105 7f). Once the confirmed save names the
		// replacement, the durable it replaced is no subscription's again: the decision is this
		// connection's, recorded under its entitlement like every route that ends a consumer, and
		// handed to the retirement owner. An unresolved save decides nothing here; whichever record
		// lands, the next tenure's settlement finds an unnamed predecessor.
		if ent, ok := c.outboundEntitlement(); ok {
			for _, r := range replaced {
				if r.hadOld && r.old != nil && r.old.Durable != _EMPTY_ {
					sess.handSourceDisposal(ent.Gen, r.old.Durable, _EMPTY_, mqttDisposalRepairReplaced,
						ent, "repair-replaced")
				}
			}
		}
		return mqttResumeReconcileResult{installed: installedList(), outcome: mqttPreConnackProceed}
	case mqttPreConnackUnresolved:
		// The record may already name the replacements, so the replaced entries are
		// KEPT — and nothing further is owed when the resolver proves them durable:
		// the record names them and the next resume verifies them present, so there
		// is no landed continuation to register. Resolution is necessarily deferred:
		// the accept transaction is still open, so it runs from the deferred-close
		// path, the periodic uncertain pass, or the next CONNECT — and no traffic is
		// served meanwhile: no SUCCESS CONNACK is written (the caller's refusal carries
		// 0x83 and this step's own diagnostic, which opens nothing) and every phase
		// subscription stays closed.
		releaseGates()
		err := cs.err
		if cs.contended {
			// The 10164 route says so, because a rejection whose resolution is
			// unavailable is a different report from a store whose response was lost.
			err = fmt.Errorf("repair save contended and its resolution is unavailable: %v", cs.err)
		}
		return mqttResumeReconcileResult{outcome: mqttPreConnackUnresolved, err: err}
	case mqttPreConnackDisposingNonForked, mqttPreConnackDisposingForked:
		// The disposal is the CALLER'S, not this phase's: it schedules
		// closeConnection on another goroutine, so a CONNACK enqueued after it has
		// lost a race with its own socket close. The phase releases its gates and
		// reports; mqttProcessConnect aborts, signals, then disposes.
		releaseGates()
		return mqttResumeReconcileResult{outcome: cs.outcome, voc: cs.voc, err: cs.err}
	default: // mqttPreConnackLocal
		// Nothing was stored and sess.seq is right: undo and refuse.
		undo()
		return mqttResumeReconcileResult{outcome: mqttPreConnackLocal, err: cs.err}
	}
}

// classifyGatedRecordSave classifies a CAS-arbitrated record write made inside a
// pre-CONNACK step's still-open transaction. It is the ONE classification the
// reconciliation phase's repair save and the lifetime grant share: both split
// 10071 from 10164, both narrow the first through a single record read, and both
// hand the second to the bounded contention resolver. Written twice, a new CAS
// kind would have to be found twice.
//
// It returns the vocabulary ITS OWN ROUTE computed, never one a caller
// re-derives: the two routes reach it differently — disposalForDirectRejection
// reads the record for the 10071 arm, mqttDisposalForRepairSave maps the
// resolver's verdict for the 10164 arm — and the 10071 route MINTS THE FORK
// GRANT, which must happen inside the caller's transaction. A caller re-deriving
// would need a second read and would have to know which route ran.
//
// contended reports the 10164 route, which is the only difference between the two
// unresolved arms: saveMaybeLanded surfaces the store error bare, while a
// contention whose resolution is unavailable says so.
//
// proveSharedMembershipOwnership deliberately keeps its own copy of this shape.
// Unifying that third consumer is what let an unclassifiable rejection reach
// F-078's live-SUBSCRIBE Foreign arm and change WILL DISPOSITION, which is a
// different question from this one. See F-084.
//
// tok must be the CALLER'S OWN open transaction. No lock held on entry; performs
// JetStream requests.
func (sess *mqttSession) classifyGatedRecordSave(tok recordTx) mqttClassifiedPreConnackSave {
	outcome, detail, serr := sess.saveClassifiedDetail(tok)
	if serr == nil {
		return mqttClassifiedPreConnackSave{outcome: mqttPreConnackProceed}
	}
	if outcome == saveMaybeLanded {
		return mqttClassifiedPreConnackSave{outcome: mqttPreConnackUnresolved, err: serr}
	}

	// Definite. The CAS kinds are deliberately not equivalent: 10071 (the
	// single-writer check) means THE SUBJECT MOVED after our accept save — not that
	// another broker owns the session, which is a stronger claim the readback below
	// often cannot support (F-084); 10164 signals clustered contention, not a lost
	// race, and gets the bounded re-read/retry treatment.
	switch {
	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceErrF):
		// 10071 disposes without ever consulting the resolver, so this is the only
		// place a sibling lineage is recognised on that arm: one record read, which
		// narrows the vocabulary and mints the fork grant when the body claims our
		// parent. Collapsing it would assert a fork's 0x89 where the record supports
		// only the unsettled one, and mint a grant with nothing to anchor it.
		//
		// The outcome is keyed on the fork BIT the classification returned, never on the
		// vocabulary's reason code: both 0x89 vocabularies share that code, so a code
		// comparison would route an unsettled record to the forked outcome. F-084.
		voc, forked := sess.disposalForDirectRejection(&detail)
		out := mqttPreConnackDisposingNonForked
		if forked {
			out = mqttPreConnackDisposingForked
		}
		return mqttClassifiedPreConnackSave{outcome: out, voc: voc, err: serr}
	case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceConstantErr):
		switch v := sess.resolveContendedRecordSave(&detail); v {
		case mqttRepairSaveLanded:
			return mqttClassifiedPreConnackSave{outcome: mqttPreConnackProceed}
		case mqttRepairSaveForeign, mqttRepairSaveForked:
			// One arm: a fork is refused exactly as a foreign record is — the write did
			// not land and this CONNECT cannot proceed — and differs only in what the
			// disposition claims and what the client is told.
			out := mqttPreConnackDisposingNonForked
			if v == mqttRepairSaveForked {
				out = mqttPreConnackDisposingForked
			}
			return mqttClassifiedPreConnackSave{
				outcome: out, voc: mqttDisposalForLineage(v == mqttRepairSaveForked), err: serr, contended: true}
		case mqttRepairSaveUnresolved:
			// The pending-write state (base, digest and the exact bytes) is already
			// established by resolveContendedRecordSave — deliberately, because a CAS
			// rejection is definite and save() did not mark it uncertain, so without
			// that the resolver would have nothing to classify or re-attempt.
			return mqttClassifiedPreConnackSave{
				outcome: mqttPreConnackUnresolved, err: serr, contended: true}
		default: // mqttRepairSaveLocalDefinite
			return mqttClassifiedPreConnackSave{
				outcome: mqttPreConnackLocal, err: serr, contended: true}
		}
	default:
		// A definite non-CAS failure: a refused fence, a refusal at entry, a test
		// seam. Nothing was stored.
		return mqttClassifiedPreConnackSave{outcome: mqttPreConnackLocal, err: serr}
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
// rejected with 10164. FOUR callers, not two: the pre-CONNACK classification the repair
// save and the lifetime grant share (classifyGatedRecordSave), the shared-membership
// ownership write, and the 3.1.1 forced leave. Contention is a rejection, never a verdict, so the
// record is re-read and the attempt classified by its own identity — the base
// sequence it CAS'd against and the digest of the exact bytes it sent:
//
//	readSeq == base                  -> nothing moved; retry the SAME bytes
//	readSeq  > base && digest match  -> our write landed, response lost; adopt
//	anything else (incl. gone)       -> not ours; refuse (NOT "another owner", F-084)
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
	// The pair this broker was acknowledged for, sampled once: the loop's landed arms
	// return immediately, so no iteration can observe a pair this call itself moved.
	sess.mu.Lock()
	ackWID, ackPWID := sess.wid, sess.pwid
	sess.mu.Unlock()
	movedVerdict := func(smsg *StoredMsg) mqttRepairSaveVerdict {
		// Reached only after the digest comparison above has ruled the record out as
		// ours, and gated on the same `> base` the landed arm uses — a record below
		// the base stays foreign fail-safe whatever its identity says.
		if smsg != nil && smsg.Sequence > d.baseSeq &&
			classifyRecordLineage(mqttDecodeSessionRecord(smsg), ackWID, ackPWID) == mqttLineageForked {
			// One mint for both of this function's moved arms, in the closure that reaches
			// the verdict — the reconcile-contended and lifetime-contended callers share it,
			// so the record and the pair are sampled together whichever one asked.
			// See sess_fork_repair.go.
			sess.mintForkedGrant(ackWID, ackPWID)
			return mqttRepairSaveForked
		}
		return mqttRepairSaveForeign
	}
	for attempt := 0; attempt < mqttRepairSaveAttempts; attempt++ {
		smsg, lerr := sess.jsa.loadSessionMsg(d.domainTk, d.cidHash)
		switch classifyRecordAgainstAttempt(smsg, lerr, d.baseSeq) {
		case mqttRecordAttemptUnreadable:
			// The re-read itself is unavailable. A CAS rejection is definite, so
			// save() deliberately did not mark this uncertain — the phase
			// establishes the pending-write state itself, carrying the base and
			// digest, so the deferred readback classifies THIS attempt.
			sess.mu.Lock()
			sess.markWriteUncertainLocked(d)
			sess.mu.Unlock()
			return mqttRepairSaveUnresolved
		case mqttRecordAttemptGone:
			// The record is gone: something released this session, and an absence names
			// neither the releaser nor a successor. Not ours.
			return mqttRepairSaveForeign
		case mqttRecordAttemptAtBase:
			// Still exactly the record we CAS'd against: the rejection was
			// transient contention. Retry the identical write.
			resp, rerr := sess.storeFramedSessionMsg(d.domainTk, d.cidHash, d.hdr, d.framed)
			if rerr == nil {
				sess.mu.Lock()
				sess.seq = resp.Sequence
				sess.adoptRecordLineageLocked(d)
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
			sess.markWriteUncertainLocked(d)
			sess.mu.Unlock()
			return mqttRepairSaveUnresolved
		default: // mqttRecordAttemptMoved
			if smsg.Sequence > d.baseSeq && mqttRecordDigest(smsg.Data) == want {
				// Byte-identical at a newer sequence: our write landed and only its
				// response was lost to the contention.
				sess.mu.Lock()
				sess.seq = smsg.Sequence
				sess.adoptRecordLineageLocked(d)
				sess.creditVoidedRegsLocked(d.voidedRegsRev)
				sess.mu.Unlock()
				return mqttRepairSaveLanded
			}
			// A newer, different record (or one below our base). Not ours either way;
			// adopting its sequence would turn our stale snapshot into a valid CAS over
			// it. Whether it SUCCEEDS ours is the separate question movedVerdict asks.
			return movedVerdict(smsg)
		}
	}

	// Retries exhausted. The last rejection was never classified — a CAS
	// rejection says the subject moved, not who moved it — so read once more
	// rather than assuming the contention was ours to lose: a 10071 on the final
	// attempt means the subject moved past our base (who moved it is F-084's
	// question), and returning local-definite there would undo our work while
	// leaving a session we no
	// longer own mapped and bound.
	smsg, lerr := sess.jsa.loadSessionMsg(d.domainTk, d.cidHash)
	switch classifyRecordAgainstAttempt(smsg, lerr, d.baseSeq) {
	case mqttRecordAttemptUnreadable:
		sess.mu.Lock()
		sess.markWriteUncertainLocked(d)
		sess.mu.Unlock()
		return mqttRepairSaveUnresolved
	case mqttRecordAttemptGone:
		return mqttRepairSaveForeign // gone: not ours, and an absence names no successor
	case mqttRecordAttemptAtBase:
		return mqttRepairSaveLocalDefinite // still ours, still unwritten
	default: // mqttRecordAttemptMoved
		if smsg.Sequence > d.baseSeq && mqttRecordDigest(smsg.Data) == want {
			sess.mu.Lock()
			sess.seq = smsg.Sequence
			sess.adoptRecordLineageLocked(d)
			sess.creditVoidedRegsLocked(d.voidedRegsRev)
			sess.mu.Unlock()
			return mqttRepairSaveLanded
		}
		return movedVerdict(smsg)
	}
}

// storeFramedSessionMsg re-issues an attempted record write's EXACT bytes — same
// CAS base, same body, framed as it went out the first time — through the same
// store boundary (the result seam included, so a test observes the re-send exactly
// as it observed the original).
//
// Re-sending rather than re-marshalling is what keeps the write's IDENTITY intact:
// the digest a later read compares against is the one these bytes carry, and a
// caller that kept its mutation may have moved memory on since. It is also what
// makes the re-send an ARBITRATION: same CAS base, so at most one of the original and
// this can commit. That exclusion used to be qualified as holding only within a leader
// term; it is now unqualified for two BRIDGE-AUTHORED CONDITIONAL writes — see the
// same-base boundary in docs/CONFORMANCE.md, and the retired argument in
// sess_uncertain.go for why identical bytes and MaxMsgsPer:1 are kept for their own
// reasons rather than as that exclusion's safety net.
//
// Both callers hold a record transaction on this goroutine — the repair's accept
// transaction, the resolver's own — which is what makes the raw re-send safe
// without re-validating the fence: nothing can have closed a transaction this
// goroutine holds.
func (sess *mqttSession) storeFramedSessionMsg(domainTk, cidHash string, hdr int, framed []byte) (*JSPubAckResponse, error) {
	resp, err := sess.jsa.storeSessionMsg(domainTk, cidHash, hdr, framed)
	if h := mqttTestSessStoreResultHookV.Load(); h != nil {
		resp, err = (*h)(sess, resp, err)
	}
	return resp, err
}

// reattemptPendingStore re-issues an ambiguous attempt through the store boundary it
// originally used. An accepted CONNECT's attempt is a two-message atomic batch, and
// re-issuing it means recomposing that batch under a fresh id — never re-sending the
// framed bytes, which carry a batch id the server may still hold staged. Everything
// that gives the re-attempt its arbitrating power is preserved regardless: the same
// record CAS base, and bodies whose digests a readback still recognizes.
func (sess *mqttSession) reattemptPendingStore(domainTk, cidHash string, pw *mqttPendingWrite) (*JSPubAckResponse, error) {
	if pw.factSubj == _EMPTY_ {
		return sess.storeFramedSessionMsg(domainTk, cidHash, pw.hdr, pw.framed)
	}
	fact, record, batchID := mqttComposeAcceptBatch(pw.factSubj, pw.factBody, domainTk, cidHash, pw.baseSeq, pw.framed[pw.hdr:])
	resp, err := sess.jsa.storeAcceptBatch(domainTk, cidHash, batchID, fact, record)
	if h := mqttTestSessStoreResultHookV.Load(); h != nil {
		resp, err = (*h)(sess, resp, err)
	}
	return resp, err
}

// sweepUnadoptedPhaseSubs removes every reconciliation-phase subscription that
// subscription restoration did not adopt. Restoration can legitimately skip a
// collected filter — it runs before the CONNACK, and a PUBREL setup failure fails a QoS 2
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

// applyRequestedLifetime applies this CONNECT's Session Expiry Interval and
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
//     mqttDeferDisconnectOwned SNAPSHOTS the lifetime into the deferred action — so a
//     requested interval of 0 that never landed would still discard a persistent
//     session, because applyDeferredDisconnect acts on that snapshot rather than
//     on whatever the readback later restores. Preferring the confirmed terms
//     costs at most one disconnect honoring the older lifetime; the alternative
//     costs the session.
//   - 10071: the subject moved after our accept save. Restoring or re-writing
//     anything would be a stale write, so the session is handed over instead
//     (DISCONNECT suppressed: no SUCCESS CONNACK was written, so the refusal
//     carries the reason instead).
//   - 10164: clustered contention is a rejection, not a verdict, so the bounded
//     same-base/same-body re-read decides.
//   - Definitely failed: nothing was stored, so memory goes back and the record
//     still describes the lifetime the session had.
//
// Like the reconciliation phase, it REPORTS a disposing verdict rather than
// acting on it: the disposal schedules closeConnection on another goroutine, so
// the caller must abort and signal the client BEFORE it runs. That is why this
// returns an outcome and a vocabulary instead of a bare error, and why a forked
// record's 0x89 now reaches the client instead of being computed, spent on a
// suppressed DISCONNECT and discarded.
//
// Runs inside the CONNECT's accept transaction, whose token it must be given.
// No lock held on entry.
// It takes neither the session manager nor the client, and that is the visible
// consequence of not disposing: both existed only to hand the session over. Keeping
// them would leave the next reader looking for the disposal that used to be here.
// mqttLifetimeFinality says what a FAILED lifetime write means for the caller,
// and it exists because the two callers differ on exactly that. It is not a
// severity dial: each value names a fact about the caller's situation.
type mqttLifetimeFinality int

const (
	// mqttLifetimeSpeculative is a CONNECT, which can still be refused. A write
	// that did not prove itself leaves the session on its CONFIRMED terms,
	// because a requested interval the broker never granted must not decide a
	// teardown -- see this function's AMBIGUOUS note above.
	mqttLifetimeSpeculative mqttLifetimeFinality = iota
	// mqttLifetimeFinal is an accepted connection's close carrying a DISCONNECT
	// override. No refusal is possible and the client has already been told
	// nothing further, so putting the old terms back on a definite failure would
	// silently ignore the client's instruction -- which is the Spec5
	// [3.1.2.11.2] violation F-041 exists to fix. The terms STAND and only the
	// durable backstop is lost.
	mqttLifetimeFinal
)

func (sess *mqttSession) applyRequestedLifetime(tok recordTx, expiry uint32,
	clean bool, finality mqttLifetimeFinality) mqttLifetimeGrantResult {

	sess.mu.Lock()
	wasExpiry, wasClean := sess.expiryInterval, sess.clean
	if wasExpiry == expiry && wasClean == clean {
		sess.mu.Unlock()
		return mqttLifetimeGrantResult{outcome: mqttPreConnackProceed}
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
		//
		// SPECULATIVE ONLY. Under Final the parked action already carries the
		// override -- mqttDeferDisconnectEx reads it from the connection, so the
		// action was never built against the confirmed terms in the first place --
		// and retargeting would rewrite a correct value with the same value. Left
		// unguarded it would also falsify the claim that this helper's retarget
		// keeps only its CONNECT caller. F-041.
		if finality == mqttLifetimeSpeculative {
			sess.retargetDeferredLifetime(priorDeferred, expiry)
		}
	}

	// The same classification the reconciliation phase's repair save uses, and for
	// the same reasons — one place for the two CAS kinds, one place that mints the
	// fork grant. See classifyGatedRecordSave.
	cs := sess.classifyGatedRecordSave(tok)
	switch cs.outcome {
	case mqttPreConnackProceed:
		return mqttLifetimeGrantResult{outcome: mqttPreConnackProceed}
	case mqttPreConnackUnresolved:
		restore()
		sess.retainOnLanded(apply)
	case mqttPreConnackDisposingNonForked, mqttPreConnackDisposingForked:
		// No restore() on either disposing arm — the session is being evicted, so
		// putting the pre-grant terms back would only mutate an object nobody reads.
		//
		// And no disposal HERE: it schedules closeConnection on another goroutine, so
		// the caller aborts, signals the client, and only then disposes with the
		// vocabulary this returns. That ordering is the whole point of the return
		// type; disposing inline is what kept this site's refusal unexplained.
		return mqttLifetimeGrantResult{outcome: cs.outcome, voc: cs.voc, err: cs.err}
	default: // mqttPreConnackLocal
		// Speculative only. Under Final the client's DISCONNECT is unrefusable, so
		// restoring the pre-override terms here would discard the instruction and
		// leave the teardown below acting on a lifetime the client replaced; the
		// caller reports the lost durability instead. See mqttLifetimeFinality.
		if finality == mqttLifetimeSpeculative {
			restore()
		}
	}
	return mqttLifetimeGrantResult{outcome: cs.outcome, err: cs.err}
}
