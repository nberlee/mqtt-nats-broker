// Recognising a session record that is not demonstrably a successor of the one this
// broker was acknowledged for.
//
// Every durable arbitration here is a per-subject JetStream CAS
// (Nats-Expected-Last-Subject-Sequence). At the pinned nats-server v2.14.5 the check
// runs once, before the proposal (checkMsgHeadersPreClusteredProposal,
// server/jetstream_batching.go), against applied state plus an in-process map that
// processStreamLeaderChange nils on every transition (server/jetstream_cluster.go); the
// clustered apply never re-checks it (`canConsistencyCheck := !isClustered ||
// traceOnly`, server/stream.go).
//
// This machinery was designed against the reading that a proposal accepted under one
// leader could therefore survive the election unapplied while the NEW leader admitted a
// second write against the SAME base. **That reading is retired on the supported floor**
// by F-030's combined dependency-canary, pinned-source and maintainer-confirmed evidence.
// test/depproof enumerates every route for a conditional write on a conforming stream:
// it fixtures the two live ones, refutes seven, and scopes post-start mirror/source drift
// outside the claim. It exercises $MQTT_willq's single-message CAS shape and, since the
// accepted-CONNECT proof, $MQTT_sess's atomic-batch shape as well.
//
// **That audit has run**, obligation by obligation, and it split this machinery in two.
//
// The CREATE side is DELETED. Every route by which a create could be handed a parent —
// the retirement watermark's retired-record identity (a field since removed outright), an
// adopted marker's PWID, a re-minted object's stamp,
// the session-interval register on $MQTT_origin — existed so that two creates into one
// emptied subject were recognisable as siblings, and every route docs/CONFORMANCE.md
// documented for such a verdict turned out to be a FALSE fork: the inherited identity
// outlived the interval it named. A create is now parentless and concedes.
//
// What remains is the AUTHORED-RECORD fork — two CAS writes off a base this broker
// actually held — and it stays for a stated reason rather than by default.
//
// The reason it used to state was an evidence gap in the accepted CONNECT's writer, and
// that gap is CLOSED. Every Raft-level fact behind it holds — at the pinned v2.14.5 no
// Raft-level field preserves "these entries came from one ProposeMulti": entries are
// pushed individually, popped as whatever prefix exists, regrouped by size thresholds,
// and each group rechecks leadership on its own — but the consequence drawn from them
// was false. Batch identity lives in the stream entry encoding and is enforced at
// apply, so a committed prefix with no commit entry applies NOTHING, and
// storeAcceptBatch's fact and record commit together or not at all. Measured by
// test/depproof (R-batch, mutation m-batch-apply-eager) and, for this broker's own use
// of the batch, by TestMQTTAcceptBatchReplicatedIndivisible.
//
// **The AUTHORED-RECORD side was then audited in its own right, and it STAYS** — on a
// route the closed gap never covered. The predicate below establishes two record BODIES
// CLAIMING one parent, never that two CAS applications off one base both landed, and an
// UNCONDITIONAL write produces the first without the second. That route is live: nothing
// validates record provenance at restore (createOrRestoreSessionWithin checks JSON, client ID,
// reserved membership, generation, the affine pair, teardown/retirement and overdue
// state, then adopts WID and PWID as given), and the reserved-namespace guards deny
// $MQTT.> to MQTT clients rather than to permitted NATS publication. See the same-base
// boundary in docs/CONFORMANCE.md for the three-part statement.
//
// TestMQTTForkedRecordE2EAnswers0x89AndPublishesTheWill is the retention oracle for that
// route — it lands a valid-shaped record with an unconditional store and proves the live
// connection reaches this classifier, is answered 0x89, and publishes its Will.
// TestMQTTAuthoredForkStillClassifies pins the predicate's SEMANTICS and nothing more:
// it constructs lineage values and calls the classifier, so it would keep passing even if
// no live route could produce the pair.
//
// Recognition here also covers an ambiguous acknowledgement, an out-of-band deletion and
// a stream recreation — none of them proposal-ordering questions — for the same records.
// Those three CONCEDE rather than classify, so they are not independent reasons to keep
// this machinery; the unconditional-write route is.
//
// The observable damage is not the lost write. It is what the broker CONCLUDES from it:
// its next CAS is rejected, it re-reads, and finds a record it did not write. Two brokers
// can reach that conclusion about each other from the same event.
//
// It USED to report "the session was taken over" there, evicting its own rightful client
// with 0x8E and telling it to stop reconnecting. F-084 withdrew that: the eviction still
// happens — the record is not ours — but the claim is now 0x89 ownership-unsettled, which
// is what a rejected CAS actually supports.
//
// A sequence cannot tell the two shapes apart, because "the subject moved past my base
// with other content" is true of a legitimate successor AND of a sibling that erased us.
// What distinguishes them is ANCESTRY, so each record carries a parent pointer: WID is
// the write's own identity, PWID is the WID of the record it was derived from. Two
// writes carrying the same PWID CLAIM one base.
//
// VOCABULARY, used throughout this package and its tests: "sibling" is shorthand for
// "a body claiming the parent we claim". It is NOT a proof of descent — restore adopts
// WID and PWID without validating provenance — and it never needs to be, because every
// conclusion drawn from it is negative: such a body does not establish a takeover.
//
// WHAT THIS DOES AND DOES NOT DO.
//
// It DETECTS; it never repairs. A body claiming our parent stops the broker asserting a takeover
// it cannot demonstrate — the client is told 0x89 "server busy" instead, which invites
// the reconnect 0x8E forbids — and licenses no write of its own. The forked record
// stays, and the rightful owner's granted state is still lost.
//
// The verdict does NOT prove the survivor erased us: a teardown claim is itself a
// full-body CAS off the same base (sess_teardown.go), so a sibling can merely have
// become current after clear() deleted our record. What it proves is narrower and
// sufficient — the survivor is not demonstrably a successor of the record we were acknowledged for,
// so "another owner holds this session" is not established.
//
// A counter would not do this job: a per-write epoch is a branch-local Lamport counter,
// and a competing branch that never read our record can advance above us and then look
// like our successor. Only a pointer AT the parent carries a CLAIM of ancestry — and a
// claim is all it is, since restore adopts the pair unvalidated. That is sufficient here
// precisely because the conclusion drawn is negative: a body claiming our parent does not
// establish that another owner holds the session.
//
// See docs/ARCHITECTURE.md and the residual in docs/CONFORMANCE.md.

package mqtt

import (
	"encoding/json"

	"github.com/nats-io/nuid"
)

// mqttNewRecordWID mints a record write's own identity. Same alphabet and generator as
// the incarnation generation and the affine generation, and minted per WRITE rather
// than per session: what it names is one record, not one incarnation.
func mqttNewRecordWID() string { return nuid.Next() }

// mqttRecordLineage is what the parent pointer establishes about a surviving record,
// and it has exactly two values on purpose.
//
// It NEVER says "ours", and never decides that a write landed. Those outcomes —
// mqttAttemptOurs, mqttRepairSaveLanded — credit void-register verdicts and run landed
// continuations, and an identity match is not proof that an attempt's BYTES are durable
// (a record can carry our acknowledged WID because it is a verbatim replay of it). The
// digest comparison remains the only thing that licenses them, and it precedes lineage
// in every caller. Lineage is consulted at exactly one place per caller: the branch that
// had already concluded foreign.
type mqttRecordLineage uint8

const (
	// mqttLineageNotForked: nothing is established. A legitimate successor, an
	// unrelated lineage, a different incarnation, a record carrying no identity, or
	// our own record — every one of them concedes exactly as the broker does today.
	mqttLineageNotForked mqttRecordLineage = iota
	// mqttLineageForked: the survivor CLAIMS the parent our acknowledged record claims,
	// so it is not demonstrably a successor of ours and is no evidence of a takeover.
	// Not a proof of descent — restore adopts the pair unvalidated — and it does not need
	// to be, because the conclusion drawn is negative.
	mqttLineageForked
)

func (l mqttRecordLineage) String() string {
	if l == mqttLineageForked {
		return "forked"
	}
	return "not-forked"
}

// classifyRecordLineage decides whether found CLAIMS the same parent as the record this
// broker was last acknowledged for, identified by (ackWID, ackPWID).
//
// It takes NO sequence and does not replace the sequence fail-safe. Every caller keeps
// its existing `Sequence > baseSeq` gate AHEAD of this call: classifyRecordAgainstAttempt
// calls every non-equal sequence "moved", including one BELOW the base, and a lower
// sequence means a reset or re-provisioned store whose record must never be adopted
// however its identity compares.
//
// Sibling-hood is defined against the ACKNOWLEDGED pair and not against the in-flight
// attempt, and the difference is the whole point. With acknowledged {a,p} and an attempt
// {c,a}:
//
//   - survivor {b,p} claims a's parent — our acknowledged record was erased. FORKED.
//     Compared against the attempt {c,a} it would look unrelated and be missed entirely.
//   - survivor {d,a} is CHILD-SHAPED against a: whoever wrote it read our acknowledged
//     record first. That is a lost race, not a fork. Not-forked — and NOT a statement that
//     anyone holds the session now, which this classifier never makes (F-084). Lineage is
//     the only question it answers; a child-shaped body is as consistent with a writer that
//     has since gone as with one still holding it.
//   - survivor {c,a} is our own attempt landing late; the digest branch classifies it
//     before lineage is reached, and its parent a is not the acknowledged parent p
//     anyway.
//
// There is deliberately no "is this our own record" arm and no "direct successor" arm:
// with only two verdicts, either would turn not-forked into not-forked, so no mutation
// could kill it and it would state nothing a test can distinguish.
func classifyRecordLineage(found *mqttPersistedSession, ackWID, ackPWID string) mqttRecordLineage {
	if found == nil {
		return mqttLineageNotForked
	}
	// The incarnation GENERATION is deliberately not consulted, and that is a reversal.
	// It used to short-circuit here — a different incarnation was held to restart the
	// lineage — which made a CLEAN-START sibling invisible: one is written by a session
	// clear() re-minted, so it ALWAYS carries a different generation, and the
	// short-circuit conceded before the pointer test ran. That particular pair now
	// concedes anyway, because the write-lineage audit made the post-re-mint write
	// parentless. The reversal stands regardless and on its own terms: record identities
	// are globally unique NUIDs, so a parent match names one actual record whatever
	// generation wrote it — a CLAIM of descent from it, which is what the verdict rests
	// on — and a generation mismatch adds nothing to that.
	// Re-introducing it would suppress an authored fork whose two writers restarted under
	// different incarnations. See parentForNextWriteLocked.
	//
	// All four conjuncts are load-bearing, and each is stated here rather than assumed
	// by the callers, because each has a shape that would otherwise be misclassified:
	//
	//   - ackWID != "": an acknowledged pair of {"", p} would otherwise make every
	//     survivor with parent p a sibling of a record that has no identity of its own.
	//     Correct writers cannot produce that shape, but createOrRestoreSessionWithin
	//     validates only the generation and the affine pair, and accepts whatever lineage
	//     the JSON carries, so the guard is what makes the fail-safe uniform against a
	//     hand-edited or future record.
	//   - ackPWID != "": without it two unrelated FIRST CREATES read as siblings.
	//   - found.WID != "": a survivor carrying no identity but a parent matching ours
	//     would otherwise satisfy the last conjunct through "" != ackWID. This is what
	//     makes an absent WID concede rather than accuse.
	//   - found.WID != ackWID: our acknowledged record's own bytes can reappear at a
	//     HIGHER sequence — storeFramedSessionMsg deliberately replays them verbatim,
	//     and a clustered duplicate proposal can land them at a different sequence — and
	//     without this conjunct such a replay is reported as a fork of itself.
	if ackWID != _EMPTY_ && ackPWID != _EMPTY_ && found.WID != _EMPTY_ &&
		found.PWID == ackPWID && found.WID != ackWID {
		return mqttLineageForked
	}
	return mqttLineageNotForked
}

// parentForNextWriteLocked is the record a new write descends from: the one we are
// acknowledged for, and nothing else. A session holding no record writes a PARENTLESS
// create.
//
// It used to fall back to the identity of the record whose REMOVAL emptied the subject,
// carrying the chain across a deletion so that a resume child {b, a} and a clean-start
// survivor written after tearing a down were recognisable as siblings. The write-lineage
// audit deleted that fallback and its three sources. Its verdicts were reachable only
// through an inherited identity that had outlived the interval it named, and every route
// docs/CONFORMANCE.md documented for one was a FALSE fork: the identity was replayed by a
// regressed register, separated from its admission by a complete remote lifecycle,
// resurrected by a regressed teardown slot, or left standing by an out-of-band deletion.
// A parentless create concedes at classifyRecordLineage's ackPWID conjunct from either
// orientation — the pre-F-029b RECOGNITION, restated as a residual. What conceding SAYS is
// no longer the takeover vocabulary: F-084 made it ownership-unsettled, because failing to
// recognise sibling-hood leaves this broker knowing less, not more.
//
// What that gives up is the CLEAN-START fork, and what it does not give up is the
// authored-record one: two CAS writes off a base both broker held. That shape still
// classifies, which is the obligation this file retains. See classifyRecordLineage.
//
// sess.mu must be held.
func (sess *mqttSession) parentForNextWriteLocked() string {
	return sess.wid
}

// adoptRecordLineageLocked adopts the lineage AND the grant an attempt STAMPED, for a caller
// that has just proven those exact bytes durable outside saveWithDetail's own success arm.
//
// It exists as a named step beside every `sess.seq = ...` adoption because the three are
// one fact: the record now at that sequence is the one carrying this identity and these
// bytes. A site that moved the sequence while leaving the lineage stale would parent the
// session's NEXT write to a record that is no longer the base — after which every genuine
// sibling of the real base classifies as unrelated and concedes. A site that moved the
// lineage while leaving the BYTES stale would then compute a fork's repair against a record
// two writes back, and re-assert state the client never held. Both belong here rather than
// at each landed arm, so a future proof site cannot adopt one without the other.
//
// sess.mu must be held.
func (sess *mqttSession) adoptRecordLineageLocked(d *mqttSessSaveDetail) {
	sess.wid, sess.pwid = d.wid, d.pwid
	sess.adoptGrantBodyLocked(d.body)
}

// mqttDecodeSessionRecord decodes a loaded record for the lineage question only.
//
// Undecodable is nil, which concedes: this runs on a branch that has ALREADY decided
// the record is foreign, so failing to read its identity simply leaves that decision
// standing. Callers decode lazily, on that branch alone — the landed and digest arms
// compare raw bytes and must not pay for a decode.
func mqttDecodeSessionRecord(smsg *StoredMsg) *mqttPersistedSession {
	if smsg == nil {
		return nil
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return nil
	}
	return ps
}

// mqttDisposalVocabulary is everything a displacement REPORTS: the DISCONNECT reason
// code, the Reason String that accompanies it, the log line, and the connection close
// reason.
//
// They travel together because they are one claim made four ways, and splitting them is
// how a review round found an implementation telling a v5 client 0x89 "server busy" and
// "session taken over by a new connection" in the same packet. Passing a reason code
// alone reproduces exactly that.
type mqttDisposalVocabulary struct {
	// reason / reasonStr are the v5 DISCONNECT's reason code and Reason String.
	reason    byte
	reasonStr string
	// logReason completes "MQTT: session %q ..." in the disposal warning. Held as the
	// variable HALF of a constant format rather than as a format of its own, so the
	// printf checker still sees one.
	logReason string
	// closeReason is the connection's close state. Nothing reads it —
	// closeConnection ignores its argument — which is both why it is never asserted
	// and why setting it accurately is free.
	closeReason ClosedState
}

var (
	// mqttDisposalTakenOver is the claim a demonstrated takeover makes: a REPLACEMENT
	// CONNECTION for this client ID is held in hand, so the client must stop reconnecting
	// under this session. Spec5 [3.14.2.1] 0x8E.
	//
	// NOT "an authoritative record exists that is a successor of ours", which is what this
	// said before F-084 and which no record can establish: restore adopts WID/PWID without
	// validating provenance, valid-shaped bodies arrive through unconditional publication,
	// and acknowledged bytes are replayed verbatim. Its only licence is
	// discardTakenOverClient, and the single-reader property is asserted by
	// TestF084OnlyOneReaderOfTheTakeoverClaim.
	mqttDisposalTakenOver = mqttDisposalVocabulary{
		reason:      mqttReasonSessionTakenOver,
		reasonStr:   mqttReasonStrSessionTakenOver,
		logReason:   "is now owned elsewhere; closing this connection",
		closeReason: DuplicateClientID,
	}
	// mqttDisposalForked is what a body claiming our parent licenses instead, and the difference
	// is the client's instruction. 0x89 "Server busy, try again later" invites the
	// reconnect that resolves the ambiguity; 0x8E tells the client the session is
	// somebody else's, which on this evidence is a guess. Spec5 [3.14.2.1] 0x89.
	mqttDisposalForked = mqttDisposalVocabulary{
		reason:      mqttReasonServerBusy,
		reasonStr:   mqttReasonStrOwnershipUnsettled,
		logReason:   "could not be shown to be owned elsewhere; closing this connection",
		closeReason: StaleConnection,
	}
	// mqttDisposalOwnershipUnsettled is what a RECORD-DERIVED displacement is licensed to
	// say, and it is the answer to the question F-084 asked: this broker may no longer
	// write the record, and nothing established WHO may.
	//
	// It is reached wherever the evidence is an absence or a stranger — the record could
	// not be read, was gone, did not decode, sat at or below our base, or moved to a body
	// that is merely valid-shaped. classifyRecordLineage says of exactly that verdict
	// "nothing is established", and 0x8E would answer it with the strongest claim the
	// broker has. Spec5 [3.14.2.1] 0x89.
	//
	// A separate value rather than a reuse of mqttDisposalForked, on the same grounds
	// mqttDisposalTeardownIncomplete is: the code and the text match, and the NAME is what
	// is read at the call site as the claim being made. The logReason differs because the
	// facts differ — a fork SAW a sibling body, this saw nothing — and it MINTS NOTHING,
	// because a fork grant is a repair anchored to a record proven to claim our parent and
	// none was.
	mqttDisposalOwnershipUnsettled = mqttDisposalVocabulary{
		reason:      mqttReasonServerBusy,
		reasonStr:   mqttReasonStrOwnershipUnsettled,
		logReason:   "could not be shown to be owned by anyone; closing this connection",
		closeReason: StaleConnection,
	}
	// mqttDisposalTeardownIncomplete is what a teardown that began DESTROYING state and
	// could not finish licenses. Its reason code matches the forked one, and the
	// rationale does not: nothing here proves a sibling, and nothing proves another
	// owner either. What is known is narrower and worse — this broker erased the
	// session's own state and cannot say whether the record survived — so the client
	// must be let go, and 0x89 "try again later" is the only honest instruction. 0x8E
	// would tell it to stop reconnecting under a session nobody demonstrably took.
	//
	// It is a separate value rather than a reuse of mqttDisposalForked because the NAME
	// is read at the call site as the claim being made. See processConnect's post-reset
	// clear() arm.
	mqttDisposalTeardownIncomplete = mqttDisposalVocabulary{
		reason:      mqttReasonServerBusy,
		reasonStr:   mqttReasonStrOwnershipUnsettled,
		logReason:   "was left inconsistent by a teardown that could not complete; closing this connection",
		closeReason: StaleConnection,
	}
)

// mqttDisposalForLineage maps a record classification onto what it licenses the broker to
// SAY, and it is the ONE mapper for every record-derived disposal.
//
// It replaced three — one per verdict enum, for the uncertainty resolver, the rejection
// probe and the CONNECT-time classified save — which had become three statements of one
// fact once F-084 removed the takeover claim from their default arms. Each took a
// different enum and asked the same question, so each caller now answers it and this
// answers what the answer means.
//
// Only a SIBLING body licenses the fork vocabulary, and only that class may mint a repair
// grant. Everything else — a stranger, a replay, a parentless create, an absence, an
// unreadable or undecodable record — is UNSETTLED: the record is not ours to write and
// nothing says whose it is. Neither arm claims a takeover, because no record shape proves
// a successor; that claim is licensed only by an in-process same-client-ID replacement
// this broker holds both ends of. See discardTakenOverClient, and F-084.
func mqttDisposalForLineage(forked bool) mqttDisposalVocabulary {
	if forked {
		return mqttDisposalForked
	}
	return mqttDisposalOwnershipUnsettled
}

// disposalForRemotePersist maps the persist-observation route's lineage verdict onto
// what it licenses the broker to SAY. The caller (evictDisplacedOnRemotePersist) keeps its
// strict `Sequence > sess.seq` gate AHEAD of this call — that is this consult's sequence
// fail-safe — and calls it only on the usable-record path: an absent, undecodable or
// below-target record concedes through the license rule without reaching lineage.
//
// It returns the fork BIT alongside the vocabulary because the caller mints on it, and the
// two must not be re-derived from each other. Recovering the bit from the reason code was
// what the caller used to do, and F-084 made that unsound: the unsettled vocabulary carries
// 0x89 too, so a reason-code test would mint a grant for a record that proved nothing.
func disposalForRemotePersist(ps *mqttPersistedSession, ackWID, ackPWID string) (mqttDisposalVocabulary, bool) {
	forked := classifyRecordLineage(ps, ackWID, ackPWID) == mqttLineageForked
	return mqttDisposalForLineage(forked), forked
}

// disposalForDirectRejection classifies the record behind a 10071 rejection — the
// CONNECT-time saves' OTHER rejection arm, which disposes immediately without consulting
// resolveContendedRecordSave and therefore has no readback of its own.
//
// It performs one load, on the rejection path of a pre-CONNACK save, never on a healthy
// connection. Without it the two helpers would reach their verdict on the bare fact that
// the CAS failed, which is the weakest evidence any of the five classified paths acts on.
//
// Unreadable, undecodable, absent or at/below the base all CONCEDE — but conceding is now
// the UNSETTLED vocabulary, not the takeover one. Each of those is an absence, and an
// absence is not a successor: the subject moved past our base, which says we may not write
// it and says nothing about who may. F-084.
//
// It returns the fork bit for the same reason disposalForRemotePersist does.
//
// No lock held on entry. Performs one JetStream request.
func (sess *mqttSession) disposalForDirectRejection(d *mqttSessSaveDetail) (mqttDisposalVocabulary, bool) {
	smsg, lerr := sess.jsa.loadSessionMsg(d.domainTk, d.cidHash)
	if lerr != nil || smsg == nil {
		return mqttDisposalOwnershipUnsettled, false
	}
	sess.mu.Lock()
	ackWID, ackPWID := sess.wid, sess.pwid
	sess.mu.Unlock()
	if smsg.Sequence > d.baseSeq &&
		classifyRecordLineage(mqttDecodeSessionRecord(smsg), ackWID, ackPWID) == mqttLineageForked {
		// The 10071 arms of both CONNECT-time saves reach their verdict here, and both
		// callers hold a record transaction on this goroutine, so this is the in-fence mint
		// for the reconcile and lifetime paths. See sess_fork_repair.go.
		sess.mintForkedGrant(ackWID, ackPWID)
		return mqttDisposalForked, true
	}
	return mqttDisposalOwnershipUnsettled, false
}
