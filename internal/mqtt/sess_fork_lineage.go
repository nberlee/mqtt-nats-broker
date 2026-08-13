// Recognising a session record that is provably NOT a successor of the one this
// broker was acknowledged for.
//
// Every durable arbitration here is a per-subject JetStream CAS
// (Nats-Expected-Last-Subject-Sequence), and that header is a linearization point only
// WITHIN a leader term. At the pinned nats-server v2.14.4 the check runs once, before
// the proposal (checkMsgHeadersPreClusteredProposal, server/jetstream_batching.go),
// against applied state plus an in-process map that processStreamLeaderChange nils on
// every transition (server/jetstream_cluster.go); the clustered apply never re-checks it
// (`canConsistencyCheck := !isClustered || traceOnly`, server/stream.go). So a proposal
// accepted under one leader can survive the election unapplied while the NEW leader
// admits a second write against the SAME base, and both apply in log order — last apply
// wins, and MaxMsgsPer:1 discards the other.
//
// The observable damage is not the lost write. It is what the broker CONCLUDES from it:
// its next CAS is rejected, it re-reads, finds a record it did not write, and reports
// "the session was taken over" — evicting its own rightful client with 0x8E "session
// taken over by a new connection", which tells that client to stop reconnecting. Two
// brokers can reach that conclusion about each other from the same event.
//
// A sequence cannot tell the two shapes apart, because "the subject moved past my base
// with other content" is true of a legitimate successor AND of a sibling that erased us.
// What distinguishes them is ANCESTRY, so each record carries a parent pointer: WID is
// the write's own identity, PWID is the WID of the record it was derived from. Two
// writes carrying the same PWID are siblings of one base.
//
// WHAT THIS DOES AND DOES NOT DO.
//
// It DETECTS; it never repairs. A proven sibling stops the broker asserting a takeover
// it cannot demonstrate — the client is told 0x89 "server busy" instead, which invites
// the reconnect 0x8E forbids — and licenses no write of its own. The forked record
// stays, and the rightful owner's granted state is still lost.
//
// The verdict does NOT prove the survivor erased us: a teardown claim is itself a
// full-body CAS off the same base (sess_teardown.go), so a sibling can merely have
// become current after clear() deleted our record. What it proves is narrower and
// sufficient — the survivor is not a successor of the record we were acknowledged for,
// so "another owner holds this session" is not established.
//
// A counter would not do this job: a per-write epoch is a branch-local Lamport counter,
// and a competing branch that never read our record can advance above us and then look
// like our successor. Only a pointer AT the parent states ancestry.
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
	// mqttLineageForked: the survivor is PROVABLY a sibling of the record we were
	// acknowledged for, so it is no successor of ours and no evidence of a takeover.
	mqttLineageForked
)

func (l mqttRecordLineage) String() string {
	if l == mqttLineageForked {
		return "forked"
	}
	return "not-forked"
}

// classifyRecordLineage decides whether found is a proven sibling of the record this
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
//   - survivor {b,p} is a sibling of a — our acknowledged record was erased. FORKED.
//     Compared against the attempt {c,a} it would look unrelated and be missed entirely.
//   - survivor {d,a} is a CHILD of a: somebody read our acknowledged record and wrote on
//     top of it. A legitimate successor and a lost race, not a fork. Not-forked.
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
	// lineage — which made the one fork this feature exists to catch invisible: a
	// CLEAN-START sibling is written by a session clear() re-minted, so it ALWAYS
	// carries a different generation, and the short-circuit conceded before the pointer
	// test ran. Record identities are globally unique NUIDs, so a parent match means
	// descent from one actual record whatever generation wrote it; a generation
	// mismatch adds nothing to that and only suppressed it. See
	// parentForNextWriteLocked.
	//
	// All four conjuncts are load-bearing, and each is stated here rather than assumed
	// by the callers, because each has a shape that would otherwise be misclassified:
	//
	//   - ackWID != "": an acknowledged pair of {"", p} would otherwise make every
	//     survivor with parent p a sibling of a record that has no identity of its own.
	//     Correct writers cannot produce that shape, but createOrRestoreSession
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

// parentForNextWriteLocked is the record a new write descends from: the one we hold, or —
// for a session that holds none — the record whose REMOVAL emptied the subject it is about
// to write into.
//
// Both parenting sites call it, and the fallback is what carries the chain across a
// deletion. Without it clear() leaves the next create parentless, and the two siblings of
// one base — a resume child {b, a} and a clean-start survivor written after tearing a down
// — have nothing in common for classifyRecordLineage to match on, so a broker whose record
// was ERASED by a sibling reports a takeover that never happened. The same holds when no
// marker survives to read and the retirement watermark is what names the removed record.
//
// The removed record's WID, never the teardown marker's: the marker is deleted by the
// release and is nobody's base.
//
// sess.mu must be held.
func (sess *mqttSession) parentForNextWriteLocked() string {
	if sess.wid != _EMPTY_ {
		return sess.wid
	}
	return sess.retiredBaseWID
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
	// mqttDisposalTakenOver is the claim a demonstrated takeover makes: an
	// authoritative record exists that is a successor of ours, so the client must stop
	// reconnecting under this session. Spec5 [3.14.2.1] 0x8E.
	mqttDisposalTakenOver = mqttDisposalVocabulary{
		reason:      mqttReasonSessionTakenOver,
		reasonStr:   mqttReasonStrSessionTakenOver,
		logReason:   "is now owned elsewhere; closing this connection",
		closeReason: DuplicateClientID,
	}
	// mqttDisposalForked is what a proven sibling licenses instead, and the difference
	// is the client's instruction. 0x89 "Server busy, try again later" invites the
	// reconnect that resolves the ambiguity; 0x8E tells the client the session is
	// somebody else's, which on this evidence is a guess. Spec5 [3.14.2.1] 0x89.
	mqttDisposalForked = mqttDisposalVocabulary{
		reason:      mqttReasonServerBusy,
		reasonStr:   mqttReasonStrOwnershipUnsettled,
		logReason:   "could not be shown to be owned elsewhere; closing this connection",
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

// mqttDisposalForForeignResolution maps an ambiguity resolution onto what it licenses
// the broker to SAY. Only the two disposing verdicts reach it; anything else would be a
// caller disposing a session it never proved gone, so the default is the conservative
// claim rather than a silent one.
func mqttDisposalForForeignResolution(res mqttUncertainResolution) mqttDisposalVocabulary {
	if res == mqttUncertainForked {
		return mqttDisposalForked
	}
	return mqttDisposalTakenOver
}

// mqttDisposalForRejection is mqttDisposalForForeignResolution for a definite rejection
// probe's verdict.
func mqttDisposalForRejection(v mqttRejectionVerdict) mqttDisposalVocabulary {
	if v == mqttRejectionForked {
		return mqttDisposalForked
	}
	return mqttDisposalTakenOver
}

// mqttDisposalForRepairSave is mqttDisposalForForeignResolution for a CONNECT-time
// classified save's verdict.
func mqttDisposalForRepairSave(v mqttRepairSaveVerdict) mqttDisposalVocabulary {
	if v == mqttRepairSaveForked {
		return mqttDisposalForked
	}
	return mqttDisposalTakenOver
}

// disposalForRemotePersist maps the persist-observation route's lineage verdict onto
// what it licenses the broker to SAY, the same shape as the other mqttDisposalFor*
// helpers. The caller (evictDisplacedOnRemotePersist) keeps its strict
// `Sequence > sess.seq` gate AHEAD of this call — that is this consult's sequence
// fail-safe — and calls it only on the usable-record path: an absent, undecodable or
// below-target record concedes through the license rule without reaching lineage.
func disposalForRemotePersist(ps *mqttPersistedSession, ackWID, ackPWID string) mqttDisposalVocabulary {
	if classifyRecordLineage(ps, ackWID, ackPWID) == mqttLineageForked {
		return mqttDisposalForked
	}
	return mqttDisposalTakenOver
}

// disposalForDirectRejection classifies the record behind a 10071 rejection — the
// CONNECT-time saves' OTHER rejection arm, which disposes immediately without consulting
// resolveContendedRecordSave and therefore has no readback of its own.
//
// It adds the only new I/O in this change: one load, on the rejection path of a
// pre-CONNACK save, never on a healthy connection. Without it the two helpers would keep
// asserting a takeover on the bare fact that the CAS failed, which is the weakest
// evidence any of the five classified paths acts on.
//
// Unreadable, undecodable, absent or at/below the base all CONCEDE — the takeover
// vocabulary, exactly today's behavior. The verdict only ever narrows.
//
// No lock held on entry. Performs one JetStream request.
func (sess *mqttSession) disposalForDirectRejection(d *mqttSessSaveDetail) mqttDisposalVocabulary {
	smsg, lerr := sess.jsa.loadSessionMsg(d.domainTk, d.cidHash)
	if lerr != nil || smsg == nil {
		return mqttDisposalTakenOver
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
		return mqttDisposalForked
	}
	return mqttDisposalTakenOver
}
