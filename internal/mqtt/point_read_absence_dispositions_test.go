// The reviewed disposition of every classification-worklist member that is not already a
// declared census node.
//
// The worklist is discovered mechanically — a 404 predicate, or a reference to an absence
// sentinel — and mechanical discovery is deliberately NOISY: `isErrorOtherThan` is one function
// and most of its callers test something that is not a per-subject point read at all. Consult 2
// required every discovered candidate to carry reviewed treatment INCLUDING a reason where the
// match is not an interpretation, so that absence from the worklist can never be mistaken for a
// judgement and a match can never be silently dropped.
//
// THREE DISPOSITIONS, AND WHY THEY ARE NOT ONE.
//
//   - `interprets`: this site maps a per-subject read's 404 to a meaning itself. It owes the
//     same axes a census node owes, and its repair premises.
//   - `consumes`: this site does not classify a read; it consumes a VERDICT another site
//     produced and acts on it. The interpretation belongs to the producer, named in Source, and
//     what this site does with the verdict is recorded because that is the consequence the
//     repair has to satisfy. A consumer is not a discharge: its own edges are enumerated by the
//     closure exactly as any other reached function's are.
//   - `not-an-interpretation`: the mechanical match is real but it is not a per-subject absence
//     decision. Every one names WHICH error identifier or which primitive it actually tests.
//     The two large families are a different error identifier — a consumer, stream, sequence or
//     CAS rejection — and a FIXED-SEQUENCE or exhaustion read, which is F-108's side of the
//     boundary and is excluded here with a per-site ruling rather than ignored.
//
// None of these is a discharge of an EDGE. A disposition says what this function does with a
// 404; the closure still enumerates every boundary edge it has, and the deficit still counts
// every obligation crossing them without a reviewed clause.

package mqtt

const (
	prDispoInterprets  = "interprets"
	prDispoConsumes    = "consumes"
	prDispoNotAnInterp = "not-an-interpretation"
)

// prWorklistDisposition is one reviewed treatment. Auth is required for `interprets` and
// `consumes`; Source is required for `consumes`; Why is required for every kind.
type prWorklistDisposition struct {
	Kind    string
	Mapping string // axis A, for `interprets`
	Auth    string // what the absent outcome authorizes HERE
	ClassEv string
	ConsEv  string
	Reach   string
	Source  string // the producing site, for `consumes`
	Repair  string
	Why     string
}

// prWorklistDispositions is keyed by censusFuncKey.String(). A member of the worklist with no
// entry here fails TestPointReadCensusWorklistCovered; an entry naming a function that is not
// on the worklist fails as stale.
var prWorklistDispositions = map[string]prWorklistDisposition{

	// ---- not an interpretation: the predicate tests a different error identifier ----------
	//
	// `isErrorOtherThan` and `IsNatsErr` are general. Their overwhelmingly common use in this
	// package is a CONSUMER, STREAM, SEQUENCE or CAS error, none of which is a per-subject
	// message-GET 404 and none of which inherits F-109's inversion. Each row names the
	// identifier it actually tests, because "it is not a point read" is a claim about THIS
	// call, not about the function.

	"ephemeral_identity.go:(mqttJSA).deleteEphemeralConsumerDefinite": {
		Kind: prDispoNotAnInterp,
		Why: "tests JSConsumerNotFoundErr after a consumer delete. Consumer existence is a " +
			"different response mechanism from a message GET, and the census records it as such",
	},
	"inflight_marker.go:(mqttAccountSessionManager).deleteInflightMarker": {
		Kind: prDispoNotAnInterp,
		Why: "tests JSSequenceNotFoundErrF and the JSStreamMsgDeleteFailedF/ErrStoreMsgNotFound " +
			"pair after a DELETE. A delete's already-gone answer is not a subject-occupancy read",
	},
	"inflight_marker.go:mqttDeleteRefusedBeforeMutation": {
		Kind: prDispoNotAnInterp,
		Why: "tests JSStreamNotFoundErr, JSBadRequestErr and the two not-enabled identifiers to " +
			"recognise a refusal that happened BEFORE any mutation. None is a message-GET 404",
	},
	"inflight_register.go:(mqttAccountSessionManager).deleteInflightRegisterConfirmed": {
		Kind: prDispoNotAnInterp,
		Why:  "the same delete-confirmation predicate as deleteInflightMarker, on the register",
	},
	"mqtt.go:(mqttSession).ensurePubRelConsumerSubscription": {
		Kind:   prDispoConsumes,
		Source: "outbound_ops.go:(mqttJSA).loadOutboundResource",
		Auth: "REFUSING the rematerialization of the generation's PUBREL consumer when the resource " +
			"its LIVE name owns reads nil (F-105 7f): an absent record refuses, it never proceeds, so " +
			"a counterfeit absence costs a withheld recreate and nothing destructive",
		Why: "its JSConsumerNotFoundErr test is a consumer LOOKUP, a separate mechanism; the resource " +
			"read is the verdict it consumes",
	},
	"outbound_pubrel_rematerialize.go:(mqttSession).decidePubRelRepair": {
		Kind:   prDispoConsumes,
		Source: "outbound_ops.go:(mqttJSA).loadOutboundResource",
		Auth: "WITHHOLDING the PUBREL consumer's repair when its resource reads nil (F-105 7f): the " +
			"repair proceeds only on a LIVE, undecided resource read positively, so an absence " +
			"refuses and a counterfeit one costs a withheld repair",
		Why: "its JSConsumerNotFoundErr test is the consumer LOOKUP whose verified absence is the " +
			"repair's trigger — a separate mechanism; the resource read is the verdict it consumes",
	},
	"mqtt.go:mqttClassifyConsumerInfoResponse": {
		Kind: prDispoNotAnInterp,
		Why: "the CONSUMER-info counterpart of mqttClassifyMsgGetResponse. Its comment — only " +
			"consumer-not-found proves absence — is sound for its own mechanism; F-109 is about " +
			"the message-GET handler, which collapses every store error into one 404",
	},
	"mqtt.go:isErrorOtherThan": {
		Kind: prDispoNotAnInterp,
		Why: "the predicate's own DEFINITION. It interprets nothing; every caller supplies the " +
			"identifier, which is why an inventory keyed on this function would be meaningless",
	},
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).deleteOrphanPubRelConsumers": {
		Kind:   prDispoConsumes,
		Source: "will_outbox.go:(mqttAccountSessionManager).loadFenceSession",
		Auth: "proceeding toward the CONSUMER DELETION when the final authoritative record " +
			"re-read at orphan_qos2_sweep.go:2684 comes back with a nil session. Its own comment " +
			"says \"unreadable is not absence\" and it refuses on an error — but loadFenceSession " +
			"maps a 404 to a nil record with a nil error, so a failed read arrives on the absent " +
			"path and the live-generation protections that guard the delete are simply not " +
			"reached. The JSConsumerNotFoundErr test on the delete itself is a different " +
			"mechanism and is not what this row is about",
		Why: "holistic round 1: dispositioning this on its delete predicate hid the absence it " +
			"actually consumes, which is the reading that would make the whole " +
			"not-an-interpretation family untrustworthy",
	},
	"outbound_duty_exec.go:(mqttAccountSessionManager).deleteAffineConsumer": {
		Kind: prDispoNotAnInterp,
		Why: "tests JSConsumerNotFoundErr to report mqttCleanupGone. Its own comment calls that " +
			"a positive answer, and for the consumer mechanism it is one",
	},
	"qos2_move.go:(mqttAccountSessionManager).releaseQoS2Record": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSSequenceNotFoundErrF and the delete-failed pair after a DELETE",
	},
	"qos2_move.go:isQoS2CASRejection": {
		Kind: prDispoNotAnInterp,
		Why: "tests the two wrong-last-sequence identifiers: a CAS REJECTION, which is a write " +
			"admission answer and belongs to F-110's question, not to an absence observation",
	},
	"qos2_release_claim.go:(mqttAccountSessionManager).arbitrateQoS2ReleaseClaims": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSStreamNotFoundErr around a bounded hold enumeration",
	},
	"qos2_release_claim.go:(mqttAccountSessionManager).purgeQoS2Hold": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSStreamNotFoundErr after a PURGE",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffOnBehalf": {
		Kind:   prDispoConsumes,
		Source: "will_outbox.go:(mqttAccountSessionManager).loadFenceSession",
		Auth: "marking the claim DISCARDED and acknowledging the source when the winner's record " +
			"reads nil at qos2_shared_affine.go:2970. The function's own reasoning is that " +
			"absence is bounded evidence, which is sound for a genuine absence and is exactly " +
			"what a failed read counterfeits. EXPERIMENTAL: chapter P's. Its " +
			"JSConsumerNotFoundErr test is a separate consumer-lookup mechanism",
		Why: "holistic round 1: the consumer predicate is not this site's absence decision",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).q2AffGroupConsumerGone": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSConsumerNotFoundErr on a consumer lookup",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).retryQ2AffineStorageConflict": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSConsumerNotFoundErr on a consumer lookup before a retry",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).withdrawQ2AffCopy": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSSequenceNotFoundErrF and the delete-failed pair after a DELETE",
	},
	"qos2_shared_affine.go:(mqttSession).ensureQ2AffineConsumerSubscription": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSConsumerNotFoundErr while rolling back an affine consumer",
	},
	"qos2_shared_affine.go:(mqttSession).finishQ2AffineUnwind": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSConsumerNotFoundErr while unwinding an affine consumer",
	},
	"sess_teardown.go:(mqttSession).deleteTeardownTargets": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSConsumerNotFoundErr across the teardown's consumer deletions",
	},
	"sess_uncertain.go:mqttIsRecordCASRejection": {
		Kind: prDispoNotAnInterp,
		Why: "tests the two wrong-last-sequence identifiers. Its own comment is explicit that " +
			"only \"did it land?\" is being asked; that is F-110's axis, not F-109's",
	},
	"sess_resume_reconcile.go:(mqttSession).classifyGatedRecordSave": {
		Kind:   prDispoConsumes,
		Source: "sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave",
		Auth: "refusing the pre-CONNACK save on the foreign and forked arms. Its IsNatsErr calls " +
			"test the two wrong-last-sequence identifiers — a CAS rejection, not a 404 — but the " +
			"mqttRepairSaveForeign it then acts on is reached from an absence verdict",
		Why: "the CAS predicate here is not a point read; the sentinel it consumes is the " +
			"absence-derived one, so this is a consumer rather than a non-interpretation",
	},
	"shared_group_arbitrate.go:(mqttSession).proveSharedMembershipOwnership": {
		Kind:   prDispoConsumes,
		Source: "sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave",
		Auth: "declaring the shared-group membership foreign, which disposes the connection. Its " +
			"own IsNatsErr calls test wrong-last-sequence identifiers",
		Why: "same shape as classifyGatedRecordSave: a CAS predicate plus a consumed absence verdict",
	},
	"stream_prereqs.go:(mqttAccountSessionManager).rollbackEphemeralConsumers": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSConsumerNotFoundErr while rolling back ephemeral consumers",
	},
	"stream_prereqs.go:ensureBrokerStream": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSStreamNotFoundErr and JSStreamNameExistErr around stream provisioning",
	},
	"stream_prereqs.go:mqttPreflightBrokerStreamBundle": {
		Kind: prDispoNotAnInterp,
		Why: "tests JSStreamNotFoundErr in the bundle preflight, and refuses explicitly where " +
			"stream information is unavailable",
	},
	"stream_prereqs.go:mqttRefuseRetiredOriginStream": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSStreamNotFoundErr for the retired origin stream",
	},
	"unsub_drain.go:(mqttSession).classifyRetiredDrainDisposal": {
		Kind: prDispoNotAnInterp,
		Why:  "tests JSConsumerNotFoundErr after a consumer delete, naming a reaper for the rest",
	},
	"mqtt.go:(mqttSession).persistRecord": {
		Kind:   prDispoConsumes,
		Source: "sess_accept_fact.go:(mqttJSA).readAcceptFact",
		Auth: "arming the unsent marker after a record-CAS rejection. mqttAcceptFactAbsent is " +
			"what gates it, and readAcceptFact is the accounted node that produces that value " +
			"from a per-subject read",
		Why: "consumes an acceptance-fact verdict; performs no read classification of its own",
	},

	// ---- not an interpretation: a FIXED-SEQUENCE or exhaustion read, which is F-108's ------
	//
	// The boundary is stated once in the dossier: fixed-sequence absence is F-108's, per-subject
	// absence is F-109's. These sites match mechanically because they spell the same predicate
	// against JSNoMessageFoundErr, and each carries its own ruling rather than a rule about a
	// method name.

	"inflight_register.go:(mqttAccountSessionManager).inflightSourceState": {
		Kind: prDispoNotAnInterp,
		Why: "loadMsg at a FIXED SEQUENCE (r.Seq), plus a consumer lookup. F-108's side of the " +
			"boundary — and the roadmap already names this site, with its own comment that only " +
			"an exact lookup confirming absence resolves it",
	},
	"mqtt.go:(mqttAccountSessionManager).sweepOrphanedConsumersBefore": {
		Kind: prDispoNotAnInterp,
		Why:  "loadNextMsgFromSeq exhaustion walk, plus JSConsumerNotFoundErr on the deletes",
	},
	"mqtt.go:(mqttAccountSessionManager).sweepSessionExpiries": {
		Kind: prDispoNotAnInterp,
		Why:  "loadNextMsgFromSeq exhaustion walk over the session stream",
	},
	"mqtt.go:(mqttSession).resendPendingRedelivery": {
		Kind: prDispoNotAnInterp,
		Why: "loadMsg at a FIXED SEQUENCE (n.sseq) plus a FirstSeq shortcut. F-108's, and named " +
			"in its bullet as the live resume reap's two arms",
	},
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).adoptAbandonedTeardowns": {
		Kind:   prDispoConsumes,
		Source: "will_outbox.go:(mqttAccountSessionManager).loadFenceSession",
		Auth: "SKIPPING the adoption when the re-read comes back nil, on the reasoning that the " +
			"record is already gone. Its enumeration is a loadNextMsgFromSeq walk and so F-108's " +
			"question, and the walk is verified opportunistic — it returns no completeness " +
			"result — but the per-subject re-read inside it is this census's, and skipping is " +
			"what a failed read then buys",
		Why: "holistic round 3: dispositioning the function on its ENUMERATION primitive hid the " +
			"per-subject absence it consumes",
	},
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).buildSessionRefs": {
		Kind: prDispoNotAnInterp,
		Why:  "loadNextMsgFromSeq exhaustion walk whose short set feeds destructive reconciliation",
	},
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).collectInboundQoS2Candidates": {
		Kind: prDispoNotAnInterp,
		Why:  "JSStreamNotFoundErr on a stream lookup, then a loadNextMsgFromSeq paged collector",
	},
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).collectInflightCandidates": {
		Kind: prDispoNotAnInterp,
		Why:  "the same shape on $MQTT_inflight",
	},
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).condemnRetiredSurvivors": {
		Kind:   prDispoConsumes,
		Source: "will_outbox.go:(mqttAccountSessionManager).loadFenceSession",
		Auth: "SKIPPING the condemnation when the fence session re-reads nil, explicitly because " +
			"the record is taken to be gone. It also consumes a retirement load. The " +
			"enumeration around it is F-108's and is opportunistic; the per-subject reads inside " +
			"it are not",
		Why: "holistic round 3, same shape as adoptAbandonedTeardowns",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).scanQ2AffClaimRecordsCharged": {
		Kind: prDispoNotAnInterp,
		Why:  "loadNextMsgFromSeq exhaustion walk. F-108's, and EXPERIMENTAL: chapter P's affine half",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).walkQ2AffClaimSegment": {
		Kind: prDispoNotAnInterp,
		Why:  "the segment walk beneath scanQ2AffClaimRecordsCharged; same ruling",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).q2AffSourceGone": {
		Kind: prDispoNotAnInterp,
		Why: "loadMsg at a FIXED SEQUENCE (claim.SourceSeq). F-108's boundary, and EXPERIMENTAL. " +
			"Its defect stands: a failed read reports the source authoritatively gone",
	},
	"sess_accept_fact.go:(mqttAccountSessionManager).scanAcceptFacts": {
		Kind: prDispoNotAnInterp,
		Why: "loadNextMsgFromSeq exhaustion walk with a STREAM.INFO cutoff. F-108's, and its " +
			"bullet names the client-visible consequence",
	},
	"unsub_drain.go:(mqttSession).recoverDrains": {
		Kind: prDispoNotAnInterp,
		Why: "a consumer lookup plus loadMsg at a FIXED SEQUENCE. F-108's, and its bullet names " +
			"both arms",
	},
	"unsub_drain.go:(mqttSession).scanPubRelRecordsCharged": {
		Kind: prDispoNotAnInterp,
		Why: "loadNextMsgFromSeq exhaustion walk whose 404 arm returns ok=true, certifying the " +
			"scan complete from a read that may have failed. SUPPORTED and defective — but the " +
			"defect is F-108's enumeration question, and F-108 is implementation-blocked",
	},
	"will_outbox.go:(mqttAccountSessionManager).scanWillOutbox": {
		Kind: prDispoNotAnInterp,
		Why:  "loadNextMsgFromSeq exhaustion walk that sets complete=true. F-108's",
	},

	// ---- interprets: this site maps a per-subject read's 404 to a meaning ----------------

	"inflight_register.go:mqttClassifyMsgGetResponse": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "the Absent/Indeterminate split for the in-flight REGISTER, MARKER and SOURCE " +
			"batches. Its predicate is the inverted one: isErrorOtherThan(rerr, " +
			"JSNoMessageFoundErr) sends a FAILED READ to the Absent arm one frame before any " +
			"caller's withholding arm can fire",
		ClassEv: classDefective, ConsEv: consDemonstrated, Reach: reachLive,
		Repair: "an exact-sequence re-attempt is unavailable: the caller supplies a SUBJECT and " +
			"the absent arm is precisely the one with no sequence to name. A direct multi-last " +
			"get carrying target and marker together is the only unblocked candidate and keeps " +
			"its six premises",
		Why: "the single authoritative definition of the split for its three batches, and the " +
			"reason repairing one caller repairs nothing",
	},
	"inflight_marker.go:mqttMarkerLoadFromResponse": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "the unsent-marker verdict a resume consumes. It switches on " +
			"mqttClassifyMsgGetResponse, so it inherits the inversion rather than repeating it",
		ClassEv: classDefective, ConsEv: consDemonstrated, Reach: reachLive,
		Source: "inflight_register.go:mqttClassifyMsgGetResponse",
		Repair: "whatever repairs the shared classifier repairs this, and nothing else does",
		Why:    "a batched marker classifier over the shared response classifier",
	},
	"inflight_marker.go:(mqttMarkerLoad).observe": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "turning a marker load into an OBSERVATION, where the Absent kind becomes " +
			"mqttMarkerAbsent. The observation is what later reasoning treats as a fact",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Source: "inflight_marker.go:mqttMarkerLoadFromResponse",
		Repair: "inherited from the marker load; it adds no read of its own",
		Why:    "the mapping from load kind to observation is where absence becomes a fact",
	},
	"inflight_register.go:(mqttInflightSourceCache).resolve": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "mqttInflightSourceComplete — the source is treated as acknowledged — under a " +
			"comment asserting \"Authoritatively absent\". A failed read reaches that arm",
		ClassEv: classDefective, ConsEv: consDemonstrated, Reach: reachLive,
		Source: "inflight_register.go:mqttClassifyMsgGetResponse",
		Repair: "the consequential caller stages sourceConsumed and occupancyOwed for a RETIRED " +
			"register, so past-PUBREC evidence is settled by an absence a failed read " +
			"manufactures. No repair candidate is available until the classifier's is",
		Why: "the batched counterpart the roadmap names at inflight_register.go:4867",
	},
	"mqtt.go:(mqttAccountSessionManager).createOrRestoreSessionWithin": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "treating the session as ABSENT and creating a fresh one. The non-404 arm refuses, " +
			"so the fail-closed intent is present and defeated by the collapse",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Repair: "an exact-sequence re-attempt is unavailable — there is no sequence for a session " +
			"that may not exist — and a create-only CAS inherits F-110",
		Why: "the CONNECT path's own session-record read",
	},
	"mqtt.go:(mqttAccountSessionManager).evictDisplacedOnRemotePersist": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "continuing the eviction decision with no record in hand; a non-404 is WARNED and " +
			"the pass proceeds either way",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Repair: "a conditional re-attempt is inappropriate: this path decides nothing durable",
		Why:    "a bounded eviction read whose failure is logged rather than arbitrated",
	},
	"mqtt.go:(mqttAccountSessionManager).loadExpiringSessionRecord": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth:    "concluding the session was cleared or resumed-then-cleaned, ending the expiry work",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Repair: "none available: the absent arm has no sequence to re-attempt against",
		Why:    "session-expiry's own record read",
	},
	"mqtt.go:(mqttAccountSessionManager).reconcileRemoteSessionExpiry": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth:    "reconciling a remote expiry against a record it may simply have failed to read",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Repair: "none available on the terms this caller needs",
		Why:    "the remote-expiry counterpart",
	},
	"sess_gone_retire.go:(mqttAccountSessionManager).proveGoneRetirement": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "the LICENCE for gone-retirement. Its guard refuses a non-404 explicitly — and the " +
			"refusal cannot fire, because the failure arrives as a 404",
		ClassEv: classDefective, ConsEv: consDemonstrated, Reach: reachLive,
		Repair: "this is the shape a conditional re-attempt suits — entitlement — but the " +
			"create-only form inherits F-110 and no sequence exists for the exact-sequence form",
		Why: "the roadmap's own row: the most carefully written guard, defeated one frame earlier",
	},
	"sess_teardown.go:(mqttSession).claimTeardown": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth:    "mqttTeardownClaimSuperseded — abandoning the claim as already released",
		ClassEv: classDefective, ConsEv: consDemonstrated, Reach: reachLive,
		Repair: "none available: abandoning is the fail-open direction and has no sequence",
		Why:    "the roadmap's sess_teardown.go:235 row",
	},
	"sess_teardown.go:(mqttSession).releaseTeardown": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "mqttTeardownReleased — \"absent: done\" — which declares the teardown complete " +
			"and licenses the deletions that follow",
		ClassEv: classDefective, ConsEv: consDemonstrated, Reach: reachLive,
		Repair: "none available; the deletion it licenses is what makes this the worst row",
		Why:    "the roadmap's sess_teardown.go:1066 row",
	},
	"sess_uncertain.go:(mqttAccountSessionManager).reconcileResidentOwnership": {
		Kind: prDispoInterprets, Mapping: mapWithheld,
		Auth: "nothing: the non-404 arm returns and the next pass asks again. The 404 arm " +
			"proceeds, so a failed read is what it proceeds on",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Repair: "a later pass re-reads, which bounds the damage without arbitrating it",
		Why:    "withheld on the error it can see, defeated on the error it cannot",
	},
	"will_outbox.go:(mqttAccountSessionManager).loadFenceSession": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth:    "the Will fence acting on an absent session record",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Repair: "none available on the terms the fence needs",
		Why:    "the roadmap's will_outbox.go:1136 row",
	},
	"sess_fork_lineage.go:(mqttSession).disposalForDirectRejection": {
		Kind: prDispoInterprets, Mapping: mapWithheld,
		Auth: "mqttDisposalOwnershipUnsettled on ANY read failure or nil message, which is the " +
			"fail-closed direction: it settles no ownership",
		ClassEv: classSound, ConsEv: consNoneFound, Reach: reachLive,
		Repair: "none needed: it does not distinguish absence from failure and claims neither",
		Why:    "one of the few sites whose mapping is sound because it withholds on both",
	},
	"sess_resume_reconcile.go:classifyRecordAgainstAttempt": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth: "mqttRecordAttemptGone, from an error handed in as an ARGUMENT. It calls no " +
			"primitive, which is why an inventory keyed on loader calls cannot see it",
		ClassEv: classDefective, ConsEv: consDemonstrated, Reach: reachLive,
		Repair: "the CAS discipline it belongs to bounds it: a gone verdict routes to foreign, " +
			"which refuses rather than resumes",
		Why: "a classifier reached without a primitive call, named in the dossier",
	},
	"sess_uncertain.go:classifyAttemptReadback": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth:    "mqttAttemptGone, which the resolvers read as \"released by a teardown or expiry\"",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Source: "sess_resume_reconcile.go:classifyRecordAgainstAttempt",
		Repair: "inherited from the verdict it maps",
		Why:    "the second error-carrying parameter classifier the dossier names",
	},
	"sess_resume_reconcile.go:classifyRejectedProbe": {
		Kind: prDispoInterprets, Mapping: mapClassifiedAbsent,
		Auth:    "mqttRejectionForeign on a gone record, which disposes the connection",
		ClassEv: classDefective, ConsEv: consSuspected, Reach: reachLive,
		Source: "sess_resume_reconcile.go:classifyRecordAgainstAttempt",
		Repair: "inherited",
		Why:    "the third parameter classifier",
	},

	// ---- renderers: a sentinel appears in a String() or label, which decides nothing -------

	"qos2_retire_classify.go:(mqttQoS2RetireVerdict).String": {
		Kind: prDispoNotAnInterp,
		Why:  "renders a verdict name for a log line; no read, no decision",
	},
	"qos2_retire_classify.go:mqttRetirementStatusLabel": {
		Kind: prDispoNotAnInterp,
		Why:  "renders a retirement status name",
	},
	"sess_teardown.go:(mqttTeardownClaim).String": {
		Kind: prDispoNotAnInterp,
		Why:  "renders a teardown-claim name",
	},
	"sess_teardown.go:(mqttTeardownRelease).String": {
		Kind: prDispoNotAnInterp,
		Why:  "renders a teardown-release name",
	},
	"sess_uncertain.go:(mqttForcedLeaveOwnership).String": {
		Kind: prDispoNotAnInterp,
		Why:  "renders a forced-leave ownership name",
	},
	"sess_uncertain.go:(mqttUncertainResolution).String": {
		Kind: prDispoNotAnInterp,
		Why:  "renders an uncertain-resolution name",
	},
	"sess_fork_lineage.go:mqttDisposalForLineage": {
		Kind: prDispoNotAnInterp,
		Why: "maps a lineage boolean to a disposal vocabulary constant. It reads nothing and " +
			"receives no read result; the Unsettled value it returns is the caller's default",
	},
	"sess_uncertain.go:(mqttSession).resolutionAfterContinuations": {
		Kind: prDispoNotAnInterp,
		Why: "downgrades an Ours resolution to Unresolved when the sequence is still uncertain. " +
			"It consults local state only — no read, and no absence",
	},

	// ---- consumes: acts on a verdict another site produced from a per-subject read ---------
	//
	// The interpretation belongs to the producer named in Source. What each of these adds is
	// the CONSEQUENCE, which is what a repair has to satisfy and what the roadmap's own rows
	// mean by a consequential caller.

	"handoff_metadata.go:(mqttJSA).settleProposal": {
		Kind: prDispoConsumes, Source: "handoff_metadata.go:(mqttJSA).resolveMetadataAttempt",
		Auth: "settling an ownership proposal once the attempt resolver stops returning " +
			"Unresolved; an unresolved verdict keeps the proposal open rather than deciding it",
		Why: "the unresolved arm is a withhold, and the settle is what the resolved arms license",
	},
	"inflight_marker.go:(mqttAccountSessionManager).reapMarkerCaptureFirst": {
		Kind: prDispoConsumes, Source: "inflight_marker.go:mqttMarkerLoadFromResponse",
		Auth: "treating the sibling as no condition when the marker load is Absent, so the reap " +
			"proceeds with nothing to delete",
		Why: "row 24a's absent arm is reached by a failed read as readily as by a real absence",
	},
	"inflight_register.go:(mqttAccountSessionManager).reapIncarnationMarkers": {
		Kind: prDispoConsumes, Source: "inflight_marker.go:mqttMarkerLoadFromResponse",
		Auth: "continuing the reap past an Absent marker — one less thing to delete. Benign in " +
			"this direction: the pass destroys rather than reserves",
		Why: "its enumeration arm tests JSStreamNotFoundErr, which is not a point read",
	},
	"inflight_register.go:(mqttAccountSessionManager).reapIncarnationRegisters": {
		Kind: prDispoConsumes, Source: "inflight_register.go:mqttClassifyMsgGetResponse",
		Auth: "deleting ordinary PUBLISH registers with neither terminal nor marker evidence. " +
			"F-105 preserves this site verbatim as one of its two counterexamples",
		Why: "the absent arm is benign for deletion and the roadmap records why the SITE is not",
	},
	"inflight_register.go:(mqttSession).mqttSlotEvidenceFromRecord": {
		Kind: prDispoConsumes, Source: "inflight_marker.go:(mqttMarkerLoad).observe",
		Auth: "mqttSlotMarkerAbsent as slot EVIDENCE, which the slot classifier then reasons from",
		Why:  "evidence assembly over a marker load",
	},
	"inflight_register.go:(mqttSession).recoverInflightRegistersOnce": {
		Kind: prDispoConsumes, Source: "inflight_register.go:mqttClassifyMsgGetResponse",
		Auth: "omitting the register from the loaded set instead of marking the identifier space " +
			"incomplete. Its Indeterminate arm calls setPISpaceIncomplete precisely so a failed " +
			"read cannot free an identifier — and that arm CANNOT FIRE for a store-read failure",
		Why: "the roadmap's inflight_register.go:8770-8785 row: the guard is defeated one frame earlier",
	},
	"inflight_register.go:(mqttSession).settleLandedRegister": {
		Kind: prDispoConsumes, Source: "inflight_marker.go:mqttMarkerLoadFromResponse",
		Auth: "the diagnostic it reports when a just-committed pair's marker reads absent",
		Why:  "consumes the marker verdict to explain a landed register",
	},
	"mqtt.go:(Server).mqttProcessConnect": {
		Kind: prDispoConsumes, Source: "sess_uncertain.go:(mqttSession).resolveUncertainWrite",
		Auth: "rebuilding from a foreign record, or REFUSING the CONNECT on Unresolved. The " +
			"refusal is the fail-closed direction and is client-visible",
		Why: "the CONNECT path consumes the uncertainty resolver's verdicts",
	},
	"mqtt.go:(Server).mqttProcessPubRel": {
		Kind: prDispoConsumes, Source: "qos2_retire_classify.go:(mqttSession).classifyQoS2ReleaseInTx",
		Auth: "proceeding with the PUBREL when the retirement verdict is Proceed",
		Why:  "consumes a retirement verdict reached from a per-subject retirement read",
	},
	"mqtt.go:(mqttAccountSessionManager).evictDecideOnce": {
		Kind: prDispoConsumes, Source: "sess_fork_lineage.go:(mqttSession).disposalForDirectRejection",
		Auth: "an Unsettled disposal where the observation cannot be classified, which withholds " +
			"rather than concludes",
		Why: "its own comment states the withhold: an unreadable record says only that this " +
			"observation cannot be classified",
	},
	"mqtt.go:(mqttPreparedClear).destroy": {
		Kind: prDispoConsumes, Source: "sess_teardown.go:(mqttSession).releaseTeardown",
		Auth: "treating the shared durables as safe to consider once the release reads Released — " +
			"which is the absent-means-done verdict, so a failed read licenses the deletions",
		Why: "the consequential caller of the teardown release's absent arm",
	},
	"orphan_qos2_sweep.go:(mqttAccountSessionManager).settleRetiredAffineRegister": {
		Kind: prDispoConsumes, Source: "qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy",
		Auth: "retaining the register unless the copy reads Absent. EXPERIMENTAL: chapter P's",
		Why:  "re-reads rather than trusting a delete response, then acts on the copy verdict",
	},
	"outbound_close_enrol.go:(mqttJSA).closeGenerationEnrolled": {
		Kind: prDispoConsumes, Source: "outbound_transition.go:(mqttJSA).loadGenClosure",
		Auth: "continuing the close only where the closure status is Absent; anything else " +
			"licenses nothing. Withheld substrate: no production path admits through it",
		Why: "consumes the outbound closure status",
	},
	"outbound_connect_ownership.go:mqttDecideConnectOwnership": {
		Kind: prDispoConsumes, Source: "outbound_transition.go:(mqttJSA).loadOutboundAuthority",
		Auth: "a RESERVATION and nothing stronger on Absent. Withheld substrate",
		Why:  "consumes the authority load's status",
	},
	"outbound_inventory_boundary.go:(mqttJSA).checkInventoryAllocation": {
		Kind: prDispoConsumes, Source: "outbound_transition.go:(mqttJSA).loadGenClosure",
		Auth: "admitting the allocation where the closure reads Absent. Withheld substrate",
		Why:  "consumes the closure status at the inventory boundary",
	},
	"outbound_ops.go:(mqttJSA).requireAuthority": {
		Kind: prDispoConsumes, Source: "outbound_transition.go:(mqttJSA).loadOutboundAuthority",
		Auth: "returning a nil authority on Absent, which callers read as \"none recorded\". " +
			"Withheld substrate",
		Why: "the authority requirement's absent arm",
	},
	"outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts": {
		Kind: prDispoConsumes, Source: "outbound_transition.go:(mqttJSA).loadGenClosure",
		Auth: "leaving the discard fact UNKNOWN on an absent closure, which blocks the gate. The " +
			"function states in capitals that an unresolved read is an error and that treating " +
			"an unreadable closure as missing would take the vacuous path",
		Why: "reached on PRODUCTION paths through finishAdoptedTeardown, which is why the census " +
			"carries a live-read-unavailable-duty class rather than filing it as withheld",
	},
	"outbound_transition.go:(mqttJSA).checkProposedGenerationIsFresh": {
		Kind: prDispoConsumes, Source: "outbound_transition.go:(mqttJSA).loadGenClosure",
		Auth: "accepting the proposed generation as fresh on Absent. Withheld substrate",
		Why:  "consumes the closure status in the freshness check",
	},
	"qos2_retire_classify.go:(mqttSession).classifyQoS2ReleaseInTx": {
		Kind: prDispoConsumes, Source: "sess_teardown.go:(mqttJSA).loadGenRetirementRecord",
		Auth: "mqttQoS2RetireProceed on an absent retirement record — the release goes ahead. Its " +
			"own comment argues absence is absence here because F-066 refuses a lone recreation " +
			"of $MQTT_teardown; that argument is about a RECREATED STREAM, not about a failed read",
		Why: "the consequential caller of the retirement record's absent arm",
	},
	"sess_teardown.go:(mqttSession).recordGenRetirement": {
		Kind: prDispoConsumes, Source: "sess_teardown.go:(mqttJSA).loadGenRetirementRecord",
		Auth: "expect = 0 — a CREATE-ONLY CAS — when the retirement record reads Absent. That " +
			"makes this a joint F-109/F-110 site: the absence may be a failed read, and the " +
			"create-only admission does not establish the subject was empty either",
		Why: "the clearest example of an absence verdict turning directly into a zero expectation",
	},
	"sess_teardown.go:(mqttAccountSessionManager).finishAdoptedTeardown": {
		Kind: prDispoConsumes, Source: "sess_teardown.go:(mqttSession).releaseTeardown",
		Auth: "completing the adopted teardown on Released, and calling the release gate on a " +
			"PRODUCTION path",
		Why: "the adoption path's consumer of the same absent-means-done verdict",
	},
	"sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration": {
		Kind: prDispoConsumes, Source: "outbound_transition.go:(mqttJSA).loadGenClosure",
		Auth: "classifying the generation where the closure is Valid or Absent, and refusing " +
			"otherwise. Its comment — UNREADABLE IS NOT ABSENT — is exactly right and exactly " +
			"what the collapsed response defeats",
		Why: "a fail-closed intent defeated by the classification one frame earlier",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffCopy": {
		Kind: prDispoConsumes, Source: "qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy",
		Auth: "the materialization decision on an Absent copy. EXPERIMENTAL: chapter P's",
		Why:  "consumes the affine copy state",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).reapQ2AffClaimChecked": {
		Kind: prDispoConsumes, Source: "qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy",
		Auth: "reaping the claim where the copy reads Absent; it fails closed on Indeterminate. " +
			"EXPERIMENTAL",
		Why: "the copy state is what makes a claim reapable",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).recoverQ2AffClaims": {
		Kind: prDispoConsumes, Source: "qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy",
		Auth: "doing nothing unless the copy reads Absent. EXPERIMENTAL",
		Why:  "recovery's own consumption of the copy state",
	},
	"qos2_shared_affine.go:(mqttAccountSessionManager).settleSourcelessQ2AffClaim": {
		Kind: prDispoConsumes, Source: "qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy",
		Auth: "the DISCARD transition, gated on an Absent copy read LAST before the write. " +
			"EXPERIMENTAL, and the dossier names this as the consequential caller",
		Why: "the discard is what an absent copy licenses",
	},
	"sess_gone_retire.go:(mqttAccountSessionManager).retireGoneBoundSession": {
		Kind: prDispoConsumes, Source: "sess_gone_retire.go:(mqttAccountSessionManager).proveGoneRetirement",
		Auth: "the gone-retirement itself, with an Unsettled disposal as its default",
		Why:  "the consequential caller of the gone-retirement licence",
	},
	"sess_rejected_write.go:(client).mqttGateDispatchOnRejection": {
		Kind: prDispoConsumes, Source: "sess_rejected_write.go:(client).settleBoundRejectionGated",
		Auth: "closing the connection and notifying a foreign disposal on Disposed or Displaced",
		Why:  "the client-visible consequence of a rejection verdict",
	},
	"sess_rejected_write.go:(client).settleBoundRejectionGated": {
		Kind: prDispoConsumes, Source: "sess_resume_reconcile.go:classifyRejectedProbe",
		Auth: "mqttBoundDisposed or mqttBoundDisplaced, which decide who owns the Will action",
		Why:  "turns a rejection verdict into a bound-connection disposition",
	},
	"sess_rejected_write.go:(mqttSession).settleRecordRejection": {
		Kind: prDispoConsumes, Source: "sess_resume_reconcile.go:classifyRejectedProbe",
		Auth: "disposing the session on Foreign or Forked",
		Why:  "the disposal path for a rejected record write",
	},
	"sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave": {
		Kind: prDispoConsumes, Source: "sess_resume_reconcile.go:classifyRecordAgainstAttempt",
		Auth: "mqttRepairSaveForeign on a GONE record — \"an absence names no successor\" — which " +
			"refuses the save rather than resuming",
		Why: "the first consumer of the attempt classifier, on the pre-CONNACK path",
	},
	"sess_uncertain.go:(Server).resolveUncertainSession": {
		Kind: prDispoConsumes, Source: "sess_uncertain.go:(mqttSession).resolveUncertainWrite",
		Auth: "the disposal on Foreign or Forked, and leaving the session alone on Unresolved",
		Why:  "the server-level consumer of the resolver",
	},
	"sess_uncertain.go:(mqttAccountSessionManager).settleRejectionFromSweep": {
		Kind: prDispoConsumes, Source: "sess_resume_reconcile.go:classifyRejectedProbe",
		Auth: "treating the session as disposed on Foreign or Forked, so nothing else touches it",
		Why:  "the sweep's consumer of the rejection verdict",
	},
	"sess_uncertain.go:(mqttSession).reattemptUncertainWrite": {
		Kind: prDispoConsumes, Source: "sess_uncertain.go:classifyAttemptReadback",
		Auth: "Foreign on a gone subject, or Unresolved where the re-attempt is itself " +
			"unresolved. This is the ambiguous-write re-attempt, which settles by a CAS rather " +
			"than by a readback — and where that CAS is create-only it inherits F-110",
		Why: "the re-attempt consumes the readback classification rather than trusting a read",
	},
	"sess_uncertain.go:(mqttSession).resolveAcceptPair": {
		Kind: prDispoConsumes, Source: "sess_uncertain.go:classifyAttemptReadback",
		Auth: "Foreign on Gone — \"committed, then released by a teardown or an expiry\" — and " +
			"Unresolved on an unreadable acceptance fact",
		Why: "pairs the acceptance fact with the attempt verdict",
	},
	"sess_uncertain.go:(mqttSession).resolveUncertainNow": {
		Kind: prDispoConsumes, Source: "sess_uncertain.go:(mqttSession).resolveUncertainWrite",
		Auth: "the disposal on Foreign or Forked",
		Why:  "the session-level consumer",
	},
	"sess_uncertain.go:(mqttSession).resolveUncertainWrite": {
		Kind: prDispoConsumes, Source: "sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx",
		Auth: "the resolution the callers act on; it adds the fence and nothing else",
		Why:  "a fence wrapper around the in-transaction resolver",
	},
	"sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx": {
		Kind: prDispoConsumes, Source: "sess_accept_fact.go:(mqttJSA).readAcceptFact",
		Auth: "the whole resolution, including the paired-read rule where an ABSENT acceptance " +
			"fact plus a foreign, forked or gone attempt is what triggers the second read",
		Why: "the acceptance fact's absent arm is one half of the pairing this function exists for",
	},
	"sub_operation_gate.go:(client).mqttSettleLiveArbitration": {
		Kind: prDispoConsumes, Source: "sess_uncertain.go:(mqttSession).resolveUncertainWrite",
		Auth: "refusing to serve and disposing on Foreign, under the live subscription gate",
		Why:  "the live-arbitration consumer of the same resolver",
	},

	// ---- the shared-group lifecycle transfer (F-105) ----------------------------------------
	//
	// The two loaders loadSharedGroup and loadSharedLedger are declared NODES; everything below
	// either CONSUMES the classification one of them produced (a create-only CAS over apparent
	// absence, which is F-110's boundary) or is a sweep whose per-subject work reaches those
	// nodes while its own enumeration is F-108's.
	"shared_lifecycle_ops.go:(mqttSession).acquireShared": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "composing a join's ledger member create-only over an ABSENT ledger (the first " +
			"acquisition of a generation), and mounting a group create-only over an absent group " +
			"record. Both settle apparent absence with a create-only CAS, which is F-110's",
		Why: "the acquire loop reads both records and branches on the absent sentinel to create",
	},
	"shared_lifecycle_ops.go:(mqttSession).abortSharedAcquisition": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "writing the init fence create-only over an ABSENT ledger, and reading unlisted-" +
			"past-every-attempt as settled; the fence is a create-only CAS, F-110's",
		Why: "the abort settlement branches on the ledger's absent sentinel",
	},
	"shared_lifecycle_ops.go:(mqttJSA).installSharedTenure": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "installing the tenure create-only over an ABSENT ledger; a create-only CAS, F-110's",
		Why: "the installation composes over the absent sentinel",
	},
	"shared_lifecycle_ops.go:(mqttJSA).sealSharedLedger": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "sealing create-only over an ABSENT ledger (a generation that never acquired); F-110's",
		Why: "the seal composes over the absent sentinel",
	},
	"shared_lifecycle_ops.go:(mqttJSA).sharedClosingComplete": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "reading an ABSENT ledger as a complete closing (nothing to release); the fail-safe " +
			"direction, since a generation with no ledger acquired nothing",
		Why: "the closing gate branches on the ledger's absent sentinel",
	},
	"shared_lifecycle_ops.go:(mqttJSA).executeSharedRelease": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "reading an ABSENT ledger, or one that no longer lists the acquisition, as the " +
			"release having SETTLED — sound because the ledger is permanent, so true absence " +
			"means the generation never acquired",
		Why: "the release executor branches on the absent sentinel to report settled",
	},
	"shared_lifecycle.go:mqttComposeSharedJoin": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedGroup",
		Auth: "composing the group create-only over an ABSENT group record (incarnation 1) and " +
			"the ledger create-only over an absent ledger; the pure composer of acquireShared's " +
			"F-110 create-only writes",
		Why: "the join composer switches on both records' absent sentinels",
	},
	"shared_lifecycle.go:mqttComposeSharedInstall": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "composing the install/init-fence create-only over an ABSENT ledger; F-110's",
		Why: "the install composer switches on the ledger's absent sentinel",
	},
	"shared_lifecycle.go:mqttComposeSharedSeal": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedLedger",
		Auth: "composing the seal create-only over an ABSENT ledger; F-110's",
		Why: "the seal composer switches on the ledger's absent sentinel",
	},
	"shared_lifecycle.go:mqttSharedPriorMatches": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedGroup",
		Auth: "classifying the store's read of a batch's prior as a CAS rejection when it is " +
			"ABSENT-but-a-prior-was-expected or present-but-none-was; the boundary's prior-vs-store " +
			"compare, applied to both the group and ledger members of a batch",
		Why: "the boundary's prior compare branches on the absent sentinel",
	},
	"shared_lifecycle_ops.go:(mqttJSA).deleteSharedIncarnationConsumer": {
		Kind: prDispoNotAnInterp,
		Why: "tests JSConsumerNotFoundErr on a CONSUMER DELETE to classify the outcome gone; a " +
			"consumer-existence mechanism, not a message-GET 404",
	},
	"shared_lifecycle_sweep.go:(mqttAccountSessionManager).sweepSharedConsumers": {
		Kind: prDispoConsumes, Source: "shared_lifecycle.go:(mqttJSA).loadSharedGroup",
		Auth: "RETAINING a shared consumer (licenses no deletion) when its group record reads " +
			"ABSENT — the fail-safe direction: a consumer is created only after its join landed, " +
			"so an absent record attributes nothing. A deletion is authorized only by the record " +
			"reading the incarnation retired or derived-retired, a POSITIVE read",
		Why: "classifySharedConsumer branches on the group record's absent sentinel, and the " +
			"absent arm is the safe one; the delete's JSConsumerNotFoundErr is a third mechanism",
	},
	"shared_lifecycle_sweep.go:(mqttAccountSessionManager).sweepSharedGroupRetirements": {
		Kind: prDispoNotAnInterp,
		Why: "loadNextMsgFromSeq exhaustion walk over the group records; a finished retirement " +
			"reads the group record positively through loadSharedGroup (a census node) and " +
			"deletes on JSConsumerNotFoundErr, neither of which is this walk's absence",
	},
	"shared_lifecycle_sweep.go:(mqttAccountSessionManager).sweepAcqReleaseVerdicts": {
		Kind: prDispoNotAnInterp,
		Why: "loadNextMsgFromSeq exhaustion walk over the session stream; a verdict it executes " +
			"reaches the ledger through executeSharedRelease, which expands loadSharedLedger, and " +
			"the walk's own JSNoMessageFoundErr end is F-108's exhaustion question",
	},
}
