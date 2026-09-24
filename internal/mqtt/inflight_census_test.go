// The in-flight slot census: the authoritative record of every site that may rewrite,
// delete, or free a QoS 1/2 in-flight identifier, and the linter that keeps it honest.
//
// WHY THIS FILE EXISTS, stated first because its history is the whole justification.
// F-092's plan tried five times to enumerate this population in prose, and five review
// rounds each found another omission: a cardinality claim that did not match the tree,
// a rule excluding numeric leaves, an "exhaustive" list missing the landed gates, sub-IDs
// with no parent, and a replacement list that violated the rule it introduced. Each fix
// was individually correct and the next round found the next gap.
//
// So the enumeration is not prose. It is the manifest below plus a scanner that reads the
// package's own source, and the properties in this file are what make the manifest unable
// to be complete merely by agreeing with itself — the failure mode
// persisted_identity_ingress_test.go's own header names, and which this file inherits its
// shape from.
//
// # What this linter guarantees, and what it does NOT
//
// Seven properties, and they are the contract:
//
//  1. INDEPENDENT DISCOVERY of protected sites and of every arm of a sink-bearing
//     dispatch — read from the source, never from the manifest.
//  2. DISTINCT, CORRECTLY TYPED coverage relations for protected sites and for sinkless
//     arms. They are different relations. Collapsing them into one bijection is
//     incoherent, because a sinkless arm's row is actionable yet has no site to map to.
//  3. PER-SITE MULTIPLICITY preserved: a function containing two calls is two sites, never
//     one. persisted_identity_ingress_test.go's bijection collapses to the enclosing
//     function and must not be copied here for that reason.
//  4. EVERY ACTIONABLE ROW paired with its test, its mutation, and its immediate oracle.
//  5. PER-FAMILY and PER-DISPATCH ANTI-VACUITY. Not one global non-empty check: a global
//     floor lets a single renamed family go silent behind the others.
//  6. UNRESOLVED INDIRECTION AND BYPASSES FAIL CLOSED — a protected sink taken as a
//     method value, aliased, or reached through interface dispatch is an error, not a
//     pass.
//  7. A FINAL-MODE PROHIBITION on planned, unresolved, stale or unmatched entries, so a
//     transitional convenience cannot become a permanent escape hatch.
//
// It is NOT a soundness proof, and saying so is not modesty — it is the same threat model
// orphan_qos2_affine_delete_guard_test.go states for itself: Go AST analysis cannot
// resolve function values, interface dispatch or reflection, and making it sound would
// need closed-world type-checked SSA. This is an ALARM with a stated reach that fails
// closed on what it cannot resolve. THE ACCEPTANCE CONDITION IS BEHAVIOURAL — the
// per-mechanism tests and the mutations that kill them.
//
// # The coverage check's OWN scope, stated because it is narrower than it looks
//
// The source-to-census direction reports a protected site only in a function the census
// ALREADY names. A brand-new function containing a protected sink is therefore NOT caught
// automatically — it has no rows, so it is not in the censused set, so its sites are not
// compared. That scoping is deliberate (the package has many pre-existing functions that
// reach these sinks and are covered by their own guards), but it means the check catches a
// new EDGE in known code rather than a new FUNCTION. New functions are censused with the
// change that introduces them, which is what the admission half's own rows below are.
//
// # The completeness claim is SCOPED, deliberately
//
// It covers independently discoverable protected sites and the arms of sink-bearing
// dispatches. Nothing else. An operation-free control-flow site that sits outside any
// sink-bearing dispatch is NOT a census row: it cannot be discovered independently, and
// relabelling it would be a claim the scanner cannot substantiate. Such behaviour lives
// in its own behavioural test instead. Claiming closure over a set the scanner cannot
// enumerate is the defect this scoping avoids.

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// mqttCensusKind is what a manifest row asserts about its site.
type mqttCensusKind string

const (
	// censusParent is a heading: it has children and is never mutated itself.
	censusParent mqttCensusKind = "parent"
	// censusEdge is an independently mutable site that must be gated.
	censusEdge mqttCensusKind = "edge"
	// censusUnreachable claims the mutation is impossible for the states at issue, and
	// owes an interlock mutation PROVING it rather than asserting it.
	censusUnreachable mqttCensusKind = "unreachability"
	// censusNonEdge is a sinkless arm of a sink-bearing dispatch: it must stay
	// operation-free, and its mutation makes it act.
	censusNonEdge mqttCensusKind = "nonedge"
	// censusExemption is a site permitted to act outside the phase gate, with a POSITIVE
	// mutation on whatever fence licenses it.
	censusExemption mqttCensusKind = "exemption"
)

// mqttCensusStatus is whether the row's code exists on this tree yet.
//
// The transitional value is what resolves the chicken-and-egg of a census written before
// the mechanism it describes: a row may be `planned` while the admission half is being
// built, and the FINAL mode forbids every one of them. Without the distinction the census
// could only be written last, which is when nobody writes it.
type mqttCensusStatus string

const (
	censusLive    mqttCensusStatus = "live"
	censusPlanned mqttCensusStatus = "planned"
)

// mqttCensusRow is one census entry. The SELECTOR is a function name plus the callee
// spelling, deliberately not a line number: every line number in F-092's plan drifted at
// least once across ten review rounds.
type mqttCensusRow struct {
	id     string
	kind   mqttCensusKind
	parent string // empty for a top-level row
	fn     string // enclosing function or method name
	sink   string // the protected callee, or the map spelling; empty for parent rows
	status mqttCensusStatus
	// test, mutation and oracle are mandatory for every ACTIONABLE row (edge,
	// unreachability, nonedge, exemption). A parent row carries none.
	test     string
	mutation string
	oracle   string
	why      string
}

// mqttProtectedSinks is the set of callees through which a durable in-flight record, an
// identifier reservation, or a send holder may be observed or mutated — grouped into
// FAMILIES, because property 5 requires a floor per family rather than one global count.
//
// A family whose spelling changes goes to zero and fails its own floor, which is exactly
// what orphan_qos2_affine_delete_guard_test.go:261 fatals on: "the call was probably
// renamed and the guard must be updated with it".
var mqttProtectedSinks = map[string][]string{
	"durable-read": {
		"loadInflightRecord",
		"loadInflightRecordsMulti",
		"scanPubRelRecords",
	},
	"durable-write": {
		"storeInflightCAS",
		"casInflightSlot",
	},
	"durable-delete": {
		"deleteInflightRegister",
		"deleteInflightRegisterConfirmed",
		"reapRegisterWithVerdict",
		"reapRegisterWithVerdictGated",
		"reapRegisterGatedOutcome",
		"reapObservedRegister",
		"reapLoadedRegister",
		"deleteInflightRegisterFor",
		"releaseInflightRegisterByIdentity",
		"deleteMsg",
	},
	"identifier-allocator": {
		"bumpPI",
		"sendReservedLocked",
	},
	"holder-mutation": {
		"beginSendLocked",
		"beginPubRelSendLocked",
		"beginRetainedSendLocked",
		"endSend",
		"sendHoldLiveLocked",
	},
	"pending-map-mutation": {
		"untrackPublishEntry",
		"untrackPubRel",
		"trackAsPubRel",
		"retrackPublishLocked",
		"retireSourceLocked",
	},
}

// mqttProtectedMaps are the session fields whose RAW mutation is census-relevant even
// though no named helper is called.
//
// This family exists because a call-only scan proves "every call to a known helper is
// inventoried" and NOT "every census-relevant mutation goes through one" — the exact
// insufficiency F-092's second escalation consult identified. `delete(sess.srcAckOwed,
// key)` in resolveRetiredSource is the concrete case: a real census edge (C05a) that no
// sink call reaches, and the linter found it by failing rather than by anyone noticing.
//
// The scanner treats a `delete(recv.field, ...)` builtin on one of these as a site whose
// sink is spelled `delete:<field>`, which keeps it in the same site space as the call
// sinks and therefore under the same bijection and the same anti-vacuity floor.
var mqttProtectedMaps = []string{
	"srcAckOwed",
	"pendingPublish",
	"pendingPubRel",
	"cpending",
	"sendsInFlight",
}

// mqttCensusManifest is the authoritative census.
//
// SEEDED from F-092's plan tables, which are explicitly NOT authoritative — this is. A
// row whose mechanism is unbuilt carries `censusPlanned`; the final mode forbids those,
// so the census cannot ship half-written.
//
// Every actionable row owes a test, a mutation and an IMMEDIATE oracle. "Immediate" means
// the value the mutation changes is observed directly — slot sequence or state, map
// membership, custody, retirement membership, deletion-attempt count, staged-release
// length, candidate classification — never through a downstream guard that could mask it.
var mqttCensusManifest = []mqttCensusRow{
	// ---- LANDED at 77c2168: gated, but every gate currently reduces to "the shape is
	// supported" because `publish` is the only state. Each acquires a real per-state arm
	// with the admission half, so each needs its mutation RE-RUN against the enlarged
	// state set — which is why they are rows here and not assumed done.
	{
		id: "C01", kind: censusEdge, fn: "runInflightAdmission",
		sink: "reapObservedRegister", status: censusLive,
		test:     "TestMQTTInflightStoreReapsCancelledOccupant",
		mutation: "N21-C01", oracle: "slot presence at the observed sequence",
		why: "a pure cancellation tombstone is PUBLISH-state by construction; reaping past PUBREC would free an identifier the client holds",
	},
	{
		id: "C04", kind: censusEdge, fn: "runInflightAdmission",
		sink: "reapObservedRegister", status: censusLive,
		test:     "TestMQTTInflightStoreReapsProvablyCompleteRegister",
		mutation: "N21-C04", oracle: "slot presence at the observed sequence",
		why: "a different source whose own source is provably complete is reapable, but only from a state where reaping frees nothing",
	},
	{
		id: "C05", kind: censusParent, fn: "resolveRetiredSource", status: censusLive,
		why: "two independently mutable edges: the bookkeeping removal and the slot reap",
	},
	{
		id: "C05a", kind: censusEdge, parent: "C05", fn: "resolveRetiredSource",
		sink: "delete:srcAckOwed", status: censusLive,
		test:     "TestMQTTRetiredSourceBookkeepingRemovalIsSeparateFromTheReap",
		mutation: "N21-C05a", oracle: "srcAckOwed membership",
		why: "settling the source is permitted and is separately mutable from the reap",
	},
	{
		id: "C05b", kind: censusEdge, parent: "C05", fn: "resolveRetiredSource",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestMQTTRetiredSourceReapRefusesAPubRelStateSlot",
		mutation: "N21-C05b", oracle: "slot presence",
		why: "a PUBREL-state slot is RETAINED; clearing the source does not authorize deletion",
	},
	{
		id: "C08", kind: censusEdge, fn: "clearRetiredSource",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestMQTTClearRetiredSourceDoesNotAuthorizeDeletion",
		mutation: "N21-C08", oracle: "slot presence",
		why: "clearing the retired SOURCE is this path's business; the slot is not",
	},
	{
		id: "C09", kind: censusEdge, fn: "markInflightRegisterRetired",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestMQTTMarkRetiredIsLegalFromEveryKnownState",
		mutation: "N21-C09", oracle: "the record's Retired field and its sequence",
		why: "the mark is about the SOURCE, not the phase, so it is legal from any known state",
	},
	{
		id: "C19", kind: censusEdge, fn: "disposeSettledAffineRegister",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestOrphanSweepCannotMutateAnActiveSessionSlot",
		mutation: "N21-C19", oracle: "slot presence",
		why: "orphan processing may dispose of a settled register but not an active or terminal session slot",
	},
	{
		id: "C20", kind: censusParent, fn: "reapRegisterWithVerdictGated", status: censusLive,
		why: "the admission check and the delete are separately mutable",
	},
	{
		id: "C20a", kind: censusEdge, parent: "C20", fn: "reapRegisterGatedOutcome",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestMQTTGatedReapRefusesAnUnpermittedTransition",
		mutation: "N21-C20a", oracle: "the deletion-attempt count",
		why: "the identity/phase admission check, ahead of any delete",
	},
	{
		id: "C20b", kind: censusEdge, parent: "C20", fn: "reapRegisterGatedOutcome",
		sink: "reapRegisterWithVerdict", status: censusLive,
		test:     "TestMQTTGatedReapDeletesOnlyThePermittedSequence",
		mutation: "N21-C20b", oracle: "slot presence at the observed sequence",
		why: "the delete itself, addressed by subject and identity rather than a remembered sequence",
	},
	{
		id: "C20c", kind: censusEdge, parent: "C20", fn: "reapRegisterWithVerdictGated",
		sink: "reapRegisterGatedOutcome", status: censusLive,
		test:     "TestMQTTInflightDestructiveDispositionsRefuseALiveAction",
		mutation: "M91", oracle: "slot presence at the observed sequence",
		why: "the boolean form delegates to the one classified door and adds no decision beyond refusing a moved sequence for a never-transmitted delete; a second copy of the checks here is how the two would drift",
	},
	{
		id: "C21", kind: censusParent, fn: "deleteInflightRegisterFor", status: censusLive,
		why: "the reference-capture fail-closed and the delegation are NOT C20's internals; this helper has exactly one delegation",
	},
	{
		id: "C21a", kind: censusNonEdge, parent: "C21", fn: "deleteInflightRegisterFor",
		sink: _EMPTY_, status: censusLive,
		test:     "TestMQTTDeleteInflightRegisterForFailsClosedWithNoCapturedSlot",
		mutation: "N21-C21a", oracle: "the deletion-attempt count",
		why: "with no captured snapshot it must refuse rather than fall back to a sequence-addressed delete, which is the addressing F-034 forbids",
	},
	{
		id: "C21b", kind: censusEdge, parent: "C21", fn: "deleteInflightRegisterFor",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestMQTTDeleteInflightRegisterForCarriesItsEntrysReference",
		mutation: "N21-C21b", oracle: "the reference passed to the helper",
		why: "the delegation carries the entry's own captured reference and an explicit disposition",
	},
	{
		id: "C26", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestMQTTRecoveryDropIsGated",
		mutation: "N21-C26", oracle: "slot presence",
		why: "recovery's post-scan drop; the scan-time verdict is not authority at apply time",
	},
	{
		id: "C27", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestMQTTRecoveryDropByNonceIsGated",
		mutation: "N21-C27", oracle: "slot presence",
		why: "same correction as C26, with confirmation forced",
	},
	{
		id: "C28", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestMQTTRecoverySecondRetirementDropIsGated",
		mutation: "N21-C28", oracle: "slot presence",
		why: "recovery proved the source, which does not authorize vacating a later phase",
	},
	{
		id: "C22", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "scanPubRelRecords", status: censusLive,
		test:     "TestMQTTInflightRecoveryScanFailureReservesTheIdentifierSpace",
		mutation: "N21-C22", oracle: "piSpaceIncomplete after a partial PUBREL scan",
		why: "the pre-CONNACK PUBREL census must be complete before absence can classify a " +
			"slot as publish-only or finished",
	},
	{
		id: "C22b", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "loadInflightRecordsMulti", status: censusLive,
		test:     "TestMQTTInflightRecoveryBatchReadFailureReservesTheIdentifierSpace",
		mutation: "N21-C22b", oracle: "the batched-read edge and piSpaceIncomplete after its failure",
		why: "an enumerated register is not absent when its batched subject read is indeterminate; " +
			"the client may still hold its identifier",
	},
	{
		id: "C22c", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "loadInflightRecordsMulti", status: censusLive,
		test:     "TestMQTTInflightMarkerBatchReadFailureReservesTheIdentifierSpace",
		mutation: "N21-C22c", oracle: "piSpaceIncomplete after a failed marker batch read",
		why: "the marker walk's batched load (F-072): an enumerated marker whose read is indeterminate " +
			"leaves its register's transmission state unknown, so the identifier space is withheld",
	},
	{
		id: "C22d", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "loadInflightRecordsMulti", status: censusLive,
		test:     "TestMQTTInflightMarkerAbsenceIsNotInterpretedAgainstAStaleSnapshot",
		mutation: "N21-C22d", oracle: "the reread replaces the walk snapshot (rows 5-9)",
		why: "the per-record register reread for an absent marker (F-072, N-5.6): absence is never " +
			"interpreted against the cached snapshot, and a failed reread withholds rather than installs",
	},
	{
		id: "C22e", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestMQTTInflightRecoveryWalkRaceFailsClosed",
		mutation: "N21-C22e", oracle: "reclassification or a withheld identifier space",
		why: "the two-walk case (F-072, N-5.6): a marker whose register the snapshot did not see loads " +
			"its sibling authoritatively — a present sibling reclassifies, an unresolved read fails closed",
	},
	{
		id: "C39", kind: censusParent, fn: "releaseInflightRegisterByIdentity", status: censusLive,
		why: "two deletion edges: the initial one and the rewritten-record one the reread loop reaches",
	},
	{
		id: "C39a", kind: censusEdge, parent: "C39", fn: "releaseBoundInflightRegister",
		sink: "reapLoadedRegister", status: censusLive,
		test:     "TestMQTTIdentityReleaseInitialEdgeIsGated",
		mutation: "N21-C39a", oracle: "slot presence at the observed sequence",
		why: "the initial deletion edge, gated through the widened contract",
	},
	{
		id: "C39b", kind: censusEdge, parent: "C39", fn: "releaseBoundInflightRegister",
		sink: "reapLoadedRegister", status: censusLive,
		test:     "TestMQTTIdentityReleaseRewrittenEdgeIsGated",
		mutation: "N21-C39b", oracle: "slot presence at the observed sequence",
		why: "the reread loop deletes again and is separately mutable",
	},
	// ---- THE ADMISSION HALF'S OWN EDGES. Added with the code that created them, which is
	// what the linter's coverage check would otherwise only notice for functions the
	// census already spoke about (see the scoping note in this file's header).
	{
		id: "C16b", kind: censusEdge, fn: "claimRecoveredInflight",
		sink: "storeInflightCAS", status: censusLive,
		test:     "TestMQTTRecoveryCustodyRefreshUsesPositiveEligibilitySet",
		mutation: "N21-C16b", oracle: "the slot's Claim after resume",
		why: "the ordered PRE-CONNACK custody contest shared by the positive transferable " +
			"publish and pubrel_ready sets. It covers every state admission can acquire from, " +
			"including a ready slot with an in-memory entry and the narrow retired-ready set " +
			"backed by an exact PUBREL record; active, terminal, flag-only retired and cancelled " +
			"slots receive no custody write",
	},
	{
		id: "C16br", kind: censusEdge, fn: "claimRecoveredInflight",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestClaimRecoveredSharedInflightRefusesAMismatchedOccupant",
		mutation: "N21-C16br", oracle: "the claim outcome and occupant sequence",
		why: "the ordered custody contest rereads a rejected or ambiguous CAS before deciding " +
			"whether this owner landed, a newer owner displaced it, the slot closed, or the " +
			"occupant is opaque; guessing any of those from the failed write is unsafe",
	},
	{
		id: "C29a4", kind: censusEdge, fn: "phaseSlotToPubRelReady",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestMQTTLivePubRecAdvancesOwnerSlotToPubRelReady",
		mutation: "M-phase", oracle: "the durable slot's state and canonical tuple after a live PUBREC",
		why: "the PUBREC handoff's `publish -> pubrel_ready` arrow, which REPLACED the register " +
			"delete that used to end a register's life at PUBREC. Discriminated by STALLING PUBREL " +
			"delivery so only this synchronous handoff (not the direct arrow) can advance the slot; " +
			"a broken readback (wrong store subject, or a QoS mismatch in the shape check) leaves it " +
			"in publish and fails the test",
	},
	// ---- THE ADMISSION HALF'S NEW SINK-BEARING FUNCTIONS. A holistic re-review found these
	// escaped the census (new functions are not auto-discovered; they are censused with the
	// change that introduces them). Each reaches exactly the sink(s) named here.
	{
		id: "C62", kind: censusEdge, fn: "admitStoredPubRel",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092AdmissionRequiresAnAtomicallyAcquiredAction",
		mutation: "N-adm-read", oracle: "admission's verdict on an indeterminate slot read",
		why: "the admission read: the slot that owns the identifier is loaded and an " +
			"indeterminate read refuses rather than being guessed into an absence",
	},
	{
		id: "C63b", kind: censusEdge, fn: "advanceFinishedSlotToTerminal",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092UnresolvedTerminalAdvanceRefusesTheResume",
		mutation: "N-advance-readback", oracle: "the resume's accept/refuse outcome per readback class",
		why: "THE OUTCOME READBACK. A failed CAS returns one `false` for a storage failure, a " +
			"disappearance, a replacement and a moved phase, and those owe the resume different " +
			"things: not-ours continues, unresolved REFUSES the resume rather than accepting a " +
			"connection whose identifier stays charged with nothing left to retry the advance",
	},
	{
		id: "C67", kind: censusEdge, fn: "transferCustodyForAdmission",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestF092AdmissionTransfersCustodyAfterPeerDrain",
		mutation: "N-custody-transfer", oracle: "the slot's Claim/ClaimSeq after an admissible custody-only refusal",
		why: "the inline custody repair: a quiescent slot whose ONLY failing admission check is " +
			"custody is moved to the proven-current incarnation, under the exact observed claim " +
			"tuple and only when the standing claim is strictly older than the proven acceptance",
	},
	{
		id: "C63", kind: censusEdge, fn: "advanceFinishedSlotToTerminal",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestMQTTInflightStaleRegisterLeavesNoPhantomSlot",
		mutation: "N-advance", oracle: "the identifier's reuse after a proven-finished recovery",
		why: "the advance a PROOF-of-finish licenses: a `pubrel_ready` slot recovery has proven " +
			"finished goes to `pubrel_terminal`, which then licenses the delete the phase gate " +
			"refuses directly from `pubrel_ready`",
	},
	{
		id: "C64", kind: censusEdge, fn: "resolveUnacquiredAction",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092UnreadableActionSlotNeverRetiresItsOnlyHolder",
		mutation: "N-unacq-read", oracle: "the tentative holder's fate on an unreadable reread",
		why: "an unacquired action's reread: the tentative holder is retired only once the reread " +
			"proves this token absent; an unreadable or indeterminate read keeps it",
	},
	{
		id: "C65a", kind: censusEdge, fn: "retireActionLocked_",
		sink: "delete:sendsInFlight", status: censusLive,
		test:     "TestF092ActionDrainRequiresTheExactToken",
		mutation: "N-retire-del", oracle: "the sendsInFlight entry's presence after a drain",
		why: "the action holder's in-memory removal: only the exact token's holder is removed",
	},
	{
		id: "C65b", kind: censusEdge, fn: "retireActionLocked_",
		sink: "sendHoldLiveLocked", status: censusLive,
		test:     "TestF092ActionDrainRequiresTheExactToken",
		mutation: "N-retire-live", oracle: "the liveness check guarding the removal",
		why: "the liveness predicate consulted before removing the action term, distinct from " +
			"the delete it guards",
	},
	{
		id: "C66", kind: censusEdge, fn: "releaseTerminalReservation",
		sink: "delete:pendingPubRel", status: censusLive,
		test:     "TestF092TerminalReleaseFreesTheIdentifier",
		mutation: "N-relterm", oracle: "the terminal reservation placeholder's removal",
		why: "the terminal reservation's EXACT nonce-bound removal: it frees the identifier only " +
			"once cleanup is confirmed, and never a later delivery that reused the map key",
	},
	{
		id: "C37a", kind: censusEdge, fn: "phaseRecoveredPublishSlotToReady",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestF092RecoveryRoutesPublishPlusExactPubRelBeforeSourceCompletion",
		mutation: "N21-C37a", oracle: "the recovered slot state and canonical tuple",
		why: "recovery route B's `publish -> pubrel_ready` arrow, licensed only by the " +
			"present exact PUBREL selected from a complete scan",
	},
	{
		id: "C37ar", kind: censusEdge, fn: "phaseRecoveredPublishSlotToReady",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092RecoveryRoutesPublishPlusExactPubRelBeforeSourceCompletion",
		mutation: "N21-C37ar", oracle: "the post-CAS slot lineage and phase",
		why: "a rejected or ambiguous route-B CAS is resolved only from an authoritative " +
			"reread of the same exchange already past PUBREC; the write result alone proves no handoff",
	},
	{
		id: "C37a2", kind: censusEdge, fn: "phaseRecoveredPublishSlotToReady",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestF105RouteBRecordsTheRecoveredPubRec",
		mutation: "M25", oracle: "the slot at pubrel_ready beside an exchange at PUBREC-observed",
		why: "the ORDINARY route-B rebind, from pubrel_intended once recordRecoveredPubRec has " +
			"recorded the PUBREC the stored record proves; never a slot-only arrow from publish",
	},
	{
		id: "C37ar2", kind: censusEdge, fn: "phaseRecoveredPublishSlotToReady",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF105RouteBRecordsTheRecoveredPubRec",
		mutation: "M25", oracle: "the rebind starts from the recorded pubrel_intended slot",
		why: "the read between the recorded PUBREC and the rebind: the rebind CASes the slot as " +
			"the PUBREC arrow left it, identity-checked, never the pre-record body",
	},
	{
		id: "C49", kind: censusEdge, fn: "acquireActionFromPublish",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestF092AdmissionRequiresAnAtomicallyAcquiredAction",
		mutation: "M5b", oracle: "the canonical tuple on the slot after transmission",
		why: "THE DIRECT ARROW: `publish -> pubrel_sending` installing the canonical identity and " +
			"the action token in ONE CAS, so evidence and authorization land together",
	},
	{
		id: "C49b", kind: censusEdge, fn: "mqttAcquirePubRelAction",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestF092StalePubRelUnderAnotherExchangesIdentifierIsRefused",
		mutation: "M1", oracle: "the PUBREL transmission count",
		why: "the ordinary `pubrel_ready -> pubrel_sending` acquisition",
	},
	{
		id: "C35a", kind: censusEdge, fn: "mqttTerminalizeOnPubComp",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestF092TerminalReleaseFreesTheIdentifier",
		mutation: "M2", oracle: "slot presence and bumpPI's next identifier",
		why: "PUBCOMP's ONE arrow to `pubrel_terminal`, after which cleanup may finish the " +
			"exchange. It is one arrow rather than two because F-098 took the live action out " +
			"of the phase: a sibling row here carried the `pubrel_sending -> " +
			"pubrel_terminal_draining` arrow, which existed only to keep a phase and a token in " +
			"step, and PUBCOMP now takes this arrow whether or not a write is out. What that " +
			"row protected — PUBCOMP frees NOTHING while a PUBREL's bytes are on the wire — is " +
			"unchanged, and is enforced by mqttSlotReleaseAuthority withholding on ANY live token",
	},
	{
		id: "C35c", kind: censusEdge, fn: "startActionDrain",
		sink: "casInflightSlot", status: censusLive,
		test:     "TestMQTTInflightPubCompDuringABlockedPubRelWrite",
		mutation: "M-drain-target", oracle: "the slot's state after the write returns",
		why: "the action's drain, whose TARGET IS DERIVED FROM THE STATE OBSERVED rather than " +
			"remembered from acquisition — a PUBCOMP arriving mid-write moves the slot underneath " +
			"the holder, and an arrow chosen earlier then fails forever",
	},
	{
		id: "C35d", kind: censusEdge, fn: "finalizeTerminalSlotObserved",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestF092TerminalReleaseFreesTheIdentifier",
		mutation: "M2", oracle: "slot presence",
		why: "the terminal EXACT deletion, which needs three things and not two: subject " +
			"addressing (F-034), identity authorization, and a FRESHNESS FENCE on the observed " +
			"sequence — plus the source-suppression obligation, which retains the slot while " +
			"the source is live or its status is unknown",
	},
	{
		id: "C29c", kind: censusParent, fn: "handOffPublishEntryLocked", status: censusLive,
		why: "F-093's ordered handoff: two map edges, and the ORDER is the correctness property",
	},
	{
		id: "C29c2", kind: censusEdge, parent: "C29c", fn: "handOffPublishEntryLocked",
		sink: "delete:pendingPublish", status: censusLive,
		test:     "TestF093CompletedExchangeLeavesNoPublishEntryOrMapping",
		mutation: "M3", oracle: "pendingPublish membership and the capacity charge",
		why: "the compare-delete of the PUBLISH entry, AFTER the owner and the suppression are in " +
			"place — removing it first would leave the identifier with no reservation at all",
	},
	{
		id: "C29c3", kind: censusEdge, parent: "C29c", fn: "handOffPublishEntryLocked",
		sink: "delete:cpending", status: censusLive,
		test:     "TestF093CompletedExchangeLeavesNoPublishEntryOrMapping",
		mutation: "M3", oracle: "the cpending mapping for the source",
		why: "the compare-delete of the source mapping, GUARDED on the identifier it names — " +
			"untrackPublishEntry deletes it unconditionally and would remove a mapping a " +
			"different identifier now owns",
	},
	{
		id: "C29c1", kind: censusEdge, parent: "C29c", fn: "handOffPublishEntryLocked",
		sink: "retireSourceLocked", status: censusLive,
		test:     "TestMQTTDrainPubRelRecreatedFromIntent",
		mutation: "M-retire-scope", oracle: "the PUBREL acknowledgement and the duplicate count",
		why: "the source suppression, installed BEFORE any removal and only where this handoff " +
			"owns the source. Recorded as already-durable because a past-PUBREC slot IS that " +
			"durable statement; marking it insecure made PUBCOMP withhold the acknowledgement " +
			"for a fact the slot already implied, and the client got a duplicate PUBREL",
	},
	{
		id: "C37c", kind: censusParent, fn: "recoverInflightRegistersOnce", status: censusLive,
		why: "route B's in-memory handoff after the durable phase CAS: remove only the exact " +
			"same PUBLISH exchange and its guarded source mapping",
	},
	{
		id: "C37c2", kind: censusEdge, parent: "C37c", fn: "recoverInflightRegistersOnce",
		sink: "delete:pendingPublish", status: censusLive,
		test:     "TestF092RecoveryRoutesPublishPlusExactPubRelBeforeSourceCompletion",
		mutation: "N21-C37c2", oracle: "pendingPublish and pendingPubRel membership",
		why: "a reused session object's exact PUBLISH entry must not survive beside the " +
			"durable slot that route B already advanced to PUBREL",
	},
	{
		id: "C37c3", kind: censusEdge, parent: "C37c", fn: "recoverInflightRegistersOnce",
		sink: "delete:cpending", status: censusLive,
		test:     "TestF092RecoveryRoutesPublishPlusExactPubRelBeforeSourceCompletion",
		mutation: "N21-C37c3", oracle: "the exact source-to-identifier mapping",
		why: "the route-B PUBLISH mapping is compare-deleted with its entry so a later " +
			"source offer cannot route back into the completed handoff",
	},
	{
		id: "C35ar", kind: censusEdge, fn: "mqttTerminalizeOnPubComp",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092TerminalReleaseFreesTheIdentifier",
		mutation: "N21-C35ar", oracle: "the arrow taken for each observed state",
		why: "the read that classifies the observed phase for PUBCOMP, and whose indeterminate " +
			"outcome must neither free nor delete: the client has answered, so what remains is a " +
			"durable slot recovery finishes",
	},
	{
		id: "C35ar2", kind: censusEdge, fn: "mqttTerminalizeOnPubComp",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF105PubCompResolvesALostResponseInline",
		mutation: "M12", oracle: "the completion is recorded inline after a landed-lost final attempt",
		why: "the EXHAUSTION read: a failed final attempt may hide a batch that landed, so the " +
			"completion is decided from the slot as it stands rather than from the attempt count",
	},
	{
		id: "C35dr", kind: censusEdge, fn: "finalizeTerminalSlotObserved",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092TerminalReleaseFreesTheIdentifier",
		mutation: "N21-C35dr", oracle: "slot presence under a rewritten sequence",
		why: "the terminal read, which carries the FRESHNESS FENCE: a rewrite between observation " +
			"and delete means a concurrent operation re-targeted the exchange, so a stale " +
			"continuation preserves the record",
	},
	{
		id: "C35dr2", kind: censusEdge, fn: "finalizeTerminalSlotObserved",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092TerminalFinalizationRereadsTheFreshnessFence",
		mutation: "N21-C35dr2", oracle: "slot presence after a rewrite during duplicate disposal",
		why: "the final reread moves the freshness fence to the irreversible edge: a rewrite " +
			"while the complete PUBREL scan is running preserves the re-targeted slot",
	},
	{
		id: "C35dr3", kind: censusEdge, fn: "finalizeTerminalSlotObserved",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF092TerminalReleaseFreesTheIdentifier",
		mutation: "N21-C35dr3", oracle: "the slot subject is absent before the local reservation is released",
		why: "the post-delete consequence check: an exact-sequence delete may succeed while a " +
			"same-identity rewrite survives at the reusable subject, so memory is released only " +
			"after the subject is absent or names a different exchange",
	},
	{
		id: "C35rt", kind: censusEdge, fn: "clearTerminalRetirementLocked_",
		sink: "delete:srcAckOwed", status: censusLive,
		test:     "TestF093CompletedExchangeLeavesNoPublishEntryOrMapping",
		mutation: "N21-C35rt", oracle: "srcAckOwed membership after terminal cleanup",
		why: "a terminal exchange whose source is positively non-reofferable releases its " +
			"matching in-memory suppression only after the slot deletion is confirmed",
	},
	{
		id: "C35ds", kind: censusEdge, fn: "disposeTerminalPubRelRecords",
		sink: "scanPubRelRecords", status: censusLive,
		test:     "TestF092IncompleteTerminalDuplicateScanRetainsTheSlot",
		mutation: "N21-C35ds", oracle: "terminal slot presence",
		why: "terminal deletion is licensed only by a complete scan of every PUBREL record " +
			"on the incarnation subject; partial enumeration is never evidence of absence",
	},
	{
		id: "C35dp", kind: censusEdge, fn: "disposeTerminalPubRelRecords",
		sink: "deleteMsg", status: censusLive,
		test:     "TestF092TerminalDuplicateDisposalPrecedesSlotDeletion",
		mutation: "N21-C35dp", oracle: "duplicate record and terminal slot presence",
		why: "each valid same-nonce canonical or duplicate record is removed with confirmation, " +
			"after affine proof discharge and before the owner slot is deleted",
	},
	{
		id: "C35cr", kind: censusEdge, fn: "startActionDrain",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestMQTTInflightPubCompDuringABlockedPubRelWrite",
		mutation: "N21-C35cr", oracle: "the drain arrow chosen per observed state",
		why: "the drain's per-attempt read, which is what makes the target derived rather than " +
			"remembered; a different valid token or an absent slot both prove this action is gone",
	},
	{
		id: "C35cr2", kind: censusEdge, fn: "startActionDrain",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestF105DrainCompletionFinalizesFromTheSlotSequence",
		mutation: "M16", oracle: "the slot is deleted inline from its own sequence",
		why: "the read after an acknowledged drain completion: the op returns the AUTHORITY " +
			"sequence, and finalize's freshness fence needs the slot's own",
	},
	// ---- DISCOVERED BY THE LINTER, not by any review round. Twelve protected sites in
	// functions the census already spoke about, which ten plan rounds of prose
	// enumeration never named. Every one is `planned`: the site exists today, its
	// per-state gate and its mutation belong to the admission half, and the final mode
	// refuses to let any of them ship unresolved. This block is the census earning its
	// keep on the day it was written.
	{
		id: "C08m", kind: censusEdge, fn: "clearRetiredSource",
		sink: "delete:srcAckOwed", status: censusLive,
		test:     "TestMQTTInflightRetiredSourceSurvivesPubComp",
		mutation: "N21-C08m", oracle: "srcAckOwed membership",
		why: "C08's sibling: the retirement bookkeeping removal, separately mutable from the reap it precedes",
	},
	{
		id: "C19d", kind: censusEdge, fn: "disposeSettledAffineRegister",
		sink: "deleteInflightRegister", status: censusLive,
		test:     "TestOrphanSweepRegisterArmDeletesTheRegisterAfterASuccessfulReap",
		mutation: "N21-C19d", oracle: "slot presence",
		why: "the affine disposer's own sequence-addressed delete, distinct from C19's gated reap",
	},
	{
		id: "X01r", kind: censusEdge, fn: "reconcileOrphanInflight",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestOrphanSweepRegisterArmRevalidatesTheExactSequence",
		mutation: "N21-X01r", oracle: "the delete-attempt count for an indeterminate read",
		why: "X01's read: an indeterminate load must not be guessed into the absence that licenses the delete",
	},
	{
		id: "C23", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "delete:pendingPublish", status: censusLive,
		test:     "TestMQTTInflightClaimConsumedCloseRetiresLiveMemory",
		mutation: "N21-C23", oracle: "pendingPublish membership",
		why: "recovery's consumed-entry map removal, ahead of any source removal",
	},
	{
		id: "C24", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "delete:srcAckOwed", status: censusLive,
		test:     "TestMQTTInflightRecoveryDropsCompletedSource",
		mutation: "N21-C24", oracle: "srcAckOwed membership",
		why: "recovery's retirement removals; TWO sites, so two rows — see C24b",
	},
	{
		id: "C24b", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "delete:srcAckOwed", status: censusLive,
		test:     "TestMQTTInflightRecoveryPreservesARetiredRegisterWhoseSourceCompleted",
		mutation: "N21-C24b", oracle: "srcAckOwed membership",
		why: "the second of recovery's two retirement removals; multiplicity is part of the inventory",
	},
	{
		id: "C25a", kind: censusEdge, fn: "recoverInflightRegistersOnce",
		sink: "retireSourceLocked", status: censusLive,
		test:     "TestMQTTInflightRecoveryPubRelRetiresRegisterWhenSourceLive",
		mutation: "N21-C25a", oracle: "the retirement's durable flag and the entry's actionability",
		why: "recovery reinstalling a retirement: must become a withheld reservation for an active slot, never an actionable owner",
	},
	{
		id: "C39ar", kind: censusEdge, fn: "releaseBoundInflightRegister",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestInflightIdentityReleaseContract",
		mutation: "N21-C39ar", oracle: "the delete-attempt count",
		why: "C39a's read: an unreadable or indeterminate occupant authorizes nothing",
	},
	{
		id: "C39br", kind: censusEdge, fn: "releaseBoundInflightRegister",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestInflightIdentityReleaseFollowsARewrite",
		mutation: "N21-C39br", oracle: "the delete-attempt count",
		why: "C39b's reread, separately mutable: the reread loop reads again before deleting again",
	},
	{
		id: "C02r", kind: censusEdge, fn: "runInflightAdmission",
		sink: "loadInflightRecord", status: censusLive,
		test:     "TestMQTTInflightStoreRefusesLiveForeignRegister",
		mutation: "N21-C02r", oracle: "the store-attempt outcome",
		why: "the store-conflict loop's read, on which every arm below it acts",
	},
	{
		id: "C56", kind: censusEdge, fn: "runInflightAdmission",
		sink: "retireSourceLocked", status: censusLive,
		test:     "TestMQTTStoreConflictRetirementAdoptionRefusesAPubRelSlot",
		mutation: "N21-C56", oracle: "srcAckOwed membership and the abandon count",
		why: "the retirement-ADOPTION arm: must not adopt-and-abandon over a PUBREL-state slot, where the allocation is not a fresh first transmission",
	},
	{
		id: "C02w", kind: censusEdge, fn: "storeInflightRegister",
		sink: "storeInflightCAS", status: censusLive,
		test:     "TestMQTTStoreConflictReplacementRefusesAPastPubRecSlot",
		mutation: "N21-C02w", oracle: "the occupant's nonce at the observed sequence",
		why: "C02's replacement arm: a rewrite over a past-PUBREC slot destroys the identifier's only durable authority as surely as a delete",
	},
	{
		id: "C55m", kind: censusEdge, fn: "confirmInflightRegister",
		sink: "delete:pendingPublish", status: censusLive,
		test:     "TestMQTTConfirmDropsOnlyItsOwnUnconfirmedAllocation",
		mutation: "N21-C55m", oracle: "pendingPublish membership",
		why: "the confirm path's own map removal, separately mutable from C55's durable delete",
	},
	{
		id: "C55", kind: censusEdge, fn: "confirmInflightRegister",
		sink: "reapRegisterWithVerdictGated", status: censusLive,
		test:     "TestMQTTConfirmedDeleteIsIdentityAddressed",
		mutation: "N21-C55", oracle: "an unrelated victim's nonce at the remembered sequence",
		why: "a landed store whose allocation disappeared is deleted only after a fresh subject read " +
			"proves the same exchange is still in publish; an absent, replaced or past-PUBREC slot " +
			"must never be destroyed by the remembered sequence",
	},
	{
		id: "X01", kind: censusExemption, fn: "reconcileOrphanInflight",
		sink: "deleteInflightRegister", status: censusLive,
		test:     "TestF036OrphanSweepRetainsAnUnlicensedRegister",
		mutation: "N21-X01-positive", oracle: "a live other-generation register's presence",
		why: "whole-generation teardown, permitted ONLY behind the retirement watermark; its mutation is POSITIVE — remove the proof and a live generation's register is deleted",
	},
}

// censusSite is one discovered call site. The FN plus SINK plus ORDINAL is the identity,
// which is what preserves multiplicity: two calls to the same sink in one function are
// two sites, and collapsing them is what property 3 forbids.
type censusSite struct {
	fn   string
	sink string
}

// censusScanProtectedSites parses every non-test file in this package and returns each
// protected-sink call site with its enclosing function, KEEPING COUNTS.
//
// This is the independent enumeration: it reads the source, not the manifest.
func censusScanProtectedSites(t *testing.T) (sites map[censusSite]int, files int, unresolved []string) {
	t.Helper()
	return censusScanSinkFamilies(t, mqttProtectedSinks)
}

// censusScanSinkFamilies is censusScanProtectedSites parameterized by the families to look
// for, so a second census over a different sink set reuses this scanner rather than growing
// a near-copy of it. The two-pass call-target discipline below is the whole value here, and
// two copies of it would drift apart on the first fix.
func censusScanSinkFamilies(t *testing.T, families map[string][]string) (sites map[censusSite]int, files int, unresolved []string) {
	t.Helper()
	sinkFamily := map[string]string{}
	for family, names := range families {
		for _, n := range names {
			sinkFamily[n] = family
		}
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	sites = map[censusSite]int{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		files++

		// TWO PASSES, and the reason is worth stating because the one-pass version of
		// this scanner was a false-positive generator. An ordinary method call
		// `sess.reapRegisterWithVerdict(...)` is a CallExpr whose Fun IS a
		// SelectorExpr, so a walk that reports every sink-named SelectorExpr reports
		// every ordinary call as an indirection — a check that fires on everything
		// discriminates nothing. So pass 1 records which expressions are call targets,
		// and pass 2 reports only the sink-named selectors that are NOT one.
		callTargets := map[ast.Expr]struct{}{}
		ownedIdents := map[*ast.Ident]struct{}{}
		ast.Inspect(f, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.CallExpr:
				callTargets[d.Fun] = struct{}{}
			case *ast.FuncDecl:
				// A method's OWN name is not an indirection. An early version of this
				// scanner reported every protected helper as taking itself as a value.
				ownedIdents[d.Name] = struct{}{}
			case *ast.SelectorExpr:
				// The `.Sel` of a selector is OWNED by that selector and is never an
				// independent value reference. Without this, `sess.reap...(x)` fires:
				// the selector is correctly recognised as a call target, and then the
				// walk descends into its own `.Sel` Ident, which is not.
				ownedIdents[d.Sel] = struct{}{}
			}
			return true
		})

		var fn string
		ast.Inspect(f, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.FuncDecl:
				fn = d.Name.Name
			case *ast.CallExpr:
				callee := censusCalleeName(d.Fun)
				if callee == _EMPTY_ {
					return true
				}
				// A RAW map mutation, which no call sink reaches. See
				// mqttProtectedMaps for why this family is not optional.
				if callee == "delete" && len(d.Args) > 0 {
					// Both shapes: `delete(sess.pendingPublish, pi)` and the NESTED
					// `delete(sess.cpending[dur], seq)`. Missing the second is not a
					// detail — cpending is only ever mutated through its inner map, so a
					// selector-only pattern sees none of those sites at all.
					target := d.Args[0]
					if idx, ok := target.(*ast.IndexExpr); ok {
						target = idx.X
					}
					if sel, ok := target.(*ast.SelectorExpr); ok {
						for _, m := range mqttProtectedMaps {
							if sel.Sel.Name == m {
								sites[censusSite{fn: fn, sink: "delete:" + m}]++
							}
						}
					}
				}
				if _, ok := sinkFamily[callee]; ok {
					sites[censusSite{fn: fn, sink: callee}]++
				}
			case *ast.SelectorExpr:
				// PROPERTY 6, failing closed: a protected sink named OUTSIDE a call
				// position — taken as a method value, aliased, or handed to something
				// that will dispatch it later. This scanner cannot follow that, and
				// the honest response is to name it rather than pass.
				if _, isCall := callTargets[ast.Expr(d)]; isCall {
					return true
				}
				if _, ok := sinkFamily[d.Sel.Name]; ok {
					unresolved = append(unresolved, fn+" takes "+d.Sel.Name+" as a value")
				}
			case *ast.Ident:
				// The same rule for a package-level function taken as a value — but
				// NOT for a declaration's own name, and not for a call target.
				if _, isCall := callTargets[ast.Expr(d)]; isCall {
					return true
				}
				if _, owned := ownedIdents[d]; owned {
					return true
				}
				if _, ok := sinkFamily[d.Name]; ok {
					unresolved = append(unresolved, fn+" takes "+d.Name+" as a value")
				}
			}
			return true
		})
	}
	return sites, files, unresolved
}

// censusCalleeName is the callee's own name for an ordinary or method call, empty for
// anything this scanner will not claim to resolve.
func censusCalleeName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return f.Sel.Name
	}
	return _EMPTY_
}

// TestInflightCensusScanIsNotVacuous is ANTI-VACUITY LAYERS 1 AND 2 (property 5).
//
// A silent parse failure or a renamed sink family makes every other assertion in this
// file agree with nothing, and a GLOBAL non-empty check would let one renamed family hide
// behind the others. So: the file count is asserted, and then EVERY family has its own
// floor.
func TestInflightCensusScanIsNotVacuous(t *testing.T) {
	t.Parallel()
	sites, files, _ := censusScanProtectedSites(t)
	if files == 0 {
		t.Fatal("the census scan parsed no production files at all, so every assertion in " +
			"this file is vacuous — the directory read or the file filter is wrong")
	}
	if len(sites) == 0 {
		t.Fatal("the census scan found no protected-sink call sites anywhere in the package, " +
			"which cannot be right: either the parse silently failed or the sink set no " +
			"longer matches the code")
	}
	seen := map[string]int{}
	for site, n := range sites {
		for family, names := range mqttProtectedSinks {
			for _, name := range names {
				if site.sink == name {
					seen[family] += n
				}
			}
		}
		if strings.HasPrefix(site.sink, "delete:") {
			seen["raw-map-mutation"] += n
		}
	}
	var bare []string
	for family := range mqttProtectedSinks {
		if seen[family] == 0 {
			bare = append(bare, family)
		}
	}
	// The raw-map family has its own floor for the same reason every call family does:
	// if the field is renamed, this whole category silently stops being checked.
	if seen["raw-map-mutation"] == 0 {
		bare = append(bare, "raw-map-mutation")
	}
	sort.Strings(bare)
	for _, family := range bare {
		t.Errorf("no call to any sink in family %q was found anywhere in the package, so this "+
			"census checks nothing for it — the calls were probably renamed and "+
			"mqttProtectedSinks must be updated with them", family)
	}
}

// TestInflightCensusRowsAreStructurallySound is the STRUCTURAL half of the linter:
// property 2's typing, property 4's pairing, and the parent-row rule five plan rounds
// failed to maintain in prose.
func TestInflightCensusRowsAreStructurallySound(t *testing.T) {
	t.Parallel()
	byID := map[string]mqttCensusRow{}
	for _, r := range mqttCensusManifest {
		if r.id == _EMPTY_ {
			t.Fatalf("a census row has no id: %+v", r)
		}
		if _, dup := byID[r.id]; dup {
			t.Errorf("census id %s appears more than once", r.id)
		}
		byID[r.id] = r
	}
	hasChild := map[string]bool{}
	for _, r := range mqttCensusManifest {
		if r.parent == _EMPTY_ {
			continue
		}
		// Every sub-ID's parent row EXISTS. Fifteen parents lived only in prose in the
		// plan's revision 8, which is the defect this assertion retires.
		if _, ok := byID[r.parent]; !ok {
			t.Errorf("census row %s names parent %s, which has no row of its own", r.id, r.parent)
			continue
		}
		hasChild[r.parent] = true
	}
	for _, r := range mqttCensusManifest {
		switch r.kind {
		case censusParent:
			// A parent is a HEADING: it owes no mutation, and it must actually have
			// children or it is a heading over nothing.
			if !hasChild[r.id] {
				t.Errorf("census row %s is a %s but no row names it as a parent", r.id, censusParent)
			}
			if r.test != _EMPTY_ || r.mutation != _EMPTY_ || r.oracle != _EMPTY_ {
				t.Errorf("census row %s is a %s and must not carry a test, mutation or oracle", r.id, censusParent)
			}
		case censusEdge, censusUnreachable, censusNonEdge, censusExemption:
			// PROPERTY 4: every actionable row owes all three, and an oracle is what
			// makes the mutation discriminate rather than pass against a default.
			if r.test == _EMPTY_ {
				t.Errorf("census row %s is actionable (%s) and names no test", r.id, r.kind)
			}
			if r.mutation == _EMPTY_ {
				t.Errorf("census row %s is actionable (%s) and names no mutation", r.id, r.kind)
			}
			if r.oracle == _EMPTY_ {
				t.Errorf("census row %s is actionable (%s) and names no immediate oracle", r.id, r.kind)
			}
			if hasChild[r.id] {
				t.Errorf("census row %s is actionable (%s) but also has children, so it is a "+
					"heading and a leaf at once", r.id, r.kind)
			}
		default:
			t.Errorf("census row %s has unknown kind %q", r.id, r.kind)
		}
		if r.why == _EMPTY_ {
			t.Errorf("census row %s states no reason, so a later reader cannot tell what it "+
				"is protecting", r.id)
		}
	}
}

// TestInflightCensusLiveRowsNameRealTests closes a different vacuity hole from the
// source-site checks below. A non-empty string is not evidence that the promised
// behavioural test exists: a typo, rename, or a planned name copied into a live row
// otherwise leaves the census paper-green while providing no executable oracle.
func TestInflightCensusLiveRowsNameRealTests(t *testing.T) {
	t.Parallel()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	declared := map[string]bool{}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, name, nil, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			declared[fn.Name.Name] = true
		}
	}
	for _, r := range mqttCensusManifest {
		if r.kind == censusParent || r.status != censusLive || r.test == _EMPTY_ {
			continue
		}
		if !declared[r.test] {
			t.Errorf("live census row %s promises behavioural test %s, but no such test is declared", r.id, r.test)
		}
	}
}

// TestInflightCensusLiveRowsStillExist is ANTI-VACUITY LAYER 3 (property 5), and it is
// the direction that catches a census describing code that has moved.
//
// orphan_qos2_affine_delete_guard_test.go states the reason exactly: without it "the
// assertions above pass against code that no longer contains what they describe".
// Scoped to `live` rows, because a `planned` row's code deliberately does not exist yet —
// and the final mode below is what stops `planned` from becoming permanent.
func TestInflightCensusLiveRowsStillExist(t *testing.T) {
	t.Parallel()
	sites, _, _ := censusScanProtectedSites(t)
	for _, r := range mqttCensusManifest {
		if r.kind == censusParent || r.status != censusLive || r.sink == _EMPTY_ {
			continue
		}
		if sites[censusSite{fn: r.fn, sink: r.sink}] == 0 {
			t.Errorf("census row %s says %s calls %s, and no such call exists — the census is "+
				"describing code that has moved, so its gate assertion is checking a "+
				"predicate nothing satisfies", r.id, r.fn, r.sink)
		}
	}
}

// TestInflightCensusSiteBijection is PROPERTY 3 — multiplicity — and PROPERTY 2's
// site-side relation.
//
// Storing counts is not asserting them: an earlier version of this file preserved
// per-site multiplicity in the scan and then only ever asked whether a site existed,
// which is `persisted_identity_ingress_test.go:459`'s own defect reproduced one level up.
// So the count is compared. Two calls to one sink in one function need TWO rows.
//
// Scoped to `live` rows in both directions, because a `planned` row describes code that
// does not exist yet and an unbuilt sink has no site to match. The final mode is what
// stops that scoping from becoming a hole.
func TestInflightCensusSiteBijection(t *testing.T) {
	t.Parallel()
	sites, _, _ := censusScanProtectedSites(t)

	// Manifest -> source: how many rows ACCOUNT FOR each (fn, sink).
	//
	// Both statuses count here, and the distinction is deliberate. A discovered site is
	// accounted for once the census KNOWS about it; whether its gate and mutation are
	// built is what `live` versus `planned` says, and the final mode is what forbids
	// `planned` from surviving into a commit. Counting only live rows would make an
	// honestly-recorded outstanding edge indistinguishable from one nobody noticed —
	// which is the whole failure this census exists to end.
	claimed := map[censusSite]int{}
	for _, r := range mqttCensusManifest {
		if r.kind == censusParent || r.sink == _EMPTY_ {
			continue
		}
		claimed[censusSite{fn: r.fn, sink: r.sink}]++
	}

	for site, want := range claimed {
		if got := sites[site]; got != want {
			t.Errorf("the census claims %d live row(s) for %s / %s but the source has %d call "+
				"site(s): multiplicity must match, because a function with two calls to one "+
				"sink is two independently mutable edges and collapsing them loses one",
				want, site.fn, site.sink, got)
		}
	}

	// Source -> manifest, restricted to functions the census already speaks about. A
	// WHOLLY UNCENSUSED sink-bearing function is deliberately NOT flagged here or by any
	// other test — see this file's header ("The coverage check's OWN scope"): the package
	// has many pre-existing functions reaching these sinks under their own guards, so the
	// scan compares a new EDGE in known code, and a new FUNCTION is censused by the change
	// that introduces it. (The admission half's own new sink-bearing functions are censused:
	// e.g. claimRecoveredInflight's storeInflightCAS is C16b/C16br.) This restriction keeps
	// that deliberate scope explicit rather than promising a check that does not exist.
	censusedFn := map[string]bool{}
	for _, r := range mqttCensusManifest {
		censusedFn[r.fn] = true
	}
	var uncovered []string
	for site, got := range sites {
		if !censusedFn[site.fn] {
			continue
		}
		if claimed[site] != got {
			uncovered = append(uncovered,
				site.fn+" / "+site.sink+" has "+itoa(got)+" site(s) and "+itoa(claimed[site])+" row(s)")
		}
	}
	sort.Strings(uncovered)
	for _, u := range uncovered {
		t.Errorf("a censused function has protected sites the census does not account for: %s", u)
	}
}

// itoa keeps the assertion messages allocation-simple; strconv would do, but this file
// deliberately has no dependency the scanner itself does not need.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestInflightCensusHasNoUnresolvedIndirection is PROPERTY 6, and it FAILS CLOSED.
//
// A protected sink taken as a method value, aliased, or dispatched through an interface is
// something this scanner cannot follow. The honest response is an error naming it, not a
// silent pass — because a bypass the linter cannot see is exactly the omitted edge the
// whole census exists to catch.
func TestInflightCensusHasNoUnresolvedIndirection(t *testing.T) {
	t.Parallel()
	_, _, unresolved := censusScanProtectedSites(t)
	sort.Strings(unresolved)
	for _, u := range unresolved {
		t.Errorf("a protected sink is reached by indirection this census cannot resolve (%s), "+
			"so it must either be called directly or added to the census with an explicit "+
			"exemption — an unresolvable bypass fails closed", u)
	}
}

// TestInflightCensusIsFinal is PROPERTY 7, and it is SKIPPED except in final mode.
//
// F-092's admission half is built incrementally, so a `planned` row is legitimate while
// the mechanism it describes is unbuilt. What must never happen is that convenience
// surviving into the commit: this test is the gate that says the census is complete, and
// Phase 7 runs it.
//
// Enabled by MQTT_CENSUS_FINAL=1, exactly as the experimental shared-QoS-2 gate is
// enabled, so the ordinary suite stays green during implementation while the final check
// is one flag away.
func TestInflightCensusIsFinal(t *testing.T) {
	if os.Getenv("MQTT_CENSUS_FINAL") != "1" {
		t.Skip("final-mode census check: set MQTT_CENSUS_FINAL=1 (Phase 7 runs it)")
	}
	var planned []string
	for _, r := range mqttCensusManifest {
		if r.status == censusPlanned {
			planned = append(planned, r.id)
		}
	}
	sort.Strings(planned)
	if len(planned) != 0 {
		t.Errorf("the census still carries %d planned row(s) — %s — so the mechanism they "+
			"describe is unbuilt and this change is not complete",
			len(planned), strings.Join(planned, ", "))
	}
}
