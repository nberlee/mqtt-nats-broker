// The interpretation surface, enumerated from SOURCE rather than from a list.
//
// Producer closure was reopened three times during F-034's plan review, each time by a
// transition class deeper inside JetStream: cross-account sourcing and TTL eviction,
// then stream lifecycle and configuration, then server-internal delivery and timer
// redelivery. Each addition was individually correct, and that is the problem —
// "enumerate every durable-state transition" enumerates JetStream's implementation and
// has no dry-out criterion, so a fourth round can always add snapshotting or
// pause/unpause.
//
// So closure is anchored to the BRIDGE'S INTERPRETATION SURFACE instead: a transition
// is in scope if and only if it can change the outcome of an enumerated interpretation
// site. Delivery and redelivery then enter through the reads and callbacks that OBSERVE
// them rather than as classes of their own — which is also how deduplication already
// enters, and exactly why pubrel_correlation.go carries an attempt counter instead of
// trusting a dedup window.
//
// That yields the completion test the transition model lacked: a new row is required
// only for a new interpretation SITE, a new observable, a new outcome partition, or a
// new authority — not for a newly discovered nats-server function.
//
// This file is the mechanical half. It parses the package's own source with go/ast and
// requires every call to a JetStream ingress method to sit in a function the manifest
// names. A new call site in a new function fails by name. The enumeration is derived
// from the AST, never from the manifest, so the manifest cannot be complete merely by
// agreeing with itself — which is the failure mode round 5 rejected when it killed an
// earlier "documented ledger plus a count" fallback.
//
// See docs/findings/persisted-identity-census.md for the census itself.

package mqtt

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// censusIngressMethods are the mqttJSA methods through which every JetStream
// observation and mutation reaches the bridge. The unit of the census is the semantic
// interpretation SITE rather than the transport wrapper — one generic loadMsg feeds
// several materially different interpretations — so this set is the ingress, and the
// manifest below is what assigns each call site its interpretation.
var censusIngressMethods = map[string]string{
	// Reads: every one of these has a four-way outcome partition at minimum —
	// present, absent, unreadable, indeterminate — and loadInflightRecord documents
	// it explicitly ("INDETERMINATE, never to be guessed into an absence").
	"loadLastMsgFor":       "read one subject's last message",
	"loadLastMsgForMulti":  "batched read of many subjects' last messages",
	"loadMsg":              "read one message by sequence",
	"loadMsgsMulti":        "batched read by sequence",
	"loadNextMsgFromSeq":   "read forward from a sequence",
	"loadSessionMsg":       "read a session record",
	"streamSubjects":       "enumerate subjects and take the stream watermark",
	"lookupStream":         "read stream configuration and state",
	"lookupConsumer":       "read one consumer's configuration and state",
	"lookupConsumersMulti": "batched consumer read",
	"consumerList":         "list a stream's consumers",

	// Writes, whose RESPONSE is itself interpreted: success, error, the returned
	// sequence, and duplicate status all change bridge behaviour.
	"storeMsg":         "store a message and interpret its ack",
	"storeMsgResult":   "classify a store response",
	"storeSessionMsg":  "store a session record",
	"storeAcceptBatch": "store an acceptance fact and a record in one atomic batch",

	// Destructive operations, whose outcome partition includes "already absent",
	// which several callers must read as SUCCESS.
	"deleteMsg":      "delete an exact sequence",
	"purgeStream":    "purge by filter",
	"deleteConsumer": "delete a consumer by name",

	// Consumer creation, which for an existing durable is an UPDATE, so its response
	// carries configuration and state the bridge interprets.
	"createEphemeralConsumer": "create or update an ephemeral consumer",
	"createDurableConsumer":   "create or update a durable consumer",
	"createStream":            "create a stream",
	"updateStream":            "update a stream configuration",

	// Acks, which mutate durable consumer state. confirmAck's own doc is the
	// evidence: it waits "until the consumer state has advanced".
	"sendAck":    "acknowledge a delivery, fire and forget",
	"confirmAck": "acknowledge a delivery and wait for the consumer state to advance",
	"sendMsg":    "publish without interpreting a response",

	// ---- the SEMANTIC wrappers, and they are the reason this set is not just the
	// transport layer.
	//
	// A mutation caught this: M5b added a new function calling loadInflightRecord and
	// the bijection PASSED, because loadInflightRecord is a package wrapper rather than
	// an mqttJSA method. So anchoring the surface at the transport would let a whole
	// class of new interpretation site through — precisely the drift this test exists
	// to catch. Consult 3 had already said so: the surface includes "specialized
	// wrappers in other files", and "the unit is the semantic interpretation site, not
	// the transport wrapper".
	//
	// These are the wrappers that establish an outcome PARTITION of their own, so a new
	// caller of one is a new interpretation site even though it touches no jsa method
	// directly.
	"loadInflightRecord":                "the four-outcome register read",
	"storeInflightCAS":                  "the register CAS",
	"enumerateInflightSubjects":         "the bounded register enumeration",
	"deleteInflightRegister":            "the fire-and-forget register delete",
	"deleteInflightRegisterConfirmed":   "the confirmed register delete, absent-is-success",
	"inflightSourceState":               "the source classification whose 'complete' is the only reap licence",
	"loadQ2AffClaim":                    "the affine claim read, fail-closed on absence",
	"storeQ2AffClaimCAS":                "the affine claim election CAS",
	"loadQ2AffCopy":                     "the copy presence proof that authorizes a source ack",
	"storeQ2AffCopy":                    "the copy store, which must converge on a presence proof",
	"withdrawQ2AffCopy":                 "the copy withdrawal at an exact sequence",
	"loadQoS2Record":                    "the inbound hold read",
	"storeQoS2HoldCAS":                  "the hold create-only CAS",
	"storeQoS2DoneCAS":                  "the hold terminal advance",
	"releaseQoS2Record":                 "the hold release at an exact sequence",
	"loadQoS2RelClaim":                  "the release-claim read",
	"storeQoS2RelClaimCAS":              "the release-claim CAS",
	"reapQoS2RelClaim":                  "the release-claim reap",
	"loadGenRetirementRecord":           "the retirement watermark read, fail-closed when corrupt",
	"storeGenRetirementCAS":             "the retirement watermark CAS",
	"loadWillOutbox":                    "the Will outbox read, which refuses an uninterpretable record",
	"storeWillOutboxCAS":                "the Will outbox CAS",
	"storeFramedSessionMsg":             "the session record store",
	"sessLastSeq":                       "the session stream watermark read",
	"confirmInflightSourceAck":          "the confirmed source ack",
	"persistRecord":                     "the session record CAS and its rejection classification",
	"enumerateInflightSubjectsBounded":  "the bounded register enumeration, returning the watermark a destructive pass is entitled to",
	"loadInflightRecordsMulti":          "the reap's batched register read",
	"deleteInflightRegistersPipelined":  "the reap's bounded-concurrency confirmed deletes",
	"reapIncarnationRegisters":          "the teardown's bounded reap of one incarnation's registers",
	"releaseInflightRegisterByIdentity": "the identity-conditioned, SUBJECT-addressed register release (F-034): six outcomes, of which mismatch PRESERVES the occupant and unreadable-or-indeterminate withholds the caller's progress",
}

// censusIngressSite is one call site, as the AST sees it.
type censusIngressSite struct {
	method string
	fn     string // the enclosing function or method name
}

// censusScanIngress parses every non-test file in this package and returns each call
// to an ingress method with its enclosing function. This is the independent
// enumeration: it reads the source, not the manifest.
func censusScanIngress(t *testing.T) []censusIngressSite {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	fset := token.NewFileSet()
	var out []censusIngressSite
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			decl, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			fname := decl.Name.Name
			if decl.Recv != nil && len(decl.Recv.List) > 0 {
				// Keep the receiver out of the name: a method's identity for this
				// audit is what it does, and two types rarely share a method name
				// here. Where they do, the manifest carries both.
				fname = decl.Name.Name
			}
			ast.Inspect(decl.Body, func(n2 ast.Node) bool {
				call, ok := n2.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if _, ok := censusIngressMethods[sel.Sel.Name]; !ok {
					return true
				}
				out = append(out, censusIngressSite{method: sel.Sel.Name, fn: fname})
				return true
			})
			return false
		})
	}
	return out
}

// TestPersistedIdentityIngressSurfaceIsEnumerated is the dump-and-check half: it
// asserts the scan finds a plausible surface at all, and reports it in a stable form
// so a reviewer can read what the census claims to cover.
//
// It deliberately does NOT assert an exact count. A count assertion is the thing round
// 5 refused — it "can easily prove only that the ledger agrees with itself" — so the
// discriminating assertion is the per-function bijection in the next test.
func TestPersistedIdentityIngressSurfaceIsEnumerated(t *testing.T) {
	t.Parallel()
	sites := censusScanIngress(t)
	if len(sites) == 0 {
		t.Fatal("the AST scan found no JetStream ingress call sites at all, which cannot be right: " +
			"either the parse silently failed or the ingress method set no longer matches the code")
	}
	byMethod := map[string]int{}
	for _, s := range sites {
		byMethod[s.method]++
	}
	// Every method the census claims to be an ingress must actually be called
	// somewhere, or the set has drifted from the code and the census is auditing a
	// surface that no longer exists.
	var unused []string
	for m := range censusIngressMethods {
		if byMethod[m] == 0 {
			unused = append(unused, m)
		}
	}
	sort.Strings(unused)
	for _, m := range unused {
		// updateStream is the audited exception: it has no production caller, which
		// is the code-level form of ARCHITECTURE's "nothing is repaired or updated in
		// place". Recorded rather than removed, so that if a caller ever appears the
		// census notices.
		if m == "updateStream" {
			continue
		}
		t.Errorf("ingress method %q is in the census's surface but has no non-test call site: "+
			"either it was removed or the surface has drifted", m)
	}
}

// censusInterpreters is the manifest: every function that reaches a JetStream ingress
// method, with the interpretation it performs there.
//
// The bijection below runs against the AST scan in BOTH directions, so this table
// cannot be complete by agreeing with itself. A new function calling an ingress method
// fails by name; an entry naming a function that no longer calls one fails too.
//
// The value is the interpretation, not a restatement of the method: what the outcome
// partition MEANS at this site, and in particular what the destructive and
// recovery-authorizing outcomes are. Where a site's own file already states the
// partition, the entry points at it rather than paraphrasing.
var censusInterpreters = map[string]string{
	// ---- the transport layer itself -------------------------------------------
	// These are the wrappers. Their "interpretation" is the outcome PARTITION they
	// establish for everyone above them, which is why they are sites in their own
	// right rather than plumbing.
	"storeMsg":                        "classifies a store response into landed / definitely failed / ambiguous; the ambiguous arm is what forbids reading a response shape as a verdict",
	"sendAck":                         "fire-and-forget ack: a lost ack costs one redelivery, which is the right trade everywhere a durable record is NOT deleted on the strength of it",
	"deleteConsumer":                  "delete by name; JSConsumerNotFoundErr is SUCCESS, and every caller must read it that way or a repeated teardown wedges",
	"deleteInflightRegister":          "fire-and-forget register delete at an exact sequence; a failed delete leaves a stale record that three other reapers remove (R5)",
	"deleteInflightRegisterConfirmed": "confirmed register delete; treats an already-absent sequence as SUCCESS, which is load-bearing for the drain's finalize",
	"loadInflightRecord":              "the four-outcome partition this whole census generalises: empty, present-but-unreadable, readable, indeterminate — and indeterminate is never guessed into an absence",
	"storeInflightCAS":                "create-only or exact-sequence CAS on a register, honouring a test seam so an injected fault cannot perturb the source or PUBREL stores",
	"enumerateInflightSubjects":       "enumerates one incarnation's register subjects; complete by construction because a 16-bit identifier bounds the filter well under JSMaxSubjectDetails",
	"inflightSourceState":             "classifies a register's SOURCE as complete, live or unknown; 'complete' is the only licence to reap, and a consumer-gone source is deliberately NOT complete",
	"confirmInflightSourceAck":        "confirms a source ack under the attempt's own reply subject, so a retirement is settled rather than left owing",
	"sessLastSeq":                     "reads the session stream's last sequence as a bound, never as a claim about where enumerated subjects sit",
	"loadSessionMsg":                  "reads a session record; absence means no session, which is a positive answer here rather than an unknown",
	"storeFramedSessionMsg":           "stores a framed session record and returns the CAS outcome for the classifier above it",
	"mqttStoreSessionRecordCASBody":   "builds and stores the record CAS body; the returned sequence becomes the next expectation",

	// ---- stream and consumer provisioning -------------------------------------
	"ensureBrokerStream":                 "create-or-lose then an unconditional authoritative RE-READ, so a create-race loser and the winner both validate what actually exists rather than what they asked for",
	"brokerStreamSpecs":                  "the ordered provisioning table; the epoch stamp is taken at the create, which is the linearization point",
	"createOrRestoreSessionWithin":       "reads the record and the retirement watermark; a retired generation is DISCARDED rather than resumed, and an unreadable watermark refuses the CONNECT",
	"mqttRefuseRetiredOriginStream":      "a lookup-only preflight: the presence of a retired stream is itself the refusal, since ensureBrokerStream would otherwise create an absent one before validating",
	"mqttProvisionEphemeralConsumers":    "provisions the per-broker ephemeral consumers as a UNIT, so a partial success is rolled back rather than left half-installed",
	"rollbackEphemeralConsumers":         "deletes both ephemeral consumer names including the FAILING create's own, because an error is not proof nothing was created",
	"processJSConsumer":                  "creates or adopts a plain QoS 1/2 consumer; an existing durable makes the create an UPDATE, and the response's config is what the record then persists",
	"processSharedJSConsumer":            "joins the shared group consumer; membership is persisted BEFORE the create so a concurrent leaver's last-holder scan sees this joiner",
	"createQ2AffineConsumer":             "creates the session-affine QoS 2 consumer; an immutable-field mismatch is distinguished from an ordinary failure",
	"ensureQ2AffineConsumerSubscription": "provisions the affine delivery path, participating in the SUBSCRIBE packet's rollback because it costs a consumer, a MaxAckPending charge and a persisted generation",
	"ensurePubRelConsumerSubscription":   "provisions PUBREL delivery; must run before the drain barrier because $MQTT_out is interest-retained and a recreate with no consumer keeps nothing",
	"lookupQ2AffineConsumer":             "reads the affine consumer's existence and instance identity",
	"lookupQ2AffOutStream":               "reads $MQTT_out's configuration when the affine path needs its shape",
	"cleanupFailedSub":                   "releases what a failed filter created; runs only when the rollback's persistence was CONFIRMED, so a group durable the record may still claim is never deleted",
	"execDeferredConsumerDelete":         "the irreversible half of a QoS downgrade, deferred until the packet's save is confirmed so a failed save can restore the pre-packet consumer",
	"removeUnsubConsumer":                "deletes an UNSUBSCRIBEd filter's consumer after its registrations are withdrawn, since a register landing afterwards would hold its identifier for the session's life",
	"releaseSharedConsumer":              "releases a group durable only when this session was the last holder",
	"releaseSharedTeardownDurables":      "releases the group durables a teardown owed, after the record is gone so the last-holder scan does not count this session",
	"sharedDurablesHeldByOthers":         "reads the record set to decide last-holder status; the answer authorizes an irreversible consumer delete",
	"releaseFailedSharedFilter":          "unwinds a shared filter whose restoration failed, without asserting the membership was never stored",
	"drainingAllowed":                    "reads whether a stream-forced InactiveThreshold would reap a drained durable, refusing at RUNTIME because a startup check cannot see a limit imposed later",
	"clearDrainInactivityTimers":         "clears the timers a drain armed once its durable is verified",

	// ---- delivery callbacks ---------------------------------------------------
	// The callback OCCURRENCE is itself an observation, which is how delivery and
	// timer-driven redelivery enter this census without being classes of their own.
	"mqttDeliverMsgCbQoS12":      "a QoS 1/2 delivery: the callback's occurrence, its delivery count, its ack subject and its headers are all interpreted, and the register store between confirmation and enqueue is the window F-034 audited",
	"mqttDeliverPubRelCb":        "a stored PUBREL replayed under its ORIGINAL identifier; the record's presence is what owns the identifier past PUBREC",
	"mqttDeliverQ2AffineCb":      "an affine copy delivery; the gate fails CLOSED on an absent claim and disposes a copy whose live claim names a different incarnation",
	"mqttInitiateMsgDeliveryJSA": "stores an inbound QoS 1/2 PUBLISH; waits on the subs barrier so a racing publish is not dropped by interest retention",
	"mqttHandlePubRetain":        "stores or tombstones a retained message; the empty-body tombstone IS the deletion authority, so no explicit delete is issued",
	"loadRetainedMessages":       "batched authoritative read of retained subjects; best-effort by design, since a missing retained message is not a correctness failure",
	"resendPendingRedelivery":    "re-offers reserved deliveries after a resume, carrying jsDur alongside each sequence so the two sequence spaces stay apart",

	// ---- inbound QoS 2 move protocol -----------------------------------------
	"storeQoS2HoldCAS":             "create-only hold CAS; a rejection is settled by RE-ATTEMPTING, never by a readback",
	"storeQoS2DoneCAS":             "advances a hold to its terminal tombstone",
	"loadQoS2Record":               "reads a hold; the four outcomes are distinct and an indeterminate read is never an absence",
	"releaseQoS2Record":            "releases a hold at its exact sequence once its release is proven",
	"storeQoS2RelClaimCAS":         "claims a release, so a discard and a releaser cannot both act on one hold",
	"loadQoS2RelClaim":             "reads a release claim; its state decides whether a discard may destroy the hold",
	"reapQoS2RelClaim":             "reaps a settled release claim at its exact sequence",
	"enumerateQoS2Holds":           "enumerates a session's hold subjects ONCE; a subject absent from that snapshot is never arbitrated, which is F-036's recorded manifestation",
	"purgeQoS2Hold":                "purges holds under a PROVEN cutoff (F-062); the marker records the entitlement rather than a completed purge",
	"collectInboundQoS2Candidates": "collects orphan hold candidates from the stream's own state",

	// ---- shared-subscription affine QoS 2 ------------------------------------
	"storeQ2AffClaimCAS":             "create-only claim CAS: the election, arbitrated durably rather than deduplicated",
	"loadQ2AffClaim":                 "reads a claim; the delivery gate fails closed on absence because an absence is not a fact",
	"scanQ2AffClaimRecords":          "walks a generation's claims for recovery and reaping",
	"storeQ2AffCopy":                 "stores the affine copy; every store outcome converges on a PRESENCE proof, because an interest-retained store with no consumer succeeds and keeps nothing",
	"loadQ2AffCopy":                  "proves a copy present at its exact sequence — the proof that authorizes the irreversible source ack (F-063)",
	"withdrawQ2AffCopy":              "withdraws a copy at its exact sequence once no claim of that incarnation can authorize it (F-065)",
	"refuseQ2AffCopy":                "NAKs a copy the gate cannot attribute, rather than acknowledging it away",
	"reapQ2AffClaimChecked":          "reaps a settled claim behind a shape precheck, because a same-copy unsupported rewrite can appear between the inventory's validation and the reaper's own load",
	"completeQ2AffOwnClaim":          "completes this session's own claim on the delivery path",
	"moveStartedSharedQoS2":          "moves a started group delivery onto the affine path, binding the copy to the claim incarnation that authorized it",
	"q2AffSourceGone":                "infers whether a claim's source is authoritatively gone; a reused sequence after a recreation breaks this inference, which the census records",
	"q2AffGroupConsumerGone":         "distinguishes a destroyed group consumer from an absent delivery",
	"recoverQ2AffineStorageConflict": "resolves a storage conflict on the affine path without assuming which writer won",
	"deleteOrphanQ2AffConsumers":     "deletes an orphaned affine consumer by name after a record re-read and an INSTANCE check, whose blind spot for a resuming session F-033's audit recorded",
	"finishQ2AffineUnwind":           "deletes the affine resources only after a record that no longer names them is persisted",
	"settleRetiredAffineRegister":    "resolves the retired affine register triple, deleting the register LAST because it is the only proof of that licence",

	// ---- outbound in-flight registers ---------------------------------------
	"storeInflightRegister":     "persist-before-send (R1): the identifier is unsendable until the register is CONFIRMED, and an ambiguous store is resolved by re-reading and matching the nonce",
	"recoverInflightRegisters":  "reserves every identifier a register or PUBREL record names BEFORE any source interest exists; a scan that cannot complete withholds allocation rather than denying the CONNECT",
	"settleRetiredSource":       "settles a retired source so its next delivery is discarded rather than re-sent",
	"confirmSourceAckAndRecord": "confirms the source ack before the register is deleted, so a register is never removed over a source that was never released",
	"collectInflightCandidates": "collects orphan register candidates using the stream's own FirstSeq/LastSeq as a cursor",
	"scanPubRelRecords":         "scans stored PUBRELs; a record's presence is what proves an exchange is past PUBREC",
	"recreateDrainPubRel":       "recreates a drained PUBREL under its RECORDED identifier, which is why those identifiers must already be reserved",
	"recoverDrainPubRels":       "restores the drains' PUBREL state before the barrier finalizes anything",
	"recoverDrains":             "reconciles drains against consumer state, skipping what the barrier could not resolve",
	"finalizeDrainDelivery":     "releases the drain's register — CONFIRMED, before the claim removal and the ack — and is the site F-034's subject addressing corrects",
	"finishDrainIfEmpty":        "completes a drain once nothing is owed, deleting its durable last",

	// ---- session records, teardown, expiry -----------------------------------
	"persistRecord":                 "the record CAS: every write is conditional, and a rejection is a positive fact about another writer rather than an error to retry blindly",
	"resolve":                       "settles an ambiguous record write by RE-ATTEMPTING the exact bytes, never by a readback (10164 returns while a write is in process)",
	"resolveUncertainWriteInTx":     "the same settlement inside a held record transaction",
	"reattemptUncertainWrite":       "re-attempts the stored bytes so the outcome becomes definite",
	"reattemptPendingStore":         "re-attempts a pending store whose outcome was never learned",
	"resolveContendedRecordSave":    "distinguishes contention from displacement on a contended save",
	"probeRecordRejectionInTx":      "probes a rejection definitively by re-attempting the write, which is the only sound test",
	"disposalForDirectRejection":    "decides the disposal vocabulary a definite rejection earns",
	"claimTeardown":                 "claims a teardown with a full-body CAS off the same base, so two claimants cannot both apply",
	"releaseTeardown":               "deletes the marked record at the sequence the marker landed on, and CLASSIFIES the outcome by re-reading, because a delete that removed nothing is not proof",
	"deleteTeardownTargets":         "the destructive half of a teardown; since F-034 its $MQTT_inflight removal is reapIncarnationRegisters, a bounded enumerate-load-classify pass deleting at exact sequences, and every other name it deletes is generation-scoped",
	"storeGenRetirementCAS":         "records the generation-retirement watermark BEFORE anything is deleted, so the fact survives a foreign unconditional write to the record subject",
	"loadGenRetirementRecord":       "reads the watermark; corrupt or unreadable FAILS CLOSED, since whether a generation was torn down cannot be guessed",
	"condemnRetiredSurvivors":       "condemns a record that survived its own teardown",
	"adoptAbandonedTeardowns":       "adopts a teardown whose claimant vanished, repeating deletions that are safe to reissue",
	"expireSession":                 "expires a session; a higher record sequence is not read as a remote owner while this broker's own write is unresolved",
	"loadExpiringSessionRecord":     "reads a record for the expiry decision, inside one transaction spanning the load, the sequence read and the decision",
	"sweepSessionExpiries":          "the periodic expiry pass; skips never-expire records outright",
	"reconcileRemoteSessionExpiry":  "reconciles an expiry another broker owns",
	"reconcileResidentOwnership":    "the committed-read backstop: feeds the eviction trigger when a record MOVED, arms the marker when it is GONE, and re-fires a trigger a refusal left undecided",
	"evictDisplacedOnRemotePersist": "evicts a displaced resident on a peer's landed write, with a digest arm so this broker's OWN ambiguous landing cannot evict the rightful client",
	"buildSessionRefs":              "builds the liveness view the orphan sweep uses: this broker's residents plus persisted records ONLY, which is F-036's recorded premise",
	"loadFenceSession":              "reads the record a fence is taken against",
	"reconcileResumedConsumers":     "verifies every recorded plain QoS 1/2 durable in one batched lookup and repairs an authoritatively absent one under a FRESH identity; an indeterminate answer refuses the CONNECT",
	"mqttProcessConnect":            "the CONNECT: reads the record, the watermark and the acceptance facts, and every failure exit before the CONNACK unwinds through the abort discipline",
	"readAcceptFact":                "reads one permanent acceptance fact",
	"loadNextAcceptFact":            "walks the acceptance facts in bounded order",

	// ---- Wills ---------------------------------------------------------------
	"storeWillOutboxCAS": "create-only enqueue then state-advance CAS on the delayed-Will outbox; a validation failure falls back to immediate publication rather than persisting a shape it cannot interpret",
	"loadWillOutbox":     "reads an outbox record; a refused record is never claimed, published, cancelled or deleted",
	"scanWillOutbox":     "the claim scan; re-reads every extant record each interval, which is why a refused backlog has a running cost",
	"willPublishWorker":  "publishes a due Will under a claim whose renewal deadline belongs to the claim rather than to a pass",

	// ---- PUBREL / QoS 2 second phase ----------------------------------------
	"mqttProcessPubRel":          "stores the correlated PUBREL, which is the handoff of identifier ownership from the register to the record",
	"mqttProcessPubComp":         "completes the exchange, and must not ack the PUBREL away while the retirement it covers is not durably recorded",
	"mqttProcessPublishReceived": "stores the correlated PUBREL on the PUBREC path, before the affine completion runs",

	// ---- orphan reconciliation ---------------------------------------------
	"sweepOrphanedQoS2State":            "the orphan pass over inbound QoS 2 state; authoritative over persisted records and this broker's residents, explicitly not over a session live elsewhere",
	"sweepOrphanedConsumers":            "the orphan pass over consumers, whose exact-sequence revalidation protects reoccupation but not a remote live owner",
	"deleteOrphanPubRelConsumers":       "deletes an orphaned PUBREL consumer by name, behind the same instance check as the affine one",
	"enumerateInflightSubjectsBounded":  "the reap's enumeration, returning the subject set AND the watermark that bounds what the pass may destroy; the resume walk's own enumeration discards that watermark because it removes nothing by filter",
	"loadInflightRecordsMulti":          "the reap's batched register read, kept separate from the resume walk's batching because the window the reap must get right lies between the enumeration and this load",
	"deleteInflightRegistersPipelined":  "the reap's bounded-concurrency confirmed deletes, aggregating failures so a partial pass reports that it was partial — the teardown must not release the record while any register of unknown status remains",
	"reapIncarnationRegisters":          "the teardown's BOUNDED reap, which replaced a blind filter purge: enumerate, batched load, identity classification, skip anything above the enumeration's own watermark, delete at exact sequences. Unreadable or indeterminate ABORTS the teardown, so the record is not released while a register of unknown status remains",
	"releaseInflightRegisterByIdentity": "addresses a register by SUBJECT and deletes only the exact sequence the subject currently holds, and only on an identity match — the contract F-034 introduced, because a caller holding a sequence it recorded earlier is holding a guess",

	// ---- the sites the SEMANTIC wrapper layer added ---------------------------
	// M5b is why these are here: with the surface anchored only at the transport, a
	// new caller of a package wrapper was invisible. Widening the surface to the
	// wrappers added 39 sites — a third again of the original 117 — which is the
	// measure of how much that under-anchoring hid.
	"mqttStoreQoS2MsgOnce":                 "stores an inbound QoS 2 hold exactly once per (client, generation, identifier); a duplicate PUBLISH must not create a second hold",
	"verifyQoS2HoldForRelease":             "proves the hold a release is about to act on is the one it claimed, rather than a later exchange on the same identifier",
	"claimQoS2ReleaseInner":                "the release claim's inner CAS, which is what stops a discard and a releaser both acting",
	"finalizeQoS2MoveInner":                "finalizes the move, reading absence as completion ONLY where the claim proves it",
	"storeQoS2FinalizeCAS":                 "the move protocol's terminal CAS",
	"discardQoS2Hold":                      "destroys a hold under an arbitrated claim, never by a blind purge, because a PUBREL that merely read a hold went on to fan out",
	"awaitQoS2Release":                     "waits briefly for a releaser that claimed first, then leaves BOTH records rather than destroying a payload its retry still needs",
	"reconcileOneInboundQoS2":              "reconciles one hold against its claim during the orphan pass",
	"qos2ClaimUnchanged":                   "re-reads a claim to confirm nothing moved under the pass",
	"advanceQ2AffClaimChecked":             "advances an affine claim behind a shape precheck, because an unsupported rewrite can appear between validation and the advance",
	"materializeQ2AffCopy":                 "the copy materialization: every store outcome converges on a presence proof, and the source ack is authorized only by sourceAckAuthorized",
	"materializeQ2AffOnBehalf":             "a contender rebuilds an absent winner's durable from its record and materializes onto it, closing the stall without the owner",
	"q2AffCopyIsDisowned":                  "decides a copy is disowned from a POSITIVE fact — a live claim of this incarnation naming a different copy — never from an absence",
	"q2AffClaimForCarriedCopy":             "resolves the claim a delivered copy names, which is how the gate attributes a copy at all",
	"forgetIfQ2AffClaimGone":               "drops in-memory affine state only once the claim is proven gone",
	"recoverQ2AffClaims":                   "the owner's own resume pass, and the only writer of Lost",
	"settleSourcelessQ2AffClaim":           "settles a claim whose source is authoritatively gone, with the phase condition as a PRECHECK inside the CAS loop because mergeProven runs after mutate and so cannot veto",
	"cancelIfSuperseded":                   "cancels an affine action whose authorizing incarnation has been superseded",
	"confirmInflightRegister":              "stamps a confirmed register onto its pending entry and clears regPending, which is the moment the identifier becomes sendable (R1)",
	"claimRecoveredSharedInflight":         "arbitrates a recovered shared register by ClaimSeq, so a contest has exactly one newer side and the older reads displacement",
	"markInflightRegisterRetired":          "writes the durable Retired mark, without which a restart re-sends a payload the client already completed",
	"markInflightRegisterCancelled":        "writes the cancellation tombstone, the durable evidence that distinguishes 'the client may hold this' from 'the client never saw it'",
	"markInflightRegisterCancelledPending": "the same tombstone for an allocation whose store is still in flight, written create-only first then at the exact sequence its own nonce is found at",
	"deleteInflightRegisterFor":            "removes the register of a pending entry a terminal path is releasing; a zero sequence is a no-op so every terminal path may call it unconditionally",
	"voidRegistersOnStream":                "the teardown's register ladder over the stream: confirmed delete, then tombstone, before the consumer that would classify those sources is destroyed",
	"reconcileOrphanInflight":              "reconciles one orphan register candidate against its record",
	"clearRetiredSource":                   "clears a retirement once its source is settled",
	"resolveRetiredSource":                 "resolves a retired source against a fresh classification, dropping the retirement only when the source is provably complete",
	"loadGenRetirement":                    "the watermark read used outside the CONNECT path",
	"storeGenRetirement":                   "the watermark write used outside the teardown path",
	"saveWithDetail":                       "the classified record save: landed, definitely failed, or ambiguous, and the ambiguous arm keeps the attempted bytes for re-attempt",
	"saveAcceptWithDetail":                 "the same classification for the accepted-CONNECT atomic batch, whose apply-layer indivisibility makes the fact and the record one commit",
	"enqueueDelayedWill":                   "enqueues a delayed Will, validating before persisting and falling back to immediate publication rather than storing a shape it cannot interpret",
	"storeWillEnqueueCAS":                  "the outbox create-only enqueue",
	"storeWillClaimCAS":                    "the outbox publish claim, whose renewal deadline belongs to the claim rather than to a scan pass",
	"storeWillFinalizeCAS":                 "the outbox terminal advance",
	"finalizeWillPublish":                  "finalizes a Will publication, recording the decision against a cutoff it was entitled to decide on",
	"tryFireWill":                          "fires a due Will, deciding on a FRESH clock sample rather than one taken at entry",
	"armDelayedWillTimer":                  "arms the local fire timer, which is the latency path; the periodic scan is the backstop",
}

// TestPersistedIdentityIngressBijection is the discriminating assertion. It is what M5
// mutates: removing a manifest entry, or adding an ingress call in a function the
// manifest does not name, must fail here — and it fails against the AST, not against a
// count.
func TestPersistedIdentityIngressBijection(t *testing.T) {
	t.Parallel()
	sites := censusScanIngress(t)

	fns := map[string]bool{}
	for _, s := range sites {
		fns[s.fn] = true
	}

	var missing, stale []string
	for fn := range fns {
		if _, ok := censusInterpreters[fn]; !ok {
			missing = append(missing, fn)
		}
	}
	for fn := range censusInterpreters {
		if !fns[fn] {
			stale = append(stale, fn)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)

	for _, fn := range missing {
		t.Errorf("%s reaches a JetStream ingress method but the census names no interpretation for it", fn)
	}
	for _, fn := range stale {
		t.Errorf("the census names an interpretation for %s, which no longer reaches any ingress method", fn)
	}
	for fn, why := range censusInterpreters {
		if strings.TrimSpace(why) == "" {
			t.Errorf("%s has an empty interpretation: the prose IS the audit", fn)
		}
	}
}
