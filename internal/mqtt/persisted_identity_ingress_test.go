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
	//
	// F-109 QUALIFIES THAT PARTITION AT THE SERVER BOUNDARY, and the descriptions below are
	// written with the qualification rather than repeating it at each site. The decoding and
	// validation guards these entries describe REMAIN TRUE: a body this broker cannot
	// interpret is a definite fact about a record that was read, and the sites that refuse it
	// still refuse it. What F-109 removes is the premise one step earlier — the pinned server
	// maps every LoadMsg, LoadNextMsg and LoadLastMsg error to JSNoMessageFoundErr, so a
	// failed STORE READ arrives as a 404 and is classified ABSENT before any of these guards
	// runs. An entry here that distinguishes a failed read from an absence is describing an
	// intent the transport does not currently support. See
	// docs/findings/point-read-absence-census.md, which owns that question and enumerates
	// which decisions rest on it.
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
	"createEphemeralConsumer": "create an ephemeral consumer; with ActionCreate its response partitions into CONFIRMED, adopted-because-identical, and refused",
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
	"deleteEphemeralConsumerDefinite":   "the two-attempt absence proof for an ephemeral consumer name: definite-absent or fail closed",
	"deleteEphemeralConsumerBestEffort": "the rollback's single-shot ephemeral-consumer delete",
	"createEphemeralConsumerOwned":      "the ActionCreate ephemeral create whose bool is the rollback's ONLY licence",
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

// censusScanIngress reports each call to an ingress method with its enclosing function.
// This is the independent enumeration: it reads the source, not the manifest.
//
// The WALK itself now lives in census_scan_test.go and is shared with F-109's point-read
// absence census, so one parse of the package serves both questions and neither grows a
// second walker. What stays here is this census's POLICY: which callees count as ingress,
// and that a site's identity for F-034's purposes is the unqualified function name — F-109
// needs the receiver and the occurrence index, and taking them here would change what this
// census's manifest is keyed on.
func censusScanIngress(t *testing.T) []censusIngressSite {
	t.Helper()
	sc := censusScanPackage(t)
	var out []censusIngressSite
	for _, c := range sc.Calls {
		if c.On == "" {
			// F-034's surface is selector calls on the JSA and its semantic wrappers; a
			// bare call of the same name is not one of them. Preserved deliberately, so
			// this refactor changes no verdict of this census.
			continue
		}
		if _, ok := censusIngressMethods[c.Callee]; !ok {
			continue
		}
		out = append(out, censusIngressSite{method: c.Callee, fn: c.Fn.Name})
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
	"advanceFinishedSlotToTerminal": "reads the slot back after an unsuccessful advance CAS and PARTITIONS the outcome: terminal-or-absent is advanced, a live action or another exchange is not-ours, and an unreadable, unattributable or indeterminate observation is UNRESOLVED, which refuses the resume rather than accepting one whose identifier stays charged",
	// ---- the transport layer itself -------------------------------------------
	// These are the wrappers. Their "interpretation" is the outcome PARTITION they
	// establish for everyone above them, which is why they are sites in their own
	// right rather than plumbing.
	"storeMsg":                        "classifies a store response into landed / definitely failed / ambiguous; the ambiguous arm is what forbids reading a response shape as a verdict",
	"sendAck":                         "fire-and-forget ack: a lost ack costs one redelivery, which is the right trade everywhere a durable record is NOT deleted on the strength of it",
	"deleteConsumer":                  "delete by name; JSConsumerNotFoundErr is SUCCESS, and every caller must read it that way or a repeated teardown wedges",
	"deleteInflightRegister":          "fire-and-forget register delete at an exact sequence; a failed delete leaves a stale record that three other reapers remove (R5)",
	"deleteInflightRegisterConfirmed": "confirmed register delete; treats an already-absent sequence as SUCCESS, which is load-bearing for the drain's finalize",
	"loadInflightRecord":              "the four-outcome partition this whole census generalises: empty, present-but-unreadable, readable, indeterminate — and indeterminate is never guessed into an absence BY THIS FUNCTION. Its indeterminate arm tests isErrorOtherThan(err, JSNoMessageFoundErr), and per F-109 a store-read failure reaches it already spelled JSNoMessageFoundErr, so for that failure the arm cannot fire",
	// ---- the F-105 write boundary's own reads ---------------------------------
	"checkRegisterReplacements": "re-reads the EXACT occupant a replacing register claims to displace, and refuses every reading that is not a record standing at the claimed sequence: an absent or unreadable occupant refuses rather than being treated as a free subject, a sequence that has moved refuses because the disposition was chosen against a record that is no longer there, and an occupant naming another delivery refuses because every replacement arm is scoped to the same (Dur, Seq). The read is what makes the boundary re-derive rather than trust the composer",
	"loadOccupantMarkerLoad":    "reads the unsent marker beside a register being replaced and keeps the three response kinds APART for the reason every other read here does: absent means the occupant was possibly attempted, present-but-undecodable is UNREADABLE and withholds, and an indeterminate read is neither — it is handed to mqttDecideMarker as an observation rather than folded into an absence, which is what stops a BATCH'S SHAPE being taken for the store's answer. That is what it does with the response it gets; per F-109 a store-read failure is not one of the responses it can tell apart, because the server has already mapped it to a 404",
	"checkManifestAgainstStore": "re-reads the STANDING manifest a chunk fence names and partitions it three ways: ABSENT is a positive answer meaning this batch RESERVES the materialization create-only, with nothing written and its cleanup owner already named; a sequence that has moved refuses because the fence was computed against a manifest that is no longer there; and an unreadable manifest refuses outright, because a chunk landing against a manifest nobody could interpret is material arriving with no fence at all",
	"loadDirShard":              "reads one directory shard's header and partitions it: ABSENT is a POSITIVE answer meaning nothing starts a traversal over that shard, so no generation in it is enrolled however many root records happen to exist there; UNREADABLE is a refusal, because a header this broker cannot interpret may be the one whose high-water covers the root a teardown is asking about",
	"loadDutyEntryAt":           "reads one duty entry WITH the sequence it stands at, so a continuation CASes against the entry it read; an entry this broker cannot interpret is an error rather than an absence, because a continuation over an absent entry would invent the duty it claims to advance",
	"publishGenCompletionCert":  "writes a generation's completion certificate create-only, and treats an ABSENT read after an unresolved publication as UNRESOLVED rather than as proof it never landed: the original write may still commit, so the caller retries the identical certificate and the recovery root stays until one is seen to stand. A conflicting bound is an error, never an overwrite",
	"retireRecoveryRoot":        "deletes a recovery root at the EXACT sequence it was read at, and only after the completion certificate is confirmed to stand — certificate first, then the root, so a crash between them leaves both and converges rather than leaving a generation with neither",
	"deleteAffineConsumer":      "performs the affine consumer's physical delete and classifies its OWN result: removed when the delete reported success, gone when it reported not-found, and NOTHING at all when it was ambiguous — the duty stays authorized and the next pass re-acts, which is safe because the durable's name carries the affine generation no live session can hold",
	"loadDirShardAt":            "reads one directory shard header WITH the sequence it stands at, so a traversal writing its position back CASes against the header it read; an unreadable header is an error rather than an absence, because a header this broker cannot interpret may be the one whose high-water covers the root a caller is asking about",
	"persistShard":              "writes a shard header back at the sequence it was read at, so a stale walker is refused rather than overwriting a newer cycle. Deliberately NOT an authority transition: a recovery pass may run on a broker holding no authority for any client in the shard, and requiring one would make traversal depend on the entitlement it exists to survive",
	"loadAffineReadiness":       "reads one generation's affine readiness assertion, and treats an unreadable record as an ERROR rather than an absence: a body this broker cannot interpret may be the assertion whose prerequisites license a provisioning seal, and reading it as missing would refuse a legal seal while admitting nothing in its place",
	"publishAffineReadiness":    "writes the readiness assertion create-only, and settles an occupied or AMBIGUOUS publication by reading that same create-only subject back: an identical record means another producer got there first and the fact is established, while a conflicting one is an error, because one generation has one affine consumer and the two cannot both be true",
	"loadGenCompletionCert":     "reads a generation's completion certificate and keeps absence apart from unreadability: absent means no certificate stands, which is a positive answer that simply fails to satisfy recovery ownership on its own, while a certificate this broker cannot interpret is a refusal — a malformed proof of completion must never be read as the generation having none, since both lead to different halves of the same gate",
	"sweepOneBatchCharged": "reads one bounded batch of a generation's duty inventory and partitions each ordinal THREE ways rather than two: terminal is covered, an open duty is outstanding, and a MISSING or MALFORMED entry is outstanding too — an inconsistency and never completed work, because a sweep that skipped what it could not read would advance past an ordinal nothing else revisits. A read that fails for any other reason STOPS the batch rather than reporting a partial one as whole, since the ordinals after it were never looked at" +
		"The CHARGED form is the only one that reaches the store, and the charge is consulted per ENTRY rather than per call: one call reads up to 128 of them, so charging the call bought 128 round trips for one unit and the pass's budget — with the shutdown latency that rests on it — bounded nothing. A refused charge ends the batch where it stands, and the outcome reports only the ordinals actually examined, so the cursor never advances over an entry nothing read",
	"persistSweep":                  "writes an advanced cursor back under an exact-sequence CAS, so a stale worker is refused rather than overwriting a newer sweep — two brokers sweeping one generation is expected, because claims reduce duplicate work and cannot exclude it. Deliberately NOT an authority transition: a recovery pass may run on a broker holding no authority for the client whose work it resumes",
	"loadRecoveryRoot":              "reads one enrolled generation's root and keeps absence apart from unreadability: absent means nothing is enrolled at that ordinal, while a root this broker cannot interpret is a REFUSAL — reading it as absent would drop a generation with outstanding duties out of every future sweep",
	"checkDutyContinuation":         "re-reads the EXACT duty entry a continuation claims to advance, and keeps the three readings apart: an unreadable entry is a REFUSAL and never a licence to rewrite, an absent one refuses because a continuation over nothing would invent the duty it claims to advance, and a sequence that has moved refuses because the continuation was decided against a record that is no longer there. The read is also what makes irreversible terminality checkable — the standing entry's state is what says whether this batch reopens a duty a recovery pass has already advanced past",
	"loadGenLedger":                 "reads one generation's inventory ledger and partitions the outcome: ABSENT is a POSITIVE answer here, because a generation that has allocated nothing has no ledger and its first allocation creates one create-only; UNREADABLE is not absence but a refusal, since a ledger this broker cannot interpret may be the one carrying the high-water an allocation is about to step; and a failed read is returned as an error rather than folded into either — which holds for every error the transport reports AS an error, and per F-109 does not hold for a store-read failure, since the server maps that to a 404 this function reads as ABSENT",
	"enumerateGenerationResources":  "enumerates one generation's backing resources and counts a record it cannot DECODE as UNREADABLE rather than skipping it, because a resource this broker cannot interpret may be the one naming an obligation that depends on the consumer about to be destroyed; a record that vanished between the enumeration and the load is left to a later pass and nothing is inferred from its absence, while a failed enumeration is returned as an error so the caller refuses rather than reading an incomplete list as an empty one",
	"storeInflightCAS":              "create-only or exact-sequence CAS on a register, honouring a test seam so an injected fault cannot perturb the source or PUBREL stores",
	"enumerateInflightSubjects":     "enumerates one incarnation's register subjects; complete by construction because a 16-bit identifier bounds the filter well under JSMaxSubjectDetails",
	"inflightSourceState":           "classifies a register's SOURCE as complete, live, unknown or LOST; a consumer-gone source is deliberately NOT complete, and neither classification is by itself a licence to reap a possibly-attempted exchange — 'complete' licenses it, and 'lost' does so only beside a marker proving non-transmission (F-102)",
	"confirmInflightSourceAck":      "confirms a source ack under the attempt's own reply subject, so a retirement is settled rather than left owing",
	"sessLastSeq":                   "reads the session stream's last sequence as a bound, never as a claim about where enumerated subjects sit",
	"loadSessionMsg":                "FORWARDS a session record read: it returns the (message, error) pair untested, so it establishes neither presence nor absence and performs no interpretation of its own. Its sixteen callers each classify, and whether the absence they read is a positive answer is F-109's question — see docs/findings/point-read-absence-census.md",
	"storeFramedSessionMsg":         "stores a framed session record and returns the CAS outcome for the classifier above it",
	"mqttStoreSessionRecordCASBody": "builds and stores the record CAS body; the returned sequence becomes the next expectation",

	// ---- stream and consumer provisioning -------------------------------------
	"ensureBrokerStream":                   "create-or-lose then an unconditional authoritative RE-READ, so a create-race loser and the winner both validate what actually exists rather than what they asked for",
	"createOrRestoreSessionWithin":         "reads the record and the retirement watermark; a retired generation is DISCARDED rather than resumed, and an unreadable watermark refuses the CONNECT",
	"mqttRefuseRetiredOriginStream":        "a lookup-only preflight: the presence of a retired stream is itself the refusal, since ensureBrokerStream would otherwise create an absent one before validating",
	"mqttPreflightBrokerStreamBundle":      "a lookup-only preflight over all ten broker-owned streams: it dispositions the (info, error) PAIR per stream — present, absent, or UNDECIDED, which fails startup rather than being read as absent — and refuses a proper subset, since ensureBrokerStream would otherwise create the missing members and re-establish the partial-recreation hazard",
	"mqttProvisionEphemeralConsumers":      "creates BOTH per-run ephemeral consumers under this run's DERIVED names — the retained observer after proving its name absent, and the session-record watch, which a retry adopts rather than replacing — and interprets each confirmation",
	"rollbackEphemeralConsumers":           "deletes only what this attempt CONFIRMABLY created, because an error proves neither that nothing was created nor that what is there is ours",
	"deleteEphemeralConsumerDefinite":      "the two-attempt absence proof: success and not-found are definite, anything else is an ambiguity that fails initialization closed",
	"deleteEphemeralConsumerBestEffort":    "the rollback's single-shot delete, whose outcome nothing depends on",
	"createEphemeralConsumerOwned":         "the ActionCreate wrapper whose bool is the rollback's ONLY licence",
	"processJSConsumer":                    "creates or adopts a plain QoS 1/2 consumer; an existing durable makes the create an UPDATE, and the response's config is what the record then persists",
	"processSharedJSConsumer":              "joins the shared group consumer; the membership (with its acquisition references) is persisted BEFORE the create, so a crash finds what to repair and a delete racing the join cannot reach it — the incarnation is named and, while its acquisition is listed, cannot be retired",
	"commitSharedLifecycleBatch":           "THE ONE writer for the shared-group lifecycle's two $MQTT_shared record classes; frames every batch through the atomic-batch request and revalidates its priors against the store, so a hand-built batch meets the same rules a composer's does",
	"deleteSharedIncarnationConsumer":      "the physical CONSUMER.DELETE of one retired incarnation's group durable, classifying its own outcome removed / gone / ambiguous; an ambiguous result records nothing and leaves the retiring entry for the sweep",
	"loadSharedGroup":                      "reads one logical group's arbitration record; corrupt and indeterminate are kept distinct from absent, because a group durable's fate is decided from a record that EXISTS, never from an enumeration's exhaustion",
	"loadSharedLedger":                     "reads one generation's acquisition ledger; the permanent, per-generation record whose seal is the generation fence",
	"sweepSharedConsumers":                 "classifies each $MQTT_SHARED_ consumer the orphaned-consumer sweep collected against its GROUP RECORD (live, retiring, derived-retired, or a shape/record that licenses nothing) and deletes only under that record's authority",
	"sweepSharedGroupRetirements":          "walks the group records and finishes every retiring entry a leaver crashed inside L2 left standing",
	"sweepAcqReleaseVerdicts":              "executes committed release verdicts from every session record, connected or not, after the same (sequence, verdicts) is observed on two consecutive passes; never writes the record",
	"classifySharedGroups":                 "reads, in one batch, whether each group durable a RESIDENT shared membership is about to be restored into exists and is canonical; a settled absence and an unreadable reply are kept distinct, and only the settled absence licenses a repair (F-077 made it repairable through mqttSharedGroupVerdictIsRepairable) while an unreadable or indeterminate reply still licenses nothing",
	"createQ2AffineConsumer":               "creates the session-affine QoS 2 consumer; an immutable-field mismatch is distinguished from an ordinary failure",
	"ensureQ2AffineConsumerSubscription":   "provisions the affine delivery path, participating in the SUBSCRIBE packet's rollback because it costs a consumer, a MaxAckPending charge and a persisted generation",
	"ensurePubRelConsumerSubscription":     "provisions PUBREL delivery; must run before the drain barrier because $MQTT_out is interest-retained and a recreate with no consumer keeps nothing",
	"lookupQ2AffineConsumer":               "reads the affine consumer's existence and instance identity",
	"lookupQ2AffOutStream":                 "reads $MQTT_out's configuration when the affine path needs its shape",
	"cleanupFailedSubClassified":           "releases what a failed filter created; runs only when the rollback's persistence was CONFIRMED, so a group durable the record may still claim is never deleted",
	"execDeferredConsumerDeleteClassified": "the irreversible teardown of a PLAIN consumer, deferred until the packet's save is confirmed so a failed save can restore the pre-packet consumer; the classified form also reports whether either of its record writes was CAS-rejected, which a live SUBSCRIBE needs before it may open its transmission gate (F-078). It serves the QoS downgrade AND the plain UNSUBSCRIBE since F-083 deleted removeUnsubConsumer, which duplicated it while mutating cons without sess.mu and never unsubscribing the deliver subject",
	"drainingAllowed":                      "reads whether a stream-forced InactiveThreshold would reap a drained durable, refusing at RUNTIME because a startup check cannot see a limit imposed later",

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
	"storeQ2AffClaimCAS": "create-only claim CAS: the election, arbitrated durably rather than deduplicated",
	"loadQ2AffClaim":     "reads a claim; the delivery gate fails closed on absence because an absence is not a fact",
	"scanQ2AffClaimRecordsCharged": "walks a generation's claims for recovery and reaping. " +
		"The CHARGED form is the only one that reaches the store, and an exhausted charge is an ERROR rather than a short answer: a caller deciding that no claim of a generation is left undiscarded is making a statement about the WHOLE set, and half of it establishes nothing",
	"walkQ2AffClaimSegment": "walks ONE fixed-horizon segment of the claim stream for the recovery " +
		"pass. It processes nothing above its bound and does not advance past it, because a " +
		"next-message read is not confined to a segment and a record it returns from beyond one " +
		"belongs to the segment that will cover it",
	"assignStreamTail": "reads the assignment stream's END and its INCARNATION, which is what lets " +
		"a bounded walk say it saw everything: completion is asserted only when a read after the " +
		"segment finds the tail exactly where the segment ended. The incarnation travels with the " +
		"sequence because a sequence means nothing across a recreation, and an equal pair either " +
		"side of one is evidence of nothing",
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
	"finishQ2AffineUnwind":           "deletes the affine resources only after a record that no longer names them is persisted",
	"settleRetiredAffineRegister":    "resolves the retired affine register triple, deleting the register LAST because it is the only proof of that licence",

	// ---- outbound in-flight registers ---------------------------------------
	"storeInflightRegister":        "persist-before-send (R1): the identifier is unsendable until the register is CONFIRMED, and an ambiguous store is resolved by re-reading and matching the nonce",
	"recoverInflightRegistersOnce": "reserves every identifier a register or PUBREL record names BEFORE any source interest exists; a scan that cannot complete withholds allocation rather than denying the CONNECT; one pass of the bounded re-entry loop, which re-runs it when the marker phase finds the register snapshot stale",
	"loadInflightMarker":           "the unsent marker's three-outcome read — absent, present, indeterminate — typed by the caller's acquisition order before the marker boundary decides; indeterminate is never guessed into an absence",
	"siblingObservation":           "the authoritative read of a marker's sibling register, AFTER the marker was captured, classified for the reap rows: absent, same exchange, another exchange, unreadable, indeterminate",
	"deleteInflightMarker":         "the unsent marker's exact-sequence delete, addressed only at a sequence an observation carries; never awaited on the send path, confirmed on the store retry and the teardown",
	"reapIncarnationMarkers":       "the generation teardown's marker pass, after the register pass and under its watermark",
	"settleRetiredSource":          "settles a retired source so its next delivery is discarded rather than re-sent",
	"confirmSourceAckAndRecord":    "confirms the source ack before the register is deleted, so a register is never removed over a source that was never released",
	"collectInflightCandidates":    "collects orphan register candidates using the stream's own FirstSeq/LastSeq as a cursor",
	"scanPubRelRecordsCharged": "scans stored PUBRELs; a record's presence is what proves an exchange is past PUBREC. " +
		"The CHARGED form is the only one that reaches the store: the unbounded entry point delegates to it with a nil charge, so the recovery pass's bounded walk and every production teardown read through one implementation. An exhausted charge reports ok=FALSE, the same disposition as a scan that could not be read in full, because a partial walk of the stored PUBRELs establishes nothing about the proofs they carry",
	"recreateDrainPubRel":   "recreates a drained PUBREL under its RECORDED identifier, which is why those identifiers must already be reserved",
	"recoverDrainPubRels":   "restores the drains' PUBREL state before the barrier finalizes anything",
	"recoverDrains":         "reconciles drains against consumer state, skipping what the barrier could not resolve",
	"finalizeDrainDelivery": "releases the drain's register — CONFIRMED, before the claim removal and the ack — and is the site F-034's subject addressing corrects",
	// The delete MOVED out of finishDrainIfEmpty when retirement was split in two: the
	// critical section issues nothing (so it is safe under an open record transaction) and
	// the request is a separate, CONFIRMED and classified step. The census entry moves with
	// it, which is the census working rather than a bookkeeping chore.
	"disposeRetiredDrainConsumer": "deletes a terminally retired drain's durable and classifies the outcome three ways",

	// ---- session records, teardown, expiry -----------------------------------
	"persistRecord":              "the record CAS: every write is conditional, and a rejection is a positive fact about another writer rather than an error to retry blindly",
	"resolve":                    "settles an ambiguous record write by RE-ATTEMPTING the exact bytes, never by a readback (10164 returns while a write is in process)",
	"resolveUncertainWriteInTx":  "the same settlement inside a held record transaction",
	"reattemptUncertainWrite":    "re-attempts the stored bytes so the outcome becomes definite",
	"reattemptPendingStore":      "re-attempts a pending store whose outcome was never learned",
	"resolveContendedRecordSave": "distinguishes contention from displacement on a contended save",
	"probeRecordRejectionInTx":   "probes a rejection definitively by re-attempting the write, which is the only sound test",
	"disposalForDirectRejection": "decides the disposal vocabulary a definite rejection earns",
	"claimTeardown":              "claims a teardown with a full-body CAS off the same base, so two claimants cannot both apply",
	"releaseTeardown":            "deletes the marked record at the sequence the marker landed on, and CLASSIFIES the outcome by re-reading, because a delete that removed nothing is not proof",
	"deleteTeardownTargets":      "the destructive half of a teardown; since F-034 its $MQTT_inflight removal is reapIncarnationRegisters, a bounded enumerate-load-classify pass deleting at exact sequences, and every other name it deletes is generation-scoped",
	"storeGenRetirementCAS":      "records the generation-retirement watermark BEFORE anything is deleted, so the fact survives a foreign unconditional write to the record subject",
	"loadGenRetirementRecord":    "reads the watermark; corrupt or unreadable FAILS CLOSED, since whether a generation was torn down cannot be guessed",
	"condemnRetiredSurvivors":    "condemns a record that survived its own teardown",
	"adoptAbandonedTeardowns":    "adopts a teardown whose claimant vanished, repeating deletions that are safe to reissue",
	// Renamed by F-036 unit 1: the timer-fired form carries the timer instance that
	// fired, so the identity check can prove it is still the installed one. The
	// no-proof wrapper (expireSession) reaches JetStream only through this.
	"expireSessionFired":            "expires a session; a higher record sequence is not read as a remote owner while this broker's own write is unresolved",
	"loadExpiringSessionRecord":     "reads a record for the expiry decision, inside one transaction spanning the load, the sequence read and the decision",
	"sweepSessionExpiries":          "the periodic expiry pass; skips never-expire records outright",
	"reconcileRemoteSessionExpiry":  "reconciles the expiry deadline carried by a record this broker does not own",
	"reconcileResidentOwnership":    "the committed-read backstop: feeds the eviction trigger when a record MOVED, arms the marker when it is GONE, and re-fires a trigger a refusal left undecided",
	"proveGoneRetirement":           "the in-fence proof of the deleted-record retirement licence (F-036 site 1): the record must still be AUTHORITATIVELY absent, read LAST so it is the final fallible observation — an unreadable record establishes nothing and refuses, and a record present again withdraws the licence entirely",
	"evictDisplacedOnRemotePersist": "evicts a displaced resident on a peer's landed write, with a digest arm so this broker's OWN ambiguous landing cannot evict the rightful client",
	"buildSessionRefs":              "builds the liveness view the orphan sweep uses: this broker's residents plus persisted records ONLY, which is F-036's recorded premise",
	"loadFenceSession":              "reads the record a fence is taken against",
	"reconcileResumedConsumers":     "verifies every recorded plain QoS 1/2 durable in one batched lookup and repairs an authoritatively absent one under a FRESH identity; an indeterminate answer refuses the CONNECT",
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
	"mqttProcessPublishReceived": "stores the correlated PUBREL on the PUBREC path, before the affine completion runs — which is why that record also carries the bound (claim subject, copy subject) reference naming the shared QoS 2 assignment it completes: it is the durable PUBREC proof for the window between the two",

	// ---- orphan reconciliation ---------------------------------------------
	"sweepOrphanedQoS2State":           "the orphan pass over inbound QoS 2 state; authoritative over persisted records and this broker's residents, explicitly not over a session live elsewhere",
	"sweepOrphanedConsumersBefore":     "the orphan pass over consumers, whose exact-sequence revalidation protects reoccupation but not a remote live owner; the eligibility cutoff is a parameter so a test can reach the pass at all, and an unreadable record ABORTS it rather than being skipped, since the walk deletes by absence of a reference",
	"deleteOrphanPubRelConsumers":      "deletes an orphaned PUBREL consumer by name, behind a complete reference walk, a grace, a final record re-read and an instance re-lookup. It is the only consumer class this pass reclaims from a CANDIDATE LISTING — the affine one was removed by F-036 course E, because no positive fact could authorize it — though teardown-marker adoption still reaches deleteTeardownTargets, which deletes an affine durable",
	"enumerateInflightSubjectsBounded": "the reap's enumeration, returning the subject set AND the watermark that bounds what the pass may destroy; the resume walk's own enumeration discards that watermark because it removes nothing by filter",
	"loadInflightRecordsMulti":         "the reap's batched register read, kept separate from the resume walk's batching because the window the reap must get right lies between the enumeration and this load",
	"deleteInflightRegistersPipelined": "the reap's bounded-concurrency confirmed deletes, aggregating failures so a partial pass reports that it was partial — the teardown must not release the record while any register of unknown status remains",
	"reapIncarnationRegisters":         "the teardown's BOUNDED reap, which replaced a blind filter purge: enumerate, batched load, identity classification, skip anything above the enumeration's own watermark, delete at exact sequences. Unreadable or indeterminate ABORTS the teardown, so the record is not released while a register of unknown status remains",
	"releaseBoundInflightRegister":     "addresses a register by SUBJECT and deletes only the exact sequence the subject currently holds, and only on an identity match — the contract F-034 introduced, because a caller holding a sequence it recorded earlier is holding a guess",

	// ---- F-105's outbound arbitration --------------------------------------
	// These share $MQTT_inflight with the registers because a JetStream atomic batch is
	// per stream, and the authority must commit with the exchange mutation it authorizes.
	// They are kept out of the register machinery's reach by their own class token, which
	// can satisfy neither the idHash nor the generation validator.
	"loadOutboundAuthority":          "reads the per-client record that arbitrates which tenure may create outbound obligations; attributed by the EXACT client and recovery epoch in the body, never by the subject's hash, and a body this broker cannot interpret is refused rather than treated as absent",
	"loadGenClosure":                 "reads the PERMANENT record that a generation is closed to new admission — authoritative, with the $MQTT_teardown watermark demoted to a recoverable index. Absence here is NOT an open generation: open admission requires the authority's positive state",
	"loadTeardownReceipt":            "reads the durable teardown receipt that AUTHORIZES a discarding close of one generation (F-105 Gap 1), attributed by the generation the subject names and refused unless it discards that exact generation; absence is NOT authority to discard — a SERVING authority with no receipt fails closed — so only a positively validated read licenses the close, and corrupt or indeterminate refuses it",
	"loadTransitionOutcome":          "reads one attempted transition's permanent verdict, which is what makes an ambiguous attempt decidable without inferring from an absence",
	"abortTransition":                "settles an undecided attempt by taking its outcome subject CREATE-ONLY, racing the original batch's own create-only member so exactly one can exist. A FAILED abort proves only that the subject is occupied; only a validated readback says by what",
	"resolveMetadataAttempt":         "classifies one OWNERSHIP PROPOSAL's METADATA half from its permanent slot on $MQTT_sess, for either shape — a handoff that displaces a named predecessor, or a first creation that displaces nothing. ONE slot answers both, because both are one proposal over one client's ownership. Absence is UNRESOLVED and never aborted — the batch may still be in flight — and a receipt that does not answer this exact proposal, its SHAPE and its predecessor included, is FOREIGN rather than a match",
	"abortMetadataAttempt":           "takes that permanent slot with an ABORTED verdict, create-only, racing the frozen attempt's own create-only member so exactly one verdict can ever exist, for either proposal shape. This is the DELAYED-WRITE FENCE: a batch still in flight is refused in its entirety once the slot is taken. A failed abort proves only that the slot is occupied. settleProposal is the single executor that sequences this one AFTER a resolve and BEFORE a re-resolve, for an acceptance's own proposal and for another claimant's alike — settling a foreign proposal is a race the slot arbitrates, never a liveness claim",
	"enumerateGenerationObligations": "discovers what one generation still OWES — its exchanges and their acquired actions — so a broker can complete admitted work without needing the serving tenure. Bounded by the filter, which names one client and one generation; every body goes through its own decoder, so a record that does not describe what its subject claims is counted UNREADABLE rather than adopted, and a caller may not treat a generation as finished while that count is non-zero",
	"proveExchangeDischarged":        "ENUMERATES one exchange's durable ACTION records to establish that NOTHING of it can emit again, which is what an identifier release and a capacity credit actually need. A connection knows only what IT consumed; this reads what the store says, and every uncertainty counts AGAINST discharge — an unreadable record, an unplaceable subject, an outcome this process cannot establish, and a TRUNCATED enumeration, which is reported as an incomplete proof rather than as a set of finished constituents. Anchored on the exchange record, so a nonce nothing admitted cannot enumerate cleanly and come back discharged with no constituents at all",
	"verifyMaterial":                 "READS a retained payload's chunks and checks them against the manifest that describes them — every ordinal present, the total length, and the digest over the whole. The seal runs it INSIDE the transaction its CAS fences, so READY states a verification this broker performed rather than one a caller asserted; a replay runs it again before emitting a byte, because material can rot after it was sealed. A chunk belonging to a LATER materialization of the same payload token is not this manifest's material and refuses rather than reassembling",
	"checkPurgeConfirmed":            "READS the interval a cleanup claims to have removed, because two counters agreeing is arithmetic and says nothing about whether anything was deleted. A chunk of THIS materialization still standing refuses the confirmation; an unresolved read refuses; a chunk of a later materialization at the same address does not block, because ours being gone is exactly what the confirmation is about. Bounded by the announcement's own size",
	"deleteChunkInterval":            "removes the chunks of ONE announced interval, loading each record before taking it — the load is how its sequence is learned, so identifying whose chunk it is costs nothing. A chunk of another materialization is left alone, which is what stops a paused cleanup destroying a replacement payload that reused the same token. Absence is expected rather than inferred from: a resumed cleanup re-deletes its whole announced interval because the store cannot say which deletes landed",
	"finishMaterialRemoval":          "removes the MANIFEST once every chunk is CONFIRMED removed, by the EXACT sequence its caller was authorized for. Last, because while it stands an interrupted cleanup can be resumed from it; by sequence, because deleting whatever occupies the address lets a paused worker destroy a later materialization's record",
	"checkExchangeOverwrites":        "reads every exchange a transition proposes to OVERWRITE — keyed on the member and not the kind, because an exchange member at a nonzero expectation is an advance whichever transition carries it, read AT THE SEQUENCE the transition is conditioned on, so the write boundary can enforce what no check on the transition alone can: that ONLY the phase differs, and only along the protocol's own per-QoS successor relation. A record read at any other sequence is refused rather than compared, because a comparison against a version this transition does not name proves nothing. Without it the phase-only guarantee would live in the optional builder and be a caller convention",
	"checkIdentifierExclusivity":     "ENUMERATES one generation's standing obligations when a transition admits an exchange with NO register beside it, and refuses the identifier it claims if anything still outstanding holds it. A batch is not the whole state: two registerless admissions committed one after the other are each legal, each carry one exchange, and leave two obligations on one identifier with nothing within either batch to object. It runs for EVERY admission: a create-only member excludes another writer at ITS OWN KEY and says nothing about a different key describing the same thing, so a registerless obligation never occupies a register subject and a later registered admission on that identifier commits beside it. An obligation this broker cannot read REFUSES, because it may be the one holding what is being claimed — and so does a TRUNCATED enumeration, because the server caps a subject list at 100,000 and an obligation outside the first page is one the claim never saw",
	"checkActionMembers":             "reads the EXCHANGE every acquired action emits for, keyed on the member and not the kind, because an action member is an acquisition whichever transition carries it. The member alone establishes its shape and its create-only expectation and nothing about the obligation: whether that exchange exists, whether its stored phase still admits this packet, and whether the identifier the bytes will carry is the one the client was told. An exchange this same batch installs create-only is read from the batch instead, because it does not stand anywhere yet. Without it the operation surface's own check would be the only one, which is the composition-only guarantee this boundary exists to replace",
	"checkPredecessorAgainstStore":   "reads the authority a transition claims to displace, at the sequence the transition names, and requires it to BE that record byte for byte. Every kind rule is a conclusion about STANDING ownership drawn from a caller-supplied struct; without this a caller copies the serving authority, calls it CLOSED at its real sequence, and displaces live ownership with no handoff and no permanent closure. Byte equality rather than field comparison, so a member added later cannot quietly stop being covered",
	"checkResourceOverwrites":        "reads every resource a transition proposes to mutate, at the sequence it names, and applies the resource's own state machine to the STORED record: identity never moves, one transition adds or removes AT MOST ONE named edge — an authorization and a cleanup completion change none — and a deletion authorization requires the list to be empty in that very CAS. A zero read out of the caller's proposed body is not the authorization's claim at all",
	"opCommitMetadata":               "reads the client's STANDING proposal and the session record it will displace, and derives the whole metadata half from them: the proposal comes from the authority rather than the caller, the expectation from the record that stands, and the receipt from both. Freezing was the last thing a caller did for itself and the last place a caller-authored conclusion could enter — a frozen attempt is a struct, so every binding its freezer established held only until the caller changed it. The frozen attempt is returned even when the commit fails, because the create-only slot is what resolves an ambiguous send and a retry must resend THAT attempt rather than compose another",
	"loadOutboundExchange":           "reads ONE obligation as it durably stands, for the operation that is about to move it. The operation surface reads it rather than taking it from its caller, which is what makes an advance the stored record with its phase changed and nothing else: an identifier reallocation or a tenure rewrite cannot ride along with a completion. Attributed by the EXACT client, epoch, generation and nonce through the exchange's own decoder, so a record that does not describe what its subject claims is refused rather than advanced",
	"loadOutboundResource":           "reads ONE backing resource as it durably stands, for the operation about to take or release an edge on it or authorize its cleanup. Read here rather than supplied, so the step is stated against the record that exists and the write boundary's proof — that the named obligation reached an ENDING — is made about the resource the caller actually named. Attributed by client, epoch, generation and incarnation through the resource's own decoder",
	"checkHandoffPredecessorSession": "reads the SESSION RECORD a handoff proposes to displace, immediately before its metadata batch, and requires both the sequence the batch writes under and the record's own write identity and generation to be the ones the proposal names. The expectation alone says only WHICH POSITION, and the session record is a single-slot register every confirmed save advances — so without this a delayed handoff naming session A commits against whatever stands now, overwrites it, and publishes a permanent receipt claiming it displaced A",
	"saveAcceptWithOwnership":        "the same classification as saveAcceptWithDetail for the accepted CONNECT, except the atomic batch is THREE members — the acceptance fact, the proposal's COMMITTED receipt and the session record — committed through persistRecord's fenced boundary, so the outbound-ownership metadata commit inherits record lineage, the void-register accounting, the ambiguous-write refusal and the record-tx fence rather than riding the standalone opCommitMetadata route. The proposal is the reservation's or preparation's STANDING one, and the create-only receipt slot is the fence that arbitrates the metadata race",
	"loadNameOwnership":              "reads a physical consumer name's ownership record on $MQTT_inflight, which arbitrates which tenure may USE that name; ABSENT is a nil record and a zero sequence because the first claim legitimately acts on it, while a CORRUPT or INDETERMINATE read is an error and never an absence — an absence a failed read invented would let a spent name be reclaimed, and a retired physical name is never reused. Attributed by the EXACT client and recovery epoch through the record's own decoder",
	"consumerInactivitySafe":         "reads a stream's consumer-inactivity limit as an admission PREREQUISITE for an enrolled source or PUBREL consumer, and REFUSES rather than falls back: a consumer whose effective threshold is nonzero could be autonomously reaped with its completion evidence, so an enrolled dependency whose lifetime is the resource's is admitted only when the stream imposes no limit and the config requests none. A stream it cannot read refuses, because an unverifiable limit is not a zero one",

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
	"claimRecoveredInflight":               "arbitrates every selected recovered register by ClaimSeq, including shared deliveries, the universal positive QoS 2 set and the exact-PUBREL-evidenced retired-ready exception, so a contest has exactly one newer side and the older reads displacement",
	"resolvePubRelIntent":                  "the RESOLUTION of a PUBREC intent CAS the caller could not see land (F-098, holistic review round 1, finding 4), whose interpretation is that the reread slot says which continuation is owed and nothing more: this exchange's identity already past PUBREC is CONFIRMED — a landed CAS whose response was lost, or another path that moved it on — and a pubrel_intended one enrols L1's materialization; this exchange still in PUBLISH, another exchange's slot, a terminal slot or no slot is NOT RECORDED and rolls the PUBREC back; an indeterminate or unreadable read is UNRESOLVED and takes neither continuation, because a slot's mere existence proves neither",
	"resolveUnacquiredPublishAction":       "the RESOLUTION of a PUBLISH acquisition the caller could not see land (F-098, holistic review round 1, finding 2), whose interpretation is that the reread slot is evidence about WHO HOLDS THE TOKEN and nothing else: only the exact identity — this section's token, at the counter the acquisition would have written, under the custody it was bound to — is adopted, and only for CLEANUP, never as transmission authority; a proven absence or a different token withdraws the section's mark; an indeterminate or unreadable read KEEPS it, so the section's end, the owed-drain registry and the reconciler own the resolution rather than nobody",
	"confirmSlotReleased":                  "the ONE post-delete subject confirmation, extracted from recoverPublishTerminalSlot so every release-settling site shares one definition (F-098, C-IV's consolidation). An exact-sequence delete reads an already-absent sequence as success and a same-identity rewrite looks exactly like that, so the subject is RE-READ: the same (Nonce, Dur, Seq) still occupying it means the release is Refuted and the caller keeps its hold (holistic review round 6); an empty subject is EstablishedAbsent, the one answer that frees an identifier; a DIFFERENT occupant is EstablishedOther, which discharges the caller's own delete obligation but never the successor's identifier; and an error or an unreadable record is Unresolved, which is not an absence and licenses nothing -- FOUR outcomes, as every read on this surface has",
	"reapRegisterGatedOutcome":             "the ONE gated destructive door (F-098): loads the slot once, refuses a state the disposition does not permit, a moved transmission counter, and — C-IV arm 2 — ANY live action token on a destructive disposition, our own included; preserves a mismatched occupant or a moved sequence; fails closed on an indeterminate or unreadable read; and reports WHICH of those happened, because deleted, absent and preserved are different facts and none of them means the identifier may be freed",
	"casInflightSlot":                      "the ONE way an existing owner slot is rewritten, and it now carries what markInflightRegisterRetired used to reach an ingress for — the durable Retired mark, without which a restart re-sends a payload the client already completed: load, prove the occupant is still this exchange, apply the phase gate, mutate only the caller's fields and CAS at the sequence the load returned. It carries the record forward rather than rebuilding it, so a caller cannot silently drop the exchange's phase, its canonical PUBREL identity or a live action token",
	"markInflightRegisterCancelled":        "writes the cancellation tombstone, the durable evidence that distinguishes 'the client may hold this' from 'the client never saw it'",
	"markInflightRegisterCancelledPending": "the same tombstone for an allocation whose store is still in flight, written create-only first then at the exact sequence its own nonce is found at",
	"reapRegisterWithVerdict":              "the route for every exact-sequence register delete whose caller can name the exchange nonce AND can reach the verdict set: a verdicted nonce is deleted with proof and forgotten against that proof, an unverdicted one keeps the caller's own confirmation semantics. A caller with no identity-fenced session resident cannot reach that set at all (F-090)",
	"disposeSettledAffineRegister":         "the sweep's classified register disposal: it deletes through the verdict property where a resident identity-fenced session can forget the verdict, RETAINS a register that CARRIES a cancellation where none is resident, and reports whether it mutated so a retained candidate charges no budget. A verdict held only in a non-resident session's record is invisible here (F-090)",
	"reconcileOrphanInflight":              "reconciles one orphan register candidate against its record",
	"resolveRetiredSource":                 "resolves a retired source against a fresh classification, dropping the retirement only when the source is provably complete",
	"resolveUnacquiredAction":              "settles a PUBREL action whose acquiring CAS did not report success. The interpretation is the four-way load partition and NOT the CAS result: a definite rejection and an ambiguous one are indistinguishable from the return, so the reread asks whether THIS EXACT token is present. Present means the acquisition landed and must be drained rather than abandoned; absent or a different token means it definitely failed; indeterminate is neither, and keeps the action live and its identifier reserved",
	"admitStoredPubRel":                    "PUBREL ADMISSION, and the interpretation is the whole of F-092's fix: the slot that owns this identifier is the SINGLE authoritative claimant, so a record is admitted only after the slot and the record match as one exchange and the slot hands out a non-revocable action — never because a correlation was inferred against whatever entry happens to hold the identifier. An indeterminate read refuses; an absent slot is the RETAINED shape (R6) and is authorized by the live in-memory owner identity or not at all; a nonce mismatch is F-092's population itself and refuses without acknowledging; pubrel_ready acquires the ordinary action and publish may take the direct evidence-plus-action arrow. A purported duplicate is disposed only after the slot's named canonical sequence decodes and matches its identifier, nonce and source, because the presence of some unrelated global stream record is not canonical proof",
	"mqttTerminalizeOnPubComp":             "PUBCOMP's durable half: reads the slot to choose between the two arrows out of the exchange, because they differ in whether anything may be freed. From pubrel_ready the slot goes to pubrel_terminal and cleanup may finish it; from pubrel_sending it goes to pubrel_terminal_draining and frees NOTHING, keeping the same action token, because a PUBREL's bytes are still on the wire under an identifier bumpPI would otherwise re-mint. An identity mismatch PRESERVES the occupant: this PUBCOMP is then about an exchange whose slot is gone, and rewriting the current one is the cross-exchange mutation F-092 exists to stop",
	"phaseSlotToPubRelReady":               "the PUBREC handoff's canonical readback (Decision 5): it reads the stored PUBREL back and advances the slot publish -> pubrel_ready ONLY when a present, structurally matching record is proven at the sequence. The interpretation is the canonical-origin invariant — a store RESPONSE is never presence proof (I12), so on the interest-retained $MQTT_out a skipped store returns a sequence naming nothing; an absent or mismatched readback leaves the slot in publish for recovery rather than writing a phantom canonical the duplicate branch would later misread",
	"phaseRecoveredPublishSlotToReady":     "recovery route B's canonical handoff: a complete PUBREL scan supplies a present record matching the slot's packet identifier, nonce, source and affine identity; only that record's observed stream sequence may become the canonical tuple, and the slot CAS preserves its current custody and concurrent source facts",
	"finalizeTerminalSlot":                 "the terminal read, whose interpretation is a THREE-part authorization rather than a presence test: subject addressing (F-034), identity match, and a FRESHNESS FENCE on the observed sequence. A rewrite between observation and delete means a concurrent operation re-targeted the exchange and reinstalled memory around it, so a stale terminal continuation preserves the record — the opposite of what TestMQTTInflightClaimTerminalRaceHealsPhantomEntry catches",
	"disposeTerminalPubRelRecords":         "the complete terminal scan and confirmed record disposal: only PUBREL records whose identifier, owner nonce, source and affine identity match the terminal slot are discharged and deleted; an incomplete scan or any uninterpretable same-exchange record retains the slot",
	"releaseTerminalReservation":           "the local consequence of confirmed terminal cleanup: compare-delete only the empty placeholder carrying the exchange nonce, then NAK deferred sources outside the session lock so newly available Receive Maximum capacity cannot remain parked",
	"acquirePublishAction":                 "the DURABLE half of a PUBLISH transmission's exclusion (C-I, F-098), whose interpretation is that the reference it CASes on is rebuilt from THIS read rather than taken from the entry's capture. The capture carries custody as it stood at allocation, and recovery refreshes custody on every eligible slot at resume, so after a reconnect the captured claim is stale and an acquisition comparing custody exactly would refuse every post-resume transmission — transmitting nothing and letting AckWait re-offer forever. The exchange IDENTITY is still checked against the entry's own nonce, and a LIVE TOKEN is refused explicitly here rather than left to the CAS, because the mutator sets the token unconditionally and would otherwise overwrite a live holder's capability",
	"attemptTerminalRegisterRelease":       "ONE attempt at the terminal cleanup a slot is owed (F-098, holistic review round 1, finding 5), whose interpretation is that the record it reads is evidence about ONE bound transmission identity — subject, nonce, source and counter — and nothing else: a successor or another exchange DISCHARGES the obligation without being touched, never re-aimed at; a live action token WAITS for its holder's drain; `publish -> publish_terminal` is written only when the caller carries the client's acknowledgement of THIS transmission, because an ordinary publish record carries no such evidence; a `publish_terminal` record carries its own authority and is deleted through the gated door; an indeterminate or unreadable read retries, and only a confirmed discharge releases the identifier hold",
	"wakePubRelRedrive":                    "the PUBREL redrive waiter's WAKE (F-098), whose interpretation is that it NAKs the exact deliveries that were refused rather than transmitting anything or mutating any state: the refusal stands and only its retry clock changes, so re-admission still performs every ordinary check. EVERY waiter for the identifier is woken, because each is a distinct stored record left without a continuation. It is reached from THREE ends and never unconditionally — the callers gate it on the combined check through wakePubRelRedriveIfClear: the end of a PUBLISH send section, after BOTH the durable action drain and the local section release, because either alone leaves a window; the drain reconciler, which is the only remaining event once that section has ended with its token still standing; and the proven-completion arms of a PUBREL action's own drain, since admission registers against any live token and a PUBREL action outlives the section that acquired it",
	"publishTransmissionStands":            "the PUBREL redrive waiter's RACE-CLOSING read (F-098), whose interpretation is that the waiter is registered FIRST and the transmission re-read AFTERWARDS, over BOTH halves — the slot's durable action token and this broker's local send hold. A blocking transmission can finish between the caller observing it and the registration, and a waiter registered after the wake has already run would never be woken at all, so the order is register, then read; reading first has exactly the hole it would be meant to close. It asks whether ANY blocker still stands rather than whether it is the SAME one, because waking on a replacement token buys a NAK, a re-offer, a fresh refusal and a fresh registration for as long as actions rotate. An UNPROVEN read answers \"stands\": an indeterminate load proves nothing and a present-but-unreadable record is not an absence, so only smsg distinguishes the proven absence that has no durable half; the transmission's own end may still wake the waiter, and AckWait remains the backstop",
	"attemptOwedMaterialization":           "L1's per-attempt read (F-098), whose interpretation is that EVERY ATTEMPT REVALIDATES from this observation and nothing is carried from enrolment but the target identity: the record, its phase, its nonce AND its transmission counter, the live token, and custody. A different nonce or a moved counter means a different transmission owns the identifier now, and re-aiming at the newer counter is exactly what C-I forbids, so the work RETIRES rather than retargets. A living predecessor's token RETAINS the work instead, because custody cannot transfer under one and the token may yet drain. Custody uncertainty is likewise never terminal — unknown acceptance and an ambiguous CAS retain, only a PROVEN displacement stops — because copying a boolean refusal into a scheduler abandons the repair on one transient failure",
	"mqttSlotEvidenceFor":                  "C-IV's SHARED EVIDENCE LOADER (F-098), whose interpretation is that a proof assembled piecemeal at each destructive site is how this subsystem came to hold several authoritative opinions of what a slot means. Every term is resolved from ONE observation or it is indeterminate: the record through loadInflightRecord with subject/body agreement, the marker through loadInflightMarker and compared by NONCE so a predecessor's marker on the same subject is never borrowed as this allocation's, and the cancellation verdict from the CONFIRMED persisted set rather than a staged local intent no restart could see. An authoritative marker ABSENCE is a determinate answer that resolves arm 1, and it proves only possibly-attempted — it never by itself reaches the proven-unsent arm",
	"drainPublishAction":                   "the PUBLISH action's drain, whose interpretation is that IT PRESERVES THE PHASE: a PUBREC handler may legitimately advance the slot past PUBLISH while the bytes are still out, so the drain re-reads, clears its own EXACT token in whatever phase it finds, and derives nothing from the phase. Acq is never reset — a drain ends a transmission's authority without un-counting it, and a reset would hand a later transmission an identity an earlier one's evidence already names. An unconfirmable drain leaves the token standing and the identifier reserved, which is the stated availability cost rather than a leak a lease would fix",
	"startActionDrain":                     "the action's drain, whose interpretation is that THE TARGET IS DERIVED FROM THE STATE OBSERVED rather than remembered from acquisition: a PUBCOMP arriving while the write is out advances the slot sending -> terminal_draining underneath the holder, so a drain aiming at the arrow it chose earlier fails its predecessor forever and leaves the identifier reserved for the life of the session. Each attempt reads the state and picks the arrow belonging to it; an absent slot or a different valid token both prove this action is gone and retire only the local holder",
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
