// Broker-owned $MQTT_* stream prerequisites.
//
// Every JetStream stream the broker owns is provisioned through one shape:
// create-or-lose, then an UNCONDITIONAL authoritative re-read, then a
// per-stream validator over that re-read (ensureBrokerStream). A stream that
// cannot run its owner's protocol is a startup failure with recreate guidance,
// never something to accommodate or mutate in place — these streams have no
// released compatibility contract (docs/ARCHITECTURE.md, "Compatibility
// boundary"), so an unknown-good configuration is an error, and mutating a
// store other brokers may still be serving is actively unsafe.
//
// The doctrine for every validator list: it is DERIVED from what that stream's
// owner actually does — the stores, CAS writes, deletes, purges, consumers and
// scans in its feature file — never copied from another stream's list. That is
// why no two lists here are the same: DenyDelete is fatal on $MQTT_sess (the
// teardown release is the only exit a record has) and waived on $MQTT_rmsgs
// (that owner issues no MESSAGE delete — the empty-body tombstone plus
// MaxMsgsPer:1 replacement carry correctness, and both that store-time eviction
// and TTL reaping are store-internal — and DenyDelete gates message deletion
// only, so the CONSUMER deletes its owner does issue are not gated by it). A setting absent from a list is
// an audited NON-prerequisite with a written reason, not an oversight:
// rejecting a setting no owner operation depends on would refuse legitimate
// deployments — policy hardening, not derivation. The audit is enforced by
// TestStreamValidatorFieldCoverage, which requires every StreamConfig field to
// carry exactly one disposition — rejected, waived, or dominated — for every
// validator, with a non-empty reason for every waiver.

package mqtt

import (
	"fmt"
	"sync/atomic"
	"time"
)

// validateBrokerOwnedStreamShape checks the facts that are prerequisites of
// EVERY $MQTT_* owner because they are prerequisites of storing and reading a
// record at a named subject at all: the exact subject space, durable file
// storage, no mirror (a mirror rejects direct writes outright), the configured
// replica count, not sealed (sealing blocks all writes), no subject transform
// (writes would land on subjects the per-subject CAS, loadLastMsgFor and the
// scan filters do not name), no republish (internal state mirrored onto a
// user-visible subject), not a counter stream (counter streams change the
// owner's own store semantics), acknowledgements on (every synchronous store
// and CAS here reads the ack), no rollup headers admitted (AllowRollup lets
// ANY writer with publish permission purge up to the whole stream in one
// Nats-Rollup message — the same evict-live-state hazard class as the global
// limits every owner rejects, but attacker-triggered; no owner emits rollup
// headers, so nothing legitimate is refused), and the default persistence
// mode (async persistence changes the durability of the owner's own
// confirmed writes).
//
// Everything semantic — retention, discard, the per-subject singleton, limits,
// deletion, purging, consumer limits, sources — stays in the per-owner list,
// because those genuinely differ per owner. Sources in particular is per-owner:
// sourcing is additive (the stream keeps accepting its own writes), so it is
// fatal only where a foreign record could occupy a protocol CAS slot — every
// stream here except $MQTT_rmsgs, where it is a supported topology.
//
// wantReplicas is 0 when no explicit replica count is configured (skip that
// check).
//
// wantSubjects is the EXACT subject set, compared order-independently and
// rejecting duplicates. Most owners name one subject; $MQTT_sess names two,
// because the permanent CONNECT acceptance facts share its stream so that an
// accepted CONNECT can commit the record and the fact in one atomic batch. Order
// independence is not laxity — JetStream does not promise to return a stream's
// subjects in the order they were configured — while a duplicate really is a
// misconfiguration: it would let a two-subject expectation be satisfied by one
// subject repeated, leaving the other subject space unowned.
func validateBrokerOwnedStreamShape(cfg *StreamConfig, wantSubjects []string, wantReplicas int) error {
	if err := validateStreamSubjectSet(cfg.Subjects, wantSubjects); err != nil {
		return err
	}
	if cfg.Storage != FileStorage {
		return fmt.Errorf("storage is %v (want file)", cfg.Storage)
	}
	if cfg.Mirror != nil {
		return fmt.Errorf("stream is a mirror (want none): a mirror rejects the owner's direct writes")
	}
	if wantReplicas > 0 && cfg.Replicas != wantReplicas {
		return fmt.Errorf("replicas is %d (want %d)", cfg.Replicas, wantReplicas)
	}
	if cfg.Sealed {
		return fmt.Errorf("stream is sealed: it blocks all writes")
	}
	if cfg.SubjectTransform != nil {
		return fmt.Errorf("a subject transform is configured: it rewrites the subjects that CAS identity, loadLastMsgFor and the scan filters depend on")
	}
	if cfg.RePublish != nil {
		return fmt.Errorf("republish is configured: it would mirror internal broker state onto a user-visible subject")
	}
	if cfg.AllowMsgCounter {
		return fmt.Errorf("allow_msg_counter is set: counter streams do not support the store semantics used here")
	}
	if cfg.AllowRollup {
		return fmt.Errorf("allow_rollup_hdrs is set: a rollup header purges up to the whole stream on a plain publish, evicting live protocol state no owner operation removes that way")
	}
	if cfg.NoAck {
		return fmt.Errorf("no_ack is set: it suppresses the acknowledgements every synchronous store and CAS here depends on")
	}
	if cfg.PersistMode != DefaultPersistMode {
		return fmt.Errorf("persist_mode is %v (want the default): its durability semantics are unproven for these protocols", cfg.PersistMode)
	}
	return nil
}

// validateStreamSubjectSet compares a stream's configured subjects against the
// exact set its owner requires: same members, no extras, no duplicates, any order.
func validateStreamSubjectSet(got, want []string) error {
	fail := func() error {
		return fmt.Errorf("subjects %v (want exactly %v, in any order)", got, want)
	}
	if len(got) != len(want) {
		return fail()
	}
	seen := make(map[string]struct{}, len(got))
	for _, s := range got {
		if _, dup := seen[s]; dup {
			return fmt.Errorf("subjects %v repeat %q (want exactly %v, each once)", got, s, want)
		}
		seen[s] = struct{}{}
	}
	for _, s := range want {
		if _, ok := seen[s]; !ok {
			return fail()
		}
	}
	return nil
}

// brokerStreamLookupAPI is what a PREFLIGHT gets: reads and nothing else.
//
// The narrowness is the mechanism, not a style preference. Both preflights here
// decide whether the account may be provisioned at all, and their whole value
// rests on their creating nothing while they decide — a create from inside one
// would provision the very shape it is deciding to refuse. Handing them an
// interface with no createStream makes that unrepresentable rather than
// asserted.
type brokerStreamLookupAPI interface {
	lookupStream(name string) (*StreamInfo, error)
}

// brokerStreamAPI is the minimal seam ensureBrokerStream needs, satisfied by
// *mqttJSA and by test stubs.
type brokerStreamAPI interface {
	brokerStreamLookupAPI
	createStream(cfg *StreamConfig) (*StreamInfo, bool, error)
}

// ensureBrokerStream is the one convergence shape every broker-owned stream is
// provisioned through: initial lookup; if absent, create-or-lose (only
// JSStreamNameExistErr is a benign loss); then an UNCONDITIONAL authoritative
// re-read, validated and returned. The re-read is the point: a create-race
// loser — and equally the winner, whose create response is NOT authoritative
// for what now exists — validates the stream that is actually there, never the
// config it asked for.
//
// The hooks preserve the session create-race seams: preCreate runs after an
// absent initial lookup and immediately BEFORE the create (testSessCreateGate's
// placement), observe receives the raw (created, err) create outcome
// (testSessCreateObserve's contract). Either may be nil.
//
// validate runs over the re-read StreamInfo and its error is returned verbatim,
// so the caller's closure carries the per-stream recreate guidance.
func ensureBrokerStream(api brokerStreamAPI, cfg *StreamConfig,
	validate func(*StreamInfo) error,
	preCreate func(), observe func(bool, error)) (*StreamInfo, error) {

	si, err := api.lookupStream(cfg.Name)
	if err != nil && isErrorOtherThan(err, JSStreamNotFoundErr) {
		return nil, fmt.Errorf("lookup stream %q: %w", cfg.Name, err)
	}
	if si == nil {
		if preCreate != nil {
			preCreate()
		}
		_, created, cerr := api.createStream(cfg)
		if observe != nil {
			observe(created, cerr)
		}
		if isErrorOtherThan(cerr, JSStreamNameExistErr) {
			return nil, fmt.Errorf("create stream %q: %w", cfg.Name, cerr)
		}
	}
	// Unconditional authoritative re-read: creation success, a create-race loss,
	// and the pre-existing-stream path all converge here.
	si, err = api.lookupStream(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("lookup stream %q after create: %w", cfg.Name, err)
	}
	if si == nil {
		return nil, fmt.Errorf("stream %q not found after create", cfg.Name)
	}
	if verr := validate(si); verr != nil {
		return nil, verr
	}
	return si, nil
}

// validateMsgsStream verifies $MQTT_msgs can carry stored PUBLISH messages and
// their consumers. Derived from what this owner does: plain stores carrying a
// per-message Nats-TTL (MQTT 5.0 Message Expiry); durable, drained and shared
// queue consumers — one per plain subscription, per shared group and per drain,
// every one created with an explicit MaxAckPending; consumer deletes; loadMsg
// by sequence. It never deletes messages and never purges, so
// DenyDelete/DenyPurge are NOT prerequisites here (verified against every
// deleteMsg/purgeStream call site).
//
// InterestPolicy is load-bearing in both directions (docs/ARCHITECTURE.md): it
// is how undelivered messages disappear with their consumers, and why a store
// with no interest succeeds and keeps nothing.
//
// ConsumerLimits.InactiveThreshold is deliberately NOT checked: plain
// subscriptions already run under exactly that policy via the broker's own
// ConsumerInactiveThreshold option, and the paths that cannot tolerate it —
// a drain (whether an UNSUBSCRIBE or a QoS 0 downgrade created it) and the
// shared/affine consumers — refuse it at runtime
// (drainingAllowed in unsub_drain.go; the shared-subscription guard), with the
// residual recorded in docs/CONFORMANCE.md. The same field IS fatal on
// $MQTT_out: that asymmetry is the derive-per-owner rule, not an inconsistency.
//
// maxAckPending is the broker's effective per-consumer cap
// (opts.MQTT.MaxAckPending or mqttDefaultMaxAckPending). A stream
// ConsumerLimits.MaxAckPending below it makes JetStream reject the consumer
// create outright, failing SUBSCRIBE; a limit at or above it changes nothing,
// so only a lower one is fatal. A reload that raises the broker cap past the
// stream limit is not covered by this startup check; the consumer-create
// rejection remains the backstop (stated residual in docs/CONFORMANCE.md).
// MaxMsgSize is a BLANKET rejection, not a threshold against the broker's max
// payload, and the distinction was review-tested in both directions: the
// PUBLISH admission preflight does bound the complete stored form by max
// payload (mqttComputeNatsMsgSize / mqttMaxQoS2FormSize), but QoS 1/2 Will
// stores bypass that preflight entirely (mqttHandleWill and deliverWillRecord
// call mqttInitiateMsgDelivery directly), so a cap equal to the max payload
// can still reject a legal CONNECT's Will — silently for an immediate Will.
// No positive cap is provably inert, so none is accepted.
//
// requireMsgTTL is true when this broker accepts MQTT 5.0 (the default): a
// stored PUBLISH may then carry a per-message Nats-TTL (Message Expiry) that a
// stream without AllowMsgTTL would reject. A max_protocol_version:4 broker
// refuses every MQTT 5 CONNECT, so no owner operation ever emits the header —
// requiring the capability there would refuse a legitimate 3.1.1-only
// deployment, so the requirement is derived only where the operation exists.
func validateMsgsStream(si *StreamInfo, wantReplicas, maxAckPending int, requireMsgTTL bool, bundleEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttStreamSubjectPrefix + ">"}, wantReplicas); err != nil {
		return err
	}
	if cfg.Retention != InterestPolicy {
		return fmt.Errorf("retention is %v (want interest): undelivered messages must disappear with their consumers, and limits retention would retain every message forever", cfg.Retention)
	}
	if cfg.MaxMsgsPer > 0 {
		return fmt.Errorf("max_msgs_per_subject is %d (want unlimited): a per-subject cap would evict older undelivered PUBLISHes on a busy topic", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict undelivered QoS 1/2 messages",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): foreign records would be delivered to clients as PUBLISHes")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): QoS 1/2 Will stores bypass the admission preflight, so no positive cap is provably unable to reject a legitimate store", cfg.MaxMsgSize)
	}
	if requireMsgTTL && !cfg.AllowMsgTTL {
		return fmt.Errorf("allow_msg_ttl is not set: stored PUBLISHes carry a per-message Nats-TTL for MQTT 5.0 Message Expiry, which the stream would reject")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages would be delivered to clients as PUBLISHes", cfg.SubjectDeleteMarkerTTL)
	}
	if cfg.MaxConsumers > 0 {
		return fmt.Errorf("max_consumers is %d (want unlimited): the broker creates one durable per subscription, shared group and drain, so a cap eventually rejects a legitimate SUBSCRIBE", cfg.MaxConsumers)
	}
	if l := cfg.ConsumerLimits.MaxAckPending; l != 0 && l < maxAckPending {
		return fmt.Errorf("stream consumer max_ack_pending limit is %d, below the broker's effective cap %d: JetStream would reject every consumer create, failing SUBSCRIBE", l, maxAckPending)
	}
	// The bundle stamp: this stream carries the account's recovery epoch, which is
	// what makes the nine broker-owned streams one recreation unit rather than nine
	// independently recoverable stores (F-066).
	if err := validateBundleEpochStamp(&cfg, bundleEpoch); err != nil {
		return err
	}
	return nil
}

// validateRetainedStream verifies $MQTT_rmsgs can carry retained messages.
// Derived from what this owner does: one plain store per topic subject with a
// per-message Nats-TTL, including empty-body tombstones; loadLastMsgForMulti
// reads; NO explicit message-delete or purge API calls at all;
// and one ephemeral AckNone consumer per broker with a 5-minute
// InactiveThreshold, which this owner CREATES and DELETES by name (see
// ephemeral_identity.go).
//
// DenyDelete is deliberately NOT a prerequisite: this owner issues no MESSAGE
// delete, and DenyDelete gates nothing else — the consumer deletes it does issue
// (the observer's pre-create delete and the rollback's, see ephemeral_identity.go)
// are CONSUMER.DELETE requests, which no stream setting here can refuse.
// The durable deletion authority is the empty-body tombstone plus
// MaxMsgsPer:1 replacement (docs/PORTING.md), and BOTH the store-time eviction
// that replacement performs and TTL expiry are reaped by the store engine
// itself, which DenyDelete does not gate. Denying deletes therefore costs
// nothing here.
// Sources is NOT a prerequisite either: cross-account retained sourcing with
// subject transforms is a supported topology (the decoder handles sourced
// records explicitly). ConsumerLimits is not checked: a stream
// InactiveThreshold below the requested 5 minutes already fails initialization
// at the consumer create itself, naming the consumer, and any positive
// MaxAckPending limit is copied into the zero-request consumer and rejected
// against AckNone at the same create — a validator entry would duplicate that
// enforcement. MaxConsumers IS checked: the ephemeral consumer is per broker
// per account, so a finite cap eventually rejects a legitimate broker start.
//
// requireMsgTTL follows validateMsgsStream: retained Message Expiry is an
// MQTT 5.0 property, so a max_protocol_version:4 broker never emits Nats-TTL
// on a retained store and must not require the capability.
func validateRetainedStream(si *StreamInfo, wantReplicas int, requireMsgTTL bool, bundleEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttRetainedMsgsStreamSubject + ">"}, wantReplicas); err != nil {
		return err
	}
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits): nothing holds consumer interest on retained messages, so any other retention drops them", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		return fmt.Errorf("discard is %v (want old): DiscardNew would reject the MaxMsgsPer:1 replacement that supersedes a retained message", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		return fmt.Errorf("max_msgs_per_subject is %d (want 1): per-topic supersession is replacement of the single record, and the tombstone protocol depends on it", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict live retained messages",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if cfg.MaxMsgSize > 0 {
		// Unlike $MQTT_msgs there is no preflight-relative threshold here: the
		// retained stored form wraps the payload in topic, props, origin, expiry
		// and TTL headers with no preflighted bound on the total, so no positive
		// cap is provably inert.
		return fmt.Errorf("max_msg_size is %d (want unlimited): the retained stored form has no preflighted bound, so a cap could reject a legitimate retained PUBLISH", cfg.MaxMsgSize)
	}
	if requireMsgTTL && !cfg.AllowMsgTTL {
		return fmt.Errorf("allow_msg_ttl is not set: retained messages with an MQTT 5.0 Message Expiry carry a per-message Nats-TTL, which the stream would reject")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages would be decoded as retained records", cfg.SubjectDeleteMarkerTTL)
	}
	if cfg.MaxConsumers > 0 {
		return fmt.Errorf("max_consumers is %d (want unlimited): every broker creates its own ephemeral retained-messages consumer, so a cap eventually rejects a legitimate broker start", cfg.MaxConsumers)
	}
	// The bundle stamp: this stream carries the account's recovery epoch, which is
	// what makes the nine broker-owned streams one recreation unit rather than nine
	// independently recoverable stores (F-066).
	if err := validateBundleEpochStamp(&cfg, bundleEpoch); err != nil {
		return err
	}
	return nil
}

// validateBundleEpochStamp checks that a stream carries the account's recovery
// epoch, which is what makes the nine broker-owned streams ONE recreation unit
// rather than nine independently recoverable stores.
//
// The epoch is minted by $MQTT_sess at its creation and adopted by every broker
// from ensureBrokerStream's authoritative re-read, so a stream stamped with a
// different value belongs to a different incarnation of the bundle: its records
// persist identities — sequences, consumer durables, subjects — that other
// streams issued and that a recreation has since reissued to unrelated state
// (docs/ARCHITECTURE.md, "Stream prerequisites"; F-066).
//
// wantEpoch is checked FIRST and a malformed or empty one is itself the
// refusal, never a wildcard that admits anything. That direction is the whole
// hazard: an epoch this broker could not read collapses every value into one
// identity, and the comparison below would then pass for a stream from any
// incarnation at all. The caller reaches this with an unusable wantEpoch only
// through an ordering fault, which is exactly when admitting would be worst.
//
// Unrelated metadata keys are accepted: this owns one key, not the map.
func validateBundleEpochStamp(cfg *StreamConfig, wantEpoch string) error {
	if !mqttValidSessEpoch(wantEpoch) {
		return fmt.Errorf("this account's recovery epoch is unusable (%q): the %s bundle cannot be compared, so no stream is admitted",
			wantEpoch, mqttSessStreamName)
	}
	if got := cfg.Metadata[mqttSessEpochMetaKey]; got != wantEpoch {
		return fmt.Errorf("%q metadata is %q, want this account's recovery epoch %q: the stream belongs to a different incarnation of the broker-owned bundle",
			mqttSessEpochMetaKey, got, wantEpoch)
	}
	return nil
}

// mqttPreflightBrokerStreamBundle refuses an account whose broker-owned streams
// are a PROPER SUBSET of the nine, before anything is created.
//
// It exists because ensureBrokerStream cannot enforce this: that helper sees one
// stream and CREATES it when the lookup says absent, so by the time any
// validator runs the missing member has already been provisioned — silently
// re-establishing the partial-recreation hazard F-066 records. The comparison
// has to happen where all nine are visible and nothing has been created yet.
//
// The names come from the SAME spec table the provisioning loop iterates, never
// from a second list: a hand-maintained copy drifts from the table, and
// TestBrokerStreamSpecTable proves the table's contents, not that some other
// list matches it.
//
// Each lookup's (si, err) PAIR is dispositioned together, exactly as
// mqttRefuseRetiredOriginStream does and for the same reason — deciding on one
// half and then the other is what produced two fail-open defects there:
//
//	si != nil, any error   -> PRESENT. Positive evidence outranks a contradictory
//	                          error, including a not-found one.
//	si == nil, not-found   -> ABSENT.
//	si == nil, other error -> UNDECIDED. An unreadable stream is not an absent one.
//	si == nil, no error    -> UNDECIDED. A reply that decoded but carried neither.
//
// Both undecided arms FAIL STARTUP, naming the stream. Folding either into
// ABSENCE is the fail-open that matters most here: a transient lookup outage
// read as absence turns an eight-present account into a zero-present one, this
// check admits, and the loop recreates the missing stream — the exact hazard,
// arriving through the check meant to close it.
//
// Then, over the presence vector: zero admits (initial creation, or a completed
// whole-bundle reset); nine admits (per-stream epoch validation in the loop
// enforces the bundle's identity from there); anything between REFUSES.
//
// The scan is nine independent lookups and therefore NOT atomic, which bounds
// what admission can promise. Under create-only concurrency — every other actor
// a broker running this same ordered provisioning — an admission cannot
// wrongfully succeed: an all-absent observation may race a peer's creation but
// then adopts that peer's epoch through create-or-lose, and an all-present
// observation is truthful because presence is monotone when nothing deletes.
// Concurrent DELETION breaks the second arm and is a stated residual
// (docs/CONFORMANCE.md): an operator deleting streams behind this scan, leaving
// $MQTT_sess for last, can be observed as nine-present. That requires violating
// the all-brokers-stopped precondition the reset procedure states.
func mqttPreflightBrokerStreamBundle(jsa brokerStreamLookupAPI, specs []brokerStreamSpec) error {
	present := make([]string, 0, len(specs))
	absent := make([]string, 0, len(specs))
	for _, spec := range specs {
		si, err := jsa.lookupStream(spec.name)
		if h := mqttTestPreflightLookupHookV.Load(); h != nil {
			(*h)(spec.name)
		}
		switch {
		case si != nil:
			present = append(present, spec.name)
		case err == nil:
			return fmt.Errorf("checking the broker-owned stream bundle: the server answered for %s "+
				"without stream information, so whether this account holds it cannot be decided",
				spec.name)
		case isErrorOtherThan(err, JSStreamNotFoundErr):
			return fmt.Errorf("checking the broker-owned stream bundle: looking up %s: %w", spec.name, err)
		default:
			absent = append(absent, spec.name)
		}
	}
	if len(absent) == 0 || len(present) == 0 {
		return nil
	}
	return fmt.Errorf("this account holds %d of the %d broker-owned streams — present: %v; absent: %v. "+
		"These streams are ONE recreation unit: records on each persist identities the others issue "+
		"(sequences, consumer durables, subjects), so recreating a proper subset leaves stale references "+
		"matching new unrelated state. Recreating the absent ones alone is NOT the recovery. "+
		"Stop ALL brokers for this account, delete every one of the %d streams (%v), and restart, which "+
		"recreates the whole bundle — this is not a rolling upgrade, see docs/ARCHITECTURE.md "+
		"\"Compatibility boundary\". A first start that was interrupted part-way leaves the same shape "+
		"and takes the same recovery",
		len(present), len(specs), present, absent, len(specs), mqttBrokerStreamNames(specs))
}

// mqttTestPreflightLookupHookV runs after each of the preflight's per-stream lookups,
// naming the stream just observed. Nil in production.
//
// It exists because the preflight's presence vector is assembled from nine INDEPENDENT
// lookups, so a broker whose scan overlaps a peer's ordered creation can observe a
// vector that never existed durably — the reason this check promises fail-closed
// refusal rather than an atomic snapshot. That interleaving cannot be produced by
// starting two brokers and hoping; it needs the scan paused at a chosen point while
// the peer provisions, which is exactly what this seam gives a test. Package-level for
// the same reason as the watch-consumer seam: the pause has to happen before any
// account manager exists to hang it on.
var mqttTestPreflightLookupHookV atomic.Pointer[func(string)]

// mqttBrokerStreamBundleList names the recreation unit for guidance text that
// cannot reach the spec table — every validator's recovery sentence. It is
// prose, and the authoritative set is the table; TestBrokerStreamSpecTable pins
// that the two agree, because guidance naming a different set than the preflight
// enforces is how an operator is sent to delete the wrong thing.
const mqttBrokerStreamBundleList = mqttSessStreamName + ", " + mqttWillQStreamName + ", " +
	mqttTeardownStreamName + ", " + mqttStreamName + ", " + mqttQoS2IncomingMsgsStreamName + ", " +
	mqttQoS2AssignStreamName + ", " + mqttInflightStreamName + ", " + mqttSharedLifecycleStreamName + ", " +
	mqttOutStreamName + ", " + mqttRetainedMsgsStreamName

// mqttBrokerStreamNames is the bundle's names in provisioning order, for
// diagnostics that must tell an operator exactly what to delete.
func mqttBrokerStreamNames(specs []brokerStreamSpec) []string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, spec.name)
	}
	return names
}

// mqttRefuseRetiredOriginStream fails startup when this account still holds
// $MQTT_origin, the session-interval register the write-lineage audit deleted.
//
// It cannot be a provisioning-table entry with a validator that always fails:
// ensureBrokerStream CREATES an absent stream before validating anything, so an
// entry would provision the very stream this refuses. Hence a LOOKUP-ONLY check,
// run before the provisioning loop — and it takes brokerStreamLookupAPI, so
// "creates nothing" is a property of the type rather than a promise.
//
// lookupStream returns the response's StreamInfo and its error INDEPENDENTLY, so the
// two are decided as a PAIR rather than in sequence. Deciding on the error first and
// the info second produced two separate fail-open defects here, one in each direction,
// so the arms below cover the whole matrix and nothing falls through:
//
//	si != nil, any error   -> PRESENT. Positive evidence that the stream exists outranks
//	                          an accompanying error, a not-found one included: a response
//	                          carrying both is self-contradictory, and the half saying the
//	                          forbidden stream EXISTS is the half that must win.
//	si == nil, not-found   -> ABSENT. The only outcome that proceeds.
//	si == nil, other error -> UNDECIDED. An unreadable account is not an account without
//	                          the stream.
//	si == nil, no error    -> UNDECIDED. A reply that decoded but carried neither: a
//	                          truncated, malformed or incompatible STREAM.INFO answer.
//
// Both undecided arms FAIL STARTUP. Folding either into absence is a fail-open on the
// one path that has to be fail-closed — the stream may well exist, and nothing
// downstream ever looks again.
//
// The register's records are not decoded, migrated or deleted. A stream this
// design no longer defines is an unsupported persisted shape, which
// docs/ARCHITECTURE.md's "Compatibility boundary" refuses rather than carries;
// pre-release that costs each development account one stream deletion.
func mqttRefuseRetiredOriginStream(jsa brokerStreamLookupAPI) error {
	si, err := jsa.lookupStream(mqttOriginStreamName)
	switch {
	case si != nil:
		// Present. Falls through to the refusal below.
	case err == nil:
		return fmt.Errorf("checking for the retired %s stream: the server answered without "+
			"stream information, so whether this account still holds it cannot be decided",
			mqttOriginStreamName)
	case isErrorOtherThan(err, JSStreamNotFoundErr):
		return fmt.Errorf("checking for the retired %s stream: %w", mqttOriginStreamName, err)
	default:
		return nil // absent, and the only outcome that proceeds
	}
	return fmt.Errorf("this account still holds %s, the session-interval register this broker "+
		"no longer defines; its identities parented session-record writes and nothing reads "+
		"them any more, so the stream is an unsupported persisted shape rather than an unused "+
		"one: stop ALL brokers for this account and delete %s (this is not a rolling upgrade — "+
		"see docs/ARCHITECTURE.md \"Compatibility boundary\")",
		mqttOriginStreamName, mqttOriginStreamName)
}

// brokerStreamSpec is one entry of the ordered provisioning table: everything
// ensureBrokerStream needs for one broker-owned stream, plus the per-stream
// post step. Provisioning iterates the table with a single loop, so no stream
// can be provisioned outside the convergence shape without deleting its entry
// — which the table-coverage test refuses.
type brokerStreamSpec struct {
	name string        // stream name, for the coverage test and error context
	txt  string        // human-readable name used in error messages
	cfg  *StreamConfig // the configuration the broker creates
	// validate wraps the stream's validator with its recreate guidance; its
	// error is surfaced verbatim as the startup failure.
	validate  func(*StreamInfo) error
	preCreate func()                  // runs between the absent lookup and the create
	observe   func(bool, error)       // receives the raw createStream outcome
	post      func(*StreamInfo) error // runs after validation, on the authoritative info
}

// brokerStreamSpecs returns the ordered provisioning table for the nine
// broker-owned $MQTT_* streams. wantReplicas is 0 when no explicit replica
// count is configured; maxAckPending is the broker's effective per-consumer
// cap; requireMsgTTL is whether this broker accepts MQTT 5.0 (see
// validateMsgsStream).
//
// The table carries no JetStream API and creates nothing itself: the retained
// observer this function used to create as a post step now lives in
// mqttProvisionEphemeralConsumers, which runs after every stream has validated.
// $MQTT_rmsgs was already the last entry, so nothing is delayed by the move —
// what it buys is that a reader confirms "no consumer is created until every
// stream validated" at one call site rather than by auditing nine entries.
func (as *mqttAccountSessionManager) brokerStreamSpecs(s *Server,
	replicas, wantReplicas, maxAckPending int, requireMsgTTL bool) []brokerStreamSpec {

	// The provisioning loop prefixes every error with the stream and account, so
	// the guidance here carries only the verdict and the recovery. The recovery is
	// the WHOLE BUNDLE, never this stream alone: records on each of the nine
	// persist identities the others issue, so recreating a proper subset leaves
	// stale references matching new unrelated state (F-066). Startup refuses a
	// proper subset outright (mqttPreflightBrokerStreamBundle), so guidance naming
	// one stream would prescribe a procedure the next start rejects.
	stopAll := func(verr error, stream, req string) error {
		return fmt.Errorf("not usable: %w; stop ALL brokers for this account, delete ALL NINE broker-owned streams (%s) and restart, which recreates the bundle with the required %s configuration — these streams are ONE recreation unit and recreating %s alone is refused at the next startup (this is not a rolling upgrade — see docs/ARCHITECTURE.md \"Compatibility boundary\")",
			verr, mqttBrokerStreamBundleList, req, stream)
	}

	// The account's recovery epoch is minted by $MQTT_sess at its creation and
	// adopted from the authoritative re-read in that entry's post step, so it does
	// not exist when this table is built. Every other stream is therefore stamped
	// by a preCreate, which runs immediately before its own create — after
	// $MQTT_sess's iteration, because $MQTT_sess is entry 0.
	//
	// A create-race loser never reaches its preCreate and validates the winner's
	// stream instead, whose stamp is the identical value: every broker adopted the
	// same epoch from the same session-stream re-read.
	//
	// An unstamped config is left unstamped rather than guessed when the epoch is
	// somehow missing, so the validator refuses the stream instead of admitting an
	// uncomparable one. That is unreachable in the provisioning order and is
	// written down as fail-closed rather than as an invariant.
	stampEpoch := func(cfg *StreamConfig) func() {
		return func() {
			if as.sessStreamEpoch == _EMPTY_ {
				return
			}
			cfg.Metadata = map[string]string{mqttSessEpochMetaKey: as.sessStreamEpoch}
		}
	}

	// Hoisted so stampEpoch can close over them: a config literal inside the table
	// cannot be stamped, because the table is built before the loop that adopts
	// the epoch.
	sessCfg := &StreamConfig{
		Name: mqttSessStreamName,
		// THREE subject spaces, one stream: the session records, the permanent
		// per-CONNECT acceptance facts an accepted CONNECT commits atomically WITH
		// its record, and the permanent handoff outcomes F-105's ownership handoff
		// commits in that same batch. All three are top-level rather than nested
		// under "sess.", because every session walk filters "sess.<domainTk>>" and
		// decodes what it gets as a session record. A batch is per stream, so the fact cannot live anywhere else
		// without reintroducing the interval in which a session is accepted and its
		// evidence is not yet durable. The accept prefix is top-level rather than
		// nested under "sess.", because every session walk in this package filters
		// "sess.<domainTk>>" and decodes what it gets as a session record.
		Subjects: []string{
			mqttSessStreamSubjectPrefix + as.domainTk + ">",
			mqttAcceptSubjectPrefix + as.domainTk + ">",
			mqttHandoffSubjectPrefix + as.domainTk + ">",
			mqttTeardownReceiptSubjectPrefix + as.domainTk + ">",
		},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Replicas:   replicas,
		MaxMsgsPer: 1,
		// Required, not optional: the accept batch carries Nats-Batch-* headers,
		// which a stream without this rejects.
		AllowAtomicPublish: true,
		// The account's recovery epoch, MINTED here. This is the only stream that
		// generates one; the other eight are stamped with the value adopted from
		// this stream's authoritative re-read.
		Metadata: map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
	}
	willQCfg := &StreamConfig{
		Name:       mqttWillQStreamName,
		Subjects:   []string{mqttWillQSubjectPrefix + as.domainTk + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Replicas:   replicas,
		MaxMsgsPer: 1,
	}
	teardownCfg := &StreamConfig{
		Name:       mqttTeardownStreamName,
		Subjects:   []string{mqttTeardownSubjectPrefix + as.domainTk + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Discard:    DiscardOld,
		Replicas:   replicas,
		MaxMsgsPer: 1,
	}
	msgsCfg := &StreamConfig{
		Name:      mqttStreamName,
		Subjects:  []string{mqttStreamSubjectPrefix + ">"},
		Storage:   FileStorage,
		Retention: InterestPolicy,
		Replicas:  replicas,
		// Honor MQTT 5.0 Message Expiry Interval: a stored PUBLISH carries a
		// per-message Nats-TTL so JetStream drops it once its lifetime has
		// elapsed, even if never delivered. Spec5 [3.3.2.3.3].
		AllowMsgTTL: true,
	}
	qos2InCfg := &StreamConfig{
		Name:       mqttQoS2IncomingMsgsStreamName,
		Subjects:   []string{mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Discard:    DiscardOld,
		MaxMsgsPer: 1,
		Replicas:   replicas,
	}
	qos2AssignCfg := &StreamConfig{
		Name:       mqttQoS2AssignStreamName,
		Subjects:   []string{mqttQoS2AssignStreamSubjectPrefix + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Discard:    DiscardOld,
		MaxMsgsPer: 1,
		Replicas:   replicas,
	}
	inflightCfg := &StreamConfig{
		Name:       mqttInflightStreamName,
		Subjects:   []string{mqttInflightStreamSubjectPrefix + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Discard:    DiscardOld,
		MaxMsgsPer: 1,
		Replicas:   replicas,
		// Required, not optional: a PUBLISH-phase register and its unsent marker are
		// created in one atomic batch (inflight_marker.go), which a stream without
		// this rejects.
		AllowAtomicPublish: true,
	}
	outCfg := &StreamConfig{
		Name:      mqttOutStreamName,
		Subjects:  []string{mqttOutSubjectPrefix + ">"},
		Storage:   FileStorage,
		Retention: InterestPolicy,
		Replicas:  replicas,
	}
	retainedCfg := &StreamConfig{
		Name:       mqttRetainedMsgsStreamName,
		Subjects:   []string{mqttRetainedMsgsStreamSubject + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Replicas:   replicas,
		MaxMsgsPer: 1,
		// Honor MQTT 5.0 Message Expiry Interval for retained messages via a
		// per-message Nats-TTL. Spec5 [3.3.2.3.3].
		AllowMsgTTL: true,
	}
	sharedCfg := &StreamConfig{
		Name:       mqttSharedLifecycleStreamName,
		Subjects:   []string{mqttSharedLifecycleStreamSubjectPrefix + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		Discard:    DiscardOld,
		MaxMsgsPer: 1,
		Replicas:   replicas,
		// A join commits the group record and the acquisition ledger in ONE atomic
		// batch (shared_lifecycle.go), which a stream without this rejects.
		AllowAtomicPublish: true,
		// These records are permanent: a group record is CAS-rewritten and an
		// acquisition ledger is sealed, but neither is ever deleted within the
		// recovery epoch. Denying delete enforces that, and it is also what keeps this
		// stream immune to the MaxMsgsPer revival defect (docs/known-upstream-issues.md):
		// with no explicit delete there is no tombstone, so no evicted-then-deleted
		// shape exists for a raw-block recovery to revive.
		DenyDelete: true,
	}
	return []brokerStreamSpec{
		{
			name: mqttSessStreamName,
			txt:  "sessions",
			// A generated recovery epoch is stamped into the stream metadata at
			// creation (the linearization point): whichever replica wins the create
			// sets the authoritative epoch, and everyone — winner included — adopts
			// it from ensureBrokerStream's authoritative re-read in the post step,
			// after validateSessStream has accepted it on that same snapshot. Every
			// other broker-owned stream is then stamped with that value, which is
			// what makes the nine one recreation unit.
			cfg: sessCfg,
			validate: func(si *StreamInfo) error {
				if verr := validateSessStream(si, as.domainTk, wantReplicas); verr != nil {
					// $MQTT_sess MINTS the account's recovery epoch, so recreating it
					// alone leaves every other stream stamped with an incarnation that
					// no longer exists — and takes every permanent CONNECT acceptance
					// fact with it, since those live on this stream, so no delayed Will
					// enqueued beforehand can be suppressed by the acceptance entitled
					// to suppress it. The recovery is the whole bundle.
					return stopAll(verr, mqttSessStreamName, "session-record")
				}
				return nil
			},
			preCreate: func() {
				if gate := s.mqtt.sessmgr.testSessCreateGate; gate != nil {
					gate() // test seam: barrier racing managers at the lookup->create boundary
				}
			},
			observe: func(created bool, err error) {
				if observe := s.mqtt.sessmgr.testSessCreateObserve; observe != nil {
					observe(created, err)
				}
			},
			post: func(si *StreamInfo) error {
				// Adopt the authoritative recovery epoch, already validated by
				// validateSessStream on this exact snapshot. Every accepted CONNECT and
				// every native Will record is stamped with this identity, and so is
				// every other broker-owned stream. See will_cancel.go.
				//
				// From the RE-READ (si), never from sessCfg: the config carries the
				// value this broker ASKED for, and a create-race loser's ask is
				// discarded. Adopting the ask would leave two brokers stamping the
				// other eight streams with two different epochs.
				as.sessStreamEpoch = si.Config.Metadata[mqttSessEpochMetaKey]
				return nil
			},
		},
		{
			name: mqttWillQStreamName,
			txt:  "will outbox",
			// Delayed-Will durable outbox stream (Layer 2). Separate from the
			// sessions stream so its per-Will subjects and sequences never interfere
			// with the in-memory session record's CAS token (sess.seq). MaxMsgsPer:1
			// makes each per-Will subject a single-record CAS slot. See will_outbox.go.
			cfg:       willQCfg,
			preCreate: stampEpoch(willQCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateWillQStream(si, as.domainTk, wantReplicas, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttWillQStreamName, "delayed-Will outbox")
				}
				return nil
			},
		},
		{
			name: mqttTeardownStreamName,
			txt:  "teardown retirement watermark",
			// Durable per-client generation-retirement watermark stream. Permanent
			// per-client retention (MaxMsgsPer:1, no global limits so a fence is never
			// evicted; DiscardOld so a CAS replacement of a client's prior watermark is
			// never rejected). See sess_teardown.go.
			cfg:       teardownCfg,
			preCreate: stampEpoch(teardownCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateTeardownStream(si, as.domainTk, wantReplicas, as.sessStreamEpoch); verr != nil {
					// A MarkerSeq is a $MQTT_sess sequence, so these permanent watermarks
					// are only comparable within the incarnation they were written under.
					// That coupling is now the bundle's, not a private pair: an unfenced
					// generation whose resources are already deleted is the cost.
					return stopAll(verr, mqttTeardownStreamName, "generation-retirement watermark")
				}
				return nil
			},
		},
		{
			name:      mqttStreamName,
			txt:       "messages",
			cfg:       msgsCfg,
			preCreate: stampEpoch(msgsCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateMsgsStream(si, wantReplicas, maxAckPending, requireMsgTTL, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttStreamName, "interest-retained messages")
				}
				return nil
			},
		},
		{
			name: mqttQoS2IncomingMsgsStreamName,
			txt:  "QoS2 incoming messages",
			// Inbound QoS 2 hold state. One record per (client, generation, packet
			// identifier) subject (MaxMsgsPer:1), holding either a `held` PUBLISH
			// awaiting its PUBREL or the terminal `delivered` tombstone that a
			// committed move leaves behind. DiscardOld is load-bearing, not a
			// preference: the tombstone is written by a CAS at the held record's
			// exact sequence, and a DiscardNew/DiscardNewPer configuration would
			// REJECT that replacement instead of performing it. storeQoS2HoldCAS and
			// storeQoS2DoneCAS are the only writers of these subjects. See
			// qos2_move.go.
			cfg:       qos2InCfg,
			preCreate: stampEpoch(qos2InCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateQoS2InStream(si, wantReplicas, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttQoS2IncomingMsgsStreamName, "move-protocol")
				}
				return nil
			},
		},
		{
			name: mqttQoS2AssignStreamName,
			txt:  "QoS2 shared assignment",
			// Shared-subscription QoS 2 assignment claims. A durable record per
			// (group durable, source sequence) elects and then holds the session
			// that owns a STARTED delivery, indefinitely and independently of any
			// deduplication window. See qos2_shared_affine.go.
			cfg:       qos2AssignCfg,
			preCreate: stampEpoch(qos2AssignCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateQoS2AssignStream(si, wantReplicas, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttQoS2AssignStreamName, "assignment-claim")
				}
				return nil
			},
		},
		{
			name: mqttInflightStreamName,
			txt:  "outbound in-flight register",
			// Outbound in-flight registers. One record per started QoS 1/2 PUBLISH,
			// keyed by the packet identifier it went out under (MaxMsgsPer:1), so an
			// identifier survives a broker restart or a failover. Its own stream:
			// $MQTT_out is interest-retained and would keep nothing, and a
			// per-delivery write to the session record would put every outbound
			// QoS 1/2 message behind the record CAS. See inflight_register.go.
			cfg:       inflightCfg,
			preCreate: stampEpoch(inflightCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateInflightStream(si, wantReplicas, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttInflightStreamName, "in-flight register")
				}
				return nil
			},
		},
		{
			name: mqttSharedLifecycleStreamName,
			txt:  "shared-group lifecycle",
			// Shared-group lifecycle records (F-105 transfer): one group record per
			// group, CAS-arbitrated, and one acquisition ledger per session generation,
			// sealed by the closing. Its own stream rather than $MQTT_inflight: a
			// permanent record co-located with the in-flight registers keeps their
			// storage block from collapsing, which is the neighbour condition the
			// upstream MaxMsgsPer revival defect needs (docs/known-upstream-issues.md).
			cfg:       sharedCfg,
			preCreate: stampEpoch(sharedCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateSharedLifecycleStream(si, wantReplicas, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttSharedLifecycleStreamName, "shared-group lifecycle")
				}
				return nil
			},
		},
		{
			name: mqttOutStreamName,
			txt:  "QoS2 outgoing",
			// $MQTT_out carries both the pending PUBREL records and the
			// session-affine copies of started shared-subscription QoS 2 deliveries.
			// Its InterestPolicy retention is load-bearing in both directions: it is
			// how a discarded session's undelivered copies disappear with its
			// consumer, and it is why a copy must never be stored before that
			// consumer exists (a store with no interest returns success and keeps
			// nothing).
			cfg:       outCfg,
			preCreate: stampEpoch(outCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateOutStream(si, wantReplicas, maxAckPending, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttOutStreamName, "interest-retained delivery")
				}
				return nil
			},
		},
		{
			name:      mqttRetainedMsgsStreamName,
			txt:       "retained messages",
			cfg:       retainedCfg,
			preCreate: stampEpoch(retainedCfg),
			validate: func(si *StreamInfo) error {
				if verr := validateRetainedStream(si, wantReplicas, requireMsgTTL, as.sessStreamEpoch); verr != nil {
					return stopAll(verr, mqttRetainedMsgsStreamName, "per-topic retained")
				}
				return nil
			},
		},
	}
}

// mqttProvisionEphemeralConsumers creates this broker's two per-run ephemeral
// consumers — the retained-messages observer and the session-record watch — AFTER
// every stream in the provisioning table has been created and validated.
//
// BOTH live here, in that order. The observer used to be the $MQTT_rmsgs entry's
// post step, which was already the last table entry, so moving it changes no
// ordering; what it buys is that "no consumer is created until every stream has
// validated" is checkable at one call site instead of by auditing nine table
// entries, and that the provisioning table itself performs no durable mutation
// beyond the stream creates ensureBrokerStream makes (F-066).
//
// What bounds the leak is no longer atomicity but the DERIVED name: both consumers
// are named from this server run's identity, so an initialization retry asks for the
// same consumer instead of minting another one, and the previous attempt's is either
// removed definitely (the observer) or adopted (this one). Retries are real —
// startMQTT opens the listener before Start initializes the manager, so a CONNECT
// arriving in that window drives initialization itself, and a failed attempt is
// retried by the next CONNECT or by Start's own call.
//
// The watch CREATES without deleting. It is DeliverNew, so it has no historical
// position to lose by adopting its own consumer, and ActionCreate is what keeps the
// adoption honest: a consumer under this name that is not config-identical to ours —
// a different run's, or one whose config was mutated out of band — is REFUSED rather
// than taken over. See mqttSessRecordWatchConsumerConfig.
//
// created records the purposes this attempt CONFIRMABLY created; a rollback deletes
// nothing else. See mqttEphemeralCreated for why an errored create licenses nothing.
//
// See sess_record_watch.go for what the watch is and why the broker needs one.
func (as *mqttAccountSessionManager) mqttProvisionEphemeralConsumers(accName, srwsubj, rmsubj, ephemeralID, ephemeralOwner string, created mqttEphemeralCreated) error {
	// The retained observer first, the watch second — the order the $MQTT_rmsgs
	// post step and this function had between them before the move.
	//
	// Deleted before it is created, and the delete must come back DEFINITE. This is
	// the one place the two ephemeral consumers differ, and the reason is the
	// retained INDEX: as.retmsgs is written only by this consumer's callback and by
	// the publishing broker's own local seed, so a DeliverAll consumer ADOPTED at
	// its current position leaves the index empty — and a wildcard SUBSCRIBE has
	// nothing but the index (a literal filter is forced through
	// loadRetainedMessages, a wildcard cannot be). A previous attempt of THIS run
	// may have drained the whole backlog into a manager that was then discarded, so
	// adoption here silently serves no retained message for the life of the
	// process. Freshness therefore comes from the protocol order, not from
	// inspecting the create response: a response cannot distinguish a fresh
	// consumer that has already delivered from an adopted one.
	rmname := mqttRetainedObserverName(ephemeralID)
	if derr := as.jsa.deleteEphemeralConsumerDefinite(mqttRetainedMsgsStreamName, rmname); derr != nil {
		as.rollbackEphemeralConsumers(created)
		return fmt.Errorf("retained messages consumer for account %q: %w", accName, derr)
	}
	rmcfg := &CreateConsumerRequest{
		Stream: mqttRetainedMsgsStreamName,
		Config: ConsumerConfig{
			Name:              rmname,
			FilterSubject:     mqttRetainedMsgsStreamSubject + ">",
			DeliverSubject:    rmsubj,
			ReplayPolicy:      ReplayInstant,
			AckPolicy:         AckNone,
			InactiveThreshold: 5 * time.Minute,
			Metadata:          mqttEphemeralConsumerMetadata(ephemeralOwner, mqttEphemeralRetainedObserver),
		},
	}
	// The confirmation is recorded BEFORE the error is handled: a create that
	// confirmably landed is the rollback's only licence, and an errored create of
	// any shape licenses nothing. See mqttEphemeralCreated.
	rmconfirmed, rmerr := as.jsa.createEphemeralConsumerOwned(rmcfg)
	if rmconfirmed {
		created.confirm(mqttEphemeralRetainedObserver)
	}
	if rmerr != nil {
		as.rollbackEphemeralConsumers(created)
		return fmt.Errorf("create retained messages consumer for account %q: %v", accName, rmerr)
	}

	ccfg := mqttSessRecordWatchConsumerConfig(as.domainTk, srwsubj, ephemeralID, ephemeralOwner)
	err := error(nil)
	// The seam models a FAILED CREATE, so it has to reach the same rollback the real
	// failure does. Returning early here instead is how the first version of this
	// function passed its own leak test while leaking.
	if h := mqttTestWatchConsumerCreateFailV.Load(); h != nil && *h {
		err = fmt.Errorf("mqtt test: session-record watch consumer creation forced to fail")
	} else {
		confirmed, cerr := as.jsa.createEphemeralConsumerOwned(ccfg)
		if confirmed {
			created.confirm(mqttEphemeralSessRecordWatch)
		}
		err = cerr
	}
	if err != nil {
		// Only what this attempt CONFIRMABLY created. An error here does not prove the
		// consumer was not created — a lost response leaves one behind under exactly
		// the name we asked for — but neither does it prove the consumer at that name
		// is OURS: both server paths return structured errors before they look the
		// name up, so a foreign occupant plus a validation error would be deleted by a
		// rollback that trusted the error. The unconfirmed create is left to the next
		// attempt of this run (which adopts it, being config-identical) or to
		// InactiveThreshold.
		as.rollbackEphemeralConsumers(created)
		return fmt.Errorf("create session-record watch consumer for account %q: %v", accName, err)
	}
	return nil
}

// rollbackEphemeralConsumers removes the per-broker ephemeral consumers this FAILED
// initialization attempt is KNOWN to have created — known being the operative word.
// Its licence is a confirmed create and nothing else, because no error can tell "our
// create landed" from "a foreign consumer now occupies that name": both the
// standalone and clustered paths return structured errors before they look the
// existing consumer up, so deleting on the strength of an error can remove a live
// peer's consumer.
//
// It is BEST-EFFORT and is NOT part of the cardinality guarantee: its own delete can
// be lost and nothing retries it. What it buys is promptness for the attempt no retry
// follows — Start's own, after which the process exits — so a cleanly failed start
// leaves nothing rather than a five-minute stray. "No retry follows" is the common
// case, not a guarantee: the listener opens before the eager initialization, so a
// CONNECT can be queued on the manager mutex while that attempt fails.
// getOrCreateMQTTAccountSessionManager refuses to create a manager once the server is
// marked not-running, and the instant that remains is a stated residual (see
// docs/CONFORMANCE.md), not a second mechanism.
//
// The guarantee lives elsewhere: derived names mean a retry reconciles the previous
// attempt's consumer (the observer deletes it definitely before creating, the watch
// adopts it), so cardinality stays at one per purpose whatever the attempt count. A
// create whose response was LOST is therefore deliberately not rolled back here; it
// waits for that reconciliation, or for InactiveThreshold. See ephemeral_identity.go.
func (as *mqttAccountSessionManager) rollbackEphemeralConsumers(created mqttEphemeralCreated) {
	ephemeralID, _ := as.jsa.c.srv.ephemeralIdentity()
	for _, c := range []struct {
		stream, name string
		purpose      mqttEphemeralPurpose
	}{
		{mqttRetainedMsgsStreamName, mqttRetainedObserverName(ephemeralID), mqttEphemeralRetainedObserver},
		{mqttSessStreamName, mqttSessRecordWatchName(ephemeralID), mqttEphemeralSessRecordWatch},
	} {
		if !created.has(c.purpose) {
			continue
		}
		// Not-found IS successful cleanup: the create never landed.
		if derr := as.jsa.deleteEphemeralConsumerBestEffort(c.stream, c.name); isErrorOtherThan(derr, JSConsumerNotFoundErr) {
			as.jsa.c.Warnf("MQTT: unable to roll back %s consumer %q after a failed initialization: %v",
				c.stream, c.name, derr)
		}
	}
}
