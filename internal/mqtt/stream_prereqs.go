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
// (that owner issues no delete API request at all; the empty-body tombstone
// plus MaxMsgsPer:1 replacement carry correctness, and both that store-time
// eviction and TTL reaping are store-internal, which DenyDelete does not gate). A setting absent from a list is
// an audited NON-prerequisite with a written reason, not an oversight:
// rejecting a setting no owner operation depends on would refuse legitimate
// deployments — policy hardening, not derivation. The audit is enforced by
// TestStreamValidatorFieldCoverage, which requires every StreamConfig field to
// carry exactly one disposition — rejected, waived, or dominated — for every
// validator, with a non-empty reason for every waiver.

package mqtt

import (
	"fmt"
	"time"

	"github.com/nats-io/nuid"
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
func validateBrokerOwnedStreamShape(cfg *StreamConfig, wantSubject string, wantReplicas int) error {
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != wantSubject {
		return fmt.Errorf("subjects %v (want [%q])", cfg.Subjects, wantSubject)
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

// brokerStreamAPI is the minimal seam ensureBrokerStream needs, satisfied by
// *mqttJSA and by test stubs.
type brokerStreamAPI interface {
	createStream(cfg *StreamConfig) (*StreamInfo, bool, error)
	lookupStream(name string) (*StreamInfo, error)
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
// the UNSUBSCRIBE drain and the shared/affine consumers — refuse it at runtime
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
func validateMsgsStream(si *StreamInfo, wantReplicas, maxAckPending int, requireMsgTTL bool) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, mqttStreamSubjectPrefix+">", wantReplicas); err != nil {
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
	return nil
}

// validateRetainedStream verifies $MQTT_rmsgs can carry retained messages.
// Derived from what this owner does: one plain store per topic subject with a
// per-message Nats-TTL, including empty-body tombstones; loadLastMsgForMulti /
// loadNextMsgFor reads; NO explicit message-delete or purge API calls at all;
// and one ephemeral AckNone consumer per broker with a 5-minute
// InactiveThreshold.
//
// DenyDelete is deliberately NOT a prerequisite: this owner issues no delete
// API request. The durable deletion authority is the empty-body tombstone plus
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
func validateRetainedStream(si *StreamInfo, wantReplicas int, requireMsgTTL bool) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, mqttRetainedMsgsStreamSubject+">", wantReplicas); err != nil {
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
	return nil
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
// validateMsgsStream); rmsubj is the delivery subject of this broker's
// retained-messages consumer, created as the $MQTT_rmsgs post step; retainedName, when
// non-nil, receives that consumer's generated name so the caller can roll it back if a
// later provisioning step fails.
func (as *mqttAccountSessionManager) brokerStreamSpecs(s *Server, accName string,
	replicas, wantReplicas, maxAckPending int, requireMsgTTL bool, rmsubj string,
	retainedName *string) []brokerStreamSpec {

	jsa := &as.jsa
	// The provisioning loop prefixes every error with the stream and account, so
	// the guidance here carries only the verdict and the recovery.
	stopAll := func(verr error, stream, req string) error {
		return fmt.Errorf("not usable: %w; stop ALL brokers for this account and recreate %s with the required %s configuration (this is not a rolling upgrade — see docs/ARCHITECTURE.md \"Compatibility boundary\")",
			verr, stream, req)
	}

	return []brokerStreamSpec{
		{
			name: mqttSessStreamName,
			txt:  "sessions",
			// A generated incarnation epoch is stamped into the stream metadata at
			// creation (the linearization point): whichever replica wins the create
			// sets the authoritative epoch, and everyone — winner included — adopts
			// it from ensureBrokerStream's authoritative re-read in the post step,
			// after validateSessStream has accepted it on that same snapshot.
			cfg: &StreamConfig{
				Name:       mqttSessStreamName,
				Subjects:   []string{mqttSessStreamSubjectPrefix + as.domainTk + ">"},
				Storage:    FileStorage,
				Retention:  LimitsPolicy,
				Replicas:   replicas,
				MaxMsgsPer: 1,
				Metadata:   map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
			},
			validate: func(si *StreamInfo) error {
				if verr := validateSessStream(si, as.domainTk, wantReplicas); verr != nil {
					// The session stream is epoch-coupled to the will-cancel marker
					// stream: recreating $MQTT_sess alone mints a new incarnation
					// epoch while the permanent watermarks keep the old one, the
					// partial-recreation hazard docs/ARCHITECTURE.md ("Operational
					// constraints for the marker") warns against. The recovery is
					// the coupled one, mirroring the marker stream's own guidance.
					return fmt.Errorf("not usable: %w; stop ALL brokers for this account and recreate %s together with %s as one logical unit — recreating the session stream alone mints a new incarnation epoch against stale takeover-suppression watermarks (see docs/ARCHITECTURE.md; this is not a rolling upgrade)",
						verr, mqttSessStreamName, mqttWillCancelStreamName)
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
				// Adopt the authoritative session-stream incarnation epoch, already
				// validated by validateSessStream on this exact snapshot. Every
				// accepted CONNECT and every native Will record is stamped with this
				// identity. See will_cancel.go.
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
			cfg: &StreamConfig{
				Name:       mqttWillQStreamName,
				Subjects:   []string{mqttWillQSubjectPrefix + as.domainTk + ">"},
				Storage:    FileStorage,
				Retention:  LimitsPolicy,
				Replicas:   replicas,
				MaxMsgsPer: 1,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateWillQStream(si, as.domainTk, wantReplicas); verr != nil {
					return stopAll(verr, mqttWillQStreamName, "delayed-Will outbox")
				}
				return nil
			},
		},
		{
			name: mqttWillCancelStreamName,
			txt:  "will-cancel",
			// Durable per-client delayed-Will cancellation watermark stream.
			// Permanent per-client retention (MaxMsgsPer:1, no global limits so a
			// marker is never evicted; DiscardOld so a CAS replacement of a client's
			// prior watermark is never rejected). See will_cancel.go.
			cfg: &StreamConfig{
				Name:       mqttWillCancelStreamName,
				Subjects:   []string{mqttWillCancelSubjectPrefix + as.domainTk + ">"},
				Storage:    FileStorage,
				Retention:  LimitsPolicy,
				Discard:    DiscardOld,
				Replicas:   replicas,
				MaxMsgsPer: 1,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateWillCancelStream(si, as.domainTk, wantReplicas); verr != nil {
					// The marker stream must never be recreated alone: its permanent
					// watermarks are the takeover-suppression evidence, and sequences
					// are only comparable within one $MQTT_sess incarnation — so the
					// recovery is the coupled one docs/ARCHITECTURE.md ("Operational
					// constraints for the marker") requires.
					return fmt.Errorf("not usable: %w; stop ALL brokers for this account and recreate %s together with %s as one logical unit — recreating the marker stream alone discards permanent takeover-suppression watermarks and can let superseded delayed Wills fire (see docs/ARCHITECTURE.md)",
						verr, mqttWillCancelStreamName, mqttSessStreamName)
				}
				return nil
			},
		},
		{
			name: mqttStreamName,
			txt:  "messages",
			cfg: &StreamConfig{
				Name:      mqttStreamName,
				Subjects:  []string{mqttStreamSubjectPrefix + ">"},
				Storage:   FileStorage,
				Retention: InterestPolicy,
				Replicas:  replicas,
				// Honor MQTT 5.0 Message Expiry Interval: a stored PUBLISH carries a
				// per-message Nats-TTL so JetStream drops it once its lifetime has
				// elapsed, even if never delivered. Spec5 [3.3.2.3.3].
				AllowMsgTTL: true,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateMsgsStream(si, wantReplicas, maxAckPending, requireMsgTTL); verr != nil {
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
			cfg: &StreamConfig{
				Name:       mqttQoS2IncomingMsgsStreamName,
				Subjects:   []string{mqttQoS2IncomingMsgsStreamSubjectPrefix + ">"},
				Storage:    FileStorage,
				Retention:  LimitsPolicy,
				Discard:    DiscardOld,
				MaxMsgsPer: 1,
				Replicas:   replicas,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateQoS2InStream(si, wantReplicas); verr != nil {
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
			cfg: &StreamConfig{
				Name:       mqttQoS2AssignStreamName,
				Subjects:   []string{mqttQoS2AssignStreamSubjectPrefix + ">"},
				Storage:    FileStorage,
				Retention:  LimitsPolicy,
				Discard:    DiscardOld,
				MaxMsgsPer: 1,
				Replicas:   replicas,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateQoS2AssignStream(si, wantReplicas); verr != nil {
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
			cfg: &StreamConfig{
				Name:       mqttInflightStreamName,
				Subjects:   []string{mqttInflightStreamSubjectPrefix + ">"},
				Storage:    FileStorage,
				Retention:  LimitsPolicy,
				Discard:    DiscardOld,
				MaxMsgsPer: 1,
				Replicas:   replicas,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateInflightStream(si, wantReplicas); verr != nil {
					return stopAll(verr, mqttInflightStreamName, "in-flight register")
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
			cfg: &StreamConfig{
				Name:      mqttOutStreamName,
				Subjects:  []string{mqttOutSubjectPrefix + ">"},
				Storage:   FileStorage,
				Retention: InterestPolicy,
				Replicas:  replicas,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateOutStream(si, wantReplicas, maxAckPending); verr != nil {
					return stopAll(verr, mqttOutStreamName, "interest-retained delivery")
				}
				return nil
			},
		},
		{
			name: mqttRetainedMsgsStreamName,
			txt:  "retained messages",
			cfg: &StreamConfig{
				Name:       mqttRetainedMsgsStreamName,
				Subjects:   []string{mqttRetainedMsgsStreamSubject + ">"},
				Storage:    FileStorage,
				Retention:  LimitsPolicy,
				Replicas:   replicas,
				MaxMsgsPer: 1,
				// Honor MQTT 5.0 Message Expiry Interval for retained messages via a
				// per-message Nats-TTL. Spec5 [3.3.2.3.3].
				AllowMsgTTL: true,
			},
			validate: func(si *StreamInfo) error {
				if verr := validateRetainedStream(si, wantReplicas, requireMsgTTL); verr != nil {
					return stopAll(verr, mqttRetainedMsgsStreamName, "per-topic retained")
				}
				return nil
			},
			post: func(_ *StreamInfo) error {
				// Create a new, uniquely named consumer for retained messages for
				// this server. The prior one will expire eventually.
				ccfg := &CreateConsumerRequest{
					Stream: mqttRetainedMsgsStreamName,
					Config: ConsumerConfig{
						Name:              mqttRetainedMsgsStreamName + "_" + nuid.Next(),
						FilterSubject:     mqttRetainedMsgsStreamSubject + ">",
						DeliverSubject:    rmsubj,
						ReplayPolicy:      ReplayInstant,
						AckPolicy:         AckNone,
						InactiveThreshold: 5 * time.Minute,
					},
				}
				// The name is recorded BEFORE the request, not after it succeeds: a
				// create whose response is lost may still have created the consumer,
				// and a rollback that only knows about confirmed creates would leave
				// exactly those behind. See mqttProvisionEphemeralConsumers.
				if retainedName != nil {
					*retainedName = ccfg.Config.Name
				}
				if _, err := jsa.createEphemeralConsumer(ccfg); err != nil {
					return fmt.Errorf("create retained messages consumer for account %q: %v", accName, err)
				}
				return nil
			},
		},
	}
}

// mqttProvisionEphemeralConsumers creates this broker's session-record watch AFTER
// every stream in the provisioning table has been created and validated, and makes
// the pair of per-broker ephemeral consumers FAILURE-ATOMIC.
//
// Both properties answer the same hazard, which is a leak measured per RETRY rather
// than per broker. Each consumer is uniquely named, so nothing later adopts or
// replaces one; manager initialization tears down only local subscriptions; and every
// MQTT connection retries the whole initialization. So a consumer created before a
// step that fails is never deleted by anything, and a persistent misconfiguration
// accumulates one per attempt until each reaches its inactivity threshold.
//
// retainedName is the retained-messages consumer already created by its stream's post
// step, or empty when it was not reached. On a watch-create failure it is deleted, so
// a failed attempt leaves the account exactly as it found it.
//
// See sess_record_watch.go for what the watch is and why the broker needs one.
func (as *mqttAccountSessionManager) mqttProvisionEphemeralConsumers(accName, srwsubj, retainedName string) error {
	ccfg := mqttSessRecordWatchConsumerConfig(as.domainTk, srwsubj)
	err := error(nil)
	// The seam models a FAILED CREATE, so it has to reach the same rollback the real
	// failure does. Returning early here instead is how the first version of this
	// function passed its own leak test while leaking.
	if h := mqttTestWatchConsumerCreateFailV.Load(); h != nil && *h {
		err = fmt.Errorf("mqtt test: session-record watch consumer creation forced to fail")
	} else {
		_, err = as.jsa.createEphemeralConsumer(ccfg)
		if h := mqttTestConsumerCreateResultHookV.Load(); h != nil {
			err = (*h)(mqttSessStreamName, ccfg.Config.Name, err)
		}
	}
	if err != nil {
		// BOTH names, including the one whose create just failed: an error here does
		// not prove the consumer was not created — a request whose response is lost
		// leaves one behind under exactly the name we asked for — so a rollback that
		// only removed the previous consumer would leak the failing one.
		as.rollbackEphemeralConsumers(retainedName, ccfg.Config.Name)
		return fmt.Errorf("create session-record watch consumer for account %q: %v", accName, err)
	}
	return nil
}

// rollbackEphemeralConsumers removes the per-broker ephemeral consumers a FAILED
// initialization attempt may have created — "may have" being the operative part: each
// name is recorded before its request is issued, so a create whose response was lost
// is covered by the same delete as a create that plainly succeeded, and a name that
// was never created deletes nothing.
//
// What this bounds is the per-RETRY accumulation, which is the real hazard: every MQTT
// connection retries initialization, and each attempt otherwise mints another uniquely
// named consumer that nothing deletes. It does NOT make cleanup guaranteed — a delete
// can itself fail or be lost, and the stray consumer is then reaped by its inactivity
// threshold as before. Removing that last window needs durable cleanup tracking or
// stable per-broker consumer identities; docs/ROADMAP.md carries it.
func (as *mqttAccountSessionManager) rollbackEphemeralConsumers(retainedName, watchName string) {
	for _, c := range []struct{ stream, name string }{
		{mqttRetainedMsgsStreamName, retainedName},
		{mqttSessStreamName, watchName},
	} {
		if c.name == _EMPTY_ {
			continue
		}
		// Not-found IS successful cleanup: the create never landed.
		if _, derr := as.jsa.deleteConsumer(c.stream, c.name, false); isErrorOtherThan(derr, JSConsumerNotFoundErr) {
			as.jsa.c.Warnf("MQTT: unable to roll back %s consumer %q after a failed initialization: %v",
				c.stream, c.name, derr)
		}
	}
}
