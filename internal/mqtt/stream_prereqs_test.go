// Tests for the broker-owned stream prerequisites: the per-validator
// rejection/acceptance tables (the human-readable subset pinning the
// deliberate asymmetries), the ensureBrokerStream convergence contract, the
// provisioning spec table, and the field-coverage audit that gives every
// (validator, StreamConfig field) pair exactly one disposition.

package mqtt

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Per-validator tables: the human-readable subset (plan items 1-2)
// ---------------------------------------------------------------------------

// Every field validateSessStream rejects, rejected independently — and positive
// rows for the exact configuration the create path produces and for the audited
// non-prerequisites.
func TestValidateSessStream(t *testing.T) {
	t.Parallel()
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:       mqttSessStreamName,
			Subjects:   []string{mqttSessStreamSubjectPrefix + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
			Replicas:   3,
			Metadata:   map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
		}}
	}
	if err := validateSessStream(good(), "", 3); err != nil {
		t.Fatalf("the create path's own configuration was rejected: %v", err)
	}
	if err := validateSessStream(good(), "", 0); err != nil {
		t.Fatalf("unexpected rejection with no replica expectation: %v", err)
	}
	if err := validateSessStream(nil, "", 0); err == nil {
		t.Fatal("expected nil stream info to be rejected")
	}

	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"wrong subject", func(c *StreamConfig) { c.Subjects = []string{"other.>"} }},
		{"extra subject", func(c *StreamConfig) { c.Subjects = append(c.Subjects, "other.>") }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"interest retention", func(c *StreamConfig) { c.Retention = InterestPolicy }},
		{"discard new", func(c *StreamConfig) { c.Discard = DiscardNew }},
		{"max msgs per 0", func(c *StreamConfig) { c.MaxMsgsPer = 0 }},
		{"max msgs per 2", func(c *StreamConfig) { c.MaxMsgsPer = 2 }},
		{"global max msgs", func(c *StreamConfig) { c.MaxMsgs = 100 }},
		{"global max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"global max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} }},
		{"sources", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} }},
		{"wrong replicas", func(c *StreamConfig) { c.Replicas = 1 }},
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		{"deny delete", func(c *StreamConfig) { c.DenyDelete = true }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) { c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"} }},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"allow msg counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
		{"allow rollup", func(c *StreamConfig) { c.AllowRollup = true }},
		{"async persist mode", func(c *StreamConfig) { c.PersistMode = AsyncPersistMode }},
		// The composed epoch check: a session stream without a valid incarnation
		// epoch must be rejected on the same snapshot the shape is (mutation 13).
		{"missing epoch metadata", func(c *StreamConfig) { c.Metadata = nil }},
		{"malformed epoch metadata", func(c *StreamConfig) { c.Metadata = map[string]string{mqttSessEpochMetaKey: "not-a-nuid"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateSessStream(si, "", 3); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}

	// A finite consumer cap IS a prerequisite now, and this row moved out of the
	// accepted list to say so: every broker creates its own ephemeral session-record
	// watch consumer on this stream (sess_record_watch.go), so a cap eventually
	// rejects a legitimate broker start. A configured 0 is NOT hostile — nats-server
	// normalizes it to unlimited — so only a positive value is rejected.
	t.Run("a finite max consumers is rejected", func(t *testing.T) {
		si := good()
		si.Config.MaxConsumers = 5
		err := validateSessStream(si, "", 3)
		if err == nil {
			t.Fatal("a finite consumer cap must be rejected: the watch consumer is per broker")
		}
		if !strings.Contains(err.Error(), "max_consumers") {
			t.Fatalf("the diagnostic must name the setting, got: %v", err)
		}
	})
	t.Run("max consumers 0 means unlimited and is accepted", func(t *testing.T) {
		si := good()
		si.Config.MaxConsumers = 0
		if err := validateSessStream(si, "", 3); err != nil {
			t.Fatalf("0 normalizes to unlimited and must be accepted: %v", err)
		}
	})

	// Audited non-prerequisites must be ACCEPTED: nothing purges the session
	// stream, the consumer limits are enforced at the watch consumer's own create,
	// and unrelated metadata keys beside a valid epoch belong to whoever wrote them.
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"deny purge is not a prerequisite", func(c *StreamConfig) { c.DenyPurge = true }},
		{"consumer inactivity limit is not a prerequisite", func(c *StreamConfig) { c.ConsumerLimits.InactiveThreshold = time.Minute }},
		{"unrelated metadata keys are accepted", func(c *StreamConfig) { c.Metadata["unrelated_key"] = "keepme" }},
		{"a duplicate window is not a prerequisite", func(c *StreamConfig) { c.Duplicates = 100 * time.Millisecond }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateSessStream(si, "", 3); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}
}

// Every field validateWillQStream rejects, rejected independently — the
// DenyDelete row's rationale is the corrected one (unbounded accumulation and
// scan churn; the durable publishing->done transition precedes the delete, so
// a Will is never re-fired by a denied delete).
func TestValidateWillQStream(t *testing.T) {
	t.Parallel()
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:       mqttWillQStreamName,
			Subjects:   []string{mqttWillQSubjectPrefix + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
			Replicas:   3,
		}}
	}
	if err := validateWillQStream(good(), "", 3); err != nil {
		t.Fatalf("the create path's own configuration was rejected: %v", err)
	}
	if err := validateWillQStream(nil, "", 0); err == nil {
		t.Fatal("expected nil stream info to be rejected")
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"wrong subject", func(c *StreamConfig) { c.Subjects = []string{"other.>"} }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"interest retention", func(c *StreamConfig) { c.Retention = InterestPolicy }},
		{"discard new", func(c *StreamConfig) { c.Discard = DiscardNew }},
		{"max msgs per 0", func(c *StreamConfig) { c.MaxMsgsPer = 0 }},
		{"max msgs per 2", func(c *StreamConfig) { c.MaxMsgsPer = 2 }},
		{"global max msgs", func(c *StreamConfig) { c.MaxMsgs = 100 }},
		{"global max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"global max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} }},
		{"sources", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} }},
		{"wrong replicas", func(c *StreamConfig) { c.Replicas = 1 }},
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		{"deny delete", func(c *StreamConfig) { c.DenyDelete = true }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) { c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"} }},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"allow msg counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
		{"allow rollup", func(c *StreamConfig) { c.AllowRollup = true }},
		{"async persist mode", func(c *StreamConfig) { c.PersistMode = AsyncPersistMode }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateWillQStream(si, "", 3); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"deny purge is not a prerequisite", func(c *StreamConfig) { c.DenyPurge = true }},
		{"max consumers is not a prerequisite", func(c *StreamConfig) { c.MaxConsumers = 5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateWillQStream(si, "", 3); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}
}

// validateMsgsStream's list differs from every register stream's, because this
// owner stores deliverable messages under interest retention and creates
// consumers. The acceptance rows pin the deliberate asymmetries — most
// importantly ConsumerLimits.InactiveThreshold, which is fatal on $MQTT_out
// but a runtime refusal here (mutation 5's discriminator: the two runtime
// tests impose the limit post-startup and cannot catch an over-strict
// validator; this row is the only thing that can).
func TestValidateMsgsStream(t *testing.T) {
	t.Parallel()
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:        mqttStreamName,
			Subjects:    []string{mqttStreamSubjectPrefix + ">"},
			Storage:     FileStorage,
			Retention:   InterestPolicy,
			Replicas:    3,
			AllowMsgTTL: true,
		}}
	}
	if err := validateMsgsStream(good(), 3, mqttDefaultMaxAckPending, true); err != nil {
		t.Fatalf("the create path's own configuration was rejected: %v", err)
	}
	if err := validateMsgsStream(nil, 0, mqttDefaultMaxAckPending, true); err == nil {
		t.Fatal("expected nil stream info to be rejected")
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"wrong subject", func(c *StreamConfig) { c.Subjects = []string{"other.>"} }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"limits retention", func(c *StreamConfig) { c.Retention = LimitsPolicy }},
		{"max msgs per subject", func(c *StreamConfig) { c.MaxMsgsPer = 1 }},
		{"global max msgs", func(c *StreamConfig) { c.MaxMsgs = 100 }},
		{"global max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"global max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} }},
		{"sources", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} }},
		{"wrong replicas", func(c *StreamConfig) { c.Replicas = 1 }},
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		{"no per-message TTL", func(c *StreamConfig) { c.AllowMsgTTL = false }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) { c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"} }},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"allow msg counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
		{"allow rollup", func(c *StreamConfig) { c.AllowRollup = true }},
		{"async persist mode", func(c *StreamConfig) { c.PersistMode = AsyncPersistMode }},
		{"finite max consumers", func(c *StreamConfig) { c.MaxConsumers = 5 }},
		{"consumer max_ack_pending below the cap", func(c *StreamConfig) { c.ConsumerLimits.MaxAckPending = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateMsgsStream(si, 3, mqttDefaultMaxAckPending, true); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		// No message delete or purge exists on this stream — verified against
		// every deleteMsg/purgeStream call site (mutation 5's sibling, mutation
		// 10's counterpart on $MQTT_rmsgs).
		{"deny delete is not a prerequisite", func(c *StreamConfig) { c.DenyDelete = true }},
		{"deny purge is not a prerequisite", func(c *StreamConfig) { c.DenyPurge = true }},
		// THE asymmetry: fatal on $MQTT_out, runtime-refused here (drain +
		// shared-subscription guards; docs/CONFORMANCE.md).
		{"consumer inactivity limit is not a prerequisite", func(c *StreamConfig) { c.ConsumerLimits.InactiveThreshold = time.Minute }},
		// The threshold form's boundary: a limit AT the effective cap changes
		// nothing and must start.
		{"consumer max_ack_pending at the cap is not a prerequisite", func(c *StreamConfig) {
			c.ConsumerLimits.MaxAckPending = mqttDefaultMaxAckPending
		}},
		{"a duplicate window is not a prerequisite", func(c *StreamConfig) { c.Duplicates = 100 * time.Millisecond }},
		{"discard new is inert with unlimited limits", func(c *StreamConfig) { c.Discard = DiscardNew }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateMsgsStream(si, 3, mqttDefaultMaxAckPending, true); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}

	// A max_protocol_version:4 broker never emits Nats-TTL (Message Expiry is
	// an MQTT 5.0 property), so a TTL-less pre-existing stream is a legitimate
	// 3.1.1-only deployment and must start; an enabled capability stays inert
	// and equally accepted.
	t.Run("3.1.1-only broker accepts a TTL-less stream", func(t *testing.T) {
		si := good()
		si.Config.AllowMsgTTL = false
		if err := validateMsgsStream(si, 3, mqttDefaultMaxAckPending, false); err != nil {
			t.Fatalf("requireMsgTTL=false must accept AllowMsgTTL=false: %v", err)
		}
		if err := validateMsgsStream(good(), 3, mqttDefaultMaxAckPending, false); err != nil {
			t.Fatalf("requireMsgTTL=false must accept an enabled (inert) capability: %v", err)
		}
	})
}

// validateRetainedStream: the acceptance rows pin the two findings that make
// this list unlike any other — DenyDelete is waived because this owner issues no
// delete API request at all (the empty-body tombstone plus MaxMsgsPer:1
// replacement carry correctness, and both the store-time eviction and TTL
// reaping are store-internal, ungated by DenyDelete; mutation 10) and
// Sources is a supported cross-account topology (mutation 11). The Sources row
// cannot be left to TestMQTTCrossAccountRetain: that test adds the source
// AFTER the manager initialized, so an over-strict validator would pass it and
// only refuse the deployment's next restart.
func TestValidateRetainedStream(t *testing.T) {
	t.Parallel()
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:        mqttRetainedMsgsStreamName,
			Subjects:    []string{mqttRetainedMsgsStreamSubject + ">"},
			Storage:     FileStorage,
			Retention:   LimitsPolicy,
			Discard:     DiscardOld,
			MaxMsgsPer:  1,
			Replicas:    3,
			AllowMsgTTL: true,
		}}
	}
	if err := validateRetainedStream(good(), 3, true); err != nil {
		t.Fatalf("the create path's own configuration was rejected: %v", err)
	}
	if err := validateRetainedStream(nil, 0, true); err == nil {
		t.Fatal("expected nil stream info to be rejected")
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"wrong subject", func(c *StreamConfig) { c.Subjects = []string{"other.>"} }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"interest retention", func(c *StreamConfig) { c.Retention = InterestPolicy }},
		{"discard new", func(c *StreamConfig) { c.Discard = DiscardNew }},
		{"max msgs per 0", func(c *StreamConfig) { c.MaxMsgsPer = 0 }},
		{"max msgs per 2", func(c *StreamConfig) { c.MaxMsgsPer = 2 }},
		{"global max msgs", func(c *StreamConfig) { c.MaxMsgs = 100 }},
		{"global max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"global max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} }},
		{"wrong replicas", func(c *StreamConfig) { c.Replicas = 1 }},
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		{"no per-message TTL", func(c *StreamConfig) { c.AllowMsgTTL = false }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) { c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"} }},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"allow msg counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
		{"allow rollup", func(c *StreamConfig) { c.AllowRollup = true }},
		{"async persist mode", func(c *StreamConfig) { c.PersistMode = AsyncPersistMode }},
		{"finite max consumers", func(c *StreamConfig) { c.MaxConsumers = 5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateRetainedStream(si, 3, true); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"deny delete is not a prerequisite (no delete API request is issued)", func(c *StreamConfig) { c.DenyDelete = true }},
		{"deny purge is not a prerequisite", func(c *StreamConfig) { c.DenyPurge = true }},
		{"sources are a supported topology", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} }},
		{"consumer inactivity limit is enforced at the consumer create", func(c *StreamConfig) { c.ConsumerLimits.InactiveThreshold = time.Minute }},
		{"consumer max_ack_pending is enforced at the consumer create", func(c *StreamConfig) { c.ConsumerLimits.MaxAckPending = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateRetainedStream(si, 3, true); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}

	// See the matching subtest in TestValidateMsgsStream: retained Message
	// Expiry is an MQTT 5.0 property, so a 3.1.1-only broker must accept a
	// TTL-less stream.
	t.Run("3.1.1-only broker accepts a TTL-less stream", func(t *testing.T) {
		si := good()
		si.Config.AllowMsgTTL = false
		if err := validateRetainedStream(si, 3, false); err != nil {
			t.Fatalf("requireMsgTTL=false must accept AllowMsgTTL=false: %v", err)
		}
		if err := validateRetainedStream(good(), 3, false); err != nil {
			t.Fatalf("requireMsgTTL=false must accept an enabled (inert) capability: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// ensureBrokerStream: the convergence contract (plan item 6, mutation 12)
// ---------------------------------------------------------------------------

// stubStreamAPI scripts lookupStream/createStream responses and records the
// call counts and hook ordering ensureBrokerStream's contract promises.
type stubStreamAPI struct {
	lookupResults []func() (*StreamInfo, error)
	createResult  func() (*StreamInfo, bool, error)
	lookups       int
	creates       int
	trace         []string
}

func (s *stubStreamAPI) lookupStream(_ string) (*StreamInfo, error) {
	s.trace = append(s.trace, "lookup")
	i := s.lookups
	s.lookups++
	if i >= len(s.lookupResults) {
		return nil, fmt.Errorf("unexpected lookup #%d", i+1)
	}
	return s.lookupResults[i]()
}

func (s *stubStreamAPI) createStream(_ *StreamConfig) (*StreamInfo, bool, error) {
	s.trace = append(s.trace, "create")
	s.creates++
	return s.createResult()
}

// The winner path must validate the authoritative re-read, not the create
// response: the stub's create returns conforming config A while the re-read
// returns rejected config B, and only an implementation performing exactly
// two lookups and one create — and handing the validator B — passes. This is
// the only test that can discriminate winner-path validation (a broker-created
// stream is conforming by construction, so an end-to-end test cannot), and it
// is what kills mutation 12. createStream really does return a *StreamInfo
// (mqttJSA.createStream), so validating it is the realistic wrong
// implementation.
func TestEnsureBrokerStream(t *testing.T) {
	t.Parallel()
	notFound := func() (*StreamInfo, error) { return nil, ApiErrors[JSStreamNotFoundErr] }
	infoA := &StreamInfo{Config: StreamConfig{Name: "A"}}
	infoB := &StreamInfo{Config: StreamConfig{Name: "B"}}
	cfg := &StreamConfig{Name: "$MQTT_test"}

	t.Run("winner validates the re-read, not the create response", func(t *testing.T) {
		stub := &stubStreamAPI{
			lookupResults: []func() (*StreamInfo, error){
				notFound,
				func() (*StreamInfo, error) { return infoB, nil },
			},
			createResult: func() (*StreamInfo, bool, error) { return infoA, true, nil },
		}
		var sawGate, sawObserve bool
		var validated []*StreamInfo
		_, err := ensureBrokerStream(stub, cfg,
			func(si *StreamInfo) error {
				validated = append(validated, si)
				if si.Config.Name == "B" {
					return fmt.Errorf("config B rejected")
				}
				return nil
			},
			func() {
				sawGate = true
				if stub.lookups != 1 || stub.creates != 0 {
					t.Errorf("preCreate must run after the absent lookup and before the create (lookups=%d creates=%d)", stub.lookups, stub.creates)
				}
			},
			func(created bool, cerr error) {
				sawObserve = true
				if !created || cerr != nil {
					t.Errorf("observe must receive the raw create outcome, got (%v, %v)", created, cerr)
				}
			})
		if err == nil {
			t.Fatal("expected the rejected re-read to fail the call")
		}
		if len(validated) != 1 || validated[0] != infoB {
			t.Fatalf("the validator must be invoked exactly once, with the re-read config B; got %d invocations", len(validated))
		}
		if stub.lookups != 2 || stub.creates != 1 {
			t.Fatalf("want exactly two lookups and one create, got %d lookups %d creates (%v)", stub.lookups, stub.creates, stub.trace)
		}
		if !sawGate || !sawObserve {
			t.Fatalf("hooks must fire on the create path (preCreate=%v observe=%v)", sawGate, sawObserve)
		}
	})

	t.Run("create-race loser converges on the winner's stream", func(t *testing.T) {
		stub := &stubStreamAPI{
			lookupResults: []func() (*StreamInfo, error){
				notFound,
				func() (*StreamInfo, error) { return infoB, nil },
			},
			createResult: func() (*StreamInfo, bool, error) { return nil, false, ApiErrors[JSStreamNameExistErr] },
		}
		si, err := ensureBrokerStream(stub, cfg,
			func(si *StreamInfo) error { return nil }, nil, nil)
		if err != nil {
			t.Fatalf("a NameExist loss is benign, got %v", err)
		}
		if si != infoB {
			t.Fatal("the loser must return the winner's authoritative info")
		}
		if stub.lookups != 2 || stub.creates != 1 {
			t.Fatalf("want two lookups and one create, got %d/%d", stub.lookups, stub.creates)
		}
	})

	t.Run("pre-existing stream still re-reads and validates", func(t *testing.T) {
		stub := &stubStreamAPI{
			lookupResults: []func() (*StreamInfo, error){
				func() (*StreamInfo, error) { return infoB, nil },
				func() (*StreamInfo, error) { return infoB, nil },
			},
		}
		si, err := ensureBrokerStream(stub, cfg,
			func(si *StreamInfo) error { return nil }, nil, nil)
		if err != nil || si != infoB {
			t.Fatalf("pre-existing conforming stream must validate, got si=%v err=%v", si, err)
		}
		if stub.creates != 0 {
			t.Fatalf("a pre-existing stream must not be re-created (creates=%d)", stub.creates)
		}
	})

	t.Run("create failure other than NameExist is fatal", func(t *testing.T) {
		stub := &stubStreamAPI{
			lookupResults: []func() (*StreamInfo, error){notFound},
			createResult:  func() (*StreamInfo, bool, error) { return nil, false, fmt.Errorf("storage full") },
		}
		if _, err := ensureBrokerStream(stub, cfg,
			func(si *StreamInfo) error { return nil }, nil, nil); err == nil {
			t.Fatal("expected the create failure to be fatal")
		}
	})
}

// ---------------------------------------------------------------------------
// The provisioning spec table (plan item 7)
// ---------------------------------------------------------------------------

// mqttTestBrokerStreamNames is the closed set of broker-owned streams, in
// provisioning order.
var mqttTestBrokerStreamNames = []string{
	mqttSessStreamName,
	mqttWillQStreamName,
	mqttWillCancelStreamName,
	mqttTeardownStreamName,
	mqttStreamName,
	mqttQoS2IncomingMsgsStreamName,
	mqttQoS2AssignStreamName,
	mqttInflightStreamName,
	mqttOutStreamName,
	mqttRetainedMsgsStreamName,
}

// testBrokerStreamSpecs builds the table as provisioning sees it, with every
// entry's preCreate already applied.
//
// Both are needed for the configurations to be the ones the broker actually
// creates. The manager carries an adopted session-stream epoch because
// $MQTT_teardown is coupled to it, and $MQTT_teardown's own preCreate is where
// that epoch is stamped into its config — it cannot be a literal, since the real
// table is built before the loop that adopts the epoch.
func testBrokerStreamSpecs() []brokerStreamSpec {
	as := &mqttAccountSessionManager{}
	as.sessStreamEpoch = mqttGenSessEpoch()
	specs := as.brokerStreamSpecs(&Server{}, "T", 3, 3, mqttDefaultMaxAckPending, true, "rmsub", nil)
	for _, spec := range specs {
		if spec.preCreate != nil {
			spec.preCreate()
		}
	}
	return specs
}

// With provisioning table-driven, bypassing ensureBrokerStream requires
// deleting a stream's entry — which this test refuses: the table must name
// exactly the ten $MQTT_* streams, each with a non-nil validator and a config
// the validator itself accepts.
func TestBrokerStreamSpecTable(t *testing.T) {
	t.Parallel()
	specs := testBrokerStreamSpecs()
	if len(specs) != len(mqttTestBrokerStreamNames) {
		t.Fatalf("the spec table has %d entries, want %d — every broker-owned stream must be provisioned through ensureBrokerStream", len(specs), len(mqttTestBrokerStreamNames))
	}
	for i, spec := range specs {
		if spec.name != mqttTestBrokerStreamNames[i] {
			t.Fatalf("entry %d is %q, want %q", i, spec.name, mqttTestBrokerStreamNames[i])
		}
		if spec.validate == nil {
			t.Fatalf("entry %q has no validator; provisioning without validation is the convergence defect this table exists to prevent", spec.name)
		}
		if spec.cfg == nil || spec.cfg.Name != spec.name {
			t.Fatalf("entry %q has a missing or misnamed config", spec.name)
		}
		if err := spec.validate(&StreamInfo{Config: *spec.cfg}); err != nil {
			t.Fatalf("entry %q rejects its own create configuration: %v", spec.name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Field coverage: every (validator, field) pair has exactly one disposition
// ---------------------------------------------------------------------------

const (
	dispRejected  = "rejected"
	dispWaived    = "waived"
	dispDominated = "dominated"
)

// fieldDisposition is one audited verdict: rejected (errWant must appear in
// the validator's error), waived (the mutation must be ACCEPTED, with a
// non-empty reason), or dominated (the mutated config is undeployable because
// a specifically named rejected prerequisite would have to change first, so
// no acceptance check applies; the reason names that prerequisite).
type fieldDisposition struct {
	kind    string
	errWant string
	reason  string
}

// streamFieldMutators is the bijection side: exactly one mutator per reflected
// StreamConfig field (ConsumerLimits expands to its two nested fields). Each
// mutator must change exactly its named field, to a value differing from every
// baseline.
var streamFieldMutators = map[string]func(*StreamConfig){
	"Name":        func(c *StreamConfig) { c.Name += "_other" },
	"Description": func(c *StreamConfig) { c.Description = "mutated" },
	"Subjects":    func(c *StreamConfig) { c.Subjects = []string{"other.>"} },
	"Retention": func(c *StreamConfig) {
		if c.Retention == LimitsPolicy {
			c.Retention = InterestPolicy
		} else {
			c.Retention = LimitsPolicy
		}
	},
	"MaxConsumers": func(c *StreamConfig) { c.MaxConsumers = 5 },
	"MaxMsgs":      func(c *StreamConfig) { c.MaxMsgs = 100 },
	"MaxBytes":     func(c *StreamConfig) { c.MaxBytes = 1 << 20 },
	"MaxAge":       func(c *StreamConfig) { c.MaxAge = time.Hour },
	"MaxMsgsPer":   func(c *StreamConfig) { c.MaxMsgsPer = 2 },
	"MaxMsgSize":   func(c *StreamConfig) { c.MaxMsgSize = 1024 },
	"Discard":      func(c *StreamConfig) { c.Discard = DiscardNew },
	"Storage":      func(c *StreamConfig) { c.Storage = MemoryStorage },
	"Replicas":     func(c *StreamConfig) { c.Replicas = 1 },
	"NoAck":        func(c *StreamConfig) { c.NoAck = true },
	"Duplicates":   func(c *StreamConfig) { c.Duplicates = 100 * time.Millisecond },
	"Placement":    func(c *StreamConfig) { c.Placement = &Placement{Cluster: "c"} },
	"Mirror":       func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} },
	"Sources":      func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} },
	"Compression":  func(c *StreamConfig) { c.Compression = S2Compression },
	"FirstSeq":     func(c *StreamConfig) { c.FirstSeq = 42 },
	"SubjectTransform": func(c *StreamConfig) {
		c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"}
	},
	"RePublish":                        func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} },
	"AllowDirect":                      func(c *StreamConfig) { c.AllowDirect = true },
	"MirrorDirect":                     func(c *StreamConfig) { c.MirrorDirect = true },
	"DiscardNewPer":                    func(c *StreamConfig) { c.DiscardNewPer = true },
	"Sealed":                           func(c *StreamConfig) { c.Sealed = true },
	"DenyDelete":                       func(c *StreamConfig) { c.DenyDelete = true },
	"DenyPurge":                        func(c *StreamConfig) { c.DenyPurge = true },
	"AllowRollup":                      func(c *StreamConfig) { c.AllowRollup = true },
	"ConsumerLimits.InactiveThreshold": func(c *StreamConfig) { c.ConsumerLimits.InactiveThreshold = time.Minute },
	"ConsumerLimits.MaxAckPending":     func(c *StreamConfig) { c.ConsumerLimits.MaxAckPending = 1 },
	// Flip relative to the baseline: required-true streams get the mandatory
	// false mutation (round-3 Q4.1), waived-false streams get true.
	"AllowMsgTTL":            func(c *StreamConfig) { c.AllowMsgTTL = !c.AllowMsgTTL },
	"SubjectDeleteMarkerTTL": func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute },
	"AllowMsgCounter":        func(c *StreamConfig) { c.AllowMsgCounter = true },
	"AllowAtomicPublish":     func(c *StreamConfig) { c.AllowAtomicPublish = true },
	"AllowMsgSchedules":      func(c *StreamConfig) { c.AllowMsgSchedules = true },
	"PersistMode":            func(c *StreamConfig) { c.PersistMode = AsyncPersistMode },
	"AllowBatchPublish":      func(c *StreamConfig) { c.AllowBatchPublish = true },
	"Metadata":               func(c *StreamConfig) { c.Metadata = map[string]string{mqttSessEpochMetaKey: "not-a-nuid"} },
}

// reflectedStreamConfigFields returns the audited field set: every top-level
// StreamConfig field, with ConsumerLimits expanded into its nested fields. A
// nats-server bump that adds a field (top-level or nested) changes this set
// and fails the bijection check by name.
func reflectedStreamConfigFields(t *testing.T) []string {
	t.Helper()
	var fields []string
	sc := reflect.TypeOf(StreamConfig{})
	for i := 0; i < sc.NumField(); i++ {
		name := sc.Field(i).Name
		if name != "ConsumerLimits" {
			fields = append(fields, name)
			continue
		}
		cl := sc.Field(i).Type
		for j := 0; j < cl.NumField(); j++ {
			fields = append(fields, "ConsumerLimits."+cl.Field(j).Name)
		}
	}
	return fields
}

// dispositionsFor assembles the audited per-stream verdicts: the shared shape
// and universally inert fields once, the per-owner semantics per stream. The
// reasons ARE the audit — non-empty prose is enforced below (mutation 9).
func dispositionsFor(stream string) map[string]fieldDisposition {
	d := map[string]fieldDisposition{
		// The shared shape (validateBrokerOwnedStreamShape): prerequisites of
		// storing and reading a record at a named subject at all.
		"Subjects":         {kind: dispRejected, errWant: "subjects"},
		"Storage":          {kind: dispRejected, errWant: "storage"},
		"Mirror":           {kind: dispRejected, errWant: "mirror"},
		"Replicas":         {kind: dispRejected, errWant: "replicas"},
		"Sealed":           {kind: dispRejected, errWant: "sealed"},
		"SubjectTransform": {kind: dispRejected, errWant: "subject transform"},
		"RePublish":        {kind: dispRejected, errWant: "republish"},
		"AllowMsgCounter":  {kind: dispRejected, errWant: "allow_msg_counter"},
		"NoAck":            {kind: dispRejected, errWant: "no_ack"},
		"PersistMode":      {kind: dispRejected, errWant: "persist_mode"},

		// Universally inert for every owner here.
		"Name":               {kind: dispWaived, reason: "guaranteed by the named lookup that produced this config; the validator never sees another stream's info"},
		"Description":        {kind: dispWaived, reason: "cosmetic; alters no API semantics any owner depends on"},
		"Placement":          {kind: dispWaived, reason: "cluster scheduling only; where the stream lives changes nothing its owner does"},
		"Compression":        {kind: dispWaived, reason: "storage-internal; alters no API semantics any owner depends on"},
		"FirstSeq":           {kind: dispWaived, reason: "creation-time initial sequence; CAS expectations come from re-reads and stored sequences, never from assumed starting points"},
		"AllowDirect":        {kind: dispWaived, reason: "owners read via the JS API, not direct gets; enabling direct reads changes nothing they do"},
		"MirrorDirect":       {kind: dispWaived, reason: "owners read via the JS API; direct reads for mirrors change nothing they do (and a mirror is rejected anyway)"},
		"AllowRollup":        {kind: dispRejected, errWant: "allow_rollup"},
		"AllowAtomicPublish": {kind: dispWaived, reason: "the owner never emits batch headers, and client-controlled direct writes are excluded by the reserved-namespace deny plus the required ACL"},
		"AllowBatchPublish":  {kind: dispWaived, reason: "the owner never emits batch headers, and client-controlled direct writes are excluded by the reserved-namespace deny plus the required ACL"},
		"AllowMsgSchedules":  {kind: dispWaived, reason: "the owner never emits schedule headers; independently enableable under DiscardOld, so this is a waiver, not domination — note a DEPLOYED schedules-enabled stream necessarily carries AllowRollup (forced at creation, server/stream.go:1819-1822), which the shared shape rejects on readback"},
		"Duplicates":         {kind: dispWaived, reason: "this owner sets no Nats-Msg-Id, so a deduplication window is inert"},
		"Metadata":           {kind: dispWaived, reason: "owner-inert: server-owned compatibility metadata (required API levels) is enforced by the server itself, and arbitrary keys change nothing the owner does"},
		"DiscardNewPer":      {kind: dispDominated, reason: "requires Discard=DiscardNew (rejected here: this owner requires DiscardOld), and has no owner-relevant effect apart from that prerequisite (server/stream.go:1784)"},
	}

	// The CAS-register commons (sess, willq, willcancel, qos2in, qos2assign,
	// inflight): single-slot subjects, permanent limits retention, no consumers.
	register := func() {
		d["Retention"] = fieldDisposition{kind: dispRejected, errWant: "retention"}
		d["Discard"] = fieldDisposition{kind: dispRejected, errWant: "discard"}
		d["MaxMsgsPer"] = fieldDisposition{kind: dispRejected, errWant: "max_msgs_per_subject"}
		d["MaxMsgs"] = fieldDisposition{kind: dispRejected, errWant: "global limits"}
		d["MaxBytes"] = fieldDisposition{kind: dispRejected, errWant: "global limits"}
		d["MaxAge"] = fieldDisposition{kind: dispRejected, errWant: "global limits"}
		d["MaxMsgSize"] = fieldDisposition{kind: dispRejected, errWant: "max_msg_size"}
		d["Sources"] = fieldDisposition{kind: dispRejected, errWant: "sources"}
		d["SubjectDeleteMarkerTTL"] = fieldDisposition{kind: dispRejected, errWant: "subject_delete_marker_ttl"}
		d["MaxConsumers"] = fieldDisposition{kind: dispWaived, reason: "no consumer is ever created on this stream, so a consumer cap cannot affect its owner"}
		d["ConsumerLimits.InactiveThreshold"] = fieldDisposition{kind: dispWaived, reason: "no consumer is ever created on this stream, so a stream-forced consumer limit cannot affect its owner"}
		d["ConsumerLimits.MaxAckPending"] = fieldDisposition{kind: dispWaived, reason: "no consumer is ever created on this stream, so a stream-forced consumer limit cannot affect its owner"}
		d["AllowMsgTTL"] = fieldDisposition{kind: dispWaived, reason: "this owner sends no Nats-TTL header, so the capability is inert if enabled"}
	}
	// The interest-delivery commons (msgs, out): unlimited subjects, consumers.
	interest := func() {
		d["Retention"] = fieldDisposition{kind: dispRejected, errWant: "retention"}
		d["Discard"] = fieldDisposition{kind: dispWaived, reason: "inert: every global and per-subject limit is required unlimited, so no discard policy ever applies"}
		d["MaxMsgsPer"] = fieldDisposition{kind: dispRejected, errWant: "max_msgs_per_subject"}
		d["MaxMsgs"] = fieldDisposition{kind: dispRejected, errWant: "global limits"}
		d["MaxBytes"] = fieldDisposition{kind: dispRejected, errWant: "global limits"}
		d["MaxAge"] = fieldDisposition{kind: dispRejected, errWant: "global limits"}
		d["MaxMsgSize"] = fieldDisposition{kind: dispRejected, errWant: "max_msg_size"}
		d["Sources"] = fieldDisposition{kind: dispRejected, errWant: "sources"}
		d["SubjectDeleteMarkerTTL"] = fieldDisposition{kind: dispRejected, errWant: "subject_delete_marker_ttl"}
		d["MaxConsumers"] = fieldDisposition{kind: dispRejected, errWant: "max_consumers"}
		d["ConsumerLimits.MaxAckPending"] = fieldDisposition{kind: dispRejected, errWant: "max_ack_pending"}
		d["DenyDelete"] = fieldDisposition{kind: dispWaived, reason: "this owner never deletes messages; interest retention removes them with their consumers"}
		d["DenyPurge"] = fieldDisposition{kind: dispWaived, reason: "this owner never purges"}
		d["DiscardNewPer"] = fieldDisposition{kind: dispDominated, reason: "requires a finite MaxMsgsPer (rejected here: this owner requires it unlimited), and has no owner-relevant effect apart from that prerequisite (server/stream.go:1784)"}
	}

	switch stream {
	case mqttSessStreamName:
		register()
		d["DenyDelete"] = fieldDisposition{kind: dispRejected, errWant: "deny_delete"}
		d["DenyPurge"] = fieldDisposition{kind: dispWaived, reason: "nothing ever purges the session stream; records leave one at a time through exact-sequence deletes"}
		// This owner DOES create a consumer now: every broker runs its own ephemeral
		// session-record watch over this stream (sess_record_watch.go), so the
		// register-commons waiver no longer describes it.
		d["MaxConsumers"] = fieldDisposition{kind: dispRejected, errWant: "max_consumers"}
		d["ConsumerLimits.InactiveThreshold"] = fieldDisposition{kind: dispWaived, reason: "a stream threshold below the watch's requested five minutes already fails the consumer create at initialization, naming the consumer; a validator entry would duplicate that enforcement"}
		d["ConsumerLimits.MaxAckPending"] = fieldDisposition{kind: dispWaived, reason: "a positive stream limit is copied into the zero-request watch consumer and then rejected against AckNone at the same create, which already fails initialization"}
		// Round-3 Q2.1: the incarnation epoch is a REAL prerequisite carried in
		// Metadata, validated by the composed callback on the same snapshot.
		d["Metadata"] = fieldDisposition{kind: dispRejected, errWant: mqttSessEpochMetaKey}
	case mqttWillQStreamName:
		register()
		d["DenyDelete"] = fieldDisposition{kind: dispRejected, errWant: "deny_delete"}
		d["DenyPurge"] = fieldDisposition{kind: dispWaived, reason: "nothing ever purges the outbox; records leave through exact-sequence deletes"}
	case mqttWillCancelStreamName:
		register()
		d["DenyDelete"] = fieldDisposition{kind: dispWaived, reason: "no operation ever deletes a watermark — markers are permanent by design"}
		d["DenyPurge"] = fieldDisposition{kind: dispWaived, reason: "no operation ever purges a watermark — markers are permanent by design"}
		d["SubjectDeleteMarkerTTL"] = fieldDisposition{kind: dispWaived, reason: "no delete ever occurs here, so no delete marker can ever be created"}
	case mqttTeardownStreamName:
		register()
		d["DenyDelete"] = fieldDisposition{kind: dispWaived, reason: "no operation ever deletes a retirement watermark — a fence is permanent by design, and reaping one would unfence a generation whose resources are already deleted"}
		d["DenyPurge"] = fieldDisposition{kind: dispWaived, reason: "no operation ever purges a retirement watermark — fences are permanent by design"}
		d["SubjectDeleteMarkerTTL"] = fieldDisposition{kind: dispWaived, reason: "no delete ever occurs here, so no delete marker can ever be created"}
		// The incarnation epoch is a REAL prerequisite here, not owner-inert
		// metadata: a watermark orders itself by a $MQTT_sess sequence, so this
		// stream is only interpretable within the session-stream incarnation it was
		// created against. See validateTeardownStream.
		d["Metadata"] = fieldDisposition{kind: dispRejected, errWant: mqttSessEpochMetaKey}
	case mqttQoS2IncomingMsgsStreamName:
		register()
		d["DenyDelete"] = fieldDisposition{kind: dispRejected, errWant: "deny_delete"}
		d["DenyPurge"] = fieldDisposition{kind: dispRejected, errWant: "deny_purge"}
	case mqttQoS2AssignStreamName:
		register()
		d["DenyDelete"] = fieldDisposition{kind: dispRejected, errWant: "deny_delete"}
		d["DenyPurge"] = fieldDisposition{kind: dispWaived, reason: "nothing here purges by subject; the claim reap deletes exact sequences"}
	case mqttInflightStreamName:
		register()
		d["DenyDelete"] = fieldDisposition{kind: dispRejected, errWant: "deny_delete"}
		d["DenyPurge"] = fieldDisposition{kind: dispRejected, errWant: "deny_purge"}
	case mqttStreamName:
		interest()
		d["AllowMsgTTL"] = fieldDisposition{kind: dispRejected, errWant: "allow_msg_ttl"}
		d["ConsumerLimits.InactiveThreshold"] = fieldDisposition{kind: dispWaived, reason: "plain subscriptions already run under the broker's own inactive-threshold policy, and the drain and shared/affine paths refuse a stream-forced limit at runtime (docs/CONFORMANCE.md); the silent-dead-resume exposure of a reaped plain durable is a property of inactivity-threshold reaping generally (the broker option included), recorded as its own docs/ROADMAP.md section-0 item, not something a startup check on one of its two sources would close"}
		d["Duplicates"] = fieldDisposition{kind: dispWaived, reason: "the release msgId is best-effort narrowing of the move protocol's unresolved interval only; ownership never rests on the window (qos2_move.go)"}
	case mqttOutStreamName:
		interest()
		d["AllowMsgTTL"] = fieldDisposition{kind: dispWaived, reason: "this owner sends no Nats-TTL header, so the capability is inert if enabled"}
		d["ConsumerLimits.InactiveThreshold"] = fieldDisposition{kind: dispRejected, errWant: "inactivity"}
		d["Duplicates"] = fieldDisposition{kind: dispWaived, reason: "the drained-PUBREL msgId is best-effort narrowing only; arbitration is the durable drain record, never the window (pubrel_correlation.go)"}
	case mqttRetainedMsgsStreamName:
		register()
		d["AllowMsgTTL"] = fieldDisposition{kind: dispRejected, errWant: "allow_msg_ttl"}
		d["MaxConsumers"] = fieldDisposition{kind: dispRejected, errWant: "max_consumers"}
		d["DenyDelete"] = fieldDisposition{kind: dispWaived, reason: "this owner issues no delete API request; the empty-body tombstone plus MaxMsgsPer:1 replacement carry correctness, and both that store-time eviction and TTL expiry are store-internal reaping DenyDelete does not gate"}
		d["DenyPurge"] = fieldDisposition{kind: dispWaived, reason: "nothing ever purges retained messages"}
		d["Sources"] = fieldDisposition{kind: dispWaived, reason: "supported cross-account retained sourcing topology; the decoder handles sourced records explicitly"}
		d["ConsumerLimits.InactiveThreshold"] = fieldDisposition{kind: dispWaived, reason: "a stream limit below the requested 5 minutes already fails initialization at the consumer create itself, naming the consumer; a validator entry would duplicate that enforcement"}
		d["ConsumerLimits.MaxAckPending"] = fieldDisposition{kind: dispWaived, reason: "any positive value is copied into the zero-request consumer and rejected against AckNone at the same create; a validator entry would duplicate that enforcement"}
	}
	return d
}

// changedTopLevelFields diffs two configs field by field, returning the names
// of top-level fields that differ.
func changedTopLevelFields(a, b *StreamConfig) []string {
	var changed []string
	va, vb := reflect.ValueOf(*a), reflect.ValueOf(*b)
	for i := 0; i < va.NumField(); i++ {
		if !reflect.DeepEqual(va.Field(i).Interface(), vb.Field(i).Interface()) {
			changed = append(changed, va.Type().Field(i).Name)
		}
	}
	return changed
}

// TestStreamValidatorFieldCoverage is the drift guard: it walks every
// StreamConfig field by reflection against every validator in the
// provisioning table. Bijection first — a dependency bump that adds a field
// fails naming it; then exactly one disposition per (validator, field), the
// baseline accepted, every mutator changing exactly its named field to a
// non-baseline value, every rejection naming its check, every waiver ACCEPTED
// (a waiver that masks a live rejection fails), and every waiver/domination
// reason non-empty prose.
func TestStreamValidatorFieldCoverage(t *testing.T) {
	t.Parallel()
	fields := reflectedStreamConfigFields(t)

	// Bijection: reflected set <-> mutator table, iterating the REFLECTED set.
	for _, f := range fields {
		if _, ok := streamFieldMutators[f]; !ok {
			t.Errorf("StreamConfig field %q has no mutator: a dependency bump added a field the audit does not cover", f)
		}
	}
	for f := range streamFieldMutators {
		found := false
		for _, rf := range fields {
			if rf == f {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("mutator %q names a StreamConfig field that no longer exists", f)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	for si, streamName := range mqttTestBrokerStreamNames {
		t.Run(streamName, func(t *testing.T) {
			disp := dispositionsFor(streamName)

			// Exactly one disposition per reflected field, and none extra.
			for _, f := range fields {
				if _, ok := disp[f]; !ok {
					t.Fatalf("field %q has no disposition for %s", f, streamName)
				}
			}
			for f := range disp {
				if _, ok := streamFieldMutators[f]; !ok {
					t.Fatalf("disposition for %q names a field with no mutator", f)
				}
			}

			// The baseline must be accepted before any mutation, else every
			// rejection below would be vacuous.
			specs := testBrokerStreamSpecs()
			spec := specs[si]
			if spec.name != streamName {
				t.Fatalf("spec order drifted: got %q want %q", spec.name, streamName)
			}
			if err := spec.validate(&StreamInfo{Config: *spec.cfg}); err != nil {
				t.Fatalf("baseline rejected: %v", err)
			}

			for _, f := range fields {
				f := f
				t.Run(f, func(t *testing.T) {
					fd := disp[f]
					if fd.kind != dispRejected && fd.reason == "" {
						t.Fatalf("%s disposition for %q carries no reason; the audit is the reason", fd.kind, f)
					}

					// Fresh baseline per mutation (the table shares slices/maps).
					baseline := testBrokerStreamSpecs()[si]
					mutated := *baseline.cfg
					streamFieldMutators[f](&mutated)

					// The mutator must change exactly its named field, away from
					// the baseline value (round-3 Q4.1's differs-from-baseline rule).
					top := f
					if i := strings.IndexByte(f, '.'); i >= 0 {
						top = f[:i]
					}
					changed := changedTopLevelFields(baseline.cfg, &mutated)
					if len(changed) != 1 || changed[0] != top {
						t.Fatalf("mutator %q changed %v, want exactly [%s]", f, changed, top)
					}

					if fd.kind == dispDominated {
						// The enabling configuration is rejected by the named
						// prerequisite, so the mutated config could never be
						// deployed and a pure validator's verdict on it proves
						// nothing either way.
						return
					}

					err := baseline.validate(&StreamInfo{Config: mutated})
					switch fd.kind {
					case dispRejected:
						if err == nil {
							t.Fatalf("%s: mutation of %q must be rejected", streamName, f)
						}
						if fd.errWant == "" {
							t.Fatalf("rejected disposition for %q names no check", f)
						}
						if !strings.Contains(err.Error(), fd.errWant) {
							t.Fatalf("%s: rejection of %q does not name its check %q: %v", streamName, f, fd.errWant, err)
						}
					case dispWaived:
						if err != nil {
							t.Fatalf("%s: %q is waived (%s) but the mutation was rejected: %v", streamName, f, fd.reason, err)
						}
					default:
						t.Fatalf("unknown disposition kind %q", fd.kind)
					}
				})
			}
		})
	}
}
