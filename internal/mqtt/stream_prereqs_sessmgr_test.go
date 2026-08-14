//go:build mqttport_fulltests

// End-to-end complements to stream_prereqs_test.go: the pure tests prove each
// validator's own logic; these prove the validators are actually wired into
// account-manager startup — refusal on a pre-existing nonconforming stream
// (without mutating it), readback of the broker's own creations, and the
// create-race loser validating the WINNER's stream.

package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// jsRawCreateStream creates a stream through the raw JS API with the server's
// own StreamConfig struct, so tests can set fields (AllowMsgTTL, Metadata,
// DenyDelete) independently of the nats.go client's config coverage.
func jsRawCreateStream(t *testing.T, nc *nats.Conn, cfg *StreamConfig) {
	t.Helper()
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal stream config: %v", err)
	}
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamCreateT, cfg.Name), b, 5*time.Second)
	if err != nil {
		t.Fatalf("create stream %s: %v", cfg.Name, err)
	}
	var resp JSApiStreamCreateResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("create stream %s: %+v", cfg.Name, resp.Error)
	}
}

// jsRawStreamConfig reads a stream's current config through the raw JS API.
func jsRawStreamConfig(t *testing.T, nc *nats.Conn, name string) *StreamConfig {
	t.Helper()
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, name), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("stream info %s: %v", name, err)
	}
	var resp JSApiStreamInfoResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("unmarshal info response: %v", err)
	}
	if resp.Error != nil || resp.StreamInfo == nil {
		t.Fatalf("stream info %s: %+v", name, resp.Error)
	}
	return &resp.StreamInfo.Config
}

// Every stream the broker itself creates must satisfy its own validator on an
// authoritative readback (the qos2_move_test pattern, extended to the four new
// validators) — this is also what proves the validators tolerate JetStream's
// creation-time normalization (MaxConsumers/MaxMsgSize 0 -> -1, the default
// duplicate window).
func TestNewValidatorsAcceptBrokerCreatedStreams(t *testing.T) {
	t.Parallel()
	s, c, _ := newBringUpServer(t)
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreate: %v", err)
	}
	for _, tc := range []struct {
		stream   string
		validate func(*StreamInfo) error
	}{
		{mqttSessStreamName, func(si *StreamInfo) error { return validateSessStream(si, asm.domainTk, 0) }},
		{mqttWillQStreamName, func(si *StreamInfo) error { return validateWillQStream(si, asm.domainTk, 0) }},
		{mqttStreamName, func(si *StreamInfo) error { return validateMsgsStream(si, 0, mqttDefaultMaxAckPending, true) }},
		{mqttRetainedMsgsStreamName, func(si *StreamInfo) error { return validateRetainedStream(si, 0, true) }},
	} {
		si, err := asm.jsa.lookupStream(tc.stream)
		if err != nil || si == nil {
			t.Fatalf("lookup %s: si=%v err=%v", tc.stream, si, err)
		}
		if verr := tc.validate(si); verr != nil {
			t.Fatalf("the %s stream the broker created does not satisfy its own validator: %v", tc.stream, verr)
		}
	}
}

// A pre-existing nonconforming stream must make account-manager initialization
// FAIL with an error naming the stream, the offending setting and the
// recovery — and must NOT be mutated in place (another account manager may
// already have initialized against it). One subtest per new validator, each poisoning exactly one
// field; the sess subtest also proves the documented recovery (recreate, then
// initialization succeeds).
func TestNewValidatorMisconfigurationFailsStartup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		cfg     func() *StreamConfig
		poison  func(cfg *StreamConfig) // the single nonconforming field
		poisonQ func(cfg *StreamConfig) bool
		want    []string
	}{
		{
			name: mqttSessStreamName,
			cfg: func() *StreamConfig {
				return &StreamConfig{
					Name:               mqttSessStreamName,
					Subjects:           testSessStreamSubjects(),
					Storage:            FileStorage,
					Retention:          LimitsPolicy,
					MaxMsgsPer:         1,
					Replicas:           1,
					AllowAtomicPublish: true,
					Metadata:           map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
				}
			},
			// A valid epoch is present, so the failure is attributable to the
			// NEW DenyDelete check, not the epoch validation failing first.
			poison:  func(cfg *StreamConfig) { cfg.DenyDelete = true },
			poisonQ: func(cfg *StreamConfig) bool { return cfg.DenyDelete },
			// The recovery must be the COUPLED one: recreating $MQTT_sess alone
			// mints a new epoch against stale retirement watermarks, and destroys
			// every permanent CONNECT acceptance fact with it.
			want: []string{mqttSessStreamName, mqttTeardownStreamName, "one logical unit", "deny_delete", "recreate"},
		},
		{
			name: mqttWillQStreamName,
			cfg: func() *StreamConfig {
				return &StreamConfig{
					Name:       mqttWillQStreamName,
					Subjects:   []string{mqttWillQSubjectPrefix + ">"},
					Storage:    FileStorage,
					Retention:  LimitsPolicy,
					MaxMsgsPer: 1,
					Replicas:   1,
				}
			},
			poison:  func(cfg *StreamConfig) { cfg.DenyDelete = true },
			poisonQ: func(cfg *StreamConfig) bool { return cfg.DenyDelete },
			want:    []string{mqttWillQStreamName, "deny_delete", "recreate"},
		},
		{
			name: mqttStreamName,
			cfg: func() *StreamConfig {
				return &StreamConfig{
					Name:        mqttStreamName,
					Subjects:    []string{mqttStreamSubjectPrefix + ">"},
					Storage:     FileStorage,
					Retention:   LimitsPolicy, // nonconforming: interest is required
					Replicas:    1,
					AllowMsgTTL: true,
				}
			},
			poison:  func(cfg *StreamConfig) {},
			poisonQ: func(cfg *StreamConfig) bool { return cfg.Retention == LimitsPolicy },
			want:    []string{mqttStreamName, "retention", "recreate"},
		},
		{
			name: mqttRetainedMsgsStreamName,
			cfg: func() *StreamConfig {
				return &StreamConfig{
					Name:        mqttRetainedMsgsStreamName,
					Subjects:    []string{mqttRetainedMsgsStreamSubject + ">"},
					Storage:     FileStorage,
					Retention:   LimitsPolicy,
					MaxMsgsPer:  2, // nonconforming: the per-topic register needs 1
					Replicas:    1,
					AllowMsgTTL: true,
				}
			},
			poison:  func(cfg *StreamConfig) {},
			poisonQ: func(cfg *StreamConfig) bool { return cfg.MaxMsgsPer == 2 },
			want:    []string{mqttRetainedMsgsStreamName, "max_msgs_per_subject", "recreate"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c, nc := newBringUpServer(t)
			cfg := tc.cfg()
			tc.poison(cfg)
			jsRawCreateStream(t, nc, cfg)

			_, err := s.getOrCreateMQTTAccountSessionManager(c)
			if err == nil {
				t.Fatalf("expected initialization to fail against the nonconforming %s, but it succeeded", tc.name)
			}
			// The message must name the problem AND tell the operator what to do.
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("startup error %q does not mention %q", err, want)
				}
			}
			// The refused startup must not have repaired the stream in place.
			after := jsRawStreamConfig(t, nc, tc.name)
			if !tc.poisonQ(after) {
				t.Fatalf("the refused startup mutated %s in place: %+v", tc.name, after)
			}
			if tc.name == mqttSessStreamName {
				if after.Metadata[mqttSessEpochMetaKey] != cfg.Metadata[mqttSessEpochMetaKey] {
					t.Fatalf("the refused startup touched the epoch metadata")
				}
			}
		})
	}

	// The documented recovery, proven rather than asserted: recreate the named
	// stream and initialization succeeds.
	t.Run("recovery", func(t *testing.T) {
		s, c, nc := newBringUpServer(t)
		jsRawCreateStream(t, nc, &StreamConfig{
			Name:               mqttSessStreamName,
			Subjects:           testSessStreamSubjects(),
			Storage:            FileStorage,
			Retention:          LimitsPolicy,
			MaxMsgsPer:         1,
			Replicas:           1,
			AllowAtomicPublish: true,
			DenyDelete:         true,
			Metadata:           map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
		})
		if _, err := s.getOrCreateMQTTAccountSessionManager(c); err == nil {
			t.Fatal("expected the nonconforming stream to refuse startup")
		}
		js, ctx := newJS(t, nc)
		if err := js.DeleteStream(ctx, mqttSessStreamName); err != nil {
			t.Fatalf("delete the nonconforming stream: %v", err)
		}
		s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
		if _, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient()); err != nil {
			t.Fatalf("initialization still fails after recreating the stream: %v", err)
		}
	})
}

// The create-race LOSER must validate the WINNER's stream, not its own
// requested config: the manager is barriered at the exact lookup->create
// boundary (the initial lookup saw the stream absent), the "winner" creates
// $MQTT_sess while the manager is blocked, and the release makes the manager's
// create lose with JSStreamNameExistErr. The winner carries a VALID session
// epoch and deviates in exactly one new-validator field (DenyDelete), so the
// failure is attributable to the new check and cannot be the composed epoch
// validation failing first. Together with the stubbed helper unit test (the
// winner path) and the spec-table test, this pins mutation 12's NameExist
// branch end to end.
func TestSessCreateRaceLoserValidatesWinnersStream(t *testing.T) {
	t.Parallel()
	s, c, nc := newBringUpServer(t)

	atGate := make(chan struct{})
	release := make(chan struct{})
	s.mqtt.sessmgr.testSessCreateGate = func() {
		close(atGate)
		<-release
	}
	var created bool
	var cerr error
	s.mqtt.sessmgr.testSessCreateObserve = func(cr bool, err error) {
		created, cerr = cr, err
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := s.getOrCreateMQTTAccountSessionManager(c)
		errCh <- err
	}()

	// The manager saw the stream absent and is parked before its create.
	<-atGate
	jsRawCreateStream(t, nc, &StreamConfig{
		Name:               mqttSessStreamName,
		Subjects:           testSessStreamSubjects(),
		Storage:            FileStorage,
		Retention:          LimitsPolicy,
		MaxMsgsPer:         1,
		Replicas:           1,
		AllowAtomicPublish: true,
		DenyDelete:         true, // the single deviation the loser must detect
		Metadata:           map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()},
	})
	close(release)

	err := <-errCh
	if err == nil {
		t.Fatal("the loser accepted the winner's nonconforming stream")
	}
	if !strings.Contains(err.Error(), "deny_delete") {
		t.Fatalf("the failure is not attributable to the winner's DenyDelete: %v", err)
	}
	if created {
		t.Fatal("the manager's create was expected to LOSE the race")
	}
	if !IsNatsErr(cerr, JSStreamNameExistErr) {
		t.Fatalf("the manager's create was expected to lose with JSStreamNameExistErr, got %v", cerr)
	}
}

// A max_protocol_version:4 broker refuses every MQTT 5 CONNECT, so it never
// emits a per-message Nats-TTL — a pre-existing $MQTT_msgs/$MQTT_rmsgs without
// AllowMsgTTL is a legitimate 3.1.1-only deployment there and initialization
// must SUCCEED (on a default, MQTT 5-accepting broker the same streams are a
// startup failure; the field-coverage audit pins that side).
func TestV4OnlyBrokerAcceptsTTLlessStreams(t *testing.T) {
	t.Parallel()
	s, c, nc := newBringUpServer(t)
	s.opts.MQTT.MaxProtocolVersion = mqttProtoLevel

	jsRawCreateStream(t, nc, &StreamConfig{
		Name:      mqttStreamName,
		Subjects:  []string{mqttStreamSubjectPrefix + ">"},
		Storage:   FileStorage,
		Retention: InterestPolicy,
		Replicas:  1,
	})
	jsRawCreateStream(t, nc, &StreamConfig{
		Name:       mqttRetainedMsgsStreamName,
		Subjects:   []string{mqttRetainedMsgsStreamSubject + ">"},
		Storage:    FileStorage,
		Retention:  LimitsPolicy,
		MaxMsgsPer: 1,
		Replicas:   1,
	})

	if _, err := s.getOrCreateMQTTAccountSessionManager(c); err != nil {
		t.Fatalf("a 3.1.1-only broker must accept TTL-less pre-existing streams: %v", err)
	}
	// And the refused startup on a default broker is the other half: the same
	// $MQTT_msgs stream must fail a v5-accepting broker's initialization.
	s2 := &Server{nrc: nc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: "node-v5"}
	s2.running.Store(true)
	s2.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	t.Cleanup(func() { close(s2.quitCh) })
	_, err := s2.getOrCreateMQTTAccountSessionManager(s2.createInternalAccountClient())
	if err == nil {
		t.Fatal("a v5-accepting broker must refuse the TTL-less stream")
	}
	if !strings.Contains(err.Error(), "allow_msg_ttl") {
		t.Fatalf("the refusal must name allow_msg_ttl: %v", err)
	}
}
