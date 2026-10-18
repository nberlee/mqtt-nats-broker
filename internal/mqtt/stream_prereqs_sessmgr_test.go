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

// testResetBundle performs the documented recovery: STOP every broker serving the
// account, then delete all nine streams.
//
// The stop is the point, not a formality. Deleting the bundle under a live manager is
// not the contract's procedure and proves nothing about it — worse, an initialized
// manager has already started the expiry, orphan and QoS 2 sweeps and the delayed-Will
// claim loop, and those goroutines keep reading and writing JetStream, which is the
// concurrent-reset window this change documents as a residual rather than something a
// fixture should reproduce. So the stop closes quitCh, which is what those goroutines
// select on, as well as deresidenting the managers.
//
// A server stopped here is NOT reusable: the caller brings up a fresh one for the
// post-reset initialization, which is also what "stop all brokers, then restart"
// means in production.
func testResetBundle(t *testing.T, nc *nats.Conn, servers ...*Server) {
	t.Helper()
	for _, s := range servers {
		testStopBringUpServer(t, s)
	}
	for _, name := range mqttTestBrokerStreamNames {
		jsRawDeleteStream(t, nc, name)
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
	return &resp.Config
}

// testBundleThenReplace brings a manager up so the account holds a real bundle
// under the epoch that manager adopted, then replaces the named members,
// STAMPING each replacement with that same epoch. For a defect that is not
// about the epoch: the preflight admits nine-present and the per-stream
// validator is what refuses, which is the production shape.
func testBundleThenReplace(t *testing.T, s *Server, c *client, nc *nats.Conn, cfgs map[string]*StreamConfig) string {
	t.Helper()
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("bringing the bundle up: %v", err)
	}
	epoch := asm.sessStreamEpoch
	for name, cfg := range cfgs {
		jsRawDeleteStream(t, nc, name)
		if cfg.Metadata == nil {
			cfg.Metadata = map[string]string{}
		}
		cfg.Metadata[mqttSessEpochMetaKey] = epoch
		jsRawCreateStream(t, nc, cfg)
	}
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	return epoch
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
	// All NINE, not the four this test used to name: every validator now also
	// compares the bundle epoch stamp, so every one of them has a new way to
	// reject a stream the broker itself created. The epoch passed is the one this
	// manager ADOPTED — using the unit-test constant here would assert against a
	// bundle that does not exist.
	e := asm.sessStreamEpoch
	for _, tc := range []struct {
		stream   string
		validate func(*StreamInfo) error
	}{
		{mqttSessStreamName, func(si *StreamInfo) error { return validateSessStream(si, asm.domainTk, 0) }},
		{mqttWillQStreamName, func(si *StreamInfo) error { return validateWillQStream(si, asm.domainTk, 0, e) }},
		{mqttTeardownStreamName, func(si *StreamInfo) error { return validateTeardownStream(si, asm.domainTk, 0, e) }},
		{mqttStreamName, func(si *StreamInfo) error {
			return validateMsgsStream(si, 0, mqttDefaultMaxAckPending, e)
		}},
		{mqttQoS2IncomingMsgsStreamName, func(si *StreamInfo) error { return validateQoS2InStream(si, 0, e) }},
		{mqttQoS2AssignStreamName, func(si *StreamInfo) error { return validateQoS2AssignStream(si, 0, e) }},
		{mqttInflightStreamName, func(si *StreamInfo) error { return validateInflightStream(si, 0, e) }},
		{mqttOutStreamName, func(si *StreamInfo) error {
			return validateOutStream(si, 0, mqttDefaultMaxAckPending, e)
		}},
		{mqttRetainedMsgsStreamName, func(si *StreamInfo) error { return validateRetainedStream(si, 0, e) }},
	} {
		si, err := asm.jsa.lookupStream(tc.stream)
		if err != nil || si == nil {
			t.Fatalf("lookup %s: si=%v err=%v", tc.stream, si, err)
		}
		if verr := tc.validate(si); verr != nil {
			t.Fatalf("the %s stream the broker created does not satisfy its own validator: %v", tc.stream, verr)
		}
	}
	// And the stamps AGREE across the whole bundle, which is what makes the nine
	// one recreation unit. Asserted here rather than inferred from the nine
	// validator calls above: each of those compares against the same e, so they
	// would all pass even if the manager had adopted an epoch no stream carried.
	if !mqttValidSessEpoch(e) {
		t.Fatalf("the adopted recovery epoch is not well-formed: %q", e)
	}
	for _, name := range mqttTestBrokerStreamNames {
		si, err := asm.jsa.lookupStream(name)
		if err != nil || si == nil {
			t.Fatalf("lookup %s: si=%v err=%v", name, si, err)
		}
		if got := si.Config.Metadata[mqttSessEpochMetaKey]; got != e {
			t.Fatalf("%s carries recovery epoch %q, want the adopted %q: the nine streams must be one bundle",
				name, got, e)
		}
	}
}

// A nonconforming stream inside an OTHERWISE COMPLETE bundle must make
// account-manager initialization FAIL with an error naming the stream, the
// offending setting and the recovery — and must NOT be mutated in place
// (another account manager may already have initialized against it).
//
// The fixture shape is forced, not preferred. This test used to hand-create ONE
// stream and let the broker provision the rest; startup now REFUSES a proper
// subset of the nine before any validator runs, so that shape would exercise the
// count rule instead of the validator. Bringing the whole bundle up and then
// replacing one member — carrying the same epoch, so only the poisoned FIELD
// differs — is what reaches the validator, and it is the shape a production account is
// in whenever a broker is serving it. It is not the only shape one can be in: an
// interrupted first provisioning, an administrative deletion or a partial restore all
// leave a partial bundle, which is REFUSED rather than prevented — and those paths have
// their own fixtures, including the Start-level one below.
//
// One subtest per validator, each poisoning exactly one field.
func TestNewValidatorMisconfigurationFailsStartup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		poison        func(cfg *StreamConfig) // the single nonconforming field
		stillPoisoned func(cfg *StreamConfig) bool
		want          []string
	}{
		{
			name: mqttSessStreamName,
			// A valid epoch is present (the helper stamps it), so the failure is
			// attributable to the DenyDelete check and not to epoch validation
			// firing first.
			poison:        func(cfg *StreamConfig) { cfg.DenyDelete = true },
			stillPoisoned: func(cfg *StreamConfig) bool { return cfg.DenyDelete },
			want:          []string{mqttSessStreamName, "deny_delete", "ALL NINE", "stop ALL brokers"},
		},
		{
			name:          mqttWillQStreamName,
			poison:        func(cfg *StreamConfig) { cfg.DenyDelete = true },
			stillPoisoned: func(cfg *StreamConfig) bool { return cfg.DenyDelete },
			want:          []string{mqttWillQStreamName, "deny_delete", "ALL NINE"},
		},
		{
			name:          mqttStreamName,
			poison:        func(cfg *StreamConfig) { cfg.Retention = LimitsPolicy },
			stillPoisoned: func(cfg *StreamConfig) bool { return cfg.Retention == LimitsPolicy },
			want:          []string{mqttStreamName, "retention", "ALL NINE"},
		},
		{
			name:          mqttRetainedMsgsStreamName,
			poison:        func(cfg *StreamConfig) { cfg.MaxMsgsPer = 2 },
			stillPoisoned: func(cfg *StreamConfig) bool { return cfg.MaxMsgsPer == 2 },
			want:          []string{mqttRetainedMsgsStreamName, "max_msgs_per_subject", "ALL NINE"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c, nc := newBringUpServer(t)
			cfg := (*StreamConfig)(nil)
			for _, bc := range testBundleConfigs(t, _EMPTY_, 1) {
				if bc.Name == tc.name {
					cfg = bc
				}
			}
			if cfg == nil {
				t.Fatalf("%s is not a member of the broker-owned bundle", tc.name)
			}
			tc.poison(cfg)
			testBundleThenReplace(t, s, c, nc, map[string]*StreamConfig{tc.name: cfg})

			_, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
			if err == nil {
				t.Fatalf("expected initialization to fail against the nonconforming %s, but it succeeded", tc.name)
			}
			// The message must name the problem AND tell the operator what to do —
			// and what to do is now the WHOLE BUNDLE, because recreating this stream
			// alone is what the next startup refuses.
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("startup error %q does not mention %q", err, want)
				}
			}
			// The refused startup must not have repaired the stream in place.
			after := jsRawStreamConfig(t, nc, tc.name)
			if !tc.stillPoisoned(after) {
				t.Fatalf("the refused startup mutated %s in place: %+v", tc.name, after)
			}
			if after.Metadata[mqttSessEpochMetaKey] == _EMPTY_ {
				t.Fatalf("the refused startup dropped %s's recovery epoch stamp", tc.name)
			}
		})
	}
}

// The documented recovery is the WHOLE BUNDLE, proven rather than asserted: from
// a refused account, stopping the broker and deleting all nine streams lets the
// next initialization provision a fresh bundle and succeed.
//
// This replaces the per-stream "recovery" subtests that used to delete ONE
// stream and expect success — the procedure they proved is the one this change
// withdraws (F-066).
func TestBrokerStreamBundleResetRecovers(t *testing.T) {
	t.Parallel()
	s, c, nc := newBringUpServer(t)
	cfg := (*StreamConfig)(nil)
	for _, bc := range testBundleConfigs(t, _EMPTY_, 1) {
		if bc.Name == mqttSessStreamName {
			bc.DenyDelete = true
			cfg = bc
		}
	}
	epoch := testBundleThenReplace(t, s, c, nc, map[string]*StreamConfig{mqttSessStreamName: cfg})

	if _, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient()); err == nil {
		t.Fatal("expected the nonconforming stream to refuse startup")
	}
	// The reset: every one of the nine, with nothing serving the account. The
	// stopped broker is not restarted — "stop all brokers, then restart" means a new
	// process, so the recovery is proven on a fresh one.
	testResetBundle(t, nc, s)
	s2, c2, _ := newBringUpServerOn(t, nc, "node-after-reset")
	asm, err := s2.getOrCreateMQTTAccountSessionManager(c2)
	if err != nil {
		t.Fatalf("initialization still fails after the whole-bundle reset: %v", err)
	}
	if asm.sessStreamEpoch == epoch {
		t.Fatalf("the reset bundle reused the old recovery epoch %q; a fresh bundle must differ", epoch)
	}
	for _, name := range mqttTestBrokerStreamNames {
		si, lerr := asm.jsa.lookupStream(name)
		if lerr != nil || si == nil {
			t.Fatalf("lookup %s after the reset: si=%v err=%v", name, si, lerr)
		}
		if got := si.Config.Metadata[mqttSessEpochMetaKey]; got != asm.sessStreamEpoch {
			t.Fatalf("%s carries %q after the reset, want the fresh %q", name, got, asm.sessStreamEpoch)
		}
	}
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

// Every broker accepts MQTT 5.0, so any stored or retained PUBLISH may carry a
// per-message Nats-TTL (Message Expiry): a pre-existing $MQTT_msgs or $MQTT_rmsgs
// without AllowMsgTTL is a startup failure, refused by that stream's validator.
func TestBrokerRefusesTTLlessMessageStreams(t *testing.T) {
	t.Parallel()
	// The bundle is built BY HAND, because no broker can provision a TTL-less member:
	// $MQTT_msgs and $MQTT_rmsgs are created with AllowMsgTTL set. ONE member is
	// TTL-less per subtest, so a validator that stopped checking cannot be masked by
	// the other member's refusal.
	for _, stream := range []string{mqttStreamName, mqttRetainedMsgsStreamName} {
		t.Run(stream, func(t *testing.T) {
			t.Parallel()
			s, c, nc := newBringUpServer(t)
			testCreateBundle(t, nc, map[string]func(*StreamConfig){
				stream: func(cfg *StreamConfig) { cfg.AllowMsgTTL = false },
			})

			_, err := s.getOrCreateMQTTAccountSessionManager(c)
			if err == nil {
				t.Fatalf("the broker accepted a TTL-less %s", stream)
			}
			if !strings.Contains(err.Error(), "allow_msg_ttl") {
				t.Fatalf("the refusal must name allow_msg_ttl: %v", err)
			}
			// The refusal names every bundle member in its recovery guidance, so only
			// this clause attributes it to the offending stream.
			if !strings.Contains(err.Error(), "recreating "+stream+" alone") {
				t.Fatalf("the refusal must come from %s's validator: %v", stream, err)
			}
			// A complete bundle is present, so a refusal mentioning the count rule
			// would mean this fixture stopped testing what it names.
			if strings.Contains(err.Error(), "of the 10 broker-owned streams") {
				t.Fatalf("the refusal came from the bundle preflight, not the TTL validator: %v", err)
			}
		})
	}
}

// The concurrent-cold-start recovery, through Server.Start rather than around it.
//
// The contract docs/CONFORMANCE.md states is specifically about Start: a broker that
// meets a partial bundle REFUSES, Start treats a failed account-manager initialization
// as FATAL — it runs Shutdown and returns the error, offering no retry — and recovery
// is therefore a RESTART, which the shipped three-replica Deployment performs by
// restarting the pod.
//
// Calling getOrCreateMQTTAccountSessionManager directly would exercise the refusal but
// not the thing the guidance rests on: that Start propagates it, tears itself down, and
// that a fresh Server then starts clean against the completed bundle. A regression in
// the fatal-start or teardown path would pass a manager-level test.
func TestBrokerStreamBundleStartRefusesThenRestartConverges(t *testing.T) {
	_, nc := runNATS(t)

	// An interrupted first provisioning: a peer created a proper subset and stopped.
	cfgs := testBundleConfigs(t, _EMPTY_, 1)
	for _, cfg := range cfgs[:4] {
		jsRawCreateStream(t, nc, cfg)
	}

	newBroker := func(node string) *Server {
		opts := testMQTTDefaultOptionsV5()
		opts.ServerName = node
		opts.MQTT.Port = -1
		return NewServer(nc, opts, node)
	}

	s1 := newBroker("bundle-refuse")
	err := s1.Start()
	if err == nil {
		<-s1.BeginShutdown()
		t.Fatal("Start must FAIL against a partial bundle: the recovery guidance rests on a " +
			"failed initialization being fatal rather than completing the bundle")
	}
	for _, want := range []string{"of the 10 broker-owned streams", "ONE recreation unit"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Start's error must carry the bundle refusal (%q), got: %v", want, err)
		}
	}
	// Start's own contract: a failed Start leaves nothing running.
	if s1.mqtt.listener != nil && len(s1.mqtt.sessmgr.sessions) != 0 {
		t.Fatal("a failed Start must have torn itself down, leaving no account manager")
	}

	// The peer completes the bundle, as the broker that admitted zero would have.
	for _, cfg := range cfgs[4:] {
		jsRawCreateStream(t, nc, cfg)
	}

	// The restart. A FRESH Server: the shutdown lifecycle is one-shot, so in
	// production this is a new process.
	s2 := newBroker("bundle-restart")
	if rerr := s2.Start(); rerr != nil {
		t.Fatalf("a restart against the completed bundle must succeed: %v", rerr)
	}
	t.Cleanup(func() { <-s2.BeginShutdown() })
	asm := s2.mqtt.sessmgr.sessions[globalAccountName]
	if asm == nil {
		t.Fatal("a successful Start must have published an account manager")
	}
	if got := asm.sessStreamEpoch; got != cfgs[0].Metadata[mqttSessEpochMetaKey] {
		t.Fatalf("the restart adopted %q, want the bundle's %q", got, cfgs[0].Metadata[mqttSessEpochMetaKey])
	}
}
