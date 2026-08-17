// $MQTT_sess incarnation-epoch resolution at account-manager initialization.
//
// The epoch is stamped into the stream metadata by whichever broker wins the
// create, and every broker — the winner included — adopts it from the
// authoritative re-read rather than from the config it proposed. It is the
// identity that makes a session SEQUENCE comparable, so a broker that resolved the
// wrong one, or none, must not serve CONNECTs at all.

package mqtt

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// newBringUpServer wires a minimal Server + internal account client against a live
// in-process JetStream (mirroring TestSessionManagerBringUp) so a test can
// pre-create streams before the account-session manager is initialized.
func newBringUpServer(t *testing.T) (*Server, *client, *nats.Conn) {
	t.Helper()
	_, nc := runNATS(t)
	s := &Server{
		nrc:      nc,
		opts:     &Options{},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	c := s.createInternalAccountClient()
	t.Cleanup(func() { close(s.quitCh) })
	return s, c, nc
}

func newJS(t *testing.T, nc *nats.Conn) (jetstream.JetStream, context.Context) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return js, ctx
}

// TestSessionManagerEpochResolution (plan test 14a/14c): a fresh bring-up stamps a
// valid nuid: epoch on $MQTT_sess, resolves it into as.sessStreamEpoch, creates a
// conformant $MQTT_willcancel, and a second getOrCreate reuses the manager and epoch
// without a config update.
func TestSessionManagerEpochResolution(t *testing.T) {
	t.Parallel()
	s, c, nc := newBringUpServer(t)

	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreate: %v", err)
	}
	if !mqttValidSessEpoch(asm.sessStreamEpoch) {
		t.Fatalf("as.sessStreamEpoch = %q, want a valid nuid: epoch", asm.sessStreamEpoch)
	}

	js, ctx := newJS(t, nc)
	si, err := js.Stream(ctx, mqttSessStreamName)
	if err != nil {
		t.Fatalf("session stream must exist: %v", err)
	}
	if got := si.CachedInfo().Config.Metadata[mqttSessEpochMetaKey]; got != asm.sessStreamEpoch {
		t.Fatalf("stream metadata epoch %q != resolved %q", got, asm.sessStreamEpoch)
	}
	if _, err := js.Stream(ctx, mqttTeardownStreamName); err != nil {
		t.Fatalf("will-cancel stream must exist: %v", err)
	}

	// Reuse: a second getOrCreate returns the same manager with the same epoch.
	asm2, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("second getOrCreate: %v", err)
	}
	if asm2 != asm || asm2.sessStreamEpoch != asm.sessStreamEpoch {
		t.Fatalf("expected cached manager and stable epoch")
	}
}

// TestSessionManagerEpochCreateRaceAdoptsWinner (plan test 14b/14c): a manager that
// finds an already-created conformant $MQTT_sess adopts the pre-existing epoch
// verbatim (the create-race-loser path), reuses it without a config update, and
// leaves unrelated stream metadata untouched.
func TestSessionManagerEpochCreateRaceAdoptsWinner(t *testing.T) {
	t.Parallel()
	s, c, nc := newBringUpServer(t)
	js, ctx := newJS(t, nc)

	// A "winner" replica created $MQTT_sess first, with its epoch and an unrelated
	// metadata key the manager must preserve.
	winnerEpoch := mqttGenSessEpoch()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: mqttSessStreamName,
		Subjects: []string{
			mqttSessStreamSubjectPrefix + ">",
			mqttAcceptSubjectPrefix + ">",
		},
		Storage:            jetstream.FileStorage,
		Retention:          jetstream.LimitsPolicy,
		MaxMsgsPerSubject:  1,
		AllowAtomicPublish: true,
		Metadata:           map[string]string{mqttSessEpochMetaKey: winnerEpoch, "unrelated_key": "keepme"},
	}); err != nil {
		t.Fatalf("pre-create winner session stream: %v", err)
	}

	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		t.Fatalf("getOrCreate: %v", err)
	}
	if asm.sessStreamEpoch != winnerEpoch {
		t.Fatalf("manager adopted %q, want the winner's epoch %q", asm.sessStreamEpoch, winnerEpoch)
	}
	si, err := js.Stream(ctx, mqttSessStreamName)
	if err != nil {
		t.Fatalf("stream lookup: %v", err)
	}
	md := si.CachedInfo().Config.Metadata
	if md[mqttSessEpochMetaKey] != winnerEpoch {
		t.Fatalf("epoch metadata changed to %q, want unchanged %q", md[mqttSessEpochMetaKey], winnerEpoch)
	}
	if md["unrelated_key"] != "keepme" {
		t.Fatalf("unrelated metadata not preserved (got %q); the manager must not update the stream config", md["unrelated_key"])
	}
}

// TestSessionManagerEpochConcurrentCreateRace (plan test 14b, deterministic race):
// two managers on the SAME store race to create $MQTT_sess from a clean slate. A
// barrier at the exact lookup->create boundary holds BOTH managers until each has
// seen the stream absent, then releases them so both call createStream — exactly one
// wins (created==true) and the other takes the race-loser path
// (JSStreamNameExistErr). Both must converge on the winner's epoch (the loser adopts
// it from ensureBrokerStream's authoritative re-read rather than its own
// discarded generated value).
func TestSessionManagerEpochConcurrentCreateRace(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)

	// Barrier: block until both managers reach the lookup->create boundary, so
	// neither can create-then-return before the other performs its absent lookup.
	var atGate sync.WaitGroup
	atGate.Add(2)
	release := make(chan struct{})
	gate := func() {
		atGate.Done()
		<-release
	}
	var obsMu sync.Mutex
	var createdCount, existErrCount int
	observe := func(created bool, err error) {
		obsMu.Lock()
		defer obsMu.Unlock()
		if created {
			createdCount++
		}
		if err != nil && IsNatsErr(err, JSStreamNameExistErr) {
			existErrCount++
		}
	}

	newServerOn := func(name string) (*Server, *client) {
		s := &Server{nrc: nc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: name}
		s.running.Store(true)
		s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
		s.mqtt.sessmgr.testSessCreateGate = gate
		s.mqtt.sessmgr.testSessCreateObserve = observe
		t.Cleanup(func() { close(s.quitCh) })
		return s, s.createInternalAccountClient()
	}
	s1, c1 := newServerOn("race1")
	s2, c2 := newServerOn("race2")

	var wg sync.WaitGroup
	var epochs [2]string
	var errs [2]error
	run := func(i int, s *Server, c *client) {
		defer wg.Done()
		asm, err := s.getOrCreateMQTTAccountSessionManager(c)
		if err != nil {
			errs[i] = err
			return
		}
		epochs[i] = asm.sessStreamEpoch
	}
	wg.Add(2)
	go run(0, s1, c1)
	go run(1, s2, c2)

	atGate.Wait()  // both have seen the stream absent, poised to create
	close(release) // release both into createStream simultaneously
	wg.Wait()

	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("both managers must come up: %v / %v", errs[0], errs[1])
	}
	if createdCount != 1 || existErrCount != 1 {
		t.Fatalf("exactly one create must win and one must lose with JSStreamNameExistErr; created=%d existErr=%d", createdCount, existErrCount)
	}
	if !mqttValidSessEpoch(epochs[0]) || epochs[0] != epochs[1] {
		t.Fatalf("race must converge on one valid epoch, got %q and %q", epochs[0], epochs[1])
	}
}

// TestSessionManagerEpochRecreationYieldsDifferentEpoch (plan test 14e): deleting
// and recreating $MQTT_sess through a fresh manager produces a different incarnation
// epoch, so sequences from the old incarnation are never numerically comparable.
func TestSessionManagerEpochRecreationYieldsDifferentEpoch(t *testing.T) {
	t.Parallel()
	s1, c1, nc1 := newBringUpServer(t)
	asm1, err := s1.getOrCreateMQTTAccountSessionManager(c1)
	if err != nil {
		t.Fatalf("first getOrCreate: %v", err)
	}
	epoch1 := asm1.sessStreamEpoch
	if !mqttValidSessEpoch(epoch1) {
		t.Fatalf("epoch1 invalid: %q", epoch1)
	}

	// Recreate on the SAME store (an administrative recreate) and bring up a fresh
	// manager against that same NATS; it must generate a new epoch.
	//
	// The unit, not $MQTT_sess alone: $MQTT_teardown is stamped with the incarnation epoch
	// it was created against and its validator REFUSES a mismatch, so deleting only the
	// session stream is the explicitly unsupported partial recreation
	// (`validateTeardownStream`; asserted from the other side in
	// TestTeardownStreamIsEpochCoupledToTheSessionStream). What this test asserts is
	// unchanged — a fresh incarnation mints a different epoch — and recreating the unit
	// is the documented recovery that reaches it.
	js, ctx := newJS(t, nc1)
	for _, name := range []string{mqttSessStreamName, mqttTeardownStreamName} {
		if err := js.DeleteStream(ctx, name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
	}

	s2 := &Server{nrc: nc1, opts: &Options{}, quitCh: make(chan struct{}), nodeName: "node2"}
	s2.running.Store(true)
	s2.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	c2 := s2.createInternalAccountClient()
	t.Cleanup(func() { close(s2.quitCh) })

	asm2, err := s2.getOrCreateMQTTAccountSessionManager(c2)
	if err != nil {
		t.Fatalf("second getOrCreate: %v", err)
	}
	if !mqttValidSessEpoch(asm2.sessStreamEpoch) {
		t.Fatalf("epoch2 invalid: %q", asm2.sessStreamEpoch)
	}
	if asm2.sessStreamEpoch == epoch1 {
		t.Fatalf("recreated stream reused the old epoch %q; a fresh incarnation must differ", epoch1)
	}
}

// TestSessionManagerEpochMissingFailsInit (plan test 14d): an existing $MQTT_sess
// stream without a valid generated epoch fails manager initialization with an
// actionable recreate error. Covers missing, empty, and malformed metadata.
func TestSessionManagerEpochMissingFailsInit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		meta map[string]string
	}{
		{"missing", nil},
		{"empty", map[string]string{mqttSessEpochMetaKey: ""}},
		{"malformed", map[string]string{mqttSessEpochMetaKey: "not-a-nuid"}},
		{"wrongprefix", map[string]string{mqttSessEpochMetaKey: "abc:0123456789012345678901"}},
		{"badcharset", map[string]string{mqttSessEpochMetaKey: "nuid:!!!!!!!!!!!!!!!!!!!!!!"}}, // 22 chars, out of alphabet
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c, nc := newBringUpServer(t)
			js, ctx := newJS(t, nc)
			if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
				Name:              mqttSessStreamName,
				Subjects:          []string{mqttSessStreamSubjectPrefix + ">"},
				Storage:           jetstream.FileStorage,
				Retention:         jetstream.LimitsPolicy,
				MaxMsgsPerSubject: 1,
				Metadata:          tc.meta,
			}); err != nil {
				t.Fatalf("pre-create bad session stream: %v", err)
			}
			_, err := s.getOrCreateMQTTAccountSessionManager(c)
			if err == nil {
				t.Fatal("expected manager init to fail on invalid session-stream epoch")
			}
			if !strings.Contains(err.Error(), "recreate") {
				t.Fatalf("error must guide recreation, got: %v", err)
			}
		})
	}
}

// TestSessionManagerWillCancelStreamValidation (plan test 15): a pre-existing
// $MQTT_willcancel with an incompatible configuration fails manager initialization
// with recreate guidance rather than being mutated in place.
