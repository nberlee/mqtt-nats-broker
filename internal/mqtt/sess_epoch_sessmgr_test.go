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
	return newBringUpServerOn(t, nc, "node1")
}

// testStopBringUpServer stops a bring-up broker and WAITS for it to be quiescent.
//
// Deresidenting the managers stops new work being handed out; the rest is Shutdown's
// own phase order, and following it exactly is the point.
//
// SIGNALLING is not quiescence. Manager initialization runs the initial expiry, orphan
// and QoS 2 sweeps before those goroutines ever reach their first quit-channel select
// (sess_manager.go), and willClaimLoop runs its initial scan the same way, so a
// signalled-but-unfinished pass is still reading and writing JetStream while a fixture
// deletes streams underneath it.
//
// Nor is grWG alone enough, which is the trap this helper fell into once: the delayed-
// Will claim loop is tracked on willClaimWG, and the fire timers and publisher workers
// on willTimerWG and willWorkerWG. Shutdown drains all three — after closing Will
// dispatch — BEFORE it closes quitCh and waits on grWG, and so does this.
//
// Idempotent, so an explicit stop and the test cleanup can both call it.
func testStopBringUpServer(t *testing.T, s *Server) {
	t.Helper()
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	if v, ok := testBringUpStops.Load(s); ok {
		v.(func())()
	}
}

// testBringUpStops holds each bring-up server's idempotent stop, so a test can stop
// a broker mid-test (the reset contract needs that) without the cleanup then closing
// an already-closed channel.
var testBringUpStops sync.Map // *Server -> func()

// newBringUpServerOn is newBringUpServer against an EXISTING connection, for a
// test that needs a second, independent broker over one store — a restart, or a
// peer.
func newBringUpServerOn(t *testing.T, nc *nats.Conn, node string) (*Server, *client, *nats.Conn) {
	t.Helper()
	s := &Server{
		nrc:      nc,
		opts:     &Options{},
		quitCh:   make(chan struct{}),
		nodeName: node,
		// Initialized because the stop below follows Shutdown's phase order, and
		// closeWillDispatch closes this channel. NewServer does the same; a minimal
		// literal that omits it panics on a nil-channel close the moment a fixture
		// stops a broker properly rather than just dropping it.
		willClaimStopCh: make(chan struct{}),
		willTimers:      make(map[string]*time.Timer),
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	c := s.createInternalAccountClient()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			// Shutdown's own phase order, minus the client phases these fixtures
			// never have: stop Will dispatch and drain BOTH its wait groups, then
			// the publisher workers, and only then close quitCh and drain the
			// background goroutines. willClaimLoop is tracked on willClaimWG, NOT
			// grWG, and it performs its initial JetStream scan before reaching its
			// first quit select — so waiting on grWG alone lets a claimant keep
			// reading the very streams a reset is deleting.
			s.closeWillDispatch()
			s.willTimerWG.Wait()
			s.willClaimWG.Wait()
			s.willWorkerWG.Wait()
			close(s.quitCh)
			s.grWG.Wait()
		})
	}
	testBringUpStops.Store(s, stop)
	t.Cleanup(func() { stop(); testBringUpStops.Delete(s) })
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

	// A "winner" replica got there first and provisioned the WHOLE BUNDLE, which is
	// what a broker that completes initialization leaves behind — and, since the
	// ten streams are one recreation unit, the only shape an arriving manager may
	// admit at all. Pre-creating $MQTT_sess alone (as this fixture used to) is now
	// a proper subset and is refused by the preflight before any of this is
	// reached, so it would no longer test adoption.
	//
	// The unrelated metadata key rides on $MQTT_sess for the same reason as before:
	// the manager must adopt the epoch WITHOUT updating the stream config.
	winnerEpoch := testCreateBundle(t, nc, map[string]func(*StreamConfig){
		mqttSessStreamName: func(cfg *StreamConfig) { cfg.Metadata["unrelated_key"] = "keepme" },
	})

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
		s := testRigServer(t, nc, name)
		s.mqtt.sessmgr.testSessCreateGate = gate
		s.mqtt.sessmgr.testSessCreateObserve = observe
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
	// And the convergence reaches the WHOLE BUNDLE, not just the two managers'
	// in-memory fields: all ten streams must carry that one epoch. Two racing
	// brokers each stamp the nine non-minting streams they create, so a manager
	// that adopted the epoch it ASKED for rather than the one that exists would
	// leave the bundle split — and the two in-memory values above would still
	// agree in the interleavings where the same broker won both.
	asm, aerr := s1.getOrCreateMQTTAccountSessionManager(c1)
	if aerr != nil || asm == nil {
		t.Fatalf("the race left no usable account manager: %v", aerr)
	}
	for _, name := range mqttTestBrokerStreamNames {
		si, err := asm.jsa.lookupStream(name)
		if err != nil || si == nil {
			t.Fatalf("lookup %s after the race: si=%v err=%v", name, si, err)
		}
		if got := si.Config.Metadata[mqttSessEpochMetaKey]; got != epochs[0] {
			t.Fatalf("%s carries recovery epoch %q after the race, want the converged %q",
				name, got, epochs[0])
		}
	}
}

// TestSessionManagerEpochRecreationYieldsDifferentEpoch (plan test 14e): resetting
// the WHOLE ten-stream bundle and bringing up a fresh manager produces a different
// recovery epoch, so sequences from the old incarnation are never numerically
// comparable.
//
// The whole bundle, not $MQTT_sess alone: every broker-owned stream now carries the
// epoch, so deleting a subset is refused by the preflight before anything is minted.
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
	// ALL TEN, not the $MQTT_sess/$MQTT_teardown pair this used to delete: every
	// broker-owned stream now carries the account's recovery epoch, so the pair is
	// a proper subset like any other and the next startup refuses it before minting
	// anything. What this test asserts is unchanged — a fresh bundle mints a
	// different epoch — and the whole-bundle reset is the documented recovery that
	// reaches it.
	// Stopped first, and waited for: the reset is an all-brokers-stopped operation,
	// and a manager whose sweeps are still running would be writing to the streams
	// being deleted.
	testStopBringUpServer(t, s1)
	js, ctx := newJS(t, nc1)
	for _, name := range mqttTestBrokerStreamNames {
		if err := js.DeleteStream(ctx, name); err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
	}

	s2 := testRigServer(t, nc1, "node2")
	c2 := s2.createInternalAccountClient()

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
			// A COMPLETE bundle whose $MQTT_sess carries the bad metadata, and
			// deliberately NOT stamped with the account's epoch — the missing or
			// malformed stamp IS the defect under test, so a helper that stamped it
			// would erase what this fixture exists to catch. The other nine are
			// present so the preflight admits and the VALIDATOR is what refuses.
			testBundleThenPoisonEpoch(t, s, s.createInternalAccountClient(), nc, mqttSessStreamName, tc.meta)
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
