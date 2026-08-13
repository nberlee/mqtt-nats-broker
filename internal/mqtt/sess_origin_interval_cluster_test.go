//go:build mqttport_fulltests

package mqtt

import (
	"sync"
	"testing"
	"time"
)

// The interval register over REAL replicated JetStream.
//
// Everywhere else this feature runs against one embedded stream, where "the create-only
// CAS arbitrates" and "the create-only CAS arbitrates across a Raft group" are the same
// statement. They are not: $MQTT_origin is Replicas=3 here, so the establish that
// decides which of two first-ever creates owns the identity is a per-subject CAS on a
// replicated stream, and the loser's rejection travels.
//
// Per feedback_cluster_test_review_patterns, this suite passing is not the evidence for
// the clustered claim — the mutations are. In particular it does NOT reproduce the
// leader-transition double-apply window, in which both establishes can apply and the two
// brokers hold different sequences; that window is a stated residual in
// docs/CONFORMANCE.md and nothing here claims to cover it.

// TestMQTTOriginFirstEverCreateRaceReplicated is test 14.
//
// Two brokers race the FIRST-EVER create for one client ID, gated so both have read the
// register — and found it absent — before either writes. Exactly one create-only CAS can
// win within a leader term; the loser's rejection is not evidence a register exists, so
// it re-reads and adopts the winner's. Both creates must therefore end up descending from
// ONE identity, which is what makes the pair recognisable instead of two unrelated first
// creates conceding a takeover neither demonstrated.
//
// The assertion names the exact identity the standing register yields, not merely that
// the two agree: agreement alone survives a derivation mutated to any other stable value.
//
// This is the ONLY test that kills the mutation "make the establish a plain publish
// instead of a create-only CAS", and the mutation table says so. The single-broker
// acceptance test was paired with it first and does not discriminate it: that test
// establishes the register exactly once and injects its sibling straight into
// $MQTT_sess, so a plain publish behaves identically there. Two racing absent-register
// establishments are what the mutation is about, and they exist only here.
func TestMQTTOriginFirstEverCreateRaceReplicated(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	a := c.startBroker(t, 0)
	defer a.cleanup()
	b := c.startBroker(t, 1)
	defer b.cleanup()

	c.waitStreamCurrent(t, testGlobalAccount, mqttOriginStreamName)
	c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)

	// Asserted inside this test rather than in one of its own, because every cluster
	// fixture costs a 3-node JetStream and this suite starves above a handful of them.
	// Without these the race below would be a statement about a single-replica stream.
	si, serr := a.s.mqtt.sessmgr.sessions[globalAccountName].jsa.lookupStream(mqttOriginStreamName)
	if serr != nil || si == nil {
		t.Fatalf("lookup %s: %+v %v", mqttOriginStreamName, si, serr)
	}
	if si.Config.Replicas != 3 {
		t.Fatalf("%s has %d replica(s), want 3: the arbitration assertion would prove nothing about HA",
			mqttOriginStreamName, si.Config.Replicas)
	}
	// MaxMsgsPer:1 is what makes the subject a single slot the create-only CAS can
	// arbitrate.
	if si.Config.MaxMsgsPer != 1 {
		t.Fatalf("%s has max_msgs_per_subject %d, want 1", mqttOriginStreamName, si.Config.MaxMsgsPer)
	}

	asmA := a.s.mqtt.sessmgr.sessions[globalAccountName]
	asmB := b.s.mqtt.sessmgr.sessions[globalAccountName]
	if asmA == nil || asmB == nil {
		t.Fatal("both brokers must have an account session manager")
	}
	// Both brokers adopted the epoch from the same authoritative re-read, so the two
	// halves of an identity agree before any of this starts. Without that the race
	// below could "agree" on a sequence and still produce different tokens.
	if asmA.originStreamEpoch != asmB.originStreamEpoch {
		t.Fatalf("both brokers must adopt the SAME register incarnation epoch: %q vs %q",
			asmA.originStreamEpoch, asmB.originStreamEpoch)
	}

	const cid = "origin-race-replicated"
	idHash := getHash(cid)
	if _, _, st := asmA.jsa.loadOriginIntervalRecord(asmA.domainTk, idHash); st != mqttOriginAbsent {
		t.Fatalf("the fixture requires a genuine first-ever interval, got register status %v", st)
	}

	// The barrier: both readers must be past their read before either establishes, or
	// the second one simply finds the first's register and the race never happens.
	//
	// Counted rather than driven by a WaitGroup, because a LOSER re-reads after its
	// rejection: a third arrival must pass straight through instead of decrementing a
	// counter that is already at zero.
	var mu sync.Mutex
	arrivals := 0
	released := make(chan struct{})
	gate := newTestHookGate(t)
	restore := setMQTTTestOriginLoadHook(func(h string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
		// The load itself is the real one; only its RETURN is delayed.
		r, seq, status := asmA.jsa.loadOriginIntervalRecord(asmA.domainTk, h)
		if h != idHash || status != mqttOriginAbsent {
			return r, seq, status
		}
		if !gate.enter() {
			return r, seq, status
		}
		gate.leave()
		mu.Lock()
		arrivals++
		n := arrivals
		mu.Unlock()
		if n > 2 {
			return r, seq, status // a loser's re-read; the barrier is already open
		}
		if n == 2 {
			close(released)
		}
		select {
		case <-released:
		case <-time.After(testMQTTTimeout):
		}
		return r, seq, status
	})
	defer restore()

	// The race is ASSERTED, not inferred from the wall clock: both brokers must have
	// attempted the establish and exactly one must have been rejected. Without this,
	// an establish mutated into a plain publish (where both writes land and the later
	// evicts the earlier) could be masked by a barrier that quietly failed to engage.
	var establishes, rejections int
	restoreStore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
		if subject != mqttOriginSubject(asmA.domainTk, idHash) {
			return asmA.jsa.storeOriginInterval(subject, cas, r)
		}
		if !cas {
			t.Error("the create-side establish must be a CREATE-ONLY CAS: with a plain publish both " +
				"racing creates land and the two brokers hold different identities")
		}
		seq, err := asmA.jsa.storeOriginIntervalRecord(subject, cas, r)
		mu.Lock()
		establishes++
		if err != nil {
			rejections++
		}
		mu.Unlock()
		return seq, err
	})
	defer restoreStore()

	type result struct {
		parent string
		err    error
	}
	results := make(chan result, 2)
	for _, env := range []struct {
		asm  *mqttAccountSessionManager
		opts *Options
	}{{asmA, a.s.getOpts()}, {asmB, b.s.getOpts()}} {
		env := env
		go func() {
			sess, existed, err := env.asm.createOrRestoreSession(cid, env.opts)
			if err != nil {
				results <- result{err: err}
				return
			}
			if existed {
				results <- result{err: errRaceNeedsTwoCreates}
				return
			}
			sess.mu.Lock()
			parent := sess.parentForNextWriteLocked()
			sess.mu.Unlock()
			results <- result{parent: parent}
		}()
	}

	var parents []string
	for i := 0; i < 2; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("create %d: %v", i, res.err)
			}
			parents = append(parents, res.parent)
		case <-time.After(testMQTTTimeout):
			t.Fatalf("create %d never returned; the gate deadlocked", i)
		}
	}

	mu.Lock()
	gotArrivals, gotEstablishes, gotRejections := arrivals, establishes, rejections
	mu.Unlock()
	if gotArrivals < 2 {
		t.Fatalf("only %d broker(s) reached the register read; the barrier never engaged and the "+
			"race was not exercised", gotArrivals)
	}
	if gotEstablishes < 2 {
		t.Fatalf("only %d establish attempt(s); both brokers must have found the register absent "+
			"and tried to create it", gotEstablishes)
	}
	if gotRejections != 1 {
		t.Fatalf("exactly one of the two establishes must be REJECTED within a leader term, got %d "+
			"rejection(s) out of %d attempts", gotRejections, gotEstablishes)
	}

	_, seq, status := asmA.jsa.loadOriginIntervalRecord(asmA.domainTk, idHash)
	if status != mqttOriginValid {
		t.Fatalf("the race must leave a valid register standing, got status %v", status)
	}
	want := mqttOriginToken(asmA.originStreamEpoch, seq)
	if want == _EMPTY_ {
		t.Fatalf("the standing register at sequence %v yielded no identity", seq)
	}
	for i, got := range parents {
		if got != want {
			t.Errorf("create %d descends from %q, want the ONE interval identity %q — two unrelated "+
				"first creates that disagree concede a takeover neither demonstrated, and the erased "+
				"side's delayed Will is cancelled", i, got, want)
		}
	}
}

// errRaceNeedsTwoCreates reports a fixture that restored instead of creating, which
// would make the race assertion vacuous.
var errRaceNeedsTwoCreates = errFixture("the race requires two CREATES; one of them restored a record")

type errFixture string

func (e errFixture) Error() string { return string(e) }
