//go:build mqtt_fulltests

// End-to-end coverage for the orphan QoS 2 sweep, against real JetStream state
// produced by real MQTT exchanges rather than hand-written records.
//
// The unit tables in orphan_qos2_sweep_test.go pin the dispositions and the
// orderings. What these add is the property those tables cannot establish on their
// own: that the state the sweep reconciles is the state a live broker actually
// creates, and that reconciling one session's leak leaves every other session's
// working state untouched.

package mqtt

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testOrphanSetGraces scopes both sweep age policies to one account manager. The
// package-level knobs cannot be used: the manager's own sweep goroutine reads them.
func testOrphanSetGraces(t *testing.T, asm *mqttAccountSessionManager, hold, release time.Duration) {
	t.Helper()
	asm.mu.Lock()
	asm.testOrphanGrace, asm.testQoS2ReleaseGrace = &hold, &release
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testOrphanGrace, asm.testQoS2ReleaseGrace = nil, nil
		asm.mu.Unlock()
	})
}

// testOrphanDropSession removes a client's session record and its in-memory
// session, which is exactly the state a broker that died mid-teardown leaves
// behind: durable QoS 2 state on a generation nothing references any more.
func testOrphanDropSession(t *testing.T, asm *mqttAccountSessionManager, idHash string) {
	t.Helper()
	// Both indexes: sessions is keyed by client ID, sessByHash by the hash.
	asm.mu.Lock()
	for id, ss := range asm.sessions {
		if ss.idHash == idHash {
			delete(asm.sessions, id)
		}
	}
	delete(asm.sessByHash, idHash)
	asm.mu.Unlock()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("loading the session record of %q: %v", idHash, err)
		}
		return
	}
	// RECORD THE RETIREMENT before deleting the record, which is the order a real
	// teardown uses: recordGenRetirement writes the watermark first, and a write it
	// cannot confirm aborts the teardown with nothing deleted.
	//
	// Since F-036 unit 2 this is not optional dressing — it is the LICENCE every
	// destructive sweep arm now requires. Deleting the record alone establishes only
	// that nothing this broker can see names the generation, which a session live on
	// ANOTHER broker with no record satisfies equally, so an unlicensed pass RETAINS.
	// These tests are all about a session that is genuinely gone, so the fixture records
	// the fact that makes it so. The RETENTION half — absence with no watermark — is
	// asserted by TestF036OrphanSweepRetainsRemoteLiveGenerationWithoutMatchingRetirement
	// and its siblings.
	if ps := mqttDecodeSessionRecord(smsg); ps != nil && ps.ID != _EMPTY_ && ps.Gen != _EMPTY_ {
		wmSubject := mqttTeardownSubject(asm.domainTk, idHash)
		_, wmSeq, status := asm.jsa.loadGenRetirement(asm.domainTk, idHash)
		var expect uint64
		if status != mqttRetirementAbsent {
			expect = wmSeq
		}
		r := &mqttGenRetirement{
			ID: ps.ID, Gen: ps.Gen, MarkerSeq: smsg.Sequence, Nonce: nuid.Next(), Origin: "orphan-e2e",
		}
		if _, werr := asm.jsa.storeGenRetirementCAS(wmSubject, expect, r); werr != nil {
			t.Fatalf("recording the retirement watermark for %q: %v", idHash, werr)
		}
	}
	// A concurrent teardown may already have removed it; that is the same end state.
	if err := asm.jsa.deleteMsg(mqttSessStreamName, smsg.Sequence, true); err != nil &&
		isErrorOtherThan(err, JSSequenceNotFoundErrF) &&
		!isJSErrCausedBy(err, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
		t.Fatalf("deleting the session record of %q: %v", idHash, err)
	}
}

// testOrphanSettle runs the full sweep enough times for quarantine to qualify a
// stable candidate. Eligibility deliberately requires a second authoritative pass
// over an unchanged identity, and a marker the sweep itself creates then needs its
// own observations, so convergence takes several passes rather than one.
func testOrphanSettle(asm *mqttAccountSessionManager, s *Server) {
	for i := 0; i < 6; i++ {
		asm.sweepOrphanedQoS2State(s)
	}
}

func testOrphanHoldPresent(t *testing.T, asm *mqttAccountSessionManager, subject string) bool {
	t.Helper()
	rec, _, err := asm.jsa.loadQoS2Record(subject)
	if err != nil {
		t.Fatalf("loading the hold record on %q: %v", subject, err)
	}
	return rec != nil
}

// TestOrphanSweepRetiresRetiredGenerationAndSparesLive is the corrected form of
// what the roadmap originally described as a wedged packet identifier.
//
// Since incarnation generations, a lost session record cannot wedge an identifier
// at all: hold subjects carry the generation, and the next CONNECT mints a new one.
// The real symptom is UNBOUNDED ACCUMULATION, so that is what this asserts — and it
// asserts it beside a concurrently live generation, because the whole risk of a
// sweep is that it reaches state somebody still owns. Matching on the client-ID hash
// instead of the (hash, generation) pair fails this test.
func TestOrphanSweepRetiresRetiredGenerationAndSparesLive(t *testing.T) {
	t.Parallel()
	b := testQoS2Setup(t, "orphan-e2e")
	asm := b.asm
	testOrphanSetGraces(t, asm, 0, 0)

	// A real inbound QoS 2 exchange, left held at PUBREC: the exact state a client
	// that never sends PUBREL leaves behind.
	const retiredPI = uint16(41)
	retiredHold := b.holdSubject(t, retiredPI)
	testMQTTPublishV5(t, b.cp, 2, retiredPI, b.topic, []byte("retired-payload"))
	testMQTTReadPubRecV5(t, b.rp, retiredPI)
	if !testOrphanHoldPresent(t, asm, retiredHold) {
		t.Fatal("the PUBREC'd exchange left no hold record to reconcile")
	}
	retiredHash := testMQTTGetClient(t, b.s, b.pubID).mqtt.sess.idHash

	// A second client, still very much alive, with a held exchange of its own.
	const livePI = uint16(42)
	liveID := "orphan-e2e-live"
	cl, rl := testMQTTConnectV5(t, &mqttV5ConnInfo{
		clientID:   liveID,
		cleanStart: false,
		props:      mqttV5ConnPropsSessionExpiry(300),
	}, b.o.MQTT.Host, b.o.MQTT.Port)
	defer cl.Close()
	testMQTTReadConnAckV5(t, rl)
	liveHold := testMQTTGetClient(t, b.s, liveID).mqttQoS2InternalSubject(livePI)
	testMQTTPublishV5(t, cl, 2, livePI, b.topic, []byte("live-payload"))
	testMQTTReadPubRecV5(t, rl, livePI)
	if !testOrphanHoldPresent(t, asm, liveHold) {
		t.Fatal("the live client's exchange left no hold record")
	}

	// The first client's broker dies without tearing anything down. The connection is
	// deliberately left open: closing it runs the disconnect path, which re-persists
	// the record and would make the generation legitimately live again.
	testOrphanDropSession(t, asm, retiredHash)

	// Sweep until the payload goes, then check what was standing in front of it at
	// that moment: the hold must never be destroyed without a fence already in place.
	fenced := false
	for i := 0; i < 6 && testOrphanHoldPresent(t, asm, retiredHold); i++ {
		asm.sweepOrphanedQoS2State(b.s)
		claim, _, err := asm.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(retiredHold))
		if err != nil {
			t.Fatalf("loading the release claim: %v", err)
		}
		if claim != nil && claim.State == mqttQoS2RelStateDiscarded {
			fenced = true
		}
		if !testOrphanHoldPresent(t, asm, retiredHold) && !fenced {
			t.Fatal("the hold was destroyed without a discard marker fencing it first")
		}
	}
	if testOrphanHoldPresent(t, asm, retiredHold) {
		t.Fatal("the retired generation's hold record was not reconciled")
	}
	if !fenced {
		t.Fatal("no discard marker was ever observed fencing the hold")
	}
	// And the marker itself is retired once it has been stably observed in turn.
	testOrphanSettle(asm, b.s)
	if _, smsg, err := asm.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(retiredHold)); err != nil || smsg != nil {
		t.Fatalf("the aged discard marker was never retired (smsg=%v err=%v)", smsg, err)
	}

	// And the live generation is untouched, so its client can still complete.
	if !testOrphanHoldPresent(t, asm, liveHold) {
		t.Fatal("the sweep destroyed a live session generation's inbound QoS 2 state")
	}
	testQoS2SendPubRel(t, cl, livePI)
	if reason, _ := testMQTTReadPubCompV5(t, rl, livePI); reason != 0 {
		t.Fatalf("the live exchange completed with reason 0x%x, want success", reason)
	}
	b.consumeOne(t, "live-payload")
}

// TestOrphanSweepSharedSourceHonorsDiscardedAssignment pins that reconciling an
// orphan winner's assignment does NOT hand its started delivery to another member.
//
// [MQTT-4.8.2-5] permits the message to die with the session it was assigned to and
// forbids redistributing it. The claim is therefore marked Discarded and kept for
// as long as the source could still be re-offered — never deleted in a way that
// would let a redelivery elect a fresh winner.
func TestOrphanSweepSharedSourceHonorsDiscardedAssignment(t *testing.T) {
	testRequireExperimentalSharedQoS2(t)

	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	b := c.startBroker(t, 0)
	asm := testMQTTGetGlobalASM(t, b.s)

	const group, topic = "orphang", "orphan/shared"
	cw, rw := testQ2AffClusterMember(t, b, "orphan-winner", group, topic)
	co, ro := testQ2AffClusterMember(t, b, "orphan-other", group, topic)
	defer co.Close()

	testQ2AffClusterPublish(t, b, "orphan-shared-pub", topic, "affine-payload")

	// Whichever member the group elected is the one we retire.
	const probe = 5 * time.Second
	winnerID, winnerConn, otherReader := "orphan-winner", cw, ro
	if _, payload, qos, _, ok := testMQTTTryReadPublishV5(rw, probe); !ok || qos != 2 || string(payload) != "affine-payload" {
		if _, payload, qos, _, ok := testMQTTTryReadPublishV5(ro, probe); !ok || qos != 2 || string(payload) != "affine-payload" {
			t.Fatalf("neither shared member received the QoS 2 delivery\n%s", c.describe())
		}
		winnerID, winnerConn, otherReader = "orphan-other", co, rw
	}

	claims := testQ2AffClusterClaims(t, b)
	if len(claims) != 1 {
		t.Fatalf("expected exactly one assignment claim, got %d\n%s", len(claims), c.describe())
	}
	var claimSubject string
	for subject := range claims {
		claimSubject = subject
	}

	// The winner's broker dies mid-exchange: its session simply ceases to exist. The
	// connection is deliberately left open, because closing it runs the disconnect
	// path, which re-persists the record and makes the generation legitimately live.
	defer winnerConn.Close()
	winnerHash := testMQTTGetClient(t, b.s, winnerID).mqtt.sess.idHash
	testOrphanDropSession(t, asm, winnerHash)
	testOrphanSetGraces(t, asm, 0, 0)

	testOrphanSettle(asm, b.s)

	// The assignment is either still there carrying Discarded, or reaped because
	// both sides were provably complete. What must never happen is the started
	// delivery being offered to the other member.
	if cur, _, err := asm.loadQ2AffClaim(claimSubject); err != nil {
		t.Fatalf("loading the assignment claim: %v", err)
	} else if cur != nil && !cur.Discarded {
		t.Fatalf("the orphan winner's assignment was left undiscarded: %+v", cur)
	}
	if _, payload, _, _, ok := testMQTTTryReadPublishV5(otherReader, 3*time.Second); ok {
		t.Fatalf("the started delivery was redistributed to a second member (payload %q), "+
			"which [MQTT-4.8.2-5] forbids\n%s", payload, c.describe())
	}
}

// TestOrphanSweepAgesAReleaseClaimUnderAPausedReleaser is the race the whole
// hold-before-fence ordering exists for, run across two real brokers.
//
// Broker B parks a releaser inside its load..claim window — a broker paused holding
// a payload it is about to deliver. Broker A's sweep then ages and reconciles the
// pair underneath it. When B resumes it wins a fresh claim against the now-empty
// subject, but its mandatory authoritative re-read must return Gone, so the witness
// subscriber sees nothing. If the sweep ever retired the fence before the payload,
// B would read Held and deliver from a session that no longer exists.
func TestOrphanSweepAgesAReleaseClaimUnderAPausedReleaser(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)
	asmA, asmB := testMQTTGetGlobalASM(t, bA.s), testMQTTGetGlobalASM(t, bB.s)
	testOrphanSetGraces(t, asmA, 0, 0)

	const topic = "orphan/paused"
	cs, rs := testQoS2ClusterSub(t, bA, "paused-witness", topic)
	defer cs.Close()

	cp, rp := testQoS2ClusterPub(t, bB, "paused-pub")
	defer cp.Close()
	pubHash := testMQTTGetClient(t, bB.s, "paused-pub").mqtt.sess.idHash

	// Park the releaser exactly where a real race lands: it has loaded the hold and
	// has not yet claimed. While it waits, the other broker reconciles the pair.
	released := make(chan struct{})
	var once sync.Once
	asmB.mu.Lock()
	asmB.testQoS2PreClaim = func(string, string, uint64) error {
		once.Do(func() {
			testOrphanDropSession(t, asmA, pubHash)
			testOrphanSettle(asmA, bA.s)
			close(released)
		})
		return nil
	}
	asmB.mu.Unlock()
	t.Cleanup(func() {
		asmB.mu.Lock()
		asmB.testQoS2PreClaim = nil
		asmB.mu.Unlock()
	})

	const pi = uint16(51)
	testMQTTPublishV5(t, cp, 2, pi, topic, []byte("paused-payload"))
	testMQTTReadPubRecV5(t, rp, pi)
	testQoS2SendPubRel(t, cp, pi)

	select {
	case <-released:
	case <-time.After(testMQTTTimeout):
		t.Fatal("the pre-claim seam never fired: the race this test exists for was not exercised")
	}

	// Whatever reason code B settles on, the message must not reach the wire.
	testMQTTExpectNothingFor(t, rs, 2*time.Second)

	checkFor(t, testMQTTTimeout, 200*time.Millisecond, func() error {
		holdSubject := mqttQoS2HoldSubject(pubHash, testMQTTGetClient(t, bB.s, "paused-pub").mqtt.sess.gen, pi)
		if testOrphanHoldPresent(t, asmA, holdSubject) {
			return fmt.Errorf("the orphaned hold survived reconciliation\n%s", c.describe())
		}
		return nil
	})
}

// TestOrphanSweepAdoptsATeardownWhileItsOwnerResumes pins that adoption and a
// returning client cannot both win.
//
// Broker A marks a teardown and stops. Broker B's sweep adopts it at the same
// moment the client reconnects through A. The teardown release is a
// sequence-targeted delete classified by re-reading, so exactly one of them
// completes the retirement — and the fresh session that follows must keep every
// resource it creates.
func TestOrphanSweepAdoptsATeardownWhileItsOwnerResumes(t *testing.T) {
	oldTimeout := testMQTTTimeout
	testMQTTTimeout = 40 * time.Second
	defer func() { testMQTTTimeout = oldTimeout }()

	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)
	asmA, asmB := testMQTTGetGlobalASM(t, bA.s), testMQTTGetGlobalASM(t, bB.s)
	testOrphanSetGraces(t, asmB, 0, 0)

	const clientID, topic = "adopt-race", "orphan/adopt"
	cp, rp := testQoS2ClusterPub(t, bA, clientID)
	idHash := testMQTTGetClient(t, bA.s, clientID).mqtt.sess.idHash

	// A broker that claimed the teardown and died.
	ps, _, err := asmA.loadFenceSession(idHash)
	if err != nil || ps == nil {
		t.Fatalf("loading the session record: ps=%v err=%v", ps, err)
	}
	ps.Teardown = &mqttSessTeardown{Origin: "deadnode", Nonce: nuid.Next(), StartedAt: time.Now().Unix()}
	// The marker's LINEAGE, as claimTeardown stamps it: its own identity, parented to the
	// record it claimed. The finisher retires that parent by name.
	ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
	raw, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshalling the marked record: %v", err)
	}
	if _, err := asmA.jsa.storeMsg(mqttSessStreamSubjectPrefix+asmA.domainTk+idHash, 0, raw); err != nil {
		t.Fatalf("writing the teardown marker: %v", err)
	}
	cp.Close()
	testOrphanDropLocalSession(asmA, idHash)

	// Adoption on B races the client's return through A.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		asmB.sweepOrphanedQoS2State(bB.s)
	}()
	cp2, rp2 := testQoS2ClusterPub(t, bA, clientID)
	defer cp2.Close()
	wg.Wait()
	_ = rp

	// Nobody resumes a marked record, so the reconnect starts a fresh incarnation —
	// and that incarnation's own exchange must complete normally, whichever side
	// finished the teardown.
	const pi = uint16(61)
	testMQTTPublishV5(t, cp2, 2, pi, topic, []byte("after-adoption"))
	testMQTTReadPubRecV5(t, rp2, pi)

	// Further sweeps on B must not touch the live incarnation.
	testOrphanSettle(asmB, bB.s)
	fresh := testMQTTGetClient(t, bA.s, clientID).mqtt.sess
	fresh.mu.Lock()
	freshGen := fresh.gen
	fresh.mu.Unlock()
	holdSubject := mqttQoS2HoldSubject(idHash, freshGen, pi)
	if !testOrphanHoldPresent(t, asmA, holdSubject) {
		t.Fatalf("the sweep destroyed the resumed session's live inbound QoS 2 state\n%s", c.describe())
	}
	testQoS2SendPubRel(t, cp2, pi)
	if reason, _ := testMQTTReadPubCompV5(t, rp2, pi); reason != 0 {
		t.Fatalf("the fresh incarnation's exchange completed with reason 0x%x, want success", reason)
	}
}

// testOrphanDropLocalSession removes a session from both in-memory indexes without
// touching its record, which is what a broker crash looks like to its peers.
func testOrphanDropLocalSession(asm *mqttAccountSessionManager, idHash string) {
	asm.mu.Lock()
	for id, ss := range asm.sessions {
		if ss.idHash == idHash {
			delete(asm.sessions, id)
		}
	}
	delete(asm.sessByHash, idHash)
	asm.mu.Unlock()
}

// TestOrphanSweepTwoSweepersConverge pins that the sweep is safe to run from every
// broker at once, which it necessarily is in a real deployment: each account
// manager runs its own.
//
// Every mutation is a CAS or an exact-sequence delete, so the loser of any race
// does nothing rather than something wrong. What must hold at the end is that the
// orphan is gone, nothing live was touched, and neither sweeper reported a state it
// could not reconcile.
func TestOrphanSweepTwoSweepersConverge(t *testing.T) {
	t.Parallel()
	c := testNewJetStreamCluster(t, 3)
	bA := c.startBroker(t, 0)
	bB := c.startBroker(t, 1)
	asmA, asmB := testMQTTGetGlobalASM(t, bA.s), testMQTTGetGlobalASM(t, bB.s)
	testOrphanSetGraces(t, asmA, 0, 0)
	testOrphanSetGraces(t, asmB, 0, 0)

	// An orphan hold on a generation no record names, written directly so both
	// brokers see identical durable state and neither owns it in memory.
	idHash, gen := getHash("two-sweepers"), mqttNewSessGen()
	holdSubject := mqttQoS2HoldSubject(idHash, gen, 9)
	pp := &mqttPublish{
		topic: []byte("orphan/two"), subject: []byte("orphan.two"),
		msg: []byte("two-sweepers"), sz: len("two-sweepers"), pi: 9, flags: 2 << 1,
	}
	msg, hdrLen := mqttNewDeliverableMessage(pp, true, 0, _EMPTY_, false, _EMPTY_, nuid.Next())
	if _, err := asmA.storeQoS2HoldCAS(holdSubject, 0, hdrLen, msg); err != nil {
		t.Fatalf("storing the orphan hold: %v", err)
	}
	// The RECLAMATION LICENCE for that generation (F-036 unit 2). This fixture writes
	// its durable state directly and never had a record to delete, so nothing else
	// supplies the retirement fact — and without it BOTH sweepers correctly retain, which
	// would make this a test about the leak rather than about convergence.
	if _, err := asmA.jsa.storeGenRetirementCAS(mqttTeardownSubject(asmA.domainTk, idHash), 0,
		&mqttGenRetirement{
			ID: "two-sweepers", Gen: gen, MarkerSeq: 1, Nonce: nuid.Next(), Origin: "orphan-e2e",
		}); err != nil {
		t.Fatalf("recording the retirement watermark: %v", err)
	}

	// Both sweep concurrently, repeatedly: the first pass fences and purges, a later
	// one retires the marker, and the interleaving is whatever the cluster gives us.
	done := make(chan struct{}, 2)
	for _, p := range []struct {
		asm *mqttAccountSessionManager
		s   *Server
	}{{asmA, bA.s}, {asmB, bB.s}} {
		go func() {
			defer func() { done <- struct{}{} }()
			testOrphanSettle(p.asm, p.s)
		}()
	}
	<-done
	<-done

	checkFor(t, testMQTTTimeout, 200*time.Millisecond, func() error {
		if testOrphanHoldPresent(t, asmA, holdSubject) {
			return fmt.Errorf("the orphan hold survived concurrent sweeps\n%s", c.describe())
		}
		_, smsg, err := asmA.jsa.loadQoS2RelClaim(mqttQoS2ClaimSubjectFor(holdSubject))
		if err != nil {
			return fmt.Errorf("loading the release claim: %w", err)
		}
		if smsg != nil {
			return fmt.Errorf("the discard marker survived concurrent sweeps\n%s", c.describe())
		}
		return nil
	})
}
