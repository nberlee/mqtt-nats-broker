package mqtt

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Tests for the durable generation-retirement watermark and its four readers: the
// winner's release-superseded arm, the CONNECT restore, the orphan sweep, and the
// record-absent create that inherits the retired record's identity. The
// window they close is the claim-won deletion window: a teardown deletes on the
// strength of a won claim, and a cross-term resume derived from the same base can
// evict the marker off the record's own subject afterwards, leaving a record that
// names resources the winner destroyed and no marker for anyone to adopt.
//
// The cross-term apply cannot be forced for real — no seam holds a JetStream
// proposal between admission and apply — so the shape is STAGED: the winner is
// parked between its claim and its deletions (mqttTestTeardownClaimedHookV) and a
// same-base record is written over the marker from the other broker, which is what
// such a resume leaves behind.

// testLoadRetirement returns a client's retirement watermark, its sequence and its
// classification, read through the real load path.
func testLoadRetirement(t *testing.T, env *testTeardownEnv, idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
	t.Helper()
	return env.asm.jsa.loadGenRetirementRecord(env.asm.domainTk, idHash)
}

// testPlantRetirement writes a watermark directly, for the readers' fixtures. The
// WRITER's own tests never use it: a planted fence would pass its assertions even if
// nothing in production ever wrote one.
func testPlantRetirement(t *testing.T, env *testTeardownEnv, idHash string, r *mqttGenRetirement, expectSeq uint64) uint64 {
	t.Helper()
	seq, err := env.asm.jsa.storeGenRetirementCAS(mqttTeardownSubject(env.asm.domainTk, idHash), expectSeq, r)
	if err != nil {
		t.Fatalf("planting the retirement watermark: %v", err)
	}
	return seq
}

// testPlantResumeOver writes ps over whatever occupies the client's record subject,
// CASing at atSeq — the shape a cross-term resume leaves: a record derived from the
// same base, applied after the marker landed, which MaxMsgsPer:1 evicts the marker
// for.
func testPlantResumeOver(t *testing.T, env *testTeardownEnv, idHash string, ps *mqttPersistedSession, atSeq uint64) uint64 {
	t.Helper()
	ps.WID, ps.PWID = mqttNewRecordWID(), ps.WID
	seq, err := mqttStoreSessionRecordCAS(&env.asm.jsa, env.asm.domainTk, ps, idHash, atSeq)
	if err != nil {
		t.Fatalf("planting the surviving record: %v", err)
	}
	return seq
}

// testSetOrphanGrace scopes the sweep's construction grace to THIS manager, which is
// what lets a test drive the retirement-condemnation class pass by pass.
func testSetOrphanGrace(t *testing.T, env *testTeardownEnv, d time.Duration) {
	t.Helper()
	env.asm.mu.Lock()
	env.asm.testOrphanGrace = &d
	env.asm.mu.Unlock()
	t.Cleanup(func() {
		env.asm.mu.Lock()
		env.asm.testOrphanGrace = nil
		env.asm.mu.Unlock()
	})
}

// TestRetirementCondemnsTheHollowSurvivor is test 1: the winner itself repairs the
// window. Its release comes back superseded by a record carrying the generation it
// just retired — resources it has already deleted — so it condemns that record with a
// fresh teardown marker instead of only reporting the outcome.
func TestRetirementCondemnsTheHollowSurvivor(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "hollow-survivor"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	retiredGen := sessA.gen
	base, _ := testLoadSessRecord(t, a, idHash)

	var stagedMarkerSeq uint64
	var ownNonce string
	gate := newTestHookGate(t)
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, nonce string, markerSeq uint64) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if sess != sessA {
			return
		}
		stagedMarkerSeq, ownNonce = markerSeq, nonce
		// The cross-term resume: B's save was derived from the same base and applies
		// now, evicting A's marker. It preserves the generation, so it names exactly
		// what A is about to delete.
		survivor := *base
		survivor.Teardown = nil
		testPlantResumeOver(t, b, idHash, &survivor, markerSeq)
	})
	defer restore()

	err := sessA.clear(false)
	if !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("clear() = %v, want the superseded sentinel", err)
	}
	if stagedMarkerSeq == 0 {
		t.Fatal("the staging hook never fired")
	}

	// The fence is durable and names the retired generation at the marker's sequence.
	r, _, status := testLoadRetirement(t, a, idHash)
	if status != mqttRetirementValid {
		t.Fatalf("retirement watermark status = %v, want valid: the deletions ran with nothing recording them", status)
	}
	if r.Gen != retiredGen {
		t.Errorf("watermark names generation %q, want the retired %q", r.Gen, retiredGen)
	}
	if r.MarkerSeq != stagedMarkerSeq {
		t.Errorf("watermark MarkerSeq = %v, want the claim marker's %v", r.MarkerSeq, stagedMarkerSeq)
	}

	// The hollow survivor is condemned: marked, content preserved, a DIFFERENT nonce
	// (this is a fresh claim on the survivor, not the evicted marker reappearing).
	ps, _ := testLoadSessRecord(t, a, idHash)
	if ps == nil {
		t.Fatal("the surviving record was deleted; condemnation is mark-only")
	}
	if ps.Teardown == nil {
		t.Fatal("the surviving record was left unmarked: it names resources the teardown deleted, " +
			"and nothing else would ever revisit it")
	}
	if ps.Teardown.Nonce == ownNonce {
		t.Error("the condemnation must be a fresh claim, not the evicted marker")
	}
	if ps.Gen != retiredGen {
		t.Errorf("the condemnation must preserve the record's content, got generation %q want %q", ps.Gen, retiredGen)
	}

	// And the marked record converges: the next CONNECT finishes it and starts fresh.
	fresh, existed, cerr := b.asm.createOrRestoreSession(clientID, b.s.getOpts())
	if cerr != nil {
		t.Fatalf("CONNECT over the condemned record: %v", cerr)
	}
	if existed {
		t.Error("a condemned record must never be resumed: Session Present must be false")
	}
	if fresh.gen == retiredGen {
		t.Error("the fresh session must not reuse the retired generation")
	}
	if got, _ := testLoadSessRecord(t, a, idHash); got != nil && got.Teardown != nil {
		t.Error("the condemnation marker was never finished")
	}
}

// TestRetirementLeavesAFreshSuccessorAlone is test 2, the discriminating pair for the
// generation comparison in condemnRetiredSurvivor: a record that superseded the
// release with a DIFFERENT generation is a legitimate successor. Condemning it would
// be collateral damage, not repair.
func TestRetirementLeavesAFreshSuccessorAlone(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "fresh-successor"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	retiredGen := sessA.gen
	base, _ := testLoadSessRecord(t, a, idHash)
	successorGen := mqttNewSessGen()

	gate := newTestHookGate(t)
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, nonce string, markerSeq uint64) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if sess != sessA {
			return
		}
		// A peer finished this teardown and started a fresh session, which saved its
		// own record. Nothing of the retired incarnation is named by it.
		successor := &mqttPersistedSession{
			ID:   base.ID,
			Gen:  successorGen,
			Subs: base.Subs,
		}
		testPlantResumeOver(t, b, idHash, successor, markerSeq)
	})
	defer restore()

	if err := sessA.clear(false); !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("clear() = %v, want the superseded sentinel", err)
	}

	ps, _ := testLoadSessRecord(t, a, idHash)
	if ps == nil {
		t.Fatal("the successor's record was deleted")
	}
	if ps.Gen != successorGen {
		t.Fatalf("the successor's record was overwritten: generation %q, want %q", ps.Gen, successorGen)
	}
	if ps.Teardown != nil {
		t.Fatal("a fresh-generation successor was condemned: the watermark retires one generation, " +
			"not a client ID")
	}

	// It resumes, which is the client-visible half of the same claim.
	sess, existed, cerr := b.asm.createOrRestoreSession(clientID, b.s.getOpts())
	if cerr != nil {
		t.Fatalf("CONNECT over the successor: %v", cerr)
	}
	if !existed || sess.gen != successorGen {
		t.Errorf("the successor must resume: existed=%v gen=%q want true/%q", existed, sess.gen, successorGen)
	}
	if r, _, status := testLoadRetirement(t, a, idHash); status != mqttRetirementValid || r.Gen != retiredGen {
		t.Errorf("the watermark must still name the retired generation, got status=%v r=%+v", status, r)
	}
}

// TestRetirementRefusesToResumeARetiredGeneration is test 3, reader 2: an unmarked
// record whose generation the watermark says was retired is discarded at CONNECT
// rather than resumed. Without it, a winner that crashed after recording the
// retirement leaves a record that every reconnect resumes hollow — answering Session
// Present 1 for consumers, holds and registers that are gone.
func TestRetirementRefusesToResumeARetiredGeneration(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "retired-connect"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	retiredGen := sessA.gen
	_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)
	_, recSeq := testLoadSessRecord(t, a, idHash)

	// The state a winner that crashed after its watermark leaves: the fence stands,
	// the record does not carry a marker.
	testPlantRetirement(t, a, idHash, &mqttGenRetirement{
		ID: clientID, Gen: retiredGen, MarkerSeq: recSeq, RWID: mqttNewRecordWID(),
		Nonce: "planted", Origin: "peer",
	}, 0)

	sess, existed, err := a.asm.createOrRestoreSession(clientID, a.s.getOpts())
	if err != nil {
		t.Fatalf("CONNECT over a retired record: %v", err)
	}
	if existed {
		t.Fatal("a retired generation must not be resumed: Session Present must be false")
	}
	if sess.gen == retiredGen {
		t.Error("the fresh session reused the retired generation")
	}
	if sess.seq != 0 {
		t.Errorf("the fresh session must be at base 0, got %v", sess.seq)
	}
	if ps, _ := testLoadSessRecord(t, a, idHash); ps != nil {
		t.Errorf("the retired record must be released by the discard, got %+v", ps)
	}
	if testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
		t.Error("the discard must delete the retired incarnation's PUBREL consumer")
	}
	// The fence is permanent, and the discard's own claim advanced it rather than
	// replacing what it names.
	r, _, status := testLoadRetirement(t, a, idHash)
	if status != mqttRetirementValid || r.Gen != retiredGen {
		t.Fatalf("the watermark must still stand and name %q, got status=%v r=%+v", retiredGen, status, r)
	}
	if r.MarkerSeq < recSeq {
		t.Errorf("the watermark regressed: MarkerSeq %v below the planted %v", r.MarkerSeq, recSeq)
	}
}

// TestRetirementWatermarkIsPermanentAndMonotonic is test 5. The fence outlives the
// session it retired, and replacement is ordered by the claim marker's sequence
// ALONE — never last-writer-wins, and never short-circuited by the generation, which
// would let a paused older writer become the final watermark.
func TestRetirementWatermarkIsPermanentAndMonotonic(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "watermark-monotonic"

	// (a) A full lifecycle: a session is torn down, a successor is created and torn
	// down too. The fence survives both and names the NEWEST retirement.
	first := testProvisionSession(t, a, clientID)
	idHash, firstGen := first.idHash, first.gen
	firstRec, _ := testLoadSessRecord(t, a, idHash)
	if firstRec == nil || firstRec.WID == _EMPTY_ {
		t.Fatal("the first record must carry an identity to retire")
	}
	if err := first.clear(false); err != nil {
		t.Fatalf("first teardown: %v", err)
	}
	r1, _, status := testLoadRetirement(t, a, idHash)
	if status != mqttRetirementValid || r1.Gen != firstGen {
		t.Fatalf("after the first teardown: status=%v r=%+v want a valid fence naming %q", status, r1, firstGen)
	}
	if r1.RWID != firstRec.WID {
		t.Fatalf("the fence must name the record THIS teardown removed: RWID=%q want %q", r1.RWID, firstRec.WID)
	}

	second := testProvisionSession(t, a, clientID)
	secondGen := second.gen
	if secondGen == firstGen {
		t.Fatal("the successor must have its own generation")
	}
	secondRec, _ := testLoadSessRecord(t, a, idHash)
	if secondRec == nil || secondRec.WID == firstRec.WID {
		t.Fatalf("the successor's record must be a different one: %+v", secondRec)
	}
	if err := second.clear(false); err != nil {
		t.Fatalf("second teardown: %v", err)
	}
	r2, _, status := testLoadRetirement(t, a, idHash)
	if status != mqttRetirementValid || r2.Gen != secondGen {
		t.Fatalf("the fence must name the newest retirement: status=%v r=%+v want %q", status, r2, secondGen)
	}
	if r2.MarkerSeq <= r1.MarkerSeq {
		t.Fatalf("the newer retirement must carry the higher marker sequence: %v vs %v", r2.MarkerSeq, r1.MarkerSeq)
	}
	// The retired record's IDENTITY advances with the fence, and that is the difference
	// between arbitration and a deduplicated first value: an implementation that advanced
	// mseq while keeping the earlier RWID would leave the next create inheriting an older
	// interval's record and manufacturing a false fork against the latest retired one.
	if r2.RWID != secondRec.WID {
		t.Fatalf("the advanced fence must name the SECOND retired record: RWID=%q want %q "+
			"(the first was %q)", r2.RWID, secondRec.WID, firstRec.WID)
	}
	// And a create into the now-empty subject descends from that second record, which is
	// the property the whole recognition rests on.
	third, _, cerr := a.asm.createOrRestoreSession(clientID, a.s.getOpts())
	if cerr != nil {
		t.Fatalf("creating after the second teardown: %v", cerr)
	}
	if err := testSessSave(t, third); err != nil {
		t.Fatalf("the create's first save: %v", err)
	}
	if created, _ := testLoadSessRecord(t, a, idHash); created == nil || created.PWID != secondRec.WID {
		t.Fatalf("the create must descend from the record the LAST retirement named: %+v want parent %q",
			created, secondRec.WID)
	}
	if err := third.clear(false); err != nil {
		t.Fatalf("tearing the create down again: %v", err)
	}
	r2, _, _ = testLoadRetirement(t, a, idHash)

	// (b) An OLDER retirement — deliberately of a different generation, so the
	// ordering guard is the only thing that can refuse it — does not regress the
	// fence.
	older := &mqttTeardownTarget{id: clientID, idHash: idHash, gen: mqttNewSessGen()}
	if err := second.recordGenRetirement(older, "older", mqttNewRecordWID(), r2.MarkerSeq-1); err != nil {
		t.Fatalf("an older retirement must be accepted as a no-op, got %v", err)
	}
	r3, _, _ := testLoadRetirement(t, a, idHash)
	// Against the RELOADED standing fence, not the second generation: the create above was
	// torn down too, so the standing retirement is that third one's.
	if r3.Gen != r2.Gen || r3.MarkerSeq != r2.MarkerSeq || r3.RWID != r2.RWID {
		t.Fatalf("an older retirement regressed the fence: %+v, want %+v", r3, r2)
	}

	// (c) A retirement of the SAME generation at a HIGHER marker sequence advances
	// it. A generation-equality early return would call this already done and leave
	// the fence naming the older claim.
	newerWID := mqttNewRecordWID()
	if err := second.recordGenRetirement(&mqttTeardownTarget{id: clientID, idHash: idHash, gen: r2.Gen},
		"newer", newerWID, r2.MarkerSeq+1); err != nil {
		t.Fatalf("a newer same-generation retirement: %v", err)
	}
	r4, _, _ := testLoadRetirement(t, a, idHash)
	if r4.MarkerSeq != r2.MarkerSeq+1 {
		t.Fatalf("a newer same-generation retirement did not advance the fence: MarkerSeq %v, want %v",
			r4.MarkerSeq, r2.MarkerSeq+1)
	}
	if r4.RWID != newerWID {
		t.Fatalf("the advanced fence must name the record THAT retirement passed: RWID=%q want %q "+
			"(the standing one named %q)", r4.RWID, newerWID, r2.RWID)
	}
}

// TestRetirementWriteFailureAbortsBeforeDeleting is test 6: the fence is a
// prerequisite of the deletions, not a best effort beside them. A write that cannot
// be confirmed fails the teardown CLOSED, leaving the record MARKED — the
// already-handled self-healing state — with every resource intact, and a later
// adopter records the retirement before repeating the deletions.
func TestRetirementWriteFailureAbortsBeforeDeleting(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "fence-fails-closed"

	sessA := testProvisionSession(t, a, clientID)
	idHash, retiredGen := sessA.idHash, sessA.gen
	_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)
	holdSubject := mqttQoS2HoldSubject(idHash, retiredGen, 1)
	wantSubject := mqttTeardownSubject(a.asm.domainTk, idHash)

	// Scoped to THIS client's subject: the hook is process-wide, and a teardown on any
	// other account manager that lands inside this window must reach the real store
	// rather than an error about a client it is not.
	restore := setMQTTTestRetirementStoreHook(func(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error) {
		if subject != wantSubject {
			return a.asm.jsa.storeGenRetirementCAS(subject, expectSeq, r)
		}
		return 0, errors.New("mqtt test: retirement watermark store forced to fail")
	})

	err := sessA.clear(false)
	restore()
	if err == nil || errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("clear() = %v, want a plain failure: an unrecordable retirement must abort the teardown", err)
	}

	if _, _, status := testLoadRetirement(t, a, idHash); status != mqttRetirementAbsent {
		t.Errorf("no fence should exist after a failed write, got status %v", status)
	}
	ps, markerSeq := testLoadSessRecord(t, a, idHash)
	if ps == nil || ps.Teardown == nil {
		t.Fatalf("the record must be left MARKED for a later adopter, got %+v", ps)
	}
	if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
		t.Error("the PUBREL consumer was deleted although the retirement was never recorded")
	}
	if _, lerr := a.asm.jsa.loadLastMsgFor(mqttQoS2IncomingMsgsStreamName, holdSubject); lerr != nil {
		t.Errorf("the QoS 2 hold was destroyed although the retirement was never recorded: %v", lerr)
	}

	// A later adopter heals it: records the fence first, then repeats the deletions
	// and releases the record.
	if aerr := b.asm.finishAdoptedTeardown(ps, idHash, markerSeq, b.s.getOpts()); aerr != nil {
		t.Fatalf("adoption after a failed fence: %v", aerr)
	}
	r, _, status := testLoadRetirement(t, b, idHash)
	if status != mqttRetirementValid || r.Gen != retiredGen {
		t.Errorf("the adopter must record the retirement: status=%v r=%+v", status, r)
	}
	if testConsumerExists(t, b, mqttOutStreamName, pubRelDur) {
		t.Error("the adopter did not repeat the deletions")
	}
	if got, _ := testLoadSessRecord(t, b, idHash); got != nil {
		t.Errorf("the adopter did not release the record, got %+v", got)
	}
}

// TestRetirementIsRecordedBeforeTheFirstDeletion is test 7. The ordering IS the
// mechanism: a fence written after the deletions describes a window it did not
// cover, and the survivor of that window is exactly what nothing else can recognise.
// Observed from the claim-window hook, which production reaches between the two.
func TestRetirementIsRecordedBeforeTheFirstDeletion(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "fence-before-delete"

	sessA := testProvisionSession(t, a, clientID)
	idHash, retiredGen := sessA.idHash, sessA.gen
	_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)

	fired := false
	gate := newTestHookGate(t)
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, _ string, markerSeq uint64) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if sess != sessA {
			return
		}
		fired = true
		r, _, status := testLoadRetirement(t, a, idHash)
		if status != mqttRetirementValid {
			t.Errorf("the retirement must be durable before the first deletion, got status %v", status)
			return
		}
		if r.Gen != retiredGen || r.MarkerSeq != markerSeq {
			t.Errorf("fence = %+v, want generation %q at marker sequence %v", r, retiredGen, markerSeq)
		}
		if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
			t.Error("a deletion ran before the retirement was recorded")
		}
	})
	defer restore()

	if err := sessA.clear(false); err != nil {
		t.Fatalf("clear(): %v", err)
	}
	if !fired {
		t.Fatal("the claim-window hook never fired")
	}
}

// TestRetirementAdoptionRecordsTheFenceFirst is test 8: an ADOPTED teardown deletes
// the same resources, so it needs the same fence, and the marker it adopted is just
// as evictable. Staged as a winner that crashed after claiming — the fence is absent
// when the adopter starts, which is what makes that generation legitimately
// resumable until the adopter fences it.
func TestRetirementAdoptionRecordsTheFenceFirst(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "adoption-fences-first"

	sessA := testProvisionSession(t, a, clientID)
	idHash, retiredGen := sessA.idHash, sessA.gen
	_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)
	ps, seq := testLoadSessRecord(t, a, idHash)

	// A wins the claim and dies there: nothing deleted, nothing fenced.
	tok := sessA.beginRecordTx()
	markerSeq, _, claim, cerr := sessA.claimTeardown(tok, ps, seq, sessA.wid)
	if claim != mqttTeardownClaimWon {
		t.Fatalf("claimTeardown = %v, err=%v", claim, cerr)
	}
	if terr := sessA.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}
	if _, _, status := testLoadRetirement(t, a, idHash); status != mqttRetirementAbsent {
		t.Fatalf("a crash before the write leaves no fence, got status %v", status)
	}

	// The adopter's write is observed in place: the targets must still be there when
	// it lands.
	var wroteBeforeDeleting, wrote bool
	gate := newTestHookGate(t)
	restore := setMQTTTestRetirementStoreHook(func(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error) {
		if !gate.enter() {
			return b.asm.jsa.storeGenRetirementCAS(subject, expectSeq, r)
		}
		defer gate.leave()
		wrote = true
		wroteBeforeDeleting = testConsumerExists(t, b, mqttOutStreamName, pubRelDur)
		return b.asm.jsa.storeGenRetirementCAS(subject, expectSeq, r)
	})

	marked, markedSeq := testLoadSessRecord(t, b, idHash)
	aerr := b.asm.finishAdoptedTeardown(marked, idHash, markedSeq, b.s.getOpts())
	restore()
	if aerr != nil {
		t.Fatalf("finishAdoptedTeardown: %v", aerr)
	}
	if !wrote {
		t.Fatal("the adopter recorded no retirement at all")
	}
	if !wroteBeforeDeleting {
		t.Error("the adopter deleted before recording the retirement")
	}
	r, _, status := testLoadRetirement(t, b, idHash)
	if status != mqttRetirementValid || r.Gen != retiredGen {
		t.Fatalf("fence after adoption: status=%v r=%+v want a valid fence naming %q", status, r, retiredGen)
	}
	if r.MarkerSeq != markerSeq {
		t.Errorf("the adopter must fence at the ADOPTED marker's sequence: %v, want %v", r.MarkerSeq, markerSeq)
	}
	if testConsumerExists(t, b, mqttOutStreamName, pubRelDur) {
		t.Error("the adoption did not repeat the deletions")
	}
	if got, _ := testLoadSessRecord(t, b, idHash); got != nil {
		t.Errorf("the adoption did not release the record, got %+v", got)
	}
}

// TestRetirementUnreadableWatermarkRefusesTheConnect is test 10: reader 2 cannot
// decide whether the record it is about to resume describes live state, so it
// refuses the CONNECT rather than resuming it — the same rule the record load itself
// follows when $MQTT_sess is unavailable.
func TestRetirementUnreadableWatermarkRefusesTheConnect(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "watermark-unreadable"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	_, recSeq := testLoadSessRecord(t, a, idHash)

	restore := setMQTTTestRetirementLoadHook(func(hash string) (*mqttGenRetirement, uint64, mqttRetirementStatus) {
		if hash != idHash {
			return a.asm.jsa.loadGenRetirementRecord(a.asm.domainTk, hash)
		}
		return nil, 0, mqttRetirementIndeterminate
	})
	_, _, err := a.asm.createOrRestoreSession(clientID, a.s.getOpts())
	restore()
	if err == nil {
		t.Fatal("an unreadable retirement watermark must refuse the CONNECT")
	}

	ps, seq := testLoadSessRecord(t, a, idHash)
	if ps == nil || seq != recSeq {
		t.Fatalf("the record must be untouched by a refused CONNECT, got %+v at %v (was %v)", ps, seq, recSeq)
	}
	if ps.Teardown != nil {
		t.Error("a refused CONNECT must not mark the record")
	}
}

// TestRetirementAdopterCondemnsTheHollowSurvivor is test 11: the ADOPTION path's
// release-superseded arm carries the same repair. Its deletions ran on the strength
// of the marker it adopted, so a same-generation survivor of them is just as hollow —
// and this arm is reached by a completely different caller, which is why the drop is
// its own mutation.
func TestRetirementAdopterCondemnsTheHollowSurvivor(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "adopter-condemns"

	sessA := testProvisionSession(t, a, clientID)
	idHash, retiredGen := sessA.idHash, sessA.gen
	base, seq := testLoadSessRecord(t, a, idHash)

	// A claims and dies.
	tok := sessA.beginRecordTx()
	_, adoptedNonce, claim, cerr := sessA.claimTeardown(tok, base, seq, sessA.wid)
	if claim != mqttTeardownClaimWon {
		t.Fatalf("claimTeardown = %v, err=%v", claim, cerr)
	}
	if terr := sessA.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}

	// B adopts. Between its deletions and its release, the cross-term resume lands.
	fired := false
	gate := newTestHookGate(t)
	restoreWindow := setMQTTTestTeardownAdoptionWindowHook(func(_, hash string, markerSeq uint64) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if hash != idHash {
			return
		}
		fired = true
		survivor := *base
		survivor.Teardown = nil
		testPlantResumeOver(t, b, idHash, &survivor, markerSeq)
	})
	defer restoreWindow()

	marked, markedSeq := testLoadSessRecord(t, b, idHash)
	err := b.asm.finishAdoptedTeardown(marked, idHash, markedSeq, b.s.getOpts())
	if !errors.Is(err, errMQTTTeardownAdoptionSuperseded) {
		t.Fatalf("finishAdoptedTeardown = %v, want the superseded sentinel", err)
	}
	if !fired {
		t.Fatal("the adoption-window hook never fired")
	}

	ps, _ := testLoadSessRecord(t, b, idHash)
	if ps == nil {
		t.Fatal("the surviving record was deleted; condemnation is mark-only")
	}
	if ps.Teardown == nil {
		t.Fatal("the adopter left the hollow survivor unmarked: its own deletions had already run")
	}
	if ps.Teardown.Nonce == adoptedNonce {
		t.Error("the condemnation must be a fresh claim, not the adopted marker")
	}
	if ps.Gen != retiredGen {
		t.Errorf("the condemnation must preserve the record's content, got generation %q", ps.Gen)
	}
}

// TestRetirementMalformedWatermarkFailsClosedEverywhere is test 12. A watermark that
// decodes but fails validation is CORRUPT, and corrupt means fail-closed in all
// three places: the writer refuses to delete behind it, reader 2 refuses the CONNECT,
// and reader 3 mutates nothing for that client. It is never repaired in place — a
// watermark we cannot read may name a retirement newer than ours.
func TestRetirementMalformedWatermarkFailsClosedEverywhere(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(r *mqttGenRetirement)
	}{
		{"a client ID that does not hash back", func(r *mqttGenRetirement) { r.ID = "somebody-else" }},
		{"no marker sequence", func(r *mqttGenRetirement) { r.MarkerSeq = 0 }},
		{"a malformed generation", func(r *mqttGenRetirement) { r.Gen = "not-a-generation" }},
		// The shape a pre-RWID watermark has, and the one a hand-authored record has:
		// it cannot say which record its deletion retired, so it can seed no lineage.
		// Refused rather than read as empty — the validator cannot tell a missing field
		// from a present empty one, so accepting it would accept the older format.
		{"no retired record identity", func(r *mqttGenRetirement) { r.RWID = _EMPTY_ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := testNewTeardownEnv(t)
			const clientID = "watermark-malformed"

			sessA := testProvisionSession(t, a, clientID)
			idHash, retiredGen := sessA.idHash, sessA.gen
			_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)
			_, recSeq := testLoadSessRecord(t, a, idHash)

			// The baseline is VALID in every field, so each row's mutation is the only
			// thing that can make it corrupt. A baseline missing RWID would make every
			// row pass for the wrong reason.
			bad := &mqttGenRetirement{
				ID: clientID, Gen: retiredGen, MarkerSeq: recSeq,
				RWID: mqttNewRecordWID(), Nonce: "planted",
			}
			tc.mut(bad)
			testPlantRetirement(t, a, idHash, bad, 0)
			if _, _, status := testLoadRetirement(t, a, idHash); status != mqttRetirementCorrupt {
				t.Fatalf("the planted watermark must classify as corrupt, got %v", status)
			}

			// Reader 2.
			if _, _, err := a.asm.createOrRestoreSession(clientID, a.s.getOpts()); err == nil {
				t.Fatal("a malformed watermark must refuse the CONNECT")
			}

			// Reader 3, driven directly: two passes, since one observation authorizes
			// nothing even at a zero grace.
			testSetOrphanGrace(t, a, 0)
			a.asm.condemnRetiredSurvivors(a.s)
			a.asm.condemnRetiredSurvivors(a.s)
			if ps, _ := testLoadSessRecord(t, a, idHash); ps == nil || ps.Teardown != nil {
				t.Fatalf("the sweep must mutate nothing behind a malformed watermark, got %+v", ps)
			}

			// The writer.
			err := sessA.clear(false)
			if err == nil || errors.Is(err, errMQTTClearSuperseded) {
				t.Fatalf("clear() = %v, want a plain failure behind a malformed watermark", err)
			}
			if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
				t.Error("the teardown deleted behind a watermark it could not read")
			}
			if ps, _ := testLoadSessRecord(t, a, idHash); ps == nil || ps.Teardown == nil {
				t.Errorf("the refused teardown must leave the record MARKED, got %+v", ps)
			}
		})
	}
}

// TestRetirementReadersValidateTheRecordFirst is test 14. A teardown marker LICENSES
// its adopters to delete every durable the record names, and the CONNECT discard
// deletes them itself — so a record naming another session's consumer is condemned by
// nobody and discarded by nobody. Each of the three DESTRUCTIVE readers applies
// mqttValidateTeardownTargets before it acts. The fourth, the record-absent create,
// deletes nothing and names no target, so it has none to validate.
func TestRetirementReadersValidateTheRecordFirst(t *testing.T) {
	a, b := testNewTeardownEnv(t)
	const clientID = "readers-validate"

	sessA := testProvisionSession(t, a, clientID)
	idHash, retiredGen := sessA.idHash, sessA.gen
	base, _ := testLoadSessRecord(t, a, idHash)
	foreignDur := getHash("somebody-else") + "_" + mqttNewSessGen()

	// Reader 1: the survivor of the deletion window names a consumer that is not this
	// session's, so it is left to the unadoptable-record rules.
	gate := newTestHookGate(t)
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, _ string, markerSeq uint64) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if sess != sessA {
			return
		}
		survivor := *base
		survivor.Teardown = nil
		survivor.Cons = map[string]*ConsumerConfig{"foo": {Durable: foreignDur}}
		testPlantResumeOver(t, b, idHash, &survivor, markerSeq)
	})
	defer restore()

	if err := sessA.clear(false); !errors.Is(err, errMQTTClearSuperseded) {
		t.Fatalf("clear() = %v, want the superseded sentinel", err)
	}
	ps, _ := testLoadSessRecord(t, a, idHash)
	if ps == nil {
		t.Fatal("the survivor was deleted")
	}
	if ps.Teardown != nil {
		t.Fatal("reader 1 condemned a record naming a consumer that is not this session's, " +
			"licensing its adopters to delete another session's durable")
	}
	if ps.Gen != retiredGen {
		t.Fatalf("staging error: the survivor must carry the retired generation, got %q", ps.Gen)
	}

	// Reader 3: same record, same refusal, driven directly across two passes.
	testSetOrphanGrace(t, a, 0)
	a.asm.condemnRetiredSurvivors(a.s)
	a.asm.condemnRetiredSurvivors(a.s)
	if got, _ := testLoadSessRecord(t, a, idHash); got == nil || got.Teardown != nil {
		t.Fatalf("reader 3 condemned an unvalidatable record, got %+v", got)
	}

	// Reader 2: the CONNECT is REFUSED rather than routed into the destructive
	// discard, which would delete the foreign durable by name.
	_, _, err := b.asm.createOrRestoreSession(clientID, b.s.getOpts())
	if err == nil {
		t.Fatal("reader 2 must refuse a retired record whose names are not this session's")
	}
	if !strings.Contains(err.Error(), "not this session's") {
		t.Errorf("the diagnostic must say what was wrong with the record, got: %v", err)
	}
	if got, _ := testLoadSessRecord(t, b, idHash); got == nil {
		t.Error("a refused CONNECT must leave the record in place")
	}
	if testConsumerExists(t, b, mqttStreamName, foreignDur) {
		t.Error("the foreign durable should never have been created by this test")
	}
}

// testRetirementBringUp wires a minimal Server plus internal account client against a
// live in-process JetStream, so a test can bring an account-session manager up (and
// bring a SECOND one up after tampering with the streams). The tagged full-suite has
// its own copy of this; the epoch coupling belongs to the default suite.
func testRetirementBringUp(t *testing.T) (*Server, *client, *nats.Conn) {
	t.Helper()
	_, nc := runNATS(t)
	return testRetirementBringUpOn(t, nc, "node1")
}

func testRetirementBringUpOn(t *testing.T, nc *nats.Conn, node string) (*Server, *client, *nats.Conn) {
	t.Helper()
	s := &Server{nrc: nc, opts: &Options{}, quitCh: make(chan struct{}), nodeName: node}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	c := s.createInternalAccountClient()
	t.Cleanup(func() { close(s.quitCh) })
	return s, c, nc
}

// testRetirementJS is an administrative JetStream handle, for the one thing the
// broker's own API deliberately cannot do: delete a stream.
func testRetirementJS(t *testing.T, nc *nats.Conn) (jetstream.JetStream, context.Context) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return js, ctx
}

// TestValidateTeardownStream exercises every validateTeardownStream branch against
// synthetic StreamInfo values, including the ones a single-node bring-up cannot reach.
// Both directions are covered: the prerequisites are rejected, and the audited
// NON-prerequisites are ACCEPTED — without those rows the audit is unfalsifiable in
// the over-strict direction, and a copy-paste from a deleting stream's validator would
// lock conforming deployments out.
func TestValidateTeardownStream(t *testing.T) {
	t.Parallel()
	const domainTk = ""
	epoch := mqttGenSessEpoch()
	wantSubj := mqttTeardownSubjectPrefix + domainTk + ">"
	base := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Subjects:   []string{wantSubj},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
			Metadata:   map[string]string{mqttSessEpochMetaKey: epoch},
		}}
	}
	if err := validateTeardownStream(base(), domainTk, 0, epoch); err != nil {
		t.Fatalf("conformant stream must validate, got %v", err)
	}
	si := base()
	si.Config.Replicas = 3
	if err := validateTeardownStream(si, domainTk, 3, epoch); err != nil {
		t.Fatalf("matching replicas must validate, got %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(si *StreamInfo)
		want   int
	}{
		{"nil", func(si *StreamInfo) {}, 0}, // replaced below
		{"wrongsubject", func(si *StreamInfo) { si.Config.Subjects = []string{"bogus.>"} }, 0},
		{"twosubjects", func(si *StreamInfo) { si.Config.Subjects = []string{wantSubj, "extra.>"} }, 0},
		{"memstorage", func(si *StreamInfo) { si.Config.Storage = MemoryStorage }, 0},
		{"interestretention", func(si *StreamInfo) { si.Config.Retention = InterestPolicy }, 0},
		{"discardnew", func(si *StreamInfo) { si.Config.Discard = DiscardNew }, 0},
		{"maxmsgsper", func(si *StreamInfo) { si.Config.MaxMsgsPer = 2 }, 0},
		{"maxmsgs", func(si *StreamInfo) { si.Config.MaxMsgs = 10 }, 0},
		{"maxbytes", func(si *StreamInfo) { si.Config.MaxBytes = 1 << 20 }, 0},
		{"maxage", func(si *StreamInfo) { si.Config.MaxAge = time.Hour }, 0},
		{"mirror", func(si *StreamInfo) { si.Config.Mirror = &StreamSource{Name: mqttSessStreamName} }, 0},
		{"sources", func(si *StreamInfo) { si.Config.Sources = []*StreamSource{{Name: mqttSessStreamName}} }, 0},
		{"replicamismatch", func(si *StreamInfo) { si.Config.Replicas = 1 }, 3},
		{"maxmsgsize", func(si *StreamInfo) { si.Config.MaxMsgSize = 1024 }, 0},
		{"noack", func(si *StreamInfo) { si.Config.NoAck = true }, 0},
		{"sealed", func(si *StreamInfo) { si.Config.Sealed = true }, 0},
		{"subjecttransform", func(si *StreamInfo) {
			si.Config.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"}
		}, 0},
		{"republish", func(si *StreamInfo) { si.Config.RePublish = &RePublish{Source: "a", Destination: "b"} }, 0},
		{"allowmsgcounter", func(si *StreamInfo) { si.Config.AllowMsgCounter = true }, 0},
		{"allowrollup", func(si *StreamInfo) { si.Config.AllowRollup = true }, 0},
		{"asyncpersist", func(si *StreamInfo) { si.Config.PersistMode = AsyncPersistMode }, 0},
		// The epoch coupling: a MarkerSeq is a $MQTT_sess sequence, so a stream from
		// another incarnation of that stream carries watermarks nothing may compare.
		{"noepoch", func(si *StreamInfo) { si.Config.Metadata = nil }, 0},
		{"emptyepoch", func(si *StreamInfo) { si.Config.Metadata = map[string]string{mqttSessEpochMetaKey: ""} }, 0},
		{"malformedepoch", func(si *StreamInfo) {
			si.Config.Metadata = map[string]string{mqttSessEpochMetaKey: "not-a-nuid"}
		}, 0},
		{"foreignepoch", func(si *StreamInfo) {
			si.Config.Metadata = map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()}
		}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var si *StreamInfo
			if tc.name != "nil" {
				si = base()
				tc.mutate(si)
			}
			if err := validateTeardownStream(si, domainTk, tc.want, epoch); err == nil {
				t.Fatalf("expected validation failure for %q", tc.name)
			}
		})
	}

	for _, tc := range []struct {
		name   string
		mutate func(si *StreamInfo)
	}{
		{"denydelete is not a prerequisite", func(si *StreamInfo) { si.Config.DenyDelete = true }},
		{"denypurge is not a prerequisite", func(si *StreamInfo) { si.Config.DenyPurge = true }},
		{"subjectdeletemarkerttl is not a prerequisite", func(si *StreamInfo) { si.Config.SubjectDeleteMarkerTTL = time.Minute }},
		{"unrelated metadata keys are accepted", func(si *StreamInfo) { si.Config.Metadata["unrelated"] = "keepme" }},
		{"a consumer cap cannot affect an owner that creates none", func(si *StreamInfo) { si.Config.MaxConsumers = 5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := base()
			tc.mutate(si)
			if err := validateTeardownStream(si, domainTk, 0, epoch); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}
}

// TestTeardownStreamIsEpochCoupledToTheSessionStream is test 13. A retirement
// watermark orders itself by a $MQTT_sess sequence, so the two streams are ONE
// logical unit, and the two partial-recreation directions are deliberately different:
//
//   - $MQTT_sess recreated alone mints a fresh incarnation epoch while the permanent
//     watermarks keep the old one, so their sequences are no longer comparable to
//     anything. That is REFUSED at startup with the coupled guidance, not classified
//     per read and not migrated.
//   - $MQTT_teardown recreated alone stamps the current epoch, validates clean, and
//     admits with NO historical fences. The lost fences are the stated residual
//     (docs/CONFORMANCE.md), exactly as for the will-cancel watermarks.
func TestTeardownStreamIsEpochCoupledToTheSessionStream(t *testing.T) {
	t.Run("the session stream recreated alone is refused", func(t *testing.T) {
		s1, c1, nc := testRetirementBringUp(t)
		asm1, err := s1.getOrCreateMQTTAccountSessionManager(c1)
		if err != nil {
			t.Fatalf("first bring-up: %v", err)
		}
		si, err := asm1.jsa.lookupStream(mqttTeardownStreamName)
		if err != nil || si == nil {
			t.Fatalf("the teardown stream must be provisioned: %+v %v", si, err)
		}
		if got := si.Config.Metadata[mqttSessEpochMetaKey]; got != asm1.sessStreamEpoch {
			t.Fatalf("the teardown stream must be stamped with the adopted session epoch: %q vs %q",
				got, asm1.sessStreamEpoch)
		}

		js, ctx := testRetirementJS(t, nc)
		if derr := js.DeleteStream(ctx, mqttSessStreamName); derr != nil {
			t.Fatalf("delete session stream: %v", derr)
		}

		s2, c2, _ := testRetirementBringUpOn(t, nc, "node2")

		_, err = s2.getOrCreateMQTTAccountSessionManager(c2)
		if err == nil {
			t.Fatal("a $MQTT_teardown stream from another session-stream incarnation must fail " +
				"initialization: its watermarks order themselves by sequences of a stream that no longer exists")
		}
		for _, want := range []string{mqttTeardownStreamName, mqttSessStreamName, "one logical unit", mqttSessEpochMetaKey} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal must name %q, got: %v", want, err)
			}
		}
	})

	t.Run("the teardown stream recreated alone admits with no fences", func(t *testing.T) {
		s1, c1, nc := testRetirementBringUp(t)
		asm1, err := s1.getOrCreateMQTTAccountSessionManager(c1)
		if err != nil {
			t.Fatalf("first bring-up: %v", err)
		}
		idHash := getHash("recreated-unit")
		// A VALID fence, RWID included: a watermark the readers would refuse anyway
		// is no fence, so its disappearance would demonstrate nothing.
		if _, serr := asm1.jsa.storeGenRetirementCAS(mqttTeardownSubject(asm1.domainTk, idHash), 0,
			&mqttGenRetirement{ID: "recreated-unit", Gen: mqttNewSessGen(), MarkerSeq: 9,
				RWID: mqttNewRecordWID()}); serr != nil {
			t.Fatalf("planting a fence: %v", serr)
		}

		js, ctx := testRetirementJS(t, nc)
		if derr := js.DeleteStream(ctx, mqttTeardownStreamName); derr != nil {
			t.Fatalf("delete teardown stream: %v", derr)
		}

		s2, c2, _ := testRetirementBringUpOn(t, nc, "node2")

		asm2, err := s2.getOrCreateMQTTAccountSessionManager(c2)
		if err != nil {
			t.Fatalf("recreating the watermark stream alone must ADMIT: %v", err)
		}
		if asm2.sessStreamEpoch != asm1.sessStreamEpoch {
			t.Fatalf("the surviving session stream keeps its incarnation: %q vs %q",
				asm2.sessStreamEpoch, asm1.sessStreamEpoch)
		}
		// The residual, asserted rather than assumed: every open fence is gone.
		if _, _, status := asm2.jsa.loadGenRetirementRecord(asm2.domainTk, idHash); status != mqttRetirementAbsent {
			t.Fatalf("the recreated stream must be empty, got status %v", status)
		}
	})
}

// ---------------------------------------------------------------------------
// The watermark names the RECORD its deletion retired (F-029b)
// ---------------------------------------------------------------------------

// TestRetirementNamesTheRetiredRecord is T1 (M10).
//
// Driven through the production path rather than by calling recordGenRetirement with the
// argument the assertion then checks: a test that supplies the token itself cannot kill a
// mutation at the production site.
func TestRetirementNamesTheRetiredRecord(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "retirement-names-record"

	sessA := testProvisionSession(t, a, clientID)
	base, _ := testLoadSessRecord(t, a, sessA.idHash)
	if base == nil || base.WID == _EMPTY_ {
		t.Fatal("the record this teardown retires must carry an identity")
	}
	if err := sessA.clear(false); err != nil {
		t.Fatalf("clear(): %v", err)
	}
	r, _, status := testLoadRetirement(t, a, sessA.idHash)
	if status != mqttRetirementValid {
		t.Fatalf("watermark status = %v, want valid", status)
	}
	if r.RWID != base.WID {
		t.Fatalf("the watermark must name the record its deletion REMOVED: RWID=%q want %q",
			r.RWID, base.WID)
	}
}

// TestRetirementAdoptionNamesTheClaimedRecord is T2 (M2).
//
// An adopted teardown retires the record the MARKER claimed, which the marker preserved as
// its own PWID — not the marker, which the release deletes and which is nobody's base.
// Naming the marker would give the two sides of a cross-term pair different tokens.
func TestRetirementAdoptionNamesTheClaimedRecord(t *testing.T) {
	testQuietExpirySweeps(t)
	a, _ := testNewTeardownEnv(t)
	const clientID = "retirement-adoption-names"

	marker, _ := testStageClaimedMarker(t, a, clientID)
	if marker.WID == _EMPTY_ || marker.PWID == _EMPTY_ || marker.WID == marker.PWID {
		t.Fatalf("the staged marker must have its own identity and a parent: %+v", marker)
	}
	if _, _, err := a.asm.createOrRestoreSession(clientID, a.s.getOpts()); err != nil {
		t.Fatalf("adopting the teardown: %v", err)
	}
	r, _, status := testLoadRetirement(t, a, getHash(clientID))
	if status != mqttRetirementValid {
		t.Fatalf("watermark status = %v, want valid", status)
	}
	if r.RWID != marker.PWID {
		t.Fatalf("an adopted teardown retires the record it CLAIMED: RWID=%q want %q "+
			"(the marker's own identity is %q)", r.RWID, marker.PWID, marker.WID)
	}
}

// TestRetirementBodyUsesTheLowercaseKey is T12's raw-body half (M18).
//
// A struct tag typo spells the field "RWID", every reader and writer in this tree agrees on
// the typo, and a replicated round trip would not notice. The persisted key is the contract.
func TestRetirementBodyUsesTheLowercaseKey(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	const clientID = "retirement-raw-key"

	sessA := testProvisionSession(t, a, clientID)
	if err := sessA.clear(false); err != nil {
		t.Fatalf("clear(): %v", err)
	}
	smsg, err := a.asm.jsa.loadLastMsgFor(mqttTeardownStreamName,
		mqttTeardownSubject(a.asm.domainTk, sessA.idHash))
	if err != nil {
		t.Fatalf("read the raw watermark: %v", err)
	}
	if !strings.Contains(string(smsg.Data), `"rwid":"`) {
		t.Fatalf("the persisted watermark must carry the lowercase \"rwid\" key: %s", smsg.Data)
	}
}

// ---------------------------------------------------------------------------
// The CONNECT rollback ARBITRATES before it fences (F-029b §4)
// ---------------------------------------------------------------------------

// testRollbackEnv is a saved, resident session with an uncontended record, plus the
// observation seam for the rollback's classification.
type testRollbackEnv struct {
	*testTeardownEnv
	sess      *mqttSession
	clientID  string
	idHash    string
	candidate uint64
	pubRelDur string
	got       func() rollbackResult
}

// testNewRollbackEnv provisions the session a rejected CONNECT leaves behind: durably
// saved, connected (DisconnectedAt == 0) and resident. effective is the session's own
// persisted lifetime, which is what the rollback branches on.
func testNewRollbackEnv(t *testing.T, clientID string, effective uint32) *testRollbackEnv {
	t.Helper()
	testQuietExpirySweeps(t)
	a, _ := testNewTeardownEnv(t)
	sess := testProvisionSession(t, a, clientID)
	_, _, pubRelDur := mqttPubRelNames(sess.idHash, sess.gen)

	sess.mu.Lock()
	sess.disconnectedAt = 0
	sess.expiryInterval = effective
	sess.mu.Unlock()
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("persist the accepted session: %v", err)
	}
	sess.mu.Lock()
	candidate := sess.seq
	sess.mu.Unlock()
	a.asm.addSession(sess, true)

	var mu sync.Mutex
	result := rollbackUnset
	seen := false
	a.asm.mu.Lock()
	a.asm.testRollbackDone = func(_ *mqttSession, r rollbackResult) {
		mu.Lock()
		result, seen = r, true
		mu.Unlock()
	}
	a.asm.mu.Unlock()
	return &testRollbackEnv{
		testTeardownEnv: a, sess: sess, clientID: clientID, idHash: sess.idHash,
		candidate: candidate, pubRelDur: pubRelDur,
		got: func() rollbackResult {
			mu.Lock()
			defer mu.Unlock()
			if !seen {
				t.Fatal("the rollback never classified: its observation hook did not fire")
			}
			return result
		},
	}
}

func (e *testRollbackEnv) rollback() {
	e.asm.rollbackAcceptedSession(e.s, e.sess, e.idHash, e.candidate)
}

func (e *testRollbackEnv) resident() bool {
	e.asm.mu.RLock()
	defer e.asm.mu.RUnlock()
	return e.asm.sessByHash[e.idHash] == e.sess
}

// TestRollbackZeroLifetimeTearsDownRatherThanDeleting is T7 (M5).
//
// A zero lifetime ends the session at connection close, and this CONNECT was rejected — so
// the candidate is TORN DOWN, not deleted: the claim licenses the fence, the fence precedes
// the first deletion, and the incarnation's durables go with it. The generation-scoped
// PUBREL consumer is the discriminator: without it the test passes against a rollback that
// only claimed, fenced and released the record, which is exactly the "full teardown" claim
// this change makes.
func TestRollbackZeroLifetimeTearsDownRatherThanDeleting(t *testing.T) {
	e := testNewRollbackEnv(t, "rollback-teardown", 0)
	base, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if base == nil || base.WID == _EMPTY_ {
		t.Fatal("the candidate record must carry an identity")
	}
	if !testConsumerExists(t, e.testTeardownEnv, mqttOutStreamName, e.pubRelDur) {
		t.Fatal("fixture: the PUBREL consumer must exist, or 'deleted' proves nothing")
	}

	// The ORDER is the mechanism: observed from the claim window, which production reaches
	// between the fence and the first deletion.
	fired := false
	gate := newTestHookGate(t)
	restore := setMQTTTestTeardownClaimedHook(func(sess *mqttSession, _ string, markerSeq uint64) {
		if !gate.enter() {
			return
		}
		defer gate.leave()
		if sess != e.sess {
			return
		}
		fired = true
		r, _, status := testLoadRetirement(t, e.testTeardownEnv, e.idHash)
		if status != mqttRetirementValid {
			t.Errorf("the retirement must be durable before the first deletion, got status %v", status)
			return
		}
		if r.RWID != base.WID {
			t.Errorf("the fence must name the candidate record: RWID=%q want %q", r.RWID, base.WID)
		}
		if ps, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash); ps == nil {
			t.Error("the record was removed before the retirement was recorded")
		}
	})
	defer restore()

	e.rollback()

	if !fired {
		t.Fatal("the claim window never opened, so the rollback took no teardown claim")
	}
	if ps, _ := testLoadSessRecord(t, e.testTeardownEnv, e.idHash); ps != nil {
		t.Fatalf("a zero-lifetime rollback must release the record: got %+v", ps)
	}
	if testConsumerExists(t, e.testTeardownEnv, mqttOutStreamName, e.pubRelDur) {
		t.Fatal("a zero-lifetime rollback runs the FULL teardown: this incarnation's PUBREL " +
			"consumer must be deleted, not left to the orphan sweep")
	}
	if got := e.got(); got != rollbackApplied {
		t.Fatalf("a completed teardown leaves the candidate non-connected: got %v, want rollbackApplied", got)
	}
	if e.resident() {
		t.Fatal("a completed rollback removes the session")
	}
}

// TestRollbackMarkedRecordClassifiesApplied is T7b (M8, M16).
//
// With the retirement store failing the teardown aborts having deleted NOTHING, leaving the
// record marked. A marked record is claimed for deletion and never resumed, so the
// candidate is not falsely connected — rollbackApplied — and neither rollbackSuperseded ("a
// legitimate newer generation") nor rollbackIndeterminate (a store failure that did not
// happen) may be reported.
func TestRollbackMarkedRecordClassifiesApplied(t *testing.T) {
	e := testNewRollbackEnv(t, "rollback-marked", 0)
	restore := setMQTTTestRetirementStoreHook(func(string, uint64, *mqttGenRetirement) (uint64, error) {
		return 0, errors.New("test: the retirement watermark is unavailable")
	})
	defer restore()

	e.rollback()

	ps, seq := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if ps == nil || ps.Teardown == nil {
		t.Fatalf("a refused fence must leave the record MARKED and undeleted: %+v", ps)
	}
	if seq <= e.candidate {
		t.Fatalf("the claim marker sits above the candidate: seq=%v candidate=%v", seq, e.candidate)
	}
	if !testConsumerExists(t, e.testTeardownEnv, mqttOutStreamName, e.pubRelDur) {
		t.Fatal("nothing may be deleted behind a fence that was never recorded")
	}
	if got := e.got(); got != rollbackApplied {
		t.Fatalf("a MARKED record above the candidate classifies rollbackApplied — it is never "+
			"resumed, so the candidate is not falsely connected. Got %v", got)
	}
}

// TestRollbackLeavesALegitimateSuccessorAlone is T8 (M4), the round-1 regression.
//
// A resumed CONNECT preserves the record's generation, so a same-generation successor can
// land above the candidate. Recording a retirement without holding the base would fence it
// permanently and route it through a destructive clear(); the CLAIM is what makes this a
// no-op instead.
func TestRollbackLeavesALegitimateSuccessorAlone(t *testing.T) {
	e := testNewRollbackEnv(t, "rollback-successor", 0)
	base, seq := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if base == nil {
		t.Fatal("fixture: the candidate record must exist")
	}
	// A CHILD of the candidate, still CONNECTED, at a higher sequence: the shape a
	// cross-term resume leaves.
	successor := *base
	successor.DisconnectedAt = 0
	successorSeq := testPlantResumeOver(t, e.testTeardownEnv, e.idHash, &successor, seq)
	if successorSeq <= e.candidate {
		t.Fatalf("fixture: the successor must sit above the candidate: %v vs %v", successorSeq, e.candidate)
	}

	e.rollback()

	if _, _, status := testLoadRetirement(t, e.testTeardownEnv, e.idHash); status != mqttRetirementAbsent {
		t.Fatalf("a rollback whose claim is superseded must fence NOTHING: watermark status %v", status)
	}
	ps, cur := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	if ps == nil || ps.Teardown != nil || ps.WID != successor.WID {
		t.Fatalf("the legitimate successor must be untouched and unmarked: %+v at %v", ps, cur)
	}
	if got := e.got(); got != rollbackSuperseded {
		t.Fatalf("a CHILD of the candidate is a legitimate newer generation: got %v", got)
	}
	// And it is still resumable, which is what a false fence would have destroyed.
	e.asm.removeSession(e.sess, true)
	restored, existed, err := e.asm.createOrRestoreSession(e.clientID, e.s.getOpts())
	if err != nil {
		t.Fatalf("the successor must still be resumable: %v", err)
	}
	if !existed {
		t.Fatal("a legitimate successor is RESUMED, not replaced by a fresh session")
	}
	restored.mu.Lock()
	wid := restored.wid
	restored.mu.Unlock()
	if wid != successor.WID {
		t.Fatalf("the resume must come from the successor's own record: %q want %q", wid, successor.WID)
	}
}

// TestRollbackRecognisesASiblingOfTheCandidate is T13 (M12, M15).
//
// The classification reads the pair captured at ENTRY, because the zero-lifetime branch
// runs clear(), which zeroes wid/pwid. Read afterwards, a genuine sibling would be reported
// rollbackSuperseded — "a legitimate newer generation" — which is exactly the claim this
// feature withholds. The marked-record arm must not swallow it either: gated on
// ps.Teardown != nil, an unmarked sibling still reaches the lineage test.
func TestRollbackRecognisesASiblingOfTheCandidate(t *testing.T) {
	e := testNewRollbackEnv(t, "rollback-sibling", 0)
	// A second save, so the candidate itself has a parent — without one no sibling can be
	// proven.
	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("give the candidate a parent: %v", err)
	}
	e.sess.mu.Lock()
	e.candidate = e.sess.seq
	ackPWID := e.sess.pwid
	e.sess.mu.Unlock()
	if ackPWID == _EMPTY_ {
		t.Fatal("fixture: the candidate must carry a parent")
	}
	base, seq := testLoadSessRecord(t, e.testTeardownEnv, e.idHash)
	sibling := *base
	sibling.DisconnectedAt = 0
	sibling.WID, sibling.PWID = mqttNewRecordWID(), ackPWID
	if _, err := mqttStoreSessionRecordCAS(&e.asm.jsa, e.asm.domainTk, &sibling, e.idHash, seq); err != nil {
		t.Fatalf("staging the sibling: %v", err)
	}

	e.rollback()

	if got := e.got(); got != rollbackForked {
		t.Fatalf("a proven SIBLING of the candidate is no successor of it: got %v, want rollbackForked", got)
	}
}

// TestRollbackRefusedDiscardIsParked is T15 (M14) and T15b (M17).
//
// A refusal deleted nothing, and the rejected session object is the only thing that can
// resolve the write it refused over — so the discard is parked and the session stays
// RESIDENT. When the recovery declines, the caller takes its historical outcome instead:
// this caller's session starts resident, so ignoring that would leave a resident object
// nobody owns.
func TestRollbackRefusedDiscardIsParked(t *testing.T) {
	t.Run("parked", func(t *testing.T) {
		e := testNewDeferredEnv(t, "rollback-parked", "rollback.parked")
		e.sess.mu.Lock()
		e.sess.disconnectedAt, e.sess.expiryInterval = 0, 0
		candidate := e.sess.seq
		e.sess.mu.Unlock()

		var got rollbackResult
		e.asm.mu.Lock()
		e.asm.testRollbackDone = func(_ *mqttSession, r rollbackResult) { got = r }
		e.asm.mu.Unlock()

		armed := false
		restore := setMQTTTestClearWindowHook(func(sess *mqttSession, window string) {
			if sess != e.sess || window != mqttClearWindowPreFence || armed {
				return
			}
			armed = true
			// An off-readLoop register store settled here and its save went ambiguous,
			// after the entry check and before the fence.
			sess.mu.Lock()
			testMarkUncertainLocked(sess, sess.seq, testAmbiguousBodyLocked(sess, "ambiguous"))
			sess.mu.Unlock()
		})
		defer restore()

		e.asm.rollbackAcceptedSession(e.s, e.sess, e.sess.idHash, candidate)

		if !armed {
			t.Fatal("the pre-fence window never opened, so the refusal was not exercised")
		}
		if got != rollbackParked {
			t.Fatalf("a refused discard is its own outcome: got %v, want rollbackParked", got)
		}
		if !e.recordPresent(t) {
			t.Fatal("a refusal deletes no record")
		}
		if !e.durablePresent() {
			t.Fatal("a refusal deletes no durable")
		}
		if !e.resident() {
			t.Fatal("the parked discard's session must stay RESIDENT: only a resident session is resolved")
		}
		action := e.parkedAction()
		if action == nil || !action.endsAtDisconnect() {
			t.Fatalf("the refused discard must be parked as a discard-only obligation: %+v", action)
		}
		if action.will != nil {
			t.Fatal("the recovery must not park a Will")
		}

		// And the obligation is real: resolving the write discharges the discard.
		e.asm.reconcileResidentRecordState(e.s)
		if e.recordPresent(t) {
			t.Fatal("the discharged discard must delete the record")
		}
		if e.resident() {
			t.Fatal("the discharged discard must remove the session")
		}
	})

	t.Run("declined", func(t *testing.T) {
		e := testNewDeferredEnv(t, "rollback-declined", "rollback.declined")
		e.sess.mu.Lock()
		e.sess.disconnectedAt, e.sess.expiryInterval = 0, 0
		candidate := e.sess.seq
		// An action of a shape reparkRefusedDiscard will not retarget: it carries a Will,
		// so nothing here can prove whose Will a retarget would overwrite.
		e.sess.deferredDisconnect = &mqttDeferredDisconnect{
			disconnectedAt: time.Now().Unix(), expiryInterval: 0,
			will: &mqttWill{topic: []byte("rollback/declined/will")},
		}
		e.sess.mu.Unlock()

		var got rollbackResult
		e.asm.mu.Lock()
		e.asm.testRollbackDone = func(_ *mqttSession, r rollbackResult) { got = r }
		e.asm.mu.Unlock()

		armed := false
		restore := setMQTTTestClearWindowHook(func(sess *mqttSession, window string) {
			if sess != e.sess || window != mqttClearWindowPreFence || armed {
				return
			}
			armed = true
			sess.mu.Lock()
			testMarkUncertainLocked(sess, sess.seq, testAmbiguousBodyLocked(sess, "ambiguous"))
			sess.mu.Unlock()
		})
		defer restore()

		e.asm.rollbackAcceptedSession(e.s, e.sess, e.sess.idHash, candidate)

		if !armed {
			t.Fatal("the pre-fence window never opened, so the refusal was not exercised")
		}
		if got == rollbackParked {
			t.Fatal("the recovery DECLINED, so the caller must not report the parked outcome")
		}
		if got != rollbackIndeterminate {
			t.Fatalf("nothing was deleted and the candidate still holds the subject: got %v, "+
				"want rollbackIndeterminate", got)
		}
		if e.resident() {
			t.Fatal("a declined repark leaves the caller its historical outcome: the session is removed")
		}
	})
}

// TestRollbackUnsetResultTakesNoTimerAction is T16 (M19).
//
// The value that means "nothing was classified" is unreachable by construction, which is
// exactly why its arm is behavior rather than decoration: the default arm CANCELS, so
// without it an impossible value silently disarms a deadline instead of being reported.
func TestRollbackUnsetResultTakesNoTimerAction(t *testing.T) {
	a, _ := testNewTeardownEnv(t)
	cidHash := getHash("rollback-unset")

	armed := func() bool {
		a.asm.mu.RLock()
		defer a.asm.mu.RUnlock()
		_, ok := a.asm.pendingExpiries[cidHash]
		return ok
	}
	a.asm.mu.Lock()
	a.asm.scheduleSessionExpiryLocked(a.s, cidHash, time.Hour)
	a.asm.mu.Unlock()
	if !armed() {
		t.Fatal("precondition: the timer must be armed")
	}

	a.asm.mu.Lock()
	a.asm.reconcileRollbackTimerLocked(a.s, cidHash, rollbackUnset, nil)
	a.asm.mu.Unlock()

	if !armed() {
		t.Fatal("an unclassified rollback must take NO timer action: the deadline it would " +
			"cancel belongs to a session nothing has decided anything about")
	}
	// The paired check that the arm is not simply inert: a superseded result DOES cancel.
	a.asm.mu.Lock()
	a.asm.reconcileRollbackTimerLocked(a.s, cidHash, rollbackSuperseded, nil)
	a.asm.mu.Unlock()
	if armed() {
		t.Fatal("a superseded rollback cancels the stale timer, or this test proves nothing")
	}
}

// TestRetirementWIDlessRecordFailsClosed is T12's record half (M9).
//
// A teardown retires the record BY NAME, so a record carrying no identity cannot be
// retired: the writer refuses to write a watermark its own readers would refuse, and the
// teardown aborts having deleted nothing, leaving the record marked. Such a record can only
// predate the lineage work, and refusing it is the same treatment a generationless record
// already gets at the CONNECT restore.
func TestRetirementWIDlessRecordFailsClosed(t *testing.T) {
	testQuietExpirySweeps(t)
	a, _ := testNewTeardownEnv(t)
	const clientID = "retirement-widless"

	sessA := testProvisionSession(t, a, clientID)
	idHash := sessA.idHash
	_, _, pubRelDur := mqttPubRelNames(idHash, sessA.gen)
	rec, seq := testLoadSessRecord(t, a, idHash)
	if rec == nil {
		t.Fatal("fixture: the provisioned session must have a record")
	}
	rec.WID, rec.PWID = _EMPTY_, _EMPTY_
	if _, err := mqttStoreSessionRecordCAS(&a.asm.jsa, a.asm.domainTk, rec, idHash, seq); err != nil {
		t.Fatalf("staging the WID-less record: %v", err)
	}
	a.asm.removeSession(sessA, true)

	// Restored from that record, so the session holds no identity to retire — the shape
	// production reaches, rather than a field cleared by hand on a live object.
	restored, existed, err := a.asm.createOrRestoreSession(clientID, a.s.getOpts())
	if err != nil {
		t.Fatalf("the restore must succeed: %v", err)
	}
	if !existed {
		t.Fatal("the record exists, so this must be a resume")
	}

	cerr := restored.clear(false)
	if cerr == nil || errors.Is(cerr, errMQTTClearSuperseded) {
		t.Fatalf("clear() = %v, want a plain failure: nothing can name the record this "+
			"deletion would retire", cerr)
	}
	if !strings.Contains(cerr.Error(), "retirement") {
		t.Errorf("the refusal must name the retirement it could not record: %v", cerr)
	}
	if !testConsumerExists(t, a, mqttOutStreamName, pubRelDur) {
		t.Error("a teardown that could not fence must delete nothing")
	}
	ps, _ := testLoadSessRecord(t, a, idHash)
	if ps == nil || ps.Teardown == nil {
		t.Fatalf("the refused teardown must leave the record MARKED, got %+v", ps)
	}
}
