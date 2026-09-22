package mqtt

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Tests for the durable generation-retirement watermark and its three readers: the
// winner's release-superseded arm, the CONNECT restore, and the orphan sweep. A fourth,
// the record-absent create that inherited the retired record's identity, was deleted by
// the write-lineage audit; that create still READS the watermark, but only to refuse a
// corrupt one. The window they close is the claim-won deletion window: a teardown deletes on the
// strength of a won claim, and an unconditional foreign write — excluded by no CAS, see
// the same-base boundary in docs/CONFORMANCE.md — can evict the marker off the record's
// own subject afterwards, leaving a record that
// names resources the winner destroyed and no marker for anyone to adopt.
//
// The shape is STAGED rather than produced by a real foreign publisher: the winner is
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
// CASing at atSeq — the shape a foreign write leaves: a record landing on the subject
// after the marker did, which MaxMsgsPer:1 evicts the marker for. Staged with a CAS at
// the marker's own sequence because that is deterministic here; the reachable producer is
// an UNCONDITIONAL write (the same-base boundary, docs/CONFORMANCE.md).
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
		// The foreign write: B's save lands on the subject now, evicting A's marker. It preserves the generation, so it names exactly
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

// TestRetirementLeavesAFreshGenerationAlone is test 2, the discriminating pair for the
// generation comparison in condemnRetiredSurvivor: a record that superseded the release
// with a DIFFERENT generation is outside the retirement's reach, because a watermark
// retires ONE GENERATION and not a client ID. Condemning it would be collateral damage,
// not repair.
//
// The claim is deliberately that narrow. Nothing here shows the fresh generation is held
// by anyone (F-084), and nothing needs to: the conclusion is a refusal to destroy.
func TestRetirementLeavesAFreshGenerationAlone(t *testing.T) {
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
		t.Fatalf("the fresh generation's record was overwritten: generation %q, want %q", ps.Gen, successorGen)
	}
	if ps.Teardown != nil {
		t.Fatal("a fresh-generation record was condemned: the watermark retires one generation, " +
			"not a client ID")
	}

	// It resumes, which is the client-visible half of the same claim.
	sess, existed, cerr := b.asm.createOrRestoreSession(clientID, b.s.getOpts())
	if cerr != nil {
		t.Fatalf("CONNECT over the successor: %v", cerr)
	}
	if !existed || sess.gen != successorGen {
		t.Errorf("the fresh generation must resume: existed=%v gen=%q want true/%q", existed, sess.gen, successorGen)
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
		ID: clientID, Gen: retiredGen, MarkerSeq: recSeq,
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
	// The fence ADVANCES rather than deduplicating to a first value, and MarkerSeq alone
	// is what says so. The watermark used to carry the retired record's identity too, and
	// that field was removed: a write identity names a record's bytes rather than one
	// occurrence of them, so it could not tell WHICH occurrence a retirement removed. See
	// docs/ROADMAP.md's vanished-record bullet for why no reader could be built on it.
	// And a create into the now-empty subject descends from NOTHING: it reads the
	// watermark — the corrupt arm is still a refusal — but draws no identity from it.
	third, _, cerr := a.asm.createOrRestoreSession(clientID, a.s.getOpts())
	if cerr != nil {
		t.Fatalf("creating after the second teardown: %v", cerr)
	}
	if err := testSessSave(t, third); err != nil {
		t.Fatalf("the create's first save: %v", err)
	}
	if created, _ := testLoadSessRecord(t, a, idHash); created == nil || created.PWID != _EMPTY_ {
		t.Fatalf("the create into an emptied subject must descend from NOTHING: %+v", created)
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
	if r3.Gen != r2.Gen || r3.MarkerSeq != r2.MarkerSeq {
		t.Fatalf("an older retirement regressed the fence: %+v, want %+v", r3, r2)
	}

	// (c) A retirement of the SAME generation at a HIGHER marker sequence advances
	// it. A generation-equality early return would call this already done and leave
	// the fence naming the older claim.
	if err := second.recordGenRetirement(&mqttTeardownTarget{id: clientID, idHash: idHash, gen: r2.Gen},
		"newer", mqttNewRecordWID(), r2.MarkerSeq+1); err != nil {
		t.Fatalf("a newer same-generation retirement: %v", err)
	}
	r4, _, _ := testLoadRetirement(t, a, idHash)
	if r4.MarkerSeq != r2.MarkerSeq+1 {
		t.Fatalf("a newer same-generation retirement did not advance the fence: MarkerSeq %v, want %v",
			r4.MarkerSeq, r2.MarkerSeq+1)
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
	markerSeq, _, claim, cerr := sessA.claimTeardown(tok, ps, seq, sessA.wid, nil)
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
	_, adoptedNonce, claim, cerr := sessA.claimTeardown(tok, base, seq, sessA.wid, nil)
	if claim != mqttTeardownClaimWon {
		t.Fatalf("claimTeardown = %v, err=%v", claim, cerr)
	}
	if terr := sessA.endRecordTx(tok); terr != nil {
		t.Fatalf("endRecordTx: %v", terr)
	}

	// B adopts. Between its deletions and its release, the foreign write lands.
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := testNewTeardownEnv(t)
			const clientID = "watermark-malformed"

			sessA := testProvisionSession(t, a, clientID)
			idHash, retiredGen := sessA.idHash, sessA.gen
			_, _, pubRelDur := mqttPubRelNames(idHash, retiredGen)
			_, recSeq := testLoadSessRecord(t, a, idHash)

			// The baseline is VALID in every field, so each row's mutation is the only
			// thing that can make it corrupt. A baseline already malformed in some other
			// field would make every row pass for the wrong reason.
			bad := &mqttGenRetirement{
				ID: clientID, Gen: retiredGen, MarkerSeq: recSeq, Nonce: "planted",
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

// The ten broker-owned streams are ONE recreation unit, asserted from both
// directions: a bundle missing ANY single member is refused, and a complete
// bundle whose members disagree on the recovery epoch is refused too.
//
// This replaces the two-subtest $MQTT_sess/$MQTT_teardown coupling fixture. The
// pair was only ever one instance of the hazard, and its second subtest asserted
// the OPPOSITE of what the contract now says: recreating $MQTT_teardown alone
// used to be ADMITTED, silently losing every open fence. That admission is the
// residual F-066 withdraws, so the assertion is inverted rather than deleted —
// and it is inverted for all nine positions, not just that one.
func TestBrokerStreamBundleRefusesAPartialAccount(t *testing.T) {
	for _, missing := range mqttTestBrokerStreamNames {
		t.Run("without "+missing, func(t *testing.T) {
			s1, c1, nc := testRetirementBringUp(t)
			asm1, err := s1.getOrCreateMQTTAccountSessionManager(c1)
			if err != nil {
				t.Fatalf("first bring-up: %v", err)
			}
			// Every member carries the adopted epoch: that stamp is what makes the
			// nine one unit, and a fixture whose premise is unchecked proves nothing.
			for _, name := range mqttTestBrokerStreamNames {
				si, lerr := asm1.jsa.lookupStream(name)
				if lerr != nil || si == nil {
					t.Fatalf("%s must be provisioned: %+v %v", name, si, lerr)
				}
				if got := si.Config.Metadata[mqttSessEpochMetaKey]; got != asm1.sessStreamEpoch {
					t.Fatalf("%s carries %q, want the adopted %q", name, got, asm1.sessStreamEpoch)
				}
			}

			js, ctx := testRetirementJS(t, nc)
			if derr := js.DeleteStream(ctx, missing); derr != nil {
				t.Fatalf("delete %s: %v", missing, derr)
			}

			s2, c2, _ := testRetirementBringUpOn(t, nc, "node2")
			_, err = s2.getOrCreateMQTTAccountSessionManager(c2)
			if err == nil {
				t.Fatalf("an account holding nine of the ten broker-owned streams must fail "+
					"initialization; %s was deleted and the rest were silently reprovisioned", missing)
			}
			// The PREFLIGHT's reason specifically. Without this the test passes on any
			// refusal at all — including a later epoch mismatch — which would make it
			// blind to the preflight being removed entirely.
			for _, want := range []string{"of the 10 broker-owned streams", "ONE recreation unit"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal must name %q, got: %v", want, err)
				}
			}
			// The two LISTS, not just the name somewhere in the message: the
			// refusal also enumerates all nine as the streams to delete, so a
			// bare Contains passes even when both vectors are empty.
			gotPresent, gotAbsent := testBundleRefusalLists(t, err)
			if len(gotAbsent) != 1 || gotAbsent[0] != missing {
				t.Errorf("the refusal's absent list is %v, want exactly [%s]", gotAbsent, missing)
			}
			if len(gotPresent) != len(mqttTestBrokerStreamNames)-1 {
				t.Errorf("the refusal's present list is %v, want the other eight streams", gotPresent)
			}
			// And it must not have recreated what it refused: a preflight that creates
			// is the defect this whole mechanism exists to prevent. Checked through
			// the FIRST manager's connection, because the refused one published no
			// manager to look through — which is itself asserted here.
			if len(s2.mqtt.sessmgr.sessions) != 0 {
				t.Fatalf("a refused initialization must publish no manager, found %d", len(s2.mqtt.sessmgr.sessions))
			}
			if si, lerr := asm1.jsa.lookupStream(missing); si != nil {
				t.Fatalf("the refused startup recreated %s (err=%v)", missing, lerr)
			}
		})
	}
}

// A COMPLETE bundle in which one member carries a valid but FOREIGN epoch is
// refused by that stream's validator.
//
// This is the coverage a partial-set test cannot give: the preflight refuses a
// proper subset before any validator runs, so without this fixture the epoch
// comparison would never be exercised end to end at all.
func TestBrokerStreamBundleRefusesAForeignEpochMember(t *testing.T) {
	for _, foreign := range mqttTestBrokerStreamNames {
		if foreign == mqttSessStreamName {
			// $MQTT_sess MINTS the epoch, so "foreign" is not a state it can be in:
			// whatever well-formed value it carries becomes the account's. Its own
			// malformed-stamp arm is TestSessionManagerEpochMissingFailsInit.
			continue
		}
		t.Run(foreign+" from another incarnation", func(t *testing.T) {
			s, c, nc := testRetirementBringUp(t)
			testBundleThenPoisonEpoch(t, s, c, nc, foreign,
				map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()})

			s2, c2, _ := testRetirementBringUpOn(t, nc, "node2")
			_, err := s2.getOrCreateMQTTAccountSessionManager(c2)
			if err == nil {
				t.Fatalf("%s carrying another incarnation's recovery epoch must fail initialization: "+
					"its records reference identities the rest of the bundle no longer issues", foreign)
			}
			for _, want := range []string{foreign, mqttSessEpochMetaKey, "different incarnation"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal must name %q, got: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), "of the 10 broker-owned streams") {
				t.Fatalf("the refusal came from the count rule, so the epoch comparison was never reached: %v", err)
			}
		})
	}
}

// TestRetirementBodyUsesTheLowercaseKey is T12's raw-body half (M18).
//
// A struct tag typo is invisible to every reader and writer in this tree, because they all
// agree on the typo, and a replicated round trip would not notice either. The PERSISTED KEY
// is the contract, so it is asserted against the raw body. Re-pointed at "mseq" and "gen"
// when the rwid key was removed; the principle is the field's, not any one field's.
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
	for _, key := range []string{`"mseq":`, `"gen":"`} {
		if !strings.Contains(string(smsg.Data), key) {
			t.Fatalf("the persisted watermark must carry the lowercase %s key: %s", key, smsg.Data)
		}
	}
	if strings.Contains(string(smsg.Data), `"rwid"`) {
		t.Fatalf("the retired-record identity was removed and must not be written: %s", smsg.Data)
	}
}

// ---------------------------------------------------------------------------

// TestRetirementWIDlessRecordFailsClosed is T12's record half (M9).
//
// A teardown retires the record BY NAME, so a record carrying no identity cannot be
// retired: recordGenRetirement refuses on an explicit precondition, before the watermark is
// built and before any deletion, and the teardown aborts having deleted nothing, leaving
// the record marked. The refusal used to come from the watermark's own validator, because
// the identity was a required stored field; the field was removed and the precondition
// stayed, since nothing else in the teardown path checks WID or PWID. Such a record can only
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
