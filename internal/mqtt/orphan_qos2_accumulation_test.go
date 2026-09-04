//go:build mqttport_experimental_shared_qos2

// Fixtures for F-064's accumulation half: settled affine QoS 2 state that nothing
// reaped while the owning session's record was live.
//
// These tests do NOT run in parallel, and that is load-bearing rather than incidental.
// Several install mqttTestDeleteMsgHookV, which is package-level, and the orphan-sweep
// environment deliberately waits out its account manager's startup sweep for the same
// reason. A parallel test beside one of these would observe another's injected failure.

package mqtt

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// testLiveAffineGeneration gives the env's session its own affine generation and
// persists it, so the claim written by testValidQ2AffClaim for the returned pair is one
// the sweep sees as REFERENCED.
//
// It then asserts the two things that make such a fixture reach the arm it names,
// because both failure modes are silent and both produce a passing test for the wrong
// reason:
//
//   - buildSessionRefs must report NO affine indeterminacy. A non-nil second return
//     makes sweepOrphanedQoS2State suppress the affine CLAIM pass, so the arm is never
//     entered and every assertion about "nothing happened" holds trivially.
//   - refs.hasAffine must be TRUE for the pair. If it is false the claim takes the
//     ABSENT-winner path instead, which can produce the same observable outcome by a
//     completely different route.
//
// Every test in this file goes through this helper, so neither check can be omitted.
func (e *testOrphanEnv) testLiveAffineGeneration(t *testing.T) (idHash, affGen string) {
	t.Helper()
	e.sess.mu.Lock()
	if e.sess.q2AffGen == _EMPTY_ {
		e.sess.q2AffGen = mqttNewQ2AffGen()
	}
	idHash, affGen = e.sess.idHash, e.sess.q2AffGen
	e.sess.mu.Unlock()
	e.persist(t)

	refs, affineBad, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	if affineBad != nil {
		t.Fatalf("the fixture's own session record makes the affine references INDETERMINATE (%v), so the "+
			"sweep would suppress the claim pass entirely and every assertion below would hold for the "+
			"wrong reason", affineBad)
	}
	if !refs.hasAffine(idHash, affGen) {
		t.Fatalf("refs.hasAffine(%q, %q) is false, so this claim would take the ABSENT-winner path "+
			"rather than the live-generation arm this test is about", idHash, affGen)
	}
	return idHash, affGen
}

// liveGroupDurable creates a real shared group consumer and stores a real source
// message on it, so a claim written against them is genuinely retained rather than
// reapable for want of a source.
func (e *testOrphanEnv) liveGroupDurable(t *testing.T, name string) (groupDur string, sseq uint64) {
	t.Helper()
	groupDur = mqttSharedConsumerDurablePrefix + name
	topic := mqttStreamSubjectPrefix + name + ".topic"
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: ConsumerConfig{
			Durable: groupDur, FilterSubject: topic,
			DeliverSubject: mqttSharedSubDeliverPrefix + name, DeliverGroup: "g",
			AckPolicy: AckExplicit, DeliverPolicy: DeliverNew, MaxAckPending: 64,
		},
	}); err != nil {
		t.Fatalf("create group durable: %v", err)
	}
	resp, err := e.asm.jsa.storeMsg(topic, 0, []byte("source"))
	if err != nil {
		t.Fatalf("store the source message: %v", err)
	}
	return groupDur, resp.Sequence
}

// putClaimFor stores a claim and returns its subject.
func (e *testOrphanEnv) putClaimFor(t *testing.T, c *mqttQ2AffClaim) string {
	t.Helper()
	subject := mqttQ2AffClaimSubject(c.GroupDurable, c.SourceSeq)
	if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, c); err != nil {
		t.Fatalf("store claim on %q: %v", subject, err)
	}
	return subject
}

func (e *testOrphanEnv) claimOn(t *testing.T, subject string) *mqttQ2AffClaim {
	t.Helper()
	c, _, err := e.asm.loadQ2AffClaim(subject)
	if err != nil {
		t.Fatalf("load claim %q: %v", subject, err)
	}
	return c
}

// sweepAffineClaims drives ONE assignment-reconciliation pass. One, deliberately: the
// live-generation arm has no quarantine gate, so a fixture that needed a second pass
// would be asserting the opposite of what the arm promises.
// countClaimLoads installs a COUNTING wrapper over the claim load that delegates to the
// real one, and returns a reader for the count on subject.
//
// This is the discriminating oracle for the arms that DECLINE rather than write. A reap
// of a `Lost` or nonterminal claim merely returns without mutating, so a
// survival-only assertion passes whether the arm called the reaper or not — the count is
// what tells "never attempted" apart from "attempted and refused".
func (e *testOrphanEnv) countClaimLoads(t *testing.T, subject string) func() int {
	t.Helper()
	var n int
	var mu sync.Mutex
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		if s == subject {
			mu.Lock()
			n++
			mu.Unlock()
		}
		return e.asm.jsa.loadQ2AffClaim(s)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQ2AffClaimLoad = nil
		e.asm.mu.Unlock()
	})
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		return n
	}
}

// ---------------------------------------------------------------------------
// Class A: terminal claims of a LIVE generation are reaped
// ---------------------------------------------------------------------------

// A claim of a live generation that is already durably terminal — AffineDone with the
// source proven — is REAPED. Before F-064's accumulation half the live reference made
// the pass skip it, so it survived for the life of a session that never resumed.
//
// Paired with M1.
func TestOrphanSweepReapsATerminalAssignmentOfALiveGeneration(t *testing.T) {
	e := testNewOrphanEnv(t, "live-gen-affinedone")
	e.setGraces(t, 0, 0)
	idHash, affGen := e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, "LIVEDONE")

	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	c.Phase, c.AffineDone, c.SourceAcked = mqttQ2AffMaterialized, true, true
	subject := e.putClaimFor(t, c)

	e.sweepAffineClaims(t, 16)

	if got := e.claimOn(t, subject); got != nil {
		t.Fatalf("a terminal assignment of a LIVE generation was not reaped in ONE pass: %+v; "+
			"that is the accumulation F-064's remaining half exists to close", got)
	}
}

// The same for the other terminal disposition, Discarded — the shape the owner's own
// sourceless arm leaves behind when its response is lost.
//
// Paired with M1.
func TestOrphanSweepReapsADiscardedAssignmentOfALiveGeneration(t *testing.T) {
	e := testNewOrphanEnv(t, "live-gen-discarded")
	e.setGraces(t, 0, 0)
	idHash, affGen := e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, "LIVEDISC")

	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	c.Discarded, c.SourceAcked = true, true
	subject := e.putClaimFor(t, c)

	e.sweepAffineClaims(t, 16)

	if got := e.claimOn(t, subject); got != nil {
		t.Fatalf("a discarded assignment of a LIVE generation with its source proven was not reaped: %+v", got)
	}
}

// `Lost` is retained AND NOT EVEN ATTEMPTED. The survival half of this assertion holds
// for any implementation, because reapable() refuses Lost unconditionally — so the LOAD
// COUNT is what actually pins the arm: it must be zero.
//
// Paired with M2, which is detectable only through that count.
func TestOrphanSweepNeverTouchesALostAssignmentOfALiveGeneration(t *testing.T) {
	e := testNewOrphanEnv(t, "live-gen-lost")
	e.setGraces(t, 0, 0)
	idHash, affGen := e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, "LIVELOST")

	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	c.Phase, c.Lost, c.SourceAcked = mqttQ2AffMaterialized, true, true
	subject := e.putClaimFor(t, c)

	loads := e.countClaimLoads(t, subject)
	e.sweepAffineClaims(t, 16)

	if got := e.claimOn(t, subject); got == nil {
		t.Fatal("a LOST assignment was deleted; it is the only durable evidence that a QoS 2 message " +
			"the broker accepted could not be proven delivered")
	}
	// The load the assertion above performs is outside the sweep, so the arm itself must
	// have loaded this subject zero times.
	if n := loads(); n != 1 {
		t.Fatalf("the Lost arm performed %d claim load(s) for %q inside the sweep; it must attempt NOTHING, "+
			"because reapable() refuses Lost unconditionally and a call is a pure round trip", n-1, subject)
	}
}

// The NONTERMINAL population of a live generation is left entirely alone, and this is
// the positive statement of the boundary consult 2 settled: no fact this pass could
// write there is both true and terminal, and writing Discarded makes a concurrent
// materializer withdraw an unpublished copy for a session that never ended.
//
// Paired with M3, which re-adds the refuted arm.
func TestOrphanSweepLeavesANonterminalLiveGenerationClaimEntirelyAlone(t *testing.T) {
	e := testNewOrphanEnv(t, "live-gen-nonterminal")
	e.setGraces(t, 0, 0)
	idHash, affGen := e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, "LIVENONTERM")

	// The W-c shape: claimed, SourceAcked false, no copy — and the source deleted, which
	// is exactly the state the refuted remedy would have settled.
	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	subject := e.putClaimFor(t, c)
	if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}

	loads := e.countClaimLoads(t, subject)
	e.sweepAffineClaims(t, 16)

	got := e.claimOn(t, subject)
	if got == nil {
		t.Fatal("a NONTERMINAL claim of a live generation was reaped")
	}
	if got.Discarded || got.SourceAcked || got.Lost || got.AffineDone || got.Phase != mqttQ2AffClaimed {
		t.Fatalf("the sweep settled a NONTERMINAL claim of a LIVE generation: %+v. That write makes a "+
			"concurrent materializer withdraw an unpublished copy for a session that never ended, and it "+
			"falsifies the [MQTT-4.8.2-5] premise q2AffCopyIsDisowned states", got)
	}
	if n := loads(); n != 1 {
		t.Fatalf("the arm performed %d claim load(s) for a nonterminal claim inside the sweep; it must attempt "+
			"nothing at all, and a survival-only assertion cannot tell that apart from an attempt that declined", n-1)
	}
}

// The reap happens even at a zero mutation budget, because the budget bounds charged
// transition ATTEMPTS and this arm charges none: the reap may write nothing, and an
// inventory headed by retained-by-design claims would otherwise consume the whole budget
// on the same prefix every pass.
//
// Paired with M33.
func TestOrphanSweepLiveGenerationReapsChargeNoBudget(t *testing.T) {
	e := testNewOrphanEnv(t, "live-gen-nobudget")
	e.setGraces(t, 0, 0)
	idHash, affGen := e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, "LIVENOBUDGET")

	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	c.Phase, c.AffineDone, c.SourceAcked = mqttQ2AffMaterialized, true, true
	subject := e.putClaimFor(t, c)

	e.sweepAffineClaims(t, 0)

	if got := e.claimOn(t, subject); got != nil {
		t.Fatalf("a terminal claim was not reaped at budget 0: %+v. The budget bounds charged transition "+
			"attempts, and this arm charges none", got)
	}
}

// A claim whose shape is not one this broker writes is REFUSED by the checked reaper
// rather than deleted, and the refusal happens against the record the reaper itself
// loads — not the one the inventory validated.
//
// Paired with M21, which reverts this call site to the unchecked reaper.
func TestOrphanSweepLiveGenerationReapIsChecked(t *testing.T) {
	e := testNewOrphanEnv(t, "live-gen-checked-reap")
	e.setGraces(t, 0, 0)
	idHash, affGen := e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, "LIVECHECKED")

	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	c.Phase, c.AffineDone, c.SourceAcked = mqttQ2AffMaterialized, true, true
	subject := e.putClaimFor(t, c)

	// Legitimate for the inventory, and rewritten to an UNKNOWN PHASE — the one
	// unsupported form the unchecked reaper would happily delete, since
	// q2AffClaimUsableForDecision does not look at the phase — by the time the reaper
	// loads it. A malformed nonce would NOT discriminate here: the unchecked reaper
	// already applies that validator on its own initial load.
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		cur, seq, err := e.asm.jsa.loadQ2AffClaim(s)
		if s == subject && cur != nil {
			cur.Phase = mqttQ2AffPhase("not-a-phase-this-broker-writes")
		}
		return cur, seq, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQ2AffClaimLoad = nil
		e.asm.mu.Unlock()
	})

	e.sweepAffineClaims(t, 16)

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = nil
	e.asm.mu.Unlock()
	if got := e.claimOn(t, subject); got == nil {
		t.Fatal("the reap deleted a claim whose SHAPE is not one this broker writes. The inventory validated " +
			"an earlier read; the reaper must refuse against the record it loads itself")
	}
}

// ---------------------------------------------------------------------------
// The settleable-register predicate
// ---------------------------------------------------------------------------

// Each clause of mqttQ2AffSettleableRegister refuses independently, and the QoS,
// cancellation and durable-stream clauses each get their own otherwise-actionable
// record — so removing any ONE of them is observable.
//
// Paired with M30a, M30b, M30c and M25.
func TestOrphanSweepSettleableRegisterRefusesEachUnsupportedShape(t *testing.T) {
	idHash, gen := getHash("settleable"), mqttNewSessGen()
	affGen := mqttNewQ2AffGen()
	_, _, affDur := mqttQ2AffineNames(idHash, affGen)
	hash := mqttQ2AffClaimHash("$MQTT_SHARED_S", 9)
	copySubj := mqttQ2AffCopySubject(idHash, affGen, hash, testQ2AffFixtureNonce)

	// Named locals rather than two inline composite literals in one multi-value
	// return, and that is a formatting fix rather than a style preference: gofmt
	// and golangci-lint's formatter indent the inline form OPPOSITELY (a change in
	// gofmt between the Go release the toolchain runs and the one golangci-lint was
	// built with), so whichever way it was written, one of the two rewrote it and
	// re-broke the other. Assignments format identically under both.
	base := func() (*mqttInflightRecord, *mqttInflightName) {
		rec := &mqttInflightRecord{State: mqttInflightPublish,
			PI: 7, Dur: affDur, Seq: 42, QoS: 2, Nonce: mqttNewInflightNonce(),
			Q2AffClaim: mqttQ2AffClaimSubject("$MQTT_SHARED_S", 9), Q2AffCopy: copySubj,
			Retired: true,
		}
		name := &mqttInflightName{
			idHash: idHash, gen: gen, pi: 7,
			subject: mqttInflightSubject(idHash, gen, 7),
		}
		return rec, name
	}
	if rec, name := base(); mqttQ2AffSettleableRegister(rec, name) != nil {
		t.Fatalf("the baseline record must be ACTIONABLE, or every subcase below refuses for the wrong reason: %v",
			mqttQ2AffSettleableRegister(rec, name))
	}

	for _, tc := range []struct {
		what    string
		breakIt func(*mqttInflightRecord)
	}{
		{"a retired exchange at QoS 1, which has no phase past PUBREC", func(r *mqttInflightRecord) { r.QoS = 1 }},
		{"an affine pair on a non-affine durable, naming a source on the wrong stream",
			func(r *mqttInflightRecord) { r.Dur = idHash + "_" + nuid.Next() }},
		{"retirement absent, so this is a LIVE pre-PUBREC delivery", func(r *mqttInflightRecord) { r.Retired = false }},
		{"a body naming an identifier other than its own subject's", func(r *mqttInflightRecord) { r.PI = 8 }},
		{"no affine assignment reference at all", func(r *mqttInflightRecord) { r.Q2AffClaim = _EMPTY_ }},
	} {
		rec, name := base()
		tc.breakIt(rec)
		if err := mqttQ2AffSettleableRegister(rec, name); err == nil {
			t.Errorf("a register with %s was accepted as settleable; every other clause was left intact, "+
				"so this one must refuse on its own", tc.what)
		}
	}

	// Retired ALONGSIDE Cancelled is a SUPPORTED shape, and the precedence rule decides
	// it: Retired dominates, so the register is settleable exactly as a pure retirement
	// is (F-087 WP3). Refusing it — as this clause once did, calling the two mutually
	// exclusive — retains the register unsettled for the life of a long-lived
	// generation, because a cancellation racing a retirement now leaves both bits
	// durably.
	rec, name := base()
	rec.Cancelled = true
	if err := mqttQ2AffSettleableRegister(rec, name); err != nil {
		t.Errorf("a retired register carrying a cancellation must be settleable under the precedence rule: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Class B: the retired affine register class
// ---------------------------------------------------------------------------

// testRetiredAffineRegister stages the whole triple this arm collects: a real affine
// consumer on $MQTT_out, a real claim, a real copy stored on the claim's own copy
// subject, and a real RETIRED register naming both.
//
// copyLive decides which arm the fixture reaches. With it true the copy stays and the
// register's source classifies Live (arm (b)); with it false the copy is deleted while
// the consumer survives, which is what "acknowledged or authoritatively absent while its
// consumer still holds interest" means, so the source classifies Complete (arm (a)).
func (e *testOrphanEnv) testRetiredAffineRegister(t *testing.T, pi uint16, name string,
	copyLive bool, shape func(*mqttQ2AffClaim)) (claimSubject, copySubject, regSubject string, regSeq uint64) {

	t.Helper()
	idHash, affGen := e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, name)

	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	c.Phase = mqttQ2AffMaterialized
	if shape != nil {
		shape(c)
	}
	claimSubject, copySubject = e.putClaimFor(t, c), c.CopySubject

	// The winner's own affine consumer, through the single constructor, so the source
	// classification sees a real consumer rather than an absence (which would be Lost).
	//
	// Created ONCE per env. Every fixture here uses the same session and therefore the
	// same affine generation and durable name, so a second create would be an update of
	// an existing consumer — and CONSUMER.CREATE-as-update against a concurrent
	// CONSUMER.LIST is exactly the known upstream nats-server race this suite already
	// trips. Skipping the redundant create keeps this change from widening exposure to it
	// more than it has to.
	cfg := buildQ2AffineConsumerConfig(idHash, affGen, time.Minute, mqttDefaultMaxAckPending, false)
	if !e.consumerExists(t, cfg.Durable) {
		if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
			Stream: mqttOutStreamName, Config: cfg,
		}); err != nil {
			t.Fatalf("create the affine consumer: %v", err)
		}
	}

	// The copy, on the claim's own copy subject, so the register's source IS the copy.
	resp, err := e.asm.jsa.storeMsg(copySubject, 0, []byte("copy"))
	if err != nil {
		t.Fatalf("store the copy: %v", err)
	}
	copySeq := resp.Sequence
	if !copyLive {
		if derr := e.asm.jsa.deleteMsg(mqttOutStreamName, copySeq, true); derr != nil {
			t.Fatalf("delete the copy: %v", derr)
		}
	}

	e.sess.mu.Lock()
	sessGen := e.sess.gen
	e.sess.mu.Unlock()
	regSubject = mqttInflightSubject(idHash, sessGen, pi)
	regSeq, err = e.asm.jsa.storeInflightCAS(regSubject, 0, &mqttInflightRecord{State: mqttInflightPublish,
		PI: pi, Dur: cfg.Durable, Seq: copySeq, QoS: 2, Nonce: mqttNewInflightNonce(),
		Q2AffClaim: claimSubject, Q2AffCopy: copySubject, Retired: true,
	})
	if err != nil {
		t.Fatalf("persist the retired register: %v", err)
	}
	return claimSubject, copySubject, regSubject, regSeq
}

// sweepInflight drives ONE in-flight reconciliation pass.
func (e *testOrphanEnv) sweepInflight(t *testing.T, budget int) {
	t.Helper()
	cands, err := e.asm.collectInflightCandidates(e.s)
	if err != nil {
		t.Fatalf("collectInflightCandidates: %v", err)
	}
	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	e.asm.reconcileOrphanInflight(e.s, cands, refs, &budget)
}

// watchRegisterDeletes records every exact-sequence register delete the arm ISSUES, and
// returns a predicate over them.
//
// State is the wrong observable here, and that is measured rather than assumed: a
// paired-suite run of these fixtures went red on
// TestOrphanSweepCollectsARetiredAffineRegisterWhoseCopyIsGone because
// deleteInflightRegister is FIRE-AND-FORGET — it pushes onto the send queue with
// wait=false — so asserting absence immediately races that queue, and asserting PRESENCE
// can pass before a wrongly-issued delete has landed. The existing
// TestMQTTInflightOrphanReconcile documents the same hazard and polls for absence; the
// seam is stronger, because it makes both directions exact instead of only one.
func (e *testOrphanEnv) watchRegisterDeletes(t *testing.T) func(seq uint64) bool {
	t.Helper()
	var mu sync.Mutex
	seen := map[uint64]bool{}
	e.asm.mu.Lock()
	e.asm.testInflightDelete = func(seq uint64) error {
		mu.Lock()
		seen[seq] = true
		mu.Unlock()
		return nil
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testInflightDelete = nil
		e.asm.mu.Unlock()
	})
	return func(seq uint64) bool {
		mu.Lock()
		defer mu.Unlock()
		return seen[seq]
	}
}

// Arm (a): the copy is gone while its consumer still holds interest, so the exchange is
// over and the whole triple is collected. This is the population that used to accumulate
// one claim-plus-register pair per exchange for a session that never resumed.
//
// Paired with M9's arm-(a) sibling.
func TestOrphanSweepCollectsARetiredAffineRegisterWhoseCopyIsGone(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-copy-gone")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 11, "REGGONE", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	if !deleted(regSeq) {
		t.Fatal("the retired register survived although its copy is complete and its claim terminal; " +
			"that pair is what accumulates for a session that never resumes")
	}
	if got := e.claimOn(t, claimSubject); got != nil {
		t.Fatalf("the claim survived alongside its collected register: %+v", got)
	}
}

// Arm (a), the CONTRADICTION: a Complete copy under a NONTERMINAL claim of the same
// assignment is a state no supported path produces, so everything is retained rather
// than trusted to the record-before-ack ordering.
//
// Paired with M32.
func TestOrphanSweepRetainsARetiredAffineRegisterContradictingItsClaim(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-contradiction")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 12, "REGCONTRA", false, nil)
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	if deleted(regSeq) {
		t.Fatal("a register whose copy is COMPLETE while its claim is NONTERMINAL was collected; " +
			"that combination is a contradiction and must be retained, not acted on")
	}
	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the nonterminal claim was reaped on a contradictory reading")
	}
	if got.AffineDone || got.Lost || got.Discarded {
		t.Fatalf("the arm wrote a terminal fact onto a contradictory record: %+v", got)
	}
}

// Arm (b), the population docs/CONFORMANCE.md named: the copy is still LIVE, the claim
// is materialized without AffineDone, and the register is the durable evidence of a
// successful PUBREC that licenses recording it. All three are collected, in order.
//
// Paired with M9.
func TestOrphanSweepSettlesARetiredAffineRegisterAgainstALiveCopy(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-live-copy")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, _, regSeq := e.testRetiredAffineRegister(t, 13, "REGLIVE", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("the copy was not collected (state %v); it is retained on the referenced consumer forever "+
			"for a session that never resumes", st)
	}
	if got := e.claimOn(t, claimSubject); got != nil {
		t.Fatalf("the claim survived: %+v", got)
	}
	if !deleted(regSeq) {
		t.Fatal("the register survived after its copy and claim were collected")
	}
}

// A NON-RETIRED affine register is ignored entirely, and this is the clause the arm's
// whole licence rests on: such a register is an ordinary pre-PUBREC delivery that
// already carries both affine references, so acting on it would record AffineDone and
// delete a copy the client has not completed.
//
// Paired with M25 at runtime, complementing the predicate-level subcase.
func TestOrphanSweepRegisterArmIgnoresANonRetiredAffineRegister(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-not-retired")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, regSubject, regSeq := e.testRetiredAffineRegister(t, 14, "REGLIVEPUB", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	// Rewrite the register as a live PUBLISH-phase delivery, keeping everything else.
	cur, smsg, err := e.asm.jsa.loadInflightRecord(regSubject)
	if err != nil || smsg == nil {
		t.Fatalf("reload the register: %v", err)
	}
	cur.Retired = false
	if _, err := e.asm.jsa.storeInflightCAS(regSubject, regSeq, cur); err != nil {
		t.Fatalf("rewrite the register as non-retired: %v", err)
	}

	e.sweepInflight(t, 16)

	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the copy of a LIVE pre-PUBREC delivery was collected (state %v); the client has not "+
			"completed that exchange", st)
	}
	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim of a live pre-PUBREC delivery was reaped")
	}
	if got.AffineDone {
		t.Fatalf("AffineDone was recorded for a delivery the client has not completed: %+v", got)
	}
	if deleted(regSeq) {
		t.Fatal("a live PUBLISH-phase register was deleted")
	}
}

// The source classifying LOST settles nothing: a destroyed consumer means the delivery
// was lost rather than finished, and this arm must never launder that into a completion.
// The claim is TERMINAL here, so it is exactly the shape arm (a) WOULD collect under
// Complete — without which the mutation would be masked by the contradiction branch.
//
// Paired with M41's sibling and M14.
func TestOrphanSweepRetainsARetiredRegisterWhoseConsumerIsGone(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-consumer-gone")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 15, "REGLOST", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	// Destroy the affine consumer, so the source classifies Lost rather than Complete.
	idHash, affGen := e.testLiveAffineGeneration(t)
	_, _, affDur := mqttQ2AffineNames(idHash, affGen)
	if _, err := e.asm.jsa.deleteConsumer(mqttOutStreamName, affDur, true); err != nil {
		t.Fatalf("delete the affine consumer: %v", err)
	}

	e.sweepInflight(t, 16)

	if deleted(regSeq) {
		t.Fatal("a register whose consumer is GONE was collected; that is a lost delivery, not a finished one")
	}
	if got := e.claimOn(t, claimSubject); got == nil {
		t.Fatal("the claim was reaped on a Lost source reading")
	}
}

// A register whose OWN SOURCE does not prove its carried pair is refused. This is
// F-065's third binding: the record's shape and the claim naming the copy back are both
// satisfied by a register that names some OTHER claim's copy while its own delivery is a
// different message entirely, and crediting that claim would let its copy be
// acknowledged away unpublished.
//
// Paired with M13, which the live-copy fixture cannot pin — there the pair IS proven, so
// dropping the check changes nothing observable.
func TestOrphanSweepRefusesARegisterItsOwnSourceDoesNotProve(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-pair-unproven")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, regSubject, regSeq := e.testRetiredAffineRegister(t, 16, "REGUNPROVEN", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	// Point the register's own delivery at a DIFFERENT live message on the affine
	// consumer's stream, so its source is live but is not the copy it names.
	other, err := e.asm.jsa.storeMsg(copySubject+".other", 0, []byte("someone else's copy"))
	if err != nil {
		t.Fatalf("store the decoy: %v", err)
	}
	cur, smsg, err := e.asm.jsa.loadInflightRecord(regSubject)
	if err != nil || smsg == nil {
		t.Fatalf("reload the register: %v", err)
	}
	cur.Seq = other.Sequence
	if _, err := e.asm.jsa.storeInflightCAS(regSubject, regSeq, cur); err != nil {
		t.Fatalf("repoint the register: %v", err)
	}

	e.sweepInflight(t, 16)

	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the named copy was collected (state %v) on a register its own source does not prove", st)
	}
	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was reaped on an unproven pair")
	}
	if got.AffineDone {
		t.Fatalf("AffineDone was credited to a claim on a register its own source does not prove: %+v; "+
			"that is how another assignment's copy gets acknowledged away unpublished", got)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted although its pair is unproven")
	}
}

// ---------------------------------------------------------------------------
// Arm (b)'s six steps: the ORDERING is the correctness property
// ---------------------------------------------------------------------------

// AffineDone must be recorded BEFORE the copy is deleted. The delete hook inspects the
// claim at the moment the copy delete fires, which is the only way to observe an
// ordering rather than an end state: reversing the two steps leaves the same final state
// on the happy path and differs only in what a crash between them would leave.
//
// Paired with M10.
func TestOrphanSweepRegisterArmRecordsBeforeItDeletesTheCopy(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-order")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, _ := e.testRetiredAffineRegister(t, 17, "REGORDER", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })

	var sawDoneAtCopyDelete, copyDeleted bool
	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream == mqttOutStreamName {
			copyDeleted = true
			if c, _, err := e.asm.jsa.loadQ2AffClaim(claimSubject); err == nil && c != nil && c.AffineDone {
				sawDoneAtCopyDelete = true
			}
		}
		return nil
	})
	defer restore()

	e.sweepInflight(t, 16)
	restore()

	if !copyDeleted {
		t.Fatal("the copy delete never fired, so this fixture proves nothing about ordering")
	}
	if !sawDoneAtCopyDelete {
		t.Fatal("the copy was deleted while its claim did NOT yet carry AffineDone. Recording first is what " +
			"makes an absent copy under a claim without AffineDone mean 'never acked by the delivery' on " +
			"every broker; reversing it makes that reading ambiguous")
	}
}

// A copy delete that cannot be proven RETAINS the register, because the steps after it
// depend on the copy being gone. The state assertions alone do not discriminate the
// fire-and-forget mutation — with a failed delete the copy is still present, so the reap
// returns retained and the register survives either way — so the load count after step 3
// is what pins it: the arm must STOP at step 4.
//
// Paired with M11.
func TestOrphanSweepKeepsTheRegisterWhenTheCopyDeleteIsUnproven(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-copy-delete-fails")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, _, regSeq := e.testRetiredAffineRegister(t, 18, "REGCOPYFAIL", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream == mqttOutStreamName {
			return fmt.Errorf("staged copy-delete failure")
		}
		return nil
	})
	defer restore()

	e.sweepInflight(t, 16)
	restore()

	got := e.claimOn(t, claimSubject)
	if got == nil || !got.AffineDone {
		t.Fatalf("AffineDone must be recorded BEFORE the copy delete is attempted, so a failed delete leaves "+
			"it on the record: %+v", got)
	}
	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the copy is not present after a staged delete failure (state %v); the fixture is not "+
			"exercising the window it names", st)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted although the copy delete could not be proven. It is the only " +
			"remaining carrier of this exchange's claim reference, so an unproven step must retain it")
	}
}

// The register is deleted only on a PROVEN reap. With the claim delete failing, the reap
// is indeterminate and the register must survive — without the typed outcome this is
// indistinguishable from success and the register would be deleted while its claim
// remains.
//
// Paired with M12.
func TestOrphanSweepKeepsTheRegisterWhenTheReapDoesNotComplete(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-reap-fails")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 19, "REGREAPFAIL", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream == mqttQoS2AssignStreamName {
			return fmt.Errorf("staged claim-delete failure")
		}
		return nil
	})
	defer restore()

	e.sweepInflight(t, 16)
	restore()

	if got := e.claimOn(t, claimSubject); got == nil {
		t.Fatal("the claim was deleted although its delete was staged to fail")
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted on an INDETERMINATE reap. Only a proven reap licenses step 6, " +
			"because the register is the only proof of step 3's licence")
	}
}

// A retired register implies a MATERIALIZED claim: the register is evidence of a
// successful PUBREC, and the copy cannot have reached the client before its claim
// reached that phase. `Retired` register + live proven copy + `claimed` claim is a
// cross-record combination no supported path emits, and neither record proves it alone.
//
// Paired with M17.
func TestOrphanSweepRefusesARetiredRegisterAgainstAClaimedClaim(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-claimed-phase")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, _, regSeq := e.testRetiredAffineRegister(t, 20, "REGCLAIMED", true,
		func(c *mqttQ2AffClaim) { c.Phase, c.SourceAcked = mqttQ2AffClaimed, true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was reaped on a cross-record combination no supported path emits")
	}
	if got.AffineDone {
		t.Fatalf("AffineDone was recorded against a claim still at phase %q: %+v. A retired register implies "+
			"a materialized claim, and neither record proves that alone", got.Phase, got)
	}
	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the copy was collected on an unsupported cross-record shape (state %v)", st)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted on an unsupported cross-record shape")
	}
}

// Step 1's exact-sequence reload: a register a resume has REWRITTEN since the discovery
// walk must be left alone. The assertion is on the OLD claim and copy — the ones the
// stale arm would have mutated — because step 6 targets the original sequence either
// way, so the replacement's survival does not discriminate.
//
// Paired with M15.
func TestOrphanSweepRegisterArmRevalidatesTheExactSequence(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-exact-seq")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, regSubject, regSeq := e.testRetiredAffineRegister(t, 21, "REGEXACT", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })

	cands, err := e.asm.collectInflightCandidates(e.s)
	if err != nil {
		t.Fatalf("collectInflightCandidates: %v", err)
	}
	// A resume rewrites the register AFTER the walk listed it, moving its sequence.
	cur, smsg, err := e.asm.jsa.loadInflightRecord(regSubject)
	if err != nil || smsg == nil {
		t.Fatalf("reload the register: %v", err)
	}
	if _, err := e.asm.jsa.storeInflightCAS(regSubject, regSeq, cur); err != nil {
		t.Fatalf("rewrite the register: %v", err)
	}

	refs, _, err := e.asm.buildSessionRefs()
	if err != nil {
		t.Fatalf("buildSessionRefs: %v", err)
	}
	budget := 16
	e.asm.reconcileOrphanInflight(e.s, cands, refs, &budget)

	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the arm reaped a claim from a STALE register snapshot")
	}
	if got.AffineDone {
		t.Fatalf("the arm mutated the claim from a stale register snapshot: %+v", got)
	}
	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the arm deleted the copy from a stale register snapshot (state %v)", st)
	}
}

// An `AffineDone ∧ Lost` record is REFUSED, not repaired. This is the one shape where
// the veto's PLACEMENT rather than its presence is what matters: `mergeProven` clears
// `Lost` on such a pair by itself, so a check evaluated after it sees a record that
// looks legitimate and lets the CAS land — silently normalising a shape this broker
// never writes. Evaluated before it, the pair is refused and the record is left alone.
//
// Paired with M18, which moves the precheck after `mergeProven`. The claimed-phase
// fixture cannot pin that mutation: there `mergeProven` changes nothing, so a late check
// still refuses.
func TestOrphanSweepRefusesAContradictoryClaimWithoutRepairingIt(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-contradictory-shape")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, _, regSeq := e.testRetiredAffineRegister(t, 22, "REGBOTH", true,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.Lost, c.SourceAcked = true, true, true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("a record asserting both AffineDone and Lost was deleted; it is a shape this broker never " +
			"writes, so it must be refused rather than acted on")
	}
	if !got.Lost {
		t.Fatalf("the contradictory record was REPAIRED rather than refused: %+v. mergeProven clears Lost on "+
			"such a pair by itself, so a veto evaluated after it normalises the record and the CAS lands. "+
			"Under the compatibility boundary an unsupported shape is refused, never migrated", got)
	}
	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the copy was collected on a contradictory record (state %v)", st)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted on a contradictory record")
	}
}

// ---------------------------------------------------------------------------
// The owner's sourceless rule: extracted, and hardened
// ---------------------------------------------------------------------------

// sourcelessFixture stages a claim of the env session's own affine generation that is
// NONTERMINAL with its copy absent, and deletes the source message so the rule applies.
//
//nolint:unparam
func (e *testOrphanEnv) sourcelessFixture(t *testing.T, name string, dropSource bool,
	shape func(*mqttQ2AffClaim)) (idHash, affGen, claimSubject string) {

	t.Helper()
	idHash, affGen = e.testLiveAffineGeneration(t)
	groupDur, sseq := e.liveGroupDurable(t, name)
	c := testValidQ2AffClaim(idHash, affGen, groupDur, sseq)
	if shape != nil {
		shape(c)
	}
	claimSubject = e.putClaimFor(t, c)
	if dropSource {
		if err := e.asm.jsa.deleteMsg(mqttStreamName, sseq, true); err != nil {
			t.Fatalf("delete the source message: %v", err)
		}
	}
	return idHash, affGen, claimSubject
}

// The rule itself: claimed, no copy, source authoritatively gone. The claim is settled
// `Discarded ∧ SourceAcked` and reaped. This is EXISTING behaviour, pinned so the
// extraction cannot change it.
func TestQ2AffResumeSettlesASourcelessClaim(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-settles")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCGONE", true, nil)

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	if got := e.claimOn(t, claimSubject); got != nil {
		t.Fatalf("a claim whose source is authoritatively gone was not settled and reaped: %+v; it would "+
			"otherwise sit at `claimed` forever while contenders keep honoring a winner for a message "+
			"that no longer exists", got)
	}
}

// The source still PRESENT leaves it alone: the group's AckWait re-offers it and
// whichever member receives it materializes from the payload, which recovery does not
// have.
//
// Paired with M36.
func TestQ2AffResumeSourcelessRuleKeepsAClaimWhoseSourceCouldRedeliver(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-source-live")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCLIVE", false, nil)

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("a claim whose SOURCE is still present was reaped")
	}
	if got.Discarded || got.SourceAcked {
		t.Fatalf("the rule settled a claim whose source can still be re-offered: %+v. That asserts a group "+
			"ack it never proved and discards a delivery that may still be made", got)
	}
}

// An UNREADABLE copy is not an absent copy. Fail closed.
//
// Paired with M7.
func TestQ2AffResumeSourcelessRuleFailsClosedOnAnUnreadableCopy(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-copy-unreadable")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCUNREAD", true, nil)

	// The OUTER read must report ABSENT, or recoverQ2AffClaims returns before the rule is
	// entered and this fixture pins the caller's fail-closed rather than the rule's — which
	// is how it first passed against a mutation that widened the rule's own check.
	var reads int
	e.asm.mu.Lock()
	e.asm.testQ2AffCopyLoad = func(string) (*StoredMsg, error) {
		reads++
		if reads == 1 {
			return nil, nil
		}
		return nil, fmt.Errorf("staged copy-load failure")
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffCopyLoad = nil; e.asm.mu.Unlock() })

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was reaped on an UNREADABLE copy reading")
	}
	if got.Discarded || got.SourceAcked {
		t.Fatalf("an unreadable copy was treated as an absent one: %+v", got)
	}
}

// A copy that becomes PRESENT between the caller's outer read and the rule's own final
// read is refused. This is the last gate on the principal store-before-phase race: a
// materializer stores the copy BEFORE its phase CAS, so the record can still read
// `claimed` while a copy exists.
//
// Paired with M37.
func TestQ2AffResumeSourcelessRuleRefusesACopyThatAppeared(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-copy-appears")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCAPPEARS", true, nil)
	c := e.claimOn(t, claimSubject)

	// Absent for the caller's outer read, PRESENT for the rule's own final one.
	var n int
	e.asm.mu.Lock()
	e.asm.testQ2AffCopyLoad = func(subject string) (*StoredMsg, error) {
		n++
		if n == 1 {
			// ABSENT, which is (nil, nil) here. An error would be UNKNOWN, and this
			// fixture would then pass by failing closed for a different reason without
			// ever reaching the final read it is about.
			return nil, nil
		}
		return &StoredMsg{Subject: subject, Sequence: 1234}, nil
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffCopyLoad = nil; e.asm.mu.Unlock() })

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatalf("the claim was reaped although a copy appeared before the decision (copy %q)", c.CopySubject)
	}
	if got.Discarded || got.SourceAcked {
		t.Fatalf("the rule settled a claim whose copy appeared between the outer read and its own: %+v. "+
			"That is the store-before-phase race, and this read is its last gate", got)
	}
}

// The freshness veto: the caller's snapshot says `claimed`, and the record the CAS would
// be applied to says `materialized`, because a materializer holding the payload completed
// in between. Nothing is written and NOTHING is reaped.
//
// Paired with M4 (hoist the check to the caller) and M5 (put it in `mutate`, where it
// cannot veto).
func TestQ2AffResumeDeclinesTheSourcelessRuleAgainstAFreshMaterialization(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-fresh-mat")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCFRESH", true, nil)

	// Every load INSIDE the rule sees materialized; the caller's own snapshot came from
	// the record scan, which does not go through this seam, so it saw `claimed`.
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		cur, seq, err := e.asm.jsa.loadQ2AffClaim(s)
		if s == claimSubject && cur != nil {
			cur.Phase = mqttQ2AffMaterialized
		}
		return cur, seq, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffClaimLoad = nil; e.asm.mu.Unlock() })

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = nil
	e.asm.mu.Unlock()
	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was REAPED against a fresh materialization; reaping a claim a materializer just " +
			"advanced is exactly the harm the incarnation work closed")
	}
	if got.Discarded {
		t.Fatalf("Discarded was written onto a freshly materialized claim: %+v. The delivery gate's terminal "+
			"branch then drops the live copy, and with the source already gone that delivery is lost", got)
	}
	if got.SourceAcked {
		t.Fatalf("SourceAcked was written onto a freshly materialized claim: %+v. This is what M5 produces — "+
			"the advance is a no-op success, so the caller proceeds and the REAP writes it", got)
	}
}

// An already `Discarded ∧ SourceAcked` claim with its copy absent is REAPED, not left
// behind: a no-op advance is still a success, and this is exactly the shape the rule
// exists to clear.
//
// Paired with the round-3 finding that a single boolean cannot distinguish this from a
// vetoed attempt.
func TestQ2AffResumeSourcelessRuleReapsAnAlreadySettledClaim(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-already-settled")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCSETTLED", true,
		func(c *mqttQ2AffClaim) { c.Discarded, c.SourceAcked = true, true })

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	if got := e.claimOn(t, claimSubject); got != nil {
		t.Fatalf("an already-settled claim was left behind: %+v. A no-op advance is a SUCCESS, and treating "+
			"it as a refusal strands exactly the population this rule clears", got)
	}
}

// The source is read FIRST and the copy LAST, so the reading the CAS consumes is as
// fresh as available. Observed through the source probe, which exists for this and
// changes nothing.
//
// Paired with M8.
func TestQ2AffResumeReadsTheCopyLastOnTheSourcelessPath(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-read-order")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCORDER", true, nil)

	var order []string
	var mu sync.Mutex
	e.asm.mu.Lock()
	e.asm.testQ2AffSourceProbe = func(*mqttQ2AffClaim) {
		mu.Lock()
		order = append(order, "source")
		mu.Unlock()
	}
	e.asm.testQ2AffCopyLoad = func(subject string) (*StoredMsg, error) {
		mu.Lock()
		order = append(order, "copy")
		mu.Unlock()
		return nil, nil // ABSENT, so the rule is actually entered
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQ2AffSourceProbe, e.asm.testQ2AffCopyLoad = nil, nil
		e.asm.mu.Unlock()
	})

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)
	_ = claimSubject

	mu.Lock()
	defer mu.Unlock()
	// The full sequence is [outer copy, source, final copy, reap's copy]: the caller reads
	// the copy first because the `materialized` arm needs it, then the RULE reads the
	// source and the copy in that order, and the reap that follows reads the copy again.
	//
	// So the assertion is on the rule's OWN two reads — entries 1 and 2 — and not on the
	// last read in the slice, which belongs to the reaper. Reversing the rule's order puts
	// "copy" at 1 and "source" at 2, which this catches.
	if len(order) < 3 {
		t.Fatalf("expected at least three reads (outer copy, then the rule's source and copy), got %v", order)
	}
	if order[0] != "copy" {
		t.Fatalf("expected the caller's outer copy read first, got %v", order)
	}
	if order[1] != "source" || order[2] != "copy" {
		t.Fatalf("the rule read %q then %q; it must read the SOURCE first and the COPY LAST, so the reading "+
			"its CAS consumes is no older than the decision. Full sequence: %v", order[1], order[2], order)
	}
}

// A stale UNSUPPORTED rewrite arriving between the caller's reads and the rule's own
// load is refused. This is the only fixture that pins the SHAPE clause at this site:
// `wantCopy` and the nonce both pass a same-identity, same-copy rewrite, and the phase
// clause passes anything that is not `materialized`, so only the shape check refuses it.
//
// Paired with M26.
func TestQ2AffResumeSourcelessRuleRefusesAStaleUnsupportedRewrite(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-stale-shape")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCSTALE", true, nil)

	// Legitimate for the caller's scan; contradictory by the time the rule loads it. The
	// phase stays `claimed`, so the phase clause cannot be what refuses it.
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		cur, seq, err := e.asm.jsa.loadQ2AffClaim(s)
		if s == claimSubject && cur != nil {
			cur.AffineDone, cur.Lost = true, true
		}
		return cur, seq, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffClaimLoad = nil; e.asm.mu.Unlock() })

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = nil
	e.asm.mu.Unlock()
	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was reaped although the record the rule loaded was an unsupported shape")
	}
	if got.Discarded || got.SourceAcked {
		t.Fatalf("the rule mutated a record asserting both AffineDone and Lost: %+v. Under the compatibility "+
			"boundary an unsupported persisted shape is REFUSED, never interpreted", got)
	}
}

// The accepted path's reap is the CHECKED one: a record that turns unsupported after the
// transition lands is refused by the reap rather than deleted.
//
// Paired with M22.
func TestQ2AffResumeSourcelessReapIsChecked(t *testing.T) {
	e := testNewOrphanEnv(t, "srcless-reap-checked")
	e.setGraces(t, 0, 0)
	idHash, affGen, claimSubject := e.sourcelessFixture(t, "SRCREAPCHK", true, nil)

	// Legitimate until the advance has landed, then an unknown phase — the form the
	// UNCHECKED reaper would happily delete, since q2AffClaimUsableForDecision does not
	// look at the phase.
	var advanced bool
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimStore = func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		seq, err := e.asm.jsa.storeQ2AffClaimCAS(subject, expectSeq, rec)
		if subject == claimSubject && err == nil {
			advanced = true
		}
		return seq, err
	}
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		cur, seq, err := e.asm.jsa.loadQ2AffClaim(s)
		if s == claimSubject && cur != nil && advanced {
			cur.Phase = mqttQ2AffPhase("not-a-phase-this-broker-writes")
		}
		return cur, seq, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQ2AffClaimLoad, e.asm.testQ2AffClaimStore = nil, nil
		e.asm.mu.Unlock()
	})

	e.asm.recoverQ2AffClaims(idHash, affGen, mqttQ2AffProofComplete)

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad, e.asm.testQ2AffClaimStore = nil, nil
	e.asm.mu.Unlock()
	if !advanced {
		t.Fatal("the transition never landed, so this fixture says nothing about the reap that follows it")
	}
	if got := e.claimOn(t, claimSubject); got == nil {
		t.Fatal("the reap DELETED a record whose shape is not one this broker writes. The accepted path's " +
			"reap must be the checked one, refusing against the record it loads itself")
	}
}

// ---------------------------------------------------------------------------
// The typed reap outcome, row by row
// ---------------------------------------------------------------------------

// An authoritatively ABSENT claim is `reaped`, not `indeterminate`: the record being gone
// IS the fact a caller gating on "the claim no longer exists" needs. Mapping it to
// indeterminate would retain such a register forever.
//
// Paired with M42. Staged so the claim is present for arm (a)'s attribution and absent at
// the reaper's own initial load, because otherwise arm (a) takes its own claim-absent
// branch and the typed reaper is never invoked at all.
func TestOrphanSweepCheckedReapTreatsAnAbsentClaimAsReaped(t *testing.T) {
	e := testNewOrphanEnv(t, "outcome-absent")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 23, "OUTABSENT", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	var attributed bool
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		if s == claimSubject {
			if attributed {
				return nil, 0, nil // absent by the time the reaper loads it
			}
			attributed = true
		}
		return e.asm.jsa.loadQ2AffClaim(s)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffClaimLoad = nil; e.asm.mu.Unlock() })

	e.sweepInflight(t, 16)

	if !attributed {
		t.Fatal("arm (a) never attributed the claim, so the reaper's own load was never reached")
	}
	if !deleted(regSeq) {
		t.Fatal("the register was retained although its claim is authoritatively ABSENT. That absence is the " +
			"fact the deletion waits on, so mapping it to indeterminate keeps the register forever")
	}
}

// A claim LOAD FAILURE is `indeterminate`, so the register is retained. Staged after a
// successful attribution, or the failure is consumed upstream and the reaper never runs.
//
// Paired with M43.
func TestOrphanSweepCheckedReapTreatsAnUnreadableClaimAsIndeterminate(t *testing.T) {
	e := testNewOrphanEnv(t, "outcome-unreadable")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 24, "OUTUNREAD", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	var attributed bool
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		if s == claimSubject {
			if attributed {
				return nil, 0, fmt.Errorf("staged claim-load failure")
			}
			attributed = true
		}
		return e.asm.jsa.loadQ2AffClaim(s)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffClaimLoad = nil; e.asm.mu.Unlock() })

	e.sweepInflight(t, 16)

	if !attributed {
		t.Fatal("arm (a) never attributed the claim, so the reaper's own load was never reached")
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted on an UNREADABLE claim. 'I could not tell' must never license a " +
			"dependent deletion")
	}
}

// A copy read of `Unknown` is `indeterminate`: an unreadable copy is not an absent one,
// so the claim is not deleted and the register is retained.
//
// Paired with M45.
func TestOrphanSweepCheckedReapTreatsAnUnreadableCopyAsIndeterminate(t *testing.T) {
	e := testNewOrphanEnv(t, "outcome-copy-unread")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 25, "OUTCOPYUNREAD", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	// The source classification uses the batched cache rather than this seam, so failing
	// every call still leaves arm (a) reachable and fails the REAPER's copy read.
	e.asm.mu.Lock()
	e.asm.testQ2AffCopyLoad = func(string) (*StoredMsg, error) {
		return nil, fmt.Errorf("staged copy-load failure")
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffCopyLoad = nil; e.asm.mu.Unlock() })

	e.sweepInflight(t, 16)

	if got := e.claimOn(t, claimSubject); got == nil {
		t.Fatal("the claim was deleted on an UNREADABLE copy reading")
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted although the reap could not read the copy")
	}
}

// A SUCCESSFUL delete is `reaped`, which licenses the register deletion. The table's most
// common row, and it had nothing pinning it.
//
// Paired with M52.
func TestOrphanSweepRegisterArmDeletesTheRegisterAfterASuccessfulReap(t *testing.T) {
	e := testNewOrphanEnv(t, "outcome-success")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 26, "OUTSUCCESS", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	if got := e.claimOn(t, claimSubject); got != nil {
		t.Fatalf("the claim was not reaped: %+v", got)
	}
	if !deleted(regSeq) {
		t.Fatal("the register was retained after a SUCCESSFUL reap; that is the ordinary path and it must " +
			"license the deletion")
	}
}

// ---------------------------------------------------------------------------
// Arm (a)'s remaining branches, and the budget
// ---------------------------------------------------------------------------

// The claim is already reaped: the register's deletion waits on exactly that fact, so it
// is collected.
//
// Paired with M39.
func TestOrphanSweepCollectsARegisterWhoseClaimIsAlreadyReaped(t *testing.T) {
	e := testNewOrphanEnv(t, "arma-claim-absent")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 27, "ARMAABSENT", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	// Somebody else reaped it already.
	_, seq, err := e.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("load the claim: %v", err)
	}
	if err := e.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); err != nil {
		t.Fatalf("pre-reap the claim: %v", err)
	}

	e.sweepInflight(t, 16)

	if !deleted(regSeq) {
		t.Fatal("a register whose claim is already reaped was retained; nothing else will ever revisit it")
	}
}

// A LEGITIMATE `retained` outcome still licenses arm (a)'s register deletion. This is the
// COMMON path: the exchange is over, and the claim is retained for a reason that has
// nothing to do with this register — so keeping the register would strand it forever.
//
// Paired with M40.
func TestOrphanSweepCollectsARegisterWhoseClaimIsLegitimatelyRetained(t *testing.T) {
	e := testNewOrphanEnv(t, "arma-retained")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 28, "ARMARETAIN", false,
		// Terminal, but Lost — so reapable() refuses it unconditionally and the reap
		// legitimately returns `retained` rather than `reaped`.
		func(c *mqttQ2AffClaim) { c.Lost, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	if got := e.claimOn(t, claimSubject); got == nil {
		t.Fatal("a LOST claim was deleted; it is the only durable evidence of an unprovable delivery")
	}
	if !deleted(regSeq) {
		t.Fatal("the register was retained on a LEGITIMATE `retained` outcome. The exchange is over and the " +
			"claim is kept for its own reasons, so the register has nothing left to carry")
	}
}

// The register arms charge the shared mutation budget, so one account cannot collect an
// unbounded number of triples in a single pass.
//
// Paired with M48 and M50.
func TestOrphanSweepRegisterArmChargesTheBudget(t *testing.T) {
	e := testNewOrphanEnv(t, "arma-budget")
	e.setGraces(t, 0, 0)
	_, _, _, firstSeq := e.testRetiredAffineRegister(t, 29, "BUDGETONE", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	_, _, _, secondSeq := e.testRetiredAffineRegister(t, 30, "BUDGETTWO", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 1)

	n := 0
	if deleted(firstSeq) {
		n++
	}
	if deleted(secondSeq) {
		n++
	}
	if n != 1 {
		t.Fatalf("a budget of 1 collected %d register(s); the budget bounds charged attempts per pass, so "+
			"exactly one candidate may be settled and the other deferred", n)
	}
}

// Neither register arm takes a quarantine observation: both act on positive, freshly
// revalidated evidence under a matching live record, so they converge in ONE pass.
//
// Paired with M51.
func TestOrphanSweepRegisterArmsNeedNoQuarantineObservation(t *testing.T) {
	e := testNewOrphanEnv(t, "arma-noquarantine")
	// A LONG grace: if either arm consulted quarantine, nothing could converge here.
	e.setGraces(t, time.Hour, time.Hour)
	_, _, _, regSeq := e.testRetiredAffineRegister(t, 31, "NOQUAR", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	if !deleted(regSeq) {
		t.Fatal("the register was not collected in ONE pass under a one-hour grace, so an arm is consulting " +
			"quarantine. A wait exists to keep a mid-construction resource from being acted on, and both " +
			"arms act on positive freshly-revalidated evidence instead")
	}
}

// Step 6 needs a PROVEN reap, and a legitimate `retained` is not one. After step 4 has
// deleted the copy, a claim whose `SourceAcked` is still false with its group source
// still offerable makes `reapable` return false — a perfectly healthy record that is
// deliberately kept. The register must survive, because it is the retry route and the
// durable PUBREC evidence for a claim this pass has just written to.
//
// Paired with M47, and independently raised as the only HIGH finding of holistic review
// round 1 before that round's tree was superseded.
func TestOrphanSweepRegisterArmStep6NeedsAReapedOutcome(t *testing.T) {
	e := testNewOrphanEnv(t, "step6-retained")
	e.setGraces(t, 0, 0)
	// SourceAcked deliberately FALSE, and liveGroupDurable leaves a real source message on
	// a live group consumer, so q2AffSourceUnofferable is false and the reaper cannot
	// complete that side itself. reapable() then refuses, legitimately.
	claimSubject, copySubject, _, regSeq := e.testRetiredAffineRegister(t, 32, "STEP6", true, nil)
	deleted := e.watchRegisterDeletes(t)

	e.sweepInflight(t, 16)

	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was reaped although its group source is still offerable and SourceAcked is false; " +
			"deleting it there would let a redelivery elect a second winner for a started delivery")
	}
	if !got.AffineDone {
		t.Fatalf("step 3 did not record AffineDone, so this fixture never reaches the outcome gate it is "+
			"about: %+v", got)
	}
	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("step 4 did not collect the copy (state %v), so the fixture stops short of step 5", st)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted on a LEGITIMATE `retained` outcome. Only a proven reap licenses " +
			"step 6: the register is the retry route and the durable PUBREC evidence for a claim this pass " +
			"has just written to, and the claim is still here")
	}
}

// The checked reaper applies its precheck at a SECOND point: inside the conditional
// SourceAcked advance, on that advance's own freshly loaded record. Reaching it needs
// `SourceAcked == false` AND an unofferable source, or the advance is skipped entirely
// and the site is never exercised.
//
// Paired with M19 (drop the precheck from that advance) and M54 (map its refusal to
// `reaped`). The register must survive both: a refusal proves nothing, so it cannot
// license a dependent deletion.
func TestOrphanSweepCheckedReapVetoesARewriteDuringItsOwnAdvance(t *testing.T) {
	e := testNewOrphanEnv(t, "reap-inner-advance")
	e.setGraces(t, 0, 0)
	// Terminal so reapable() can pass, SourceAcked FALSE so the inner advance is entered.
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 33, "INNERADV", false,
		func(c *mqttQ2AffClaim) { c.AffineDone = true })
	deleted := e.watchRegisterDeletes(t)

	// The source message gone makes q2AffSourceUnofferable true, so the reaper tries to
	// complete SourceAcked itself — which is the advance this fixture is about.
	c := e.claimOn(t, claimSubject)
	if err := e.asm.jsa.deleteMsg(mqttStreamName, c.SourceSeq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}

	// Legitimate for arm (a)'s attribution and for the reaper's initial load; an unknown
	// phase by the time the inner advance loads it.
	var loads int
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		cur, seq, err := e.asm.jsa.loadQ2AffClaim(s)
		if s == claimSubject && cur != nil {
			loads++
			if loads >= 3 { // 1: arm (a)'s attribution, 2: the reaper's own load, 3+: the advance
				cur.Phase = mqttQ2AffPhase("not-a-phase-this-broker-writes")
			}
		}
		return cur, seq, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffClaimLoad = nil; e.asm.mu.Unlock() })

	e.sweepInflight(t, 16)

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = nil
	e.asm.mu.Unlock()
	if loads < 3 {
		t.Fatalf("the reaper's inner SourceAcked advance was never reached (%d claim loads); this fixture "+
			"says nothing about the second precheck application", loads)
	}
	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was DELETED after its record turned unsupported inside the reaper's own advance")
	}
	if got.SourceAcked {
		t.Fatalf("SourceAcked was written onto an unsupported record inside the reaper's advance: %+v", got)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted although the reap refused inside its own advance. A refusal is " +
			"INDETERMINATE, never a licence for a dependent deletion")
	}
}

// A register that CARRIES an affine reference and is not settleable is retained AND
// diagnosed. The diagnostic is the point: the state is permanent and does not self-heal,
// because nothing else revisits such a register while its session generation is
// referenced, so silence would hide both continuing accumulation and an unsupported
// persisted shape from the operator who has to act on it.
//
// Paired with M38 (drop `rec.Q2AffClaim != _EMPTY_`) — whose refusal is observable only
// through this diagnostic, since the state outcome is identical either way.
func TestOrphanSweepDiagnosesAnUnsettleableAffineRegister(t *testing.T) {
	lc := &logCapture{}
	e := testNewOrphanEnvWithLogger(t, "reg-diagnosed", lc)
	e.setGraces(t, 0, 0)
	idHash, affGen := e.testLiveAffineGeneration(t)
	hash := mqttQ2AffClaimHash("$MQTT_SHARED_DIAG", 3)
	_, _, affDur := mqttQ2AffineNames(idHash, affGen)

	e.sess.mu.Lock()
	sessGen := e.sess.gen
	e.sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, sessGen, 34)
	// Seeded through the RAW builder, not storeInflightCAS. This record names affine state
	// through the copy alone and carries no claim — half an affine reference — which the
	// legal-shape matrix at the write boundary now REFUSES outright. That refusal is the
	// intended behaviour and this fixture is about the SWEEP's treatment of such a record
	// arriving from elsewhere, so it has to be planted rather than written.
	rec := &mqttInflightRecord{State: mqttInflightPublish,
		PI: 34, Dur: affDur, Seq: 7, QoS: 2, Nonce: mqttNewInflightNonce(),
		// It NAMES affine state through the copy, and carries no claim — the exact clause
		// M38 removes.
		Q2AffCopy: mqttQ2AffCopySubject(idHash, affGen, hash, testQ2AffFixtureNonce),
		Retired:   true,
	}
	if verr := mqttValidateInflightShape(rec); verr == nil {
		t.Fatal("the matrix accepts this shape, so planting it round the write boundary is " +
			"unnecessary and this fixture no longer describes what it claims")
	}
	msg, hdrLen := buildInflightRecord(0, rec)
	resp, err := e.asm.jsa.storeMsg(subject, hdrLen, msg)
	if err != nil {
		t.Fatalf("persist the unsettleable register: %v", err)
	}
	seq := resp.Sequence

	e.sweepInflight(t, 16)

	if _, smsg, lerr := e.asm.jsa.loadInflightRecord(subject); lerr != nil || smsg == nil {
		t.Fatalf("an unsettleable register was not retained (err=%v)", lerr)
	}
	// The DIAGNOSTIC moved with the refusal, and that is the point of asserting it rather
	// than just the retention. This shape used to reach the affine arm and be reported as
	// "not settleable"; the closed schema now refuses it at the decoder, so no arm can
	// inspect the state it names — and the sweep reports THAT instead. Either way the
	// operator hears about a register that accumulates and that nothing else will revisit.
	lc.requireLine(t, "not a shape this broker writes", subject)
	_ = seq
}

// The retry-time shape veto on arm (b)'s ADVANCE, exercised through the window that only
// it can catch: the record is legitimate when arm (b) attributes it and unsupported by
// the time `advanceQ2AffClaimChecked` loads it. A statically malformed record would be
// refused earlier and leave this site untested.
//
// The planted form is `AffineDone ∧ Lost` at phase `materialized`, because that is the
// one shape where `mergeProven` itself changes the record: the phase clause passes it, so
// only the shape clause refuses, and a veto placed after `mergeProven` would see a
// repaired record and let the CAS land.
//
// Paired with M16.
func TestOrphanSweepRegisterArmRefusesAStaleUnsupportedRewrite(t *testing.T) {
	e := testNewOrphanEnv(t, "regb-stale-shape")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, _, regSeq := e.testRetiredAffineRegister(t, 35, "REGBSTALE", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	// Legitimate for arm (b)'s attribution through q2AffClaimForCarriedCopy; contradictory
	// from the CAS-loop load onwards.
	var loads int
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		cur, seq, err := e.asm.jsa.loadQ2AffClaim(s)
		if s == claimSubject && cur != nil {
			loads++
			if loads >= 2 {
				cur.AffineDone, cur.Lost = true, true
			}
		}
		return cur, seq, err
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffClaimLoad = nil; e.asm.mu.Unlock() })

	e.sweepInflight(t, 16)

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = nil
	e.asm.mu.Unlock()
	if loads < 2 {
		t.Fatalf("arm (b) never reached its CAS-loop load (%d claim loads), so this fixture says nothing "+
			"about the retry-time veto", loads)
	}
	got := e.claimOn(t, claimSubject)
	if got == nil {
		t.Fatal("the claim was reaped although the record the advance loaded was unsupported")
	}
	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyPresent {
		t.Fatalf("the copy was deleted on a stale unsupported rewrite (state %v)", st)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted on a stale unsupported rewrite")
	}
}

// Arm (a)'s SUPERSEDED branch: the claim subject now holds a LATER incarnation, so this
// register's carried copy can never be authorized by anybody. That is a positive fact
// about a record just read, and the successor made no delivery, so nothing is written to
// it and the register is collected. Without this branch the old register accumulates for
// as long as the successor stays nonterminal.
//
// Paired with M31.
func TestOrphanSweepCollectsAnOldRegisterWhoseClaimWasSucceeded(t *testing.T) {
	e := testNewOrphanEnv(t, "arma-superseded")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 36, "ARMASUPER", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	// A REPLACEMENT incarnation takes the claim subject: same winner, generation and claim
	// hash, a fresh nonce, and therefore a different copy subject.
	old := e.claimOn(t, claimSubject)
	_, seq, err := e.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("load the claim: %v", err)
	}
	if err := e.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); err != nil {
		t.Fatalf("reap the first incarnation: %v", err)
	}
	successor := testValidQ2AffClaim(old.Winner, old.Gen, old.GroupDurable, old.SourceSeq)
	successor.Nonce = mqttNewQ2AffGen()
	successor.CopySubject = mqttQ2AffCopySubject(old.Winner, old.Gen, old.ClaimHash, successor.Nonce)
	successor.Phase = mqttQ2AffMaterialized
	if _, err := e.asm.storeQ2AffClaimCAS(claimSubject, 0, successor); err != nil {
		t.Fatalf("elect the successor: %v", err)
	}

	e.sweepInflight(t, 16)

	after := e.claimOn(t, claimSubject)
	if after == nil {
		t.Fatal("the SUCCESSOR claim was reaped; the old register says nothing about it")
	}
	if after.Nonce != successor.Nonce {
		t.Fatalf("the record no longer names the successor: %+v", after)
	}
	if after.AffineDone || after.Lost || after.Discarded {
		t.Fatalf("the successor was credited with a delivery it never made: %+v", after)
	}
	if !deleted(regSeq) {
		t.Fatal("the old register was retained although its carried copy is provably superseded; it then " +
			"accumulates for as long as the successor stays nonterminal")
	}
}

// Arm (a)'s register delete is gated on the reap outcome too: an INDETERMINATE reap
// retains the register, so a transient failure does not lose the exchange's last
// reference to its claim.
//
// Paired with M24.
func TestOrphanSweepRegisterArmAKeepsTheRegisterOnAnIndeterminateReap(t *testing.T) {
	e := testNewOrphanEnv(t, "arma-indeterminate")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 37, "ARMAINDET", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream == mqttQoS2AssignStreamName {
			return fmt.Errorf("staged claim-delete failure")
		}
		return nil
	})
	defer restore()

	e.sweepInflight(t, 16)
	restore()

	if got := e.claimOn(t, claimSubject); got == nil {
		t.Fatal("the claim was deleted although its delete was staged to fail")
	}
	if deleted(regSeq) {
		t.Fatal("arm (a) deleted the register on an INDETERMINATE reap. The register is this exchange's last " +
			"reference to its claim, so a transient failure must retain it for the next pass")
	}
}

// Settling the durable triple must also drop the RESIDENT retirement, or the same
// failure population keeps accumulating in the session's heap instead of on the streams.
//
// This arm is what makes that permanent: the in-memory entry is created by the same
// failed-`AffineDone` path, nothing re-examines it afterwards (both
// `resolveRetiredSource` callers run when a retirement is CREATED, not periodically), and
// the only thing that used to clear it was a later redelivery of the copy. Deleting that
// copy removes exactly that route.
//
// Raised as the sole HIGH finding of holistic review round 3.
func TestOrphanSweepSettlementForgetsTheResidentRetirement(t *testing.T) {
	e := testNewOrphanEnv(t, "resident-retirement")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, regSubject, regSeq := e.testRetiredAffineRegister(t, 38, "RESIDENT", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	// The resident half, exactly as the failed-AffineDone path leaves it.
	cur, _, err := e.asm.jsa.loadInflightRecord(regSubject)
	if err != nil || cur == nil {
		t.Fatalf("reload the register: %v", err)
	}
	key := mqttDeferredKey{cur.Dur, cur.Seq}
	e.sess.mu.Lock()
	e.sess.retireSourceLocked(cur.Dur, cur.Seq, &mqttRetiredSource{
		pi: cur.PI, regSeq: regSeq, nonce: cur.Nonce,
		q2AffClaim: claimSubject, q2AffCopy: copySubject, durable: true,
	})
	staged := e.sess.srcAckOwed[key] != nil
	e.sess.mu.Unlock()
	if !staged {
		t.Fatal("the resident retirement was not staged, so this fixture proves nothing")
	}

	e.sweepInflight(t, 16)

	if !deleted(regSeq) {
		t.Fatal("the durable settlement did not complete, so the resident half was never reached")
	}
	e.sess.mu.Lock()
	still := e.sess.srcAckOwed[key]
	e.sess.mu.Unlock()
	if still != nil {
		t.Fatalf("the resident retirement survived the durable settlement: %+v. Nothing re-examines it and "+
			"the copy that would have cleared it is gone, so it is now permanent — one map entry per such "+
			"exchange, which is the growth law the durable cleanup just closed, moved into the heap", still)
	}
}

// The resident cleanup is identity-fenced: a retirement belonging to a DIFFERENT exchange
// on the same session is untouched, because clearing it would resurrect a delivery this
// broker still owes.
func TestOrphanSweepSettlementSparesAnotherExchangesRetirement(t *testing.T) {
	e := testNewOrphanEnv(t, "resident-fenced")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, regSubject, regSeq := e.testRetiredAffineRegister(t, 39, "RESFENCE", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	_ = e.watchRegisterDeletes(t)

	cur, _, err := e.asm.jsa.loadInflightRecord(regSubject)
	if err != nil || cur == nil {
		t.Fatalf("reload the register: %v", err)
	}
	// Same (durable, sequence) key, a DIFFERENT register nonce: another exchange.
	other := mqttNewInflightNonce()
	otherKey := mqttDeferredKey{cur.Dur, cur.Seq}
	e.sess.mu.Lock()
	e.sess.retireSourceLocked(cur.Dur, cur.Seq, &mqttRetiredSource{
		pi: 200, regSeq: 999, nonce: other,
		q2AffClaim: claimSubject, q2AffCopy: copySubject, durable: true,
	})
	e.sess.mu.Unlock()

	e.sweepInflight(t, 16)
	_ = regSeq

	e.sess.mu.Lock()
	still := e.sess.srcAckOwed[otherKey]
	e.sess.mu.Unlock()
	if still == nil || still.nonce != other {
		t.Fatal("a retirement belonging to a DIFFERENT exchange was cleared. Its nonce is the fence: clearing " +
			"it resurrects a delivery this broker still owes to suppress")
	}
}

// The resident cleanup's GENERATION fence, exercised directly because reaching it through
// a full sweep pass needs a stale record naming a second generation of the same client —
// machinery that would obscure what is being tested. A register subject names one
// incarnation, and its retirement lives in THAT incarnation's map; clearing an entry from
// a different one would drop a suppression the resident session still needs.
func TestOrphanSweepResidentCleanupIsGenerationFenced(t *testing.T) {
	e := testNewOrphanEnv(t, "resident-genfence")
	e.setGraces(t, 0, 0)

	e.sess.mu.Lock()
	idHash, liveGen := e.sess.idHash, e.sess.gen
	nonce := mqttNewInflightNonce()
	key := mqttDeferredKey{mqttQ2AffineConsumerDurablePrefix + "GENFENCE", 77}
	e.sess.retireSourceLocked(key.jsDur, key.sseq, &mqttRetiredSource{
		pi: 41, regSeq: 5, nonce: nonce, durable: true,
	})
	e.sess.mu.Unlock()

	other := mqttNewSessGen()
	if other == liveGen {
		t.Skip("generated the same generation twice")
	}
	// A register naming a DIFFERENT incarnation of this same client ID.
	e.asm.forgetResidentRetirement(&mqttInflightName{
		idHash: idHash, gen: other, pi: 41,
		subject: mqttInflightSubject(idHash, other, 41),
	}, &mqttInflightRecord{State: mqttInflightPublish,
		PI: 41, Dur: key.jsDur, Seq: key.sseq, QoS: 2, Nonce: nonce, Retired: true,
	})

	e.sess.mu.Lock()
	still := e.sess.srcAckOwed[key]
	e.sess.mu.Unlock()
	if still == nil {
		t.Fatal("a retirement belonging to the RESIDENT incarnation was cleared by a register naming a " +
			"different one. The subject names one incarnation, and its retirement lives in that " +
			"incarnation's map only")
	}

	// And the same register under the LIVE generation does clear it, so the fence is not
	// simply refusing everything.
	e.asm.forgetResidentRetirement(&mqttInflightName{
		idHash: idHash, gen: liveGen, pi: 41,
		subject: mqttInflightSubject(idHash, liveGen, 41),
	}, &mqttInflightRecord{State: mqttInflightPublish,
		PI: 41, Dur: key.jsDur, Seq: key.sseq, QoS: 2, Nonce: nonce, Retired: true,
	})
	e.sess.mu.Lock()
	gone := e.sess.srcAckOwed[key] == nil
	e.sess.mu.Unlock()
	if !gone {
		t.Fatal("the matching generation did NOT clear the retirement, so the fence refuses everything and " +
			"the negative half above proves nothing")
	}
}

// The GENERATION floor, which is the clause q2AffShapePrecheck adds beyond the two
// validators it composes. A malformed `Gen` with every derived name RECOMPUTED to stay
// internally consistent passes both of those — validateOrphanQ2AffClaim only checks that
// the names follow from the fields, and q2AffClaimUsableForDecision does not look at Gen
// at all — so only this clause refuses it.
//
// It calls the checked reaper DIRECTLY, because a normal inventory collection rejects a
// malformed generation before the sweep ever reaches the precheck, and driving it through
// a full pass would never exercise the clause.
//
// Paired with M27.
func TestOrphanSweepCheckedReapVetoesAMalformedGeneration(t *testing.T) {
	e := testNewOrphanEnv(t, "gen-floor")
	e.setGraces(t, 0, 0)
	groupDur, sseq := e.liveGroupDurable(t, "GENFLOOR")

	idHash := getHash("gen-floor-winner")
	bad := "not a valid generation"
	hash := mqttQ2AffClaimHash(groupDur, sseq)
	_, _, affDur := mqttQ2AffineNames(idHash, bad)
	c := &mqttQ2AffClaim{
		Winner: idHash, Gen: bad, GroupDurable: groupDur, SourceSeq: sseq,
		ClaimHash: hash, Nonce: testQ2AffFixtureNonce,
		// Recomputed from the malformed generation, so every derivation check passes.
		CopySubject: mqttQ2AffCopySubject(idHash, bad, hash, testQ2AffFixtureNonce),
		AffDurable:  affDur,
		Phase:       mqttQ2AffMaterialized,
		AffineDone:  true, SourceAcked: true,
	}
	subject := e.putClaimFor(t, c)

	// The two composed validators must ACCEPT it, or this fixture is not about the floor.
	if err := validateOrphanQ2AffClaim(subject, c); err != nil {
		t.Fatalf("the sweep shape validator already refuses this record (%v), so it cannot isolate the "+
			"generation floor", err)
	}
	if err := q2AffClaimUsableForDecision(subject, c); err != nil {
		t.Fatalf("the decision validator already refuses this record (%v), so it cannot isolate the "+
			"generation floor", err)
	}

	out := e.asm.reapQ2AffClaimChecked(e.s, subject, q2AffShapePrecheck(subject))

	if out != mqttQ2AffReapIndeterminate {
		t.Fatalf("the checked reap returned %v for a record whose GENERATION is not a valid identity; a "+
			"refusal is indeterminate and must not license anything", out)
	}
	if got := e.claimOn(t, subject); got == nil {
		t.Fatal("the record was DELETED although its generation is not one this broker writes")
	}
}

// Either authoritative not-found form at the claim delete is `reaped`: the record is
// gone, which is the ordinary-success reading this delete has always applied. Mapping it
// to indeterminate would retain every settled register forever.
//
// Paired with M46.
func TestOrphanSweepCheckedReapTreatsAnAlreadyGoneRecordAsReaped(t *testing.T) {
	e := testNewOrphanEnv(t, "outcome-notfound")
	e.setGraces(t, 0, 0)
	_, _, _, regSeq := e.testRetiredAffineRegister(t, 40, "NOTFOUND", false,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	deleted := e.watchRegisterDeletes(t)

	// The delete answers "no such sequence", which is what a peer having already reaped it
	// looks like from here.
	restore := setMQTTTestDeleteMsgHook(func(stream string, seq uint64) error {
		if stream == mqttQoS2AssignStreamName {
			return ApiErrors[JSSequenceNotFoundErrF]
		}
		return nil
	})
	defer restore()

	e.sweepInflight(t, 16)
	restore()

	if !deleted(regSeq) {
		t.Fatal("the register was retained although the claim delete answered NOT FOUND. The record is gone, " +
			"which is exactly the fact the register's deletion waits on; reading it as indeterminate keeps " +
			"every settled register forever")
	}
}

// The budget is charged when the write is ATTEMPTED, so a candidate whose claim already
// carries AffineDone — where the transition is a no-op — still consumes it. This is what
// pins the removal of the write-detection oracle an earlier revision carried: with that
// oracle restored, a no-op would be refunded and a second candidate would proceed.
//
// Paired with M49.
func TestOrphanSweepRegisterArmChargesTheBudgetForANoOpWrite(t *testing.T) {
	e := testNewOrphanEnv(t, "budget-noop")
	e.setGraces(t, 0, 0)
	// First candidate: its claim ALREADY carries AffineDone, so step 3 writes nothing.
	_, _, _, noopSeq := e.testRetiredAffineRegister(t, 41, "NOOPONE", true,
		func(c *mqttQ2AffClaim) { c.AffineDone, c.SourceAcked = true, true })
	// Second candidate, behind it, which must be DEFERRED because the budget is spent.
	_, _, _, secondSeq := e.testRetiredAffineRegister(t, 42, "NOOPTWO", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	deleted := e.watchRegisterDeletes(t)

	// The discovery cursor is seeded PSEUDO-RANDOMLY once per process, so without pinning
	// it to the first sequence the two candidates arrive in an arbitrary order and this
	// fixture passes whenever the second one happens to be visited first — proving
	// nothing about the no-op charge. The no-op register was stored first, so it holds the
	// lower sequence.
	e.asm.mu.Lock()
	e.asm.testQoS2SweepSeed = func(firstSeq, _ uint64) uint64 { return firstSeq }
	e.asm.inflightSweepSeeded = false
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQoS2SweepSeed = nil
		e.asm.mu.Unlock()
	})

	e.sweepInflight(t, 1)

	n := 0
	if deleted(noopSeq) {
		n++
	}
	if deleted(secondSeq) {
		n++
	}
	if n != 1 {
		t.Fatalf("a budget of 1 settled %d candidates. The charge is on the ATTEMPT, so a no-op write still "+
			"consumes it; refunding one lets a pass do unbounded work whenever its writes happen to be "+
			"no-ops", n)
	}
}

// A resume must not re-install a retirement the sweep has already settled. The session is
// registered in sessByHash BEFORE recoverInflightRegisters runs, so the sweep can settle
// the exact same exchange between recovery's source classification and its install — and
// the one-shot resident cleanup then finds an empty map and clears nothing.
//
// It drives the REAL recovery entry point rather than calling the resolution helper
// directly. That distinction is not cosmetic: a fixture that calls the helper itself
// proves the helper works and says nothing about whether recovery invokes it.
//
// What this pins, exactly: recovery's OWN source classification is taken inside the pass,
// so a copy the sweep has already deleted reads Complete and the register is dropped
// without a retirement ever being installed. That covers the case where the settlement
// completed before recovery began.
//
// What it does NOT pin, stated rather than implied: the narrower window where the sweep
// deletes the copy BETWEEN recovery's classification and its install. Recovery's
// post-install re-resolution closes that one, and reaching it from a test would need a
// pause seam inside the resume path between those two points — new production surface for
// one window — so that loop carries no mutation. See the ledger.
//
// Raised as the sole HIGH finding of holistic review round 4.
func TestQ2AffRecoveryDoesNotReviveASettledRetirement(t *testing.T) {
	e := testNewOrphanEnv(t, "recovery-revive")
	e.setGraces(t, 0, 0)
	claimSubject, copySubject, regSubject, _ := e.testRetiredAffineRegister(t, 43, "REVIVE", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })

	cur, _, err := e.asm.jsa.loadInflightRecord(regSubject)
	if err != nil || cur == nil {
		t.Fatalf("reload the register: %v", err)
	}
	key := mqttDeferredKey{cur.Dur, cur.Seq}

	// The sweep settles the exchange: copy deleted, claim reaped, register gone — and the
	// resident map is still empty, so the one-shot cleanup clears nothing.
	e.sweepInflight(t, 16)
	if st, _ := e.asm.loadQ2AffCopy(copySubject); st != mqttQ2AffCopyAbsent {
		t.Fatalf("the settlement did not collect the copy (state %v), so the window is not staged", st)
	}
	if got := e.claimOn(t, claimSubject); got != nil {
		t.Fatalf("the settlement did not reap the claim: %+v", got)
	}

	// A register survives the settlement whenever its delete was not confirmed, which is
	// exactly what recovery would then find. Put it back at the same identity so recovery
	// classifies it and installs its retirement.
	newSeq, err := e.asm.jsa.storeInflightCAS(regSubject, 0, cur)
	if err != nil {
		t.Fatalf("stage the surviving register recovery would find: %v", err)
	}
	// Recovery's own register drop is FIRE-AND-FORGET too, so its absence is observed
	// through the delete seam rather than by loading the record. Asserting on state here
	// raced the send queue and made this fixture flake 2 runs in 5 — the same defect the
	// sweep fixtures already hit, reintroduced by reaching for the obvious assertion.
	dropped := e.watchRegisterDeletes(t)

	rc := &client{srv: e.s}
	rc.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	if _, err := e.sess.recoverInflightRegisters(rc); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	e.sess.mu.Lock()
	still := e.sess.srcAckOwed[key]
	e.sess.mu.Unlock()
	if !dropped(newSeq) {
		t.Fatal("recovery kept a register whose copy is already gone; its own classification must read that " +
			"source as COMPLETE and drop it, which is what keeps a settled exchange from being revived")
	}
	if still != nil {
		t.Fatalf("recovery installed a retirement for an exchange the sweep had already settled: %+v. Its "+
			"copy is gone, both other resolveRetiredSource call sites run only where a retirement is "+
			"CREATED, and a redelivery cannot happen without the copy — so this entry is permanent, one "+
			"per interleaving", still)
	}
}

// The reaper's inner advance returning a NIL RECORD — the claim disappeared between this
// function's own load and the advance's — is indeterminate, not reaped. A nil record is
// not a proof of anything, and treating it as one would license deleting the register
// while nothing has established what happened to the claim.
//
// Paired with M44's nil half, which a store-failure fixture cannot reach: that half is
// about the record vanishing, not the write failing.
func TestOrphanSweepCheckedReapTreatsAVanishedRecordAsIndeterminate(t *testing.T) {
	e := testNewOrphanEnv(t, "reap-vanished")
	e.setGraces(t, 0, 0)
	// Terminal so the reap is plausible, SourceAcked FALSE so the inner advance is entered.
	claimSubject, _, _, regSeq := e.testRetiredAffineRegister(t, 44, "VANISH", false,
		func(c *mqttQ2AffClaim) { c.AffineDone = true })
	deleted := e.watchRegisterDeletes(t)

	c := e.claimOn(t, claimSubject)
	if err := e.asm.jsa.deleteMsg(mqttStreamName, c.SourceSeq, true); err != nil {
		t.Fatalf("delete the source message: %v", err)
	}

	var loads int
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = func(s string) (*mqttQ2AffClaim, uint64, error) {
		if s == claimSubject {
			loads++
			if loads >= 3 { // 1: arm (a)'s attribution, 2: the reaper's load, 3+: the advance
				return nil, 0, nil // vanished
			}
		}
		return e.asm.jsa.loadQ2AffClaim(s)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testQ2AffClaimLoad = nil; e.asm.mu.Unlock() })

	e.sweepInflight(t, 16)

	e.asm.mu.Lock()
	e.asm.testQ2AffClaimLoad = nil
	e.asm.mu.Unlock()
	if loads < 3 {
		t.Fatalf("the reaper's inner advance was never reached (%d loads), so the nil-record half is untested", loads)
	}
	if deleted(regSeq) {
		t.Fatal("the register was deleted although the claim VANISHED inside the reaper's own advance. A nil " +
			"record proves nothing, and reading it as reaped removes this exchange's last reference on no evidence")
	}
}

// The sweep's retired-register arm proves the SOURCE is past PUBREC. It does NOT hold a
// PUBREL bound to this claim incarnation, so it must never write PubRecProven — the one
// fact that outranks a recorded Lost. If it could, a sweep with no carrier at all would
// clear a genuine loss verdict and reap the evidence.
//
// Asserted on the TRANSITION HISTORY rather than on the final record, because the arm
// reaps what it settles and a reaped claim cannot be read back.
//
// Paired mutation M15b: make settleRetiredAffineRegister's proveQ2AffCopyDelivered call
// pass pubRecProven = true.
func TestOrphanSweepRegisterArmNeverProvesPubRec(t *testing.T) {
	e := testNewOrphanEnv(t, "reg-no-pubrec")
	e.setGraces(t, 0, 0)
	claimSubject, _, _, _ := e.testRetiredAffineRegister(t, 21, "REGNOPREC", true,
		func(c *mqttQ2AffClaim) { c.SourceAcked = true })

	var writes int
	e.asm.mu.Lock()
	e.asm.testQ2AffClaimStore = func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		if subject == claimSubject && rec != nil {
			writes++
			if rec.PubRecProven {
				t.Errorf("the sweep's register arm wrote a PUBREC proof it does not hold: %+v", rec)
			}
		}
		return e.asm.jsa.storeQ2AffClaimCAS(subject, expectSeq, rec)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testQ2AffClaimStore = nil
		e.asm.mu.Unlock()
	})

	e.sweepInflight(t, 16)

	// POSITIVE CONTROL: the arm really did transition this claim, so "no PubRecProven"
	// is a fact about what it wrote rather than about it never running.
	if writes == 0 {
		t.Fatal("the sweep never transitioned this claim; the fixture is not on its route")
	}
}
