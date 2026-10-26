//go:build mqtt_fulltests && mqtt_experimental_shared_qos2

package mqtt

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// The PUBREC proof, end to end.
//
// mqttProcessPublishReceived stores the correlated PUBREL BEFORE
// mqttFinishQ2AffineDelivery records the affine side, so a broker that stops between
// the two — with the affine consumer then destroyed out of band, taking its copies
// with it off the interest-retained $MQTT_out — used to leave exactly the shape
// recoverQ2AffClaims reports as a LOST delivery, for a message the client had already
// answered and whose handshake completes from that very PUBREL.
//
// The correction is that the PUBREL names the assignment it completes, as a BOUND
// (claim subject, copy subject) pair. These tests are about that reference: that it is
// written, that it is structurally exact, that resolving it proves the delivery, that
// an unresolved one never lets a loss be inferred, and that nothing acknowledges it
// away while the proof it carries is unrecorded.

// The stored PUBREL of a real affine delivery names its assignment.
//
// Paired mutation M1: drop the reference at the store site in
// mqttProcessPublishReceived. The record then carries no affine headers and this fails.
func TestQ2AffPubRelCarriesItsClaimReference(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "prefwrite")
	ca, ra := b.member(t, "pwA", 300)
	defer ca.Close()
	b.bind(t, "pwA")

	sess := testMQTTGetClient(t, b.s, "pwA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	pubRelSubject := sess.pubRelSubject
	sess.mu.Unlock()
	if gen == _EMPTY_ {
		t.Fatal("the affine path was never provisioned")
	}

	b.publish(t, 1, "proofwrite")
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(ra, 5*time.Second)
	if !ok || qos != 2 || string(payload) != "proofwrite" {
		t.Fatalf("shared QoS 2 delivery: ok=%v qos=%d payload=%q", ok, qos, payload)
	}
	claimSubject, claim := b.waitForClaim(t, "materialized", func(c *mqttQ2AffClaim) bool {
		return c.Phase == mqttQ2AffMaterialized
	})

	// PUBREC only. The PUBREL record is stored on this path.
	testMQTTSendPIPacket(mqttPacketPubRec, t, ca, pi)

	var found mqttPubRelAffineRef
	testQ2AffEventually(t, 5*time.Second, func() error {
		smsg, err := sess.jsa.loadNextMsgFromSeq(mqttOutStreamName, pubRelSubject, 1)
		if err != nil {
			return err
		}
		found = mqttParsePubRelAffineRef(smsg.Header)
		if !found.valid() {
			return fmt.Errorf("the stored PUBREL carries no valid affine reference: %+v", found)
		}
		return nil
	})
	if found.Claim != claimSubject {
		t.Fatalf("the PUBREL names claim %q, want %q", found.Claim, claimSubject)
	}
	if found.Copy != claim.CopySubject {
		t.Fatalf("the PUBREL names copy %q, want %q", found.Copy, claim.CopySubject)
	}
}

// A LOST verdict is an INFERENCE from an absent copy. A stored PUBREL naming the same
// assignment incarnation is a PROOF that the client answered PUBREC. The proof must be
// able to correct the inference, whichever landed first — otherwise a cross-broker race
// makes a mislabelling permanent, because mergeProven used to give whichever arrived
// first the last word.
//
// Paired mutations: M9 (key dominance on AffineDone instead of PubRecProven) and M10
// (drop the dominance rule entirely) both leave Lost standing here.
func TestQ2AffLostIsCorrectedByALaterProof(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		writeRef   bool
		wantProven bool
	}{
		// The positive control comes first and is not decoration: without it a test
		// asserting "Lost was cleared" cannot tell a working correction from a fixture
		// that never wrote Lost in the first place.
		{name: "no reference on the PUBREL, so the loss stands", writeRef: false},
		{name: "a bound reference corrects it", writeRef: true, wantProven: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQ2AffSetup(t, "lostfix")
			ca, _ := b.member(t, "lfA", 300)
			defer ca.Close()
			b.bind(t, "lfA")

			c := testMQTTGetClient(t, b.s, "lfA")
			sess := c.mqtt.sess
			sess.mu.Lock()
			gen := sess.q2AffGen
			pubRelSubject := sess.pubRelSubject
			sess.mu.Unlock()
			idHash := getHash("lfA")

			claimSubject, copySubject := testQ2AffSeedLostClaim(t, b, idHash, gen, 4711)

			ref := mqttPubRelAffineRef{}
			if tc.writeRef {
				ref = mqttPubRelAffineRef{Claim: claimSubject, Copy: copySubject}
			}
			testQ2AffStorePubRel(t, sess, pubRelSubject, 7, ref)

			l := &captureWarnLogger{warn: make(chan string, 64)}
			b.s.SetLogger(l, false, false)

			if got := sess.reconcileQ2AffPubRelProofs(c); got != mqttQ2AffProofComplete {
				t.Fatalf("the proof pass reported incomplete evidence (%v) on a readable fixture", got)
			}

			// The RETRACTION is owed to whoever read the loss report. A superseded
			// broker's PUBREL store is not CAS-conditioned, so it can land after a loss
			// verdict has already been written and logged; clearing the verdict silently
			// leaves the operator with an erroneous report and nothing contradicting it,
			// because the claim is then reaped.
			// The two diagnostics are emitted by different goroutines against a record
			// that is the real authority, so no ORDER between them is asserted — only
			// that a retraction is emitted when, and only when, a proof clears a
			// recorded loss, and that the loss report itself never claims a final fact.
			retractions := testQ2AffCountWarnings(l, "retracting the LOST report")
			if want := map[bool]int{true: 1, false: 0}[tc.wantProven]; retractions != want {
				t.Fatalf("the loss report was retracted %d times, want %d", retractions, want)
			}

			got, _, err := b.asm.loadQ2AffClaim(claimSubject)
			if err != nil {
				t.Fatalf("loading the claim: %v", err)
			}
			if !tc.wantProven {
				if got == nil {
					t.Fatal("an unproven lost assignment was reaped")
				}
				if !got.Lost || got.PubRecProven || got.AffineDone {
					t.Fatalf("a PUBREL with no reference changed the verdict: %+v", got)
				}
				return
			}
			// Proven claims are reaped, so absence here is the success case — but only
			// because the pass ran; the control above proves the fixture reaches it.
			if got != nil {
				if !got.PubRecProven || got.Lost || !got.AffineDone {
					t.Fatalf("the proof did not correct the loss: %+v", got)
				}
				t.Fatalf("the corrected assignment was not reaped: %+v", got)
			}
		})
	}
}

// The scoping test. AffineDone means "terminal copy disposition" and is written by
// seven routes, one of which is the delivery gate's own `AffineDone || Lost` arm. If
// generic AffineDone outranked Lost, "this claim is already Lost" would become the
// reason to clear Lost — a circular laundering of an unproven outcome into a clean
// reap. Only a resolved PUBREL carrier may set PubRecProven, and only PubRecProven
// dominates.
//
// Paired mutation M9: make mergeProven key the dominance on AffineDone.
func TestQ2AffGenericAffineDoneStillLosesToLost(t *testing.T) {
	t.Parallel()
	// The merge rule itself, in both arrival orders.
	lost := &mqttQ2AffClaim{AffineDone: true}
	lost.mergeProven(&mqttQ2AffClaim{Lost: true})
	if lost.AffineDone || !lost.Lost {
		t.Fatalf("a bare terminal disposition outranked a recorded loss: %+v", lost)
	}
	proven := &mqttQ2AffClaim{PubRecProven: true, AffineDone: true}
	proven.mergeProven(&mqttQ2AffClaim{Lost: true})
	if !proven.PubRecProven || !proven.AffineDone || proven.Lost {
		t.Fatalf("a PUBREC proof failed to outrank a recorded loss: %+v", proven)
	}
	// And the reverse order, which is the cross-broker race that motivated the field.
	late := &mqttQ2AffClaim{PubRecProven: true, AffineDone: true}
	late.mergeProven(&mqttQ2AffClaim{Lost: true, Phase: mqttQ2AffMaterialized})
	if late.Lost {
		t.Fatalf("a Lost that landed first survived a later proof: %+v", late)
	}

	// And the route: the generic finish path must pass pubRecProven=false, so a claim
	// it touches under a recorded loss keeps that loss.
	b := testQ2AffSetup(t, "genericdone")
	ca, _ := b.member(t, "gdA", 300)
	defer ca.Close()
	b.bind(t, "gdA")
	c := testMQTTGetClient(t, b.s, "gdA")
	sess := c.mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("gdA")

	claimSubject, copySubject := testQ2AffSeedLostClaim(t, b, idHash, gen, 5150)
	c.mqttFinishQ2AffineDelivery(claimSubject, copySubject, _EMPTY_)

	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil {
		t.Fatal("the generic finish route reaped a lost assignment")
	}
	if got.PubRecProven {
		t.Fatalf("the generic finish route claimed a PUBREC proof it does not hold: %+v", got)
	}
	if !got.Lost {
		t.Fatalf("the generic finish route cleared a recorded loss: %+v", got)
	}
}

// An absent copy is only evidence of a loss once the thing that would refute it has
// actually been looked for. A resume whose PUBREL walk failed has looked for nothing,
// so it must write nothing — and it must converge on the next resume, which is what
// separates a deferral from a stall.
//
// The scan failure is ONE-SHOT deliberately. A permanently unreadable fixture would
// also leave Lost unwritten through recoverQ2AffClaims' own unreadable-copy arm, so the
// test would pass against a broker with no evidence gate at all.
//
// Paired mutation M4: let recovery infer a loss on incomplete evidence.
func TestQ2AffRecoveryDefersOnIncompleteEvidence(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "deferev")
	ca, _ := b.member(t, "deA", 300)
	defer ca.Close()
	b.bind(t, "deA")

	c := testMQTTGetClient(t, b.s, "deA")
	sess := c.mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("deA")

	claimSubject, _ := testQ2AffSeedMaterializedClaim(t, b, idHash, gen, 8801)

	// A resume that gathered nothing must not conclude a loss.
	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofIncomplete)
	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil {
		t.Fatal("a deferred assignment was reaped")
	}
	if got.Lost {
		t.Fatalf("a loss was inferred from evidence that was never gathered: %+v", got)
	}

	// The very next resume, with the walk complete, settles it. Without this the test
	// would also pass against a broker that simply never writes Lost.
	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)
	got, _, err = b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim after the complete resume: %v", err)
	}
	if got == nil || !got.Lost {
		t.Fatalf("a complete resume did not settle the assignment: %+v", got)
	}
}

// A claim subject is REUSED by a later incarnation of the same assignment. A reference
// that resolved onto whatever occupies the subject now would credit a SUCCESSOR claim
// with a delivery it never made, and its own copy would then be acknowledged away
// unpublished. The copy half of the pair is what prevents that.
//
// Paired mutation M2: remove BOTH the resolver's copy binding and
// advanceQ2AffClaimChecked's wantCopy CAS fence, so the pass performs a genuinely bare
// transition. Removing only the resolver leaves the CAS rejecting the successor, which
// is why the mutation has to take both.
func TestQ2AffProofDoesNotTouchASuccessorClaim(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "successor")
	ca, _ := b.member(t, "suA", 300)
	defer ca.Close()
	b.bind(t, "suA")

	c := testMQTTGetClient(t, b.s, "suA")
	sess := c.mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	pubRelSubject := sess.pubRelSubject
	sess.mu.Unlock()
	idHash := getHash("suA")

	// The SUCCESSOR occupies the subject; the PUBREL names the PREDECESSOR's copy.
	claimSubject, successorCopy := testQ2AffSeedMaterializedClaim(t, b, idHash, gen, 9001)
	groupDur := b.groupDurable(t)
	predecessorCopy := mqttQ2AffCopySubject(idHash, gen, mqttQ2AffClaimHash(groupDur, 9001),
		testQ2AffFixtureNonce2)
	if predecessorCopy == successorCopy {
		t.Fatal("the fixture's two incarnations share a copy subject; it cannot test what it claims")
	}
	testQ2AffStorePubRel(t, sess, pubRelSubject, 11,
		mqttPubRelAffineRef{Claim: claimSubject, Copy: predecessorCopy})

	sess.reconcileQ2AffPubRelProofs(c)

	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil {
		t.Fatal("the successor assignment was reaped on a predecessor's evidence")
	}
	if got.PubRecProven || got.AffineDone {
		t.Fatalf("a predecessor's PUBREL credited the successor with its delivery: %+v", got)
	}
	if got.CopySubject != successorCopy {
		t.Fatalf("the successor's own copy binding was rewritten: %+v", got)
	}
}

// A record shape this broker does not write is REFUSED, not normalised. Route-level
// rather than a direct call on the validator: a unit test of validateOrphanQ2AffClaim
// keeps passing when the call sites are mutated to nil prechecks, and the precheck is
// exactly what is under test.
//
// Paired mutation M11: pass a nil precheck instead of q2AffShapePrecheck in the pass
// and in the PUBCOMP arm.
func TestQ2AffProofRefusesContradictoryRecordShapes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		breakIt func(*mqttQ2AffClaim)
	}{
		{"a proven PUBREC beside a recorded loss", func(c *mqttQ2AffClaim) {
			c.PubRecProven, c.Lost = true, true
		}},
		{"a proven PUBREC with no terminal disposition", func(c *mqttQ2AffClaim) {
			c.PubRecProven, c.AffineDone = true, false
		}},
		{"an unknown phase", func(c *mqttQ2AffClaim) { c.Phase = mqttQ2AffPhase("wat") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQ2AffSetup(t, "refuseshape")
			ca, _ := b.member(t, "rsA", 300)
			defer ca.Close()
			b.bind(t, "rsA")

			c := testMQTTGetClient(t, b.s, "rsA")
			sess := c.mqtt.sess
			sess.mu.Lock()
			gen := sess.q2AffGen
			pubRelSubject := sess.pubRelSubject
			sess.mu.Unlock()
			idHash := getHash("rsA")

			claimSubject, copySubject := testQ2AffSeedMaterializedClaim(t, b, idHash, gen, 2200)
			before, seq, err := b.asm.loadQ2AffClaim(claimSubject)
			if err != nil || before == nil {
				t.Fatalf("loading the seeded claim: %v %+v", err, before)
			}
			tc.breakIt(before)
			if _, err := b.asm.storeQ2AffClaimCAS(claimSubject, seq, before); err != nil {
				t.Fatalf("writing the contradictory record: %v", err)
			}
			testQ2AffStorePubRel(t, sess, pubRelSubject, 13,
				mqttPubRelAffineRef{Claim: claimSubject, Copy: copySubject})

			if got := sess.reconcileQ2AffPubRelProofs(c); got != mqttQ2AffProofIncomplete {
				t.Fatalf("the resume pass did not report an unsupported record shape as unresolved: %v", got)
			}
			// The PUBCOMP arm is its OWN call site and is asserted separately: a mutation
			// that drops only ITS precheck is invisible to the pass's assertion above.
			// Withholding here is what stops that arm normalising an unsupported record
			// and then destroying the carrier that names it.
			if sess.dischargeQ2AffPubRelProof(c, 77, mqttDeferredKey{}, mqttPubRelAffineRef{
				Claim: claimSubject, Copy: copySubject, State: mqttPubRelAffineRefValid,
			}) {
				t.Fatal("the PUBCOMP arm acknowledged a PUBREL naming a record shape this broker does not write")
			}
			after, _, err := b.asm.loadQ2AffClaim(claimSubject)
			if err != nil {
				t.Fatalf("re-loading the claim: %v", err)
			}
			if after == nil {
				t.Fatal("an unsupported record was reaped rather than refused")
			}
			if *after != *before {
				t.Fatalf("an unsupported record was normalised rather than refused:\n before %+v\n after  %+v",
					before, after)
			}
		})
	}
}

// The sweep's register arm proves the SOURCE is past PUBREC, not that a PUBREL bound to
// this claim incarnation exists. It must therefore never set PubRecProven, or the field
// stops meaning what the dominance rule relies on.
//
// Paired mutation M15: make that route pass pubRecProven = true.
func TestQ2AffSweepCannotProvePubRec(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "sweepnoproof")
	ca, _ := b.member(t, "snA", 300)
	defer ca.Close()
	b.bind(t, "snA")

	c := testMQTTGetClient(t, b.s, "snA")
	sess := c.mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("snA")

	claimSubject, copySubject := testQ2AffSeedLostClaim(t, b, idHash, gen, 3300)
	// The generic funnel, which every route other than the two proof routes uses.
	c.mqttFinishQ2AffineDelivery(claimSubject, copySubject, _EMPTY_)

	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil {
		t.Fatal("a lost assignment was reaped by a route that proves nothing about PUBREC")
	}
	if got.PubRecProven {
		t.Fatalf("a route with no PUBREL carrier recorded a PUBREC proof: %+v", got)
	}
}

// Absent and malformed are DIFFERENT facts. Absent is the current, permanent shape of
// every PUBREL that does not complete an affine copy; malformed is a persisted shape
// this broker does not write, and folding it into absent would let a loss be inferred
// around a record that may hold the proof and let that record be acknowledged away.
//
// Paired mutations M8 (read a partial pair as absent) and M12 (loosen the claim-subject
// validator back to a bare prefix-and-sequence check).
func TestPubRelAffineRefTriState(t *testing.T) {
	t.Parallel()
	goodClaim := mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcdef", 42)
	goodCopy := mqttQ2AffCopySubject("abcd1234", testQ2AffFixtureNonce,
		strings.Repeat("a", mqttQ2AffClaimHashLen), testQ2AffFixtureNonce2)
	if !mqttValidQ2AffClaimSubject(goodClaim) || !mqttValidQ2AffCopySubject(goodCopy) {
		t.Fatalf("the fixture's own subjects are not valid: %q %q", goodClaim, goodCopy)
	}
	for _, tc := range []struct {
		name        string
		claim, copy string
		want        mqttPubRelAffineRefState
	}{
		{"neither header", _EMPTY_, _EMPTY_, mqttPubRelAffineRefAbsent},
		{"the exact pair", goodClaim, goodCopy, mqttPubRelAffineRefValid},
		{"claim only", goodClaim, _EMPTY_, mqttPubRelAffineRefMalformed},
		{"copy only", _EMPTY_, goodCopy, mqttPubRelAffineRefMalformed},
		{"claim without the stream prefix", "$MQTT.other." + goodClaim, goodCopy, mqttPubRelAffineRefMalformed},
		{"claim durable of the wrong width",
			mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"short", 42), goodCopy, mqttPubRelAffineRefMalformed},
		{"claim durable outside the alphabet",
			mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcde-", 42), goodCopy, mqttPubRelAffineRefMalformed},
		{"claim durable without its own prefix",
			mqttQoS2AssignStreamSubjectPrefix + "0123456789abcdef.42", goodCopy, mqttPubRelAffineRefMalformed},
		{"claim sequence not decimal", mqttQoS2AssignStreamSubjectPrefix +
			mqttSharedConsumerDurablePrefix + "0123456789abcdef.x", goodCopy, mqttPubRelAffineRefMalformed},
		{"claim sequence zero",
			mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcdef", 0), goodCopy, mqttPubRelAffineRefMalformed},
		{"copy without the affine prefix", goodClaim, "$MQTT.out.other.a.b.c.d", mqttPubRelAffineRefMalformed},
		{"copy with too few tokens", goodClaim,
			mqttQ2AffineSubjectPrefix + "abcd1234." + testQ2AffFixtureNonce + ".x", mqttPubRelAffineRefMalformed},
		{"copy with too many tokens", goodClaim, goodCopy + ".extra", mqttPubRelAffineRefMalformed},
		{"copy id hash of the wrong width", goodClaim,
			mqttQ2AffCopySubject("abc", testQ2AffFixtureNonce, strings.Repeat("a", mqttQ2AffClaimHashLen),
				testQ2AffFixtureNonce2), mqttPubRelAffineRefMalformed},
		{"copy generation malformed", goodClaim,
			mqttQ2AffCopySubject("abcd1234", "short", strings.Repeat("a", mqttQ2AffClaimHashLen),
				testQ2AffFixtureNonce2), mqttPubRelAffineRefMalformed},
		{"copy claim hash of the wrong width", goodClaim,
			mqttQ2AffCopySubject("abcd1234", testQ2AffFixtureNonce, "short", testQ2AffFixtureNonce2),
			mqttPubRelAffineRefMalformed},
		{"claim sequence with a leading zero", mqttQoS2AssignStreamSubjectPrefix +
			mqttSharedConsumerDurablePrefix + "0123456789abcdef.042", goodCopy, mqttPubRelAffineRefMalformed},
		{"claim sequence with a sign", mqttQoS2AssignStreamSubjectPrefix +
			mqttSharedConsumerDurablePrefix + "0123456789abcdef.+42", goodCopy, mqttPubRelAffineRefMalformed},
		{"copy nonce malformed", goodClaim,
			mqttQ2AffCopySubject("abcd1234", testQ2AffFixtureNonce,
				strings.Repeat("a", mqttQ2AffClaimHashLen), "short"), mqttPubRelAffineRefMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg, hdrLen := mqttNewCorrelatedPubRel(3, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_, tc.claim, tc.copy)
			// The builder only emits the pair when BOTH are set, so a one-sided case is
			// assembled by hand — which is the shape a corrupted record actually has.
			hdr := msg[:hdrLen]
			if (tc.claim == _EMPTY_) != (tc.copy == _EMPTY_) {
				hdr = testQ2AffOneSidedPubRelHeader(tc.claim, tc.copy)
			}
			if got := mqttParsePubRelAffineRef(hdr).State; got != tc.want {
				t.Fatalf("state %v, want %v (claim %q copy %q)", got, tc.want, tc.claim, tc.copy)
			}
		})
	}
}

// Every PUBREL form that carries no affine reference must be byte-for-byte what it was
// APART FROM the owner header, which is now mandatory on every stored PUBREL. The affine
// reference is still an addition to one form rather than a change to the encoding.
//
// This test used to require the correlated builder's uncorrelated output to equal
// mqttNewDeliverablePubRel's byte for byte, and that assertion is now FALSE BY DESIGN: the
// owner identity is what turns a stored PUBREL from a claim on a packet identifier into
// evidence about one exchange, so a form without it is one this broker refuses to write.
// The assertion is inverted rather than dropped — the two forms must DIFFER, and by
// exactly that header — because "they are equal" and "they differ in the way intended" are
// different facts and only the second one is true.
func TestOrdinaryPubRelFormsAreUnchanged(t *testing.T) {
	t.Parallel()
	const owner = "xxxxxxxxxxxxxxxxxxxxxx"
	plain, plainLen := mqttNewDeliverablePubRel(9)
	got, gotLen := mqttNewCorrelatedPubRel(9, owner, _EMPTY_, 0, _EMPTY_, _EMPTY_, _EMPTY_)
	if string(got) == string(plain) {
		t.Fatalf("the owner header is mandatory, so the stored form cannot equal the "+
			"deliverable one: %q", got)
	}
	// And it differs by the owner header ALONE: removing it recovers the plain form
	// exactly, which is what "an addition to the encoding, not a change to it" means.
	ownerHdr := mqttPubRelOwnerHeader + ":" + owner + _CRLF_
	if stripped := strings.Replace(string(got[:gotLen]), ownerHdr, _EMPTY_, 1); stripped != string(plain[:plainLen]) {
		t.Fatalf("the stored form differs from the deliverable one by more than the owner "+
			"header:\n want %q\n got  %q", plain[:plainLen], stripped)
	}
	// The owner round-trips, so the header is evidence rather than padding.
	if o := mqttParsePubRelOwner(got[:gotLen]); !o.valid() || o.Nonce != owner {
		t.Fatalf("the owner identity does not round-trip: %+v", o)
	}
	// The builder REFUSES a form with no owner, which is the property that makes the
	// inequality above a rule rather than an accident.
	if msg, _ := mqttNewCorrelatedPubRel(9, _EMPTY_, _EMPTY_, 0, _EMPTY_, _EMPTY_, _EMPTY_); msg != nil {
		t.Fatalf("a stored PUBREL with no exchange identity must not be built: %q", msg)
	}
	// A correlated PUBREL with no affine reference: the two source headers and the
	// deduplication identity, in the order they have always been written.
	corr, corrLen := mqttNewCorrelatedPubRel(9, "xxxxxxxxxxxxxxxxxxxxxx", "$MQTT_SHARED_0123456789abcdef_1", 17, "q2rel.x.y.17.0",
		_EMPTY_, _EMPTY_)
	hdr := string(corr[:corrLen])
	if strings.Contains(hdr, mqttPubRelAffClaimHeader) || strings.Contains(hdr, mqttPubRelAffCopyHeader) {
		t.Fatalf("a non-affine correlated PUBREL grew affine headers: %q", hdr)
	}
	if c := mqttParsePubRelCorrelation(corr[:corrLen]); !c.Ok || c.SrcDur != "$MQTT_SHARED_0123456789abcdef_1" || c.SrcSeq != 17 {
		t.Fatalf("the source correlation no longer round-trips: %+v", c)
	}
	if r := mqttParsePubRelAffineRef(corr[:corrLen]); r.State != mqttPubRelAffineRefAbsent {
		t.Fatalf("a PUBREL with no affine headers did not parse as absent: %+v", r)
	}
}

// The enlarged affine PUBREL is budgeted as its OWN alternative form. Asserting only
// that it fits the overall QoS 2 budget would pass against the bound being omitted
// entirely, because the proven assignment-claim form is larger and dominates the
// maximum — the default-value trap this suite exists to avoid.
//
// Paired mutations M14 (drop the affine-reference contribution from the calculator) and
// M16 (add that contribution to mqttQ2AffineFormMax instead, summing two forms that
// never coexist, which also breaks the affine bound's own tightness assertion).
func TestQ2AffPubRelFormMaxMatchesTheEncoder(t *testing.T) {
	t.Parallel()
	bound := mqttQ2AffPubRelFormMax()
	maxClaimSubject, _, genW := mqttQ2AffMaxIdentityWidths()
	maxDur := len(mqttSharedConsumerDurablePrefix) + 16
	maxCopySubject := len(mqttQ2AffineSubjectPrefix) + sysHashLen + 1 + genW + 1 +
		mqttQ2AffClaimHashLen + 1 + genW
	msgId := mqttPubRelMsgId(strings.Repeat("x", sysHashLen), strings.Repeat("x", maxDur),
		math.MaxUint64, math.MaxUint64)
	_, encoded := mqttNewCorrelatedPubRel(math.MaxUint16, "xxxxxxxxxxxxxxxxxxxxxx", strings.Repeat("x", maxDur), math.MaxUint64,
		msgId, strings.Repeat("x", maxClaimSubject), strings.Repeat("x", maxCopySubject))
	if bound != encoded {
		t.Fatalf("the affine PUBREL bound is %d, the real encoder emits %d", bound, encoded)
	}
	// Its own candidate: the reference must not have been folded into the affine COPY's
	// bound, which describes a record that never coexists with this one.
	withRef, _ := mqttNewCorrelatedPubRel(1, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_,
		strings.Repeat("x", maxClaimSubject), strings.Repeat("x", maxCopySubject))
	withoutRef, _ := mqttNewCorrelatedPubRel(1, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_, _EMPTY_, _EMPTY_)
	if contribution := len(withRef) - len(withoutRef); contribution <= 0 {
		t.Fatalf("the affine reference contributes %d bytes; the bound cannot be exercised", contribution)
	}
	// And it is actually COMPARED. Asserting only that the overall maximum is at least
	// this bound proves nothing, because the assignment-claim form is larger and
	// dominates: that assertion holds with this candidate removed entirely. So the
	// candidate SET is checked by name, which is what a removal actually changes.
	pp := &mqttPublish{topic: []byte("t"), subject: []byte("t"), sz: 1}
	cands := mqttQoS2FormCandidates(pp, 1, false, _EMPTY_)
	got, ok := cands["affine pubrel"]
	if !ok {
		t.Fatalf("the affine PUBREL form is not among the budgeted QoS 2 forms: %v", cands)
	}
	if got != bound {
		t.Fatalf("the budgeted affine PUBREL candidate is %d, its own bound is %d", got, bound)
	}
	max := 0
	for _, n := range cands {
		if n > max {
			max = n
		}
	}
	if total := mqttMaxQoS2FormSize(pp, 1, false, _EMPTY_); total != max {
		t.Fatalf("mqttMaxQoS2FormSize returned %d, the maximum over its candidates is %d", total, max)
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// testQ2AffFixtureNonce2 is a SECOND incarnation nonce, so a fixture can name a
// predecessor's copy under a subject a successor now occupies.
const testQ2AffFixtureNonce2 = "F065fixtureNonce000002"

// testQ2AffSeedMaterializedClaim writes a nonterminal materialized claim owned by
// (idHash, gen) and returns its subject and copy subject.
func testQ2AffSeedMaterializedClaim(t *testing.T, b *testQ2AffBroker, idHash, gen string, sseq uint64) (string, string) {
	t.Helper()
	return testQ2AffSeedClaim(t, b, idHash, gen, sseq, func(*mqttQ2AffClaim) {})
}

// testQ2AffSeedLostClaim writes a claim already carrying the LOST verdict, which is the
// state a proof has to be able to correct.
func testQ2AffSeedLostClaim(t *testing.T, b *testQ2AffBroker, idHash, gen string, sseq uint64) (string, string) {
	t.Helper()
	return testQ2AffSeedClaim(t, b, idHash, gen, sseq, func(c *mqttQ2AffClaim) { c.Lost = true })
}

func testQ2AffSeedClaim(t *testing.T, b *testQ2AffBroker, idHash, gen string, sseq uint64,
	mutate func(*mqttQ2AffClaim)) (string, string) {
	t.Helper()
	groupDur := b.groupDurable(t)
	claimHash := mqttQ2AffClaimHash(groupDur, sseq)
	subject := mqttQ2AffClaimSubject(groupDur, sseq)
	copySubject := mqttQ2AffCopySubject(idHash, gen, claimHash, testQ2AffFixtureNonce)
	_, _, affDur := mqttQ2AffineNames(idHash, gen)
	rec := &mqttQ2AffClaim{
		Winner: idHash, Gen: gen, GroupDurable: groupDur, SourceSeq: sseq,
		ClaimHash: claimHash, Nonce: testQ2AffFixtureNonce, CopySubject: copySubject,
		AffDurable: affDur, Phase: mqttQ2AffMaterialized, SourceAcked: true,
	}
	mutate(rec)
	if _, err := b.asm.storeQ2AffClaimCAS(subject, 0, rec); err != nil {
		t.Fatalf("seeding the claim on %q: %v", subject, err)
	}
	return subject, copySubject
}

// testQ2AffStorePubRel stores one PUBREL record on the session's PUBREL subject,
// carrying ref when it names both halves. An empty ref produces the ordinary
// uncorrelated form, which is the positive control's shape.
func testQ2AffStorePubRel(t *testing.T, sess *mqttSession, pubRelSubject string, pi uint16,
	ref mqttPubRelAffineRef) {
	t.Helper()
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_, ref.Claim, ref.Copy)
	if _, err := sess.jsa.storeMsg(pubRelSubject, hdrLen, msg); err != nil {
		t.Fatalf("storing the PUBREL on %q: %v", pubRelSubject, err)
	}
}

// testQ2AffOneSidedPubRelHeader assembles a PUBREL header block carrying exactly one of
// the two affine headers, which the builder will not produce and a corrupted record can.
func testQ2AffOneSidedPubRelHeader(claim, copySubject string) []byte {
	var b strings.Builder
	b.WriteString(hdrLine)
	b.WriteString(mqttNatsPubRelHeader)
	b.WriteString(":3")
	b.WriteString(_CRLF_)
	if claim != _EMPTY_ {
		b.WriteString(mqttPubRelAffClaimHeader)
		b.WriteByte(':')
		b.WriteString(claim)
		b.WriteString(_CRLF_)
	}
	if copySubject != _EMPTY_ {
		b.WriteString(mqttPubRelAffCopyHeader)
		b.WriteByte(':')
		b.WriteString(copySubject)
		b.WriteString(_CRLF_)
	}
	b.WriteString(_CRLF_)
	return []byte(b.String())
}

// testQ2AffEventually retries fn until it returns nil or the deadline passes, failing
// with the last error. JetStream stores are asynchronous with respect to the client's
// PUBREC being written, so a single immediate read is a race rather than an assertion.
func testQ2AffEventually(t *testing.T, d time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	var last error
	for time.Now().Before(deadline) {
		if last = fn(); last == nil {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("condition never held within %v: %v", d, last)
}

// A PUBREL is the only durable evidence that the client answered PUBREC for its
// assignment. PUBCOMP must not acknowledge it away while that evidence is unrecorded —
// and, crucially, withholding must KEEP THE RESERVATION. An entry untracked and then
// withheld frees its packet identifier; after a wrap a different PUBREL is tracked under
// it, and when the original redelivers the duplicate branch acknowledges it away. The
// reservation IS the pending entry, so that is what this asserts.
//
// Paired mutations M5 (untrack before withholding) and M17a-d (drop each of the
// re-lock fence's four conditions).
func TestQ2AffPubCompHoldsTheIdentifierWhileItWithholds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		ref      func(claim, copySubject string) mqttPubRelAffineRef
		breakIt  func(t *testing.T, b *testQ2AffBroker, claimSubject string)
		wantHeld bool
	}{
		{
			// The positive control. Without it, "the entry is still there" cannot tell a
			// working hold from a fixture that never reached the PUBCOMP route.
			name: "a resolvable proof is recorded and the entry is released",
			ref: func(claim, copySubject string) mqttPubRelAffineRef {
				return mqttPubRelAffineRef{Claim: claim, Copy: copySubject, State: mqttPubRelAffineRefValid}
			},
		},
		{
			name: "an unresolvable claim withholds and keeps the identifier",
			ref: func(claim, copySubject string) mqttPubRelAffineRef {
				return mqttPubRelAffineRef{Claim: claim, Copy: copySubject, State: mqttPubRelAffineRefValid}
			},
			breakIt: func(t *testing.T, b *testQ2AffBroker, claimSubject string) {
				b.asm.mu.Lock()
				b.asm.testQ2AffClaimLoad = func(string) (*mqttQ2AffClaim, uint64, error) {
					return nil, 0, errors.New("injected: the claim record is unreadable")
				}
				b.asm.mu.Unlock()
				t.Cleanup(func() {
					b.asm.mu.Lock()
					b.asm.testQ2AffClaimLoad = nil
					b.asm.mu.Unlock()
				})
			},
			wantHeld: true,
		},
		{
			name: "a malformed carrier withholds and keeps the identifier",
			ref: func(claim, copySubject string) mqttPubRelAffineRef {
				return mqttPubRelAffineRef{Claim: "not-a-claim-subject", Copy: copySubject,
					State: mqttPubRelAffineRefMalformed}
			},
			wantHeld: true,
		},
		{
			// The claim is gone: acknowledging is right. With no record there is nothing
			// for a later resume to report untruthfully, and withholding would wedge the
			// exchange on something that can never reappear.
			name: "an absent claim acknowledges rather than wedging",
			ref: func(claim, copySubject string) mqttPubRelAffineRef {
				return mqttPubRelAffineRef{
					Claim: mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcdef", 999),
					Copy:  copySubject, State: mqttPubRelAffineRefValid,
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQ2AffSetup(t, "pubcomphold")
			ca, _ := b.member(t, "phA", 300)
			defer ca.Close()
			b.bind(t, "phA")

			c := testMQTTGetClient(t, b.s, "phA")
			sess := c.mqtt.sess
			sess.mu.Lock()
			gen := sess.q2AffGen
			sess.mu.Unlock()
			idHash := getHash("phA")

			claimSubject, copySubject := testQ2AffSeedMaterializedClaim(t, b, idHash, gen, 6600)

			const pi = uint16(21)
			sess.mu.Lock()
			sess.pendingPubRel = map[uint16]*mqttPending{pi: {
				jsDur:        "$MQTT_PUBREL_fixture",
				sseq:         5,
				jsAckSubject: "$JS.ACK.fixture.1.1.1.1.0",
				pubRelAffRef: tc.ref(claimSubject, copySubject),
			}}
			sess.mu.Unlock()
			if tc.breakIt != nil {
				tc.breakIt(t, b, claimSubject)
			}

			c.mqttProcessPubComp(pi)

			sess.mu.Lock()
			_, stillHeld := sess.pendingPubRel[pi]
			sess.mu.Unlock()
			if stillHeld != tc.wantHeld {
				t.Fatalf("after PUBCOMP the identifier is held=%v, want %v — a withheld exchange must "+
					"keep its packet identifier and Receive Maximum slot, and an acknowledged one must free them",
					stillHeld, tc.wantHeld)
			}
		})
	}
}

// NOT TESTED HERE, and deliberately not papered over: the pass's independence from a
// FAILING affine restoration. The constraint is real — the recovery-and-reap route at the
// end of resendPendingRedelivery is skipped when ensureQ2AffineConsumerSubscription fails
// there, which is why the pass runs before it and performs its own reap — but a fixture
// for it could not be built. Every way of making that restoration fail that this suite can
// reach (an unaccountable consumer config, a refused create) fails EARLIER, pre-CONNACK,
// so resendPendingRedelivery is never entered at all and the fixture proves nothing. The
// contender's create seam does not help: it covers materializeQ2AffOnBehalf only, leaving
// the owner's own provisioning on the direct path by design. An earlier draft of this file
// carried such a fixture; it passed while never reaching its route, so it was removed
// rather than kept as evidence it could not supply.

// The final re-lock fence. The affine discharge runs WITHOUT the session lock, so by the
// time PUBCOMP is ready to remove the entry a teardown can have cleared `pendingPubRel`,
// and a clean start or takeover can have rebound the session and re-minted this
// identifier for a new incarnation. Untracking blind there hands back an empty
// acknowledgement subject, or removes and acknowledges an exchange that is not the one
// this PUBCOMP answered.
//
// Each subtest mutates exactly the state one condition guards, so M17a-d each have
// something to kill. The reference used throughout resolves to an ABSENT claim, so the
// discharge returns "acknowledge" and the fence is the only thing left deciding.
//
// Paired mutations M17a (drop the whole fence), M17b (drop the empty-ack-subject
// condition), M17c (drop the jsDur/sseq identity) and M17d (drop `sess.c == c`).
func TestQ2AffPubCompFenceRejectsAReplacedExchange(t *testing.T) {
	t.Parallel()
	absentClaim := mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcdef", 999)
	for _, tc := range []struct {
		name    string
		replace func(*mqttPending) *mqttPending
		rebind  bool
		wantOut bool // the entry should be REMOVED (acknowledged)
	}{
		{
			// The positive control: nothing moved, so the exchange completes.
			name:    "nothing moved",
			replace: func(a *mqttPending) *mqttPending { return a },
			wantOut: true,
		},
		{
			name: "the entry is a placeholder with no acknowledgement subject",
			replace: func(a *mqttPending) *mqttPending {
				b := *a
				b.jsAckSubject = _EMPTY_
				return &b
			},
		},
		{
			name: "a new incarnation re-minted the identifier for a different record",
			replace: func(a *mqttPending) *mqttPending {
				b := *a
				b.sseq, b.jsDur = a.sseq+1, "$MQTT_PUBREL_other"
				return &b
			},
		},
		{
			// The session was rebound to another connection while the discharge ran: a
			// takeover, or a clean start. This PUBCOMP no longer owns the exchange.
			name:    "the session was rebound to another connection",
			rebind:  true,
			replace: func(a *mqttPending) *mqttPending { return a },
		},
		{
			// The same record redelivered by AckWait: the reply subject changes while
			// (durable, sequence) stay put. This one must be ALLOWED through, which is
			// what keeps the fence from turning an ordinary redelivery into a wedge.
			name: "an AckWait redelivery of the same record",
			replace: func(a *mqttPending) *mqttPending {
				b := *a
				b.jsAckSubject = "$JS.ACK.fixture.1.1.1.2.0"
				return &b
			},
			wantOut: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQ2AffSetup(t, "pubcompfence")
			ca, _ := b.member(t, "pfA", 300)
			defer ca.Close()
			b.bind(t, "pfA")

			c := testMQTTGetClient(t, b.s, "pfA")
			sess := c.mqtt.sess

			const pi = uint16(31)
			base := &mqttPending{
				jsDur: "$MQTT_PUBREL_fixture", sseq: 9,
				jsAckSubject: "$JS.ACK.fixture.1.1.1.1.0",
				pubRelAffRef: mqttPubRelAffineRef{
					Claim: absentClaim,
					Copy: mqttQ2AffCopySubject("abcd1234", testQ2AffFixtureNonce,
						strings.Repeat("a", mqttQ2AffClaimHashLen), testQ2AffFixtureNonce2),
					State: mqttPubRelAffineRefValid,
				},
			}
			// The swap happens between the discharge and the re-lock, which is the window
			// the fence exists for. testQ2AffClaimLoad is on the discharge's own path, so
			// it is the exact seam.
			b.asm.mu.Lock()
			b.asm.testQ2AffClaimLoad = func(string) (*mqttQ2AffClaim, uint64, error) {
				sess.mu.Lock()
				sess.pendingPubRel[pi] = tc.replace(base)
				if tc.rebind {
					sess.c = nil
				}
				sess.mu.Unlock()
				return nil, 0, nil // authoritatively absent
			}
			b.asm.mu.Unlock()
			t.Cleanup(func() {
				b.asm.mu.Lock()
				b.asm.testQ2AffClaimLoad = nil
				b.asm.mu.Unlock()
			})

			sess.mu.Lock()
			sess.pendingPubRel = map[uint16]*mqttPending{pi: base}
			sess.mu.Unlock()

			c.mqttProcessPubComp(pi)

			sess.mu.Lock()
			_, stillHeld := sess.pendingPubRel[pi]
			sess.mu.Unlock()
			if stillHeld == tc.wantOut {
				t.Fatalf("after PUBCOMP the entry is held=%v, want held=%v", stillHeld, !tc.wantOut)
			}
		})
	}
}

// The proof is resolved BEFORE the loss inference is attempted, on the real resume path.
//
// With the dominance rule in place the final record is correct either way, which is
// exactly why this test asserts on the DIAGNOSTIC: reporting a loss and then silently
// retracting it is the untruthfulness this item is about, and the ordering is what stops
// it being emitted at all. It also covers the rule that the loss line is emitted only for
// a loss that is ON the record the transition left behind — a nil error from
// advanceQ2AffClaim is not the verdict, because a concurrent proof turns the Lost write
// into a merge no-op and a vanished record reports success too.
//
// The seeded claim is `materialized`, so recovery takes the LOSS arm rather than the
// sourceless `default` arm; without that the test would prove less than it says.
//
// Paired mutations M3 (order the pass after recoverQ2AffClaims) and M18 (log on a nil
// error instead of on a landed Lost).
func TestQ2AffProofPrecedesTheLossInference(t *testing.T) {
	b := testQ2AffSetup(t, "proofbefore")
	l := &captureErrorLogger{errCh: make(chan string, 64)}
	b.s.SetLogger(l, false, false)

	ca, _ := b.member(t, "pbA", 300)
	b.bind(t, "pbA")

	sess := testMQTTGetClient(t, b.s, "pbA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	pubRelSubject := sess.pubRelSubject
	sess.mu.Unlock()
	idHash := getHash("pbA")

	// A nonterminal assignment whose copy is gone: exactly the shape recovery reads as a
	// loss. The PUBREL naming it is the proof that it was in fact delivered.
	claimSubject, copySubject := testQ2AffSeedMaterializedClaim(t, b, idHash, gen, 9100)
	testQ2AffStorePubRel(t, sess, pubRelSubject, 23,
		mqttPubRelAffineRef{Claim: claimSubject, Copy: copySubject})

	ca.Close()
	cb, _ := b.reconnect(t, "pbA", 300)
	defer cb.Close()

	testQ2AffEventually(t, 5*time.Second, func() error {
		got, _, err := b.asm.loadQ2AffClaim(claimSubject)
		if err != nil {
			return err
		}
		if got != nil {
			return fmt.Errorf("the proven assignment was not reaped: %+v", got)
		}
		return nil
	})
	for {
		select {
		case msg := <-l.errCh:
			if strings.Contains(msg, "LOST QoS 2 delivery") {
				t.Fatalf("a delivery the client had answered was reported as lost: %s", msg)
			}
		default:
			return
		}
	}
}

// The pass runs, and performs its own reap, even when the affine consumer cannot be
// restored. That is the roadmap bullet's binding constraint: the recovery-and-reap route
// at the end of resendPendingRedelivery is skipped whenever
// ensureQ2AffineConsumerSubscription fails there, so discharging the reap by ordering is
// unavailable.
//
// The restoration is failed through `q2AffClosing`, which is that function's first
// refusal. Driving resendPendingRedelivery directly is deliberate: every way of failing
// the restoration through a real reconnect fails EARLIER, pre-CONNACK, so
// resendPendingRedelivery is never entered at all and the fixture would prove nothing.
//
// Paired mutations M6 (remove the pass's own reap) and M7 (move the pass inside
// ensureQ2AffineConsumerSubscription's success arm).
func TestQ2AffProofRunsWhenAffineRestorationFails(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "restorefail")
	ca, _ := b.member(t, "rfA", 300)
	defer ca.Close()
	b.bind(t, "rfA")

	c := testMQTTGetClient(t, b.s, "rfA")
	sess := c.mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	pubRelSubject := sess.pubRelSubject
	sess.mu.Unlock()
	idHash := getHash("rfA")

	claimSubject, copySubject := testQ2AffSeedLostClaim(t, b, idHash, gen, 7700)
	testQ2AffStorePubRel(t, sess, pubRelSubject, 19,
		mqttPubRelAffineRef{Claim: claimSubject, Copy: copySubject})

	sess.mu.Lock()
	sess.q2AffClosing = true
	sess.mu.Unlock()
	t.Cleanup(func() {
		sess.mu.Lock()
		sess.q2AffClosing = false
		sess.mu.Unlock()
	})

	// POSITIVE CONTROL, taken before the run: the restoration really does refuse under
	// this fixture, so recoverQ2AffClaims is never reached inside resendPendingRedelivery
	// and the reap below can only have come from the pass itself.
	tok := sess.beginRecordTx()
	_, ensureErr := sess.ensureQ2AffineConsumerSubscription(c, tok)
	if terr := sess.endRecordTx(tok); terr != nil {
		t.Fatalf("closing the probe record transaction: %v", terr)
	}
	if ensureErr == nil {
		t.Fatal("the affine delivery path restored cleanly; the fixture is not on its route")
	}

	sess.resendPendingRedelivery(c, nil, nil)
	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got != nil {
		t.Fatalf("the proven assignment was not reaped by the pass itself: %+v", got)
	}
}

// A duplicate PUBREL record under an identifier already tracked by a DIFFERENT record is
// acknowledged away — but not before its own affine reference is discharged. Several
// records under one identifier is a supported stale-broker shape, and the duplicate can
// carry the only reference to a different assignment.
//
// Paired mutation M19: acknowledge the duplicate without discharging it.
func TestQ2AffDuplicatePubRelDischargesItsProofBeforeAcking(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "duppubrel")
	ca, _ := b.member(t, "dpA", 300)
	defer ca.Close()
	b.bind(t, "dpA")

	c := testMQTTGetClient(t, b.s, "dpA")
	sess := c.mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("dpA")

	claimSubject, copySubject := testQ2AffSeedLostClaim(t, b, idHash, gen, 8200)

	const pi = uint16(41)
	sess.mu.Lock()
	sess.pendingPubRel = map[uint16]*mqttPending{pi: {
		jsDur: "$MQTT_PUBREL_fixture", sseq: 3,
		jsAckSubject: "$JS.ACK.fixture.1.1.3.1.0",
	}}
	sess.mu.Unlock()

	// The duplicate arrives on a DIFFERENT stream sequence, so the branch under test is
	// the one that acknowledges it immediately, and it names its own assignment.
	if !sess.dischargeQ2AffPubRelProof(c, pi, mqttDeferredKey{},
		mqttPubRelAffineRef{Claim: claimSubject, Copy: copySubject, State: mqttPubRelAffineRefValid}) {
		t.Fatal("the duplicate's proof could not be discharged on a readable fixture")
	}
	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got != nil {
		t.Fatalf("the duplicate's assignment was not proven and reaped: %+v", got)
	}
}

// Two occurrences of an affine header is a shape this broker does not write, and reading
// only the first would silently pick one of two assignments a resolution might then act
// on. It is REFUSED, not read as the first.
//
// Paired mutation M20: drop the exactly-one requirement from the parser.
func TestPubRelAffineRefRefusesRepeatedHeaders(t *testing.T) {
	t.Parallel()
	goodClaim := mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"0123456789abcdef", 42)
	otherClaim := mqttQ2AffClaimSubject(mqttSharedConsumerDurablePrefix+"fedcba9876543210", 43)
	goodCopy := mqttQ2AffCopySubject("abcd1234", testQ2AffFixtureNonce,
		strings.Repeat("a", mqttQ2AffClaimHashLen), testQ2AffFixtureNonce2)

	var b strings.Builder
	b.WriteString(hdrLine)
	b.WriteString(mqttNatsPubRelHeader)
	b.WriteString(":3")
	b.WriteString(_CRLF_)
	for _, cl := range []string{goodClaim, otherClaim} {
		b.WriteString(mqttPubRelAffClaimHeader)
		b.WriteByte(':')
		b.WriteString(cl)
		b.WriteString(_CRLF_)
	}
	b.WriteString(mqttPubRelAffCopyHeader)
	b.WriteByte(':')
	b.WriteString(goodCopy)
	b.WriteString(_CRLF_)
	b.WriteString(_CRLF_)

	// Positive control: the single-header form of the very same pair is valid, so a
	// failure below is about the repetition and not about the fixture's subjects.
	single, singleLen := mqttNewCorrelatedPubRel(3, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_, goodClaim, goodCopy)
	if got := mqttParsePubRelAffineRef(single[:singleLen]).State; got != mqttPubRelAffineRefValid {
		t.Fatalf("the fixture's own single-header pair is not valid: %v", got)
	}
	if got := mqttParsePubRelAffineRef([]byte(b.String())).State; got != mqttPubRelAffineRefMalformed {
		t.Fatalf("a record carrying two claim headers parsed as %v, want malformed", got)
	}
}

// The crash window, end to end, on a real delivery.
//
// Deliver an affine copy, let the client answer PUBREC — which stores the PUBREL with its
// reference — and then FAIL the AffineDone transition, which is precisely the state a
// broker leaves when it stops between the two writes. Destroy the affine consumer out of
// band, so the copy goes with it off the interest-retained $MQTT_out, and resume. Before
// this item the owner's resume reported that assignment as a LOST delivery.
//
// Paired mutations M1 (omit the carrier at the store site) and M3 (order the pass after
// recoverQ2AffClaims).
func TestQ2AffPubRelProofClosesTheCrashWindow(t *testing.T) {
	b := testQ2AffSetup(t, "crashwindow")
	l := &captureErrorLogger{errCh: make(chan string, 64)}
	b.s.SetLogger(l, false, false)

	ca, ra := b.member(t, "cwA", 300)
	b.bind(t, "cwA")

	sess := testMQTTGetClient(t, b.s, "cwA").mqtt.sess
	sess.mu.Lock()
	gen, cfg := sess.q2AffGen, sess.q2AffConsumer
	sess.mu.Unlock()
	if gen == _EMPTY_ || cfg == nil {
		t.Fatal("the affine path was never provisioned")
	}
	idHash := getHash("cwA")
	_, _, durable := mqttQ2AffineNames(idHash, gen)

	// The affine completion cannot be recorded — the crash window, made deterministic.
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimStore = func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
		if rec != nil && rec.AffineDone {
			return 0, errors.New("injected: the broker stopped before recording the affine side")
		}
		return b.asm.jsa.storeQ2AffClaimCAS(subject, expectSeq, rec)
	}
	b.asm.mu.Unlock()

	b.publish(t, 1, "crashwindow")
	_, payload, qos, pi, ok := testMQTTTryReadPublishV5(ra, 5*time.Second)
	if !ok || qos != 2 || string(payload) != "crashwindow" {
		t.Fatalf("shared QoS 2 delivery: ok=%v qos=%d payload=%q", ok, qos, payload)
	}
	claimSubject, _ := b.waitForClaim(t, "materialized", func(c *mqttQ2AffClaim) bool {
		return c.Phase == mqttQ2AffMaterialized
	})
	testMQTTSendPIPacket(mqttPacketPubRec, t, ca, pi)

	// POSITIVE CONTROL: the fixture really is in the crash-window state — the PUBREL is
	// stored and carries its reference, and AffineDone is not on the record.
	testQ2AffEventually(t, 5*time.Second, func() error {
		smsg, err := sess.jsa.loadNextMsgFromSeq(mqttOutStreamName, sess.pubRelSubject, 1)
		if err != nil {
			return err
		}
		if ref := mqttParsePubRelAffineRef(smsg.Header); !ref.valid() || ref.Claim != claimSubject {
			return fmt.Errorf("the stored PUBREL does not name this assignment: %+v", ref)
		}
		got, _, gerr := b.asm.loadQ2AffClaim(claimSubject)
		if gerr != nil {
			return gerr
		}
		if got == nil || got.AffineDone {
			return fmt.Errorf("the affine side was recorded after all: %+v", got)
		}
		return nil
	})

	// The consumer is destroyed out of band, taking the copy with it, and recreated so
	// nothing downstream can infer anything from its absence.
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimStore = nil
	b.asm.mu.Unlock()
	if _, err := b.jsa.deleteConsumer(mqttOutStreamName, durable, false); err != nil {
		t.Fatalf("deleting the affine consumer: %v", err)
	}
	if _, err := b.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName, Config: *cfg,
	}); err != nil {
		t.Fatalf("recreating the affine consumer: %v", err)
	}

	ca.Close()
	cb, _ := b.reconnect(t, "cwA", 300)
	defer cb.Close()

	testQ2AffEventually(t, 5*time.Second, func() error {
		got, _, err := b.asm.loadQ2AffClaim(claimSubject)
		if err != nil {
			return err
		}
		if got == nil {
			return nil // proven and reaped
		}
		if got.Lost {
			return fmt.Errorf("a delivery the client answered with PUBREC was recorded as LOST: %+v", got)
		}
		return fmt.Errorf("the assignment is neither proven nor settled: %+v", got)
	})
	for {
		select {
		case msg := <-l.errCh:
			if strings.Contains(msg, "LOST QoS 2 delivery") {
				t.Fatalf("a delivery the client had answered was reported as lost: %s", msg)
			}
		default:
			return
		}
	}
}

// A nil error from the Lost transition is NOT the verdict. advanceQ2AffClaim reports
// success for a write mergeProven turned into a no-op — which is exactly what a
// concurrent PubRecProven does, since it clears Lost — and for a record that vanished
// between the reading and its own load. Announcing a loss on either reports the very
// thing this item exists to stop: a delivered message called LOST, and here without even
// a record saying so.
//
// Paired mutation M18: log on a nil error instead of on a landed Lost.
func TestQ2AffLossIsReportedOnlyWhenItLands(t *testing.T) {
	b := testQ2AffSetup(t, "lostlands")
	l := &captureErrorLogger{errCh: make(chan string, 64)}
	b.s.SetLogger(l, false, false)

	ca, _ := b.member(t, "llA", 300)
	defer ca.Close()
	b.bind(t, "llA")

	sess := testMQTTGetClient(t, b.s, "llA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("llA")

	claimSubject, _ := testQ2AffSeedMaterializedClaim(t, b, idHash, gen, 9300)

	// A proof lands between recovery's scan and its own transition load, which is the
	// cross-broker race: the first load is the scan's, every later one is the CAS loop's.
	var loads int
	b.asm.mu.Lock()
	b.asm.testQ2AffClaimLoad = func(subject string) (*mqttQ2AffClaim, uint64, error) {
		rec, seq, err := b.asm.jsa.loadQ2AffClaim(subject)
		loads++
		if rec != nil && subject == claimSubject {
			rec.PubRecProven, rec.AffineDone, rec.Lost = true, true, false
		}
		return rec, seq, err
	}
	b.asm.mu.Unlock()
	t.Cleanup(func() {
		b.asm.mu.Lock()
		b.asm.testQ2AffClaimLoad = nil
		b.asm.mu.Unlock()
	})

	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)

	// POSITIVE CONTROL: recovery really did reach its loss arm and attempt the write.
	// The scan walks the stream directly, so every load through this seam is the
	// transition's own: one or more means recovery really did attempt the Lost write.
	if loads < 1 {
		t.Fatal("recovery never attempted the Lost transition; the fixture is not on its route")
	}
	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got != nil && got.Lost {
		t.Fatalf("a proof racing the loss write did not stop the Lost verdict: %+v", got)
	}
	for {
		select {
		case msg := <-l.errCh:
			if strings.Contains(msg, "LOST QoS 2 delivery") {
				t.Fatalf("a loss was reported although none was recorded: %s", msg)
			}
		default:
			return
		}
	}
}

// The duplicate-record branch of the PUBREL delivery callback, driven directly.
//
// Several PUBREL records under one packet identifier is a supported stale-broker shape:
// a superseded broker can store its own PUBREL after a peer recreated the exchange. When
// one is already tracked, a record arriving on a DIFFERENT stream sequence is
// acknowledged away immediately — and it may carry the only reference to a different
// assignment, so its proof is discharged first. Without that, round 1's proof-destruction
// bug returns: the record is destroyed and a later resume infers a loss for a message the
// client answered.
//
// Driven through mqttDeliverPubRelCb rather than through dischargeQ2AffPubRelProof,
// because it is the WIRING that is under test: a unit call on the discharge cannot see a
// branch that never calls it.
//
// Paired mutation M19: acknowledge the duplicate without discharging it.
func TestQ2AffDuplicatePubRelCallbackDischargesBeforeAcking(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		unreadable bool
	}{
		{name: "a resolvable reference is proven"},
		{name: "an unresolvable one is left alone", unreadable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testQ2AffSetup(t, "dupcb")
			ca, _ := b.member(t, "dcA", 300)
			defer ca.Close()
			b.bind(t, "dcA")

			c := testMQTTGetClient(t, b.s, "dcA")
			sess := c.mqtt.sess
			sess.mu.Lock()
			gen := sess.q2AffGen
			sess.mu.Unlock()
			idHash := getHash("dcA")

			claimSubject, copySubject := testQ2AffSeedLostClaim(t, b, idHash, gen, 8600)

			// An entry is already tracked on this identifier, on stream sequence 3.
			const pi = uint16(51)
			sess.mu.Lock()
			sess.pendingPubRel = map[uint16]*mqttPending{pi: {
				jsDur: "$MQTT_PUBREL_fixture", sseq: 3,
				jsAckSubject: testQ2AffAckReply(3),
			}}
			sess.mu.Unlock()

			if tc.unreadable {
				b.asm.mu.Lock()
				b.asm.testQ2AffClaimLoad = func(string) (*mqttQ2AffClaim, uint64, error) {
					return nil, 0, errors.New("injected: the claim record is unreadable")
				}
				b.asm.mu.Unlock()
				t.Cleanup(func() {
					b.asm.mu.Lock()
					b.asm.testQ2AffClaimLoad = nil
					b.asm.mu.Unlock()
				})
			}

			// The acknowledgement itself is observed, not inferred. Without this a
			// regression that calls the discharge, IGNORES its false result and acks
			// anyway passes both subcases — the readable claim is still proven and the
			// unreadable one is still untouched — while the record it destroys is
			// invisible.
			reply := testQ2AffAckReply(9)
			nc := natsConnect(t, b.s.ClientURL())
			defer nc.Close()
			ackSub := natsSubSync(t, nc, reply)
			if err := nc.Flush(); err != nil {
				t.Fatalf("flushing the ack observer: %v", err)
			}

			// The DUPLICATE arrives on sequence 9, naming its own assignment.
			msg, hdrLen := mqttNewCorrelatedPubRel(pi, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_, claimSubject, copySubject)
			pc := testMQTTGetClient(t, b.s, "dcA")
			pc.pa.hdr = hdrLen
			t.Cleanup(func() { pc.pa.hdr = 0 })
			mqttDeliverPubRelCb(&subscription{client: c}, pc, nil, sess.pubRelSubject, reply, msg)

			_, ackErr := ackSub.NextMsg(500 * time.Millisecond)
			acked := ackErr == nil

			// The verification read must not go through the injected fault.
			b.asm.mu.Lock()
			b.asm.testQ2AffClaimLoad = nil
			b.asm.mu.Unlock()

			got, _, err := b.asm.loadQ2AffClaim(claimSubject)
			if err != nil {
				t.Fatalf("loading the claim: %v", err)
			}
			if tc.unreadable {
				if got == nil || !got.Lost || got.PubRecProven {
					t.Fatalf("an unresolvable duplicate changed the record: %+v", got)
				}
				if acked {
					t.Fatal("a duplicate PUBREL whose proof could not be resolved was acknowledged away; " +
						"that record may be the only evidence the client answered PUBREC")
				}
				return
			}
			if !acked {
				t.Fatal("a discharged duplicate PUBREL was never acknowledged, so it redelivers forever")
			}
			// Proven claims are reaped. The unreadable case above is the control that
			// this absence is the discharge's doing and not the fixture's.
			if got != nil {
				t.Fatalf("the duplicate's assignment was acknowledged away without its proof "+
					"being recorded: %+v", got)
			}
		})
	}
}

// testQ2AffAckReply builds a JetStream ack reply subject whose STREAM SEQUENCE is sseq,
// which is the token mqttDeliverPubRelCb's duplicate test reads.
func testQ2AffAckReply(sseq uint64) string {
	return fmt.Sprintf("$JS.ACK.acc.hash.$MQTT_out.CONS.1.%d.1.0.0", sseq)
}

// A refused carrier is withheld and re-offered by AckWait forever, so the diagnostic that
// explains it is emitted ONCE per RECORD per connection. Repeating it on every attempt
// turns one unchanging fact about a corrupt record into an unbounded log stream that
// buries every other diagnostic — and keying it by anything coarser than the record
// silences a genuinely different one, since several records legitimately share a packet
// identifier.
//
// Paired mutations M24 (warn unconditionally) and M26 (key the suppression by the packet
// identifier instead of by the record).
func TestQ2AffRefusedCarrierIsReportedOnce(t *testing.T) {
	b := testQ2AffSetup(t, "warnonce")
	l := &captureWarnLogger{warn: make(chan string, 64)}
	b.s.SetLogger(l, false, false)

	ca, _ := b.member(t, "woA", 300)
	defer ca.Close()
	b.bind(t, "woA")

	c := testMQTTGetClient(t, b.s, "woA")
	sess := c.mqtt.sess

	const pi = uint16(61)
	sess.mu.Lock()
	sess.pendingPubRel = map[uint16]*mqttPending{pi: {
		jsDur: sess.pubRelDurable(), sseq: 5, jsAckSubject: testQ2AffAckReply(5),
	}}
	sess.mu.Unlock()

	ref := mqttPubRelAffineRef{Claim: "not-a-claim", Copy: "not-a-copy",
		State: mqttPubRelAffineRefMalformed}
	// Three attempts, with a REAL AckWait redelivery of the same record between them.
	// The redelivery is the part that matters: trackAsPubRel replaces the pending entry,
	// and a suppression flag that goes with it turns "once per connection" into "once per
	// AckWait cycle" — which is the unbounded stream the rule exists to prevent.
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_, "not-a-claim", "not-a-copy")
	pc := testMQTTGetClient(t, b.s, "woA")
	pc.pa.hdr = hdrLen
	t.Cleanup(func() { pc.pa.hdr = 0 })
	for i := 0; i < 3; i++ {
		if sess.dischargeQ2AffPubRelProof(c, pi, mqttDeferredKey{sess.pubRelDurable(), 5}, ref) {
			t.Fatalf("attempt %d acknowledged a refused carrier", i)
		}
		mqttDeliverPubRelCb(&subscription{client: c}, pc, nil, sess.pubRelSubject,
			testQ2AffAckReply(5), msg)
	}

	// A DIFFERENT record under the SAME packet identifier — the supported stale-broker
	// shape — is its own fact and gets its own report. Suppressing it would be the
	// identifier-keyed bug this bookkeeping was rewritten to remove.
	for i := 0; i < 2; i++ {
		if sess.dischargeQ2AffPubRelProof(c, pi, mqttDeferredKey{sess.pubRelDurable(), 9}, ref) {
			t.Fatalf("attempt %d acknowledged a refused carrier on the second record", i)
		}
	}

	refusals := testQ2AffCountWarnings(l, "is not a shape this broker writes")
	// POSITIVE CONTROL in both directions: exactly one per RECORD. "At most one" would
	// pass against a broker that never explains the refusal at all, and "at least one"
	// against one that repeats it on every attempt.
	if refusals != 2 {
		t.Fatalf("two distinct refused records were reported %d times across five withheld attempts, "+
			"want exactly 2 — one each", refusals)
	}
}

// testQ2AffCountWarnings drains the capture logger and counts the messages containing sub.
func testQ2AffCountWarnings(l *captureWarnLogger, sub string) int {
	var n int
	for {
		select {
		case msg := <-l.warn:
			if strings.Contains(msg, sub) {
				n++
			}
			continue
		default:
		}
		return n
	}
}

// The duplicate-delivery branch reads the PUBREL consumer's durable to key its refusal
// bookkeeping. That pointer is written under sess.mu by teardown and by consumer
// recreation, so the read must happen while the lock is held — reading it after the unlock
// is a data race, and it would key the record's bookkeeping by an empty or replacement
// durable rather than by the record's own.
//
// Run under -race, which is what turns the overlap below into a failure rather than a
// coincidence.
//
// Paired mutation M27: move the durable read after sess.mu.Unlock().
func TestQ2AffDuplicatePubRelReadsItsDurableUnderTheLock(t *testing.T) {
	b := testQ2AffSetup(t, "dupdurrace")
	ca, _ := b.member(t, "ddA", 300)
	defer ca.Close()
	b.bind(t, "ddA")

	c := testMQTTGetClient(t, b.s, "ddA")
	sess := c.mqtt.sess

	const pi = uint16(71)
	sess.mu.Lock()
	sess.pendingPubRel = map[uint16]*mqttPending{pi: {
		jsDur: sess.pubRelDurable(), sseq: 4, jsAckSubject: testQ2AffAckReply(4),
	}}
	prior := sess.pubRelConsumer
	sess.mu.Unlock()

	msg, hdrLen := mqttNewCorrelatedPubRel(pi, "xxxxxxxxxxxxxxxxxxxxxx", _EMPTY_, 0, _EMPTY_, "not-a-claim", "not-a-copy")
	pc := testMQTTGetClient(t, b.s, "ddA")
	pc.pa.hdr = hdrLen
	t.Cleanup(func() { pc.pa.hdr = 0 })

	// A consumer replacement runs concurrently with the duplicate delivery, which is the
	// window the unlocked read would fall into.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			sess.mu.Lock()
			sess.pubRelConsumer = &ConsumerConfig{Durable: fmt.Sprintf("$MQTT_PUBREL_replacement_%d", i)}
			sess.mu.Unlock()
		}
		sess.mu.Lock()
		sess.pubRelConsumer = prior
		sess.mu.Unlock()
	}()
	for i := 0; i < 200; i++ {
		mqttDeliverPubRelCb(&subscription{client: c}, pc, nil, sess.pubRelSubject,
			testQ2AffAckReply(11), msg)
	}
	<-done
}

// The loss report never claims a FINAL fact, and that is what makes the pair of
// diagnostics order-independent.
//
// The record is the authority; the log is a derived view and the two cannot be made
// transactional. A concurrent proof can clear a just-written loss verdict and reap the
// claim between recovery's CAS and its own log line, so a rule that the retraction must
// follow the report is one the code could not keep. Wording the report as PROVISIONAL
// removes the requirement instead of adding machinery to meet it.
//
// Paired mutation M30: drop the provisional qualification from the loss report.
func TestQ2AffLossReportIsProvisional(t *testing.T) {
	b := testQ2AffSetup(t, "provisional")
	l := &captureErrorLogger{errCh: make(chan string, 64)}
	b.s.SetLogger(l, false, false)

	ca, _ := b.member(t, "pvA", 300)
	defer ca.Close()
	b.bind(t, "pvA")

	sess := testMQTTGetClient(t, b.s, "pvA").mqtt.sess
	sess.mu.Lock()
	gen := sess.q2AffGen
	sess.mu.Unlock()
	idHash := getHash("pvA")

	claimSubject, _ := testQ2AffSeedMaterializedClaim(t, b, idHash, gen, 9500)
	b.asm.recoverQ2AffClaims(idHash, gen, mqttQ2AffProofComplete)

	// POSITIVE CONTROL: the loss really was recorded, so this is a statement about how
	// the report reads and not about it never being emitted.
	got, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim: %v", err)
	}
	if got == nil || !got.Lost {
		t.Fatalf("the fixture did not record a loss, so there is no report to inspect: %+v", got)
	}
	var report string
	for {
		select {
		case msg := <-l.errCh:
			if strings.Contains(msg, "LOST QoS 2 delivery") {
				report = msg
			}
			continue
		default:
		}
		break
	}
	if report == _EMPTY_ {
		t.Fatal("no loss report was emitted for a recorded loss")
	}
	if !strings.Contains(report, "PROVISIONAL") {
		t.Fatalf("the loss report claims a final fact it cannot guarantee, so a retraction "+
			"arriving before it would leave the operator with a false verdict: %s", report)
	}
}

// Teardown must not destroy a PUBREC proof before the assignment it proves has been
// corrected.
//
// The sequence is the supported late-store race: recovery records Lost, a superseded
// broker's PUBREL — whose store is not CAS-conditioned — lands afterwards proving the
// client answered PUBREC, and the session is then torn down. Teardown deletes the PUBREL
// consumer, and $MQTT_out is interest-retained, so the record goes with it; the discard
// that follows preserves Lost, and `reapable` refuses a `Lost ∧ Discarded` claim forever.
// The authoritative record would then call a delivered message lost, permanently.
//
// Paired mutations M31 (delete the consumer without reconciling first) and M32 (reconcile
// AFTER the delete instead of before).
func TestQ2AffTeardownSecuresPubRecProofsBeforeDestroyingThem(t *testing.T) {
	t.Parallel()
	b := testQ2AffSetup(t, "teardownproof")
	ca, _ := b.member(t, "tpA", 300)
	defer ca.Close()
	b.bind(t, "tpA")

	sess := testMQTTGetClient(t, b.s, "tpA").mqtt.sess
	sess.mu.Lock()
	gen, affGen := sess.gen, sess.q2AffGen
	pubRelSubject := sess.pubRelSubject
	pubRelDur := sess.pubRelDurable()
	sess.mu.Unlock()
	idHash := getHash("tpA")

	// Recovery already recorded the loss, and the proving PUBREL lands afterwards.
	claimSubject, copySubject := testQ2AffSeedLostClaim(t, b, idHash, affGen, 9700)
	testQ2AffStorePubRel(t, sess, pubRelSubject, 29,
		mqttPubRelAffineRef{Claim: claimSubject, Copy: copySubject})

	// POSITIVE CONTROL: the fixture really is in the state the test is about — a Lost
	// verdict on the record, and a PUBREL that contradicts it.
	before, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil || before == nil || !before.Lost || before.PubRecProven {
		t.Fatalf("the fixture is not a recorded loss awaiting its proof: %+v (%v)", before, err)
	}
	if pubRelDur == _EMPTY_ {
		t.Fatal("the session has no PUBREL consumer, so teardown would delete nothing")
	}

	target := &mqttTeardownTarget{
		id: "tpA", idHash: idHash, gen: gen,
		pubRelDur: pubRelDur, q2AffGen: affGen,
	}
	if err := sess.deleteTeardownTargets(target, false); err != nil {
		t.Fatalf("tearing down: %v", err)
	}

	// The claim is either reaped (proof recorded, then the discard and reap settled it)
	// or still present — but it must NEVER be left asserting a loss, which is the state
	// nothing can correct once the PUBREL is gone with its consumer.
	after, _, err := b.asm.loadQ2AffClaim(claimSubject)
	if err != nil {
		t.Fatalf("loading the claim after teardown: %v", err)
	}
	if after != nil && after.Lost {
		t.Fatalf("teardown destroyed the PUBREC proof and left the assignment permanently "+
			"recorded as LOST for a message the client answered: %+v", after)
	}
	if after != nil && !after.PubRecProven {
		t.Fatalf("teardown neither recorded the proof nor reaped the assignment: %+v", after)
	}
}
