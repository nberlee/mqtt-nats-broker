// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"testing"

	"github.com/nats-io/nuid"
)

// The recovery claim FLOOR, the third disposition of staging, and the rollback
// reclassification — the three sites that share one classification of "is this delivery
// still owed to the client under [MQTT-3.10.4-3]?".
//
// What licenses the floor is F-072's durable discriminator: a possibly-attempted entry may
// have reached the client, so the delivery is owed, while a proven-unsent one demonstrably
// was not. Without it a post-adoption delivery is acked away on the next resume AND its
// identifier is held for the life of the session with no delivery that could discharge it.

// testTrackSource installs a cpending mapping plus its pending entry, which is the shape
// recovery's apply phase leaves for a live, attributable source.
func testTrackSource(sess *mqttSession, dur string, sseq uint64, pi uint16, ack *mqttPending) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	if sess.cpending[dur] == nil {
		sess.cpending[dur] = make(map[uint64]uint16)
	}
	sess.cpending[dur][sseq] = pi
	if ack != nil {
		if sess.pendingPublish == nil {
			sess.pendingPublish = make(map[uint16]*mqttPending)
		}
		ack.jsDur, ack.sseq = dur, sseq
		sess.pendingPublish[pi] = ack
	}
}

// TestFloorEnrolsAPossiblyAttemptedPostAdoptionDelivery is the loss the floor closes.
//
// The persisted Started set records what was owed WHEN IT WAS WRITTEN. A delivery the
// ordinary consumer started after the adoption is in no drain's claim set, so on the next
// resume the restored drain does not claim it and the callback's unclaimed arm acks the
// source away — on an interest-retained stream, so the message is destroyed, while the
// recovered register holds its identifier forever.
func TestFloorEnrolsAPossiblyAttemptedPostAdoptionDelivery(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-enrol")
	const dur = "dur-floor-enrol"

	// A drain restored from a record whose Started set predates the post-adoption send.
	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})
	// POSSIBLY ATTEMPTED: the marker is gone and no intent is unresolved, so the client
	// may hold this identifier.
	testTrackSource(sess, dur, 77, 12, &mqttPending{transmitted: true, regSeq: 900})

	sess.mu.Lock()
	sess.enrolFloorClaimsLocked()
	st := d.seqs[77]
	sess.mu.Unlock()

	if st == nil || !st.claimed {
		t.Fatal("a possibly-attempted delivery on a restored drain's durable must be ENROLLED: " +
			"the client may hold it, and the unclaimed arm would otherwise destroy the source")
	}
	if st.pi != 12 || st.regSeq != 900 {
		t.Fatalf("the claim is built from the ENTRY's own identity, got pi=%d regSeq=%d", st.pi, st.regSeq)
	}
}

// TestRecoveryAppliesTheFloorEndToEnd drives the floor through REAL register recovery, so
// what is under test is the wiring as well as the rule.
//
// The direct-call tests above prove the helper's decisions; none of them would notice the
// call site being removed from recoverInflightRegisters, and a floor that is never invoked
// closes nothing. This one starts from a transmitted exchange on the stream, forgets the
// in-memory state exactly as a restart does, and requires recovery itself to put the claim
// back on a drain whose persisted Started set never had it.
func TestRecoveryAppliesTheFloorEndToEnd(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-floor-e2e")
	dur := e.sess.idHash + "_" + nuid.Next()

	// A real source and a real exchange, TRANSMITTED — so the marker is gone and recovery
	// classifies the entry possibly-attempted, which is what makes the delivery owed.
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.transmit(t, pi, dur, sseq, true)
	e.waitMarkerAbsent(t, pi)

	// The retained adoption's record shape: the durable is a drain again, and its Started
	// set does not mention this sequence, because it was started while the consumer was
	// ordinary.
	d := addTestDrain(e.sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})

	e.forgetInMemory()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}

	e.sess.mu.Lock()
	st := d.seqs[sseq]
	complete := e.sess.applyProvenanceCompleteLocked()
	e.sess.mu.Unlock()

	if !complete {
		t.Fatal("fixture: this walk must have APPLIED, or the floor's per-source rule is " +
			"not the thing being tested")
	}
	if st == nil || !st.claimed {
		t.Fatalf("recovery must apply the claim floor: the drain's persisted set never had "+
			"%d, so without it the unclaimed arm acks the source away and the recovered "+
			"register holds its identifier with no delivery left to discharge it", sseq)
	}
}

// TestAFailedWalkLeavesTheApplyProvenanceBehind is the provenance's own end-to-end test, and
// it is what distinguishes stamping at the APPLY BOUNDARY from stamping on entry.
//
// An enumeration failure returns before the apply phase, so an unknown population may have
// been missed entirely: a reservation that looks fully attributable proves nothing about the
// rest, and the restriction has to be session-wide. Only a walk that reached its apply
// boundary may say otherwise, which is why the stamp cannot live at the top of the walk.
func TestAFailedWalkLeavesTheApplyProvenanceBehind(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-floor-failed-walk")
	dur := e.sess.idHash + "_" + nuid.Next()
	sseq := e.publishSource(t, dur)
	pi := e.track(t, dur, sseq, 1)
	e.transmit(t, pi, dur, sseq, true)
	e.waitMarkerAbsent(t, pi)

	// A walk that APPLIES: provenance current, so attribution is per source.
	e.forgetInMemory()
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("the applying walk: %v", err)
	}
	e.sess.mu.Lock()
	appliedComplete := e.sess.applyProvenanceCompleteLocked()
	unrelatedBefore := e.sess.durableRestrictedLocked("dur-unrelated")
	e.sess.mu.Unlock()
	if !appliedComplete || unrelatedBefore {
		t.Fatalf("fixture: an applied walk leaves the provenance current and an unrelated "+
			"durable unrestricted, got complete=%v restricted=%v", appliedComplete, unrelatedBefore)
	}

	// Now a walk that FAILS before the apply phase.
	e.asm.mu.Lock()
	e.asm.testInflightEnumerate = func(string) (map[string]uint64, error) {
		return nil, errMQTTInvalidSession
	}
	e.asm.mu.Unlock()
	e.forgetInMemory()
	_, _ = e.sess.recoverInflightRegisters(e.c)
	e.asm.mu.Lock()
	e.asm.testInflightEnumerate = nil
	e.asm.mu.Unlock()

	e.sess.mu.Lock()
	failedComplete := e.sess.applyProvenanceCompleteLocked()
	unrelatedAfter := e.sess.durableRestrictedLocked("dur-unrelated")
	e.sess.mu.Unlock()

	if failedComplete {
		t.Fatal("a walk that returned before its apply phase must NOT advance the " +
			"completed-walk number: stamping on entry makes a failed reconstruction look " +
			"applied, which is the authorization defect this exists to prevent")
	}
	if !unrelatedAfter {
		t.Fatal("after a failed reconstruction the restriction is session-wide, including " +
			"for durables no surviving reservation names")
	}
}

// TestFloorDoesNotEnrolAProvenUnsentDelivery is the opposite direction, and it is what stops
// the floor becoming "retain everything": a proven-unsent entry still holds its unsent
// marker, so [MQTT-3.10.4-3] does not cover it and claiming it would make the drain
// redeliver, as a started delivery, a payload the client has never seen.
func TestFloorDoesNotEnrolAProvenUnsentDelivery(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-proven-unsent")
	const dur = "dur-floor-unsent"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})
	// PROVEN UNSENT: the marker is still there, so nothing was transmitted.
	testTrackSource(sess, dur, 78, 13, &mqttPending{markerSeq: 555})

	sess.mu.Lock()
	proven := sess.provenUnsentReleasableLocked(sess.pendingPublish[13])
	sess.enrolFloorClaimsLocked()
	st := d.seqs[78]
	sess.mu.Unlock()

	if !proven {
		t.Fatal("fixture: the entry must be PROVEN unsent, or this asserts nothing")
	}
	if st != nil {
		t.Fatal("a proven-unsent delivery adds no claim: it was never transmitted, so " +
			"enrolling it would redeliver a payload the client has never seen as a started one")
	}
}

// TestFloorDoesNotEnrolAFreshSource is the plain case that must keep working: a sequence with
// no register at all is a FRESH message on a drained filter, and the client unsubscribed, so
// it is discarded rather than claimed.
func TestFloorDoesNotEnrolAFreshSource(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-fresh")
	const dur = "dur-floor-fresh"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})
	// A mapping with no entry behind it: nothing recovery could classify.
	testTrackSource(sess, dur, 79, 14, nil)

	sess.mu.Lock()
	sess.enrolFloorClaimsLocked()
	st := d.seqs[79]
	sess.mu.Unlock()

	if st != nil {
		t.Fatal("an unclassifiable mapping adds no claim: the floor enrols on positive " +
			"evidence, never on the absence of it")
	}
}

// TestFloorSkipsAPubRelSourceReservation is the eligibility split, and the reason the
// classification takes an eligible PUBLISH entry rather than deciding phase for itself.
//
// A recovered PUBREL reserves its SOURCE under the same cpending map while pendingPublish
// holds nothing for that identifier. It is positively identified, its own machinery owns its
// completion, and it restricts nothing — treating it as unknown withheld the callback for an
// exchange recovery had fully classified.
func TestFloorSkipsAPubRelSourceReservation(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-pubrel")
	const dur = "dur-floor-pubrel"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 2, map[uint64]*drainSeq{})
	testTrackSource(sess, dur, 80, 15, nil) // the source reservation, no PUBLISH entry
	sess.mu.Lock()
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	sess.pendingPubRel = map[uint16]*mqttPending{15: {jsDur: dur, sseq: 80}}
	_, eligible, knownPubRel := sess.drainClaimEligibleLocked(15)
	sess.enrolFloorClaimsLocked()
	st := d.seqs[80]
	restricted := sess.durableRestrictedLocked(dur)
	sess.mu.Unlock()

	if eligible {
		t.Fatal("a PUBREL source reservation is not an eligible PUBLISH entry")
	}
	if !knownPubRel {
		t.Fatal("it must be reported as a KNOWN phase, not as an unresolved one: that " +
			"distinction is what keeps it from restricting anything")
	}
	if st != nil {
		t.Fatal("the floor adds no PUBLISH claim for a past-PUBREC obligation: redelivering " +
			"its payload would duplicate an exchange the client has already answered")
	}
	if restricted {
		t.Fatal("a positively identified PUBREL must not restrict its durable, or its own " +
			"callback is withheld for an exchange recovery fully classified")
	}
}

// TestFloorIgnoresACompletedSourceReservation is the counterexample that killed the stronger
// form of the floor's rule.
//
// Source completion and receiver-exchange completion are different things: a
// possibly-attempted register over a FINISHED source is deliberately retained with no
// cpending mapping, because the payload can never be re-offered while the receiver may still
// hold the identifier. Enrolling it would manufacture a claim for a delivery that can never
// happen, which blocks retirement forever.
func TestFloorIgnoresACompletedSourceReservation(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-complete")
	const dur = "dur-floor-complete"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})
	// Installed transmitted with NO source mapping, which is the completed-source shape.
	sess.mu.Lock()
	sess.pendingPublish = map[uint16]*mqttPending{
		16: {jsDur: dur, sseq: 81, transmitted: true},
	}
	sess.enrolFloorClaimsLocked()
	claims := len(d.seqs)
	sess.mu.Unlock()

	if claims != 0 {
		t.Fatal("a reservation with no source mapping is not the floor's business: walking " +
			"cpending is what keeps completed sources out without a second exclusion rule")
	}
}

// TestABareIdentifierReservationRestrictsTheSession is a holistic-review finding, and the
// arm the flag-only predicate missed.
//
// Recovery installs the UNREADABLE and illegal-shape registers as bare identifier
// reservations: regSeq only, no durable, no sequence, and neither markerOpaque nor
// regForeign set — they are not another owner's, and their marker was never reached. Testing
// only those two flags read them as RESOLVED, so once the apply counter was stamped the
// restriction did not fire, and an unclaimed drain delivery could be acked away or its
// consumer retired while the client may still hold the exchange.
//
// An entry with no source identity is unresolved by construction, and cannot be attributed
// to any durable, so it restricts the whole session.
func TestABareIdentifierReservationRestrictsTheSession(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-bare-reservation")
	const dur = "dur-bare-reservation"

	sess.mu.Lock()
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	// Exactly the shape recovery's opaque arm installs for an unreadable register.
	bare := &mqttPending{regSeq: 4242}
	sess.pendingPublish = map[uint16]*mqttPending{21: bare}
	unresolved := unresolvedReservationLocked(bare)
	durRestricted := sess.durableRestrictedLocked(dur)
	srcRestricted := sess.sourceRestrictedLocked(dur, 91)
	sess.mu.Unlock()

	if !unresolved {
		t.Fatal("a bare identifier reservation is UNRESOLVED: it carries no source identity, " +
			"so nothing can say which delivery it belongs to")
	}
	if !durRestricted || !srcRestricted {
		t.Fatalf("an unattributable reservation restricts the whole session, because it "+
			"could belong to any durable: durable=%v source=%v", durRestricted, srcRestricted)
	}
}

// TestACancelledAllocationRestrictsNothing is the other side of the bare-reservation rule,
// and it is a regression this change caused and then fixed.
//
// A cancelled allocation carries no source identity either — the untrack clears its durable
// and sequence — but it is POSITIVELY classified: it was given up, nothing is owed on it,
// and its identifier is released. Reading "no identity" as "unattributable" therefore
// restricted the whole session for an entry recovery had already decided about, and an
// ordinary QoS 0 downgrade stopped tearing its consumer down, because staging read the
// empty mapping set as "completeness unknown" rather than "nothing owed".
//
// So the classified state has to be answered before the identity test, not after it.
func TestACancelledAllocationRestrictsNothing(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-cancelled")
	const dur = "dur-cancelled"

	sess.mu.Lock()
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	// Exactly what a cancelled, untracked allocation leaves behind.
	cancelled := &mqttPending{regCancelled: true}
	sess.pendingPublish = map[uint16]*mqttPending{1: cancelled}
	unresolved := unresolvedReservationLocked(cancelled)
	restricted := sess.durableRestrictedLocked(dur)
	sess.mu.Unlock()

	if unresolved {
		t.Fatal("a cancelled allocation is CLASSIFIED, not unresolved: it was given up, so " +
			"nothing is owed and its identifier is released")
	}
	if restricted {
		t.Fatal("it must restrict nothing, or an ordinary downgrade stops tearing its " +
			"consumer down because an empty mapping set reads as completeness unknown")
	}
}

// TestPositivelyClassifiedEntriesWithoutASourceRestrictNothing closes the CLASS rather than
// its instances, which is why it is table-driven.
//
// Three holistic findings landed on this one predicate. The first: testing markerOpaque and
// regForeign alone missed the bare identifier reservation recovery installs for an
// unreadable register, so nothing restricted and an owed delivery could be acked away. The
// fix — "no source identity means unresolved" — then swept in two shapes that also lack
// identity but are positively CLASSIFIED, and each broke a destructive path that was
// supposed to run: a cancelled allocation, and a retained replay, which never had a source
// because it is not a stream delivery at all.
//
// So the rule is stated positively — a reservation that names a REGISTER but no SOURCE —
// and this test pins both directions of it, so a fourth shape cannot quietly join either
// side.
func TestPositivelyClassifiedEntriesWithoutASourceRestrictNothing(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "floor-no-source-class")
	const dur = "dur-no-source-class"

	for _, tc := range []struct {
		name       string
		ack        *mqttPending
		unresolved bool
		why        string
	}{
		{
			name:       "bare identifier reservation from an unreadable register",
			ack:        &mqttPending{regSeq: 4242},
			unresolved: true,
			why:        "a register was found and its delivery cannot be identified, so the obligation is unaccounted for",
		},
		{
			name:       "cancelled allocation",
			ack:        &mqttPending{regCancelled: true},
			unresolved: false,
			why:        "given up: nothing is owed and the identifier is released",
		},
		{
			name:       "retained replay",
			ack:        &mqttPending{qos: 1, regNonce: "nonce-retained-replay"},
			unresolved: false,
			why:        "a retained send has no register and no stream source by construction (R6)",
		},
		{
			name:       "live stream delivery",
			ack:        &mqttPending{jsDur: dur, sseq: 9, transmitted: true},
			unresolved: false,
			why:        "fully identified",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess.mu.Lock()
			sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
			sess.pendingPublish = map[uint16]*mqttPending{1: tc.ack}
			got := unresolvedReservationLocked(tc.ack)
			restricted := sess.durableRestrictedLocked("dur-unrelated")
			sess.mu.Unlock()

			if got != tc.unresolved {
				t.Fatalf("unresolved=%v, want %v: %s", got, tc.unresolved, tc.why)
			}
			// An unresolved entry with no attributable source restricts the session; a
			// classified one must restrict nothing, or a destructive path that should run
			// silently stops running.
			if restricted != tc.unresolved {
				t.Fatalf("restricted=%v, want %v: %s", restricted, tc.unresolved, tc.why)
			}
		})
	}
}

// TestStagingWithholdsTheTeardownUnderUnknownCompleteness is staging's third disposition.
//
// stageDrain's callers read false as "proceed without a drain", which destroys the consumer.
// That is only sound when the emptiness is a FACT: an opaque recovered reservation loses its
// cpending mapping while keeping its source identity, so an empty mapping set can equally
// mean "an obligation exists and recovery could not place it".
func TestStagingWithholdsTheTeardownUnderUnknownCompleteness(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "stage-incomplete")
	const sid, dur = "foo.bar", "dur-stage-incomplete"
	cc := &ConsumerConfig{
		Durable:        dur,
		DeliverSubject: mqttSubPrefix + dur,
		FilterSubject:  mqttStreamSubjectPrefix + "foo.bar",
		AckPolicy:      AckExplicit,
	}

	sess.mu.Lock()
	sess.cons = map[string]*ConsumerConfig{sid: cc}
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	// Nothing mapped for this durable, BUT an unresolved reservation still names it.
	sess.pendingPublish = map[uint16]*mqttPending{
		20: {jsDur: dur, sseq: 91, markerOpaque: true},
	}
	staged := sess.stageDrain(sid, cc, nil)
	d := sess.drains[sid]
	byDur := sess.drainByDur[dur]
	consGone := sess.cons[sid] == nil
	sess.mu.Unlock()

	if !staged {
		t.Fatal("with completeness unknown staging must NOT report 'nothing owed': the " +
			"caller destroys the consumer on that answer, and the obligation is real")
	}
	if d == nil || byDur == nil || !consGone {
		t.Fatalf("the incomplete disposition installs a drain in BOTH maps and gives up the "+
			"cons entry: drains=%v byDur=%v consGone=%v", d, byDur, consGone)
	}
	if len(d.seqs) != 0 {
		t.Fatalf("it claims nothing — the claims are unknown, not known-empty — got %d", len(d.seqs))
	}
	if err := testSessSave(t, sess); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, inDrains := testRecordNamesDurable(t, sess, dur); !inDrains {
		t.Fatal("the empty drain must be NAMED, or the durable is unreferenced and a sibling " +
			"sweep deletes the consumer this disposition exists to preserve")
	}
}

// TestStagingTearsDownWhenNothingIsOwedAndTheViewIsComplete is the arm that stops the test
// above passing for a disposition that is simply unconditional.
func TestStagingTearsDownWhenNothingIsOwedAndTheViewIsComplete(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "stage-ordinary")
	const sid, dur = "foo.bar", "dur-stage-ordinary"
	cc := &ConsumerConfig{Durable: dur, FilterSubject: mqttStreamSubjectPrefix + "foo.bar"}

	sess.mu.Lock()
	sess.cons = map[string]*ConsumerConfig{sid: cc}
	sess.inflightApplyCompletedWalk, sess.inflightWalks = 1, 1
	staged := sess.stageDrain(sid, cc, nil)
	drains := len(sess.drains)
	sess.mu.Unlock()

	if staged || drains != 0 {
		t.Fatalf("nothing mapped and nothing restricting the durable is genuinely 'nothing "+
			"owed', so the ordinary teardown stands: staged=%v drains=%d", staged, drains)
	}
}

// TestRollbackCancelsRatherThanSkipsAParkedAllocation is the rollback half, and the schedule
// it closes needs no crash.
//
// A SUBSCRIBE rollback restores the PRE-ADOPTION drain object, which claims nothing started
// since. Merely skipping an allocation whose register store is still in flight is not enough:
// confirmInflightRegister checks the pending ENTRY's identity, not drain membership, so that
// callback would confirm and transmit a sequence the reactivated drain does not claim.
func TestRollbackCancelsRatherThanSkipsAParkedAllocation(t *testing.T) {
	t.Parallel()
	_, _, sess, _ := drainTestSession(t, "rollback-reclassify")
	const dur = "dur-rollback"

	d := addTestDrain(sess, "foo.bar", dur, "foo.bar", 1, map[uint64]*drainSeq{})
	// Three post-adoption deliveries, one per disposition.
	testTrackSource(sess, dur, 101, 31, &mqttPending{regPending: true, jsAckSubject: "ack.101"}) // cancel
	testTrackSource(sess, dur, 102, 32, &mqttPending{markerSeq: 77, jsAckSubject: "ack.102"})    // withdraw
	testTrackSource(sess, dur, 103, 33, &mqttPending{transmitted: true, regSeq: 901})            // enrol

	sess.mu.Lock()
	withdrawn := sess.reclassifyReactivatedDrainsLocked([]string{dur})
	cancelledEntry := sess.pendingPublish[31]
	withdrawnEntry := sess.pendingPublish[32]
	enrolled := d.seqs[103]
	cancelledClaim := d.seqs[101]
	withdrawnClaim := d.seqs[102]
	deferredCancel := sess.deferred[mqttDeferredKey{dur, 101}]
	sess.mu.Unlock()

	if cancelledEntry != nil {
		t.Fatal("a parked allocation must be UNTRACKED, not merely verdicted: the entry is " +
			"what confirmInflightRegister checks, so leaving it lets the callback transmit " +
			"a sequence this drain does not claim")
	}
	if withdrawnEntry != nil {
		t.Fatal("a proven-unsent entry is withdrawn and untracked")
	}
	if len(withdrawn) != 1 || withdrawn[0].sseq != 102 {
		t.Fatalf("the withdrawn pairs go to the CALLER, because commitDrains collects "+
			"d.withdrawn from staged drains only and a snapshot-restored drain is not "+
			"staged: got %+v", withdrawn)
	}
	if enrolled == nil || !enrolled.claimed {
		t.Fatal("a possibly-attempted post-adoption delivery must be claimed by the " +
			"reactivated drain, or it acks away a source it owes")
	}
	if cancelledClaim != nil || withdrawnClaim != nil {
		t.Fatal("neither a cancelled nor a withdrawn delivery becomes a claim")
	}
	if deferredCancel != "ack.101" {
		t.Fatalf("the withheld marker is reinstalled so a later first transmission still "+
			"carries DUP=0, got %q", deferredCancel)
	}
}
