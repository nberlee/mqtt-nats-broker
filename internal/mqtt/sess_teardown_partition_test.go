//go:build mqttport_fulltests

// Copyright 2026 The mqtt-nats-broker Authors
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
	"strings"
	"testing"
)

// Who owns a retiring generation's cleanup, decided BEFORE the first deletion.
//
// The release gate asks its own question after `deleteTeardownTargets` has run, which is right for
// a RELEASE decision and useless for a DELETION one: by then the affine consumer would be gone
// whatever the answer. These are about the decision that runs first.

// TestF105AVacuousGenerationOwnsItsOwnCleanup: with neither closure nor ledger, nothing was
// admitted through the authority, so there is no duty a release could strand and the teardown
// deletes exactly what it deletes today.
func TestF105AVacuousGenerationOwnsItsOwnCleanup(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-part-vacuous")
	defer cleanup()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	target := &mqttTeardownTarget{
		id: e.id, idHash: getHash(e.id), gen: testF105Gen,
		q2AffDur: durable, q2AffGen: affGen, pubRelDur: pubRelDur,
	}
	own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, testF105Gen, target)
	if err != nil {
		t.Fatalf("classifying a generation that admitted nothing: %v", err)
	}
	if own.Class != mqttTeardownOwnerVacuous {
		t.Fatalf("a generation with neither closure nor ledger classified as %v", own.Class)
	}
	if own.Affine != nil {
		t.Fatalf("a vacuous generation reported an enrolled affine target: %+v", own.Affine)
	}
}

// TestF105APreservingClosureKeepsItsAffineConsumer: an inventory exists, its closure does not
// discard, so the affine consumer was never transferred and stays the teardown's to delete.
func TestF105APreservingClosureKeepsItsAffineConsumer(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-part-preserving")
	defer cleanup()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)
	auth, _ := e.bootstrap(t, "txn-boot")

	if err := e.asm.jsa.closeGenerationEnrolled(e.id, e.epoch, auth.Gen, auth.Tenure,
		auth.OwnerTxn, false, mqttClosingTargets{}); err != nil {
		t.Fatalf("closing without a discard: %v", err)
	}
	target := &mqttTeardownTarget{
		id: e.id, idHash: getHash(e.id), gen: testF105Gen,
		q2AffDur: durable, q2AffGen: affGen, pubRelDur: pubRelDur,
	}
	own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, testF105Gen, target)
	if err != nil {
		t.Fatalf("classifying a preserving closure: %v", err)
	}
	if own.Class != mqttTeardownOwnerPreserving {
		t.Fatalf("a non-discarding closure classified as %v", own.Class)
	}
	if own.Affine != nil {
		t.Fatalf("a preserving closure reported an enrolled affine target: %+v", own.Affine)
	}
	// AND THE TEARDOWN DELETES IT, because ownership is per target and this one was not
	// transferred. The flag the caller derives from this class is what says so.
	if own.Class == mqttTeardownOwnerDiscarding {
		t.Fatal("the preserving class would defer a deletion nobody will perform")
	}
}

// TestF105ADiscardingClosureTransfersItsAffineConsumer: the enrolled target is recovery's, and the
// classification names the resource the duty is about.
func TestF105ADiscardingClosureTransfersItsAffineConsumer(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-part-discarding")
	defer cleanup()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	gen, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	target := &mqttTeardownTarget{
		id: e.id, idHash: getHash(e.id), gen: gen,
		q2AffDur: durable, q2AffGen: affGen, pubRelDur: pubRelDur,
	}
	own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, target)
	if err != nil {
		t.Fatalf("classifying a discarding closure: %v", err)
	}
	if own.Class != mqttTeardownOwnerDiscarding {
		t.Fatalf("a discarding closure with an enrolled target classified as %v", own.Class)
	}
	if own.Affine == nil || own.Affine.Inc != r.Inc {
		t.Fatalf("the classification named %+v, and the enrolment is %q", own.Affine, r.Inc)
	}
}

// TestF105ADiscardingClosureRequiresItsEnrolment is the "required enrolment missing" case, and it
// refuses AT CLASSIFICATION — before any downstream work, because a later refusal at the release
// gate would leave the deletions already done.
func TestF105ADiscardingClosureRequiresItsEnrolment(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-part-missing")
	defer cleanup()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)

	gen, _ := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	// The teardown holds a DIFFERENT affine consumer from the one the closure enrolled, which is
	// the shape of an incomplete enrolment: a durable this teardown still holds that no resource
	// record names.
	target := &mqttTeardownTarget{
		id: e.id, idHash: getHash(e.id), gen: gen,
		q2AffDur: "$MQTT_Q2AFF_unenrolled", q2AffGen: affGen, pubRelDur: pubRelDur,
	}
	_, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, target)
	if err == nil {
		t.Fatal("a discarding teardown holding an unenrolled affine consumer was classified")
	}
	if !strings.Contains(err.Error(), "no resource record names") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// TestF105IndeterminateEvidenceRefusesTheTeardown: an inventory with no closure says nothing about
// whether its obligations were abandoned or preserved, and "I could not say" is never the vacuous
// answer — which is the one that licenses deleting everything.
func TestF105IndeterminateEvidenceRefusesTheTeardown(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-part-indeterminate")
	defer cleanup()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), testF105Gen)
	e.bootstrap(t, "txn-boot")

	// An inventory, and no closure: the generation allocated an ordinal and nothing has ended it.
	if err := testF105SubmitInventory(t, e, "txn-alloc",
		testF105LedgerMember(t, e, 0, &mqttGenLedger{
			Lo: 1, Del: 1,
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, State: mqttGenLedgerOpen, Hi: 1,
		}),
		testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyCleanup, "inc-a",
			mqttDutyNotYetActionable)),
	); err != nil {
		t.Fatalf("planting an open inventory: %v", err)
	}

	target := &mqttTeardownTarget{
		id: e.id, idHash: getHash(e.id), gen: testF105Gen,
		q2AffDur: durable, q2AffGen: affGen, pubRelDur: pubRelDur,
	}
	own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, testF105Gen, target)
	if err == nil {
		t.Fatalf("an inventory with no closure classified as %v", own.Class)
	}
	if !strings.Contains(err.Error(), "no closure record") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
	// AND THE ZERO VALUE IS A REFUSAL, so a caller that forgets to classify stalls rather than
	// deleting.
	var unset mqttTeardownOwnership
	if unset.Class != mqttTeardownOwnerUnknown {
		t.Fatalf("the zero classification is %v rather than unknown", unset.Class)
	}
}

// TestF105AnEnrolledTeardownMakesNoAffineDeleteAttempt is the oracle that distinguishes a deferred
// deletion from one that happened anyway.
//
// A consumer that is simply GONE afterwards proves nothing: a synchronous delete that ran would
// leave the same state. So this creates the affine consumer for real and asserts it is STILL THERE
// when the teardown's deletions have run — the transfer is observable only as an absence of the
// act.
func TestF105AnEnrolledTeardownMakesNoAffineDeleteAttempt(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-part-noattempt")
	defer cleanup()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	_ = affGen

	gen, _ := testF105DiscardingCloseWithAffine(t, e, affGen, durable)

	// A REAL consumer on $MQTT_out, at the name this generation's affine consumer DERIVES, so
	// "still there" is a fact about the store and about the right consumer.
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			Durable: durable, FilterSubject: mqttOutSubjectPrefix + "partnoattempt",
			AckPolicy: AckExplicit, DeliverSubject: mqttSubPrefix + durable, MaxAckPending: 1024,
		},
	}); err != nil {
		t.Fatalf("creating the affine consumer on %s: %v", mqttOutStreamName, err)
	}

	sess := mqttSessionCreate(&e.asm.jsa, e.id, getHash(e.id), gen, 0, _EMPTY_, _EMPTY_,
		e.asm.server().getOpts())
	sess.asm = e.asm
	target := &mqttTeardownTarget{
		id: e.id, idHash: getHash(e.id), gen: gen,
		q2AffDur: durable, q2AffGen: "aff-1",
		deferAffineDelete: true,
	}
	if err := sess.deleteTeardownTargets(target, false); err != nil {
		t.Fatalf("running the teardown's deletions: %v", err)
	}
	if _, err := e.asm.jsa.lookupConsumer(mqttOutStreamName, durable); err != nil {
		t.Fatalf("a deferred affine consumer was deleted by the teardown anyway: %v", err)
	}

	// AND WITHOUT THE DEFERRAL IT GOES, which is what says the assertion above can see the
	// difference at all.
	target.deferAffineDelete = false
	if err := sess.deleteTeardownTargets(target, false); err != nil {
		t.Fatalf("running the teardown's deletions without the deferral: %v", err)
	}
	if _, err := e.asm.jsa.lookupConsumer(mqttOutStreamName, durable); err == nil {
		t.Fatal("the teardown left the affine consumer standing with no deferral in force")
	}
}
