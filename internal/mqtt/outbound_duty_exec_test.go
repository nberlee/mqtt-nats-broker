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
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// What a recovery pass does about an outstanding duty, and what it refuses to do.

// testF105DiscardingCloseWithAffine closes the bootstrap generation through the production route
// — prepare, metadata commit, finalize — carrying a discarding retirement that names an affine
// consumer, and returns the closed generation and its enrolled affine resource.
func testF105DiscardingCloseWithAffine(t *testing.T, e *testF105Env,
	affGen, durable string) (string, *mqttOutboundResource) {

	return testF105DiscardingClose(t, e, affGen, durable, true)
}

// testF105DiscardingClose is the same with the SHARED enrolment optional.
//
// It has to be optional: a shared cleanup duty is deliberately not transferred by this change, so
// a generation carrying one can never retire — and a witness that wants to see a CERTIFICATE must
// therefore close a generation whose only enrolled duty is the affine one. A fixture that always
// enrolled a shared durable would make certification impossible by construction and could not tell
// a broken retirement from an unfinished one.
func testF105DiscardingClose(t *testing.T, e *testF105Env,
	affGen, durable string, withSecond bool) (string, *mqttOutboundResource) {

	t.Helper()
	jsa := &e.asm.jsa
	first, _ := e.bootstrap(t, "txn-1")
	// A SECOND enrolled resource where the fixture asks for one: a source consumer provisioned
	// under the first generation's serving authority, which gives the binding test a second
	// cleanup duty. It is omitted where the fixture needs the generation to be able to RETIRE.
	// (It used to be a shared group durable; those are no resource kind any more — a group
	// consumer is held by arbitration on its own record, shared_lifecycle.go.)
	if withSecond {
		if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov-b", "op-prov-b", first.Gen,
			first.Tenure, "inc-second", "src-second", mqttResourceSourceConsumer); err != nil {
			t.Fatalf("provisioning the second resource: %v", err)
		}
	}
	ps, seq := testF105StandingSession(t, e)
	// DERIVED, not invented: the resource's PUBREL durable must be the one its own generation
	// owns, because that is where the proofs it owes are read from.
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), first.Gen)

	intent := &mqttHandoffIntent{
		ID: "intent-clean", PrevGen: first.Gen, PrevTenure: first.Tenure,
		PrevOwnerTxn: first.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: testF105Gen2, Tenure: "tenure-clean", CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: first.Gen, Tenure: first.Tenure, OwnerTxn: first.OwnerTxn,
			SessWID: ps.WID, SessSeq: seq, Reason: mqttRetireCleanStart, Discard: true,
			AffineDurable: durable, AffineGen: affGen, PubRelDurable: pubRelDur,
		},
	}
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-clean", intent); err != nil {
		t.Fatalf("preparing the clean start: %v", err)
	}
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: intent.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the clean start's metadata half: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin-clean", intent.ID); err != nil {
		t.Fatalf("finalizing the discarding handoff: %v", err)
	}
	res, unreadable, rerr := e.asm.enumerateGenerationResources(e.id, e.epoch, first.Gen)
	if rerr != nil || unreadable != 0 {
		t.Fatalf("enumerating the enrolment: %v (%d unreadable)", rerr, unreadable)
	}
	for _, r := range res {
		if r.Kind == mqttResourceAffineConsumer {
			return first.Gen, r
		}
	}
	t.Fatal("the discarding finalization enrolled no affine consumer")
	return _EMPTY_, nil
}

// testF105SweepUntil runs passes until cond holds or the shard walk has been round twice.
func testF105SweepUntil(t *testing.T, e *testF105Env, cond func() bool) bool {
	t.Helper()
	for i := 0; i < 2*mqttDirShards; i++ {
		if cond() {
			return true
		}
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}
	return cond()
}

// TestF105ThePassCompletesAnEnrolledAffineCleanup is the executable core of the transfer: with
// the prerequisites asserted, a pass authorizes the deletion, performs it, classifies the result
// and ends the duty — from the durable records alone, with no teardown and no session object.
func TestF105ThePassCompletesAnEnrolledAffineCleanup(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-exec-cleanup")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)

	// The prerequisites, asserted by the teardown's producer. The consumer itself was never
	// created in this fixture, so the physical act will report NOT FOUND — which is a POSITIVE
	// answer, classified `gone`, and not an assumption.
	if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
		ID: e.id, Epoch: e.epoch, Gen: gen,
		AffGen: r.AffGen, Durable: r.Name, PubRelDur: r.PubRelDur,
	}); err != nil {
		t.Fatalf("publishing the readiness: %v", err)
	}

	done := testF105SweepUntil(t, e, func() bool {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		return cur != nil && cur.State == mqttResourceCleanupComplete
	})
	if !done {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		t.Fatalf("the enrolled affine cleanup never completed: %+v", cur)
	}

	cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
	if cur.Prov != mqttProvisioningSealed {
		t.Fatalf("the cleanup completed without the provisioning seal: %+v", cur)
	}
	if cur.Outcome != mqttCleanupGone {
		t.Fatalf("the cleanup recorded outcome %q for a consumer that was never created", cur.Outcome)
	}

	// AND THE DUTY ENDED WITH IT. Completing a resource does not complete its inventory entry,
	// so this is the assertion that says the entry was continued rather than left behind for
	// every later sweep to re-examine.
	has, herr := e.asm.jsa.cleanupDutyStandsFor(e.id, e.epoch, gen, r.Inc)
	if herr != nil {
		t.Fatalf("reading the duty back: %v", herr)
	}
	if !has {
		t.Fatal("the cleanup duty vanished rather than being continued")
	}
	led, _, _ := e.asm.jsa.loadGenLedger(e.id, e.epoch, gen)
	if led == nil {
		t.Fatal("the generation's ledger is gone")
	}
	for ord := uint64(1); ord <= led.SealedHi; ord++ {
		d, _, derr := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, ord)
		if derr != nil || d == nil {
			t.Fatalf("duty %d could not be read: %v", ord, derr)
		}
		if d.Ref == r.Inc && d.State != mqttDutyTerminal {
			t.Fatalf("the affine cleanup's duty stands at state %d after its resource completed",
				d.State)
		}
	}
}

// TestF105ThePassIsTheReadinessProducerOfLastResort: with no teardown running at all, the pass
// does the prerequisite work itself and only then seals.
//
// This is the window a discarding finalization leaves — it replaces the predecessor's metadata, so
// no marked record anchors a teardown, and "the successor's acceptance will do it" names a live
// caller rather than a restartable one. The fixture runs no teardown whatsoever.
func TestF105ThePassIsTheReadinessProducerOfLastResort(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-exec-lastresort")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	if ready, rerr := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); rerr != nil || ready != nil {
		t.Fatalf("readiness stood before any producer ran: %+v (%v)", ready, rerr)
	}

	if !testF105SweepUntil(t, e, func() bool {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		return cur != nil && cur.State == mqttResourceCleanupComplete
	}) {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		t.Fatalf("the pass did not finish the cleanup on its own: %+v", cur)
	}

	// AND IT ASSERTED THE PREREQUISITES, naming what it covered rather than leaving the seal
	// resting on nothing.
	ready, rerr := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen)
	if rerr != nil || ready == nil {
		t.Fatalf("the pass sealed without asserting readiness: %v", rerr)
	}
	if ready.AffGen != r.AffGen || ready.Durable != r.Name || ready.PubRelDur != r.PubRelDur {
		t.Fatalf("the assertion does not name this consumer: %+v", ready)
	}
}

// TestF105ANonDiscardingClosureEnrolsNoAffineTarget is the structural half of the same rule: the
// pass has no affine input at all for a generation whose obligations were not abandoned, so there
// is nothing for a discard guard to protect.
func TestF105ANonDiscardingClosureEnrolsNoAffineTarget(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-exec-preserving")
	defer cleanup()
	auth, _ := e.bootstrap(t, "txn-boot")

	// THE COMPOSER IGNORES TARGETS WITHOUT A DISCARD. Production never passes a non-discarding
	// closure any target, so the rule is pinned on the composer itself: handed the session's
	// affine and PUBREL durables with discard false, it enrols no resource at all.
	led, ledSeq, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, auth.Gen)
	if lerr != nil {
		t.Fatalf("reading the ledger: %v", lerr)
	}
	hdr, hdrSeq, herr := e.asm.jsa.loadDirShardAt(mqttDirShardFor(getHash(e.id)))
	if herr != nil {
		t.Fatalf("reading the shard header: %v", herr)
	}
	enr, cerr := mqttComposeClosingEnrolment(e.id, e.epoch, auth.Gen, false, mqttClosingTargets{
		AffineDurable: "$MQTT_Q2AFF_a", AffineGen: "aff-1", PubRelDurable: "$MQTT_PUBREL_x",
	}, led, ledSeq, hdr, hdrSeq)
	if cerr != nil {
		t.Fatalf("composing a non-discarding enrolment: %v", cerr)
	}
	if len(enr.Incs) != 0 {
		t.Fatalf("a non-discarding enrolment minted resource incarnations: %v", enr.Incs)
	}
	for _, m := range enr.Members {
		if strings.HasPrefix(m.subj, mqttResourceSubject(getHash(e.id), auth.Gen, "")) {
			t.Fatalf("a non-discarding enrolment carries resource member %q", m.subj)
		}
	}

	// A close-admission cannot carry a discard at all, which is why this is the ordinary shape.
	if _, err := e.asm.jsa.opCloseGeneration(e.id, e.epoch, mqttNewInflightNonce(), auth.Gen, auth.Tenure,
		auth.OwnerTxn); err != nil {
		t.Fatalf("closing without a discard: %v", err)
	}
	res, unreadable, rerr := e.asm.enumerateGenerationResources(e.id, e.epoch, testF105Gen)
	if rerr != nil || unreadable != 0 {
		t.Fatalf("enumerating the enrolment: %v (%d unreadable)", rerr, unreadable)
	}
	for _, r := range res {
		if r.Kind == mqttResourceAffineConsumer {
			t.Fatalf("a non-discarding closure enrolled an affine consumer: %+v", r)
		}
	}

	// AND NO PASS DOES ANYTHING ABOUT IT: with no enrolment there is no duty, so the readiness
	// work is unreachable rather than merely guarded.
	for i := 0; i < 2*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}
	if ready, lerr := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, testF105Gen); lerr != nil || ready != nil {
		t.Fatalf("a readiness assertion was written for a preserving closure: %+v (%v)", ready, lerr)
	}
}

// TestF105AnAbsentAuthorityWithholdsEveryDuty exercises the guard on an ALREADY AUTHORIZED
// cleanup, so removing it would reach the physical delete rather than being masked by
// opAuthorizeDeletion's own refusal.
func TestF105AnAbsentAuthorityWithholdsEveryDuty(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-exec-noauth")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
		ID: e.id, Epoch: e.epoch, Gen: gen,
		AffGen: r.AffGen, Durable: r.Name, PubRelDur: r.PubRelDur,
	}); err != nil {
		t.Fatalf("publishing the readiness: %v", err)
	}
	// Drive it to DELETE_AUTHORIZED through the ordinary route.
	if !testF105SweepUntil(t, e, func() bool {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		return cur != nil && cur.State != mqttResourceLive
	}) {
		t.Fatal("the cleanup was never authorized")
	}

	// NOW AGAINST A CLIENT THAT HAS NO AUTHORITY AT ALL. Every operation this pass performs CASes
	// one, so with nothing there the pass may do nothing — and an ABSENCE is never permission.
	// A client that never claimed anything is the shape that reaches that branch: a foreign epoch
	// would be refused earlier as unattributable, which is a different rule.
	root := &mqttRecoveryRoot{ID: "client-f105-never-claimed", Epoch: e.epoch, Gen: gen}
	d := mqttSweptDuty{Ord: 1}
	if err := e.asm.jsa.opResolveDuty(root, d, mqttNewInflightNonce()); err == nil {
		t.Fatal("a duty was resolved under an authority this broker cannot attribute")
	}
	cur, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
	if err := e.asm.jsa.opSealAffineProvisioning(root, cur, rseq, mqttNewInflightNonce()); err == nil {
		t.Fatal("a resource was sealed for a client that carries no authority")
	}
	if err := e.asm.jsa.completeCleanupAndResolve(root, cur, rseq, mqttSweptDuty{Ord: 1},
		mqttNewInflightNonce(), mqttCleanupGone); err == nil {
		t.Fatal("a cleanup was completed for a client that carries no authority")
	}

	// AND THE PHYSICAL ACT IS WITHHELD TOO, which the three write helpers above cannot show.
	//
	// Each of them CASes an authority and is refused by the boundary; the DELETE does not, so
	// "an absent authority withholds every duty" was true of the records and false of the one
	// duty that destroys something. An already-authorized resource dispatched under a client
	// with no authority must make NO delete attempt at all — the executor reads the authority
	// before the irreversible half, not only before the record of it.
	// The fixture is the REAL generation, whose resource is already DELETE_AUTHORIZED, with its
	// client's AUTHORITY RECORD REMOVED — rather than a hand-built duty, which would prove only
	// that a struct literal reaches a branch.
	authSubj := mqttOutboundAuthoritySubject(getHash(e.id))
	asmsg, aerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, authSubj)
	if aerr != nil || asmsg == nil {
		t.Fatalf("reading the authority to remove it: %v", aerr)
	}
	if derr := e.asm.jsa.deleteMsg(mqttInflightStreamName, asmsg.Sequence, true); derr != nil {
		t.Fatalf("removing the authority: %v", derr)
	}

	// THE REAL DUTY, DISPATCHED DIRECTLY. Going through a whole pass does not reach this branch
	// — the root's sweep has already passed the ordinal — and a fixture that never reaches the
	// route proves nothing about it.
	var ord uint64
	var entry *mqttDutyEntry
	for o := uint64(1); o <= 8 && entry == nil; o++ {
		if ent, _, lerr := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, o); lerr == nil &&
			ent != nil && ent.Ref == r.Inc {
			entry, ord = ent, o
		}
	}
	if entry == nil {
		t.Fatal("the authorized cleanup duty could not be found, so nothing was dispatched")
	}

	// THE CONSUMER ITSELF IS THE ORACLE. Its continued existence is what a withheld delete looks
	// like, and it cannot be satisfied by a refusal that happens AFTER the delete: the resource
	// stays DELETE_AUTHORIZED either way, because the completion is refused for want of the same
	// authority, so state alone cannot tell the two apart.
	// The enrolment fixture writes the RECORD, not the consumer, so the physical thing whose
	// survival is the oracle has to be made to exist.
	_, deliver, _ := mqttQ2AffineNames(getHash(e.id), r.AffGen)
	if _, ccerr := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			Durable: r.Name, DeliverSubject: deliver, AckPolicy: AckExplicit,
			FilterSubject: mqttQ2AffineSubjectPrefix + getHash(e.id) + "." + r.AffGen + ".>",
		},
	}); ccerr != nil {
		t.Fatalf("creating the affine consumer the oracle needs: %v", ccerr)
	}
	if _, cerr := e.asm.jsa.lookupConsumer(mqttOutStreamName, r.Name); cerr != nil {
		t.Fatalf("the affine consumer %q was already gone before the dispatch: %v", r.Name, cerr)
	}
	live := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: gen}
	e.asm.executeRecoveryDuty(e.asm.server(), live,
		mqttSweptDuty{Ord: ord, Entry: entry}, newMqttRecoveryBudget(), nil, true)
	if _, cerr := e.asm.jsa.lookupConsumer(mqttOutStreamName, r.Name); cerr != nil {
		t.Fatalf("the affine consumer %q was DELETED for a client whose authority is gone, so "+
			"the irreversible half ran before the authority was established: %v", r.Name, cerr)
	}
	if after, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc); after == nil ||
		after.State != mqttResourceDeleteAuthorized {
		t.Fatalf("the resource moved off DELETE_AUTHORIZED with no authority to license it: %+v",
			after)
	}
}

// TestF105AnAbsentAuthorityWithholdsTheReadinessWorkToo is the same rule on the OTHER mutating
// branch, and it exists because fixing the first one alone was the mistake.
//
// The readiness work is dispatched from the WITHHELD arm rather than the acting one, so the guard
// added for the cleanup never covered it: with no readiness record and the authority gone, the
// pass still secured proofs, marked owned assignments Discarded and could publish readiness — and
// the seal that would have been refused comes after all of it. A refusal that arrives after the
// irreversible half is not a refusal.
func TestF105AnAbsentAuthorityWithholdsTheReadinessWorkToo(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-noauth-readiness")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingClose(t, e, affGen, durable, false)

	// AN OWNED ASSIGNMENT, so the branch has something to mutate and its absence of change is
	// measurable. A planted claim of THIS client's affine generation is what the discard targets.
	claim := &mqttQ2AffClaim{
		Winner: getHash(e.id), Gen: affGen,
		GroupDurable: "$MQTT_SHARED_noauth", SourceSeq: 1,
		ClaimHash: "hash", Nonce: mqttNewInflightNonce(),
		CopySubject: mqttQ2AffCopySubject(getHash(e.id), affGen, "hash", "nonce"),
	}
	body, merr := json.Marshal(claim)
	if merr != nil {
		t.Fatalf("marshalling the owned claim: %v", merr)
	}
	subj := mqttQ2AffClaimSubject(claim.GroupDurable, claim.SourceSeq)
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(subj, hdr, msg); err != nil {
		t.Fatalf("planting the owned claim: %v", err)
	}

	authSubj := mqttOutboundAuthoritySubject(getHash(e.id))
	asmsg, aerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, authSubj)
	if aerr != nil || asmsg == nil {
		t.Fatalf("reading the authority to remove it: %v", aerr)
	}
	if derr := e.asm.jsa.deleteMsg(mqttInflightStreamName, asmsg.Sequence, true); derr != nil {
		t.Fatalf("removing the authority: %v", derr)
	}

	var walks int
	restore := setMQTTTestRecoveryReadinessHook(func() { walks++ })
	defer restore()
	for i := 0; i < 4*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}

	if walks != 0 {
		t.Fatalf("the readiness walk ran %d time(s) for a client whose authority is gone", walks)
	}
	if ready, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); ready != nil {
		t.Fatal("readiness was published for a client that carries no authority")
	}
	// THE ASSIGNMENT IS UNTOUCHED, which is the acid test: a walk that ran and was refused later
	// would have left this Discarded.
	after, _, aerr2 := e.asm.jsa.loadQ2AffClaim(subj)
	if aerr2 != nil || after == nil {
		t.Fatalf("the owned claim is gone, so the readiness work reaped it: %v", aerr2)
	}
	if after.Discarded {
		t.Fatal("the owned assignment was DISCARDED for a client that carries no authority")
	}
	if cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc); cur == nil ||
		cur.Prov != mqttProvisioningPending {
		t.Fatalf("the affine resource left PENDING provisioning with no authority: %+v", cur)
	}
}

// TestF105TheBoundaryJudgesTheEndingAgainstTheBatch is the S-xor-Delta rule, with the middle arm
// pinned so no other validator can mask the binding.
func TestF105TheBoundaryJudgesTheEndingAgainstTheBatch(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-ending-binding")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
		ID: e.id, Epoch: e.epoch, Gen: gen,
		AffGen: r.AffGen, Durable: r.Name, PubRelDur: r.PubRelDur,
	}); err != nil {
		t.Fatalf("publishing the readiness: %v", err)
	}
	if !testF105SweepUntil(t, e, func() bool {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		return cur != nil && cur.State == mqttResourceDeleteAuthorized
	}) {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		if cur == nil || cur.State != mqttResourceCleanupComplete {
			t.Fatalf("the cleanup never reached an authorized state: %+v", cur)
		}
		t.Skip("the pass completed the cleanup in one visit, so there is no authorized state to " +
			"compose against; the combined batch is exercised end to end by the cleanup test")
	}

	// THE COMBINED BATCH IS ACCEPTED, and that is the whole point of judging the ending against
	// the state the batch RESULTS IN: the stored resource is still DELETE_AUTHORIZED while this
	// is validated, so a rule reading only the store would refuse a batch that is correct.
	cur, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
	led, _, _ := e.asm.jsa.loadGenLedger(e.id, e.epoch, gen)
	var ord uint64
	for i := uint64(1); i <= led.SealedHi; i++ {
		d, _, _ := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, i)
		if d != nil && d.Ref == r.Inc && d.Kind == mqttDutyCleanup {
			ord = i
			break
		}
	}
	if ord == 0 {
		t.Fatal("the affine resource has no cleanup duty")
	}
	root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: gen}
	if err := e.asm.jsa.completeCleanupAndResolve(root, cur, rseq,
		mqttSweptDuty{Ord: ord}, mqttNewInflightNonce(), mqttCleanupGone); err != nil {
		t.Fatalf("the combined completion batch was refused: %v", err)
	}
	after, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
	if after == nil || after.State != mqttResourceCleanupComplete {
		t.Fatalf("the resource did not complete: %+v", after)
	}
	d, _, _ := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, ord)
	if d == nil || d.State != mqttDutyTerminal {
		t.Fatalf("the duty did not end in the same batch: %+v", d)
	}
}

// TestF105CleanupOfG1RunsWhileG2Serves: a retired generation's disposal runs under the CLOSED
// authority the client carries, and it neither moves the successor's ownership nor resolves a
// proposal standing beside it.
func TestF105CleanupOfG1RunsWhileG2Serves(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-exec-g1g2")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	// The successor is SERVING after the discarding finalization, and it is a different
	// generation from the one being cleaned up.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityServing {
		t.Fatalf("the successor is not serving: status %v", al.status)
	}
	if al.rec.Gen == gen {
		t.Fatalf("the successor carries the generation being cleaned up")
	}
	before := al.rec

	if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
		ID: e.id, Epoch: e.epoch, Gen: gen,
		AffGen: r.AffGen, Durable: r.Name, PubRelDur: r.PubRelDur,
	}); err != nil {
		t.Fatalf("publishing the readiness: %v", err)
	}
	if !testF105SweepUntil(t, e, func() bool {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		return cur != nil && cur.State == mqttResourceCleanupComplete
	}) {
		t.Fatal("the predecessor's cleanup never completed while its successor served")
	}

	after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.status != mqttOutboundValid {
		t.Fatalf("the authority no longer reads: status %v", after.status)
	}
	if after.rec.Gen != before.Gen || after.rec.Tenure != before.Tenure ||
		after.rec.OwnerTxn != before.OwnerTxn || after.rec.State != before.State {
		t.Fatalf("cleaning up a retired generation moved the serving ownership: %+v -> %+v",
			before, after.rec)
	}
}

// TestF105AnUnsupportedOutcomeIsRefused pins what a completion may say.
func TestF105AnUnsupportedOutcomeIsRefused(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-exec-outcome")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	cur, rseq, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
	root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: gen}
	err := e.asm.jsa.completeCleanupAndResolve(root, cur, rseq, mqttSweptDuty{Ord: 1},
		mqttNewInflightNonce(), "invented")
	if err == nil {
		t.Fatal("a cleanup completed with an outcome this broker does not write")
	}
	if !strings.Contains(err.Error(), "state") && !strings.Contains(err.Error(), "outcome") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// TestF105ARestartedBrokerCompletesTheEnrolledAffineCleanup is the WITNESS.
//
// Everything a running broker knows is destroyed between the enrolment and the recovery: the
// server is shut down and a fresh one is brought up against the same JetStream state, so the new
// broker has none of the first one's memory — no session object, no teardown target, no in-flight
// intent. What survives is the durable evidence the closing batch wrote.
//
// The oracle is the whole lifecycle, and each step is asserted rather than assumed:
//
//  1. the teardown's own classification DEFERS the affine deletion, and the consumer is still
//     standing when the teardown's deletions have run — the transfer is observable only as the
//     absence of the act, because a consumer that is merely gone afterwards is equally explained
//     by a synchronous delete that ran anyway;
//  2. the metadata release is AUTHORIZED with the cleanup still outstanding;
//  3. a restarted broker completes it from persisted evidence alone;
//  4. the duty reaches TERMINAL and the generation is CERTIFIED — which is why this fixture
//     closes a generation whose only enrolled duty is the affine one: a shared duty is
//     deliberately untransferred, so a generation carrying one can never retire and a witness
//     built on it could not tell a broken retirement from an unfinished one;
//  5. only then, the consumer is gone.
func TestF105ARestartedBrokerCompletesTheEnrolledAffineCleanup(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const clientID = "client-f105-witness"
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	asm := testMQTTGetAccountSessionManager(t, s, clientID)
	asm.mu.RLock()
	epoch := asm.sessStreamEpoch
	asm.mu.RUnlock()
	if epoch == _EMPTY_ {
		t.Fatal("the account has no recovery epoch, so nothing here can be attributed")
	}
	e := &testF105Env{asm: asm, id: clientID, epoch: epoch}
	testF105ClearMintedAuthority(t, asm, clientID, nil)

	// THE AFFINE GENERATION AND ITS DERIVED NAMES. The resource's own name is derived from the
	// affine generation, so the fixture cannot invent one: a record naming any other consumer is
	// refused on read, which is the rule this witness would otherwise be free to sidestep.
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(clientID), affGen)
	if _, err := asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			Durable: durable, FilterSubject: mqttOutSubjectPrefix + "witness",
			AckPolicy: AckExplicit, DeliverSubject: mqttSubPrefix + durable, MaxAckPending: 1024,
		},
	}); err != nil {
		t.Fatalf("creating the affine consumer: %v", err)
	}

	// AFFINE-ONLY, so the generation can retire once that one duty ends.
	gen, res := testF105DiscardingClose(t, e, affGen, durable, false)
	if res.Name != durable {
		t.Fatalf("the enrolment names %q, and the consumer is %q", res.Name, durable)
	}

	// (1) THE TEARDOWN DEFERS THE DELETION. Its own classification decides that, before the first
	// physical act, and the consumer is asserted STILL STANDING afterwards.
	own, cerr := asm.classifyTeardownGeneration(clientID, epoch, gen, &mqttTeardownTarget{
		id: clientID, idHash: getHash(clientID), gen: gen,
		q2AffDur: durable, q2AffGen: affGen,
	})
	if cerr != nil {
		t.Fatalf("classifying the enrolled teardown: %v", cerr)
	}
	if own.Class != mqttTeardownOwnerDiscarding || own.Affine == nil {
		t.Fatalf("the enrolled teardown classified as %v (%+v)", own.Class, own.Affine)
	}
	sess := mqttSessionCreate(&asm.jsa, clientID, getHash(clientID), gen, 0, _EMPTY_, _EMPTY_,
		s.getOpts())
	sess.asm = asm
	if derr := sess.deleteTeardownTargets(&mqttTeardownTarget{
		id: clientID, idHash: getHash(clientID), gen: gen,
		q2AffDur: durable, q2AffGen: affGen, deferAffineDelete: true,
	}, false); derr != nil {
		t.Fatalf("running the teardown's deletions: %v", derr)
	}
	if _, err := asm.jsa.lookupConsumer(mqttOutStreamName, durable); err != nil {
		t.Fatalf("the teardown deleted a DEFERRED affine consumer: %v", err)
	}

	// (2) THE RELEASE IS AUTHORIZED WITH THE CLEANUP STILL OUTSTANDING, which is the property the
	// whole transfer exists for: the gate asks for ENROLMENT, not for completion.
	facts, ferr := asm.gatherAdoptedReleaseFacts(clientID, epoch, gen)
	if ferr != nil {
		t.Fatalf("gathering the release facts: %v", ferr)
	}
	if gerr := mqttAdoptedReleaseAuthorized(clientID, gen, facts); gerr != nil {
		t.Fatalf("the release was withheld although the cleanup is enrolled and reachable: %v", gerr)
	}
	cur, _, _ := asm.jsa.loadOutboundResource(clientID, epoch, gen, res.Inc)
	if cur == nil || cur.State != mqttResourceLive {
		t.Fatalf("the cleanup was not still outstanding at the release: %+v", cur)
	}

	// EVERY TRANSIENT OBJECT GOES.
	c.Close()
	s.Shutdown()
	s = testMQTTRestartServerSameStore(t, s, o)
	if err := s.Start(); err != nil {
		t.Fatalf("restarting the broker: %v", err)
	}
	defer testMQTTShutdownServer(s)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "client-f105-witness-other", cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	asm2 := testMQTTGetAccountSessionManager(t, s, "client-f105-witness-other")
	if asm2 == asm {
		t.Fatal("the restarted broker is still using the first one's account manager")
	}
	asm2.mu.RLock()
	epoch2 := asm2.sessStreamEpoch
	asm2.mu.RUnlock()
	if epoch2 != epoch {
		t.Fatalf("the restarted broker reads recovery epoch %q, and the enrolment was written "+
			"under %q", epoch2, epoch)
	}

	// (3) AND THE FRESH BROKER FINISHES IT, from the persisted evidence alone.
	var certified bool
	for i := 0; i < 6*mqttDirShards && !certified; i++ {
		asm2.sweepOutboundRecovery(s, nil)
		cert, _ := asm2.jsa.loadGenCompletionCert(clientID, epoch, gen)
		certified = cert != nil
	}
	after, _, _ := asm2.jsa.loadOutboundResource(clientID, epoch, gen, res.Inc)
	if after == nil || after.State != mqttResourceCleanupComplete {
		t.Fatalf("a restarted broker did not complete the enrolled cleanup: %+v", after)
	}
	if after.Outcome != mqttCleanupRemoved {
		t.Fatalf("the cleanup recorded %q for a consumer that existed and was deleted", after.Outcome)
	}

	// (4) THE DUTY ENDED, and the generation is CERTIFIED — the two facts that say the enrolled
	// cleanup is what completed it, and that nothing of this generation's inventory is left over.
	led, _, _ := asm2.jsa.loadGenLedger(clientID, epoch, gen)
	if led == nil {
		t.Fatal("the generation's ledger is gone")
	}
	for ord := uint64(1); ord <= led.SealedHi; ord++ {
		d, _, derr := asm2.jsa.loadDutyEntryAt(clientID, epoch, gen, ord)
		if derr != nil || d == nil {
			t.Fatalf("duty %d could not be read after the restart: %v", ord, derr)
		}
		if d.State != mqttDutyTerminal {
			t.Fatalf("duty %d stands at state %d after the restart", ord, d.State)
		}
	}
	cert, cerr2 := asm2.jsa.loadGenCompletionCert(clientID, epoch, gen)
	if cerr2 != nil || cert == nil {
		t.Fatalf("the generation was never certified: %v", cerr2)
	}
	if cert.SealedHi != led.SealedHi {
		t.Fatalf("the certificate covers bound %d and the inventory sealed at %d",
			cert.SealedHi, led.SealedHi)
	}
	// AND ITS ROOT IS GONE, which is what retirement does after the certificate stands.
	closed, _, _ := asm2.jsa.loadGenClosure(clientID, epoch, gen)
	if closed == nil {
		t.Fatal("the closure is gone")
	}
	if root, _, rerr := asm2.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd); rerr != nil || root != nil {
		t.Fatalf("the root outlived its certificate: %+v (%v)", root, rerr)
	}

	// (5) ONLY NOW, the consumer's absence — which on its own proves nothing.
	if _, err := asm2.jsa.lookupConsumer(mqttOutStreamName, durable); err == nil {
		t.Fatal("the affine consumer outlived a completed cleanup")
	}
}

// TestF105TheEndingRuleBindsToItsOwnResource is the wrong-resource fixture, pinned so no other
// validator can mask the binding.
//
// The batch carries a completion for resource A that is independently valid at its own CAS
// expectation, and a terminal duty continuation for resource B — which EXISTS, stands
// cleanup-INCOMPLETE, holds a valid pending cleanup duty at the expectation the continuation
// names, and has NO completion member of its own. Every member passes its own validator; B cannot
// support its own ending; only the resource binding can reject the pairing.
func TestF105TheEndingRuleBindsToItsOwnResource(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-ending-ab")
	defer cleanup()

	// TWO resources in one generation, each with its own pending cleanup duty: the affine
	// consumer the closure enrols, and a source consumer provisioned before it.
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, a := testF105DiscardingClose(t, e, affGen, durable, true)
	var b *mqttOutboundResource
	res, _, rerr := e.asm.enumerateGenerationResources(e.id, e.epoch, gen)
	if rerr != nil {
		t.Fatalf("enumerating the enrolment: %v", rerr)
	}
	for _, r := range res {
		if r.Kind == mqttResourceSourceConsumer {
			b = r
		}
	}
	if b == nil {
		t.Fatal("this generation carries no second resource to bind against")
	}

	// A is driven to an authorized cleanup, so its completion member is independently legal.
	if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{
		ID: e.id, Epoch: e.epoch, Gen: gen,
		AffGen: affGen, Durable: durable, PubRelDur: a.PubRelDur,
	}); err != nil {
		t.Fatalf("publishing the readiness: %v", err)
	}
	if !testF105SweepUntil(t, e, func() bool {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, a.Inc)
		return cur != nil && cur.State != mqttResourceLive
	}) {
		t.Fatal("resource A never reached an authorized cleanup")
	}
	curA, seqA, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, a.Inc)
	if curA.State != mqttResourceDeleteAuthorized {
		t.Skipf("resource A completed in one visit (state %d), so there is no authorized state to "+
			"compose A's completion from", curA.State)
	}

	curB, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, b.Inc)
	if curB == nil || curB.State == mqttResourceCleanupComplete {
		t.Fatalf("resource B is not cleanup-incomplete: %+v", curB)
	}
	led, _, _ := e.asm.jsa.loadGenLedger(e.id, e.epoch, gen)
	var ordB, seqB uint64
	for i := uint64(1); i <= led.SealedHi; i++ {
		d, sq, _ := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, i)
		if d != nil && d.Ref == b.Inc && d.Kind == mqttDutyCleanup {
			ordB, seqB = i, sq
			break
		}
	}
	if ordB == 0 {
		t.Fatal("resource B has no pending cleanup duty")
	}

	// THE PAIRING: A's completion, B's ending.
	nextA := *curA
	nextA.State, nextA.Outcome = mqttResourceCleanupComplete, mqttCleanupGone
	abody, merr := json.Marshal(&nextA)
	if merr != nil {
		t.Fatalf("marshalling A's completion: %v", merr)
	}
	dB, _, _ := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, ordB)
	dbody, berr := mqttResolvedDutyBody(dB, string(mqttCleanupGone))
	if berr != nil {
		t.Fatalf("composing B's ending: %v", berr)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	idHash := getHash(e.id)
	txn := mqttNewInflightNonce()
	_, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: al.rec.State, Rev: al.rec.Rev + 1, Txn: txn, OwnerTxn: al.rec.OwnerTxn,
			Intent: al.rec.Intent, Creation: al.rec.Creation,
		},
		Extra: []mqttTransitionMember{
			{subj: mqttResourceSubject(idHash, gen, a.Inc), expect: seqA, body: abody},
			{subj: mqttDutySubject(idHash, gen, ordB), expect: seqB, body: dbody},
		},
	})
	if err == nil {
		t.Fatal("a terminal duty for resource B was earned by a completion for resource A")
	}
	if !strings.Contains(err.Error(), "cleanup duty") {
		t.Fatalf("refused for the wrong reason: %v", err)
	}
}

// TestF105ReadinessConvergesPastOnePassOfClaims is round 2's finding 2, pinned.
//
// The assignment stream is keyed by GROUP DURABLE, not by client, so establishing that a retiring
// generation's assignments are discarded is an account-wide walk that cannot be filtered. Charging
// that walk against the per-operation budget does not bound it — it STARVES it: with a stable
// population larger than one pass the budget expires before the end, every pass repeats the same
// prefix, and readiness is never published for any generation. That is permanent starvation on a
// FINITE population, which is a different failure from the absent latency bound this mechanism
// does state.
//
// So the fixture plants MORE unrelated claims than one pass could ever charge for, and requires
// the generation to certify anyway.
func TestF105ReadinessConvergesPastOnePassOfClaims(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-readiness-converge")
	defer cleanup()

	// Unrelated, stable, and more numerous than the whole pass budget.
	const planted = mqttRecoveryOpBudget + 64
	for i := 0; i < planted; i++ {
		// WELL-FORMED, so the walk reads them rather than aborting on one: the attribution floor
		// every account-wide scan applies wants a winner, a valid affine generation, a group
		// durable, a source sequence and a copy subject.
		claim := &mqttQ2AffClaim{
			Winner: getHash("someone-else"), Gen: "0123456789abcdefghijkx",
			GroupDurable: "$MQTT_SHARED_unrelated", SourceSeq: uint64(i + 1),
			ClaimHash: "hash", Nonce: mqttNewInflightNonce(),
			CopySubject: mqttQ2AffCopySubject(getHash("someone-else"), "0123456789abcdefghijkx",
				"hash", "nonce"),
		}
		body, merr := json.Marshal(claim)
		if merr != nil {
			t.Fatalf("marshalling a planted claim: %v", merr)
		}
		msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
		subj := mqttQ2AffClaimSubject(claim.GroupDurable, claim.SourceSeq)
		if _, err := e.asm.jsa.storeMsg(subj, hdr, msg); err != nil {
			t.Fatalf("planting claim %d: %v", i, err)
		}
	}

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, r := testF105DiscardingClose(t, e, affGen, durable, false)

	certified := false
	for i := 0; i < 6*mqttDirShards && !certified; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		cert, _ := e.asm.jsa.loadGenCompletionCert(e.id, e.epoch, gen)
		certified = cert != nil
	}
	if !certified {
		cur, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, r.Inc)
		ready, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen)
		t.Fatalf("a generation with no claims of its own never certified behind %d unrelated "+
			"ones: resource %+v, readiness %+v", planted, cur, ready)
	}
}

// TestF105TheReadinessWalkIsCancellableMidScan: what protects the shared sweep goroutine from an
// account-wide walk is CANCELLATION, since the walk cannot be charged without starving it.
//
// So the stop is made to fire part-way through, and the assertion is that the walk ended there —
// no readiness record, because partial work never licenses the assertion — and that a later pass
// with no stop completes it.
func TestF105TheReadinessWalkIsCancellableMidScan(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-readiness-cancel")
	defer cleanup()

	for i := 0; i < 64; i++ {
		claim := &mqttQ2AffClaim{
			Winner: getHash("someone-else"), Gen: "0123456789abcdefghijkx",
			GroupDurable: "$MQTT_SHARED_cancel", SourceSeq: uint64(i + 1),
			ClaimHash: "hash", Nonce: mqttNewInflightNonce(),
			CopySubject: mqttQ2AffCopySubject(getHash("someone-else"), "0123456789abcdefghijkx",
				"hash", "nonce"),
		}
		body, merr := json.Marshal(claim)
		if merr != nil {
			t.Fatalf("marshalling a planted claim: %v", merr)
		}
		msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(mqttQ2AffClaimSubject(claim.GroupDurable, claim.SourceSeq),
			hdr, msg); err != nil {
			t.Fatalf("planting claim %d: %v", i, err)
		}
	}

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, _ := testF105DiscardingClose(t, e, affGen, durable, false)

	// A STOP THAT FIRES ONCE THE WALK HAS BEGUN, keyed on the walk itself rather than on a call
	// count, so the cancellation lands at the walk's first store operation no matter how many
	// operations the traversal that reached it happened to cost.
	// THE STOP FIRES AFTER THE WALK HAS DONE SOME UNITS, not at its entry. Entry alone would not
	// show what the contract actually promises: units already entered run to their end, and the
	// stop lands BETWEEN units. Counting the walk's own checks is what places it there.
	var walks, checks int
	restore := setMQTTTestRecoveryReadinessHook(func() { walks++ })
	defer restore()
	stop := func() bool {
		if walks == 0 {
			return false
		}
		checks++
		return checks > 8
	}
	for i := 0; i < 4*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), stop)
	}
	if walks == 0 {
		t.Fatal("the readiness walk was never entered, so nothing was interrupted")
	}
	if checks <= 8 {
		t.Fatalf("the walk consulted the stop only %d times, so it never reached the point the "+
			"stop was placed at and the interruption below proves nothing", checks)
	}
	if ready, rerr := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); rerr != nil || ready != nil {
		t.Fatalf("an interrupted walk published readiness: %+v (%v)", ready, rerr)
	}

	// AND AN UNINTERRUPTED PASS FINISHES IT, which is what says the absence above is the stop's
	// doing rather than something else refusing.
	restore()
	for i := 0; i < 4*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		if ready, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); ready != nil {
			return
		}
	}
	t.Fatal("readiness was never published once the stop was lifted")
}

// testF105SecondDiscardingClose closes the CURRENTLY SERVING generation with a discarding
// disposition, so one client ends up with TWO enrolled roots in ONE directory shard.
//
// One shard is the point: a root's shard is derived from its client (mqttDirShardFor(getHash(id))),
// so two generations of one client are consecutive ordinals of the same traversal — which is the
// only arrangement in which one root can starve another of the pass's single readiness attempt.
func testF105SecondDiscardingClose(t *testing.T, e *testF105Env,
	nextGen, affGen, durable string) (string, *mqttOutboundResource) {

	t.Helper()
	jsa := &e.asm.jsa
	cur := jsa.loadOutboundAuthority(e.id, e.epoch)
	if cur.status != mqttOutboundValid || cur.rec == nil {
		t.Fatalf("no serving authority to close: status %v", cur.status)
	}
	ps, seq := testF105StandingSession(t, e)
	_, _, pubRelDur := mqttPubRelNames(getHash(e.id), cur.rec.Gen)

	intent := &mqttHandoffIntent{
		ID: "intent-second-" + nextGen, PrevGen: cur.rec.Gen, PrevTenure: cur.rec.Tenure,
		PrevOwnerTxn: cur.rec.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: nextGen, Tenure: "tenure-second-" + nextGen, CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: cur.rec.Gen, Tenure: cur.rec.Tenure, OwnerTxn: cur.rec.OwnerTxn,
			SessWID: ps.WID, SessSeq: seq, Reason: mqttRetireCleanStart, Discard: true,
			AffineDurable: durable, AffineGen: affGen, PubRelDurable: pubRelDur,
		},
	}
	prev := cur.rec.Gen
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-second-"+nextGen, intent); err != nil {
		t.Fatalf("preparing the second close: %v", err)
	}
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: intent.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the second close's metadata half: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin-second-"+nextGen, intent.ID); err != nil {
		t.Fatalf("finalizing the second close: %v", err)
	}
	res, unreadable, rerr := e.asm.enumerateGenerationResources(e.id, e.epoch, prev)
	if rerr != nil || unreadable != 0 {
		t.Fatalf("enumerating the second enrolment: %v (%d unreadable)", rerr, unreadable)
	}
	for _, r := range res {
		if r.Kind == mqttResourceAffineConsumer {
			return prev, r
		}
	}
	t.Fatal("the second discarding finalization enrolled no affine consumer")
	return _EMPTY_, nil
}

// TestF105APermanentlyFailingRootDoesNotStarveItsSibling is holistic round 3's finding 1.
//
// The pass's single readiness attempt is spent BEFORE either helper can succeed, and roots are
// visited in ordinal order, so a root whose proof resolution fails permanently would take the
// attempt on every pass and the sibling visited after it would never get one. The fix couples the
// readiness quantum to the traversal quantum: the pass ends after the root that spends the
// attempt, with the shard position advanced past it and persisted, so the shard resumes at the
// sibling.
//
// Two generations of ONE client is the arrangement that tests this, because a root's shard is
// derived from its client — the two roots are consecutive ordinals of one traversal by
// construction rather than by luck.
func TestF105APermanentlyFailingRootDoesNotStarveItsSibling(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-starve")
	defer cleanup()

	const affA, affB = "0123456789abcdefghijka", "0123456789abcdefghijkb"
	_, _, durA := mqttQ2AffineNames(getHash(e.id), affA)
	genA, _ := testF105DiscardingClose(t, e, affA, durA, false)

	_, _, durB := mqttQ2AffineNames(getHash(e.id), affB)
	genB, resB := testF105SecondDiscardingClose(t, e, "0123456789abcdefghijkc", affB, durB)
	if genA == genB {
		t.Fatalf("both closes named generation %q, so there is only one root", genA)
	}
	if resB == nil {
		t.Fatal("the second close enrolled no affine resource")
	}

	// A FAILS FOREVER, and only A. The proof scan is what resolves first, so refusing it is the
	// permanent failure the finding describes — and it is scoped by subject, so B's own scan is
	// untouched and any starvation observed is the scheduler's, not the fixture's.
	subjA, _, _ := mqttPubRelNames(getHash(e.id), genA)
	var refusals int
	e.asm.mu.Lock()
	e.asm.testInflightPubRelScan = func(subject string) ([]mqttStoredPubRel, int, bool) {
		if subject == subjA {
			refusals++
			return nil, 0, false
		}
		return nil, 0, true
	}
	e.asm.mu.Unlock()

	// B MUST STILL GET THERE. The bound is generous on purpose: what is under test is whether B
	// converges at all, not how fast.
	ready := false
	for i := 0; i < 8*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		if r, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, genB); r != nil {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatalf("generation %q never published readiness in %d passes while %q was failing "+
			"(%d refusals), so the failing root held the attempt against its sibling",
			genB, 8*mqttDirShards, genA, refusals)
	}
	if refusals == 0 {
		t.Fatal("the failing root never attempted readiness, so nothing was contended for")
	}

	// AND A IS STILL TRIED, rather than being dropped to make room: the fix reschedules the
	// attempt, it does not retire the root that failed.
	before := refusals
	for i := 0; i < 2*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}
	if refusals == before {
		t.Fatalf("the failing root was never retried after its sibling converged (%d refusals)",
			refusals)
	}
}

// TestF105ThePassEndsWithTheRootThatSpendsTheReadinessAttempt is the MECHANISM behind the fairness
// claim, asserted directly.
//
// The starvation it prevents cannot be reproduced on demand — that needs two roots whose sweeps
// roll in lockstep, and no fixture arranges that deterministically. What CAN be asserted is the
// coupling that makes the starvation impossible whatever the alignment: a pass that spends its one
// readiness attempt visits no further root, so the shard's traversal position is left just past
// the spender and the next visit begins at its sibling.
func TestF105ThePassEndsWithTheRootThatSpendsTheReadinessAttempt(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-quantum")
	defer cleanup()

	// THREE roots, not two. With two, the root that spends the attempt is always the LAST one its
	// shard walk reaches, so "the walk stops there" is unobservable — the walk was going to stop
	// anyway. A third root is what puts something AFTER the spender for the walk to skip.
	const affA, affB, affC = "0123456789abcdefghijka", "0123456789abcdefghijkb",
		"0123456789abcdefghijkd"
	_, _, durA := mqttQ2AffineNames(getHash(e.id), affA)
	testF105DiscardingClose(t, e, affA, durA, false)
	_, _, durB := mqttQ2AffineNames(getHash(e.id), affB)
	testF105SecondDiscardingClose(t, e, "0123456789abcdefghijkc", affB, durB)
	_, _, durC := mqttQ2AffineNames(getHash(e.id), affC)
	testF105SecondDiscardingClose(t, e, "0123456789abcdefghijke", affC, durC)

	var visitsThisPass, visitsAfterSpend int
	var spent bool
	restoreVisit := setMQTTTestRecoveryRootVisitHook(func() {
		visitsThisPass++
		if spent {
			visitsAfterSpend++
		}
	})
	defer restoreVisit()
	restoreReady := setMQTTTestRecoveryReadinessHook(func() { spent = true })
	defer restoreReady()

	// THE PASS IS STARTED ON THE SHARD THAT HOLDS BOTH ROOTS, every pass. That makes the cursor
	// a measurement of the pass-level half of the coupling: a pass that ends with the spending
	// root takes exactly ONE shard, while one that carries on takes up to mqttRecoveryShardsPerPass
	// — and a root in a later shard would be denied the spent attempt and then advanced past,
	// which is the same starvation one shard along.
	shard := mqttDirShardFor(getHash(e.id))
	spendingPasses := 0
	for i := 0; i < 8*mqttDirShards; i++ {
		e.asm.mu.Lock()
		e.asm.recoveryShardCursor = shard
		e.asm.mu.Unlock()
		visitsThisPass, visitsAfterSpend, spent = 0, 0, false

		e.asm.sweepOutboundRecovery(e.asm.server(), nil)

		if !spent {
			continue
		}
		spendingPasses++
		e.asm.mu.RLock()
		after := e.asm.recoveryShardCursor
		e.asm.mu.RUnlock()
		if want := (shard + 1) % mqttDirShards; after != want {
			t.Fatalf("a pass that spent the readiness attempt on shard %d left the cursor at %d, "+
				"want %d — it went on to visit further shards", shard, after, want)
		}
		if visitsAfterSpend != 0 {
			t.Fatalf("%d root(s) were visited after the readiness attempt was spent, so the pass "+
				"does not end with the root that spends it and a failing root can hold the "+
				"attempt against its siblings", visitsAfterSpend)
		}
	}

	// THE FIXTURE MUST HAVE REACHED THE ROUTE, or everything above is an assertion about nothing.
	if spendingPasses == 0 {
		t.Fatal("no pass spent the readiness attempt, so the coupling was never exercised")
	}
}

// testF105PlantClaim writes one well-formed unrelated assignment claim.
func testF105PlantClaim(t *testing.T, e *testF105Env, group string, sseq uint64) {
	t.Helper()
	claim := &mqttQ2AffClaim{
		Winner: getHash("someone-else"), Gen: "0123456789abcdefghijkx",
		GroupDurable: group, SourceSeq: sseq,
		ClaimHash: "hash", Nonce: mqttNewInflightNonce(),
		CopySubject: mqttQ2AffCopySubject(getHash("someone-else"), "0123456789abcdefghijkx",
			"hash", "nonce"),
	}
	body, merr := json.Marshal(claim)
	if merr != nil {
		t.Fatalf("marshalling a planted claim: %v", merr)
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttQ2AffClaimSubject(group, sseq), hdr, msg); err != nil {
		t.Fatalf("planting a claim on %q/%d: %v", group, sseq, err)
	}
}

// TestF105TheAssignmentWalkExtendsItsHorizonAndCompletes is holistic round 4's finding, arm 1:
// a tail that MOVES during the walk is followed, and the walk still completes.
//
// The moving tail is supplied from inside the walk itself, so it is not a race the fixture hopes
// to win: the seam fires once the attempt is taken, and the records it plants land ahead of the
// reader's first horizon by construction.
func TestF105TheAssignmentWalkExtendsItsHorizonAndCompletes(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-horizon")
	defer cleanup()

	for i := 0; i < 8; i++ {
		testF105PlantClaim(t, e, "$MQTT_SHARED_before", uint64(i+1))
	}
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, _ := testF105DiscardingClose(t, e, affGen, durable, false)

	// A SHORT BURST FROM INSIDE THE WALK, then stillness. The writes must land AFTER the walk has
	// taken its first horizon — a burst at the walk's ENTRY is already below it and needs no
	// extension at all, which would make this fixture agree with a walk that never extends. The
	// walk's own cancellation check is the hook that guarantees the ordering.
	var walks, planted int
	restore := setMQTTTestRecoveryReadinessHook(func() { walks++ })
	defer restore()
	const burst = 4
	// THE SECOND TAIL READ IS THE FIRST VERIFICATION. The first read takes H0, the segment is
	// walked, and the second read is the one that decides whether the walk may claim completion —
	// so planting on THAT one, and only that one, puts the records strictly above H0 and requires
	// exactly ONE extension. Planting on the first read would land them below H0 and need none,
	// which is what made an earlier version of this fixture agree with a broker that never
	// extends at all.
	tailReads := 0
	restoreTail := setMQTTTestAssignTailHook(func() {
		tailReads++
		if tailReads != 2 {
			return
		}
		for i := 0; i < burst; i++ {
			planted++
			testF105PlantClaim(t, e, "$MQTT_SHARED_during", uint64(planted))
		}
	})
	defer restoreTail()

	for i := 0; i < 8*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		if r, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); r != nil {
			if walks == 0 || planted < burst {
				t.Fatalf("readiness appeared without a moved horizon (walks=%d planted=%d), so "+
					"the extension was never exercised", walks, planted)
			}
			// THE SAME WALK THAT SAW THE TAIL MOVE IS THE ONE THAT COMPLETED. Accepting a later
			// walk would accept a broker that refuses to extend at all: the planting happens
			// once, so every attempt after the first meets a still stream and completes whatever
			// the extension rule is.
			if walks != 1 {
				t.Fatalf("the walk that met the moved horizon did not complete; readiness took "+
					"%d attempts, so the horizon was not extended but waited out", walks)
			}
			return
		}
	}
	t.Fatalf("the walk never completed across a moving horizon (%d attempts, %d planted)",
		walks, planted)
}

// TestF105TheAssignmentWalkWithholdsReadinessOnAMovingTail is arm 3: a tail that keeps moving
// past the permitted extensions ends the attempt rather than chasing it, and asserts NOTHING.
//
// This is the finding's actual hazard — an attempt that never returns holds the maintenance
// goroutine, which also carries session expiry and both orphan sweeps — so the assertion is that
// the pass RETURNS, with no readiness and no seal, and that the generation still converges once
// the writing stops.
func TestF105TheAssignmentWalkWithholdsReadinessOnAMovingTail(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-moving-tail")
	defer cleanup()

	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, res := testF105DiscardingClose(t, e, affGen, durable, false)

	// A WRITE IN EVERY VERIFICATION INTERVAL is all it takes — the tail need not outrun the
	// reader, it need only never be still when the reader looks.
	var walks, planted int
	moving := true
	restore := setMQTTTestRecoveryReadinessHook(func() { walks++ })
	defer restore()
	// THE WRITER RIDES THE WALK'S OWN CANCELLATION CHECK. That check runs before every record
	// AND before every horizon read, so a write from here lands in every verification interval by
	// construction rather than by winning a race — and it needs no seam of its own.
	stop := func() bool {
		// BOUNDED, because the assertion is about ONE attempt: enough writes to outlast the
		// permitted extensions, not a load generator. mqttRecoveryClaimScanExtensions + 2
		// verification intervals is one more than the walk can absorb.
		if walks > 0 && moving && planted < 4*(mqttRecoveryClaimScanExtensions+2) {
			planted++
			testF105PlantClaim(t, e, "$MQTT_SHARED_moving", uint64(planted))
		}
		return false
	}

	for i := 0; i < mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), stop)
		if walks > 0 {
			break
		}
	}
	if walks == 0 || planted == 0 {
		t.Fatalf("the moving tail was never exercised (walks=%d planted=%d)", walks, planted)
	}
	if r, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); r != nil {
		t.Fatal("readiness was published while the walk could not see the whole claim set")
	}
	if cur, _, cerr := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, res.Inc); cerr == nil &&
		cur != nil && cur.Prov != mqttProvisioningPending {
		t.Fatalf("the affine resource was sealed on an incomplete walk: provisioning %q", cur.Prov)
	}

	// AND IT CONVERGES ONCE THE STREAM IS STILL, which is what says the withholding was the
	// tail's doing and not a permanent refusal.
	moving = false
	for i := 0; i < 8*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), stop)
		if r, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); r != nil {
			return
		}
	}
	t.Fatal("readiness was never published after the assignment stream went still")
}

// TestF105AnUnreadableSuffixIsNotACompletedSegment is holistic round 6's store-error conflation,
// found by consult 9 in the pinned server: every error returned by LoadMsg, LoadNextMsg and
// LoadLastMsg becomes JSNoMessageFoundErr, so a FAILED READ and an empty suffix are the same
// response on the wire.
//
// A segment that reads that as its own end reports success having skipped whatever lay beyond it,
// and the verification read cannot catch it — that read asks whether the TAIL MOVED, not whether
// the enumeration succeeded. So the response is indeterminate and readiness is withheld.
//
// The fixture makes the response arise honestly rather than by injection: deleting the stream's
// LAST record leaves the high-water above every live one, so a read from beyond the last live
// record returns no-message while the segment's bound says there should be more.
func TestF105AnUnreadableSuffixIsNotACompletedSegment(t *testing.T) {
	e, cleanup := testF105NewEnv(t, "client-f105-unreadable-suffix")
	defer cleanup()

	for i := 0; i < 4; i++ {
		testF105PlantClaim(t, e, "$MQTT_SHARED_suffix", uint64(i+1))
	}
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, _ := testF105DiscardingClose(t, e, affGen, durable, false)

	si, serr := e.asm.jsa.lookupStream(mqttQoS2AssignStreamName)
	if serr != nil || si == nil {
		t.Fatalf("reading the assignment stream: %v", serr)
	}
	last := si.State.LastSeq
	if last == 0 {
		t.Fatal("the assignment stream is empty, so there is no suffix to make unreadable")
	}
	if err := e.asm.jsa.deleteMsg(mqttQoS2AssignStreamName, last, true); err != nil {
		t.Fatalf("removing the last assignment record: %v", err)
	}

	var walks int
	restore := setMQTTTestRecoveryReadinessHook(func() { walks++ })
	defer restore()
	for i := 0; i < 4*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
	}
	if walks == 0 {
		t.Fatal("the readiness walk was never attempted, so the suffix was never read")
	}
	if r, _ := e.asm.jsa.loadAffineReadiness(e.id, e.epoch, gen); r != nil {
		t.Fatal("readiness was published although the walk's own segment could not be " +
			"established: a no-message response is not an absence certificate, because the " +
			"pinned server returns it for a failed store read too")
	}
}
