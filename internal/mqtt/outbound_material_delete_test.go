// Copyright 2026 The mqtt-nats-bridge Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

// testF105Materialize reserves a payload, writes `chunks` chunks through the fence, and SEALS
// it — the whole life of a materialization up to the point a cleanup can begin.
func testF105Materialize(t *testing.T, e *testF105Env, chunks uint64) uint64 {
	t.Helper()
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)
	if err := testF105SubmitMaterial(t, e, "txn-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, chunks, 0)),
	); err != nil {
		t.Fatalf("reserving the materialization: %v", err)
	}
	for ord := uint64(1); ord <= chunks; ord++ {
		_, seq := testF105LoadManifest(t, e)
		if err := testF105SubmitMaterial(t, e, "txn-chunk-"+mqttNewInflightNonce(),
			testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, chunks, ord)),
			testF105ChunkMember(t, e, ord, 0),
		); err != nil {
			t.Fatalf("writing chunk %d: %v", ord, err)
		}
	}
	_, seq := testF105LoadManifest(t, e)
	return seq
}

// testF105SealManifest takes a materialization to READY, which is what a materialization's last
// act does — and what a payload resource's own provisioning seal now requires to have happened.
func testF105SealManifest(t *testing.T, e *testF105Env, chunks uint64) {
	t.Helper()
	_, seq := testF105LoadManifest(t, e)
	if err := testF105SubmitMaterial(t, e, "txn-ready",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestReady, chunks, chunks)),
	); err != nil {
		t.Fatalf("sealing the manifest READY: %v", err)
	}
}

// testF105CloseManifest takes a materialization to CLOSED, which is what a cleanup does first.
func testF105CloseManifest(t *testing.T, e *testF105Env, chunks uint64) {
	t.Helper()
	_, seq := testF105LoadManifest(t, e)
	m := testF105Manifest(e, mqttManifestClosed, chunks, chunks)
	if err := testF105SubmitInventory(t, e, "txn-close",
		testF105ManifestMember(t, e, seq, m)); err != nil {
		t.Fatalf("closing the manifest: %v", err)
	}
}

// testF105AuthorizeDeletion takes the payload's cleanup owner to DELETE_AUTHORIZED the way
// production does: seal its provisioning, then authorize.
//
// Through the real assemblers rather than by writing the state, because the boundary refuses an
// asserted authorization — which is exactly what makes it worth relying on here.
func testF105AuthorizeDeletion(t *testing.T, e *testF105Env, inc string) {
	t.Helper()
	r, rseq := testF105LoadResource(t, e, inc)
	// The planting fixture may already have left provisioning sealed; only an UNSEALED one has
	// a seal to run, and a second one is refused rather than treated as done.
	if r.Prov != mqttProvisioningSealed {
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		seal, serr := mqttSealProvisioningTransition(al.rec, al.seq, "txn-seal-"+inc, "op-seal", r, rseq)
		if serr != nil {
			t.Fatalf("assembling the provisioning seal of %q: %v", inc, serr)
		}
		if _, cerr := e.asm.jsa.commitAuthorityTransition(seal); cerr != nil {
			t.Fatalf("sealing the provisioning of %q: %v", inc, cerr)
		}
		r, rseq = testF105LoadResource(t, e, inc)
	}
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	auth, aerr := mqttAuthorizeDeletionTransition(al.rec, al.seq, "txn-auth-"+inc, "op-auth", r, rseq)
	if aerr != nil {
		t.Fatalf("assembling the deletion authorization of %q: %v", inc, aerr)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(auth); cerr != nil {
		t.Fatalf("authorizing the deletion of %q: %v", inc, cerr)
	}
}

// TestF105MaterialIsRemovedOnlyUnderBothFacts is the contract-6 boundary.
//
// This lane's audit called the cleanup contract harmless on the reasoning that an unguarded
// deleter merely ACCUMULATES material. A review took that apart: the reasoning assumes deletion
// was already authorized against every dependency. Without that premise a deleter destroys
// material an obligation still needs; with it, deleting before closure still lets a delayed
// writer resurrect material after an apparently completed cleanup. Neither licenses one.
func TestF105MaterialIsRemovedOnlyUnderBothFacts(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-material-delete")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 2)

	// NEITHER FACT YET: the manifest is still provisioning and the owner is still live.
	m, seq := testF105LoadManifest(t, e)
	if _, err := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-purge-early", m, seq); err == nil {
		t.Fatalf("material was removed with no authorization and an open manifest")
	}

	// CLOSURE ALONE IS NOT ENOUGH. Nothing can write more of this material, but something may
	// still be replaying it.
	testF105CloseManifest(t, e, 2)
	m, seq = testF105LoadManifest(t, e)
	err := func() error {
		_, err := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-purge-unauth", m, seq)
		return err
	}()
	if err == nil {
		t.Fatalf("material was removed on closure alone")
	}
	if !strings.Contains(err.Error(), "nothing depends on this material") {
		t.Fatalf("the refusal was not the missing authorization: %v", err)
	}

	// AUTHORIZATION ALONE IS NOT ENOUGH EITHER, which is the other half and the one a
	// state-machine reading would miss: the two facts answer different questions.
	if err := mqttMaterialDeletionAllowed(
		testF105Manifest(e, mqttManifestReady, 2, 2),
		&mqttOutboundResource{Inc: "inc-pay", Gen: testF105Gen, State: mqttResourceDeleteAuthorized},
	); err == nil {
		t.Fatalf("material was removable on authorization alone")
	}

	// AND THE AUTHORIZATION IS OF THE EXACT RESOURCE. One that happens to be deletable is not
	// permission to delete somebody else's material.
	if err := mqttMaterialDeletionAllowed(
		testF105Manifest(e, mqttManifestClosed, 2, 2),
		&mqttOutboundResource{Inc: "inc-other", Gen: testF105Gen, State: mqttResourceDeleteAuthorized},
	); err == nil {
		t.Fatalf("one resource's authorization removed another's material")
	}

	// BOTH FACTS: the material goes, and the records that describe it go last.
	testF105AuthorizeDeletion(t, e, "inc-pay")
	// ANNOUNCE, then DELETE-AND-CONFIRM. Two acts, because the deletes run between them.
	m, seq = testF105LoadManifest(t, e)
	if more, perr := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-announce", m, seq); perr != nil || !more {
		t.Fatalf("announcing the removal: more=%v err=%v", more, perr)
	}
	m, seq = testF105LoadManifest(t, e)
	if m.Announced != 2 || m.Purged != 0 {
		t.Fatalf("the announcement recorded %d announced and %d confirmed", m.Announced, m.Purged)
	}
	if more, perr := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-purge", m, seq); perr != nil || !more {
		t.Fatalf("removing authorized material of a closed manifest: more=%v err=%v", more, perr)
	}
	for ord := uint64(1); ord <= 2; ord++ {
		subj := mqttChunkSubject(getHash(e.id), testF105Gen, testF105Payload, ord)
		if smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj); lerr == nil && smsg != nil {
			t.Fatalf("chunk %d is still standing after its removal", ord)
		}
	}
	// THE MANIFEST SURVIVES the step that removed the chunks: it is what a resumed cleanup
	// reads, and it goes only once nothing is left to resume.
	m, seq = testF105LoadManifest(t, e)
	if m.Purged != 2 {
		t.Fatalf("the manifest confirms %d chunk(s) removed, not 2", m.Purged)
	}
	if more, perr := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-purge-2", m, seq); perr != nil || more {
		t.Fatalf("finishing the removal: more=%v err=%v", more, perr)
	}
	if smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttManifestSubject(getHash(e.id), testF105Gen, testF105Payload)); lerr == nil && smsg != nil {
		t.Fatalf("the manifest is still standing after its material is gone")
	}
}

// TestF105APurgeIsAnnouncedBeforeItRemoves is why the count exists.
//
// A JetStream delete cannot ride in the atomic batch, so the order is the only thing that makes
// an interrupted cleanup resumable: announce, then remove. The opposite order leaves, on a
// crash, material nothing knows to finish removing while the manifest still claims it is whole.
func TestF105APurgeIsAnnouncedBeforeItRemoves(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-purge-announce")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 2)
	testF105CloseManifest(t, e, 2)
	testF105AuthorizeDeletion(t, e, "inc-pay")

	// THE ANNOUNCEMENT IS A CAS ON THE MANIFEST, so it is refused against a manifest that has
	// moved — which is what stops two cleanups announcing the same interval independently.
	m, seq := testF105LoadManifest(t, e)
	if _, err := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-stale", m, seq-1); err == nil {
		t.Fatalf("a purge was announced against a manifest sequence that has moved")
	}
	// AND NOTHING WAS REMOVED BY THE ATTEMPT THAT FAILED TO ANNOUNCE. This is the order
	// itself, asserted: a step that deleted first and recorded after would leave, on exactly
	// this failure, material gone from a payload whose manifest still claims it is whole.
	for ord := uint64(1); ord <= 2; ord++ {
		subj := mqttChunkSubject(getHash(e.id), testF105Gen, testF105Payload, ord)
		if smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj); lerr != nil || smsg == nil {
			t.Fatalf("chunk %d was removed by a step whose announcement never landed", ord)
		}
	}

	// AND IT ONLY EVER RISES. A cleanup that could lower it would re-announce material that is
	// already gone and then believe it had more to do than it does.
	_, seq = testF105LoadManifest(t, e)
	back := testF105Manifest(e, mqttManifestClosed, 2, 2)
	back.Announced, back.Purged = 0, 0
	if _, perr := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-purge", m, seq); perr != nil {
		t.Fatalf("the first purge step: %v", perr)
	}
	_, seq = testF105LoadManifest(t, e)
	err := testF105SubmitInventory(t, e, "txn-rewind", testF105ManifestMember(t, e, seq, back))
	if err == nil {
		t.Fatalf("the purged count was lowered")
	}
	// AND THE REFUSAL IS THE MONOTONICITY RULE'S. A lowered count also underflows the
	// step-size comparison, which refuses it for a reason that is an accident of arithmetic
	// rather than a statement about cleanups — and a rule that is only ever enforced by
	// accident is not enforced.
	if !strings.Contains(err.Error(), "lowers the announced count") {
		t.Fatalf("the rewind was refused by something other than the monotonicity rule: %v", err)
	}
}

// TestF105ARemovalIsBoundedAndResumable pins the two properties an interruption needs.
func TestF105ARemovalIsBoundedAndResumable(t *testing.T) {
	e := &testF105Env{id: testF105ID, epoch: testF105Epoch}
	m := testF105Manifest(e, mqttManifestClosed, mqttPurgeBatchMax+5, mqttPurgeBatchMax+5)

	// BOUNDED: one announcement covers at most what one step may cover, so a crash inside a
	// removal is always a crash inside a bounded interval.
	from, to, more := mqttNextPurgeInterval(m)
	if !more || from != 1 || to != mqttPurgeBatchMax {
		t.Fatalf("the first interval is %d..%d (more=%v)", from, to, more)
	}
	// RESUMABLE: the count says where to continue, and the continuation is the REST rather
	// than a repeat.
	m.Announced = to
	from, to, more = mqttNextPurgeInterval(m)
	if !more || from != mqttPurgeBatchMax+1 || to != mqttPurgeBatchMax+5 {
		t.Fatalf("the resumed interval is %d..%d (more=%v)", from, to, more)
	}
	m.Announced = m.Chunks
	if _, _, more := mqttNextPurgeInterval(m); more {
		t.Fatalf("a fully announced payload still offered material to announce")
	}
	// AND THE PENDING INTERVAL IS WHAT A RESUME OWES: everything announced and not confirmed,
	// re-deleted whole, because the store cannot say which of the announced deletes landed.
	from, to, pending := mqttPendingPurgeInterval(m)
	if !pending || from != 1 || to != m.Chunks {
		t.Fatalf("the pending interval is %d..%d (pending=%v)", from, to, pending)
	}
	// AND AN ANNOUNCEMENT IS NOT A REMOVAL. This is the distinction the two counts exist for:
	// with everything announced and nothing confirmed, the material is all still standing, and
	// a cleanup that read the announcement as done would remove the record describing it.
	if mqttMaterialFullyPurged(m) {
		t.Fatalf("a payload with everything announced and nothing confirmed reported its " +
			"material removed")
	}
	m.Purged = m.Chunks
	if _, _, pending := mqttPendingPurgeInterval(m); pending {
		t.Fatalf("a fully confirmed payload still owes deletes")
	}
	if !mqttMaterialFullyPurged(m) {
		t.Fatalf("a payload with every chunk CONFIRMED removed is not fully purged")
	}
	// AND ONLY A CLOSED MANIFEST IS EVER FULLY PURGED, because removal begins after closure.
	open := *m
	open.State = mqttManifestReady
	if mqttMaterialFullyPurged(&open) {
		t.Fatalf("an open manifest reported its material fully removed")
	}
}

// TestF105TheManifestGoesLastAndByItsOwnSequence is the review's fourth finding.
//
// A cleanup that reloads the manifest subject and deletes whatever stands there can delete a
// DIFFERENT materialization: worker A pauses in final cleanup, the old manifest is removed, a
// new materialization reserves the same payload token with a live cleanup owner of its own —
// which the reservation rules permit — and A then deletes the newcomer's record, stranding
// obligations already admitted against it.
func TestF105TheManifestGoesLastAndByItsOwnSequence(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-material-records")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 1)
	testF105CloseManifest(t, e, 1)
	testF105AuthorizeDeletion(t, e, "inc-pay")

	// WHILE MATERIAL STANDS, the record that describes it stays: it is what a resumed cleanup
	// reads.
	m, mSeq := testF105LoadManifest(t, e)
	if err := e.asm.finishMaterialRemoval(e.id, m, mSeq); err == nil {
		t.Fatalf("the manifest was removed while material was still standing")
	}

	// Announce, then delete-and-confirm. Two acts, because the deletes run between them.
	for i := 0; i < 2; i++ {
		m, mSeq = testF105LoadManifest(t, e)
		if more, err := e.asm.purgeMaterialStep(e.id, e.epoch,
			"txn-purge-"+mqttNewInflightNonce(), m, mSeq); err != nil || !more {
			t.Fatalf("purge step %d: more=%v err=%v", i, more, err)
		}
	}
	stale, staleSeq := testF105LoadManifest(t, e)
	if stale.Purged != stale.Chunks {
		t.Fatalf("the material is not confirmed removed: %d of %d", stale.Purged, stale.Chunks)
	}

	// AND ONCE THE MATERIAL IS GONE, the record that described it goes too — last of all.
	if _, err := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-finish", stale, staleSeq); err != nil {
		t.Fatalf("finishing the removal: %v", err)
	}
	if smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttManifestSubject(getHash(e.id), testF105Gen, testF105Payload)); lerr == nil && smsg != nil {
		t.Fatalf("the manifest is still standing after its material is gone")
	}

	// A REPLACEMENT MATERIALIZATION reserves the same payload token, with a cleanup owner of
	// its own — which the reservation rules permit.
	testF105PlantUnsealedResource(t, e, "inc-pay-2", mqttResourceRetainedPayload)
	fresh := testF105Manifest(e, mqttManifestProvisioning, 1, 0)
	fresh.Inc = "inc-pay-2"
	if err := testF105SubmitMaterial(t, e, "txn-reserve-2",
		testF105ManifestMember(t, e, 0, fresh)); err != nil {
		t.Fatalf("reserving the replacement: %v", err)
	}
	_, freshSeq := testF105LoadManifest(t, e)

	// THE PAUSED WORKER RESUMES against the state it was holding. Its removal names the
	// sequence it READ the manifest at, which nothing occupies any more, so it takes nothing.
	// A removal that reloaded the SUBJECT instead would take the newcomer's record here,
	// stranding obligations already admitted against it. The stale sequence is one this
	// materialization's own removal already emptied, so the assertion below is the only thing
	// this call can disturb.
	_ = e.asm.finishMaterialRemoval(e.id, stale, staleSeq)
	smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttManifestSubject(getHash(e.id), testF105Gen, testF105Payload))
	if lerr != nil || smsg == nil {
		t.Fatalf("a stale removal took the replacement's manifest")
	}
	if smsg.Sequence != freshSeq {
		t.Fatalf("the standing manifest moved during a stale removal: %d, want %d",
			smsg.Sequence, freshSeq)
	}
	if got, derr := mqttDecodeRetainedManifest(smsg.Data, e.id, e.epoch, testF105Gen,
		testF105Payload); derr != nil {
		t.Fatalf("the surviving manifest does not decode: %v", derr)
	} else if got.Inc != "inc-pay-2" {
		t.Fatalf("the surviving manifest belongs to %q", got.Inc)
	}
}

// TestF105AnAnnouncementIsCheckedAtTheBoundaryToo is the validator's half.
//
// `purgeMaterialStep` is the only route this broker takes, but the rules cannot live only
// there: a batch assembled by anything at all reaches the store, and "the executor checks it"
// is the caller contract this file was written to replace.
func TestF105AnAnnouncementIsCheckedAtTheBoundaryToo(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-purge-boundary")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 2)
	testF105SealManifest(t, e, 2)

	announce := func(txn string, state mqttManifestState, announced uint64) error {
		_, seq := testF105LoadManifest(t, e)
		m := testF105Manifest(e, state, 2, 2)
		m.Announced = announced
		return testF105SubmitInventory(t, e, txn, testF105ManifestMember(t, e, seq, m))
	}

	// CLOSING AND ANNOUNCING IN ONE STEP IS REFUSED. Closure is what a delayed chunk writer
	// meets, and a writer whose batch was already in flight has not met it until it has landed.
	testF105AuthorizeDeletion(t, e, "inc-pay")
	err := announce("txn-close-and-purge", mqttManifestClosed, 1)
	if err == nil {
		t.Fatalf("a manifest was closed and its material announced removed in one step")
	}
	if !strings.Contains(err.Error(), "rather than closed") {
		t.Fatalf("the refusal was not the closure requirement: %v", err)
	}

	testF105CloseManifest(t, e, 2)
	if err := announce("txn-purge-ok", mqttManifestClosed, 1); err != nil {
		t.Fatalf("an authorized announcement was refused: %v", err)
	}

	// ONE STEP DOES ONE THING. The deletes run BETWEEN the announcement and the confirmation,
	// outside any transaction, so an act that did both would confirm work that cannot have
	// happened — which is the single-count protocol a review took apart.
	_, seq := testF105LoadManifest(t, e)
	both := testF105Manifest(e, mqttManifestClosed, 2, 2)
	both.Announced, both.Purged = 2, 2
	err = testF105SubmitInventory(t, e, "txn-both", testF105ManifestMember(t, e, seq, both))
	if err == nil {
		t.Fatalf("one act announced a removal and confirmed it")
	}
	if !strings.Contains(err.Error(), "both announces and confirms") {
		t.Fatalf("the refusal was not the one-thing rule: %v", err)
	}

	// AND A CONFIRMATION IS EVIDENCE, NOT A CLAIM. This batch is arithmetically perfect — it
	// confirms exactly what was announced, under a closed manifest with an authorized owner —
	// and the chunk it says it removed is still standing. Accepting it would let a caller reach
	// `Purged == Chunks` without deleting a byte and have the manifest removed over material
	// nothing describes: the deleted witness's defect, at the cleanup boundary.
	confirmed := testF105Manifest(e, mqttManifestClosed, 2, 2)
	confirmed.Announced, confirmed.Purged = 1, 1
	err = testF105SubmitInventory(t, e, "txn-confirm", testF105ManifestMember(t, e, seq, confirmed))
	if err == nil {
		t.Fatalf("a confirmation was accepted for a chunk that is still standing")
	}
	if !strings.Contains(err.Error(), "evidence of a delete") {
		t.Fatalf("the refusal was not the evidence rule: %v", err)
	}

	// THE POSITIVE CONTROL: the executor, which deletes and then confirms. The owner's
	// deletion is already authorized above, which is what let the announcement land.
	for i := 0; i < 2; i++ {
		m, mseq := testF105LoadManifest(t, e)
		if more, perr := e.asm.purgeMaterialStep(e.id, e.epoch,
			"txn-drive-"+mqttNewInflightNonce(), m, mseq); perr != nil || !more {
			t.Fatalf("driving the removal: more=%v err=%v", more, perr)
		}
	}
	if m, _ := testF105LoadManifest(t, e); m.Purged == 0 {
		t.Fatalf("the executor confirmed nothing")
	}
}

// TestF105MaterialBelongsToTheGenerationThatWritesIt keeps a materialization inside its own
// generation.
//
// Cleanup is the exemption and the only one: a disposal of G1 runs while G2 serves, which is
// why that kind exists. Every other kind writes material for the generation it is admitted
// under — a materialization that could name an earlier generation's payload would grow material
// in a generation that may already be closed.
func TestF105MaterialBelongsToTheGenerationThatWritesIt(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-material-gen")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)

	const otherGen = "zyxwvu9876543210lkjihg"
	foreign := testF105Manifest(e, mqttManifestProvisioning, 1, 0)
	foreign.Gen = otherGen
	body, merr := json.Marshal(foreign)
	if merr != nil {
		t.Fatalf("marshalling: %v", merr)
	}
	member := mqttTransitionMember{
		subj: mqttManifestSubject(getHash(e.id), otherGen, testF105Payload), body: body,
	}
	err := testF105SubmitMaterial(t, e, "txn-foreign", member)
	if err == nil {
		t.Fatalf("a materialization wrote material of another generation")
	}
	if !strings.Contains(err.Error(), "rather than the") {
		t.Fatalf("the refusal was not the generation rule: %v", err)
	}
	// The positive control: the same reservation under this transition's own generation.
	if err := testF105SubmitMaterial(t, e, "txn-own",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)),
	); err != nil {
		t.Fatalf("a reservation of this generation's material was refused: %v", err)
	}
}

// TestF105APurgeStepMovesOneCountByOneRule is the purge state machine on its own.
//
// Through the pure function, because the interesting cases need a manifest with more chunks
// than a store fixture should have to write — and because each rule here is about the RELATION
// between the stored record and the proposed one, which is exactly what this function is.
func TestF105APurgeStepMovesOneCountByOneRule(t *testing.T) {
	a := &mqttTransitionAttempt{Txn: "txn-1"}
	cur := func(announced, purged uint64) *mqttRetainedManifest {
		return &mqttRetainedManifest{
			Payload: testF105Payload, State: mqttManifestClosed, Chunks: 200,
			Announced: announced, Purged: purged,
		}
	}
	next := func(announced, purged uint64) *mqttRetainedManifest {
		return cur(announced, purged)
	}

	for name, tc := range map[string]struct {
		cur, next *mqttRetainedManifest
		ok        bool
	}{
		"an announcement":            {cur(0, 0), next(64, 0), true},
		"a confirmation":             {cur(64, 0), next(64, 64), true},
		"a second announcement":      {cur(64, 64), next(128, 64), true},
		"nothing at all":             {cur(64, 64), next(64, 64), true},
		"an announcement that falls": {cur(64, 0), next(32, 0), false},
		"a confirmation that falls":  {cur(64, 64), next(64, 32), false},
		"both at once":               {cur(0, 0), next(64, 64), false},
		"an over-large announcement": {cur(0, 0), next(mqttPurgeBatchMax+1, 0), false},
		// A PARTIAL CONFIRMATION leaves the rest indistinguishable from material a later pass
		// must re-delete — which it is — while claiming progress no bounded interval describes.
		"a partial confirmation": {cur(64, 0), next(64, 32), false},
		// AND ONE THAT OUTRUNS ITS ANNOUNCEMENT confirms chunks nothing said it was removing.
		"a confirmation past the announcement": {cur(64, 0), next(64, 100), false},
	} {
		err := mqttCheckPurgeStep(a, tc.cur, tc.next)
		if tc.ok && err != nil {
			t.Fatalf("%s was refused: %v", name, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

// TestF105APausedCleanupDoesNotTakeAReplacementsChunks is round I-6's first finding.
//
// The exact-sequence fix protects the MANIFEST and nothing else. Worker A pauses with an
// announced interval; worker B finishes the removal and the record goes; a new materialization
// reserves the same payload token with a live owner of its own. A then resumes and deletes the
// interval BY SUBJECT — destroying the newcomer's material, which obligations may already have
// been admitted against. Its own CAS fails afterwards, long after the damage.
//
// Every chunk now says which materialization it belongs to, and the delete already loaded the
// record to learn its sequence — so the check costs nothing and the paused worker finds
// somebody else's chunk and leaves it alone.
func TestF105APausedCleanupDoesNotTakeAReplacementsChunks(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-paused-cleanup")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 1)
	testF105CloseManifest(t, e, 1)
	testF105AuthorizeDeletion(t, e, "inc-pay")

	// WORKER A ANNOUNCES and then pauses: it holds the manifest as it stood.
	paused, pausedSeq := testF105LoadManifest(t, e)
	if more, err := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-announce", paused, pausedSeq); err != nil || !more {
		t.Fatalf("announcing: more=%v err=%v", more, err)
	}
	pending, pendingSeq := testF105LoadManifest(t, e)

	// WORKER B FINISHES the removal.
	if more, err := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-b-confirm", pending, pendingSeq); err != nil || !more {
		t.Fatalf("confirming: more=%v err=%v", more, err)
	}
	done2, doneSeq := testF105LoadManifest(t, e)
	if _, err := e.asm.purgeMaterialStep(e.id, e.epoch, "txn-b-finish", done2, doneSeq); err != nil {
		t.Fatalf("finishing: %v", err)
	}

	// A REPLACEMENT MATERIALIZATION reuses the payload token, with a cleanup owner of its own.
	testF105PlantUnsealedResource(t, e, "inc-pay-2", mqttResourceRetainedPayload)
	fresh := testF105Manifest(e, mqttManifestProvisioning, 1, 0)
	fresh.Inc = "inc-pay-2"
	if err := testF105SubmitMaterial(t, e, "txn-reserve-2",
		testF105ManifestMember(t, e, 0, fresh)); err != nil {
		t.Fatalf("reserving the replacement: %v", err)
	}
	_, fseq := testF105LoadManifest(t, e)
	written := testF105Manifest(e, mqttManifestProvisioning, 1, 1)
	written.Inc = "inc-pay-2"
	body, merr := json.Marshal(&mqttRetainedChunk{Ord: 1, Inc: "inc-pay-2", Data: testF105ChunkData})
	if merr != nil {
		t.Fatalf("marshalling the replacement chunk: %v", merr)
	}
	if err := testF105SubmitMaterial(t, e, "txn-chunk-2",
		testF105ManifestMember(t, e, fseq, written),
		mqttTransitionMember{
			subj: mqttChunkSubject(getHash(e.id), testF105Gen, testF105Payload, 1), body: body,
		},
	); err != nil {
		t.Fatalf("writing the replacement's material: %v", err)
	}

	// WORKER A RESUMES against the state it was holding, and must take nothing.
	_ = e.asm.deleteChunkInterval(e.id, paused, 1, 1)
	smsg, lerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttChunkSubject(getHash(e.id), testF105Gen, testF105Payload, 1))
	if lerr != nil || smsg == nil {
		t.Fatalf("a paused cleanup deleted a replacement materialization's chunk")
	}
	c, derr := mqttDecodeRetainedChunk(smsg.Data, 1, "inc-pay-2")
	if derr != nil {
		t.Fatalf("the surviving chunk is not the replacement's: %v", derr)
	}
	if c.Inc != "inc-pay-2" {
		t.Fatalf("the surviving chunk belongs to %q", c.Inc)
	}
}
