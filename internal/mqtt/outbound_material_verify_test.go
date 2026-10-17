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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

// testF105OtherChunkMember writes a chunk of DIFFERENT bytes of the same length.
//
// The fence checks that a chunk landed, not what is in it, which is exactly the gap a seal has
// to close: equal-length material with a different content passes every layout rule.
func testF105OtherChunkMember(t *testing.T, e *testF105Env, ord uint64) mqttTransitionMember {
	t.Helper()
	body, err := json.Marshal(&mqttRetainedChunk{Ord: ord, Inc: "inc-pay", Data: []byte("87654321")})
	if err != nil {
		t.Fatalf("marshalling the chunk: %v", err)
	}
	return mqttTransitionMember{
		subj: mqttChunkSubject(getHash(e.id), testF105Gen, testF105Payload, ord), body: body,
	}
}

// TestF105ASealVerifiesTheMaterialItSeals is the review's second finding, and the reason the
// witness record is gone.
//
// My first answer was a durable witness of a verification. A caller supplied it, and the
// validator only compared it with the manifest's own claims — so reserving a manifest whose
// digest describes bytes A, writing bytes B of equal length through the fence, and sealing
// beside a witness copying the manifest's numbers passed every check with nothing ever reading
// B. A record that says "I verified" is not a verification.
//
// The seal now READS THE MATERIAL, inside the transaction the CAS fences.
func TestF105ASealVerifiesTheMaterialItSeals(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-verifies")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)

	// A manifest whose digest describes the fixture's bytes, and a chunk holding OTHER bytes of
	// the same length.
	if err := testF105SubmitMaterial(t, e, "txn-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)),
	); err != nil {
		t.Fatalf("reserving: %v", err)
	}
	_, seq := testF105LoadManifest(t, e)
	if err := testF105SubmitMaterial(t, e, "txn-chunk",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 1, 1)),
		testF105OtherChunkMember(t, e, 1),
	); err != nil {
		t.Fatalf("writing a chunk of other bytes: %v", err)
	}

	// EVERY LAYOUT RULE IS SATISFIED: one chunk claimed, one chunk written, the count is the
	// fence's own trace. Only reading the bytes can tell.
	_, seq = testF105LoadManifest(t, e)
	err := testF105SubmitMaterial(t, e, "txn-ready",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestReady, 1, 1)))
	if err == nil {
		t.Fatalf("a payload sealed READY over material that is not the material it describes")
	}
	if !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("the refusal was not the verification: %v", err)
	}

	// THE POSITIVE CONTROL: the same seal once the material IS what the manifest describes.
	// A chunk is immutable, so this is a fresh payload rather than a rewrite.
	e2, done2 := testF105NewEnv(t, "f105-seal-verifies-ok")
	defer done2()
	e2.bootstrap(t, "txn-1")
	seq2 := testF105Materialize(t, e2, 2)
	if err := testF105SubmitMaterial(t, e2, "txn-ready",
		testF105ManifestMember(t, e2, seq2, testF105Manifest(e2, mqttManifestReady, 2, 2)),
	); err != nil {
		t.Fatalf("sealing material that verifies was refused: %v", err)
	}
}

// TestF105SealingIsTheMaterializationsOwnAct keeps the seal off the kind that outlives closure.
//
// Disposal carries manifests — closing one is the first thing a cleanup does — and it is
// permitted under a CLOSED authority because cleanup must outlive closure. Sealing is the
// opposite: READY is the state an admission relies on, and a generation that may no longer
// admit anything has nothing to seal material for.
func TestF105SealingIsTheMaterializationsOwnAct(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-kind")
	defer done()
	e.bootstrap(t, "txn-1")
	seq := testF105Materialize(t, e, 1)

	err := testF105SubmitInventory(t, e, "txn-dispose-seal",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestReady, 1, 1)))
	if err == nil {
		t.Fatalf("a disposal sealed a payload READY")
	}
	if !strings.Contains(err.Error(), "materialization's own act") {
		t.Fatalf("the refusal was not the kind rule: %v", err)
	}
	// The positive control: the same seal under the kind that owns it.
	if err := testF105SubmitMaterial(t, e, "txn-ready",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestReady, 1, 1)),
	); err != nil {
		t.Fatalf("sealing under the materialization kind was refused: %v", err)
	}
}

// TestF105MaterialIsNotWrittenByADisposal is the same rule for the chunks themselves.
//
// The fixture is deliberately UNIMPEACHABLE: the batch under test is a legitimate next chunk of
// a manifest that still expects it, so the only thing wrong with it is the kind carrying it. An
// earlier version wrote a chunk the manifest did not claim, which every other rule refuses —
// and a mutation of the kind rule survived it.
func TestF105MaterialIsNotWrittenByADisposal(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-material-kind")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)
	if err := testF105SubmitMaterial(t, e, "txn-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 2, 0)),
	); err != nil {
		t.Fatalf("reserving: %v", err)
	}
	_, seq := testF105LoadManifest(t, e)
	if err := testF105SubmitMaterial(t, e, "txn-chunk-1",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 2, 1)),
		testF105ChunkMember(t, e, 1, 0),
	); err != nil {
		t.Fatalf("writing the first chunk: %v", err)
	}

	_, seq = testF105LoadManifest(t, e)
	err := testF105SubmitInventory(t, e, "txn-dispose-chunk",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 2, 2)),
		testF105ChunkMember(t, e, 2, 0),
	)
	if err == nil {
		t.Fatalf("a disposal wrote material")
	}
	if !strings.Contains(err.Error(), "under a serving authority") {
		t.Fatalf("the refusal was not the kind rule: %v", err)
	}
	if err := testF105SubmitMaterial(t, e, "txn-chunk-2",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 2, 2)),
		testF105ChunkMember(t, e, 2, 0),
	); err != nil {
		t.Fatalf("the same batch under the materialization kind was refused: %v", err)
	}
}

// TestF105VerifyingMaterialReadsWhatIsActuallyThere is the verifier both sides use.
//
// The seal runs it inside the transaction; a replay runs it before emitting a byte. The review
// required both — "as well, not instead" — because material can rot after it was sealed.
func TestF105VerifyingMaterialReadsWhatIsActuallyThere(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-verify-material")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 2)
	m, _ := testF105LoadManifest(t, e)

	material, err := e.asm.jsa.verifyMaterial(m)
	if err != nil {
		t.Fatalf("verifying whole material was refused: %v", err)
	}
	if string(material) != string(testF105MaterialOf(2)) {
		t.Fatalf("the material reassembled to %q", material)
	}

	// A MANIFEST CLAIMING MORE THAN IS STORED does not reassemble, and the refusal NAMES THE
	// CHUNK: a missing chunk read as a shorter payload would be refused by the length rule too,
	// and that refusal says the byte count is wrong when what is wrong is that material is
	// MISSING. An unresolved read must never be reported as a measurement.
	more := *m
	more.Chunks, more.Bytes = 3, 24
	_, err = e.asm.jsa.verifyMaterial(&more)
	if err == nil {
		t.Fatalf("material verified with a chunk that was never written")
	}
	if !strings.Contains(err.Error(), "chunk 3") {
		t.Fatalf("the refusal did not name the chunk that is not there: %v", err)
	}
	short := *m
	short.Bytes = 15
	if _, err := e.asm.jsa.verifyMaterial(&short); err == nil {
		t.Fatalf("material verified against a byte count it does not have")
	}
	rotten := *m
	rotten.Digest = mqttMaterialDigest([]byte("something else"))
	if _, err := e.asm.jsa.verifyMaterial(&rotten); err == nil {
		t.Fatalf("material verified against a digest of other bytes")
	}
	if _, err := e.asm.jsa.verifyMaterial(nil); err == nil {
		t.Fatalf("material was verified with no manifest describing it")
	}
}

// testF105TrySeal runs the real provisioning seal for one resource and returns what the
// boundary made of it.
func testF105TrySeal(t *testing.T, e *testF105Env, inc string) error {
	t.Helper()
	r, rseq := testF105LoadResource(t, e, inc)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	seal, serr := mqttSealProvisioningTransition(al.rec, al.seq, "txn-seal-"+inc, "op-seal", r, rseq)
	if serr != nil {
		t.Fatalf("assembling the provisioning seal of %q: %v", inc, serr)
	}
	_, cerr := e.asm.jsa.commitAuthorityTransition(seal)
	return cerr
}

// TestF105SealingAPayloadResourceIsAnswerable is contract 4's class at the RESOURCE boundary.
//
// The seal says nothing can materialize this resource any more, and a deletion authorization
// rests on it — but for replay material the store can refute that claim: the manifest is in this
// stream, at the address the resource names. A seal taken while it is still provisioning
// authorizes deleting a name that is not free, and the material it describes is then refused
// removal forever, because removal requires a manifest that reached CLOSED.
func TestF105SealingAPayloadResourceIsAnswerable(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-answerable")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 2)

	// STILL PROVISIONING: the material may still grow, and the seal says it cannot.
	err := testF105TrySeal(t, e, "inc-pay")
	if err == nil {
		t.Fatalf("a payload resource was sealed while its material was still provisioning")
	}
	if !strings.Contains(err.Error(), "still provisioning") {
		t.Fatalf("the refusal did not come from the material: %v", err)
	}

	// READY IS NOT ENOUGH SINCE F-105 7c: an admission may still take a READY copy, so the seal
	// would describe material a delivery is about to need. The copy must be CLOSED first.
	testF105SealManifest(t, e, 2)
	err = testF105TrySeal(t, e, "inc-pay")
	if err == nil {
		t.Fatalf("a payload resource was sealed over READY material an admission could still take")
	}
	if !strings.Contains(err.Error(), "not closed") {
		t.Fatalf("the refusal did not come from the material's closure: %v", err)
	}

	// THE POSITIVE CONTROL: once the copy is CLOSED, nothing further can land through the fence or
	// be admitted against it, and the resource's seal states a fact.
	testF105CloseManifest(t, e, 2)
	if err := testF105TrySeal(t, e, "inc-pay"); err != nil {
		t.Fatalf("a payload resource over closed material was refused: %v", err)
	}
}

// TestF105SealingAConsumerResourceIsTheCallersFact is the other arm.
//
// A consumer's physical provisioning settles in the JetStream API, not in this stream, so there
// is nothing durable to read it against and the caller is the only one who can state it.
// Refusing there would be refusing every seal this broker can actually take.
func TestF105SealingAConsumerResourceIsTheCallersFact(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-consumer")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantUnsealedResource(t, e, "inc-src", mqttResourceSourceConsumer)
	if err := testF105TrySeal(t, e, "inc-src"); err != nil {
		t.Fatalf("a consumer resource's seal was refused: %v", err)
	}
}

// TestF105SealedMaterialIsGoneMaterialOrNoMaterial covers the two cases where nothing stands at
// the address the resource names.
func TestF105SealedMaterialIsGoneMaterialOrNoMaterial(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-absent")
	defer done()
	e.bootstrap(t, "txn-1")

	// NOTHING WAS EVER MATERIALIZED. The seal is what keeps it that way — the fence below
	// refuses to reserve against a sealed owner — so there is nothing to wait for.
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)
	if err := testF105TrySeal(t, e, "inc-pay"); err != nil {
		t.Fatalf("a payload resource with no material was refused a seal: %v", err)
	}
	err := testF105SubmitMaterial(t, e, "txn-late-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)))
	if err == nil {
		t.Fatalf("material was reserved against an owner whose provisioning is sealed")
	}
	if !strings.Contains(err.Error(), "provisioning is sealed") {
		t.Fatalf("the refusal did not come from the seal: %v", err)
	}
}

// TestF105MaterialAndItsOwnerNameEachOther is what makes the seal's read address the right
// record.
//
// A resource whose Name did not address its manifest could be sealed while material stood under
// it, because the boundary would be reading an address nothing occupies. So the agreement is
// required where material is created, in both directions and by kind.
func TestF105MaterialAndItsOwnerNameEachOther(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-material-names-owner")
	defer done()
	e.bootstrap(t, "txn-1")

	// A RESOURCE OF ANOTHER KIND owns no replay material.
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceSourceConsumer)
	err := testF105SubmitMaterial(t, e, "txn-wrong-kind",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)))
	if err == nil {
		t.Fatalf("material was reserved against a consumer resource")
	}
	if !strings.Contains(err.Error(), "owned by the payload resource") {
		t.Fatalf("the refusal did not come from the kind: %v", err)
	}

	// AND A PAYLOAD RESOURCE PROVISIONED FOR OTHER MATERIAL owns none of this.
	e2, done2 := testF105NewEnv(t, "f105-material-names-owner-2")
	defer done2()
	e2.bootstrap(t, "txn-1")
	r, _ := testF105PlantUnsealedResource(t, e2, "inc-pay", mqttResourceRetainedPayload)
	if r.Name != testF105Payload {
		t.Fatalf("the fixture's payload resource names %q", r.Name)
	}
	testF105RenameResource(t, e2, "inc-pay", "pay-other")
	err = testF105SubmitMaterial(t, e2, "txn-wrong-name",
		testF105ManifestMember(t, e2, 0, testF105Manifest(e2, mqttManifestProvisioning, 1, 0)))
	if err == nil {
		t.Fatalf("material was reserved against a resource provisioned for other material")
	}
	if !strings.Contains(err.Error(), "material is filed under another name") &&
		!strings.Contains(err.Error(), "whose material is") {
		t.Fatalf("the refusal did not come from the name: %v", err)
	}
}

// testF105RenameResource rewrites a planted resource's Name directly, which is the one thing no
// transition may do — a retirement is about the incarnation it was computed for.
func testF105RenameResource(t *testing.T, e *testF105Env, inc, name string) {
	t.Helper()
	r, seq := testF105LoadResource(t, e, inc)
	r.Name = name
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshalling resource %q: %v", inc, err)
	}
	msg, hdr := mqttFrameBatchWrite(seq, b, _EMPTY_, 0, false)
	if _, serr := e.asm.jsa.storeMsg(mqttResourceSubject(getHash(e.id), testF105Gen, inc),
		hdr, msg); serr != nil {
		t.Fatalf("renaming resource %q: %v", inc, serr)
	}
}

// TestF105ASealReadsItsOwnMaterialOrRefuses is the identity half of the seal's read.
//
// The address a payload resource names can be occupied by a LATER materialization. Reading that
// record as this resource's own is the class this lane keeps finding, so the boundary refuses
// rather than interprets — the protocol order never produces it, and a refusal costs a lingering
// record where a guess would cost a seal taken over material nobody looked at.
func TestF105ASealReadsItsOwnMaterialOrRefuses(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-foreign")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 1)
	testF105SealManifest(t, e, 1)

	// A manifest standing at this resource's name, belonging to another materialization.
	m, seq := testF105LoadManifest(t, e)
	m.Inc = "inc-pay-2"
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshalling the replacement's manifest: %v", err)
	}
	msg, hdr := mqttFrameBatchWrite(seq, body, _EMPTY_, 0, false)
	if _, serr := e.asm.jsa.storeMsg(mqttManifestSubject(getHash(e.id), testF105Gen,
		testF105Payload), hdr, msg); serr != nil {
		t.Fatalf("planting the replacement's manifest: %v", serr)
	}

	serr := testF105TrySeal(t, e, "inc-pay")
	if serr == nil {
		t.Fatalf("a seal was taken over another materialization's record")
	}
	if !strings.Contains(serr.Error(), "belongs to materialization") {
		t.Fatalf("the refusal did not come from the identity: %v", serr)
	}
}

// testF105ResourceMember carries one resource record as a batch member.
func testF105ResourceMember(t *testing.T, e *testF105Env, expect uint64,
	r *mqttOutboundResource) mqttTransitionMember {
	t.Helper()
	body, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshalling resource %q: %v", r.Inc, err)
	}
	return mqttTransitionMember{
		subj: mqttResourceSubject(getHash(e.id), testF105Gen, r.Inc), expect: expect, body: body,
	}
}

// TestF105OnlyAMaterializationReservesMaterial is round I-7's finding, fixed where consult 8
// ruled it belongs: at what JUSTIFIED the operation rather than at the coincidence.
//
// The bypass was a dispose-resource batch carrying the payload resource's SEAL beside a
// create-only manifest. Both halves of the seal were checked against the STORE — no manifest
// standing, an owner not yet sealed — so both passed. The seal was only the visible half. The
// invisible one is that a DISPOSAL was reserving material at all: the chunk loop has always
// refused a non-materializing kind, and the branch that brings a payload into existence never
// got the same rule.
func TestF105OnlyAMaterializationReservesMaterial(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-reserve-kind")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)

	// A BARE RESERVATION UNDER A DISPOSAL, with nothing else riding on it. This is the arm that
	// says the rule is about the KIND and not about the seal that happened to be beside it.
	err := testF105SubmitInventory(t, e, "txn-dispose-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)))
	if err == nil {
		t.Fatalf("a disposal reserved material")
	}
	if !strings.Contains(err.Error(), "does not materialize") {
		t.Fatalf("the refusal did not come from the kind: %v", err)
	}

	// THE POSITIVE CONTROL for the first permitted kind.
	if err := testF105SubmitMaterial(t, e, "txn-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)),
	); err != nil {
		t.Fatalf("an ordinary reservation was refused: %v", err)
	}
}

// TestF105ADisposalCannotSealAnOwnerAndReserveItsMaterial is round I-7's batch exactly, kept as
// its own test so a mutation of the kind rule shows THIS arm firing.
//
// Split from the bare-reservation case deliberately: with both in one test the first Fatalf
// hides the second, and what a mutation proves is then unmeasurable
// (feedback_split_the_mutation_kills).
func TestF105ADisposalCannotSealAnOwnerAndReserveItsMaterial(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-same-batch")
	defer done()
	e.bootstrap(t, "txn-1")
	r, rseq := testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)

	sealed := *r
	sealed.Prov = mqttProvisioningSealed
	err := testF105SubmitInventory(t, e, "txn-seal-and-reserve",
		testF105ResourceMember(t, e, rseq, &sealed),
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)),
	)
	if err == nil {
		t.Fatalf("one batch sealed an owner and reserved material for it")
	}
	if !strings.Contains(err.Error(), "does not materialize") {
		t.Fatalf("the refusal did not come from the kind: %v", err)
	}
}

// TestF105AProvisioningMayReserveTheMaterialItOwns is the second permitted kind, and the shape
// checkManifestOwnerExists documents: the owner may be CREATED by the batch that reserves the
// material, with both predeclared duties.
func TestF105AProvisioningMayReserveTheMaterialItOwns(t *testing.T) {
	provision := func(t *testing.T, env *testF105Env, txn string, prov mqttProvisioningState) error {
		t.Helper()
		owner := &mqttOutboundResource{
			ID: env.id, Epoch: env.epoch, Gen: testF105Gen, Inc: "inc-pay",
			Kind: mqttResourceRetainedPayload, Name: testF105Payload, State: mqttResourceLive,
			Prov: prov, Deps: []mqttResourceEdge{},
		}
		return testF105SubmitInventoryAs(t, env, mqttTransitionProvisionResource, txn,
			testF105ResourceMember(t, env, 0, owner),
			testF105ManifestMember(t, env, 0, testF105Manifest(env, mqttManifestProvisioning, 1, 0)),
			testF105LedgerMember(t, env, 0, testF105Ledger(env, mqttGenLedgerOpen, 2)),
			testF105DutyMember(t, env, 0, testF105Duty(env, 1, mqttDutyProvision, "inc-pay",
				mqttDutyPending)),
			testF105DutyMember(t, env, 0, testF105Duty(env, 2, mqttDutyCleanup, "inc-pay",
				mqttDutyNotYetActionable)),
		)
	}

	e, done := testF105NewEnv(t, "f105-provision-reserve")
	defer done()
	e.bootstrap(t, "txn-1")
	if err := provision(t, e, "txn-own-and-reserve", mqttProvisioningPending); err != nil {
		t.Fatalf("an owner created beside its own material was refused: %v", err)
	}

	// AND CREATING IT ALREADY SEALED is refused — by the create-only owner read, which has
	// always been there and is what makes the two cross-reads consult 8 deleted unnecessary.
	e2, done2 := testF105NewEnv(t, "f105-provision-reserve-2")
	defer done2()
	e2.bootstrap(t, "txn-1")
	err := provision(t, e2, "txn-own-sealed-and-reserve", mqttProvisioningSealed)
	if err == nil {
		t.Fatalf("material was reserved against an owner created already sealed")
	}
	if !strings.Contains(err.Error(), "provisioning is sealed") {
		t.Fatalf("the refusal did not come from the seal: %v", err)
	}
}

// TestF105AClosureIsNotItsOwnSealsEvidence is consult 8's second required regression.
//
// A seal must rest on a cessation of materialization that is ALREADY ESTABLISHED. One act that
// closes the manifest AND seals the resource consumes its own closure as the seal's evidence —
// a second fact riding on a seal, which "one act, one fact" already refuses for dependency
// edges. The seal therefore reads the STORED manifest and not the one this batch is writing.
func TestF105AClosureIsNotItsOwnSealsEvidence(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-close-and-seal")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105Materialize(t, e, 1)

	// ONE ACT, BOTH FACTS: refused.
	r, rseq := testF105LoadResource(t, e, "inc-pay")
	sealed := *r
	sealed.Prov = mqttProvisioningSealed
	_, mseq := testF105LoadManifest(t, e)
	err := testF105SubmitInventory(t, e, "txn-close-and-seal",
		testF105ResourceMember(t, e, rseq, &sealed),
		testF105ManifestMember(t, e, mseq, testF105Manifest(e, mqttManifestClosed, 1, 1)),
	)
	if err == nil {
		t.Fatalf("one act closed a manifest and sealed its owner on the strength of it")
	}
	if !strings.Contains(err.Error(), "still provisioning") {
		t.Fatalf("the refusal did not come from the stored material: %v", err)
	}

	// TWO ACTS, IN ORDER: accepted. A materialization that crashed mid-write is closable —
	// PROVISIONING to CLOSED is permitted — so this is a sequence the executor can always run.
	testF105CloseManifest(t, e, 1)
	if serr := testF105TrySeal(t, e, "inc-pay"); serr != nil {
		t.Fatalf("a seal over closed material was refused: %v", serr)
	}
}
