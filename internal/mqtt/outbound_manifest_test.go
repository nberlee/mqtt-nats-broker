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
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

const testF105Payload = "pay-1"

// testF105ChunkData is the eight bytes every fixture chunk carries.
//
// Fixed so the manifest's digest can be the REAL digest of the material the fixture will write.
// It used to be a placeholder string, which was harmless while nothing compared it with
// anything; a seal now binds to a verification that actually read the chunks, and a fixture
// whose claimed digest could never be produced would make that binding untestable.
var testF105ChunkData = []byte("12345678")

func testF105MaterialOf(chunks uint64) []byte {
	return bytes.Repeat(testF105ChunkData, int(chunks))
}

func testF105Manifest(e *testF105Env, state mqttManifestState, chunks, written uint64) *mqttRetainedManifest {
	return &mqttRetainedManifest{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload, State: state,
		Topic: "a/b", QoS: 1, Bytes: chunks * 8, Chunks: chunks, Written: written,
		Digest: mqttMaterialDigest(testF105MaterialOf(chunks)), Inc: "inc-pay",
	}
}

// testF105SubmitMaterial submits a batch under the kind that WRITES material.
//
// Material lands only under a serving authority, so a fixture that submitted a chunk as a
// disposal would be refused by the kind rule and never reach the fence it meant to test.
func testF105SubmitMaterial(t *testing.T, e *testF105Env, txn string,
	members ...mqttTransitionMember) error {
	t.Helper()
	return testF105SubmitInventoryAs(t, e, mqttTransitionMaterializePayload, txn, members...)
}

func testF105ManifestMember(t *testing.T, e *testF105Env, expect uint64, m *mqttRetainedManifest) mqttTransitionMember {
	t.Helper()
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshalling the manifest: %v", err)
	}
	return mqttTransitionMember{
		subj:   mqttManifestSubject(getHash(e.id), testF105Gen, testF105Payload),
		expect: expect, body: body,
	}
}

func testF105ChunkMember(t *testing.T, e *testF105Env, ord uint64, expect uint64) mqttTransitionMember {
	t.Helper()
	body, err := json.Marshal(&mqttRetainedChunk{Ord: ord, Inc: "inc-pay", Data: testF105ChunkData})
	if err != nil {
		t.Fatalf("marshalling the chunk: %v", err)
	}
	return mqttTransitionMember{
		subj:   mqttChunkSubject(getHash(e.id), testF105Gen, testF105Payload, ord),
		expect: expect, body: body,
	}
}

func testF105LoadManifest(t *testing.T, e *testF105Env) (*mqttRetainedManifest, uint64) {
	t.Helper()
	subj := mqttManifestSubject(getHash(e.id), testF105Gen, testF105Payload)
	smsg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj)
	if err != nil || smsg == nil {
		t.Fatalf("reading the manifest: %v", err)
	}
	m, derr := mqttDecodeRetainedManifest(smsg.Data, e.id, e.epoch, testF105Gen, testF105Payload)
	if derr != nil {
		t.Fatalf("decoding the manifest: %v", derr)
	}
	return m, smsg.Sequence
}

// TestF105AChunkLandsOnlyThroughItsManifestFence is D4's central rule.
//
// "The chunk fence is an atomic manifest-state CAS together with the chunk creation, in one
// batch. A manifest read followed by an independent chunk publish does NOT implement it." The
// difference is a window: between the read and the publish, the manifest can be sealed READY or
// closed for cleanup, and the chunk then lands against a payload that was already declared whole
// or already being deleted.
func TestF105AChunkLandsOnlyThroughItsManifestFence(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-chunk-fence")
	defer done()
	e.bootstrap(t, "txn-1")
	// THE CLEANUP OWNER MUST EXIST. Naming one does not reserve one: material whose owner never
	// existed is material no duty sweep will discover and nothing will reclaim.
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)

	// RESERVED FIRST, with nothing written and its cleanup owner already named — so a crash
	// mid-materialization leaves material a retirement worker can find and own.
	if err := testF105SubmitMaterial(t, e, "txn-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 2, 0)),
	); err != nil {
		t.Fatalf("reserving the materialization was refused: %v", err)
	}

	// A CHUNK WITH NO MANIFEST MEMBER IS REFUSED, and the absence is the violation rather than
	// an exemption from a rule conditioned on one.
	if err := testF105SubmitMaterial(t, e, "txn-bare",
		testF105ChunkMember(t, e, 1, 0)); err == nil {
		t.Fatalf("a chunk landed with no manifest member")
	} else if !strings.Contains(err.Error(), "no manifest member") {
		t.Fatalf("the refusal did not come from the fence: %v", err)
	}

	// THE POSITIVE CONTROL: the chunk beside the manifest CAS that counts it.
	_, seq := testF105LoadManifest(t, e)
	if err := testF105SubmitMaterial(t, e, "txn-chunk-1",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 2, 1)),
		testF105ChunkMember(t, e, 1, 0),
	); err != nil {
		t.Fatalf("a fenced chunk was refused: %v", err)
	}

	// THE COUNT IS THE TRACE. A manifest stepping its own count without carrying the chunk
	// would seal READY over material nobody wrote.
	_, seq = testF105LoadManifest(t, e)
	if err := testF105SubmitMaterial(t, e, "txn-lie",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 2, 2)),
	); err == nil {
		t.Fatalf("the written count rose with no chunk in the batch")
	}

	// A CHUNK IS IMMUTABLE, and the expectation used here is the chunk's ACTUAL standing
	// sequence — which is the shape that matters.
	//
	// A wrong sequence proves nothing: the store's own CAS refuses it, so the validator is
	// never reached. The first version of this fixture did exactly that and a mutation of the
	// rule survived it. What the validator adds over the store is that NO expectation is
	// acceptable on a chunk at all: a rewrite at the correct sequence is a well-formed CAS the
	// store would happily commit, replacing bytes an obligation may already be replaying.
	chunkSubj := mqttChunkSubject(getHash(e.id), testF105Gen, testF105Payload, 1)
	planted, cerr := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, chunkSubj)
	if cerr != nil || planted == nil {
		t.Fatalf("reading the chunk back: %v", cerr)
	}
	if err := testF105SubmitMaterial(t, e, "txn-rewrite",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 2, 2)),
		testF105ChunkMember(t, e, 1, planted.Sequence),
	); err == nil {
		t.Fatalf("a chunk was rewritten at its own standing sequence")
	} else if !strings.Contains(err.Error(), "replay material is immutable") {
		t.Fatalf("the refusal did not come from the immutability rule: %v", err)
	}

	// SEALING REQUIRES EVERY CHUNK. One of two written is not a whole payload.
	if err := testF105SubmitMaterial(t, e, "txn-early-ready",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestReady, 2, 1)),
	); err == nil {
		t.Fatalf("a manifest sealed READY with one chunk of two written")
	}

	// The second chunk, then the seal.
	if err := testF105SubmitMaterial(t, e, "txn-chunk-2",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestProvisioning, 2, 2)),
		testF105ChunkMember(t, e, 2, 0),
	); err != nil {
		t.Fatalf("the second fenced chunk was refused: %v", err)
	}
	_, seq = testF105LoadManifest(t, e)
	if err := testF105SubmitMaterial(t, e, "txn-ready",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestReady, 2, 2)),
	); err != nil {
		t.Fatalf("sealing a whole payload was refused: %v", err)
	}

	// AND MATERIAL NO LONGER LANDS. A chunk beside a READY manifest is material arriving after
	// the payload was declared whole.
	_, seq = testF105LoadManifest(t, e)
	if err := testF105SubmitMaterial(t, e, "txn-late-chunk",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestReady, 2, 3)),
		testF105ChunkMember(t, e, 3, 0),
	); err == nil {
		t.Fatalf("a chunk landed against a sealed payload")
	}
}

// TestF105AClosedManifestNeverReopens is why cleanup seals BEFORE it deletes.
//
// Closing precedes removing anything, so a delayed chunk writer arriving afterwards finds a
// closed manifest and its batch fails. If closing could be undone, that writer would recreate
// material the cleanup had already deleted — which is the window the ordering exists to close.
func TestF105AClosedManifestNeverReopens(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-manifest-closed")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantUnsealedResource(t, e, "inc-pay", mqttResourceRetainedPayload)

	if err := testF105SubmitMaterial(t, e, "txn-reserve",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)),
	); err != nil {
		t.Fatalf("reserving: %v", err)
	}
	_, seq := testF105LoadManifest(t, e)
	// ABANDONED mid-provisioning, which is one of the two ways CLOSED is reached.
	if err := testF105SubmitInventory(t, e, "txn-close",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestClosed, 1, 0)),
	); err != nil {
		t.Fatalf("closing an abandoned materialization was refused: %v", err)
	}

	m, seq := testF105LoadManifest(t, e)
	if mqttManifestAdmits(m) {
		t.Fatalf("a closed manifest admits obligations")
	}
	for name, state := range map[string]mqttManifestState{
		"back to provisioning": mqttManifestProvisioning,
		"forward to ready":     mqttManifestReady,
	} {
		if err := testF105SubmitInventory(t, e, "txn-reopen-"+mqttNewInflightNonce(),
			testF105ManifestMember(t, e, seq, testF105Manifest(e, state, 1, 0)),
		); err == nil {
			t.Fatalf("a closed manifest was reopened %s", name)
		}
	}
	// AND NO CHUNK LANDS AGAINST IT, which is the delayed writer the ordering is about.
	if err := testF105SubmitMaterial(t, e, "txn-late",
		testF105ManifestMember(t, e, seq, testF105Manifest(e, mqttManifestClosed, 1, 1)),
		testF105ChunkMember(t, e, 1, 0),
	); err == nil {
		t.Fatalf("a delayed writer recreated material against a closed manifest")
	}
}

// TestF105ManifestBodiesAreRefusedWhenUnsupported pins the record shape.
func TestF105ManifestBodiesAreRefusedWhenUnsupported(t *testing.T) {
	e := &testF105Env{id: testF105ID, epoch: testF105Epoch}
	for name, tc := range map[string]struct {
		m  *mqttRetainedManifest
		ok bool
	}{
		"a provisioning manifest": {testF105Manifest(e, mqttManifestProvisioning, 2, 1), true},
		"a ready manifest":        {testF105Manifest(e, mqttManifestReady, 2, 2), true},
		"an empty payload": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestReady, Topic: "a/b", QoS: 2, Bytes: 0, Chunks: 0, Written: 0,
			Digest: "d", Inc: "inc-pay"}, true},

		// READY WITH MATERIAL MISSING would admit an obligation against a payload that is not
		// all there.
		"ready with a chunk missing": {testF105Manifest(e, mqttManifestReady, 2, 1), false},
		// MORE WRITTEN THAN EXIST is material the manifest cannot account for.
		"more written than chunks": {testF105Manifest(e, mqttManifestProvisioning, 2, 3), false},
		// BYTES WITHOUT CHUNKS claims material nothing stores.
		"bytes with no chunks": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestProvisioning, Topic: "a/b", QoS: 1, Bytes: 10, Chunks: 0,
			Digest: "d", Inc: "inc-pay"}, false},
		// RETAINED QoS 0 HAS NO OBLIGATION to replay material for.
		"qos 0": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestProvisioning, Topic: "a/b", QoS: 0, Bytes: 8, Chunks: 1,
			Digest: "d", Inc: "inc-pay"}, false},
		"no digest": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestProvisioning, Topic: "a/b", QoS: 1, Bytes: 8, Chunks: 1,
			Inc: "inc-pay"}, false},
		// A LAYOUT NO CHUNKS COULD CARRY. Without this a manifest can claim any number of
		// bytes, and a reader that sizes a buffer from the claim allocates whatever the record
		// said — a description is not material.
		"more bytes than its chunks can hold": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestProvisioning, Topic: "a/b", QoS: 1, Bytes: mqttChunkMaxBytes + 1,
			Chunks: 1, Digest: "d", Inc: "inc-pay"}, false},
		"more chunks than bytes": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestProvisioning, Topic: "a/b", QoS: 1, Bytes: 2, Chunks: 3,
			Digest: "d", Inc: "inc-pay"}, false},
		// REMOVAL BEGINS AFTER CLOSURE, so a record that is not closed and claims one
		// describes a state this broker never writes.
		"material removed while still provisioning": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestProvisioning, Topic: "a/b", QoS: 1, Bytes: 16, Chunks: 2,
			Written: 2, Announced: 1, Purged: 1, Digest: "d", Inc: "inc-pay"}, false},
		"more removed than the payload has": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestClosed, Topic: "a/b", QoS: 1, Bytes: 16, Chunks: 2,
			Written: 2, Announced: 3, Purged: 3, Digest: "d", Inc: "inc-pay"}, false},
		"a closed manifest part-way through its removal": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestClosed, Topic: "a/b", QoS: 1, Bytes: 16, Chunks: 2,
			Written: 2, Announced: 2, Purged: 1, Digest: "d", Inc: "inc-pay"}, true},
		// A CONFIRMATION ANSWERS AN ANNOUNCEMENT, so one that outran it reports chunks removed
		// that nothing had said it was removing.
		"more confirmed than announced": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestClosed, Topic: "a/b", QoS: 1, Bytes: 16, Chunks: 2,
			Written: 2, Announced: 1, Purged: 2, Digest: "d", Inc: "inc-pay"}, false},
		"no cleanup owner": {&mqttRetainedManifest{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Payload: testF105Payload,
			State: mqttManifestProvisioning, Topic: "a/b", QoS: 1, Bytes: 8, Chunks: 1,
			Digest: "d"}, false},
	} {
		err := mqttValidateRetainedManifest(tc.m, e.id, e.epoch, testF105Gen, testF105Payload)
		if tc.ok && err != nil {
			t.Fatalf("%s was refused: %v", name, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}

	// AND A CHUNK'S OWN SHAPE. The size bound is what keeps a payload from becoming an
	// unbounded atomic batch.
	if err := mqttValidateRetainedChunk(&mqttRetainedChunk{Ord: 1, Inc: "inc-pay",
		Data: []byte("x")}, 1, "inc-pay"); err != nil {
		t.Fatalf("an ordinary chunk was refused: %v", err)
	}
	for name, c := range map[string]*mqttRetainedChunk{
		"an empty chunk":     {Ord: 1, Inc: "inc-pay"},
		"an oversized chunk": {Ord: 1, Inc: "inc-pay", Data: make([]byte, mqttChunkMaxBytes+1)},
		"ordinal zero":       {Ord: 0, Inc: "inc-pay", Data: []byte("x")},
		// A CHUNK THAT DOES NOT SAY WHICH MATERIALIZATION IT BELONGS TO is a record a cleanup
		// cannot identify, and a cleanup does not remove what it cannot identify.
		"no materialization": {Ord: 1, Data: []byte("x")},
		// AND ONE OF ANOTHER MATERIALIZATION at the same address is not this payload's.
		"another materialization": {Ord: 1, Inc: "inc-other", Data: []byte("x")},
	} {
		if err := mqttValidateRetainedChunk(c, c.Ord, "inc-pay"); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

// TestF105ReservedMaterialMustHaveAnExistingOwner is round I-3's first finding, and it is the
// representation-as-enforcement gap in its purest form.
//
// The manifest names its cleanup owner by incarnation. Before this rule, ANY syntactically valid
// token was accepted — so a crash after reservation left durable payload material whose owner
// does not exist. The duty sweep never discovers it, because discovery goes through the owner's
// predeclared entries; and if the generation has neither a ledger nor a closure record, the
// metadata-release gate treats it as vacuous too. Material with no owner and no enrolment is
// material nothing will ever reclaim.
//
// Naming an owner does not reserve one.
func TestF105ReservedMaterialMustHaveAnExistingOwner(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-manifest-owner")
	defer done()
	e.bootstrap(t, "txn-1")

	// NO OWNER AT ALL.
	err := testF105SubmitMaterial(t, e, "txn-orphan",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)))
	if err == nil {
		t.Fatalf("material was reserved against a cleanup owner that does not exist")
	}
	if !strings.Contains(err.Error(), "naming an owner does not reserve one") {
		t.Fatalf("the refusal did not come from the owner rule: %v", err)
	}

	// AN OWNER WHOSE DISPOSAL IS ALREADY AUTHORIZED takes on no new material either: attaching
	// it would attach material to a cleanup that has already been decided.
	// Sealed, because an authorization is refused over provisioning that is still in flight —
	// and with no material standing, a seal is exactly what says none will arrive.
	testF105PlantResource(t, e, "inc-pay", mqttResourceRetainedPayload)
	if _, aerr := e.asm.jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-auth",
		testF105Gen, "inc-pay"); aerr != nil {
		t.Fatalf("authorizing the owner's deletion: %v", aerr)
	}
	if err := testF105SubmitMaterial(t, e, "txn-late",
		testF105ManifestMember(t, e, 0, testF105Manifest(e, mqttManifestProvisioning, 1, 0)),
	); err == nil {
		t.Fatalf("material was reserved against an owner whose disposal was authorized")
	}

	// THE POSITIVE CONTROL: a live owner, on a fresh generation so the one above stays disposed.
	e2, done2 := testF105NewEnv(t, "f105-manifest-owner-ok")
	defer done2()
	e2.bootstrap(t, "txn-1")
	testF105PlantUnsealedResource(t, e2, "inc-pay", mqttResourceRetainedPayload)
	if err := testF105SubmitMaterial(t, e2, "txn-ok",
		testF105ManifestMember(t, e2, 0, testF105Manifest(e2, mqttManifestProvisioning, 1, 0)),
	); err != nil {
		t.Fatalf("material was refused against a live owner: %v", err)
	}
}

// TestF105ReadingAChunkNamesTheMaterializationOrSaysItIsNotChecking keeps the two chunk reads
// apart.
//
// One decoder is for a caller that knows which materialization the chunk must belong to; the
// other is for the two cleanup callers whose question is "whatever stands here". A single
// decoder taking a blank identity to mean "do not check" makes the permissive read an accident
// of an empty argument — the same shape this lane refuses everywhere else.
func TestF105ReadingAChunkNamesTheMaterializationOrSaysItIsNotChecking(t *testing.T) {
	body, err := json.Marshal(&mqttRetainedChunk{Ord: 1, Inc: "inc-pay", Data: testF105ChunkData})
	if err != nil {
		t.Fatalf("marshalling a chunk: %v", err)
	}

	// THE CHECKING DECODER REFUSES A BLANK IDENTITY rather than treating it as a licence.
	if _, derr := mqttDecodeRetainedChunk(body, 1, _EMPTY_); derr == nil {
		t.Fatalf("a blank materialization was accepted as permission not to check")
	}
	// AND IT REFUSES A CHUNK OF ANOTHER MATERIALIZATION.
	if _, derr := mqttDecodeRetainedChunk(body, 1, "inc-pay-2"); derr == nil {
		t.Fatalf("a chunk of another materialization was accepted")
	}
	// THE ONE IT NAMES IS TAKEN.
	if c, derr := mqttDecodeRetainedChunk(body, 1, "inc-pay"); derr != nil {
		t.Fatalf("the chunk it names: %v", derr)
	} else if c.Inc != "inc-pay" {
		t.Fatalf("decoded a chunk of %q", c.Inc)
	}

	// THE DISCOVERY DECODER TAKES WHATEVER STANDS THERE, and still checks everything that is
	// not identity: a wrong ordinal is a wrong record whoever wrote it.
	if c, derr := mqttDiscoverRetainedChunk(body, 1); derr != nil {
		t.Fatalf("discovering the chunk: %v", derr)
	} else if c.Inc != "inc-pay" {
		t.Fatalf("discovered a chunk of %q", c.Inc)
	}
	if _, derr := mqttDiscoverRetainedChunk(body, 2); derr == nil {
		t.Fatalf("a chunk at another ordinal was discovered as chunk 2")
	}
}
