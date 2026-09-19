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

func testF105ReleaseFacts() mqttAdoptedReleaseFacts {
	return mqttAdoptedReleaseFacts{
		Closed:       &mqttGenClosure{},
		Ledger:       testF105SealedLedger(10),
		RootEnrolled: true,
		// POSITIVELY ESTABLISHED, both of them. The zero value of each is UNKNOWN and blocks,
		// so a fixture that left them out would be testing the unknown rule in every case.
		Discard:           mqttFactDoesNotApply,
		TargetsEnumerated: mqttFactApplies,
	}
}

// TestF105MetadataReleaseWaitsForTheRightThings is question 5, and the emphasis is on RIGHT.
//
// finishAdoptedTeardown currently releases metadata only after deleteTeardownTargets returns, so
// a CONNECT behind it waits on physical deletion. The design's coupling is the opposite: release
// depends on durable FACTS about the old generation, and deletion becomes a duty authorized
// later by the resource CAS. The point was never "stop waiting" — it is "wait for the right
// thing".
func TestF105MetadataReleaseWaitsForTheRightThings(t *testing.T) {
	const id, gen = "client-a", testF105Gen

	// THE POSITIVE CONTROL, first: all four facts, release permitted. Without it every refusal
	// below would be satisfied by a gate that refused everything.
	if err := mqttAdoptedReleaseAuthorized(id, gen, testF105ReleaseFacts()); err != nil {
		t.Fatalf("a generation with every fact established was refused: %v", err)
	}

	for name, tc := range map[string]struct {
		mutate func(*mqttAdoptedReleaseFacts)
		want   string
	}{
		// WITHOUT COMMITTED CLOSURE a delayed admission can still add a duty to a generation
		// the metadata has already called finished.
		"no closure record": {
			func(f *mqttAdoptedReleaseFacts) { f.Closed = nil },
			"committed closure",
		},
		// AN OPEN LEDGER has no bound, so there is nothing for a coverage proof to be about.
		"an open inventory": {
			func(f *mqttAdoptedReleaseFacts) {
				f.Ledger = &mqttGenLedger{
					ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen,
					State: mqttGenLedgerOpen, Hi: 10,
				}
			},
			"sealed inventory",
		},
		"a closure with no ledger": {
			func(f *mqttAdoptedReleaseFacts) { f.Ledger = nil },
			"sealed inventory",
		},
		// AN UNKNOWN DISPOSITION IS NOT A DISPOSITION. This is the permissive default a review
		// required be removed: a caller that cannot say used to pass false, and the gate read
		// "nobody said" as "no".
		"an unknown discard disposition": {
			func(f *mqttAdoptedReleaseFacts) { f.Discard = mqttFactUnknown },
			"discard disposition",
		},
		// AND AN EMPTY LIST NOBODY BUILT establishes nothing about what this teardown holds.
		"an unenumerated target set": {
			func(f *mqttAdoptedReleaseFacts) { f.TargetsEnumerated = mqttFactUnknown },
			"enrolled shared and affine cleanup",
		},
		// NEITHER ENROLLED NOR CERTIFIED means the surviving duties are abandoned: no broker
		// will ever come back for them, and releasing here is what makes that permanent.
		"no recovery ownership": {
			func(f *mqttAdoptedReleaseFacts) { f.RootEnrolled = false },
			"recovery ownership",
		},
		// TARGETS NAMED ONLY BY THE TRANSIENT TEARDOWN OBJECT are lost outright by a crash
		// after the release, because nothing durable names them.
		"unenrolled shared durables": {
			func(f *mqttAdoptedReleaseFacts) {
				f.UnenrolledTargets = []string{"$MQTT_shared_grp"}
			},
			"enrolled shared and affine cleanup",
		},
	} {
		f := testF105ReleaseFacts()
		tc.mutate(&f)
		err := mqttAdoptedReleaseAuthorized(id, gen, f)
		if err == nil {
			t.Fatalf("%s did not block the release", name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s was blocked for the wrong reason: %v", name, err)
		}
	}

	// A DISCARDING TEARDOWN IS NOT A BLOCKED ONE. What the gate needs is a DISPOSITION, either
	// way, and a generation whose closure recorded a discard has one.
	f := testF105ReleaseFacts()
	f.Discard = mqttFactApplies
	if err := mqttAdoptedReleaseAuthorized(id, gen, f); err != nil {
		t.Fatalf("an established discard disposition did not satisfy its condition: %v", err)
	}
}

// TestF105CompletionCertificateSatisfiesRecoveryOwnership is the deadlock consult 5 named.
//
// Requiring an OPEN cursor unconditionally would block the release forever whenever recovery
// finished first and retired the root. So a durable certificate satisfies condition 3 too — and
// because it must remain acceptable after the root and every entry are gone, it certifies the
// exact identity and bound rather than pointing at records that no longer exist.
func TestF105CompletionCertificateSatisfiesRecoveryOwnership(t *testing.T) {
	const id, gen = "client-a", testF105Gen

	f := testF105ReleaseFacts()
	f.RootEnrolled = false
	f.Cert = &mqttGenCompletionCertificate{
		ID: id, Epoch: testF105Epoch, Gen: gen, SealedHi: 10,
	}
	if err := mqttAdoptedReleaseAuthorized(id, gen, f); err != nil {
		t.Fatalf("a completion certificate did not satisfy recovery ownership: %v", err)
	}

	// A CERTIFICATE ABOUT A DIFFERENT BOUND covers a different inventory. Accepting it would
	// release the generation on the strength of a sweep that finished before the duties now
	// standing were ever allocated — which is the whole reason the bound is IN the certificate.
	f.Cert = &mqttGenCompletionCertificate{
		ID: id, Epoch: testF105Epoch, Gen: gen, SealedHi: 4,
	}
	if err := mqttAdoptedReleaseAuthorized(id, gen, f); err == nil {
		t.Fatalf("a certificate covering bound 4 released a generation sealed at 10")
	}

	// A ZERO BOUND IS LEGAL, and it has to be: a generation that admitted nothing seals its
	// ledger at 0, so its completion covers 0. What used to be protected by refusing zero here is
	// protected in two better places now — `sealed_hi` is a REQUIRED, non-null member of the
	// body, so an omitted or null bound is refused on READ rather than decoding into this legal
	// value, and the gate compares the value against the SEALED bound of the ledger that stands.
	if err := mqttValidateGenCert(&mqttGenCompletionCertificate{
		ID: id, Epoch: testF105Epoch, Gen: gen,
	}, id, testF105Epoch, gen); err != nil {
		t.Fatalf("a certificate covering an empty inventory was refused: %v", err)
	}
	// AND THE GATE STILL REFUSES IT against a generation sealed higher, which is the fact the
	// bound is carried for.
	empty := f
	empty.Cert = &mqttGenCompletionCertificate{ID: id, Epoch: testF105Epoch, Gen: gen}
	if err := mqttAdoptedReleaseAuthorized(id, gen, empty); err == nil {
		t.Fatalf("a certificate covering bound 0 released a generation sealed at 10")
	}
	if err := mqttValidateGenCert(&mqttGenCompletionCertificate{
		ID: id, Epoch: testF105Epoch, Gen: gen, SealedHi: 10,
	}, id, testF105Epoch, gen); err != nil {
		t.Fatalf("a well-formed certificate was refused: %v", err)
	}
}

// TestF105ReleaseGateIsVacuousForAnUnconvertedGeneration is what lets this land on the
// production path before the routes that create inventory are converted.
//
// A generation with no ledger and no closure record never admitted an obligation through the
// authority. There is no inventory for these facts to be about and no duty a release could
// strand — so the gate reports nothing, exactly as checkDestructionAuthorized does for a
// generation with no resource records.
//
// This is NOT an exemption, and the difference matters: the moment such a generation acquires
// either record, every condition above applies to it.
func TestF105ReleaseGateIsVacuousForAnUnconvertedGeneration(t *testing.T) {
	const id, gen = "client-a", testF105Gen

	if b := mqttAdoptedReleaseBlockedBy(id, gen, mqttAdoptedReleaseFacts{}); len(b) != 0 {
		t.Fatalf("a generation that admitted nothing was blocked: %v", b)
	}
	// Even with a discard owed and shared durables outstanding: those are facts ABOUT an
	// inventory, and there is none.
	if b := mqttAdoptedReleaseBlockedBy(id, gen, mqttAdoptedReleaseFacts{
		Discard:           mqttFactApplies,
		UnenrolledTargets: []string{"$MQTT_shared_grp"},
	}); len(b) != 0 {
		t.Fatalf("a generation that admitted nothing was blocked: %v", b)
	}

	// AND THE UNKNOWN FACTS ARE VACUOUS TOO while there is no inventory, which is what lets
	// the gate stand on a production path today: the teardown call site can establish neither
	// the discard disposition nor the target enumeration, and both are UNKNOWN there.
	if b := mqttAdoptedReleaseBlockedBy(id, gen, mqttAdoptedReleaseFacts{}); len(b) != 0 {
		t.Fatalf("unknown facts blocked a generation that has no inventory: %v", b)
	}

	// THE MOMENT IT HAS AN INVENTORY the conditions apply — including the unknown ones, which
	// is the deadline a review set: these gaps close before an admission creates an inventory,
	// and after that the same teardown refuses until somebody answers them.
	if b := mqttAdoptedReleaseBlockedBy(id, gen, mqttAdoptedReleaseFacts{
		Closed: &mqttGenClosure{}, Ledger: testF105SealedLedger(3), RootEnrolled: true,
	}); len(b) != 2 {
		t.Fatalf("a generation with an inventory and two unknown facts reported %d block(s): %v",
			len(b), b)
	}
	// Without this row the vacuous case above would be satisfied by a gate that never blocked
	// anything.
	if b := mqttAdoptedReleaseBlockedBy(id, gen, mqttAdoptedReleaseFacts{
		Ledger: testF105SealedLedger(3),
	}); len(b) == 0 {
		t.Fatalf("a generation with an inventory and no closure was not blocked")
	}
}

// TestF105ReleaseFactsAreReadUnderTheAccountsRecoveryEpoch exercises the READS the wired gate
// makes, which is a different thing from the verdict the tests above pin.
//
// It exists because the first wiring passed the BROKER'S RUN ID where the account's recovery
// epoch belongs. Every F-105 record is stamped with the $MQTT_sess incarnation identity, and a
// record read under the wrong epoch is refused as unattributable — so that bug would have turned
// every fact into an error rather than an answer, and gatherAdoptedReleaseFacts would have failed
// the teardown instead of clearing it. Nothing caught it, because the gate is vacuous today
// whichever way the reads go.
func TestF105ReleaseFactsAreReadUnderTheAccountsRecoveryEpoch(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-release-facts")
	defer done()
	e.bootstrap(t, "txn-1")

	// NOTHING ADMITTED YET: every fact is absent, and the gate's vacuous path clears it.
	f, err := e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts for a generation that admitted nothing: %v", err)
	}
	if f.Ledger != nil || f.Cert != nil || f.RootEnrolled {
		t.Fatalf("a generation that admitted nothing reported facts: %+v", f)
	}
	// AND THE DISPOSITION IS UNKNOWN with no closure standing, which is the honest answer:
	// there is no record, so nobody has said. It is not the permissive one — the gate blocks
	// on it — and it is reached without a caller supplying anything.
	if f.Discard != mqttFactUnknown {
		t.Fatalf("a generation with no closure reported disposition %v", f.Discard)
	}
	if berr := mqttAdoptedReleaseAuthorized(e.id, testF105Gen, f); berr != nil {
		t.Fatalf("a generation that admitted nothing was blocked: %v", berr)
	}

	// GIVE IT AN INVENTORY. The ledger must now be READ BACK — under the account's own recovery
	// epoch — and the gate must block, because an inventory with no closure is a generation a
	// delayed admission can still add to.
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	f, err = e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts for a generation with an inventory: %v", err)
	}
	if f.Ledger == nil {
		t.Fatalf("the ledger was not read back under the account's recovery epoch: %+v", f)
	}
	if f.Ledger.Hi != 1 {
		t.Fatalf("the ledger read back with high-water %d, want 1", f.Ledger.Hi)
	}
	if berr := mqttAdoptedReleaseAuthorized(e.id, testF105Gen, f); berr == nil {
		t.Fatalf("a generation with an inventory and no closure was cleared for release")
	}

	// AND A ROOT IS FOUND AT THE ADDRESS ITS GENERATION'S CLOSURE RECORDS, which is how the gate
	// establishes recovery ownership without an enumeration that can come back short. The closure
	// is planted first, because without one there is no address to read and the gate never looks.
	testF105PlantClosureAt(t, e, testF105Gen, mqttDirShardFor(getHash(e.id)), 1, false)
	root := testF105EnvRoot(e, 1, 1, 1, 0)
	testF105PlantRoot(t, e, root)

	// A ROOT ALONE IS NOT ENROLMENT. Without a shard HEADER nothing starts a traversal over
	// this shard, so the root is a record at an address no recovery pass reaches — and
	// accepting it would clear a teardown whose duties nothing will ever resume. This is round
	// I-3's second finding, and the fixture that missed it planted exactly this shape.
	f, err = e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts with an unreachable root: %v", err)
	}
	if f.RootEnrolled {
		t.Fatalf("a root in a shard with no header was reported as enrolled: %+v", f)
	}

	// WITH THE HEADER, and its high-water covering the root's ordinal, it is reachable.
	testF105PlantShard(t, e, root.Shard, root.Ord)
	f, err = e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts with a root enrolled: %v", err)
	}
	if !f.RootEnrolled {
		t.Fatalf("an enrolled root with an unfinished sweep was not found: %+v", f)
	}

	// A FINISHED SWEEP IS STILL RECOVERY OWNERSHIP, and this is the correction the driver made
	// necessary. A root whose sweep has finished but whose generation has not retired — unresolved
	// work, or a bound below the sealed high-water — is enrolled and WILL be swept again when the
	// sweep rolls. Asking whether the sweep is UNFINISHED made one goroutine's liveness into the
	// gate's safety condition, and the last batch of every sweep persists exactly that state.
	finished := *root
	finished.Resume = finished.Bound + 1
	testF105PlantRootAt(t, e, &finished)
	f, err = e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts with a finished sweep: %v", err)
	}
	if !f.RootEnrolled {
		t.Fatalf("a finished but unretired root was not recovery ownership: %+v", f)
	}

	// AND A ROOT THAT NAMES ANOTHER IDENTITY IS NOT THIS GENERATION'S ENROLMENT. The address is
	// reachable and the sweep unfinished, so nothing else about this root is wrong — only whose
	// it is. A gate that skipped the identity check would clear a teardown on somebody else's
	// evidence, which is the one thing the epoch and the generation are carried for.
	foreign := *root
	foreign.Epoch = "epoch-that-is-not-this-accounts"
	testF105PlantRootAt(t, e, &foreign)
	f, err = e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts with a foreign root: %v", err)
	}
	if f.RootEnrolled {
		t.Fatalf("a root stamped with another recovery epoch was read as this one's: %+v", f)
	}
	testF105PlantRootAt(t, e, root)

	// AND AN ORDINAL PAST THE SHARD'S HIGH-WATER is outside every traversal too. The root is
	// readable, UNFINISHED and names this generation; what it is not is a position the header
	// ever allocated, so no pass walks to it.
	//
	// The sweep is restored to unfinished first, deliberately. A mutation of the high-water
	// check SURVIVED an earlier version of this block because the root was still finished from
	// the case above — so "not open" was already true for an unrelated reason and the
	// assertion could not discriminate.
	unfinished := *root
	unfinished.SweepStart, unfinished.Resume, unfinished.Bound, unfinished.Unresolved = 1, 1, 1, 0
	testF105PlantRootAt(t, e, &unfinished)
	testF105PlantShard(t, e, root.Shard, root.Ord)
	f, err = e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts with the sweep restored: %v", err)
	}
	if !f.RootEnrolled {
		t.Fatalf("the restored unfinished sweep was not reported open: %+v", f)
	}

	testF105PlantShard(t, e, root.Shard, 0)
	f, err = e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts with a root past the high-water: %v", err)
	}
	if f.RootEnrolled {
		t.Fatalf("a root past the shard's high-water was reported as enrolled: %+v", f)
	}
}

// TestF105TheDiscardDispositionIsReadFromTheClosure is the producer half of the gate's discard
// rule, and the reason there is no second "and its record stands" fact beside it.
//
// The disposition is a MANDATORY member of the closure record — a body without it is refused
// rather than defaulted — and the closure is written by the transition that closes the
// generation, at the moment the protocol event is true. A teardown re-deriving it later would be
// stating a claim about something it can no longer observe, which is the shape the whole gate
// exists to remove from its own inputs.
func TestF105TheDiscardDispositionIsReadFromTheClosure(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-disposition")
	defer done()
	e.bootstrap(t, "txn-1")

	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, err := e.asm.jsa.opCloseGeneration(e.id, e.epoch, "txn-close", al.rec.Gen,
		al.rec.Tenure, al.rec.OwnerTxn); err != nil {
		t.Fatalf("closing the generation: %v", err)
	}

	f, err := e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, al.rec.Gen, nil)
	if err != nil {
		t.Fatalf("gathering facts for a closed generation: %v", err)
	}
	if f.Closed == nil {
		t.Fatalf("the closure was not read back: %+v", f)
	}
	// ENDING A GENERATION IS NOT DISCARDING IT, and the disposition says so POSITIVELY rather
	// than by being absent.
	if f.Discard != mqttFactDoesNotApply {
		t.Fatalf("a non-discarding closure reported disposition %v", f.Discard)
	}

	// AND A DISCARDING CLOSE-ADMISSION IS REFUSED AT THE BOUNDARY. A discard is authorized by a
	// retirement intent; this kind carries none, so a discarding closure here would be that
	// authorization asserted by whoever assembled the batch.
	e2, done2 := testF105NewEnv(t, "f105-discard-disposition-2")
	defer done2()
	e2.bootstrap(t, "txn-1")
	al2 := e2.asm.jsa.loadOutboundAuthority(e2.id, e2.epoch)
	_, cerr := e2.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e2.id, Epoch: e2.epoch, Txn: "txn-close", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: al2.rec.OwnerTxn, Rev: al2.rec.Rev + 1,
			Gen: al2.rec.Gen, Tenure: al2.rec.Tenure, OwnerTxn: al2.rec.OwnerTxn,
		},
		Prev: al2.rec, PrevSeq: al2.seq,
		Auth: &mqttOutboundAuthority{
			ID: e2.id, Epoch: e2.epoch, Gen: al2.rec.Gen, Tenure: al2.rec.Tenure,
			State: mqttAuthorityClosed, Rev: al2.rec.Rev + 1, Txn: "txn-close",
			OwnerTxn: al2.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: e2.id, Epoch: e2.epoch, Gen: al2.rec.Gen, Txn: "txn-close", Discard: true,
		},
	})
	if cerr == nil {
		t.Fatalf("a close-admission wrote a discarding closure")
	}
	if !strings.Contains(cerr.Error(), "not authority to abandon its obligations") {
		t.Fatalf("the refusal did not come from the disposition rule: %v", cerr)
	}

	// AND A DISCARDING CLOSURE READS BACK AS APPLIES. The record is planted directly, because
	// the transition that legitimately writes one is a FINALIZE carrying a retirement intent
	// with Discard set — a different boundary, tested where it lives. What is under test here
	// is the READ: with only the non-discarding case above, a gatherer that hard-coded
	// "does not apply" passed every assertion, and a mutation showed exactly that.
	e3, done3 := testF105NewEnv(t, "f105-discard-disposition-3")
	defer done3()
	e3.bootstrap(t, "txn-1")
	al3 := e3.asm.jsa.loadOutboundAuthority(e3.id, e3.epoch)
	body, merr := json.Marshal(&mqttGenClosure{
		ID: e3.id, Epoch: e3.epoch, Gen: al3.rec.Gen, Txn: "txn-discard", Discard: true,
		// A CLOSURE CARRIES ITS ROOT'S ADDRESS, and a planted one is no exception: the record is
		// refused on READ without it, so a fixture that omitted it would be testing the decoder
		// rather than the disposition. Shard 0 is a real shard, so the address is derived the
		// same way the allocator derives it.
		RootShard: mqttDirShardFor(getHash(e3.id)), RootOrd: 1,
	})
	if merr != nil {
		t.Fatalf("marshalling the discarding closure: %v", merr)
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, serr := e3.asm.jsa.storeMsg(mqttGenClosureSubject(getHash(e3.id), al3.rec.Gen),
		hdr, msg); serr != nil {
		t.Fatalf("planting the discarding closure: %v", serr)
	}
	f3, ferr := e3.asm.gatherAdoptedReleaseFacts(e3.id, e3.epoch, al3.rec.Gen, nil)
	if ferr != nil {
		t.Fatalf("gathering facts for a discarded generation: %v", ferr)
	}
	if f3.Discard != mqttFactApplies {
		t.Fatalf("a discarding closure reported disposition %v", f3.Discard)
	}
}

// TestF105TheTargetEnumerationIsDerivedFromTheRecords is P2's gatherer half.
//
// The gate asks what this teardown still holds that NO DURABLE RECORD names, because those are
// the durables a crash after the metadata release loses outright. The answer is derived from the
// generation's backing resources — the records that make a cleanup duty come back for each one —
// and never from the teardown's own optimism.
func TestF105TheTargetEnumerationIsDerivedFromTheRecords(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-target-enrolment")
	defer done()
	e.bootstrap(t, "txn-1")

	// ENROLLED: a resource of this generation naming the durable. The admission beside it
	// gives the generation an INVENTORY, without which the gate is vacuous and the walk is
	// never run — the question only exists for a generation that admitted something.
	enrolled, _ := testF105PlantResource(t, e, "inc-shared", mqttResourceSourceConsumer)
	testF105AdmitOn(t, e, "txn-adm", 7, "inc-shared")
	held := []string{enrolled.Name, "$MQTT_shared_grp_unenrolled"}

	f, err := e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, held)
	if err != nil {
		t.Fatalf("gathering facts with durables held: %v", err)
	}
	// THE ENUMERATION IS ESTABLISHED, positively, because the resource walk answered in whole.
	if f.TargetsEnumerated != mqttFactApplies {
		t.Fatalf("a complete resource walk did not establish the enumeration: %v", f.TargetsEnumerated)
	}
	if len(f.UnenrolledTargets) != 1 || f.UnenrolledTargets[0] != "$MQTT_shared_grp_unenrolled" {
		t.Fatalf("the unenrolled durables were reported as %v", f.UnenrolledTargets)
	}

	// AND THE GATE BLOCKS ON IT once there is an inventory for the fact to be about.
	f.Ledger, f.Closed = testF105SealedLedger(10), &mqttGenClosure{}
	f.RootEnrolled, f.Discard = true, mqttFactDoesNotApply
	berr := mqttAdoptedReleaseAuthorized(e.id, testF105Gen, f)
	if berr == nil {
		t.Fatalf("a durable no record names did not block the release")
	}
	if !strings.Contains(berr.Error(), "enrolled shared and affine cleanup") {
		t.Fatalf("the block did not come from the enrolment: %v", berr)
	}

	// HOLDING ONLY ENROLLED DURABLES CLEARS IT. This is the positive control, and without it a
	// rule that reported every held durable unenrolled would pass the assertions above.
	f2, err := e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, testF105Gen, []string{enrolled.Name})
	if err != nil {
		t.Fatalf("gathering facts with only enrolled durables: %v", err)
	}
	if len(f2.UnenrolledTargets) != 0 {
		t.Fatalf("an enrolled durable was reported unenrolled: %v", f2.UnenrolledTargets)
	}
	f2.Ledger, f2.Closed = testF105SealedLedger(10), &mqttGenClosure{}
	f2.RootEnrolled, f2.Discard = true, mqttFactDoesNotApply
	if berr := mqttAdoptedReleaseAuthorized(e.id, testF105Gen, f2); berr != nil {
		t.Fatalf("a generation whose durables are all enrolled was blocked: %v", berr)
	}

	// AND A RESOURCE THIS BROKER CANNOT READ leaves the enumeration UNKNOWN. That record might
	// BE the durable being asked about, so "I could not read it" is not "it is not there" —
	// which is the same rule the walk applies to a truncated page, arriving from the other side.
	e2, done2 := testF105NewEnv(t, "f105-target-enrolment-2")
	defer done2()
	e2.bootstrap(t, "txn-1")
	testF105PlantResource(t, e2, "inc-shared", mqttResourceSourceConsumer)
	testF105AdmitOn(t, e2, "txn-adm", 7, "inc-shared")
	msg, hdr := mqttFrameBatchWrite(0, []byte("{\"kind\":"), _EMPTY_, 0, false)
	if _, serr := e2.asm.jsa.storeMsg(mqttResourceSubject(getHash(e2.id), testF105Gen,
		"inc-unreadable"), hdr, msg); serr != nil {
		t.Fatalf("planting an unreadable resource: %v", serr)
	}
	f3, err := e2.asm.gatherAdoptedReleaseFacts(e2.id, e2.epoch, testF105Gen, held)
	if err != nil {
		t.Fatalf("gathering facts with an unreadable resource: %v", err)
	}
	if f3.TargetsEnumerated != mqttFactUnknown {
		t.Fatalf("an enumeration with an unreadable record established %v", f3.TargetsEnumerated)
	}
	if len(f3.UnenrolledTargets) != 0 {
		t.Fatalf("an unestablished enumeration still named durables: %v", f3.UnenrolledTargets)
	}
}

// TestF105OnlyTheKindsThatAreAboutMembersMayCarryThem is consult 9's constraint 4.
//
// Five transition kinds enumerated what they may carry; five said NOTHING, so close-admission,
// finalize, finalize-creation, prepare-intent and cancel-intent could carry any F-105 class the
// generic member check allowed. That is a write no rule about those kinds has ever looked at —
// and the two that write a PERMANENT closure record are the worst of them, because a resource or
// an obligation riding on a closure lands in a generation that is ending.
//
// The rule is now a closed SET, stated once, so a new kind is refused by default rather than
// inheriting a permission by being forgotten.
func TestF105OnlyTheKindsThatAreAboutMembersMayCarryThem(t *testing.T) {
	for _, k := range []mqttTransitionKind{
		mqttTransitionAdmitExchange, mqttTransitionAcquireAction, mqttTransitionAdvanceExchange,
		mqttTransitionProvisionResource, mqttTransitionDisposeResource,
		mqttTransitionMaterializePayload,
	} {
		if !mqttKindCarriesMembers(k) {
			t.Fatalf("kind %q is about its members and may not carry them", k)
		}
	}
	for _, k := range []mqttTransitionKind{
		mqttTransitionCloseAdmit, mqttTransitionFinalize, mqttTransitionFinalizeCreation,
		mqttTransitionPrepareIntent, mqttTransitionCancelIntent, mqttTransitionReserveCreation,
	} {
		if mqttKindCarriesMembers(k) {
			t.Fatalf("kind %q moves ownership and may not carry members", k)
		}
	}

	// AND THE BOUNDARY ENFORCES IT, on the kind that writes a permanent closure.
	e, done := testF105NewEnv(t, "f105-kind-members")
	defer done()
	e.bootstrap(t, "txn-1")
	r, _ := testF105PlantUnsealedResource(t, e, "inc-rider", mqttResourceSourceConsumer)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	_, cerr := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close-rider", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityClosed, Rev: al.rec.Rev + 1, Txn: "txn-close-rider",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Txn: "txn-close-rider",
		},
		Extra: []mqttTransitionMember{testF105ResourceMember(t, e, 0, r)},
	})
	if cerr == nil {
		t.Fatalf("a closure carried an obligation member")
	}
	if !strings.Contains(cerr.Error(), "and nothing else") {
		t.Fatalf("the refusal did not come from the closing kind's member rule: %v", cerr)
	}

}

// TestF105AClosureCannotAdmitAnObligation is the arm that makes the closed-set member rule
// LOAD-BEARING, kept as its own test so a mutation shows it firing.
//
// A create-only resource on a closure is refused twice over — the provisioning rule catches it
// too — so that arm alone cannot show what this rule uniquely stops. An exchange admitted
// create-only is the one nothing else looks at: without the rule it lands in a generation that
// is ENDING, admitted by the act whose whole purpose is to stop admission.
func TestF105AClosureCannotAdmitAnObligation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closure-admits")
	defer done()
	e.bootstrap(t, "txn-1")
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)

	nonce := mqttNewInflightNonce()
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: nonce, PI: 9, QoS: 2,
		Phase: mqttExchangePublish, AdmitTxn: "txn-close-rider-2", Tenure: al.rec.Tenure,
	}
	xbody, merr := json.Marshal(x)
	if merr != nil {
		t.Fatalf("marshalling the exchange: %v", merr)
	}
	_, xerr := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close-rider-2", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityClosed, Rev: al.rec.Rev + 1, Txn: "txn-close-rider-2",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Txn: "txn-close-rider-2",
		},
		Extra: []mqttTransitionMember{{
			subj: mqttExchangeSubject(getHash(e.id), al.rec.Gen, nonce), expect: 0, body: xbody,
		}},
	})
	if xerr == nil {
		t.Fatalf("a closure admitted an obligation into the generation it was ending")
	}
	if !strings.Contains(xerr.Error(), "and nothing else") {
		t.Fatalf("the exchange refusal did not come from the closing kind's member rule: %v", xerr)
	}
}

// TestF105AClosureCannotAllocateInventory is the arm that ONLY the closed-set rule stops.
//
// Resource and exchange members are each refused a second time by their own kind rules, so
// neither can show what this rule uniquely adds. An INVENTORY allocation is the one nothing else
// looks at: the ledger CAS and its duty entries are validated as an allocation — correctly,
// against the standing ledger — and no rule asks which KIND of transition is taking the ordinal.
// Without this rule a closure allocates a duty in the generation it is ending.
func TestF105AClosureCannotAllocateInventory(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closure-allocates")
	defer done()
	e.bootstrap(t, "txn-1")
	r, _ := testF105PlantUnsealedResource(t, e, "inc-alloc", mqttResourceRetainedPayload)
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)

	_, cerr := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close-alloc", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityClosed, Rev: al.rec.Rev + 1, Txn: "txn-close-alloc",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Txn: "txn-close-alloc",
		},
		Extra: []mqttTransitionMember{
			// SEALED, so this fixture reaches the rule it is about. A closing batch's ledger
			// must be the sealed one — that is its own rule, and an OPEN body here would be
			// refused by it before the binding below was ever consulted.
			testF105LedgerMember(t, e, 0, testF105Ledger(e, mqttGenLedgerSealed, 1)),
			testF105DutyMember(t, e, 0, testF105Duty(e, 1, mqttDutyCleanup, r.Inc,
				mqttDutyNotYetActionable)),
		},
	})
	if cerr == nil {
		t.Fatalf("a closure allocated a duty in the generation it was ending")
	}
	if !strings.Contains(cerr.Error(), "which it does not enrol") {
		t.Fatalf("the allocation refusal did not come from the enrolment binding: %v", cerr)
	}
}

// testF105PlantClosureAt plants a closure record naming a root address.
//
// Direct, because what is under test here is the gate's READ of an address, and the transition
// that legitimately writes one is the closing batch — a different boundary, tested where it lives.
func testF105PlantClosureAt(t *testing.T, e *testF105Env, gen string, shard, ord uint64, discard bool) {
	t.Helper()
	body, err := json.Marshal(&mqttGenClosure{
		ID: e.id, Epoch: e.epoch, Gen: gen, Txn: "txn-planted-closure", Discard: discard,
		RootShard: shard, RootOrd: ord,
	})
	if err != nil {
		t.Fatalf("marshalling the closure: %v", err)
	}
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	if _, serr := e.asm.jsa.storeMsg(mqttGenClosureSubject(getHash(e.id), gen), hdr, msg); serr != nil {
		t.Fatalf("planting the closure: %v", serr)
	}
}
