// Copyright 2026 The mqtt-nats-bridge Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"time"
)

// testF105PlantSessionRecord writes one persisted session record, for the holder scans that read
// the whole record set rather than a live session's memory.
func testF105PlantSessionRecord(t *testing.T, e *testF105Env, id, gen string,
	sharedKeys ...string) {

	t.Helper()
	ps := &mqttPersistedSession{
		Origin: "test", ID: id, Gen: gen, Clean: false,
		SharedSubs: map[string]byte{},
		WID:        mqttNewRecordWID(),
	}
	for _, k := range sharedKeys {
		ps.SharedSubs[k] = 1
	}
	body, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshalling the session record of %q: %v", id, err)
	}
	subj := mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash(id)
	if _, serr := e.asm.jsa.storeMsg(subj, 0, body); serr != nil {
		t.Fatalf("planting the session record of %q: %v", id, serr)
	}
}

// TestF105DeferredSharedCleanupCountsASameClientSuccessor is consult 9's constraint 3.
//
// The live scan SKIPS every persisted record carrying its own client ID, on the stated grounds
// that the session's in-memory membership covers it. For a duty executed LATER there is no such
// session, and a record under the same client ID is not a stale copy but a SUCCESSOR that
// legitimately holds the group. Skipping it reports "no other holder" and deletes a consumer
// that is serving somebody.
func TestF105DeferredSharedCleanupCountsASameClientSuccessor(t *testing.T) {
	const group = "$share/grp/a/b"
	e, done := testF105NewEnv(t, "f105-shared-deferred")
	defer done()

	durs := mqttSharedDurablesForKey(group)
	if len(durs) == 0 {
		t.Fatalf("the fixture's group key maps to no durable")
	}
	dur := durs[0]

	// THE RETIRING GENERATION'S OWN RECORD is the one this cleanup may disregard.
	testF105PlantSessionRecord(t, e, e.id, testF105Gen, group)
	v, why := e.asm.jsa.classifySharedCleanup(e.id, testF105Gen, dur)
	if v != mqttSharedCleanupDelete {
		t.Fatalf("the retiring generation's own membership withheld its own cleanup: %v (%s)", v, why)
	}

	// A SAME-CLIENT SUCCESSOR HOLDS IT. Same ID, different generation — a reconnected client,
	// and the consumer it is using is the one this duty was about to delete.
	testF105PlantSessionRecord(t, e, e.id, "successor-gen-0123456", group)
	v, why = e.asm.jsa.classifySharedCleanup(e.id, testF105Gen, dur)
	if v != mqttSharedCleanupWithhold {
		t.Fatalf("a same-client successor's hold did not withhold the cleanup: %v", v)
	}
	if !strings.Contains(why, "still holds group durable") {
		t.Fatalf("the refusal did not name the holder: %s", why)
	}
}

// TestF105DeferredSharedCleanupFailsClosed is the other half of the same answer.
//
// It authorizes an IRREVERSIBLE delete of a consumer whose backlog is interest-retained, so "no
// other holder" may only be reported out of a record set read IN FULL. A record this broker
// cannot interpret is not a record it may disregard.
func TestF105DeferredSharedCleanupFailsClosed(t *testing.T) {
	const group = "$share/grp/a/b"
	e, done := testF105NewEnv(t, "f105-shared-deferred-closed")
	defer done()
	dur := mqttSharedDurablesForKey(group)[0]

	// NOBODY HOLDS IT, so the positive control deletes.
	if v, why := e.asm.jsa.classifySharedCleanup(e.id, testF105Gen, dur); v != mqttSharedCleanupDelete {
		t.Fatalf("an unheld durable was withheld: %v (%s)", v, why)
	}

	// AN UNREADABLE RECORD withholds every durable, because it may be the holder.
	subj := mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash("f105-shared-garbage")
	if _, serr := e.asm.jsa.storeMsg(subj, 0, []byte("{\"shared_subs\":")); serr != nil {
		t.Fatalf("planting an unreadable session record: %v", serr)
	}
	v, why := e.asm.jsa.classifySharedCleanup(e.id, testF105Gen, dur)
	if v != mqttSharedCleanupWithhold {
		t.Fatalf("an unreadable record did not withhold the cleanup: %v", v)
	}
	if !strings.Contains(why, "could not be read in full") {
		t.Fatalf("the refusal did not name the incomplete reading: %s", why)
	}
}

// TestF105AClosureEnrolsTheDurablesItsGenerationHolds is the POSITIVE control for the one
// permission a closing transition has over members — and it is the test the three refusals
// cannot stand without, because a rule that refused everything would pass all of them.
func TestF105AClosureEnrolsTheDurablesItsGenerationHolds(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closure-enrols")
	defer done()
	e.bootstrap(t, "txn-1")
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)

	// TWO MEMBERS OF ONE GROUP produce ONE enrolment, which is why the assembler dedupes.
	enrol, err := mqttEnrolSharedDurables(e.id, e.epoch, al.rec.Gen,
		[]string{"$MQTT_shared_grp_b", "$MQTT_shared_grp_a", "$MQTT_shared_grp_b"}, nil, 0)
	if err != nil {
		t.Fatalf("assembling the enrolment: %v", err)
	}
	if len(enrol.Incs) != 2 {
		t.Fatalf("the enrolment minted %d incarnations for two distinct durables", len(enrol.Incs))
	}

	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close-enrol", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityClosed, Rev: al.rec.Rev + 1, Txn: "txn-close-enrol",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Txn: "txn-close-enrol",
		},
		Extra: enrol.Members,
	})); cerr != nil {
		t.Fatalf("a closure enrolling its generation's shared durables was refused: %v", cerr)
	}

	// AND THE GATE CAN NOW SEE THEM. This is the whole point of the enrolment: the release
	// gate's target enumeration reads the generation's resources, and a durable one of them
	// names is no longer reported unenrolled.
	f, gerr := e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, al.rec.Gen,
		[]string{"$MQTT_shared_grp_a", "$MQTT_shared_grp_b"})
	if gerr != nil {
		t.Fatalf("gathering facts after the enrolment: %v", gerr)
	}
	if f.TargetsEnumerated != mqttFactApplies {
		t.Fatalf("the enrolment did not establish the enumeration: %v", f.TargetsEnumerated)
	}
	if len(f.UnenrolledTargets) != 0 {
		t.Fatalf("an enrolled durable was still reported unenrolled: %v", f.UnenrolledTargets)
	}

	// AND THE RELEASE ITSELF: does the gate now say YES? It does NOT, and this arm is here
	// because the two facts above do not answer that question and reading them as if they did
	// is exactly how a mechanism gets called finished.
	//
	// The refusal is on DURABLE RECOVERY OWNERSHIP, a condition no generation can satisfy on
	// this tree: root and shard are deliberately not obligation classes, nothing allocates a
	// root (`persistSweep` only advances an existing one's cursor), nothing calls
	// `sweepOneBatch`, and nothing executes a duty verdict. So the enrolment establishes
	// everything the TARGET conditions asked for and release is still withheld, because the
	// question "who will come back for this duty" has no answer yet. Consult 10 ruled that
	// this lane records that blocked, keeps the synchronous cleanup, and does not begin the
	// cutover on it.
	//
	// WHEN THE RECOVERY PASS LANDS, THIS ARM INVERTS rather than being deleted: the same
	// fixture must then be PERMITTED, which is consult 7's property 2.
	// THE ARM THIS FIXTURE ALWAYS SAID WOULD INVERT, and it has: the closing batch now allocates
	// a reachable recovery root, so "who will come back for this duty" HAS an answer and the
	// release is PERMITTED with the cleanup still outstanding. That is consult 7's property 2,
	// and it is the whole point of the recovery pass — the gate asks for ENROLMENT, not for
	// completion, because a gate that waited for cleanup would be the synchronous wait again.
	if berr := mqttAdoptedReleaseAuthorized(e.id, al.rec.Gen, f); berr != nil {
		t.Fatalf("the gate withheld a release whose duties are enrolled and reachable: %v", berr)
	}

	// AND IT IS STILL WITHHELD WITHOUT THAT REACHABILITY, which is what says the permission above
	// rests on the root rather than on the enrolment alone.
	noRoot := f
	noRoot.RootEnrolled, noRoot.Cert = false, nil
	if rerr := mqttAdoptedReleaseAuthorized(e.id, al.rec.Gen, noRoot); rerr == nil {
		t.Fatalf("the gate released a generation with neither a reachable root nor a certificate")
	} else if !strings.Contains(rerr.Error(), "recovery root") {
		t.Fatalf("release was withheld for some reason other than recovery ownership: %v", rerr)
	}

	// AND NOT FOR THE REASONS THE ENROLMENT WAS ABOUT. A refusal that also named a target
	// condition would mean the enrolment had not done its job.
	const targetFact = "enrolled shared and affine cleanup"
	if rerr := mqttAdoptedReleaseAuthorized(e.id, al.rec.Gen, noRoot); strings.Contains(rerr.Error(), targetFact) {
		t.Fatalf("the enrolment left a target condition unsatisfied: %v", rerr)
	}
	// AND THAT ABSENCE IS A MEASUREMENT, not a hope that the wording never changes: with one
	// target put back the way the enrolment found it, the same check fires.
	unenrolled := f
	unenrolled.UnenrolledTargets = []string{"$MQTT_shared_grp_a"}
	uerr := mqttAdoptedReleaseAuthorized(e.id, al.rec.Gen, unenrolled)
	if uerr == nil || !strings.Contains(uerr.Error(), targetFact) {
		t.Fatalf("the check above cannot see a target condition at all: %v", uerr)
	}

	// AND THE INVENTORY IS SEALED BY THE SAME ACT, which is what makes the enumeration's
	// completeness a fact rather than a snapshot: no further ordinal can be taken in it.
	led, _, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, al.rec.Gen)
	if lerr != nil || led == nil {
		t.Fatalf("reading the sealed ledger: %v", lerr)
	}
	if led.State != mqttGenLedgerSealed || led.SealedHi != 2 {
		t.Fatalf("the enrolment left the ledger in state %d at bound %d", led.State, led.SealedHi)
	}
}

// TestF105AFinalizationEnrolsOnlyThePredecessorItCloses is the arm the close-admission cases
// cannot reach.
//
// A discarding FINALIZATION installs the successor's generation while closing the predecessor's,
// so it is the one transition where "the transition's generation" and "the generation the
// closure names" are DIFFERENT. The cross-generation permission is scoped to the second: enrolling
// into the successor would file a cleanup duty in an inventory that is still open and still
// growing, under a generation whose own closure has not happened and whose seal is therefore not
// the enrolment's boundary at all.
func TestF105AFinalizationEnrolsOnlyThePredecessorItCloses(t *testing.T) {
	jsa := &mqttJSA{}
	idHash := getHash(testF105ID)

	build := func(enrolGen string) *mqttAuthorityTransition {
		prep := testF105PreparingAuthority()
		prep.Rev = 1
		tr := testF105Transition()
		tr.Prev = prep
		tr.Attempt.Kind = mqttTransitionFinalize
		tr.Attempt.PrevTxn = prep.OwnerTxn
		tr.Attempt.Intent = prep.Intent.ID
		tr.Attempt.Gen = prep.Intent.Gen
		tr.Attempt.Tenure = prep.Intent.Tenure
		tr.Attempt.OwnerTxn = tr.Attempt.Txn
		tr.Auth = &mqttOutboundAuthority{
			ID: testF105ID, Epoch: testF105Epoch,
			Gen: prep.Intent.Gen, Tenure: prep.Intent.Tenure,
			State: mqttAuthorityServing, Rev: tr.Attempt.Rev,
			Txn: tr.Attempt.Txn, OwnerTxn: tr.Attempt.Txn,
		}
		tr.Closure = &mqttGenClosure{
			ID: testF105ID, Epoch: testF105Epoch, Gen: prep.Intent.PrevGen,
			Txn: tr.Attempt.Txn, Discard: true,
		}
		enrol, err := mqttEnrolSharedDurables(testF105ID, testF105Epoch, enrolGen,
			[]string{"$MQTT_shared_grp_a"}, nil, 0)
		if err != nil {
			t.Fatalf("assembling the enrolment for %q: %v", enrolGen, err)
		}
		tr.Extra = enrol.Members
		return tr
	}

	// INTO THE SUCCESSOR: refused, and by the closing rule rather than by the generic
	// generation rule, which permits either generation on a transition that writes a closure.
	_, err := jsa.commitAuthorityTransition(build(testF105Gen2))
	if err == nil {
		t.Fatalf("a finalization enrolled durables into the generation it was installing")
	}
	if !strings.Contains(err.Error(), "is not in the generation") {
		t.Fatalf("the refusal did not come from the closure's generation: %v", err)
	}
	if !strings.Contains(err.Error(), idHash) {
		t.Fatalf("the refusal did not name the member it refused: %v", err)
	}

	// AND A FINALIZATION THAT CLOSES NOTHING CARRIES NOTHING. A takeover preserves the
	// predecessor's generation, so there is no closure and no seal — and therefore no act that
	// could make an enrolment's completeness establishable. Members on one of those are members
	// the permission was never about.
	prep := testF105PreparingAuthority()
	prep.Rev = 1
	prep.Intent.CleanReq = false
	prep.Intent.Gen = prep.Intent.PrevGen
	prep.Intent.Retire.Reason = mqttRetireTakeover
	prep.Intent.Retire.Discard = false
	tr := testF105Transition()
	tr.Prev = prep
	tr.Attempt.Kind = mqttTransitionFinalize
	tr.Attempt.PrevTxn = prep.OwnerTxn
	tr.Attempt.Intent = prep.Intent.ID
	tr.Attempt.Gen = prep.Intent.Gen
	tr.Attempt.Tenure = prep.Intent.Tenure
	tr.Attempt.OwnerTxn = tr.Attempt.Txn
	tr.Auth = &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch,
		Gen: prep.Intent.Gen, Tenure: prep.Intent.Tenure,
		State: mqttAuthorityServing, Rev: tr.Attempt.Rev,
		Txn: tr.Attempt.Txn, OwnerTxn: tr.Attempt.Txn,
	}
	enrol, eerr := mqttEnrolSharedDurables(testF105ID, testF105Epoch, prep.Intent.Gen,
		[]string{"$MQTT_shared_grp_a"}, nil, 0)
	if eerr != nil {
		t.Fatalf("assembling the enrolment: %v", eerr)
	}
	tr.Extra = enrol.Members
	_, nerr := jsa.commitAuthorityTransition(tr)
	if nerr == nil {
		t.Fatalf("a finalization that closes nothing carried an enrolment")
	}
	if !strings.Contains(nerr.Error(), "without writing a closure") {
		t.Fatalf("the refusal did not come from the missing closure: %v", nerr)
	}
}

// TestF105AnEnrolledDurablesCleanupAsksTheGroup is where the enrolled kind and the deferred scan
// meet: the duty classifier.
//
// Every other resource's authorization follows from nothing DEPENDING on it, because this
// generation provisioned it and its dependents are in its own list. A group durable's other
// holders are not in that list and never will be — they are other sessions — so the classifier
// asks the record set, and an unresolved answer withholds.
func TestF105AnEnrolledDurablesCleanupAsksTheGroup(t *testing.T) {
	const group = "$share/grp/a/b"
	e, done := testF105NewEnv(t, "f105-enrolled-cleanup")
	defer done()
	e.bootstrap(t, "txn-1")
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	dur := mqttSharedDurablesForKey(group)[0]

	enrol, err := mqttEnrolSharedDurables(e.id, e.epoch, al.rec.Gen, []string{dur}, nil, 0)
	if err != nil {
		t.Fatalf("assembling the enrolment: %v", err)
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close-enrol", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityClosed, Rev: al.rec.Rev + 1, Txn: "txn-close-enrol",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Txn: "txn-close-enrol",
		},
		Extra: enrol.Members,
	})); cerr != nil {
		t.Fatalf("the enrolling closure was refused: %v", cerr)
	}

	duty := &mqttDutyEntry{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Ord: 1,
		Kind: mqttDutyCleanup, Ref: enrol.Incs[dur], State: mqttDutyNotYetActionable,
	}

	// NOBODY ELSE HOLDS IT: the authorizing CAS is owed, exactly as for any other resource.
	v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, al.rec.Gen, duty)
	if v.Disposition != mqttDutyOwesWork || v.Work != mqttDutyWorkAuthorizeDeletion {
		t.Fatalf("an unheld group durable was %v/%v, want owes-work/authorize-deletion: %s",
			v.Disposition, v.Work, v.Why)
	}

	// A SUCCESSOR OF THE SAME CLIENT HOLDS IT: withheld, and the name is not this generation's
	// to free. Nothing about the resource's own record changed — its dependency list is still
	// empty and its provisioning is still sealed — which is exactly why the list cannot answer
	// this question.
	testF105PlantSessionRecord(t, e, e.id, "successor-gen-0123456", group)
	v = e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, al.rec.Gen, duty)
	if v.Disposition != mqttDutyWithheld {
		t.Fatalf("a held group durable was %v, want withheld: %s", v.Disposition, v.Why)
	}
	if !strings.Contains(v.Why, "still holds group durable") {
		t.Fatalf("the verdict did not name the holder: %s", v.Why)
	}
}

// TestF105ADiscardingFinalizationEnrolsThePredecessorItCloses is the PRODUCTION shape, against a
// real store.
//
// The refusal arms beside it are validator arms on an empty JSA: they establish what the
// permission excludes. What they cannot establish is that the shape the conversion will actually
// use is ADMITTED — and that shape is not the close-admission's. A discarding handoff installs
// the successor's generation in the same act that closes the predecessor's, so its transition's
// own generation is G2 while every enrolled member names G1: the resource records, the duty
// entries and the ledger CAS. If any boundary silently required the transition's own generation,
// the mechanism would be provably correct on a shape production never takes.
//
// It is also the arm that kills the permission's mutation with a REFUSAL rather than a panic:
// on an empty JSA the kind rules are the only thing standing between a member and a nil store.
func TestF105ADiscardingFinalizationEnrolsThePredecessorItCloses(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-finalize-enrols")
	defer done()
	jsa := &e.asm.jsa

	first, _ := e.bootstrap(t, "txn-1")
	ps, seq := testF105StandingSession(t, e)

	// A CLEAN START: a new generation, and the predecessor's discarded.
	intent := &mqttHandoffIntent{
		ID: "intent-clean", PrevGen: first.Gen, PrevTenure: first.Tenure,
		PrevOwnerTxn: first.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: testF105Gen2, Tenure: "tenure-clean", CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: first.Gen, Tenure: first.Tenure, OwnerTxn: first.OwnerTxn,
			SessWID: ps.WID, SessSeq: seq, Reason: mqttRetireCleanStart, Discard: true,
			// THE RETIREMENT NAMES THE DURABLES THIS FIXTURE GOES ON TO ENROL, because
			// opFinalizeHandoff composes the enrolment FROM these — and since holistic round 11
			// the boundary binds the enrolment to them. Without this the fixture enrolled
			// durables its own intent never selected, which is the substitution the binding
			// exists to refuse, and it claimed to compose "exactly as opFinalizeHandoff does".
			SharedDurables: []string{"$MQTT_shared_grp_a", "$MQTT_shared_grp_b"},
		},
	}
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-clean", intent); err != nil {
		t.Fatalf("preparing the clean start was refused: %v", err)
	}
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: intent.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the clean start's metadata half was refused: %v", err)
	}

	pl := jsa.loadOutboundAuthority(e.id, e.epoch)
	if pl.status != mqttOutboundValid || pl.rec.State != mqttAuthorityPreparing || pl.rec.Intent == nil {
		t.Fatalf("the preparation did not land (status %v)", pl.status)
	}
	i := pl.rec.Intent

	// THE ENROLMENT NAMES THE GENERATION BEING CLOSED, which is the predecessor's, while the
	// transition installs the successor's. Its ledger is that generation's, and it has none
	// yet, so the CAS is create-only.
	cur, curSeq, lerr := jsa.loadGenLedger(e.id, e.epoch, i.PrevGen)
	if lerr != nil {
		t.Fatalf("reading the predecessor's ledger: %v", lerr)
	}
	enrol, eerr := mqttEnrolSharedDurables(e.id, e.epoch, i.PrevGen,
		[]string{"$MQTT_shared_grp_a", "$MQTT_shared_grp_b"}, cur, curSeq)
	if eerr != nil {
		t.Fatalf("assembling the enrolment: %v", eerr)
	}

	// The finalization is composed exactly as opFinalizeHandoff composes it — the operation
	// carries no members yet, which is the conversion's work — plus the enrolment.
	if _, cerr := jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-fin-clean", Op: "op-fin-clean",
			Kind: mqttTransitionFinalize, PrevTxn: pl.rec.OwnerTxn, Rev: pl.rec.Rev + 1,
			Gen: i.Gen, Tenure: i.Tenure, OwnerTxn: "txn-fin-clean", Intent: i.ID,
		},
		Prev: pl.rec, PrevSeq: pl.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: i.Gen, Tenure: i.Tenure,
			State: mqttAuthorityServing, Rev: pl.rec.Rev + 1,
			Txn: "txn-fin-clean", OwnerTxn: "txn-fin-clean",
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: i.PrevGen, Txn: "txn-fin-clean", Discard: true,
		},
		Extra: enrol.Members,
	})); cerr != nil {
		t.Fatalf("a discarding finalization enrolling its predecessor's durables was refused: %v", cerr)
	}

	// AND THE SUCCESSOR IS SERVING, so the enrolment did not cost the handoff its effect.
	fl := jsa.loadOutboundAuthority(e.id, e.epoch)
	if fl.status != mqttOutboundValid || fl.rec.State != mqttAuthorityServing || fl.rec.Gen != i.Gen {
		t.Fatalf("the finalization did not install the successor: %+v", fl.rec)
	}

	// AND THE GATE READS THE ENROLMENT OFF THE CLOSED GENERATION. This is the production
	// shape's own version of the close-admission's assertion: the facts the release gate
	// derives are about the generation the closure named, not the one now serving.
	f, gerr := e.asm.gatherAdoptedReleaseFacts(e.id, e.epoch, i.PrevGen,
		[]string{"$MQTT_shared_grp_a", "$MQTT_shared_grp_b"})
	if gerr != nil {
		t.Fatalf("gathering facts for the closed generation: %v", gerr)
	}
	if f.TargetsEnumerated != mqttFactApplies {
		t.Fatalf("the enrolment did not establish the enumeration: %v", f.TargetsEnumerated)
	}
	if len(f.UnenrolledTargets) != 0 {
		t.Fatalf("an enrolled durable was still reported unenrolled: %v", f.UnenrolledTargets)
	}
	if f.Discard != mqttFactApplies {
		t.Fatalf("the closure's discard disposition did not reach the gate: %v", f.Discard)
	}

	// AND THE PREDECESSOR'S INVENTORY IS SEALED, by the act that closed it.
	led, _, plerr := jsa.loadGenLedger(e.id, e.epoch, i.PrevGen)
	if plerr != nil || led == nil {
		t.Fatalf("reading the closed generation's ledger: %v", plerr)
	}
	if led.State != mqttGenLedgerSealed || led.SealedHi != 2 {
		t.Fatalf("the enrolment left the ledger in state %d at bound %d", led.State, led.SealedHi)
	}
}

// TestF105AResumingFinalizationCarriesNoEnrolment is the same production route with the one
// difference that matters: the handoff PRESERVES the generation, so there is no closure and no
// seal.
//
// It exists store-backed rather than as another validator arm because of what a validator arm
// cannot prove here. On an empty JSA the kind rules are the only thing between a member and a
// nil store, so removing the closure requirement kills the arm by PANICKING in the loader — a
// failure that says nothing about whether the write would have been refused. Against a real
// store the same mutation lets the batch through, and the refusal this test demands is the only
// thing that can fail.
func TestF105AResumingFinalizationCarriesNoEnrolment(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-resume-no-enrol")
	defer done()
	jsa := &e.asm.jsa

	first, _ := e.bootstrap(t, "txn-1")
	ps, seq := testF105StandingSession(t, e)

	// A RESUME: the same generation, a new tenure, nothing retired and nothing closed.
	intent := &mqttHandoffIntent{
		ID: "intent-resume", PrevGen: first.Gen, PrevTenure: first.Tenure,
		PrevOwnerTxn: first.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: first.Gen, Tenure: "tenure-resumed", CleanReq: false,
	}
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-resume", intent); err != nil {
		t.Fatalf("preparing the resume was refused: %v", err)
	}
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: intent.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the resume's metadata half was refused: %v", err)
	}
	pl := jsa.loadOutboundAuthority(e.id, e.epoch)
	if pl.status != mqttOutboundValid || pl.rec.State != mqttAuthorityPreparing || pl.rec.Intent == nil {
		t.Fatalf("the preparation did not land (status %v)", pl.status)
	}
	i := pl.rec.Intent

	cur, curSeq, lerr := jsa.loadGenLedger(e.id, e.epoch, i.Gen)
	if lerr != nil {
		t.Fatalf("reading the generation's ledger: %v", lerr)
	}
	enrol, eerr := mqttEnrolSharedDurables(e.id, e.epoch, i.Gen,
		[]string{"$MQTT_shared_grp_a"}, cur, curSeq)
	if eerr != nil {
		t.Fatalf("assembling the enrolment: %v", eerr)
	}

	_, cerr := jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-fin-resume", Op: "op-fin-resume",
			Kind: mqttTransitionFinalize, PrevTxn: pl.rec.OwnerTxn, Rev: pl.rec.Rev + 1,
			Gen: i.Gen, Tenure: i.Tenure, OwnerTxn: "txn-fin-resume", Intent: i.ID,
		},
		Prev: pl.rec, PrevSeq: pl.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: i.Gen, Tenure: i.Tenure,
			State: mqttAuthorityServing, Rev: pl.rec.Rev + 1,
			Txn: "txn-fin-resume", OwnerTxn: "txn-fin-resume",
		},
		Extra: enrol.Members,
	}))
	if cerr == nil {
		t.Fatalf("a finalization that closes nothing enrolled durables into a LIVE generation")
	}
	if !strings.Contains(cerr.Error(), "without writing a closure") {
		t.Fatalf("the refusal did not come from the missing closure: %v", cerr)
	}

	// AND IT WROTE NOTHING. A refusal that had already allocated an ordinal would leave the
	// live generation carrying a duty for a cleanup nobody asked for.
	led, _, glerr := jsa.loadGenLedger(e.id, e.epoch, i.Gen)
	if glerr != nil {
		t.Fatalf("reading the generation's ledger after the refusal: %v", glerr)
	}
	if led != nil {
		t.Fatalf("the refused enrolment left a ledger standing at high-water %d", led.Hi)
	}
}

// TestF105AClosureCarriesTheSealedLedger is the rule that makes the enrolment's completeness a
// fact rather than a composer's habit.
//
// The enrolment's whole claim is that the act which allocates is the act which seals, so no
// further ordinal can be taken in that generation afterwards. Checking only the ledger member's
// GENERATION left the seal to the composer: a batch carrying an otherwise valid enrolment beside
// a ledger body still OPEN closes the generation with its inventory unsealed, and then a later
// disposal — deliberately permitted to name an earlier generation — allocates into an inventory
// this act was supposed to have shut. What the release gate reads off that generation would be a
// snapshot again, which is the thing the enrolment exists to stop being.
func TestF105AClosureCarriesTheSealedLedger(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closure-seal")
	defer done()
	e.bootstrap(t, "txn-1")
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)

	enrol, err := mqttEnrolSharedDurables(e.id, e.epoch, al.rec.Gen,
		[]string{"$MQTT_shared_grp_a"}, nil, 0)
	if err != nil {
		t.Fatalf("assembling the enrolment: %v", err)
	}

	// UNSEAL THE LEDGER MEMBER and leave everything else exactly as the composer produced it.
	members := make([]mqttTransitionMember, len(enrol.Members))
	copy(members, enrol.Members)
	var found bool
	for i, m := range members {
		if !mqttIsLedgerSubject(m.subj) {
			continue
		}
		led, derr := mqttDecodeGenLedger(m.body, e.id, e.epoch, al.rec.Gen)
		if derr != nil {
			t.Fatalf("decoding the enrolment's ledger: %v", derr)
		}
		led.State, led.SealedHi = mqttGenLedgerOpen, 0
		body, merr := json.Marshal(led)
		if merr != nil {
			t.Fatalf("marshalling the unsealed ledger: %v", merr)
		}
		members[i] = mqttTransitionMember{subj: m.subj, expect: m.expect, body: body}
		found = true
	}
	if !found {
		t.Fatalf("the enrolment carried no ledger member, so this fixture tests nothing")
	}

	_, cerr := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-close-open", Op: "op-close",
			Kind: mqttTransitionCloseAdmit, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityClosed, Rev: al.rec.Rev + 1, Txn: "txn-close-open",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Closure: &mqttGenClosure{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Txn: "txn-close-open",
		},
		Extra: members,
	}))
	if cerr == nil {
		t.Fatalf("a closure left its generation's inventory open")
	}
	if !strings.Contains(cerr.Error(), "leaving its inventory at state") {
		t.Fatalf("the refusal did not come from the seal: %v", cerr)
	}

	// AND NOTHING WAS WRITTEN: neither the ledger it would have left open, nor the closure.
	if led, _, lerr := e.asm.jsa.loadGenLedger(e.id, e.epoch, al.rec.Gen); lerr != nil || led != nil {
		t.Fatalf("the refused closure left a ledger standing: %+v (%v)", led, lerr)
	}
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, al.rec.Gen); st != mqttOutboundAbsent {
		t.Fatalf("the refused closure wrote a closure record anyway (status %v)", st)
	}

	// AND THE COMPOSER'S OWN MEMBERS ARE ADMITTED, so the arm above is about the SEAL and not
	// about closing enrolments in general.
	if oerr := func() error {
		l := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		_, err := e.asm.jsa.commitAuthorityTransition(testF105EnrolClosing(t, e, &mqttAuthorityTransition{
			Attempt: &mqttTransitionAttempt{
				ID: e.id, Epoch: e.epoch, Txn: "txn-close-sealed", Op: "op-close",
				Kind: mqttTransitionCloseAdmit, PrevTxn: l.rec.OwnerTxn, Rev: l.rec.Rev + 1,
				Gen: l.rec.Gen, Tenure: l.rec.Tenure, OwnerTxn: l.rec.OwnerTxn,
			},
			Prev: l.rec, PrevSeq: l.seq,
			Auth: &mqttOutboundAuthority{
				ID: e.id, Epoch: e.epoch, Gen: l.rec.Gen, Tenure: l.rec.Tenure,
				State: mqttAuthorityClosed, Rev: l.rec.Rev + 1, Txn: "txn-close-sealed",
				OwnerTxn: l.rec.OwnerTxn,
			},
			Closure: &mqttGenClosure{
				ID: e.id, Epoch: e.epoch, Gen: l.rec.Gen, Txn: "txn-close-sealed",
			},
			Extra: enrol.Members,
		}))
		return err
	}(); oerr != nil {
		t.Fatalf("the composer's own sealed enrolment was refused: %v", oerr)
	}
}

// TestF105TheDeferredScanAttributesBeforeItSkips covers the two ways a record can fail to be
// evidence while still decoding.
//
// A DECODE THAT SUCCEEDS IS NOT AN IDENTITY. `null` and `{}` unmarshal without error into a
// record naming nobody, contribute no holders, and leave the walk calling itself complete — an
// absence manufactured out of something the broker could not interpret, which is exactly the
// inference this scan exists to refuse.
//
// AND THE BODY'S IDENTITY IS THE SUBJECT'S. The one record this cleanup may disregard is the one
// it is cleaning up after, and that is decided from the BODY. A record standing on ANOTHER
// client's subject whose body claims the retiring identity is therefore skipped — while its
// SharedSubs names the very durable about to be deleted. The disagreement between a record's name
// and its contents may not be resolved in the direction that permits a deletion.
func TestF105TheDeferredScanAttributesBeforeItSkips(t *testing.T) {
	const key = "$share/grp/a/b"
	durs := mqttSharedDurablesForKey(key)
	if len(durs) == 0 {
		t.Fatalf("the fixture's group key maps to no durable")
	}
	durable := durs[0]

	t.Run("a record naming nobody withholds", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-scan-nameless")
		defer done()
		subj := mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash("f105-scan-nameless-other")
		if _, serr := e.asm.jsa.storeMsg(subj, 0, []byte(`{}`)); serr != nil {
			t.Fatalf("planting the empty record: %v", serr)
		}
		scan := e.asm.jsa.sharedDurablesHeldAfterRetirement(e.id, testF105Gen, []string{durable})
		if scan.Complete {
			t.Fatalf("a record naming nobody was read as a complete measurement: %+v", scan)
		}
		if !scan.Held[durable] {
			t.Fatalf("the scan reported the durable free on the strength of a record it could "+
				"not interpret: %+v", scan)
		}
	})

	t.Run("a record on somebody else's subject withholds", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-scan-misattributed")
		defer done()
		// THE BODY CLAIMS THE RETIRING IDENTITY, which is what would earn it the skip, and it
		// stands on a subject belonging to another client while naming the durable.
		ps := &mqttPersistedSession{
			Origin: "test", ID: e.id, Gen: testF105Gen,
			SharedSubs: map[string]byte{key: 1}, WID: mqttNewRecordWID(),
		}
		body, merr := json.Marshal(ps)
		if merr != nil {
			t.Fatalf("marshalling the misattributed record: %v", merr)
		}
		subj := mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash("f105-somebody-else")
		if _, serr := e.asm.jsa.storeMsg(subj, 0, body); serr != nil {
			t.Fatalf("planting the misattributed record: %v", serr)
		}
		scan := e.asm.jsa.sharedDurablesHeldAfterRetirement(e.id, testF105Gen, []string{durable})
		if scan.Complete {
			t.Fatalf("a misattributed record was read as a complete measurement: %+v", scan)
		}
		if !scan.Held[durable] {
			t.Fatalf("a record whose name and body disagree was skipped as this cleanup's own, "+
				"and the durable it names reported free: %+v", scan)
		}
	})

	t.Run("a record naming nobody AT NOBODY'S SUBJECT withholds", func(t *testing.T) {
		// THIS IS THE INPUT THAT SEPARATES THE TWO CHECKS. getHash(_EMPTY_) is a perfectly good
		// subject token, so a record with no identity standing at the address that names no
		// client passes the attribution comparison — its contents and its name agree, on
		// nobody. Only the identity check refuses it. The first arm above cannot show this,
		// because there the disagreement does the work.
		e, done := testF105NewEnv(t, "f105-scan-nobody-subject")
		defer done()
		subj := mqttSessStreamSubjectPrefix + e.asm.domainTk + getHash(_EMPTY_)
		if _, serr := e.asm.jsa.storeMsg(subj, 0, []byte(`{}`)); serr != nil {
			t.Fatalf("planting the record at nobody's subject: %v", serr)
		}
		scan := e.asm.jsa.sharedDurablesHeldAfterRetirement(e.id, testF105Gen, []string{durable})
		if scan.Complete {
			t.Fatalf("a record with no identity was read as a complete measurement: %+v", scan)
		}
		if !scan.Held[durable] {
			t.Fatalf("the scan reported the durable free over a record naming nobody: %+v", scan)
		}
	})

	t.Run("and a correctly attributed retiring record IS skipped", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-scan-own")
		defer done()
		testF105PlantSessionRecord(t, e, e.id, testF105Gen, key)
		scan := e.asm.jsa.sharedDurablesHeldAfterRetirement(e.id, testF105Gen, []string{durable})
		if !scan.Complete {
			t.Fatalf("the walk did not complete over a record set it can read: %+v", scan)
		}
		if scan.Held[durable] {
			t.Fatalf("the retiring generation's own record was counted as another holder: %+v", scan)
		}
	})
}
