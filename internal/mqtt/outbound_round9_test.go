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
	"fmt"
	"strings"
	"testing"
	"time"
)

// The findings of implementation review 9, each against the store.
//
// All six are in the OPERATION SURFACE, and five of them are one shape: an operation that
// loads the current state answers "what is there", which is not the same question as "is this
// request still the one this owner asked for". Reading is necessary and is not sufficient.

// TestF105StaleRequestsDoNotRetargetTheCurrentOwner is finding 2.
//
// An operation that takes no target adopts whatever it loads. A closure composed for G1 and
// delayed until G2 serves would load G2 and end it — permanently, with every boundary check
// passing, because the operation itself had substituted the target. The same shape reaches
// admission and acquisition, which would otherwise admit G1's delivery as G2's work under a
// tenure that never authorized it.
func TestF105StaleRequestsDoNotRetargetTheCurrentOwner(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-stale-target")
	defer done()

	jsa := &e.asm.jsa
	g1, g1Seq := e.bootstrap(t, "txn-1")

	// The caller captures its entitlement HERE, under G1.
	capturedGen, capturedTenure, capturedOwner := g1.Gen, g1.Tenure, g1.OwnerTxn

	// Ownership moves on: G1 is closed and G2 is claimed beside it.
	closing := testF105Closure(e, g1, g1Seq, "txn-close-g1")
	closeSeq, cerr := jsa.commitAuthorityTransition(closing)
	if cerr != nil {
		t.Fatalf("closing G1 was refused: %v", cerr)
	}
	g2, _ := e.claim(t, closing.Auth, closeSeq, "txn-g2", testF105Gen2, "tenure-g2")

	// THE DELAYED CLOSURE MUST NOT END G2.
	if _, err := jsa.opCloseGeneration(e.id, e.epoch, "txn-close-late", capturedGen, capturedTenure, capturedOwner); err == nil {
		t.Fatalf("a closure composed for %q ended the standing generation instead", capturedGen)
	}
	if al := jsa.loadOutboundAuthority(e.id, e.epoch); al.status != mqttOutboundValid ||
		al.rec.State != mqttAuthorityServing || al.rec.Gen != g2.Gen {
		t.Fatalf("the refused closure moved the authority: %+v", al.rec)
	}

	// THE DELAYED ADMISSION MUST NOT BE ADMITTED AS G2's WORK.
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-adm-late", Op: "op-adm",
		Gen: capturedGen, Tenure: capturedTenure,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a delivery decided under %q/%q was admitted under the successor",
			capturedGen, capturedTenure)
	}

	// AND THE DELAYED ACQUISITION MUST NOT EITHER.
	if _, _, err := jsa.opAcquireAction(&mqttAcquireActionRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-acq-late", Op: "op-acq",
		Gen: capturedGen, Tenure: capturedTenure,
		Nonce: mqttNewInflightNonce(), Acq: 1, Kind: mqttActionPublish,
		Binding: mqttBindConnection(4242),
	}); err == nil {
		t.Fatalf("an acquisition captured under %q/%q ran under the successor",
			capturedGen, capturedTenure)
	}

	// The same operations under the CURRENT entitlement still work, so the refusals above
	// are the target and not the fixture.
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-adm-now", Op: "op-adm",
		Gen: g2.Gen, Tenure: g2.Tenure,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 8, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err != nil {
		t.Fatalf("an admission under the standing entitlement was refused: %v", err)
	}
}

// TestF105AcquisitionIsDecidedByTheExchange is finding 1.
//
// Acquisition is the one operation that puts bytes on the wire, and it took a caller-built
// action without ever reading the obligation it emitted for. A fresh acquisition counter is
// create-only and well-formed, so nothing refused a PUBLISH for an exchange the client had
// already refused or completed, or a PUBREL for one still at its publish — the packet would
// simply be wrong.
func TestF105AcquisitionIsDecidedByTheExchange(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-acquire-rules")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	bind := mqttBindConnection(4242)

	acquire := func(txn string, kind mqttActionKind, acq uint64, n string) error {
		_, _, err := jsa.opAcquireAction(testF105WithRegister(t, e, 7, &mqttAcquireActionRequest{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Gen: auth.Gen, Tenure: auth.Tenure,
			Nonce: n, Acq: acq, Kind: kind, Binding: bind,
		}))
		return err
	}

	// AN EXCHANGE THAT DOES NOT EXIST AUTHORIZES NOTHING.
	if err := acquire("txn-ghost", mqttActionPublish, 1, mqttNewInflightNonce()); err == nil {
		t.Fatalf("an action was acquired for an exchange nothing admitted")
	}
	// A PUBREL IS NOT OWED AT THE PUBLISH PHASE: the client has transferred nothing yet.
	if err := acquire("txn-early-rel", mqttActionPubRel, 1, nonce); err == nil {
		t.Fatalf("a PUBREL was acquired for an exchange still at its publish")
	}
	// A PUBLISH IS. And the identifier comes from the exchange, not from the caller.
	if err := acquire("txn-pub", mqttActionPublish, 1, nonce); err != nil {
		t.Fatalf("acquiring the publish was refused: %v", err)
	}
	amsg, aerr := jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttActionSubject(getHash(e.id), auth.Gen, nonce, 1))
	if aerr != nil {
		t.Fatalf("the acquired action does not load back: %v", aerr)
	}
	act, derr := mqttDecodeOutboundAction(amsg.Data, e.id, e.epoch, auth.Gen, nonce, 1)
	if derr != nil {
		t.Fatalf("the acquired action does not decode: %v", derr)
	}
	if act.PI != 7 || act.Tenure != auth.Tenure || act.Kind != mqttActionPublish {
		t.Fatalf("the derived action is %+v", act)
	}

	// PAST A SUCCESSFUL PUBREC the release is owed and a further PUBLISH is not: the client
	// has taken the message.
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-rec", "op-rec",
		auth.Gen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("recording the PUBREC was refused: %v", err)
	}
	if err := acquire("txn-repub", mqttActionPublish, 2, nonce); err == nil {
		t.Fatalf("a PUBLISH was acquired after the client took ownership of the message")
	}
	if err := acquire("txn-rel", mqttActionPubRel, 2, nonce); err != nil {
		t.Fatalf("acquiring the owed release was refused: %v", err)
	}

	// AND A REFUSED EXCHANGE ACQUIRES NOTHING AT ALL.
	other := testF105AdmitOn(t, e, "txn-adm-2", 9, "inc-src")
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-rej", "op-rej",
		auth.Gen, other, mqttExchangeRejected); err != nil {
		t.Fatalf("recording the refusal was refused: %v", err)
	}
	if err := acquire("txn-after-rej", mqttActionPublish, 1, other); err == nil {
		t.Fatalf("a PUBLISH was acquired for an exchange the client refused")
	}
	if err := acquire("txn-rel-after-rej", mqttActionPubRel, 1, other); err == nil {
		t.Fatalf("a PUBREL was acquired for an exchange that never transferred ownership")
	}
}

// TestF105DiscardingHandoffFinalizesThroughTheSurface is finding 3, which was an outright
// defect rather than a gap: the derived closure left Discard false, and the kind rule refuses
// exactly that — so every clean-start handoff composed by the surface failed finalization,
// and with its metadata half committed it could not be cancelled either. The authority would
// have stayed PREPARING with admission frozen.
func TestF105DiscardingHandoffFinalizesThroughTheSurface(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-cleanstart-finalize")
	defer done()

	jsa := &e.asm.jsa
	i := testF105Intent(t, e, "clean")
	testF105Prepare(t, e, i)

	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: i.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the clean start's metadata half was refused: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin", i.ID); err != nil {
		t.Fatalf("finalizing a DISCARDING handoff was refused: %v — a clean start that "+
			"cannot finalize leaves the authority preparing and admission frozen", err)
	}

	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityServing ||
		al.rec.Gen != i.Gen || al.rec.Tenure != i.Tenure {
		t.Fatalf("the finalization installed %+v rather than the proposed tenure", al.rec)
	}
	// AND THE CLOSURE SAYS THE SESSION WAS DISCARDED. A closure is the permanent record of
	// WHY a generation ended; one that ended a clean start while saying it did not would
	// tell a later reader the session was resumable.
	c, _, st := jsa.loadGenClosure(e.id, e.epoch, i.PrevGen)
	if st != mqttOutboundValid {
		t.Fatalf("the discarded generation has no permanent closure (status %v)", st)
	}
	if !c.Discard {
		t.Fatalf("the clean start's closure does not record the discard")
	}
}

// TestF105OperationsReturnTheEvidenceToResolveThemselves is finding 4.
//
// An operation composes its transition internally, so on a lost acknowledgement the caller
// holds neither that transition nor its digest — and calling the operation again is NOT a
// retry: it reloads the state, composes a different revision, and may be refused because what
// it asks for already stands. The handle is what makes the ambiguity decidable, and it is
// returned even when the send fails.
func TestF105OperationsReturnTheEvidenceToResolveThemselves(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-op-evidence")
	defer done()

	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	out, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-rec", "op-rec",
		testF105Gen, nonce, mqttExchangePubRecObserved)
	if err != nil {
		t.Fatalf("the advance was refused: %v", err)
	}
	if out.Handle == nil {
		t.Fatalf("the operation returned no handle, so a lost acknowledgement has nothing to ask")
	}
	if v := jsa.resolveTransitionHandle(out.Handle); v != mqttTransitionLanded {
		t.Fatalf("the committed operation's handle resolves %v, want LANDED", v)
	}

	// AND IT SURVIVES INTERVENING REVISIONS, which is the whole point: a caller that comes
	// back after a restart is not asking whether the authority still stands where it left
	// it, but whether ITS attempt landed.
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-comp", "op-comp",
		testF105Gen, nonce, mqttExchangeTerminal); err != nil {
		t.Fatalf("the second advance was refused: %v", err)
	}
	if v := jsa.resolveTransitionHandle(out.Handle); v != mqttTransitionSuperseded {
		t.Fatalf("after a later revision the handle resolves %v, want SUPERSEDED", v)
	}

	// A REFUSED operation still yields its handle, and the handle says the attempt never
	// landed once it is settled — which is what a caller with an ambiguous send does.
	bad, berr := jsa.opAdvanceExchange(e.id, e.epoch, "txn-bad", "op-bad",
		testF105Gen, nonce, mqttExchangePubRecObserved)
	if berr == nil {
		t.Fatalf("an advance to a phase already passed was accepted")
	}
	if bad.Handle != nil {
		if v := jsa.resolveTransitionHandle(bad.Handle); v != mqttTransitionUnresolved {
			t.Fatalf("an unsent attempt's handle resolves %v, want UNRESOLVED", v)
		}
	}

	// The metadata half returns its frozen attempt for the same reason, and a RETRY RESENDS
	// THAT ATTEMPT: the slot is create-only, so a recomposed one would be a different
	// commit racing the first.
	i := testF105Intent(t, e, "evidence")
	testF105Prepare(t, e, i)
	f, _, merr := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: i.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	})
	if merr != nil {
		t.Fatalf("the metadata commit was refused: %v", merr)
	}
	if f == nil {
		t.Fatalf("the metadata operation returned no attempt to retry with")
	}
	if _, rerr := jsa.commitMetadataAttempt(f); rerr == nil {
		t.Fatalf("resending the frozen attempt committed a second time")
	}
	if v := jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, f.Proposal); v != mqttMetadataCommitted {
		t.Fatalf("the resent attempt's proposal resolves %v, want COMMITTED", v)
	}
}

// TestF105MetadataCommitsOnlyForAStandingProposal is finding 5.
//
// Deriving the receipt proves an attempt is internally consistent; it says nothing about
// whether the proposal was ever claimed. A caller could invent a creation proposal, supply
// the standing session sequence and a replacement body at a generation of its choosing, and
// commit over a LIVE session having reserved nothing — and a creation displaces nothing, so
// the predecessor check does not reach it either.
func TestF105MetadataCommitsOnlyForAStandingProposal(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-proposal-provenance")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")

	// AN UNCLAIMED CREATION over a serving authority is refused.
	invented := testF105CreationPropAt(t, "invented", mqttNewSessGen())
	f := testF105FreezeProposal(t, e, invented, testF105RecordSeq(t, e))
	if _, err := jsa.commitMetadataAttempt(f); err == nil {
		t.Fatalf("a creation nobody reserved committed over a live session")
	}
	if v := jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, invented); v != mqttMetadataUnresolved {
		t.Fatalf("the refused commit left verdict %v behind", v)
	}
	if al := jsa.loadOutboundAuthority(e.id, e.epoch); al.rec.Gen != auth.Gen {
		t.Fatalf("the refused commit moved the authority to %q", al.rec.Gen)
	}

	// A HANDOFF WHOSE PREPARATION DOES NOT STAND is refused the same way, even though its
	// receipt and its batch agree perfectly.
	i := testF105Intent(t, e, "unprepared")
	unprepared := testF105FreezeAttempt(t, e, i, testF105RecordSeq(t, e))
	if _, err := jsa.commitMetadataAttempt(unprepared); err == nil {
		t.Fatalf("a handoff nobody prepared committed")
	}

	// And the operation refuses to compose one at all, which is the earlier and better
	// refusal: it reads the proposal from the authority rather than from its caller.
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: "intent-nobody", SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err == nil {
		t.Fatalf("the metadata operation composed a commit for a proposal nothing carries")
	}

	testF105Prepare(t, e, i)

	// WITH A PROPOSAL STANDING, naming ANOTHER one is still refused — and this is the case
	// that discriminates: the authority now carries a proposal, so an operation that took
	// the identity from its caller would compose the standing proposal's commit under the
	// caller's name and report success for a commit it never asked for.
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: "intent-somebody-else", SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err == nil {
		t.Fatalf("the metadata operation committed the STANDING proposal for a caller that "+
			"named %q", "intent-somebody-else")
	}
	if v := jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataUnresolved {
		t.Fatalf("the misnamed request committed the standing proposal anyway (verdict %v)", v)
	}

	// The correctly named one commits, so every refusal above is the provenance and not the
	// fixture.
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: i.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the standing handoff's metadata half was refused: %v", err)
	}
}

// TestF105ResourcesAreProvisionedThroughTheSurface is finding 6.
//
// An admission may only depend on a resource that EXISTS, and nothing in the surface created
// one — so a dependency-bearing admission could only be performed by writing the record
// directly, which is the escape hatch the surface exists to remove and would put a resource
// on the store that no authority ever arbitrated.
func TestF105ResourcesAreProvisionedThroughTheSurface(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-provision")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")

	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov", "op-prov",
		auth.Gen, auth.Tenure, "inc-1", "$MQTT_src", mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning the resource was refused: %v", err)
	}
	r, _, rerr := jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-1")
	if rerr != nil {
		t.Fatalf("the provisioned resource does not load back: %v", rerr)
	}
	if r.State != mqttResourceLive || r.Deps == nil || len(r.Deps) != 0 {
		t.Fatalf("the provisioned resource is %+v, want LIVE with an empty dependent list", r)
	}

	// IT NEVER REPLACES ONE. A second provisioning of the same incarnation would reset a
	// live resource's dependents to nothing, and authorize deleting a consumer obligations
	// still need.
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-1")
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov-2", "op-prov",
		auth.Gen, auth.Tenure, "inc-1", "$MQTT_src", mqttResourceSourceConsumer); err == nil {
		t.Fatalf("a second provisioning replaced a resource an obligation depends on")
	}
	// AND THE REFUSAL IS THE BOUNDARY'S, at the sequence the store WOULD accept: a
	// hand-composed provisioning naming the resource's own sequence is refused for being a
	// replacement at all, not for losing a CAS.
	cur, curSeq, lerr := jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-1")
	if lerr != nil {
		t.Fatalf("the provisioned resource does not load back: %v", lerr)
	}
	// The substituted body is the STORED ONE, unchanged: identity, state, name and dependent
	// list all agree, so every resource-step rule is satisfied and the only thing left to
	// object to is that a provisioning is REPLACING an incarnation at all.
	body, merr := json.Marshal(cur)
	if merr != nil {
		t.Fatalf("marshalling the replacement resource failed: %v", merr)
	}
	al2 := jsa.loadOutboundAuthority(e.id, e.epoch)
	hand := testF105AdmitNothing(e, al2.rec, al2.seq, "txn-prov-hand")
	hand.Attempt.Kind = mqttTransitionProvisionResource
	hand.Extra = []mqttTransitionMember{{
		subj: mqttResourceSubject(getHash(e.id), auth.Gen, "inc-1"), expect: curSeq, body: body,
	}}
	if _, err := jsa.commitAuthorityTransition(hand); err == nil {
		t.Fatalf("a hand-composed provisioning replaced a live resource at its own sequence")
	}
	again, _, _ := jsa.loadOutboundResource(e.id, e.epoch, auth.Gen, "inc-1")
	if len(again.Deps) != 1 || again.Deps[0].Nonce != nonce {
		t.Fatalf("the refused provisioning changed the dependent list to %v", again.Deps)
	}

	// AND IT IS NEW WORK, so a closed generation provisions nothing.
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	closing := testF105Closure(e, al.rec, al.seq, "txn-close")
	if _, cerr := jsa.commitAuthorityTransition(closing); cerr != nil {
		t.Fatalf("closing the generation was refused: %v", cerr)
	}
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov-late", "op-prov",
		auth.Gen, auth.Tenure, "inc-2", "$MQTT_src", mqttResourceSourceConsumer); err == nil {
		t.Fatalf("a closed generation provisioned a new resource")
	}
}

// ---------------------------------------------------------------------------
// Review round 10
// ---------------------------------------------------------------------------

// TestF105AResumeIsNotTheSameOwnership is round 10's finding 1.
//
// Comparing the GENERATION alone looked sufficient while the fixtures all clean-started. It is
// not, and the case that shows it is the ordinary one: a RESUME PRESERVES THE GENERATION and
// changes only the tenure. A closure captured under tenure A and delayed until tenure B has
// finished resuming would match on the generation and permanently close the generation B is
// serving — which is the whole defect, reached through the one handoff shape that does not
// change the generation.
func TestF105AResumeIsNotTheSameOwnership(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-resume-ownership")
	defer done()

	jsa := &e.asm.jsa
	first, _ := e.bootstrap(t, "txn-1")
	capturedGen, capturedTenure, capturedOwner := first.Gen, first.Tenure, first.OwnerTxn

	// A RESUMING handoff: the same generation, a new tenure, and no retirement.
	ps, seq := testF105StandingSession(t, e)
	resume := &mqttHandoffIntent{
		ID: "intent-resume", PrevGen: first.Gen, PrevTenure: first.Tenure,
		PrevOwnerTxn: first.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: first.Gen, Tenure: "tenure-resumed", CleanReq: false,
	}
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-resume", resume); err != nil {
		t.Fatalf("preparing the resume was refused: %v", err)
	}
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: resume.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the resume's metadata half was refused: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin-resume", resume.ID); err != nil {
		t.Fatalf("finalizing the resume was refused: %v", err)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.Gen != capturedGen ||
		al.rec.Tenure == capturedTenure {
		t.Fatalf("the fixture did not produce a same-generation tenure change: %+v", al.rec)
	}

	// THE DELAYED CLOSURE NAMES THE OLD TENURE, and the generation it names is still the one
	// standing. It must still be refused.
	if _, err := jsa.opCloseGeneration(e.id, e.epoch, "txn-close-late",
		capturedGen, capturedTenure, capturedOwner); err == nil {
		t.Fatalf("a closure captured under tenure %q ended the generation a LATER tenure is "+
			"serving: a resume preserves the generation, so the generation alone does not "+
			"identify an ownership", capturedTenure)
	}
	if after := jsa.loadOutboundAuthority(e.id, e.epoch); after.rec.State != mqttAuthorityServing {
		t.Fatalf("the refused closure moved the authority to state %d", after.rec.State)
	}

	// The closure naming the STANDING ownership is accepted, so the refusal is the tenure
	// and not the fixture.
	if _, err := jsa.opCloseGeneration(e.id, e.epoch, "txn-close-now",
		al.rec.Gen, al.rec.Tenure, al.rec.OwnerTxn); err != nil {
		t.Fatalf("closing the standing ownership was refused: %v", err)
	}
}

// TestF105AnEmittingReleaseWaitsForItsDrain is round 10's finding 2.
//
// A PUBREL is two different things depending on what is being asked. Written to the store and
// nothing else it is a durable duty with no socket effect, and the completion rules ignore
// outstanding output for it deliberately. Acquired HERE it is bound to a connection that will
// emit bytes — so it is new output like any other, and it must wait for the transmission it
// would race.
func TestF105AnEmittingReleaseWaitsForItsDrain(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-release-drain")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	bind := mqttBindConnection(4242)

	acquire := func(txn string, acq uint64, outstanding bool) error {
		_, _, err := jsa.opAcquireAction(testF105WithRegister(t, e, 7, &mqttAcquireActionRequest{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Gen: auth.Gen, Tenure: auth.Tenure,
			Nonce: nonce, Acq: acq, Kind: mqttActionPubRel, Binding: bind,
			Outstanding: outstanding,
		}))
		return err
	}

	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-rec", "op-rec",
		auth.Gen, nonce, mqttExchangePubRecObserved); err != nil {
		t.Fatalf("recording the PUBREC was refused: %v", err)
	}
	// AT PUBREC-OBSERVED, with an earlier PUBLISH still executing.
	if err := acquire("txn-rel-outstanding", 2, true); err == nil {
		t.Fatalf("a release was acquired while an earlier transmission could still emit")
	}
	if err := acquire("txn-rel", 2, false); err != nil {
		t.Fatalf("acquiring the release after the drain was refused: %v", err)
	}

	// AND AGAIN, where the transmission it would race is an earlier RELEASE: the exchange
	// still stands at PUBREC-observed, which is where every release is owed.
	if err := acquire("txn-rel-again-outstanding", 3, true); err == nil {
		t.Fatalf("a repeated release was acquired while the first could still emit")
	}
	if err := acquire("txn-rel-again", 3, false); err != nil {
		t.Fatalf("acquiring the repeated release after the drain was refused: %v", err)
	}
}

// TestF105ActionMembersAreCheckedAgainstTheirExchange is round 10's finding 4.
//
// The operation derives the action from the exchange, which makes the SURFACE correct. It does
// not make the BOUNDARY correct, and the boundary is what the documentation claims: an action
// member's shape and create-only expectation were checked, and nothing about the obligation it
// emits for. A hand-composed action could commit for an exchange that had ended, or carry an
// identifier the client was never told.
func TestF105ActionMembersAreCheckedAgainstTheirExchange(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-action-boundary")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	// withToken puts the register token member production composes beside the record (F-105 row 2).
	// The refusals below leave it off: each is refused by the exchange check, which runs first.
	handWith := func(txn string, act *mqttOutboundAction, withToken bool) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		body, merr := json.Marshal(act)
		if merr != nil {
			t.Fatalf("marshalling the action failed: %v", merr)
		}
		tr := testF105AdmitNothing(e, al.rec, al.seq, txn)
		tr.Attempt.Kind = mqttTransitionAcquireAction
		tr.Extra = []mqttTransitionMember{{
			subj:   mqttActionSubject(getHash(e.id), act.Gen, act.Nonce, act.Acq),
			expect: 0, body: body,
		}}
		if withToken {
			tr.Extra = append(tr.Extra, testF105TokenMember(t, e, act.Gen, act))
		}
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}
	hand := func(txn string, act *mqttOutboundAction) error { return handWith(txn, act, false) }
	action := func(n string, acq uint64, pi uint16, kind mqttActionKind) *mqttOutboundAction {
		return &mqttOutboundAction{
			ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: n, Acq: acq,
			Kind: kind, PI: pi, Run: "run-1", Conn: "conn-1",
			Tenure: auth.Tenure, AcqTxn: "placeholder",
		}
	}

	// AN EXCHANGE THAT DOES NOT EXIST.
	ghost := action(mqttNewInflightNonce(), 1, 7, mqttActionPublish)
	ghost.AcqTxn = "txn-ghost"
	if err := hand("txn-ghost", ghost); err == nil {
		t.Fatalf("an action was written for an exchange nothing admitted")
	}
	// A RELEASE FOR AN EXCHANGE STILL AT ITS PUBLISH.
	early := action(nonce, 1, 7, mqttActionPubRel)
	early.AcqTxn = "txn-early-rel"
	if err := hand("txn-early-rel", early); err == nil {
		t.Fatalf("a PUBREL was written for an exchange still at its publish")
	}
	// AN IDENTIFIER THE EXCHANGE DOES NOT HOLD.
	wrongPI := action(nonce, 1, 9, mqttActionPublish)
	wrongPI.AcqTxn = "txn-wrong-pi"
	if err := hand("txn-wrong-pi", wrongPI); err == nil {
		t.Fatalf("an action carrying another identifier was written for the exchange")
	}
	// The well-formed one commits, so the refusals above are the exchange and not the
	// fixture.
	good := action(nonce, 1, 7, mqttActionPublish)
	good.AcqTxn = "txn-good"
	if err := handWith("txn-good", good, true); err != nil {
		t.Fatalf("a well-formed action was refused: %v", err)
	}

	// AND AN ENDED EXCHANGE ACQUIRES NOTHING, through the boundary as through the surface. The
	// action above holds its token, so the refusal is recorded pending-drain and ended by the
	// drain's completion: no batch ends an exchange directly beside a live holder (F-112).
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-rej-pd", "op-rej-pd",
		auth.Gen, nonce, mqttExchangeRejectedPendingDrain); err != nil {
		t.Fatalf("recording the refusal was refused: %v", err)
	}
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-rej", "op-rej",
		auth.Gen, nonce, mqttExchangeRejected); err != nil {
		t.Fatalf("recording the refusal's drain completion was refused: %v", err)
	}
	late := action(nonce, 2, 7, mqttActionPublish)
	late.AcqTxn = "txn-late"
	if err := hand("txn-late", late); err == nil {
		t.Fatalf("an action was written for an exchange the client refused")
	}
}

// TestF105MetadataProvenanceComparesTheWholeProposal is round 10's finding 3.
//
// Comparing the proposal's identity and what it PROPOSES leaves its PREDECESSOR uncovered, and
// those fields are not decoration. A receipt naming a predecessor tenure or ownership the
// standing intent does not carry passes the session-record check — that one compares the write
// identity and the generation — takes the permanent COMMITTED slot, and is then read as
// FOREIGN by every later resolution, because a receipt answers a proposal only if it answers
// all of it. Neither finalization nor cancellation could then proceed.
func TestF105MetadataProvenanceComparesTheWholeProposal(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-provenance-whole")
	defer done()

	jsa := &e.asm.jsa
	i := testF105Intent(t, e, "whole")
	testF105Prepare(t, e, i)

	for name, mutate := range map[string]func(*mqttHandoffIntent){
		"another predecessor tenure":   func(m *mqttHandoffIntent) { m.PrevTenure = "tenure-invented" },
		"another predecessor owner":    func(m *mqttHandoffIntent) { m.PrevOwnerTxn = "txn-invented" },
		"another predecessor sequence": func(m *mqttHandoffIntent) { m.PrevSessSeq = m.PrevSessSeq - 1 },
	} {
		m := *i
		retire := *i.Retire
		m.Retire = &retire
		mutate(&m)
		m.Retire.Tenure, m.Retire.OwnerTxn = m.PrevTenure, m.PrevOwnerTxn
		m.Retire.SessSeq = m.PrevSessSeq
		// The substitution is COHERENT: the attempt's receipt is derived from the mutated
		// proposal, so nothing internal to it disagrees. Only the standing intent does.
		f, ferr := testF105TryFreeze(t, e, &m, m.PrevSessSeq)
		if ferr != nil {
			t.Fatalf("freezing the %s attempt failed: %v", name, ferr)
		}
		if _, cerr := jsa.commitMetadataAttempt(f); cerr == nil {
			t.Fatalf("a metadata half naming %s committed for a standing proposal that names "+
				"something else", name)
		}
		if v := jsa.resolveMetadataAttempt(_EMPTY_, e.id, e.epoch, testF105Prop(t, i)); v != mqttMetadataUnresolved {
			t.Fatalf("the refused %s attempt left verdict %v on the standing proposal", name, v)
		}
	}

	// The unmutated attempt commits and the standing proposal can then be finalized, which
	// is the state the substitutions would have made unreachable.
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: i.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the standing proposal's metadata half was refused: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin", i.ID); err != nil {
		t.Fatalf("finalizing the committed proposal was refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Review round 11 — what a CREATE-ONLY member was never asked
// ---------------------------------------------------------------------------

// TestF105CreatedExchangesStartAtTheirPublish is round 11's finding 2.
//
// A create-only member had "nothing to compare against", which is exactly what made it the way
// round the protocol: the successor relation is consulted for an ADVANCE, so an admission whose
// exchange body says TERMINAL simply commits. A dependency release then accepts that fabricated
// ending and authorizes deleting a consumer the client's real obligation still needs. A phase
// relation is only a protocol if the FIRST phase is enforced too.
func TestF105CreatedExchangesStartAtTheirPublish(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-created-exchange")
	defer done()

	jsa := &e.asm.jsa
	auth, authSeq := e.bootstrap(t, "txn-1")

	admit := func(txn string, mutate func(*mqttOutboundExchange)) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: 7, QoS: 2, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		mutate(x)
		tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
		if terr != nil {
			// The builder refuses some of these itself, which is not the property under
			// test: the member is then composed by hand at the same subject.
			body, merr := json.Marshal(x)
			if merr != nil {
				t.Fatalf("marshalling the exchange failed: %v", merr)
			}
			tr = testF105AdmitNothing(e, al.rec, al.seq, txn)
			tr.Extra = []mqttTransitionMember{{
				subj:   mqttExchangeSubject(getHash(e.id), x.Gen, x.Nonce),
				expect: 0, body: body,
			}}
		}
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}

	// Minted transition ids, not slices of the case name: two cases sharing an id would make
	// the second lose on its outcome subject rather than on the body under test, and an id
	// carrying a space is refused by the attempt's own shape check.
	for _, c := range []struct {
		name   string
		mutate func(*mqttOutboundExchange)
	}{
		{"already terminal", func(x *mqttOutboundExchange) { x.Phase = mqttExchangeTerminal }},
		{"already refused", func(x *mqttOutboundExchange) { x.Phase = mqttExchangeRejected }},
		{"already past its PUBREC", func(x *mqttOutboundExchange) { x.Phase = mqttExchangePubRecObserved }},
		{"admitted by another transition", func(x *mqttOutboundExchange) { x.AdmitTxn = "txn-somebody-else" }},
		{"admitted under another generation", func(x *mqttOutboundExchange) { x.Gen = testF105Gen2 }},
		{"admitted under another tenure", func(x *mqttOutboundExchange) { x.Tenure = "tenure-invented" }},
	} {
		if err := admit("txn-bad-"+mqttNewInflightNonce(), c.mutate); err == nil {
			t.Fatalf("an exchange %s was admitted", c.name)
		}
	}

	// The well-formed admission commits, so every refusal above is the body and not the
	// fixture.
	if err := admit("txn-good", func(*mqttOutboundExchange) {}); err != nil {
		t.Fatalf("a well-formed admission was refused: %v", err)
	}
	_ = auth
	_ = authSeq
}

// TestF105AnActionIsLegalAgainstWhatItsBatchLeaves is round 11's finding 1.
//
// checkActionMembers read the STORE, which is the state before the batch. A batch is atomic, so
// what an action has to be legal against is the state the batch RESULTS IN — and the two differ
// exactly when one batch both acquires an action and advances that exchange. Each half is legal
// alone; together they install emitting output for an obligation the same batch ended, and the
// release that follows is then entitled to delete the consumer under it.
func TestF105AnActionIsLegalAgainstWhatItsBatchLeaves(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-action-batch-state")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	cur, curSeq := testF105LoadExchange(t, e, testF105Gen, nonce)
	ended := *cur
	ended.Phase = mqttExchangeRejected
	xbody, xerr := json.Marshal(&ended)
	if xerr != nil {
		t.Fatalf("marshalling the advanced exchange failed: %v", xerr)
	}
	act := &mqttOutboundAction{
		ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: nonce, Acq: 1,
		Kind: mqttActionPublish, PI: cur.PI, Run: "run-1", Conn: "conn-1",
		Tenure: auth.Tenure, AcqTxn: "txn-both",
	}
	abody, aerr := json.Marshal(act)
	if aerr != nil {
		t.Fatalf("marshalling the action failed: %v", aerr)
	}

	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	both := testF105AdmitNothing(e, al.rec, al.seq, "txn-both")
	both.Attempt.Kind = mqttTransitionAcquireAction
	both.Extra = []mqttTransitionMember{
		{subj: mqttExchangeSubject(getHash(e.id), auth.Gen, nonce), expect: curSeq, body: xbody},
		{subj: mqttActionSubject(getHash(e.id), auth.Gen, nonce, 1), expect: 0, body: abody},
	}
	if _, err := jsa.commitAuthorityTransition(both); err == nil {
		t.Fatalf("a batch acquired emitting output beside the advance that ended its exchange")
	}
	// Neither half landed: the refusal is the whole batch.
	after, _ := testF105LoadExchange(t, e, testF105Gen, nonce)
	if after.Phase != mqttExchangePublish {
		t.Fatalf("the refused batch advanced the exchange to phase %d", after.Phase)
	}
	if _, err := jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttActionSubject(getHash(e.id), auth.Gen, nonce, 1)); err == nil {
		t.Fatalf("the refused batch installed its action")
	}

	// The same action WITHOUT the advance is legal, so the refusal is the combination.
	alone := testF105AdmitNothing(e, al.rec, al.seq, "txn-alone")
	alone.Attempt.Kind = mqttTransitionAcquireAction
	solo := *act
	solo.AcqTxn = "txn-alone"
	sbody, serr := json.Marshal(&solo)
	if serr != nil {
		t.Fatalf("marshalling the lone action failed: %v", serr)
	}
	alone.Extra = []mqttTransitionMember{
		{subj: mqttActionSubject(getHash(e.id), auth.Gen, nonce, 1), expect: 0, body: sbody},
		testF105TokenMember(t, e, auth.Gen, &solo),
	}
	if _, err := jsa.commitAuthorityTransition(alone); err != nil {
		t.Fatalf("an acquisition for an exchange still at its publish was refused: %v", err)
	}
}

// TestF105CreatedResourcesOweNothing is round 11's finding 3.
//
// A create-only resource skipped the state machine entirely. A hand-composed provisioning could
// install a resource already naming a dependent obligation that does not exist — which no
// release can ever discharge, because a release has to prove that obligation ended — or one
// already DELETE_AUTHORIZED, which is the authorization this mechanism exists to make provable.
// And because disposal is deliberately permitted after closure, a creation carried by a
// disposal would provision under a closed generation through the other kind.
func TestF105CreatedResourcesOweNothing(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-created-resource")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")

	provision := func(txn string, kind mqttTransitionKind, mutate func(*mqttOutboundResource)) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		r := &mqttOutboundResource{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Inc: "inc-" + txn,
			Kind: mqttResourceSourceConsumer, Name: "$MQTT_src",
			State: mqttResourceLive, Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{},
			ProvTenure: al.rec.Tenure,
		}
		mutate(r)
		body, merr := json.Marshal(r)
		if merr != nil {
			t.Fatalf("marshalling the resource failed: %v", merr)
		}
		tr := testF105AdmitNothing(e, al.rec, al.seq, txn)
		tr.Attempt.Kind = kind
		// A PROVISIONING OWES BOTH OF ITS DUTIES — the operation reserves them together, and
		// this fixture builds its member by hand so it reserves them by hand too. The cleanup
		// entry starts NOT_YET_ACTIONABLE because nothing depends on the resource yet.
		led, ledSeq, lerr := jsa.loadGenLedger(e.id, e.epoch, r.Gen)
		if lerr != nil {
			t.Fatalf("reading the ledger: %v", lerr)
		}
		inv, ierr := mqttAllocateInventoryMembers(e.id, e.epoch, r.Gen, led, ledSeq,
			[]mqttDutyDeclaration{
				{Kind: mqttDutyProvision, Ref: r.Inc, State: mqttDutyPending},
				{Kind: mqttDutyCleanup, Ref: r.Inc, State: mqttDutyNotYetActionable},
			})
		if ierr != nil {
			t.Fatalf("allocating the provisioning inventory: %v", ierr)
		}
		tr.Extra = append([]mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(e.id), r.Gen, r.Inc),
			expect: 0, body: body,
		}}, inv...)
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}

	// The transition ids are minted rather than sliced out of the case name: an id carrying a
	// space is refused by the attempt's own shape check, which would make every case here
	// pass for a reason that has nothing to do with the resource.
	for _, c := range []struct {
		name   string
		mutate func(*mqttOutboundResource)
	}{
		{"already needed by an obligation", func(r *mqttOutboundResource) {
			r.Deps = []mqttResourceEdge{{
				Nonce: mqttNewInflightNonce(), Role: mqttRoleSourceReplay,
			}}
		}},
		{"already authorized for deletion", func(r *mqttOutboundResource) {
			r.State, r.AuthTxn = mqttResourceDeleteAuthorized, "txn-invented"
		}},
		{"already cleaned up", func(r *mqttOutboundResource) {
			r.State, r.AuthTxn, r.Outcome = mqttResourceCleanupComplete, "txn-invented", mqttCleanupRemoved
		}},
	} {
		if err := provision("txn-bad-"+mqttNewInflightNonce(), mqttTransitionProvisionResource, c.mutate); err == nil {
			t.Fatalf("a resource %s was provisioned", c.name)
		}
	}

	// AND ONLY PROVISIONING PROVISIONS. A disposal carrying a creation would reach the same
	// write under a kind that is deliberately permitted after closure.
	if err := provision("txn-by-disposal", mqttTransitionDisposeResource, func(*mqttOutboundResource) {}); err == nil {
		t.Fatalf("a dispose-resource transition created a resource")
	}
	// The well-formed provisioning commits.
	if err := provision("txn-good", mqttTransitionProvisionResource, func(*mqttOutboundResource) {}); err != nil {
		t.Fatalf("a well-formed provisioning was refused: %v", err)
	}
	_ = auth
}

// TestF105AClosedGenerationIsNeverClaimedAgain is round 11's finding 4.
//
// The kind rules compare a claim with its IMMEDIATE predecessor, and a permanent closure
// outlives every predecessor it had. So after G1 was closed, G2 served and G2 closed, nothing
// stopped a claim reserving G1 again with fresh transition and proposal identities: its
// metadata half commits, its finalization installs SERVING at G1, and new work enters a
// generation whose permanent closure says it admits none — after which the next closure fails
// outright, because that slot is already taken.
func TestF105AClosedGenerationIsNeverClaimedAgain(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-generation-reuse")
	defer done()

	jsa := &e.asm.jsa
	g1, g1Seq := e.bootstrap(t, "txn-1")

	closing := testF105Closure(e, g1, g1Seq, "txn-close-g1")
	closeSeq, cerr := jsa.commitAuthorityTransition(closing)
	if cerr != nil {
		t.Fatalf("closing G1 was refused: %v", cerr)
	}
	g2, g2Seq := e.claim(t, closing.Auth, closeSeq, "txn-g2", testF105Gen2, "tenure-g2")
	closing2 := testF105Closure(e, g2, g2Seq, "txn-close-g2")
	if _, err := jsa.commitAuthorityTransition(closing2); err != nil {
		t.Fatalf("closing G2 was refused: %v", err)
	}

	// THE HISTORICAL GENERATION IS STILL CLOSED. Its immediate predecessor is now G2's
	// closure, so nothing local to the claim notices.
	if _, err := jsa.opClaimClient(e.id, e.epoch, "txn-again", g1.Gen,
		"tenure-again", "intent-again"); err == nil {
		t.Fatalf("a claim reserved generation %q, which was permanently closed two "+
			"generations ago", g1.Gen)
	}
	if al := jsa.loadOutboundAuthority(e.id, e.epoch); al.rec.State != mqttAuthorityClosed {
		t.Fatalf("the refused claim moved the authority to state %d", al.rec.State)
	}

	// A genuinely fresh generation is claimable, so the refusal is the history and not the
	// closed predecessor.
	fresh, _ := e.claim(t, closing2.Auth, 0, "txn-g3", mqttNewSessGen(), "tenure-g3")
	if fresh.State != mqttAuthorityServing {
		t.Fatalf("a fresh generation could not be claimed after two closures")
	}

	// AND A CLEAN START MAY NOT PROPOSE ONE EITHER: a preparation names the generation it
	// will install, and that is a claim on it.
	ps, seq := testF105StandingSession(t, e)
	reuse := &mqttHandoffIntent{
		ID: "intent-reuse", PrevGen: fresh.Gen, PrevTenure: fresh.Tenure,
		PrevOwnerTxn: fresh.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: g1.Gen, Tenure: "tenure-reuse", CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: fresh.Gen, Tenure: fresh.Tenure, OwnerTxn: fresh.OwnerTxn,
			SessWID: ps.WID, SessSeq: seq, Reason: mqttRetireCleanStart, Discard: true,
		},
	}
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-reuse", reuse); err == nil {
		t.Fatalf("a clean start proposed the permanently closed generation %q", g1.Gen)
	}
}

// ---------------------------------------------------------------------------
// Review round 12
// ---------------------------------------------------------------------------

// TestF105NewWorkIsCarriedOnlyByTheKindThatAdmitsIt is round 12's finding 1.
//
// The kind rules confine members for every kind that has a member rule, and the OWNERSHIP
// kinds have none. So a close-admission — including the one an abandoned claim must make over
// a reservation whose metadata half was ABORTED — could carry a fresh exchange and its action:
// a generation would acquire emitting output in the very transaction that closes it
// permanently.
//
// The two restrictions are exercised SEPARATELY. A batch carrying both is refused by either
// one, which would let a mutation of one hide behind the other.
func TestF105NewWorkIsCarriedOnlyByTheKindThatAdmitsIt(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closure-carries-work")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	standing := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	closure := func(txn string, extra []mqttTransitionMember) error {
		cur := jsa.loadOutboundAuthority(e.id, e.epoch)
		tr := testF105Closure(e, cur.rec, cur.seq, txn)
		// APPENDED, not assigned: the fixture's closure now carries its own enrolment — every
		// closure seals and roots — and overwriting Extra would leave a closing batch the
		// boundary refuses for a reason that has nothing to do with the member under test.
		tr.Extra = append(tr.Extra, extra...)
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}

	// (1) A CLOSURE CARRYING A FRESH EXCHANGE. Everything about the body is right — its
	// phase, its admitting transition, its generation and tenure — so only the kind can
	// refuse it.
	fresh := mqttNewInflightNonce()
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: fresh,
		PI: 9, QoS: 2, Phase: mqttExchangePublish,
		AdmitTxn: "txn-close-adm", Tenure: auth.Tenure,
	}
	xbody, xerr := json.Marshal(x)
	if xerr != nil {
		t.Fatalf("marshalling the exchange failed: %v", xerr)
	}
	if err := closure("txn-close-adm", []mqttTransitionMember{
		{subj: mqttExchangeSubject(getHash(e.id), auth.Gen, fresh), expect: 0, body: xbody},
	}); err == nil {
		t.Fatalf("a closure admitted a new obligation for the generation it was ending")
	}

	// (2) A CLOSURE CARRYING AN ACTION for an obligation that really stands, at a phase that
	// really permits a PUBLISH, attributed to the closure itself. Again only the kind can
	// refuse it.
	act := &mqttOutboundAction{
		ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: standing, Acq: 1,
		Kind: mqttActionPublish, PI: 7, Run: "run-1", Conn: "conn-1",
		Tenure: auth.Tenure, AcqTxn: "txn-close-acq",
	}
	abody, aerr := json.Marshal(act)
	if aerr != nil {
		t.Fatalf("marshalling the action failed: %v", aerr)
	}
	if err := closure("txn-close-acq", []mqttTransitionMember{
		{subj: mqttActionSubject(getHash(e.id), auth.Gen, standing, 1), expect: 0, body: abody},
	}); err == nil {
		t.Fatalf("a closure acquired emitting output for the generation it was ending")
	}

	// Nothing landed, and the authority did not move.
	if _, err := jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttExchangeSubject(getHash(e.id), auth.Gen, fresh)); err == nil {
		t.Fatalf("the refused closure installed its exchange")
	}
	if _, err := jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttActionSubject(getHash(e.id), auth.Gen, standing, 1)); err == nil {
		t.Fatalf("the refused closure installed its action")
	}
	if cur := jsa.loadOutboundAuthority(e.id, e.epoch); cur.rec.State != mqttAuthorityServing {
		t.Fatalf("a refused closure moved the authority to state %d", cur.rec.State)
	}

	// (3) THE CLOSURE ALONE SUCCEEDS, so what was refused is the work it carried.
	if err := closure("txn-close-alone", nil); err != nil {
		t.Fatalf("the closure carrying nothing was refused: %v", err)
	}
	if _, _, st := jsa.loadGenClosure(e.id, e.epoch, auth.Gen); st != mqttOutboundValid {
		t.Fatalf("the closed generation has no permanent closure (status %v)", st)
	}
}

// TestF105AnActionIsAttributedToWhatAcquiredIt is round 12's finding 2.
//
// An action's acquiring transition and tenure are immutable evidence a captured completion
// handler resolves against, and the decoder checks only that they are well-formed tokens. A
// hand-composed action could therefore name any transition and any tenure while committing
// under the current authority — so after a resume from one tenure to the next, an action
// claiming the DISPLACED tenure would be permanent evidence attributed to ownership that never
// authorized it.
func TestF105AnActionIsAttributedToWhatAcquiredIt(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-action-attribution")
	defer done()

	jsa := &e.asm.jsa
	first, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	displaced := first.Tenure

	// A RESUME: the same generation, a new tenure.
	ps, seq := testF105StandingSession(t, e)
	resume := &mqttHandoffIntent{
		ID: "intent-resume", PrevGen: first.Gen, PrevTenure: first.Tenure,
		PrevOwnerTxn: first.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: first.Gen, Tenure: "tenure-resumed", CleanReq: false,
	}
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-resume", resume); err != nil {
		t.Fatalf("preparing the resume was refused: %v", err)
	}
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: resume.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the resume's metadata half was refused: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin-resume", resume.ID); err != nil {
		t.Fatalf("finalizing the resume was refused: %v", err)
	}

	hand := func(txn string, acq uint64, mutate func(*mqttOutboundAction)) error {
		cur := jsa.loadOutboundAuthority(e.id, e.epoch)
		act := &mqttOutboundAction{
			ID: e.id, Epoch: e.epoch, Gen: cur.rec.Gen, Nonce: nonce, Acq: acq,
			Kind: mqttActionPublish, PI: 7, Run: "run-1", Conn: "conn-1",
			Tenure: cur.rec.Tenure, AcqTxn: txn,
		}
		mutate(act)
		body, merr := json.Marshal(act)
		if merr != nil {
			t.Fatalf("marshalling the action failed: %v", merr)
		}
		tr := testF105AdmitNothing(e, cur.rec, cur.seq, txn)
		tr.Attempt.Kind = mqttTransitionAcquireAction
		tr.Extra = []mqttTransitionMember{{
			subj:   mqttActionSubject(getHash(e.id), act.Gen, act.Nonce, act.Acq),
			expect: 0, body: body,
		}}
		tr.Extra = append(tr.Extra, testF105TokenMember(t, e, act.Gen, act))
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}

	// THE DISPLACED TENURE, which is well-formed and is not this transition's.
	if err := hand("txn-attr-1", 1, func(act *mqttOutboundAction) { act.Tenure = displaced }); err == nil {
		t.Fatalf("an action was acquired naming the tenure a resume displaced")
	}
	// AND A TRANSITION THAT DID NOT ACQUIRE IT.
	if err := hand("txn-attr-2", 2, func(act *mqttOutboundAction) { act.AcqTxn = "txn-somebody-else" }); err == nil {
		t.Fatalf("an action was acquired naming a transition that did not acquire it")
	}
	// The correctly attributed one commits, so both refusals are the attribution.
	if err := hand("txn-attr-3", 3, func(*mqttOutboundAction) {}); err != nil {
		t.Fatalf("a correctly attributed action was refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Review round 13
// ---------------------------------------------------------------------------

// TestF105AnAdmissionsRecordsDescribeOneObligation is round 13's finding.
//
// The builder binds the exchange to its register — same identifier, same QoS, the exchange's
// nonce IS the register's lineage — and that is what made it a caller convention: every
// binding a batch states about itself still holds for a body substituted after the builder
// ran. Change only the exchange's identifier and both records still pass their own validation,
// and the batch installs an obligation whose allocation disagrees with the register recovery
// will read: an acquisition derives its identifier from the exchange, recovery derives the
// slot from the register, and one obligation holds two identifiers with neither side able to
// tell.
func TestF105AnAdmissionsRecordsDescribeOneObligation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-pair-binding")
	defer done()

	jsa := &e.asm.jsa
	auth, authSeq := e.bootstrap(t, "txn-1")

	// Each case starts from a batch the BUILDER produced and substitutes one member
	// afterwards, which is exactly the shape a builder cannot defend against.
	admit := func(txn string, mutate func(*mqttAuthorityTransition)) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		adm := testF105Admission(e, al.rec, al.seq, txn, 7, nil)
		tr, terr := mqttAdmitPublishTransition(adm)
		if terr != nil {
			t.Fatalf("assembling the admission failed: %v", terr)
		}
		mutate(tr)
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}
	rewriteExchange := func(tr *mqttAuthorityTransition, f func(*mqttOutboundExchange)) {
		for i, m := range tr.Extra {
			cls, ok := mqttF105ObligationClass(m.subj)
			if !ok || cls != mqttExchangeClassTk {
				continue
			}
			toks := strings.Split(m.subj[len(mqttF105SubjectPrefix()):], ".")
			x, derr := mqttDecodeOutboundExchange(m.body, e.id, e.epoch, toks[2], toks[3])
			if derr != nil {
				t.Fatalf("decoding the assembled exchange failed: %v", derr)
			}
			f(x)
			body, merr := json.Marshal(x)
			if merr != nil {
				t.Fatalf("marshalling the substituted exchange failed: %v", merr)
			}
			tr.Extra[i].body = body
			return
		}
		t.Fatalf("the assembled admission carries no exchange member")
	}

	if err := admit("txn-pi", func(tr *mqttAuthorityTransition) {
		rewriteExchange(tr, func(x *mqttOutboundExchange) { x.PI = 9 })
	}); err == nil {
		t.Fatalf("an exchange allocating another identifier was admitted beside its register")
	}
	if err := admit("txn-qos", func(tr *mqttAuthorityTransition) {
		rewriteExchange(tr, func(x *mqttOutboundExchange) { x.QoS = 1 })
	}); err == nil {
		t.Fatalf("an exchange at another QoS was admitted beside its register")
	}
	// The lineage case moves the SUBJECT with the body, so the exchange is internally
	// consistent and only its disagreement with the register is left to refuse it.
	if err := admit("txn-lin", func(tr *mqttAuthorityTransition) {
		fresh := mqttNewInflightNonce()
		for i, m := range tr.Extra {
			cls, ok := mqttF105ObligationClass(m.subj)
			if !ok || cls != mqttExchangeClassTk {
				continue
			}
			toks := strings.Split(m.subj[len(mqttF105SubjectPrefix()):], ".")
			x, derr := mqttDecodeOutboundExchange(m.body, e.id, e.epoch, toks[2], toks[3])
			if derr != nil {
				t.Fatalf("decoding the assembled exchange failed: %v", derr)
			}
			x.Nonce = fresh
			body, merr := json.Marshal(x)
			if merr != nil {
				t.Fatalf("marshalling the substituted exchange failed: %v", merr)
			}
			tr.Extra[i].subj = mqttExchangeSubject(getHash(e.id), x.Gen, fresh)
			tr.Extra[i].body = body
			return
		}
		t.Fatalf("the assembled admission carries no exchange member")
	}); err == nil {
		t.Fatalf("an exchange keyed on another lineage was admitted beside its register")
	}

	// AND THE MARKER NAMES ITS REGISTER'S ALLOCATION. The pair's whole meaning is that the
	// marker's presence says THIS allocation has transmitted nothing.
	if err := admit("txn-marker", func(tr *mqttAuthorityTransition) {
		for i, m := range tr.Extra {
			if _, merr := mqttParseInflightMarkerSubject(m.subj); merr != nil {
				continue
			}
			body, berr := mqttBuildInflightMarker(mqttNewInflightNonce())
			if berr != nil {
				t.Fatalf("building the substituted marker failed: %v", berr)
			}
			tr.Extra[i].body = body
			return
		}
		t.Fatalf("the assembled admission carries no marker member")
	}); err == nil {
		t.Fatalf("a marker naming another allocation was admitted beside its register")
	}

	// The unmutated admission commits, so every refusal above is the binding.
	if err := admit("txn-good", func(*mqttAuthorityTransition) {}); err != nil {
		t.Fatalf("the unmutated admission was refused: %v", err)
	}

	// A REGISTERLESS ADMISSION IS STILL SUPPORTED: an exchange with no register in its batch
	// has nothing to disagree with, and that route is what an obligation with no slot uses.
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	lone := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
		PI: 11, QoS: 2, Phase: mqttExchangePublish,
		AdmitTxn: "txn-lone", Tenure: al.rec.Tenure,
	}
	tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, "txn-lone", "op-lone", lone)
	if terr != nil {
		t.Fatalf("assembling the registerless admission failed: %v", terr)
	}
	if _, err := jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("a registerless admission was refused: %v", err)
	}
	_ = auth
	_ = authSeq
}

// ---------------------------------------------------------------------------
// Review round 14
// ---------------------------------------------------------------------------

// TestF105ARegisterSaysWhatItsSubjectNames is round 14's finding.
//
// A slot is NAMED by its subject and SAYS what it is in its body, and nothing required the two
// to agree. Moving a valid admission's register and marker to an unused identifier while
// leaving every body untouched passes each member's own validation and passes the pair binding
// too — the register and the exchange still agree with each other, because both were left
// saying the old identifier.
//
// The record that lands is one nobody can use: recovery treats a subject/body disagreement as
// unreadable and retirement declines to delete what it cannot interpret, so the allocation is
// stranded for the whole recovery epoch. The operation derives the subject from the body and
// cannot produce it; a member composed some other way can.
func TestF105ARegisterSaysWhatItsSubjectNames(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-register-subject")
	defer done()

	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")

	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	adm := testF105Admission(e, al.rec, al.seq, "txn-moved", 7, nil)
	tr, terr := mqttAdmitPublishTransition(adm)
	if terr != nil {
		t.Fatalf("assembling the admission failed: %v", terr)
	}
	// THE BODIES ARE UNTOUCHED; only the subjects move, to an identifier nothing occupies.
	moved := mqttInflightSubject(getHash(e.id), testF105Gen, 8)
	for i, m := range tr.Extra {
		if _, perr := mqttParseInflightSubject(m.subj); perr == nil {
			tr.Extra[i].subj = moved
			continue
		}
		if _, merr := mqttParseInflightMarkerSubject(m.subj); merr == nil {
			tr.Extra[i].subj = moved + "." + mqttInflightMarkerSuffix
		}
	}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("a register whose body allocates 7 was stored under the subject for 8")
	}
	// NOTHING DURABLE HAPPENED, at either identifier.
	for _, subj := range []string{moved, mqttInflightSubject(getHash(e.id), testF105Gen, 7)} {
		if _, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj); err == nil {
			t.Fatalf("the refused admission installed a register on %q", subj)
		}
	}
	if after := jsa.loadOutboundAuthority(e.id, e.epoch); after.seq != al.seq {
		t.Fatalf("the refused admission advanced the authority")
	}

	// The same admission with its subjects left alone commits, so the refusal is the
	// disagreement and not the fixture.
	good := testF105Admission(e, al.rec, al.seq, "txn-kept", 7, nil)
	gtr, gerr := mqttAdmitPublishTransition(good)
	if gerr != nil {
		t.Fatalf("assembling the unmoved admission failed: %v", gerr)
	}
	if _, err := jsa.commitAuthorityTransition(gtr); err != nil {
		t.Fatalf("the unmoved admission was refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Review round 16
// ---------------------------------------------------------------------------

// TestF105AnObligationHoldsOneAllocation is round 16's finding.
//
// The pair binding matched an exchange against a register carrying its lineage — and picked
// that register out of a Go map, whose iteration order is not a rule. A batch carrying TWO
// valid register/marker pairs at different identifiers, both naming one lineage, therefore
// passed whenever the matching one happened to be visited first: subject uniqueness, every
// register body, and the marker rules all hold, because the defect is reached by ADDING a
// member rather than substituting one.
//
// What lands is two recovery allocations for a single obligation, while the exchange ledger —
// and the dependency accounting that rests on it — represents one.
func TestF105AnObligationHoldsOneAllocation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-one-allocation")
	defer done()

	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")

	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	adm := testF105Admission(e, al.rec, al.seq, "txn-two-regs", 7, nil)
	tr, terr := mqttAdmitPublishTransition(adm)
	if terr != nil {
		t.Fatalf("assembling the admission failed: %v", terr)
	}
	// A SECOND, INDIVIDUALLY VALID PAIR at another identifier, carrying the SAME lineage.
	second := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 9, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	second.Lin = adm.Exchange.Nonce
	sbody, serr := json.Marshal(second)
	if serr != nil {
		t.Fatalf("marshalling the second register failed: %v", serr)
	}
	smarker, mkerr := mqttBuildInflightMarker(second.Nonce)
	if mkerr != nil {
		t.Fatalf("building the second marker failed: %v", mkerr)
	}
	ninth := mqttInflightSubject(getHash(e.id), testF105Gen, 9)
	// The extra pair goes FIRST, so the register the exchange agrees with is the one a
	// last-write-wins lookup would keep. A fixture that appended it would be refused by the
	// identifier comparison instead, and would prove nothing about the duplication.
	tr.Extra = append([]mqttTransitionMember{
		{subj: ninth, expect: 0, body: sbody},
		{subj: ninth + "." + mqttInflightMarkerSuffix, expect: 0, body: smarker},
	}, tr.Extra...)
	// REGARDLESS OF MAP ORDER: the refusal is about the pair of registers, so running it
	// repeatedly is the test, not a single attempt that might have been lucky.
	for i := 0; i < 8; i++ {
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("a batch carrying two allocations for one obligation was admitted")
		}
	}
	for _, subj := range []string{ninth, mqttInflightSubject(getHash(e.id), testF105Gen, 7)} {
		if _, err := jsa.loadLastMsgFor(mqttInflightStreamName, subj); err == nil {
			t.Fatalf("the refused admission installed a register on %q", subj)
		}
	}

	// The single-pair admission commits, so the refusal is the duplication.
	good := testF105Admission(e, al.rec, al.seq, "txn-one-reg", 7, nil)
	gtr, gerr := mqttAdmitPublishTransition(good)
	if gerr != nil {
		t.Fatalf("assembling the single admission failed: %v", gerr)
	}
	if _, err := jsa.commitAuthorityTransition(gtr); err != nil {
		t.Fatalf("an admission carrying one allocation was refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Review round 17
// ---------------------------------------------------------------------------

// TestF105OneAcquisitionPerObligationInABatch is round 17's finding.
//
// Two actions for one exchange land TOGETHER and are therefore executable together, which is
// precisely the serialization the acquisition rules impose: another transmission waits while
// an acquired action may still emit. Their subjects differ — the acquisition counter is part
// of the name — so subject deduplication passes, and each is individually legal against the
// same phase. The composing operation refuses this by construction; the boundary did not.
func TestF105OneAcquisitionPerObligationInABatch(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-one-acquisition")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	action := func(txn string, acq uint64) mqttTransitionMember {
		act := &mqttOutboundAction{
			ID: e.id, Epoch: e.epoch, Gen: auth.Gen, Nonce: nonce, Acq: acq,
			Kind: mqttActionPublish, PI: 7, Run: "run-1", Conn: "conn-1",
			Tenure: auth.Tenure, AcqTxn: txn,
		}
		body, merr := json.Marshal(act)
		if merr != nil {
			t.Fatalf("marshalling the action failed: %v", merr)
		}
		return mqttTransitionMember{
			subj:   mqttActionSubject(getHash(e.id), auth.Gen, nonce, acq),
			expect: 0, body: body,
		}
	}
	commit := func(txn string, members ...mqttTransitionMember) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		tr := testF105AdmitNothing(e, al.rec, al.seq, txn)
		tr.Attempt.Kind = mqttTransitionAcquireAction
		tr.Extra = members
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}
	// The register token member production composes beside the one acquisition (F-105 row 2).
	token := func(acq uint64) mqttTransitionMember {
		return testF105TokenMember(t, e, auth.Gen, &mqttOutboundAction{Nonce: nonce, Acq: acq, Kind: mqttActionPublish, PI: 7})
	}

	if err := commit("txn-two", action("txn-two", 1), action("txn-two", 2)); err == nil {
		t.Fatalf("a batch acquired two emitting actions for one obligation")
	}
	for _, acq := range []uint64{1, 2} {
		if _, err := jsa.loadLastMsgFor(mqttInflightStreamName,
			mqttActionSubject(getHash(e.id), auth.Gen, nonce, acq)); err == nil {
			t.Fatalf("the refused batch installed its action at acquisition %d", acq)
		}
	}

	// ONE commits, so the refusal is the pair and not the member.
	if err := commit("txn-one", action("txn-one", 1), token(1)); err != nil {
		t.Fatalf("a batch acquiring one action was refused: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Review round 18
// ---------------------------------------------------------------------------

// TestF105OneIdentifierPerBatchAdmission is round 18's finding.
//
// Two exchanges admitted together at one identifier are two outstanding messages a client can
// only answer ambiguously: an acknowledgement names the identifier and nothing else. Their
// nonces differ, so their subjects differ and every other rule is satisfied — and a
// REGISTERLESS admission has no register to collide with either, which is the shape that
// reaches it. Each member is legal; the pair is not.
func TestF105OneIdentifierPerBatchAdmission(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-one-identifier")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")

	exchange := func(txn string, pi uint16) mqttTransitionMember {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		body, merr := json.Marshal(x)
		if merr != nil {
			t.Fatalf("marshalling the exchange failed: %v", merr)
		}
		return mqttTransitionMember{
			subj:   mqttExchangeSubject(getHash(e.id), x.Gen, x.Nonce),
			expect: 0, body: body,
		}
	}
	commit := func(txn string, members ...mqttTransitionMember) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		tr := testF105AdmitNothing(e, al.rec, al.seq, txn)
		tr.Extra = testF105CoverExchanges(t, e, testF105Gen, members)
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}

	if err := commit("txn-collide",
		exchange("txn-collide", 7), exchange("txn-collide", 7)); err == nil {
		t.Fatalf("two obligations were admitted together on one packet identifier")
	}
	// DISTINCT IDENTIFIERS ARE FINE, so what was refused is the collision and not the pair.
	if err := commit("txn-distinct",
		exchange("txn-distinct", 7), exchange("txn-distinct", 8)); err != nil {
		t.Fatalf("two obligations on distinct identifiers were refused: %v", err)
	}
	_ = auth
}

// ---------------------------------------------------------------------------
// Review round 19
// ---------------------------------------------------------------------------

// TestF105AnAdmittedPairStartsTogether is round 19's finding.
//
// The pair binding compared lineage, identifier and QoS, and never the register's STATE. A
// register already at publish_terminal is one recovery treats as completed and eligible for
// terminal cleanup, while this mechanism would go on permitting another PUBLISH and holding
// the obligation's dependency edges open; one at a PUBREL state describes an exchange that has
// already transferred ownership, which an admission has not. Each record passes its own
// validation, and the operation derived the contradiction rather than refusing it.
func TestF105AnAdmittedPairStartsTogether(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-pair-start")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")

	for _, st := range []mqttInflightState{
		mqttInflightPublishTerminal, mqttInflightPubRelReady, mqttInflightPubRelIntended,
	} {
		// THROUGH THE BOUNDARY, with the batch otherwise exactly what the builder produced.
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		adm := testF105Admission(e, al.rec, al.seq, "txn-b-"+mqttNewInflightNonce(), 7, nil)
		tr, terr := mqttAdmitPublishTransition(adm)
		if terr != nil {
			t.Fatalf("assembling the admission failed: %v", terr)
		}
		for i, m := range tr.Extra {
			if _, perr := mqttParseInflightSubject(m.subj); perr != nil {
				continue
			}
			rec, derr := mqttDecodeInflightRecord(m.body)
			if derr != nil {
				t.Fatalf("decoding the assembled register failed: %v", derr)
			}
			rec.State = st
			body, merr := json.Marshal(rec)
			if merr != nil {
				t.Fatalf("marshalling the substituted register failed: %v", merr)
			}
			tr.Extra[i].body = body
			break
		}
		if _, err := jsa.commitAuthorityTransition(tr); err == nil {
			t.Fatalf("an obligation was admitted at its publish beside a register in state %q", st)
		}

		// AND THROUGH THE OPERATION, which derived a publish-phase exchange from whatever
		// register it was handed.
		reg := freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		})
		reg.State = st
		if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-o-" + mqttNewInflightNonce(), Op: "op-adm",
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, Register: reg,
		}); err == nil {
			t.Fatalf("the admission operation accepted a register in state %q", st)
		}
	}

	// The publish-state pair commits, so every refusal above is the state and not the
	// fixture.
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	good := testF105Admission(e, al.rec, al.seq, "txn-good", 7, nil)
	gtr, gerr := mqttAdmitPublishTransition(good)
	if gerr != nil {
		t.Fatalf("assembling the well-formed admission failed: %v", gerr)
	}
	if _, err := jsa.commitAuthorityTransition(gtr); err != nil {
		t.Fatalf("an admission beside a publish-state register was refused: %v", err)
	}
	_ = auth
}

// ---------------------------------------------------------------------------
// Review round 20
// ---------------------------------------------------------------------------

// TestF105AnAdvanceStillHoldsItsIdentifier is round 20's finding.
//
// The collision check counted only exchanges a batch CREATES, so a batch that advanced an
// existing obligation along its protocol while admitting a new one at the same identifier
// passed: the advance is legal, the admission is legal, and the identifier is claimed twice.
// The check now runs over the batch's RESULTING state, where an advance still holds its
// identifier and only an ENDING gives it up.
func TestF105AnAdvanceStillHoldsItsIdentifier(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-advance-holds-pi")
	defer done()

	jsa := &e.asm.jsa
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	standing := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")

	advance := func(to mqttExchangePhase) mqttTransitionMember {
		cur, curSeq := testF105LoadExchange(t, e, testF105Gen, standing)
		next := *cur
		next.Phase = to
		body, merr := json.Marshal(&next)
		if merr != nil {
			t.Fatalf("marshalling the advanced exchange failed: %v", merr)
		}
		return mqttTransitionMember{
			subj:   mqttExchangeSubject(getHash(e.id), cur.Gen, cur.Nonce),
			expect: curSeq, body: body,
		}
	}
	admission := func(txn string, pi uint16) mqttTransitionMember {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		body, merr := json.Marshal(x)
		if merr != nil {
			t.Fatalf("marshalling the exchange failed: %v", merr)
		}
		return mqttTransitionMember{
			subj:   mqttExchangeSubject(getHash(e.id), x.Gen, x.Nonce),
			expect: 0, body: body,
		}
	}
	commit := func(txn string, members ...mqttTransitionMember) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		tr := testF105AdmitNothing(e, al.rec, al.seq, txn)
		tr.Extra = testF105CoverExchanges(t, e, testF105Gen, members)
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}

	// THE ADVANCE DOES NOT RELEASE THE IDENTIFIER. A PUBREC means the client has taken the
	// message and still owes the release; the obligation is more outstanding, not less.
	if err := commit("txn-mixed",
		advance(mqttExchangePubRecObserved), admission("txn-mixed", 7)); err == nil {
		t.Fatalf("a batch admitted a new obligation on an identifier its own advance still holds")
	}
	after, _ := testF105LoadExchange(t, e, testF105Gen, standing)
	if after.Phase != mqttExchangePublish {
		t.Fatalf("the refused batch advanced the standing exchange to phase %d", after.Phase)
	}

	// A DISTINCT IDENTIFIER IS FINE, so what was refused is the collision.
	if err := commit("txn-distinct",
		advance(mqttExchangePubRecObserved), admission("txn-distinct", 8)); err != nil {
		t.Fatalf("an advance beside an admission on another identifier was refused: %v", err)
	}
	_ = auth
}

// ---------------------------------------------------------------------------
// Review round 21
// ---------------------------------------------------------------------------

// TestF105AnIdentifierIsExclusiveAgainstStandingObligations is round 21's finding, and it is
// the one that disproved my own proposed oracle.
//
// I had claimed the property behind every intra-batch finding was "the same members written
// ONE AT A TIME would each have been accepted". They would: two REGISTERLESS admissions
// committed one after the other, each carrying a single exchange with its own nonce, each
// legal, leave two obligations outstanding at one identifier. Each batch's collision map holds
// one exchange and their subjects differ, so nothing within either batch objects.
//
// The property is invariant preservation over the RESULTING state — the batch overlaid on what
// already stands — and the registerless route is where it bites, because a registered
// admission is already exclusive: the register's subject IS the identifier and it is written
// create-only.
func TestF105AnIdentifierIsExclusiveAgainstStandingObligations(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-identifier-exclusive")
	defer done()

	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")

	admit := func(txn string, pi uint16) (string, error) {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
		if terr != nil {
			t.Fatalf("assembling the registerless admission failed: %v", terr)
		}
		_, err := jsa.commitAuthorityTransition(tr)
		return x.Nonce, err
	}

	first, err := admit("txn-a", 7)
	if err != nil {
		t.Fatalf("the first registerless admission was refused: %v", err)
	}
	// SEQUENTIALLY, not in one batch: this is the shape no intra-batch rule can see.
	if _, err := admit("txn-b", 7); err == nil {
		t.Fatalf("a second obligation was admitted on an identifier the first still holds")
	}
	// Another identifier is free.
	if _, err := admit("txn-c", 8); err != nil {
		t.Fatalf("an admission on a free identifier was refused: %v", err)
	}

	// AND THE IDENTIFIER IS RELEASED BY AN ENDING, not by time or by absence. The first
	// obligation is refused by the client, and the identifier becomes claimable again.
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-rej", "op-rej",
		testF105Gen, first, mqttExchangeRejected); err != nil {
		t.Fatalf("recording the refusal was refused: %v", err)
	}
	if _, err := admit("txn-d", 7); err != nil {
		t.Fatalf("an identifier whose obligation ended was not claimable again: %v", err)
	}

	// AND A REGISTERED ADMISSION IS NOT EXEMPT. A create-only member excludes another writer
	// at ITS OWN KEY and says nothing about a different key describing the same thing: the
	// registerless obligation below never occupied the register subject, so the registered
	// admission's create-only write succeeds and both would stand on one identifier.
	standing, aerr := admit("txn-registerless", 11)
	if aerr != nil {
		t.Fatalf("the registerless admission was refused: %v", aerr)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-registered", Op: "op-adm",
		Gen: al.rec.Gen, Tenure: al.rec.Tenure,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 11, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a REGISTERED admission took identifier 11 while registerless obligation %q "+
			"still holds it", standing)
	}

	// AND AN UNREADABLE OBLIGATION REFUSES. A record this broker cannot interpret may be the
	// one holding the identifier, so treating it as absent is the inference the whole
	// mechanism exists to remove — and it is the only reading under which a free-looking
	// identifier is not free. The enumeration is the generation's INVENTORY (F-112): every
	// exchange is admitted with its duty in the same batch, so the obligation made unreadable
	// here is one the inventory names — the registerless one standing on identifier 11.
	corrupt := mqttExchangeSubject(getHash(e.id), testF105Gen, standing)
	if _, err := jsa.storeMsg(corrupt, -1, []byte(`{"id":"nonsense"`)); err != nil {
		t.Fatalf("planting the unreadable obligation failed: %v", err)
	}
	if _, err := admit("txn-e", 9); err == nil {
		t.Fatalf("an identifier was claimed while an obligation nobody can read stands in " +
			"the same generation")
	}
}

// ---------------------------------------------------------------------------
// Review round 23
// ---------------------------------------------------------------------------

// TestF105ATruncatedEnumerationIsNotAnAnswer is round 23's finding, carried onto the enumeration
// that replaced the walk it was first stated about.
//
// The exclusivity check asks whether anything still holds an identifier. It used to page a
// stream-info subject list; since F-112 it reads the generation's INVENTORY [Lo, Hi] at the ledger
// sequence its own batch CASes, because exchange records are now deleted behind the floor and a
// paged subject list shifts under a deletion. The property is unchanged: an answer this broker
// could not read completely is not an answer, and absence is exactly what the claim rests on. The
// fixture makes the inventory unreadable at its root — the ledger — and then restores it.
func TestF105ATruncatedEnumerationIsNotAnAnswer(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-truncated-walk")
	defer done()

	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")

	admit := func(txn string, pi uint16) error {
		al := jsa.loadOutboundAuthority(e.id, e.epoch)
		x := &mqttOutboundExchange{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 2, Phase: mqttExchangePublish,
			AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, txn, "op-"+txn, x)
		if terr != nil {
			t.Fatalf("assembling the admission failed: %v", terr)
		}
		_, err := jsa.commitAuthorityTransition(tr)
		return err
	}
	if err := admit("txn-first", 5); err != nil {
		t.Fatalf("the first admission was refused: %v", err)
	}
	ledSubj := mqttGenLedgerSubject(getHash(e.id), testF105Gen)
	good, err := jsa.loadLastMsgFor(mqttInflightStreamName, ledSubj)
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}

	// AN UNREADABLE INVENTORY: the ledger that bounds it will not decode.
	if _, err := jsa.storeMsg(ledSubj, -1, []byte(`{"id":"nonsense"`)); err != nil {
		t.Fatalf("planting the unreadable ledger: %v", err)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	x := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
		PI: 7, QoS: 2, Phase: mqttExchangePublish, AdmitTxn: "txn-truncated", Tenure: al.rec.Tenure,
	}
	// The composer reads the ledger too, so the batch is assembled against the good body and
	// submitted against the unreadable one: the boundary is what must refuse.
	if _, err := jsa.storeMsg(ledSubj, -1, good.Data); err != nil {
		t.Fatalf("restoring the ledger for composition: %v", err)
	}
	tr, terr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, "txn-truncated", "op-truncated", x)
	if terr != nil {
		t.Fatalf("assembling the admission failed: %v", terr)
	}
	if _, err := jsa.storeMsg(ledSubj, -1, []byte(`{"id":"nonsense"`)); err != nil {
		t.Fatalf("planting the unreadable ledger: %v", err)
	}
	if _, err := jsa.commitAuthorityTransition(tr); err == nil ||
		!strings.Contains(err.Error(), "could not enumerate generation") {
		t.Fatalf("an identifier was claimed on an inventory the boundary could not read: %v", err)
	}

	// THE READABLE INVENTORY admits — so what refused above is the unreadable answer and not
	// the identifier.
	if _, err := jsa.storeMsg(ledSubj, -1, good.Data); err != nil {
		t.Fatalf("restoring the ledger: %v", err)
	}
	if err := admit("txn-complete", 7); err != nil {
		t.Fatalf("an identifier was refused on a complete answer that holds nothing on it: %v", err)
	}
}

// TestF105ACompleteAnswerIsTheWholeTotal covers the decision the enumeration seam cannot: whether
// the page a stream-info response carries can be vouched for against the envelope beside it, and
// whether it ends the walk.
//
// Separated from its request because that request cannot be exercised without a store. The
// original cases keep their intent under paging: an empty answer is complete; a whole answer in
// one page is complete; one page short of a total past it does NOT complete the walk (it now says
// where the next page starts, instead of ending it); any shortfall within a page is refused.
func TestF105ACompleteAnswerIsTheWholeTotal(t *testing.T) {
	const filter = "$MQTT.f105.x.h.g.*"
	subjects := func(from, n int) map[string]uint64 {
		m := make(map[string]uint64, n)
		for i := from; i < from+n; i++ {
			m[fmt.Sprintf("$MQTT.f105.x.h.g.n%07d", i)] = 1
		}
		return m
	}
	for _, c := range []struct {
		name     string
		p        mqttSubjectPage
		offset   int
		next     int
		done, ok bool
	}{
		// The original four.
		{"an empty stream", mqttSubjectPage{}, 0, 0, true, true},
		{"everything, in one page", mqttSubjectPage{Subjects: subjects(0, 5), Total: 5, Limit: 100_000}, 0, 5, true, true},
		{"the server's page size, one past it", mqttSubjectPage{Subjects: subjects(0, 100_000), Total: 100_001, Limit: 100_000}, 0, 100_000, false, true},
		{"any shortfall at all", mqttSubjectPage{Subjects: subjects(0, 1), Total: 2, Limit: 100_000}, 0, 0, false, false},
		// Paging.
		{"a full middle page", mqttSubjectPage{Subjects: subjects(4, 4), Total: 10, Limit: 4, Offset: 4}, 4, 8, false, true},
		{"the exact last page", mqttSubjectPage{Subjects: subjects(8, 2), Total: 10, Limit: 4, Offset: 8}, 8, 10, true, true},
		{"a short last page", mqttSubjectPage{Subjects: subjects(8, 1), Total: 10, Limit: 4, Offset: 8}, 8, 0, false, false},
		{"a nonempty short middle page", mqttSubjectPage{Subjects: subjects(4, 3), Total: 10, Limit: 4, Offset: 4}, 4, 0, false, false},
		{"a prematurely empty middle page", mqttSubjectPage{Subjects: map[string]uint64{}, Total: 10, Limit: 4, Offset: 4}, 4, 0, false, false},
		{"the all-zero empty answer past the first page", mqttSubjectPage{}, 4, 0, true, false},
		{"a different echoed offset", mqttSubjectPage{Subjects: subjects(4, 4), Total: 10, Limit: 4, Offset: 0}, 4, 0, false, false},
		{"a positive total with page size zero", mqttSubjectPage{Subjects: subjects(0, 0), Total: 10, Limit: 0}, 0, 0, false, false},
		{"a total at or below the requested offset", mqttSubjectPage{Subjects: map[string]uint64{}, Total: 4, Limit: 4, Offset: 4}, 4, 0, false, false},
		{"a subject outside the filter", mqttSubjectPage{Subjects: map[string]uint64{"$MQTT.f105.x.h.other.n": 1}, Total: 1, Limit: 4}, 0, 0, false, false},
		{"subjects under an all-zero envelope", mqttSubjectPage{Subjects: subjects(0, 1)}, 0, 0, false, false},
	} {
		next, done, ok := mqttSubjectPageAdvance(c.p, filter, c.offset)
		if ok != c.ok || (ok && (next != c.next || done != c.done)) {
			t.Fatalf("%s: advance gave next=%d done=%t ok=%t, want next=%d done=%t ok=%t",
				c.name, next, done, ok, c.next, c.done, c.ok)
		}
	}
}

// ---------------------------------------------------------------------------
// Review round 24
// ---------------------------------------------------------------------------

// TestF105AnAllocationIsAdmittedWithItsObligation is round 24's finding.
//
// The exchange-to-register binding was ONE-DIRECTIONAL: an exchange had to agree with the
// register beside it, and a register alone was nobody's. So a batch carrying only a fresh
// register and its marker allocated an identifier with no obligation to answer for it — and
// because the standing obligation it collides with is registerless, it never occupied that
// subject, so every create-only expectation succeeds.
func TestF105AnAllocationIsAdmittedWithItsObligation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-allocation-obligation")
	defer done()

	jsa := &e.asm.jsa
	e.bootstrap(t, "txn-1")

	// A REGISTERLESS obligation holds identifier 7 and occupies no register subject.
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	standing := &mqttOutboundExchange{
		ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Nonce: mqttNewInflightNonce(),
		PI: 7, QoS: 2, Phase: mqttExchangePublish,
		AdmitTxn: "txn-registerless", Tenure: al.rec.Tenure,
	}
	str, serr := testF105AdmitExchangeTr(t, e, al.rec, al.seq, "txn-registerless", "op-r", standing)
	if serr != nil {
		t.Fatalf("assembling the registerless admission failed: %v", serr)
	}
	if _, err := jsa.commitAuthorityTransition(str); err != nil {
		t.Fatalf("the registerless admission was refused: %v", err)
	}

	// A PAIR-ONLY ALLOCATION on the same identifier: register and marker, no obligation.
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	reg := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	rbody, merr := json.Marshal(reg)
	if merr != nil {
		t.Fatalf("marshalling the register failed: %v", merr)
	}
	marker, mkerr := mqttBuildInflightMarker(reg.Nonce)
	if mkerr != nil {
		t.Fatalf("building the marker failed: %v", mkerr)
	}
	regSubj := mqttInflightSubject(getHash(e.id), al.rec.Gen, 7)
	pairOnly := testF105AdmitNothing(e, al.rec, al.seq, "txn-pair-only")
	pairOnly.Extra = []mqttTransitionMember{
		{subj: regSubj, expect: 0, body: rbody},
		{subj: regSubj + "." + mqttInflightMarkerSuffix, expect: 0, body: marker},
	}
	if _, err := jsa.commitAuthorityTransition(pairOnly); err == nil {
		t.Fatalf("an identifier was allocated with no obligation behind it, beside a standing " +
			"registerless obligation that holds it")
	}
	if _, err := jsa.loadLastMsgFor(mqttInflightStreamName, regSubj); err == nil {
		t.Fatalf("the refused allocation installed its register")
	}

	// AN UNMATCHED EXTRA REGISTER beside an otherwise valid admission is refused the same
	// way: the admission's own pair is fine, and the extra one answers to nothing.
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	adm := testF105Admission(e, al.rec, al.seq, "txn-extra", 8, nil)
	tr, terr := mqttAdmitPublishTransition(adm)
	if terr != nil {
		t.Fatalf("assembling the admission failed: %v", terr)
	}
	extra := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 9, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	ebody, eerr := json.Marshal(extra)
	if eerr != nil {
		t.Fatalf("marshalling the extra register failed: %v", eerr)
	}
	emarker, emerr := mqttBuildInflightMarker(extra.Nonce)
	if emerr != nil {
		t.Fatalf("building the extra marker failed: %v", emerr)
	}
	ninth := mqttInflightSubject(getHash(e.id), al.rec.Gen, 9)
	tr.Extra = append(tr.Extra,
		mqttTransitionMember{subj: ninth, expect: 0, body: ebody},
		mqttTransitionMember{subj: ninth + "." + mqttInflightMarkerSuffix, expect: 0, body: emarker},
	)
	if _, err := jsa.commitAuthorityTransition(tr); err == nil {
		t.Fatalf("an admission carried an extra allocation with no obligation behind it")
	}

	// The admission alone commits, so the refusal is the unmatched allocation.
	al = jsa.loadOutboundAuthority(e.id, e.epoch)
	good := testF105Admission(e, al.rec, al.seq, "txn-good", 8, nil)
	gtr, gerr := mqttAdmitPublishTransition(good)
	if gerr != nil {
		t.Fatalf("assembling the good admission failed: %v", gerr)
	}
	if _, err := jsa.commitAuthorityTransition(gtr); err != nil {
		t.Fatalf("a whole admission was refused: %v", err)
	}
}
