//go:build mqttport_fulltests

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

package mqtt

import (
	"encoding/json"
	"strings"
	"testing"
)

// testF105Resource builds a resource whose dependents are named by nonce alone, each in the
// SOURCE-REPLAY role. Most fixtures do not care which role they hold, and spelling one out
// everywhere would bury the cases that do; testF105ResourceEdges is for those.
// testF105Edge is the ordinary source-replay edge for one obligation.
func testF105Edge(nonce string) mqttResourceEdge {
	return mqttResourceEdge{Nonce: nonce, Role: mqttRoleSourceReplay}
}

// testF105EdgeFor is the edge an admission took on THIS resource, derived from its kind.
//
// A release must name the role it discharges, and naming the wrong one is refused — which is the
// whole point of the change and is why a fixture cannot keep a single default. Deriving it from
// the resource in hand keeps the call sites short without letting them guess.
func testF105EdgeFor(r *mqttOutboundResource, nonce string) mqttResourceEdge {
	role := mqttRoleSourceReplay
	if r != nil && r.Kind == mqttResourcePubRelConsumer {
		role = mqttRoleCanonicalPubRel
	}
	return mqttResourceEdge{Nonce: nonce, Role: role}
}

func testF105Resource(state mqttResourceState, nonces []string) *mqttOutboundResource {
	// A NIL LIST IS PASSED THROUGH AS NIL. The validator refuses a null dependent list, and a
	// helper that quietly turned nil into [] would make that rule untestable.
	if nonces == nil {
		return testF105ResourceEdges(state, nil)
	}
	deps := make([]mqttResourceEdge, 0, len(nonces))
	for _, n := range nonces {
		deps = append(deps, mqttResourceEdge{Nonce: n, Role: mqttRoleSourceReplay})
	}
	return testF105ResourceEdges(state, deps)
}

func testF105ResourceEdges(state mqttResourceState, deps []mqttResourceEdge) *mqttOutboundResource {
	r := &mqttOutboundResource{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Inc: "inc-1",
		Kind: mqttResourcePubRelConsumer, Name: "$MQTT_pubrel_x", State: state,
		// SEALED, for the same reason the planting fixture is: these stand for resources that
		// already exist, and deletion authorization requires the seal. A test about the window
		// BEFORE the seal sets Prov itself.
		Prov: mqttProvisioningSealed, Deps: deps, ProvTenure: "prov-t0",
	}
	if state != mqttResourceLive {
		r.AuthTxn = "txn-auth"
	}
	if state == mqttResourceCleanupComplete {
		r.Outcome = mqttCleanupRemoved
	}
	return r
}

// TestF105OnlyAWonAuthorizationLicensesDeletion is the rule this unit exists for.
//
// The defect it closes: a teardown deletes the PUBREL consumer, $MQTT_out is
// interest-retained, so the records that consumer carried — the only evidence the client
// answered PUBREC — go with it, and an obligation admitted before that teardown is left with
// no completion route.
//
// A live resource with zero dependencies is deliberately NOT deletable. Nothing has excluded
// a dependency arriving between the reading and the acting, and that exclusion is the entire
// content of the authorization.
func TestF105OnlyAWonAuthorizationLicensesDeletion(t *testing.T) {
	if mqttResourceDeletable(testF105Resource(mqttResourceLive, []string{})) {
		t.Fatalf("a LIVE resource with no dependencies was deletable: nothing has excluded a " +
			"dependency arriving between the read and the act")
	}
	if mqttResourceDeletable(testF105Resource(mqttResourceLive, []string{"xch-a", "xch-b", "xch-c"})) {
		t.Fatalf("a live resource that obligations still need was deletable")
	}
	if !mqttResourceDeletable(testF105Resource(mqttResourceDeleteAuthorized, []string{})) {
		t.Fatalf("a resource with a won authorization was not deletable")
	}
	if mqttResourceDeletable(nil) {
		t.Fatalf("a missing resource was deletable: absence is not permission")
	}
}

// TestF105AuthorizationIsSelfConsistent pins that the record cannot say two things at once.
// A won authorization MEANS the count was zero, so a body carrying both is contradictory,
// and admitting it would let a deletion proceed over live dependencies.
func TestF105AuthorizationIsSelfConsistent(t *testing.T) {
	bad := testF105Resource(mqttResourceDeleteAuthorized, []string{"xch-a", "xch-b"})
	if err := mqttValidateOutboundResource(bad, testF105ID, testF105Epoch, testF105Gen, "inc-1"); err == nil {
		t.Fatalf("an authorized resource carrying live dependencies was admitted")
	}

	live := testF105Resource(mqttResourceLive, []string{})
	live.AuthTxn = "txn-auth"
	if err := mqttValidateOutboundResource(live, testF105ID, testF105Epoch, testF105Gen, "inc-1"); err == nil {
		t.Fatalf("a live resource describing an authorization it does not have was admitted")
	}

	done := testF105Resource(mqttResourceCleanupComplete, []string{})
	done.Outcome = "vanished"
	if err := mqttValidateOutboundResource(done, testF105ID, testF105Epoch, testF105Gen, "inc-1"); err == nil {
		t.Fatalf("a completed resource carrying an unsupported outcome was admitted")
	}
	// The three outcomes are kept apart on purpose: a successful delete is not a completed
	// cleanup, because an exact-sequence delete succeeds for an already-gone sequence and a
	// peer that acquired the name rewrites at a new one.
	for _, o := range []mqttCleanupOutcome{mqttCleanupRemoved, mqttCleanupGone, mqttCleanupReplaced} {
		ok := testF105Resource(mqttResourceCleanupComplete, []string{})
		ok.Outcome = o
		if err := mqttValidateOutboundResource(ok, testF105ID, testF105Epoch, testF105Gen, "inc-1"); err != nil {
			t.Fatalf("outcome %q was refused: %v", o, err)
		}
	}
}

// TestF105AuthorizationContendsWithDependencyAddition is the store-level property: both the
// authorization and a dependency change write the SAME resource record at the sequence they
// read, so exactly one wins. That is what makes "nothing depended on it, and nothing can be
// added" a property of the store rather than of the order two passes happened to run in.
func TestF105AuthorizationContendsWithDependencyAddition(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-retire")
	defer done()

	e.bootstrap(t, "txn-1")
	// A SEALABLE kind: the generic contention between an authorization and a dependency addition.
	// For a source or PUBREL consumer the same contention runs through its disposal decision
	// (TestF105DisposalRequestFencesAdmission).
	res, resSeq := testF105PlantResource(t, e, "inc-1", mqttResourceRetainedPayload)

	// An admission takes a dependency, at that sequence.
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-1")

	// THE AUTHORIZATION COMPUTED AGAINST THE STALE READ must now lose, and must leave the
	// resource live. If it won, a deletion would proceed over a dependency taken after the
	// resource was observed — which is the whole hazard.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	auth, err := mqttAuthorizeDeletionTransition(al.rec, al.seq, "txn-auth", "op-auth", res, resSeq)
	if err != nil {
		t.Fatalf("assembling the authorization failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(auth); err == nil {
		t.Fatalf("a deletion authorization won against a resource an obligation had come to " +
			"depend on since it was read")
	}
	got, gotSeq := testF105LoadResource(t, e, "inc-1")
	if got.State != mqttResourceLive || len(got.Deps) != 1 || got.Deps[0].Nonce != nonce {
		t.Fatalf("the refused authorization changed the resource: state %d deps %v",
			got.State, got.Deps)
	}
	if mqttResourceDeletable(got) {
		t.Fatalf("the resource became deletable despite the obligation that needs it")
	}

	// Against the CURRENT sequence, with the obligation FINISHED and its edge released in
	// the same act, it wins.
	testF105FinishExchange(t, e, nonce)
	got, gotSeq = testF105LoadResource(t, e, "inc-1")
	cleared, cerr := mqttResourceReleaseDependency(got, testF105EdgeFor(got, nonce))
	if cerr != nil {
		t.Fatalf("releasing the finished obligation's dependency failed: %v", cerr)
	}
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	auth2, err := mqttAuthorizeDeletionTransition(al.rec, al.seq, "txn-auth2", "op-auth2",
		cleared, gotSeq)
	if err != nil {
		t.Fatalf("assembling the second authorization failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(auth2); err != nil {
		t.Fatalf("the authorization against the current sequence was refused: %v", err)
	}
	got2, _ := testF105LoadResource(t, e, "inc-1")
	if !mqttResourceDeletable(got2) {
		t.Fatalf("the resource is not deletable after a won authorization: state %d", got2.State)
	}
}

// TestF105DependencyEdgesRefuseIllegalMoves pins what a NAMED edge gives that a count did
// not.
//
// A count can be decremented by anything: "one fewer" is an assertion, and a caller draining
// it one step at a time reaches zero with every obligation still outstanding. An edge can
// only be removed if it is there, and only once — the second attempt names something the
// list no longer holds, which a count cannot distinguish from a first.
func TestF105DependencyEdgesRefuseIllegalMoves(t *testing.T) {
	if _, err := mqttResourceReleaseDependency(testF105Resource(mqttResourceLive, []string{}), testF105Edge("xch-a")); err == nil {
		t.Fatalf("an edge nothing holds was released")
	}
	held := testF105Resource(mqttResourceLive, []string{"xch-a"})
	once, err := mqttResourceReleaseDependency(held, testF105Edge("xch-a"))
	if err != nil {
		t.Fatalf("releasing a held edge failed: %v", err)
	}
	if len(once.Deps) != 0 {
		t.Fatalf("releasing the only edge left %v", once.Deps)
	}
	// RELEASED EXACTLY ONCE. A second release of the same obligation is refused, which is
	// the property a count structurally cannot have.
	if _, err := mqttResourceReleaseDependency(once, testF105Edge("xch-a")); err == nil {
		t.Fatalf("one obligation released the same dependency twice")
	}
	// And an obligation depends on a resource once.
	if _, err := mqttResourceTakeDependency(held, testF105Edge("xch-a")); err == nil {
		t.Fatalf("one obligation took the same dependency twice")
	}
	if _, err := mqttResourceTakeDependency(held, testF105Edge("not a token")); err == nil {
		t.Fatalf("a malformed obligation name was taken as a dependency")
	}

	for name, state := range map[string]mqttResourceState{
		"authorized": mqttResourceDeleteAuthorized,
		"completed":  mqttResourceCleanupComplete,
	} {
		if _, err := mqttResourceTakeDependency(testF105Resource(state, []string{}), testF105Edge("xch-a")); err == nil {
			t.Fatalf("a dependency was added to an %s resource, which its authorization "+
				"promised could not happen", name)
		}
		if _, err := mqttResourceReleaseDependency(testF105Resource(state, []string{}), testF105Edge("xch-a")); err == nil {
			t.Fatalf("a dependency was released on an %s resource", name)
		}
	}
}

// TestF105CleanupSurvivesClosure is the correction Codex's consult 8 found.
//
// The deletion authorization was assembled as an admit-exchange transition, whose rules
// require SERVING authority. Cleanup of an old generation's resources runs precisely AFTER
// that generation is closed — closure is what makes them reclaimable — so that kind made the
// work impossible at the only time it matters, and would have pushed a caller towards
// reopening or borrowing an authority to do it.
func TestF105CleanupSurvivesClosure(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-cleanup-closed")
	defer done()

	auth, seq := e.bootstrap(t, "txn-1")

	// Close the generation.
	closing := testF105Closure(e, auth, seq, "txn-close")
	closeSeq, err := e.asm.jsa.commitAuthorityTransition(closing)
	if err != nil {
		t.Fatalf("the close-admission was refused: %v", err)
	}

	// Plant a resource of that closed generation with no dependencies.
	res := testF105Resource(mqttResourceLive, []string{})
	res.ID, res.Epoch = e.id, e.epoch
	// A SEALABLE kind: this is the generic resource state machine, and a source or PUBREL
	// consumer is retired only on a route's disposal decision with its name in the same batch
	// (F-105 7c-lc), which TestF105DisposalPairIsEnforced and the disposal tests cover.
	res.Kind, res.Name, res.ProvTenure = mqttResourceRetainedPayload, testF105Payload, _EMPTY_
	body, _ := json.Marshal(res)
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	resp, err := e.asm.jsa.storeMsg(mqttResourceSubject(getHash(e.id), testF105Gen, "inc-1"), hdr, msg)
	if err != nil {
		t.Fatalf("planting the resource failed: %v", err)
	}

	// THE POINT: authorizing its deletion must succeed over a CLOSED authority.
	dispose, err := mqttAuthorizeDeletionTransition(closing.Auth, closeSeq, "txn-dispose", "op-dispose", res, resp.Sequence)
	if err != nil {
		t.Fatalf("assembling the disposal failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(dispose); err != nil {
		t.Fatalf("cleanup of a CLOSED generation's resource was refused: %v — closure is "+
			"exactly when this work runs", err)
	}

	// And it moved nothing: the authority is still closed, still the same ownership.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority does not load back after disposal (status %v)", al.status)
	}
	if al.rec.State != mqttAuthorityClosed {
		t.Fatalf("the disposal reopened the authority to state %d", al.rec.State)
	}
	if al.rec.Gen != testF105Gen || al.rec.OwnerTxn != "txn-1" {
		t.Fatalf("the disposal moved ownership to %q/%q", al.rec.Gen, al.rec.OwnerTxn)
	}

	// The closure stands untouched.
	if _, _, st := e.asm.jsa.loadGenClosure(e.id, e.epoch, testF105Gen); st != mqttOutboundValid {
		t.Fatalf("the permanent closure did not survive the disposal (status %v)", st)
	}

	smsg, _ := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttResourceSubject(getHash(e.id), testF105Gen, "inc-1"))
	got, derr := mqttDecodeOutboundResource(smsg.Data, e.id, e.epoch, testF105Gen, "inc-1")
	if derr != nil {
		t.Fatalf("the disposed resource does not decode: %v", derr)
	}
	if !mqttResourceDeletable(got) {
		t.Fatalf("the resource is not deletable after a won disposal: state %d", got.State)
	}
}

// TestF105DisposalMovesNothingElse pins that the disposal kind is narrow: it may not move
// ownership, change the authority's state, write a closure, or carry anything that is not a
// resource. A kind exempt from the serving requirement must be exempt from nothing else.
func TestF105DisposalMovesNothingElse(t *testing.T) {
	jsa := &mqttJSA{mqttJSACore: &mqttJSACore{}}

	base := func() *mqttAuthorityTransition {
		tr := testF105Transition()
		tr.Attempt.Kind = mqttTransitionDisposeResource
		r := testF105Resource(mqttResourceLive, []string{})
		b, _ := json.Marshal(r)
		tr.Extra = []mqttTransitionMember{{
			subj: mqttResourceSubject(getHash(testF105ID), testF105Gen, "inc-1"), expect: 1, body: b,
		}}
		return tr
	}
	if err := mqttCheckTransitionKindRules(base()); err != nil {
		t.Fatalf("a well-formed disposal was refused: %v", err)
	}

	for name, mutate := range map[string]func(*mqttAuthorityTransition){
		"changes the authority state": func(tr *mqttAuthorityTransition) {
			tr.Auth.State = mqttAuthorityClosed
		},
		"moves ownership":  func(tr *mqttAuthorityTransition) { tr.Auth.OwnerTxn = "own-other" },
		"moves the tenure": func(tr *mqttAuthorityTransition) { tr.Auth.Tenure = "tenure-other" },
		"writes a closure": func(tr *mqttAuthorityTransition) {
			tr.Closure = &mqttGenClosure{
				ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Txn: tr.Attempt.Txn,
			}
		},
		"carries no resource": func(tr *mqttAuthorityTransition) { tr.Extra = nil },
	} {
		tr := base()
		mutate(tr)
		if err := mqttCheckTransitionKindRules(tr); err == nil {
			t.Fatalf("a disposal that %s was admitted", name)
		}
	}

	// A non-resource member is refused even though the transition is otherwise fine.
	tr := base()
	tr.Extra = append(tr.Extra, mqttTransitionMember{
		subj: mqttExchangeSubject(getHash(testF105ID), testF105Gen, "xch-1"), expect: 0, body: []byte("{}"),
	})
	if err := mqttCheckTransitionKindRules(tr); err == nil {
		t.Fatalf("a disposal carrying an exchange was admitted")
	}
	_ = jsa
}

// TestF105CleanupOfAnOldGenerationWhileANewOneServes is the correction implementation
// review 4 found, and it is the case deferred cleanup exists for.
//
// The resource-member validator required a resource's generation to equal the CURRENT
// authority's. After closing G1 and admitting G2, disposing G1's resources therefore failed —
// and retrying under G1's old authority failed its CAS instead, so there was no route at all.
func TestF105CleanupOfAnOldGenerationWhileANewOneServes(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-crossgen")
	defer done()

	// G1 serves, owns a resource, then closes.
	auth, seq := e.bootstrap(t, "txn-1")
	res := testF105Resource(mqttResourceLive, []string{})
	res.ID, res.Epoch = e.id, e.epoch
	// A SEALABLE kind: this is the generic resource state machine, and a source or PUBREL
	// consumer is retired only on a route's disposal decision with its name in the same batch
	// (F-105 7c-lc), which TestF105DisposalPairIsEnforced and the disposal tests cover.
	res.Kind, res.Name, res.ProvTenure = mqttResourceRetainedPayload, testF105Payload, _EMPTY_
	body, _ := json.Marshal(res)
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	resp, err := e.asm.jsa.storeMsg(mqttResourceSubject(getHash(e.id), testF105Gen, "inc-1"), hdr, msg)
	if err != nil {
		t.Fatalf("planting the resource failed: %v", err)
	}

	closing := testF105Closure(e, auth, seq, "txn-close")
	closeSeq, err := e.asm.jsa.commitAuthorityTransition(closing)
	if err != nil {
		t.Fatalf("the closure was refused: %v", err)
	}

	// G2 is CLAIMED beside it and is what the authority now names.
	g2, g2Seq := e.claim(t, closing.Auth, closeSeq, "txn-g2", testF105Gen2, "tenure-g2")

	// THE POINT: G1's resource is disposed under G2's authority. Both the authority's
	// generation and the resource's are named, and they differ.
	dispose, err := mqttAuthorizeDeletionTransition(g2, g2Seq, "txn-disp", "op-disp",
		res, resp.Sequence)
	if err != nil {
		t.Fatalf("assembling the cross-generation disposal failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(dispose); err != nil {
		t.Fatalf("disposing an OLD generation's resource while a NEW one serves was refused: "+
			"%v — this is exactly when deferred cleanup must work", err)
	}

	fmsg, _ := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttResourceSubject(getHash(e.id), testF105Gen, "inc-1"))
	got, derr := mqttDecodeOutboundResource(fmsg.Data, e.id, e.epoch, testF105Gen, "inc-1")
	if derr != nil {
		t.Fatalf("the disposed resource does not decode: %v", derr)
	}
	if !mqttResourceDeletable(got) {
		t.Fatalf("the old generation's resource is not deletable after its disposal: state %d", got.State)
	}
	// G2 is untouched by G1's cleanup.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.Gen != testF105Gen2 || al.rec.State != mqttAuthorityServing {
		t.Fatalf("cleaning up G1 disturbed G2 (status %v)", al.status)
	}

	// An ADMISSION still may not reach another generation's resource: a fresh obligation
	// taking a dependency on something a closed generation owns would be a dependency
	// nothing can discharge.
	adm := testF105Admission(e, al.rec, al.seq, "txn-bad", 7, []mqttResourceDependency{
		{Resource: res, Seq: fmsg.Sequence, Role: mqttRoleSourceReplay},
	})
	// The fixture builds its exchange in G1, so correct it to G2 first: the point being
	// tested is the DEPENDENCY reaching across generations, not the exchange doing so.
	adm.Exchange.Gen, adm.Exchange.Tenure = testF105Gen2, al.rec.Tenure
	adm.Register = freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	adm.RegSubj = mqttInflightSubject(getHash(e.id), testF105Gen2, 7)
	if _, err := mqttAdmitPublishTransition(adm); err == nil {
		t.Fatalf("an admission in G2 took a dependency on a G1 resource: that dependency " +
			"could never be discharged, because G1's obligations are not G2's to complete")
	}

	// And an exchange admitted into a generation its authority does not serve is refused
	// too — that is an obligation admitted after closure, reached by naming the old
	// generation rather than by racing the closure.
	wrongGen := testF105Admission(e, al.rec, al.seq, "txn-oldgen", 8, nil)
	if _, err := mqttAdmitPublishTransition(wrongGen); err == nil {
		t.Fatalf("an obligation was admitted into a generation its authority does not serve")
	}
}

// TestF105DisposalPreservesAPendingProposal pins that cleanup running beside a reservation
// or a preparation resolves neither.
//
// The disposal builder dropped the creation proposal, which made its own output invalid
// under RESERVING — a body this broker would refuse to read back — and a transition that
// rewrote either proposal would silently abandon a handoff nobody resolved.
func TestF105DisposalPreservesAPendingProposal(t *testing.T) {
	base := func(state mqttAuthorityState) *mqttAuthorityTransition {
		tr := testF105Transition()
		tr.Attempt.Kind = mqttTransitionDisposeResource
		r := testF105Resource(mqttResourceLive, []string{})
		b, _ := json.Marshal(r)
		tr.Extra = []mqttTransitionMember{{
			subj: mqttResourceSubject(getHash(testF105ID), testF105Gen, "inc-1"), expect: 1, body: b,
		}}
		prev := testF105PreparingAuthority()
		prev.Rev = 1
		prev.Gen, prev.Tenure, prev.OwnerTxn = tr.Prev.Gen, tr.Prev.Tenure, tr.Prev.OwnerTxn
		prev.Intent.PrevGen, prev.Intent.PrevTenure, prev.Intent.PrevOwnerTxn = prev.Gen, prev.Tenure, prev.OwnerTxn
		prev.Intent.Retire.Gen, prev.Intent.Retire.Tenure, prev.Intent.Retire.OwnerTxn = prev.Gen, prev.Tenure, prev.OwnerTxn
		prev.State = state
		if state == mqttAuthorityReserving {
			prev.Intent = nil
			prev.Creation = &mqttCreationIntent{ID: "create-1", Gen: prev.Gen, Tenure: prev.Tenure}
		}
		tr.Prev = prev
		auth := *prev
		auth.Rev, auth.Txn = prev.Rev+1, tr.Attempt.Txn
		tr.Auth = &auth
		tr.Attempt.Rev = auth.Rev
		tr.Attempt.Gen, tr.Attempt.Tenure, tr.Attempt.OwnerTxn = auth.Gen, auth.Tenure, auth.OwnerTxn
		tr.Attempt.PrevTxn = prev.OwnerTxn
		return tr
	}

	for name, state := range map[string]mqttAuthorityState{
		"a pending handoff":            mqttAuthorityPreparing,
		"a first-creation reservation": mqttAuthorityReserving,
	} {
		ok := base(state)
		if err := mqttCheckTransitionKindRules(ok); err != nil {
			t.Fatalf("a disposal beside %s was refused: %v", name, err)
		}
		dropped := base(state)
		dropped.Auth.Intent, dropped.Auth.Creation = nil, nil
		if err := mqttCheckTransitionKindRules(dropped); err == nil {
			t.Fatalf("a disposal beside %s DROPPED the pending proposal: the authority would "+
				"remain in that state with nothing left to finalize", name)
		}
	}

	// And a rewritten proposal is refused too, not merely a dropped one.
	rew := base(mqttAuthorityReserving)
	c := *rew.Prev.Creation
	c.Tenure = "tenure-elsewhere"
	rew.Auth.Creation = &c
	if err := mqttCheckTransitionKindRules(rew); err == nil {
		t.Fatalf("a disposal rewrote the pending creation proposal")
	}
}

// TestF105ResourceDependentListIsNeverNull pins that an empty list is written as `[]`.
//
// A null decodes to a nil slice that reads identically to an empty one, so "this record says
// nothing about its dependents" would be indistinguishable from "nothing depends on it" —
// and the second authorizes a deletion. That is the absence-as-a-fact inference every other
// record in this mechanism refuses, reached through a JSON spelling.
func TestF105ResourceDependentListIsNeverNull(t *testing.T) {
	ok := testF105Resource(mqttResourceLive, []string{})
	body, err := json.Marshal(ok)
	if err != nil {
		t.Fatalf("marshalling the resource: %v", err)
	}
	if !strings.Contains(string(body), `"deps":[]`) {
		t.Fatalf("an empty dependent list was not written as []: %s", body)
	}
	if _, derr := mqttDecodeOutboundResource(body, testF105ID, testF105Epoch, testF105Gen, "inc-1"); derr != nil {
		t.Fatalf("a resource with an empty dependent list was refused: %v", derr)
	}

	nulled := strings.Replace(string(body), `"deps":[]`, `"deps":null`, 1)
	if _, derr := mqttDecodeOutboundResource([]byte(nulled), testF105ID, testF105Epoch, testF105Gen, "inc-1"); derr == nil {
		t.Fatalf("a resource with a null dependent list was admitted: absence would then read " +
			"as 'nothing depends on it', which authorizes a deletion")
	}
	// And the validator refuses it directly, not only through the decoder.
	nilled := testF105Resource(mqttResourceLive, nil)
	if err := mqttValidateOutboundResource(nilled, testF105ID, testF105Epoch, testF105Gen, "inc-1"); err == nil {
		t.Fatalf("a resource carrying a nil dependent list was admitted by the validator")
	}
}

// TestF105OneResourceHeldInTwoRolesReleasesThemSeparately is the defect the ROLE exists for.
//
// Consult 5's finding, restated as a fixture. A QoS 2 exchange needs the PUBREL consumer twice
// over: to carry its canonical release record, and — later — for the output that emits it. Under
// a bare-nonce edge those were ONE edge, so discharging either discharged both, and the resource
// became authorizable for deletion while the other need still stood. The boundary's own check
// did not catch it, because what it checks is mqttExchangePhaseEnded, and Terminal means
// PROTOCOL completion with no outstanding output — not that settlement, a captured handler's
// duty or a shared/affine disposition has finished.
//
// So the property is: two needs, two edges, two discharges, and releasing one leaves the other
// standing and the resource undeletable.
func TestF105OneResourceHeldInTwoRolesReleasesThemSeparately(t *testing.T) {
	const nonce = "xch-two-roles"
	carry := mqttResourceEdge{Nonce: nonce, Role: mqttRoleCanonicalPubRel}
	emit := mqttResourceEdge{Nonce: nonce, Role: mqttRoleOutputAction, Ref: "acq-1"}

	held := testF105ResourceEdges(mqttResourceLive, nil)
	held.Deps = []mqttResourceEdge{}
	withCarry, err := mqttResourceTakeDependency(held, carry)
	if err != nil {
		t.Fatalf("taking the canonical-PUBREL need failed: %v", err)
	}
	// THE SECOND NEED IS NOT A DUPLICATE. A version keyed on the nonce refuses this, which is
	// exactly how the two collapsed into one.
	both, err := mqttResourceTakeDependency(withCarry, emit)
	if err != nil {
		t.Fatalf("taking a second role on the same resource was refused as a duplicate: %v", err)
	}
	if len(both.Deps) != 2 {
		t.Fatalf("one obligation's two needs did not produce two edges: %v", both.Deps)
	}

	// Releasing the OUTPUT need leaves the carrying need standing.
	left, err := mqttResourceReleaseDependency(both, emit)
	if err != nil {
		t.Fatalf("releasing the output need failed: %v", err)
	}
	if len(left.Deps) != 1 || left.Deps[0] != carry {
		t.Fatalf("releasing one role did not leave exactly the other: %v", left.Deps)
	}
	if mqttResourceDeletable(left) {
		t.Fatalf("the resource became deletable while its canonical-PUBREL need still stands")
	}
	// And the same release a second time is refused: a need is discharged once.
	if _, err := mqttResourceReleaseDependency(left, emit); err == nil {
		t.Fatalf("the output need was released twice")
	}

	// THE DELTA SEES ONE EDGE CHANGE, not "no change". A version keyed on the nonce would call
	// this no change at all, because the nonce is still there — and a mutation that dropped the
	// role from the key would pass every assertion above and fail only here.
	added, removed, derr := mqttResourceEdgeDelta(both, left)
	if derr != nil {
		t.Fatalf("the delta refused a single-role release: %v", derr)
	}
	if added != nil {
		t.Fatalf("a release added an edge: %v", added)
	}
	if removed == nil || *removed != emit {
		t.Fatalf("the delta did not name the exact edge released: %v", removed)
	}
}

// TestF105ResourceEdgeShapesAreRefused pins the two rules that stop roles recreating the very
// collapse they fix.
//
// A role that can have several constituents outstanding at once must name WHICH one, or the
// edge stands for all of them and releasing it discharges needs nobody proved. A role that is
// one aggregate obligation for the pair must NOT carry a constituent, or a reader cannot tell
// an empty reference from a forgotten one.
func TestF105ResourceEdgeShapesAreRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		edge mqttResourceEdge
		ok   bool
	}{
		// The positive controls, one of each kind, without which a validator that refused
		// everything would satisfy every row below.
		"an aggregate role with no reference": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleCanonicalPubRel}, true},
		"a constituent role naming its constituent": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleOutputAction, Ref: "acq-1"}, true},

		"a constituent role with no constituent": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleOutputAction}, false},
		"an ack handler with no binding": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleAckHandler}, false},
		"a shared/affine duty with no duty": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleSharedAffine}, false},
		"an aggregate role carrying a constituent": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleSourceReplay, Ref: "acq-1"}, false},
		"a settlement role carrying a constituent": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleSourceSettlement, Ref: "x"}, false},
		"no role at all": {
			mqttResourceEdge{Nonce: "xch-a"}, false},
		"a role this broker does not know": {
			mqttResourceEdge{Nonce: "xch-a", Role: "whatever"}, false},
		"a malformed obligation": {
			mqttResourceEdge{Nonce: "not a token", Role: mqttRoleSourceReplay}, false},
		"a malformed constituent": {
			mqttResourceEdge{Nonce: "xch-a", Role: mqttRoleOutputAction, Ref: "not a token"}, false},
	} {
		err := mqttValidateResourceEdge(tc.edge)
		if tc.ok && err != nil {
			t.Fatalf("%s was refused: %v", name, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

// TestF105DeletionRequiresSealedProvisioning is D5's condition that a zero dependency count
// does not cover.
//
// A resource record is written BEFORE the durable it stands for is confirmed to exist:
// opProvisionResource commits the record so an admission has something to depend on, and the
// physical create is a separate act whose outcome may be unknown. Authorizing a deletion in that
// window makes an orphan — the delete finds nothing, the delayed create lands afterwards, and
// the account carries a durable that no record names and no sweep will attribute.
//
// Zero dependents is therefore necessary and not sufficient, and this pins the difference.
func TestF105DeletionRequiresSealedProvisioning(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-gate")
	defer done()
	e.bootstrap(t, "txn-1")

	// A RETAINED-PAYLOAD resource, a kind the broker can SEAL. This test governs the seal gate,
	// which under the cutover applies to the kinds whose materialization the broker controls;
	// source and PUBREL consumers are exempt (their create is unfenceable) and are arbitrated by
	// the logical-use fence instead — see TestF105LogicalUseKindAbandonsWithoutASeal.
	res, seq := testF105PlantUnsealedResource(t, e, "inc-1", mqttResourceRetainedPayload)
	if len(res.Deps) != 0 {
		t.Fatalf("the fixture planted a resource that already has dependents: %v", res.Deps)
	}

	// NOTHING DEPENDS ON IT, and it is still not authorizable.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, err := mqttAuthorizeDeletionTransition(al.rec, al.seq, "txn-early", "op-early",
		res, seq); err == nil {
		t.Fatalf("a deletion was authorized while the resource's provisioning was still in flight")
	}

	// AND THE BOUNDARY REFUSES IT TOO, submitted DIRECTLY so the composer cannot supply the
	// refusal. The first mutation run showed this was not academic: disabling the boundary's
	// own check killed nothing, because every test reached it through the composer, and a
	// check no test can reach is a check nobody is maintaining. The two answer different
	// callers — the composer tells its caller why the batch cannot be assembled, the boundary
	// refuses a batch assembled by anything at all — so neither is redundant with the other.
	forged := *res
	forged.State, forged.AuthTxn = mqttResourceDeleteAuthorized, "txn-forged"
	fbody, _ := json.Marshal(&forged)
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-forged", Op: "op-forged",
			Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityServing, Rev: al.rec.Rev + 1, Txn: "txn-forged",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(e.id), testF105Gen, "inc-1"),
			expect: seq,
			body:   fbody,
		}},
	}); err == nil {
		t.Fatalf("the write boundary admitted a deletion authorization over unsealed provisioning")
	} else if !strings.Contains(err.Error(), "is not sealed") {
		t.Fatalf("the refusal did not come from the boundary's sealed rule: %v", err)
	}

	// THE SEAL IS THE MISSING FACT. After it — and nothing else changes — the same
	// authorization succeeds, which is what makes this a gate rather than a refusal of
	// everything.
	if _, err := e.asm.jsa.opSealProvisioning(e.id, e.epoch, "txn-seal", "op-seal",
		testF105Gen, "inc-1"); err != nil {
		t.Fatalf("sealing the provisioning was refused: %v", err)
	}
	sealed, sealedSeq, lerr := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, "inc-1")
	if lerr != nil || sealed.Prov != mqttProvisioningSealed {
		t.Fatalf("the seal did not land: %v %v", sealed, lerr)
	}
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	auth, err := mqttAuthorizeDeletionTransition(al.rec, al.seq, "txn-late", "op-late",
		sealed, sealedSeq)
	if err != nil {
		t.Fatalf("assembling the authorization over a sealed resource failed: %v", err)
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(auth); err != nil {
		t.Fatalf("the authorization over a sealed resource was refused: %v", err)
	}
}

// TestF105ProvisioningSealIsForwardOnlyAndAlone pins the two rules that make a seal worth
// relying on.
//
// UNSEALING would let a delayed create land after a deletion was authorized on the strength of
// the seal — the seal's whole value is that it is not revocable. And a seal that ALSO moved an
// edge would carry two different pieces of evidence under one transition, so a reader could not
// say which one the boundary had checked.
//
// Driven at the WRITE BOUNDARY, by submitting the batches directly, because the composer refuses
// them earlier and a kill claimed through the composer would be one the boundary never made.
func TestF105ProvisioningSealIsForwardOnlyAndAlone(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-seal-rules")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-1", mqttResourceSourceConsumer)

	submit := func(t *testing.T, txn string, mutate func(r *mqttOutboundResource)) error {
		t.Helper()
		cur, curSeq, lerr := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, "inc-1")
		if lerr != nil {
			t.Fatalf("reading the resource: %v", lerr)
		}
		next := *cur
		next.Deps = append([]mqttResourceEdge{}, cur.Deps...)
		mutate(&next)
		body, _ := json.Marshal(&next)
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		_, err := e.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
			Attempt: &mqttTransitionAttempt{
				ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
				Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
				Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
			},
			Prev: al.rec, PrevSeq: al.seq,
			Auth: &mqttOutboundAuthority{
				ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
				State: mqttAuthorityServing, Rev: al.rec.Rev + 1, Txn: txn,
				OwnerTxn: al.rec.OwnerTxn,
			},
			Extra: []mqttTransitionMember{{
				subj:   mqttResourceSubject(getHash(e.id), testF105Gen, "inc-1"),
				expect: curSeq,
				body:   body,
			}},
		})
		return err
	}

	// THE POSITIVE CONTROL, first: a plain no-op rewrite of a sealed resource is accepted, so
	// the two refusals below are about what they say and not about this shape being rejected
	// wholesale.
	if err := submit(t, "txn-noop", func(r *mqttOutboundResource) {}); err != nil {
		t.Fatalf("an unchanged sealed resource was refused: %v", err)
	}

	if err := submit(t, "txn-unseal", func(r *mqttOutboundResource) {
		r.Prov = mqttProvisioningPending
	}); err == nil {
		t.Fatalf("a sealed resource was unsealed")
	} else if !strings.Contains(err.Error(), "not revocable") {
		t.Fatalf("the refusal did not come from the forward-only rule: %v", err)
	}

	// And a SEAL beside an edge change. It has to be a RELEASE rather than an addition: an
	// added edge is refused earlier, by the rule that a dependency is taken only by the
	// admission creating its obligation, and a fixture that tripped that would prove nothing
	// about this rule. So the resource is planted unsealed, an obligation is admitted on it and
	// finished, and the batch then tries to seal and release in one act.
	e2, done2 := testF105NewEnv(t, "f105-seal-alone")
	defer done2()
	e2.bootstrap(t, "txn-1")
	// A SEALABLE kind, deliberately: a source or PUBREL consumer is never sealed, and its hold is
	// released by its obligation's ending rather than by a later act (F-105 7c-lc), so it could not
	// offer the edge this rule needs to see beside a seal.
	testF105PlantUnsealedResource(t, e2, "inc-2", mqttResourceRetainedPayload)
	nonce := testF105AdmitOn(t, e2, "txn-adm", 7, "inc-2")
	testF105FinishExchange(t, e2, nonce)
	cur, curSeq, lerr := e2.asm.jsa.loadOutboundResource(e2.id, e2.epoch, testF105Gen, "inc-2")
	if lerr != nil {
		t.Fatalf("reading the unsealed resource: %v", lerr)
	}
	if len(cur.Deps) != 1 {
		t.Fatalf("the fixture did not leave exactly one edge to release: %v", cur.Deps)
	}
	both := *cur
	both.Prov = mqttProvisioningSealed
	both.Deps = []mqttResourceEdge{}
	body, _ := json.Marshal(&both)
	al := e2.asm.jsa.loadOutboundAuthority(e2.id, e2.epoch)
	_, err := e2.asm.jsa.commitAuthorityTransition(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e2.id, Epoch: e2.epoch, Txn: "txn-both", Op: "op-both",
			Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e2.id, Epoch: e2.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: mqttAuthorityServing, Rev: al.rec.Rev + 1, Txn: "txn-both",
			OwnerTxn: al.rec.OwnerTxn,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(e2.id), testF105Gen, "inc-2"),
			expect: curSeq,
			body:   body,
		}},
	})
	if err == nil {
		t.Fatalf("one act both sealed the provisioning and changed the dependents")
	}
	if !strings.Contains(err.Error(), "two facts with two proofs") {
		t.Fatalf("the refusal did not come from the one-act-one-fact rule: %v", err)
	}

	// A SECOND SEAL IS REFUSED rather than quietly succeeding: a transition establishing
	// nothing looks to its caller exactly like one that did.
	if _, err := e.asm.jsa.opSealProvisioning(e.id, e.epoch, "txn-reseal", "op-reseal",
		testF105Gen, "inc-1"); err == nil {
		t.Fatalf("an already-sealed resource was sealed again")
	}
}

// TestF105RoleReleaseNeedsThatRolesOwnEvidence is round I-2's third finding, which is the one
// that showed the roles were representation without enforcement.
//
// Adding Role and Ref DISTINGUISHES edges. It does not, by itself, make releasing one safe: the
// boundary still checked only mqttExchangePhaseEnded, which is PROTOCOL completion with no
// outstanding output. That is not a certificate that source settlement, a captured handler's
// duty or a shared/affine disposition has finished — so an ended exchange could release its
// source-settlement edge, that edge could be the last one on a provisioning-sealed resource, and
// the resource the unfinished duty still needs became deletable. F-105's own defect, reached
// through the mechanism built to prevent it.
//
// So the release rule is now a TABLE: the exchange's ending is the floor for every role and the
// whole proof for exactly two — the ones the protocol itself concludes. The other four fail
// CLOSED until their evidence exists, which costs reclamation and never costs an obligation.
func TestF105RoleReleaseNeedsThatRolesOwnEvidence(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-role-evidence")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-rel")
	testF105FinishExchange(t, e, nonce)

	// THE OBLIGATION HAS ENDED. That is the floor, and for the canonical release it is also the
	// whole proof — so that edge went, in the ending's own batch (F-105 7c-lc), which is what
	// makes every refusal below about the ROLE rather than about the exchange.
	testF105RequireDeps(t, e, "inc-rel")

	for _, role := range []mqttResourceRole{
		mqttRoleOutputAction, mqttRoleAckHandler, mqttRoleSourceSettlement, mqttRoleSharedAffine,
	} {
		// The edge has to exist to be released, so it is taken first — which also proves the
		// refusal below is the RELEASE rule and not the resource refusing the role outright.
		cur, curSeq := testF105LoadResource(t, e, "inc-rel")
		// The REF is supplied only where the role needs one: an aggregate role carrying a
		// constituent is refused by the edge validator, which would be a different refusal
		// than the one under test.
		edge := mqttResourceEdge{Nonce: nonce, Role: role}
		if mqttRoleNeedsRef(role) {
			edge.Ref = "ref-1"
		}
		withEdge := *cur
		withEdge.Deps = append(append([]mqttResourceEdge{}, cur.Deps...), edge)
		body, merr := json.Marshal(&withEdge)
		if merr != nil {
			t.Fatalf("marshalling: %v", merr)
		}
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		// Planted directly: taking an edge through the boundary requires the admission that
		// creates its obligation, and this obligation already exists.
		msg, hdr := mqttFrameBatchWrite(curSeq, body, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(
			mqttResourceSubject(getHash(e.id), testF105Gen, "inc-rel"), hdr, msg); err != nil {
			t.Fatalf("planting the %q edge: %v", role, err)
		}
		_ = al

		err := testF105ReleaseEdge(t, e, "inc-rel", "txn-rel-"+string(role), edge)
		if err == nil {
			t.Fatalf("the %q edge was released on its obligation's ending alone", role)
		}
		if !strings.Contains(err.Error(), "not this role's discharge") {
			t.Fatalf("the %q refusal did not come from the discharge table: %v", role, err)
		}

		// AND THE EDGE STILL STANDS, so the resource stays needed rather than quietly freed.
		after, _ := testF105LoadResource(t, e, "inc-rel")
		found := false
		for _, have := range after.Deps {
			if have == edge {
				found = true
			}
		}
		if !found {
			t.Fatalf("the refused release removed the %q edge anyway", role)
		}
		// Remove it directly so the next role starts clean.
		cleaned := *after
		cleaned.Deps = nil
		for _, have := range after.Deps {
			if have != edge {
				cleaned.Deps = append(cleaned.Deps, have)
			}
		}
		if cleaned.Deps == nil {
			cleaned.Deps = []mqttResourceEdge{}
		}
		cbody, cerr := json.Marshal(&cleaned)
		if cerr != nil {
			t.Fatalf("marshalling the cleaned resource: %v", cerr)
		}
		_, afterSeq := testF105LoadResource(t, e, "inc-rel")
		cmsg, chdr := mqttFrameBatchWrite(afterSeq, cbody, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(
			mqttResourceSubject(getHash(e.id), testF105Gen, "inc-rel"), chdr, cmsg); err != nil {
			t.Fatalf("clearing the %q edge: %v", role, err)
		}
	}
}
