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
)

// Every claim about STORED state is checked against the store, at the write boundary.
//
// This file is one class, not three tests. Three separate findings had the same shape — the
// exchange's phase-only guarantee, the authority's predecessor, the resource's zero dependency
// count — and in each the rule was enforced by the helper that COMPOSED the transition. A
// helper is optional. Every check a transition can make about itself still passes for a body
// substituted after it was built, and the CAS passes because the sequence is right, so the
// guarantee held only for callers that chose to use the builder.

// TestF105PredecessorIsComparedWithTheStore pins that the rules deciding what a transition
// may displace are conclusions about the STANDING authority.
//
// Every kind rule reads t.Prev: which state may be displaced, whether ownership is preserved,
// whether a proposal is pending. Without this check a caller copies the standing SERVING
// authority, sets its State to CLOSED, keeps its real sequence, and reserves a fresh
// generation over it — live ownership displaced with no handoff and no permanent closure.
func TestF105PredecessorIsComparedWithTheStore(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-prev-boundary")
	defer done()

	auth, _ := e.bootstrap(t, "txn-1")
	load := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if load.status != mqttOutboundValid || load.rec.State != mqttAuthorityServing {
		t.Fatalf("the serving authority does not load back (status %v)", load.status)
	}

	// THE SUBSTITUTION: a SERVING authority described as CLOSED, at its real sequence, so a
	// claim may be reserved over it.
	lying := *load.rec
	lying.State = mqttAuthorityClosed
	claim := testF105ReserveCreation(e, &lying, load.seq, "txn-claim", testF105Gen2,
		"tenure-claim", "intent-claim")
	if _, err := e.asm.jsa.commitAuthorityTransition(claim); err == nil {
		t.Fatalf("a claim was reserved over a SERVING authority described as closed: the " +
			"predecessor is trusted as supplied")
	}
	after := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if after.rec.State != mqttAuthorityServing || after.seq != load.seq {
		t.Fatalf("the refused claim moved the authority: %+v", after.rec)
	}

	// The same substitution in its other direction: a pending proposal ERASED by describing
	// a predecessor that carries none. Reach PREPARING first.
	intent := &mqttHandoffIntent{
		ID: "intent-1", PrevGen: auth.Gen, PrevTenure: auth.Tenure,
		PrevOwnerTxn: auth.OwnerTxn, PrevSessWID: "wid-0", PrevSessSeq: 12,
		Tenure: "tenure-next", Gen: testF105Gen2, CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
			SessWID: "wid-0", SessSeq: 12, Reason: mqttRetireCleanStart, Discard: true,
		},
	}
	prep := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-prep", Op: "op-prep",
			Kind: mqttTransitionPrepareIntent, PrevTxn: load.rec.OwnerTxn, Rev: load.rec.Rev + 1,
			Gen: load.rec.Gen, Tenure: load.rec.Tenure, OwnerTxn: load.rec.OwnerTxn,
		},
		Prev: load.rec, PrevSeq: load.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: load.rec.Gen, Tenure: load.rec.Tenure,
			State: mqttAuthorityPreparing, Rev: load.rec.Rev + 1, Txn: "txn-prep",
			OwnerTxn: load.rec.OwnerTxn, Intent: intent,
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(prep); err != nil {
		t.Fatalf("the preparation was refused: %v", err)
	}
	load = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if load.rec.State != mqttAuthorityPreparing || load.rec.Intent == nil {
		t.Fatalf("the authority is not preparing with an intent (state %d)", load.rec.State)
	}

	// A PREPARING authority described as SERVING with no pending proposal, so an admission
	// passes the freeze it should have been refused by.
	erased := *load.rec
	erased.State, erased.Intent = mqttAuthorityServing, nil
	admit := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-sneak", Op: "op-sneak",
			Kind: mqttTransitionAdmitExchange, PrevTxn: erased.OwnerTxn, Rev: erased.Rev + 1,
			Gen: erased.Gen, Tenure: erased.Tenure, OwnerTxn: erased.OwnerTxn,
		},
		Prev: &erased, PrevSeq: load.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: erased.Gen, Tenure: erased.Tenure,
			State: mqttAuthorityServing, Rev: erased.Rev + 1, Txn: "txn-sneak",
			OwnerTxn: erased.OwnerTxn,
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(admit); err == nil {
		t.Fatalf("an admission ran against a PREPARING authority described as serving, and " +
			"erased its pending handoff")
	}
	back := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if back.rec.State != mqttAuthorityPreparing || back.rec.Intent == nil ||
		back.rec.Intent.ID != intent.ID {
		t.Fatalf("the refused admission disturbed the pending handoff: %+v", back.rec)
	}

	// The legitimate transition over the SAME predecessor still commits, so the check
	// refuses substitutions rather than everything. A cancellation needs its proposal's
	// metadata half to have been positively ABORTED — the authority record's shape is not
	// what licenses it.
	hp, herr := mqttProposalFromHandoff(intent)
	if herr != nil {
		t.Fatalf("viewing the pending handoff as a proposal failed: %v", herr)
	}
	if v := e.asm.jsa.settleProposal(_EMPTY_, e.id, e.epoch, hp); v != mqttMetadataAborted {
		t.Fatalf("settling the pending handoff returned %v, want ABORTED", v)
	}
	cancel := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-cancel", Op: "op-cancel",
			Kind: mqttTransitionCancelIntent, PrevTxn: back.rec.OwnerTxn, Rev: back.rec.Rev + 1,
			Gen: intent.PrevGen, Tenure: intent.PrevTenure, OwnerTxn: intent.PrevOwnerTxn,
			Intent: intent.ID,
		},
		Prev: back.rec, PrevSeq: back.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: intent.PrevGen, Tenure: intent.PrevTenure,
			State: mqttAuthorityServing, Rev: back.rec.Rev + 1, Txn: "txn-cancel",
			OwnerTxn: intent.PrevOwnerTxn,
		},
	}
	if _, err := e.asm.jsa.commitAuthorityTransition(cancel); err != nil {
		t.Fatalf("a legitimate cancellation over the standing predecessor was refused: %v", err)
	}
}

// TestF105ResourceReleaseMustProveTheObligationEnded is the whole meaning of a deletion
// authorization, and the correction to the version that only counted.
//
// A COUNT CAN BE DRAINED BY TRANSITIONS THAT ENDED NOTHING. "One fewer" is an assertion; a
// caller stepping it down one at a time reaches zero with every obligation still outstanding,
// and then authorizes deleting the PUBREL consumer whose interest-retained records are those
// obligations' only PUBREC evidence. A NAMED edge can be removed only by proving THAT
// obligation reached its end, and only once, because the second attempt names something the
// list no longer holds.
func TestF105ResourceReleaseMustProveTheObligationEnded(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-res-boundary")
	defer done()

	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)

	// TWO obligations depend on the PUBREL consumer.
	a := testF105AdmitOn(t, e, "txn-adm-1", 7, "inc-rel")
	b := testF105AdmitOn(t, e, "txn-adm-2", 8, "inc-rel")
	held, heldSeq := testF105LoadResource(t, e, "inc-rel")
	if len(held.Deps) != 2 {
		t.Fatalf("the PUBREL consumer is needed by %v, want both obligations", held.Deps)
	}

	// (1) A PREMATURE RELEASE IS REFUSED. Neither exchange has ended.
	if err := testF105ReleaseDependency(t, e, "inc-rel", a, "txn-early"); err == nil {
		t.Fatalf("a dependency was released while its obligation was still outstanding")
	}

	// (2) AN ASSERTED ZERO IS REFUSED — the shape the count-only version admitted, and the
	// shape a caller reaches by writing the body directly rather than through a helper.
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	lying := *held
	// An EMPTY list, not a nil one: a nil marshals to null and is refused by the record's
	// own shape, which would mask the rule under test.
	lying.State, lying.AuthTxn, lying.Deps = mqttResourceDeleteAuthorized, "txn-lie", []mqttResourceEdge{}
	body, merr := json.Marshal(&lying)
	if merr != nil {
		t.Fatalf("marshalling the substituted resource: %v", merr)
	}
	sneak := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-lie", Op: "op-lie",
			Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: al.rec.State, Rev: al.rec.Rev + 1, Txn: "txn-lie", OwnerTxn: al.rec.OwnerTxn,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(e.id), testF105Gen, "inc-rel"),
			expect: heldSeq, body: body,
		}},
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(sneak); cerr == nil {
		t.Fatalf("a deletion was authorized by dropping two live obligations from the list")
	}

	// (3) AN EDGE MAY NOT BE ASSERTED EITHER. Adding a name this transition does not admit
	// would let a caller pin a resource undeletable forever.
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	inflated := *held
	inflated.Deps = append(append([]mqttResourceEdge{}, held.Deps...),
		mqttResourceEdge{Nonce: mqttNewInflightNonce(), Role: mqttRoleSourceReplay})
	ibody, _ := json.Marshal(&inflated)
	pin := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: "txn-pin", Op: "op-pin",
			Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: al.rec.State, Rev: al.rec.Rev + 1, Txn: "txn-pin", OwnerTxn: al.rec.OwnerTxn,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(e.id), testF105Gen, "inc-rel"),
			expect: heldSeq, body: ibody,
		}},
	}
	if _, cerr := e.asm.jsa.commitAuthorityTransition(pin); cerr == nil {
		t.Fatalf("a dependency was asserted for an obligation this transition does not admit")
	}

	// (4) FINISH ONE OBLIGATION, and only its edge goes — IN THE ENDING'S OWN BATCH (F-105 7c-lc):
	// the terminal advance releases a's hold, and nothing else.
	testF105FinishExchange(t, e, a)
	if err := testF105ReleaseDependency(t, e, "inc-rel", b, "txn-wrong"); err == nil {
		t.Fatalf("finishing one obligation released another obligation's dependency")
	}
	// NO LATER RELEASE DOOR. The ending already released a's edge; a standalone release of a
	// hold edge is refused whatever the stored phase says.
	if err := testF105ReleaseDependency(t, e, "inc-rel", a, "txn-rel-a2"); err == nil {
		t.Fatalf("a finished obligation's hold was released a second time, after its ending")
	}

	// (5) The resource is still not authorizable: the second obligation stands.
	cur, curSeq0 := testF105LoadResource(t, e, "inc-rel")
	if len(cur.Deps) != 1 || cur.Deps[0].Nonce != b {
		t.Fatalf("the PUBREL consumer is needed by %v, want only the outstanding obligation", cur.Deps)
	}
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, aerr := mqttAuthorizeDeletionTransition(al.rec, al.seq, "txn-auth-early", "op-auth",
		cur, curSeq0); aerr == nil {
		t.Fatalf("a deletion was authorized while an obligation still needs the resource")
	}

	// (6) Finish the second: its ending releases the last edge in its own batch (F-105 7c-lc),
	// and only then does the authorization win.
	testF105FinishExchange(t, e, b)
	last, curSeq := testF105LoadResource(t, e, "inc-rel")
	if len(last.Deps) != 0 {
		t.Fatalf("the second obligation's ending did not release the last edge: %v", last.Deps)
	}
	_ = curSeq
	// A PUBREL consumer is retired on a route's disposal decision, and its authorization retires
	// its name in the same batch: through the paired production operation (F-105 7c-lc).
	al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	if _, derr := e.asm.jsa.opRequestDisposal(&mqttRequestDisposalRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-disp", Gen: testF105Gen, Inc: "inc-rel",
		Reason: mqttDisposalUnsubscribed, Entitlement: mqttOutboundEntitlement{Gen: al.rec.Gen, Tenure: al.rec.Tenure},
	}); derr != nil {
		t.Fatalf("disposing of the released consumer was refused: %v", derr)
	}
	if _, cerr := e.asm.jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-auth", testF105Gen, "inc-rel"); cerr != nil {
		t.Fatalf("authorizing the disposed, released consumer was refused: %v", cerr)
	}
	done2, doneSeq := testF105LoadResource(t, e, "inc-rel")
	if !mqttResourceDeletable(done2) {
		t.Fatalf("the resource is not deletable after a won authorization: state %d", done2.State)
	}

	// (7) Nothing may be added or resurrected afterwards.
	for name, mutate := range map[string]func(r *mqttOutboundResource){
		"adding a dependency after authorization": func(r *mqttOutboundResource) {
			r.Deps = []mqttResourceEdge{{
				Nonce: mqttNewInflightNonce(), Role: mqttRoleSourceReplay,
			}}
		},
		"dropping the dependent list entirely": func(r *mqttOutboundResource) {
			r.Deps = nil
		},
		"resurrecting it to live": func(r *mqttOutboundResource) {
			r.State, r.AuthTxn = mqttResourceLive, _EMPTY_
		},
		"re-authorizing under another transition": func(r *mqttOutboundResource) {
			r.AuthTxn = "txn-someone-else"
		},
	} {
		al = e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		m := *done2
		mutate(&m)
		mbody, _ := json.Marshal(&m)
		bad := &mqttAuthorityTransition{
			Attempt: &mqttTransitionAttempt{
				ID: e.id, Epoch: e.epoch, Txn: "txn-after", Op: "op-after",
				Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
				Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
			},
			Prev: al.rec, PrevSeq: al.seq,
			Auth: &mqttOutboundAuthority{
				ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
				State: al.rec.State, Rev: al.rec.Rev + 1, Txn: "txn-after", OwnerTxn: al.rec.OwnerTxn,
			},
			Extra: []mqttTransitionMember{{
				subj:   mqttResourceSubject(getHash(e.id), testF105Gen, "inc-rel"),
				expect: doneSeq, body: mbody,
			}},
		}
		if _, cerr := e.asm.jsa.commitAuthorityTransition(bad); cerr == nil {
			t.Fatalf("an authorized resource was mutated by %s", name)
		}
	}
}

// testF105LoadResource reads one resource as it durably stands, with its sequence.
func testF105LoadResource(t *testing.T, e *testF105Env, inc string) (*mqttOutboundResource, uint64) {
	t.Helper()
	msg, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttResourceSubject(getHash(e.id), testF105Gen, inc))
	if err != nil {
		t.Fatalf("loading resource %q: %v", inc, err)
	}
	r, derr := mqttDecodeOutboundResource(msg.Data, e.id, e.epoch, testF105Gen, inc)
	if derr != nil {
		t.Fatalf("decoding resource %q: %v", inc, derr)
	}
	return r, msg.Sequence
}

// TestF105AdmissionRequiresLiveOwnedName is the step-1 illegal boundary (F-105 §5.1 point 3,
// Codex consult 11): admitting a dependency on a name-governed consumer (source/PUBREL)
// requires its name-ownership record to stand LIVE and be owned by exactly this incarnation.
// A missing, RETIRED, or foreign-owned name refuses the admission. The dependency edge alone
// must never license depending on a consumer a retirement may already have spent — otherwise a
// teardown could delete the consumer that carries this obligation's PUBREC evidence.
func TestF105AdmissionRequiresLiveOwnedName(t *testing.T) {
	plantSource := func(t *testing.T, e *testF105Env, inc string) {
		t.Helper()
		r := &mqttOutboundResource{
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Inc: inc,
			Kind: mqttResourceSourceConsumer, Name: "$MQTT_" + inc, State: mqttResourceLive,
			Prov: mqttProvisioningSealed, Deps: []mqttResourceEdge{}, ProvTenure: "prov-t0",
		}
		b, _ := json.Marshal(r)
		msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(mqttResourceSubject(getHash(e.id), testF105Gen, inc), hdr, msg); err != nil {
			t.Fatalf("planting source resource %q: %v", inc, err)
		}
	}
	plantOwn := func(t *testing.T, e *testF105Env, name string, state mqttNameOwnState, ownerInc string) {
		t.Helper()
		o := &mqttNameOwnership{
			ID: e.id, Epoch: e.epoch, Stream: mqttOwnedStreamMsgs, Name: name, State: state,
			OwnerGen: testF105Gen, OwnerInc: ownerInc,
		}
		b, _ := json.Marshal(o)
		msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
		if _, err := e.asm.jsa.storeMsg(mqttNameOwnershipSubject(mqttOwnedStreamMsgs, name), hdr, msg); err != nil {
			t.Fatalf("planting ownership for %q: %v", name, err)
		}
	}
	admit := func(t *testing.T, e *testF105Env, inc string) error {
		t.Helper()
		reg := freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		})
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		if al.status != mqttOutboundValid {
			t.Fatalf("the authority does not load back: status %v", al.status)
		}
		_, err := e.asm.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-adm", Op: "op-adm",
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, Register: reg,
			Deps: []mqttAdmitDependency{{Inc: inc, Role: mqttRoleSourceReplay}},
		})
		return err
	}

	t.Run("a missing owner refuses the admission", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-own-missing")
		defer done()
		e.bootstrap(t, "txn-1")
		plantSource(t, e, "inc-src") // resource only; NO ownership record
		err := admit(t, e, "inc-src")
		if err == nil {
			t.Fatal("an admission against a consumer with no name-ownership record was accepted")
		}
		if !strings.Contains(err.Error(), "no name-ownership record stands") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	t.Run("a retired owner refuses the admission", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-own-retired")
		defer done()
		e.bootstrap(t, "txn-1")
		plantSource(t, e, "inc-src")
		plantOwn(t, e, "$MQTT_inc-src", mqttNameOwnRetired, "inc-src") // spent name
		err := admit(t, e, "inc-src")
		if err == nil {
			t.Fatal("an admission against a RETIRED name was accepted")
		}
		if !strings.Contains(err.Error(), "not LIVE") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	t.Run("a foreign-owned name refuses the admission", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-own-foreign")
		defer done()
		e.bootstrap(t, "txn-1")
		plantSource(t, e, "inc-src")
		plantOwn(t, e, "$MQTT_inc-src", mqttNameOwnLive, "other-inc") // LIVE but a different owner
		err := admit(t, e, "inc-src")
		if err == nil {
			t.Fatal("an admission against a name owned by another incarnation was accepted")
		}
		if !strings.Contains(err.Error(), "owned by incarnation") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})
}
