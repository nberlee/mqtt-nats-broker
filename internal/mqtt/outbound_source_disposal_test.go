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
	"errors"
	"testing"
)

// F-105 7c-lc: a logical-use consumer is retired only on a route's disposal decision, taken under
// the entitlement that still serves, and its retirement retires its name in the same batch.

func testDisposalEnt(auth *mqttOutboundAuthority) mqttOutboundEntitlement {
	return mqttOutboundEntitlement{Gen: auth.Gen, Tenure: auth.Tenure}
}

func testRequestDisposal(t *testing.T, e *testF105Env, auth *mqttOutboundAuthority, inc, txn string) {
	t.Helper()
	if _, err := e.asm.jsa.opRequestDisposal(&mqttRequestDisposalRequest{
		ID: e.id, Epoch: e.epoch, Txn: txn, Gen: testF105Gen, Inc: inc,
		Reason: mqttDisposalUnsubscribed, Entitlement: testDisposalEnt(auth),
	}); err != nil {
		t.Fatalf("disposing of %q was refused: %v", inc, err)
	}
}

// testDisposeBatch builds a dispose-resource transition carrying the given resource members over
// the standing authority, for a direct boundary submission.
func testDisposeBatch(t *testing.T, e *testF105Env, txn string, members ...mqttTransitionMember) *mqttAuthorityTransition {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: e.id, Epoch: e.epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionDisposeResource, PrevTxn: al.rec.OwnerTxn, Rev: al.rec.Rev + 1,
			Gen: al.rec.Gen, Tenure: al.rec.Tenure, OwnerTxn: al.rec.OwnerTxn,
		},
		Prev: al.rec, PrevSeq: al.seq,
		Auth: &mqttOutboundAuthority{
			ID: e.id, Epoch: e.epoch, Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			State: al.rec.State, Rev: al.rec.Rev + 1, Txn: txn, OwnerTxn: al.rec.OwnerTxn,
		},
		Extra: members,
	}
}

func testResourceMember(t *testing.T, e *testF105Env, r *mqttOutboundResource, seq uint64) mqttTransitionMember {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return mqttTransitionMember{subj: mqttResourceSubject(getHash(e.id), r.Gen, r.Inc), expect: seq, body: b}
}

// T4: an admission and a disposal contend on the resource record, in both orders.
func TestF105DisposalRequestFencesAdmission(t *testing.T) {
	t.Run("disposal first refuses the admission at the boundary", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-disp-t4a")
		defer done()
		auth, _ := e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		testRequestDisposal(t, e, auth, "inc-src", "txn-disp")
		// THROUGH THE OPERATION, which composes the edge without consulting the decision: the
		// refusal is the boundary's (M6).
		al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)
		_, err := e.asm.jsa.opAdmitPublish(&mqttAdmitPublishRequest{
			ID: e.id, Epoch: e.epoch, Txn: "txn-adm", Op: "op-adm", Gen: al.rec.Gen, Tenure: al.rec.Tenure,
			Register: freshRegisterLineage(&mqttInflightRecord{
				State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 1, Nonce: mqttNewInflightNonce(),
			}),
			Deps: []mqttAdmitDependency{{Inc: "inc-src", Role: mqttRoleSourceReplay}},
		})
		testRequireRefusal(t, err, "admits no new obligation")
		testF105RequireDeps(t, e, "inc-src")
		// And readiness says so too, before any batch.
		if rerr := testReadyForUse(&e.asm.jsa, e.id, e.epoch, testF105Gen, "inc-src"); rerr == nil {
			t.Fatal("a disposed consumer was reported ready for use")
		}
	})
	t.Run("admission first holds the retirement until its ending", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-disp-t4b")
		defer done()
		auth, _ := e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		nonce, _, _, _ := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
		testRequestDisposal(t, e, auth, "inc-src", "txn-disp")
		if _, err := e.asm.jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth-early", "op-a", testF105Gen, "inc-src"); err == nil {
			t.Fatal("a disposed consumer was authorized while an obligation still holds it")
		}
		if _, err := testAdvanceLive(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, nonce, mqttExchangeTerminal); err != nil {
			t.Fatalf("ending the obligation was refused: %v", err)
		}
		if _, err := e.asm.jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-a", testF105Gen, "inc-src"); err != nil {
			t.Fatalf("authorizing the disposed, released consumer was refused: %v", err)
		}
		r, _ := testF105LoadResource(t, e, "inc-src")
		if r.State != mqttResourceDeleteAuthorized || r.Disposal == nil {
			t.Fatalf("the authorized resource is %+v", r)
		}
		o, _, _ := e.asm.jsa.loadNameOwnership(e.id, e.epoch, mqttOwnedStreamMsgs, r.Name)
		if o == nil || o.State != mqttNameOwnRetired {
			t.Fatalf("the authorization did not retire the name: %+v", o)
		}
	})
	t.Run("the decision is permanent", func(t *testing.T) {
		e, done := testF105NewEnv(t, "f105-disp-t4c")
		defer done()
		auth, _ := e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		testRequestDisposal(t, e, auth, "inc-src", "txn-disp")
		// Idempotent: a second route finds the decision and stops.
		testRequestDisposal(t, e, auth, "inc-src", "txn-disp-2")
		r, seq := testF105LoadResource(t, e, "inc-src")
		if r.Disposal.Txn != "txn-disp" {
			t.Fatalf("a second decision replaced the first: %+v", r.Disposal)
		}
		cleared := *r
		cleared.Disposal = nil
		_, err := e.asm.jsa.commitAuthorityTransition(testDisposeBatch(t, e, "txn-clear", testResourceMember(t, e, &cleared, seq)))
		testRequireRefusal(t, err, "rewrites or clears the disposal decision")
	})
}

// T5: an idle serving consumer is not retired, and the refusal is made at both layers (M7, M8a, M8b).
func TestF105IdleServingSourceIsNotRetired(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-disp-t5")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)

	// The classifier withholds the cleanup of a serving consumer (M7).
	v := e.asm.jsa.classifyResourceDuty(e.id, e.epoch, testF105Gen, false,
		&mqttDutyEntry{Kind: mqttDutyCleanup, Ref: "inc-src"})
	if v.Disposition != mqttDutyWithheld {
		t.Fatalf("an idle serving consumer's cleanup was classified %+v, want withheld", v)
	}

	// The COMPOSER refuses, with its own reason — under M8a the batch would reach the boundary and
	// be refused there instead, with a different one.
	_, err := e.asm.jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-a", testF105Gen, "inc-src")
	testRequireRefusal(t, err, "no route has disposed of, so it is serving")

	// And the BOUNDARY refuses a hand-built authorization that pairs the name retirement (M8b).
	r, seq := testF105LoadResource(t, e, "inc-src")
	auth := *r
	auth.State, auth.AuthTxn = mqttResourceDeleteAuthorized, "txn-forged"
	om, merr := mqttNameOwnershipMember(e.id, e.epoch, mqttOwnedStreamMsgs, r.Name, mqttNameOwnRetired,
		testF105Gen, "inc-src", testF105OwnershipSeq(t, e, r.Name))
	if merr != nil {
		t.Fatal(merr)
	}
	_, err = e.asm.jsa.commitAuthorityTransition(testDisposeBatch(t, e, "txn-forged",
		testResourceMember(t, e, &auth, seq), om))
	testRequireRefusal(t, err, "which no route has disposed of")
	if got, _ := testF105LoadResource(t, e, "inc-src"); got.State != mqttResourceLive {
		t.Fatalf("a refused authorization moved the resource to %d", got.State)
	}
}

func testF105OwnershipSeq(t *testing.T, e *testF105Env, name string) uint64 {
	t.Helper()
	_, seq, err := e.asm.jsa.loadNameOwnership(e.id, e.epoch, mqttOwnedStreamMsgs, name)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// T11: the deletion authorization and the name retirement ride one batch, both directions (M12a-c).
func TestF105DisposalPairIsEnforced(t *testing.T) {
	setup := func(t *testing.T, id string) (*testF105Env, func(), *mqttOutboundResource, uint64) {
		e, done := testF105NewEnv(t, id)
		auth, _ := e.bootstrap(t, "txn-1")
		testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
		testF105PlantResource(t, e, "inc-two", mqttResourceSourceConsumer)
		testRequestDisposal(t, e, auth, "inc-src", "txn-disp")
		r, seq := testF105LoadResource(t, e, "inc-src")
		return e, done, r, seq
	}
	authorized := func(r *mqttOutboundResource, txn string) *mqttOutboundResource {
		a := *r
		a.State, a.AuthTxn = mqttResourceDeleteAuthorized, txn
		return &a
	}
	t.Run("resource without its name", func(t *testing.T) {
		e, done, r, seq := setup(t, "f105-pair-a")
		defer done()
		_, err := e.asm.jsa.commitAuthorityTransition(testDisposeBatch(t, e, "txn-x",
			testResourceMember(t, e, authorized(r, "txn-x"), seq)))
		testRequireRefusal(t, err, "without retiring its name")
	})
	t.Run("name without its resource", func(t *testing.T) {
		e, done, r, _ := setup(t, "f105-pair-b")
		defer done()
		om, _ := mqttNameOwnershipMember(e.id, e.epoch, mqttOwnedStreamMsgs, r.Name, mqttNameOwnRetired,
			testF105Gen, "inc-src", testF105OwnershipSeq(t, e, r.Name))
		_, err := e.asm.jsa.commitAuthorityTransition(testDisposeBatch(t, e, "txn-x", om))
		testRequireRefusal(t, err, "without authorizing that resource's deletion")
	})
	t.Run("mismatched pair", func(t *testing.T) {
		e, done, r, seq := setup(t, "f105-pair-c")
		defer done()
		two, _ := testF105LoadResource(t, e, "inc-two")
		om, _ := mqttNameOwnershipMember(e.id, e.epoch, mqttOwnedStreamMsgs, two.Name, mqttNameOwnRetired,
			testF105Gen, "inc-two", testF105OwnershipSeq(t, e, two.Name))
		_, err := e.asm.jsa.commitAuthorityTransition(testDisposeBatch(t, e, "txn-x",
			testResourceMember(t, e, authorized(r, "txn-x"), seq), om))
		if err == nil {
			t.Fatal("an authorization paired with another name's retirement was accepted")
		}
	})
	t.Run("the paired production authorization lands", func(t *testing.T) {
		e, done, _, _ := setup(t, "f105-pair-d")
		defer done()
		if _, err := e.asm.jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-a", testF105Gen, "inc-src"); err != nil {
			t.Fatalf("the paired authorization was refused: %v", err)
		}
	})
}

// T7b-1: a decision is licensed by the entitlement it was taken under, at both layers (M17a/b).
func TestF105DisposalStaleEntitlementContract(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-disp-stale")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	stale := mqttOutboundEntitlement{Gen: auth.Gen, Tenure: "tenure-displaced"}

	// The composer refuses with the typed reason the custodian drops on.
	_, err := e.asm.jsa.opRequestDisposal(&mqttRequestDisposalRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-stale", Gen: testF105Gen, Inc: "inc-src",
		Reason: mqttDisposalUnsubscribed, Entitlement: stale,
	})
	if !errors.Is(err, errMQTTDisposalEntitlementMoved) {
		t.Fatalf("a stale entitlement's disposal was not refused by the composer: %v", err)
	}

	// The boundary refuses the hand-built write.
	r, seq := testF105LoadResource(t, e, "inc-src")
	d := *r
	d.Disposal = &mqttResourceDisposal{Txn: "txn-stale-b", Reason: mqttDisposalUnsubscribed,
		Gen: stale.Gen, Tenure: stale.Tenure}
	_, err = e.asm.jsa.commitAuthorityTransition(testDisposeBatch(t, e, "txn-stale-b", testResourceMember(t, e, &d, seq)))
	testRequireRefusal(t, err, "which is not the serving one")
	if got, _ := testF105LoadResource(t, e, "inc-src"); got.Disposal != nil {
		t.Fatalf("a stale decision was written: %+v", got.Disposal)
	}
}

// retirementPendingFor reports whether a disposal of the named consumer is in the session's
// retirement custody.
func (sess *mqttSession) retirementPendingFor(name string) bool {
	rc := &sess.retainedReclaim
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for k, st := range rc.pending {
		if k.src && st.src != nil && st.src.name == name {
			return true
		}
	}
	return false
}

// H-1: a resource is never CREATED carrying a disposal decision — that would bypass the
// entitlement licence and the introduction rule at once. The batch is the production
// provisioning batch, complete with its duties and its name claim, with only the disposal
// added on the way to the store: without the rule it would land.
func TestF105ProvisionCarryingADisposalIsRefused(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-disp-create")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		for i := range tr.Extra {
			if cls, ok := mqttF105ObligationClass(tr.Extra[i].subj); !ok || cls != mqttResourceClassTk {
				continue
			}
			var r mqttOutboundResource
			if err := json.Unmarshal(tr.Extra[i].body, &r); err != nil {
				t.Errorf("decoding the provisioned resource: %v", err)
				return 0, err
			}
			r.Disposal = &mqttResourceDisposal{Txn: "txn-elsewhere", Reason: mqttDisposalUnsubscribed,
				Gen: auth.Gen, Tenure: "tenure-stale"}
			tr.Extra[i].body, _ = json.Marshal(&r)
		}
		return send()
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	_, err := e.asm.jsa.opProvisionResource(e.id, e.epoch, "txn-prov", "op-prov",
		testF105Gen, auth.Tenure, "inc-new", "$MQTT_inc-new", mqttResourceSourceConsumer)
	e.asm.jsa.testTransitionCommit.Store(nil)
	testRequireRefusal(t, err, "already carrying a disposal decision")
	if got, _, _ := e.asm.jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, "inc-new"); got != nil {
		t.Fatalf("a resource created with a disposal decision landed: %+v", got)
	}
}

// A dispose-resource batch overwrites at most ONE resource: a legitimate step on one record may not
// carry an unrelated change to another (the last angle of the H-1..H-3 class).
func TestF105DisposeBatchActsOnOneResource(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-disp-one")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	testF105PlantUnsealedResource(t, e, "inc-other", mqttResourceSourceConsumer)
	r, seq := testF105LoadResource(t, e, "inc-src")
	d := *r
	d.Disposal = &mqttResourceDisposal{Txn: "txn-two", Reason: mqttDisposalUnsubscribed, Gen: auth.Gen, Tenure: auth.Tenure}
	_, err := e.asm.jsa.commitAuthorityTransition(testDisposeBatch(t, e, "txn-two",
		testResourceMember(t, e, &d, seq), testSealMember(t, &e.asm.jsa, e.id, e.epoch, testF105Gen, "inc-other")))
	testRequireRefusal(t, err, "overwrites at most one")
	if got, _ := testF105LoadResource(t, e, "inc-other"); got.Prov == mqttProvisioningSealed {
		t.Fatal("a dispose batch sealed an unrelated resource")
	}
}
