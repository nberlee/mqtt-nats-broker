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

//go:build mqtt_fulltests

package mqtt

import "testing"

// TestF105LogicalUseKindAbandonsWithoutASeal pins the source/PUBREL half of the provisioning
// split (F-105 cutover, consult 4). A source or PUBREL consumer's create is an unfenceable
// queued request, so it never produces a provisioning SEAL; its lifecycle is arbitrated by the
// name-ownership record instead. So its provisioning duty resolves on the claim, its cleanup is
// authorizable with nothing depending on it though unsealed, authorizing deletion retires the
// name, and a retired name may never be reclaimed. The seal keeps its exact meaning for the
// kinds the broker can seal — TestF105DeletionRequiresSealedProvisioning and
// TestF105AResourceDutyOwesTheRightStage now govern that on a retained payload.
func TestF105LogicalUseKindAbandonsWithoutASeal(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-logical-use")
	defer done()
	jsa := &e.asm.jsa
	e.bootstrap(t, "boot")

	const name = "$MQTT_src_logical_use"
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov", "op-prov",
		testF105Gen, "tenure-boot", "inc-lu", name, mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning the source consumer was refused: %v", err)
	}

	// Its PROVISION duty MAY RESOLVE — there is no durable materialization to seal; the claim
	// rode the provisioning batch and readiness for use is a live admission gate.
	prov := &mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
		Kind: mqttDutyProvision, Ref: "inc-lu", State: mqttDutyPending}
	if v := jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, prov); v.Disposition != mqttDutyMayResolve {
		t.Fatalf("a source provision duty was %v, want MayResolve: %s", v.Disposition, v.Why)
	}

	// READINESS FOR USE holds while the resource is LIVE and its name ownership is LIVE and this
	// resource's; the stream check is exercised by TestF105InactivityPrerequisite, so here it may
	// legitimately refuse on an unverifiable stream — assert only that it does NOT refuse for an
	// ownership reason at this point.
	if err := testReadyForUse(jsa, e.id, e.epoch, testF105Gen, "inc-lu"); err != nil {
		if !containsAny(err.Error(), "inactivity", "could not be read", "no message") {
			t.Fatalf("readiness refused for a non-stream reason while LIVE: %v", err)
		}
	}

	// Its CLEANUP with nothing depending on it is WITHHELD while it serves — an idle consumer is
	// not an ended one — and OWES the authorizing CAS once a route disposes of it, though it is
	// unsealed: a delayed create is fenced by name ownership, not by a seal (F-105 7c-lc).
	cleanup := &mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 2,
		Kind: mqttDutyCleanup, Ref: "inc-lu", State: mqttDutyNotYetActionable}
	if v := jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, cleanup); v.Disposition != mqttDutyWithheld {
		t.Fatalf("a serving source's cleanup was %v/%v, want withheld: %s", v.Disposition, v.Work, v.Why)
	}
	if _, err := jsa.opRequestDisposal(&mqttRequestDisposalRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-disp", Gen: testF105Gen, Inc: "inc-lu",
		Reason: mqttDisposalUnsubscribed, Entitlement: mqttOutboundEntitlement{Gen: testF105Gen, Tenure: "tenure-boot"},
	}); err != nil {
		t.Fatalf("disposing of the source was refused: %v", err)
	}
	if v := jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, cleanup); v.Disposition != mqttDutyOwesWork ||
		v.Work != mqttDutyWorkAuthorizeDeletion {
		t.Fatalf("an unsealed source cleanup with no deps was %v/%v, want the authorizing CAS: %s",
			v.Disposition, v.Work, v.Why)
	}

	// AUTHORIZING DELETION succeeds WITHOUT a seal, and it RETIRES the name.
	if _, err := jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-auth", testF105Gen, "inc-lu"); err != nil {
		t.Fatalf("authorizing a source deletion required a seal it can never have: %v", err)
	}
	own, _, oerr := jsa.loadNameOwnership(e.id, e.epoch, mqttOwnedStreamMsgs, name)
	if oerr != nil || own == nil || own.State != mqttNameOwnRetired {
		t.Fatalf("authorizing deletion did not retire the name: %+v (%v)", own, oerr)
	}

	// A RETIRED name may never be reclaimed: a fresh provisioning of the SAME name is refused.
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-reclaim", "op-reclaim",
		testF105Gen, "tenure-boot", "inc-reuse", name, mqttResourceSourceConsumer); err == nil {
		t.Fatal("a retired physical name was reclaimed by a fresh provisioning")
	}

	// And readiness for use now refuses on the retired ownership.
	if err := testReadyForUse(jsa, e.id, e.epoch, testF105Gen, "inc-lu"); err == nil {
		t.Fatal("readiness for use held over a retired name")
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}
