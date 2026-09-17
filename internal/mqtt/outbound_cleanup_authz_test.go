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

import "testing"

// Destruction is authorized by the resource (F-105, row 8).
//
// The defect being closed is concrete: deleting the PUBREL consumer drops its interest-retained
// records, which are the only evidence the client answered PUBREC for the obligations that
// depend on it. So every case here is about whether the dependency is what decides.

func testAuthzResource(inc, name string, nonces []string) *mqttOutboundResource {
	// WRITTEN AS [] AND NEVER nil: a null decodes to a nil slice that reads identically to an
	// empty one, and "this record says nothing about its dependents" would become
	// indistinguishable from "nothing depends on it".
	deps := make([]mqttResourceEdge, 0, len(nonces))
	for _, n := range nonces {
		deps = append(deps, mqttResourceEdge{Nonce: n, Role: mqttRoleCanonicalPubRel})
	}
	return &mqttOutboundResource{
		ID: "c", Epoch: "e", Gen: testF105Gen, Inc: inc, Kind: mqttResourcePubRelConsumer,
		Name: name, State: mqttResourceLive, Prov: mqttProvisioningPending, Deps: deps,
	}
}

func TestF105DestructionIsBlockedByANamedObligation(t *testing.T) {
	pubrel := testAuthzResource("inc-rel", "$MQTT_pubrel_dur", []string{"E1"})
	source := testAuthzResource("inc-src", "$MQTT_src_dur", nil)

	// THE POSITIVE CONTROL: a durable no resource protects is not blocked, which is what
	// makes this gate a no-op for every path the conversion has not reached yet.
	if b := mqttDestructionBlockedBy([]*mqttOutboundResource{pubrel, source}, 0,
		[]string{"$MQTT_unrelated"}); len(b) != 0 {
		t.Fatalf("an unprotected durable was blocked: %v", b)
	}
	// A resource with NO dependents does not block here either. The authorizing CAS is what
	// excludes a dependency arriving next; this gate refuses only what the records forbid.
	if b := mqttDestructionBlockedBy([]*mqttOutboundResource{pubrel, source}, 0,
		[]string{"$MQTT_src_dur"}); len(b) != 0 {
		t.Fatalf("a resource with no dependents blocked its own destruction: %v", b)
	}

	// AND THE DEFECT ITSELF: the PUBREL consumer an obligation still needs.
	b := mqttDestructionBlockedBy([]*mqttOutboundResource{pubrel, source}, 0,
		[]string{"$MQTT_src_dur", "$MQTT_pubrel_dur"})
	if len(b) != 1 || b[0].Inc != "inc-rel" {
		t.Fatalf("the PUBREL consumer an obligation depends on was not protected: %v", b)
	}
	// NAMED, not counted. A count can be decremented by a transition that ended nothing; the
	// refusal has to say WHICH obligation forbids it — and now also in WHICH ROLE, because
	// "still needed" is not actionable and "still needed to carry its release" is.
	if len(b[0].Deps) != 1 || b[0].Deps[0] != "canonical-pubrel:E1" {
		t.Fatalf("the refusal did not name the obligation and its role: %v", b[0].Deps)
	}
}

func TestF105AnUnreadableResourceBlocksDestruction(t *testing.T) {
	// A record this broker cannot interpret may be the one naming an obligation that depends
	// on the consumer about to be destroyed. Treating it as absent is the inference the whole
	// mechanism exists to remove.
	b := mqttDestructionBlockedBy(nil, 1, []string{"$MQTT_pubrel_dur"})
	if len(b) != 1 {
		t.Fatalf("an unreadable resource record did not block destruction: %v", b)
	}
	// With nothing to destroy there is nothing to authorize, unreadable records or not.
	if b := mqttDestructionBlockedBy(nil, 1, nil); len(b) != 0 {
		t.Fatalf("a destruction of nothing was blocked: %v", b)
	}
}

// TestF105ATruncatedResourcePageIsNotAnAnswer is round I-6's third finding.
//
// The destruction check asks which resources still name a dependent. A first page read as the
// whole answer omits whichever resource sits past the cut — and the check then returns NO BLOCK
// for a durable something still depends on, which is an absence read as a fact in the one place
// this mechanism exists to stop it.
func TestF105ATruncatedResourcePageIsNotAnAnswer(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-resource-truncated")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)

	truncated := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 100_001, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&truncated)
	if _, _, err := e.asm.enumerateGenerationResources(e.id, e.epoch, testF105Gen); err == nil {
		t.Fatalf("a first page was read as this generation's whole resource inventory")
	}

	// THE SAME EMPTY PAGE, REPORTED COMPLETE, is an answer — so what refused above is the
	// truncation and not the emptiness.
	complete := func(string) (map[string]uint64, int, error) {
		return map[string]uint64{}, 0, nil
	}
	e.asm.jsa.exchangeSubjectsObs.Store(&complete)
	if _, _, err := e.asm.enumerateGenerationResources(e.id, e.epoch, testF105Gen); err != nil {
		t.Fatalf("a complete answer holding nothing was refused: %v", err)
	}
	e.asm.jsa.exchangeSubjectsObs.Store(nil)
}
