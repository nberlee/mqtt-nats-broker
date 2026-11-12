//go:build mqtt_fulltests

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
	"testing"
)

// The OPERATION SURFACE, against the store.
//
// These tests are the other half of outbound_boundary_test.go. That file drives the write
// boundary with hand-composed transitions, because the boundary is the defence being tested.
// This one drives the operations a converted caller will actually use, and its subject is
// what the caller does NOT get to say: no authority body, no predecessor, no closure, no
// member of its own.

// TestOperationsCarryOneObligationEndToEnd walks a whole obligation through the surface
// and nothing else: claim, finalize, admit, advance, release, authorize, complete.
//
// If any step needed a hand-composed transition the surface would be incomplete, and the
// first converted caller would reach for the transition struct — which is the escape hatch
// this shape exists to remove.
func TestOperationsCarryOneObligationEndToEnd(t *testing.T) {
	e, done := testNewEnv(t, "f105-ops-endtoend")
	defer done()

	jsa := &e.asm.jsa

	// (1)-(2) ACCEPT AND BIND, the one route to a serving authority (issue #133).
	e.claim(t, nil, 0, "txn-claim", testGen, "tenure-1")

	// (3) PROVISION the backing resource and ADMIT against it, both through the surface.
	// The fixture plants nothing: a lifecycle test that wrote the resource itself would be
	// demonstrating a surface that cannot actually carry one.
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov", "op-prov",
		testGen, "tenure-1", "inc-src", "$MQTT_src", mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning the backing resource was refused: %v", err)
	}
	reg := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-adm", Op: "op-adm",
		Gen: testGen, Tenure: "tenure-1",
		Register: reg, Deps: []mqttAdmitDependency{
			{Inc: "inc-src", Role: mqttRoleSourceReplay},
		},
	}); err != nil {
		t.Fatalf("admitting the publish was refused: %v", err)
	}
	// THE EXCHANGE WAS DERIVED, not supplied: its identity is the register's lineage, its
	// allocation and QoS the register's, and its tenure the authority's.
	x, _, xerr := jsa.loadOutboundExchange(e.id, e.epoch, testGen, reg.Lin)
	if xerr != nil {
		t.Fatalf("the admitted exchange does not load back: %v", xerr)
	}
	if x.PI != 7 || x.QoS != 2 || x.Phase != mqttExchangePublish || x.AdmitTxn != "txn-adm" {
		t.Fatalf("the derived exchange is %+v", x)
	}
	res, _, rerr := jsa.loadOutboundResource(e.id, e.epoch, testGen, "inc-src")
	if rerr != nil {
		t.Fatalf("the resource does not load back: %v", rerr)
	}
	if len(res.Deps) != 1 || res.Deps[0].Nonce != reg.Lin {
		t.Fatalf("the admission did not take its named edge: %v", res.Deps)
	}

	// While the obligation stands, its hold refuses the authorization.
	if _, err := jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-early-auth", "op-auth",
		testGen, "inc-src"); err == nil {
		t.Fatalf("a deletion was authorized while an obligation still named the resource")
	}

	// (4) ADVANCE, one step at a time, each reading the exchange as it stands. The terminal
	// advance is the ending, and it leaves the hold release-owed (issue #107 7c-lc).
	for _, to := range []mqttExchangePhase{
		mqttExchangePubRecObserved, mqttExchangeTerminal,
	} {
		if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, reg.Lin, to); err != nil {
			t.Fatalf("advancing to %d was refused: %v", to, err)
		}
	}

	// (5) THE RELEASE-HOLDS BATCH RELEASES THE OWED HOLD, so nothing names the resource; and no
	// other release door exists for it. Then (6) AUTHORIZE, (7) COMPLETE.
	if res, _, _ := jsa.loadOutboundResource(e.id, e.epoch, testGen, "inc-src"); res == nil || len(res.Deps) != 1 {
		t.Fatalf("the terminal advance did not leave its hold owed: %+v", res)
	}
	testReleaseOwed(t, e, reg.Lin)
	if res, _, _ := jsa.loadOutboundResource(e.id, e.epoch, testGen, "inc-src"); res == nil || len(res.Deps) != 0 {
		t.Fatalf("the release did not release the owed hold: %+v", res)
	}
	if _, err := jsa.opReleaseDependency(e.id, e.epoch, "txn-rel", "op-rel",
		testGen, "inc-src", mqttResourceEdge{
			Nonce: reg.Lin, Role: mqttRoleSourceReplay,
		}); err == nil {
		t.Fatalf("a hold was released again after the ending had released it")
	}
	// SEAL BEFORE DISPOSING. The resource was provisioned through the surface, so its record
	// says nothing yet about whether the durable exists; the authorization below requires that
	// settled, and this is the act that settles it.
	if _, err := jsa.opSealProvisioning(e.id, e.epoch, "txn-seal", "op-seal",
		testGen, "inc-src"); err != nil {
		t.Fatalf("sealing the provisioning was refused: %v", err)
	}
	if err := testCompleteCleanup(t, e, testGen, "inc-src", "txn-early-done",
		mqttCleanupRemoved); err == nil {
		t.Fatalf("a cleanup was completed that nothing had authorized")
	}
	// A served consumer is not authorizable until a route disposes of it (issue #107 7c-lc).
	if _, err := jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth-serving", "op-auth",
		testGen, "inc-src"); err == nil {
		t.Fatalf("a consumer no route disposed of was authorized")
	}
	if _, err := jsa.opRequestDisposal(&mqttRequestDisposalRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-disp", Gen: testGen, Inc: "inc-src",
		Reason: mqttDisposalUnsubscribed, Entitlement: mqttOutboundEntitlement{Gen: testGen, Tenure: "tenure-1"},
	}); err != nil {
		t.Fatalf("disposing of the consumer was refused: %v", err)
	}
	if _, err := jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-auth",
		testGen, "inc-src"); err != nil {
		t.Fatalf("authorizing the deletion was refused: %v", err)
	}
	if err := testCompleteCleanup(t, e, testGen, "inc-src", "txn-done",
		mqttCleanupRemoved); err != nil {
		t.Fatalf("completing the cleanup was refused: %v", err)
	}
	fin, _, ferr := jsa.loadOutboundResource(e.id, e.epoch, testGen, "inc-src")
	if ferr != nil {
		t.Fatalf("the cleaned-up resource does not load back: %v", ferr)
	}
	if fin.State != mqttResourceCleanupComplete || fin.Outcome != mqttCleanupRemoved {
		t.Fatalf("the completed resource is %+v", fin)
	}
	// The authorizing transition is preserved, never restated by the completion.
	if fin.AuthTxn != "txn-auth" {
		t.Fatalf("the completion re-attributed the authorization to %q", fin.AuthTxn)
	}
}

// TestOperationsRefuseAnUnreadableAuthority pins the reading that must never be treated
// as absence.
//
// A record this broker cannot decode is not a record that is not there. If an operation read
// CORRUPT as ABSENT, the first claim would succeed against a live owner's unreadable
// authority — which is release-on-absence, reached through the loader instead of through a
// caller.
func TestOperationsRefuseAnUnreadableAuthority(t *testing.T) {
	e, done := testNewEnv(t, "f105-ops-corrupt")
	defer done()

	jsa := &e.asm.jsa
	subj := mqttOutboundAuthoritySubject(getHash(e.id))
	msg, hdr := mqttFrameBatchWrite(0, []byte(`{"id":"nonsense"`), _EMPTY_, 0, false)
	if _, err := jsa.storeMsg(subj, hdr, msg); err != nil {
		t.Fatalf("planting the unreadable authority failed: %v", err)
	}
	if load := jsa.loadOutboundAuthority(e.id, e.epoch); load.status != mqttOutboundCorrupt {
		t.Fatalf("the planted authority reads %v, want CORRUPT", load.status)
	}

	if _, err := jsa.composeAcceptance(mqttAcceptRequest{ID: e.id, Epoch: e.epoch, Txn: "txn-claim",
		Tenure: "tenure-1", Nonce: "intent-1", At: 1}, jsa.loadOutboundAuthority(e.id, e.epoch),
		jsa.loadLifecycleRegistryShard(e.epoch, mqttLifecycleRegistryShardFor(getHash(e.id)))); err == nil {
		t.Fatalf("a client whose authority could not be read was accepted as if it were free")
	}
	if _, err := testCloseGenerationOp(e.asm, e.id, e.epoch, testGen, "tenure-1", "txn-1"); err == nil {
		t.Fatalf("a generation was closed over an authority nothing could read")
	}
	if _, err := testAdvanceLive(t, jsa, e.id, e.epoch, testGen, mqttNewInflightNonce(), mqttExchangeTerminal); err == nil {
		t.Fatalf("an exchange advanced under an authority nothing could read")
	}
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-adm", Op: "op-adm",
		Gen: testGen, Tenure: "tenure-1",
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a publish was admitted under an authority nothing could read")
	}
}
