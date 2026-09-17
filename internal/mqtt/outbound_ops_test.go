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
	"testing"
)

// testF105GenInvented is a well-formed generation no authority in these fixtures stands at.
// A LIE the operations must overwrite has to be well-formed, or the intent validator refuses
// it and the derivation is never reached.
const testF105GenInvented = "0123456789abcdefghijkZ"

// The OPERATION SURFACE, against the store.
//
// These tests are the other half of outbound_boundary_test.go. That file drives the write
// boundary with hand-composed transitions, because the boundary is the defence being tested.
// This one drives the operations a converted caller will actually use, and its subject is
// what the caller does NOT get to say: no authority body, no predecessor, no closure, no
// member of its own.

// TestF105OperationsCarryOneObligationEndToEnd walks a whole obligation through the surface
// and nothing else: claim, finalize, admit, advance, release, authorize, complete.
//
// If any step needed a hand-composed transition the surface would be incomplete, and the
// first converted caller would reach for the transition struct — which is the escape hatch
// this shape exists to remove.
func TestF105OperationsCarryOneObligationEndToEnd(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ops-endtoend")
	defer done()

	jsa := &e.asm.jsa

	// (1) CLAIM. A reservation serves nothing yet.
	if _, err := jsa.opClaimClient(e.id, e.epoch, "txn-claim", testF105Gen, "tenure-1", "intent-1"); err != nil {
		t.Fatalf("claiming the client was refused: %v", err)
	}
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec.State != mqttAuthorityReserving {
		t.Fatalf("the claim did not install a reservation (status %v)", al.status)
	}
	// An admission under it is refused: the reservation grants no serving permission, and
	// the operation refuses before composing anything.
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-early", Op: "op-early",
		Gen: testF105Gen, Tenure: "tenure-1",
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 5, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a publish was admitted under a reservation that serves nothing")
	}

	// (2) THE METADATA HALF, then the finalization it licenses.
	if _, err := jsa.opFinalizeClaim(e.id, e.epoch, "txn-fin", "intent-1"); err == nil {
		t.Fatalf("a reservation was finalized with no committed metadata half")
	}
	e.commitCreation(t, al.rec.Creation)
	if _, err := jsa.opFinalizeClaim(e.id, e.epoch, "txn-fin", "intent-other"); err == nil {
		t.Fatalf("a finalization naming another proposal was accepted")
	}
	if _, err := jsa.opFinalizeClaim(e.id, e.epoch, "txn-fin", "intent-1"); err != nil {
		t.Fatalf("finalizing the claim was refused: %v", err)
	}

	// (3) PROVISION the backing resource and ADMIT against it, both through the surface.
	// The fixture plants nothing: a lifecycle test that wrote the resource itself would be
	// demonstrating a surface that cannot actually carry one.
	if _, err := jsa.opProvisionResource(e.id, e.epoch, "txn-prov", "op-prov",
		testF105Gen, "tenure-1", "inc-src", "$MQTT_src", mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning the backing resource was refused: %v", err)
	}
	reg := freshRegisterLineage(&mqttInflightRecord{
		State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
		Nonce: mqttNewInflightNonce(),
	})
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-adm", Op: "op-adm",
		Gen: testF105Gen, Tenure: "tenure-1",
		Register: reg, Deps: []mqttAdmitDependency{
			{Inc: "inc-src", Role: mqttRoleSourceReplay},
		},
	}); err != nil {
		t.Fatalf("admitting the publish was refused: %v", err)
	}
	// THE EXCHANGE WAS DERIVED, not supplied: its identity is the register's lineage, its
	// allocation and QoS the register's, and its tenure the authority's.
	x, _, xerr := jsa.loadOutboundExchange(e.id, e.epoch, testF105Gen, reg.Lin)
	if xerr != nil {
		t.Fatalf("the admitted exchange does not load back: %v", xerr)
	}
	if x.PI != 7 || x.QoS != 2 || x.Phase != mqttExchangePublish || x.AdmitTxn != "txn-adm" {
		t.Fatalf("the derived exchange is %+v", x)
	}
	res, _, rerr := jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, "inc-src")
	if rerr != nil {
		t.Fatalf("the resource does not load back: %v", rerr)
	}
	if len(res.Deps) != 1 || res.Deps[0].Nonce != reg.Lin {
		t.Fatalf("the admission did not take its named edge: %v", res.Deps)
	}

	// (4) ADVANCE, one step at a time, each reading the exchange as it stands.
	for _, to := range []mqttExchangePhase{
		mqttExchangePubRecObserved, mqttExchangePubRelOwed, mqttExchangeTerminal,
	} {
		if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-adv-"+string(rune('a'+to)),
			"op-adv", testF105Gen, reg.Lin, to); err != nil {
			t.Fatalf("advancing to %d was refused: %v", to, err)
		}
	}

	// (5) RELEASE, (6) AUTHORIZE, (7) COMPLETE.
	if _, err := jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-early-auth", "op-auth",
		testF105Gen, "inc-src"); err == nil {
		t.Fatalf("a deletion was authorized while an obligation still named the resource")
	}
	// SEAL BEFORE DISPOSING. The resource was provisioned through the surface, so its record
	// says nothing yet about whether the durable exists; the authorization below requires that
	// settled, and this is the act that settles it.
	if _, err := jsa.opSealProvisioning(e.id, e.epoch, "txn-seal", "op-seal",
		testF105Gen, "inc-src"); err != nil {
		t.Fatalf("sealing the provisioning was refused: %v", err)
	}
	if _, err := jsa.opReleaseDependency(e.id, e.epoch, "txn-rel", "op-rel",
		testF105Gen, "inc-src", mqttResourceEdge{
			Nonce: reg.Lin, Role: mqttRoleSourceReplay,
		}); err != nil {
		t.Fatalf("releasing the edge was refused: %v", err)
	}
	if _, err := jsa.opCompleteCleanup(e.id, e.epoch, "txn-early-done", "op-done",
		testF105Gen, "inc-src", mqttCleanupRemoved); err == nil {
		t.Fatalf("a cleanup was completed that nothing had authorized")
	}
	if _, err := jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-auth",
		testF105Gen, "inc-src"); err != nil {
		t.Fatalf("authorizing the deletion was refused: %v", err)
	}
	if _, err := jsa.opCompleteCleanup(e.id, e.epoch, "txn-done", "op-done",
		testF105Gen, "inc-src", mqttCleanupRemoved); err != nil {
		t.Fatalf("completing the cleanup was refused: %v", err)
	}
	fin, _, ferr := jsa.loadOutboundResource(e.id, e.epoch, testF105Gen, "inc-src")
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

// TestF105OperationsReadTheAuthorityTheyActUnder pins the property the whole surface exists
// for: no operation takes the authority, the predecessor or its sequence from its caller.
//
// Each case moves the standing authority to a state the operation may not act under, WITHOUT
// telling the operation, and the operation must refuse. A surface that took a caller-supplied
// predecessor would act on the stale one it was handed.
func TestF105OperationsReadTheAuthorityTheyActUnder(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ops-reads")
	defer done()

	jsa := &e.asm.jsa

	// With no authority at all, everything but a claim refuses — and the refusal is the
	// ABSENCE of a licence, never a fallback that admits work anyway.
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-none", Op: "op-none",
		Gen: testF105Gen, Tenure: "tenure-txn-1",
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 3, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a publish was admitted for a client with no authority")
	}
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-none-adv", "op-adv",
		testF105Gen, mqttNewInflightNonce(), mqttExchangeTerminal); err == nil {
		t.Fatalf("an exchange advanced under no authority")
	}
	if _, err := jsa.opCloseGeneration(e.id, e.epoch, "txn-none-close", testF105Gen, "tenure-1", "txn-1"); err == nil {
		t.Fatalf("a generation was closed for a client with no authority")
	}

	auth, _ := e.bootstrap(t, "txn-1")

	// A SECOND CLAIM over a SERVING authority is refused — the operation loads it, so a
	// caller cannot claim by describing the client as free.
	if _, err := jsa.opClaimClient(e.id, e.epoch, "txn-second", testF105Gen2,
		"tenure-2", "intent-2"); err == nil {
		t.Fatalf("a second claim displaced a serving authority")
	}

	// PREPARING freezes admission, and the admission operation discovers that by reading.
	// THE INTENT LIES ABOUT ITS PREDECESSOR, deliberately: it names a generation, tenure and
	// ownership that are not the ones standing. The operation must overwrite all six with
	// what it read, because a handoff proposing a predecessor that is not there would
	// finalize over a tenure nobody displaced.
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep", &mqttHandoffIntent{
		ID: "intent-h", Tenure: "tenure-next", Gen: testF105Gen2,
		PrevGen: testF105GenInvented, PrevTenure: "tenure-invented", PrevOwnerTxn: "txn-invented",
		PrevSessWID: "wid-0", PrevSessSeq: 12, CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: testF105GenInvented, Tenure: "tenure-invented", OwnerTxn: "txn-invented",
			SessWID: "wid-0", SessSeq: 12, Reason: mqttRetireCleanStart, Discard: true,
		},
	}); err != nil {
		t.Fatalf("preparing the handoff was refused: %v", err)
	}
	pl := jsa.loadOutboundAuthority(e.id, e.epoch)
	if pl.status != mqttOutboundValid || pl.rec.State != mqttAuthorityPreparing || pl.rec.Intent == nil {
		t.Fatalf("the preparation did not land (status %v)", pl.status)
	}
	// THE INTENT'S PREDECESSOR MEMBERS WERE DERIVED from the standing authority, not taken
	// from the caller, which is what stops a handoff proposing a predecessor that is not
	// there.
	if pl.rec.Intent.PrevGen != auth.Gen || pl.rec.Intent.PrevTenure != auth.Tenure ||
		pl.rec.Intent.PrevOwnerTxn != auth.OwnerTxn {
		t.Fatalf("the preparation carries predecessor %q/%q/%q, want the standing %q/%q/%q",
			pl.rec.Intent.PrevGen, pl.rec.Intent.PrevTenure, pl.rec.Intent.PrevOwnerTxn,
			auth.Gen, auth.Tenure, auth.OwnerTxn)
	}
	if r := pl.rec.Intent.Retire; r == nil || r.Gen != auth.Gen || r.Tenure != auth.Tenure ||
		r.OwnerTxn != auth.OwnerTxn {
		t.Fatalf("the preparation's retirement request names %+v rather than the standing "+
			"%q/%q/%q", r, auth.Gen, auth.Tenure, auth.OwnerTxn)
	}
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-frozen", Op: "op-frozen",
		Gen: auth.Gen, Tenure: auth.Tenure,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 4, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a publish was admitted while the authority was preparing")
	}

	// A cancellation restores the predecessor's ownership, from the intent's own record of
	// what it displaced, and it needs the aborted verdict the boundary checks.
	if _, err := jsa.opCancelHandoff(e.id, e.epoch, "txn-cancel", "intent-h"); err == nil {
		t.Fatalf("a preparation was cancelled with no aborted metadata half")
	}
	p, perr := mqttProposalFromHandoff(pl.rec.Intent)
	if perr != nil {
		t.Fatalf("viewing the intent as a proposal failed: %v", perr)
	}
	if v := jsa.settleProposal(_EMPTY_, e.id, e.epoch, p); v != mqttMetadataAborted {
		t.Fatalf("settling the intent returned %v, want ABORTED", v)
	}
	if _, err := jsa.opCancelHandoff(e.id, e.epoch, "txn-cancel", "intent-h"); err != nil {
		t.Fatalf("cancelling the preparation was refused: %v", err)
	}
	cl := jsa.loadOutboundAuthority(e.id, e.epoch)
	if cl.rec.State != mqttAuthorityServing || cl.rec.OwnerTxn != auth.OwnerTxn ||
		cl.rec.Gen != auth.Gen || cl.rec.Tenure != auth.Tenure {
		t.Fatalf("the cancellation installed %+v rather than restoring the predecessor", cl.rec)
	}

	// CLOSED admits nothing and is claimable again, by the same three steps as the first
	// claim.
	if _, err := jsa.opCloseGeneration(e.id, e.epoch, "txn-close", cl.rec.Gen, cl.rec.Tenure, cl.rec.OwnerTxn); err != nil {
		t.Fatalf("closing the generation was refused: %v", err)
	}
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-late", Op: "op-late",
		Gen: auth.Gen, Tenure: auth.Tenure,
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 6, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a publish was admitted over a closed authority")
	}
	if _, err := jsa.opClaimClient(e.id, e.epoch, "txn-again", testF105Gen,
		"tenure-again", "intent-again"); err == nil {
		t.Fatalf("a post-closure claim reused the closed generation")
	}
	if _, err := jsa.opClaimClient(e.id, e.epoch, "txn-again", testF105Gen2,
		"tenure-again", "intent-again"); err != nil {
		t.Fatalf("a post-closure claim of a fresh generation was refused: %v", err)
	}
}

// TestF105OperationsRefuseAnUnreadableAuthority pins the reading that must never be treated
// as absence.
//
// A record this broker cannot decode is not a record that is not there. If an operation read
// CORRUPT as ABSENT, the first claim would succeed against a live owner's unreadable
// authority — which is release-on-absence, reached through the loader instead of through a
// caller.
func TestF105OperationsRefuseAnUnreadableAuthority(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-ops-corrupt")
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

	if _, err := jsa.opClaimClient(e.id, e.epoch, "txn-claim", testF105Gen,
		"tenure-1", "intent-1"); err == nil {
		t.Fatalf("a client whose authority could not be read was claimed as if it were free")
	}
	if _, err := jsa.opCloseGeneration(e.id, e.epoch, "txn-close", testF105Gen, "tenure-1", "txn-1"); err == nil {
		t.Fatalf("a generation was closed over an authority nothing could read")
	}
	if _, err := jsa.opAdvanceExchange(e.id, e.epoch, "txn-adv", "op-adv",
		testF105Gen, mqttNewInflightNonce(), mqttExchangeTerminal); err == nil {
		t.Fatalf("an exchange advanced under an authority nothing could read")
	}
	if _, err := jsa.opAdmitPublish(&mqttAdmitPublishRequest{
		ID: e.id, Epoch: e.epoch, Txn: "txn-adm", Op: "op-adm",
		Gen: testF105Gen, Tenure: "tenure-1",
		Register: freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: 7, Dur: "$MQTT_src", Seq: 1, QoS: 2,
			Nonce: mqttNewInflightNonce(),
		}),
	}); err == nil {
		t.Fatalf("a publish was admitted under an authority nothing could read")
	}
}
