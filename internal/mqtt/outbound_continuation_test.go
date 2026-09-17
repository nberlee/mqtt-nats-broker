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
	"fmt"
	"testing"
)

// TestF105AnOutstandingObligationOwesTheRightThing walks one obligation through every phase and
// pins what a recovery pass concludes at each.
//
// The four answers are NOT interchangeable, and each pair that looks alike is a mistake this
// project has already made once:
//
//   - PUBLISH owes DELIVERY, and past PUBREC owes the RELEASE and not the payload. Re-delivering
//     after the client answered is the retransmission [MQTT-3.3.1-1] forbids.
//   - A PENDING-DRAIN phase owes a DRAIN and is NOT an ending. It still holds its identifier, so
//     treating it as finished is how an identifier is reused while output can still execute.
//   - Only Terminal and Rejected may resolve.
func TestF105AnOutstandingObligationOwesTheRightThing(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-continuation-xch")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-src")
	d, _ := testF105LoadDuty(t, e, 1)

	// At its publish, before anything is advanced.
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, d); v.Disposition !=
		mqttDutyOwesWork || v.Work != mqttDutyWorkDeliver {
		t.Fatalf("an obligation at its publish owes %v/%v, want owes-work/deliver: %s",
			v.Disposition, v.Work, v.Why)
	}

	for i, step := range []struct {
		phase mqttExchangePhase
		disp  mqttDutyDisposition
		work  mqttDutyWork
	}{
		{mqttExchangePubRecObserved, mqttDutyOwesWork, mqttDutyWorkRelease},
		{mqttExchangePubRelOwed, mqttDutyOwesWork, mqttDutyWorkRelease},
		{mqttExchangeTerminalPendingDrain, mqttDutyOwesWork, mqttDutyWorkDrain},
		{mqttExchangeTerminal, mqttDutyMayResolve, mqttDutyWorkNone},
	} {
		if _, err := e.asm.jsa.opAdvanceExchange(e.id, e.epoch,
			fmt.Sprintf("txn-step-%d", i), "op-step", testF105Gen, nonce, step.phase); err != nil {
			t.Fatalf("advancing to phase %d: %v", int(step.phase), err)
		}
		v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, d)
		if v.Disposition != step.disp || v.Work != step.work {
			t.Fatalf("phase %d classified %v/%v, want %v/%v: %s",
				int(step.phase), v.Disposition, v.Work, step.disp, step.work, v.Why)
		}
	}
}

// TestF105AnUnreadableSubjectIsWithheldNeverResolved is the rule the whole classifier rests on.
//
// A duty this broker cannot read is one it can conclude NOTHING about. Resolving it would retire
// work nobody established anything about; calling it actionable would act on a record nobody
// read. Withheld is the only honest answer, and the sweep comes back.
func TestF105AnUnreadableSubjectIsWithheldNeverResolved(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-continuation-blind")
	defer done()
	e.bootstrap(t, "txn-1")

	for name, d := range map[string]*mqttDutyEntry{
		"a duty entry that could not be read": nil,
		"an obligation that does not exist": {
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
			Kind: mqttDutyExchange, Ref: "xch-nowhere", State: mqttDutyPending,
		},
		"a resource that does not exist": {
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
			Kind: mqttDutyCleanup, Ref: "inc-nowhere", State: mqttDutyNotYetActionable,
		},
		"a kind this broker does not interpret": {
			ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
			Kind: "something-else", Ref: "x", State: mqttDutyPending,
		},
	} {
		v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, d)
		if v.Disposition != mqttDutyWithheld {
			t.Fatalf("%s was classified %v rather than withheld: %s", name, v.Disposition, v.Why)
		}
		if v.Why == "" {
			t.Fatalf("%s was withheld with no reason", name)
		}
	}
}

// TestF105AResourceDutyOwesTheRightStage pins the cleanup continuation, whose states are the
// easiest to collapse.
//
// "Not yet actionable" and "authorizable" look alike from a distance and are opposite answers.
// And the third case is the one that makes provisioning a separate axis at all: nothing depends
// on the resource, but its name is still not free because a delayed create may land under it.
func TestF105AResourceDutyOwesTheRightStage(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-continuation-res")
	defer done()
	e.bootstrap(t, "txn-1")

	// UNSEALED, with nothing depending on it: NOT authorizable. Authorizing here is how the
	// orphan is made — the delete finds nothing and the delayed create lands afterwards.
	testF105PlantUnsealedResource(t, e, "inc-a", mqttResourceSourceConsumer)
	cleanup := &mqttDutyEntry{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 1,
		Kind: mqttDutyCleanup, Ref: "inc-a", State: mqttDutyNotYetActionable,
	}
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, cleanup); v.Disposition !=
		mqttDutyWithheld {
		t.Fatalf("an unsealed resource with no dependents was %v, want withheld: %s",
			v.Disposition, v.Why)
	}
	// Its PROVISIONING duty, by contrast, is exactly what is owed.
	provision := &mqttDutyEntry{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 2,
		Kind: mqttDutyProvision, Ref: "inc-a", State: mqttDutyPending,
	}
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, provision); v.Disposition !=
		mqttDutyOwesWork || v.Work != mqttDutyWorkProvision {
		t.Fatalf("an unsealed resource's provisioning duty was %v/%v: %s",
			v.Disposition, v.Work, v.Why)
	}

	// SEALED with a dependent: not yet actionable, which is a withholding and not work.
	testF105PlantResource(t, e, "inc-b", mqttResourcePubRelConsumer)
	nonce := testF105AdmitOn(t, e, "txn-adm", 7, "inc-b")
	bCleanup := &mqttDutyEntry{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 3,
		Kind: mqttDutyCleanup, Ref: "inc-b", State: mqttDutyNotYetActionable,
	}
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, bCleanup); v.Disposition !=
		mqttDutyWithheld {
		t.Fatalf("a resource with a dependent was %v, want withheld: %s", v.Disposition, v.Why)
	}
	// Its provisioning duty may resolve, because sealing is the fact that duty is about.
	bProvision := &mqttDutyEntry{
		ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: 4,
		Kind: mqttDutyProvision, Ref: "inc-b", State: mqttDutyPending,
	}
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, bProvision); v.Disposition !=
		mqttDutyMayResolve {
		t.Fatalf("a sealed resource's provisioning duty was %v: %s", v.Disposition, v.Why)
	}

	// SEALED with its dependent released: NOW the authorizing CAS is owed.
	testF105FinishExchange(t, e, nonce)
	if err := testF105ReleaseEdge(t, e, "inc-b", "txn-rel", mqttResourceEdge{
		Nonce: nonce, Role: mqttRoleCanonicalPubRel,
	}); err != nil {
		t.Fatalf("releasing the edge: %v", err)
	}
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, bCleanup); v.Disposition !=
		mqttDutyOwesWork || v.Work != mqttDutyWorkAuthorizeDeletion {
		t.Fatalf("a sealed resource with no dependents was %v/%v, want the authorizing CAS: %s",
			v.Disposition, v.Work, v.Why)
	}

	// AUTHORIZED: the cleanup itself is owed, and a delete returning success would not end it.
	if _, err := e.asm.jsa.opAuthorizeDeletion(e.id, e.epoch, "txn-auth", "op-auth",
		testF105Gen, "inc-b"); err != nil {
		t.Fatalf("authorizing the deletion: %v", err)
	}
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, bCleanup); v.Disposition !=
		mqttDutyOwesWork || v.Work != mqttDutyWorkCleanup {
		t.Fatalf("an authorized deletion was %v/%v, want the cleanup: %s",
			v.Disposition, v.Work, v.Why)
	}

	// CLASSIFIED: only now may the duty resolve.
	if _, err := e.asm.jsa.opCompleteCleanup(e.id, e.epoch, "txn-done", "op-done",
		testF105Gen, "inc-b", mqttCleanupRemoved); err != nil {
		t.Fatalf("completing the cleanup: %v", err)
	}
	if v := e.asm.jsa.classifyOutstandingDuty(e.id, e.epoch, testF105Gen, bCleanup); v.Disposition !=
		mqttDutyMayResolve {
		t.Fatalf("a classified cleanup was %v, want may-resolve: %s", v.Disposition, v.Why)
	}
}
