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

import (
	"encoding/json"
	"strings"
	"testing"
)

// The tests that used to live here exercised the deferred shared-holder scan and the enrolment
// of a retiring generation's shared group durables as per-generation cleanup resources. Both
// mechanisms are deleted with the shared-group lifecycle transfer (shared_lifecycle.go): a group
// consumer is held by arbitration on its own group record, and a generation's acquisitions of
// it are released by the closing from the generation's acquisition ledger. What survives here is
// the one property the closing composer still owes and that has nothing to do with shared
// durables: the closing batch SEALS its generation's inventory.

// TestF105AClosureCarriesTheSealedLedger is holistic round 8's finding, pinned at the boundary.
//
// The seal is the closing act. A batch carrying an otherwise valid enrolment beside a ledger body
// still OPEN closes the generation with its inventory unsealed, and then a later disposal —
// deliberately permitted to name an earlier generation — allocates into an inventory this act was
// supposed to have shut. What the release gate reads off that generation would be a snapshot
// again, which is the thing the enrolment exists to stop being.
func TestF105AClosureCarriesTheSealedLedger(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-closure-seal")
	defer done()
	e.bootstrap(t, "txn-1")
	al := e.asm.jsa.loadOutboundAuthority(e.id, e.epoch)

	seal, err := mqttSealGenLedgerMember(e.id, e.epoch, al.rec.Gen, nil, 0)
	if err != nil {
		t.Fatalf("composing the seal: %v", err)
	}

	// UNSEAL THE LEDGER MEMBER and leave everything else exactly as the composer produced it.
	led, derr := mqttDecodeGenLedger(seal.body, e.id, e.epoch, al.rec.Gen)
	if derr != nil {
		t.Fatalf("decoding the composed ledger: %v", derr)
	}
	led.State, led.SealedHi = mqttGenLedgerOpen, 0
	body, merr := json.Marshal(led)
	if merr != nil {
		t.Fatalf("marshalling the unsealed ledger: %v", merr)
	}
	unsealed := []mqttTransitionMember{{subj: seal.subj, expect: seal.expect, body: body}}

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
		Extra: unsealed,
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

	// AND THE COMPOSER'S OWN MEMBER IS ADMITTED, so the arm above is about the SEAL and not
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
			Extra: []mqttTransitionMember{seal},
		}))
		return err
	}(); oerr != nil {
		t.Fatalf("the composer's own sealed member was refused: %v", oerr)
	}
}
