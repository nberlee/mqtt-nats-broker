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

import "testing"

// TestF105TerminalPendingDrainStillHoldsItsCharge pins the phase that exists for this.
func TestF105TerminalPendingDrainStillHoldsItsCharge(t *testing.T) {
	for name, tc := range map[string]struct {
		phase       mqttExchangePhase
		outstanding bool
		want        mqttChargeDisposition
	}{
		"publish outstanding":      {mqttExchangePublish, true, mqttChargeHeld},
		"publish drained":          {mqttExchangePublish, false, mqttChargeHeld},
		"past PUBREC":              {mqttExchangePubRecObserved, false, mqttChargeHeld},
		"terminal pending drain":   {mqttExchangeTerminalPendingDrain, false, mqttChargeHeldPendingDrain},
		"terminal, still emitting": {mqttExchangeTerminal, true, mqttChargeHeldPendingDrain},
		"terminal, drained":        {mqttExchangeTerminal, false, mqttChargeReleasable},
	} {
		x := testF105Exchange(2, tc.phase)
		got, err := mqttExchangeCharge(x, tc.outstanding)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: charge %d, want %d", name, got, tc.want)
		}
	}
}

// TestF105UnreadableObligationsOccupyCapacity pins that an obligation this broker cannot
// interpret counts against Receive Maximum, and that an unreadable ACTION is not billed as a
// further obligation.
//
// A record that cannot be read is not an absent one, and treating it as free capacity is how
// an identifier gets reused under an exchange nobody could read. An exchange's actions are
// not separate obligations, so charging one obligation once per undecodable action would bill
// it several times: the accounting is meant to be conservative, not arbitrary.
//
// Stated against the live gate: the resume's custody reconstruction over real records, read
// back through capacityAvailableLocked with Receive Maximum the binding budget.
func TestF105UnreadableObligationsOccupyCapacity(t *testing.T) {
	t.Parallel()
	readable := testWithdrawAdmitted(t, "custody-unreadable-occupy", 2)
	e := readable.e
	unreadable := testWithdrawAdmitMore(t, e, testRealPubRelSrcSeq+7, 2)
	sess := e.sess
	sess.mu.Lock()
	sess.rmax, sess.maxp = 10, 100
	idHash := sess.idHash
	sess.mu.Unlock()

	// The second obligation's exchange record is undecodable, and the first one's acquired
	// actions are: three action records no reader can interpret.
	garbage := []byte("not a record")
	if _, err := e.asm.jsa.storeMsg(mqttExchangeSubject(idHash, unreadable.gen, unreadable.lin), -1, garbage); err != nil {
		t.Fatalf("overwriting the exchange: %v", err)
	}
	for acq := uint64(1); acq <= 3; acq++ {
		if _, err := e.asm.jsa.storeMsg(mqttActionSubject(idHash, readable.gen, readable.lin, acq), -1, garbage); err != nil {
			t.Fatalf("storing undecodable action %d: %v", acq, err)
		}
	}

	// A fresh ledger for the same generation, rebuilt as a resume would.
	sess.mu.Lock()
	sess.custody = newMQTTCustodyLedger(readable.gen)
	sess.mu.Unlock()
	sess.rebuildCustodyForResume(readable.gen)

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.custody.withholdsNew() {
		t.Fatal("the reconstruction withholds new work, so capacity says nothing about what it charged")
	}
	if !sess.custody.subjectUncertain(unreadable.gen, mqttExchangeSubject(idHash, unreadable.gen, unreadable.lin)) {
		t.Fatal("the undecodable exchange was not recorded as uncertain")
	}
	// One held exchange plus one unreadable one: two slots of ten. The three unreadable
	// actions add nothing, or one obligation is charged several times.
	if occ := sess.receiveMaxOccupancyLocked(); occ != 2 {
		t.Fatalf("Receive Maximum occupancy %d, want 2: the readable obligation plus the unreadable "+
			"one, and no charge for the three unreadable ACTIONS", occ)
	}
	if c := sess.capacityAvailableLocked(); c != 8 {
		t.Fatalf("capacity %d at Receive Maximum 10, want 8: an unreadable obligation is not free "+
			"capacity, and an unreadable action is not a further obligation", c)
	}
}
