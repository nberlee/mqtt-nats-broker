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

// TestF105ForgettingMetadataReleasesNothing is the defect F-106 named, stated as a test.
//
// Capacity derives from the scheduling maps today, so REMOVING A MAP ENTRY RELEASES
// CAPACITY — and two metadata routes do exactly that: forgetCancelledReg drops a parked entry
// because its verdict retires, and releaseSettledVoidReservationsLocked has the same
// metadata-to-capacity release. A peer may already have transmitted that allocation, and a
// tombstone proves neither non-transmission nor completion. At Receive Maximum 1 the result
// is a second delivery admitted while the first is still outstanding.
//
// Here the charge is owned by the exchange, so metadata operations have nothing to release.
// The refusal is EXPLICIT rather than by omission, because before this no code anywhere said
// "that is not a reason" — the release simply happened as a side effect.
func TestF105ForgettingMetadataReleasesNothing(t *testing.T) {
	ok, why := mqttChargeReleaseAllowed(mqttReleaseEvidence{MetadataForgotten: true})
	if ok {
		t.Fatalf("forgetting scheduling metadata released a charge")
	}
	if why == _EMPTY_ {
		t.Fatalf("the refusal carried no reason")
	}

	// Nor does it help alongside a retired verdict's other trappings: only the three
	// positive grounds count.
	if ok, _ := mqttChargeReleaseAllowed(mqttReleaseEvidence{}); ok {
		t.Fatalf("a charge was released with no evidence at all")
	}
}

// TestF105ChargeNeedsTerminalAndDrain pins that terminal evidence alone is not enough.
//
// Releasing on terminal evidence while output may still execute lets a late first
// transmission escape after its own release boundary — the identifier would be reusable while
// bytes for the old exchange are still in flight.
func TestF105ChargeNeedsTerminalAndDrain(t *testing.T) {
	if ok, _ := mqttChargeReleaseAllowed(mqttReleaseEvidence{Terminal: true}); ok {
		t.Fatalf("terminal evidence alone released the charge while output could still execute")
	}
	ok, why := mqttChargeReleaseAllowed(mqttReleaseEvidence{Terminal: true, Drained: true})
	if !ok {
		t.Fatalf("terminal evidence with output drained did not release the charge: %s", why)
	}

	// The two other grounds stand alone, and deliberately: nothing was ever transmitted, or
	// the generation's obligations were authorized to be abandoned.
	for name, e := range map[string]mqttReleaseEvidence{
		"whole-exchange unsent":         {UnsentWhole: true},
		"authorized generation discard": {DiscardedGeneration: true},
	} {
		if ok, why := mqttChargeReleaseAllowed(e); !ok {
			t.Fatalf("%s did not release the charge: %s", name, why)
		}
	}
}

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
		"release owed":             {mqttExchangePubRelOwed, false, mqttChargeHeld},
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
// interpret counts against Receive Maximum.
//
// A record that cannot be read is not an absent one, and treating it as free capacity is how
// an identifier gets reused under an exchange nobody could read. The same rule covers an
// exchange whose charge cannot be decided.
func TestF105UnreadableObligationsOccupyCapacity(t *testing.T) {
	obs := &mqttGenerationObligations{
		exchanges: []*mqttOutboundExchange{
			testF105Exchange(2, mqttExchangePublish),  // held
			testF105Exchange(2, mqttExchangeTerminal), // releasable when drained
			{ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Nonce: "bad", Phase: 99},
		},
		unreadableExchanges: 2,
		// An exchange's undecodable ACTIONS are not separate obligations, so they must not
		// be billed as further charges — one obligation would be counted several times.
		unreadableOther: 3,
	}
	n, err := mqttChargedCount(obs, func(string) bool { return false })
	if err != nil {
		t.Fatalf("counting: %v", err)
	}
	// two unreadable + one held + one undecidable = 4; the drained terminal is free.
	if n != 4 {
		t.Fatalf("charged count %d, want 4: two unreadable exchanges plus one held plus one "+
			"undecidable — and the three unreadable ACTIONS must not be billed, or one "+
			"obligation is charged several times", n)
	}

	// A caller that cannot answer "is anything still emitting" must be treated as though
	// something is, so the drained terminal is charged too.
	n, err = mqttChargedCount(obs, nil)
	if err != nil {
		t.Fatalf("counting with no outstanding oracle: %v", err)
	}
	if n != 5 {
		t.Fatalf("charged count %d with an unknown outstanding state, want 5: an unknown "+
			"action is an outstanding one", n)
	}
}

// TestF105IdentifierWaitsForItsBindings pins the one thing the identifier needs beyond the
// charge: no captured acknowledgement handler on the original connection may still name the
// old exchange, or a client could read a second PUBLISH under that identifier while the
// first is unfinished.
func TestF105IdentifierWaitsForItsBindings(t *testing.T) {
	releasable := mqttReleaseEvidence{Terminal: true, Drained: true}

	if ok, why := mqttIdentifierReleaseAllowed(releasable, false); ok {
		t.Fatalf("an identifier was released while a captured handler could still name its "+
			"exchange (%s)", why)
	}
	if ok, why := mqttIdentifierReleaseAllowed(releasable, true); !ok {
		t.Fatalf("an identifier with a releasable charge and discharged bindings was withheld: %s", why)
	}
	// And it never outruns the charge.
	if ok, _ := mqttIdentifierReleaseAllowed(mqttReleaseEvidence{MetadataForgotten: true}, true); ok {
		t.Fatalf("an identifier was released on forgotten metadata")
	}

	// ACCOUNTING RELEASE IS NOT TRANSPORT PERMISSION. An authorized generation discard says
	// the old generation's obligations do not count against the fresh connection. It says
	// NOTHING about whether an old connection can still emit an acquired action — and
	// reusing the identifier there would put two different messages under one identifier.
	if ok, why := mqttChargeReleaseAllowed(mqttReleaseEvidence{DiscardedGeneration: true}); !ok {
		t.Fatalf("an authorized generation discard did not release the accounting charge: %s", why)
	}
	if ok, why := mqttIdentifierReleaseAllowed(mqttReleaseEvidence{DiscardedGeneration: true}, true); ok {
		t.Fatalf("an identifier was released on an authorized generation discard alone, with "+
			"no output disposition for its original connection (%s)", why)
	}
	if ok, why := mqttIdentifierReleaseAllowed(
		mqttReleaseEvidence{DiscardedGeneration: true, Drained: true}, true); !ok {
		t.Fatalf("a discarded generation with output drained withheld its identifier: %s", why)
	}

	// The unsent-whole ground carries its own output disposition — nothing was transmitted
	// and none can subsequently occur — so it needs no separate drain.
	if ok, why := mqttIdentifierReleaseAllowed(mqttReleaseEvidence{UnsentWhole: true}, true); !ok {
		t.Fatalf("a proven whole-exchange unsent cancellation withheld its identifier: %s", why)
	}
}
