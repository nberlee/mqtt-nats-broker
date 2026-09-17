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

// The two capacity projections (F-105 row 5, absorbing F-106).
//
// The defect these replace is that a verdict RETIRING frees a Receive Maximum slot. So the
// cases here are about what does and does not release occupancy, and the two limits are kept
// apart because an earlier revision of the plan collapsed them into one.

func testCapExchange(nonce string, phase mqttExchangePhase) *mqttOutboundExchange {
	return &mqttOutboundExchange{
		ID: "c", Epoch: "e", Gen: testF105Gen, Nonce: nonce, PI: 7, QoS: 2, Phase: phase,
		AdmitTxn: "txn", Tenure: "t",
	}
}

func TestF105CapacityCountsObligationsNotMetadata(t *testing.T) {
	// An obligation past PUBREC is STILL HELD BY THE CLIENT: Spec5 [4.9] frees the quota only
	// on PUBACK/PUBCOMP or a failed PUBREC, so awaiting PUBCOMP is occupancy.
	obs := &mqttGenerationObligations{exchanges: []*mqttOutboundExchange{
		testCapExchange("E1", mqttExchangePublish),
		testCapExchange("E2", mqttExchangePubRelOwed),
	}}
	p, err := mqttProjectCapacity(obs, 0, func(string) bool { return false })
	if err != nil {
		t.Fatalf("projecting capacity failed: %v", err)
	}
	if p.ReceiveMaximum != 2 {
		t.Fatalf("two live obligations charged %d", p.ReceiveMaximum)
	}

	// AN UNREADABLE OBLIGATION COUNTS. A record this broker cannot interpret is not free
	// capacity, and treating it as such is how an identifier gets reused under an exchange
	// nobody could read.
	obs.unreadableExchanges = 1
	p, _ = mqttProjectCapacity(obs, 0, func(string) bool { return false })
	if p.ReceiveMaximum != 3 {
		t.Fatalf("an unreadable obligation was treated as free capacity: %d", p.ReceiveMaximum)
	}
}

func TestF105CapacityKeepsItsTwoLimitsApart(t *testing.T) {
	obs := &mqttGenerationObligations{exchanges: []*mqttOutboundExchange{
		testCapExchange("E1", mqttExchangePublish),
	}}

	// The SOURCE cap counts deliveries taken and not yet acked, which is a different
	// population from obligations: one obligation, three unacked source deliveries.
	p, err := mqttProjectCapacity(obs, 3, func(string) bool { return false })
	if err != nil {
		t.Fatalf("projecting capacity failed: %v", err)
	}
	if p.ReceiveMaximum != 1 || p.Source != 3 {
		t.Fatalf("the projections were conflated: %+v", p)
	}

	// At Receive Maximum 1 the protocol limit binds even though the consumer has room.
	if p.admitsAnother(1, 100) {
		t.Fatalf("a second delivery was admitted at Receive Maximum 1 while the first is held")
	}
	// With no Receive Maximum negotiated the consumer's cap is the only limit, and it binds.
	if p.admitsAnother(0, 3) {
		t.Fatalf("a delivery was admitted past MaxAckPending")
	}
	if !p.admitsAnother(0, 100) {
		t.Fatalf("a delivery was refused although neither limit binds")
	}
	// BOTH apply when both exist: the protocol limit stands beside the consumer's rather
	// than replacing it.
	if p.admitsAnother(5, 3) {
		t.Fatalf("a Receive Maximum with room let a delivery past the consumer's cap")
	}
}

func TestF105CapacityTreatsUnknownOutputAsOutstanding(t *testing.T) {
	obs := &mqttGenerationObligations{exchanges: []*mqttOutboundExchange{
		testCapExchange("E1", mqttExchangeTerminalPendingDrain),
	}}
	// A caller that CANNOT answer Outstanding must say TRUE, because an unknown action is an
	// outstanding one — and an obligation whose output may still execute is still charged.
	p, _ := mqttProjectCapacity(obs, 0, func(string) bool { return true })
	if p.ReceiveMaximum != 1 {
		t.Fatalf("a pending-drain obligation with unknown output was released: %d", p.ReceiveMaximum)
	}
	// Positively drained AND terminal is the release, and it is the only one.
	obs.exchanges[0] = testCapExchange("E1", mqttExchangeTerminal)
	p, _ = mqttProjectCapacity(obs, 0, func(string) bool { return false })
	if p.ReceiveMaximum != 0 {
		t.Fatalf("a terminal, drained obligation stayed charged: %d", p.ReceiveMaximum)
	}
}
