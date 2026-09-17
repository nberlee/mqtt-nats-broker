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
	"strings"
	"testing"
)

// TestF105TheAckPhaseIsDerivedFromThePacket is the contract-3 correction.
//
// The boundary downstream checks that a proposed move is LEGAL from where the exchange stands.
// That check is real and it is not this property: a fabricated phase can be perfectly legal and
// still answer the wrong protocol event — a caller that read a PUBACK and supplied a
// PUBREC-shaped phase would record a transfer of ownership the client never made, whenever that
// move happened to be legal.
func TestF105TheAckPhaseIsDerivedFromThePacket(t *testing.T) {
	for name, tc := range map[string]struct {
		in   mqttAckArrival
		qos  byte
		want mqttExchangePhase
	}{
		"a QoS 1 PUBACK ends the delivery": {
			mqttAckArrival{Packet: mqttPacketPubAck}, 1, mqttExchangeTerminal},
		// A REFUSED PUBACK IS STILL AN ENDING, and it ends at TERMINAL rather than at a
		// rejection: the rejection phases record a refused PUBREC, and the record's own
		// validator refuses one on a QoS 1 exchange because there is no PUBREC to refuse.
		"a refused QoS 1 PUBACK still ends it": {
			mqttAckArrival{Packet: mqttPacketPubAck, Reason: mqttReasonUnspecifiedError}, 1,
			mqttExchangeTerminal},
		// THE ONLY PACKET THAT TRANSFERS OWNERSHIP.
		"a QoS 2 PUBREC transfers ownership": {
			mqttAckArrival{Packet: mqttPacketPubRec}, 2, mqttExchangePubRecObserved},
		// A REFUSED ONE TRANSFERS NOTHING, so no PUBREL is owed for it.
		"a refused QoS 2 PUBREC rejects it": {
			mqttAckArrival{Packet: mqttPacketPubRec, Reason: mqttReasonPacketIDInUse}, 2,
			mqttExchangeRejected},
		"a QoS 2 PUBCOMP ends the exchange": {
			mqttAckArrival{Packet: mqttPacketPubComp}, 2, mqttExchangeTerminal},
		// THE ONLY FAILURE A PUBCOMP MAY CARRY says the client no longer holds the exchange,
		// so it is over either way — and calling it a REJECTION would say no ownership was
		// transferred when a PUBREC already did.
		"a PUBCOMP that cannot find the identifier still ends it": {
			mqttAckArrival{Packet: mqttPacketPubComp, Reason: mqttReasonPacketIDNotFound}, 2,
			mqttExchangeTerminal},

		// OUTSTANDING OUTPUT SPLITS EVERY ENDING IN TWO, and the derivation owns that too: a
		// caller choosing the drain variant itself would be back to choosing a phase.
		"an ending while output is outstanding pends the drain": {
			mqttAckArrival{Packet: mqttPacketPubComp, OutstandingOutput: true}, 2,
			mqttExchangeTerminalPendingDrain},
		"a rejection while output is outstanding pends the drain": {
			mqttAckArrival{Packet: mqttPacketPubRec, Reason: mqttReasonUnspecifiedError,
				OutstandingOutput: true}, 2,
			mqttExchangeRejectedPendingDrain},
		// A TRANSFER IS NOT AN ENDING, so the drain has nothing to say about it.
		"a PUBREC while output is outstanding still transfers": {
			mqttAckArrival{Packet: mqttPacketPubRec, OutstandingOutput: true}, 2,
			mqttExchangePubRecObserved},
	} {
		got, err := mqttPhaseForAck(tc.in, tc.qos)
		if err != nil {
			t.Fatalf("%s was refused: %v", name, err)
		}
		if got != tc.want {
			t.Fatalf("%s derived phase %d, want %d", name, got, tc.want)
		}
	}
}

// TestF105AnAckOfTheWrongKindAnswersADifferentObligation is the mismatch the derivation is the
// only place that can see.
//
// A PUBACK answers a QoS 1 delivery and a PUBREC a QoS 2 one. The packet has no QoS field, so
// the pair is what makes a mismatch detectable at all — and a mismatch is not a badly-placed
// move, it is an answer to a different obligation.
func TestF105AnAckOfTheWrongKindAnswersADifferentObligation(t *testing.T) {
	for name, tc := range map[string]struct {
		in  mqttAckArrival
		qos byte
	}{
		"a PUBACK for a QoS 2 exchange":  {mqttAckArrival{Packet: mqttPacketPubAck}, 2},
		"a PUBREC for a QoS 1 exchange":  {mqttAckArrival{Packet: mqttPacketPubRec}, 1},
		"a PUBCOMP for a QoS 1 exchange": {mqttAckArrival{Packet: mqttPacketPubComp}, 1},
		"a PUBACK for a QoS 0 delivery":  {mqttAckArrival{Packet: mqttPacketPubAck}, 0},
		// A PUBREL IS THIS BROKER'S OWN PACKET, not an acknowledgement of an outbound
		// obligation, and a phase derived from it would be one no protocol event produced.
		"a PUBREL":  {mqttAckArrival{Packet: mqttPacketPubRel}, 2},
		"a PUBLISH": {mqttAckArrival{Packet: mqttPacketPub}, 2},
		"a PINGREQ": {mqttAckArrival{Packet: mqttPacketPing}, 2},
	} {
		if _, err := mqttPhaseForAck(tc.in, tc.qos); err == nil {
			t.Fatalf("%s derived an acknowledgement phase", name)
		}
	}
}

// TestF105AResolutionDerivesRatherThanAccepts pins the two halves together.
//
// Deriving correctly is no use if the resolution still takes a phase, so the resolution's own
// signature is what enforces it: there is nowhere left to pass one in.
func TestF105AResolutionDerivesRatherThanAccepts(t *testing.T) {
	var b mqttAckBindings
	if err := b.capture(mqttAckBinding{Gen: testF105Gen, Nonce: "E1", PI: 7, Acq: 1, QoS: 2}); err != nil {
		t.Fatalf("capturing: %v", err)
	}

	got, err := mqttResolveAck(&b, 7, mqttAckArrival{Packet: mqttPacketPubRec})
	if err != nil || !got.Resolved || got.Phase != mqttExchangePubRecObserved {
		t.Fatalf("a PUBREC did not resolve to the transfer: %+v %v", got, err)
	}

	// AN ACKNOWLEDGEMENT OF THE WRONG KIND FOR A CAPTURED IDENTIFIER IS AN ERROR, not an
	// unresolved one: the binding exists, so this is a packet answering the wrong obligation
	// rather than one this connection knows nothing about.
	_, err = mqttResolveAck(&b, 7, mqttAckArrival{Packet: mqttPacketPubAck})
	if err == nil {
		t.Fatalf("a PUBACK resolved against a QoS 2 obligation")
	}
	if !strings.Contains(err.Error(), "E1") {
		t.Fatalf("the refusal did not name the exchange it was resolved against: %v", err)
	}
}

// TestF105ACaptureWithNothingToAcknowledgeIsRefused keeps the derivation's one non-packet input
// honest.
//
// The QoS is captured at DISPATCH rather than supplied with the acknowledgement, because a
// caller free to state it could make a PUBACK derive a legal ending for a QoS 2 exchange — which
// is the very defect this derivation replaces, wearing the new API. A QoS 0 binding has nothing
// to acknowledge at all.
func TestF105ACaptureWithNothingToAcknowledgeIsRefused(t *testing.T) {
	var b mqttAckBindings
	if err := b.capture(mqttAckBinding{Gen: testF105Gen, Nonce: "E0", PI: 3, Acq: 1}); err == nil {
		t.Fatalf("a QoS 0 binding was captured, so an arriving PUBACK could derive a phase for it")
	}
	if got, err := mqttResolveAck(&b, 3, mqttAckArrival{Packet: mqttPacketPubAck}); err != nil || got.Resolved {
		t.Fatalf("a refused capture still resolved: %+v %v", got, err)
	}
	if err := b.capture(mqttAckBinding{Gen: testF105Gen, Nonce: "E1", PI: 3, Acq: 1, QoS: 1}); err != nil {
		t.Fatalf("a QoS 1 binding was refused: %v", err)
	}
}
