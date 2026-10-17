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

import "fmt"

// The acknowledgement phase, DERIVED from the packet (F-105, row 4; contract 3).
//
// # A check that was adjacent to the property, not the property
//
// This lane's audit marked "the acknowledgement's PHASE is the caller's reading of the packet"
// as closed, because the boundary checks the proposed move is legal from where the exchange
// stands. A review refused that: **a fabricated phase can be perfectly legal and still answer
// the wrong protocol event.** A caller that read a PUBACK and supplied a PUBREC-shaped phase is
// accepted whenever that move happens to be legal — and it has just recorded a transfer of
// ownership the client never made.
//
// So the phase is not a parameter with a validity rule. It is DERIVED, here, from the three
// facts the dispatch site actually has: which packet arrived, what the exchange's QoS is, and
// what the packet's reason code says. A caller cannot choose it, which is the only version of
// this contract that is not a caller contract.
//
// # The QoS is part of the derivation, not a check afterwards
//
// A PUBACK answers a QoS 1 delivery and a PUBREC a QoS 2 one, so a packet that does not match
// the exchange's QoS is not a badly-placed move — it is an answer to a different obligation.
// Deriving from the pair refuses it in the one place that can tell.
//
// # Outstanding output splits every ending in two
//
// An ending observed while this broker may still emit for the exchange is a PENDING-DRAIN
// ending, and that distinction is not about the packet. It is carried here because the two are
// one decision: a caller that derived the ending and then chose the drain variant itself would
// be back to choosing a phase.

// mqttAckArrival is everything the dispatch site knows about one arriving acknowledgement.
type mqttAckArrival struct {
	// Packet is the control packet type as read off the wire.
	Packet byte
	// Reason is the MQTT 5 reason code, or mqttReasonSuccess for a 3.1.1 packet that carries
	// none. A 3.1.1 acknowledgement cannot express a refusal, so the success value is the
	// honest translation rather than a default.
	Reason byte
	// OutstandingOutput reports that this broker may still emit for the exchange. It is what
	// separates an ending from a PENDING-DRAIN ending.
	OutstandingOutput bool
}

// mqttPhaseForAck derives the phase one arriving acknowledgement establishes.
//
// Total over the packets that ARE acknowledgements of an outbound obligation, and a refusal for
// everything else: a phase derived from a packet that acknowledges nothing would be a phase
// nobody's protocol event produced.
func mqttPhaseForAck(in mqttAckArrival, qos byte) (mqttExchangePhase, error) {
	refused := in.Reason >= mqttReasonUnspecifiedError
	switch in.Packet {
	case mqttPacketPubAck:
		// A PUBACK ENDS A QoS 1 DELIVERY, in both directions.
		if qos != 1 {
			return 0, fmt.Errorf("a PUBACK answers a QoS 1 delivery, and this exchange is QoS %d: "+
				"an acknowledgement of the wrong kind answers a different obligation", qos)
		}
		// A REFUSED PUBACK STILL ENDS IT AT TERMINAL, and deliberately not at a rejection.
		// The rejection phases record a REFUSED PUBREC, and a QoS 1 exchange has no PUBREC to
		// refuse — the record's own validator says so. Terminal is the honest phase here:
		// nothing about ownership is claimed either way, which is exactly the situation.
		return mqttEndingPhase(mqttExchangeTerminal, in.OutstandingOutput), nil

	case mqttPacketPubRec:
		if qos != 2 {
			return 0, fmt.Errorf("a PUBREC answers a QoS 2 delivery, and this exchange is QoS %d: "+
				"an acknowledgement of the wrong kind answers a different obligation", qos)
		}
		// A SUCCESSFUL PUBREC IS THE TRANSFER OF OWNERSHIP, and the only packet that produces
		// it. A REFUSED one transfers nothing: the client is telling this broker the delivery
		// is not its problem, and no PUBREL is owed for it.
		if refused {
			return mqttEndingPhase(mqttExchangeRejected, in.OutstandingOutput), nil
		}
		return mqttExchangePubRecObserved, nil

	case mqttPacketPubComp:
		if qos != 2 {
			return 0, fmt.Errorf("a PUBCOMP answers a QoS 2 delivery, and this exchange is QoS "+
				"%d: an acknowledgement of the wrong kind answers a different obligation", qos)
		}
		// A PUBCOMP ENDS THE EXCHANGE WHATEVER ITS REASON CODE. The only failure the spec
		// permits here is "Packet Identifier Not Found" [MQTT-3.7.2.1], which says the client
		// no longer holds the exchange — so the obligation is over in both arms, and calling
		// that a REJECTION would say no ownership was transferred when a PUBREC already did.
		return mqttEndingPhase(mqttExchangeTerminal, in.OutstandingOutput), nil
	}
	return 0, fmt.Errorf("packet type 0x%x does not acknowledge an outbound obligation", in.Packet)
}

// mqttEndingPhase picks between an ending and its PENDING-DRAIN variant.
//
// One function so the pairing has one home: an ending recorded as terminal while output is
// still outstanding would license the cleanup that a still-executing action needs not to
// happen, and the two constants are otherwise easy to mix up at a call site.
func mqttEndingPhase(ended mqttExchangePhase, outstanding bool) mqttExchangePhase {
	if !outstanding {
		return ended
	}
	switch ended {
	case mqttExchangeTerminal:
		return mqttExchangeTerminalPendingDrain
	case mqttExchangeRejected:
		return mqttExchangeRejectedPendingDrain
	}
	return ended
}
