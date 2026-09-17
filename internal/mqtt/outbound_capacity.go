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

import "fmt"

// The two capacity projections (F-105 row 5, absorbing F-106).
//
// qos12InflightForCap counts the scheduling maps, and that is the defect: forgetCancelledReg
// and releaseSettledVoidReservationsLocked remove counted entries on METADATA disposition, so a
// verdict retiring frees a Receive Maximum slot that no completion evidence ever released. At
// Receive Maximum 1 that admits a second delivery while the client still holds the first.
//
// # It is TWO caps in one function, and they are not the same cap
//
// The single helper carries two different limits and the plan's first version collapsed them:
//
//   - RECEIVE MAXIMUM is a protocol limit on what the CLIENT is holding unacknowledged. QoS 2
//     exchanges awaiting PUBCOMP still count, because Spec5 [4.9] frees the quota only on
//     PUBACK/PUBCOMP or a failed PUBREC. This is the one that must derive from exchange-owned
//     custody, because it is a statement about obligations.
//   - THE SOURCE CAP is JetStream MaxAckPending, used when no Receive Maximum was negotiated,
//     and it also paces deferred delivery. It is a statement about DELIVERIES this broker has
//     taken from a consumer and not yet acked, which is not the same population: a delivery
//     whose exchange ended but whose source ack is still owed occupies the second and not the
//     first.
//
// effectiveMaxAck is where they meet, and it keeps meeting there. What changes is only where
// the OCCUPANCY comes from.
//
// # Nothing is exempt from the Receive Maximum projection
//
// It is tempting to discount an obligation whose consumer is gone, since it can never complete
// — but Receive Maximum bounds what the client is holding, and a consumer disappearing does not
// prove the client completed anything. An unreadable obligation counts for the same reason: a
// record this broker cannot interpret is not free capacity. A session stuck this way is a lost
// message being reported honestly, which is the cost F-106's bullet already accepts.

// mqttCapacityProjections is one generation's occupancy, split by the limit it counts against.
type mqttCapacityProjections struct {
	// ReceiveMaximum is how many obligations the client is holding unacknowledged, derived
	// from the durable exchanges via mqttChargedCount.
	ReceiveMaximum int
	// Source is how many deliveries this broker has taken and not yet acked to JetStream.
	// Supplied by the caller, because it is a fact about consumer state rather than about
	// obligations, and no durable record of ours carries it.
	Source int
}

// mqttProjectCapacity derives both projections for one generation's obligations.
//
// outstandingFor answers whether a given exchange still has output that may execute; a caller
// that cannot answer must say TRUE, because an unknown action is an outstanding one. That
// contract is mqttChargedCount's and is restated here because this is where callers meet it.
func mqttProjectCapacity(obs *mqttGenerationObligations, sourceHeld int,
	outstandingFor func(nonce string) bool) (mqttCapacityProjections, error) {

	if obs == nil {
		return mqttCapacityProjections{}, fmt.Errorf("capacity asked about no obligations")
	}
	charged, err := mqttChargedCount(obs, outstandingFor)
	if err != nil {
		return mqttCapacityProjections{}, err
	}
	if sourceHeld < 0 {
		return mqttCapacityProjections{}, fmt.Errorf("a source cap cannot hold %d deliveries", sourceHeld)
	}
	return mqttCapacityProjections{ReceiveMaximum: charged, Source: sourceHeld}, nil
}

// admitsAnother reports whether one more QoS 1/2 delivery may be admitted under both limits.
//
// rmax is the negotiated Receive Maximum, 0 when none was negotiated, and maxAck the JetStream
// MaxAckPending. BOTH are applied when a Receive Maximum exists: the protocol limit does not
// replace the consumer's, it stands beside it, and admitting past either one is a defect of a
// different kind.
func (p mqttCapacityProjections) admitsAnother(rmax, maxAck int) bool {
	if maxAck > 0 && p.Source >= maxAck {
		return false
	}
	if rmax > 0 && p.ReceiveMaximum >= rmax {
		return false
	}
	// NO RECEIVE MAXIMUM IS NOT UNLIMITED OBLIGATIONS. Without one the protocol sets no
	// bound on what the client holds, and the consumer's own cap is the only limit — which
	// is what the source projection above has already applied.
	return true
}
