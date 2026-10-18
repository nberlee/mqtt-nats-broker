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

// Exchange-owned custody and capacity (F-105, absorbing F-106).
//
// THE DEFECT: capacity is derived from the scheduling maps. qos12InflightForCap counts
// pendingPublish and, at Receive Maximum, pendingPubRel — so REMOVING A MAP ENTRY RELEASES
// CAPACITY, and two metadata routes do exactly that. forgetVoidedReg drops a parked entry
// because its verdict retires, and releaseSettledVoidReservationsLocked has the same
// metadata-to-capacity release. A peer may already have transmitted that allocation; a
// tombstone proves neither non-transmission nor completion. At Receive Maximum 1 the result
// is a second delivery admitted while the first is still outstanding.
//
// The correction is a change of OWNER, not an added check. The durable exchange ledger owns
// the charge; the scheduling maps become indexes. Metadata operations may forget scheduling
// metadata all they like — what they cannot do is release a charge, because they no longer
// hold one.
//
// Three facts stay separate, and collapsing any two is what produced the defect:
//
//   - COMPLETION: this obligation ended.
//   - OCCUPANCY: some structure currently holds a record about it.
//   - OBSERVATION FINISHED: a continuation has no further work.
//
// Neither occupancy nor the end of observation establishes completion.

// mqttChargeDisposition is why a charge is or is not held.
type mqttChargeDisposition uint8

const (
	// mqttChargeHeld: the obligation is outstanding and holds its Receive Maximum charge.
	mqttChargeHeld mqttChargeDisposition = iota + 1
	// mqttChargeHeldPendingDrain: protocol completion is recorded but output may still be
	// executing, so the charge is STILL HELD. This is the state a naive implementation
	// releases, and releasing it lets a late first transmission escape after its own
	// release boundary.
	mqttChargeHeldPendingDrain
	// mqttChargeReleasable: nothing further can be emitted and the obligation ended.
	mqttChargeReleasable
)

// mqttExchangeCharge reports whether one exchange holds a Receive Maximum charge, and why.
//
// outstanding is a fact about that exchange's ACTIONS — whether any may still emit — and is
// supplied by the caller from the action records and their connection bindings. It is not
// derived from the exchange's phase, because the phase records the PROTOCOL and the two
// answer different questions.
func mqttExchangeCharge(x *mqttOutboundExchange, outstanding bool) (mqttChargeDisposition, error) {
	if x == nil {
		return 0, fmt.Errorf("charge asked about no exchange")
	}
	if !mqttValidExchangePhase(x.Phase) {
		return 0, fmt.Errorf("exchange carries an unsupported phase %d", x.Phase)
	}
	// AN ABANDONED EXCHANGE STAYS RELEASED. Its charge went at the abandonment (the resumed
	// connection will never send it), and a later client acknowledgement that advances the
	// phase — a successful PUBREC, and the PUBREL output that follows it — does not recharge
	// it. The fact, not the phase, is what says so: the phase a census reads may be any one
	// of the acknowledgement's successors, the abandonment's own phase never observed. The
	// IDENTIFIER is a separate question (mqttCustodyDisposition): it follows the ending.
	if x.Abandoned != nil {
		return mqttChargeReleasable, nil
	}
	switch x.Phase {
	case mqttExchangePublish, mqttExchangePubRecObserved:
		return mqttChargeHeld, nil
	case mqttExchangeTerminalPendingDrain, mqttExchangeRejectedPendingDrain:
		return mqttChargeHeldPendingDrain, nil
	case mqttExchangeTerminal, mqttExchangeRejected, mqttExchangeSupersededUnsent,
		mqttExchangeWithdrawnUnsent, mqttExchangeDeliveryAbandoned, mqttExchangeDiscarded:
		// A DISCARDED generation's obligation (F-105 7d) releases its charge on the authorized
		// discard ground — no connection of that generation will send it — and like every other
		// arm here only once nothing can still emit. The IDENTIFIER does not follow: Discarded is
		// not an ending (mqttCustodyDisposition reads mqttExchangePhaseEnded), so a discard
		// manufactures no drain and no handler discharge.
		// ENDED — completed, refused, or SUPERSEDED or WITHDRAWN while proven-unsent — AND
		// nothing can still emit. A rejection releases on the same terms as a completion: the receiver will not
		// take the message, so there is no release to owe and nothing further to hold a charge
		// for. A proven-unsent supersession releases on the same terms too: the PUBLISH never
		// left the broker, so its obligation owed nothing further once its identifier moved on.
		//
		// If an action may yet execute the exchange should not be in this phase at all, but
		// the check is here rather than assumed: the cost of being wrong is an identifier
		// reused under a live transmission.
		if outstanding {
			return mqttChargeHeldPendingDrain, nil
		}
		return mqttChargeReleasable, nil
	}
	return 0, fmt.Errorf("exchange carries an unsupported phase %d", x.Phase)
}
