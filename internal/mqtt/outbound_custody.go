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

// Exchange-owned custody and capacity (F-105, absorbing F-106).
//
// THE DEFECT: capacity is derived from the scheduling maps. qos12InflightForCap counts
// pendingPublish and, at Receive Maximum, pendingPubRel — so REMOVING A MAP ENTRY RELEASES
// CAPACITY, and two metadata routes do exactly that. forgetCancelledReg drops a parked entry
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
	switch x.Phase {
	case mqttExchangePublish, mqttExchangePubRecObserved:
		return mqttChargeHeld, nil
	case mqttExchangeTerminalPendingDrain, mqttExchangeRejectedPendingDrain:
		return mqttChargeHeldPendingDrain, nil
	case mqttExchangeTerminal, mqttExchangeRejected, mqttExchangeSupersededUnsent,
		mqttExchangeWithdrawnUnsent:
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

// mqttChargedCount is the Receive Maximum occupancy of one generation's obligations.
//
// Computed from the durable exchanges, never from a scheduling map. outstandingFor answers
// whether a given exchange still has output that may execute; a caller that cannot answer
// must say TRUE, because an unknown action is an outstanding one.
func mqttChargedCount(obs *mqttGenerationObligations, outstandingFor func(nonce string) bool) (int, error) {
	if obs == nil {
		return 0, fmt.Errorf("charge asked about no obligations")
	}
	// AN UNREADABLE OBLIGATION COUNTS. A record this broker cannot interpret is not an
	// absent one, and treating it as free capacity is how an identifier gets reused under
	// an exchange nobody could read.
	//
	// Only the undecodable EXCHANGES are charged. An exchange's undecodable actions are not
	// separate obligations, so counting them would bill one obligation several times — the
	// accounting is meant to be conservative, not arbitrary. They still block a
	// finished-generation claim, which is what obs.unreadableOther is for.
	n := obs.unreadableExchanges
	for _, x := range obs.exchanges {
		out := true
		if outstandingFor != nil {
			out = outstandingFor(x.Nonce)
		}
		d, err := mqttExchangeCharge(x, out)
		if err != nil {
			// Same rule: an exchange whose charge cannot be decided holds one.
			n++
			continue
		}
		if d != mqttChargeReleasable {
			n++
		}
	}
	return n, nil
}

// mqttReleaseEvidence is what a caller offers as grounds for releasing a charge.
type mqttReleaseEvidence struct {
	// Terminal reports bound terminal evidence for THIS exchange — an acknowledgement the
	// client sent, tied to this obligation rather than to an identifier.
	Terminal bool
	// Drained reports that no action of this exchange may still emit, established
	// positively: the holder settled, or a closure certificate names its connection.
	Drained bool
	// UnsentWhole reports proven whole-exchange unsent CANCELLATION: the exchange was never
	// transmitted in any attempt AND no attempt can subsequently execute. Both halves are
	// required. A historical observation that nothing had been written yet does not
	// qualify, because an acquired action may still be sitting in front of its first write.
	UnsentWhole bool
	// DiscardedGeneration reports an AUTHORIZED generation discard covering this exchange.
	DiscardedGeneration bool

	// MetadataForgotten reports that some scheduling metadata about this exchange was
	// dropped — a retired verdict, a settled reservation sweep. It is carried so the
	// decision can REFUSE it explicitly rather than by omission.
	MetadataForgotten bool
}

// mqttChargeReleaseAllowed decides whether a charge may be released.
//
// Exactly three grounds, and metadata is not one of them. The explicit refusal is the point
// of this function: before F-105 the release was a side effect of deleting a map entry, so
// no code anywhere said "this is not a reason" — it simply happened. A caller offering only
// forgotten metadata is told so.
func mqttChargeReleaseAllowed(e mqttReleaseEvidence) (bool, string) {
	switch {
	case e.UnsentWhole:
		// Nothing was ever put on the wire for this obligation, so nothing can arrive for
		// it later. This is whole-exchange evidence: a single unsent attempt does not
		// qualify, because another attempt may have transmitted.
		return true, "the whole exchange is proven unsent"
	case e.DiscardedGeneration:
		return true, "an authorized generation discard covers this exchange"
	case e.Terminal && e.Drained:
		return true, "bound terminal evidence, with output drained"
	case e.Terminal && !e.Drained:
		return false, "terminal evidence stands but output may still execute; releasing here " +
			"would let a late first transmission escape after its own release boundary"
	case e.MetadataForgotten:
		return false, "forgetting scheduling metadata is not completion: a retired verdict " +
			"proves neither non-transmission nor that the client finished, and a peer may " +
			"already have transmitted this allocation"
	}
	return false, "no evidence offered establishes that this obligation ended"
}

// mqttIdentifierReleaseAllowed decides whether an exchange's packet identifier may be
// returned to the allocator.
//
// ACCOUNTING RELEASE AND IDENTIFIER RELEASE ARE DIFFERENT QUESTIONS, and conflating them was
// a defect. An authorized generation discard releases the CHARGE — it says the old
// generation's obligations do not count against the fresh connection — and says nothing
// whatever about whether an old connection can still emit an acquired action for that
// exchange. Reusing the identifier on that connection while it can is how a client receives
// two different messages under one identifier.
//
// So the identifier needs the charge to be releasable AND both connection-bound facts:
//
//   - the output disposition: no action of this exchange may still emit, established
//     positively by settlement or by a closure certificate for its connection;
//   - the handler discharge: no captured acknowledgement handler on that connection could
//     still name this exchange.
//
// Neither is implied by the accounting ground. A discarded generation with a live old
// connection satisfies the charge and neither of these.
func mqttIdentifierReleaseAllowed(e mqttReleaseEvidence, bindingsDischarged bool) (bool, string) {
	ok, why := mqttChargeReleaseAllowed(e)
	if !ok {
		return false, why
	}
	// The unsent-whole ground already carries "and none can subsequently occur", which is
	// the output disposition for an exchange that never transmitted. Every other ground
	// needs it stated.
	if !e.UnsentWhole && !e.Drained {
		return false, "the charge is releasable in the fresh generation's accounting, but an " +
			"action of this exchange may still emit on its original connection; reusing the " +
			"identifier there would put two messages under one identifier"
	}
	if !bindingsDischarged {
		return false, "the charge is releasable but a captured acknowledgement handler could " +
			"still name this exchange on its original connection"
	}
	return true, why
}
