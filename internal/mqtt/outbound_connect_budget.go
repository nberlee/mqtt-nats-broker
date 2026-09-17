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

import (
	"fmt"
	"time"
)

// The ownership protocol's deadline (F-105, row 9).
//
// # F-051 does not supply this, and an earlier draft claimed it did
//
// `mqttProcessConnect` calls `mqttAdmitConnectLocked`, which STOPS F-051's timer
// (`connect_deadline.go`), and it does so BEFORE authorization and ownership processing. So the
// interval between that stop and a servable authority had no deadline at all — a CONNECT could
// sit in the ownership protocol indefinitely while the client waited for a CONNACK.
//
// This budget is therefore a new one, and it is ANCHORED AT F-051'S STOP so the whole interval it
// claims to cover really is covered. A budget that started at the first ownership WRITE would
// leave the reads and the recovery before it uncovered, which is precisely the gap.
//
// # One absolute budget, never reset
//
// Not per operation: a protocol that gave each CAS its own timeout would let a retry loop extend
// the total without bound, which is the failure a deadline exists to prevent. The REMAINING
// budget is carried through every ownership read, recovery step, reservation, metadata commit and
// finalization, and a retry consumes it like anything else.
//
// # Expiry has THREE dispositions, and they are not interchangeable
//
// This is the part that matters most, because two of them look alike and mean opposite things.
//
//   - ABORTED. The proposal positively lost. This acceptance closes its generation and REFUSES
//     the CONNECT; it does not mint a replacement, which is the aborted-proposal loser bound.
//   - COMMITTED BUT NOT FINALIZED. NOT a refusal of the protocol — only of this connection. The
//     metadata half is permanent and another acceptance finalizes it exactly as proposed, then
//     takes its own tenure by an ordinary handoff. A committed slot is never aborted; that is
//     the one act the create-only fence forbids.
//   - UNRESOLVED, which is a third class and NOT the same as aborted. Absence of a committed
//     verdict is not proof of non-commitment. The connection is refused, NOTHING is aborted, and
//     the proposal is left for the next acceptance to resolve.
//
// No disposition deletes an arbitration record, so an expired attempt leaves the client servable
// by the next acceptance.
//
// # "Recoverable" is not "guaranteed to succeed"
//
// Under continued failures the protocol promises that the client is not permanently unservable.
// It does not promise the next acceptance completes. Stating the weaker thing is the honest claim
// and it is the one this mechanism actually supports.

// mqttOwnershipBudget is one CONNECT's absolute budget for the ownership protocol.
type mqttOwnershipBudget struct {
	// Anchor is when F-051's deadline STOPPED, and Total the whole budget from there. Both are
	// frozen: a retry may consume the budget and may never extend it.
	Anchor time.Time
	Total  time.Duration
}

// mqttNewOwnershipBudget anchors a budget at the moment F-051's deadline stopped.
func mqttNewOwnershipBudget(anchor time.Time, total time.Duration) (*mqttOwnershipBudget, error) {
	if anchor.IsZero() {
		return nil, fmt.Errorf("an ownership budget must be anchored at the moment F-051's " +
			"deadline stopped, so the interval it covers is the interval that was uncovered")
	}
	if total <= 0 {
		return nil, fmt.Errorf("an ownership budget of %v covers nothing", total)
	}
	return &mqttOwnershipBudget{Anchor: anchor, Total: total}, nil
}

// remaining reports how much budget is left at now.
//
// Derived from the ANCHOR every time rather than decremented as steps complete. A decremented
// counter measures the work that reported back; this measures the wall clock, which is what a
// client waiting for a CONNACK actually experiences — and it is why a step that hangs cannot
// quietly avoid spending the budget.
func (b *mqttOwnershipBudget) remaining(now time.Time) time.Duration {
	if b == nil {
		return 0
	}
	spent := now.Sub(b.Anchor)
	if spent >= b.Total {
		return 0
	}
	return b.Total - spent
}

// expired reports whether the whole budget is gone.
func (b *mqttOwnershipBudget) expired(now time.Time) bool { return b.remaining(now) <= 0 }

// mqttProposalState is what this broker has established about its own ownership proposal.
type mqttProposalState uint8

const (
	// mqttProposalUnresolved: no verdict was read. NOT the same as aborted.
	mqttProposalUnresolved mqttProposalState = iota + 1
	// mqttProposalAborted: positively established that the proposal lost.
	mqttProposalAborted
	// mqttProposalCommitted: the metadata half landed permanently.
	mqttProposalCommitted
	// mqttProposalFinalized: committed AND finalized, so this connection may be served.
	mqttProposalFinalized
)

// mqttExpiryDisposition is what an expiry means for this connection and for the proposal.
type mqttExpiryDisposition struct {
	// RefuseConnect is true whenever the budget ran out before a servable authority existed.
	// Every disposition below refuses THIS connection; what they disagree about is the
	// PROPOSAL.
	RefuseConnect bool
	// CloseGeneration is true only for the aborted loser, which closes its generation rather
	// than minting a replacement.
	CloseGeneration bool
	// LeaveSettleable is true when the proposal stands and a later acceptance resolves it. It
	// is the opposite of CloseGeneration and the two are never both set.
	LeaveSettleable bool
	// Why is the fact the disposition turned on.
	Why string
}

// mqttDisposeExpiredOwnership decides what an expired ownership protocol means.
//
// It NEVER deletes an arbitration record and never aborts a committed slot. Those two sentences
// are the whole safety argument: an expired attempt leaves the client servable by the next
// acceptance, and the one act the create-only fence forbids is undoing a commitment.
func mqttDisposeExpiredOwnership(state mqttProposalState) (mqttExpiryDisposition, error) {
	switch state {
	case mqttProposalFinalized:
		// NOT AN EXPIRY OUTCOME AT ALL. A finalized proposal has a servable authority, so the
		// budget is spent but nothing is refused — and a caller asking this question about a
		// finalized proposal has confused two states.
		return mqttExpiryDisposition{}, fmt.Errorf("a finalized proposal has a servable " +
			"authority, so its budget expiring refuses nothing")

	case mqttProposalAborted:
		return mqttExpiryDisposition{
			RefuseConnect: true, CloseGeneration: true,
			Why: "the proposal positively lost, so this acceptance closes its generation and " +
				"refuses the CONNECT rather than minting a replacement",
		}, nil

	case mqttProposalCommitted:
		// THE METADATA HALF IS PERMANENT. Refusing this CONNECT is not refusing the protocol:
		// another acceptance finalizes the proposal EXACTLY AS PROPOSED and then takes its own
		// tenure by an ordinary handoff. Aborting here would be the one act the create-only
		// fence forbids.
		return mqttExpiryDisposition{
			RefuseConnect: true, LeaveSettleable: true,
			Why: "the metadata half committed permanently, so this connection is refused while " +
				"the proposal stands and stays settleable by the next acceptance",
		}, nil

	case mqttProposalUnresolved:
		// THE THIRD CLASS, and the reason it exists: absence of a committed verdict is NOT
		// proof of non-commitment. Treating it as aborted would close a generation whose
		// metadata may in fact have landed.
		return mqttExpiryDisposition{
			RefuseConnect: true, LeaveSettleable: true,
			Why: "no verdict was established, and absence of a committed verdict is not proof " +
				"of non-commitment: nothing is aborted and the proposal is left to be resolved",
		}, nil
	}
	return mqttExpiryDisposition{}, fmt.Errorf("proposal state %d has no expiry disposition", state)
}

// mqttOwnershipStepAllowed reports whether another ownership step may begin.
//
// Asked BEFORE each step rather than after, because a step that starts with no budget left will
// finish after the client has already been failed — and the point of the budget is what the
// client experiences, not what the broker records.
func mqttOwnershipStepAllowed(b *mqttOwnershipBudget, now time.Time, step string) error {
	if b == nil {
		return fmt.Errorf("the ownership step %q has no budget, so nothing bounds it", step)
	}
	if b.expired(now) {
		return fmt.Errorf("the ownership step %q has no budget left of %v: the whole protocol "+
			"shares one absolute budget anchored where F-051's deadline stopped, and a retry "+
			"consumes it like anything else", step, b.Total)
	}
	return nil
}
