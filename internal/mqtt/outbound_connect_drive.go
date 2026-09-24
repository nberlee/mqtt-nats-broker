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

// Driving the ownership protocol to a servable authority (F-105, row 9).
//
// `mqttDecideConnectOwnership` says which transition a CONNECT must run NEXT. It is stateless
// and decides from one read. What is missing — and what this is — is the thing that carries a
// sequence of those decisions to a conclusion under ONE budget, and that disposes of the
// conclusion correctly when the budget runs out instead.
//
// # Why a driver exists at all rather than a loop at the call site
//
// Three rules have to hold ACROSS the steps, and none of them can be stated by a per-step
// decision:
//
//   - the budget is absolute and shared, so a retry consumes it and never extends it;
//   - the acceptance binds itself to ONE foreign proposal and never chases a succession of
//     them, which is the abort war the bound exists to prevent;
//   - an acceptance whose OWN proposal was aborted LOSES: it closes its generation, refuses,
//     and does not mint a replacement. The decision function cannot enforce that — it is
//     stateless and cannot tell a fresh acceptance from the loser re-entering — so the caller
//     owes it, and this is the caller.
//
// # What it deliberately does NOT do
//
// It performs no transition itself. Each decided action is handed to an executor the caller
// supplies, because the transitions are authority-batch commits whose composition belongs to
// the ownership writer, and a driver that composed them would be a second place where a batch
// is assembled. What the driver owns is ORDER, the budget, and the two bounds above.
//
// It also does not decide whether the client may be SERVED. Reaching a servable authority is
// necessary and not sufficient; the restore gate's refusals happen after the acceptance save,
// which is later than anything here.
//
// # REFUSED AND EXPIRED ARE FINAL FOR THIS CONNECTION
//
// A servable authority appearing afterwards does not revive a socket this run already refused,
// and that distinction has to be held by the caller because the two facts are genuinely
// different. Durable protocol FINALIZATION is permanent and belongs to the proposal: an
// acceptance whose metadata half committed leaves it settleable, and a later acceptance
// finalizes it exactly as proposed. PERMISSION TO ACTIVATE belongs to one connection and was
// spent when this run returned.
//
// Conflating them would mean a CONNECT refused on its deadline could be activated moments later
// because somebody else finished the protocol — serving a client on a socket whose own
// acceptance had already failed.

// mqttOwnershipStep is one decided action, handed to the caller's executor.
type mqttOwnershipStep struct {
	Action mqttConnectOwnershipAction
	// Recovery is set only for mqttOwnershipRecoverForeign.
	Recovery *mqttForeignRecovery
	// Reason is the decision's own explanation, carried so a refusal can say which
	// ownership situation it hit rather than that one occurred.
	Reason string
	// Remaining is the budget left when this step was admitted, and the executor MUST honour
	// it as its own deadline.
	//
	// Checking a deadline before starting a step does NOT bound a step that then blocks:
	// the driver would sit inside the executor for as long as the store took, and the budget
	// it checked would be long gone by the time it looked again. A caller that ignores this
	// and blocks indefinitely has an unbounded CONNECT, and no amount of checking between
	// steps recovers that.
	Remaining time.Duration
}

// mqttOwnershipExecutor performs one step and reports what to read next.
//
// It returns the input for the NEXT decision rather than a success flag, because every step's
// outcome is re-derived from durable state: a step that "succeeded" and a step whose response
// was lost are indistinguishable to the caller, and both are resolved by reading again.
type mqttOwnershipExecutor func(mqttOwnershipStep) (mqttConnectOwnershipInput, error)

// mqttOwnershipOutcome is how the protocol ended.
type mqttOwnershipOutcome uint8

const (
	// mqttOwnershipServable: a servable authority stands. The only outcome that admits a
	// CONNACK, and still not on its own — the restore gate runs after this.
	mqttOwnershipServable mqttOwnershipOutcome = iota + 1
	// mqttOwnershipRefused: the protocol reached a state this acceptance may not proceed
	// from. Nothing is aborted by this outcome; the refusal is about this connection.
	mqttOwnershipRefused
	// mqttOwnershipExpired: the budget ran out. What that MEANS for the proposal is
	// mqttDisposeExpiredOwnership's answer and is carried on the result.
	mqttOwnershipExpired
)

// mqttOwnershipResult is the whole protocol's answer.
type mqttOwnershipResult struct {
	Outcome mqttOwnershipOutcome
	// Disposition is set only for mqttOwnershipExpired, and is what the expiry means for
	// the PROPOSAL as distinct from this connection.
	Disposition mqttExpiryDisposition
	// Steps is how many actions were executed, so a caller can say whether the budget was
	// spent making progress or spent on one step that hung.
	Steps  int
	Reason string
}

// mqttOwnershipMaxSteps bounds the number of durable TRANSITIONS one run may attempt.
//
// STATED CAREFULLY, because an earlier comment here claimed more than it should. The wall-clock
// budget already guarantees TERMINATION: the clock advances, the budget is finite and absolute,
// so a run of instant retries expires on its own. A test that hangs without this bound is
// demonstrating a frozen test clock, not a production failure — and that is what the first
// version of its mutation actually showed.
//
// What this bound adds is a limit on WORK rather than on time. Each step is a durable
// transition against contended records, so without it a run could attempt unboundedly many of
// them inside the time the budget allows — hammering the store and contending with the very
// brokers it is racing, right up until the clock stopped it.
//
// The number is small deliberately. The legitimate paths are short — reserve then finalize, or
// prepare then finalize, with at most one foreign recovery ahead of them. A run that needs many
// more is not making progress, it is contending, and stopping it early is kinder to everything
// else contending with it.
const mqttOwnershipMaxSteps = 8

// mqttOwnershipStepReattempts is the per-CONNECT allowance of authority-step RE-ATTEMPTS
// (reconcileOwnershipStep): shared by every step of every drive and re-drive of one CONNECT, so
// the work one CONNECT may do is mqttOwnershipMaxSteps transitions per drive plus at most this
// many re-attempts overall. A re-attempt is admitted only after the previous attempt is proven
// never to have landed, so each one replaces a transition that did not happen rather than
// adding one beside it.
const mqttOwnershipStepReattempts = 4

// mqttDriveOwnership carries the ownership protocol to a conclusion.
func mqttDriveOwnership(in mqttConnectOwnershipInput, ownTenure string,
	b *mqttOwnershipBudget, now func() time.Time,
	exec mqttOwnershipExecutor) (mqttOwnershipResult, error) {

	if exec == nil {
		return mqttOwnershipResult{}, fmt.Errorf("driving ownership requires an executor")
	}
	if ownTenure == _EMPTY_ {
		// WITHOUT ITS OWN TENURE the driver cannot tell "somebody is serving" from "I am
		// serving", which is the whole of finding 1. Refusing is the only safe answer: the
		// alternative is a run that returns servable for somebody else's ownership.
		return mqttOwnershipResult{}, fmt.Errorf("driving ownership requires the tenure this " +
			"acceptance installs, or it cannot tell its own ownership from anyone else's")
	}
	if now == nil {
		return mqttOwnershipResult{}, fmt.Errorf("driving ownership requires a clock")
	}

	// THE ACCEPTANCE'S OWN BOUNDS, held here because no single decision can hold them.
	var bound *mqttForeignRecovery
	ownProposalAborted := false
	// AND THE COMMITTED VERDICT IS STICKY, for the same reason the abort is.
	//
	// A committed metadata half is PERMANENT — never aborted, which is the one act the
	// create-only fence forbids — so once this run has observed one, it stays observed. An
	// earlier version re-derived the verdict from whatever the latest read carried, and a step
	// running in between therefore LOST it: the run then disposed an expiry as UNRESOLVED,
	// telling its caller nothing is known about a proposal whose metadata half is in fact
	// permanently committed. The two dispositions carry the same flags, so nothing downstream
	// would have noticed; what differs is what the NEXT acceptance must do, and it must
	// finalize a committed proposal exactly as proposed rather than settle it either way.
	ownProposalCommitted := false

	for steps := 0; ; steps++ {
		if steps >= mqttOwnershipMaxSteps {
			return mqttOwnershipResult{
				Outcome: mqttOwnershipRefused, Steps: steps,
				Reason: fmt.Sprintf("the ownership protocol took %d steps without reaching a "+
					"servable authority: that is contention rather than progress, and a "+
					"connection is refused rather than spun", steps),
			}, nil
		}

		// SERVABLE MEANS *THIS ACCEPTANCE'S* OWNERSHIP, not merely that somebody is serving.
		//
		// An earlier version returned here for ANY serving authority, and that was the
		// finalization-versus-activation confusion in its most damaging form: an ordinary
		// reconnect finding another broker's tenure would return servable having executed ZERO
		// steps, skipping the handoff preparation entirely — and the same happened after
		// recovering a committed FOREIGN proposal, whose tenure becomes serving while this
		// acceptance has acquired nothing. A caveat in a header does not repair a skipped
		// transition.
		//
		// The tenure is the test because a tenure is what an acceptance installs. Checking the
		// generation would not separate them: a handoff preserves the generation and changes
		// the tenure, which is exactly the case being skipped.
		if in.Load.status == mqttOutboundValid && mqttOwnershipAdmitsDelivery(in.Load.rec) &&
			in.Load.rec.Tenure == ownTenure {
			return mqttOwnershipResult{
				Outcome: mqttOwnershipServable, Steps: steps,
				Reason: "this acceptance's own tenure is serving",
			}, nil
		}

		// ONE CLOCK READING PER ITERATION, used for BOTH the gate check and the deadline handed
		// to the step. Two readings can disagree, and then a step admitted by the first is
		// given a deadline computed from the second — which is how a step gets told it has no
		// time left immediately after being told it could start.
		at := now()

		// THE BUDGET IS ASKED BEFORE EACH STEP, never after. A step admitted with nothing left
		// finishes after the client has already been failed.
		if err := mqttOwnershipStepAllowed(b, at, "ownership"); err != nil {
			state := mqttProposalUnresolved
			switch {
			case ownProposalAborted:
				state = mqttProposalAborted
			case ownProposalCommitted:
				state = mqttProposalCommitted
			}
			d, derr := mqttDisposeExpiredOwnership(state)
			if derr != nil {
				return mqttOwnershipResult{}, derr
			}
			return mqttOwnershipResult{
				Outcome: mqttOwnershipExpired, Disposition: d, Steps: steps,
				Reason: err.Error(),
			}, nil
		}

		// THE VERDICTS ARE LATCHED FROM THE READ, not from what the decision happens to do
		// with them. Both directions, and for the same reason.
		//
		// An earlier version set ownProposalAborted only when the decision returned
		// CloseAbandoned — immediately before returning — so it never protected a subsequent
		// reload. If a peer aborted and CLOSED this acceptance's reservation before its next
		// read, the input arrived as ABORTED over a closed authority, the decision returned
		// ReserveOverClosed, and the driver minted a REPLACEMENT proposal: the loser bound
		// violated by the very run it is supposed to stop.
		if in.Metadata == mqttMetadataCommitted {
			ownProposalCommitted = true
		}
		if in.Metadata == mqttMetadataAborted && in.IntentID != _EMPTY_ {
			ownProposalAborted = true
		}

		remaining := b.remaining(at)
		in.Recovering = bound
		d := mqttDecideConnectOwnership(in)

		// ONCE THIS ACCEPTANCE HAS LOST, THE ONLY ACT LEFT TO IT IS CLOSING ITS OWN GENERATION.
		//
		// The decision function is stateless and cannot tell a fresh acceptance from the loser
		// re-entering, so it keeps offering a way forward — and over an authority a peer has
		// ALREADY closed it offers ReserveOverClosed, which is a replacement proposal. That is
		// the abort war the bound exists to prevent, entered through the loser's own retry.
		//
		// Stated as a whitelist rather than a step count. An earlier version refused only from
		// the second iteration onward, which let exactly this case through on the first: the
		// abort is known from the READ, and nothing says the close is still owed.
		if ownProposalAborted && d.Action != mqttOwnershipCloseAbandoned {
			return mqttOwnershipResult{
				Outcome: mqttOwnershipRefused, Steps: steps,
				Reason: "this acceptance's own proposal was aborted, so it refuses rather than " +
					"minting a replacement: an acceptance that proposed again after losing " +
					"would be an internal retry loop racing whichever broker aborted it",
			}, nil
		}

		switch d.Action {
		case mqttOwnershipRefuse:
			return mqttOwnershipResult{
				Outcome: mqttOwnershipRefused, Steps: steps, Reason: d.Reason,
			}, nil

		case mqttOwnershipCloseAbandoned:
			// THE LOSER CLOSES ITS GENERATION ONCE AND THEN REFUSES, which is why this arm
			// returns rather than looping: the decision function is stateless and would keep
			// returning this action, and a second close would be an internal retry loop racing
			// whichever broker aborted the proposal.
			//
			// The flag is NOT set here. It is latched from the READ above, so it already
			// protects the case this arm cannot — a peer aborting and closing the reservation
			// between two reads, where the decision returns something else entirely.
			next, err := exec(mqttOwnershipStep{
				Action: d.Action, Reason: d.Reason, Remaining: remaining,
			})
			if err != nil {
				return mqttOwnershipResult{}, err
			}
			// The reloaded state is deliberately unused: this acceptance has no act left that
			// could depend on it.
			_ = next
			return mqttOwnershipResult{
				Outcome: mqttOwnershipRefused, Steps: steps + 1,
				Reason: "this acceptance's own proposal was aborted, so it closed its " +
					"generation and refuses rather than minting a replacement",
			}, nil

		case mqttOwnershipRecoverForeign:
			// BOUND TO ONE TARGET. An acceptance free to chase whatever foreign proposal it
			// finds on each reload would abort a succession of them, which is an abort war
			// rather than a recovery.
			if bound == nil {
				bound = d.Recovery
			} else if d.Recovery != nil && !mqttSameForeignTarget(bound, d.Recovery) {
				return mqttOwnershipResult{
					Outcome: mqttOwnershipRefused, Steps: steps,
					Reason: "the ownership protocol was asked to settle a SECOND foreign " +
						"proposal; one acceptance settles one, and chasing a succession of " +
						"them is an abort war rather than a recovery",
				}, nil
			}
		}

		next, err := exec(mqttOwnershipStep{
			Action: d.Action, Recovery: d.Recovery, Reason: d.Reason, Remaining: remaining,
		})
		if err != nil {
			return mqttOwnershipResult{}, err
		}
		in = next
	}
}

// mqttSameForeignTarget reports whether two recoveries name the same proposal.
//
// Compared by the PROPOSAL's identity — its kind and id — rather than by pointer, because each
// reload produces a fresh struct describing the same durable thing. And by the proposal rather
// than by the authority sequence, because the sequence moves whenever anything touches the
// authority while the proposal being settled has not changed.
//
// The KIND is part of the identity and not decoration: a creation and a handoff are different
// proposals and a recovery bound to one must not silently retarget onto the other because their
// ids happen to coincide.
func mqttSameForeignTarget(a, b *mqttForeignRecovery) bool {
	if a == nil || b == nil {
		return false
	}
	return a.Proposal.Kind == b.Proposal.Kind && a.Proposal.ID == b.Proposal.ID
}
