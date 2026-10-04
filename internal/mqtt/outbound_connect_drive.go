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
	"errors"
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
//
// It carries no deadline of its own. The protocol's one deadline is a property of the assistant
// view the executor runs every request on (outbound_connect_deadline.go): a per-step duration
// would let a step of N requests spend N times the budget.
type mqttOwnershipStep struct {
	Action mqttConnectOwnershipAction
	// Recovery is set when the step acts on a proposal this acceptance did not make in this
	// drive: the settlement itself (RecoverForeign), or the continuation its verdict licensed.
	Recovery *mqttForeignRecovery
	// Reason is the decision's own explanation, carried so a refusal can say which
	// ownership situation it hit rather than that one occurred.
	Reason string
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
	// mqttOwnershipSettled: a settle-only run left no proposal standing. It made none of its
	// own, so it is NOT servable; it says only that what stood has been decided.
	mqttOwnershipSettled
)

// mqttOwnObservation is everything one drive has ESTABLISHED about its own proposal.
//
// The executor writes it at the point each fact is established — the reservation or
// preparation landing, a permanent verdict resolved from the proposal's slot — and the driver
// reads it after EVERY executor return, error or not. That is the point: a verdict established
// inside a step that then fails (a cleanup timing out after the slot read ABORTED, an adoption
// readback timing out after it read COMMITTED) never reaches the driver through the step's
// input, and disposing that run as UNRESOLVED would tell the caller nothing is known about a
// proposal whose fate is in fact permanent.
type mqttOwnObservation struct {
	// Own is this drive's proposal, once its reservation or preparation landed.
	Own *mqttOwnershipProposal
	// OwnState is the authority state it was proposed in: RESERVING or PREPARING, which is
	// what decides how an abandoned one is undone (closed, or cancelled back).
	OwnState mqttAuthorityState
	// Verdict is the own proposal's permanent verdict once one was read. It never regresses:
	// COMMITTED and ABORTED are slot facts.
	Verdict mqttMetadataVerdict
	// AbandonConfirmed reports that the own proposal's abandonment (close or cancel) landed.
	AbandonConfirmed bool
	// SelfAborted reports that THIS acceptance's own create-only abort write for this proposal
	// positively succeeded — the only proof that an ABORTED verdict is its own settlement of a
	// lost record race rather than another party's abort. Never inferred from a read.
	SelfAborted bool
}

// forbidsRedrive reports whether this drive's own proposal forbids the CONNECT from driving
// again with a fresh proposal (F-105 8a, consult 120). It is decided from what the drive
// ESTABLISHED, never from the identity of whatever error a handler returned: a re-drive is
// licensed only when no own proposal may have landed, when the own proposal COMMITTED (the
// next drive finalizes it exactly as proposed), or when this acceptance PROVABLY aborted it
// itself and its abandonment confirmed (re-drive arbitration against a settled authority,
// consult 10). Every other own proposal — aborted by another party, unresolved, foreign, or
// abandoned without confirmation — belongs to an acceptance that lost or cannot show it did
// not, and a CONNECT in that state refuses. An unknown verdict stays unknown: refusing does not
// license abandoning it.
func (o mqttOwnObservation) forbidsRedrive() bool {
	return o.Own != nil && o.Verdict != mqttMetadataCommitted && !(o.SelfAborted && o.AbandonConfirmed)
}

// noteVerdict records a permanent verdict for the own proposal. Unresolved records nothing.
func (o *mqttOwnObservation) noteVerdict(v mqttMetadataVerdict) {
	if o == nil || v == mqttMetadataUnresolved || o.Verdict != mqttMetadataUnresolved {
		return
	}
	o.Verdict = v
}

// noteOwn records the proposal this drive made, in the authority state it was made in.
func (o *mqttOwnObservation) noteOwn(p mqttOwnershipProposal, state mqttAuthorityState) {
	if o == nil {
		return
	}
	o.Own, o.OwnState = &p, state
}

// mqttConnectOwnershipState is what ONE CONNECT carries across every drive and re-drive.
//
// Declared above the CONNECT's re-entry point, because both of its facts are bounds on the
// whole CONNECT, and a re-drive that reset them would be a fresh acceptance in all but name.
type mqttConnectOwnershipState struct {
	// ownIntents is every intent id any drive of this CONNECT proposed. A proposal one drive
	// left standing is this connection's own residue to the next, not a rival.
	ownIntents map[string]struct{}
	// foreignBound is the ONE foreign proposal this CONNECT settles, across every drive.
	foreignBound *mqttForeignRecovery
}

func newMQTTConnectOwnershipState() *mqttConnectOwnershipState {
	return &mqttConnectOwnershipState{ownIntents: make(map[string]struct{})}
}

// mqttOwnershipResult is the whole protocol's answer.
type mqttOwnershipResult struct {
	Outcome mqttOwnershipOutcome
	// Disposition is set only for mqttOwnershipExpired, and is what the expiry means for
	// the PROPOSAL as distinct from this connection.
	Disposition mqttExpiryDisposition
	// Obs is what the run established about its own proposal, on EVERY outcome and on an
	// error return too: a refusal carrying own-ABORTED evidence owes an abandonment whatever
	// path the run ended by.
	Obs mqttOwnObservation
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
// prepare then finalize, with at most one foreign settlement ahead of them. A run that needs
// many more is not making progress, it is contending, and stopping it early is kinder to
// everything else contending with it.
const mqttOwnershipMaxSteps = 8

// mqttOwnershipStepReattempts is the per-CONNECT allowance of authority-step RE-ATTEMPTS
// (reconcileOwnershipStep): shared by every step of every drive and re-drive of one CONNECT, so
// the work one CONNECT may do is mqttOwnershipMaxSteps transitions per drive plus at most this
// many re-attempts overall. A re-attempt is admitted only after the previous attempt is proven
// never to have landed, so each one replaces a transition that did not happen rather than
// adding one beside it.
const mqttOwnershipStepReattempts = 4

// mqttDriveOwnership carries the ownership protocol to a conclusion for a single, stand-alone
// run: its own CONNECT state and its own observation. See mqttDriveOwnershipRun.
func mqttDriveOwnership(in mqttConnectOwnershipInput, ownTenure string,
	b *mqttOwnershipBudget, now func() time.Time,
	exec mqttOwnershipExecutor) (mqttOwnershipResult, error) {
	return mqttDriveOwnershipRun(in, ownTenure, b, now, exec, newMQTTConnectOwnershipState(), nil, false)
}

// mqttDriveOwnershipRun carries the ownership protocol to a conclusion.
//
// st is the CONNECT's state across drives, obs the drive's own observation (which the
// executor writes). settleOnly runs the SETTLEMENT of whatever proposal stands and nothing
// else: it never reserves or prepares, and it ends as soon as no proposal stands.
func mqttDriveOwnershipRun(in mqttConnectOwnershipInput, ownTenure string,
	b *mqttOwnershipBudget, now func() time.Time, exec mqttOwnershipExecutor,
	st *mqttConnectOwnershipState, obs *mqttOwnObservation, settleOnly bool) (mqttOwnershipResult, error) {

	if obs == nil {
		obs = &mqttOwnObservation{}
	}
	if st == nil {
		st = newMQTTConnectOwnershipState()
	}
	if exec == nil {
		return mqttOwnershipResult{}, fmt.Errorf("driving ownership requires an executor")
	}
	if ownTenure == _EMPTY_ && !settleOnly {
		// WITHOUT ITS OWN TENURE the driver cannot tell "somebody is serving" from "I am
		// serving", which is the whole of finding 1. Refusing is the only safe answer: the
		// alternative is a run that returns servable for somebody else's ownership.
		return mqttOwnershipResult{}, fmt.Errorf("driving ownership requires the tenure this " +
			"acceptance installs, or it cannot tell its own ownership from anyone else's")
	}
	if now == nil {
		return mqttOwnershipResult{}, fmt.Errorf("driving ownership requires a clock")
	}

	// THE PROPOSAL THIS DRIVE IS SETTLING. Foreign or an earlier drive's own; the CONNECT-wide
	// foreign bound is checked separately, on st.
	var current *mqttForeignRecovery
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

	// THE VERDICTS ARE LATCHED FROM EVERYTHING THE RUN HAS ESTABLISHED: the read, and the
	// observation the executor wrote — including inside a step that then failed.
	latch := func() {
		if in.Metadata == mqttMetadataCommitted || obs.Verdict == mqttMetadataCommitted {
			ownProposalCommitted = true
		}
		if (in.Metadata == mqttMetadataAborted && in.IntentID != _EMPTY_) || obs.Verdict == mqttMetadataAborted {
			ownProposalAborted = true
		}
	}
	expired := func(steps int, why string) (mqttOwnershipResult, error) {
		state := mqttProposalUnresolved
		switch {
		case ownProposalAborted:
			state = mqttProposalAborted
		case ownProposalCommitted:
			state = mqttProposalCommitted
		}
		d, derr := mqttDisposeExpiredOwnership(state)
		if derr != nil {
			return mqttOwnershipResult{Obs: *obs}, derr
		}
		return mqttOwnershipResult{
			Outcome: mqttOwnershipExpired, Disposition: d, Obs: *obs, Steps: steps, Reason: why,
		}, nil
	}
	// run executes one step. An executor error at or past the deadline IS an expiry: the
	// step was cut off by the budget, and what the run had established by then — latched
	// from the observation the step wrote before failing — is what the expiry means.
	run := func(steps int, step mqttOwnershipStep) (bool, mqttOwnershipResult, error) {
		next, err := exec(step)
		latch()
		if err != nil && errors.Is(err, errMQTTOwnershipTargetMoved) && !b.expired(now()) {
			// THE NAMED PROPOSAL NO LONGER STANDS — another settler acted on it first. Nothing
			// of this step landed; decide again from the read the executor returned with it.
			// Bounded by mqttOwnershipMaxSteps and the one-target bound like any other step.
			in = next
			return false, mqttOwnershipResult{}, nil
		}
		if err != nil {
			if errors.Is(err, errMQTTOwnershipDeadline) || b.expired(now()) {
				r, rerr := expired(steps+1, err.Error())
				return true, r, rerr
			}
			return true, mqttOwnershipResult{Obs: *obs, Steps: steps + 1}, err
		}
		in = next
		return false, mqttOwnershipResult{}, nil
	}

	for steps := 0; ; steps++ {
		if steps >= mqttOwnershipMaxSteps {
			return mqttOwnershipResult{
				Outcome: mqttOwnershipRefused, Obs: *obs, Steps: steps,
				Reason: fmt.Sprintf("the ownership protocol took %d steps without reaching a "+
					"servable authority: that is contention rather than progress, and a "+
					"connection is refused rather than spun", steps),
			}, nil
		}

		if settleOnly {
			// NOTHING IT MAY SETTLE STANDS. An authority that is absent, or serving or closed,
			// carries no proposal; one that is UNREADABLE (corrupt) names none this run could act
			// on, and refusing it is the ordinary protocol's business — the restore's and the
			// drive's own refusals, an identifier collision's included, still apply after this
			// run. An INDETERMINATE load is different: the store did not answer, so a proposal
			// may stand, and the decision below refuses it — within the deadline, before any
			// later phase spends its own bound on the same unanswering store. A settle-only run
			// never proposes.
			if in.Load.status == mqttOutboundAbsent || in.Load.status == mqttOutboundCorrupt ||
				(in.Load.status == mqttOutboundValid && in.Load.rec != nil &&
					in.Load.rec.State != mqttAuthorityReserving &&
					in.Load.rec.State != mqttAuthorityPreparing) {
				return mqttOwnershipResult{
					Outcome: mqttOwnershipSettled, Obs: *obs, Steps: steps,
					Reason: "no ownership proposal stands",
				}, nil
			}
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
		if !settleOnly && in.Load.status == mqttOutboundValid && mqttOwnershipAdmitsDelivery(in.Load.rec) &&
			in.Load.rec.Tenure == ownTenure {
			return mqttOwnershipResult{
				Outcome: mqttOwnershipServable, Obs: *obs, Steps: steps,
				Reason: "this acceptance's own tenure is serving",
			}, nil
		}

		// LATCH BEFORE THE BUDGET IS ASKED. A verdict first read on the reload that exhausts
		// the budget is the one the expiry must dispose; latching after the check lost it.
		latch()

		// ONE CLOCK READING PER ITERATION for the gate check.
		at := now()

		// THE BUDGET IS ASKED BEFORE EACH STEP, never after. A step admitted with nothing left
		// finishes after the client has already been failed.
		if err := mqttOwnershipStepAllowed(b, at, "ownership"); err != nil {
			return expired(steps, err.Error())
		}

		in.Recovering = current
		d := mqttDecideConnectOwnership(in)

		// ONCE THIS ACCEPTANCE HAS LOST, THE ONLY ACT LEFT TO IT IS ABANDONING ITS OWN PROPOSAL.
		//
		// The decision function is stateless and cannot tell a fresh acceptance from the loser
		// re-entering, so it keeps offering a way forward — and over an authority a peer has
		// ALREADY closed it offers ReserveOverClosed, which is a replacement proposal. That is
		// the abort war the bound exists to prevent, entered through the loser's own retry.
		//
		// Stated as a whitelist rather than a step count. An earlier version refused only from
		// the second iteration onward, which let exactly this case through on the first: the
		// abort is known from the READ, and nothing says the close is still owed. An own
		// aborted PREPARATION is not abandoned here: the executor's in-place cleanup cancels it,
		// and a cancellation that did not confirm is owed to the abandonment worker the caller
		// launches on the refusal (F-105 8a) — one path, never a second inline one.
		if ownProposalAborted && !(d.Action == mqttOwnershipCloseAbandoned && d.Recovery == nil) {
			return mqttOwnershipResult{
				Outcome: mqttOwnershipRefused, Obs: *obs, Steps: steps,
				Reason: "this acceptance's own proposal was aborted, so it refuses rather than " +
					"minting a replacement: an acceptance that proposed again after losing " +
					"would be an internal retry loop racing whichever broker aborted it",
			}, nil
		}

		switch d.Action {
		case mqttOwnershipRefuse:
			return mqttOwnershipResult{
				Outcome: mqttOwnershipRefused, Obs: *obs, Steps: steps, Reason: d.Reason,
			}, nil

		case mqttOwnershipReserve, mqttOwnershipReserveOverClosed, mqttOwnershipPrepareHandoff:
			if settleOnly {
				// Unreachable by the stop condition above, which ends a settle-only run before
				// the decision can offer a proposal; stated rather than assumed.
				return mqttOwnershipResult{
					Outcome: mqttOwnershipSettled, Obs: *obs, Steps: steps,
					Reason: "no ownership proposal stands",
				}, nil
			}

		case mqttOwnershipCloseAbandoned:
			if d.Recovery == nil {
				// THE LOSER CLOSES ITS GENERATION ONCE AND THEN REFUSES, which is why this arm
				// returns rather than looping: the decision function is stateless and would keep
				// returning this action, and a second close would be an internal retry loop
				// racing whichever broker aborted the proposal.
				//
				// The flag is NOT set here. It is latched from the READ above, so it already
				// protects the case this arm cannot — a peer aborting and closing the
				// reservation between two reads, where the decision returns something else.
				if done, r, err := run(steps, mqttOwnershipStep{Action: d.Action, Reason: d.Reason}); done {
					return r, err
				}
				obs.AbandonConfirmed = true
				return mqttOwnershipResult{
					Outcome: mqttOwnershipRefused, Obs: *obs, Steps: steps + 1,
					Reason: "this acceptance's own proposal was aborted, so it closed its " +
						"generation and refuses rather than minting a replacement",
				}, nil
			}
			// A FOREIGN reservation settled as aborted: closing it is the settlement's act, not
			// a defeat of this acceptance, which goes on to reserve over the closure.

		case mqttOwnershipRecoverForeign:
			// BOUND TO ONE TARGET PER CONNECT. An acceptance free to chase whatever foreign
			// proposal it finds on each reload would abort a succession of them, which is an
			// abort war rather than a recovery. An earlier drive's own residue spends no bound.
			if d.Recovery != nil && !d.Recovery.EarlierOwn {
				if st.foreignBound == nil {
					st.foreignBound = d.Recovery
				} else if !mqttSameForeignTarget(st.foreignBound, d.Recovery) {
					return mqttOwnershipResult{
						Outcome: mqttOwnershipRefused, Obs: *obs, Steps: steps,
						Reason: "the ownership protocol was asked to settle a SECOND foreign " +
							"proposal; one acceptance settles one, and chasing a succession of " +
							"them is an abort war rather than a recovery",
					}, nil
				}
			}
			current = d.Recovery
		}

		if done, r, err := run(steps, mqttOwnershipStep(d)); done {
			return r, err
		}
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
