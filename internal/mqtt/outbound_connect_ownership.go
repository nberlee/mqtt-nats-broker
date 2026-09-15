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

// What a CONNECT must do about outbound ownership (F-105).
//
// This is the decision CONNECT makes before it composes anything: given what the authority
// record currently says, which transition does this acceptance run first? Kept as a pure
// function of the LOAD so it can be exercised over every state without a broker, because the
// states it must not confuse — a reservation somebody else is holding, a preparation nobody
// finalized, a closed generation — are exactly the ones a live fixture reaches rarely and by
// accident.
//
// Two rules it exists to enforce, both of which were available to get wrong:
//
//   - AN UNREADABLE AUTHORITY IS NOT AN ABSENT ONE. Corrupt and indeterminate refuse the
//     CONNECT. Treating either as "no authority" would let a first-creation reservation be
//     taken over a record this broker simply could not read, and the reservation is
//     create-only precisely so that exactly one claimant holds it.
//   - ABSENT IS NOT "MINE TO TAKE" EITHER, it is only "no ownership is recorded". That
//     licenses a RESERVATION, which serves nothing, and never a serving authority: the
//     metadata half has not committed, and nothing may be owed to a client whose ownership
//     is not yet established.
//
// NOT WIRED. mqttProcessConnect does not call this yet; the integration that does must also
// convert the outbound send, acknowledgement, recovery and teardown routes, because an
// authority respected by only some paths cannot support a generation-wide closure claim.

// mqttConnectOwnershipAction is what a CONNECT must run first.
type mqttConnectOwnershipAction uint8

const (
	// mqttOwnershipRefuse: this CONNECT cannot proceed. The authority could not be read,
	// or it is in a state this acceptance may not displace.
	mqttOwnershipRefuse mqttConnectOwnershipAction = iota + 1
	// mqttOwnershipReserve: no ownership is recorded, so claim it create-only. Grants no
	// serving permission.
	mqttOwnershipReserve
	// mqttOwnershipFinalizeCreation: this broker's own reservation stands and its metadata
	// half has committed, so make it servable.
	mqttOwnershipFinalizeCreation
	// mqttOwnershipPrepareHandoff: a serving authority stands, so reserve the handoff from
	// it before committing metadata.
	mqttOwnershipPrepareHandoff
	// mqttOwnershipFinalizeHandoff: this broker's own preparation stands and its metadata
	// half has committed, so install the proposed tenure.
	mqttOwnershipFinalizeHandoff
	// mqttOwnershipCancelHandoff: a preparation stands whose metadata half was positively
	// ABORTED, so restore the predecessor's ownership.
	mqttOwnershipCancelHandoff
	// mqttOwnershipCloseAbandoned: a reservation of THIS acceptance was positively
	// aborted, so close its generation. Closing is the recoverable abandonment — the
	// reservation is never deleted — and it is a separate step from claiming again,
	// because a claim requires an absent or a CLOSED predecessor and an aborted reservation
	// is not yet one.
	//
	// THIS ACCEPTANCE LOST, AND STOPS HERE. Having closed its own aborted proposal it
	// refuses the CONNECT; the client's next CONNECT is a fresh acceptance that decides
	// again from the closed authority. An acceptance that minted a replacement proposal
	// after its own was aborted would be an internal retry loop racing whichever broker
	// aborted it — the abort war the one-foreign-target bound exists to prevent, reached
	// through its own proposal instead of somebody else's.
	//
	// The decision function cannot enforce this: it is stateless and cannot tell a fresh
	// acceptance from the loser re-entering. The caller owes it, and it is recorded in
	// docs/ARCHITECTURE.md as a constraint on the integration rather than left implicit.
	mqttOwnershipCloseAbandoned
	// mqttOwnershipRecoverForeign: a proposal ANOTHER claimant made stands, and this
	// acceptance may settle it. Settling is not a liveness claim and never a deletion: the
	// recoverer takes the proposal's permanent metadata slot create-only with an ABORTED
	// verdict, which the original claimant's own commit is racing for. Whoever loses is
	// TOLD it lost rather than discovering it by absence, and a live claimant may
	// legitimately lose.
	//
	// Without it a claimant that died between writing its create-only reservation and
	// resolving its metadata half leaves the client permanently unservable: the reservation
	// is never deleted — deleting it would make the next claimant's create-only write
	// succeed against an absence — so nothing else can ever move.
	mqttOwnershipRecoverForeign
	// mqttOwnershipReserveOverClosed: the generation is closed, so reserve a FRESH
	// proposal over it. Deliberately a reservation and not a direct readmission: a
	// readmission installs SERVING outright, with no frozen proposal and no metadata half,
	// which would reintroduce the bootstrap gap on the second attempt.
	mqttOwnershipReserveOverClosed
)

// mqttConnectOwnershipInput is what the decision is made from.
type mqttConnectOwnershipInput struct {
	// Load is the authority as read, with its status. The status is consulted BEFORE the
	// record, because a record this broker cannot interpret says nothing about ownership.
	Load mqttAuthorityLoad

	// IntentID names the reservation or handoff this acceptance is resuming, when it is
	// resuming one. Empty on a first attempt.
	IntentID string
	// Metadata is the verdict of that intent's metadata half, and is consulted ONLY when
	// IntentID names something. Unresolved is not permission to proceed and not permission
	// to cancel: the batch may still be in flight.
	Metadata mqttMetadataVerdict

	// Recovering is the ONE foreign proposal this acceptance has already bound itself to
	// settling, if any. It is deliberately SEPARATE from IntentID: the acceptance's own
	// proposal and the proposal it is helping settle are different objects, and an
	// acceptance that conflated them would finalize somebody else's ownership as its own.
	//
	// It exists to bound the recovery to one target. An acceptance free to chase whatever
	// foreign proposal it finds on each reload would abort a succession of them, which is
	// an abort war rather than a recovery.
	Recovering *mqttForeignRecovery
}

// mqttForeignRecovery is one foreign proposal an acceptance is settling.
//
// It carries the VALIDATED PROPOSAL and not its id. An id names a subject and says nothing
// about what stands there, so a recovery driven by one could abort a slot belonging to a
// proposal nobody read — and the abort is permanent.
type mqttForeignRecovery struct {
	// Proposal is the standing proposal, as read from the authority record.
	Proposal mqttOwnershipProposal
	// AuthSeq is the sequence the authority carrying it was read at. Every authority
	// mutation of this recovery CASes against it, so a recovery composed against ownership
	// that has since moved loses rather than retargeting.
	AuthSeq uint64
	// AuthState is the state the authority stood in. It selects the continuation: a
	// reservation and a preparation abandon differently, and the difference is not cosmetic
	// — a cancelled preparation RESTORES its predecessor and an abandoned reservation
	// closes its generation.
	AuthState mqttAuthorityState
}

// mqttConnectOwnershipDecision is what a CONNECT must run first, and why.
type mqttConnectOwnershipDecision struct {
	Action mqttConnectOwnershipAction
	// Recovery is set only for mqttOwnershipRecoverForeign, and names what to settle.
	Recovery *mqttForeignRecovery
	// Reason is returned rather than logged so a caller can put it in a refusal.
	Reason string
}

// mqttDecideConnectOwnership returns the transition a CONNECT must run first, and why.
//
// The reason string is returned rather than logged so a caller can put it in a refusal: a
// CONNECT refused for an ownership reason should say which one, because the alternatives —
// "somebody else is mid-claim", "a handoff nobody finished", "this record is unreadable" —
// need different operator responses.
func mqttDecideConnectOwnership(in mqttConnectOwnershipInput) mqttConnectOwnershipDecision {
	refuse := func(why string) mqttConnectOwnershipDecision {
		return mqttConnectOwnershipDecision{Action: mqttOwnershipRefuse, Reason: why}
	}
	act := func(a mqttConnectOwnershipAction, why string) mqttConnectOwnershipDecision {
		return mqttConnectOwnershipDecision{Action: a, Reason: why}
	}
	switch in.Load.status {
	case mqttOutboundCorrupt:
		return refuse("the outbound authority is unreadable; an unreadable " +
			"record is not an absent one, and claiming over it would take a reservation " +
			"another broker may hold")
	case mqttOutboundIndeterminate:
		return refuse("the outbound authority could not be loaded; retrying " +
			"is the caller's business, but proceeding is not")
	case mqttOutboundAbsent:
		// No ownership is recorded. That licenses a RESERVATION and nothing stronger.
		return act(mqttOwnershipReserve, "no outbound ownership is recorded for this client")
	case mqttOutboundValid:
	default:
		return refuse("the outbound authority load reported an unsupported status")
	}

	a := in.Load.rec
	switch a.State {
	case mqttAuthorityServing:
		// AND THIS IS ALSO WHERE A HELPER LANDS after finalizing somebody else's committed
		// proposal on its behalf. Performing that finalization installs the proposal's OWN
		// tenure, not the helper's: the helper then takes an ordinary handoff from it like
		// any other acceptance, because nothing about having pushed a stalled proposal over
		// the line entitles it to serve under the tenure it installed.
		return act(mqttOwnershipPrepareHandoff, "a serving tenure holds this client")

	case mqttAuthorityClosed:
		return act(mqttOwnershipReserveOverClosed, "this generation is permanently closed, so a "+
			"successor is reserved over it rather than served outright")

	case mqttAuthorityReserving:
		// Somebody's first-creation reservation stands. Only its OWN claimant may finalize
		// it, and only once the metadata half has committed.
		if a.Creation == nil {
			return refuse("a reserving authority carries no proposal")
		}
		if in.IntentID != a.Creation.ID {
			p, perr := mqttProposalFromCreation(a.Creation)
			if perr != nil {
				return refuse("another claimant's first-creation reservation carries a " +
					"proposal this broker cannot act on: " + perr.Error())
			}
			return mqttDecideForeignRecovery(in, p, a.State)
		}
		switch in.Metadata {
		case mqttMetadataCommitted:
			return act(mqttOwnershipFinalizeCreation, "this broker's reservation committed its metadata half")
		case mqttMetadataAborted:
			// A creation whose metadata half was aborted is abandoned by CLOSING its
			// generation, never by deleting the reservation: deleting it would make the
			// next claimant's create-only write succeed against an absence. Closing is the
			// FIRST step and not the whole answer — claiming again is a separate
			// reservation over the resulting closure.
			return act(mqttOwnershipCloseAbandoned, "this broker's reservation was positively aborted")
		default:
			return refuse("this reservation's metadata half is unresolved; the " +
				"batch may still be in flight, so neither finalizing nor abandoning is licensed")
		}

	case mqttAuthorityPreparing:
		if a.Intent == nil {
			return refuse("a preparing authority carries no handoff intent")
		}
		if in.IntentID != a.Intent.ID {
			p, perr := mqttProposalFromHandoff(a.Intent)
			if perr != nil {
				return refuse("another acceptance's pending handoff carries an intent this " +
					"broker cannot act on: " + perr.Error())
			}
			return mqttDecideForeignRecovery(in, p, a.State)
		}
		switch in.Metadata {
		case mqttMetadataCommitted:
			return act(mqttOwnershipFinalizeHandoff, "this broker's handoff committed its metadata half")
		case mqttMetadataAborted:
			return act(mqttOwnershipCancelHandoff, "this broker's handoff was positively aborted")
		default:
			return refuse("this handoff's metadata half is unresolved; the batch " +
				"may still be in flight, so neither finalizing nor cancelling is licensed")
		}
	}
	return refuse(fmt.Sprintf("the outbound authority carries an unsupported state %d", a.State))
}

// mqttDecideForeignRecovery is what to do about a proposal this acceptance did not make.
//
// One acceptance settles AT MOST ONE foreign proposal. Having settled it, it proceeds by the
// ordinary rules; it does not go looking for the next one. Without that bound a reconnect
// storm becomes an abort war, in which each acceptance aborts the proposal the previous one
// had just made and none of them ever serves.
func mqttDecideForeignRecovery(in mqttConnectOwnershipInput, p mqttOwnershipProposal,
	state mqttAuthorityState) mqttConnectOwnershipDecision {

	if in.Recovering != nil && in.Recovering.Proposal.ID != p.ID {
		return mqttConnectOwnershipDecision{
			Action: mqttOwnershipRefuse,
			Reason: "this acceptance is already settling proposal " + in.Recovering.Proposal.ID +
				" and the authority now carries " + p.ID + "; chasing a second foreign proposal " +
				"is an abort war, not a recovery",
		}
	}
	return mqttConnectOwnershipDecision{
		Action: mqttOwnershipRecoverForeign,
		Recovery: &mqttForeignRecovery{
			Proposal: p, AuthSeq: in.Load.seq, AuthState: state,
		},
		Reason: "another claimant's " + string(p.Kind) + " proposal stands unresolved; this " +
			"acceptance settles it rather than refusing, because the proposal is never deleted " +
			"and nothing else can move until its metadata half has a permanent verdict",
	}
}

// mqttForeignRecoveryContinuation is what a settled foreign proposal licenses.
//
// The verdict comes from the proposal's permanent slot AFTER the settlement attempt, never
// from whether the abort itself succeeded: a failed abort establishes only that the slot is
// occupied, and by what is a question only a validated readback answers.
//
// Two things this deliberately does NOT do. A cancelled preparation is RESTORED to its
// predecessor and not closed — the predecessor's own generation was never displaced, and
// closing it because somebody else's handoff failed would destroy a session nothing retired.
// And a committed proposal is finalized EXACTLY as proposed; the finalization is not an
// opportunity to install something else.
func mqttForeignRecoveryContinuation(r *mqttForeignRecovery, v mqttMetadataVerdict) (mqttConnectOwnershipAction, string) {
	if r == nil {
		return mqttOwnershipRefuse, "no foreign recovery target was named"
	}
	switch v {
	case mqttMetadataCommitted:
		// IT WON. The proposal's own generation and tenure are installed, on its behalf.
		switch r.AuthState {
		case mqttAuthorityReserving:
			return mqttOwnershipFinalizeCreation, "the foreign reservation's metadata half had " +
				"committed, so it is finalized as proposed; this acceptance takes its own " +
				"tenure by an ordinary handoff afterwards, never by having finalized it"
		case mqttAuthorityPreparing:
			return mqttOwnershipFinalizeHandoff, "the foreign handoff's metadata half had " +
				"committed, so it is finalized as proposed; this acceptance takes its own " +
				"tenure by an ordinary handoff afterwards, never by having finalized it"
		}
		return mqttOwnershipRefuse, "a committed foreign proposal stood under an authority state " +
			"that carries no proposal to finalize"
	case mqttMetadataAborted:
		switch r.AuthState {
		case mqttAuthorityReserving:
			return mqttOwnershipCloseAbandoned, "the foreign reservation was settled as aborted, " +
				"so its generation is closed; the reservation itself is never deleted"
		case mqttAuthorityPreparing:
			return mqttOwnershipCancelHandoff, "the foreign handoff was settled as aborted, so " +
				"its predecessor's ownership is restored; a failed handoff retires nothing"
		}
		return mqttOwnershipRefuse, "an aborted foreign proposal stood under an authority state " +
			"that carries no proposal to abandon"
	case mqttMetadataForeign:
		return mqttOwnershipRefuse, "the receipt standing on this proposal's slot does not answer " +
			"it; a contradiction licenses no transition"
	}
	return mqttOwnershipRefuse, "the foreign proposal's metadata half is still unresolved after " +
		"the settlement attempt; the batch may yet commit, so nothing is licensed"
}

// mqttOwnershipAdmitsDelivery reports whether an authority in this state may have NEW
// outbound obligations admitted under it.
//
// Exactly one state does. Kept as its own function because the question is asked in more
// than one place and the answer must not drift: a second reading of "can this serve" that
// admitted a reservation or a preparation would be an admission before the metadata half
// committed, or during a frozen handoff.
func mqttOwnershipAdmitsDelivery(a *mqttOutboundAuthority) bool {
	return a != nil && a.State == mqttAuthorityServing
}
