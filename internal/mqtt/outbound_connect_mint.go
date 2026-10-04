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

// Production authority minting for an accepted CONNECT (F-105 cutover, row 9's minting half).
//
// mqttDriveOwnership decides which ownership transition an accepted CONNECT must run next;
// this is the production EXECUTOR it drives and the reader that turns each step's durable
// result back into the next decision's input. Together they carry a CONNECT from an absent (or
// closed) authority to a SERVING one via reserve -> commit-metadata -> finalize, exactly as
// executor-map.md lays out.
//
// # UNWIRED, deliberately
//
// Nothing calls mqttMintOutboundAuthority yet. Minting REPLACES the accept save
// (saveAcceptWithDetail) rather than sitting beside it, because opCommitMetadata persists the
// session record and the acceptance fact — the very writes the accept save makes — so running
// both would double-write them. That replacement is the coordinated cutover phase (the routes
// flip to consume the authority, activation last), and it reddens the accept suite while it is
// in flight. This file is the reusable core that phase consumes: the step->op mapping and the
// re-read, which are the same whether driven from a test or from the converted accept path.
//
// # Why the executor RE-READS after every step
//
// A step's durable result and a lost response to it are indistinguishable to the caller, and
// both are resolved the same way: read the authority again and let the next decision be made
// from what actually stands. So every arm ends by loading the authority into the next input
// rather than trusting the op's return.

// mqttOwnershipMetadataInputs is what the metadata-commit half of minting needs from the
// accepting CONNECT: the session body and acceptance fact opCommitMetadata persists. The flip
// supplies these from the accept context (the same values saveAcceptWithDetail built); a test
// supplies synthetic ones.
type mqttOwnershipMetadataInputs struct {
	DomainTk string
	// Record is the session body to install; opCommitMetadata overwrites its client,
	// generation and write identity from the proposal and SessWID.
	Record   *mqttPersistedSession
	SessWID  string
	AcceptID string
	AcceptAt int64
}

// mqttOwnIdentity is what one CONNECT knows about its OWN proposals, which is the only basis on
// which a standing proposal may be called its own (F-105 8a).
//
// The current proposal is the one this drive made, once its reservation or preparation landed;
// the earlier ones are what previous drives of the SAME CONNECT made before a re-drive. A
// proposal is never classified as own because the authority carries it: an authority carries
// whatever was left standing, and a crash-left or concurrent acceptance's proposal read as own
// is finalized under the wrong id, or makes this acceptance a loser it never was.
type mqttOwnIdentity struct {
	Current *mqttOwnershipProposal
	Earlier map[string]struct{}
}

// loadConnectOwnershipInput reads the standing authority and derives the next decision's input:
// the load; THIS acceptance's own proposal and its verdict, read from the proposal's own slot
// whatever the authority now carries; the verdict of any OTHER proposal the authority carries;
// and the one foreign proposal this acceptance has bound itself to settling.
func (jsa *mqttJSA) loadConnectOwnershipInput(id, epoch, domainTk string, own mqttOwnIdentity,
	recovering *mqttForeignRecovery) mqttConnectOwnershipInput {
	in := mqttConnectOwnershipInput{Load: jsa.loadOutboundAuthority(id, epoch), Recovering: recovering}
	if own.Current != nil {
		// THE OWN VERDICT COMES FROM THE OWN SLOT. A peer that aborted this proposal and closed
		// the authority over it leaves CLOSED standing: reading the verdict off the authority
		// would lose the abort, and the decision would offer a replacement reservation — the
		// loser bound broken by the run it exists to stop.
		in.IntentID = own.Current.ID
		in.Metadata = jsa.resolveMetadataAttempt(domainTk, id, epoch, *own.Current)
	}
	a := in.Load.rec
	if in.Load.status != mqttOutboundValid || a == nil {
		return in
	}
	var standing *mqttOwnershipProposal
	switch {
	case a.State == mqttAuthorityReserving && a.Creation != nil:
		if p, err := mqttProposalFromCreation(a.Creation); err == nil {
			standing = &p
		}
	case a.State == mqttAuthorityPreparing && a.Intent != nil:
		if p, err := mqttProposalFromHandoff(a.Intent); err == nil {
			standing = &p
		}
	}
	if standing == nil || (own.Current != nil && standing.ID == own.Current.ID) {
		return in
	}
	_, in.StandingEarlierOwn = own.Earlier[standing.ID]
	in.StandingVerdict = jsa.resolveMetadataAttempt(domainTk, id, epoch, *standing)
	return in
}

// newCreationOwnershipExecutor returns the executor that mints a FRESH serving authority for a
// client with no live ownership (an absent or CLOSED predecessor). ownGen and ownTenure are
// this acceptance's, minted by the caller; intentID names the reservation this run installs and
// then finalizes, so every step of the run refers to the one proposal.
//
// The handoff arms (a resume displacing a live serving authority) are NOT implemented here:
// they need the handoff intent derived from the standing serving authority and the retirement
// request, which is the resume half of the flip. This executor refuses them explicitly rather
// than mint a malformed handoff.
func (jsa *mqttJSA) newCreationOwnershipExecutor(id, epoch, ownGen, ownTenure, intentID string,
	meta mqttOwnershipMetadataInputs) mqttOwnershipExecutor {
	var own *mqttOwnershipProposal
	return func(step mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		txn := mqttNewInflightNonce()
		switch step.Action {
		case mqttOwnershipReserve, mqttOwnershipReserveOverClosed:
			own = &mqttOwnershipProposal{Kind: mqttProposalCreation, ID: intentID, Gen: ownGen, Tenure: ownTenure}
			// Reserve the client's ownership (create-only over an absent or CLOSED
			// predecessor), then commit the metadata half the CONNECT owes between reserving
			// and finalizing — the record and the acceptance fact land here, once.
			if _, err := jsa.opClaimClient(id, epoch, txn, ownGen, ownTenure, intentID); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
				ID: id, Epoch: epoch, DomainTk: meta.DomainTk, IntentID: intentID,
				SessWID: meta.SessWID, Record: meta.Record,
				AcceptID: meta.AcceptID, AcceptAt: meta.AcceptAt,
			}); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipFinalizeCreation:
			// The reservation's metadata half stands committed; make it servable. The
			// intent is READ from the standing reservation, never re-supplied.
			if _, err := jsa.opFinalizeClaim(id, epoch, txn, intentID); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipCloseAbandoned:
			// This acceptance's own reservation was positively aborted: close its
			// generation (the recoverable abandonment) and let the driver refuse — the
			// loser bound. The close names the RESERVATION'S OWN owning transition, read
			// from the standing authority, not this step's fresh nonce: opClaimClient stamped
			// OwnerTxn with the reserve call's own txn, which no later call reconstructs.
			al := jsa.loadOutboundAuthority(id, epoch)
			if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityReserving {
				return mqttConnectOwnershipInput{}, fmt.Errorf("client %q carries no standing "+
					"reservation to close for an abandoned proposal", id)
			}
			if _, err := jsa.opCloseGeneration(id, epoch, txn, al.rec.Gen, al.rec.Tenure, al.rec.OwnerTxn); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipRecoverForeign:
			// Settle the one foreign proposal this acceptance bound itself to, by taking its
			// permanent metadata slot create-only with an ABORTED verdict.
			if step.Recovery == nil {
				return mqttConnectOwnershipInput{}, fmt.Errorf("a foreign-recovery step carried no proposal")
			}
			// The abort's error is never the verdict; the reload reads the slot (F-105 8a).
			jsa.settleProposal(meta.DomainTk, id, epoch, step.Recovery.Proposal)
		case mqttOwnershipPrepareHandoff, mqttOwnershipFinalizeHandoff, mqttOwnershipCancelHandoff:
			return mqttConnectOwnershipInput{}, fmt.Errorf("minting a fresh authority reached a "+
				"handoff step %v: the resume/handoff path is not wired in this executor", step.Action)
		case mqttOwnershipRefuse:
			return mqttConnectOwnershipInput{}, fmt.Errorf("the executor was handed a refuse step, " +
				"which the driver resolves itself")
		default:
			return mqttConnectOwnershipInput{}, fmt.Errorf("unsupported ownership step %v", step.Action)
		}
		return jsa.loadConnectOwnershipInput(id, epoch, meta.DomainTk, mqttOwnIdentity{Current: own}, step.Recovery), nil
	}
}

// mqttMintOutboundAuthority drives an accepted CONNECT to a serving authority, minting a fresh
// generation and tenure for a client with no live ownership. UNWIRED — the coordinated accept
// flip calls this in place of saveAcceptWithDetail, threading the same record and acceptance
// fact into meta and the remaining budget from F-051's stop.
//
// It returns the serving authority's generation and tenure (which the shared-lifecycle tenure
// and the session's own bookkeeping then adopt), or the ownership result's refusal/expiry.
func (jsa *mqttJSA) mintOutboundAuthority(id, epoch string, meta mqttOwnershipMetadataInputs,
	budget *mqttOwnershipBudget, now func() time.Time) (gen, tenure string, res mqttOwnershipResult, err error) {
	gen = mqttNewInflightNonce()
	tenure = mqttNewInflightNonce()
	intentID := mqttNewInflightNonce()
	exec := jsa.newCreationOwnershipExecutor(id, epoch, gen, tenure, intentID, meta)
	res, err = mqttDriveOwnership(
		jsa.loadConnectOwnershipInput(id, epoch, meta.DomainTk, mqttOwnIdentity{}, nil),
		tenure, budget, now, exec)
	return gen, tenure, res, err
}

// # The resume/handoff arm
//
// A CONNECT that meets a LIVE authority (serving, or a preparation/reservation still being
// resolved) does not mint a fresh one: it displaces the standing ownership through a HANDOFF.
// A RESUME preserves the outbound generation — two brokers may legitimately serve one
// generation, and only the record CAS decides the session — so the handoff installs a new
// tenure at the SAME generation. A CLEAN START mints a fresh generation and asks that the
// predecessor be retired.
//
// The executor uses the run's own intent for the PREPARE it issues, and the STANDING intent's
// identity for a FINALIZE or CANCEL, so it correctly finishes either its own handoff or a prior
// preparation the decision function drove it to resolve.

// standingProposalID returns the identity of whatever proposal the client's authority currently
// carries — a preparation's intent or a reservation's creation — so a finalize/cancel names the
// proposal that actually stands rather than one captured before it moved.
func (jsa *mqttJSA) standingProposalID(id, epoch string) (string, error) {
	load := jsa.loadOutboundAuthority(id, epoch)
	a := load.rec
	if load.status != mqttOutboundValid || a == nil {
		return _EMPTY_, fmt.Errorf("client %q carries no readable authority to finalize", id)
	}
	switch {
	case a.State == mqttAuthorityPreparing && a.Intent != nil:
		return a.Intent.ID, nil
	case a.State == mqttAuthorityReserving && a.Creation != nil:
		return a.Creation.ID, nil
	}
	return _EMPTY_, fmt.Errorf("client %q carries no standing proposal to finalize (state %d)", id, a.State)
}

// newHandoffOwnershipExecutor returns the executor that drives a CONNECT displacing a live
// authority to a serving one. intentID/hGen/hTenure/cleanReq/retire describe the handoff this
// run PROPOSES; a finalize or cancel reads the standing proposal so it also resolves a
// preparation a prior run left behind.
func (jsa *mqttJSA) newHandoffOwnershipExecutor(id, epoch, intentID, hGen, hTenure string,
	cleanReq bool, retire *mqttRetirementRequest, prevSessWID string, prevSessSeq uint64,
	meta mqttOwnershipMetadataInputs) mqttOwnershipExecutor {
	var own *mqttOwnershipProposal
	return func(step mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		txn := mqttNewInflightNonce()
		switch step.Action {
		case mqttOwnershipPrepareHandoff:
			// Reserve the handoff from the standing serving authority, then commit the
			// metadata half — the record and the acceptance fact land here, once. opPrepareHandoff
			// fills the Prev gen/tenure/ownerTxn (and the retire request's) from the serving
			// authority; the caller supplies the predecessor SESSION record's write identity and
			// sequence, which the authority does not carry and the metadata commit CASes against.
			intent := &mqttHandoffIntent{
				ID: intentID, Gen: hGen, Tenure: hTenure, CleanReq: cleanReq, Retire: retire,
				PrevSessWID: prevSessWID, PrevSessSeq: prevSessSeq,
			}
			if _, err := jsa.opPrepareHandoff(id, epoch, txn, intent); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			if p, perr := mqttProposalFromHandoff(intent); perr == nil {
				own = &p
			}
			if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
				ID: id, Epoch: epoch, DomainTk: meta.DomainTk, IntentID: intentID,
				SessWID: meta.SessWID, Record: meta.Record,
				AcceptID: meta.AcceptID, AcceptAt: meta.AcceptAt,
			}); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipFinalizeHandoff:
			sid, err := jsa.standingProposalID(id, epoch)
			if err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			if _, err := jsa.opFinalizeHandoff(id, epoch, txn, sid); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipCancelHandoff:
			sid, err := jsa.standingProposalID(id, epoch)
			if err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			if _, err := jsa.opCancelHandoff(id, epoch, txn, sid); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipRecoverForeign:
			if step.Recovery == nil {
				return mqttConnectOwnershipInput{}, fmt.Errorf("a foreign-recovery step carried no proposal")
			}
			// The abort's error is never the verdict; the reload reads the slot (F-105 8a).
			jsa.settleProposal(meta.DomainTk, id, epoch, step.Recovery.Proposal)
		case mqttOwnershipRefuse:
			return mqttConnectOwnershipInput{}, fmt.Errorf("the executor was handed a refuse step, " +
				"which the driver resolves itself")
		case mqttOwnershipReserve, mqttOwnershipReserveOverClosed, mqttOwnershipFinalizeCreation, mqttOwnershipCloseAbandoned:
			// A creation step reached the handoff executor: the live authority this run displaced
			// vanished under it (an absent/closed predecessor now stands). Refuse rather than mint
			// a fresh reservation from the resume path; the caller re-drives from the new state.
			return mqttConnectOwnershipInput{}, fmt.Errorf("the resume/handoff executor reached a creation "+
				"step %v: the authority it displaced no longer stands", step.Action)
		default:
			return mqttConnectOwnershipInput{}, fmt.Errorf("unsupported ownership step %v", step.Action)
		}
		return jsa.loadConnectOwnershipInput(id, epoch, meta.DomainTk, mqttOwnIdentity{Current: own}, step.Recovery), nil
	}
}

// mqttMintOrHandoffAuthority drives an accepted CONNECT to a SERVING authority for either case:
// a client with no live ownership (absent or CLOSED) is minted fresh via the creation path; a
// client whose authority stands live is displaced through a handoff. A RESUME preserves the
// outbound generation; a CLEAN START mints a fresh generation and asks that the predecessor be
// retired. It returns the serving generation and tenure, or the ownership result's refusal/expiry.
//
// UNWIRED — the coordinated accept flip calls this in place of saveAcceptWithDetail.
func (jsa *mqttJSA) mqttMintOrHandoffAuthority(id, epoch string, cleanStart bool,
	prevSessWID string, prevSessSeq uint64,
	meta mqttOwnershipMetadataInputs, budget *mqttOwnershipBudget, now func() time.Time) (gen, tenure string, res mqttOwnershipResult, err error) {
	load := jsa.loadOutboundAuthority(id, epoch)
	a := load.rec
	if load.status != mqttOutboundValid || a == nil || a.State == mqttAuthorityClosed {
		// No live ownership: mint a fresh serving authority.
		return jsa.mintOutboundAuthority(id, epoch, meta, budget, now)
	}
	// A live authority stands. Displace it through a handoff.
	tenure = mqttNewInflightNonce()
	intentID := mqttNewInflightNonce()
	var retire *mqttRetirementRequest
	if cleanStart {
		// A fresh generation; the predecessor is DISCARDED (clean start). opPrepareHandoff stamps
		// the retire request's predecessor GEN/TENURE/OWNER from the serving authority; the caller
		// supplies the predecessor SESSION identity, which must match the intent's. The full
		// backing-resource enumeration a retirement drives is row 8's (the teardown targets);
		// here the request names the predecessor generation and session to retire.
		gen = mqttNewInflightNonce()
		retire = &mqttRetirementRequest{
			SessWID: prevSessWID, SessSeq: prevSessSeq,
			Reason: mqttRetireCleanStart, Discard: true,
		}
	} else {
		// A resume preserves the generation the authority already serves.
		gen = a.Gen
	}
	exec := jsa.newHandoffOwnershipExecutor(id, epoch, intentID, gen, tenure, cleanStart, retire, prevSessWID, prevSessSeq, meta)
	res, err = mqttDriveOwnership(jsa.loadConnectOwnershipInput(id, epoch, meta.DomainTk, mqttOwnIdentity{}, nil), tenure, budget, now, exec)
	return gen, tenure, res, err
}
