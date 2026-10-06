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
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Driving an accepted CONNECT to a SERVING authority IN the accept transaction (F-105 cutover,
// row 9 / Codex consult-5 steps 3-4). This is mqttMintOrHandoffAuthority's session-context twin:
// where that one commits the metadata half through the standalone opCommitMetadata (its own
// batch), THIS one commits it through sess.saveAcceptWithOwnership — so the record, the
// acceptance fact AND the ownership receipt land through persistRecord's fence, carrying the
// four invariants (lineage, void accounting, ambiguous-write refusal, the record-tx fence).
//
// # The generation is the SESSION's, not a fresh nonce
//
// The metadata commit validates that the record it installs agrees with the proposal's
// generation (commitMetadataAttempt: record.Gen == proposal.Gen), so the outbound authority
// generation IS the session generation — es.gen for a resume (preserved), the freshly-minted
// session generation for a clean start. Minting a fresh unrelated nonce would be refused at the
// metadata commit. Every arm here therefore proposes sess.gen.

// mqttCleanStartRetireTargets carries a clean start's predecessor backing-resource names —
// captured from the resident session BEFORE it is reset — so the discarding handoff's retire
// request can enrol them for recovery in the closing batch. Empty where the predecessor holds
// none; the shared-group durables are deliberately absent, since a generation's shared
// acquisitions are released from its own ledger by the closing, never named by a retirement.
type mqttCleanStartRetireTargets struct {
	AffineDurable string
	AffineGen     string
	PubRelDurable string
}

// errMQTTDisplacedRecordUnproven refuses a clean start, before anything is detached, whose
// displaced record could not be proven: unreadable, or undecodable or naming another identity at
// the very sequence the object holds. Falling through to the discard there would let its claim
// WIN at that sequence and retire what the object's memory names.
var errMQTTDisplacedRecordUnproven = errors.New("the session record a clean start would displace could not be proven")

// displacedRetireTargets is the clean start's ONE retirement disposition (F-105 8b, P2;
// consult 123), run as the retirement's pre-detach proof from the discard boundary's CARRIED
// load. A clean start that hands off from a SERVING predecessor must name the targets of the
// record its accepted batch DISPLACES, never the object's memory; and a clean start must never
// take the discard arm where its memory-built marker could later be continued into a receipt
// (claimTeardownReceipt) for a generation that may still serve. So, defaulting to REFUSE:
//
//	no record (seq 0)                                     -> the discard arm (local resources)
//	load not read, indeterminate, corrupt, malformed      -> refuse
//	RESERVING or PREPARING, at ANY generation             -> refuse (a preparation can cancel
//	                                                         back to SERVING at this generation)
//	ABSENT, CLOSED at any generation, SERVING at another  -> the discard arm (no receipt can be
//	                                                         authored for this generation)
//	SERVING at this object's generation                   -> prove the record:
//	  no write identity                                   -> refuse
//	  unreadable; undecodable or another identity AT seq  -> refuse
//	  gone, or moved past seq                             -> the discard arm (its claim is
//	                                                         superseded; the re-drive runs)
//	  this object's record at seq                         -> hand off, naming ITS targets
//
// ok reports a handoff; an error refuses the CONNECT with the incumbent intact (the proof's
// unwind). The batch's CAS at the proven sequence is what makes the record read the displaced
// one. Other discards — disconnect, expiry, gone-retirement — mark from the owning
// incarnation's own state, which is theirs to snapshot; this disposition is the CONNECT's.
func (sess *mqttSession) displacedRetireTargets(p *mqttPreparedClear) (mqttCleanStartRetireTargets, bool, error) {
	sess.mu.Lock()
	wid, seq, gen, domainTk, idHash := sess.wid, sess.seq, sess.gen, sess.domainTk, sess.idHash
	sess.mu.Unlock()
	if seq == 0 {
		return mqttCleanStartRetireTargets{}, false, nil
	}
	al := p.auth
	switch {
	case !p.authRead:
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the discard boundary read no authority", errMQTTDisplacedRecordUnproven)
	case al.status == mqttOutboundAbsent:
		return mqttCleanStartRetireTargets{}, false, nil
	case al.status != mqttOutboundValid || al.rec == nil:
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the authority reads %v", errMQTTDisplacedRecordUnproven, al.status)
	}
	switch al.rec.State {
	case mqttAuthorityClosed:
		return mqttCleanStartRetireTargets{}, false, nil
	case mqttAuthorityServing:
		if al.rec.Gen != gen {
			return mqttCleanStartRetireTargets{}, false, nil
		}
	case mqttAuthorityReserving, mqttAuthorityPreparing:
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: an ownership proposal still stands (state %d)", errMQTTDisplacedRecordUnproven, al.rec.State)
	default:
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the authority is in state %d", errMQTTDisplacedRecordUnproven, al.rec.State)
	}
	if wid == _EMPTY_ {
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: the record at %d carries no write identity", errMQTTDisplacedRecordUnproven, seq)
	}
	smsg, lerr := sess.jsa.loadSessionMsg(domainTk, idHash)
	switch {
	case lerr != nil && isErrorOtherThan(lerr, JSNoMessageFoundErr):
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: %v", errMQTTDisplacedRecordUnproven, lerr)
	case lerr != nil || smsg == nil || smsg.Sequence != seq:
		return mqttCleanStartRetireTargets{}, false, nil
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: %v", errMQTTDisplacedRecordUnproven, uerr)
	}
	if ps.WID != wid || ps.Gen != gen || ps.Teardown != nil {
		return mqttCleanStartRetireTargets{}, false, fmt.Errorf("%w: at sequence %d it names write identity %q generation %q, not %q %q",
			errMQTTDisplacedRecordUnproven, seq, ps.WID, ps.Gen, wid, gen)
	}
	t := mqttTeardownTargetFromRecord(ps, idHash)
	return mqttCleanStartRetireTargets{AffineDurable: t.q2AffDur, AffineGen: t.q2AffGen, PubRelDurable: t.pubRelDur}, true, nil
}

// newAcceptOwnershipExecutorRun is the executor mqttDriveOwnershipRun drives for an accepting
// CONNECT. Its metadata step is sess.saveAcceptWithOwnership (fenced); every other step is the
// ordinary authority op. Every request goes through jsa, the run's deadline view. It re-reads
// the authority after each step, so a step's durable result and a lost response to it are
// resolved the same way, and it writes what it establishes about its own proposal into obs.
func (sess *mqttSession) newAcceptOwnershipExecutorRun(jsa *mqttJSA, st *mqttConnectOwnershipState,
	obs *mqttOwnObservation, tok recordTx, clientID, sessEpoch, gen, tenure, intentID string,
	cleanStart bool, retire *mqttRetirementRequest, prevSessWID string, prevSessSeq uint64,
	retryOK func() error, stepAllow func() error) mqttOwnershipExecutor {
	id := sess.id
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	domainTk := sess.domainTk
	// THE FORK FENCE, captured once for the whole drive: every preparation attempt this executor
	// makes — including a re-attempt after one that positively never landed — CASes the sequence
	// fork repair read under, never a fresher one (F-105 7f).
	sess.mu.Lock()
	fence := sess.forkFence
	sess.mu.Unlock()
	reconcile := func(op func(txn string) (mqttOpOutcome, error)) (string, error) {
		return mqttReconcileOwnershipStep(jsa, op, stepAllow)
	}
	load := func(rec *mqttForeignRecovery) mqttConnectOwnershipInput {
		return jsa.loadConnectOwnershipInput(id, epoch, domainTk,
			mqttOwnIdentity{Current: obs.Own, Earlier: st.ownIntents}, rec)
	}
	return func(step mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		if err := sess.noteOwnershipStepEntry(step.Action); err != nil {
			return mqttConnectOwnershipInput{}, err
		}
		switch step.Action {
		case mqttOwnershipReserve, mqttOwnershipReserveOverClosed:
			// Reserve the client's ownership create-only at the SESSION generation, then commit
			// the metadata half THROUGH the accept transaction — the record, the fact and the
			// COMMITTED receipt land as one fenced batch.
			proposal := mqttOwnershipProposal{Kind: mqttProposalCreation, ID: intentID, Gen: gen, Tenure: tenure}
			txn, err := reconcile(func(txn string) (mqttOpOutcome, error) {
				return jsa.opClaimClient(id, epoch, txn, gen, tenure, intentID)
			})
			if err != nil {
				// A reservation that MAY have landed is this acceptance's own proposal for every
				// decision that follows: the CONNECT may not propose again over it.
				if errors.Is(err, errMQTTOwnershipStepUnresolved) {
					obs.noteOwn(proposal, mqttAuthorityReserving)
				}
				return mqttConnectOwnershipInput{}, err
			}
			obs.noteOwn(proposal, mqttAuthorityReserving)
			if err := sess.noteOwnershipBeforeMetadata(step.Action); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			sess.noteOwnershipMetadataEntry()
			detail, err := sess.saveAcceptWithOwnership(jsa, tok, clientID, sessEpoch, proposal)
			if err != nil {
				// The reservation landed but the metadata half did not commit through the accept
				// batch. A same-subject CAS contention (10164) decided NOTHING — the three-member
				// batch is atomic, so a rejected send stored nothing — and the SAME frozen attempt
				// is re-sent within the shared budget (Codex consult 10). Any other outcome (a
				// rival moved the record, an ambiguous send) settles this attempt's own proposal so
				// a re-drive meets a SETTLED authority rather than our stalled RESERVING proposal.
				// The cleanup for the settled-loser branch closes the reservation's OWN generation
				// (the recoverable abandonment; the reservation is never deleted), naming the
				// reservation's OWN owning transition — the claim attempt that LANDED, which a
				// re-attempted claim makes different from the first nonce — so a moved ownership is
				// refused rather than closed.
				cleanup := func() error {
					_, cerr := reconcile(func(ctxn string) (mqttOpOutcome, error) {
						return jsa.opCloseGeneration(id, epoch, ctxn, gen, tenure, txn)
					})
					if cerr == nil {
						obs.AbandonConfirmed = true
					}
					return cerr
				}
				return sess.resolveOrRetryMetadataCommit(jsa, obs, proposal, detail, err, domainTk, step,
					retryOK, cleanup, load)
			}
			obs.noteVerdict(mqttMetadataCommitted)
		case mqttOwnershipPrepareHandoff:
			// Reserve the handoff from the standing serving authority; opPrepareHandoff fills the
			// intent's Prev gen/tenure/ownerTxn (and the retire request's) from it. Then read the
			// PREPARING authority back for the proposal it now carries — its Prev* fields are the
			// ones the metadata receipt must name — and commit the metadata half through the fence.
			intent := &mqttHandoffIntent{
				ID: intentID, Gen: gen, Tenure: tenure, CleanReq: cleanStart, Retire: retire,
				PrevSessWID: prevSessWID, PrevSessSeq: prevSessSeq,
			}
			if _, err := reconcile(func(txn string) (mqttOpOutcome, error) {
				return jsa.opPrepareHandoffFenced(id, epoch, txn, intent, fence)
			}); err != nil {
				// A preparation that MAY have landed is this acceptance's own proposal for every
				// decision that follows: the CONNECT may not propose again over it.
				// The operation fills the predecessor fields on its own copy, so the identity is
				// recorded by what this acceptance chose — enough to forbid proposing again; the
				// proposal itself is settled by whichever acceptance meets it standing.
				if errors.Is(err, errMQTTOwnershipStepUnresolved) {
					obs.noteOwn(mqttOwnershipProposal{Kind: mqttProposalHandoff, ID: intentID, Gen: gen, Tenure: tenure},
						mqttAuthorityPreparing)
				}
				return mqttConnectOwnershipInput{}, err
			}
			al := jsa.loadOutboundAuthority(id, epoch)
			if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityPreparing || al.rec.Intent == nil {
				return mqttConnectOwnershipInput{}, fmt.Errorf("no standing preparation to commit metadata for after prepare")
			}
			// THE PREPARATION MUST BE THIS ACCEPTANCE'S. A prepare that landed behind a lost
			// response, or lost to a rival, leaves SOME preparation standing; committing this
			// acceptance's metadata for another's proposal would bind its record to an ownership
			// it never proposed.
			if al.rec.Intent.ID != intentID {
				return mqttConnectOwnershipInput{}, fmt.Errorf("client %q: the standing preparation %q is "+
					"not this acceptance's proposal %q", id, al.rec.Intent.ID, intentID)
			}
			proposal, perr := mqttProposalFromHandoff(al.rec.Intent)
			if perr != nil {
				return mqttConnectOwnershipInput{}, perr
			}
			obs.noteOwn(proposal, mqttAuthorityPreparing)
			if err := sess.noteOwnershipBeforeMetadata(step.Action); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			sess.noteOwnershipMetadataEntry()
			detail, err := sess.saveAcceptWithOwnership(jsa, tok, clientID, sessEpoch, proposal)
			if err != nil {
				// The prepare landed but the metadata half did not commit through the accept
				// batch. A same-subject CAS contention (10164) decided nothing and the SAME frozen
				// attempt is re-sent within the shared budget (Codex consult 10). A predecessor
				// that moved in the clear pre-fence window (errMQTTHandoffPredecessorStale) is NOT
				// a contention — it settles this attempt and the re-drive re-decides against what
				// now stands. The cleanup for the settled-loser branch cancels the handoff,
				// restoring the predecessor's serving authority from the intent's own record of
				// what it displaced.
				cleanup := func() error {
					_, cerr := reconcile(func(ctxn string) (mqttOpOutcome, error) {
						return jsa.opCancelHandoff(id, epoch, ctxn, proposal.ID)
					})
					if cerr == nil {
						obs.AbandonConfirmed = true
					}
					return cerr
				}
				return sess.resolveOrRetryMetadataCommit(jsa, obs, proposal, detail, err, domainTk, step,
					retryOK, cleanup, load)
			}
			obs.noteVerdict(mqttMetadataCommitted)
		case mqttOwnershipRefuse:
			return mqttConnectOwnershipInput{}, fmt.Errorf("the executor was handed a refuse step, " +
				"which the driver resolves itself")
		default:
			// THE SETTLEMENT ARMS act on a NAMED proposal: this acceptance's own (the loser's
			// close, its own finalize) or the one the step's recovery names.
			named := intentID
			if step.Recovery != nil {
				named = step.Recovery.Proposal.ID
			}
			if err := jsa.runOwnershipSettlementArm(id, epoch, domainTk, step, named, reconcile); err != nil {
				if errors.Is(err, errMQTTOwnershipTargetMoved) {
					return load(step.Recovery), err
				}
				return mqttConnectOwnershipInput{}, err
			}
			if step.Recovery == nil && step.Action == mqttOwnershipCloseAbandoned {
				obs.AbandonConfirmed = true
			}
		}
		return load(step.Recovery), nil
	}
}

// errMQTTOwnershipTargetMoved reports that a settlement arm's NAMED proposal no longer stands —
// another settler acted on it first, or replaced it. Nothing of this step landed. The executor
// returns it WITH a fresh read, and the driver continues from that read: the act was owed to a
// proposal that is no longer there, so the right next step is to decide again, never to act on
// whatever now stands in its place (F-105 8a).
var errMQTTOwnershipTargetMoved = errors.New("the proposal this step names no longer stands")

// runOwnershipSettlementArm performs one SETTLEMENT act on the named proposal: settle its slot,
// finalize it, cancel it, or close it. These are the arms an acceptance shares with the
// settle-only runs at the CONNECT's top and at the discard boundary, and they act on a proposal
// by NAME: an act whose target no longer stands returns errMQTTOwnershipTargetMoved rather than
// acting on what replaced it.
func (jsa *mqttJSA) runOwnershipSettlementArm(id, epoch, domainTk string, step mqttOwnershipStep, named string,
	reconcile func(op func(txn string) (mqttOpOutcome, error)) (string, error)) error {
	moved := func(cause error) error {
		sid, serr := jsa.standingProposalID(id, epoch)
		if serr != nil || sid != named {
			if cause != nil {
				return fmt.Errorf("%w (%v)", errMQTTOwnershipTargetMoved, cause)
			}
			return errMQTTOwnershipTargetMoved
		}
		return cause
	}
	switch step.Action {
	case mqttOwnershipRecoverForeign:
		if step.Recovery == nil {
			return fmt.Errorf("a foreign-recovery step carried no proposal")
		}
		// SETTLE, THEN READ. The abort's own error is never the verdict — a decided slot refuses
		// it necessarily — so the verdict is what the slot says afterwards, which the reload
		// reads and the decision continues from.
		jsa.settleProposal(domainTk, id, epoch, step.Recovery.Proposal)
		return nil
	case mqttOwnershipFinalizeCreation:
		al := jsa.loadOutboundAuthority(id, epoch)
		if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityReserving ||
			al.rec.Creation == nil || al.rec.Creation.ID != named {
			return moved(nil)
		}
		if _, err := reconcile(func(txn string) (mqttOpOutcome, error) {
			return jsa.opFinalizeClaim(id, epoch, txn, named)
		}); err != nil {
			return moved(err)
		}
	case mqttOwnershipFinalizeHandoff, mqttOwnershipCancelHandoff:
		// THE TARGET IS PINNED ONCE, before any attempt: a re-attempt that re-discovered the
		// standing proposal could finalize or cancel a rival's that replaced the named one.
		sid, err := jsa.standingProposalID(id, epoch)
		if err != nil || sid != named {
			return moved(nil)
		}
		op := jsa.opFinalizeHandoff
		if step.Action == mqttOwnershipCancelHandoff {
			op = jsa.opCancelHandoff
		}
		if _, err := reconcile(func(txn string) (mqttOpOutcome, error) {
			return op(id, epoch, txn, sid)
		}); err != nil {
			return moved(err)
		}
	case mqttOwnershipCloseAbandoned:
		// The close names the RESERVATION'S OWN ownership — its generation, tenure and owning
		// transition, read from the standing authority — and only while the standing reservation
		// is the named one: a closure naming the wrong owner would either be refused or, worse,
		// close an ownership that has since moved on.
		al := jsa.loadOutboundAuthority(id, epoch)
		if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityReserving ||
			al.rec.Creation == nil || al.rec.Creation.ID != named {
			return moved(nil)
		}
		cGen, cTenure, cOwner := al.rec.Gen, al.rec.Tenure, al.rec.OwnerTxn // pinned once
		if _, err := reconcile(func(txn string) (mqttOpOutcome, error) {
			return jsa.opCloseGeneration(id, epoch, txn, cGen, cTenure, cOwner)
		}); err != nil {
			return moved(err)
		}
	default:
		return fmt.Errorf("unsupported ownership step %v", step.Action)
	}
	return nil
}

// driveAcceptToServingAuthority drives this accepting CONNECT to a SERVING outbound authority,
// committing the metadata half through the accept transaction: reserve -> commit-metadata
// (fenced) -> finalize, leaving a serving authority beside the record.
//
// The authority generation is sess.gen (the unified generation, above). A client with no live
// authority is minted through the creation arm; a client whose authority stands is displaced
// through a handoff (a resume preserves sess.gen; a clean start carries CleanReq + a retire
// request for the predecessor). Every request runs on the budget's deadline view, st carries the
// CONNECT's bounds across drives, and the result's observation is what the run established about
// its own proposal, on every outcome.
func (sess *mqttSession) driveAcceptToServingAuthority(tok recordTx, clientID, sessEpoch string,
	cleanStart bool, prevSessWID string, prevSessSeq uint64, targets mqttCleanStartRetireTargets,
	budget *mqttOwnershipBudget, now func() time.Time, retryOK func() error,
	stepAllow func() error, st *mqttConnectOwnershipState) (gen, tenure string, res mqttOwnershipResult, err error) {
	jsa := sess.jsa.withDeadline(budget.deadline())
	id := sess.id
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	domainTk := sess.domainTk

	gen = sess.gen
	tenure = mqttNewInflightNonce()
	intentID := mqttNewInflightNonce()
	st.ownIntents[intentID] = struct{}{}

	var retire *mqttRetirementRequest
	if cleanStart {
		// A clean start's handoff (over a live predecessor) discards the predecessor; the retire
		// request names the predecessor SESSION identity AND its backing-resource targets, so the
		// closure that ends its generation can enrol them AFTER the accepted batch has displaced
		// the record that made them nameable (F-105 row 8/9 ordering, Codex consult 6).
		// opPrepareHandoff stamps the predecessor gen/tenure/owner from the serving authority.
		retire = &mqttRetirementRequest{
			SessWID: prevSessWID, SessSeq: prevSessSeq,
			Reason: mqttRetireCleanStart, Discard: true,
		}
		retire.AffineDurable, retire.AffineGen, retire.PubRelDurable =
			mqttAffineRetireDescriptor(targets.AffineDurable, targets.AffineGen, targets.PubRelDurable)
	}
	obs := &mqttOwnObservation{}
	exec := sess.newAcceptOwnershipExecutorRun(jsa, st, obs, tok, clientID, sessEpoch, gen, tenure, intentID,
		cleanStart, retire, prevSessWID, prevSessSeq, retryOK, stepAllow)
	res, err = mqttDriveOwnershipRun(
		jsa.loadConnectOwnershipInput(id, epoch, domainTk, mqttOwnIdentity{Earlier: st.ownIntents}, nil),
		tenure, budget, now, exec, st, obs, false)
	return gen, tenure, res, err
}

// mqttIsMetadataContention reports whether an ownership metadata-commit error is a same-subject
// CAS contention (JetStream 10164). Such a rejection decided NOTHING: the three-member metadata
// batch is atomic, so a rejected send stored no member, and the SAME frozen attempt may be
// re-sent (Codex consult 5 step 4, consult 10). Any other error settles the attempt.
//
// The store error travels %w-wrapped out of persistRecord ("unable to persist session %q
// ownership metadata ... %w") and raw out of a retry's commitMetadataAttempt, so it is
// classified from the UNWRAPPED ApiError — the same wrap-aware discipline the accept loop's CAS
// arms use, because IsNatsErr's bare type assertion cannot see a code through a %w wrap.
func mqttIsMetadataContention(err error) bool {
	ce := jsAPIError(err)
	return ce != nil && IsNatsErr(ce, JSStreamWrongLastSequenceConstantErr)
}

// resolveOrRetryMetadataCommit is the metadata-half error handler for an accepting CONNECT's
// ownership executor, shared by the creation and handoff arms (Codex consult 10).
//
// A same-subject CAS contention (10164) decided nothing, so the permanent metadata slot — never
// settleProposal — is the arbiter: aborting our own slot merely because a send contended was the
// bug that stranded a RESERVING authority with an unresolved half the next CONNECT could neither
// finalize nor abandon. On an UNRESOLVED slot the SAME frozen attempt is re-sent within the one
// shared budget (retryOK charges the redrive allowance and the ownership deadline); a fresh
// transport batch id is minted inside commitMetadataAttempt and nothing semantic is regenerated,
// so the proposal, receipt and record bytes are identical across every retry. Any non-contention
// error, or budget exhaustion, settles the attempt to a permanent verdict and cleans up, so a
// later CONNECT meets a SETTLED authority. Every verdict it reads is written to obs BEFORE
// anything that can fail after it.
func (sess *mqttSession) resolveOrRetryMetadataCommit(jsa *mqttJSA, obs *mqttOwnObservation,
	proposal mqttOwnershipProposal, detail mqttSessSaveDetail, commitErr error, domainTk string,
	step mqttOwnershipStep, retryOK func() error, cleanup func() error,
	load func(*mqttForeignRecovery) mqttConnectOwnershipInput) (mqttConnectOwnershipInput, error) {
	id := sess.id
	err := commitErr
	for {
		if detail.ownFrozen == nil || !mqttIsMetadataContention(err) {
			// Not a re-sendable contention (a predecessor moved, an ambiguous send, a non-frozen
			// write): settle this attempt's own proposal and clean up if we lost.
			return sess.settleFailedMetadataCommit(jsa, obs, proposal, detail, domainTk, step, cleanup, load, err)
		}
		v := jsa.resolveMetadataAttempt(domainTk, id, sess.asmEpoch(), proposal)
		obs.noteVerdict(v)
		switch v {
		case mqttMetadataCommitted:
			// An ambiguous batch in fact landed. Adopt its proven sequence (located by write
			// identity) and return the reloaded input for the drive to finalize.
			if aerr := sess.adoptResolvedMetadata(jsa, detail, id, domainTk); aerr != nil {
				return mqttConnectOwnershipInput{}, aerr
			}
			return load(step.Recovery), nil
		case mqttMetadataAborted, mqttMetadataForeign:
			// Another party settled us the loser, or the slot holds a contradiction. Clean up our
			// reservation and refuse — with a fresh error, NOT one wrapping the 10164 or the
			// cleanup's own error, so the accept loop reads a plain refusal and does not re-drive
			// a now-closed generation as though the contention were still live. The observation
			// (an own proposal, not COMMITTED, not self-aborted) forbids a re-drive as well.
			if cerr := cleanup(); cerr != nil {
				return mqttConnectOwnershipInput{}, fmt.Errorf("cleaning up client %q's ownership "+
					"after its metadata attempt was settled the loser: %v", id, cerr)
			}
			return mqttConnectOwnershipInput{}, fmt.Errorf("client %q's ownership metadata attempt "+
				"was settled the loser before it could commit", id)
		}
		// UNRESOLVED: nothing committed and the create-only slot is empty. Re-send the SAME frozen
		// attempt if the shared budget and redrive allowance permit; otherwise settle and refuse.
		if rerr := retryOK(); rerr != nil {
			return sess.settleFailedMetadataCommit(jsa, obs, proposal, detail, domainTk, step, cleanup, load, rerr)
		}
		time.Sleep(mqttRecordCreateRetryWait)
		landedSeq, cerr := jsa.commitMetadataAttempt(detail.ownFrozen)
		if cerr == nil {
			obs.noteVerdict(mqttMetadataCommitted)
			sess.adoptCommittedMetadata(detail, landedSeq)
			return load(step.Recovery), nil
		}
		err = cerr
	}
}

// settleFailedMetadataCommit drives this attempt's own proposal to a PERMANENT verdict and
// cleans up if it lost. It is the terminal, non-retrying disposition: a re-drive that follows
// meets a settled authority rather than our stalled RESERVING/PREPARING proposal.
//
// Abort is create-only and loses to a commit, so the verdict is authoritative either way. A
// COMMITTED verdict means an ambiguous batch in fact landed — the acceptance succeeded — so the
// proven sequence is adopted (the latent gap the old switch left: it returned success without
// adopting, leaving sess.seq stale for the accept's subsequent saves) and the reloaded input is
// returned for the drive to finalize. An ABORTED verdict runs the arm's cleanup (close the
// reservation, or cancel the handoff) and returns the original error. Either verdict is recorded
// in obs FIRST, so a cleanup or readback cut off by the deadline does not lose it.
func (sess *mqttSession) settleFailedMetadataCommit(jsa *mqttJSA, obs *mqttOwnObservation,
	proposal mqttOwnershipProposal, detail mqttSessSaveDetail, domainTk string, step mqttOwnershipStep,
	cleanup func() error, load func(*mqttForeignRecovery) mqttConnectOwnershipInput,
	err error) (mqttConnectOwnershipInput, error) {
	// WHO ABORTED IT decides what the abort means (F-105 8a, H-1, H-2). Aborted by another
	// party means the acceptance has LOST — it cleans up and refuses, and no re-drive may
	// propose again for it. Aborted by this acceptance's own settlement means only that the
	// attempt lost the record race (a rival's record, a moved predecessor) and nothing of it
	// committed: the caller re-drives against the winner, which is re-drive arbitration against
	// a settled authority (Codex consult 10), not a loser proposing again.
	//
	// THE PROOF IS THE ABORT WRITE ITSELF. A read showing an empty slot proves nothing — a peer
	// can abort between that read and this acceptance's own abort, and an unreadable slot reads
	// empty too — so the ONLY evidence that the abort was this acceptance's settlement is its own
	// create-only abort write positively succeeding. Without that proof, or with a cleanup that
	// did not confirm, the acceptance is treated as having LOST: it refuses, terminally.
	v := jsa.resolveMetadataAttempt(domainTk, sess.id, sess.asmEpoch(), proposal)
	if v == mqttMetadataUnresolved {
		if jsa.abortMetadataAttempt(domainTk, sess.id, sess.asmEpoch(), proposal) == nil {
			obs.SelfAborted = true
		}
		v = jsa.resolveMetadataAttempt(domainTk, sess.id, sess.asmEpoch(), proposal)
	}
	obs.noteVerdict(v)
	switch v {
	case mqttMetadataCommitted:
		if aerr := sess.adoptResolvedMetadata(jsa, detail, sess.id, domainTk); aerr != nil {
			return mqttConnectOwnershipInput{}, aerr
		}
		return load(step.Recovery), nil
	case mqttMetadataAborted:
		cerr := cleanup()
		if obs.SelfAborted && cerr == nil {
			// THE ONE PATH ON WHICH THE RETRYABLE ORIGINAL ESCAPES: its own, proven settlement of
			// a lost record race, cleaned up. The caller re-drives against the winner.
			return mqttConnectOwnershipInput{}, err
		}
		if cerr != nil {
			return mqttConnectOwnershipInput{}, fmt.Errorf("client %q's ownership proposal was aborted, "+
				"and this acceptance cannot show it was its own settlement of a lost race; cleaning it "+
				"up failed: %v", sess.id, cerr)
		}
		return mqttConnectOwnershipInput{}, fmt.Errorf("client %q's ownership proposal was aborted by "+
			"another party, so this acceptance lost and refuses rather than proposing again", sess.id)
	}
	// UNRESOLVED or FOREIGN: nothing is known that licenses proposing again, and the proposal
	// stands for the next acceptance to settle. A fresh error, wrapping nothing retryable.
	return mqttConnectOwnershipInput{}, fmt.Errorf("client %q's ownership proposal could not be settled "+
		"after a failed metadata commit (verdict %v): it stands for the next acceptance (original: %v)",
		sess.id, v, err)
}

// asmEpoch is the session stream epoch the ownership records are scoped by.
func (sess *mqttSession) asmEpoch() string {
	if sess.asm == nil {
		return _EMPTY_
	}
	return sess.asm.sessStreamEpoch
}

// adoptResolvedMetadata adopts the landed record sequence for a metadata batch a permanent
// COMMITTED verdict proved landed but whose acknowledgement this broker never saw. The verdict
// carries no sequence, so the record is READ BACK and its sequence adopted only if the standing
// record is THIS attempt's — matched by write identity, never merely by position. Adopting an
// unrelated newer record's sequence is the one thing consult 10 forbids: a later writer's record
// at this subject is a foreign body, and CASing the session's next save against it would corrupt
// the single-slot register. A foreign or unreadable standing record refuses the acceptance.
func (sess *mqttSession) adoptResolvedMetadata(jsa *mqttJSA, detail mqttSessSaveDetail, id, domainTk string) error {
	smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(domainTk, getHash(id)))
	if lerr != nil {
		return fmt.Errorf("client %q's ownership metadata committed but its session record could "+
			"not be read back to adopt the landed sequence: %w", id, lerr)
	}
	ps := mqttDecodeSessionRecord(smsg)
	if ps == nil || ps.WID != detail.wid {
		got := _EMPTY_
		if ps != nil {
			got = ps.WID
		}
		return fmt.Errorf("client %q's ownership metadata committed as write identity %q but the "+
			"standing session record carries %q: a committed receipt is not a licence to adopt an "+
			"unrelated record's sequence", id, detail.wid, got)
	}
	sess.adoptCommittedMetadata(detail, smsg.Sequence)
	return nil
}

// mqttReconcileOwnershipStep runs ONE authority operation of an accepting CONNECT to a durable
// conclusion, and returns the transaction id of the attempt that landed. Every resolution and
// abort runs on jsa, the run's deadline view.
//
// The ownership operations are not idempotent retries: a claim or a preparation refuses the very
// state its own landed success creates, and a finalize or cancel loses its target once it has
// acted. So a failed attempt is never simply repeated. Its outcome is RESOLVED first, from the
// handle the operation froze before sending:
//
//   - landed, or landed and since superseded: the transition HAPPENED; the caller continues from
//     the durable state, and a later step re-reads what that state now permits;
//   - unresolved: settled by taking the attempt's outcome subject (abortTransition, create-only,
//     which loses to a batch that committed) and resolved again — the pre-submit refusal a
//     concurrent authority writer causes sent no batch, so its abort wins;
//   - positively never landed: nothing happened and nothing can, so ONE fresh attempt is admitted
//     by allow — the operation re-reads the authority itself — within the per-CONNECT allowance
//     and the absolute ownership deadline.
//
// "Any error" governs entry into this reconciliation, never what follows it: an operation that
// refused before composing a transition (an unreadable authority, a state its rules refuse)
// returns no handle and its error is returned as is, and an attempt still unresolved after the
// abort concludes nothing.
//
// Takes no session lock.
func mqttReconcileOwnershipStep(jsa *mqttJSA, op func(txn string) (mqttOpOutcome, error),
	allow func() error) (string, error) {
	for {
		txn := mqttNewInflightNonce()
		out, err := op(txn)
		if err == nil {
			return txn, nil
		}
		if out.Handle == nil {
			return _EMPTY_, err
		}
		v := jsa.resolveTransitionHandle(out.Handle)
		if v == mqttTransitionUnresolved {
			_ = jsa.abortTransition(&out.Handle.attempt) // the re-read, not this result, decides
			v = jsa.resolveTransitionHandle(out.Handle)
		}
		switch v {
		case mqttTransitionLanded, mqttTransitionSuperseded:
			return txn, nil
		case mqttTransitionNeverLanded:
			if aerr := allow(); aerr != nil {
				return _EMPTY_, fmt.Errorf("%w (and no re-attempt is admitted: %v)", err, aerr)
			}
			continue
		}
		// STILL UNRESOLVED: the transition may have landed. Marked, so a caller whose transition
		// would have created a proposal records it as one that may stand (F-105 8a, consult 120).
		return _EMPTY_, fmt.Errorf("%w: %w", errMQTTOwnershipStepUnresolved, err)
	}
}

// errMQTTOwnershipStepUnresolved marks an ownership step whose transition could be neither
// confirmed nor ruled out: it may have landed.
var errMQTTOwnershipStepUnresolved = errors.New("the ownership step's outcome is unresolved")

// noteOwnershipMetadataEntry reports to an armed test seam that the executor is entering its
// fenced metadata commit.
func (sess *mqttSession) noteOwnershipMetadataEntry() {
	if sess.asm == nil {
		return
	}
	sess.asm.mu.RLock()
	hook := sess.asm.testOwnershipMetadataEntry
	sess.asm.mu.RUnlock()
	if hook != nil {
		hook()
	}
}

// noteOwnershipStepEntry reports to an armed test seam that the executor is entering a step, and
// returns the seam's error, which ends the step as a stopped process would.
func (sess *mqttSession) noteOwnershipStepEntry(action mqttConnectOwnershipAction) error {
	if sess.asm == nil {
		return nil
	}
	sess.asm.mu.RLock()
	hook := sess.asm.testOwnershipStepEntry
	sess.asm.mu.RUnlock()
	if hook != nil {
		return hook(action)
	}
	return nil
}

// noteOwnershipBeforeMetadata reports to an armed test seam that a reservation or preparation
// landed and its metadata half is next, and returns the seam's error, which ends the step as a
// stopped process would.
func (sess *mqttSession) noteOwnershipBeforeMetadata(action mqttConnectOwnershipAction) error {
	if sess.asm == nil {
		return nil
	}
	sess.asm.mu.RLock()
	hook := sess.asm.testOwnershipBeforeMetadata
	sess.asm.mu.RUnlock()
	if hook != nil {
		return hook(action)
	}
	return nil
}
