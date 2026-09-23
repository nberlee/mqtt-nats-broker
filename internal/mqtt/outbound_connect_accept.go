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

// newAcceptOwnershipExecutor is the executor mqttDriveOwnership drives for an accepting CONNECT.
// Its metadata step is sess.saveAcceptWithOwnership (fenced); every other step is the ordinary
// authority op. It re-reads the authority after each step, exactly as the minting executors do,
// so a step's durable result and a lost response to it are resolved the same way.
func (sess *mqttSession) newAcceptOwnershipExecutor(tok recordTx, clientID, sessEpoch, gen, tenure, intentID string,
	cleanStart bool, retire *mqttRetirementRequest, prevSessWID string, prevSessSeq uint64,
	retryOK func() error) mqttOwnershipExecutor {
	jsa := sess.jsa
	id := sess.id
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	domainTk := sess.domainTk
	return func(step mqttOwnershipStep) (mqttConnectOwnershipInput, error) {
		txn := mqttNewInflightNonce()
		switch step.Action {
		case mqttOwnershipReserve, mqttOwnershipReserveOverClosed:
			// Reserve the client's ownership create-only at the SESSION generation, then commit
			// the metadata half THROUGH the accept transaction — the record, the fact and the
			// COMMITTED receipt land as one fenced batch.
			if _, err := jsa.opClaimClient(id, epoch, txn, gen, tenure, intentID); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			proposal := mqttOwnershipProposal{Kind: mqttProposalCreation, ID: intentID, Gen: gen, Tenure: tenure}
			detail, err := sess.saveAcceptWithOwnership(tok, clientID, sessEpoch, proposal)
			if err != nil {
				// The reservation landed but the metadata half did not commit through the accept
				// batch. A same-subject CAS contention (10164) decided NOTHING — the three-member
				// batch is atomic, so a rejected send stored nothing — and the SAME frozen attempt
				// is re-sent within the shared budget (Codex consult 10). Any other outcome (a
				// rival moved the record, an ambiguous send) settles this attempt's own proposal so
				// a re-drive meets a SETTLED authority rather than our stalled RESERVING proposal.
				// The cleanup for the settled-loser branch closes the reservation's OWN generation
				// (the recoverable abandonment; the reservation is never deleted), naming the
				// reservation's OWN owning transition (this txn) so a moved ownership is refused
				// rather than closed.
				cleanup := func() error {
					_, cerr := jsa.opCloseGeneration(id, epoch, mqttNewInflightNonce(), gen, tenure, txn)
					return cerr
				}
				return sess.resolveOrRetryMetadataCommit(proposal, detail, err, id, epoch, domainTk, step, retryOK, cleanup)
			}
		case mqttOwnershipFinalizeCreation:
			if _, err := jsa.opFinalizeClaim(id, epoch, txn, intentID); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipPrepareHandoff:
			// Reserve the handoff from the standing serving authority; opPrepareHandoff fills the
			// intent's Prev gen/tenure/ownerTxn (and the retire request's) from it. Then read the
			// PREPARING authority back for the proposal it now carries — its Prev* fields are the
			// ones the metadata receipt must name — and commit the metadata half through the fence.
			intent := &mqttHandoffIntent{
				ID: intentID, Gen: gen, Tenure: tenure, CleanReq: cleanStart, Retire: retire,
				PrevSessWID: prevSessWID, PrevSessSeq: prevSessSeq,
			}
			if _, err := jsa.opPrepareHandoff(id, epoch, txn, intent); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
			al := jsa.loadOutboundAuthority(id, epoch)
			if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityPreparing || al.rec.Intent == nil {
				return mqttConnectOwnershipInput{}, fmt.Errorf("no standing preparation to commit metadata for after prepare")
			}
			proposal, perr := mqttProposalFromHandoff(al.rec.Intent)
			if perr != nil {
				return mqttConnectOwnershipInput{}, perr
			}
			detail, err := sess.saveAcceptWithOwnership(tok, clientID, sessEpoch, proposal)
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
					_, cerr := jsa.opCancelHandoff(id, epoch, mqttNewInflightNonce(), proposal.ID)
					return cerr
				}
				return sess.resolveOrRetryMetadataCommit(proposal, detail, err, id, epoch, domainTk, step, retryOK, cleanup)
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
		case mqttOwnershipCloseAbandoned:
			// This acceptance's own reservation was positively aborted: close its generation
			// (the recoverable abandonment) and let the driver refuse — the loser bound. The
			// close names the RESERVATION'S OWN ownership — its generation, tenure and owning
			// transition, read from the standing authority — not this step's fresh nonce: a
			// closure that named the wrong owner would either be refused or, worse, close an
			// ownership that has since moved on.
			al := jsa.loadOutboundAuthority(id, epoch)
			if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityReserving {
				return mqttConnectOwnershipInput{}, fmt.Errorf("client %q carries no standing "+
					"reservation to close for an abandoned proposal", id)
			}
			if _, err := jsa.opCloseGeneration(id, epoch, txn, al.rec.Gen, al.rec.Tenure, al.rec.OwnerTxn); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipRecoverForeign:
			if step.Recovery == nil {
				return mqttConnectOwnershipInput{}, fmt.Errorf("a foreign-recovery step carried no proposal")
			}
			if err := jsa.abortMetadataAttempt(domainTk, id, epoch, step.Recovery.Proposal); err != nil {
				return mqttConnectOwnershipInput{}, err
			}
		case mqttOwnershipRefuse:
			return mqttConnectOwnershipInput{}, fmt.Errorf("the executor was handed a refuse step, " +
				"which the driver resolves itself")
		default:
			return mqttConnectOwnershipInput{}, fmt.Errorf("unsupported ownership step %v", step.Action)
		}
		return jsa.loadConnectOwnershipInput(id, epoch, domainTk, step.Recovery), nil
	}
}

// driveAcceptToServingAuthority drives this accepting CONNECT to a SERVING outbound authority,
// committing the metadata half through the accept transaction. It replaces saveAcceptWithDetail:
// where that made the two-member fact+record batch, this makes the ownership protocol's
// reserve -> commit-metadata(fenced) -> finalize, leaving a serving authority beside the record.
//
// The authority generation is sess.gen (the unified generation, above). A client with no live
// authority is minted through the creation arm; a client whose authority stands is displaced
// through a handoff (a resume preserves sess.gen; a clean start carries CleanReq + a retire
// request for the predecessor). It returns the ownership result and the standing generation and
// tenure, so mintSharedTenure and the session's bookkeeping can adopt them.
func (sess *mqttSession) driveAcceptToServingAuthority(tok recordTx, clientID, sessEpoch string,
	cleanStart bool, prevSessWID string, prevSessSeq uint64, targets mqttCleanStartRetireTargets,
	budget *mqttOwnershipBudget, now func() time.Time, retryOK func() error) (gen, tenure string, res mqttOwnershipResult, err error) {
	jsa := sess.jsa
	id := sess.id
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	domainTk := sess.domainTk

	gen = sess.gen
	tenure = mqttNewInflightNonce()
	intentID := mqttNewInflightNonce()

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
			AffineDurable: targets.AffineDurable, AffineGen: targets.AffineGen,
			PubRelDurable: targets.PubRelDurable,
		}
	}
	exec := sess.newAcceptOwnershipExecutor(tok, clientID, sessEpoch, gen, tenure, intentID,
		cleanStart, retire, prevSessWID, prevSessSeq, retryOK)
	res, err = mqttDriveOwnership(jsa.loadConnectOwnershipInput(id, epoch, domainTk, nil),
		tenure, budget, now, exec)
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
// later CONNECT meets a SETTLED authority.
func (sess *mqttSession) resolveOrRetryMetadataCommit(proposal mqttOwnershipProposal,
	detail mqttSessSaveDetail, commitErr error, id, epoch, domainTk string, step mqttOwnershipStep,
	retryOK func() error, cleanup func() error) (mqttConnectOwnershipInput, error) {
	jsa := sess.jsa
	err := commitErr
	for {
		if detail.ownFrozen == nil || !mqttIsMetadataContention(err) {
			// Not a re-sendable contention (a predecessor moved, an ambiguous send, a non-frozen
			// write): settle this attempt's own proposal and clean up if we lost.
			return sess.settleFailedMetadataCommit(proposal, detail, id, epoch, domainTk, step, cleanup, err)
		}
		switch jsa.resolveMetadataAttempt(domainTk, id, epoch, proposal) {
		case mqttMetadataCommitted:
			// An ambiguous batch in fact landed. Adopt its proven sequence (located by write
			// identity) and return the reloaded input for the drive to finalize.
			if aerr := sess.adoptResolvedMetadata(detail, id, domainTk); aerr != nil {
				return mqttConnectOwnershipInput{}, aerr
			}
			return jsa.loadConnectOwnershipInput(id, epoch, domainTk, step.Recovery), nil
		case mqttMetadataAborted, mqttMetadataForeign:
			// Another party settled us the loser, or the slot holds a contradiction. Clean up our
			// reservation and refuse — with a fresh error, NOT one wrapping the 10164, so the
			// accept loop reads a plain refusal and does not re-drive a now-closed generation as
			// though the contention were still live.
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
			return sess.settleFailedMetadataCommit(proposal, detail, id, epoch, domainTk, step, cleanup, rerr)
		}
		time.Sleep(mqttRecordCreateRetryWait)
		landedSeq, cerr := jsa.commitMetadataAttempt(detail.ownFrozen)
		if cerr == nil {
			sess.adoptCommittedMetadata(detail, landedSeq)
			return jsa.loadConnectOwnershipInput(id, epoch, domainTk, step.Recovery), nil
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
// reservation, or cancel the handoff) and returns the original error.
func (sess *mqttSession) settleFailedMetadataCommit(proposal mqttOwnershipProposal,
	detail mqttSessSaveDetail, id, epoch, domainTk string, step mqttOwnershipStep,
	cleanup func() error, err error) (mqttConnectOwnershipInput, error) {
	switch sess.jsa.settleProposal(domainTk, id, epoch, proposal) {
	case mqttMetadataCommitted:
		if aerr := sess.adoptResolvedMetadata(detail, id, domainTk); aerr != nil {
			return mqttConnectOwnershipInput{}, aerr
		}
		return sess.jsa.loadConnectOwnershipInput(id, epoch, domainTk, step.Recovery), nil
	case mqttMetadataAborted:
		if cerr := cleanup(); cerr != nil {
			return mqttConnectOwnershipInput{}, fmt.Errorf("cleaning up client %q's ownership after "+
				"a failed metadata commit: %v (original: %w)", id, cerr, err)
		}
	}
	return mqttConnectOwnershipInput{}, err
}

// adoptResolvedMetadata adopts the landed record sequence for a metadata batch a permanent
// COMMITTED verdict proved landed but whose acknowledgement this broker never saw. The verdict
// carries no sequence, so the record is READ BACK and its sequence adopted only if the standing
// record is THIS attempt's — matched by write identity, never merely by position. Adopting an
// unrelated newer record's sequence is the one thing consult 10 forbids: a later writer's record
// at this subject is a foreign body, and CASing the session's next save against it would corrupt
// the single-slot register. A foreign or unreadable standing record refuses the acceptance.
func (sess *mqttSession) adoptResolvedMetadata(detail mqttSessSaveDetail, id, domainTk string) error {
	smsg, lerr := sess.jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(domainTk, getHash(id)))
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
