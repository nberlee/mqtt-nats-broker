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
	cleanStart bool, retire *mqttRetirementRequest, prevSessWID string, prevSessSeq uint64) mqttOwnershipExecutor {
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
			if _, err := sess.saveAcceptWithOwnership(tok, clientID, sessEpoch, proposal); err != nil {
				// The reservation landed but the metadata half did not commit through the accept
				// batch (a rival moved the record, an ambiguous send, a CAS reject). Settle THIS
				// attempt's own proposal before returning, so a re-drive meets a SETTLED authority
				// rather than our stalled RESERVING proposal — which it would read as an unresolved
				// metadata half it may neither finalize nor abandon. Abort is create-only and loses
				// to a commit, so the permanent verdict is authoritative either way:
				//   COMMITTED -> an ambiguous batch in fact landed; the acceptance succeeded, so
				//                return the reloaded input for the drive to finalize.
				//   ABORTED   -> close the reservation's own generation (the recoverable
				//                abandonment; the reservation is never deleted), naming the
				//                reservation's OWN owning transition (this txn) so a moved
				//                ownership is refused rather than closed.
				switch jsa.settleProposal(domainTk, id, epoch, proposal) {
				case mqttMetadataCommitted:
					return jsa.loadConnectOwnershipInput(id, epoch, domainTk, step.Recovery), nil
				case mqttMetadataAborted:
					if _, cerr := jsa.opCloseGeneration(id, epoch, mqttNewInflightNonce(), gen, tenure, txn); cerr != nil {
						return mqttConnectOwnershipInput{}, fmt.Errorf("closing the abandoned reservation "+
							"after a failed creation metadata commit for %q: %v (original: %w)", id, cerr, err)
					}
				}
				return mqttConnectOwnershipInput{}, err
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
			if _, err := sess.saveAcceptWithOwnership(tok, clientID, sessEpoch, proposal); err != nil {
				// The prepare landed but the metadata half did not commit through the accept batch
				// — a rival won the predecessor record in the clear pre-fence window, most often
				// (errMQTTHandoffPredecessorStale). Settle THIS attempt's own proposal before
				// returning, so a re-drive meets a SETTLED serving authority rather than our
				// stalled PREPARING proposal — which it would read as an unresolved metadata half
				// it may neither finalize nor cancel. Abort is create-only and loses to a commit,
				// so the permanent verdict is authoritative either way:
				//   COMMITTED -> an ambiguous batch in fact landed; the acceptance succeeded, so
				//                return the reloaded input for the drive to finalize.
				//   ABORTED   -> cancel the handoff, restoring the predecessor's serving authority
				//                from the intent's own record of what it displaced.
				switch jsa.settleProposal(domainTk, id, epoch, proposal) {
				case mqttMetadataCommitted:
					return jsa.loadConnectOwnershipInput(id, epoch, domainTk, step.Recovery), nil
				case mqttMetadataAborted:
					if _, cerr := jsa.opCancelHandoff(id, epoch, mqttNewInflightNonce(), proposal.ID); cerr != nil {
						return mqttConnectOwnershipInput{}, fmt.Errorf("restoring the predecessor after a "+
							"failed handoff metadata commit for %q: %v (original: %w)", id, cerr, err)
					}
				}
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
	budget *mqttOwnershipBudget, now func() time.Time) (gen, tenure string, res mqttOwnershipResult, err error) {
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
		cleanStart, retire, prevSessWID, prevSessSeq)
	res, err = mqttDriveOwnership(jsa.loadConnectOwnershipInput(id, epoch, domainTk, nil),
		tenure, budget, now, exec)
	return gen, tenure, res, err
}
