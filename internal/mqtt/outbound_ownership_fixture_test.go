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

import (
	"encoding/json"
	"fmt"
	"time"
)

// Test-only ownership fixtures. Production reaches a serving authority through ONE route, the
// accepting CONNECT's executor driven by mqttDriveOwnershipRun with the metadata half committed
// through sess.saveAcceptWithOwnership's fenced batch. The two entry points below are what the
// decision-function and op-level tests drive instead: a stand-alone drive with a fresh CONNECT
// state, and a metadata commit outside any session's record fence. Neither is a production
// route, which is why they live here and not beside the code they exercise.

// mqttDriveOwnership carries the ownership protocol to a conclusion for a single, stand-alone
// run: its own CONNECT state and its own observation. See mqttDriveOwnershipRun.
func mqttDriveOwnership(in mqttConnectOwnershipInput, ownTenure string,
	b *mqttOwnershipBudget, now func() time.Time,
	exec mqttOwnershipExecutor) (mqttOwnershipResult, error) {
	return mqttDriveOwnershipRun(in, ownTenure, b, now, exec, newMQTTConnectOwnershipState(), nil, false)
}

// mqttCommitMetadataRequest is the metadata half of a standing proposal, as its caller states
// it.
//
// The caller names WHICH proposal by its identity and supplies the session body this
// acceptance persists — the payload. It does not supply the proposal view, the receipt, the
// expectation the record is written under, or the acceptance fact's subject: all of those are
// derived from the standing authority and the standing record.
type mqttCommitMetadataRequest struct {
	ID       string
	Epoch    string
	DomainTk string

	// IntentID names the standing reservation or preparation whose metadata half this is.
	IntentID string
	// SessWID is the write identity this acceptance installs on the session record.
	SessWID string
	// Record is the session body to persist. Its client, generation and write identity are
	// OVERWRITTEN from the proposal and SessWID, because those three are what the permanent
	// receipt reports and a body disagreeing with the receipt is the defect this closes.
	Record *mqttPersistedSession
	// AcceptID and AcceptAt are the permanent acceptance fact's identity and time.
	AcceptID string
	AcceptAt int64
}

// opCommitMetadata commits the metadata half of the proposal STANDING on the client's own
// authority.
//
// Freezing was the last thing a caller did for itself, and it was the last place a
// caller-authored conclusion could enter: the frozen attempt is a struct, so every binding the
// freezer established held only until the caller changed it, and the freezer would compose an
// attempt for any proposal at all — including one no authority had ever claimed.
//
// Here the proposal is READ from the authority, the expectation is READ from the standing
// session record, and the receipt is derived from both. The returned attempt is the handle for
// a retry: a retry RESENDS IT UNCHANGED, because the metadata slot is create-only and a
// recomposed attempt would be a different proposal's commit racing the first one's.
func (jsa *mqttJSA) opCommitMetadata(r *mqttCommitMetadataRequest) (*mqttFrozenMetadataAttempt, uint64, error) {
	if r == nil || r.Record == nil {
		return nil, 0, fmt.Errorf("committing a metadata half requires the session record it installs")
	}
	auth, _, err := jsa.requireAuthority(r.ID, r.Epoch)
	if err != nil {
		return nil, 0, err
	}
	if auth == nil {
		return nil, 0, fmt.Errorf("client %q carries no authority, so it carries no proposal to commit", r.ID)
	}
	// THE PROPOSAL IS THE RECORD'S, and the caller only says which one it means.
	var p mqttOwnershipProposal
	switch {
	case auth.State == mqttAuthorityReserving && auth.Creation != nil:
		if auth.Creation.ID != r.IntentID {
			return nil, 0, fmt.Errorf("client %q's reservation carries proposal %q, not %q",
				r.ID, auth.Creation.ID, r.IntentID)
		}
		p, err = mqttProposalFromCreation(auth.Creation)
	case auth.State == mqttAuthorityPreparing && auth.Intent != nil:
		if auth.Intent.ID != r.IntentID {
			return nil, 0, fmt.Errorf("client %q's preparation carries intent %q, not %q",
				r.ID, auth.Intent.ID, r.IntentID)
		}
		p, err = mqttProposalFromHandoff(auth.Intent)
	default:
		return nil, 0, fmt.Errorf("client %q's authority is in state %d and carries no proposal "+
			"whose metadata half could commit", r.ID, auth.State)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("client %q carries a proposal this broker cannot interpret: %w", r.ID, err)
	}

	// THE EXPECTATION IS THE STANDING RECORD'S. A handoff additionally has to be the record
	// its own proposal names, which the write boundary re-establishes.
	baseSeq := uint64(0)
	if smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, mqttSessSubject(r.DomainTk, getHash(r.ID))); lerr == nil {
		baseSeq = smsg.Sequence
	} else if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
		return nil, 0, fmt.Errorf("client %q's session record could not be read, so this "+
			"metadata half cannot name what it displaces: %w", r.ID, lerr)
	}

	rec := *r.Record
	rec.ID, rec.Gen, rec.WID = r.ID, p.Gen, r.SessWID
	recBody, merr := json.Marshal(&rec)
	if merr != nil {
		return nil, 0, merr
	}
	fact := &mqttAcceptFact{
		ClientID: r.ID, SessEpoch: r.Epoch, AcceptID: r.AcceptID, At: r.AcceptAt,
	}
	factBody, ferr := json.Marshal(fact)
	if ferr != nil {
		return nil, 0, ferr
	}
	f, ferr2 := mqttFreezeMetadataAttempt(r.ID, r.Epoch, r.DomainTk, p, r.SessWID,
		mqttAcceptSubject(r.DomainTk, getHash(r.ID), r.Epoch, r.AcceptID), factBody, baseSeq, recBody)
	if ferr2 != nil {
		return nil, 0, ferr2
	}
	// THE ATTEMPT COMES BACK EVEN WHEN THE COMMIT FAILS: the slot is create-only and
	// permanent, so an ambiguous send is resolved by asking it, and a retry resends THIS
	// attempt rather than composing another.
	seq, cerr := jsa.commitMetadataAttempt(f)
	return f, seq, cerr
}
