// Copyright 2025 The mqtt-nats-bridge Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"sync/atomic"

	"github.com/nats-io/nuid"
)

// The DURABLE TEARDOWN RECEIPT is the create-only authorization a discarding close
// reads (F-105, row 8, Gap 1).
//
// Closing the socket does not close authority; discarding the session does — and a
// discarding closure abandons a generation's outbound obligations, so it needs an
// author. A close-admission transition may not carry a discard for exactly that
// reason (outbound_transition.go): ending a generation is not authority to abandon
// what it owes. The receipt is that authority, made durable.
//
// It is the FOURTH top-level subject space on $MQTT_sess, beside the session record,
// the acceptance fact and the handoff outcome, for the same reason the fact is
// top-level: every session walk filters "sess.<domainTk>>" and decodes what it finds
// as a record, so the receipt must not live under that prefix. Living on the same
// STREAM is the point — it is written in the SAME atomic batch as the teardown
// marker's record CAS, so the successful discard decision is made permanent and
// independently checkable in the one act that claims the record. A receipt written
// separately after merely observing a marker is insufficient: the marker can be
// displaced afterwards.
//
// It is CREATE-ONLY and per generation. The teardown marker is written last, carrying
// the commit flag, and its CAS on the record's own sequence is the arbitration point;
// the receipt is staged first as a conditioned non-commit member and becomes durable
// only when that CAS commits the batch. A generation is torn down once, so a fresh
// create-only slot per generation is exactly the shape, and a slot already occupied by
// a different teardown is a supersession rather than an error.
const mqttTeardownReceiptSubjectPrefix = mqttPrefix + "trcpt."

// mqttTeardownReceiptSubject is the per-client, per-generation receipt subject. gen is
// a single subject token (mqttNewSessGen mints one), so the subject carries exactly two
// tokens after the prefix — the client hash the domain token routes and the generation —
// which the stream's "trcpt.<domainTk>>" filter covers and no record walk ever reads.
func mqttTeardownReceiptSubject(domainTk, cidHash, gen string) string {
	return mqttTeardownReceiptSubjectPrefix + domainTk + cidHash + "." + gen
}

// mqttSessTeardownReceipt is the authorization one discarding close consumes.
//
// Retire names the EXACT predecessor ownership being retired — generation, tenure,
// owning transition and the predecessor's session-record identity — together with the
// discard reason and the captured retirement targets the closure enrols. It is a
// mqttRetirementRequest because that is what mqttStandingRetirement returns and what the
// transition boundary validates a closure's affine enrolment against: a Clean Start
// handoff carries the same request on its intent, and a standalone teardown carries it
// here. Nonce and MarkerWID bind the receipt to the specific teardown marker committed
// beside it, so an adopter can prove the two are one act.
type mqttSessTeardownReceipt struct {
	// Nonce is the teardown marker's nonce, the same value the marker's Teardown carries.
	Nonce string `json:"nonce"`
	// MarkerWID is the write identity of the marker record committed in the same batch.
	MarkerWID string `json:"marker_wid"`
	// Retire is the durable authorization: whom to retire, why, and what to enrol.
	Retire mqttRetirementRequest `json:"retire"`
}

// mqttComposeTeardownReceipt builds the receipt body for a discarding teardown of gen,
// under the authority captured before the record was reset. It refuses to author a
// receipt that does not discard or does not name the generation it is stored under: a
// receipt is only ever an authority to abandon obligations, and one that named a
// different generation would authorize the wrong close.
func mqttComposeTeardownReceipt(gen, nonce, markerWID string, retire mqttRetirementRequest) (*mqttSessTeardownReceipt, error) {
	if !retire.Discard {
		return nil, fmt.Errorf("refusing to author a teardown receipt that does not discard generation %q", gen)
	}
	if retire.Gen != gen {
		return nil, fmt.Errorf("refusing to author a teardown receipt on generation %q that names %q", gen, retire.Gen)
	}
	if !mqttValidRetirementReason(retire.Reason) {
		return nil, fmt.Errorf("refusing to author a teardown receipt with unsupported reason %q", retire.Reason)
	}
	if nonce == _EMPTY_ || markerWID == _EMPTY_ {
		return nil, fmt.Errorf("refusing to author a teardown receipt with no marker identity")
	}
	return &mqttSessTeardownReceipt{Nonce: nonce, MarkerWID: markerWID, Retire: retire}, nil
}

// mqttDecodeTeardownReceipt validates a decoded receipt against the generation it was
// stored under. Every binding is checked because the receipt is evidence used to CLOSE a
// generation discarding: an unchecked binding is a way to author the abandonment of one
// generation's obligations with a receipt written about another.
func mqttDecodeTeardownReceipt(data []byte, gen string) (*mqttSessTeardownReceipt, error) {
	r := &mqttSessTeardownReceipt{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, fmt.Errorf("teardown receipt is unreadable: %w", err)
	}
	if !r.Retire.Discard {
		return nil, fmt.Errorf("teardown receipt of generation %q does not discard", gen)
	}
	if r.Retire.Gen != gen {
		return nil, fmt.Errorf("teardown receipt stored on generation %q names %q", gen, r.Retire.Gen)
	}
	if !mqttValidRetirementReason(r.Retire.Reason) {
		return nil, fmt.Errorf("teardown receipt of generation %q carries unsupported reason %q", gen, r.Retire.Reason)
	}
	if r.Nonce == _EMPTY_ || r.MarkerWID == _EMPTY_ {
		return nil, fmt.Errorf("teardown receipt of generation %q carries no marker identity", gen)
	}
	return r, nil
}

// loadTeardownReceipt reads the durable teardown receipt for one generation. It uses the
// same four-way status the authority and closure reads use: an absent receipt is not a
// corrupt one, and only a positively validated receipt authorizes a discarding close —
// an unreadable or indeterminate read refuses it rather than proceeding.
func (jsa *mqttJSA) loadTeardownReceipt(domainTk, cidHash, gen string) (*mqttSessTeardownReceipt, uint64, mqttOutboundLoadStatus) {
	subject := mqttTeardownReceiptSubject(domainTk, cidHash, gen)
	smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, mqttOutboundIndeterminate
		}
		return nil, 0, mqttOutboundAbsent
	}
	r, derr := mqttDecodeTeardownReceipt(smsg.Data, gen)
	if derr != nil {
		return nil, smsg.Sequence, mqttOutboundCorrupt
	}
	return r, smsg.Sequence, mqttOutboundValid
}

// mqttTeardownClaimBatchSize is the two members of a teardown claim that authors a
// receipt: the create-only receipt staged first, the committing marker last.
const mqttTeardownClaimBatchSize = 2

// storeTeardownClaimBatch commits a teardown marker AND its durable authorization receipt
// in one atomic batch on $MQTT_sess, returning the COMMIT's PubAck sequence — the
// MARKER's, because the reply rides the commit member and the marker is the message that
// carries it (consult 8, arbitration A).
//
// The receipt is staged FIRST, create-only and non-commit: it becomes durable only when
// the marker's CAS commits the batch. The marker is LAST, carrying the commit flag and a
// CAS on the record's OWN sequence — that CAS is the arbitration point, so whoever wins it
// wins the teardown, and both members' conditions must pass for either to land. This is
// the accept batch's reasoning (mqttComposeAcceptBatch) with a marker in place of a record
// and a receipt in place of a fact.
//
// The store-suppression seam is honored here exactly as storeAcceptBatch and storeSessionMsg
// honor it: the single-CAS claim it replaces went through storeSessionMsg, so a test that
// suppresses the record store must still be able to model the "issued but did not land" half
// of ambiguity for a teardown that authors a receipt.
func (jsa *mqttJSA) storeTeardownClaimBatch(domainTk, cidHash string, seq uint64,
	receiptSubj string, receiptBody, markerBody []byte) (uint64, error) {
	if mqttTestSessStoreSuppressV.Load() {
		return 0, errors.New("mqtt test: session record store suppressed")
	}
	// FRESH per attempt: reusing a batch id while one is still staged leaves a sequence gap
	// on the server's staging store and abandons the batch. What stays stable across attempts
	// is the two bodies and the CAS base, never the framing.
	batchID := nuid.Next()
	rb, rh := mqttFrameBatchWrite(0, receiptBody, batchID, 1, false)
	mb, mh := mqttFrameBatchWrite(seq, markerBody, batchID, 2, true)
	bi, err := jsa.newAtomicBatchRequest(mqttJSASessPersist, cidHash, []mqttBatchMsg{
		{subj: receiptSubj, hdr: rh, msg: rb},
		{subj: mqttSessSubject(domainTk, cidHash), hdr: mh, msg: mb},
	})
	if err != nil {
		return 0, err
	}
	smr := bi.(*JSPubAckResponse)
	if aerr := smr.ToError(); aerr != nil {
		return 0, aerr
	}
	// Validated as storeAcceptBatch validates its own: a success not bound to THIS batch's two
	// members is not evidence about it, and the caller adopts the returned sequence as the
	// marker's.
	if smr.PubAck == nil {
		return 0, fmt.Errorf("the teardown claim batch was acknowledged with no publish acknowledgement")
	}
	if smr.BatchId != batchID || smr.BatchSize != mqttTeardownClaimBatchSize {
		return 0, fmt.Errorf("the teardown claim batch was acknowledged as %d message(s) under batch %q, but this broker sent %d under %q",
			smr.BatchSize, smr.BatchId, mqttTeardownClaimBatchSize, batchID)
	}
	if smr.Stream != mqttSessStreamName {
		return 0, fmt.Errorf("the teardown claim batch was acknowledged on stream %q (want %q)", smr.Stream, mqttSessStreamName)
	}
	return smr.Sequence, nil
}

// mqttTestReceiptContinuationHookV runs in claimTeardownReceipt after the marker is read and
// before the batch is sent: the window a test displaces the marker in. nil in production.
var mqttTestReceiptContinuationHookV atomic.Pointer[func(id string)]

// claimTeardownReceipt is the RECEIPTLESS CONTINUATION of an adopted teardown (F-105 8b,
// consult 121): a marker that carries no receipt, over an authority that — once every standing
// proposal is settled — still SERVES the marker's generation. Withholding the closing there
// forever would strand the record (an aborted preparation settles back to exactly that state),
// and reading the marker then writing a receipt beside it would author nothing: the receipt file
// states why, the marker can be displaced afterwards.
//
// So the receipt is committed the way the owner's claim commits it: in ONE atomic batch whose
// commit member is the marker itself, re-written byte-identical and CAS'd at the exact sequence
// the adoption read it at. Exactly one of {this batch, any other write to the record} lands. The
// marker is the record's terminal claim, and the authority serving its generation can be retired
// or handed off only by a proposal the caller has just settled — so committing the receipt with
// that marker is the one act that makes "this teardown discards this generation" durable.
//
// It returns the sequence the marker now stands at, which the adoption continues from. A
// displaced marker is errMQTTTeardownAdoptionSuperseded. An unresolved outcome is classified by
// the RECEIPT, never by the marker's nonce (the nonce predates this batch): the receipt slot is
// create-only and lands only inside such a batch, so a receipt for this generation carrying this
// marker's nonce and write identity proves a batch landed — this one or a peer adopter's.
func (as *mqttAccountSessionManager) claimTeardownReceipt(ps *mqttPersistedSession, idHash string, seq uint64,
	auth mqttAuthorityLoad) (uint64, error) {

	if ps.Teardown == nil || auth.status != mqttOutboundValid || auth.rec == nil ||
		auth.rec.State != mqttAuthorityServing || auth.rec.Gen != ps.Gen {
		return 0, fmt.Errorf("the receiptless continuation of session %q needs its marker and an authority serving its generation", ps.ID)
	}
	smsg, lerr := as.jsa.loadSessionMsg(as.domainTk, idHash)
	if lerr != nil {
		return 0, fmt.Errorf("reading the teardown marker of session %q: %w", ps.ID, lerr)
	}
	if smsg.Sequence != seq {
		return 0, fmt.Errorf("the teardown marker of session %q moved from %d to %d: %w",
			ps.ID, seq, smsg.Sequence, errMQTTTeardownAdoptionSuperseded)
	}
	target := mqttTeardownTargetFromRecord(ps, idHash)
	// The retired record is the marker's PARENT (ps.PWID); the sequence it stood at is not
	// recoverable from the marker, so SessSeq is the PRE-BATCH MARKER's sequence — not that
	// record's (see mqttRetirementRequest.SessSeq; consult 122).
	retire := mqttRetirementRequest{
		Gen: ps.Gen, Tenure: auth.rec.Tenure, OwnerTxn: auth.rec.OwnerTxn,
		SessWID: ps.PWID, SessSeq: seq,
		Reason: mqttRetireSessionEnd, Discard: true,
		AffineDurable: target.q2AffDur, AffineGen: target.q2AffGen, PubRelDurable: target.pubRelDur,
	}
	rcpt, cerr := mqttComposeTeardownReceipt(ps.Gen, ps.Teardown.Nonce, ps.WID, retire)
	if cerr != nil {
		return 0, cerr
	}
	body, merr := json.Marshal(rcpt)
	if merr != nil {
		return 0, merr
	}
	if h := mqttTestReceiptContinuationHookV.Load(); h != nil {
		(*h)(ps.ID)
	}
	subj := mqttTeardownReceiptSubject(as.domainTk, idHash, ps.Gen)
	newSeq, serr := as.jsa.storeTeardownClaimBatch(as.domainTk, idHash, seq, subj, body, smsg.Data)
	if serr == nil {
		return newSeq, nil
	}
	// Unresolved or refused: the receipt decides whether a batch landed.
	r, _, rst := as.jsa.loadTeardownReceipt(as.domainTk, idHash, ps.Gen)
	switch rst {
	case mqttOutboundValid:
		if r.Nonce != ps.Teardown.Nonce || r.MarkerWID != ps.WID {
			return 0, fmt.Errorf("a receipt for generation %q names another teardown: %w", ps.Gen, errMQTTTeardownAdoptionSuperseded)
		}
		cur, cerr := as.jsa.loadSessionMsg(as.domainTk, idHash)
		if cerr != nil {
			return 0, fmt.Errorf("resolving the receipt continuation of session %q: %w", ps.ID, cerr)
		}
		cps := &mqttPersistedSession{}
		if uerr := json.Unmarshal(cur.Data, cps); uerr != nil || cps.Teardown == nil ||
			cps.Teardown.Nonce != ps.Teardown.Nonce || cps.WID != ps.WID {
			return 0, fmt.Errorf("the marker of session %q no longer stands after its receipt landed: %w",
				ps.ID, errMQTTTeardownAdoptionSuperseded)
		}
		return cur.Sequence, nil
	case mqttOutboundAbsent:
		// No batch landed. A marker that is no longer the one this adoption holds means the
		// record moved on — another teardown re-claimed it, or a newer record stands — and the
		// adoption is not ours to finish; an unchanged marker is plain contention, retried by
		// the next adopter.
		if cur, cerr := as.jsa.loadSessionMsg(as.domainTk, idHash); cerr == nil && cur.Sequence != seq {
			return 0, fmt.Errorf("the marker of session %q moved from %d to %d before its receipt landed: %w",
				ps.ID, seq, cur.Sequence, errMQTTTeardownAdoptionSuperseded)
		}
		return 0, fmt.Errorf("the receipt continuation of session %q did not land: %w", ps.ID, serr)
	default:
		return 0, fmt.Errorf("the receipt continuation of session %q is unresolved (%v): %w", ps.ID, rst, serr)
	}
}
