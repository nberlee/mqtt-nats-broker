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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/nats-io/nuid"
)

// errMQTTHandoffPredecessorStale marks a handoff's metadata commit refused because the session
// record it proposes to displace is no longer the one the proposal named — it moved under the
// acceptance, was torn down, or now carries a different identity. It is a RE-DRIVABLE condition,
// not a failure of the CONNECT: nothing landed (the three-member batch is atomic and this
// pre-check refuses before the send), so the accepting CONNECT re-reads the authoritative record
// and re-drives the handoff against what now stands, exactly as a record-CAS rejection does. The
// rival that moved the record is the one the re-drive adopts.
var errMQTTHandoffPredecessorStale = errors.New("the handoff predecessor session record is no longer the one proposed")

// errMQTTForkFenceMoved is an accepting CONNECT's handoff preparation finding the outbound authority
// past the sequence fork repair read its consumers under (F-105 7f): something — a disposal among
// them — wrote it since, so what the merge reinstated is no longer proven to serve. Nothing landed;
// the CONNECT re-drives from the authoritative restore, which re-runs fork repair on fresh reads.
var errMQTTForkFenceMoved = errors.New("the outbound authority moved past fork repair's reads")

// mqttTestForkFenceRedriveV observes the accept loop taking the fork-fence re-drive. nil in
// production.
var mqttTestForkFenceRedriveV atomic.Pointer[func(clientID string)]

// The metadata half of an ownership proposal (F-105).
//
// The accepted CONNECT's atomic batch is the handoff's metadata commit. It already commits
// the session record and the permanent acceptance fact together — that pairing exists to
// remove the interval in which a session is accepted and its evidence is not yet durable —
// and it gains a third member here: the permanent `METADATA_COMMITTED(I)` for the handoff.
//
// INDIVISIBILITY MEANS EXTENDING THE TRANSACTION, NOT KEEPING ITS MEMBER COUNT AT TWO. Both
// load-bearing properties of the existing batch survive: the fact and the record commit
// together, and the fact's sequence is below the record's while the acknowledgement names
// the record.
//
// THE FROZEN ATTEMPT is the other half of this file, and it is a correction to how the
// existing loop works. Today the acceptance is recomposed on every retry iteration, which is
// right when a retry is "write this again" and wrong once the batch carries a CREATE-ONLY
// member: a recomposed attempt would mint a new intent, and the permanent slot for the old
// one would be left to be resolved by nobody. A retry therefore resends the SAME frozen
// logical batch — same intent, same write identity, same acceptance id and time, same bodies
// and expectations — under a fresh transport batch id, because reusing a batch id while one
// is still staged abandons it.
//
// What a rejection means also changes, and a caller that does not know this will misread it.
// The batch previously had one create-only member, so a rejection with the fact absent meant
// the record's expectation failed. Now there are two, and the second has a legitimate
// COMPETING WRITER: another resolver may have installed `METADATA_ABORTED(I)`. A rejection
// is therefore classified from the permanent slot rather than inferred from what is absent.

// mqttFrozenMetadataAttempt is one composed, immutable metadata commit.
//
// Composed once and resent unchanged. Nothing in it is regenerated between attempts; only
// the transport framing is.
type mqttFrozenMetadataAttempt struct {
	// The proposal this attempt commits the metadata half of, in either shape.
	Proposal mqttOwnershipProposal
	// Outcome is the permanent receipt, built from the intent at composition time. It
	// carries no sequence, because the body is assembled before the batch that would
	// produce one.
	Outcome *mqttHandoffOutcome

	DomainTk string
	CIDHash  string

	// FactSubj and FactBody are the permanent acceptance fact, create-only.
	FactSubj string
	FactBody []byte
	// BaseSeq is the session record's expected base, and RecBody the record.
	BaseSeq uint64
	RecBody []byte
}

// mqttFreezeMetadataAttempt composes the attempt once.
//
// The outcome is derived from the proposal here rather than supplied, so a caller cannot hand
// in a receipt describing a proposal other than the one being committed — including one of
// the other SHAPE, since the kind comes from the proposal too.
//
// baseSeq is deliberately unconstrained by the kind. A first creation over no record passes
// zero, which frames the session record create-only; a fresh proposal over a CLOSED
// generation is creation-shaped and passes the standing record's sequence. Both are ordinary,
// so neither is asserted.
func mqttFreezeMetadataAttempt(id, epoch, domainTk string, p mqttOwnershipProposal,
	sessWID string, factSubj string, factBody []byte, baseSeq uint64, recBody []byte) (*mqttFrozenMetadataAttempt, error) {

	if !mqttValidProposalKind(p.Kind) || !mqttF105IdentTk(p.ID) {
		return nil, fmt.Errorf("freezing a metadata attempt requires a constructed proposal")
	}
	if sessWID == _EMPTY_ {
		return nil, fmt.Errorf("freezing a metadata attempt requires the write identity it installs")
	}
	if factSubj == _EMPTY_ || len(factBody) == 0 || len(recBody) == 0 {
		return nil, fmt.Errorf("freezing a metadata attempt requires both bodies and the fact subject")
	}
	// A HANDOFF'S SESSION CAS IS ITS PREDECESSOR'S. The receipt copies PrevSessSeq and
	// PrevSessWID, so if the expectation the batch actually writes under were some other
	// sequence, a delayed handoff naming session A could overwrite whatever stands there now
	// — session B — and publish a receipt claiming it displaced A. The resolver would accept
	// that receipt, because it compares the PROPOSAL's fields and not the record that was
	// actually replaced. A creation displaces nothing and is deliberately unconstrained.
	if p.Kind == mqttProposalHandoff && baseSeq != p.PrevSessSeq {
		return nil, fmt.Errorf("a handoff naming predecessor session record %d cannot commit "+
			"against expectation %d: the receipt and the write must name one record",
			p.PrevSessSeq, baseSeq)
	}
	cidHash := getHash(id)
	o, oerr := mqttCommittedOutcomeFor(id, epoch, p, sessWID)
	if oerr != nil {
		return nil, oerr
	}
	return &mqttFrozenMetadataAttempt{
		Proposal: p, Outcome: o, DomainTk: domainTk, CIDHash: cidHash,
		FactSubj: factSubj, FactBody: factBody, BaseSeq: baseSeq, RecBody: recBody,
	}, nil
}

// compose frames the three members under a FRESH transport batch id.
//
// Ordering is deliberate and preserves what the two-member batch established: the acceptance
// fact first so its sequence is below the record's, the metadata receipt second, and the
// session record last carrying the commit so the acknowledgement names the record whose
// sequence the session adopts.
func (f *mqttFrozenMetadataAttempt) compose() ([]mqttBatchMsg, string, error) {
	outBody, err := json.Marshal(f.Outcome)
	if err != nil {
		return nil, _EMPTY_, err
	}
	batchID := nuid.Next()
	fb, fh := mqttFrameBatchWrite(0, f.FactBody, batchID, 1, false)
	ob, oh := mqttFrameBatchWrite(0, outBody, batchID, 2, false)
	rb, rh := mqttFrameBatchWrite(f.BaseSeq, f.RecBody, batchID, 3, true)
	return []mqttBatchMsg{
		{subj: f.FactSubj, hdr: fh, msg: fb},
		{subj: mqttHandoffOutcomeSubject(f.DomainTk, f.CIDHash, f.Proposal.ID), hdr: oh, msg: ob},
		{subj: mqttSessSubject(f.DomainTk, f.CIDHash), hdr: rh, msg: rb},
	}, batchID, nil
}

// mqttMetadataVerdict is what a metadata half's permanent slot says.
type mqttMetadataVerdict int

const (
	// mqttMetadataUnresolved is the honest answer when nothing positive stands. The batch
	// may still be in flight and may still commit, so no caller may act as though it will
	// not.
	mqttMetadataUnresolved mqttMetadataVerdict = iota
	// mqttMetadataCommitted reports a matching committed receipt.
	mqttMetadataCommitted
	// mqttMetadataAborted reports a matching aborted receipt, which can never become a
	// commit afterwards because the slot is taken.
	mqttMetadataAborted
	// mqttMetadataForeign reports a receipt that does not answer this intent. A
	// contradiction, never a match.
	mqttMetadataForeign
)

// resolveMetadataAttempt classifies one proposal's metadata half from its permanent slot.
//
// The same slot answers a creation and a handoff, because both are one proposal over one
// client's ownership and splitting the slot by shape would give two of them independent
// verdicts over the same client.
func (jsa *mqttJSA) resolveMetadataAttempt(domainTk, id, epoch string, p mqttOwnershipProposal) mqttMetadataVerdict {
	if !mqttValidProposalKind(p.Kind) || p.ID == _EMPTY_ {
		return mqttMetadataUnresolved
	}
	subject := mqttHandoffOutcomeSubject(domainTk, getHash(id), p.ID)
	smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if err != nil {
		return mqttMetadataUnresolved
	}
	o, derr := mqttDecodeHandoffOutcome(smsg.Data, id, epoch, p.ID)
	if derr != nil {
		return mqttMetadataForeign
	}
	if merr := mqttOutcomeAnswersProposal(o, p); merr != nil {
		return mqttMetadataForeign
	}
	if o.Verdict == mqttHandoffAborted {
		return mqttMetadataAborted
	}
	return mqttMetadataCommitted
}

// abortMetadataAttempt takes the handoff's permanent slot with an ABORTED verdict.
//
// Create-only, racing the frozen attempt's own create-only member, so exactly one can exist.
// A FAILED abort establishes only that the slot is occupied — by a commit, by another
// resolver's abort, or not at all if the failure was transport. The caller re-reads; this
// returns the error rather than a verdict for that reason.
func (jsa *mqttJSA) abortMetadataAttempt(domainTk, id, epoch string, p mqttOwnershipProposal) error {
	if !mqttValidProposalKind(p.Kind) || !mqttF105IdentTk(p.ID) {
		return fmt.Errorf("aborting a metadata attempt requires a constructed proposal")
	}
	o := &mqttHandoffOutcome{
		ID: id, Epoch: epoch, Intent: p.ID, Verdict: mqttHandoffAborted, Kind: p.Kind,
		PrevGen: p.PrevGen, PrevTenure: p.PrevTenure, PrevOwnerTxn: p.PrevOwnerTxn,
		PrevSessWID: p.PrevSessWID, PrevSessSeq: p.PrevSessSeq,
	}
	if verr := mqttValidateHandoffOutcome(o, id, epoch, p.ID); verr != nil {
		return fmt.Errorf("refusing to abort with an unsupported receipt: %w", verr)
	}
	body, _ := json.Marshal(o)
	msg, hdr := mqttFrameBatchWrite(0, body, _EMPTY_, 0, false)
	_, err := jsa.storeMsg(mqttHandoffOutcomeSubject(domainTk, getHash(id), p.ID), hdr, msg)
	return err
}

// commitMetadataAttempt sends the frozen attempt and returns the SESSION RECORD's sequence.
//
// Every retry calls this with the SAME attempt: a fresh transport batch id is minted inside,
// and nothing semantic is regenerated. A caller that recomposed instead would mint a new
// proposal and orphan the permanent slot of the old one.
func (jsa *mqttJSA) commitMetadataAttempt(f *mqttFrozenMetadataAttempt) (uint64, error) {
	if f == nil {
		return 0, fmt.Errorf("unreachable: committing a nil metadata attempt")
	}
	// THE ATTEMPT IS VALIDATED HERE, not merely where it was frozen. The freezer is a
	// helper and the frozen attempt is a mutable struct, so every binding it establishes
	// holds only for callers that used it and did not touch the result afterwards. This is
	// the boundary the batch is actually composed at, so it is where the receipt has to be
	// shown to describe the batch.
	if err := mqttCheckFrozenMetadataAttempt(f); err != nil {
		return 0, err
	}
	// AND THE PROPOSAL MUST BE THE CLIENT'S OWN STANDING ONE. Deriving the receipt from the
	// attempt proves the attempt is internally consistent; it says nothing about whether
	// this proposal was ever claimed. Without this a caller invents a creation proposal,
	// supplies the current session sequence and a replacement body at a generation of its
	// choosing, and commits over a LIVE session having reserved nothing — and a creation
	// displaces nothing, so the predecessor check does not reach it either.
	if err := jsa.checkProposalProvenance(f); err != nil {
		return 0, err
	}
	// AND THE RECORD AT THAT SEQUENCE MUST BE THE PREDECESSOR THE PROPOSAL NAMES. The
	// expectation alone says only "this position"; a position is not an identity, and the
	// session record is a single-slot register every confirmed save advances. Checking the
	// stored write identity and generation is what makes "this handoff displaced THAT
	// session" true rather than merely likely.
	if f.Proposal.Kind == mqttProposalHandoff {
		if err := jsa.checkHandoffPredecessorSession(f); err != nil {
			return 0, err
		}
	}
	msgs, batchID, err := f.compose()
	if err != nil {
		return 0, err
	}
	// The observation seam, armed only by the sentinel that proves this THREE-member batch
	// — the acceptance fact, the COMMITTED receipt and the session record — is indivisible
	// over a real replicated stream. Inert and free when unarmed. Fired here, before the
	// send, so an interceptor can tear the batch and the paired mutation is reachable.
	if oerr := jsa.observeMetadataCommit(mqttAcceptStoreIssuing, batchID, msgs, 0); oerr != nil {
		return 0, oerr
	}
	bi, err := jsa.newAtomicBatchRequest(mqttJSAMsgStore, f.CIDHash, msgs)
	if err != nil {
		return 0, err
	}
	resp := bi.(*JSPubAckResponse)
	if aerr := resp.ToError(); aerr != nil {
		return 0, aerr
	}
	if resp.PubAck == nil {
		return 0, fmt.Errorf("the metadata commit for intent %q was acknowledged with no publish acknowledgement",
			f.Proposal.ID)
	}
	if resp.BatchId != batchID || resp.BatchSize != uint64(len(msgs)) {
		return 0, fmt.Errorf("the metadata commit for intent %q was acknowledged as %d message(s) under batch %q, but this broker sent %d under %q",
			f.Proposal.ID, resp.BatchSize, resp.BatchId, len(msgs), batchID)
	}
	if resp.Stream != mqttSessStreamName {
		return 0, fmt.Errorf("the metadata commit for intent %q was acknowledged on stream %q (want %q)",
			f.Proposal.ID, resp.Stream, mqttSessStreamName)
	}
	// Fired only once the acknowledgement has been validated as THIS batch's, so the
	// sequence handed to an observer is the SESSION RECORD's and not merely the one that
	// came back.
	if oerr := jsa.observeMetadataCommit(mqttAcceptStoreAcked, batchID, msgs, resp.Sequence); oerr != nil {
		return 0, oerr
	}
	return resp.Sequence, nil
}

// settleProposal drives one proposal's metadata half to a PERMANENT verdict.
//
// THE ONE SETTLEMENT EXECUTOR, used both by an acceptance settling its own proposal and by
// one settling another claimant's. They are the same operation: the slot is create-only and
// does not know who is writing, so a second implementation would only be a second place for
// the ordering to drift.
//
// The order is the whole thing:
//
//  1. RESOLVE FIRST. A standing verdict is permanent, and attempting an abort over one is
//     both pointless and misleading — its failure would be read as "occupied by something"
//     when the answer was already known.
//  2. Only an UNRESOLVED slot is aborted, create-only, racing the original claimant's own
//     create-only commit member. This is not a liveness claim: a live claimant may lose it,
//     and losing tells it so rather than leaving it to infer from an absence.
//  3. RE-RESOLVE, and return THAT. The abort's own error is never the verdict — a failed
//     abort establishes only that the slot is occupied, and by a commit, by another
//     resolver's abort, or not at all if the failure was transport. Only a validated
//     readback distinguishes them.
//
// A FOREIGN or still-UNRESOLVED result licenses no authority transition. Unresolved after an
// abort attempt means the readback itself failed; the batch may yet commit.
func (jsa *mqttJSA) settleProposal(domainTk, id, epoch string, p mqttOwnershipProposal) mqttMetadataVerdict {
	if v := jsa.resolveMetadataAttempt(domainTk, id, epoch, p); v != mqttMetadataUnresolved {
		return v
	}
	// The error is deliberately discarded HERE and not propagated as a verdict: what the
	// slot says next is the only answer, and the re-resolve is what asks.
	_ = jsa.abortMetadataAttempt(domainTk, id, epoch, p)
	return jsa.resolveMetadataAttempt(domainTk, id, epoch, p)
}

// mqttCheckFrozenMetadataAttempt requires a metadata attempt's RECEIPT to describe the batch
// it travels with, and both to describe the proposal.
//
// THE RECEIPT IS DERIVED, NOT COMPARED FIELD BY FIELD. It is rebuilt from the proposal and
// the write identity the session record actually installs, and the caller's receipt must
// equal those bytes. A field list would silently stop covering whatever member is added to
// the outcome next, which is the same failure mode byte equality was chosen for at the
// authority boundary.
//
// What this closes: the freezer accepted any non-empty record body, so an attempt naming
// proposal P could carry the PREDECESSOR's session body — its old write identity and
// generation — and still publish a permanent COMMITTED receipt saying P installed a new
// one. The receipt is what every later ownership move reads, so the authority would be
// finalized over a session that never moved. The same hole let the outcome be substituted
// after freezing, and let a handoff commit against an expectation other than the record its
// receipt names.
func mqttCheckFrozenMetadataAttempt(f *mqttFrozenMetadataAttempt) error {
	p := f.Proposal
	if !mqttValidProposalKind(p.Kind) || !mqttF105IdentTk(p.ID) {
		return fmt.Errorf("a metadata attempt carries no constructed proposal")
	}
	if f.Outcome == nil {
		return fmt.Errorf("the metadata attempt for proposal %q carries no receipt", p.ID)
	}
	id, epoch := f.Outcome.ID, f.Outcome.Epoch
	if f.CIDHash != getHash(id) {
		return fmt.Errorf("the metadata attempt for proposal %q is routed to %q, which is not "+
			"client %q's subject tree", p.ID, f.CIDHash, id)
	}
	// A HANDOFF'S SESSION CAS IS ITS PREDECESSOR'S, enforced at the write and not only at
	// composition: the receipt copies PrevSessSeq, so committing against another position
	// would publish a permanent claim to have displaced a record it never touched.
	if p.Kind == mqttProposalHandoff && f.BaseSeq != p.PrevSessSeq {
		return fmt.Errorf("the handoff for proposal %q names predecessor session record %d but "+
			"commits against expectation %d: the receipt and the write must name one record",
			p.ID, p.PrevSessSeq, f.BaseSeq)
	}

	// THE ACCEPTANCE FACT IS THIS ACCEPTANCE'S, and it is validated through the one decoder
	// that applies its closed schema, against the subject the batch will actually publish
	// it on. A fact on another client's or another incarnation's subject would make the
	// suppression evidence this batch establishes belong to somebody else.
	fact, ferr := mqttDecodeAcceptFact(f.DomainTk, epoch, &StoredMsg{Subject: f.FactSubj, Data: f.FactBody})
	if ferr != nil {
		return fmt.Errorf("the metadata attempt for proposal %q carries an unsupported "+
			"acceptance fact: %w", p.ID, ferr)
	}
	if fact.ClientID != id {
		return fmt.Errorf("the metadata attempt for proposal %q carries client %q's acceptance fact",
			p.ID, fact.ClientID)
	}

	// THE SESSION RECORD IS THE ONE THE PROPOSAL PROPOSES. Its generation is the proposed
	// generation and its write identity is the one the receipt reports, because the receipt
	// is the only durable statement of what this commit installed.
	rec := &mqttPersistedSession{}
	if derr := json.Unmarshal(f.RecBody, rec); derr != nil {
		return fmt.Errorf("the metadata attempt for proposal %q carries a session record it "+
			"cannot read back: %w", p.ID, derr)
	}
	if rec.ID != id {
		return fmt.Errorf("the metadata attempt for proposal %q installs client %q's session record",
			p.ID, rec.ID)
	}
	if rec.Gen != p.Gen {
		return fmt.Errorf("the metadata attempt for proposal %q installs a session record at "+
			"generation %q but proposes %q", p.ID, rec.Gen, p.Gen)
	}
	if rec.WID == _EMPTY_ {
		return fmt.Errorf("the metadata attempt for proposal %q installs a session record with "+
			"no write identity", p.ID)
	}
	if p.Kind == mqttProposalHandoff && rec.WID == p.PrevSessWID {
		return fmt.Errorf("the metadata attempt for proposal %q installs write identity %q, "+
			"which is the predecessor's: a handoff that reinstalls the record it displaces "+
			"has displaced nothing", p.ID, rec.WID)
	}

	want, werr := mqttCommittedOutcomeFor(id, epoch, p, rec.WID)
	if werr != nil {
		return werr
	}
	gotBody, gerr := json.Marshal(f.Outcome)
	if gerr != nil {
		return gerr
	}
	wantBody, wberr := json.Marshal(want)
	if wberr != nil {
		return wberr
	}
	if !bytes.Equal(gotBody, wantBody) {
		return fmt.Errorf("the metadata attempt for proposal %q carries a receipt that is not "+
			"the one its proposal and its session record produce", p.ID)
	}
	return nil
}

// mqttCommittedOutcomeFor builds the ONE committed receipt a proposal and the write identity
// it installs produce. Both the freezer and the write boundary derive from it, so there is
// one definition of what a commit reports rather than a builder and a checker that can drift.
func mqttCommittedOutcomeFor(id, epoch string, p mqttOwnershipProposal,
	sessWID string) (*mqttHandoffOutcome, error) {

	o := &mqttHandoffOutcome{
		ID: id, Epoch: epoch, Intent: p.ID, Verdict: mqttHandoffCommitted, Kind: p.Kind,
		PrevGen: p.PrevGen, PrevTenure: p.PrevTenure, PrevOwnerTxn: p.PrevOwnerTxn,
		PrevSessWID: p.PrevSessWID, PrevSessSeq: p.PrevSessSeq,
		SessWID: sessWID, ProposedGen: p.Gen, ProposedTenure: p.Tenure,
	}
	if verr := mqttValidateHandoffOutcome(o, id, epoch, p.ID); verr != nil {
		return nil, fmt.Errorf("refusing an unsupported metadata receipt: %w", verr)
	}
	return o, nil
}

// checkProposalProvenance requires the proposal a metadata commit answers to be the one
// STANDING on the client's own authority.
//
// A creation's proposal must be a RESERVING authority's, and a handoff's a PREPARING
// authority's, with the identity matching. That is the ordering the whole mechanism rests on:
// the claim is written first and serves nothing, and the metadata half is what makes it
// servable — so a metadata commit for a proposal no authority carries is a commit for a claim
// that was never made.
//
// Read before the batch rather than trusted: the authority can move afterwards, and the
// create-only metadata slot remains the fence that arbitrates that race. What this adds is
// that a proposal nobody ever claimed is refused outright, rather than committing and waiting
// for a finalization that no reservation will ever license.
func (jsa *mqttJSA) checkProposalProvenance(f *mqttFrozenMetadataAttempt) error {
	p := f.Proposal
	id, epoch := f.Outcome.ID, f.Outcome.Epoch
	load := jsa.loadOutboundAuthority(id, epoch)
	if load.status != mqttOutboundValid {
		return fmt.Errorf("the metadata half of proposal %q cannot commit: client %q's "+
			"authority reads %v, so nothing establishes that this proposal was ever claimed",
			p.ID, id, load.status)
	}
	a := load.rec
	// THE STANDING PROPOSAL IS VIEWED, AND THE WHOLE VIEW IS COMPARED. A field list would
	// leave the predecessor members uncovered, and those are not decoration: a receipt
	// naming a PrevTenure or a PrevOwnerTxn the standing intent does not carry passes the
	// session-record check — that one compares the WID and the generation — takes the
	// permanent COMMITTED slot, and is then read as FOREIGN by every later resolution,
	// because a receipt answers a proposal only if it answers all of it. Neither
	// finalization nor cancellation could proceed, and admission would stay frozen for good.
	var want mqttOwnershipProposal
	var werr error
	switch p.Kind {
	case mqttProposalCreation:
		if a.State != mqttAuthorityReserving || a.Creation == nil {
			return fmt.Errorf("the metadata half of creation %q cannot commit: client %q's "+
				"authority is in state %d and carries no reservation", p.ID, id, a.State)
		}
		want, werr = mqttProposalFromCreation(a.Creation)
	case mqttProposalHandoff:
		if a.State != mqttAuthorityPreparing || a.Intent == nil {
			return fmt.Errorf("the metadata half of handoff %q cannot commit: client %q's "+
				"authority is in state %d and carries no preparation", p.ID, id, a.State)
		}
		want, werr = mqttProposalFromHandoff(a.Intent)
	default:
		return fmt.Errorf("the metadata half of proposal %q carries an unsupported shape", p.ID)
	}
	if werr != nil {
		return fmt.Errorf("client %q carries a standing proposal this broker cannot interpret: %w", id, werr)
	}
	if want != p {
		return fmt.Errorf("the metadata half of proposal %q is not the one client %q's "+
			"authority carries: the standing proposal is %+v and this attempt commits %+v",
			p.ID, id, want, p)
	}
	return nil
}

// checkHandoffPredecessorSession verifies the session record a handoff proposes to displace.
//
// Read immediately before the batch rather than trusted from composition time: a stale read
// would be refused by the batch's own expectation anyway, so the only thing this can do is
// refuse EARLIER and for the right reason. What it adds over the expectation is IDENTITY —
// the sequence says which position, the write identity and generation say which session.
func (jsa *mqttJSA) checkHandoffPredecessorSession(f *mqttFrozenMetadataAttempt) error {
	subject := mqttSessSubject(f.DomainTk, f.CIDHash)
	smsg, lerr := jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if lerr != nil {
		// The predecessor subject is unreadable — a winner tore it down, most often — so the
		// record the proposal named is gone. Re-drivable: the re-drive finds it absent and
		// takes the creation arm.
		return fmt.Errorf("the handoff for proposal %q names a predecessor session record that "+
			"could not be read: %w: %w", f.Proposal.ID, errMQTTHandoffPredecessorStale, lerr)
	}
	if smsg.Sequence != f.BaseSeq {
		return fmt.Errorf("the handoff for proposal %q is conditioned on session record %d but "+
			"it stands at %d: %w", f.Proposal.ID, f.BaseSeq, smsg.Sequence, errMQTTHandoffPredecessorStale)
	}
	ps := &mqttPersistedSession{}
	if derr := json.Unmarshal(smsg.Data, ps); derr != nil {
		return fmt.Errorf("the handoff for proposal %q cannot read the session record it "+
			"displaces: %w", f.Proposal.ID, derr)
	}
	if ps.WID != f.Proposal.PrevSessWID {
		return fmt.Errorf("the handoff for proposal %q names predecessor write identity %q but "+
			"the standing record carries %q: a sequence is a position, not an identity: %w",
			f.Proposal.ID, f.Proposal.PrevSessWID, ps.WID, errMQTTHandoffPredecessorStale)
	}
	if ps.Gen != f.Proposal.PrevGen {
		return fmt.Errorf("the handoff for proposal %q names predecessor generation %q but the "+
			"standing record carries %q: %w", f.Proposal.ID, f.Proposal.PrevGen, ps.Gen, errMQTTHandoffPredecessorStale)
	}
	return nil
}
