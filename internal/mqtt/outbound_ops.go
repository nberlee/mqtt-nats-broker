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
	"fmt"
)

// THE OPERATION SURFACE (F-105).
//
// This is what a caller of the outbound arbitration mechanism uses, and it is deliberately
// the ONLY thing a caller uses. Every entry point here takes the operation's own arguments —
// identities, a payload, the evidence it rests on — loads the state it reasons about from
// the store, derives the successor records itself, composes the whole atomic batch, and
// commits it.
//
// # Why the surface is shaped this way
//
// Four consecutive review rounds produced one finding each time, and every instance had the
// same shape: a caller composed a durable CONCLUSION — an authority body, a predecessor, a
// resource record, a metadata receipt, a raw (subject, expectation, body) member — and some
// rule about what that conclusion was allowed to say lived in the optional helper that built
// it rather than in the writer. Each fix moved one rule to the write boundary. The class does
// not end there, because the boundary was proving a caller's conclusion permissible rather
// than deriving it.
//
// The distinction this file draws is CALLER INTENT versus CALLER-AUTHORED DURABLE
// CONCLUSIONS. A caller may say "admit this PUBLISH", "this exchange observed a PUBREC",
// "release this obligation's edge on that consumer", and hand over the identities and the
// payload that go with it. It may not hand over the authority record that results, the
// predecessor it was derived from, the generation closure, the metadata receipt, or a member
// of its own choosing.
//
// # The write boundary does not go away
//
// commitAuthorityTransition still re-derives every fact about stored state, and its rules are
// still the enforcement point. Two layers, doing different jobs: the operations own
// COMPOSITION, so a caller cannot state a conclusion; the boundary owns ENFORCEMENT, so a
// composition bug cannot land. The boundary's own tests drive it directly with hand-built
// transitions for exactly that reason — they are the defence being tested.
//
// # What this surface does NOT establish, and the caller must
//
// Stated here because a precondition nobody wrote down is a precondition nobody keeps. The
// boundary enforces relations between DURABLE RECORDS. It cannot enforce a fact about the
// world that no record carries, and three of those are load-bearing:
//
//   - OUTSTANDING is the caller's. opAcquireAction is told whether another action for this
//     exchange may still emit, and the completion rules act on that answer. Immutable action
//     records cannot establish it: an acquired action that has finished looks exactly like one
//     still executing. A caller that cannot answer must say TRUE, because an unknown action is
//     an outstanding one.
//   - THE PHASE AN ADVANCE SELECTS is the caller's. opAdvanceExchange is told which phase the
//     evidence establishes; the boundary checks only that the move is one the protocol
//     permits from where the exchange stands. Whether a PUBREC really arrived, and whether it
//     succeeded or was refused, is the caller's reading of the packet.
//   - TERMINAL VERSUS TERMINAL-PENDING-DRAIN is the same question in its sharpest form: the
//     difference between them is whether output can still execute, which is a fact about a
//     socket and a scheduler, not about a record.
//
// The conversion rows own these: actual acknowledgement evidence, serialized output
// acquisition, a trustworthy Outstanding, drain and closure evidence, complete backing-resource
// selection, and the physical cleanup result a completion reports. Register REPLACEMENT is
// theirs too, and must reuse the slot state machine's decision rather than restate it.
//
// NOT WIRED. No production path calls these yet; the conversion rows in docs/ROADMAP.md are
// where that happens, and they consume this surface rather than the transition struct.

// ---------------------------------------------------------------------------
// Loaders the operations derive from
// ---------------------------------------------------------------------------

// loadOutboundExchange reads one exchange as it durably stands.
func (jsa *mqttJSA) loadOutboundExchange(id, epoch, gen, nonce string) (*mqttOutboundExchange, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(getHash(id), gen, nonce))
	if err != nil {
		return nil, 0, fmt.Errorf("exchange %q of generation %q could not be read: %w", nonce, gen, err)
	}
	x, derr := mqttDecodeOutboundExchange(smsg.Data, id, epoch, gen, nonce)
	if derr != nil {
		return nil, 0, fmt.Errorf("exchange %q of generation %q is unsupported: %w", nonce, gen, derr)
	}
	return x, smsg.Sequence, nil
}

// loadOutboundResource reads one backing resource as it durably stands.
func (jsa *mqttJSA) loadOutboundResource(id, epoch, gen, inc string) (*mqttOutboundResource, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, mqttResourceSubject(getHash(id), gen, inc))
	if err != nil {
		return nil, 0, fmt.Errorf("resource %q of generation %q could not be read: %w", inc, gen, err)
	}
	r, derr := mqttDecodeOutboundResource(smsg.Data, id, epoch, gen, inc)
	if derr != nil {
		return nil, 0, fmt.Errorf("resource %q of generation %q is unsupported: %w", inc, gen, derr)
	}
	return r, smsg.Sequence, nil
}

// requireAuthority loads the standing authority and refuses every reading that is not a
// record this broker can reason about.
//
// ABSENT is returned as a nil authority rather than an error, because two operations — the
// first claim and nothing else — legitimately act on it. CORRUPT and INDETERMINATE are
// errors and are never treated as absent: a record this broker cannot read is not a record
// that is not there, and the whole mechanism exists to stop an absence licensing a claim.
func (jsa *mqttJSA) requireAuthority(id, epoch string) (*mqttOutboundAuthority, uint64, error) {
	load := jsa.loadOutboundAuthority(id, epoch)
	switch load.status {
	case mqttOutboundValid:
		return load.rec, load.seq, nil
	case mqttOutboundAbsent:
		return nil, 0, nil
	}
	return nil, 0, fmt.Errorf("the outbound authority for %q reads %v, which licenses nothing",
		id, load.status)
}

// servingAuthority loads the authority and requires it to be SERVING, which is what every
// operation that admits new work needs.
func (jsa *mqttJSA) servingAuthority(id, epoch string) (*mqttOutboundAuthority, uint64, error) {
	a, seq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return nil, 0, err
	}
	if a == nil {
		return nil, 0, fmt.Errorf("client %q has no outbound authority, so nothing may be "+
			"admitted for it: there is deliberately no missing-authority fallback", id)
	}
	if a.State != mqttAuthorityServing {
		return nil, 0, fmt.Errorf("the outbound authority for %q is in state %d and admits "+
			"no new work", id, a.State)
	}
	return a, seq, nil
}

// ---------------------------------------------------------------------------
// What an operation returns
// ---------------------------------------------------------------------------

// mqttOpOutcome is what every operation returns: the authority sequence its batch landed at,
// and a HANDLE for the attempt it sent.
//
// The handle is why this is a struct rather than a sequence. An operation composes its
// transition internally, so on a lost acknowledgement the caller holds neither that
// transition nor its digest — and calling the operation again is not a retry of the original
// attempt: it reloads the state, composes a DIFFERENT revision, and may be refused because
// the phase it asks for already stands. That is the ambiguity this whole mechanism exists to
// resolve, so the evidence to resolve it has to come back out.
//
// The handle is frozen BEFORE the batch is sent and is returned even when the send fails,
// because a transport error is exactly the case where it is needed: `resolveTransitionHandle`
// then answers LANDED, NEVER LANDED, SUPERSEDED or UNRESOLVED from the store alone.
type mqttOpOutcome struct {
	Seq    uint64
	Handle *mqttTransitionHandle
}

// commitOp freezes the attempt's handle, sends the batch, and returns both.
func (jsa *mqttJSA) commitOp(t *mqttAuthorityTransition) (mqttOpOutcome, error) {
	h, herr := mqttNewTransitionHandle(t)
	if herr != nil {
		return mqttOpOutcome{}, herr
	}
	seq, err := jsa.commitAuthorityTransition(t)
	return mqttOpOutcome{Seq: seq, Handle: h}, err
}

// ---------------------------------------------------------------------------
// Ownership
// ---------------------------------------------------------------------------

// opClaimClient reserves a client's ownership, over an absent predecessor or a CLOSED one.
//
// The reservation serves nothing: it grants no admission right, and only a committed
// metadata half followed by opFinalizeClaim makes it servable. The predecessor is LOADED —
// the caller says which client it is claiming, never what stands there now.
func (jsa *mqttJSA) opClaimClient(id, epoch, txn, gen, tenure, intentID string) (mqttOpOutcome, error) {
	prev, prevSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	rev := uint64(1)
	prevTxn := _EMPTY_
	if prev != nil {
		if prev.State != mqttAuthorityClosed {
			return mqttOpOutcome{}, fmt.Errorf("client %q carries an authority in state %d; only an absent "+
				"or a CLOSED one may be claimed over", id, prev.State)
		}
		rev, prevTxn = prev.Rev+1, prev.OwnerTxn
	}
	c := &mqttCreationIntent{ID: intentID, Gen: gen, Tenure: tenure}
	if verr := mqttValidateCreationIntent(c); verr != nil {
		return mqttOpOutcome{}, fmt.Errorf("refusing to claim %q on an unsupported proposal: %w", id, verr)
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionReserveCreation, PrevTxn: prevTxn, Rev: rev,
			Gen: gen, Tenure: tenure, OwnerTxn: txn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: gen, Tenure: tenure,
			State: mqttAuthorityReserving, Rev: rev, Txn: txn, OwnerTxn: txn, Creation: c,
		},
	})
}

// opFinalizeClaim turns a standing reservation into a serving authority.
//
// It installs exactly what the reservation proposed, and the caller names only WHICH proposal
// it is answering: the generation, the tenure and the ownership all come from the record.
// The metadata verdict is the write boundary's to check, and it refuses anything but a
// permanent COMMITTED one.
func (jsa *mqttJSA) opFinalizeClaim(id, epoch, txn, intentID string) (mqttOpOutcome, error) {
	prev, prevSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if prev == nil || prev.State != mqttAuthorityReserving || prev.Creation == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q carries no standing reservation to finalize", id)
	}
	c := prev.Creation
	if c.ID != intentID {
		return mqttOpOutcome{}, fmt.Errorf("the reservation on %q carries proposal %q, not %q", id, c.ID, intentID)
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionFinalizeCreation, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: c.Gen, Tenure: c.Tenure, OwnerTxn: prev.OwnerTxn, Intent: c.ID,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: c.Gen, Tenure: c.Tenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
	})
}

// opCloseGeneration ends ONE NAMED generation permanently.
//
// One operation covers both the ordinary closure of a serving generation and the abandonment
// of a reservation whose metadata half was positively aborted, because they are one durable
// act: the authority becomes CLOSED and a permanent closure record lands beside it. An
// abandoned reservation is never deleted — deleting it would let the next claimant's
// create-only write succeed against an absence — and the verdict that licenses the
// abandonment is the boundary's to check.
//
// THE CALLER NAMES THE OWNERSHIP IT MEANS — generation, tenure AND the transition that owns
// it — and this refuses if that is not the one standing. Loading the current authority says
// what is there; it does not say which owner the request belongs to.
//
// The generation alone is not enough, and the case that shows it is the ordinary one: a RESUME
// PRESERVES THE GENERATION and changes only the tenure. A closure captured under tenure A and
// delayed until tenure B has finished resuming would otherwise match on the generation and
// permanently close the generation B is serving.
func (jsa *mqttJSA) opCloseGeneration(id, epoch, txn, gen, tenure, ownerTxn string) (mqttOpOutcome, error) {
	prev, prevSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if prev == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q carries no authority, so there is no generation to close", id)
	}
	if prev.Gen != gen || prev.Tenure != tenure || prev.OwnerTxn != ownerTxn {
		return mqttOpOutcome{}, fmt.Errorf("closing %q/%q/%q for %q: the standing authority "+
			"carries %q/%q/%q, so this request belongs to an ownership that has moved on",
			gen, tenure, ownerTxn, id, prev.Gen, prev.Tenure, prev.OwnerTxn)
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionCloseAdmit, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityClosed, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
		},
		Closure: &mqttGenClosure{ID: id, Epoch: epoch, Gen: prev.Gen, Txn: txn},
	})
}

// opPrepareHandoff freezes admission and reserves a handoff from the serving authority.
//
// The INTENT is the caller's, because it describes what the accepting connection proposes —
// its generation, its tenure, and the retirement it requests. Everything derived from the
// PREDECESSOR is not: the intent's `Prev*` members are filled in from the standing authority
// and the session record the caller names, so a caller cannot propose a handoff from a
// predecessor that is not there.
func (jsa *mqttJSA) opPrepareHandoff(id, epoch, txn string, i *mqttHandoffIntent) (mqttOpOutcome, error) {
	prev, prevSeq, err := jsa.servingAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if i == nil {
		return mqttOpOutcome{}, fmt.Errorf("preparing a handoff for %q requires the intent it proposes", id)
	}
	next := *i
	next.PrevGen, next.PrevTenure, next.PrevOwnerTxn = prev.Gen, prev.Tenure, prev.OwnerTxn
	if next.Retire != nil {
		next.Retire.Gen, next.Retire.Tenure = prev.Gen, prev.Tenure
		next.Retire.OwnerTxn = prev.OwnerTxn
	}
	if verr := mqttValidateHandoffIntent(&next); verr != nil {
		return mqttOpOutcome{}, fmt.Errorf("refusing to prepare an unsupported handoff for %q: %w", id, verr)
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionPrepareIntent, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: mqttAuthorityPreparing, Rev: prev.Rev + 1, Txn: txn,
			OwnerTxn: prev.OwnerTxn, Intent: &next,
		},
	})
}

// opFinalizeHandoff installs the tenure a standing preparation proposed, and nothing else.
func (jsa *mqttJSA) opFinalizeHandoff(id, epoch, txn, intentID string) (mqttOpOutcome, error) {
	prev, prevSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if prev == nil || prev.State != mqttAuthorityPreparing || prev.Intent == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q carries no standing preparation to finalize", id)
	}
	i := prev.Intent
	if i.ID != intentID {
		return mqttOpOutcome{}, fmt.Errorf("the preparation on %q carries intent %q, not %q", id, i.ID, intentID)
	}
	t := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionFinalize, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: i.Gen, Tenure: i.Tenure, OwnerTxn: txn, Intent: i.ID,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: i.Gen, Tenure: i.Tenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: txn,
		},
	}
	// A handoff that DISCARDS its predecessor's generation closes it in the same batch, and
	// the closure carries the discard: a closure is the permanent record of WHY a generation
	// ended, and one that ended a clean start while saying it did not would tell a later
	// reader the session was resumable. The closure is derived from the intent's own
	// retirement request, never supplied.
	if i.Retire != nil && i.Retire.Discard {
		t.Closure = &mqttGenClosure{
			ID: id, Epoch: epoch, Gen: i.PrevGen, Txn: txn, Discard: true,
		}
	}
	return jsa.commitOp(t)
}

// opCancelHandoff returns a preparing authority to serving under its ORIGINAL ownership, on
// proof that the metadata half was positively aborted. The restored body comes from the
// intent's own record of what it displaced.
func (jsa *mqttJSA) opCancelHandoff(id, epoch, txn, intentID string) (mqttOpOutcome, error) {
	prev, prevSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if prev == nil || prev.State != mqttAuthorityPreparing || prev.Intent == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q carries no standing preparation to cancel", id)
	}
	i := prev.Intent
	if i.ID != intentID {
		return mqttOpOutcome{}, fmt.Errorf("the preparation on %q carries intent %q, not %q", id, i.ID, intentID)
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: "op-" + txn,
			Kind: mqttTransitionCancelIntent, Intent: i.ID,
			PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: i.PrevGen, Tenure: i.PrevTenure, OwnerTxn: i.PrevOwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: i.PrevGen, Tenure: i.PrevTenure,
			State: mqttAuthorityServing, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: i.PrevOwnerTxn,
		},
	})
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

// ---------------------------------------------------------------------------
// Obligations
// ---------------------------------------------------------------------------

// mqttAdmitPublishRequest is one outbound PUBLISH's admission, as its CALLER states it.
//
// What the caller supplies is the register the delivery path built — the payload, in the
// shape inflight_register.go already gives it — the identities of the transition, and the
// incarnations of the resources this obligation will need. What it does NOT supply is the
// authority it runs under, the exchange record, the marker, the resources' successor bodies,
// or any subject: all of those are derived here.
type mqttAdmitPublishRequest struct {
	ID    string
	Epoch string
	Txn   string
	Op    string

	// Gen and Tenure are the ENTITLEMENT THE CALLER CAPTURED when it decided to deliver.
	// They are compared with the authority as it stands rather than adopted from it: a
	// delivery decided under G1 and delayed until G2 serves would otherwise be admitted as
	// G2's work, under a tenure that never authorized it, and every boundary check would
	// pass because this operation had substituted the target.
	Gen    string
	Tenure string

	// Register is HEAD's in-flight record for this allocation. Its Lin IS the obligation's
	// identity and its PI and QoS are the exchange's.
	Register *mqttInflightRecord

	// DepIncs names the backing resources the obligation will need in order to complete,
	// by incarnation. Each is read here and its edge is taken in the same batch.
	DepIncs []string
}

// opAdmitPublish admits one outbound PUBLISH: the register and its unsent marker, the durable
// exchange, an edge on each backing resource, and the authority's own revision, in ONE batch.
func (jsa *mqttJSA) opAdmitPublish(r *mqttAdmitPublishRequest) (mqttOpOutcome, error) {
	if r == nil || r.Register == nil {
		return mqttOpOutcome{}, fmt.Errorf("admitting a publish requires its register")
	}
	// THE REGISTER IS AT ITS PUBLISH TOO. The exchange this derives is at the beginning of
	// its protocol, so a register in any other state would pair an obligation that has not
	// started with a slot that has — and the operation would derive the contradiction rather
	// than refuse it.
	if r.Register.State != mqttInflightPublish {
		return mqttOpOutcome{}, fmt.Errorf("admitting a publish for %q requires a register at "+
			"its publish, not one in state %q", r.ID, r.Register.State)
	}
	auth, authSeq, err := jsa.servingAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth.Gen != r.Gen || auth.Tenure != r.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("admitting a publish for %q under %q/%q: the standing authority "+
			"carries %q/%q, so this delivery was decided under an entitlement that has moved on",
			r.ID, r.Gen, r.Tenure, auth.Gen, auth.Tenure)
	}
	// THE EXCHANGE IS DERIVED FROM THE REGISTER, not supplied beside it. Its identity is the
	// register's lineage, its allocation and QoS are the register's, and it starts at the
	// only phase an admission can install.
	x := &mqttOutboundExchange{
		ID: r.ID, Epoch: r.Epoch, Gen: auth.Gen, Nonce: r.Register.Lin,
		PI: r.Register.PI, QoS: r.Register.QoS, Phase: mqttExchangePublish,
		AdmitTxn: r.Txn, Tenure: auth.Tenure,
	}
	deps := make([]mqttResourceDependency, 0, len(r.DepIncs))
	for _, inc := range r.DepIncs {
		res, seq, lerr := jsa.loadOutboundResource(r.ID, r.Epoch, auth.Gen, inc)
		if lerr != nil {
			return mqttOpOutcome{}, lerr
		}
		deps = append(deps, mqttResourceDependency{Resource: res, Seq: seq})
	}
	tr, terr := mqttAdmitPublishTransition(&mqttPublishAdmission{
		Auth: auth, AuthSeq: authSeq, Txn: r.Txn, Op: r.Op,
		Exchange: x,
		Register: r.Register,
		RegSubj:  mqttInflightSubject(getHash(r.ID), auth.Gen, r.Register.PI),
		Deps:     deps,
	})
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}

// opAdvanceExchange moves ONE exchange along its protocol, durably.
//
// The caller names the exchange and the phase its evidence establishes; the record and the
// sequence it is CASed at are read here. That is what stops an advance carrying an identifier
// reallocation or a tenure rewrite: the successor is the stored record with its phase
// changed, and nothing else can differ.
func (jsa *mqttJSA) opAdvanceExchange(id, epoch, txn, op, gen, nonce string,
	to mqttExchangePhase) (mqttOpOutcome, error) {

	auth, authSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no obligation of its "+
			"may be advanced", id)
	}
	cur, curSeq, lerr := jsa.loadOutboundExchange(id, epoch, gen, nonce)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	tr, terr := mqttAdvanceExchangeTransition(auth, authSeq, txn, op, cur, curSeq, to)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}

// mqttAcquireActionRequest names ONE emitting operation a connection is about to run.
//
// The caller states WHICH obligation, WHICH packet and WHICH connection will emit it. It does
// not state the action record: the identifier comes from the exchange, the tenure from the
// authority, and whether that packet is permitted at all comes from the exchange's phase.
type mqttAcquireActionRequest struct {
	ID    string
	Epoch string
	Txn   string
	Op    string

	// Gen and Tenure are the entitlement the caller captured, compared with the standing
	// authority rather than adopted from it.
	Gen    string
	Tenure string

	// Nonce is the exchange this action emits for, and Acq its acquisition counter — a
	// retransmission is a NEW action rather than an overwrite of the one that may still be
	// executing.
	Nonce string
	Acq   uint64

	// Kind is the packet. Whether the exchange may emit it is decided from its stored
	// phase, never from this field.
	Kind mqttActionKind

	// Binding names the connection that will emit the bytes. It is the ORIGINAL binding a
	// captured completion handler later resolves against, so it is never rewritten.
	Binding *mqttConnBinding

	// Outstanding reports whether another action for this exchange may still emit. A caller
	// that cannot answer must say TRUE, because an unknown action is an outstanding one.
	Outstanding bool
}

// opAcquireAction acquires one emitting operation for an exchange it READS.
//
// Acquisition is the one operation the authority governs, and it is also the one that can put
// bytes on the wire for an obligation that no longer permits them. Before this it took a
// caller-built action body and never looked at the exchange: a fresh acquisition counter
// therefore bought a PUBLISH for an exchange the client had already rejected or completed, or
// a PUBREL for one still at its publish — the packet would be well-formed, create-only, and
// wrong. The identifier is taken from the exchange for the same reason: an action carrying a
// different one would put bytes on the wire that disagree with what the client was told.
func (jsa *mqttJSA) opAcquireAction(r *mqttAcquireActionRequest) (mqttOpOutcome, error) {
	if r == nil {
		return mqttOpOutcome{}, fmt.Errorf("acquiring an action requires the request")
	}
	auth, authSeq, err := jsa.servingAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth.Gen != r.Gen || auth.Tenure != r.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("acquiring an action for %q under %q/%q: the standing authority "+
			"carries %q/%q, so this acquisition belongs to an entitlement that has moved on",
			r.ID, r.Gen, r.Tenure, auth.Gen, auth.Tenure)
	}
	if r.Binding == nil {
		return mqttOpOutcome{}, fmt.Errorf("acquiring an action for %q requires the connection that will emit it", r.ID)
	}
	if r.Acq == 0 {
		return mqttOpOutcome{}, fmt.Errorf("acquiring an action for exchange %q requires its acquisition counter", r.Nonce)
	}
	x, _, lerr := jsa.loadOutboundExchange(r.ID, r.Epoch, auth.Gen, r.Nonce)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	// WHICH PACKET THIS EXCHANGE MAY EMIT is decided from its stored phase, through the one
	// function that states that relation: a PUBLISH only while the delivery is unanswered, a
	// PUBREL only once a successful PUBREC transferred ownership.
	if perr := mqttActionPermittedAtPhase(x.QoS, x.Phase, r.Kind); perr != nil {
		return mqttOpOutcome{}, fmt.Errorf("exchange %q may not acquire a %q: %w", r.Nonce, r.Kind, perr)
	}
	// AND WHETHER IT MAY BE ACQUIRED NOW is the completion rules' question, asked as
	// ACQUIRING MORE OUTPUT for BOTH packets.
	//
	// Not as "materializing an owed duty", which is what a PUBREL is when it is written to
	// the store and nothing else: that arm deliberately ignores outstanding output because a
	// durable duty has no socket effect. THIS operation binds an action to a connection that
	// will emit bytes, so an owed release acquired here is new output like any other and must
	// wait for the transmission it would race — an earlier PUBLISH still executing, or an
	// earlier release of the same exchange.
	v, verr := mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpAcquireMore, Auth: auth, Exchange: x, Outstanding: r.Outstanding,
	})
	if verr != nil {
		return mqttOpOutcome{}, verr
	}
	if v != mqttCompletionProceed {
		return mqttOpOutcome{}, fmt.Errorf("exchange %q at phase %d may not acquire a %q now (verdict %d)",
			r.Nonce, x.Phase, r.Kind, v)
	}
	a := &mqttOutboundAction{
		ID: r.ID, Epoch: r.Epoch, Gen: auth.Gen, Nonce: r.Nonce, Acq: r.Acq,
		Kind: r.Kind, PI: x.PI,
		Run: r.Binding.run, Conn: r.Binding.conn, Tenure: auth.Tenure, AcqTxn: r.Txn,
	}
	tr, terr := mqttAcquireActionTransition(auth, authSeq, r.Txn, r.Op, a)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

// opProvisionResource records a backing resource this generation owns.
//
// An admission may only take a dependency on a resource that EXISTS, so without this the
// surface could not perform a dependency-bearing admission at all and a caller would have to
// write the record itself — which is the escape hatch the surface exists to remove, and it
// would put a resource on the store that no authority ever arbitrated.
//
// The record is derived: LIVE, with an explicitly EMPTY dependent list, at the generation the
// authority stands in. An empty list is written as `[]` and never null, because a null list
// would read as "nothing depends on it" when what it says is nothing at all.
func (jsa *mqttJSA) opProvisionResource(id, epoch, txn, op, gen, tenure, inc, name string,
	kind mqttResourceKind) (mqttOpOutcome, error) {

	auth, authSeq, err := jsa.servingAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth.Gen != gen || auth.Tenure != tenure {
		return mqttOpOutcome{}, fmt.Errorf("provisioning resource %q for %q under %q/%q: the standing "+
			"authority carries %q/%q, so this request belongs to an entitlement that has "+
			"moved on", inc, id, gen, tenure, auth.Gen, auth.Tenure)
	}
	r := &mqttOutboundResource{
		ID: id, Epoch: epoch, Gen: auth.Gen, Inc: inc,
		Kind: kind, Name: name, State: mqttResourceLive, Deps: []string{},
	}
	if verr := mqttValidateOutboundResource(r, id, epoch, auth.Gen, inc); verr != nil {
		return mqttOpOutcome{}, fmt.Errorf("refusing to provision an unsupported resource: %w", verr)
	}
	body, merr := json.Marshal(r)
	if merr != nil {
		return mqttOpOutcome{}, merr
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: id, Epoch: epoch, Txn: txn, Op: op,
			Kind: mqttTransitionProvisionResource, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		Auth: &mqttOutboundAuthority{
			ID: id, Epoch: epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: mqttAuthorityServing, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(id), auth.Gen, inc),
			expect: 0,
			body:   body,
		}},
	})
}

// opReleaseDependency releases ONE obligation's named edge on a backing resource.
//
// Both records are read here, so the release is stated against the resource as it stands and
// the boundary's proof — that the named exchange has reached an ENDING — is made about the
// obligation the caller actually named.
func (jsa *mqttJSA) opReleaseDependency(id, epoch, txn, op, gen, inc, nonce string) (mqttOpOutcome, error) {
	auth, authSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no edge of its may be released", id)
	}
	cur, curSeq, lerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	next, rerr := mqttResourceReleaseDependency(cur, nonce)
	if rerr != nil {
		return mqttOpOutcome{}, rerr
	}
	return jsa.commitResourceStep(auth, authSeq, txn, op, next, curSeq)
}

// opAuthorizeDeletion authorizes the cleanup of a backing resource nothing depends on.
func (jsa *mqttJSA) opAuthorizeDeletion(id, epoch, txn, op, gen, inc string) (mqttOpOutcome, error) {
	auth, authSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no resource of its may be disposed", id)
	}
	cur, curSeq, lerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	tr, terr := mqttAuthorizeDeletionTransition(auth, authSeq, txn, op, cur, curSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}

// opCompleteCleanup records the classified outcome of a cleanup an earlier transition
// authorized. The authorizing transition is preserved from the record, never restated.
func (jsa *mqttJSA) opCompleteCleanup(id, epoch, txn, op, gen, inc string,
	outcome mqttCleanupOutcome) (mqttOpOutcome, error) {

	auth, authSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no cleanup of its may be completed", id)
	}
	cur, curSeq, lerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	if cur.State != mqttResourceDeleteAuthorized {
		return mqttOpOutcome{}, fmt.Errorf("resource %q is in state %d, so there is no authorized cleanup to complete",
			inc, cur.State)
	}
	if !mqttValidCleanupOutcome(outcome) {
		return mqttOpOutcome{}, fmt.Errorf("resource %q was offered an unsupported cleanup outcome %q", inc, outcome)
	}
	next := *cur
	next.State, next.Outcome = mqttResourceCleanupComplete, outcome
	return jsa.commitResourceStep(auth, authSeq, txn, op, &next, curSeq)
}

// commitResourceStep is the one composer for a transition whose whole content is a resource
// record's own step. Every rule about which step is legal lives at the write boundary; this
// only frames it.
func (jsa *mqttJSA) commitResourceStep(auth *mqttOutboundAuthority, authSeq uint64,
	txn, op string, next *mqttOutboundResource, resSeq uint64) (mqttOpOutcome, error) {

	body, err := json.Marshal(next)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: auth.ID, Epoch: auth.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionDisposeResource, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		Auth: &mqttOutboundAuthority{
			// Preserved exactly, pending proposals included: cleanup runs beside a
			// reservation or a preparation and resolves neither.
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: []mqttTransitionMember{{
			subj:   mqttResourceSubject(getHash(auth.ID), next.Gen, next.Inc),
			expect: resSeq,
			body:   body,
		}},
	})
}
