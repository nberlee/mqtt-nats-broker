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
	"errors"
	"fmt"
	"sync/atomic"
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
//   - THE PHASE AN ADVANCE SELECTS is the caller's. opAdvanceExchangeAck is told which phase the
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
// The production routes reach the authority through these operations, never through the
// transition struct directly.

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

// servingAuthority loads this session's own serving outbound authority.
//
// It is the session-side accessor the converted delivery, acknowledgement and retirement routes
// use to reach the authority under this client's recovery epoch, rather than each threading the
// id and epoch itself. It carries no policy of its own: jsa.servingAuthority is the one place the
// "no missing-authority fallback" rule and the serving-state requirement live, so this only
// supplies the identity. Unwired until the routes flip.
func (sess *mqttSession) servingAuthority() (*mqttOutboundAuthority, uint64, error) {
	if sess.asm == nil {
		return nil, 0, fmt.Errorf("session %q has no account manager, so its outbound authority "+
			"cannot be resolved", sess.id)
	}
	return sess.jsa.servingAuthority(sess.id, sess.asm.sessStreamEpoch)
}

// mqttOutboundEntitlement is the generation and tenure one connection was accepted under.
type mqttOutboundEntitlement struct {
	Gen    string
	Tenure string
}

func (e mqttOutboundEntitlement) isZero() bool { return e.Gen == _EMPTY_ || e.Tenure == _EMPTY_ }

// setOutboundEntitlement records the connection's accepted entitlement, once. A second call with a
// different pair is refused silently rather than applied: the entitlement is a fact about the
// accept that made this connection, and nothing after it may rewrite that.
func (c *client) setOutboundEntitlement(e mqttOutboundEntitlement) {
	if c == nil || c.mqtt == nil || e.isZero() {
		return
	}
	c.mu.Lock()
	if c.mqtt.outEnt.isZero() {
		c.mqtt.outEnt = e
	}
	c.mu.Unlock()
}

// outboundEntitlement reads the connection's accepted entitlement; false when it has none.
func (c *client) outboundEntitlement() (mqttOutboundEntitlement, bool) {
	if c == nil || c.mqtt == nil {
		return mqttOutboundEntitlement{}, false
	}
	c.mu.Lock()
	e := c.mqtt.outEnt
	c.mu.Unlock()
	return e, !e.isZero()
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
	// EVERY CLOSURE SEALS AND ROOTS (F-105, row 6), so this composes the enrolment with an empty
	// target set rather than being a second, barer way to close a generation. A closure with no
	// root would be refused at the boundary — and the reason it is refused there rather than
	// merely composed correctly here is that the rule must hold for a batch assembled by
	// anything at all.

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

	cur, curSeq, lerr := jsa.loadGenLedger(id, epoch, gen)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	hdr, hdrSeq, herr := jsa.loadDirShardAt(mqttDirShardFor(getHash(id)))
	if herr != nil {
		return mqttOpOutcome{}, herr
	}
	// A non-discarding closure enrols no target: the affine consumer stays the incarnation's that
	// continues (mqttComposeClosingEnrolment), so the seal and the root are the whole enrolment.
	enr, cerr := mqttComposeClosingEnrolment(id, epoch, gen, false, mqttClosingTargets{}, cur, curSeq, hdr, hdrSeq)
	if cerr != nil {
		return mqttOpOutcome{}, cerr
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
		Closure: &mqttGenClosure{
			ID: id, Epoch: epoch, Gen: prev.Gen, Txn: txn, Discard: false,
			RootShard: enr.RootShard, RootOrd: enr.RootOrd,
		},
		Extra: enr.Members,
	})
}

// mqttTestPrepareHandoffReadHookV runs before a handoff preparation reads the authority: the window
// between an accept drive's input load and the preparation's own read. nil in production.
var mqttTestPrepareHandoffReadHookV atomic.Pointer[func(id string)]

// opPrepareHandoffFenced freezes admission and reserves a handoff from the serving authority, for
// an accepting CONNECT whose fork repair read its reinstated consumers at authority sequence fence
// (zero: no fence). The intent is the caller's; its Prev* members are filled from the standing
// authority and the named session record, so no caller can propose a handoff from a predecessor
// that is not there. The preparation must displace
// EXACTLY that authority: its own read must stand at fence, and its transition CASes it. A later
// sequence means the authority was written after those reads — a successor-tenure disposal among
// the possibilities — and is refused with errMQTTForkFenceMoved, never refreshed (F-105 7f).
func (jsa *mqttJSA) opPrepareHandoffFenced(id, epoch, txn string, i *mqttHandoffIntent, fence uint64) (mqttOpOutcome, error) {
	if h := mqttTestPrepareHandoffReadHookV.Load(); h != nil {
		(*h)(id)
	}
	prev, prevSeq, err := jsa.servingAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if fence != 0 && prevSeq != fence {
		return mqttOpOutcome{}, fmt.Errorf("preparing the handoff of %q at authority sequence %d, but fork "+
			"repair read its reinstated consumers at %d: %w", id, prevSeq, fence, errMQTTForkFenceMoved)
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
	if verr := mqttValidateHandoffIntentFor(getHash(id), &next); verr != nil {
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
		// AND IT ENROLS THE GENERATION IT CLOSES, in the same batch (F-105, row 6). The
		// predecessor's held durables are the intent's to name — it was written while the
		// predecessor's record still stood, which is what makes them nameable after the accepted
		// batch displaced that record — and the enrolment, the seal, the shard allocation and the
		// root all commit with the closure or not at all.
		cur, curSeq, lerr := jsa.loadGenLedger(id, epoch, i.PrevGen)
		if lerr != nil {
			return mqttOpOutcome{}, lerr
		}
		hdr, hdrSeq, herr := jsa.loadDirShardAt(mqttDirShardFor(getHash(id)))
		if herr != nil {
			return mqttOpOutcome{}, herr
		}
		enr, cerr := mqttComposeClosingEnrolment(id, epoch, i.PrevGen, true,
			mqttClosingTargets{
				AffineDurable: i.Retire.AffineDurable,
				AffineGen:     i.Retire.AffineGen,
				PubRelDurable: i.Retire.PubRelDurable,
			}, cur, curSeq, hdr, hdrSeq)
		if cerr != nil {
			return mqttOpOutcome{}, cerr
		}
		t.Closure = &mqttGenClosure{
			ID: id, Epoch: epoch, Gen: i.PrevGen, Txn: txn, Discard: true,
			RootShard: enr.RootShard, RootOrd: enr.RootOrd,
		}
		t.Extra = append(t.Extra, enr.Members...)
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

	// Deps names the backing resources the obligation will need in order to complete, by
	// incarnation AND by the ROLE it needs each in. Each is read here and its edge is taken in
	// the same batch.
	//
	// These are the roles an ADMISSION establishes — the ones the obligation needs from the
	// moment it exists. The roles it acquires later, as it takes an action or captures a
	// handler, are taken by those acts and not predicted here.
	Deps []mqttAdmitDependency

	// Shared is a shared-group delivery's dependency: the exact acquisition and the shared tenure
	// the delivery callback captured (outbound_shared_acq.go). Set exactly when the register's
	// source is a shared group durable; its readiness is checked here, after the authority read.
	Shared *mqttAdmitShared
}

// mqttAdmitDependency is one resource an admission takes an edge on, in one role.
type mqttAdmitDependency struct {
	// Inc is the resource incarnation and Role is why this obligation needs it. An edge is
	// identified by (obligation nonce, role) alone: no live role names a constituent, and the
	// resource boundary refuses a persisted edge Ref.
	Inc  string
	Role mqttResourceRole
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
	// A CREATE STARTS A FRESH OBLIGATION, so its register's lineage is its own nonce
	// (mqttLineageForWrite). The caller's Lin is what custody charges for this attempt and what
	// the exchange below is named by; a register naming another lineage would be charged as one
	// obligation while it claims another, and nothing else on this route states the rule — a
	// create-only collision with a still-standing exchange would refuse it only by accident.
	if lin, _ := mqttLineageForWrite(mqttLineageWriteCreate, nil, r.Register.Nonce); r.Register.Lin != lin {
		return mqttOpOutcome{}, fmt.Errorf("admitting a publish for %q: its register carries lineage %q, "+
			"but a create admits a FRESH obligation whose lineage is its own nonce %q, so the lineage "+
			"custody charges would not be the lineage the batch writes", r.ID, r.Register.Lin, lin)
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
	if err := jsa.admitSharedDependency(r.ID, r.Epoch, auth.Gen, r.Register, r.Shared, x); err != nil {
		return mqttOpOutcome{}, err
	}
	deps := make([]mqttResourceDependency, 0, len(r.Deps))
	for _, d := range r.Deps {
		res, seq, lerr := jsa.loadOutboundResource(r.ID, r.Epoch, auth.Gen, d.Inc)
		if lerr != nil {
			return mqttOpOutcome{}, lerr
		}
		deps = append(deps, mqttResourceDependency{
			Resource: res, Seq: seq, Role: d.Role,
		})
	}
	led, ledSeq, lerr := jsa.loadGenLedger(r.ID, r.Epoch, auth.Gen)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	tr, terr := mqttAdmitPublishTransition(&mqttPublishAdmission{
		Auth: auth, AuthSeq: authSeq, Txn: r.Txn, Op: r.Op,
		Exchange: x,
		Register: r.Register,
		RegSubj:  mqttInflightSubject(getHash(r.ID), auth.Gen, r.Register.PI),
		Deps:     deps,
		Ledger:   led, LedgerSeq: ledSeq,
	})
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}

// mqttReplacePublishRequest names one same-delivery replacement: an allocation displacing the
// occupant of its slot. The mode is the ladder's, decided by mqttDecideMarker — a register-only
// replacement CONTINUES the occupant's (possibly-attempted) obligation; a pair replacement admits
// a FRESH obligation over a proven-unsent occupant and must dispose of the predecessor. This
// operation is the replacement twin of opAdmitPublish: it loads the authority (and, for a pair,
// the ledger and resources) and commits the composed replacement.
type mqttReplacePublishRequest struct {
	ID, Epoch, Txn, Op string
	// Gen and Tenure are the entitlement the caller captured, compared with the standing
	// authority rather than adopted from it.
	Gen, Tenure string
	// RegisterOnly selects the mode.
	RegisterOnly bool
	// Occupant is the exact record displaced, at OccupantSeq; MarkerSeq its marker (pair only).
	Occupant    *mqttInflightRecord
	OccupantSeq uint64
	MarkerSeq   uint64
	// Register is the displacing allocation. Its QoS is derived by the composer from the
	// occupant; its Lin must already be mqttLineageForWrite's value for the mode, which the
	// composer re-derives and refuses on disagreement.
	Register *mqttInflightRecord
	// Deps are the fresh obligation's dependencies (pair only), by incarnation and role.
	Deps []mqttAdmitDependency
	// Shared is the fresh obligation's shared dependency (pair only), exactly as a create states it.
	Shared *mqttAdmitShared
}

// admitSharedDependency states a fresh obligation's shared dependency on its exchange, or refuses.
// A register on a shared group durable is admitted only WITH its dependency, only at QoS 1, and only
// against a readiness observation taken now — after the caller's authority read, inside this
// attempt — so the batch's authority CAS fences it (outbound_shared_acq.go). A dependency supplied
// for any other register is refused.
func (jsa *mqttJSA) admitSharedDependency(id, epoch, gen string, reg *mqttInflightRecord, s *mqttAdmitShared,
	x *mqttOutboundExchange) error {

	if !mqttIsSharedDurable(reg.Dur) {
		if s != nil {
			return fmt.Errorf("admitting a publish for %q on %q: a shared dependency was supplied for a "+
				"non-shared source", id, reg.Dur)
		}
		return nil
	}
	if s == nil {
		return fmt.Errorf("admitting a publish for %q on %q: a shared delivery is never admitted without "+
			"its acquisition", id, reg.Dur)
	}
	if x.QoS != 1 {
		return fmt.Errorf("admitting a publish for %q on %q at QoS %d: supported shared delivery is QoS 1",
			id, reg.Dur, x.QoS)
	}
	if err := jsa.mqttSharedAcqReadyForDependency(id, epoch, gen, reg.Dur, s); err != nil {
		return err
	}
	ref := s.Acq
	x.SharedAcq = &ref
	return mqttCheckSharedAcqPair(reg, x)
}

// opReplacePublish admits one same-delivery replacement. The register-only mode is wired; the
// pair mode (a fresh obligation over a proven-unsent occupant, which must also dispose of the
// predecessor obligation and release its resource edges) is pending its design and is refused.
func (jsa *mqttJSA) opReplacePublish(r *mqttReplacePublishRequest) (mqttOpOutcome, error) {
	if r == nil || r.Register == nil || r.Occupant == nil {
		return mqttOpOutcome{}, fmt.Errorf("replacing a publish requires its register and the occupant it displaces")
	}
	if r.Register.State != mqttInflightPublish {
		return mqttOpOutcome{}, fmt.Errorf("replacing a publish for %q requires a register at its publish, "+
			"not one in state %q", r.ID, r.Register.State)
	}
	auth, authSeq, err := jsa.servingAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth.Gen != r.Gen || auth.Tenure != r.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("replacing a publish for %q under %q/%q: the standing authority "+
			"carries %q/%q, so this delivery was decided under an entitlement that has moved on",
			r.ID, r.Gen, r.Tenure, auth.Gen, auth.Tenure)
	}
	if !r.RegisterOnly {
		// PAIR: a proven-unsent occupant, replaced by a FRESH obligation. The occupant's PUBLISH
		// obligation is SUPERSEDED (mqttExchangeSupersededUnsent) in the same batch so its packet
		// identifier is re-allocated to the fresh obligation rather than dropped, and the fresh
		// obligation is admitted with the OCCUPANT's preserved QoS — an incoming QoS 1 replacing a
		// QoS 2 occupant still owes the release, so r.Deps (assembled by the caller against the
		// occupant's QoS) still carries the canonical-PUBREL edge. The predecessor's OWN resource
		// edges are left standing here and released by a separate durable continuation (Codex
		// consult 15): mqttCheckResourceStep forbids adding the fresh edge and removing the old one
		// in a single resource mutation, and keeping both temporarily is safe because the fresh
		// edges protect the resources throughout.
		pred, predSeq, plerr := jsa.loadOutboundExchange(r.ID, r.Epoch, auth.Gen, r.Occupant.Lin)
		if plerr != nil {
			return mqttOpOutcome{}, fmt.Errorf("replacing a publish for %q: its proven-unsent predecessor "+
				"%q could not be read: %w", r.ID, r.Occupant.Lin, plerr)
		}
		if pred == nil {
			return mqttOpOutcome{}, fmt.Errorf("replacing a publish for %q: no predecessor obligation stands "+
				"for occupant %q, so there is nothing to supersede", r.ID, r.Occupant.Lin)
		}
		if pred.Phase != mqttExchangePublish {
			return mqttOpOutcome{}, fmt.Errorf("replacing a publish for %q: its predecessor %q stands at phase "+
				"%d, not PUBLISH, so it is not proven-unsent and may not be superseded", r.ID, r.Occupant.Lin, pred.Phase)
		}
		if perr := mqttCheckSharedAcqPair(r.Occupant, pred); perr != nil {
			return mqttOpOutcome{}, fmt.Errorf("replacing a publish for %q: %w", r.ID, perr)
		}
		superseded := *pred
		superseded.Phase = mqttExchangeSupersededUnsent
		// The fresh obligation is named by the one lineage derivation, not by a third statement
		// of "a pair's lineage is its nonce"; the composer refuses a register that disagrees.
		freshLin, _ := mqttLineageForWrite(mqttLineageWritePair, r.Occupant, r.Register.Nonce)
		x := &mqttOutboundExchange{
			ID: r.ID, Epoch: r.Epoch, Gen: auth.Gen, Nonce: freshLin,
			PI: r.Register.PI, QoS: r.Occupant.QoS, Phase: mqttExchangePublish,
			AdmitTxn: r.Txn, Tenure: auth.Tenure,
		}
		if serr := jsa.admitSharedDependency(r.ID, r.Epoch, auth.Gen, r.Register, r.Shared, x); serr != nil {
			return mqttOpOutcome{}, serr
		}
		deps := make([]mqttResourceDependency, 0, len(r.Deps))
		for _, d := range r.Deps {
			res, seq, lerr := jsa.loadOutboundResource(r.ID, r.Epoch, auth.Gen, d.Inc)
			if lerr != nil {
				return mqttOpOutcome{}, lerr
			}
			deps = append(deps, mqttResourceDependency{Resource: res, Seq: seq, Role: d.Role})
		}
		led, ledSeq, lerr := jsa.loadGenLedger(r.ID, r.Epoch, auth.Gen)
		if lerr != nil {
			return mqttOpOutcome{}, lerr
		}
		tr, terr := mqttReplacePublishTransition(&mqttReplacingAdmission{
			Auth: auth, AuthSeq: authSeq, Txn: r.Txn, Op: r.Op, Mode: mqttReplacePair,
			RegSubj:        mqttInflightSubject(getHash(r.ID), auth.Gen, r.Register.PI),
			Occupant:       r.Occupant,
			OccupantSeq:    r.OccupantSeq,
			MarkerSeq:      r.MarkerSeq,
			Register:       r.Register,
			Exchange:       x,
			Deps:           deps,
			Predecessor:    &superseded,
			PredecessorSeq: predSeq,
			Ledger:         led, LedgerSeq: ledSeq,
		})
		if terr != nil {
			return mqttOpOutcome{}, terr
		}
		// The superseded predecessor's holds go in this batch, merged into the fresh obligation's
		// own member where both hold one resource (F-105 7c-lc).
		if herr := jsa.appendHoldReleases(tr, pred, mqttExchangeSupersededUnsent); herr != nil {
			return mqttOpOutcome{}, herr
		}
		return jsa.commitOp(tr)
	}
	tr, terr := mqttReplacePublishTransition(&mqttReplacingAdmission{
		Auth: auth, AuthSeq: authSeq, Txn: r.Txn, Op: r.Op, Mode: mqttReplaceRegisterOnly,
		RegSubj:     mqttInflightSubject(getHash(r.ID), auth.Gen, r.Register.PI),
		Occupant:    r.Occupant,
		OccupantSeq: r.OccupantSeq,
		Register:    r.Register,
	})
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

	// Register is the exact in-flight register occupant this acquisition installs its output
	// custody TOKEN on, and RegisterSeq the sequence it was read at; Token is the lease itself.
	// The token acquisition and the action record are committed as ONE atomic authority batch
	// (F-105 row 2, §4.3 point 1): the register CAS at RegisterSeq installs the token, binds it to
	// the occupant's existing custody, and advances Acq, BESIDE the create-only action record —
	// neither is an independent acquisition. REQUIRED: an action record with no token beside it
	// excludes no second acquisition. A retained send has no durable slot and so acquires no
	// action here; its fence is 7c's to state.
	Register    *mqttInflightRecord
	RegisterSeq uint64
	Token       string

	// Rebind, for the PUBREL direct arrow only, is the canonical PUBREL the delivered record
	// proved: the token member then ALSO moves the slot `pubrel_intended -> pubrel_ready` and
	// installs this tuple, in the same batch — the evidence and the authorization established
	// together, so no window leaves the slot past PUBREC with nobody owning its canonical record.
	Rebind *mqttPubRelRebind
}

// mqttPubRelRebind names the canonical PUBREL record a direct-arrow acquisition installs.
type mqttPubRelRebind struct {
	Durable   string
	StreamSeq uint64
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
//
// It returns the action it created — the exact record marshalled into the batch — so the caller
// holds the acquisition its frame will consume rather than a counter it would have to re-derive.
// The action is returned only with a nil error: an ambiguous outcome is resolved by rereading the
// register, and an acquisition adopted that way never emits.
func (jsa *mqttJSA) opAcquireAction(r *mqttAcquireActionRequest) (mqttOpOutcome, *mqttOutboundAction, error) {
	if r == nil {
		return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action requires the request")
	}
	auth, authSeq, err := jsa.servingAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, nil, err
	}
	if auth.Gen != r.Gen || auth.Tenure != r.Tenure {
		return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for %q under %q/%q: the standing authority "+
			"carries %q/%q, so this acquisition belongs to an entitlement that has moved on: %w",
			r.ID, r.Gen, r.Tenure, auth.Gen, auth.Tenure, errMQTTEntitlementEnded)
	}
	if r.Binding == nil {
		return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for %q requires the connection that will emit it", r.ID)
	}
	if r.Acq == 0 {
		return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q requires its acquisition counter", r.Nonce)
	}
	if r.Register == nil {
		return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q requires the register its "+
			"output-custody token is installed on: an action record alone excludes no second acquisition", r.Nonce)
	}
	x, _, lerr := jsa.loadOutboundExchange(r.ID, r.Epoch, auth.Gen, r.Nonce)
	if lerr != nil {
		return mqttOpOutcome{}, nil, lerr
	}
	// WHICH PACKET THIS EXCHANGE MAY EMIT is decided from its stored phase, through the one
	// function that states that relation: a PUBLISH only while the delivery is unanswered, a
	// PUBREL only once a successful PUBREC transferred ownership.
	if perr := mqttActionPermittedAtPhase(x.QoS, x.Phase, r.Kind); perr != nil {
		return mqttOpOutcome{}, nil, fmt.Errorf("exchange %q may not acquire a %q: %w", r.Nonce, r.Kind, perr)
	}
	// NO OUTPUT OVER A CONTRADICTORY PAIR: a shared register whose exchange does not name exactly
	// its acquisition (or any register whose exchange names one it may not) is a shape this broker
	// does not write, refused BEFORE any action exists.
	if perr := mqttCheckSharedAcqPair(r.Register, x); perr != nil {
		return mqttOpOutcome{}, nil, fmt.Errorf("exchange %q may not acquire a %q: %w", r.Nonce, r.Kind, perr)
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
		return mqttOpOutcome{}, nil, verr
	}
	if v != mqttCompletionProceed {
		return mqttOpOutcome{}, nil, fmt.Errorf("exchange %q at phase %d may not acquire a %q now (verdict %d)",
			r.Nonce, x.Phase, r.Kind, v)
	}
	a := &mqttOutboundAction{
		ID: r.ID, Epoch: r.Epoch, Gen: auth.Gen, Nonce: r.Nonce, Acq: r.Acq,
		Kind: r.Kind, PI: x.PI,
		Run: r.Binding.run, Conn: r.Binding.conn, Tenure: auth.Tenure, AcqTxn: r.Txn,
	}
	tr, terr := mqttAcquireActionTransition(auth, authSeq, r.Txn, r.Op, a)
	if terr != nil {
		return mqttOpOutcome{}, nil, terr
	}
	{
		// THE OUTPUT-CUSTODY TOKEN, INSTALLED IN THE SAME BATCH (F-105 row 2, §4.3 point 1). The
		// register is CASed at the sequence it was read at: the token is installed, bound to the
		// custody the slot already carries (the validator requires the two equal), and the
		// acquisition counter advances. The batch's action record and this token member must
		// describe the SAME acquisition, so their exchange identity, identifier and counter agree
		// — an action record acquired beside a token for a different slot is refused.
		if r.Register.ActionToken != _EMPTY_ {
			return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q: the register on "+
				"identifier %d already carries a live token, so a second acquisition may not overwrite it",
				r.Nonce, r.Register.PI)
		}
		if r.Register.Lin != r.Nonce || r.Register.PI != x.PI || r.Register.Acq+1 != r.Acq {
			return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q at acquisition %d: its "+
				"token member names obligation %q/identifier %d at counter %d, which is not the same "+
				"acquisition", r.Nonce, r.Acq, r.Register.Lin, r.Register.PI, r.Register.Acq+1)
		}
		next := *r.Register
		next.ActionToken = r.Token
		next.ActionClaim = r.Register.Claim
		next.ActionClaimSeq = r.Register.ClaimSeq
		next.Acq = r.Register.Acq + 1
		// A RETRANSMISSION OVER A KNOWN-FALSE CANCELLATION clears it (F-088): the write boundary
		// lets the member land only when the allocation's unsent marker reads authoritatively
		// absent (the PUBLISH was already attempted, so the bit's "never sent" is false); a
		// standing marker is a genuine cancellation and the acquisition is refused there.
		if r.Register.CancelOrigin == mqttCancelOriginPendingCreate {
			// A pending fence is never acquired: it is not an exchange's identity carrier (F-088).
			return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q: the register is a "+
				"pending cancellation fence, which no acquisition may take", r.Nonce)
		}
		if r.Register.State == mqttInflightPublish && r.Register.Cancelled && !r.Register.Retired {
			next.Cancelled, next.CancelOrigin = false, _EMPTY_
		}
		if r.Rebind != nil {
			// THE DIRECT ARROW: the canonical PUBREL is installed and the slot moves past its
			// intent in the same member, which only a PUBREL acquisition of an owed slot may do.
			if r.Kind != mqttActionPubRel || r.Register.State != mqttInflightPubRelIntended ||
				r.Rebind.Durable == _EMPTY_ || r.Rebind.StreamSeq == 0 ||
				(r.Register.PubRelDurable != _EMPTY_ && r.Register.PubRelDurable != r.Rebind.Durable) {
				return mqttOpOutcome{}, nil, fmt.Errorf("acquiring a %q action for exchange %q: a canonical "+
					"rebind is the PUBREL direct arrow from an owed slot, and slot %q naming %q may not "+
					"take %s/%d", r.Kind, r.Nonce, string(r.Register.State), r.Register.PubRelDurable,
					r.Rebind.Durable, r.Rebind.StreamSeq)
			}
			next.State = mqttInflightPubRelReady
			next.PubRelDurable = r.Rebind.Durable
			next.PubRelStreamSeq = r.Rebind.StreamSeq
		}
		if kerr := mqttActionKindRegisterState(r.Kind, r.Register.State, next.State); kerr != nil {
			return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q: %w", r.Nonce, kerr)
		}
		if verr := mqttValidateInflightShape(&next); verr != nil {
			return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q: its token member is an "+
				"unsupported register shape: %w", r.Nonce, verr)
		}
		body, merr := json.Marshal(&next)
		if merr != nil {
			return mqttOpOutcome{}, nil, merr
		}
		tr.Extra = append(tr.Extra, mqttTransitionMember{
			subj:   mqttInflightSubject(getHash(r.ID), auth.Gen, r.Register.PI),
			expect: r.RegisterSeq,
			body:   body,
		})
	}
	// THE CERTIFICATE'S CONTEXT IS REGISTERED BEFORE THE BATCH IS SENT (F-105 8c): from here on
	// this connection may hold a token, whether or not this call learns that it landed.
	if rerr := r.Binding.registerAcquisition(jsa.newCertPersistCtx(r.ID, r.Epoch, auth.Gen)); rerr != nil {
		return mqttOpOutcome{}, nil, fmt.Errorf("acquiring an action for exchange %q: %w", r.Nonce, rerr)
	}
	out, cerr := jsa.commitOp(tr)
	if cerr != nil {
		return out, nil, cerr
	}
	return out, a, nil
}

// errMQTTEntitlementEnded marks an acquisition refused because the standing authority's tenure is
// not the one the requesting connection was accepted under. It is TERMINAL for the delivery that
// asked: the connection's entitlement will not come back, so re-dispatching the same delivery can
// only ask again under a tenure that is not its own.
var errMQTTEntitlementEnded = errors.New("the connection's accepted entitlement is no longer the serving one")

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
		Kind: kind, Name: name, State: mqttResourceLive,
		// PENDING, because committing this record is not evidence the durable exists. The
		// physical create is a separate act and its outcome may be unknown; sealing is its
		// own transition, and until it wins this name cannot be freed.
		Prov: mqttProvisioningPending, Deps: []mqttResourceEdge{},
	}
	if mqttResourceLogicalUseKind(kind) {
		// The tenure that claims it, which the boundary proves is the serving one (F-105 7f).
		r.ProvTenure = auth.Tenure
	}
	if verr := mqttValidateOutboundResource(r, id, epoch, auth.Gen, inc); verr != nil {
		return mqttOpOutcome{}, fmt.Errorf("refusing to provision an unsupported resource: %w", verr)
	}
	body, merr := json.Marshal(r)
	if merr != nil {
		return mqttOpOutcome{}, merr
	}
	// BOTH OF THIS RESOURCE'S DUTIES ARE PREDECLARED HERE, in the batch that admits it.
	//
	// The provisioning duty is obvious. The CLEANUP duty is the one that has to be allocated
	// now and is easy to get wrong: it becomes actionable when the resource's last dependent
	// goes, which can happen long AFTER the generation is closed — and allocating an ordinal
	// against a sealed ledger is exactly what the seal forbids. So it is reserved with the
	// resource and starts NOT_YET_ACTIONABLE, which is UNRESOLVED: a sweep keeps coming back
	// for it rather than counting it finished.
	cur, curSeq, lerr := jsa.loadGenLedger(id, epoch, auth.Gen)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	inv, ierr := mqttAllocateInventoryMembers(id, epoch, auth.Gen, cur, curSeq,
		[]mqttDutyDeclaration{
			{Kind: mqttDutyProvision, Ref: inc, State: mqttDutyPending},
			{Kind: mqttDutyCleanup, Ref: inc, State: mqttDutyNotYetActionable},
		})
	if ierr != nil {
		return mqttOpOutcome{}, ierr
	}
	members := append([]mqttTransitionMember{{
		subj:   mqttResourceSubject(getHash(id), auth.Gen, inc),
		expect: 0,
		body:   body,
	}}, inv...)
	// A SOURCE or PUBREL consumer's physical NAME is claimed LIVE in this same batch (F-105
	// cutover, consult 4). The physical CONSUMER.CREATE is a queued request the broker can
	// neither fence nor retract, so the name — not the create — is what an obligation depends
	// on, and this record is what makes "this name is owned by exactly one live resource"
	// durable. The affine and payload kinds have no such name: the affine consumer's name
	// carries its own generation and is the affine lifecycle's, and the payload is not a
	// consumer.
	if stream, owned := mqttOwnedStreamForResourceKind(kind); owned {
		prevOwn, prevOwnSeq, oerr := jsa.loadNameOwnership(id, epoch, stream, name)
		if oerr != nil {
			return mqttOpOutcome{}, oerr
		}
		om, merr := mqttNameOwnershipMember(id, epoch, stream, name, mqttNameOwnLive, auth.Gen, inc, prevOwnSeq)
		if merr != nil {
			return mqttOpOutcome{}, merr
		}
		// A legal claim is a first one (absent) or an idempotent re-claim by the SAME owner;
		// a name a different owner holds LIVE, or one already RETIRED, is refused here as well
		// as at the boundary, so the composer's caller learns why rather than only the CAS.
		next := &mqttNameOwnership{ID: id, Epoch: epoch, Stream: stream, Name: name,
			State: mqttNameOwnLive, OwnerGen: auth.Gen, OwnerInc: inc}
		if lerr := mqttNameOwnershipTransitionLegal(prevOwn, next); lerr != nil {
			return mqttOpOutcome{}, fmt.Errorf("provisioning resource %q: %w", inc, lerr)
		}
		members = append(members, om)
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
		Extra: members,
	})
}

// opReleaseDependency releases ONE obligation's named edge on a backing resource.
//
// Both records are read here, so the release is stated against the resource as it stands and
// the boundary's proof — that the named exchange has reached an ENDING — is made about the
// obligation the caller actually named.
func (jsa *mqttJSA) opReleaseDependency(id, epoch, txn, op, gen, inc string,
	edge mqttResourceEdge) (mqttOpOutcome, error) {
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
	next, rerr := mqttResourceReleaseDependency(cur, edge)
	if rerr != nil {
		return mqttOpOutcome{}, rerr
	}
	return jsa.commitResourceStep(auth, authSeq, txn, op, next, curSeq)
}

// errMQTTDisposalEntitlementMoved reports that the entitlement a disposal decision was taken under
// no longer serves: the decision is never re-made under the successor, so its custodian drops it
// and the resource is left for the recovery executor (F-105 7c-lc's handoff to 7d).
var errMQTTDisposalEntitlementMoved = errors.New("the disposal's entitlement no longer serves")

// errMQTTDisposalEntitlementPreparing reports that the entitlement a disposal was taken under is
// the one a handoff is PREPARING to displace (F-105 7f). That is not yet a move: a preparation can
// be cancelled, which restores exactly this tenure, so the decision is withheld and re-attempted
// rather than dropped — dropping it would strand the consumer with no successor to settle it.
var errMQTTDisposalEntitlementPreparing = errors.New("a handoff is preparing to displace the disposal's entitlement")

// mqttRequestDisposalRequest is one route's decision that a logical-use consumer is ended.
type mqttRequestDisposalRequest struct {
	ID, Epoch string
	Txn       string
	// Gen and Inc name the resource; Reason the route.
	Gen, Inc string
	Reason   string
	// Entitlement is the serving pair the deciding route held, captured when it decided.
	Entitlement mqttOutboundEntitlement
}

// opRequestDisposal records that a source or PUBREL consumer is ended (F-105 7c-lc).
//
// The write is licensed by the entitlement the route decided under, never by the authority that
// happens to stand when it is sent (mqttCheckDisposalLicence). Idempotent on a decision already
// standing: a lost response re-attempted, or a second route ending the same consumer, finds the
// fact and stops — the first decision is the one carried.
func (jsa *mqttJSA) opRequestDisposal(r *mqttRequestDisposalRequest) (mqttOpOutcome, error) {
	if r == nil || r.Entitlement.isZero() || !mqttValidDisposalReason(r.Reason) {
		return mqttOpOutcome{}, fmt.Errorf("a disposal requires its request, its reason and its entitlement")
	}
	auth, authSeq, err := jsa.requireAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth != nil && auth.State == mqttAuthorityPreparing &&
		auth.Gen == r.Entitlement.Gen && auth.Tenure == r.Entitlement.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("disposing of %q/%q: %w", r.Gen, r.Inc, errMQTTDisposalEntitlementPreparing)
	}
	if auth == nil || auth.State != mqttAuthorityServing ||
		auth.Gen != r.Entitlement.Gen || auth.Tenure != r.Entitlement.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("disposing of %q/%q: %w", r.Gen, r.Inc, errMQTTDisposalEntitlementMoved)
	}
	cur, curSeq, lerr := jsa.loadOutboundResource(r.ID, r.Epoch, r.Gen, r.Inc)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	if cur == nil {
		return mqttOpOutcome{}, fmt.Errorf("disposing of %q/%q: no resource stands", r.Gen, r.Inc)
	}
	if !mqttResourceLogicalUseKind(cur.Kind) {
		return mqttOpOutcome{}, fmt.Errorf("resource %q is kind %q, which is not retired by a disposal", r.Inc, cur.Kind)
	}
	if cur.Disposal != nil {
		return mqttOpOutcome{}, nil
	}
	if cur.State != mqttResourceLive {
		return mqttOpOutcome{}, fmt.Errorf("resource %q is in state %d, so no route can still end it", r.Inc, cur.State)
	}
	next := *cur
	next.Disposal = &mqttResourceDisposal{Txn: r.Txn, Reason: r.Reason,
		Gen: r.Entitlement.Gen, Tenure: r.Entitlement.Tenure}
	return jsa.commitResourceStep(auth, authSeq, r.Txn, "op-dispose-"+r.Reason, &next, curSeq)
}

// opSealProvisioning records that nothing further can materialize a resource.
//
// Separate from provisioning itself because the record is written BEFORE the durable is
// confirmed to exist: opProvisionResource commits a record an admission can depend on, and the
// physical create is a separate act whose outcome may be unknown. This is where that outcome
// becomes durable.
func (jsa *mqttJSA) opSealProvisioning(id, epoch, txn, op, gen, inc string) (mqttOpOutcome, error) {
	auth, authSeq, err := jsa.requireAuthority(id, epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no resource "+
			"of its may be sealed", id)
	}
	cur, curSeq, lerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	tr, terr := mqttSealProvisioningTransition(auth, authSeq, txn, op, cur, curSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
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
	// AUTHORIZING DELETION IS WHERE THE PHYSICAL NAME IS SPENT (F-105 cutover, consult 4). For
	// a source or PUBREL consumer the same batch retires the name's ownership LIVE -> RETIRED,
	// retaining the owner: from here no obligation may depend on the name and no replacement
	// may claim it, so a queued CREATE that resurrects the physical consumer afterwards is a
	// harmless orphan. The owner is checked at the boundary against the standing LIVE record.
	if stream, owned := mqttOwnedStreamForResourceKind(cur.Kind); owned {
		prevOwn, prevOwnSeq, oerr := jsa.loadNameOwnership(id, epoch, stream, cur.Name)
		if oerr != nil {
			return mqttOpOutcome{}, oerr
		}
		if prevOwn == nil {
			return mqttOpOutcome{}, fmt.Errorf("authorizing deletion of resource %q: consumer name %q has no "+
				"ownership record, so its retirement cannot be authorized (a missing record is withheld, "+
				"never taken as permission)", inc, cur.Name)
		}
		// THE DISPOSING RESOURCE MUST BE THE NAME'S OWNER (F-105 cutover, Codex consult 14 finding 1).
		// This batch retires the name on behalf of resource (gen, inc); if the standing ownership names
		// a DIFFERENT owner, this resource does not own the name and disposing it must not retire another
		// owner's LIVE name. Enforced explicitly rather than copying the owner through, so a RETIRED
		// record always names the resource whose disposal produced it.
		if prevOwn.OwnerGen != gen || prevOwn.OwnerInc != inc {
			return mqttOpOutcome{}, fmt.Errorf("authorizing deletion of resource %q/%q: consumer name %q is owned "+
				"by %q/%q, not this resource incarnation, so disposing this resource may not retire it",
				gen, inc, cur.Name, prevOwn.OwnerGen, prevOwn.OwnerInc)
		}
		om, merr := mqttNameOwnershipMember(id, epoch, stream, cur.Name, mqttNameOwnRetired,
			prevOwn.OwnerGen, prevOwn.OwnerInc, prevOwnSeq)
		if merr != nil {
			return mqttOpOutcome{}, merr
		}
		next := &mqttNameOwnership{ID: id, Epoch: epoch, Stream: stream, Name: cur.Name,
			State: mqttNameOwnRetired, OwnerGen: prevOwn.OwnerGen, OwnerInc: prevOwn.OwnerInc}
		if lerr := mqttNameOwnershipTransitionLegal(prevOwn, next); lerr != nil {
			return mqttOpOutcome{}, fmt.Errorf("authorizing deletion of resource %q: %w", inc, lerr)
		}
		tr.Extra = append(tr.Extra, om)
	}
	return jsa.commitOp(tr)
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

// mqttSourcePubRelReadyForUse reports whether the DURABLE, CONFIGURATION half of readiness for
// USE holds for a source or PUBREL consumer's resource (F-105 cutover, consult 4's §5.1).
//
// Readiness for use is the live admission gate a source/PUBREL dependency must pass: a usable
// backing consumer, a safe effective configuration, and matching LIVE ownership. Two of those
// three are durable/config facts this broker can check without touching the connection, and
// they are what this establishes:
//
//   - the resource stands LIVE for a logical-use kind, and
//   - its physical name's ownership stands LIVE and names THIS exact resource incarnation, and
//   - the stream carries no effective inactivity timer that could reap a depended-on consumer
//     (streamInactivitySafe, the drainingAllowed prerequisite — a refusal, never a fallback).
//
// The THIRD fact, that the physical consumer itself is usable, is a live lookup the converting
// admission path adds before it admits a dependency; it is not durable and is deliberately not
// asserted here. A caller admits a source/PUBREL dependency only when this returns nil AND its
// own consumer-usable check passes. An error names the missing fact so a refusal is legible.
func (jsa *mqttJSA) mqttSourcePubRelReadyForUse(id, epoch, gen, inc string) error {
	r, _, rerr := jsa.loadOutboundResource(id, epoch, gen, inc)
	if rerr != nil {
		return fmt.Errorf("readiness for use of %q/%q: %w", gen, inc, rerr)
	}
	if r == nil {
		return fmt.Errorf("readiness for use of %q/%q: no resource stands", gen, inc)
	}
	if !mqttResourceLogicalUseKind(r.Kind) {
		return fmt.Errorf("resource %q/%q is kind %q, which is not arbitrated by name-ownership readiness",
			gen, inc, r.Kind)
	}
	if r.State != mqttResourceLive {
		return fmt.Errorf("resource %q/%q is in state %d, so it is not ready for use", gen, inc, r.State)
	}
	// A DISPOSED consumer serves nothing new: the route that ended it has decided, and the
	// boundary refuses an edge against it anyway (F-105 7c-lc).
	if r.Disposal != nil {
		return fmt.Errorf("resource %q/%q was disposed (%s), so it is not ready for use", gen, inc, r.Disposal.Reason)
	}
	stream, owned := mqttOwnedStreamForResourceKind(r.Kind)
	if !owned {
		return fmt.Errorf("resource %q/%q of kind %q has no owned stream", gen, inc, r.Kind)
	}
	o, _, oerr := jsa.loadNameOwnership(id, epoch, stream, r.Name)
	if oerr != nil {
		return fmt.Errorf("readiness for use of %q/%q: %w", gen, inc, oerr)
	}
	// A MISSING ownership record is WITHHELD, never read as ready: the name is claimed BEFORE
	// its CREATE is issued, so an absent record means no claim landed, not a free name.
	if o == nil {
		return fmt.Errorf("readiness for use of %q/%q: no ownership record stands for name %q", gen, inc, r.Name)
	}
	if o.State != mqttNameOwnLive {
		return fmt.Errorf("readiness for use of %q/%q: name %q ownership is in state %d, not LIVE",
			gen, inc, r.Name, o.State)
	}
	if o.OwnerGen != gen || o.OwnerInc != inc {
		return fmt.Errorf("readiness for use of %q/%q: name %q is owned by %q/%q, not this resource",
			gen, inc, r.Name, o.OwnerGen, o.OwnerInc)
	}
	streamName, serr := mqttStreamForOwned(stream)
	if serr != nil {
		return fmt.Errorf("readiness for use of %q/%q: %w", gen, inc, serr)
	}
	if ierr := jsa.streamInactivitySafe(streamName); ierr != nil {
		return fmt.Errorf("readiness for use of %q/%q: %w", gen, inc, ierr)
	}
	return nil
}
