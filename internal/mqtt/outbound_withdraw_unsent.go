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
)

// An unsent obligation is ENDED, not forgotten (F-105, the prerequisite of row 5).
//
// Every route that destroys an ordinary register because its delivery was never transmitted —
// Message Expiry, the oversize disposal, a cancelled allocation, a drain, recovery's
// marker-proven drop — used to delete the register and its marker and write nothing to the
// exchange, which then read Publish for ever: an obligation no client can ever answer. Once the
// exchange owns the Receive Maximum charge that is a slot held for the life of the generation.
//
// The ending is written where the destruction happens — inside the one gated destructive door,
// both of its entrances — so no route can reach the delete without passing it. It is written
// only on positive whole-exchange evidence (below); where the door admits a destruction on some
// other proof (a tombstone, a nonce verdict) and that evidence is not there, the destruction
// proceeds exactly as before and the exchange is left at Publish. Manufacturing an ending
// without the evidence would be the inference this whole mechanism exists to remove.

// errMQTTWithdrawNoEvidence reports that the op's own read found no whole-exchange unsent
// evidence: the register moved, carries a token, is not at publish, or the marker naming its
// allocation is gone. Nothing was written.
var errMQTTWithdrawNoEvidence = errors.New("no whole-exchange evidence that the obligation was never transmitted")

// errMQTTWithdrawNoMarker is the one no-evidence case a caller may act on differently: the
// register and the exchange stand as a withdrawal needs them, but no unsent marker naming the
// allocation does, so nothing proves the PUBLISH was never written. The door then destroys as it
// always did and leaves the obligation where it is; it never retries, because a marker is never
// recreated for an allocation.
var errMQTTWithdrawNoMarker = fmt.Errorf("%w: no unsent marker naming the allocation stands", errMQTTWithdrawNoEvidence)

// errMQTTNoDestroyLicence reports a withdrawal attempted with no licence at all: a caller that never
// decides to end an unsent obligation (a terminal, retired or affine cleanup) met one. Nothing is
// written, and the door refuses the destruction rather than end an obligation nobody decided to end.
var errMQTTNoDestroyLicence = errors.New("no licence to end an unsent obligation")

// mqttDestroyLicence is what AUTHORIZES ending an ordinary obligation that was never transmitted
// (F-105 7b). Choosing to withdraw a delivery is owner-dependent — it is the serving connection's
// decision for the session — so the licence is the ENTITLEMENT of the connection whose decision
// this is, compared with the serving authority inside the committing batch: a displaced
// connection's withdrawal fails there, and never falls back to a destruction. The zero value
// licenses nothing: a caller that never decides to end an unsent obligation (a terminal, retired
// or affine cleanup) carries it, and meeting one refuses the destruction. No route withdraws an
// obligation of a generation that is no longer served, so there is no other kind.
type mqttDestroyLicence struct {
	ent   mqttOutboundEntitlement
	entOK bool
	// admit, when set, admits each authority-transition attempt the destruction makes (the
	// withdrawal, the tombstone): a BACKGROUND destruction yields every attempt to announced
	// foreground operations (outbound_foreground_priority.go). Nil for a foreground caller.
	admit func() bool
}

// withAdmission is l with each of its destruction's authority attempts admitted by admit (a
// background caller, outbound_foreground_priority.go). The entitlement — whose decision it is — is
// unchanged.
func (l mqttDestroyLicence) withAdmission(admit func() bool) mqttDestroyLicence {
	l.admit = admit
	return l
}

// mqttLicenceOf is the licence of the connection c: its captured accepted entitlement, or none.
func mqttLicenceOf(c *client) mqttDestroyLicence {
	if c == nil {
		return mqttDestroyLicence{}
	}
	ent, ok := c.outboundEntitlement()
	return mqttDestroyLicence{ent: ent, entOK: ok}
}

// mqttWithdrawUnsentRequest names the register whose obligation is to be ended. The op loads
// everything else itself: the caller's view is a hint, and the evidence is the store's.
type mqttWithdrawUnsentRequest struct {
	ID, Epoch, Txn, Op string
	// Gen is the generation the register and its exchange belong to — possibly earlier than
	// the standing authority's.
	Gen string
	// Subject is the register's subject.
	Subject string
	// Nonce and Lin are the ALLOCATION and the obligation the caller validated for disposal. The
	// op ends that obligation or nothing: a different occupant on the subject — a successor
	// admitted after the caller looked — is no evidence about the allocation being disposed of,
	// and ending it would disable a valid delivery.
	Nonce, Lin string
	// Licence authorizes the ending (mqttDestroyLicence). Validated inside the op after the
	// authority read, so the batch's authority CAS fences it.
	Licence mqttDestroyLicence
	// afterEvidence, when set, runs after every evidence read and before the commit. A test seam
	// for the read-to-commit window; nil in production.
	afterEvidence func()
}

// checkDestroyLicence validates a withdrawal's licence against the standing authority read in the
// same attempt: the entitlement must be the SERVING one, for the register's own generation.
func (jsa *mqttJSA) checkDestroyLicence(r *mqttWithdrawUnsentRequest, auth *mqttOutboundAuthority) error {
	lic := r.Licence
	if !lic.entOK {
		return fmt.Errorf("withdrawing %q: %w", r.Subject, errMQTTNoDestroyLicence)
	}
	// Compared with the SERVING authority, not with the obligation's own generation: the connection
	// the standing generation serves may end an earlier generation's unsent obligation (the
	// generation exception), and no other connection may end anything.
	if auth.State != mqttAuthorityServing || auth.Gen != lic.ent.Gen || auth.Tenure != lic.ent.Tenure {
		return fmt.Errorf("withdrawing %q under %q/%q: the authority serves %q/%q in state %d: %w",
			r.Subject, lic.ent.Gen, lic.ent.Tenure, auth.Gen, auth.Tenure, auth.State, errMQTTEntitlementEnded)
	}
	return nil
}

// opWithdrawUnsent ends one admitted PUBLISH obligation that was never transmitted.
//
// THE EVIDENCE, and why its order is the fence. An ordinary PUBLISH is written only by the holder
// of an output token acquired through the authority, and only after the allocation's unsent
// marker is durably gone — the section's confirmed intent delete, or an earlier one. So: read the
// standing authority, then the register (at publish, no token), then the exchange (Publish, the
// register's obligation), then the marker naming the register's ALLOCATION. The batch CASes the
// authority, the register and the exchange at the sequences read: an acquisition since the
// register read rewrote it, so the batch fails and nothing is concluded from the stale marker.
// The write boundary re-checks every clause itself (checkWithdrawalRegister).
//
// THE LICENCE (F-105 7b). Choosing to withdraw is owner-dependent, so the request's licence is
// checked against the authority read here and the batch CASes that authority: a displaced
// connection's entitlement fails. The authority changes nothing but its revision.
func (jsa *mqttJSA) opWithdrawUnsent(r *mqttWithdrawUnsentRequest) (mqttOpOutcome, error) {
	if r == nil || r.Subject == _EMPTY_ || r.Gen == _EMPTY_ || r.Nonce == _EMPTY_ || r.Lin == _EMPTY_ {
		return mqttOpOutcome{}, fmt.Errorf("withdrawing an unsent obligation requires its register, generation, " +
			"allocation and lineage")
	}
	auth, authSeq, err := jsa.requireAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth == nil {
		return mqttOpOutcome{}, fmt.Errorf("client %q has no outbound authority, so no obligation of its may be ended", r.ID)
	}
	if lerr := jsa.checkDestroyLicence(r, auth); lerr != nil {
		return mqttOpOutcome{}, lerr
	}
	reg, rmsg, rerr := jsa.loadInflightRecord(r.Subject)
	if rerr != nil {
		return mqttOpOutcome{}, rerr
	}
	if rmsg == nil || reg == nil || reg.State != mqttInflightPublish || reg.ActionToken != _EMPTY_ || reg.Lin == _EMPTY_ {
		return mqttOpOutcome{}, errMQTTWithdrawNoEvidence
	}
	if reg.Nonce != r.Nonce || reg.Lin != r.Lin {
		// NOT THE ALLOCATION THE CALLER VALIDATED. The subject is reused, so whatever stands on it
		// now may be a successor; nothing about it was ever proven by the caller's disposal.
		return mqttOpOutcome{}, errMQTTWithdrawNoEvidence
	}
	x, xSeq, xerr := jsa.loadOutboundExchange(r.ID, r.Epoch, r.Gen, reg.Lin)
	if xerr != nil {
		return mqttOpOutcome{}, xerr
	}
	if x.Phase != mqttExchangePublish || x.PI != reg.PI || x.QoS != reg.QoS {
		return mqttOpOutcome{}, errMQTTWithdrawNoEvidence
	}
	if !mqttMarkerNamesAllocation(jsa.loadOccupantMarkerLoad(r.Subject), reg.Nonce) {
		return mqttOpOutcome{}, errMQTTWithdrawNoMarker
	}
	if r.afterEvidence != nil {
		r.afterEvidence()
	}
	tr, terr := mqttWithdrawUnsentTransition(auth, authSeq, r.Txn, r.Op, r.Subject, reg, rmsg.Sequence, x, xSeq)
	if terr == nil {
		terr = jsa.appendHoldReleases(tr, x, mqttExchangeWithdrawnUnsent)
	}
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}

// mqttWithdrawUnsentTransition composes the withdrawal of the obligation whose register reg stands
// on subject at regSeq and whose exchange x stands at xSeq, under the standing authority prev.
//
// It ASSERTS no evidence — the op gathers it and the write boundary re-checks it — so it is the
// one composer both the op and a boundary test hand a batch to.
func mqttWithdrawUnsentTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op, subject string,
	reg *mqttInflightRecord, regSeq uint64, x *mqttOutboundExchange, xSeq uint64) (*mqttAuthorityTransition, error) {

	if prev == nil || reg == nil || x == nil || regSeq == 0 || xSeq == 0 {
		return nil, fmt.Errorf("a withdrawal requires the authority, the register and the exchange as read")
	}
	if reg.CancelOrigin == mqttCancelOriginPendingCreate {
		// A pending fence carries no obligation to withdraw (F-088): it is disposed by its own rule.
		return nil, fmt.Errorf("withdrawing %q: the register is a pending cancellation fence, not an obligation", subject)
	}
	tomb := *reg
	tomb.Cancelled, tomb.CancelOrigin = true, mqttCancelOriginOccupant
	if verr := mqttValidateInflightShape(&tomb); verr != nil {
		return nil, fmt.Errorf("withdrawing %q builds an unsupported tombstone: %w", subject, verr)
	}
	regBody, merr := json.Marshal(&tomb)
	if merr != nil {
		return nil, merr
	}
	ended := *x
	ended.Phase = mqttExchangeWithdrawnUnsent
	xBody, merr := json.Marshal(&ended)
	if merr != nil {
		return nil, merr
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionWithdrawUnsent, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		// Reinstalled exactly as it stood: an ending is not an ownership event.
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
		Extra: []mqttTransitionMember{
			{subj: subject, expect: regSeq, body: regBody},
			{subj: mqttExchangeSubject(getHash(prev.ID), x.Gen, x.Nonce), expect: xSeq, body: xBody},
		},
	}, nil
}

// mqttWithdrawResult is what one withdrawal attempt established.
type mqttWithdrawResult uint8

const (
	// mqttWithdrawLanded: the ending is durable (committed, or resolved as landed/superseded).
	mqttWithdrawLanded mqttWithdrawResult = iota + 1
	// mqttWithdrawNoEvidence: the op's own read found the register or exchange not as a
	// withdrawal needs them (moved, tokened, not at publish); nothing was written.
	mqttWithdrawNoEvidence
	// mqttWithdrawNoMarker: register and exchange stand, but no marker naming the allocation
	// does; nothing was written, and nothing proves the obligation unsent.
	mqttWithdrawNoMarker
	// mqttWithdrawNeverLanded: the attempt is positively known not to have landed.
	mqttWithdrawNeverLanded
	// mqttWithdrawUnresolved: whether it landed is unknown. Nothing may be concluded.
	mqttWithdrawUnresolved
	// mqttWithdrawUnlicensed: the caller's licence does not authorize ending this obligation (its
	// entitlement is no longer the serving one, or it carries none). Nothing was written, and the
	// caller must NOT fall back to destroying the register: the obligation is the serving owner's.
	mqttWithdrawUnlicensed
)

// withdrawUnsent runs one withdrawal of the register on subject and classifies its outcome with
// the resolve-abort-resolve settlement every F-105 attempt uses: a lost response is not a
// refusal, and the re-read — never the abort's own result — decides.
//
// Session lock NOT held on entry.
func (sess *mqttSession) withdrawUnsent(gen, subject, nonce, lin string, lic mqttDestroyLicence,
	afterEvidence func()) mqttWithdrawResult {
	// A withdrawal that landed ended the exchange; one whose landing is unknown may have. The
	// custody charge is released only by the exchange's own WithdrawnUnsent phase.
	defer sess.hintCustody(gen, lin)
	as := sess.asm
	if as == nil || sess.jsa == nil {
		return mqttWithdrawUnresolved
	}
	if lic.admit != nil && !lic.admit() {
		return mqttWithdrawNeverLanded // shutdown: no attempt was made
	}
	req := &mqttWithdrawUnsentRequest{
		ID: sess.id, Epoch: as.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-withdraw-unsent",
		Gen: gen, Subject: subject, Nonce: nonce, Lin: lin, Licence: lic, afterEvidence: afterEvidence,
	}
	write := func() (mqttOpOutcome, error) { return sess.jsa.opWithdrawUnsent(req) }
	as.mu.RLock()
	hook := as.testWithdrawUnsent
	as.mu.RUnlock()
	var out mqttOpOutcome
	var err error
	if hook != nil {
		out, err = hook(req, write)
	} else {
		out, err = write()
	}
	if err == nil {
		return mqttWithdrawLanded
	}
	if errors.Is(err, errMQTTWithdrawNoMarker) {
		return mqttWithdrawNoMarker
	}
	if errors.Is(err, errMQTTWithdrawNoEvidence) {
		return mqttWithdrawNoEvidence
	}
	if errors.Is(err, errMQTTEntitlementEnded) || errors.Is(err, errMQTTNoDestroyLicence) {
		return mqttWithdrawUnlicensed
	}
	if out.Handle == nil {
		// Refused before a batch was composed and sent: nothing landed.
		return mqttWithdrawNeverLanded
	}
	jsa := sess.jsa
	v := jsa.resolveTransitionHandle(out.Handle)
	if v == mqttTransitionUnresolved {
		_ = jsa.abortTransition(&out.Handle.attempt)
		v = jsa.resolveTransitionHandle(out.Handle)
	}
	switch v {
	case mqttTransitionLanded, mqttTransitionSuperseded:
		return mqttWithdrawLanded
	case mqttTransitionNeverLanded:
		return mqttWithdrawNeverLanded
	}
	return mqttWithdrawUnresolved
}

// endUnsentBeforeDestroy is the withdrawal step of the gated destructive door, run by BOTH of its
// entrances after the door has admitted the destruction and before the delete. It returns the
// register sequence and record the delete must now act on, or false when the door must refuse.
//
// It applies to an ORDINARY register still at publish that names its obligation, destroyed as
// never transmitted (NeverTransmitted, or Complete as dropProvenUnsentPublishEntry uses it). Its
// outcomes:
//
//   - the exchange already ended (a re-run after a crash or a lost response): proceed;
//   - the exchange at Publish with the marker of this allocation standing: withdraw, then proceed
//     against the TOMBSTONE's sequence — which must be OUR tombstone, re-read by identity, so a
//     superseded or lost-response landing never licenses deleting a successor;
//   - no marker evidence, or an exchange that is MISSING: REFUSE (F-088). The door admits a
//     never-transmitted destruction only on the standing marker (arm 4), so reaching here without
//     it means the marker vanished between the door's probe and the op's read — a sender's intent
//     consumed it, or something else removed it — and destroying the register then would be the
//     inference arm 4 no longer makes: the register is the only durable carrier of what a late
//     acknowledgement completes. An ordinary register without its exchange is refused, never
//     interpreted: admission co-creates the pair, and the one exchange-less ordinary register this
//     broker writes - the pending cancellation fence - never reaches this row (it is disposed of
//     under its own provenance rule, mqttDoorDisposition);
//   - a RETIRED register owes no ending: proceed; a pure cancellation tombstone is withdrawn like
//     any register when its evidence stands;
//   - an exchange that is UNREADABLE (the read could not be completed, or the record did not
//     decode): refuse (holistic review H7b-2): the destruction would otherwise skip the only
//     step that fences it by the deciding connection's entitlement;
//   - a withdrawal whose landing is unknown, or evidence that moved twice: refuse. A register
//     whose ending may or may not have landed is never deleted.
//
// Session lock NOT held on entry.
func (sess *mqttSession) endUnsentBeforeDestroy(subject string, seq uint64, cur *mqttInflightRecord,
	d mqttInflightDisposition, lic mqttDestroyLicence) (uint64, *mqttInflightRecord, bool) {

	// Not an unsent obligation, so no ending is owed and the destruction is owner-independent:
	// a RETIRED register is past PUBREC (the client's own answer). A pure cancellation tombstone
	// is NOT exempt here: its exchange may still stand at Publish with its marker, and is then
	// withdrawn like any other (T14); where no withdrawal applies the destruction is refused.
	if cur == nil || cur.State != mqttInflightPublish || cur.Lin == _EMPTY_ || cur.Retired ||
		(d != mqttSlotDeleteNeverTransmitted && d != mqttSlotDeleteComplete) ||
		mqttDurIsSlotOnlyClass(cur.Dur) {
		return seq, cur, true
	}
	as := sess.asm
	if as == nil || sess.jsa == nil {
		// No store to read the exchange from: exactly the "missing or unreadable" row — the
		// destruction proceeds as it always did and nothing is manufactured.
		return seq, cur, true
	}
	name, perr := mqttParseInflightSubject(subject)
	if perr != nil {
		return seq, cur, true
	}
	jsa := sess.jsa
	for attempt := 0; attempt < 2; attempt++ {
		x, _, xerr := jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, name.gen, cur.Lin)
		if xerr != nil && !mqttExchangeReadAbsent(xerr) {
			// UNREADABLE is not missing (F-105 7b): nothing here has read whether this
			// obligation is still owed, and the destruction would skip the only step that
			// fences it by the deciding connection's entitlement. Refused: a void falls to its
			// entitlement-fenced tombstone rung, recovery keeps the pair for its next pass.
			return 0, nil, false
		}
		if xerr != nil || x == nil {
			// MISSING: an ordinary register without its exchange is a shape admission never
			// writes. Refused, never interpreted: no ending can be recorded for it and nothing
			// proves its PUBLISH was never sent.
			return 0, nil, false
		}
		if mqttExchangePhaseEnded(x.Phase) {
			return seq, cur, true
		}
		if mqttCheckSharedAcqPair(cur, x) != nil {
			return 0, nil, false // a contradictory pair: refuse, never destroy over it
		}
		if x.Phase != mqttExchangePublish || x.PI != cur.PI || x.QoS != cur.QoS {
			return seq, cur, true // not an unsent PUBLISH obligation of this register: left as it is
		}
		as.mu.RLock()
		seam := as.testWithdrawAfterEvidence
		as.mu.RUnlock()
		// THE MARKER EVIDENCE IS THE OP'S TO READ (and the write boundary's to re-check), never a
		// precheck here: one reading of it, so the door cannot disagree with the op about what
		// counts.
		switch sess.withdrawUnsent(name.gen, subject, cur.Nonce, cur.Lin, lic, seam) {
		case mqttWithdrawUnresolved:
			// Landing unknown: the door refuses, and the withdrawal is OWED to the custody
			// dispatcher under the deciding licence (F-088, WP3 "withdrawOwed lifecycle").
			sess.recordWithdrawOwed(name.gen, subject, cur.Nonce, cur.Lin, seq, lic)
			return 0, nil, false
		case mqttWithdrawNoMarker:
			// The marker the door admitted on is gone (F-088): the PUBLISH may now be on the wire,
			// so this register is possibly-attempted and is the only durable carrier of what a
			// late acknowledgement completes. Refused; the exchange stays at Publish and its owner
			// (the acquiring section, or the client's acknowledgement) settles it.
			return 0, nil, false
		case mqttWithdrawLanded:
			// OUR tombstone, by identity. A successor on the subject is not ours to delete.
			rec, smsg, lerr := jsa.loadInflightRecord(subject)
			if lerr != nil || smsg == nil || rec == nil || !rec.Cancelled || rec.Nonce != cur.Nonce ||
				rec.Lin != cur.Lin || rec.ActionToken != _EMPTY_ {
				return 0, nil, false
			}
			return smsg.Sequence, rec, true
		case mqttWithdrawNoEvidence, mqttWithdrawNeverLanded:
			// Re-read once: the register may have moved (refuse), or the evidence may be gone
			// (proceed as before), or the same evidence may stand (retry).
			rec, smsg, lerr := jsa.loadInflightRecord(subject)
			if lerr != nil || smsg == nil || rec == nil || smsg.Sequence != seq {
				return 0, nil, false
			}
			continue
		default:
			return 0, nil, false
		}
	}
	return 0, nil, false
}

// mqttExchangeReadAbsent reports whether an exchange read failed because the subject holds no
// record, as opposed to a read that could not be completed or a record that did not decode.
func mqttExchangeReadAbsent(err error) bool {
	ae := jsAPIError(err)
	return ae != nil && IsNatsErr(ae, JSNoMessageFoundErr)
}

// withdrawIfEvidenced withdraws the obligation of an ordinary publish register when the
// whole-exchange evidence stands, and reports 0 when there is nothing to withdraw on (not
// ordinary, no lineage, the exchange missing, unreadable or not at Publish, or — the op's answer
// — no marker naming this allocation): the cases in which a caller keeps its existing behaviour.
//
// Session lock NOT held on entry.
func (sess *mqttSession) withdrawIfEvidenced(gen, subject string, cur *mqttInflightRecord,
	lic mqttDestroyLicence) mqttWithdrawResult {
	as := sess.asm
	if as == nil || sess.jsa == nil || cur == nil || cur.State != mqttInflightPublish || cur.Lin == _EMPTY_ ||
		cur.ActionToken != _EMPTY_ || mqttDurIsSlotOnlyClass(cur.Dur) {
		return 0
	}
	x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, gen, cur.Lin)
	if xerr != nil || x == nil || x.Phase != mqttExchangePublish || x.PI != cur.PI || x.QoS != cur.QoS ||
		mqttCheckSharedAcqPair(cur, x) != nil {
		return 0
	}
	if r := sess.withdrawUnsent(gen, subject, cur.Nonce, cur.Lin, lic, nil); r != mqttWithdrawNoMarker {
		return r
	}
	return 0
}

// A cancellation WITHOUT whole-exchange evidence is still a DECISION (F-105 7b).
//
// Where no unsent marker proves the PUBLISH was never written, a cancelled ordinary allocation is
// recorded by a tombstone on its register alone and its exchange is left where it stands (row 5's
// "destroyed without evidence" population). Deciding to cancel is owner-dependent — it is the
// serving connection's choice for the session — so the tombstone is an authority transition under
// that connection's entitlement: a connection a successor has displaced cannot tombstone a
// register the successor serves, and loses nothing by trying, because the refusal is terminal and
// never falls back to a plain write. The experimental slot-only class keeps its own plain CAS:
// its ownership is its claim's, which chapter P owns.

// mqttTombstoneRegisterRequest names the register to tombstone and the licence the decision
// carries. Expect is the occupant's exact sequence, or 0 for the pending form: the allocation's own
// store is still in flight, so the tombstone is created over an absent subject.
type mqttTombstoneRegisterRequest struct {
	ID, Epoch, Txn, Op string
	Ent                mqttOutboundEntitlement
	Subject            string
	Expect             uint64
	Body               *mqttInflightRecord
}

// opTombstoneRegister writes one ordinary cancellation tombstone under the deciding connection's
// entitlement, which must be the serving authority's generation and tenure.
func (jsa *mqttJSA) opTombstoneRegister(r *mqttTombstoneRegisterRequest) (mqttOpOutcome, error) {
	if r == nil || r.Subject == _EMPTY_ || r.Body == nil {
		return mqttOpOutcome{}, fmt.Errorf("tombstoning a register requires its subject and body")
	}
	auth, authSeq, err := jsa.servingAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth.Gen != r.Ent.Gen || auth.Tenure != r.Ent.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("tombstoning %q under %q/%q: the authority serves %q/%q: %w",
			r.Subject, r.Ent.Gen, r.Ent.Tenure, auth.Gen, auth.Tenure, errMQTTEntitlementEnded)
	}
	body, merr := json.Marshal(r.Body)
	if merr != nil {
		return mqttOpOutcome{}, merr
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: auth.ID, Epoch: auth.Epoch, Txn: r.Txn, Op: r.Op,
			Kind: mqttTransitionTombstoneRegister, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: r.Ent.Gen, Tenure: r.Ent.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		// Reinstalled exactly as it stood: a cancellation is not an ownership event.
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: r.Txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: []mqttTransitionMember{{subj: r.Subject, expect: r.Expect, body: body}},
	})
}

// storeCancellationTombstone writes a cancellation tombstone at expect (0: create-only). An
// ordinary register goes through opTombstoneRegister under lic's entitlement, settled with the
// resolve-abort-resolve every F-105 attempt uses; a slot-only register keeps its plain CAS. An
// entitlement that is not the serving one, or no entitlement at all, is returned wrapping
// errMQTTEntitlementEnded / errMQTTNoDestroyLicence: TERMINAL for the caller, never a fallback.
//
// Session lock NOT held on entry.
func (sess *mqttSession) storeCancellationTombstone(subject string, expect uint64, rec *mqttInflightRecord,
	lic mqttDestroyLicence) error {

	as := sess.asm
	if lic.admit != nil && !lic.admit() {
		return fmt.Errorf("the cancellation tombstone on %q was not attempted: the server is shutting down", subject)
	}
	if mqttDurIsSlotOnlyClass(rec.Dur) {
		// The slot-only class has no authority transition, so its pending fence goes through
		// the one validated create-only entry; every other slot-only cancellation is an
		// occupant CAS through the generic sink, which refuses the fence provenance.
		if rec.CancelOrigin == mqttCancelOriginPendingCreate {
			_, err := as.storePendingFenceCAS(subject, expect, rec)
			return err
		}
		_, err := as.storeInflightCAS(subject, expect, rec)
		return err
	}
	if !lic.entOK {
		return fmt.Errorf("tombstoning %q: %w", subject, errMQTTNoDestroyLicence)
	}
	// The instance-scoped register-store seam, honoured exactly as storeInflightCAS honours it:
	// a rig that fault-injects the register stream reaches this write too.
	as.mu.RLock()
	hook := as.testInflightStore
	as.mu.RUnlock()
	if hook != nil {
		_, err := hook(subject, expect, rec)
		return err
	}
	if sess.jsa == nil {
		return fmt.Errorf("tombstoning %q: session %q has no store to commit an authority transition to", subject, sess.id)
	}
	if verr := mqttValidateInflightShape(rec); verr != nil {
		return fmt.Errorf("tombstoning %q builds an unsupported record: %w", subject, verr)
	}
	jsa := sess.jsa
	out, err := jsa.opTombstoneRegister(&mqttTombstoneRegisterRequest{
		ID: sess.id, Epoch: as.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-tombstone-register",
		Ent: lic.ent, Subject: subject, Expect: expect, Body: rec,
	})
	if err == nil || out.Handle == nil {
		return err
	}
	v := jsa.resolveTransitionHandle(out.Handle)
	if v == mqttTransitionUnresolved {
		_ = jsa.abortTransition(&out.Handle.attempt)
		v = jsa.resolveTransitionHandle(out.Handle)
	}
	if v == mqttTransitionLanded || v == mqttTransitionSuperseded {
		return nil
	}
	return err
}

// mqttTombstoneRefusedTerminally reports whether a tombstone write was refused for want of a
// serving entitlement: the decision was not this connection's to make any more, so the caller
// stops rather than retry or fall back.
func mqttTombstoneRefusedTerminally(err error) bool {
	return errors.Is(err, errMQTTEntitlementEnded) || errors.Is(err, errMQTTNoDestroyLicence)
}
