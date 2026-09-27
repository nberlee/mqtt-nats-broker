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
	// afterEvidence, when set, runs after every evidence read and before the commit. A test seam
	// for the read-to-commit window; nil in production.
	afterEvidence func()
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
// STANDING AUTHORITY, not serving, for the acknowledgement's reason: ending work already
// authorised needs no entitlement, and a closed or displaced generation's obligation must still
// be able to end. The authority changes nothing but its revision.
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
	tomb := *reg
	tomb.Cancelled = true
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
)

// withdrawUnsent runs one withdrawal of the register on subject and classifies its outcome with
// the resolve-abort-resolve settlement every F-105 attempt uses: a lost response is not a
// refusal, and the re-read — never the abort's own result — decides.
//
// Session lock NOT held on entry.
func (sess *mqttSession) withdrawUnsent(gen, subject, nonce, lin string, afterEvidence func()) mqttWithdrawResult {
	// A withdrawal that landed ended the exchange; one whose landing is unknown may have. The
	// custody charge is released only by the exchange's own WithdrawnUnsent phase.
	defer sess.hintCustody(gen, lin)
	as := sess.asm
	if as == nil || sess.jsa == nil {
		return mqttWithdrawUnresolved
	}
	req := &mqttWithdrawUnsentRequest{
		ID: sess.id, Epoch: as.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-withdraw-unsent",
		Gen: gen, Subject: subject, Nonce: nonce, Lin: lin, afterEvidence: afterEvidence,
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
//   - no marker evidence, or an exchange that is missing or unreadable: proceed exactly as before
//     and leave the exchange where it is (consult 32: the door admitted this destruction on its
//     own proof, and the reservation for it is the custody ledger's, not this step's);
//   - a withdrawal whose landing is unknown, or evidence that moved twice: refuse. A register
//     whose ending may or may not have landed is never deleted.
//
// Session lock NOT held on entry.
func (sess *mqttSession) endUnsentBeforeDestroy(subject string, seq uint64, cur *mqttInflightRecord,
	d mqttInflightDisposition) (uint64, *mqttInflightRecord, bool) {

	if cur == nil || cur.State != mqttInflightPublish || cur.Lin == _EMPTY_ ||
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
		if xerr != nil || x == nil {
			return seq, cur, true // missing or unreadable: nothing is manufactured
		}
		if mqttExchangePhaseEnded(x.Phase) {
			return seq, cur, true
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
		switch sess.withdrawUnsent(name.gen, subject, cur.Nonce, cur.Lin, seam) {
		case mqttWithdrawNoMarker:
			return seq, cur, true // no whole-exchange evidence: HEAD's destruction, exchange left
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

// withdrawIfEvidenced withdraws the obligation of an ordinary publish register when the
// whole-exchange evidence stands, and reports 0 when there is nothing to withdraw on (not
// ordinary, no lineage, the exchange missing, unreadable or not at Publish, or — the op's answer
// — no marker naming this allocation): the cases in which a caller keeps its existing behaviour.
//
// Session lock NOT held on entry.
func (sess *mqttSession) withdrawIfEvidenced(gen, subject string, cur *mqttInflightRecord) mqttWithdrawResult {
	as := sess.asm
	if as == nil || sess.jsa == nil || cur == nil || cur.State != mqttInflightPublish || cur.Lin == _EMPTY_ ||
		cur.ActionToken != _EMPTY_ || mqttDurIsSlotOnlyClass(cur.Dur) {
		return 0
	}
	x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, gen, cur.Lin)
	if xerr != nil || x == nil || x.Phase != mqttExchangePublish || x.PI != cur.PI || x.QoS != cur.QoS {
		return 0
	}
	if r := sess.withdrawUnsent(gen, subject, cur.Nonce, cur.Lin, nil); r != mqttWithdrawNoMarker {
		return r
	}
	return 0
}
