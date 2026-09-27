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
	"strings"
)

// A restored delivery denied on resume is ABANDONED, durably (F-105 row 5, consult 34).
//
// A PUBLISH a previous connection may have transmitted, which the client never acknowledged, is
// restored on a same-generation resume and re-offered; if the delivery-time authorization guard
// then DENIES it under the resuming identity, the broker will never send it. HEAD freed its
// identifier and slot and acked the source away. Under exchange-owned custody that exchange
// would otherwise stay Publish and hold its Receive Maximum charge for the life of the
// generation, so the decision is written down as what it is: the broker abandoned delivery
// under its authorization policy. Not a completion (no client answer), and not "never
// transmitted" (it may have been).
//
// What the abandonment releases, and what it does not. The send quota is connection-local
// (Spec5 §4.9: it is re-initialized with each Network Connection), and the resumed connection
// will never send this message, so its CHARGE goes. The IDENTIFIER does not: §4.3.2-4.3.3 tie
// reuse to the acknowledgement, and a local ACL decision is not one. It is held until the
// generation is discarded. Abandoning the retransmission §4.4 would otherwise require is a
// stated conformance deviation (docs/CONFORMANCE.md), exactly as HEAD's deny was.

// mqttAbandonReasonACLDeny is the one reason this broker abandons a delivery for: a proven
// delivery-time authorization deny of a restored delivery.
const mqttAbandonReasonACLDeny = "acl-deny"

// mqttValidAbandonReason is the closed set of abandonment reasons.
func mqttValidAbandonReason(r string) bool {
	return r == mqttAbandonReasonACLDeny
}

// errMQTTAbandonNoEvidence reports that the op's own reads did not find the delivery as an
// abandonment needs it — moved, tokened, not at publish, not ordinary, not the delivery the
// decision named, or its last acquisition unreadable. Nothing was written.
var errMQTTAbandonNoEvidence = errors.New("no evidence that the denied delivery stands as an abandonment needs it")

// errMQTTAbandonCurrentOutput reports the current-connection veto: the delivery's last
// acquisition was taken under THIS connection's tenure, so the release of its charge would not
// be a previous connection's quota. Nothing was written.
var errMQTTAbandonCurrentOutput = errors.New("the delivery was last acquired under the deciding connection's own tenure")

// mqttDenyEvidence is the immutable record of one positive deny decision: the exact delivery it
// was about and the connection binding it was made under. Built ONCE at the deny, from the
// connection's accepted entitlement and the restored entry — never re-read from the authority
// or from whichever connection is current — and carried unchanged into every attempt, so a
// retry cannot adopt a successor's identity or entitlement.
type mqttDenyEvidence struct {
	Gen, Tenure string
	Subject     string
	Dur         string
	Seq         uint64
	Nonce, Lin  string
	PI          uint16
	Reason      string
	// Reply is the source's JetStream acknowledgement subject the denied offer carried. It is
	// written into the durable abandonment fact, so the source can be settled after a restart
	// without waiting for a redelivery; it must bind to exactly Dur and Seq.
	Reply string
}

func (ev *mqttDenyEvidence) complete() bool {
	return ev != nil && ev.Gen != _EMPTY_ && ev.Tenure != _EMPTY_ && ev.Subject != _EMPTY_ &&
		ev.Dur != _EMPTY_ && ev.Seq != 0 && ev.Nonce != _EMPTY_ && ev.Lin != _EMPTY_ && ev.PI != 0 &&
		mqttValidAbandonReason(ev.Reason) && mqttSourceAckReplyBinds(ev.Reply, mqttStreamName, ev.Dur, ev.Seq)
}

// mqttAbandonDeliveryRequest is one attempt's request.
type mqttAbandonDeliveryRequest struct {
	ID, Epoch, Txn, Op string
	Evidence           mqttDenyEvidence
	// VettedAcq is the acquisition ordinal the caller's veto checked (abandonCurrentOutputVeto):
	// the op abandons only a register still carrying exactly it, so an acquisition after the veto
	// — which advances the counter and rewrites the register — is never abandoned on a stale
	// veto.
	VettedAcq uint64
	// afterEvidence, when set, runs after every evidence read and before the commit. A test seam
	// for the read-to-commit window; nil in production.
	afterEvidence func()
}

// opAbandonDeniedDelivery abandons one restored ordinary delivery on a positive deny.
//
// SERVING AUTHORITY AT THE DECISION'S BINDING, read FIRST: the decision was made under one
// connection's entitlement, and it is only this broker's to act on while that entitlement still
// serves. A takeover changed the tenure, and the decision is refused (here and at the write
// boundary). Then the register (publish, no token, ordinary, the exact delivery), the exchange
// (Publish, the register's obligation), and the register's acquisition ordinal, which must be
// exactly the one the caller's current-connection veto judged (VettedAcq). The batch CASes the
// authority, the register and the exchange at the sequences read, so an acquisition after the
// read (which rewrites the register) fails it.
func (jsa *mqttJSA) opAbandonDeniedDelivery(r *mqttAbandonDeliveryRequest) (mqttOpOutcome, error) {
	if r == nil || !r.Evidence.complete() {
		return mqttOpOutcome{}, fmt.Errorf("abandoning a delivery requires a complete deny decision")
	}
	ev := r.Evidence
	auth, authSeq, err := jsa.servingAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth.Gen != ev.Gen || auth.Tenure != ev.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("the deny was decided under %q/%q, but %q/%q serves: a displaced "+
			"connection's decision abandons nothing", ev.Gen, ev.Tenure, auth.Gen, auth.Tenure)
	}
	reg, rmsg, rerr := jsa.loadInflightRecord(ev.Subject)
	if rerr != nil {
		return mqttOpOutcome{}, rerr
	}
	if rmsg == nil || reg == nil || reg.State != mqttInflightPublish || reg.ActionToken != _EMPTY_ ||
		mqttDurIsSlotOnlyClass(reg.Dur) || reg.Nonce != ev.Nonce || reg.Lin != ev.Lin || reg.PI != ev.PI ||
		reg.Dur != ev.Dur || reg.Seq != ev.Seq {
		return mqttOpOutcome{}, errMQTTAbandonNoEvidence
	}
	x, xSeq, xerr := jsa.loadOutboundExchange(r.ID, r.Epoch, ev.Gen, reg.Lin)
	if xerr != nil {
		return mqttOpOutcome{}, xerr
	}
	if x.Phase != mqttExchangePublish || x.PI != reg.PI || x.QoS != reg.QoS {
		return mqttOpOutcome{}, errMQTTAbandonNoEvidence
	}
	if reg.Acq != r.VettedAcq {
		// The last acquisition is not the one the veto judged: re-judge with fresh evidence.
		return mqttOpOutcome{}, errMQTTAbandonNoEvidence
	}
	if r.afterEvidence != nil {
		r.afterEvidence()
	}
	tr, terr := mqttAbandonDeliveryTransition(auth, authSeq, r.Txn, r.Op, ev, reg, rmsg.Sequence, x, xSeq)
	if terr != nil {
		return mqttOpOutcome{}, terr
	}
	return jsa.commitOp(tr)
}

// mqttAbandonDeliveryTransition composes the abandonment of the delivery whose register reg
// stands at regSeq and whose exchange x stands at xSeq, under the serving authority prev. It
// asserts no evidence — the op gathers it and the write boundary re-checks what it can — so it
// is the one composer both the op and a boundary test hand a batch to.
func mqttAbandonDeliveryTransition(prev *mqttOutboundAuthority, prevSeq uint64, txn, op string, ev mqttDenyEvidence,
	reg *mqttInflightRecord, regSeq uint64, x *mqttOutboundExchange, xSeq uint64) (*mqttAuthorityTransition, error) {

	if prev == nil || reg == nil || x == nil || regSeq == 0 || xSeq == 0 {
		return nil, fmt.Errorf("an abandonment requires the authority, the register and the exchange as read")
	}
	next := *reg
	next.State = mqttInflightDeliveryAbandoned
	next.AbandonReason = ev.Reason
	next.AbandonTenure = ev.Tenure
	if verr := mqttValidateInflightShape(&next); verr != nil {
		return nil, fmt.Errorf("abandoning %q builds an unsupported register: %w", ev.Subject, verr)
	}
	regBody, merr := json.Marshal(&next)
	if merr != nil {
		return nil, merr
	}
	ended := *x
	ended.Phase = mqttExchangeDeliveryAbandoned
	// THE HISTORICAL FACT, bound to exactly the register this batch rewrites: its allocation,
	// its acquisition, its source, and the reply the denied offer carried.
	ended.Abandoned = &mqttExchangeAbandonment{
		Reason: ev.Reason, Tenure: ev.Tenure, Alloc: reg.Nonce, Acq: reg.Acq,
		Dur: reg.Dur, Seq: reg.Seq, Reply: ev.Reply,
	}
	if verr := mqttValidateExchangeAbandonment(ended.Abandoned); verr != nil {
		return nil, fmt.Errorf("abandoning %q builds an unsupported fact: %w", ev.Subject, verr)
	}
	xBody, merr := json.Marshal(&ended)
	if merr != nil {
		return nil, merr
	}
	return &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: prev.ID, Epoch: prev.Epoch, Txn: txn, Op: op,
			Kind: mqttTransitionAbandonDelivery, PrevTxn: prev.OwnerTxn, Rev: prev.Rev + 1,
			Gen: prev.Gen, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn,
		},
		Prev: prev, PrevSeq: prevSeq,
		// Reinstalled exactly as it stood: a disposal is not an ownership event.
		Auth: &mqttOutboundAuthority{
			ID: prev.ID, Epoch: prev.Epoch, Gen: prev.Gen, Tenure: prev.Tenure,
			State: prev.State, Rev: prev.Rev + 1, Txn: txn, OwnerTxn: prev.OwnerTxn,
			Intent: prev.Intent, Creation: prev.Creation,
		},
		Extra: []mqttTransitionMember{
			{subj: ev.Subject, expect: regSeq, body: regBody},
			{subj: mqttExchangeSubject(getHash(prev.ID), x.Gen, x.Nonce), expect: xSeq, body: xBody},
		},
	}, nil
}

// checkAbandonmentRegister is the write boundary's rule for the register member of an
// abandon-delivery transition. cur is the exact predecessor checkRegisterReplacements loaded at
// the member's expectation.
//
// Its contract is the BINDING, not the authorization policy: the boundary cannot re-evaluate a
// delivery-time ACL decision from a bare register. What it proves is that this exact delivery —
// a tokenless ordinary PUBLISH register, paired with its own exchange's abandonment — is
// abandoned under the tenure the batch itself is committed under, and that nothing else about the
// register changes.
func (jsa *mqttJSA) checkAbandonmentRegister(a *mqttTransitionAttempt, extra []mqttTransitionMember,
	subj string, cur, rec *mqttInflightRecord) error {

	if cur.State != mqttInflightPublish {
		return fmt.Errorf("transition %q abandons the register on %q in state %q: only a delivery still "+
			"at its publish can be abandoned", a.Txn, subj, string(cur.State))
	}
	if cur.ActionToken != _EMPTY_ {
		return fmt.Errorf("transition %q abandons the register on %q while output token %q stands: "+
			"a holder may be writing its PUBLISH right now", a.Txn, subj, cur.ActionToken)
	}
	if mqttDurIsSlotOnlyClass(cur.Dur) {
		return fmt.Errorf("transition %q abandons the register on %q of source %q: only an ordinary "+
			"delivery is abandoned; a shared or affine one keeps its own disposal", a.Txn, subj, cur.Dur)
	}
	paired := false
	for _, x := range extra {
		if cls, ok := mqttF105ObligationClass(x.subj); !ok || cls != mqttExchangeClassTk {
			continue
		}
		toks := strings.Split(x.subj[len(mqttF105SubjectPrefix()):], ".")
		if len(toks) != 4 {
			continue
		}
		xb, derr := mqttDecodeOutboundExchange(x.body, a.ID, a.Epoch, toks[2], toks[3])
		if derr != nil || xb == nil {
			continue
		}
		// The exchange's fact must name THIS register's delivery and the committing tenure: a
		// fact bound to another allocation, acquisition or source would record an abandonment
		// of something this batch does not abandon.
		if f := xb.Abandoned; xb.Phase == mqttExchangeDeliveryAbandoned && xb.Nonce == cur.Lin &&
			xb.PI == cur.PI && xb.QoS == cur.QoS && f != nil && f.Reason == rec.AbandonReason &&
			f.Tenure == a.Tenure && f.Alloc == cur.Nonce && f.Acq == cur.Acq && f.Dur == cur.Dur &&
			f.Seq == cur.Seq {
			paired = true
		}
	}
	if !paired {
		return fmt.Errorf("transition %q abandons the register on %q without the abandonment of its "+
			"obligation %q beside it", a.Txn, subj, cur.Lin)
	}
	if !mqttValidAbandonReason(rec.AbandonReason) {
		return fmt.Errorf("transition %q abandons the register on %q for reason %q, which this broker "+
			"does not abandon for", a.Txn, subj, rec.AbandonReason)
	}
	if rec.AbandonTenure != a.Tenure {
		return fmt.Errorf("transition %q abandons the register on %q under tenure %q while it commits "+
			"under %q: the decision's binding is the committing authority's", a.Txn, subj, rec.AbandonTenure, a.Tenure)
	}
	want := *cur
	want.State = mqttInflightDeliveryAbandoned
	want.AbandonReason = rec.AbandonReason
	want.AbandonTenure = rec.AbandonTenure
	if want != *rec {
		return fmt.Errorf("transition %q abandons the register on %q with a body that changes more than "+
			"its disposition: an abandonment records a decision, it does not rewrite the delivery", a.Txn, subj)
	}
	return nil
}

// mqttAbandonResult is what one abandonment attempt established, for the deny's caller.
type mqttAbandonResult uint8

const (
	// mqttAbandonNone: there is no restored ordinary obligation for this source to abandon; the
	// caller disposes of the source as before.
	mqttAbandonNone mqttAbandonResult = iota + 1
	// mqttAbandonDone: the abandonment is durable (committed, or found already standing for this
	// exact delivery). The caller may untrack, suppress and ack.
	mqttAbandonDone
	// mqttAbandonPending: refused or vetoed — positively not abandoned. Nothing is untracked and
	// the source is NOT acked: its AckWait re-offer re-runs the deny, which retries with fresh
	// evidence.
	mqttAbandonPending
	// mqttAbandonUnresolved: whether the abandonment landed is UNKNOWN. The deny's caller treats it
	// as pending, but the delivery stays fenced from drain claims until a later attempt settles it
	// (the batch may have committed).
	mqttAbandonUnresolved
)

// abandonDeniedDelivery runs one abandonment attempt and classifies it with the
// resolve-abort-resolve settlement every F-105 attempt uses. A refusal whose cause is that the
// abandonment ALREADY STANDS for this exact delivery (an earlier attempt landed behind a lost
// response) is recognised by re-reading the pair, never by the refusal's text.
//
// Session lock NOT held on entry.
func (sess *mqttSession) abandonDeniedDelivery(ev mqttDenyEvidence, prior []*mqttTransitionHandle) (mqttAbandonResult, []*mqttTransitionHandle, error) {
	as := sess.asm
	if as == nil || sess.jsa == nil {
		return mqttAbandonPending, prior, fmt.Errorf("session %q has no store", sess.id)
	}
	// EARLIER ATTEMPTS FIRST. An earlier attempt whose landing is unknown may have committed: it
	// is settled by resolve-abort-resolve before anything else is decided, and only a positive
	// answer drops it. A landed one IS the abandonment; one still unknown keeps the delivery
	// unresolved (and fenced) — no failed read on a retry may stand in for its settlement.
	var unsettled []*mqttTransitionHandle
	for _, h := range prior {
		switch sess.settleCustodyHandle(h) {
		case mqttTransitionLanded, mqttTransitionSuperseded:
			return mqttAbandonDone, nil, nil
		case mqttTransitionNeverLanded:
		default:
			unsettled = append(unsettled, h)
		}
	}
	if len(unsettled) > 0 {
		return mqttAbandonUnresolved, unsettled, fmt.Errorf("an earlier abandonment's landing is still unknown")
	}
	as.mu.RLock()
	hook := as.testAbandonDelivery
	as.mu.RUnlock()
	// BOUNDED RE-ATTEMPTS ON CONTENTION, each with FRESH evidence. A batch refused because a
	// member moved between the op's reads and the commit (a register rewrite, an authority
	// revision from the resume's own concurrent writes) is positively never-landed and says
	// nothing about the delivery; the next attempt re-reads everything, so a moved register is
	// re-judged, never blindly refreshed. A veto, missing evidence, or an unresolved landing
	// stops at once: those are answers, not contention.
	var lastErr error
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		acq, verr := sess.abandonCurrentOutputVeto(ev)
		if verr != nil {
			if sess.deliveryAbandonedStands(ev) {
				return mqttAbandonDone, nil, nil
			}
			return mqttAbandonPending, nil, verr
		}
		req := &mqttAbandonDeliveryRequest{
			ID: sess.id, Epoch: as.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-abandon-delivery",
			Evidence: ev, VettedAcq: acq,
		}
		write := func() (mqttOpOutcome, error) { return sess.jsa.opAbandonDeniedDelivery(req) }
		var out mqttOpOutcome
		var err error
		if hook != nil {
			out, err = hook(req, write)
		} else {
			out, err = write()
		}
		if err == nil {
			return mqttAbandonDone, nil, nil
		}
		lastErr = err
		if out.Handle == nil {
			// Refused before a batch was composed: the op's own reads decided. The pair may already
			// stand (an earlier attempt landed behind a lost response); otherwise it is an answer.
			if sess.deliveryAbandonedStands(ev) {
				return mqttAbandonDone, nil, nil
			}
			return mqttAbandonPending, nil, err
		}
		switch sess.settleCustodyHandle(out.Handle) {
		case mqttTransitionLanded, mqttTransitionSuperseded:
			return mqttAbandonDone, nil, nil
		case mqttTransitionUnresolved:
			return mqttAbandonUnresolved, []*mqttTransitionHandle{out.Handle}, fmt.Errorf("the abandonment's landing is unknown: %w", err)
		}
		// Positively never landed: contention. Re-attempt with fresh evidence.
		if sess.deliveryAbandonedStands(ev) {
			return mqttAbandonDone, nil, nil
		}
	}
	return mqttAbandonPending, nil, fmt.Errorf("the abandonment kept losing its batch to contention: %w", lastErr)
}

// abandonCurrentOutputVeto is the current-connection veto: it reads the register's acquisition
// ordinal and, when one was ever taken, finds that exact action among the generation's
// obligations. If it was acquired under the deciding connection's own tenure, the delivery's
// charge is this connection's quota and nothing is abandoned (errMQTTAbandonCurrentOutput). An
// unreadable register, census or action — or an ordinal whose action the census does not hold —
// refuses (errMQTTAbandonNoEvidence): token absence alone never proves the current connection
// never transmitted it. It returns the ordinal it judged, which the op then requires unchanged.
//
// Session lock NOT held on entry.
func (sess *mqttSession) abandonCurrentOutputVeto(ev mqttDenyEvidence) (uint64, error) {
	as := sess.asm
	reg, smsg, err := as.loadInflightRecord(ev.Subject)
	if err != nil || smsg == nil || reg == nil || reg.Nonce != ev.Nonce || reg.Lin != ev.Lin {
		return 0, errMQTTAbandonNoEvidence
	}
	if reg.Acq == 0 {
		return 0, nil
	}
	obs, oerr := as.enumerateGenerationObligations(sess.id, as.sessStreamEpoch, ev.Gen)
	if oerr != nil || obs == nil {
		return 0, errMQTTAbandonNoEvidence
	}
	for _, act := range obs.actions {
		if act == nil || act.Nonce != reg.Lin || act.Acq != reg.Acq {
			continue
		}
		if act.Tenure == ev.Tenure {
			return 0, errMQTTAbandonCurrentOutput
		}
		return reg.Acq, nil
	}
	return 0, errMQTTAbandonNoEvidence
}

// deliveryAbandonedStands reports whether the abandonment of exactly this delivery is already
// durable: its register at delivery_abandoned for the same allocation, and its exchange at
// DeliveryAbandoned.
//
// Session lock NOT held on entry.
func (sess *mqttSession) deliveryAbandonedStands(ev mqttDenyEvidence) bool {
	as := sess.asm
	if as == nil {
		return false
	}
	reg, smsg, err := as.loadInflightRecord(ev.Subject)
	if err != nil || smsg == nil || reg == nil || reg.State != mqttInflightDeliveryAbandoned ||
		reg.Nonce != ev.Nonce || reg.Lin != ev.Lin {
		return false
	}
	x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, ev.Gen, ev.Lin)
	return xerr == nil && x != nil && x.Phase == mqttExchangeDeliveryAbandoned
}
