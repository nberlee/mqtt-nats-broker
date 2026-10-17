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
// A RETAINED delivery (F-105 7c) is abandoned by the same op and the same fact, in its retained
// arm: the fact names the copy instead of a source, no source is settled, and a second reason
// exists — a possibly-sent replay that no longer fits the resumed connection's Maximum Packet
// Size. The decision is made before any output is acquired (outbound_retained_abandon.go).
//
// What the abandonment releases, and what it does not. The send quota is connection-local
// (Spec5 §4.9: it is re-initialized with each Network Connection), and the resumed connection
// will never send this message, so its CHARGE goes. The IDENTIFIER does not: §4.3.2-4.3.3 tie
// reuse to the acknowledgement, and a local ACL decision is not one. It is held until the
// generation is discarded. Abandoning the retransmission §4.4 would otherwise require is a
// stated conformance deviation (docs/CONFORMANCE.md), exactly as HEAD's deny was.

// mqttAbandonReasonACLDeny is the one reason this broker abandons an ORDINARY delivery for, and
// one of the two a retained one is abandoned for: a proven delivery-time authorization deny of a
// restored delivery.
const mqttAbandonReasonACLDeny = "acl-deny"

// mqttAbandonReasonPacketTooLarge is the RETAINED arm's second reason (F-105 7c): a possibly
// sent retained replay that no longer fits the resumed connection's Maximum Packet Size. Spec5
// [3.1.2.11.4]: the server "MUST discard it without sending it and then behave as if it had
// completed sending that Application Message". Never valid on the ordinary arm.
const mqttAbandonReasonPacketTooLarge = "packet-too-large"

// mqttValidAbandonReason is the closed set of ORDINARY abandonment reasons.
func mqttValidAbandonReason(r string) bool {
	return r == mqttAbandonReasonACLDeny
}

// mqttValidRetainedAbandonReason is the closed set of RETAINED abandonment reasons.
func mqttValidRetainedAbandonReason(r string) bool {
	return r == mqttAbandonReasonACLDeny || r == mqttAbandonReasonPacketTooLarge
}

// mqttValidateRetainedAbandonment is the retained arm's closed schema: the reason in its set,
// tenure and allocation well formed, the payload a material identity, and NO source coordinate.
func mqttValidateRetainedAbandonment(f *mqttExchangeAbandonment) error {
	switch {
	case !mqttValidRetainedAbandonReason(f.Reason):
		return fmt.Errorf("the retained abandonment fact carries reason %q, which this broker does not abandon for", f.Reason)
	case !mqttF105IdentTk(f.Tenure):
		return fmt.Errorf("the abandonment fact carries a malformed tenure %q", f.Tenure)
	case !mqttF105IdentTk(f.Alloc):
		return fmt.Errorf("the abandonment fact carries a malformed allocation %q", f.Alloc)
	case !mqttF105IdentTk(f.Payload):
		return fmt.Errorf("the retained abandonment fact names payload %q, which is not a material identity", f.Payload)
	case f.Dur != _EMPTY_ || f.Seq != 0 || f.Reply != _EMPTY_:
		return fmt.Errorf("the retained abandonment fact carries source coordinates %q/%d: a retained "+
			"delivery was never a source delivery", f.Dur, f.Seq)
	}
	return nil
}

// errMQTTAbandonNoEvidence reports that the op's own reads did not find the delivery as an
// abandonment needs it — moved, tokened, not at publish, of another arm (affine, or ordinary for a
// retained decision and the reverse), not the delivery the decision named, or its last
// acquisition unreadable. Nothing was written.
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
	// Payload is set on the RETAINED arm instead of Dur, Seq and Reply (F-105 7c): a retained
	// delivery was never a source delivery, so its decision names the copy it would have sent.
	Payload string
}

func (ev *mqttDenyEvidence) complete() bool {
	if ev == nil || ev.Gen == _EMPTY_ || ev.Tenure == _EMPTY_ || ev.Subject == _EMPTY_ ||
		ev.Nonce == _EMPTY_ || ev.Lin == _EMPTY_ || ev.PI == 0 {
		return false
	}
	if ev.Payload != _EMPTY_ {
		return mqttValidRetainedAbandonReason(ev.Reason) && ev.Dur == _EMPTY_ && ev.Seq == 0 && ev.Reply == _EMPTY_
	}
	return ev.Dur != _EMPTY_ && ev.Seq != 0 && mqttValidAbandonReason(ev.Reason) &&
		mqttSourceAckReplyBinds(ev.Reply, mqttStreamName, ev.Dur, ev.Seq)
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

// opAbandonDeniedDelivery abandons one restored delivery on a positive decision: an ordinary one
// on a deny, a RETAINED one (F-105 7c) on a deny or on a replay that no longer fits the resumed
// connection's Maximum Packet Size. The retained arm names the copy instead of a source and
// settles no source.
//
// SERVING AUTHORITY AT THE DECISION'S BINDING, read FIRST: the decision was made under one
// connection's entitlement, and it is only this broker's to act on while that entitlement still
// serves. A takeover changed the tenure, and the decision is refused (here and at the write
// boundary). Then the register (publish, no token, of the decision's arm, the exact delivery), the exchange
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
	retained := ev.Payload != _EMPTY_
	if rmsg == nil || reg == nil || reg.State != mqttInflightPublish || reg.ActionToken != _EMPTY_ ||
		reg.Nonce != ev.Nonce || reg.Lin != ev.Lin || reg.PI != ev.PI || reg.retainedSource() != retained ||
		(retained && reg.Payload != ev.Payload) ||
		(!retained && (mqttDurIsSlotOnlyClass(reg.Dur) || reg.Dur != ev.Dur || reg.Seq != ev.Seq)) {
		return mqttOpOutcome{}, errMQTTAbandonNoEvidence
	}
	x, xSeq, xerr := jsa.loadOutboundExchange(r.ID, r.Epoch, ev.Gen, reg.Lin)
	if xerr != nil {
		return mqttOpOutcome{}, xerr
	}
	if x != nil {
		if perr := mqttCheckSharedAcqPair(reg, x); perr != nil {
			return mqttOpOutcome{}, perr
		}
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
	// its acquisition, and its source and the reply the denied offer carried — or, on the
	// retained arm, the copy it would have sent.
	ended.Abandoned = &mqttExchangeAbandonment{
		Reason: ev.Reason, Tenure: ev.Tenure, Alloc: reg.Nonce, Acq: reg.Acq,
		Dur: reg.Dur, Seq: reg.Seq, Reply: ev.Reply,
	}
	if reg.retainedSource() {
		ended.Abandoned = &mqttExchangeAbandonment{
			Reason: ev.Reason, Tenure: ev.Tenure, Alloc: reg.Nonce, Acq: reg.Acq, Payload: reg.Payload,
		}
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
// a tokenless ordinary or retained PUBLISH register, paired with its own exchange's abandonment of
// the same arm (a retained one naming the same copy) — is
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
	retained := cur.retainedSource()
	if !retained && mqttDurIsSlotOnlyClass(cur.Dur) {
		return fmt.Errorf("transition %q abandons the register on %q of source %q: only an ordinary "+
			"or retained delivery is abandoned; an affine one keeps its own disposal", a.Txn, subj, cur.Dur)
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
			f.Seq == cur.Seq && f.Payload == cur.Payload && f.retained() == retained {
			paired = true
		}
	}
	if !paired {
		return fmt.Errorf("transition %q abandons the register on %q without the abandonment of its "+
			"obligation %q beside it", a.Txn, subj, cur.Lin)
	}
	validReason := mqttValidAbandonReason
	if retained {
		validReason = mqttValidRetainedAbandonReason
	}
	if !validReason(rec.AbandonReason) {
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
	// mqttAbandonPending: no callback source-ACK authority. A retained session owner
	// retries independently of delivery traffic, unless a positive veto ended the proposal.
	mqttAbandonPending
)

// submitAbandonDeniedDelivery performs one attempt. Its caller owns the handle,
// retry clock and immutable evidence before entering this primitive.
func (sess *mqttSession) submitAbandonDeniedDelivery(ev mqttDenyEvidence, acq uint64) (mqttOpOutcome, error) {
	req := &mqttAbandonDeliveryRequest{
		ID: sess.id, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(),
		Op: "op-abandon-delivery", Evidence: ev, VettedAcq: acq,
	}
	sess.asm.mu.RLock()
	hook := sess.asm.testAbandonDelivery
	sess.asm.mu.RUnlock()
	write := func() (mqttOpOutcome, error) { return sess.jsa.opAbandonDeniedDelivery(req) }
	if hook != nil {
		return hook(req, write)
	}
	return write()
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
