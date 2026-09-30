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
	"time"
)

// A vanished canonical PUBREL is RE-MATERIALISED, not waited on for ever (F-105 7b, row 6's
// continuation 3).
//
// An ordinary `pubrel_ready` slot names its canonical PUBREL record by sequence. A PUBCOMP ends
// the exchange before any record disposal (and since row 2 the record is not acknowledged before
// the completion is recorded), so a record that is ABSENT while the exchange still reads
// PubRecObserved vanished for another reason — and nothing would ever deliver the PUBREL the
// client is waiting for; the identifier would be held until the generation is discarded.
//
// The slot is NEVER re-phased: `pubrel_intended` is not an arm the PUBCOMP handler, the ack arrow
// or the exchange origin accept, so a re-phase would strand the PUBCOMP of a PUBREL already sent.
// It stays `pubrel_ready` on its old tuple throughout, and re-materialisation is a REBIND of that
// tuple, in four steps, each re-derived from the store on every attempt:
//
//  1. ELIGIBILITY from the resource record: the PUBREL consumer resource is LIVE and ready for use
//     and carries this exchange's canonical-pubrel edge. That edge's discharge requires the
//     exchange's ending, which also moves the register, so while the rebind can land the resource
//     cannot be deletion-authorized. Anything else is withheld; no consumer is ever created here.
//  2. PUBLISH a new canonical record from the slot's identity with an EMPTY message ID (a retained
//     dedup history could otherwise acknowledge the publish without restoring the record), and
//     require a READBACK of it: an interest-retained stream answers a store with a sequence while
//     keeping nothing.
//  3. REBIND in one rebind-canonical authority batch under the recovering connection's
//     entitlement: the register at its exact sequence, only its canonical sequence moved, beside
//     an exchange the boundary reads at PubRecObserved.
//  4. SETTLE the rebind through its attempt handle — landed (or superseded) is done; rejected
//     disposes of the published candidate only after re-reading the register and proving it does
//     not name the candidate; unresolved is resolved before anything else, never guessed.
//
// It creates no admission, no exchange, no ordinal and no charge. A crash anywhere leaves at worst
// a non-canonical duplicate record, which the PUBREL delivery callback refuses against the slot's
// canonical sequence and which recovery treats as a duplicate of the canonical choice.

// checkCanonicalRebindRegister is the write boundary's rule for a rebind-canonical register member:
// the exact untokened ordinary pubrel_ready occupant with only its canonical sequence moved, to a
// different non-zero one, beside the occupant's exchange standing at PubRecObserved.
func (jsa *mqttJSA) checkCanonicalRebindRegister(a *mqttTransitionAttempt, gen, subj string,
	cur, rec *mqttInflightRecord) error {

	if cur.State != mqttInflightPubRelReady || cur.ActionToken != _EMPTY_ || mqttDurIsSlotOnlyClass(cur.Dur) {
		return fmt.Errorf("transition %q re-binds the register on %q in state %q: only an ordinary untokened "+
			"pubrel_ready slot is re-bound", a.Txn, subj, string(cur.State))
	}
	want := *cur
	want.PubRelStreamSeq = rec.PubRelStreamSeq
	if rec.PubRelStreamSeq == 0 || rec.PubRelStreamSeq == cur.PubRelStreamSeq || want != *rec {
		return fmt.Errorf("transition %q re-binds the register on %q with a body that changes more than "+
			"its canonical sequence, or moves it nowhere", a.Txn, subj)
	}
	x, _, xerr := jsa.loadOutboundExchange(a.ID, a.Epoch, gen, cur.Lin)
	if xerr != nil || x == nil || x.Phase != mqttExchangePubRecObserved || x.PI != cur.PI || x.QoS != 2 {
		return fmt.Errorf("transition %q re-binds the register on %q while its exchange %q does not stand at "+
			"PubRecObserved: only an owed release is re-bound", a.Txn, subj, cur.Lin)
	}
	return nil
}

// mqttRebindCanonicalRequest names the slot to re-bind: its subject, its exact sequence, and the
// body with the new canonical sequence; Ent is the deciding connection's entitlement.
type mqttRebindCanonicalRequest struct {
	ID, Epoch, Txn, Op string
	Ent                mqttOutboundEntitlement
	Subject            string
	Expect             uint64
	Body               *mqttInflightRecord
}

// opRebindCanonical commits one rebind-canonical transition under the serving authority, which
// must be Ent's generation and tenure.
func (jsa *mqttJSA) opRebindCanonical(r *mqttRebindCanonicalRequest) (mqttOpOutcome, error) {
	if r == nil || r.Subject == _EMPTY_ || r.Body == nil || r.Expect == 0 {
		return mqttOpOutcome{}, fmt.Errorf("re-binding a canonical PUBREL requires the slot, its sequence and its body")
	}
	auth, authSeq, err := jsa.servingAuthority(r.ID, r.Epoch)
	if err != nil {
		return mqttOpOutcome{}, err
	}
	if auth.Gen != r.Ent.Gen || auth.Tenure != r.Ent.Tenure {
		return mqttOpOutcome{}, fmt.Errorf("re-binding %q under %q/%q: the authority serves %q/%q: %w",
			r.Subject, r.Ent.Gen, r.Ent.Tenure, auth.Gen, auth.Tenure, errMQTTEntitlementEnded)
	}
	if verr := mqttValidateInflightShape(r.Body); verr != nil {
		return mqttOpOutcome{}, fmt.Errorf("re-binding %q builds an unsupported slot: %w", r.Subject, verr)
	}
	body, merr := json.Marshal(r.Body)
	if merr != nil {
		return mqttOpOutcome{}, merr
	}
	return jsa.commitOp(&mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: auth.ID, Epoch: auth.Epoch, Txn: r.Txn, Op: r.Op,
			Kind: mqttTransitionRebindCanonical, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: r.Ent.Gen, Tenure: r.Ent.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: authSeq,
		// Reinstalled exactly as it stood: a re-bind is not an ownership event.
		Auth: &mqttOutboundAuthority{
			ID: auth.ID, Epoch: auth.Epoch, Gen: auth.Gen, Tenure: auth.Tenure,
			State: auth.State, Rev: auth.Rev + 1, Txn: r.Txn, OwnerTxn: auth.OwnerTxn,
			Intent: auth.Intent, Creation: auth.Creation,
		},
		Extra: []mqttTransitionMember{{subj: r.Subject, expect: r.Expect, body: body}},
	})
}

// mqttRematerialization is one enrolled re-materialisation: the exact slot identity recovery found,
// the canonical sequence it names that vanished, and the licence of the connection that enrolled it.
type mqttRematerialization struct {
	slot   mqttInflightSlotRef
	oldSeq uint64
	lin    string
	lic    mqttDestroyLicence
	done   bool
}

// scheduleCanonicalRematerialization is the continuation's timer, the owed materialization's
// pattern: unlimited attempts with a fixed delay, armed only while the shutdown gate admits it, and
// stopped by a generation change or a settled outcome. A refused arming leaves the durable slot,
// which the next resume re-enrolls.
//
// Session lock NOT held on entry.
func (sess *mqttSession) scheduleCanonicalRematerialization(c *client, rm *mqttRematerialization) {
	if rm == nil {
		return
	}
	sess.mu.Lock()
	if rm.done || rm.slot.name.gen != sess.gen {
		rm.done = true
		sess.mu.Unlock()
		return
	}
	sess.mu.Unlock()
	srv := sess.serverFor(c)
	if srv == nil || !srv.admitWillWorker() {
		return
	}
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer srv.willWorkerWG.Done()
		sess.mu.Lock()
		stop := rm.done || rm.slot.name.gen != sess.gen
		sess.mu.Unlock()
		if stop {
			return
		}
		sess.attemptCanonicalRematerialization(c, rm)
	})
}

// mqttRematerializeOutcome is what one attempt concluded.
type mqttRematerializeOutcome uint8

const (
	// mqttRematerializeRetry: nothing settled (withheld, unreadable, unresolved); try again.
	mqttRematerializeRetry mqttRematerializeOutcome = iota
	// mqttRematerializeDone: re-bound, or there is nothing left to re-materialise (the slot moved
	// on, the record is present, the exchange ended, or the decision is no longer this
	// connection's).
	mqttRematerializeDone
)

// attemptCanonicalRematerialization makes one attempt and reschedules unless it settled.
//
// Session lock NOT held on entry.
func (sess *mqttSession) attemptCanonicalRematerialization(c *client, rm *mqttRematerialization) {
	if sess.rematerializeOnce(c, rm) == mqttRematerializeDone {
		sess.mu.Lock()
		rm.done = true
		sess.mu.Unlock()
		return
	}
	sess.scheduleCanonicalRematerialization(c, rm)
}

// rematerializeOnce is one attempt of the four steps, re-deriving every fact from the store.
//
// Session lock NOT held on entry.
func (sess *mqttSession) rematerializeOnce(c *client, rm *mqttRematerialization) mqttRematerializeOutcome {
	as, jsa := sess.asm, sess.jsa
	if as == nil || jsa == nil || !rm.lic.entOK {
		return mqttRematerializeDone
	}
	epoch, gen := as.sessStreamEpoch, rm.slot.name.gen
	subject := rm.slot.name.subject
	cur, smsg, lerr := as.loadInflightRecord(subject)
	switch {
	case lerr != nil:
		return mqttRematerializeRetry
	case smsg == nil || cur == nil || !rm.slot.matchesRecord(cur) || cur.State != mqttInflightPubRelReady ||
		cur.PubRelStreamSeq != rm.oldSeq || mqttDurIsSlotOnlyClass(cur.Dur):
		// The slot moved on: completed, re-bound by another attempt, or not this exchange.
		return mqttRematerializeDone
	case cur.ActionToken != _EMPTY_:
		return mqttRematerializeRetry // a live holder: its drain settles first
	}
	// The named record, by POINT READ: present means nothing vanished.
	pubRelSubject, _, dur := mqttPubRelNames(rm.slot.name.idHash, gen)
	if pubRelSubject == _EMPTY_ || dur == _EMPTY_ || cur.PubRelDurable != dur {
		return mqttRematerializeDone
	}
	old, oerr := jsa.loadMsg(mqttOutStreamName, rm.oldSeq)
	switch {
	case oerr != nil && isErrorOtherThan(oerr, JSNoMessageFoundErr):
		return mqttRematerializeRetry
	case oerr == nil && old != nil:
		return mqttRematerializeDone
	}
	// The exchange, read AFTER the record's absence: a PUBCOMP ends it before any disposal.
	x, _, xerr := jsa.loadOutboundExchange(sess.id, epoch, gen, cur.Lin)
	if xerr != nil || x == nil {
		return mqttRematerializeRetry
	}
	if x.Phase != mqttExchangePubRecObserved {
		return mqttRematerializeDone
	}
	// 1. Eligibility.
	if !sess.pubRelResourceHoldsExchange(c, epoch, gen, dur, cur.Lin) {
		return mqttRematerializeRetry
	}
	// 2. Publish, and prove the record by reading it back.
	msg, hdrLen := mqttNewCorrelatedPubRel(cur.PI, cur.Nonce, cur.Dur, cur.Seq, _EMPTY_, cur.Q2AffClaim, cur.Q2AffCopy)
	if msg == nil {
		return mqttRematerializeDone
	}
	resp, serr := jsa.storeMsg(pubRelSubject, hdrLen, msg)
	if serr != nil || resp == nil || resp.Error != nil || resp.Sequence == 0 {
		return mqttRematerializeRetry
	}
	candidate := resp.Sequence
	if !sess.canonicalPubRelProven(pubRelSubject, candidate, cur) {
		return mqttRematerializeRetry
	}
	// 3. Rebind.
	next := *cur
	next.PubRelStreamSeq = candidate
	out, rerr := jsa.opRebindCanonical(&mqttRebindCanonicalRequest{
		ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-rebind-canonical",
		Ent: rm.lic.ent, Subject: subject, Expect: smsg.Sequence, Body: &next,
	})
	// 4. Settle.
	v := mqttTransitionLanded
	if rerr != nil {
		if out.Handle == nil {
			v = mqttTransitionNeverLanded
		} else {
			v = jsa.resolveTransitionHandle(out.Handle)
			if v == mqttTransitionUnresolved {
				_ = jsa.abortTransition(&out.Handle.attempt)
				v = jsa.resolveTransitionHandle(out.Handle)
			}
		}
	}
	switch v {
	case mqttTransitionLanded, mqttTransitionSuperseded:
		return mqttRematerializeDone
	case mqttTransitionNeverLanded:
		sess.disposeRematerializeCandidate(subject, pubRelSubject, candidate)
		if errors.Is(rerr, errMQTTEntitlementEnded) {
			return mqttRematerializeDone // the successor's recovery owns it now
		}
		return mqttRematerializeRetry
	}
	return mqttRematerializeRetry // unresolved: the candidate is kept, nothing is guessed
}

// pubRelResourceHoldsExchange is step 1: the PUBREL consumer resource is ready for use (LIVE, its
// name-ownership naming it, the physical consumer present) and carries this exchange's
// canonical-pubrel edge.
//
// Session lock NOT held on entry.
func (sess *mqttSession) pubRelResourceHoldsExchange(c *client, epoch, gen, dur, lin string) bool {
	inc, ok := sess.readyDependencyInc(c, mqttOwnedStreamOut, mqttOutStreamName, dur, epoch, gen)
	if !ok {
		return false
	}
	r, _, err := sess.jsa.loadOutboundResource(sess.id, epoch, gen, inc)
	if err != nil || r == nil || r.State != mqttResourceLive {
		return false
	}
	for _, e := range r.Deps {
		if e.Role == mqttRoleCanonicalPubRel && e.Nonce == lin {
			return true
		}
	}
	return false
}

// canonicalPubRelProven reads a stored PUBREL back and requires a present record on the
// incarnation's PUBREL subject whose shape matches the slot.
//
// Session lock NOT held on entry.
func (sess *mqttSession) canonicalPubRelProven(pubRelSubject string, seq uint64, slot *mqttInflightRecord) bool {
	smsg, err := sess.jsa.loadMsg(mqttOutStreamName, seq)
	if err != nil || smsg == nil || smsg.Subject != pubRelSubject {
		return false
	}
	shape, derr := mqttDecodeStoredPubRel(smsg.Header)
	return derr == nil && mqttStoredPubRelShapeMatchesSlot(shape, slot)
}

// disposeRematerializeCandidate deletes a published candidate that did not become canonical — but
// only after re-reading the register and proving it does not name the candidate (or is gone). A
// register that cannot be read keeps the candidate: a duplicate is harmless, deleting the
// canonical record is not.
//
// Session lock NOT held on entry.
func (sess *mqttSession) disposeRematerializeCandidate(subject, pubRelSubject string, candidate uint64) {
	cur, smsg, lerr := sess.asm.loadInflightRecord(subject)
	if lerr != nil {
		return
	}
	if smsg != nil && cur != nil && cur.PubRelStreamSeq == candidate {
		return
	}
	_ = sess.jsa.deleteMsg(mqttOutStreamName, candidate, true)
}
