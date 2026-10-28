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

// A subscription's fresh deliveries are admitted several at a time.

package mqtt

import (
	"fmt"
)

// mqttAdmissionBatchMax bounds how many fresh deliveries of one subscription are admitted in one
// batch, and so how many offers a subscription holds while it reads the ones already queued behind
// them. A batch carries 4 members per delivery plus one per resource and three more, far below the
// server's default atomic batch limit of 1000 messages.
const mqttAdmissionBatchMax = 16

// mqttBatchAdmission is one fresh delivery a batched admission carries: the allocation the delivery
// callback made for it, and what its send section needs once it is admitted.
type mqttBatchAdmission struct {
	pi    uint16
	qos   byte
	jsDur string
	sseq  uint64
	nonce string

	// rec is the register this admission writes, built by the ladder's preamble; nil until then.
	rec     *mqttInflightRecord
	subject string
	// stored is the outcome: the register is confirmed and the delivery may be sent.
	stored bool
	// done: this delivery's admission has concluded, stored or not.
	done bool
}

// mqttTestAdmissionBatchAttemptHook observes every attempt a batched admission submits: its
// participants' allocation nonces in order, and the write's error. Nil in production.
var mqttTestAdmissionBatchAttemptHook = new(mqttKnob[func(sess *mqttSession, nonces []string, err error)])

// mqttTestAdmissionBatchSettledHook observes the verdict applied to every participant of one batch
// attempt, immediately after it was applied. Nil in production.
var mqttTestAdmissionBatchSettledHook = new(mqttKnob[func(sess *mqttSession, nonces []string, v mqttTransitionVerdict)])

// mqttTestAdmissionBatchSingleHook observes a delivery a batched admission hands to the single
// ladder, before the ladder runs. Nil in production.
var mqttTestAdmissionBatchSingleHook = new(mqttKnob[func(sess *mqttSession, nonce string)])

// admitInflightPublishBatch admits fresh, ordinary (non-shared, not drain-claimed) deliveries of one
// subscription, in their order, and reports for each whether it may be sent.
//
// It is admitThroughAuthority for several creates at once, and every rule of that route holds for
// each delivery: the custody reservation is opened for every delivery in one critical section and
// held until the delivery's admission concludes; the entitlement is the connection's own, captured
// once; the dependencies are readiness-gated once per QoS for the whole batch; and each attempt is
// ONE atomic batch (opAdmitPublishes) whose outcome is settled ONCE and applied to every participant
// before anything is retried, split or abandoned.
//
// A failure keeps the deliveries' order. One defeated only by a stale expectation is re-attempted
// whole while every slot still reads empty, under the budget and the delivery entrance's contention
// extension; if a slot is occupied the batch splits there, the occupant takes the single ladder
// (whose classifier decides it), and the deliveries on either side are admitted around it in order.
// Any other failure hands the first delivery to the single ladder and re-attempts the rest. An
// attempt whose outcome stays unresolved abandons every participant exactly as the ladder's
// exhaustion does, so each keeps the continuity record its unresolved attempt owes.
//
// Session lock NOT held on entry.
func (sess *mqttSession) admitInflightPublishBatch(c *client, items []*mqttBatchAdmission) {
	if len(items) == 0 {
		return
	}
	if len(items) == 1 {
		// ONE DELIVERY IS THE SINGLE ROUTE, unchanged: a batch of one would only gate its
		// dependencies twice (here and again in the ladder it would hand the delivery to).
		it := items[0]
		it.stored = sess.admitInflightPublish(c, it.pi, it.qos, it.jsDur, it.sseq, it.nonce, _EMPTY_, _EMPTY_)
		it.done = true
		return
	}
	// THE CUSTODY RESERVATIONS, one per delivery, opened together. A delivery abandoned between its
	// tracking and here is not admitted at all, as on the single route.
	sess.mu.Lock()
	gen := sess.gen
	var open []*mqttBatchAdmission
	for _, it := range items {
		if sess.openAdmissionCustodyLocked(gen, it.pi, it.nonce) {
			open = append(open, it)
		} else {
			it.done = true
		}
	}
	sess.mu.Unlock()
	defer func() {
		for _, it := range open {
			sess.finishAdmissionCustody(gen, it.nonce)
		}
	}()
	if len(open) == 0 {
		return
	}
	release := sess.announceForegroundAuthority()
	defer release()
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testAdmitReserved
		as.mu.RUnlock()
		if hook != nil {
			for _, it := range open {
				hook(sess, it.nonce)
			}
		}
	}
	abandon := func(its []*mqttBatchAdmission) {
		for _, it := range its {
			if !it.done {
				sess.abandonInflightRegister(mqttLicenceOf(c), it.pi, it.jsDur, it.sseq, it.nonce)
				it.done = true
			}
		}
	}
	auth, eok := c.outboundEntitlement()
	if !eok {
		c.Warnf("MQTT: refusing to admit %d deliveries for client %q: the connection carries no accepted "+
			"outbound entitlement", len(open), sess.id)
		abandon(open)
		return
	}
	epoch := _EMPTY_
	if sess.asm != nil {
		epoch = sess.asm.sessStreamEpoch
	}
	// THE DEPENDENCIES, readiness-gated once per QoS present: every delivery of one subscription
	// depends on the same source consumer, and a QoS 2 one on the generation's PUBREL consumer too.
	depsByQoS := make(map[byte][]mqttAdmitDependency)
	var ready []*mqttBatchAdmission
	for _, it := range open {
		deps, ok := depsByQoS[it.qos]
		if !ok {
			var dok bool
			if deps, dok = sess.assembleAdmitDeps(c, it.jsDur, it.qos, epoch, auth.Gen); !dok {
				deps = nil
			}
			depsByQoS[it.qos] = deps
		}
		if deps == nil {
			abandon([]*mqttBatchAdmission{it})
			continue
		}
		ready = append(ready, it)
	}
	ready = sess.admissionBatchPreamble(c, ready)
	if len(ready) == 0 {
		return
	}
	sess.runAdmissionBatch(c, gen, epoch, auth, depsByQoS, ready, abandon)
}

// admissionBatchPreamble is runInflightAdmission's preamble for each delivery: the allocation must
// still be this one, and its register is built and validated. A delivery that fails it is settled
// exactly as the ladder settles it and leaves the batch.
//
// Session lock NOT held on entry.
func (sess *mqttSession) admissionBatchPreamble(c *client, items []*mqttBatchAdmission) []*mqttBatchAdmission {
	as := sess.asm
	sess.persistVoidedRegs(c)
	sess.mu.Lock()
	idHash, gen, regClaim, regClaimSeq := sess.idHash, sess.gen, sess.regClaim, sess.seq
	ours := make([]bool, len(items))
	replies := make([]string, len(items))
	for i, it := range items {
		ack := sess.pendingPublish[it.pi]
		ours[i] = ack != nil && ack.regPending && !ack.regCancelled &&
			ack.jsDur == it.jsDur && ack.sseq == it.sseq && ack.regNonce == it.nonce
		if ours[i] && !mqttDurIsSlotOnlyClass(it.jsDur) {
			replies[i] = ack.registerSourceAckReply()
		}
	}
	sess.mu.Unlock()
	var kept []*mqttBatchAdmission
	for i, it := range items {
		if as == nil || idHash == _EMPTY_ || gen == _EMPTY_ || it.nonce == _EMPTY_ {
			sess.abandonInflightRegister(mqttLicenceOf(c), it.pi, it.jsDur, it.sseq, it.nonce)
			it.done = true
			continue
		}
		if !ours[i] {
			sess.releaseCancelledReservation(it.pi, it.nonce)
			it.done = true
			continue
		}
		createLin, _ := mqttLineageForWrite(mqttLineageWriteCreate, nil, it.nonce)
		it.subject = mqttInflightSubject(idHash, gen, it.pi)
		it.rec = &mqttInflightRecord{
			PI: it.pi, Dur: it.jsDur, Seq: it.sseq, QoS: it.qos, SourceAckReply: replies[i],
			State: mqttInflightPublish, Nonce: it.nonce, Lin: createLin,
			Claim: regClaim, ClaimSeq: regClaimSeq,
		}
		if verr := mqttValidateInflightShape(it.rec); verr != nil {
			mqttWarnf(c, "MQTT: refusing to store an in-flight register for packet identifier %d of "+
				"client %q: %v", it.pi, sess.id, verr)
			sess.abandonInflightRegister(mqttLicenceOf(c), it.pi, it.jsDur, it.sseq, it.nonce)
			it.done = true
			continue
		}
		kept = append(kept, it)
	}
	return kept
}

// runAdmissionBatch submits the batch and carries out what its settled outcome licenses, in
// delivery order. Segments are worked front to back, so a split never lets a later delivery be
// admitted ahead of an earlier one that is still undecided.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runAdmissionBatch(c *client, gen, epoch string, auth mqttOutboundEntitlement,
	depsByQoS map[byte][]mqttAdmitDependency, items []*mqttBatchAdmission, abandon func([]*mqttBatchAdmission)) {

	for len(items) > 0 {
		if len(items) == 1 {
			sess.admitBatchDeliverySingly(c, gen, items[0])
			return
		}
		rest, single := sess.attemptAdmissionBatch(c, gen, epoch, auth, depsByQoS, items, abandon)
		if single != nil {
			sess.admitBatchDeliverySingly(c, gen, single)
		}
		items = rest
	}
}

// attemptAdmissionBatch runs one batch to its conclusion: landed (each delivery settled), abandoned,
// or split. On a split it returns the delivery the caller hands to the single ladder next and the
// deliveries after it, in order; any deliveries before the split point it has already admitted.
//
// Session lock NOT held on entry.
func (sess *mqttSession) attemptAdmissionBatch(c *client, gen, epoch string, auth mqttOutboundEntitlement,
	depsByQoS map[byte][]mqttAdmitDependency, items []*mqttBatchAdmission,
	abandon func([]*mqttBatchAdmission)) (rest []*mqttBatchAdmission, single *mqttBatchAdmission) {

	ext := &mqttDeliveryContentionExtension{sess: sess, c: c}
	if c != nil && c.mqtt != nil {
		ext.closing = c.mqtt.tracker.closingSignal()
	}
	budget := mqttForegroundAcquireAttempts
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt >= budget {
			if !ext.extend(attempt, lastErr) {
				break
			}
			budget = attempt + 1
		}
		nonces := make([]string, len(items))
		entries := make([]mqttAdmitPublishesEntry, len(items))
		for i, it := range items {
			nonces[i] = it.nonce
			entries[i] = mqttAdmitPublishesEntry{Register: it.rec, Deps: depsByQoS[it.qos]}
		}
		out, err := sess.admitWrite(false, func() (mqttOpOutcome, error) {
			return sess.jsa.opAdmitPublishes(&mqttAdmitPublishesRequest{
				ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-admit-publishes",
				Gen: auth.Gen, Tenure: auth.Tenure, Entries: entries,
			})
		})
		if h := mqttTestAdmissionBatchAttemptHook.Get(); h != nil {
			h(sess, nonces, err)
		}
		verdict := sess.settleBatchAttempt(gen, items, out, err)
		if h := mqttTestAdmissionBatchSettledHook.Get(); h != nil {
			h(sess, nonces, verdict)
		}
		switch verdict {
		case mqttTransitionLanded, mqttTransitionSuperseded:
			sess.settleLandedBatch(c, gen, items)
			return nil, nil
		case mqttTransitionUnresolved:
			// NOTHING CAN BE CONCLUDED, so nothing may be retried or split: a re-attempt of a batch
			// that landed would meet its own registers, and a split would re-admit its deliveries.
			// Every participant is abandoned as the ladder abandons on exhaustion; each keeps the
			// continuity record its unsettled attempt owes.
			c.Errorf("MQTT: the admission of %d deliveries for client %q could not be resolved: %v",
				len(items), sess.id, err)
			abandon(items)
			return nil, nil
		}
		// POSITIVELY NEVER LANDED, or refused before it was submitted.
		lastErr = err
		ext.noteWrite(err)
		if !mqttIsStaleExpectation(err) {
			// A refusal that is not contention: the first delivery takes the single ladder, whose
			// classifier decides what its slot holds, and the rest are attempted again after it.
			return items[1:], items[0]
		}
		if out.Handle != nil && attempt+1 < budget {
			sess.authorityContentionWait()
		}
		// WHICH SLOT, if any, made it stale: the store's create-only rejection cannot say, so each
		// register subject is read, after the wait, in order.
		for k, it := range items {
			cur, smsg, lerr := sess.asm.loadInflightRecord(it.subject)
			if lerr == nil && smsg == nil && cur == nil {
				continue
			}
			if k == 0 {
				return items[1:], items[0]
			}
			// The deliveries before k are admitted first, as a batch of their own; k follows singly.
			sess.runAdmissionBatch(c, gen, epoch, auth, depsByQoS, items[:k], abandon)
			return items[k+1:], items[k]
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("the admission was not resolved after %d attempts", budget)
	}
	c.Errorf("MQTT: unable to admit %d deliveries for client %q: %v", len(items), sess.id, lastErr)
	abandon(items)
	return nil, nil
}

// admitBatchDeliverySingly runs the single ladder for one delivery of a batch, under the custody
// reservation the batch opened for it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) admitBatchDeliverySingly(c *client, gen string, it *mqttBatchAdmission) {
	if it.done {
		return
	}
	if h := mqttTestAdmissionBatchSingleHook.Get(); h != nil {
		h(sess, it.nonce)
	}
	it.stored = sess.admitUnderReservation(c, gen, it.pi, it.qos, it.jsDur, it.sseq, it.nonce, _EMPTY_, _EMPTY_, nil)
	it.done = true
}

// settleBatchAttempt records one batch attempt on every participant's reservation and settles it
// ONCE: the handle is resolved (and, unresolved, aborted create-only and resolved again) a single
// time, and the verdict applied to every participant in one critical section. A refusal decided
// before submission carries no handle, records nothing and reads as never landed.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleBatchAttempt(gen string, items []*mqttBatchAdmission, out mqttOpOutcome, err error) mqttTransitionVerdict {
	if err == nil {
		sess.mu.Lock()
		for _, it := range items {
			sess.custody.attemptLanded(gen, it.nonce, nil, it.rec.Lin)
		}
		sess.mu.Unlock()
		return mqttTransitionLanded
	}
	if out.Handle == nil {
		return mqttTransitionNeverLanded
	}
	sess.mu.Lock()
	for _, it := range items {
		sess.custody.recordAttempt(gen, it.nonce, mqttCustodyAttempt{handle: out.Handle, lin: it.rec.Lin})
	}
	sess.mu.Unlock()
	v := sess.settleCustodyHandle(out.Handle)
	released := false
	sess.mu.Lock()
	switch v {
	case mqttTransitionLanded, mqttTransitionSuperseded:
		for _, it := range items {
			sess.custody.attemptLanded(gen, it.nonce, out.Handle, it.rec.Lin)
		}
	case mqttTransitionNeverLanded:
		for _, it := range items {
			if sess.custody.attemptNeverLanded(gen, it.nonce, out.Handle) {
				released = true
			}
		}
	}
	sess.mu.Unlock()
	if released {
		sess.releaseDeferredAfterCustody()
	}
	return v
}

// settleLandedBatch settles every register of a landed batch in order, as the ladder settles its
// own landed write: the register is read back by its nonce (the batch's acknowledgement names its
// authority member, not a register), and the send state derived from what landed. A delivery whose
// read-back fails takes the single ladder under its reservation, whose create meets the standing
// register and whose classifier adopts it by nonce.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleLandedBatch(c *client, gen string, items []*mqttBatchAdmission) {
	for _, it := range items {
		seq, err := sess.landedRegisterSeq(it.subject, it.nonce)
		if err != nil {
			sess.admitBatchDeliverySingly(c, gen, it)
			continue
		}
		it.stored = sess.settleLandedRegister(c, mqttLandedRegister{gen: gen, pi: it.pi, jsDur: it.jsDur,
			sseq: it.sseq, regSeq: seq, nonce: it.nonce, lin: it.rec.Lin, qos: it.rec.QoS}, it.subject, false)
		it.done = true
	}
}
