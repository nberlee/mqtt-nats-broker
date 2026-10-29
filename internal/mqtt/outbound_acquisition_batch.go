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

// A flush's admitted deliveries are acquired together: every send section opened, one
// acquire-action transition for all of them, then each written and ended in offer order.

package mqtt

import (
	"errors"
	"time"
)

// mqttAcquirePart is how one opened section of a flush came out of the batched acquisition.
type mqttAcquirePart struct {
	// fellOut: the section's premise did not hold when the batch read its register (a token
	// standing, the slot moved, an unreadable read), or the batch was refused before submission;
	// it takes the single acquisition at its own turn.
	fellOut bool
	// acquired: the batch landed and the section adopted its acquisition; it may transmit.
	acquired bool
	cur      *mqttInflightRecord
	regSeq   uint64
}

// mqttTestAcquisitionBatchHook observes every settled batch attempt of a flush: the participants'
// identifiers and the verdict. Nil in production.
var mqttTestAcquisitionBatchHook = new(mqttKnob[func(sess *mqttSession, pis []uint16, v mqttTransitionVerdict)])

// mqttTestAcquisitionBatchAdoptedHook runs for each participant after its acquisition was adopted
// and before any section of the flush writes. Nil in production.
var mqttTestAcquisitionBatchAdoptedHook = new(mqttKnob[func(sess *mqttSession, pi uint16, tok *mqttSendToken)])

// mqttTestAcquisitionBatchPauseHook runs once per flush after the batched acquisition settled and
// before the first section writes. Nil in production.
var mqttTestAcquisitionBatchPauseHook = new(mqttKnob[func(sess *mqttSession, pis []uint16)])

// mqttTestAcquisitionBatchCommitHook, when set, replaces the batch's submission: it is handed the
// requests and returns what opAcquireActions would. Nil in production.
var mqttTestAcquisitionBatchCommitHook = new(mqttKnob[func(sess *mqttSession,
	reqs []*mqttAcquireActionRequest) (mqttOpOutcome, []*mqttOutboundAction, error)])

// mqttTestAcquisitionBatchReadBackHook, when set and returning false, fails a participant's
// read-back after a landed batch. Nil in production.
var mqttTestAcquisitionBatchReadBackHook = new(mqttKnob[func(sess *mqttSession, pi uint16) bool])

// sendAdmittedPublishes sends a flush's admitted deliveries: every section opened, their PUBLISH
// actions acquired in ONE authority transition, then each written and ended in offer order.
//
// WRITES STAY SEQUENTIAL, IN OFFER ORDER, on the subscription's one dispatch goroutine, which is
// what carries per-(Topic, QoS) order ([MQTT-4.6.0-6]); only the durable acquisition is shared.
// Each section is ended (its token drained, its reservation and shutdown barrier released) before
// the next one writes, so no first drain of a holder can run while its frame is being written,
// and the later sections hold their acquisitions while the earlier ones write.
//
// A connection whose output no longer admits writes (sealed by a close, a takeover's
// certification or a torn writer) stops the flush before the next section: that section and every
// later one end without recording intent, so none deletes its unsent marker after the seal.
//
// Session lock NOT held on entry.
func (sess *mqttSession) sendAdmittedPublishes(live []*mqttCollectedDelivery) {
	var secs []*mqttPublishSection
	// ARMED BEFORE ANY SECTION OPENS: every opened section is ended exactly once, in order, on the
	// sealed-output exit and on a panic too ("only endSend removes" must not depend on no early
	// return). end is idempotent, so the sections the loop already ended are skipped.
	defer func() {
		for _, sec := range secs {
			sec.end()
		}
	}()
	for _, d := range live {
		if !d.adm.stored {
			continue
		}
		if h := mqttTestDeliveryAdmittedPauseV.Load(); h != nil {
			(*h)(sess, d.adm.pi)
		}
		sec := sess.openPublishSection(d.pc, d.cc, d.sub, d.adm.pi, d.adm.qos, d.adm.sseq, d.reply, d.retainAsPub,
			d.subID, d.topic, d.msg, d.props)
		if sec != nil {
			secs = append(secs, sec)
		}
	}
	switch len(secs) {
	case 0:
		return
	case 1:
		// ONE SECTION IS THE SINGLE ROUTE: no batch transition for one acquisition.
		sec := secs[0]
		if sess.acquirePublishAction(sec.cc, sec.pi, &sec.tok) {
			sess.transmitAcquired(sec)
		}
		sec.end()
		return
	}
	parts := sess.acquirePublishActions(secs)
	if h := mqttTestAcquisitionBatchPauseHook.Get(); h != nil {
		pis := make([]uint16, len(secs))
		for i, sec := range secs {
			pis[i] = sec.pi
		}
		h(sess, pis)
	}
	for i, sec := range secs {
		if b := sec.cc.outBind; b != nil && !b.mayWrite() {
			return // the deferred loop ends this section and every later one, with no intent
		}
		switch {
		case parts[i].fellOut:
			if sess.acquirePublishAction(sec.cc, sec.pi, &sec.tok) {
				sess.transmitAcquired(sec)
			}
		case parts[i].acquired:
			sess.transmitAcquired(sec)
		}
		sec.end()
	}
}

// acquirePublishActions acquires the PUBLISH actions of a flush's opened sections in ONE
// acquire-action transition, under the connection's pinned entitlement.
//
// Each section's premise is the single acquisition's, read from its register: the slot stands,
// names the section's allocation, is at publish, carries NO token and is of an ordinary class. A
// section whose premise fails falls out and takes the single acquisition at its turn. The others
// are marked as their tokens' holders FOR CLEANUP before anything is submitted, exactly as the
// single acquisition arms its tentative ownership.
//
// EVERY SUBMITTED ATTEMPT IS SETTLED BY ITS RECEIPT, never by reading what it may have written: a
// batch whose request timed out can still land after a negative read, and only the create-only
// abort makes "never landed" permanent. LANDED (or SUPERSEDED) adopts every participant; NEVER
// LANDED is re-attempted with the participants whose premise still holds; UNRESOLVED concludes
// nothing and releases nothing: the attempt is recorded as an owed settlement the custody
// dispatcher carries until the receipt decides it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) acquirePublishActions(secs []*mqttPublishSection) []mqttAcquirePart {
	parts := make([]mqttAcquirePart, len(secs))
	as := sess.asm
	if as == nil {
		for i := range parts {
			parts[i].fellOut = true
		}
		return parts
	}
	c := secs[0].cc
	// ONE FOREGROUND ANNOUNCEMENT for the whole batch, held through every attempt and the
	// settlement (outbound_foreground_priority.go).
	release := sess.announceForegroundAuthority()
	defer release()
	var in []int
	for i, sec := range secs {
		cur, seq, ok := sess.acquirePremise(as, sec)
		if !ok || sec.cc != c {
			parts[i].fellOut = true
			continue
		}
		parts[i].cur, parts[i].regSeq = cur, seq
		in = append(in, i)
	}
	if len(in) == 0 {
		return parts
	}
	sess.mu.Lock()
	for _, i := range in {
		tok := &secs[i].tok
		tok.actionHeld = true
		sess.markAckResolutionPendingLocked(tok.res, tok.token)
	}
	sess.mu.Unlock()
	for _, i := range in {
		if out := secs[i].tok.out; out != nil {
			out.lin = parts[i].cur.Lin
		}
	}
	// THE CONNECTION'S OWN ENTITLEMENT, captured once, never the serving authority's.
	ent, entOK := c.outboundEntitlement()
	if !entOK {
		for _, i := range in {
			sess.releaseUnacquiredPublishAction(c, &secs[i].tok)
		}
		return parts
	}
	// A BATCH THAT LOSES ONLY TO STALE EXPECTATIONS KEEPS ITS PLACE, under the delivery entrance's
	// contention extension, exactly as the admission batch does: giving up would leave every
	// participant to a re-offer queued behind later messages.
	ext := &mqttDeliveryContentionExtension{sess: sess, c: c}
	if c.mqtt != nil {
		ext.closing = c.mqtt.tracker.closingSignal()
	}
	budget := mqttForegroundAcquireAttempts
	for attempt := 0; ; attempt++ {
		txn := mqttNewInflightNonce()
		reqs := make([]*mqttAcquireActionRequest, len(in))
		pis := make([]uint16, len(in))
		for k, i := range in {
			p := parts[i]
			reqs[k] = &mqttAcquireActionRequest{
				ID: sess.id, Epoch: as.sessStreamEpoch, Txn: txn, Op: "op-acquire-publishes",
				Gen: ent.Gen, Tenure: ent.Tenure,
				Nonce: p.cur.Lin, Acq: p.cur.Acq + 1, Kind: mqttActionPublish,
				Binding: c.outBind, Outstanding: false,
				Register: p.cur, RegisterSeq: p.regSeq, Token: secs[i].tok.token,
			}
			pis[k] = secs[i].pi
		}
		var (
			out  mqttOpOutcome
			acts []*mqttOutboundAction
			err  error
		)
		if h := mqttTestAcquisitionBatchCommitHook.Get(); h != nil {
			out, acts, err = h(sess, reqs)
		} else {
			out, acts, err = sess.jsa.opAcquireActions(reqs)
		}
		v := mqttTransitionLanded
		if err != nil {
			v = mqttTransitionNeverLanded
			if out.Handle != nil {
				v = sess.settleCustodyHandle(out.Handle)
			}
		}
		if h := mqttTestAcquisitionBatchHook.Get(); h != nil {
			h(sess, pis, v)
		}
		switch v {
		case mqttTransitionLanded, mqttTransitionSuperseded:
			for k, i := range in {
				var act *mqttOutboundAction
				if k < len(acts) {
					act = acts[k]
				}
				parts[i].acquired = sess.adoptBatchAcquisition(c, secs[i], parts[i].cur, act)
			}
			return parts
		case mqttTransitionNeverLanded:
			if out.Handle == nil && !errors.Is(err, errMQTTEntitlementEnded) {
				// REFUSED BEFORE SUBMISSION, and nothing says by which participant: every one takes
				// the single acquisition at its turn, which judges it alone, so one participant's
				// refusal cannot hold back the others.
				for _, i := range in {
					sess.releaseUnacquiredPublishAction(c, &secs[i].tok)
					parts[i].fellOut = true
				}
				return parts
			}
			ext.noteWrite(err)
			giveUp := errors.Is(err, errMQTTEntitlementEnded)
			if !giveUp {
				if attempt+1 >= budget {
					// The extension spaces the next attempt itself.
					giveUp = !ext.extend(attempt+1, err)
					budget = attempt + 2
				} else {
					sess.authorityContentionWait()
				}
			}
			if giveUp {
				for _, i := range in {
					sess.releaseUnacquiredPublishAction(c, &secs[i].tok)
				}
				return parts
			}
			// CONTENTION: nothing of this attempt can land any more. Each participant's premise is
			// read again; one that no longer holds leaves the batch unsent (AckWait re-offers it).
			var keep []int
			for _, i := range in {
				cur, seq, ok := sess.acquirePremise(as, secs[i])
				if !ok {
					sess.releaseUnacquiredPublishAction(c, &secs[i].tok)
					continue
				}
				parts[i].cur, parts[i].regSeq = cur, seq
				keep = append(keep, i)
			}
			in = keep
			if len(in) == 0 {
				return parts
			}
		default:
			// UNRESOLVED: the store answered neither the resolution nor the abort. The attempt may
			// still land, so nothing transmits and the owed settlement keeps every token until the
			// receipt decides, then drains them (landed) or releases them (never landed). A section's
			// own end may drain before that and find nothing; that concludes nothing the record
			// relies on, because the record drains each exact token again once the attempt landed.
			toks := make([]mqttSendToken, 0, len(in))
			for _, i := range in {
				toks = append(toks, secs[i].tok)
			}
			sess.recordAcquireSettleOwed(c, out.Handle, toks)
			return parts
		}
	}
}

// acquirePremise reads a section's register and reports it when the single acquisition's premise
// holds: the slot stands, names the section's allocation, is at publish, carries no token and is
// of an ordinary class.
//
// Session lock NOT held on entry.
func (sess *mqttSession) acquirePremise(as *mqttAccountSessionManager, sec *mqttPublishSection) (*mqttInflightRecord, uint64, bool) {
	tok := &sec.tok
	if tok.token == _EMPTY_ || tok.slot.name.subject == _EMPTY_ || !tok.slot.slotOK() {
		return nil, 0, false
	}
	cur, smsg, lerr := as.loadInflightRecord(tok.slot.name.subject)
	if lerr != nil || smsg == nil || cur == nil || cur.Nonce != tok.slot.nonce ||
		cur.State != mqttInflightPublish || cur.ActionToken != _EMPTY_ || mqttDurIsSlotOnlyClass(cur.Dur) ||
		mqttSlotRefFromRecord(tok.slot.name.subject, smsg.Sequence, cur) == nil {
		return nil, 0, false
	}
	return cur, smsg.Sequence, true
}

// adoptBatchAcquisition adopts one participant of a landed batch: its register read back for its
// own sequence (the batch acknowledges on its authority member), and our exact token required on
// it. A participant whose read-back fails keeps its tentative ownership and is not transmitted:
// the receipt says its token is on the record, so its section's end drains it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) adoptBatchAcquisition(c *client, sec *mqttPublishSection, cur *mqttInflightRecord,
	act *mqttOutboundAction) bool {
	tok := &sec.tok
	if h := mqttTestAcquisitionBatchReadBackHook.Get(); h != nil && !h(sess, sec.pi) {
		return false
	}
	now, seqMsg, err := sess.asm.loadInflightRecord(tok.slot.name.subject)
	if err != nil || seqMsg == nil || now == nil || now.ActionToken != tok.token || act == nil {
		return false
	}
	if tok.out != nil {
		tok.out.action = act
		tok.out.acq = cur.Acq + 1
	}
	sess.adoptAcquiredPublishAction(c, sec.pi, tok, cur, seqMsg.Sequence)
	if h := mqttTestAcquisitionBatchAdoptedHook.Get(); h != nil {
		h(sess, sec.pi, tok)
	}
	return true
}

// releaseUnacquiredPublishAction withdraws a section's tentative ownership of an acquisition that
// is KNOWN not to have landed (refused before submission, or settled never landed): the exchange's
// standing binding stands, and any acknowledgement cleanup waiting on the attempt may run.
//
// Session lock NOT held on entry.
func (sess *mqttSession) releaseUnacquiredPublishAction(c *client, tok *mqttSendToken) {
	tok.actionHeld = false
	sess.mu.Lock()
	run, ackPI, bound := sess.publishAckResolutionLocked(tok.res, mqttInflightSlotRef{}, false, tok.token)
	sess.mu.Unlock()
	if run {
		sess.handOffAcknowledgedPublish(c, ackPI, &mqttPending{slot: bound})
	}
}

// mqttAcquireSettleOwed is a batched acquisition attempt whose landing is UNKNOWN: its receipt
// could be neither resolved nor aborted. Until the receipt decides, the attempt may still land, so
// no register read can say its tokens are over; the record keeps them, with the handle that will
// decide. Its owner is the custody dispatcher, which runs one settlement per unit; the server's
// shutdown makes one final attempt.
//
// What it is NOT: evidence. A landed verdict drains each EXACT token, which re-reads and clears
// only what is ours; a never-landed verdict releases the sections' tentative ownership.
type mqttAcquireSettleOwed struct {
	gen    string
	c      *client
	handle *mqttTransitionHandle
	toks   []mqttSendToken
	// Dispatcher scheduling: the next time a unit may run, and the current backoff.
	nextEligibleAt time.Time
	delay          time.Duration
}

// recordAcquireSettleOwed records an unsettled batch attempt for the custody dispatcher and the
// shutdown's final pass.
//
// Session lock NOT held on entry.
func (sess *mqttSession) recordAcquireSettleOwed(c *client, h *mqttTransitionHandle, toks []mqttSendToken) {
	if h == nil || len(toks) == 0 {
		return
	}
	o := &mqttAcquireSettleOwed{gen: toks[0].slot.name.gen, c: c, handle: h, toks: toks}
	sess.mu.Lock()
	if sess.gen == o.gen {
		if sess.acquireSettleOwed == nil {
			sess.acquireSettleOwed = make(map[*mqttAcquireSettleOwed]struct{})
		}
		sess.acquireSettleOwed[o] = struct{}{}
		sess.kickCustodyItemsLocked()
	}
	sess.mu.Unlock()
	if c != nil && c.srv != nil {
		c.srv.noteOwedAcquireSettle(sess, o)
	}
}

// runAcquireSettleOwedUnit is ONE settlement of an owed acquisition attempt, and the record's
// disposition from it: decided, its tokens are drained (landed) or released (never landed) and the
// record retires; still undecided, it backs off.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runAcquireSettleOwedUnit(gen string, o *mqttAcquireSettleOwed) {
	if !sess.settleOwedAcquisition(o) {
		sess.mu.Lock()
		if _, ok := sess.acquireSettleOwed[o]; ok && sess.gen == gen {
			base, capd := sess.custodyDelaysLocked()
			o.delay = mqttCustodyNextDelay(o.delay, base, capd)
			o.nextEligibleAt = time.Now().Add(mqttCustodyJitter(o.delay))
		}
		sess.mu.Unlock()
		return
	}
	sess.retireAcquireSettleOwed(o)
}

// settleOwedAcquisition settles o's receipt once and, decided, discharges its tokens. It reports
// whether the receipt was decided.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleOwedAcquisition(o *mqttAcquireSettleOwed) bool {
	switch sess.settleCustodyHandle(o.handle) {
	case mqttTransitionLanded, mqttTransitionSuperseded:
		for _, tok := range o.toks {
			sess.drainPublishAction(o.c, tok)
		}
		return true
	case mqttTransitionNeverLanded:
		for i := range o.toks {
			sess.releaseUnacquiredPublishAction(o.c, &o.toks[i])
		}
		return true
	}
	return false
}

// retireAcquireSettleOwed removes o from the session's dispatcher queue and the server's shutdown
// registry.
//
// Session lock NOT held on entry.
func (sess *mqttSession) retireAcquireSettleOwed(o *mqttAcquireSettleOwed) {
	sess.mu.Lock()
	delete(sess.acquireSettleOwed, o)
	sess.mu.Unlock()
	if o.c != nil && o.c.srv != nil {
		o.c.srv.clearOwedAcquireSettle(o)
	}
}

// dropAcquireSettleOwedLocked forgets the owed settlements of a generation the session no longer
// serves: its discard owns that generation's tokens.
//
// Session lock held on entry.
func (sess *mqttSession) dropAcquireSettleOwedLocked() {
	for o := range sess.acquireSettleOwed {
		if o.c != nil && o.c.srv != nil {
			o.c.srv.clearOwedAcquireSettle(o)
		}
	}
	sess.acquireSettleOwed = nil
}

// mqttOwedAcquireSettle is one owed acquisition settlement held with the session that owns it, for
// the shutdown's final pass.
type mqttOwedAcquireSettle struct {
	sess *mqttSession
	o    *mqttAcquireSettleOwed
}

// noteOwedAcquireSettle records an owed acquisition settlement for the shutdown's final pass.
func (s *Server) noteOwedAcquireSettle(sess *mqttSession, o *mqttAcquireSettleOwed) {
	if s == nil || sess == nil || o == nil {
		return
	}
	s.owedAcquireSettlesMu.Lock()
	if s.owedAcquireSettles == nil {
		s.owedAcquireSettles = make(map[*mqttAcquireSettleOwed]mqttOwedAcquireSettle, 1)
	}
	s.owedAcquireSettles[o] = mqttOwedAcquireSettle{sess: sess, o: o}
	s.owedAcquireSettlesMu.Unlock()
}

// clearOwedAcquireSettle removes an owed settlement a unit decided or a discard forgot.
func (s *Server) clearOwedAcquireSettle(o *mqttAcquireSettleOwed) {
	if s == nil || o == nil {
		return
	}
	s.owedAcquireSettlesMu.Lock()
	delete(s.owedAcquireSettles, o)
	s.owedAcquireSettlesMu.Unlock()
}

// finishOwedAcquireSettles makes ONE final settlement attempt for every owed acquisition, after
// the publish-send barrier closed (every section ended) and before the owed drains, while
// JetStream still answers. A landed attempt's drains that cannot resolve enrol the owed drains,
// which run next.
func (s *Server) finishOwedAcquireSettles() {
	s.owedAcquireSettlesMu.Lock()
	owed := make([]mqttOwedAcquireSettle, 0, len(s.owedAcquireSettles))
	for _, o := range s.owedAcquireSettles {
		owed = append(owed, o)
	}
	s.owedAcquireSettles = nil
	s.owedAcquireSettlesMu.Unlock()
	for _, o := range owed {
		if o.sess == nil {
			continue
		}
		if o.sess.settleOwedAcquisition(o.o) {
			o.sess.mu.Lock()
			delete(o.sess.acquireSettleOwed, o.o)
			o.sess.mu.Unlock()
		}
	}
}
