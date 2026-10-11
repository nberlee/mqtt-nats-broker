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

import "time"

// ADMISSION CONTINUITY (F-097 WP5, witnessed by W5b).
//
// An admission can LAND while this broker cannot tell that it did: its response is lost, every
// readback of its register fails, its custody resolution stays unresolved, and the ladder gives
// up. The abandonment then drops the entry AND the source mapping, so the source's next offer is
// a stranger to this session and is admitted again, under another identifier and another
// lineage, while the first still stands at Publish: two un-ended exchanges for ONE source
// delivery (interleaving 3). One source delivery must have at most one un-ended exchange.
//
// So an abandonment whose admission the custody ledger reports landed or unsettled leaves a
// CONTINUITY record keyed by the source delivery, and trackPublish refuses that delivery's
// offers while it stands. The record is owned by the custody dispatcher, which decides it one
// bounded step per unit from what the store says:
//
//   - the admission attempt still unresolved: settle that attempt (resolve, abort, resolve);
//   - the exchange owes no further DELIVERY (ended, its delivery abandoned, or its generation
//     discarded): the refusal goes (the ending, the abandonment or the discard settles the source);
//   - nothing of the allocation landed (register and exchange absent, the attempt settled): the
//     refusal goes and the withheld offer is re-offered (NAK);
//   - landed, its unsent marker standing, no token, the exchange at Publish (W5b's population):
//     the obligation is WITHDRAWN under the deciding entitlement (WithdrawnUnsent), then the
//     refusal goes and the source is re-offered as a fresh delivery; a stale entitlement is
//     refused at the boundary (mqttWithdrawUnlicensed) and the record is dropped, its
//     successor's recovery reconstructing the obligation;
//   - anything else (possibly attempted, past PUBREC, an action token, an unreadable
//     read): NOT decided here. No witness reached those dispositions for this population, so
//     the refusal STANDS (conservative: the source is withheld, never doubled) and the record
//     is retried at its backoff; a generation discard or the next resume's recovery (which
//     restores the landed obligation itself) supersedes it: a restored entry of the same
//     allocation retires the record (here, and in trackPublish, which then serves the entry).
//
// Each unit takes ONE of these steps - settle, classify, or withdraw - so each stays within its
// own dispatcher row (the 152 per-pass bound holds).
//
// What it is NOT: evidence. It refuses offers; it authorizes no output and ends nothing on its
// own say-so.
type mqttAdmissionContinuity struct {
	gen, epoch string
	dur        string
	seq        uint64
	nonce, lin string
	pi         uint16
	// lic is the ADMITTING connection's licence, captured when its ladder abandoned the
	// admission: the withdrawal is that connection's decision, never the session's current
	// binding's (a successor's is refused at the boundary and reconstructs it itself).
	lic mqttDestroyLicence
	// reply is the withheld offer's acknowledgement subject, NAKed when the refusal goes.
	reply string
	// withdrawDue: the last classification found W5b's population; the next unit withdraws.
	withdrawDue bool

	nextEligibleAt time.Time
	delay          time.Duration
}

// admissionContinuityLocked is the continuity record refusing (dur, seq)'s offers, or nil.
//
// Session lock held on entry.
func (sess *mqttSession) admissionContinuityLocked(dur string, seq uint64) *mqttAdmissionContinuity {
	return sess.admissionContinuity[mqttDeferredKey{dur, seq}]
}

// installAdmissionContinuityLocked records the continuity of an abandoned admission whose attempt
// the ledger reports landed or unsettled. The ledger-admitted classes (ordinary and shared): the
// experimental slot-only class has no ledger admission and is chapter P's, and a retained copy
// has no source delivery to refuse.
//
// It takes the abandoned allocation's identity by value (its source delivery (dur, seq), nonce,
// lineage and whether it is a retained copy), never the pending entry: the entry is untracked by
// the abandonment, and the record is the source delivery's, not the entry's. lic is the
// abandoning ladder's own connection's licence.
//
// Session lock held on entry.
func (sess *mqttSession) installAdmissionContinuityLocked(pi uint16, dur string, seq uint64, nonce, lin string,
	retained bool, reply string, lic mqttDestroyLicence) bool {
	if nonce == _EMPTY_ || sess.custody == nil || sess.asm == nil || !lic.entOK ||
		mqttDurIsSlotOnlyClass(dur) || retained {
		return false
	}
	if lin == _EMPTY_ {
		lin = nonce
	}
	// The abandonment runs INSIDE the ladder, while the admission is still held: what matters is
	// whether an attempt LANDED or is still UNSETTLED. Every attempt settled never-landed is a
	// positive never-landed: nothing to continue (W5a), and the source is re-offered as before.
	if !sess.custody.admissionMayHaveLanded(sess.gen, nonce) && !sess.custody.hasExchange(sess.gen, lin) {
		return false
	}
	if sess.admissionContinuity == nil {
		sess.admissionContinuity = make(map[mqttDeferredKey]*mqttAdmissionContinuity)
	}
	sess.admissionContinuity[mqttDeferredKey{dur, seq}] = &mqttAdmissionContinuity{
		gen: sess.gen, epoch: sess.asm.sessStreamEpoch, dur: dur, seq: seq,
		nonce: nonce, lin: lin, pi: pi, lic: lic.withAdmission(nil), reply: reply,
	}
	sess.kickCustodyItemsLocked()
	return true
}

// continuityRestoredLocked reports whether a resume restored THIS allocation's own entry (same
// identifier and nonce) for the record's delivery: the restored entry then owns the delivery, its
// retransmission and its acknowledgement, and the record is superseded.
//
// Session lock held on entry.
func (sess *mqttSession) continuityRestoredLocked(c *mqttAdmissionContinuity) bool {
	p := sess.pendingPublish[c.pi]
	return p != nil && p.regNonce == c.nonce && p.jsDur == c.dur && p.sseq == c.seq
}

// retireAdmissionContinuityLocked removes c only while it is still the record for its delivery
// (pointer identity), and returns the withheld offer's subject to NAK.
//
// Session lock held on entry.
func (sess *mqttSession) retireAdmissionContinuityLocked(c *mqttAdmissionContinuity) (string, bool) {
	k := mqttDeferredKey{c.dur, c.seq}
	if sess.admissionContinuity[k] != c {
		return _EMPTY_, false
	}
	delete(sess.admissionContinuity, k)
	return c.reply, true
}

// runAdmissionContinuityUnit is ONE bounded step for a continuity record (see the type).
//
// Session lock NOT held on entry.
func (sess *mqttSession) runAdmissionContinuityUnit(gen string, c *mqttAdmissionContinuity) {
	if c == nil || c.gen != gen || sess.asm == nil {
		return
	}
	retire, nak, again := false, false, false
	subject := mqttInflightSubject(sess.idHash, gen, c.pi)
	sess.mu.Lock()
	unsettled := sess.custody.admissionHeld(gen, c.nonce)
	superseded := sess.continuityRestoredLocked(c)
	withdrawDue := c.withdrawDue
	sess.mu.Unlock()
	// ONE APPLICABLE STEP PER UNIT, each within its own dispatcher row (WP4 cost): settle the
	// attempt (<= 5 per attempt), OR classify (2 reads), OR withdraw (one withdrawUnsent, <= 19).
	switch {
	case superseded:
		// A resume restored this allocation's own entry: the entry owns the delivery now.
		retire = true
	case unsettled:
		sess.observeCustodyTarget(gen, mqttCustodyTarget{kind: mqttCustodyTargetAdmission, key: c.nonce})
	case withdrawDue:
		switch sess.withdrawUnsent(gen, subject, c.nonce, c.lin, c.lic, nil) {
		case mqttWithdrawLanded:
			retire, nak = true, true
		case mqttWithdrawUnlicensed:
			retire = true // the successor's recovery reconstructs it under its own licence
		default:
			c.withdrawDue = false // re-classify next unit: the register may have moved
		}
	default:
		rec, smsg, rerr := sess.asm.loadInflightRecord(subject)
		x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, c.epoch, gen, c.lin)
		switch {
		case rerr != nil || (xerr != nil && !mqttExchangeReadAbsent(xerr)):
			// Indeterminate: retain.
		case x != nil && mqttExchangeDeliveryDutyEnded(x.Phase):
			// AN EXCHANGE THAT OWES NO FURTHER DELIVERY HAS NO ADMISSION LEFT TO CONTINUE: its
			// source is disposed of by its own ending, by the abandonment (its durable fact, under
			// the same source rule as every abandoned delivery: retired, or a shared group's handed
			// back under its held binding), or by its generation's discard. These phases are
			// absorbing, so a refusal kept past one would recur until the generation is discarded.
			retire = true
		case x == nil && (smsg == nil || rec == nil || rec.Nonce != c.nonce):
			// Nothing of this allocation landed, and its attempt is settled.
			retire, nak = true, true
		case x != nil && x.Phase == mqttExchangePublish && rec != nil && rec.Nonce == c.nonce &&
			rec.State == mqttInflightPublish && rec.ActionToken == _EMPTY_ && !rec.Retired:
			// The withdrawal is the NEXT unit's step (its own row's bound); the op re-reads and
			// the write boundary re-checks everything, including the standing marker.
			c.withdrawDue, again = true, true
		}
	}
	sess.mu.Lock()
	var reply string
	ok := false
	if retire {
		reply, ok = sess.retireAdmissionContinuityLocked(c)
	} else if sess.admissionContinuity[mqttDeferredKey{c.dur, c.seq}] == c {
		if again {
			c.nextEligibleAt = time.Time{}
			sess.kickCustodyItemsLocked()
		} else {
			base, capd := sess.custodyDelaysLocked()
			c.delay = mqttCustodyNextDelay(c.delay, base, capd)
			c.nextEligibleAt = time.Now().Add(mqttCustodyJitter(c.delay))
		}
	}
	sess.mu.Unlock()
	if ok && nak && reply != _EMPTY_ && sess.jsa != nil {
		sess.jsa.sendMsg(reply, AckNak)
	}
}
