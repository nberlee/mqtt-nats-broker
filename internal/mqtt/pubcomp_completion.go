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

// The PUBCOMP completion worker (F-105, the PUBCOMP arm).
//
// # Why PUBCOMP needs an owner of its own
//
// The client's PUBCOMP is recorded on the exchange and the slot in one batch. When that batch
// cannot be CONFIRMED, the canonical PUBREL's JetStream ack is WITHHELD: acknowledging it would
// remove, from interest-retained $MQTT_out, the one record that lets a retry, a redelivery or a
// restart finish the exchange — and a recovery that then found "ready slot, no record" would have
// to call the exchange finished on nothing but an absence.
//
// So something must keep trying, and it cannot live on the pending entry. AckWait redelivers the
// PUBREL while the completion is unconfirmed, admission re-acquires from pubrel_ready, and
// trackAsPubRel REPLACES the entry — anything carried on it is gone, and retainAndReconcileTerminal
// refuses to enrol over a delivered entry at all. The worker's record is its own.
//
// # One owner for every unsettled cleanup of an ordinary exchange
//
// The same worker, with an empty ack set, owns every terminal cleanup of a RECORDED ordinary
// completion that did not settle inline — the drain's, the handler's, recovery's — so none of
// them depends on a structure a delivered entry can displace (the cleanup-ownership property;
// consults 23-24). Shared and affine slots keep retainAndReconcileTerminal: a different class,
// defined once by mqttDurIsSlotOnlyClass.
//
// # What it does not do
//
// It never consumes a delivered entry's acknowledgement debt and never frees its capacity: a
// redelivered PUBREL is a real delivery the client owes a PUBCOMP for, and that PUBCOMP (or the
// connection's teardown) settles it. It writes no durable state: process loss takes it with the
// in-memory reservation it guards, and the durable truth — the slot, the exchange, the unacked
// record — carries on without it. It does not settle a holder that has died: retrying cannot
// drain a token nobody will clear.

// mqttPubCompKey names one exchange's completion: its identifier and its nonce.
type mqttPubCompKey struct {
	pi    uint16
	nonce string
}

// mqttPubCompOwed is one unsettled completion.
//
// MAP MEMBERSHIP AND CLEANUP OWNERSHIP ARE DIFFERENT FACTS. The record is in pubCompOwed while it
// may still gather withheld acks; once its worker has recorded the completion it DETACHES — takes
// the acks, leaves the map — and goes on owning the cleanup privately until it settles. A later
// withheld PUBCOMP then starts a new record; pointer identity keeps the two apart, and neither
// revokes the other's obligation.
type mqttPubCompOwed struct {
	ref  mqttInflightSlotRef
	acks []string
}

// enrolPubCompOwed hands an unsettled completion to its worker: ack is the withheld JetStream
// acknowledgement to send once the completion is recorded, or empty for a cleanup whose
// completion is already recorded.
//
// JOINS a record already gathering acks for this exchange rather than starting another worker.
// Reserves the identifier as retainAndReconcileTerminal did — the terminal placeholder, installed
// only when nothing reserves the identifier (never over a delivered entry, which reserves it
// itself, or over a successor).
//
// Session lock NOT held on entry.
func (sess *mqttSession) enrolPubCompOwed(c *client, ref mqttInflightSlotRef, ack string) {
	if !ref.slotOK() || ref.name.subject == _EMPTY_ || ref.name.pi == 0 {
		return
	}
	key := mqttPubCompKey{pi: ref.name.pi, nonce: ref.nonce}
	sess.mu.Lock()
	if ref.name.gen != sess.gen {
		sess.mu.Unlock()
		return
	}
	if sess.pendingPubRel[ref.name.pi] == nil {
		sess.installTerminalReservationLocked(ref)
	}
	if rec := sess.pubCompOwed[key]; rec != nil {
		if ack != _EMPTY_ {
			rec.acks = append(rec.acks, ack)
		}
		sess.mu.Unlock()
		sess.notePubCompEnrolled(ref.name.pi, false)
		return
	}
	rec := &mqttPubCompOwed{ref: ref}
	if ack != _EMPTY_ {
		rec.acks = []string{ack}
	}
	if sess.pubCompOwed == nil {
		sess.pubCompOwed = make(map[mqttPubCompKey]*mqttPubCompOwed, 1)
	}
	sess.pubCompOwed[key] = rec
	sess.mu.Unlock()
	sess.notePubCompEnrolled(ref.name.pi, true)
	sess.schedulePubCompOwed(c, rec)
}

// ownTerminalCleanup is the one route an unsettled terminal cleanup of a RECORDED ordinary
// completion takes to an owner: the completion worker, with no ack to carry.
//
// Session lock NOT held on entry.
func (sess *mqttSession) ownTerminalCleanup(c *client, ref mqttInflightSlotRef) {
	sess.enrolPubCompOwed(c, ref, _EMPTY_)
}

// schedulePubCompOwed arms the worker's next attempt: unlimited attempts at a capped delay,
// shutdown-gated exactly as the other session-owned continuations are.
//
// GATE REFUSED: the worker retires its own record and leaves its acks UNSENT. What continues it
// depends on what it owned. An unconfirmed completion's canonical PUBREL is still unacked — it
// redelivers after the resume if the batch did not land, and if it did land the terminal slot is
// there for recovery to finalize. A recorded cleanup's continuation is the terminal slot, which
// recovery finalizes; its record may already be acknowledged or deleted.
//
// Session lock NOT held on entry.
func (sess *mqttSession) schedulePubCompOwed(c *client, rec *mqttPubCompOwed) {
	srv := sess.serverFor(c)
	if srv == nil || !srv.admitWillWorker() {
		sess.retirePubCompOwed(rec)
		return
	}
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer srv.willWorkerWG.Done()
		sess.attemptPubCompOwed(c, rec)
	})
}

// attemptPubCompOwed is one attempt of the worker.
//
// Session lock NOT held on entry.
func (sess *mqttSession) attemptPubCompOwed(c *client, rec *mqttPubCompOwed) {
	ref := rec.ref
	sess.mu.Lock()
	retiredGeneration := ref.name.gen != sess.gen
	member := sess.pubCompOwed[mqttPubCompKey{pi: ref.name.pi, nonce: ref.nonce}] == rec
	sess.mu.Unlock()
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testPubCompAttempt
		as.mu.RUnlock()
		if hook != nil {
			hook(rec, member)
		}
	}
	if retiredGeneration {
		// A proven generation discard: the old generation's records are teardown material. Send
		// nothing, free only this worker's placeholder, and retire — teardown never edits the
		// record, so the worker removes itself.
		sess.retirePubCompOwed(rec)
		sess.releaseTerminalReservation(ref)
		return
	}
	res := sess.mqttTerminalizeOnPubComp(c, ref)
	if !res.recorded {
		sess.schedulePubCompOwed(c, rec)
		return
	}
	// RECORDED: detach, then send what was carried. The acks leave the record in the SAME
	// critical section that removes it from the map, so an install either joins before (and is
	// sent here) or finds nothing and starts a new record (sent by that record's worker). No ack
	// is ever carried by a record nobody runs.
	acks := sess.detachPubCompOwed(rec)
	sess.notePubCompDetached(ref.name.pi)
	for _, subj := range acks {
		sess.sendPubCompAck(subj, true)
	}
	switch res.outcome {
	case mqttTerminalDone:
		sess.releaseTerminalReservation(ref)
	case mqttTerminalRetainedReoffer:
		// The retained slot outlives this cleanup: the capacity goes, the identifier's EXCLUSION
		// is handed on together with the slot's cleanup owner, which holds the obligation until
		// the slot is gone (retained_terminal_cleanup.go). A blanket release here would leave a
		// retained slot unexcluded.
		sess.releaseRetainedTerminalReservation(c, ref)
	case mqttTerminalHeldByAction:
		if sess.localActionHolds(ref.name.pi, res.heldToken) {
			// THIS process's live action holds exactly the observed token, so its drain carries
			// the cleanup (startActionDrain -> the paired drain completion) and its hold reserves
			// the identifier.
			sess.releaseTerminalReservation(ref)
			return
		}
		// A holder this broker cannot see finish: the placeholder stays and the worker keeps
		// looking until the token clears.
		sess.schedulePubCompOwed(c, rec)
	default:
		sess.schedulePubCompOwed(c, rec)
	}
}

// detachPubCompOwed removes rec from the map — only rec, by pointer — and returns the acks it
// carried, in ONE critical section.
//
// Takes the session lock itself.
func (sess *mqttSession) detachPubCompOwed(rec *mqttPubCompOwed) []string {
	key := mqttPubCompKey{pi: rec.ref.name.pi, nonce: rec.ref.nonce}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.pubCompOwed[key] != rec {
		return nil
	}
	acks := rec.acks
	rec.acks = nil
	delete(sess.pubCompOwed, key)
	return acks
}

// retirePubCompOwed removes rec from the map, by pointer, sending nothing.
//
// Takes the session lock itself.
func (sess *mqttSession) retirePubCompOwed(rec *mqttPubCompOwed) {
	key := mqttPubCompKey{pi: rec.ref.name.pi, nonce: rec.ref.nonce}
	sess.mu.Lock()
	if sess.pubCompOwed[key] == rec {
		delete(sess.pubCompOwed, key)
	}
	sess.mu.Unlock()
}

// localActionHolds reports whether THIS process's live action for pi holds exactly token.
//
// Takes the session lock itself.
func (sess *mqttSession) localActionHolds(pi uint16, token string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.localActionHoldsLocked(pi, token)
}

// localActionHoldsLocked is the one reader of "does this process's live action hold exactly
// this token". It is NOT "is this identifier reserved" — that stays sendReservedLocked's, and it
// is too weak here: an exclusion alone, or an older action whose successor holds the durable
// token, reserves the identifier while proving nothing about which drain will clear the token.
// A delegation of terminal cleanup to a local drain is sound only when that drain's action holds
// the very token finalize observed.
//
// sess.mu must be held.
// registeredActionLocked reports whether act — this exact holder object, not merely one carrying
// its token — is the identifier's registered, undrained PUBREL action. A value copy of a holder
// shares its token and none of its lifecycle, so only pointer identity says the lifecycle being
// judged is the one the registry and every drain act on.
//
// Session lock held on entry.
func (sess *mqttSession) registeredActionLocked(pi uint16, act *mqttSendAction) bool {
	if act == nil {
		return false
	}
	h := sess.sendsInFlight[pi]
	return h != nil && h.action == act && !act.drained
}

func (sess *mqttSession) localActionHoldsLocked(pi uint16, token string) bool {
	if token == _EMPTY_ {
		return false
	}
	h := sess.sendsInFlight[pi]
	return h != nil && h.action != nil && !h.action.drained && h.action.token == token
}

// sendPubCompAck sends one JetStream acknowledgement of a canonical PUBREL on the PUBCOMP path,
// through the observer seam.
func (sess *mqttSession) sendPubCompAck(subject string, byWorker bool) {
	if subject == _EMPTY_ {
		return
	}
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testPubCompAck
		as.mu.RUnlock()
		if hook != nil {
			hook(subject, byWorker)
		}
	}
	sess.jsa.sendAck(subject)
}

func (sess *mqttSession) notePubCompEnrolled(pi uint16, created bool) {
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testPubCompEnrolled
		as.mu.RUnlock()
		if hook != nil {
			hook(pi, created)
		}
	}
}

func (sess *mqttSession) notePubCompDetached(pi uint16) {
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testPubCompDetached
		as.mu.RUnlock()
		if hook != nil {
			hook(pi)
		}
	}
}
