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
	"time"
)

// THE FLOOR UNIT (F-112): the custody dispatcher's item that advances a live generation's floor,
// deletes what the floor retired, resolves the exchange duties a live generation never resolved
// before, and gives the ended-lineage veto its exit.
//
// It is ONE more dispatcher queue, not a worker of its own, so it inherits the dispatcher's
// lifetime, lease, close-cycle stop, captured-identity checks and tick. Its demand is a LATCH:
//
//   - armed by an accepted ENDED observation, by any resident ledger read that finds deletion owed
//     (Del < Lo) or a span worth advancing, and by its own periodic re-arm while the inventory
//     holds a span or retired positions it may later act on — which is what discovers a PEER's
//     completions and reclamations on a healthy, continuously connected session;
//   - cleared ONLY by a run that found nothing to do; never by a timer firing, a refused start or
//     a quit, so a demand survives a closing incarnation and is re-armed at reopen;
//   - replaced with the generation, with the rest of the custody state.

// mqttFloorTick is the periodic re-arm while a generation still has span, ended lineages or
// retired positions to look at.
const mqttFloorTick = 30 * time.Second

// mqttFloorPruneBatch bounds the register reads one unit spends on the ended-lineage veto's exit.
const mqttFloorPruneBatch = 16

// mqttFloorUnitPositions bounds the positions one floor unit scans, retires or deletes. A unit runs
// INLINE in a dispatcher iteration beside the cheap custody units, so it is kept to a few dozen
// store reads; the floor's throughput comes from the unit being due again at once while it makes
// progress, not from one long unit.
const mqttFloorUnitPositions = 32

// mqttFloorHorizon is one broker's progress toward a qualifying floor advance. It is scan state
// only: correctness never depends on it, and a restart re-derives it from Lo.
type mqttFloorHorizon struct {
	// base is the floor the horizon was measured from; a floor that moved invalidates it.
	base uint64
	// next is the first position not yet classified.
	next uint64
	// reclaim and relocate count the classified positions in [base, next).
	reclaim, relocate uint64
	// executing is set once [base, next) qualified: the unit then advances the floor in bounded
	// batches up to end.
	executing bool
	end       uint64
}

// armFloorLocked latches floor demand due at at (zero: now). An existing deadline is never
// postponed, and while the unit is backing off a new arm never pulls it earlier: repeated
// observations must not defeat its no-progress backoff. It reports whether the latch was newly
// set, so a caller outside the dispatcher can start it.
//
// Lock held on entry.
func (sess *mqttSession) armFloorLocked(gen string, at time.Time) bool {
	if gen != sess.gen || sess.custody == nil {
		return false
	}
	if !sess.floorWanted {
		sess.floorWanted, sess.floorDueAt = true, at
		return true
	}
	if sess.floorDelay == 0 && at.Before(sess.floorDueAt) {
		sess.floorDueAt = at
	}
	return false
}

// noteLedgerObservationLocked arms the floor from a ledger a resident traversal read: deletion
// owed below the floor, or a span worth advancing. It reports whether the latch was newly set.
//
// Lock held on entry.
func (sess *mqttSession) noteLedgerObservationLocked(gen string, led *mqttGenLedger) bool {
	if led == nil {
		return false
	}
	if led.Del < led.Lo || (led.State == mqttGenLedgerOpen && mqttLedgerSpan(led) > mqttFloorSlack) {
		return sess.armFloorLocked(gen, time.Time{})
	}
	return false
}

// floorDueLocked reports whether the floor unit is due.
//
// Lock held on entry.
func (sess *mqttSession) floorDueLocked(now time.Time) bool {
	return sess.floorWanted && mqttCustodyDue(sess.floorDueAt, now)
}

// runFloorUnit is ONE bounded step of the floor: delete retired positions if any are owed,
// else extend or execute the horizon; then one bounded prune of the ended-lineage veto. It
// reschedules itself from what it found, and clears the latch only when it found nothing to do.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runFloorUnit(gen, epoch string) bool {
	if s := sess.custodyDriverSeams(); s.floorHeld != nil && s.floorHeld(gen) {
		sess.mu.Lock()
		if sess.gen == gen && sess.floorWanted {
			sess.floorDueAt = time.Now().Add(time.Second)
		}
		sess.mu.Unlock()
		return false
	}
	jsa := sess.jsa
	id := sess.id
	progress, more := false, false
	led, _, lerr := jsa.loadGenLedger(id, epoch, gen)
	switch {
	case lerr != nil:
		// UNREAD: nothing concluded; back off and look again.
		more = true
	case led == nil:
	case led.Del < led.Lo:
		// EVERY AUTHORITY ATTEMPT OF THE FLOOR IS BACKGROUND (outbound_foreground_priority.go): it
		// yields to announced foreground operations, so the floor trails the traffic it reclaims
		// rather than competing with it.
		if !sess.admitBackgroundAuthority() {
			return false
		}
		_, derr := jsa.deleteRetiredPositionsUpTo(id, epoch, gen, mqttFloorUnitPositions)
		progress, more = derr == nil, true
	case led.State == mqttGenLedgerOpen && mqttLedgerSpan(led) > mqttFloorSlack:
		p, m := sess.floorHorizonStep(gen, epoch, led)
		progress, more = p, m
	}
	pruned, pruneMore := sess.pruneEndedLineagesStep(gen, epoch)
	if pruned {
		progress = true
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.gen != gen || sess.custody == nil {
		return false
	}
	now := time.Now()
	switch {
	case progress && (more || pruneMore):
		// MORE TO DO NOW: due at once, backoff reset.
		sess.floorDelay, sess.floorDueAt = 0, time.Time{}
	case more || pruneMore:
		// BLOCKED: back off.
		base, capd := sess.custodyDelaysLocked()
		sess.floorDelay = mqttCustodyNextDelay(sess.floorDelay, base, capd)
		sess.floorDueAt = now.Add(mqttCustodyJitter(sess.floorDelay))
	default:
		// NOTHING TO DO NOW. Re-armed periodically while the inventory itself holds something the
		// floor may later find (a span a peer's completions may make reclaimable, retired
		// positions not yet deleted); otherwise the latch clears and only a new arm sets it. Ended
		// lineages whose register still stands do NOT keep it ticking: they are pruned by the next
		// run any ending or resident ledger read arms, so a session with no further traffic keeps
		// at most the entries it had and goes quiescent.
		sess.floorDelay = 0
		owed := led != nil && (led.Del < led.Lo || (led.State == mqttGenLedgerOpen && mqttLedgerSpan(led) > mqttFloorSlack))
		if owed {
			sess.floorDueAt = now.Add(mqttCustodyJitter(mqttFloorTick))
		} else {
			sess.floorWanted, sess.floorDueAt, sess.floorHorizon = false, time.Time{}, nil
		}
	}
	return false
}

// floorHorizonStep extends the horizon by one bounded scan, or executes one bounded advance once
// it qualifies. It reports progress, and whether more remains to do now.
//
// Session lock NOT held on entry.
func (sess *mqttSession) floorHorizonStep(gen, epoch string, led *mqttGenLedger) (bool, bool) {
	jsa := sess.jsa
	id := sess.id
	sess.mu.Lock()
	var h mqttFloorHorizon
	if sess.floorHorizon != nil && sess.floorHorizon.base == led.Lo {
		h = *sess.floorHorizon
	} else {
		// A FLOOR THAT MOVED (a peer advanced it) invalidates the horizon measured from the old one.
		h = mqttFloorHorizon{base: led.Lo, next: led.Lo}
	}
	sess.mu.Unlock()
	save := func(nh *mqttFloorHorizon) {
		sess.mu.Lock()
		if sess.gen == gen {
			sess.floorHorizon = nh
		}
		sess.mu.Unlock()
	}

	if !h.executing {
		end := min(led.Hi+1, h.next+mqttFloorUnitPositions)
		scanned, blocked := false, false
		var resolve []mqttSweptDuty
		for ord := h.next; ord < end; ord++ {
			d, _, derr := jsa.loadDutyEntryAt(id, epoch, gen, ord)
			if derr != nil || d == nil {
				blocked = true // undecided: the horizon cannot pass it now
				break
			}
			if d.Kind == mqttDutyExchange && d.State != mqttDutyTerminal && d.State != mqttDutyRelocated {
				// LIVE RESOLUTION, DISCOVERED FROM THE INVENTORY: a Pending exchange duty whose
				// obligation reached a delivery-duty ending is resolved, whether or not this
				// process ever held the exchange — a fresh process after a crash between the
				// completion and the resolution included. In BULK, one batch per scan, and the
				// horizon resumes at the first resolved position so it is classified resolved.
				if x, _, xerr := jsa.loadOutboundExchange(id, epoch, gen, d.Ref); xerr == nil && x != nil &&
					mqttExchangeDeliveryDutyEnded(x.Phase) {
					resolve = append(resolve, mqttSweptDuty{Ord: ord, Entry: d})
					if len(resolve) == mqttFloorBatchCarry {
						break
					}
					continue
				}
			}
			if len(resolve) > 0 {
				continue // the horizon stops counting at the first position it resolves
			}
			cls, _ := jsa.classifyFloorPosition(id, epoch, gen, d)
			if cls == mqttFloorUndecided {
				blocked = true
				break
			}
			if cls == mqttFloorReclaim {
				h.reclaim++
			} else {
				h.relocate++
			}
			h.next = ord + 1
			scanned = true
		}
		if len(resolve) > 0 {
			if !sess.admitBackgroundAuthority() {
				return scanned, false
			}
			root := &mqttRecoveryRoot{ID: id, Epoch: epoch, Gen: gen}
			if rerr := jsa.opResolveDuties(root, resolve, mqttNewInflightNonce()); rerr != nil {
				save(&h)
				return scanned, true
			}
			save(&h)
			return true, true
		}
		if h.reclaim >= max(uint64(mqttFloorMinReclaim), 2*h.relocate) {
			h.executing, h.end = true, h.next
			save(&h)
			return true, true
		}
		if blocked {
			// AN UNDECIDED POSITION: keep what was measured, and back off before looking again.
			save(&h)
			return scanned, true
		}
		if h.next > led.Hi {
			// EXHAUSTED WITHOUT QUALIFYING: nothing to advance now; the periodic re-arm looks again.
			save(nil)
			return scanned, false
		}
		save(&h)
		return scanned, true
	}

	// EXECUTING: one bounded batch from the floor.
	f, ferr := jsa.composeFloorBatch(id, epoch, gen, min(h.end, led.Lo+mqttFloorUnitPositions))
	if ferr != nil || f == nil || f.Ledger.Lo != led.Lo {
		save(nil)
		return false, ferr != nil
	}
	if len(f.Positions) == 0 {
		save(nil)
		return false, true
	}
	if !sess.admitBackgroundAuthority() {
		return false, false
	}
	if err := jsa.opAdvanceFloor(f, mqttNewInflightNonce()); err != nil {
		// LOST THE LEDGER OR AN ENTRY CAS (an admission, a peer's advance, a continuation of a
		// relocated duty), or refused: re-read and recompute on the next step.
		save(nil)
		return false, true
	}
	ledNow := f.Ledger
	newLo := ledNow.Lo + uint64(len(f.Positions))
	if newLo >= h.end {
		save(nil)
	} else {
		nh := h
		nh.base = newLo
		save(&nh)
	}
	return true, true
}

// pruneEndedLineagesStep is the ended-lineage veto's EXIT (F-112), at most mqttFloorPruneBatch
// register reads. An entry for lineage L may go only when nothing can ever again install an
// AckRoute for L on this session object, which needs all three, re-checked under the lock after
// the register read:
//
//  1. no pending projection on this object names L (an AckRoute is installed only from the
//     removal of a projection);
//  2. no resume is in flight (a resume that captured L's register before it went could still
//     create a projection);
//  3. the register subject (gen, PI) no longer names L: register non-resurrection makes that
//     absorbing, so no later resume can capture a register naming L.
//
// It reports whether anything was pruned, and whether more candidates remain than one batch
// read: entries kept because their register still names them, a projection does or a resume is in
// flight are NOT "more", so they never keep the floor ticking on their own.
//
// Session lock NOT held on entry.
func (sess *mqttSession) pruneEndedLineagesStep(gen, epoch string) (bool, bool) {
	type cand struct {
		lin string
		pi  uint16
	}
	sess.mu.Lock()
	if sess.gen != gen || sess.custody == nil || sess.custody.resumeApplies != 0 {
		sess.mu.Unlock()
		return false, false
	}
	ledger := sess.custody
	var cands []cand
	full := false
	for lin, pi := range ledger.endedLineages {
		if len(cands) == mqttFloorPruneBatch {
			full = true
			break
		}
		if sess.lineageProjectedLocked(gen, lin) {
			continue
		}
		cands = append(cands, cand{lin, pi})
	}
	sess.mu.Unlock()
	if len(cands) == 0 {
		return false, false
	}
	gone := make([]cand, 0, len(cands))
	for _, c := range cands {
		rec, smsg, rerr := sess.jsa.loadInflightRecord(mqttInflightSubject(getHash(sess.id), gen, c.pi))
		if rerr != nil || (rec == nil && smsg != nil) {
			continue // unread or unreadable: it may still name L
		}
		if rec != nil && rec.Lin == c.lin {
			continue
		}
		gone = append(gone, c)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	pruned := false
	if sess.gen == gen && sess.custody == ledger && ledger.resumeApplies == 0 {
		for _, c := range gone {
			if !sess.lineageProjectedLocked(gen, c.lin) {
				delete(ledger.endedLineages, c.lin)
				pruned = true
			}
		}
	}
	return pruned, full && pruned
}

// lineageProjectedLocked reports whether any pending projection on this session object names
// lineage lin of generation gen.
//
// Lock held on entry.
func (sess *mqttSession) lineageProjectedLocked(gen, lin string) bool {
	for _, p := range sess.pendingPublish {
		if p != nil && p.ev.gen == gen && p.ev.lin == lin {
			return true
		}
	}
	for _, p := range sess.pendingPubRel {
		if p != nil && p.ev.gen == gen && p.ev.lin == lin {
			return true
		}
	}
	return false
}
