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
	"math/rand"
	"strconv"
	"time"
)

// THE AUTONOMOUS CUSTODY RECONCILIATION DRIVER (F-097 / F-088, WP4).
//
// Before this, the custody observer ran only when something hinted it or a withholding gate asked
// for a pass, and "there is no timer": a failed read released nothing and nothing re-read it, an
// owed withdrawal waited for the next resume, and at Receive Maximum 1 a withheld delivery waited
// for AckWait's re-offer — while a peer's completion of the exchange holding the slot was never
// rediscovered without traffic. The driver is that observer made autonomous, and it is still the
// ONLY thing that releases an exchange's charge:
//
//   - LIFETIME. One start site, startCustodyDispatcher. It runs only as an ADMITTED worker
//     (srv.admitWillWorker) holding the incarnation lease; the single-flight flag is set only after
//     both succeeded, and both are held for the whole run. Refused admission or lease: nothing
//     starts, and the durable records carry the work. Shutdown joins timers, then workers; the
//     loop checks the dispatch gate after every unit.
//   - DISPATCH. Each iteration runs at most mqttCustodyDispatchUnits units, round-robin across the
//     non-empty queues — hints, the requested pass's targets, due read retries, due items (owed
//     withdrawals; admission continuity records, outbound_admit_continuity.go) — and each unit
//     checks its captured (generation, epoch, ledger). The census is
//     the one heavy operation: not a unit, admitted at most once per cap interval across every
//     trigger (lastHeavyAt).
//   - CADENCE. Nothing retries inline. A unit without progress doubles its item's delay from the
//     base to the cap, with +/-20% jitter; progress resets it. The tick — a tracked timer, the same
//     one delayed Wills use — is armed for the earliest eligible item, a latched demand, or a
//     throttled census, and its callback is the start site.
//   - IT NEVER TOUCHES piSpaceIncomplete. Census and reconciliation release exchange custody;
//     reopening allocation after an incomplete recovery is the next complete resume's (C2).

// mqttCustodyDispatchUnits is the most units one dispatcher iteration runs.
const mqttCustodyDispatchUnits = 8

const (
	mqttCustodyBaseDelay = 100 * time.Millisecond
	mqttCustodyMaxDelay  = 5 * time.Second
)

// mqttCustodyDriverSeams instruments one session's dispatcher (F-097 T2b, T2f, T2g, T2i). Nil in
// production; a fixture installs it under the session lock before the dispatcher starts.
type mqttCustodyDriverSeams struct {
	// startEntry runs at the start site's entry, BEFORE admission and the lease.
	startEntry func()
	// started observes a run that passed admission, the lease and the single-flight flag.
	started func(gen string)
	// iteration observes each iteration's units and whether the census runs in it.
	iteration func(gen string, units []mqttCustodyUnit, census bool)
	// unit observes each unit as it is dispatched, under its captured generation.
	unit func(gen string, u mqttCustodyUnit)
	// delays replaces THIS session's (base, cap). Called with the session lock held.
	delays func() (base, cap time.Duration)
	// tickStarted runs inside a firing tick's callback, after it reached the start site and while
	// its timer is still registered under its key.
	tickStarted func(gen, key string)
	// demandDue observes a latched demand's deadline becoming a requested pass, with that
	// deadline. Called with the session lock held.
	demandDue func(gen string, dueAt time.Time)
	// idle runs when an iteration found no work, before the run's exit takes the session lock.
	idle func(gen string)
	// exited observes an exit that ENDED its run, once it settled what stays armed. Called with
	// the session lock held.
	exited func(gen string)
	// floorHeld, when it reports true, holds this session's floor unit back: the unit does
	// nothing and is due again later, its latch kept (F-112 T14: a process that crashes between a
	// completion and its resolution).
	floorHeld func(gen string) bool
}

// custodyExitSettledLocked reports an exit that ended its run to the exited seam.
//
// Lock held on entry.
func (sess *mqttSession) custodyExitSettledLocked(gen string) {
	if s := sess.testCustodyDriver; s != nil && s.exited != nil {
		s.exited(gen)
	}
}

// custodyDriverSeams is the installed seam set, never nil.
//
// Session lock NOT held on entry.
func (sess *mqttSession) custodyDriverSeams() *mqttCustodyDriverSeams {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if s := sess.testCustodyDriver; s != nil {
		return s
	}
	return &mqttCustodyDriverSeams{}
}

// mqttCustodyRetry is one exchange whose last read was indeterminate.
type mqttCustodyRetry struct {
	nextEligibleAt time.Time
	delay          time.Duration
}

// custodyDelaysLocked is the dispatcher's (base, cap): base 100 ms, cap max(base, min(5 s,
// AckWait/4)).
//
// Lock held on entry.
func (sess *mqttSession) custodyDelaysLocked() (time.Duration, time.Duration) {
	if s := sess.testCustodyDriver; s != nil && s.delays != nil {
		return s.delays()
	}
	ackWait := mqttDefaultAckWait
	if srv := sess.serverFor(nil); srv != nil {
		if aw := srv.getOpts().MQTT.AckWait; aw > 0 {
			ackWait = aw
		}
	}
	capd := ackWait / 4
	if capd > mqttCustodyMaxDelay {
		capd = mqttCustodyMaxDelay
	}
	if capd < mqttCustodyBaseDelay {
		capd = mqttCustodyBaseDelay
	}
	return mqttCustodyBaseDelay, capd
}

// mqttCustodyNextDelay doubles prev from base up to capd; zero starts at base.
func mqttCustodyNextDelay(prev, base, capd time.Duration) time.Duration {
	if prev <= 0 {
		return base
	}
	if next := prev * 2; next < capd {
		return next
	}
	return capd
}

// mqttCustodyJitter applies +/-20% jitter.
func mqttCustodyJitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// noteCustodyReadLocked records an exchange read's outcome for the retry queue: an indeterminate
// read backs off and is retried by the tick; a readable one clears the retry.
//
// Lock held on entry.
func (sess *mqttSession) noteCustodyReadLocked(gen, lin string, indeterminate bool) {
	if gen != sess.gen || lin == _EMPTY_ {
		return
	}
	if !indeterminate {
		delete(sess.custodyRetry, lin)
		return
	}
	base, capd := sess.custodyDelaysLocked()
	r := sess.custodyRetry[lin]
	if r == nil {
		if sess.custodyRetry == nil {
			sess.custodyRetry = make(map[string]*mqttCustodyRetry)
		}
		r = &mqttCustodyRetry{}
		sess.custodyRetry[lin] = r
	}
	r.delay = mqttCustodyNextDelay(r.delay, base, capd)
	r.nextEligibleAt = time.Now().Add(mqttCustodyJitter(r.delay))
}

// noteAllocationSucceededLocked clears the latched demand once a withheld allocation has since
// succeeded with nothing deferred (the demand it latched has been met). The other end of the
// latch is its own deadline: see noAllocationCanBeWaitingLocked.
//
// Lock held on entry.
func (sess *mqttSession) noteAllocationSucceededLocked() {
	if sess.custodyDemand && len(sess.deferred) == 0 {
		sess.custodyDemand, sess.custodyDemandDelay, sess.custodyDemandDueAt = false, 0, time.Time{}
	}
}

// noAllocationCanBeWaitingLocked reports that a latched demand names no waiter any more: no
// delivery is deferred, no identifier is unknown, and the ledger holds no admission and no
// exchange. A demand latches for a withheld allocation, and its waiter may leave without ever
// being allocated (an UNSUBSCRIBE drain, a consumer teardown or a resume drops a deferred entry;
// the retained downgrade and allocator exhaustion defer nothing at all). With nobody deferred,
// nothing unknown and no custody held, no re-observation can change what any allocation would
// find, so the demand is retired instead of re-observing for the session's life; a withholding
// after this re-latches under the same lock.
//
// Lock held on entry.
func (sess *mqttSession) noAllocationCanBeWaitingLocked(ledger *mqttCustodyLedger) bool {
	return len(sess.deferred) == 0 && !ledger.withholdsAllocation() && !ledger.holdsCustody()
}

// mqttCustodyUnitKind is what one dispatcher unit does.
type mqttCustodyUnitKind uint8

const (
	// mqttCustodyUnitTarget: settle one custody target (an exchange observation, an admission
	// attempt settlement, an uncertain subject) — a hint, a pass target or a read retry.
	mqttCustodyUnitTarget mqttCustodyUnitKind = iota + 1
	// mqttCustodyUnitWithdraw: ONE withdrawUnsent for an owed withdrawal.
	mqttCustodyUnitWithdraw
	// mqttCustodyUnitContinuity: ONE bounded step for an admission continuity record (F-097 WP5).
	mqttCustodyUnitContinuity
	// mqttCustodyUnitFloor: ONE bounded step of the inventory floor (F-112, outbound_floor_unit.go).
	mqttCustodyUnitFloor
)

// mqttCustodyQueue is the dispatcher queue a unit was taken from, in the round-robin order.
type mqttCustodyQueue int

const (
	mqttCustodyQueueHints mqttCustodyQueue = iota
	mqttCustodyQueuePass
	mqttCustodyQueueRetry
	mqttCustodyQueueWithdraw
	mqttCustodyQueueContinuity
	mqttCustodyQueueFloor
	mqttCustodyQueues = 6
)

type mqttCustodyUnit struct {
	kind   mqttCustodyUnitKind
	target mqttCustodyTarget
	w      *mqttWithdrawOwed
	cont   *mqttAdmissionContinuity
	// queue is where the unit came from: a unit a quit leaves unrun goes back there.
	queue mqttCustodyQueue
}

// startCustodyDispatcher is THE start site: every hint, gate request, item and tick reaches the
// dispatcher through it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) startCustodyDispatcher() {
	seams := sess.custodyDriverSeams()
	if seams.startEntry != nil {
		seams.startEntry()
	}
	srv := sess.serverFor(nil)
	sess.mu.Lock()
	if sess.custodyObserving || sess.custody == nil || sess.jsa == nil || sess.asm == nil {
		sess.mu.Unlock()
		return
	}
	leases, gen := sess.leases, sess.gen
	sess.mu.Unlock()
	// ADMISSION BEFORE THE FLAG. A server's shutdown waits on willWorkerWG, so an admitted run
	// cannot outlive it; a closed gate starts nothing. (A session with no server — a unit
	// fixture — has no shutdown to join and runs unadmitted.)
	admitted := false
	if srv != nil {
		if !srv.admitWillWorker() {
			return
		}
		admitted = true
	}
	var lease *mqttLease
	if leases != nil {
		l, ok := leases.acquire()
		if !ok {
			// A retirement owns the incarnation: nothing starts.
			if admitted {
				srv.willWorkerWG.Done()
			}
			return
		}
		lease = l
	}
	sess.mu.Lock()
	if sess.custodyObserving || sess.gen != gen {
		sess.mu.Unlock()
		if lease != nil {
			lease.release()
		}
		if admitted {
			srv.willWorkerWG.Done()
		}
		return
	}
	sess.custodyObserving = true
	epoch := sess.asm.sessStreamEpoch
	sess.mu.Unlock()
	if seams.started != nil {
		seams.started(gen)
	}
	go sess.runCustodyDispatcher(srv, admitted, lease, gen, epoch)
}

// mqttCustodyTickKey is the tracked-timer key of ONE arm of a generation's custody tick. Every arm
// is a fresh key: a tracked timer stays registered until its callback returns, so a run the tick
// started that exits while the callback is still inside would be refused a re-arm under a key it
// shared with the firing tick, and nothing would be left armed for its outstanding work.
func mqttCustodyTickKey(clientID, gen string, arm uint64) string {
	return "$MQTT_custody_tick." + clientID + "." + gen + "." + strconv.FormatUint(arm, 10)
}

// rearmCustodyAfterReopen is the incarnation tracker's reopen callback: a closing tracker
// refused every start (no lease), so the hints, the requested pass, the latched demand, the
// read retries and the owed items it left queued are re-armed now that the incarnation
// re-admits. Nothing queued: nothing starts.
//
// Session lock NOT held on entry.
func (sess *mqttSession) rearmCustodyAfterReopen() {
	sess.mu.Lock()
	// A throttled census is owed too: the tick a closing incarnation consumed was its only arm.
	owed := len(sess.custodyHints) > 0 || sess.custodyPassWanted || len(sess.custodyPassQueue) > 0 ||
		sess.custodyDemand || len(sess.custodyRetry) > 0 || len(sess.withdrawOwed) > 0 ||
		len(sess.admissionContinuity) > 0 || (sess.custody != nil && sess.custody.censusWanted) ||
		sess.floorWanted
	sess.mu.Unlock()
	if owed {
		sess.startCustodyDispatcher()
	}
}

// runCustodyDispatcher is the single-flight dispatcher loop.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runCustodyDispatcher(srv *Server, admitted bool, lease *mqttLease, gen, epoch string) {
	defer func() {
		if lease != nil {
			lease.release()
		}
		if admitted {
			srv.willWorkerWG.Done()
		}
	}()
	// A RETIRING INCARNATION STOPS THE RUN. The lease is held until the run returns, and a
	// retirement waits for it to drain before the generation may change, so a run whose queues
	// keep refilling (read retries due again before the others finish) must not hold it: once
	// the incarnation is closing the run stops between units and leaves its work queued. The
	// CURRENT close cycle is read on every check: a cycle that reopened re-admitted, and the
	// lease, still counted, is waited on only by the next close.
	var tracker *mqttIncarnationTracker
	if lease != nil {
		tracker = lease.tr
	}
	quit := func() bool {
		if admitted && !srv.willDispatchOpen() {
			return true
		}
		select {
		case <-tracker.closingSignal():
			return true
		default:
			return false
		}
	}
	seams := sess.custodyDriverSeams()
	for {
		if quit() {
			sess.exitCustodyDispatcher(srv, gen, false)
			return
		}
		units, census, ledger := sess.nextCustodyUnits(gen, epoch)
		if ledger == nil {
			sess.exitCustodyDispatcher(srv, gen, false)
			return
		}
		if len(units) == 0 && !census {
			if seams.idle != nil {
				seams.idle(gen)
			}
			if sess.exitCustodyDispatcher(srv, gen, true) {
				continue
			}
			return
		}
		if seams.iteration != nil {
			seams.iteration(gen, units, census)
		}
		released := false
		if census {
			sess.reconstructCustody(gen)
			released = true // a completed census may reopen capacity
		}
		for i, u := range units {
			if quit() {
				sess.requeueCustodyUnits(gen, units[i:])
				break
			}
			if seams.unit != nil {
				seams.unit(gen, u)
			}
			if sess.runCustodyUnit(gen, epoch, ledger, u) {
				released = true
			}
		}
		if released {
			sess.releaseDeferredAfterCustody()
		}
	}
}

// mqttCustodyDue is THE definition of an item being due: its eligibility has come. A zero
// eligibility — an item just recorded, or one whose next step is immediate — is due at once.
func mqttCustodyDue(eligibleAt, now time.Time) bool {
	return !eligibleAt.After(now)
}

// custodyItemDueLocked reports whether any read retry, owed withdrawal or admission continuity
// record is due.
//
// Lock held on entry.
func (sess *mqttSession) custodyItemDueLocked(now time.Time) bool {
	for _, r := range sess.custodyRetry {
		if mqttCustodyDue(r.nextEligibleAt, now) {
			return true
		}
	}
	for _, w := range sess.withdrawOwed {
		if mqttCustodyDue(w.nextEligibleAt, now) {
			return true
		}
	}
	for _, c := range sess.admissionContinuity {
		if mqttCustodyDue(c.nextEligibleAt, now) {
			return true
		}
	}
	return sess.floorDueLocked(now)
}

// nextCustodyUnits takes the next iteration's units: at most mqttCustodyDispatchUnits, round-robin
// across the non-empty queues, plus whether the census runs now. A nil ledger means the captured
// identity no longer holds and the loop exits.
//
// Session lock NOT held on entry.
func (sess *mqttSession) nextCustodyUnits(gen, epoch string) ([]mqttCustodyUnit, bool, *mqttCustodyLedger) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.gen != gen || sess.asm == nil || sess.asm.sessStreamEpoch != epoch || !sess.custody.forGen(gen) {
		return nil, false, nil
	}
	ledger := sess.custody
	now := time.Now()
	_, capd := sess.custodyDelaysLocked()

	// A REQUESTED PASS refills the pass queue and asks for the census; the census itself is
	// admitted at most once per cap interval across every trigger.
	census := false
	// A LATCHED DEMAND whose deadline came is a requested pass while an allocation may still be
	// waiting on a re-observation; once none can be, the demand is retired at this unit (its
	// subject is decided), so a waiter that left without being allocated does not leave a pass
	// served every cap interval for the session's life.
	if sess.custodyDemand && !sess.custodyDemandDueAt.IsZero() && !now.Before(sess.custodyDemandDueAt) {
		if sess.noAllocationCanBeWaitingLocked(ledger) {
			sess.custodyDemand, sess.custodyDemandDelay, sess.custodyDemandDueAt = false, 0, time.Time{}
		} else {
			if s := sess.testCustodyDriver; s != nil && s.demandDue != nil {
				s.demandDue(gen, sess.custodyDemandDueAt)
			}
			sess.custodyDemandDueAt = time.Time{}
			sess.custodyPassWanted = true
		}
	}
	if sess.custodyPassWanted {
		sess.custodyPassWanted = false
		if ledger.withholdsNew() {
			ledger.censusWanted = true
		}
		if len(sess.custodyPassQueue) == 0 {
			sess.custodyPassQueue = ledger.targets(gen, mqttCustodyReobserveBudget)
		}
	}
	if ledger.censusWanted && (ledger.lastHeavyAt.IsZero() || now.Sub(ledger.lastHeavyAt) >= capd) {
		ledger.censusWanted = false
		ledger.lastHeavyAt = now
		census = true
	}

	// The queues, in a fixed order the round-robin cursor rotates over.
	var hints []string
	for lin := range sess.custodyHints {
		hints = append(hints, lin)
	}
	var retries []string
	for lin, r := range sess.custodyRetry {
		if mqttCustodyDue(r.nextEligibleAt, now) {
			retries = append(retries, lin)
		}
	}
	var items []*mqttWithdrawOwed
	for _, w := range sess.withdrawOwed {
		if mqttCustodyDue(w.nextEligibleAt, now) {
			items = append(items, w)
		}
	}
	var conts []*mqttAdmissionContinuity
	for _, c := range sess.admissionContinuity {
		if mqttCustodyDue(c.nextEligibleAt, now) {
			conts = append(conts, c)
		}
	}
	floorDue := sess.floorDueLocked(now)
	queues := mqttCustodyQueues
	var units []mqttCustodyUnit
	idx := [mqttCustodyQueues]int{}
	for len(units) < mqttCustodyDispatchUnits {
		progressed := false
		for k := 0; k < queues && len(units) < mqttCustodyDispatchUnits; k++ {
			q := mqttCustodyQueue((sess.custodyRR + k) % queues)
			switch q {
			case mqttCustodyQueueHints:
				if idx[0] < len(hints) {
					lin := hints[idx[0]]
					idx[0]++
					delete(sess.custodyHints, lin)
					units = append(units, mqttCustodyUnit{kind: mqttCustodyUnitTarget,
						target: mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: lin}, queue: q})
					progressed = true
				}
			case mqttCustodyQueuePass:
				if len(sess.custodyPassQueue) > 0 {
					t := sess.custodyPassQueue[0]
					sess.custodyPassQueue = sess.custodyPassQueue[1:]
					units = append(units, mqttCustodyUnit{kind: mqttCustodyUnitTarget, target: t, queue: q})
					progressed = true
				}
			case mqttCustodyQueueRetry:
				if idx[2] < len(retries) {
					lin := retries[idx[2]]
					idx[2]++
					// Not re-picked until its read decides: the read's outcome reschedules it.
					if r := sess.custodyRetry[lin]; r != nil {
						r.nextEligibleAt = now.Add(capd)
					}
					units = append(units, mqttCustodyUnit{kind: mqttCustodyUnitTarget,
						target: mqttCustodyTarget{kind: mqttCustodyTargetExchange, key: lin}, queue: q})
					progressed = true
				}
			case mqttCustodyQueueWithdraw:
				if idx[3] < len(items) {
					w := items[idx[3]]
					idx[3]++
					w.nextEligibleAt = now.Add(capd) // decided by the unit's outcome
					units = append(units, mqttCustodyUnit{kind: mqttCustodyUnitWithdraw, w: w, queue: q})
					progressed = true
				}
			case mqttCustodyQueueContinuity:
				if idx[4] < len(conts) {
					c := conts[idx[4]]
					idx[4]++
					c.nextEligibleAt = now.Add(capd) // decided by the unit's outcome
					units = append(units, mqttCustodyUnit{kind: mqttCustodyUnitContinuity, cont: c, queue: q})
					progressed = true
				}
			case mqttCustodyQueueFloor:
				if floorDue {
					floorDue = false
					// Not re-picked until the unit decides: its outcome reschedules it.
					sess.floorDueAt = now.Add(capd)
					units = append(units, mqttCustodyUnit{kind: mqttCustodyUnitFloor, queue: q})
					progressed = true
				}
			}
		}
		if !progressed {
			break
		}
	}
	sess.custodyRR = (sess.custodyRR + 1) % queues
	return units, census, ledger
}

// requeueCustodyUnits gives back the units a quit left unrun. Taking a unit REMOVED it from its
// queue (a hint, a pass target) or re-timed it as if its outcome would decide it (a retry, an
// owed withdrawal, a continuity record), so an unrun unit returns where it came from, due — it
// was due when taken and nothing decided it since. A generation that moved discarded them all.
//
// Session lock NOT held on entry.
func (sess *mqttSession) requeueCustodyUnits(gen string, units []mqttCustodyUnit) {
	if len(units) == 0 {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.gen != gen {
		return
	}
	now := time.Now()
	var pass []mqttCustodyTarget
	for _, u := range units {
		switch u.queue {
		case mqttCustodyQueueHints:
			if sess.custodyHints == nil {
				sess.custodyHints = make(map[string]struct{})
			}
			sess.custodyHints[u.target.key] = struct{}{}
		case mqttCustodyQueuePass:
			pass = append(pass, u.target)
		case mqttCustodyQueueRetry:
			if r := sess.custodyRetry[u.target.key]; r != nil {
				r.nextEligibleAt = now
			}
		case mqttCustodyQueueWithdraw:
			if u.w != nil && sess.withdrawOwed[u.w.lin] == u.w {
				u.w.nextEligibleAt = now
			}
		case mqttCustodyQueueContinuity:
			if c := u.cont; c != nil && sess.admissionContinuity[mqttDeferredKey{c.dur, c.seq}] == c {
				c.nextEligibleAt = now
			}
		case mqttCustodyQueueFloor:
			// The latch was never cleared by taking the unit; it is due again.
			if sess.floorWanted {
				sess.floorDueAt = now
			}
		}
	}
	if len(pass) > 0 {
		// In front of anything queued since: these were taken first.
		sess.custodyPassQueue = append(pass, sess.custodyPassQueue...)
	}
}

// runCustodyUnit runs ONE unit under its captured identity, and reports whether a charge was
// released.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runCustodyUnit(gen, epoch string, ledger *mqttCustodyLedger, u mqttCustodyUnit) bool {
	sess.mu.Lock()
	same := sess.gen == gen && sess.custody == ledger && sess.asm != nil && sess.asm.sessStreamEpoch == epoch
	sess.mu.Unlock()
	if !same {
		return false
	}
	switch u.kind {
	case mqttCustodyUnitTarget:
		return sess.observeCustodyTarget(gen, u.target)
	case mqttCustodyUnitWithdraw:
		sess.runWithdrawOwedUnit(gen, u.w)
	case mqttCustodyUnitContinuity:
		sess.runAdmissionContinuityUnit(gen, u.cont)
	case mqttCustodyUnitFloor:
		sess.runFloorUnit(gen, epoch)
	}
	return false
}

// runWithdrawOwedUnit is ONE withdrawUnsent for an owed withdrawal (never the door's two-attempt
// loop), and the record's disposition from its outcome:
//   - landed: the obligation is ended (WithdrawnUnsent); the record retires and the exchange's own
//     observation releases its custody;
//   - unlicensed: the deciding entitlement no longer serves; the record is compare-deleted, and
//     the successor's recovery reconstructs the withdrawal under its own licence;
//   - no marker: the unsent marker is gone, so nothing proves the PUBLISH unsent any more; the
//     withdrawal is no longer owed (the exchange is possibly-attempted and its acknowledgement or
//     the next resume owns it), and the record retires;
//   - no evidence, the exchange OWES NO FURTHER DELIVERY (ended, its delivery abandoned, or its
//     generation discarded): the debt is discharged (an earlier attempt of this withdrawal may
//     have landed behind a lost response), and the record retires - the record only;
//   - no evidence otherwise: the register MOVED; it is re-read and the record RECLASSIFIED to the
//     moved sequence (never erased) while the same allocation stands, or retired when it does not;
//   - never landed / unresolved: backed off and retried.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runWithdrawOwedUnit(gen string, w *mqttWithdrawOwed) {
	if w == nil || w.gen != gen {
		return
	}
	r := sess.withdrawUnsent(w.gen, w.subject, w.nonce, w.lin, w.lic, nil)
	var moved uint64
	stands, discharged := false, false
	if r == mqttWithdrawNoEvidence && sess.asm != nil && sess.jsa != nil {
		// AN EXCHANGE THAT OWES NO FURTHER DELIVERY CARRIES NO UNSENT OBLIGATION, so the debt that
		// names it is discharged. That is an ending, an abandoned delivery or a discarded
		// generation (mqttExchangeDeliveryDutyEnded, the one retirement predicate shared with
		// admission continuity): each is absorbing, and the withdrawal is owed only while the
		// exchange is at Publish. A withdrawal that committed while its settlement could not be
		// read ended it already, and its tombstone (same allocation, same lineage) is the door's
		// to delete, not this record's to wait on; an abandoned delivery's register is its
		// disposal record, kept for the client's late acknowledgement.
		//
		// ONLY THE ITEM RETIRES. The identifier, the ACK binding, the register and the holds are
		// untouched: the duty question is not the identifier question (mqttExchangePhaseEnded
		// keeps that one, and the destruction door keeps its own), so an abandoned exchange's
		// identifier stays closed. An unreadable exchange concludes nothing.
		x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, w.gen, w.lin)
		switch {
		case xerr == nil && x != nil && x.Nonce == w.lin && mqttExchangeDeliveryDutyEnded(x.Phase):
			discharged = true
		case xerr != nil && !mqttExchangeReadAbsent(xerr):
			stands = true // unread: nothing concluded, keep it
		default:
			if cur, smsg, lerr := sess.asm.loadInflightRecord(w.subject); lerr != nil {
				stands = true // unread: nothing concluded, keep it
			} else if smsg != nil && cur != nil && cur.Nonce == w.nonce && cur.Lin == w.lin {
				stands, moved = true, smsg.Sequence
			}
		}
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.withdrawOwed[w.lin] != w {
		return // replaced or retired meanwhile: never act on a newer record
	}
	switch r {
	case mqttWithdrawLanded, mqttWithdrawUnlicensed, mqttWithdrawNoMarker:
		sess.retireWithdrawOwedLocked(w)
		return
	case mqttWithdrawNoEvidence:
		if discharged || !stands {
			sess.retireWithdrawOwedLocked(w)
			return
		}
		if moved != 0 {
			w.regSeq = moved
		}
	}
	base, capd := sess.custodyDelaysLocked()
	w.delay = mqttCustodyNextDelay(w.delay, base, capd)
	w.nextEligibleAt = time.Now().Add(mqttCustodyJitter(w.delay))
}

// exitCustodyDispatcher ends a run: it clears the single-flight flag, unless work became due
// meanwhile (then it reports true and the loop continues), and arms the tick for the earliest
// eligible item, latched demand or throttled census.
//
// Session lock NOT held on entry.
func (sess *mqttSession) exitCustodyDispatcher(srv *Server, gen string, mayContinue bool) bool {
	sess.mu.Lock()
	now := time.Now()
	// WORK RECORDED WHILE THE FLAG STOOD IS THIS RUN'S. A recorder that found the flag set started
	// nothing, so a hint, a pass request or a due item that arrived after the run last looked is
	// taken by the loop, or nothing would ever take it.
	if mayContinue && sess.gen == gen && (len(sess.custodyHints) > 0 || sess.custodyPassWanted ||
		len(sess.custodyPassQueue) > 0 || sess.custodyItemDueLocked(now)) {
		sess.mu.Unlock()
		return true
	}
	sess.custodyObserving = false
	if sess.gen != gen || sess.custody == nil {
		sess.custodyExitSettledLocked(gen)
		sess.mu.Unlock()
		return false
	}
	// A run that may not continue still leaves a due item armed: a zero eligibility is "now", never
	// "nothing to arm".
	var at time.Time
	earliest := func(t time.Time) {
		if mqttCustodyDue(t, now) {
			t = now
		}
		if at.IsZero() || t.Before(at) {
			at = t
		}
	}
	for _, r := range sess.custodyRetry {
		earliest(r.nextEligibleAt)
	}
	for _, w := range sess.withdrawOwed {
		earliest(w.nextEligibleAt)
	}
	for _, c := range sess.admissionContinuity {
		earliest(c.nextEligibleAt)
	}
	if sess.floorWanted {
		earliest(sess.floorDueAt)
	}
	// Hints and pass work have no eligibility of their own: queued, they are due now. A run that
	// may not continue — its epoch moved, or it quit — leaves them to the tick (a closing
	// incarnation refuses its start, and the reopen re-arms; shutdown refuses the arm).
	if len(sess.custodyHints) > 0 || sess.custodyPassWanted || len(sess.custodyPassQueue) > 0 {
		earliest(now)
	}
	base, capd := sess.custodyDelaysLocked()
	if sess.custodyDemand {
		// THE DEMAND LATCH: a withholding gate's demand is re-served on the dispatcher's own
		// cadence until a withheld allocation has since succeeded with nothing deferred.
		// The pass is requested WHEN THE DEADLINE COMES (nextCustodyUnits), not now: between
		// deadlines nothing is queued and the dispatcher is idle.
		//
		// A deadline stands until it is consumed: only a run that consumed it (or a demand not yet
		// given one) backs off and sets the next. Any other exit — a hint's run — leaves it as it
		// is, or periodic hints arriving more often than the delay would postpone the demand's
		// pass for as long as they continue.
		if sess.custodyDemandDueAt.IsZero() {
			sess.custodyDemandDelay = mqttCustodyNextDelay(sess.custodyDemandDelay, base, capd)
			sess.custodyDemandDueAt = now.Add(mqttCustodyJitter(sess.custodyDemandDelay))
		}
		earliest(sess.custodyDemandDueAt)
	}
	if sess.custody.censusWanted {
		earliest(sess.custody.lastHeavyAt.Add(capd))
	}
	if at.IsZero() || srv == nil {
		sess.custodyExitSettledLocked(gen)
		sess.mu.Unlock()
		return false
	}
	if !sess.custodyTickAt.IsZero() && !sess.custodyTickAt.After(at) {
		sess.custodyExitSettledLocked(gen)
		sess.mu.Unlock()
		return false // an earlier (or equal) tick is already armed
	}
	// THE ARM IDENTITY. Each arm takes the next custodyTickArm and its own key, stored with the
	// deadline; the tick that is current is the one whose arm the session still holds. A
	// superseded arm is cancelled by its STORED key, and one that fires anyway is inert.
	oldKey := sess.custodyTickKey
	sess.custodyTickArm++
	arm := sess.custodyTickArm
	key := mqttCustodyTickKey(sess.id, gen, arm)
	sess.custodyTickAt, sess.custodyTickKey = at, key
	sess.custodyExitSettledLocked(gen)
	sess.mu.Unlock()

	if oldKey != _EMPTY_ {
		srv.cancelWillTimer(oldKey)
	}
	delay := time.Until(at)
	if !srv.scheduleWillTimer(key, delay, func() {
		sess.mu.Lock()
		// A tick of a retired generation, or a superseded arm, is INERT: the generation's work
		// went with its ledger, and the arm that superseded it carries the session's deadline.
		current := sess.gen == gen && sess.custodyTickArm == arm
		if current {
			sess.custodyTickAt, sess.custodyTickKey = time.Time{}, _EMPTY_
		}
		sess.mu.Unlock()
		if current {
			sess.startCustodyDispatcher()
			if s := sess.custodyDriverSeams(); s.tickStarted != nil {
				s.tickStarted(gen, key)
			}
		}
	}) {
		// A closed dispatch gate (shutdown): nothing more to arm. Clear this arm's deadline, if
		// it is still the current one, so a later exit can arm again.
		sess.mu.Lock()
		if sess.gen == gen && sess.custodyTickArm == arm {
			sess.custodyTickAt, sess.custodyTickKey = time.Time{}, _EMPTY_
		}
		sess.mu.Unlock()
		return false
	}
	// Scheduling ran outside the session lock, so this arm may have been superseded — replaced
	// by a later exit, or discarded with its generation — before its timer registered, when the
	// cancellation of its stored key found nothing to cancel. Such a timer is cancelled now.
	sess.mu.Lock()
	superseded := sess.gen != gen || sess.custodyTickArm != arm
	sess.mu.Unlock()
	if superseded {
		srv.cancelWillTimer(key)
	}
	return false
}
