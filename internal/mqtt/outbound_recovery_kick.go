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
	"sync"
	"sync/atomic"
	"time"
)

// The prompt drive of a discarded generation's recovery (issue #107 7d).
//
// A teardown hands its source, drain and PUBREL consumers to the generation's recovery root
// instead of deleting them. The periodic recovery pass would retire them — eventually, at its own
// cadence — and until then each one keeps its stream's interest-retained traffic. So the teardown
// KICKS the root: this worker visits it with the pass's own visitRecoveryRoot, the same executor,
// the same cursor, the same budget per visit. It is a CALLER, not a mechanism: nothing is correct
// because it runs, and everything it leaves is the pass's.
//
// It is a FIXED, BOUNDED attempt and judges no progress. Each kick grants its root at most
// mqttRecoveryKickMaxVisits CHARGED visits, served mqttRecoveryKickQuantum per turn, round-robin in
// FIFO order across the roots that are due; the root leaves the set when it concludes or its
// visits are spent. A root whose work is withheld, unreadable or contended therefore costs at most
// that many visits and cannot starve another (one quantum per turn), and a kick that finds the set
// full is dropped. Neither case loses anything: the root stays in the durable directory, which the
// periodic pass walks with eventual visitation and no latency bound.
//
// A VISIT ITS CLIENT'S FOREGROUND REFUSED IS NOT A VISIT (outbound_foreground_priority.go). An
// attempt is uncharged exactly when a coordinator check inside it actually refused it — at the
// visit's entry or at any authority attempt past it; every other attempt keeps its charge, a
// closure or root that cannot be read before the visit included (an announcement standing then
// exempts nothing). A refused root ends its turn and goes back behind a NOT-BEFORE time,
// mqttRecoveryKickBackoffMin doubling to mqttRecoveryKickBackoffMax and reset by a charged
// visit, so a client that stays announced holds its own cleanup back and nobody else's. The
// worker serves due roots only; it sleeps — outside its mutex — only when none is due, until the
// earliest is or a new kick arrives, and a kick of a root already queued never postpones it.
//
// Lifecycle: the worker is started by the first kick that finds none running and exits when the
// set is empty or the server is shutting down; a later kick starts a fresh one. One per account.

const (
	// mqttRecoveryKickMaxVisits bounds the charged visits one kick grants its root. A typical
	// discarded generation — each consumer disposed, authorized, deleted; each obligation
	// discarded and its records deleted; the roll; the conclusion — completes in well under it.
	mqttRecoveryKickMaxVisits = 32
	// mqttRecoveryKickQuantum is how many of them a root receives per turn.
	mqttRecoveryKickQuantum = 8
	// mqttRecoveryKickCap bounds the roots awaiting the prompt drive.
	mqttRecoveryKickCap = 256
)

// mqttRecoveryKickBackoffMin and mqttRecoveryKickBackoffMax bound the not-before delay of a root
// its client's foreground refused. Knobs only so a test can drive many refusals in bounded time.
var (
	mqttRecoveryKickBackoffMin = newMQTTKnob(50 * time.Millisecond)
	mqttRecoveryKickBackoffMax = newMQTTKnob(time.Second)
)

// mqttRecoveryKickKey names one generation's root.
type mqttRecoveryKickKey struct {
	id, epoch, gen string
}

// mqttRecoveryKickState is one queued root's standing: the charged visits it has had, and the
// not-before time and backoff its last refusal set.
type mqttRecoveryKickState struct {
	visits    int
	notBefore time.Time
	backoff   time.Duration
}

// mqttRecoveryKick is the account's prompt-drive state.
type mqttRecoveryKick struct {
	mu      sync.Mutex
	queue   []mqttRecoveryKickKey
	keys    map[mqttRecoveryKickKey]*mqttRecoveryKickState
	running bool
	// wake tells a sleeping worker a new root was queued.
	wake chan struct{}
}

// mqttRecoveryVisitOutcome is what one prompt-drive attempt came to.
type mqttRecoveryVisitOutcome uint8

const (
	// mqttRecoveryVisitCharged: the attempt counts against the kick — a visit performed, or a
	// closure or root this worker could not read. The root remains.
	mqttRecoveryVisitCharged mqttRecoveryVisitOutcome = iota
	// mqttRecoveryVisitGone: concluded, or never this worker's to visit.
	mqttRecoveryVisitGone
	// mqttRecoveryVisitRefused: a coordinator check refused it; nothing was charged.
	mqttRecoveryVisitRefused
)

// mqttTestRecoveryKickHookV, when set, is consulted before each queued root's turn: returning false
// PARKS the worker (it exits, leaving the set intact), which is how a test observes a teardown's
// state before any prompt retirement, and how it isolates the periodic pass from the prompt drive.
var mqttTestRecoveryKickHookV atomic.Pointer[func(id, gen string) bool]

// mqttTestRecoveryKickVisitV, when set, REPLACES one visit and reports its outcome, so a test can
// measure the worker's scheduling — the allowance, the quantum, the order, the refusals — apart
// from what a visit does.
var mqttTestRecoveryKickVisitV atomic.Pointer[func(id, gen string) mqttRecoveryVisitOutcome]

// mqttTestRecoveryKickOutcomeV, when set, observes every attempt's outcome. Nil in production.
var mqttTestRecoveryKickOutcomeV atomic.Pointer[func(id, gen string, out mqttRecoveryVisitOutcome)]

// kickRecoveryRoot asks for a prompt visit of one discarded generation's root. Never blocks; safe
// with no lock held and under an open record transaction (it issues no request).
func (as *mqttAccountSessionManager) kickRecoveryRoot(id, epoch, gen string) {
	if as == nil || id == _EMPTY_ || gen == _EMPTY_ {
		return
	}
	k := mqttRecoveryKickKey{id: id, epoch: epoch, gen: gen}
	rk := &as.recoveryKick
	rk.mu.Lock()
	if rk.keys == nil {
		rk.keys = make(map[mqttRecoveryKickKey]*mqttRecoveryKickState)
	}
	if _, queued := rk.keys[k]; !queued {
		// THE CAP COUNTS EVERY HELD ROOT, the one the worker is visiting included: its slot is
		// reserved while it is out of the queue, so a kick landing meanwhile cannot take it and
		// a requeue always finds it.
		if len(rk.keys) >= mqttRecoveryKickCap {
			// FULL: the periodic pass owns it, as it owns everything this worker leaves.
			rk.mu.Unlock()
			return
		}
		rk.keys[k] = &mqttRecoveryKickState{}
		rk.queue = append(rk.queue, k)
		// A sleeping worker learns of a root due now. A key already queued keeps its not-before.
		select {
		case rk.wakeLocked() <- struct{}{}:
		default:
		}
	}
	// A key already queued is not queued twice, but a worker that a test parked is started
	// again: the set outlives the worker that served it.
	start := !rk.running
	rk.running = true
	rk.mu.Unlock()
	if !start {
		return
	}
	s := as.server()
	if s == nil {
		rk.mu.Lock()
		rk.running = false
		rk.mu.Unlock()
		return
	}
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.runRecoveryKick(s)
	})
}

// wakeLocked returns the wake channel, making it.
//
// Lock held on entry.
func (rk *mqttRecoveryKick) wakeLocked() chan struct{} {
	if rk.wake == nil {
		rk.wake = make(chan struct{}, 1)
	}
	return rk.wake
}

// nextDueLocked returns the position of the first queued root that is due at now, or -1 and how
// long until the earliest is.
//
// Lock held on entry.
func (rk *mqttRecoveryKick) nextDueLocked(now time.Time) (int, time.Duration) {
	var wait time.Duration = -1
	for i, k := range rk.queue {
		st := rk.keys[k]
		if st == nil || !st.notBefore.After(now) {
			return i, 0
		}
		if d := st.notBefore.Sub(now); wait < 0 || d < wait {
			wait = d
		}
	}
	return -1, wait
}

// runRecoveryKick serves the queued roots until the set is empty, the server stops, or a test parks
// it.
func (as *mqttAccountSessionManager) runRecoveryKick(s *Server) {
	rk := &as.recoveryKick
	stop := func() bool {
		select {
		case <-s.quitCh:
			return true
		default:
			return false
		}
	}
	for {
		rk.mu.Lock()
		if len(rk.queue) == 0 || stop() {
			if stop() {
				rk.queue, rk.keys = nil, nil
			}
			rk.running = false
			rk.mu.Unlock()
			return
		}
		i, wait := rk.nextDueLocked(time.Now())
		if i < 0 {
			// NOTHING IS DUE: sleep until the earliest is, a new root arrives, or shutdown.
			wake := rk.wakeLocked()
			rk.mu.Unlock()
			t := time.NewTimer(wait)
			select {
			case <-wake:
			case <-t.C:
			case <-s.quitCh:
			}
			t.Stop()
			continue
		}
		k := rk.queue[i]
		rk.queue = append(rk.queue[:i:i], rk.queue[i+1:]...)
		rk.mu.Unlock()

		if h := mqttTestRecoveryKickHookV.Load(); h != nil && !(*h)(k.id, k.gen) {
			rk.mu.Lock()
			rk.requeueLocked(k, true)
			rk.running = false
			rk.mu.Unlock()
			return
		}

		concluded, refused := false, false
		for n := 0; n < mqttRecoveryKickQuantum && !concluded && !refused && !stop(); n++ {
			rk.mu.Lock()
			st := rk.keys[k]
			if st == nil || st.visits >= mqttRecoveryKickMaxVisits {
				rk.mu.Unlock()
				break
			}
			st.visits++
			rk.mu.Unlock()
			out := as.visitKickedRoot(s, k, stop)
			if h := mqttTestRecoveryKickOutcomeV.Load(); h != nil {
				(*h)(k.id, k.gen, out)
			}
			switch out {
			case mqttRecoveryVisitGone:
				concluded = true
			case mqttRecoveryVisitRefused:
				// NOT CHARGED: the visit its client's foreground refused gives its charge back.
				rk.mu.Lock()
				st.visits--
				rk.mu.Unlock()
				refused = true
			}
		}

		rk.mu.Lock()
		st := rk.keys[k]
		switch {
		case st == nil:
		case concluded || st.visits >= mqttRecoveryKickMaxVisits:
			delete(rk.keys, k)
		case refused:
			st.backoff = min(max(2*st.backoff, mqttRecoveryKickBackoffMin.Get()), mqttRecoveryKickBackoffMax.Get())
			st.notBefore = time.Now().Add(st.backoff)
			rk.requeueLocked(k, false)
		default:
			st.backoff, st.notBefore = 0, time.Time{}
			rk.requeueLocked(k, false)
		}
		rk.mu.Unlock()
	}
}

// requeueLocked puts back a root this worker took, in front or behind, into the slot it kept: a
// held root stays in the key set, which is what the cap counts, so no kick taken while it was out
// can have displaced it, and a refused root with visits left is never dropped.
//
// Lock held on entry.
func (rk *mqttRecoveryKick) requeueLocked(k mqttRecoveryKickKey, front bool) {
	if front {
		rk.queue = append([]mqttRecoveryKickKey{k}, rk.queue...)
	} else {
		rk.queue = append(rk.queue, k)
	}
}

// visitKickedRoot performs one visit of a kicked root, reloaded fresh, and reports its outcome:
// gone — concluded, or never this worker's to visit (no closure, no reachable root) —,
// refused by its client's foreground, or charged.
func (as *mqttAccountSessionManager) visitKickedRoot(s *Server, k mqttRecoveryKickKey, stop func() bool) mqttRecoveryVisitOutcome {
	if h := mqttTestRecoveryKickVisitV.Load(); h != nil {
		return (*h)(k.id, k.gen)
	}
	closed, _, st := as.jsa.loadGenClosure(k.id, k.epoch, k.gen)
	switch {
	case st == mqttOutboundAbsent:
		return mqttRecoveryVisitGone
	case st != mqttOutboundValid:
		return mqttRecoveryVisitCharged // unreadable: spends a visit, the pass owns the rest
	}
	root, rootSeq, err := as.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd)
	if err != nil {
		return mqttRecoveryVisitCharged
	}
	if root == nil || root.ID != k.id || root.Epoch != k.epoch || root.Gen != k.gen {
		return mqttRecoveryVisitGone // retired, or not this generation's
	}
	if as.visitRecoveryRoot(s, root, rootSeq, newMqttRecoveryBudget(), stop) {
		return mqttRecoveryVisitRefused
	}
	return mqttRecoveryVisitCharged
}
