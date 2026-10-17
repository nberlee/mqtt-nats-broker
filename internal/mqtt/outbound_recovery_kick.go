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
)

// The prompt drive of a discarded generation's recovery (F-105 7d).
//
// A teardown hands its source, drain and PUBREL consumers to the generation's recovery root
// instead of deleting them. The periodic recovery pass would retire them — eventually, at its own
// cadence — and until then each one keeps its stream's interest-retained traffic. So the teardown
// KICKS the root: this worker visits it with the pass's own visitRecoveryRoot, the same executor,
// the same cursor, the same budget per visit. It is a CALLER, not a mechanism: nothing is correct
// because it runs, and everything it leaves is the pass's.
//
// It is a FIXED, BOUNDED attempt and judges no progress. Each kick grants its root at most
// mqttRecoveryKickMaxVisits visits, served mqttRecoveryKickQuantum per turn, round-robin in FIFO
// order across roots; the root leaves the set when it concludes or its visits are spent. A root
// whose work is withheld, unreadable or contended therefore costs at most that many visits and
// cannot starve another (one quantum per turn), and a kick that finds the set full is dropped.
// Neither case loses anything: the root stays in the durable directory, which the periodic pass
// walks with eventual visitation and no latency bound.
//
// Lifecycle: the worker is started by the first kick that finds none running and exits when the
// set is empty or the server is shutting down; a later kick starts a fresh one. One per account.

const (
	// mqttRecoveryKickMaxVisits bounds the visits one kick grants its root. A typical discarded
	// generation — each consumer disposed, authorized, deleted; each obligation discarded and its
	// records deleted; the roll; the conclusion — completes in well under it.
	mqttRecoveryKickMaxVisits = 32
	// mqttRecoveryKickQuantum is how many of them a root receives per turn.
	mqttRecoveryKickQuantum = 8
	// mqttRecoveryKickCap bounds the roots awaiting the prompt drive.
	mqttRecoveryKickCap = 256
)

// mqttRecoveryKickKey names one generation's root.
type mqttRecoveryKickKey struct {
	id, epoch, gen string
}

// mqttRecoveryKick is the account's prompt-drive state.
type mqttRecoveryKick struct {
	mu      sync.Mutex
	queue   []mqttRecoveryKickKey
	visits  map[mqttRecoveryKickKey]int
	running bool
}

// mqttTestRecoveryKickHookV, when set, is consulted before each queued root's turn: returning false
// PARKS the worker (it exits, leaving the set intact), which is how a test observes a teardown's
// state before any prompt retirement, and how it isolates the periodic pass from the prompt drive.
var mqttTestRecoveryKickHookV atomic.Pointer[func(id, gen string) bool]

// mqttTestRecoveryKickVisitV, when set, REPLACES one visit and reports whether the root concluded,
// so a test can measure the worker's scheduling — the allowance, the quantum, the order — apart
// from what a visit does.
var mqttTestRecoveryKickVisitV atomic.Pointer[func(id, gen string) bool]

// kickRecoveryRoot asks for a prompt visit of one discarded generation's root. Never blocks; safe
// with no lock held and under an open record transaction (it issues no request).
func (as *mqttAccountSessionManager) kickRecoveryRoot(id, epoch, gen string) {
	if as == nil || id == _EMPTY_ || gen == _EMPTY_ {
		return
	}
	k := mqttRecoveryKickKey{id: id, epoch: epoch, gen: gen}
	rk := &as.recoveryKick
	rk.mu.Lock()
	if rk.visits == nil {
		rk.visits = make(map[mqttRecoveryKickKey]int)
	}
	if _, queued := rk.visits[k]; !queued {
		if len(rk.queue) >= mqttRecoveryKickCap {
			// FULL: the periodic pass owns it, as it owns everything this worker leaves.
			rk.mu.Unlock()
			return
		}
		rk.visits[k] = 0
		rk.queue = append(rk.queue, k)
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
				rk.queue, rk.visits = nil, nil
			}
			rk.running = false
			rk.mu.Unlock()
			return
		}
		k := rk.queue[0]
		rk.queue = rk.queue[1:]
		rk.mu.Unlock()

		if h := mqttTestRecoveryKickHookV.Load(); h != nil && !(*h)(k.id, k.gen) {
			rk.mu.Lock()
			rk.queue = append([]mqttRecoveryKickKey{k}, rk.queue...)
			rk.running = false
			rk.mu.Unlock()
			return
		}

		concluded := false
		for i := 0; i < mqttRecoveryKickQuantum && !concluded && !stop(); i++ {
			rk.mu.Lock()
			if rk.visits[k] >= mqttRecoveryKickMaxVisits {
				rk.mu.Unlock()
				break
			}
			rk.visits[k]++
			rk.mu.Unlock()
			concluded = as.visitKickedRoot(s, k, stop)
		}

		rk.mu.Lock()
		if concluded || rk.visits[k] >= mqttRecoveryKickMaxVisits {
			delete(rk.visits, k)
		} else {
			rk.queue = append(rk.queue, k)
		}
		rk.mu.Unlock()
	}
}

// visitKickedRoot performs one visit of a kicked root, reloaded fresh, and reports whether the root
// is gone — concluded, or never this worker's to visit (no discarding closure, no reachable root).
func (as *mqttAccountSessionManager) visitKickedRoot(s *Server, k mqttRecoveryKickKey, stop func() bool) bool {
	if h := mqttTestRecoveryKickVisitV.Load(); h != nil {
		return (*h)(k.id, k.gen)
	}
	closed, _, st := as.jsa.loadGenClosure(k.id, k.epoch, k.gen)
	switch {
	case st == mqttOutboundAbsent:
		return true
	case st != mqttOutboundValid:
		return false // unreadable: spends a visit, the pass owns the rest
	case !closed.Discard:
		return true
	}
	root, rootSeq, err := as.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd)
	if err != nil {
		return false
	}
	if root == nil || root.ID != k.id || root.Epoch != k.epoch || root.Gen != k.gen {
		return true // retired, or not this generation's
	}
	as.visitRecoveryRoot(s, root, rootSeq, newMqttRecoveryBudget(), stop)
	return false
}
