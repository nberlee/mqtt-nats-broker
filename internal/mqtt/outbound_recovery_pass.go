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

import "sync/atomic"

// The recovery pass: what walks the directory and finishes what a closure left (F-105, row 6).
//
// # Who runs it: any broker serving the account, and no election decides which
//
// An election is an availability dependency on exactly the state being recovered. What makes that
// workable is that every duty execution here is safe under CONCURRENT execution: the deletion
// authorization is a resource CAS, the physical act is a delete of a generation-private name whose
// second execution reports not-found, and every completion and cursor write is a CAS at a sequence
// the writer read. Two brokers walking one root is expected; the loser of any CAS re-reads.
//
// # Budgets bound WORK, and a reserve is held back for progress
//
// One budget covers the work a pass does, counted in UNITS rather than in raw store operations:
// counting invocations bounded nothing, because one classification can read several records, so a
// unit is charged where work is DISPATCHED and priced by what that dispatch costs at worst.
//
// A UNIT IS NOT A ROUND TRIP, and saying so would overclaim. An authorization spends two units and
// then calls opAuthorizeDeletion, which reads twice and whose commitAuthorityTransition performs
// further predecessor and resource validation reads before publishing its batch; the cleanup
// completion and the provisioning seal carry nested reads their caller does not itemise either.
// mqttRecoveryOpBudget is therefore 512 WORK UNITS, and the store-call count is that multiplied by
// a bounded per-unit constant — still bounded, because every helper a unit dispatches performs a
// statically known number of reads and at most one batch, but not enforced at the store-call
// boundary. A RESERVE is held back for the progress writes and
// sized for what they cost under contention: the cursor CAS, its re-read after a lost CAS, and a
// bounded number of such conflicts. A failed CAS is bounded work, not persisted progress.
//
// A pass begins only with enough budget for one complete unit — read, classify, execute, persist.
// Below that it does nothing rather than half of something.
//
// # Three states, because two of them are not "unresolved"
//
// The root invariant is Unresolved < Resume, so an ordinal the pass never examined may not enter
// the accumulator:
//
//	examined and unresolved -> enters this sweep's accumulator, below Resume
//	read but DEFERRED       -> Resume is not advanced past it; the next pass reads it again
//	not read at all         -> the same
//
// An OVERSIZED unit — one whose classification and execution cannot fit a minimum pass — is
// EXAMINED AND UNRESOLVED on every sweep that passes it, atomically with the advance. Not
// "recorded once": mqttRollSweep RESETS the accumulator, so a once-recorded ordinal would be
// skipped by the next sweep and the root could finish with an empty accumulator and certify while
// that duty was still pending.
//
// # Fairness is scheduled, and no latency is promised
//
// The shard walk RESUMES where it stopped rather than restarting at shard 0, which would starve
// the later shards under a per-pass budget. Within a shard the walk always advances past a root it
// visited, whatever that visit concluded, so a permanently withheld or unreadable root is
// revisited on the next cycle rather than consuming every pass. Under unrestricted growth of the
// root population there is no latency bound, only eventual visitation — the same shape the
// orphaned-QoS-2 sweep states for its own candidates.

const (
	// mqttRecoveryShardsPerPass bounds how many shards one pass opens.
	mqttRecoveryShardsPerPass = 8
	// mqttRecoveryRootsPerShard bounds how many roots one pass visits in one shard, which is the
	// per-shard QUANTUM: a continuously productive shard cannot monopolize successive passes.
	mqttRecoveryRootsPerShard = 16
	// mqttRecoveryOpBudget is the store operations one pass may spend, and
	// mqttRecoveryOpReserve the part of it held back for progress writes.
	mqttRecoveryOpBudget  = 512
	mqttRecoveryOpReserve = 24
	// mqttRecoveryMinimumUnit is what one complete duty costs at worst: its entry read, the
	// records a classification reads, the acts an execution performs, and the cursor write.
	//
	// 16 since F-105 7d, counted from the spends: the entry read (1), the dispatch (1), a
	// discarded exchange's classification with its canonical PUBREL and register (4), and the
	// costliest work step — a cleanup's resource read, delete and completion batch (1 + 3), or a
	// discard step's reads and act (3) — plus the cursor write from the reserve. Every visit's
	// first entry therefore fits, so a visit always classifies at least one entry and the census
	// can never starve its own executor (TestF105RecoveryEntryUnitCoversTheCostliestEntry).
	mqttRecoveryMinimumUnit = 16
)

// mqttRecoveryBudget is one pass's accounting.
//
// Every store operation decrements it, and `spend` reports whether the operation may happen at
// all. The reserve is what `spendProgress` may reach and ordinary work may not, so the cursor
// write can never be the thing that does not fit.
type mqttRecoveryBudget struct {
	left     int
	reserve  int
	progress int
	// readiness is the ONE account-wide readiness attempt a pass may make.
	//
	// The assignment stream is keyed by GROUP DURABLE, not by client, so establishing that a
	// retiring generation's assignments are discarded is an account-wide walk and cannot be
	// expressed as a filtered one. Charging it against the per-operation budget does not bound
	// it — it starves it: with a stable population larger than one pass, the budget expires
	// before the walk reaches the end, every pass repeats the same prefix, and readiness is never
	// published. That is permanent starvation on a FINITE population, which is a different thing
	// from the absent latency bound this mechanism does state.
	//
	// So the readiness attempt is bounded by COUNT rather than by operations: at most one per
	// pass, run to completion, and cancellable between RECORD UNITS — a unit already entered
	// finishes, nested retries and mutations included. It costs exactly what the
	// production teardown already pays for the same generation — `discardQ2AffClaimsFor` is that
	// same account-wide walk — so the pass performing it adds no cost that the teardown did not.
	readiness bool
}

func newMqttRecoveryBudget() *mqttRecoveryBudget {
	return &mqttRecoveryBudget{left: mqttRecoveryOpBudget, reserve: mqttRecoveryOpReserve}
}

// spend takes ONE ordinary store operation, refusing once only the reserve is left.
func (b *mqttRecoveryBudget) spend(n int) bool {
	if b.left-n < b.reserve {
		return false
	}
	b.left -= n
	return true
}

// spendProgress takes from the reserve, and is the only thing that may.
func (b *mqttRecoveryBudget) spendProgress(n int) bool {
	if b.left-n < 0 {
		return false
	}
	b.left -= n
	b.progress += n
	return true
}

// fitsAUnit reports whether a complete duty still fits, which is what a pass requires before it
// reads anything at all.
func (b *mqttRecoveryBudget) fitsAUnit() bool {
	return b.left-b.reserve >= mqttRecoveryMinimumUnit
}

// takeReadinessAttempt claims this pass's single account-wide readiness attempt.
func (b *mqttRecoveryBudget) takeReadinessAttempt() bool {
	if b.readiness {
		return false
	}
	b.readiness = true
	return true
}

// readinessSpent says whether this pass has already performed its one account-wide readiness walk.
//
// The pass ENDS after the root that spent it (see walkRecoveryShard), which is what makes the
// single attempt fair: a root that spends the attempt and fails is left behind by the shard's own
// traversal position, so the next visit to that shard offers the attempt to its SIBLING rather
// than to the same failing root again.
func (b *mqttRecoveryBudget) readinessSpent() bool {
	return b.readiness
}

// mqttRecoveryPassRuns counts COMPLETED passes, for the tests that wait on one. Package level and
// atomic because several account managers sweep concurrently and an observer watches across them.
var mqttRecoveryPassRuns atomic.Uint64

// sweepOutboundRecovery walks part of the directory and finishes what it can.
//
// stop is checked between store operations rather than only between roots, so shutdown latency is
// bounded by one operation rather than by a whole sweep — EXCEPT inside the account-wide readiness
// walk, whose granularity is one RECORD-PROCESSING UNIT (see performAffineReadiness), and plus the
// bounded progress bookkeeping a stopping pass still performs.
func (as *mqttAccountSessionManager) sweepOutboundRecovery(s *Server, stop func() bool) {
	defer mqttRecoveryPassRuns.Add(1)

	b := newMqttRecoveryBudget()
	if !b.fitsAUnit() {
		return
	}
	for visited := 0; visited < mqttRecoveryShardsPerPass; visited++ {
		if stop != nil && stop() {
			return
		}
		shard := as.nextRecoveryShard()
		if !b.spend(1) {
			return
		}
		hdr, hdrSeq, herr := as.jsa.loadDirShardAt(shard)
		if herr != nil {
			// AN UNREADABLE HEADER IS NOT AN EMPTY SHARD. It may be the one whose high-water
			// covers a root with outstanding work, so the pass reports it and moves on rather
			// than concluding anything about the generations under it.
			s.Debugf("MQTT recovery pass: directory shard %d could not be read: %v", shard, herr)
			continue
		}
		if hdr == nil {
			// NO HEADER, NO TRAVERSAL, and no generation enrolled here.
			continue
		}
		as.walkRecoveryShard(s, hdr, hdrSeq, b, stop)
		if b.readinessSpent() {
			// The shard walk has already written its traversal position, so ending the pass here
			// costs root throughput for this pass and nothing durable.
			return
		}
		if !b.fitsAUnit() {
			return
		}
	}
}

// nextRecoveryShard returns the shard this pass starts at and advances the resume position.
//
// RETAINED ACROSS PASSES. A per-pass budget with a walk that always starts at shard 0 never
// reaches the later shards at all, which is starvation by construction rather than by load.
func (as *mqttAccountSessionManager) nextRecoveryShard() uint64 {
	as.mu.Lock()
	defer as.mu.Unlock()
	shard := as.recoveryShardCursor % mqttDirShards
	as.recoveryShardCursor = (shard + 1) % mqttDirShards
	return shard
}

// walkRecoveryShard visits this shard's roots from its traversal position, within its cycle.
func (as *mqttAccountSessionManager) walkRecoveryShard(s *Server, hdr *mqttDirShard, hdrSeq uint64,
	b *mqttRecoveryBudget, stop func() bool) {

	pos := hdr.Next
	for visited := 0; visited < mqttRecoveryRootsPerShard && pos <= hdr.Horizon; visited++ {
		if stop != nil && stop() {
			return
		}
		if !b.fitsAUnit() {
			break
		}
		if !b.spend(1) {
			break
		}
		root, rootSeq, rerr := as.jsa.loadRecoveryRoot(hdr.Shard, pos)
		switch {
		case rerr != nil:
			// UNREADABLE IS NOT ABSENT, and it is not a retirement either: this root may be the
			// enrolment of a generation with outstanding duties. The walk advances past it so one
			// unreadable record cannot consume every pass, and the next cycle comes back.
			s.Debugf("MQTT recovery pass: root %d/%d could not be read: %v", hdr.Shard, pos, rerr)
		case root == nil:
			// A HOLE IS AN EMPTY TRAVERSAL SLOT. It says nothing about any generation: Hi never
			// decreases, so a retired root leaves its ordinal behind, and the CERTIFICATE is what
			// claims a generation completed. Nothing is written here and nothing is concluded.
		default:
			as.visitRecoveryRoot(s, root, rootSeq, b, stop)
		}
		pos++
		if b.readinessSpent() {
			// THE READINESS QUANTUM IS THE TRAVERSAL QUANTUM. The attempt is spent BEFORE either
			// helper can succeed, so a root whose proof resolution fails permanently would take it
			// on every pass and a sibling visited after it would never get one — the roots are
			// visited in ordinal order, so that starvation is stable rather than incidental.
			//
			// Ending the walk here, AFTER pos has advanced past this root and BEFORE another root
			// is visited, couples the two: the shard resumes at the sibling, and the failing root
			// comes back on the next cycle. The break falls through to the traversal write below
			// rather than returning, because a position that is never persisted starves exactly
			// the sibling this is here to protect.
			break
		}
	}

	// THE TRAVERSAL POSITION IS WRITTEN BACK AGAINST THE HEADER AS IT NOW STANDS, so a concurrent
	// allocation's high-water is preserved and no walker's progress is rolled back. The wrap is a
	// CAS from the exact cycle this walk belonged to.
	if !b.spendProgress(2) {
		return
	}
	stored, storedSeq, herr := as.jsa.loadDirShardAt(hdr.Shard)
	if herr != nil || stored == nil {
		return
	}
	next, aerr := mqttAdvanceShard(stored, storedSeq, hdr.Cycle, pos)
	if aerr != nil || next == nil {
		return
	}
	if _, perr := as.jsa.persistShard(next, storedSeq); perr != nil {
		// A LOST CAS IS NOT A LOST PASS. Another walker recorded at least as much progress, or a
		// closure allocated beneath this one; either way the next pass re-reads. What was already
		// committed — an authorization, a completed duty, another root's cursor — stands.
		s.Debugf("MQTT recovery pass: shard %d's position was not confirmed: %v", hdr.Shard, perr)
	}
}

// visitRecoveryRoot sweeps one batch of one root, executes what it may, and records progress.
func (as *mqttAccountSessionManager) visitRecoveryRoot(s *Server, root *mqttRecoveryRoot,
	rootSeq uint64, b *mqttRecoveryBudget, stop func() bool) {

	// COUNTED, so that "the pass ends after the root that spends the readiness attempt" is a
	// measurable fact rather than a claim. Whether that coupling holds cannot be read off the
	// starvation it prevents: reproducing the starvation needs two roots whose sweeps roll in
	// lockstep, which no fixture can arrange deterministically.
	if h := mqttTestRecoveryRootVisitHookV.Load(); h != nil {
		(*h)()
	}
	if mqttSweepFinished(root) {
		as.concludeRecoveryRoot(s, root, rootSeq, b)
		return
	}
	// THE SWEEP IS CHARGED PER ENTRY, not per call: one call reads up to 128 of them, and a pass
	// that bought all of them for one unit bounded nothing. The charge also carries the SHUTDOWN
	// check, so the promise that a pass stops within one store operation is enforced inside the
	// traversal rather than only between roots.
	charge := func() bool {
		if stop != nil && stop() {
			return false
		}
		return b.spend(1)
	}
	if !charge() {
		return
	}
	// THE GENERATION'S DISPOSITION, read ONCE per visit (F-105 7d). A discarded generation owes
	// its retirement, and its sweep runs the census over resolved exchange duties; a closure this
	// broker cannot read concludes nothing about either, so the visit withholds rather than
	// sweeping it as a live one.
	if !charge() {
		return
	}
	closure, _, cst := as.jsa.loadGenClosure(root.ID, root.Epoch, root.Gen)
	if cst != mqttOutboundValid && cst != mqttOutboundAbsent {
		s.Debugf("MQTT recovery pass: the closure of %q/%q reads %v, so its disposition is unknown",
			root.ID, root.Gen, cst)
		return
	}
	discarded := cst == mqttOutboundValid && closure.Discard
	swept, out, serr := as.jsa.sweepOneBatchCharged(root, charge, discarded)
	if serr != nil {
		// AN UNRESOLVED READ IS NOT AN ANSWER, and sweepOneBatch stops rather than reporting a
		// partial batch as a whole one. Nothing is advanced.
		s.Debugf("MQTT recovery pass: sweeping %q/%q: %v", root.ID, root.Gen, serr)
		return
	}

	examined := out.Examined
	// The lowest CENSUS entry whose discard work remained when it was classified (F-105 7d). The
	// batch read keeps census entries out of its own accumulator, so this is the only place they
	// enter it — which is why no early exit below may advance past one it did not classify.
	var censusLowest uint64
	for _, d := range swept {
		// EVERY EARLY EXIT TRUNCATES. The cursor is told about the PREFIX this pass actually
		// worked through, so Resume never advances past an ordinal nothing classified — on
		// shutdown exactly as on budget exhaustion.
		if (stop != nil && stop()) || !b.fitsAUnit() {
			if d.Ord > root.Resume {
				examined = d.Ord - root.Resume
			} else {
				examined = 0
			}
			break
		}
		if as.executeRecoveryDuty(s, root, d, b, stop, discarded) && d.Census && censusLowest == 0 {
			censusLowest = d.Ord
		}
	}

	adv := out
	adv.Examined = examined
	if adv.LowestUnresolved != 0 && adv.LowestUnresolved >= root.Resume+examined {
		// An unresolved ordinal this pass did not reach is not this sweep's to record.
		adv.LowestUnresolved = 0
	}
	if censusLowest != 0 && (adv.LowestUnresolved == 0 || censusLowest < adv.LowestUnresolved) {
		adv.LowestUnresolved = censusLowest
	}
	next, aerr := mqttAdvanceSweep(root, adv)
	if aerr != nil || next == nil {
		return
	}
	if !b.spendProgress(1) {
		return
	}
	if _, perr := as.jsa.persistSweep(next, rootSeq); perr != nil {
		// The cursor is the one update this pass could not confirm. Everything it committed
		// before this stands, and the next visit re-derives from the records themselves.
		s.Debugf("MQTT recovery pass: the cursor of %q/%q was not confirmed: %v", root.ID, root.Gen, perr)
	}
}

// mqttTestRecoveryRootVisitHookV fires once per root the pass visits.
//
// Package level for the same reason the readiness seam is: the walk is reached through helpers
// that hold no test state.
var mqttTestRecoveryRootVisitHookV atomic.Pointer[func()]

func setMQTTTestRecoveryRootVisitHook(h func()) func() {
	prev := mqttTestRecoveryRootVisitHookV.Load()
	if h == nil {
		mqttTestRecoveryRootVisitHookV.Store(nil)
	} else {
		mqttTestRecoveryRootVisitHookV.Store(&h)
	}
	return func() { mqttTestRecoveryRootVisitHookV.Store(prev) }
}
