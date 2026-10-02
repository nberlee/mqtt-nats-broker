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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The PUBCOMP completion worker (pubcomp_completion.go): a PUBCOMP whose completion cannot be
// confirmed withholds the canonical PUBREL's ack, and the worker — which owns its record alone —
// records the completion, sends what it carried, and owns the cleanup until it settles.

// testPubCompWorld is a real-store env with one seeded exchange delivered once, so a PUBCOMP
// through the handler has a delivered entry to answer.
type testPubCompWorld struct {
	e    *testInflightEnv
	seed testRealPubRelSeed
	obs  *testPubCompObs
	// refuse, while true, refuses THIS exchange's PUBCOMP batch without writing.
	refuse atomic.Bool
	// allowOnce lets exactly one PUBCOMP batch through while refuse holds.
	allowOnce atomic.Bool
	refusals  atomic.Int32
}

// testPubCompObs records every canonical-PUBREL ack the PUBCOMP path sends and every worker
// enrolment and attempt.
type testPubCompObs struct {
	mu       sync.Mutex
	acks     []string
	byWorker []string
	enrolled int
	created  int
	attempts []bool // member, per attempt
	byRecord map[*mqttPubCompOwed][]bool
}

func (o *testPubCompObs) snapshot() (acks, byWorker []string, enrolled, created int, attempts []bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.acks...), append([]string(nil), o.byWorker...), o.enrolled, o.created,
		append([]bool(nil), o.attempts...)
}

func testNewPubCompWorld(t *testing.T, clientID string) *testPubCompWorld {
	t.Helper()
	w := &testPubCompWorld{obs: &testPubCompObs{}}
	w.e = testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, w.e)
	w.seed = testSeedRealPubRelReady(t, w.e)
	testDeliverPubRelAs(t, w.e, w.seed.pi, w.seed.reply(1), w.seed.nonce)
	asm := w.e.asm
	asm.mu.Lock()
	asm.testPubCompAck = func(subject string, byWorker bool) {
		w.obs.mu.Lock()
		w.obs.acks = append(w.obs.acks, subject)
		if byWorker {
			w.obs.byWorker = append(w.obs.byWorker, subject)
		}
		w.obs.mu.Unlock()
	}
	asm.testPubCompEnrolled = func(_ uint16, created bool) {
		w.obs.mu.Lock()
		w.obs.enrolled++
		if created {
			w.obs.created++
		}
		w.obs.mu.Unlock()
	}
	asm.testPubCompAttempt = func(rec *mqttPubCompOwed, member bool) {
		w.obs.mu.Lock()
		w.obs.attempts = append(w.obs.attempts, member)
		if w.obs.byRecord == nil {
			w.obs.byRecord = make(map[*mqttPubCompOwed][]bool)
		}
		w.obs.byRecord[rec] = append(w.obs.byRecord[rec], member)
		w.obs.mu.Unlock()
	}
	asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if !testF105IsPubCompAdvance(req, w.seed.lin) || !w.refuse.Load() || w.allowOnce.CompareAndSwap(true, false) {
			return write()
		}
		w.refusals.Add(1)
		return mqttOpOutcome{}, errors.New("test: the PUBCOMP completion is refused")
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testPubCompAck, asm.testPubCompEnrolled, asm.testPubCompAttempt, asm.testAckAdvance = nil, nil, nil, nil
		asm.mu.Unlock()
	})
	return w
}

// owedRecord is the worker record for the seeded exchange, if any.
func (w *testPubCompWorld) owedRecord() *mqttPubCompOwed {
	w.e.sess.mu.Lock()
	defer w.e.sess.mu.Unlock()
	for k, rec := range w.e.sess.pubCompOwed {
		if k.pi == w.seed.pi {
			return rec
		}
	}
	return nil
}

func (w *testPubCompWorld) owedCount() int {
	w.e.sess.mu.Lock()
	defer w.e.sess.mu.Unlock()
	return len(w.e.sess.pubCompOwed)
}

// recordPresent reports whether the canonical PUBREL still stands on $MQTT_out.
func (w *testPubCompWorld) recordPresent(t *testing.T) bool {
	t.Helper()
	smsg, err := w.e.asm.jsa.loadMsg(mqttOutStreamName, w.seed.canon)
	if err != nil {
		if IsNatsErr(err, JSNoMessageFoundErr) {
			return false
		}
		t.Fatalf("reading the canonical PUBREL: %v", err)
	}
	return smsg != nil
}

func (w *testPubCompWorld) pending() (entry *mqttPending, capacity int) {
	w.e.sess.mu.Lock()
	defer w.e.sess.mu.Unlock()
	if p := w.e.sess.pendingPubRel[w.seed.pi]; p != nil {
		cp := *p
		entry = &cp
	}
	return entry, w.e.sess.receiveMaxOccupancyLocked()
}

// T5. An unconfirmed completion withholds the ack, and its worker survives a redelivered
// PUBREL replacing the terminal placeholder.
//
// Mutation M5: ack unconditionally again -> an ack is observed before the completion is
// recorded. M6: the worker does not send what it carries -> no worker ack.
func TestF105UnrecordedPubCompWithholdsTheAckAndSurvivesARedelivery(t *testing.T) {
	// Deliberately NOT t.Parallel(): testCountPubRelWrites counts at a process-global write
	// hook, so a parallel sibling's PUBREL would be counted here
	// (TestParallelTestsTouchNoProcessGlobalState).
	w := testNewPubCompWorld(t, "f105-worker-redelivery")
	w.refuse.Store(true)

	w.e.c.mqttProcessPubComp(w.seed.pi)

	if w.refusals.Load() == 0 {
		t.Fatal("the PUBCOMP batch was never attempted, so nothing here is under test")
	}
	if acks, _, _, created, _ := w.obs.snapshot(); len(acks) != 0 || created != 1 {
		t.Fatalf("an unconfirmed completion sent %d acks and created %d worker records, want 0 and 1", len(acks), created)
	}
	if !w.recordPresent(t) {
		t.Fatal("the canonical PUBREL was removed although the completion was never recorded")
	}
	rec := w.owedRecord()
	if rec == nil || len(rec.acks) != 1 || rec.acks[0] != w.seed.reply(1) {
		t.Fatalf("the worker record does not carry the withheld ack: %+v", rec)
	}
	if entry, _ := w.pending(); entry == nil || entry.jsAckSubject != _EMPTY_ {
		t.Fatalf("the identifier is not reserved by the terminal placeholder: %+v", entry)
	}

	// THE REDELIVERY: AckWait re-offers the unacked PUBREL. The slot is still pubrel_ready with
	// no action, so admission re-sends it and trackAsPubRel replaces the placeholder.
	writes := testCountPubRelWrites(t, w.e, func() {
		testDeliverPubRelAs(t, w.e, w.seed.pi, w.seed.reply(2), w.seed.nonce)
	})
	if writes != 1 {
		t.Fatalf("the redelivered PUBREL was transmitted %d time(s), want 1", writes)
	}
	if entry, _ := w.pending(); entry == nil || entry.jsAckSubject != w.seed.reply(2) {
		t.Fatalf("the redelivery did not install its delivered entry: %+v", entry)
	}
	if again := w.owedRecord(); again != rec || len(again.acks) != 1 {
		t.Fatalf("the redelivery disturbed the worker's own record: %+v (was %+v)", again, rec)
	}

	// The store recovers: the worker records the completion and sends what it carried.
	w.refuse.Store(false)
	checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if _, byWorker, _, _, _ := w.obs.snapshot(); len(byWorker) != 1 || byWorker[0] != w.seed.reply(1) {
			return fmt.Errorf("the worker has sent %v, want exactly the withheld ack", byWorker)
		}
		return nil
	})
	if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminal {
		t.Fatalf("the worker's completion left the exchange at phase %d", got)
	}
	if w.owedCount() != 0 {
		t.Fatal("the worker did not retire its record once it had sent its acks")
	}

	// The client answers the re-sent PUBREL; that PUBCOMP completes its own delivery.
	w.e.c.mqttProcessPubComp(w.seed.pi)
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if slot := testRealSlot(t, w.e, w.seed); slot != nil {
			return fmt.Errorf("the slot still stands: %+v", slot)
		}
		if w.recordPresent(t) {
			return fmt.Errorf("the canonical PUBREL still stands")
		}
		if entry, capacity := w.pending(); entry != nil || capacity != 0 {
			return fmt.Errorf("pending entry %+v and capacity %d remain", entry, capacity)
		}
		return nil
	})
	if acks, byWorker, _, _, _ := w.obs.snapshot(); len(acks) != 2 || len(byWorker) != 1 {
		t.Fatalf("acks sent %v (worker %v), want the worker's carried one and the redelivery's own", acks, byWorker)
	}
}

// T5d. A recorded completion whose cleanup cannot finish yet stays OWNED: the worker keeps
// retrying after its record leaves the map, and with a redelivered PUBREL delivered in between it
// never deletes under its stale transmission binding — the redelivery's acquisition advanced Acq,
// and a reference captured before an acquisition may not release after it (the slot-ref
// contract). The redelivered entry's own PUBCOMP, which carries the current binding, finishes it.
//
// Mutation M14: the worker hands recorded-but-indeterminate cleanup to retainAndReconcileTerminal
// -> that refuses over the delivered entry, so no detached attempt ever follows.
func TestF105WorkerKeepsTheCleanupOverADeliveredEntry(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-worker-cleanup")
	w.refuse.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	testDeliverPubRelAs(t, w.e, w.seed.pi, w.seed.reply(2), w.seed.nonce)
	if entry, _ := w.pending(); entry == nil || entry.jsAckSubject == _EMPTY_ {
		t.Fatalf("the fixture needs a delivered entry standing: %+v", entry)
	}
	w.refuse.Store(false)

	// The worker records the completion and keeps its cleanup: detached attempts continue.
	checkFor(t, 4*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		_, _, _, _, attempts := w.obs.snapshot()
		detached := 0
		for _, member := range attempts {
			if !member {
				detached++
			}
		}
		if detached < 1 {
			return fmt.Errorf("no attempt after the worker detached: %v", attempts)
		}
		return nil
	})
	if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminal {
		t.Fatalf("the worker's completion left the exchange at phase %d", got)
	}
	// It never deleted under the binding the redelivery superseded.
	if slot := testRealSlot(t, w.e, w.seed); slot == nil || slot.State != mqttInflightPubRelTerminal {
		t.Fatalf("the slot must stand terminal until the current transmission's PUBCOMP: %+v", slot)
	}
	if entry, _ := w.pending(); entry == nil || entry.jsAckSubject != w.seed.reply(2) {
		t.Fatalf("the worker touched the delivered entry, which is its own PUBCOMP's: %+v", entry)
	}

	// The redelivered PUBREL's PUBCOMP finishes the cleanup, and the worker then retires.
	w.e.c.mqttProcessPubComp(w.seed.pi)
	checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if slot := testRealSlot(t, w.e, w.seed); slot != nil {
			return fmt.Errorf("the slot still stands at %q", slot.State)
		}
		if w.owedCount() != 0 {
			return fmt.Errorf("the worker record remains")
		}
		if entry, capacity := w.pending(); entry != nil || capacity != 0 {
			return fmt.Errorf("pending entry %+v and capacity %d remain", entry, capacity)
		}
		return nil
	})
}

// T5e. An install racing the worker's detach is never lost: an ack installed after the detach
// starts a NEW record whose worker sends it.
//
// Mutation M15: the detach splits the snapshot from the removal -> the ack installed in the gap
// joins a record the worker then removes, and it is never sent.
func TestF105WorkerDetachNeverLosesAnInstalledAck(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-worker-detach")
	w.refuse.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	rec := w.owedRecord()
	if rec == nil {
		t.Fatal("no worker record")
	}
	ref := rec.ref
	const second = "$JS.ACK.test.second.install"
	var installed atomic.Bool
	var installCreated atomic.Bool
	asm := w.e.asm
	asm.mu.Lock()
	asm.testPubCompDetached = func(uint16) {
		if !installed.CompareAndSwap(false, true) {
			return
		}
		before := w.obs.created
		w.e.sess.enrolPubCompOwed(w.e.c, ref, second)
		w.obs.mu.Lock()
		installCreated.Store(w.obs.created > before)
		w.obs.mu.Unlock()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testPubCompDetached = nil
		asm.mu.Unlock()
	})
	w.refuse.Store(false)

	checkFor(t, 4*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		_, byWorker, _, _, _ := w.obs.snapshot()
		if len(byWorker) != 2 {
			return fmt.Errorf("worker acks sent: %v", byWorker)
		}
		return nil
	})
	if !installed.Load() {
		t.Fatal("the detach seam never ran")
	}
	if !installCreated.Load() {
		t.Fatal("the install after the detach joined an existing record: the detach did not remove it " +
			"in the same critical section it took the acks in")
	}
	_, byWorker, _, _, _ := w.obs.snapshot()
	if byWorker[0] != w.seed.reply(1) || byWorker[1] != second {
		t.Fatalf("worker acks %v, want the original then the second install, each once", byWorker)
	}
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if n := w.owedCount(); n != 0 {
			return fmt.Errorf("%d worker records remain", n)
		}
		return nil
	})
}

// T5f. A generation change retires the worker: it sends nothing and frees only its placeholder.
func TestF105WorkerRetiresOnAGenerationChange(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-worker-generation")
	w.refuse.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	if w.owedRecord() == nil {
		t.Fatal("no worker record")
	}
	w.e.sess.mu.Lock()
	w.e.sess.gen = mqttNewSessGen()
	w.e.sess.mu.Unlock()

	checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if w.owedCount() != 0 {
			return fmt.Errorf("the worker record survives the generation change")
		}
		return nil
	})
	if acks, _, _, _, _ := w.obs.snapshot(); len(acks) != 0 {
		t.Fatalf("a retired generation's worker sent acks %v", acks)
	}
	if entry, _ := w.pending(); entry != nil {
		t.Fatalf("the worker's placeholder survived its retirement: %+v", entry)
	}
}

// T5g(a). A closed worker gate retires the worker with its acks unsent; the canonical PUBREL
// stays unacked for the resume.
func TestF105WorkerGateRefusalLeavesThePubRelUnacked(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-worker-gate")
	w.refuse.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	if w.owedRecord() == nil {
		t.Fatal("no worker record")
	}
	w.e.s.willDispatchMu.Lock()
	w.e.s.willDispatchClosed = true
	w.e.s.willDispatchMu.Unlock()

	checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if w.owedCount() != 0 {
			return fmt.Errorf("the worker record survives the closed gate")
		}
		return nil
	})
	if acks, _, _, _, _ := w.obs.snapshot(); len(acks) != 0 {
		t.Fatalf("a gate-refused worker sent acks %v", acks)
	}
	if !w.recordPresent(t) {
		t.Fatal("the canonical PUBREL is gone although no completion was recorded")
	}
}

// T5h. The redelivered entry's own PUBCOMP completes the exchange first; the worker then finds it
// recorded, sends what it carried, and removes only its own record.
func TestF105RedeliveredPubCompCompletesBeforeTheWorker(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-worker-second-first")
	w.refuse.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	testDeliverPubRelAs(t, w.e, w.seed.pi, w.seed.reply(2), w.seed.nonce)

	// The client's second PUBCOMP gets its batch through; the worker's are still refused.
	w.allowOnce.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	if w.allowOnce.Load() {
		t.Fatal("the second PUBCOMP never reached its batch")
	}
	if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminal {
		t.Fatalf("the second PUBCOMP left the exchange at phase %d", got)
	}
	if acks, byWorker, _, _, _ := w.obs.snapshot(); len(acks) != 1 || len(byWorker) != 0 || acks[0] != w.seed.reply(2) {
		t.Fatalf("after the inline completion acks are %v (worker %v), want only the second delivery's", acks, byWorker)
	}

	checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if _, byWorker, _, _, _ := w.obs.snapshot(); len(byWorker) != 1 || byWorker[0] != w.seed.reply(1) {
			return fmt.Errorf("the worker sent %v", byWorker)
		}
		if w.owedCount() != 0 {
			return fmt.Errorf("the worker record remains")
		}
		return nil
	})
	if entry, capacity := w.pending(); entry != nil || capacity != 0 {
		t.Fatalf("pending entry %+v and capacity %d remain", entry, capacity)
	}
}

// T5i. A detached worker whose cleanup is still unsettled keeps retrying after it has left the
// map, and never touches the newer record that took its key.
//
// Mutation M19: the worker stops when its record is no longer the map's -> no attempt is observed
// with member=false, and with the newer worker's cleanup held too the slot survives.
func TestF105DetachedWorkerKeepsItsCleanupOwnership(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-worker-overlap")
	w.refuse.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	rec := w.owedRecord()
	if rec == nil {
		t.Fatal("no worker record")
	}
	ref := rec.ref
	// Hold every finalize indeterminate until released.
	var hold atomic.Bool
	hold.Store(true)
	asm := w.e.asm
	asm.mu.Lock()
	asm.testInflightPubRelScan = func(subject string) ([]mqttStoredPubRel, int, bool) {
		if hold.Load() {
			return nil, 0, false
		}
		asm.mu.Lock()
		asm.testInflightPubRelScan = nil
		asm.mu.Unlock()
		return w.e.sess.scanPubRelRecords(subject)
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testInflightPubRelScan = nil
		asm.mu.Unlock()
	})
	w.refuse.Store(false)

	// W1 records and detaches; its finalize is held.
	checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if w.owedCount() != 0 {
			return fmt.Errorf("W1 has not detached")
		}
		return nil
	})
	// A second withheld PUBCOMP starts R2/W2 under the same key.
	w.e.sess.enrolPubCompOwed(w.e.c, ref, "$JS.ACK.test.overlap.second")
	r2 := w.owedRecord()
	if r2 == nil || r2 == rec {
		t.Fatalf("the second install did not create a new record: %+v", r2)
	}
	// W1 ITSELF keeps retrying while it is no longer the map's record: at least two attempts
	// after its detach (one could be a worker that stopped right after looking).
	checkFor(t, 5*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		w.obs.mu.Lock()
		w1 := append([]bool(nil), w.obs.byRecord[rec]...)
		w.obs.mu.Unlock()
		detached := 0
		for _, member := range w1 {
			if !member {
				detached++
			}
		}
		if detached < 2 {
			return fmt.Errorf("W1's attempts %v: fewer than two after its detach", w1)
		}
		return nil
	})
	// R2 is removed only by ITS OWN worker, which detaches it once it records the completion and
	// sends the ack R2 carried; a removal without that send is the detached worker's doing.
	if got := w.owedRecord(); got != r2 {
		_, byWorker, _, _, _ := w.obs.snapshot()
		sent := false
		for _, a := range byWorker {
			sent = sent || a == "$JS.ACK.test.overlap.second"
		}
		if !sent {
			t.Fatalf("the newer record was removed without its own worker sending its ack: %+v", got)
		}
	}
	hold.Store(false)
	checkFor(t, 4*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if slot := testRealSlot(t, w.e, w.seed); slot != nil {
			return fmt.Errorf("the slot still stands at %q", slot.State)
		}
		if n := w.owedCount(); n != 0 {
			return fmt.Errorf("%d worker records remain", n)
		}
		return nil
	})
}

// T5b. A stale continuation meeting a successor on the identifier writes nothing, reports the
// completion settled for ITS exchange, and leaves the successor's identifier excluded.
func TestF105WorkerLeavesASuccessorOccupantUntouched(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-worker-successor")
	w.refuse.Store(true)
	w.e.c.mqttProcessPubComp(w.seed.pi)
	if w.owedRecord() == nil {
		t.Fatal("no worker record")
	}
	// A SUCCESSOR takes the identifier's subject: another exchange's slot (a fresh nonce and
	// lineage), written at the standing sequence.
	cur, smsg, err := w.e.asm.loadInflightRecord(w.seed.subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the slot: %v", err)
	}
	succ := *cur
	succ.Nonce, succ.Lin = mqttNewInflightNonce(), mqttNewInflightNonce()
	succ.State, succ.PubRelDurable, succ.PubRelStreamSeq = mqttInflightPublish, _EMPTY_, 0
	succ.Seq = 77
	testRegisterSourceReply(&succ)
	if _, err := w.e.asm.jsa.storeInflightCAS(w.seed.subject, smsg.Sequence, &succ); err != nil {
		t.Fatalf("installing the successor: %v", err)
	}
	w.refuse.Store(false)
	checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
		if w.owedCount() != 0 {
			return fmt.Errorf("the worker record remains")
		}
		return nil
	})
	after, _, _ := w.e.asm.loadInflightRecord(w.seed.subject)
	if after == nil || after.Nonce != succ.Nonce || after.State != mqttInflightPublish || after.Seq != 77 {
		t.Fatalf("the successor was rewritten: %+v", after)
	}
	w.e.sess.mu.Lock()
	excluded := w.e.sess.slotExclusionInstLocked(w.seed.pi) != 0
	w.e.sess.mu.Unlock()
	if !excluded {
		t.Fatal("the successor's identifier is not excluded after the stale continuation settled")
	}
}

// testPendingDrainWorld seeds the world one step further: a live PUBREL action, and a PUBCOMP
// recorded pending-drain over it.
func testPendingDrainWorld(t *testing.T, clientID string) (*testPubCompWorld, *mqttSendAction) {
	t.Helper()
	w := &testPubCompWorld{obs: &testPubCompObs{}}
	w.e = testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, w.e)
	w.seed = testSeedRealPubRelReady(t, w.e)
	ref := testF105SlotRefAt(t, w.e.sess, w.seed.pi, mqttInflightPubRelReady, false)
	act := w.e.sess.testAcquirePubRel(w.e.c, ref)
	if act == nil {
		t.Fatal("the PUBREL action was not acquired")
	}
	live := testF105SlotRefAt(t, w.e.sess, w.seed.pi, mqttInflightPubRelReady, true)
	if res := w.e.sess.mqttTerminalizeOnPubComp(w.e.c, live); !res.recorded || res.outcome != mqttTerminalHeldByAction {
		t.Fatalf("PUBCOMP over a live action concluded %+v", res)
	}
	if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminalPendingDrain {
		t.Fatalf("the exchange stands at phase %d, want terminal-pending-drain", got)
	}
	return w, act
}

// T6. The PUBREL drain's paired completion, landed behind a lost response or refused outright,
// still ends the exchange and finishes the slot — without a restart or another PUBCOMP.
//
// Mutation M18: the landed-lost branch takes the bare early return -> the slot survives with its
// identifier excluded.
func TestF105PubRelDrainCompletionSurvivesALostOrRefusedBatch(t *testing.T) {
	t.Run("landed, response lost", func(t *testing.T) {
		t.Parallel()
		w, act := testPendingDrainWorld(t, "f105-drain-lost")
		var calls atomic.Int32
		asm := w.e.asm
		asm.mu.Lock()
		asm.testDrainComplete = func(req *mqttCompleteExchangeDrainRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if calls.Add(1) == 1 {
				if _, err := write(); err != nil {
					t.Errorf("the batch behind the lost response failed: %v", err)
				}
				return mqttOpOutcome{}, errors.New("test: response lost")
			}
			return write()
		}
		asm.mu.Unlock()
		t.Cleanup(func() { asm.mu.Lock(); asm.testDrainComplete = nil; asm.mu.Unlock() })

		start := time.Now()
		w.e.sess.startActionDrain(w.e.c, act)
		if calls.Load() != 1 {
			t.Fatalf("the drain made %d completion attempts, want exactly the one that landed", calls.Load())
		}
		if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminal {
			t.Fatalf("the exchange stands at phase %d, want Terminal", got)
		}
		testF105SlotGone(t, w.e.sess, w.seed.pi, mqttActionReconcileDelay/2)
		if time.Since(start) >= mqttActionReconcileDelay {
			t.Fatal("the cleanup waited for a timer rather than running inline")
		}
		w.e.sess.mu.Lock()
		excluded := w.e.sess.slotExclusionInstLocked(w.seed.pi) != 0
		w.e.sess.mu.Unlock()
		if excluded {
			t.Fatal("the identifier is still excluded after the drain completed the exchange")
		}
	})
	t.Run("refused, then reconciled", func(t *testing.T) {
		t.Parallel()
		w, act := testPendingDrainWorld(t, "f105-drain-refused")
		var refuse atomic.Bool
		refuse.Store(true)
		var refusals atomic.Int32
		asm := w.e.asm
		asm.mu.Lock()
		asm.testDrainComplete = func(req *mqttCompleteExchangeDrainRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if refuse.Load() {
				refusals.Add(1)
				return mqttOpOutcome{}, errors.New("test: refused")
			}
			return write()
		}
		asm.mu.Unlock()
		t.Cleanup(func() { asm.mu.Lock(); asm.testDrainComplete = nil; asm.mu.Unlock() })

		w.e.sess.startActionDrain(w.e.c, act)
		if refusals.Load() < int32(mqttInflightStoreAttempts) {
			t.Fatalf("the drain gave up after %d refusals, want its full budget", refusals.Load())
		}
		if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminalPendingDrain {
			t.Fatalf("a refused drain moved the exchange to phase %d", got)
		}
		refuse.Store(false)
		checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
			if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminal {
				return fmt.Errorf("phase %d", got)
			}
			if slot := testRealSlot(t, w.e, w.seed); slot != nil {
				return fmt.Errorf("the slot stands at %q", slot.State)
			}
			return nil
		})
	})
}

// T9. The write boundary admits exactly the pairings the new operations write, and refuses each
// otherwise-valid composition that breaks one rule — asserted through the complete commit path.
//
// Mutation M3: the token/phase agreement check deleted -> the live-token Terminal composition is
// accepted. M11: the stored-predecessor check deleted -> the two wrong-predecessor compositions
// are accepted. M13: pubrel_terminal removed from mqttAdvanceKindSlotState -> the accepted
// compositions are refused.
func TestF105BoundaryAdmitsOnlyThePubCompAndDrainPairings(t *testing.T) {
	type state struct {
		w    *testPubCompWorld
		auth *mqttOutboundAuthority
		aseq uint64
		x    *mqttOutboundExchange
		xseq uint64
		cur  *mqttInflightRecord
		cseq uint64
	}
	read := func(t *testing.T, w *testPubCompWorld) state {
		t.Helper()
		st := state{w: w}
		al := w.e.asm.jsa.loadOutboundAuthority(w.e.sess.id, w.e.asm.sessStreamEpoch)
		st.auth, st.aseq = al.rec, al.seq
		w.e.sess.mu.Lock()
		gen := w.e.sess.gen
		w.e.sess.mu.Unlock()
		x, xseq, err := w.e.asm.jsa.loadOutboundExchange(w.e.sess.id, w.e.asm.sessStreamEpoch, gen, w.seed.lin)
		if err != nil {
			t.Fatalf("reading the exchange: %v", err)
		}
		st.x, st.xseq = x, xseq
		cur, smsg, lerr := w.e.asm.loadInflightRecord(w.seed.subject)
		if lerr != nil || cur == nil || smsg == nil {
			t.Fatalf("reading the slot: %v", lerr)
		}
		st.cur, st.cseq = cur, smsg.Sequence
		return st
	}
	ackReq := func(st state, to mqttExchangePhase) *mqttAdvanceExchangeAckRequest {
		return &mqttAdvanceExchangeAckRequest{
			ID: st.w.e.sess.id, Epoch: st.w.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-t9",
			Gen: st.x.Gen, Nonce: st.x.Nonce, To: to, Acq: st.cur.Acq, Register: st.cur, RegisterSeq: st.cseq,
		}
	}
	// retamper rewrites one member's JSON body.
	retamper := func(t *testing.T, tr *mqttAuthorityTransition, exchange bool, edit func(m map[string]any)) {
		t.Helper()
		for i := range tr.Extra {
			isX := false
			if cls, ok := mqttF105ObligationClass(tr.Extra[i].subj); ok && cls == mqttExchangeClassTk {
				isX = true
			}
			if isX != exchange {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal(tr.Extra[i].body, &m); err != nil {
				t.Fatal(err)
			}
			edit(m)
			b, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			tr.Extra[i].body = b
			return
		}
		t.Fatal("no member to tamper")
	}
	// commit adds the hold releases the composed ending owes (F-105 7c-lc: an ending releases its
	// holds in its own batch, through the production helper), then submits.
	commit := func(st state, tr *mqttAuthorityTransition) error {
		for _, m := range tr.Extra {
			if cls, ok := mqttF105ObligationClass(m.subj); ok && cls == mqttExchangeClassTk {
				var nx mqttOutboundExchange
				if json.Unmarshal(m.body, &nx) == nil {
					if err := st.w.e.asm.jsa.appendHoldReleases(tr, st.x, nx.Phase); err != nil {
						return err
					}
				}
				break
			}
		}
		_, err := st.w.e.asm.jsa.commitAuthorityTransition(tr)
		return err
	}
	mustRefuse := func(t *testing.T, err error, why string) {
		t.Helper()
		if err == nil {
			t.Fatalf("the composition was ACCEPTED; want it refused by %s", why)
		}
		if !strings.Contains(err.Error(), why) {
			t.Fatalf("refused for another reason: %v (want %q)", err, why)
		}
	}

	t.Run("accepted: PUBCOMP with no action", func(t *testing.T) {
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t9-acc1")
		st := read(t, w)
		tr, err := mqttAdvanceExchangeAckTransition(st.auth, st.aseq, ackReq(st, mqttExchangeTerminal), st.x, st.xseq)
		if err != nil {
			t.Fatal(err)
		}
		if err := commit(st, tr); err != nil {
			t.Fatalf("the PUBCOMP pairing was refused: %v", err)
		}
	})
	t.Run("accepted: PUBCOMP over a live action, then its drain", func(t *testing.T) {
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t9-acc2")
		st0 := read(t, w)
		ref := mqttSlotRefFromRecord(w.seed.subject, st0.cseq, st0.cur)
		act := w.e.sess.testAcquirePubRel(w.e.c, *ref)
		if act == nil {
			t.Fatal("no action")
		}
		st := read(t, w)
		tr, err := mqttAdvanceExchangeAckTransition(st.auth, st.aseq, ackReq(st, mqttExchangeTerminalPendingDrain), st.x, st.xseq)
		if err != nil {
			t.Fatal(err)
		}
		if err := commit(st, tr); err != nil {
			t.Fatalf("the pending-drain PUBCOMP pairing was refused: %v", err)
		}
		st = read(t, w)
		dr, err := mqttCompleteExchangeDrainTransition(st.auth, st.aseq, &mqttCompleteExchangeDrainRequest{
			ID: w.e.sess.id, Epoch: w.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-t9-drain",
			Gen: st.x.Gen, Nonce: st.x.Nonce, Token: act.token, Register: st.cur, RegisterSeq: st.cseq,
		}, st.x, st.xseq)
		if err != nil {
			t.Fatal(err)
		}
		if err := commit(st, dr); err != nil {
			t.Fatalf("the drain-completion pairing was refused: %v", err)
		}
	})
	t.Run("refused: Terminal over a live action", func(t *testing.T) {
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t9-r1")
		st0 := read(t, w)
		if w.e.sess.testAcquirePubRel(w.e.c, *mqttSlotRefFromRecord(w.seed.subject, st0.cseq, st0.cur)) == nil {
			t.Fatal("no action")
		}
		st := read(t, w)
		tr, err := mqttAdvanceExchangeAckTransition(st.auth, st.aseq, ackReq(st, mqttExchangeTerminalPendingDrain), st.x, st.xseq)
		if err != nil {
			t.Fatal(err)
		}
		retamper(t, tr, true, func(m map[string]any) { m["phase"] = float64(mqttExchangeTerminal) })
		mustRefuse(t, commit(st, tr), "live=true")
	})
	t.Run("refused: pending drain with no action", func(t *testing.T) {
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t9-r2")
		st := read(t, w)
		tr, err := mqttAdvanceExchangeAckTransition(st.auth, st.aseq, ackReq(st, mqttExchangeTerminal), st.x, st.xseq)
		if err != nil {
			t.Fatal(err)
		}
		retamper(t, tr, true, func(m map[string]any) { m["phase"] = float64(mqttExchangeTerminalPendingDrain) })
		mustRefuse(t, commit(st, tr), "live=false")
	})
	t.Run("refused: a PUBCOMP re-phase beside the drain's advance", func(t *testing.T) {
		t.Parallel()
		w, _ := testPendingDrainWorld(t, "f105-t9-r3")
		// Put the slot back at pubrel_ready (token kept): an otherwise-valid PUBCOMP member
		// beside a legal pending-drain -> terminal exchange advance.
		st0 := read(t, w)
		back := *st0.cur
		back.State = mqttInflightPubRelReady
		if _, err := w.e.asm.jsa.storeInflightCAS(w.seed.subject, st0.cseq, &back); err != nil {
			t.Fatal(err)
		}
		st := read(t, w)
		tr, err := mqttAdvanceExchangeTransition(st.auth, st.aseq, mqttNewInflightNonce(), "op-t9-r3", st.x, st.xseq, mqttExchangeTerminal)
		if err != nil {
			t.Fatal(err)
		}
		next := *st.cur
		next.State = mqttInflightPubRelTerminal
		body, _ := json.Marshal(&next)
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: w.seed.subject, expect: st.cseq, body: body})
		mustRefuse(t, commit(st, tr), "beside an exchange standing at phase")
	})
	t.Run("refused: a token clearance beside a PUBCOMP advance", func(t *testing.T) {
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t9-r4")
		st0 := read(t, w)
		act := w.e.sess.testAcquirePubRel(w.e.c, *mqttSlotRefFromRecord(w.seed.subject, st0.cseq, st0.cur))
		if act == nil {
			t.Fatal("no action")
		}
		st1 := read(t, w)
		term := *st1.cur
		term.State = mqttInflightPubRelTerminal // the slot terminal with its token; the exchange still PUBREC-observed
		if _, err := w.e.asm.jsa.storeInflightCAS(w.seed.subject, st1.cseq, &term); err != nil {
			t.Fatal(err)
		}
		st := read(t, w)
		tr, err := mqttAdvanceExchangeTransition(st.auth, st.aseq, mqttNewInflightNonce(), "op-t9-r4", st.x, st.xseq, mqttExchangeTerminal)
		if err != nil {
			t.Fatal(err)
		}
		clear := *st.cur
		clear.ActionToken, clear.ActionClaim, clear.ActionClaimSeq = _EMPTY_, _EMPTY_, 0
		body, _ := json.Marshal(&clear)
		tr.Extra = append(tr.Extra, mqttTransitionMember{subj: w.seed.subject, expect: st.cseq, body: body})
		mustRefuse(t, commit(st, tr), "a token clearance completes only")
	})
	t.Run("refused: a clearance that also moves Acq", func(t *testing.T) {
		t.Parallel()
		w, act := testPendingDrainWorld(t, "f105-t9-r5")
		st := read(t, w)
		dr, err := mqttCompleteExchangeDrainTransition(st.auth, st.aseq, &mqttCompleteExchangeDrainRequest{
			ID: w.e.sess.id, Epoch: w.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-t9-r5",
			Gen: st.x.Gen, Nonce: st.x.Nonce, Token: act.token, Register: st.cur, RegisterSeq: st.cseq,
		}, st.x, st.xseq)
		if err != nil {
			t.Fatal(err)
		}
		retamper(t, dr, false, func(m map[string]any) { m["acq"] = m["acq"].(float64) + 1 })
		err = commit(st, dr)
		if err == nil {
			t.Fatal("a clearance that moves Acq was accepted")
		}
		if strings.Contains(err.Error(), "clears the PUBREL token") {
			t.Fatalf("refused by the ack checker, but the recogniser should have refused it first: %v", err)
		}
	})
	t.Run("refused: a stale exchange expectation, at its own gate", func(t *testing.T) {
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t9-r6")
		st := read(t, w)
		tr, err := mqttAdvanceExchangeAckTransition(st.auth, st.aseq, ackReq(st, mqttExchangeTerminal), st.x, st.xseq)
		if err != nil {
			t.Fatal(err)
		}
		for i := range tr.Extra {
			if cls, ok := mqttF105ObligationClass(tr.Extra[i].subj); ok && cls == mqttExchangeClassTk {
				tr.Extra[i].expect--
			}
		}
		mustRefuse(t, commit(st, tr), "but it stands at")
	})
}

// deliver hands the canonical PUBREL's n-th delivery to the callback, correlated to the seeded
// exchange's own source.
func (s testRealPubRelSeed) deliver(t *testing.T, e *testInflightEnv, n int) {
	t.Helper()
	testDeliverPubRelCorrelated(t, e, s.pi, s.reply(n), s.nonce, testRealPubRelSrcDur, s.sseq)
}

// testRealExchangePhase reads the seeded exchange's phase.
func testRealExchangePhase(t *testing.T, e *testInflightEnv, s testRealPubRelSeed) mqttExchangePhase {
	t.Helper()
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, gen, s.lin)
	if err != nil || x == nil {
		t.Fatalf("reading the exchange: %+v (%v)", x, err)
	}
	return x.Phase
}
