//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// An exchange this broker has decided complete is not admitted again, and a terminal cleanup
// carries the observation it acts on (the §0.4 "three broker defects the ledger reds traced").
// Every schedule here is sequenced by the test through real PUBREC, PUBREL and PUBCOMP handling on
// the real store: the PUBCOMP's terminal advance is held open through asm.testAckAdvance, which
// runs after the handler converted the entry into its terminal reservation and before the slot
// leaves pubrel_ready — the window a JetStream redelivery of the canonical PUBREL used to land in.

// testAnsweredReady drives one ordinary QoS 2 exchange to pubrel_ready with its first PUBREL
// delivered and its action drained, the source reading st, and returns the fixture and the slot's
// canonical PUBREL coordinates.
func testAnsweredReady(t *testing.T, st mqttInflightSourceState) (*testAbandonFix, string, uint64) {
	t.Helper()
	f := testOrdinaryOffered(t)
	e := f.e
	testRetainedSourceState(e, &st)
	if err := e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "successful PUBREC materializes", func() bool {
		r, _ := f.register(t)
		return r != nil && r.State == mqttInflightPubRelReady && r.PubRelStreamSeq != 0
	})
	r, _ := f.register(t)
	if n := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelCorrelated(t, e, f.pi,
			testInflightAckSubject(r.PubRelDurable, r.PubRelStreamSeq, 1), r.Nonce, r.Dur, r.Seq)
	}); n != 1 {
		t.Fatalf("fixture: the first PUBREL must be written once, got %d", n)
	}
	e.s.finishOwedActions()
	testCustodyEventually(t, "the first PUBREL's action drains", func() bool {
		cur, _ := f.register(t)
		return cur != nil && cur.ActionToken == ""
	})
	return f, r.PubRelDurable, r.PubRelStreamSeq
}

// testAnsweredRedeliver re-offers the canonical PUBREL (delivery count dc) and reports how many
// PUBREL frames it wrote.
func testAnsweredRedeliver(t *testing.T, f *testAbandonFix, relDur string, relSeq uint64, dc int) int {
	t.Helper()
	r, _ := f.register(t)
	if r == nil {
		t.Fatal("fixture: the slot is gone before the redelivery")
	}
	return testCountPubRelWrites(t, f.e, func() {
		testDeliverPubRelCorrelated(t, f.e, f.pi, testInflightAckSubject(relDur, relSeq, dc), r.Nonce, r.Dur, r.Seq)
	})
}

// testAnsweredHoldAdvance installs a testAckAdvance hook that runs `during` inside the FIRST
// pubrel_ready advance for pi, before that advance is written. It is removed on cleanup.
func testAnsweredHoldAdvance(t *testing.T, f *testAbandonFix, during func()) *atomic.Int64 {
	t.Helper()
	var calls atomic.Int64
	e := f.e
	e.asm.mu.Lock()
	e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.Register != nil && req.Register.PI == f.pi && req.Register.State == mqttInflightPubRelReady &&
			calls.Add(1) == 1 {
			during()
		}
		return write()
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testAckAdvance = nil
		e.asm.mu.Unlock()
	})
	return &calls
}

// testAnsweredConverged waits for the exchange's terminal cleanup to finish: the slot deleted and
// the identifier allocatable again (no entry, no send hold, no exclusion).
func testAnsweredConverged(t *testing.T, f *testAbandonFix, what string) {
	t.Helper()
	f.e.s.finishOwedTerminals()
	testCustodyEventually(t, what, func() bool {
		r, _ := f.register(t)
		return r == nil && testCustodyMints(f.e.sess, f.pi) == f.pi
	})
}

// D1, the callback half. A redelivery of the canonical PUBREL arriving after the client's PUBCOMP
// and before the slot leaves pubrel_ready is WITHHELD before admission: no PUBREL frame, and no
// action acquired (the slot's transmission counter does not move).
//
// Mutation M1: delete the answeredByPubComp refusal in mqttDeliverPubRelCb. Admission then
// acquires an action and the counter advances (the bind refusal still keeps the frame off the
// wire), so this test fails on the counter.
func TestMQTTPubCompAnsweredPubRelIsNotAdmittedAgain(t *testing.T) {
	f, relDur, relSeq := testAnsweredReady(t, mqttInflightSourceComplete)
	e := f.e
	before, _ := f.register(t)
	var writes int
	var during *mqttInflightRecord
	var reservation bool
	calls := testAnsweredHoldAdvance(t, f, func() {
		e.sess.mu.Lock()
		reservation = e.sess.pendingPubRel[f.pi].answeredByPubComp(before.Nonce)
		e.sess.mu.Unlock()
		writes = testAnsweredRedeliver(t, f, relDur, relSeq, 2)
		during, _ = f.register(t)
	})
	e.c.mqttProcessPubComp(f.pi)
	if calls.Load() == 0 {
		t.Fatal("fixture: the PUBCOMP's terminal advance never ran, so no redelivery was offered in its window")
	}
	if !reservation {
		t.Fatal("fixture: the PUBCOMP must have converted its entry into the terminal reservation before its advance")
	}
	if during == nil || during.State != mqttInflightPubRelReady {
		t.Fatalf("fixture: the redelivery must land while the slot is still pubrel_ready, got %+v", during)
	}
	if writes != 0 {
		t.Fatalf("a PUBREL the client already answered was written %d times after its PUBCOMP [MQTT-4.3.3]", writes)
	}
	if during.Acq != before.Acq || during.ActionToken != "" {
		t.Fatalf("the answered exchange's redelivery acquired an action: counter %d -> %d, token %q",
			before.Acq, during.Acq, during.ActionToken)
	}
	testAnsweredConverged(t, f, "the answered exchange completes and frees its identifier")
}

// D2's helper: a terminal-cleanup reference adopts the WHOLE observation it acts on.
//
// Mutation M7: drop acq from adoptObservation.
func TestMQTTSlotRefAdoptsTheWholeObservation(t *testing.T) {
	ref := mqttInflightSlotRef{seq: 3, state: mqttInflightPubRelReady, acq: 1}
	ref.adoptObservation(9, &mqttInflightRecord{State: mqttInflightPubRelTerminal, Acq: 4})
	if ref.seq != 9 || ref.state != mqttInflightPubRelTerminal || ref.acq != 4 {
		t.Fatalf("the reference must be the observation: seq %d state %q acq %d", ref.seq, ref.state, ref.acq)
	}
}

// testAnsweredRedeliverParked starts a redelivery of the canonical PUBREL on its own goroutine,
// parked at mqttTestPubRelBeforeAdmissionV — PAST the callback's answered-exchange check, before
// anything is acquired. release lets it run on; done is closed when the callback has returned
// (its deferred action drain included).
func testAnsweredRedeliverParked(t *testing.T, f *testAbandonFix, relDur string, relSeq uint64,
	dc int) (release func(), done <-chan struct{}) {
	t.Helper()
	r, _ := f.register(t)
	parked, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	seam := func() {
		if once.CompareAndSwap(false, true) {
			close(parked)
			<-resume
		}
	}
	mqttTestPubRelBeforeAdmissionV.Store(&seam)
	t.Cleanup(func() { mqttTestPubRelBeforeAdmissionV.Store(nil) })
	go func() {
		defer close(finished)
		testDeliverPubRelCorrelated(t, f.e, f.pi, testInflightAckSubject(relDur, relSeq, dc), r.Nonce, r.Dur, r.Seq)
	}()
	select {
	case <-parked:
	case <-time.After(testCustodyBound):
		t.Fatal("fixture: the redelivery never reached the seam past its answered-exchange check")
	}
	var released atomic.Bool
	return func() {
		if released.CompareAndSwap(false, true) {
			close(resume)
		}
	}, finished
}

// testAnsweredBetweenCheckAndAcquisition runs the PUBCOMP while a redelivery is parked past the
// callback's check, then — inside the PUBCOMP's first terminal advance, after the conversion —
// lets the redelivery acquire, be refused at its bind and drain. It returns the frames written
// and the slot as the PUBCOMP's terminal advance then finds it, and the counter before.
func testAnsweredBetweenCheckAndAcquisition(t *testing.T, f *testAbandonFix, relDur string,
	relSeq uint64) (writes int, before, during *mqttInflightRecord) {
	t.Helper()
	before, _ = f.register(t)
	writes = testCountPubRelWrites(t, f.e, func() {
		release, done := testAnsweredRedeliverParked(t, f, relDur, relSeq, 2)
		defer release()
		calls := testAnsweredHoldAdvance(t, f, func() {
			release()
			select {
			case <-done:
			case <-time.After(testCustodyBound):
				t.Error("fixture: the released redelivery never returned")
			}
			f.e.s.finishOwedActions()
			testCustodyEventually(t, "the refused redelivery's action drains", func() bool {
				cur, _ := f.register(t)
				return cur != nil && cur.ActionToken == ""
			})
			during, _ = f.register(t)
		})
		f.e.c.mqttProcessPubComp(f.pi)
		if calls.Load() == 0 {
			t.Fatal("fixture: the PUBCOMP's terminal advance never ran")
		}
	})
	if during == nil || during.State != mqttInflightPubRelReady || during.Acq != before.Acq+1 {
		t.Fatalf("fixture: the redelivery must acquire once AFTER the PUBCOMP captured counter %d, "+
			"leaving the slot pubrel_ready at %d; got %+v", before.Acq, before.Acq+1, during)
	}
	return writes, before, during
}

// D1's bind half and D2's terminalization: a PUBCOMP converting the entry AFTER a redelivery passed
// the callback's check but BEFORE it acquired. The redelivery then acquires (the counter moves past
// the one the handler captured), its bind refuses the terminal reservation, and nothing is written;
// the terminalization adopts the counter it observes and finishes the slot.
//
// Mutation M2: delete the answeredByPubComp refusal in bindActionToPubRelLocked -> a PUBREL frame
// is written after the PUBCOMP.
// Mutation M3: in mqttTerminalizeOnPubComp's loop head, adopt only the sequence and state -> the
// gated reap refuses the stale counter and the slot is never finished.
func TestMQTTPubCompBetweenCheckAndAcquisitionCompletesTheExchange(t *testing.T) {
	f, relDur, relSeq := testAnsweredReady(t, mqttInflightSourceComplete)
	writes, _, _ := testAnsweredBetweenCheckAndAcquisition(t, f, relDur, relSeq)
	if writes != 0 {
		t.Fatalf("a PUBREL the client already answered was written %d times after its PUBCOMP [MQTT-4.3.3]", writes)
	}
	testAnsweredConverged(t, f, "the terminalization adopts the observed counter and finishes the slot")
}

// D2, the terminal-observed arm: a completion worker's reference that predates an acquisition,
// re-entering over a slot already pubrel_terminal, finishes it.
//
// Mutation M3 (as above).
func TestMQTTTerminalizeAdoptsTheObservedCounterOnATerminalSlot(t *testing.T) {
	f, relDur, relSeq := testAnsweredReady(t, mqttInflightSourceUnknown)
	e := f.e
	testRetainedCleanupPacing(e, func(int) time.Duration { return testRetainedNeverOnItsOwn })
	stale, staleSeq := f.register(t)
	ref := *mqttSlotRefFromRecord(f.subject, staleSeq, stale)
	if n := testAnsweredRedeliver(t, f, relDur, relSeq, 2); n != 1 {
		t.Fatalf("fixture: an unanswered redelivery must be written, got %d", n)
	}
	e.s.finishOwedActions()
	testCustodyEventually(t, "the redelivery's action drains", func() bool {
		cur, _ := f.register(t)
		return cur != nil && cur.ActionToken == ""
	})
	e.c.mqttProcessPubComp(f.pi)
	testCustodyEventually(t, "PUBCOMP retains the terminal slot (source Unknown)", func() bool {
		cur, _ := f.register(t)
		return cur != nil && cur.State == mqttInflightPubRelTerminal
	})
	cur, _ := f.register(t)
	if cur.Acq == ref.acq {
		t.Fatalf("fixture: the reference must predate the redelivery's acquisition (both %d)", cur.Acq)
	}
	testRetainedSourceState(e, nil)
	if res := e.sess.mqttTerminalizeOnPubComp(e.c, ref); res.outcome != mqttTerminalDone {
		t.Fatalf("a terminal slot observed afresh must be finished whatever counter the reference "+
			"captured: outcome %v (recorded %v)", res.outcome, res.recorded)
	}
	if r, _ := f.register(t); r != nil {
		t.Fatalf("the terminal slot survived its finalization: %+v", r)
	}
}

// D2, the retained owner (the measured TestMQTTDrainBarrierFailsClosed hold): the PUBCOMP retains
// the slot for a source that may re-offer and enrolls the owner from the handler's captured
// reference, which predates the redelivery's acquisition. Once the source settles, the owner
// adopts the counter it observes and finishes the slot.
//
// Mutation M4: in attemptRetainedCleanup, adopt only the sequence and state -> every owner attempt
// is refused by the gated reap and the identifier stays excluded.
func TestMQTTRetainedTerminalCleanupAdoptsTheObservedCounter(t *testing.T) {
	f, relDur, relSeq := testAnsweredReady(t, mqttInflightSourceUnknown)
	e := f.e
	testRetainedCleanupPacing(e, func(int) time.Duration { return 10 * time.Millisecond })
	writes, _, during := testAnsweredBetweenCheckAndAcquisition(t, f, relDur, relSeq)
	if writes != 0 {
		t.Fatalf("a PUBREL the client already answered was written %d times after its PUBCOMP", writes)
	}
	testCustodyEventually(t, "PUBCOMP retains the terminal slot under an owner", func() bool {
		cur, _ := f.register(t)
		return cur != nil && cur.State == mqttInflightPubRelTerminal && testRetainedOwner(e) != nil
	})
	w := testRetainedOwner(e)
	e.sess.mu.Lock()
	ownerAcq := w.ref.acq
	e.sess.mu.Unlock()
	if ownerAcq == during.Acq {
		t.Fatalf("fixture: the owner must hold the counter captured before the acquisition, got %d", ownerAcq)
	}
	testRetainedSourceState(e, nil)
	testCustodyEventually(t, "the owner attempts over the settled source", func() bool {
		return testRetainedOwnerAttempts(e) > 2 || testRetainedOwner(e) == nil
	})
	testRetainedConverged(t, f, "the owner adopts the observed counter and finishes the slot")
}

// testAckReleaseFix admits one ordinary QoS 1 delivery whose PUBACK the test will process.
func testAckReleaseFix(t *testing.T, name string) (*testInflightEnv, uint16, string) {
	t.Helper()
	e := testNewInflightEnv(t, name)
	e.c.mqtt = &mqtt{asm: e.asm, sess: e.sess}
	dur := e.sess.idHash + "_plain"
	sseq, reply := e.publishOfferedSource(t, dur)
	pi := e.trackAdmitted(t, dur, sseq, 1, reply)
	e.sess.mu.Lock()
	e.sess.c = e.c
	e.sess.mu.Unlock()
	return e, pi, mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
}

// testAckReleaseHooks installs the acknowledgement-advance hook (decide gets the call's ordinal and
// the real write) and counts contention waits.
func testAckReleaseHooks(t *testing.T, e *testInflightEnv, pi uint16,
	decide func(n int64, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)) (calls, waits *atomic.Int64) {
	t.Helper()
	calls, waits = new(atomic.Int64), new(atomic.Int64)
	e.asm.mu.Lock()
	e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.Register != nil && req.Register.PI == pi && req.Register.State == mqttInflightPublish {
			return decide(calls.Add(1), write)
		}
		return write()
	}
	e.asm.testAuthorityContentionWait = func(time.Duration) { waits.Add(1) }
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testAckAdvance, e.asm.testAuthorityContentionWait = nil, nil
		e.asm.mu.Unlock()
	})
	return calls, waits
}

var errTestAckContended = errors.New("test: transition is conditioned on the authority at a sequence it no longer stands at")

// D3: one contended advance is re-attempted inside the same acknowledgement, after a contention
// wait and a fresh read, instead of waiting out the continuation's delay.
//
// Mutation M5: give the re-attempt loop a budget of one attempt -> one advance call, and the
// register is still standing when the PUBACK returns.
func TestMQTTAcknowledgedReleaseReattemptsAContendedAdvance(t *testing.T) {
	e, pi, subject := testAckReleaseFix(t, "ack-release-contended")
	calls, waits := testAckReleaseHooks(t, e, pi, func(n int64, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if n == 1 {
			return mqttOpOutcome{}, errTestAckContended // refused, nothing written
		}
		return write()
	})
	if err := e.c.mqttProcessPubAck(pi); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || waits.Load() != 1 {
		t.Fatalf("the contended advance must be re-attempted once, after one contention wait, within the "+
			"acknowledgement: %d advance calls, %d waits", calls.Load(), waits.Load())
	}
	if _, smsg, err := e.asm.loadInflightRecord(subject); err != nil || smsg != nil {
		t.Fatalf("the acknowledged register must be gone when the PUBACK returns (err %v, present %v)", err, smsg != nil)
	}
}

// D3: an advance that LANDED behind a lost response is adopted by the re-attempt's fresh read
// (publish_terminal), never advanced a second time.
//
// Mutation M6: re-attempt by resubmitting the first attempt's advance instead of re-running the
// attempt -> a second advance call.
func TestMQTTAcknowledgedReleaseAdoptsALandedAdvance(t *testing.T) {
	e, pi, subject := testAckReleaseFix(t, "ack-release-landed")
	calls, _ := testAckReleaseHooks(t, e, pi, func(n int64, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		out, err := write()
		if n == 1 && err == nil {
			return out, errors.New("test: the advance landed but its response was lost")
		}
		return out, err
	})
	if err := e.c.mqttProcessPubAck(pi); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("a landed advance must be adopted, not advanced again: %d advance calls", calls.Load())
	}
	if _, smsg, err := e.asm.loadInflightRecord(subject); err != nil || smsg != nil {
		t.Fatalf("the acknowledged register must be gone when the PUBACK returns (err %v, present %v)", err, smsg != nil)
	}
}

// D3: the budget is bounded. An advance refused every time is attempted exactly
// mqttForegroundAcquireAttempts times within the acknowledgement, and the continuation finishes
// it once the contention ends.
//
// Mutation M5 (as above) -> one call instead of the budget.
func TestMQTTAcknowledgedReleaseExhaustsToTheContinuation(t *testing.T) {
	e, pi, subject := testAckReleaseFix(t, "ack-release-exhausted")
	var refuse atomic.Bool
	refuse.Store(true)
	calls, waits := testAckReleaseHooks(t, e, pi, func(_ int64, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if refuse.Load() {
			return mqttOpOutcome{}, errTestAckContended
		}
		return write()
	})
	if err := e.c.mqttProcessPubAck(pi); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != mqttForegroundAcquireAttempts || waits.Load() != mqttForegroundAcquireAttempts-1 {
		t.Fatalf("an always-contended advance must use exactly its budget: %d calls (want %d), %d waits",
			calls.Load(), mqttForegroundAcquireAttempts, waits.Load())
	}
	if cur, _, err := e.asm.loadInflightRecord(subject); err != nil || cur == nil || cur.State != mqttInflightPublish {
		t.Fatalf("an exhausted release must leave the register for its continuation: %+v (%v)", cur, err)
	}
	refuse.Store(false)
	e.s.finishOwedTerminals()
	testCustodyEventually(t, "the continuation finishes the release", func() bool {
		_, smsg, err := e.asm.loadInflightRecord(subject)
		return err == nil && smsg == nil
	})
}

// D1, the delegated arm (holistic review round 1). A redelivery that ACQUIRED and has not bound yet
// is parked; the client's PUBCOMP then runs to completion: the slot carries that redelivery's live
// token, so the completion pends its drain (HeldByAction) and the handler hands the cleanup to that
// local action and releases the terminal reservation. The resumed redelivery finds no reservation
// to refuse it — the action itself carries the answer, and nothing is written.
//
// Mutation M8: answerLocalActionLocked delegates without marking the action -> the resumed
// redelivery binds over the empty identifier and writes a PUBREL after the PUBCOMP.
func TestMQTTPubCompDelegatedToAnAcquiredRedeliveryWritesNothing(t *testing.T) {
	f, relDur, relSeq := testAnsweredReady(t, mqttInflightSourceComplete)
	e := f.e
	r, _ := f.register(t)
	parked, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	seam := func() {
		if once.CompareAndSwap(false, true) {
			close(parked)
			<-resume
		}
	}
	mqttTestBeforePubRelPromotionV.Store(&seam)
	t.Cleanup(func() { mqttTestBeforePubRelPromotionV.Store(nil) })
	var released atomic.Bool
	release := func() {
		if released.CompareAndSwap(false, true) {
			close(resume)
		}
	}
	t.Cleanup(release)
	writes := testCountPubRelWrites(t, e, func() {
		go func() {
			defer close(finished)
			testDeliverPubRelCorrelated(t, e, f.pi, testInflightAckSubject(relDur, relSeq, 2), r.Nonce, r.Dur, r.Seq)
		}()
		select {
		case <-parked:
		case <-time.After(testCustodyBound):
			t.Fatal("fixture: the redelivery never acquired and reached the promotion seam")
		}
		if cur, _ := f.register(t); cur == nil || cur.ActionToken == "" || cur.Acq != r.Acq+1 {
			t.Fatalf("fixture: the parked redelivery must hold a live acquisition, got %+v", cur)
		}
		e.c.mqttProcessPubComp(f.pi)
		e.sess.mu.Lock()
		resident := e.sess.pendingPubRel[f.pi]
		e.sess.mu.Unlock()
		if resident != nil {
			t.Fatalf("fixture: the PUBCOMP must have delegated to the local action and released its "+
				"reservation, got %+v", resident)
		}
		release()
		select {
		case <-finished:
		case <-time.After(testCustodyBound):
			t.Fatal("the resumed redelivery never returned")
		}
	})
	if writes != 0 {
		t.Fatalf("a PUBREL the client already answered was written %d times after its PUBCOMP [MQTT-4.3.3]", writes)
	}
	e.s.finishOwedActions()
	testAnsweredConverged(t, f, "the delegated drain finishes the answered exchange")
}

// The delegated arm for the slot-only class (experimental affine, chapter P), pinned structurally:
// a delegation over the identifier's send hold marks its live action, and write entry then refuses
// it.
//
// Mutation M8 (as above).
func TestMQTTSlotOnlyDelegationMarksTheLiveAction(t *testing.T) {
	e := testNewInflightEnv(t, "answered-slot-only")
	testReadyForPubRelDelivery(t, e)
	dur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_" + mqttNewInflightNonce()
	ref := testRow2SlotOnlyReady(t, e, 23, dur)
	act, _ := e.sess.mqttAcquirePubRelAction(e.c, ref, ref.nonce, 0)
	if act == nil {
		t.Fatal("fixture: the slot-only holder was not acquired")
	}
	defer e.sess.startActionDrain(e.c, act)
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if e.sess.pendingPubRel == nil {
		e.sess.pendingPubRel = make(map[uint16]*mqttPending)
	}
	e.sess.pendingPubRel[23] = &mqttPending{kind: mqttPendingAffine, regNonce: act.slot.nonce, slot: act.slot}
	if !e.sess.pubRelWriteKindLocked(23, act) {
		t.Fatal("fixture: an unanswered slot-only action must be writable")
	}
	if !e.sess.answerLocalActionLocked(23, act.token, true) {
		t.Fatal("the live slot-only action reserving the identifier must take the delegation")
	}
	if !act.pubCompAnswered || e.sess.pubRelWriteKindLocked(23, act) {
		t.Fatalf("the delegated slot-only action must be marked answered and refused at write entry "+
			"(answered %v)", act.pubCompAnswered)
	}
	delete(e.sess.pendingPubRel, 23)
}

// A STALE delegation never answers a successor (holistic review round 2): a slot-only handler whose
// finalize observed one token, resuming after another action took the identifier, still reports the
// identifier reserved but marks nothing — the successor's PUBREL is still owed.
//
// Mutation M9: mark the slot-only identifier's live action whatever token finalize observed.
func TestMQTTSlotOnlyStaleDelegationLeavesTheSuccessorWritable(t *testing.T) {
	e := testNewInflightEnv(t, "answered-slot-only-stale")
	testReadyForPubRelDelivery(t, e)
	dur := mqttQ2AffineConsumerDurablePrefix + e.sess.idHash + "_" + mqttNewInflightNonce()
	ref := testRow2SlotOnlyReady(t, e, 24, dur)
	act, _ := e.sess.mqttAcquirePubRelAction(e.c, ref, ref.nonce, 0)
	if act == nil {
		t.Fatal("fixture: the slot-only successor was not acquired")
	}
	defer e.sess.startActionDrain(e.c, act)
	e.sess.mu.Lock()
	defer e.sess.mu.Unlock()
	if e.sess.pendingPubRel == nil {
		e.sess.pendingPubRel = make(map[uint16]*mqttPending)
	}
	e.sess.pendingPubRel[24] = &mqttPending{kind: mqttPendingAffine, regNonce: act.slot.nonce, slot: act.slot}
	defer delete(e.sess.pendingPubRel, 24)
	stale := mqttNewInflightNonce() // the token a predecessor's finalize observed
	if !e.sess.answerLocalActionLocked(24, stale, true) {
		t.Fatal("the slot-only identifier is still reserved by the successor's send hold")
	}
	if act.pubCompAnswered || !e.sess.pubRelWriteKindLocked(24, act) {
		t.Fatalf("a stale delegation answered the successor's action (answered %v)", act.pubCompAnswered)
	}
}
