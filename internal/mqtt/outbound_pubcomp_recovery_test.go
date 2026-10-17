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
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// Recovery and the recovered PUBREC for an ordinary exchange, on the real store: the terminal
// cleanup owners recovery hands to, and every route that records a PUBREC from durable evidence
// before it lets a PUBREL reach the client (plan §§4.4, 4.9, 4.11).

// testAdmittedPublishWorld is an ordinary QoS 2 delivery admitted through the authority and never
// answered: slot publish, exchange Publish.
func testAdmittedPublishWorld(t *testing.T, clientID string, sseq uint64) (*testInflightEnv, uint16, string, string) {
	t.Helper()
	e := testNewInflightEnv(t, clientID)
	testReadyForRealPubRelDelivery(t, e)
	pi := e.trackAdmitted(t, testRealPubRelSrcDur, sseq, 2)
	e.sess.mu.Lock()
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	e.sess.mu.Unlock()
	cur, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || cur.State != mqttInflightPublish {
		t.Fatalf("the admitted slot is %+v (%v), want publish", cur, err)
	}
	return e, pi, subject, cur.Lin
}

// testStoreCanonical stores the exchange's canonical PUBREL through the production builder, the
// drained-intent late-store shape, and returns its sequence.
func testStoreCanonical(t *testing.T, e *testInflightEnv, pi uint16, nonce string, sseq uint64) uint64 {
	t.Helper()
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, nonce, testRealPubRelSrcDur, sseq, _EMPTY_, _EMPTY_, _EMPTY_)
	resp, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdrLen, msg)
	if err != nil || resp == nil || resp.Sequence == 0 {
		t.Fatalf("storing the canonical PUBREL: %+v (%v)", resp, err)
	}
	return resp.Sequence
}

func testExchangePhaseOf(t *testing.T, e *testInflightEnv, lin string) mqttExchangePhase {
	t.Helper()
	e.sess.mu.Lock()
	gen := e.sess.gen
	e.sess.mu.Unlock()
	x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, gen, lin)
	if err != nil || x == nil {
		t.Fatalf("reading the exchange: %+v (%v)", x, err)
	}
	return x.Phase
}

func testSlotAt(t *testing.T, e *testInflightEnv, subject string) *mqttInflightRecord {
	t.Helper()
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil {
		t.Fatalf("reading the slot: %v", err)
	}
	if smsg == nil {
		return nil
	}
	return cur
}

// T14. Route B for an ORDINARY slot records the PUBREC the stored record proves on its exchange,
// then rebinds the canonical tuple.
//
// Mutation M25: route B's ordinary arm reverted to the slot-only CAS -> the exchange stays PUBLISH.
func TestF105RouteBRecordsTheRecoveredPubRec(t *testing.T) {
	t.Run("both steps", func(t *testing.T) {
		t.Parallel()
		e, pi, subject, lin := testAdmittedPublishWorld(t, "f105-routeb", testRealPubRelSrcSeq)
		nonce := e.nonceOf(pi)
		canon := testStoreCanonical(t, e, pi, nonce, testRealPubRelSrcSeq)
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePublish {
			t.Fatalf("the fixture's exchange starts at phase %d", got)
		}
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		slot := testSlotAt(t, e, subject)
		if slot == nil || slot.State != mqttInflightPubRelReady || slot.PubRelStreamSeq != canon {
			t.Fatalf("route B left the slot at %+v, want pubrel_ready naming %d", slot, canon)
		}
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
			t.Fatalf("route B left the exchange at phase %d, want PUBREC-observed", got)
		}
	})
	t.Run("the rebind fails after the PUBREC commits", func(t *testing.T) {
		t.Parallel()
		e, pi, subject, lin := testAdmittedPublishWorld(t, "f105-routeb-cut", testRealPubRelSrcSeq)
		testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq)
		var refused atomic.Bool
		e.asm.mu.Lock()
		e.asm.testInflightStore = func(s string, expect uint64, r *mqttInflightRecord) (uint64, error) {
			if s == subject && r.State == mqttInflightPubRelReady {
				refused.Store(true)
				return 0, errors.New("test: the rebind is refused")
			}
			return e.asm.jsa.storeInflightCAS(s, expect, r)
		}
		e.asm.mu.Unlock()
		t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testInflightStore = nil; e.asm.mu.Unlock() })
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		if !refused.Load() {
			t.Fatal("the rebind was never attempted")
		}
		slot := testSlotAt(t, e, subject)
		if slot == nil || slot.State != mqttInflightPubRelIntended {
			t.Fatalf("the cut left the slot at %+v, want pubrel_intended", slot)
		}
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
			t.Fatalf("the cut left the exchange at phase %d, want PUBREC-observed", got)
		}
	})
	for _, lost := range []int{1, mqttInflightStoreAttempts} {
		lost := lost
		t.Run(fmt.Sprintf("the PUBREC lands behind a lost response on attempt %d", lost), func(t *testing.T) {
			t.Parallel()
			e, pi, subject, lin := testAdmittedPublishWorld(t, fmt.Sprintf("f105-routeb-lost-%d", lost), testRealPubRelSrcSeq)
			testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq)
			var calls atomic.Int32
			e.asm.mu.Lock()
			e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				if req.To != mqttExchangePubRecObserved || req.Nonce != lin {
					return write()
				}
				n := int(calls.Add(1))
				if n < lost {
					return mqttOpOutcome{}, errors.New("test: refused without writing")
				}
				if n == lost {
					if _, err := write(); err != nil {
						t.Errorf("the write behind the lost response failed: %v", err)
					}
					return mqttOpOutcome{}, errors.New("test: response lost")
				}
				return write()
			}
			e.asm.mu.Unlock()
			t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testAckAdvance = nil; e.asm.mu.Unlock() })
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatalf("recoverInflightRegisters: %v", err)
			}
			if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPubRelReady {
				t.Fatalf("route B did not complete behind the lost response: %+v", slot)
			}
			if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
				t.Fatalf("the exchange stands at phase %d", got)
			}
		})
	}
	t.Run("the PUBREC is unresolved: the route-B failure disposition", func(t *testing.T) {
		t.Parallel()
		e, pi, subject, lin := testAdmittedPublishWorld(t, "f105-routeb-fail", testRealPubRelSrcSeq)
		testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq)
		e.asm.mu.Lock()
		e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if req.To == mqttExchangePubRecObserved {
				return mqttOpOutcome{}, errors.New("test: refused")
			}
			return write()
		}
		e.asm.mu.Unlock()
		t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testAckAdvance = nil; e.asm.mu.Unlock() })
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPublish {
			t.Fatalf("an unrecorded PUBREC moved the slot: %+v", slot)
		}
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePublish {
			t.Fatalf("the exchange moved to phase %d", got)
		}
		e.sess.mu.Lock()
		pending := e.sess.pendingPublish[pi]
		_, mapped := e.sess.cpending[testRealPubRelSrcDur][testRealPubRelSrcSeq]
		incomplete := e.sess.piSpaceIncomplete
		e.sess.mu.Unlock()
		if pending == nil || pending.jsDur != _EMPTY_ || pending.sseq != 0 || mapped || !incomplete {
			t.Fatalf("failed route B left a resident PUBLISH entry sendable: pending=%+v mapped=%v incomplete=%v",
				pending, mapped, incomplete)
		}
	})
	t.Run("a record naming another exchange records nothing", func(t *testing.T) {
		t.Parallel()
		e, pi, subject, lin := testAdmittedPublishWorld(t, "f105-routeb-foreign", testRealPubRelSrcSeq)
		testStoreCanonical(t, e, pi, mqttNewInflightNonce(), testRealPubRelSrcSeq)
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePublish {
			t.Fatalf("a foreign record recorded a PUBREC: phase %d", got)
		}
		if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPublish {
			t.Fatalf("a foreign record moved the slot: %+v", slot)
		}
	})
	t.Run("the source is unfinished", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "f105-routeb-live-source")
		testReadyForRealPubRelDelivery(t, e)
		stored, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+"f092.source", 0, []byte("source"))
		if err != nil {
			t.Fatal(err)
		}
		pi := e.trackAdmitted(t, testRealPubRelSrcDur, stored.Sequence, 2)
		e.sess.mu.Lock()
		subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
		e.sess.mu.Unlock()
		lin := testSlotAt(t, e, subject).Lin
		testStoreCanonical(t, e, pi, e.nonceOf(pi), stored.Sequence)
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
			t.Fatalf("route B waited on the source: phase %d", got)
		}
		if slot := testSlotAt(t, e, subject); slot == nil || !mqttInflightPastPubRec(slot.State) {
			t.Fatalf("route B waited on the source: %+v", slot)
		}
	})
	t.Run("the record appears after the register read", func(t *testing.T) {
		t.Parallel()
		e, pi, subject, lin := testAdmittedPublishWorld(t, "f105-routeb-late", testRealPubRelSrcSeq)
		nonce := e.nonceOf(pi)
		var stored atomic.Bool
		e.asm.mu.Lock()
		e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
			// Only the REGISTER batch: the inventory traversal's duty batches (F-105 7b) run
			// first and must not spend the one shot, or the record lands before the walk.
			if !slices.Contains(subjects, subject) {
				return e.asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
			}
			// One-shot: the real batch read, then the record lands behind it.
			e.asm.mu.Lock()
			e.asm.testInflightLoadMulti = nil
			e.asm.mu.Unlock()
			out, err := e.asm.loadInflightRecordsMulti(subjects)
			if stored.CompareAndSwap(false, true) {
				testStoreCanonical(t, e, pi, nonce, testRealPubRelSrcSeq)
			}
			return out, err
		}
		e.asm.mu.Unlock()
		t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testInflightLoadMulti = nil; e.asm.mu.Unlock() })
		if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
			t.Fatalf("recoverInflightRegisters: %v", err)
		}
		if !stored.Load() {
			t.Fatal("the register batch read never ran")
		}
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
			t.Fatalf("the record read after the registers did not route B: phase %d", got)
		}
		if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPubRelReady {
			t.Fatalf("the slot is %+v", slot)
		}
	})
}

// T13. The drain resume records the PUBREC its intent proves BEFORE it confirms or recreates the
// PUBREL, so no PUBREL reaches the client while the exchange still reads PUBLISH.
//
// Mutation M24: skip recordDrainIntentPubRec -> the exchange still reads PUBLISH when the drain
// resume returns (admission would only repair it AT delivery).
func TestF105DrainResumeRecordsTheIntentsPubRec(t *testing.T) {
	for _, withRecord := range []bool{false, true} {
		withRecord := withRecord
		name := "record absent: recreate"
		if withRecord {
			name = "record present: confirm"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e, pi, subject, lin := testAdmittedPublishWorld(t, fmt.Sprintf("f105-drain-intent-%v", withRecord), testRealPubRelSrcSeq)
			nonce := e.nonceOf(pi)
			// THE CRASH CUT: the drain intent is durable, the PUBREC advance never ran.
			cc := &ConsumerConfig{Durable: testRealPubRelSrcDur, DeliverSubject: mqttSubPrefix + "f105.drain"}
			e.sess.mu.Lock()
			e.sess.drains = map[string]*mqttDrain{"f105/topic": {
				cc: cc, qos: 2,
				seqs: map[uint64]*drainSeq{testRealPubRelSrcSeq: {claimed: true, intent: true, pi: pi, ownerNonce: nonce}},
			}}
			e.sess.drainByDur = map[string]*mqttDrain{testRealPubRelSrcDur: e.sess.drains["f105/topic"]}
			e.sess.mu.Unlock()
			if withRecord {
				testStoreCanonical(t, e, pi, nonce, testRealPubRelSrcSeq)
			}
			if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePublish {
				t.Fatalf("the cut's exchange starts at phase %d", got)
			}
			if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPublish {
				t.Fatalf("the cut's slot is %+v", slot)
			}

			skip := e.sess.recoverDrainPubRels(e.c)
			if skip[testRealPubRelSrcDur] {
				t.Fatalf("the drain was withheld: %v", skip)
			}
			if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
				t.Fatalf("the drain resume left the exchange at phase %d before any delivery", got)
			}
			if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPubRelIntended {
				t.Fatalf("the drain resume left the slot at %+v, want pubrel_intended", slot)
			}
		})
	}
}

// T15. The ordinary direct arrow's publish origin: refused while the PUBREC cannot be recorded,
// kept from pubrel_intended, and — T15b — the live connection's next delivery makes progress.
//
// Mutation M27: admission refuses without recording the PUBREC -> T15b never delivers.
func TestF105AdmissionRecordsThePubRecTheDeliveredRecordProves(t *testing.T) {
	// Deliberately NOT t.Parallel(): testCountPubRelWrites counts at a process-global write hook.
	e, pi, subject, lin := testAdmittedPublishWorld(t, "f105-admission-pubrec", testRealPubRelSrcSeq)
	nonce := e.nonceOf(pi)
	canon := testStoreCanonical(t, e, pi, nonce, testRealPubRelSrcSeq)
	e.sess.mu.Lock()
	_, _, pubRelDur := mqttPubRelNames(e.sess.idHash, e.sess.gen)
	e.sess.mu.Unlock()
	var refuse atomic.Bool
	refuse.Store(true)
	e.asm.mu.Lock()
	e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if req.To == mqttExchangePubRecObserved && refuse.Load() {
			return mqttOpOutcome{}, errors.New("test: refused")
		}
		return write()
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testAckAdvance = nil; e.asm.mu.Unlock() })

	writes := testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject(pubRelDur, canon, 1), nonce)
	})
	if writes != 0 {
		t.Fatalf("a PUBREL was transmitted while its exchange could not record the PUBREC (%d writes)", writes)
	}
	if slot := testSlotAt(t, e, subject); slot == nil || slot.State != mqttInflightPublish || slot.ActionToken != _EMPTY_ {
		t.Fatalf("the refused delivery moved the slot: %+v", slot)
	}
	if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePublish {
		t.Fatalf("the refused delivery moved the exchange to phase %d", got)
	}

	// T15b: the store recovers and the next re-offer — no reconnect — records the PUBREC and
	// transmits from pubrel_intended.
	refuse.Store(false)
	writes = testCountPubRelWrites(t, e, func() {
		testDeliverPubRelAs(t, e, pi, testInflightAckSubject(pubRelDur, canon, 2), nonce)
	})
	if writes != 1 {
		t.Fatalf("the re-offer on the live connection transmitted %d PUBREL(s), want 1", writes)
	}
	if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePubRecObserved {
		t.Fatalf("the re-offer left the exchange at phase %d", got)
	}
	e.c.mqttProcessPubComp(pi)
	checkFor(t, 2*time.Second, 20*time.Millisecond, func() error {
		if got := testExchangePhaseOf(t, e, lin); got != mqttExchangeTerminal {
			return fmt.Errorf("phase %d", got)
		}
		return nil
	})
}

// T15c. The direct-arrow primitive itself never moves an ordinary publish slot past PUBREC.
//
// Mutation M26: remove the primitive's ordinary-publish refusal -> it acquires.
func TestF105DirectArrowRefusesAnOrdinaryPublishOrigin(t *testing.T) {
	t.Parallel()
	e, pi, subject, lin := testAdmittedPublishWorld(t, "f105-direct-primitive", testRealPubRelSrcSeq)
	canon := testStoreCanonical(t, e, pi, e.nonceOf(pi), testRealPubRelSrcSeq)
	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("reading the slot: %v", err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	e.sess.mu.Lock()
	_, _, pubRelDur := mqttPubRelNames(e.sess.idHash, e.sess.gen)
	e.sess.mu.Unlock()
	if act, _ := e.sess.acquireActionFromPublish(e.c, *ref, testLinOf(e.sess, ref), pubRelDur, canon); act != nil {
		t.Fatalf("the direct arrow acquired from an ordinary publish slot: %+v", act)
	}
	if after := testSlotAt(t, e, subject); after == nil || after.State != mqttInflightPublish || after.ActionToken != _EMPTY_ {
		t.Fatalf("the refused arrow moved the slot: %+v", after)
	}
	if got := testExchangePhaseOf(t, e, lin); got != mqttExchangePublish {
		t.Fatalf("the refused arrow moved the exchange to phase %d", got)
	}
	e.sess.mu.Lock()
	holder := e.sess.localActionHoldingLocked(pi, cur.ActionToken)
	e.sess.mu.Unlock()
	if holder != nil {
		t.Fatal("the refused arrow left a local holder")
	}
}

// T12. Recovery's and the handler's unsettled terminal cleanups go to the completion worker, and
// the worker settles each outcome by its own rule.
func TestF105TerminalCleanupOwnersSettleByOutcome(t *testing.T) {
	t.Run("recovery's indeterminate finalize: the worker owns it and the resume continues", func(t *testing.T) {
		// Mutation M20: recovery's default arm back to retainAndReconcileTerminal -> no worker.
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t12a")
		// A RESUME has no in-memory PUBREL entry; with one, recovery leaves the slot to it.
		w.e.sess.mu.Lock()
		delete(w.e.sess.pendingPubRel, w.seed.pi)
		w.e.sess.mu.Unlock()
		// Record the completion; hold its first cleanup indeterminate so recovery finds a
		// terminal slot.
		// failScans is how many upcoming PUBREL scans fail; passScans how many pass first.
		// Recovery's own pre-resume census is a PUBREL scan too, and a failed census aborts the
		// resume before any finalize, so the census passes and only the finalize's disposal
		// scan fails.
		var failScans, passScans atomic.Int32
		failScans.Store(1)
		asm := w.e.asm
		realScan := func(subject string) ([]mqttStoredPubRel, int, bool) {
			asm.mu.Lock()
			hook := asm.testInflightPubRelScan
			asm.testInflightPubRelScan = nil
			asm.mu.Unlock()
			defer func() { asm.mu.Lock(); asm.testInflightPubRelScan = hook; asm.mu.Unlock() }()
			return w.e.sess.scanPubRelRecords(subject)
		}
		asm.mu.Lock()
		asm.testInflightPubRelScan = func(subject string) ([]mqttStoredPubRel, int, bool) {
			if passScans.Load() > 0 {
				passScans.Add(-1)
				return realScan(subject)
			}
			if failScans.Load() > 0 {
				failScans.Add(-1)
				return nil, 0, false
			}
			return realScan(subject)
		}
		asm.mu.Unlock()
		t.Cleanup(func() { asm.mu.Lock(); asm.testInflightPubRelScan = nil; asm.mu.Unlock() })
		ref := testF105SlotRefAt(t, w.e.sess, w.seed.pi, mqttInflightPubRelReady, false)
		if res := w.e.sess.mqttTerminalizeOnPubComp(w.e.c, ref); !res.recorded || res.outcome != mqttTerminalIndeterminate {
			t.Fatalf("the held cleanup concluded %+v", res)
		}
		_, _, _, before, _ := w.obs.snapshot()
		passScans.Store(1) // the census
		failScans.Store(1) // the finalize's disposal scan
		if _, err := w.e.sess.recoverInflightRegisters(w.e.c); err != nil {
			t.Fatalf("an indeterminate recovery finalize refused the resume: %v", err)
		}
		if _, _, _, after, _ := w.obs.snapshot(); after != before+1 {
			t.Fatalf("recovery's indeterminate finalize enrolled %d new completion worker(s), want 1", after-before)
		}
		checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
			if slot := testRealSlot(t, w.e, w.seed); slot != nil {
				return fmt.Errorf("the slot stands at %q", slot.State)
			}
			return nil
		})
	})
	t.Run("a holder this broker cannot see finish keeps the reservation", func(t *testing.T) {
		// Mutation M21: nonlocal HeldByAction treated as settled -> the placeholder is released
		// while the token stands.
		t.Parallel()
		w, act := testPendingDrainWorld(t, "f105-t12b")
		// The local holder is gone (as if its process died) but its durable token stands.
		w.e.sess.finishActionAndWake(act, false)
		ref := testF105SlotRefAt(t, w.e.sess, w.seed.pi, mqttInflightPubRelTerminal, true)
		w.e.sess.ownTerminalCleanup(w.e.c, ref)
		time.Sleep(mqttActionReconcileDelay + mqttActionReconcileDelay/2)
		if entry, _ := w.pending(); entry == nil || entry.jsAckSubject != _EMPTY_ {
			t.Fatalf("the reservation was released while an unseen holder's token stands: %+v", entry)
		}
		if slot := testRealSlot(t, w.e, w.seed); slot == nil || slot.ActionToken != act.token {
			t.Fatalf("the slot moved under an unseen holder: %+v", slot)
		}
		// The peer's drain completes both records; the worker then finalizes and releases.
		cur, smsg, _ := w.e.asm.loadInflightRecord(w.seed.subject)
		w.e.sess.mu.Lock()
		gen := w.e.sess.gen
		w.e.sess.mu.Unlock()
		if _, err := w.e.asm.jsa.opCompleteExchangeDrain(&mqttCompleteExchangeDrainRequest{
			ID: w.e.sess.id, Epoch: w.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-peer-drain",
			Gen: gen, Nonce: w.seed.lin, Token: act.token, Register: cur, RegisterSeq: smsg.Sequence,
		}); err != nil {
			t.Fatalf("the peer's drain: %v", err)
		}
		checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
			if slot := testRealSlot(t, w.e, w.seed); slot != nil {
				return fmt.Errorf("the slot stands at %q", slot.State)
			}
			if entry, _ := w.pending(); entry != nil {
				return fmt.Errorf("the placeholder remains: %+v", entry)
			}
			return nil
		})
	})
	t.Run("a stale local action does not stand for the durable token's holder", func(t *testing.T) {
		// Mutation M22: delegation back to sendReservedLocked -> the placeholder is released
		// while another action's token stands.
		t.Parallel()
		w := testNewPubCompWorld(t, "f105-t12c")
		ref := testF105SlotRefAt(t, w.e.sess, w.seed.pi, mqttInflightPubRelReady, false)
		actA := w.e.sess.testAcquirePubRel(w.e.c, ref)
		if actA == nil {
			t.Fatal("no local action")
		}
		// The DURABLE token becomes another holder's (B), while A's local hold stays live.
		cur, smsg, _ := w.e.asm.loadInflightRecord(w.seed.subject)
		swapped := *cur
		swapped.ActionToken = mqttNewInflightNonce()
		if _, err := w.e.asm.jsa.storeInflightCAS(w.seed.subject, smsg.Sequence, &swapped); err != nil {
			t.Fatal(err)
		}
		w.e.c.mqttProcessPubComp(w.seed.pi)
		if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminalPendingDrain {
			t.Fatalf("PUBCOMP over B's token recorded phase %d", got)
		}
		if entry, _ := w.pending(); entry == nil || entry.jsAckSubject != _EMPTY_ {
			t.Fatalf("the reservation was released on A's hold although B's token stands: %+v", entry)
		}
		if _, _, enrolled, _, _ := w.obs.snapshot(); enrolled == 0 {
			t.Fatal("no cleanup owner was enrolled for B's slot")
		}
		// B's drain completes it; the worker finalizes. A's own drain then finds its token gone.
		cur, smsg, _ = w.e.asm.loadInflightRecord(w.seed.subject)
		w.e.sess.mu.Lock()
		gen := w.e.sess.gen
		w.e.sess.mu.Unlock()
		if _, err := w.e.asm.jsa.opCompleteExchangeDrain(&mqttCompleteExchangeDrainRequest{
			ID: w.e.sess.id, Epoch: w.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-peer-drain",
			Gen: gen, Nonce: w.seed.lin, Token: swapped.ActionToken, Register: cur, RegisterSeq: smsg.Sequence,
		}); err != nil {
			t.Fatalf("B's drain: %v", err)
		}
		checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
			if slot := testRealSlot(t, w.e, w.seed); slot != nil {
				return fmt.Errorf("the slot stands at %q", slot.State)
			}
			return nil
		})
		w.e.sess.startActionDrain(w.e.c, actA)
	})
	t.Run("a worker's retained re-offer hands the exclusion on", func(t *testing.T) {
		// Mutation M23: the worker's settle is a blanket releaseTerminalReservation -> the
		// retained slot's identifier is not excluded.
		//
		// THE RECOVERY-REACHED SHAPE: no PUBREL was sent in this process (a send's own drain
		// would install an exclusion and mask the one under test), and the worker holds only the
		// placeholder it installed itself.
		t.Parallel()
		e := testNewInflightEnv(t, "f105-t12d")
		testReadyForRealPubRelDelivery(t, e)
		stored, err := e.asm.jsa.storeMsg(mqttStreamSubjectPrefix+"f092.source", 0, []byte("source"))
		if err != nil {
			t.Fatal(err)
		}
		seed := testSeedRealPubRelReadyAt(t, e, stored.Sequence)
		e.sess.mu.Lock()
		delete(e.sess.pendingPubRel, seed.pi)
		excludedBefore := e.sess.slotExclusionInstLocked(seed.pi) != 0
		e.sess.mu.Unlock()
		if excludedBefore {
			t.Fatal("the fixture must start with the identifier unexcluded, or the assertion below proves nothing")
		}
		// The completion lands; its read-back fails once, so the cleanup is unsettled.
		var armed, failed atomic.Bool
		e.asm.mu.Lock()
		e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			out, err := write()
			if err == nil && testF105IsPubCompAdvance(req, seed.lin) {
				armed.Store(true)
			}
			return out, err
		}
		e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
			if subject == seed.subject && armed.Load() && failed.CompareAndSwap(false, true) {
				return nil, nil, errors.New("test: the post-completion read fails once")
			}
			return e.asm.jsa.loadInflightRecord(subject)
		}
		e.asm.mu.Unlock()
		t.Cleanup(func() {
			e.asm.mu.Lock()
			e.asm.testAckAdvance, e.asm.testInflightLoad = nil, nil
			e.asm.mu.Unlock()
		})
		ref := testF105SlotRefAt(t, e.sess, seed.pi, mqttInflightPubRelReady, false)
		res := e.sess.mqttTerminalizeOnPubComp(e.c, ref)
		if !failed.Load() || !res.recorded || res.outcome != mqttTerminalIndeterminate {
			t.Fatalf("the fixture did not produce a recorded, unsettled completion: %+v", res)
		}
		e.sess.ownTerminalCleanup(e.c, ref)
		checkFor(t, 3*mqttActionReconcileDelay, 20*time.Millisecond, func() error {
			e.sess.mu.Lock()
			defer e.sess.mu.Unlock()
			if len(e.sess.pubCompOwed) != 0 {
				return fmt.Errorf("the worker has not settled")
			}
			if p := e.sess.pendingPubRel[seed.pi]; p != nil {
				return fmt.Errorf("the reservation remains: %+v", p)
			}
			if e.sess.slotExclusionInstLocked(seed.pi) == 0 {
				return fmt.Errorf("the retained slot's identifier is not excluded")
			}
			return nil
		})
		if slot := testRealSlot(t, e, seed); slot == nil || slot.State != mqttInflightPubRelTerminal {
			t.Fatalf("the retained slot is %+v", slot)
		}
	})
}

// T11. PUBCOMP's batch losing the authority CAS to an unrelated authority writer re-attempts.
func TestF105PubCompReattemptsAfterAnAuthorityWrite(t *testing.T) {
	t.Parallel()
	w := testNewPubCompWorld(t, "f105-pubcomp-contention")
	var calls atomic.Int32
	var firstRefused atomic.Bool
	var inPubComp atomic.Bool
	w.e.asm.mu.Lock()
	w.e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if testF105IsPubCompAdvance(req, w.seed.lin) {
			inPubComp.Store(true)
			defer inPubComp.Store(false)
		}
		return write()
	}
	w.e.asm.mu.Unlock()
	// The contention lands between the op's authority read and its commit: an unrelated
	// authority writer (a second delivery's admission) moves the authority's revision.
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if !inPubComp.Load() || calls.Add(1) != 1 {
			return send()
		}
		inPubComp.Store(false)
		w.e.trackAdmitted(t, testRealPubRelSrcDur, 99, 1)
		inPubComp.Store(true)
		seq, err := send()
		firstRefused.Store(err != nil)
		return seq, err
	}
	w.e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { w.e.asm.jsa.testTransitionCommit.Store(nil) })
	w.e.c.mqttProcessPubComp(w.seed.pi)
	if !firstRefused.Load() {
		t.Fatal("the first PUBCOMP batch was not refused, so the contention was never exercised")
	}
	if got := testRealExchangePhase(t, w.e, w.seed); got != mqttExchangeTerminal {
		t.Fatalf("the re-attempted PUBCOMP left the exchange at phase %d", got)
	}
}
