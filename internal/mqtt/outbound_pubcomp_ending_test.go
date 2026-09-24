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
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The F-105 PUBCOMP arm: an ordinary QoS 2 exchange ends, durably, with the client's PUBCOMP —
// the exchange and its slot in one batch — and a live PUBREL action makes that ending
// pending-drain until the holder's own drain completes both records.

// testF105PubRelReady drives an ordinary QoS 2 delivery to the state PUBCOMP answers: the client
// PUBREC'd, the PUBREL was written, and the slot stands at pubrel_ready with no action live.
func testF105PubRelReady(t *testing.T, o *Options, s *Server, clientID string) (net.Conn, *mqttReader,
	*mqttSession, *client, uint16, string, mqttInflightSlotRef) {
	t.Helper()
	sub, subr, sess, pi, nonce := testF105QoS2Delivered(t, o, s, clientID)
	testMQTTSendPIPacket(mqttPacketPubRec, t, sub, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, subr, pi)
	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangePubRecObserved)
	ref := testF105SlotRefAt(t, sess, pi, mqttInflightPubRelReady, false)
	sess.mu.Lock()
	c := sess.c
	sess.mu.Unlock()
	return sub, subr, sess, c, pi, nonce, ref
}

// testF105SlotSubject is the session's in-flight subject for pi.
func testF105SlotSubject(sess *mqttSession, pi uint16) string {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return mqttInflightSubject(getHash(sess.id), sess.gen, pi)
}

// testF105SlotRefAt waits for the slot to stand at want (with a live token or not) and returns a
// reference captured from it.
func testF105SlotRefAt(t *testing.T, sess *mqttSession, pi uint16, want mqttInflightState, token bool) mqttInflightSlotRef {
	t.Helper()
	subj := testF105SlotSubject(sess, pi)
	var ref *mqttInflightSlotRef
	checkFor(t, 3*time.Second, 10*time.Millisecond, func() error {
		cur, smsg, err := sess.asm.loadInflightRecord(subj)
		if err != nil || cur == nil || smsg == nil {
			return fmt.Errorf("the slot does not load: %+v (%v)", cur, err)
		}
		if cur.State != want || (cur.ActionToken != _EMPTY_) != token {
			return fmt.Errorf("the slot stands at %q with token %q, want %q (token=%v)",
				cur.State, cur.ActionToken, want, token)
		}
		ref = mqttSlotRefFromRecord(subj, smsg.Sequence, cur)
		if ref == nil {
			return fmt.Errorf("no reference from the slot")
		}
		return nil
	})
	return *ref
}

// testF105SlotGone waits for the slot to be deleted.
func testF105SlotGone(t *testing.T, sess *mqttSession, pi uint16, within time.Duration) {
	t.Helper()
	subj := testF105SlotSubject(sess, pi)
	checkFor(t, within, 10*time.Millisecond, func() error {
		cur, smsg, err := sess.asm.loadInflightRecord(subj)
		if err != nil {
			return err
		}
		if smsg != nil {
			return fmt.Errorf("the slot still stands: %+v", cur)
		}
		return nil
	})
}

// testF105Verdict is what recovery would conclude about the exchange.
func testF105Verdict(sess *mqttSession, nonce string) mqttDutyVerdict {
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	return sess.jsa.classifyOutstandingDuty(sess.id, sess.asm.sessStreamEpoch, gen,
		&mqttDutyEntry{Kind: mqttDutyExchange, Ref: nonce})
}

// testF105PubCompSeams records what the PUBCOMP path did: every canonical-PUBREL ack it sent and
// every completion-worker enrolment.
type testF105PubCompSeams struct {
	mu        sync.Mutex
	acks      []string
	workerAck int
	enrolled  int
	created   int
}

func testF105ObservePubComp(t *testing.T, sess *mqttSession) *testF105PubCompSeams {
	t.Helper()
	o := &testF105PubCompSeams{}
	asm := sess.asm
	asm.mu.Lock()
	asm.testPubCompAck = func(subject string, byWorker bool) {
		o.mu.Lock()
		o.acks = append(o.acks, subject)
		if byWorker {
			o.workerAck++
		}
		o.mu.Unlock()
	}
	asm.testPubCompEnrolled = func(pi uint16, created bool) {
		o.mu.Lock()
		o.enrolled++
		if created {
			o.created++
		}
		o.mu.Unlock()
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testPubCompAck, asm.testPubCompEnrolled = nil, nil
		asm.mu.Unlock()
	})
	return o
}

func (o *testF105PubCompSeams) counts() (acks, byWorker, enrolled, created int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.acks), o.workerAck, o.enrolled, o.created
}

// testF105SetAckAdvance installs the acknowledgement-advance seam for the test's life.
func testF105SetAckAdvance(t *testing.T, sess *mqttSession,
	hook func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)) {
	t.Helper()
	asm := sess.asm
	asm.mu.Lock()
	asm.testAckAdvance = hook
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testAckAdvance = nil
		asm.mu.Unlock()
	})
}

// testF105IsPubCompAdvance says whether an ack-advance request is THIS exchange's PUBCOMP.
func testF105IsPubCompAdvance(req *mqttAdvanceExchangeAckRequest, nonce string) bool {
	return req.Nonce == nonce &&
		(req.To == mqttExchangeTerminal || req.To == mqttExchangeTerminalPendingDrain) &&
		req.Register != nil && req.Register.State == mqttInflightPubRelReady
}

// T1. The client's PUBCOMP ends the ordinary exchange durably: the exchange reaches Terminal, the
// verdict is neither "release owed" nor "drain owed", the slot is disposed, and the identifier is
// free for the next delivery.
//
// Mutation M1: PUBCOMP's exchange arm back to the slot-only CAS -> the exchange stays at
// PUBREC-observed and the verdict is release owed. M10: the destination-only arrow map -> the
// PUBCOMP member becomes publish_terminal and is refused.
func TestF105OrdinaryPubCompEndsTheExchange(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	sub, subr, sess, _, pi, nonce, _ := testF105PubRelReady(t, o, s, "f105-pubcomp-ends")
	seams := testF105ObservePubComp(t, sess)

	testMQTTSendPIPacket(mqttPacketPubComp, t, sub, pi)
	testMQTTFlush(t, sub, nil, subr)

	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
	if v := testF105Verdict(sess, nonce); v.Disposition != mqttDutyMayResolve {
		t.Fatalf("a completed exchange reads as %+v, want a durable ending (neither release nor drain owed)", v)
	}
	testF105SlotGone(t, sess, pi, time.Second)
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if n := sess.qos12InflightForCap(); n != 0 || len(sess.pendingPubRel) != 0 {
			return fmt.Errorf("capacity %d and %d pending PUBRELs remain after the PUBCOMP", n, len(sess.pendingPubRel))
		}
		return nil
	})
	if acks, _, enrolled, _ := seams.counts(); acks != 1 || enrolled != 0 {
		t.Fatalf("an inline completion sent %d acks and enrolled %d workers, want 1 and 0", acks, enrolled)
	}
}

// T2. A PUBCOMP arriving while this broker's PUBREL action is still live ends the exchange
// PENDING-DRAIN, keeps the token, custody and transmission on the terminal slot, and the
// holder's own drain then completes both records INLINE.
//
// Mutation M2: OutstandingOutput hard-wired false -> the boundary refuses Terminal over a live
// token and no pending-drain is recorded. M4: the drain completion kept slot-only -> the
// exchange stays pending-drain.
func TestF105PubCompOverALivePubRelActionPendsItsDrain(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-pubcomp-pending")
	seams := testF105ObservePubComp(t, sess)

	act := sess.mqttAcquirePubRelAction(c, ref)
	if act == nil {
		t.Fatalf("the PUBREL action was not acquired")
	}
	live := testF105SlotRefAt(t, sess, pi, mqttInflightPubRelReady, true)
	before, _, _ := sess.asm.loadInflightRecord(live.name.subject)

	res := sess.mqttTerminalizeOnPubComp(c, live)
	if !res.recorded || res.outcome != mqttTerminalHeldByAction || res.heldToken != act.token {
		t.Fatalf("PUBCOMP over a live action concluded %+v, want recorded HeldByAction naming token %q", res, act.token)
	}
	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminalPendingDrain)
	after, _, _ := sess.asm.loadInflightRecord(live.name.subject)
	if after == nil || after.State != mqttInflightPubRelTerminal || after.ActionToken != before.ActionToken ||
		after.ActionClaim != before.ActionClaim || after.ActionClaimSeq != before.ActionClaimSeq || after.Acq != before.Acq {
		t.Fatalf("the PUBCOMP'd slot is %+v, want pubrel_terminal carrying the SAME action (%+v)", after, before)
	}
	if v := testF105Verdict(sess, nonce); v.Disposition != mqttDutyOwesWork || v.Work != mqttDutyWorkDrain {
		t.Fatalf("a pending-drain completion reads as %+v, want the drain owed", v)
	}

	start := time.Now()
	sess.startActionDrain(c, act)
	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
	testF105SlotGone(t, sess, pi, mqttActionReconcileDelay/2)
	if time.Since(start) >= mqttActionReconcileDelay {
		t.Fatalf("the drain's cleanup waited for a reconcile timer rather than running inline")
	}
	if _, _, enrolled, _ := seams.counts(); enrolled != 0 {
		t.Fatalf("the inline drain completion enrolled %d cleanup workers, want 0", enrolled)
	}
	if v := testF105Verdict(sess, nonce); v.Disposition != mqttDutyMayResolve {
		t.Fatalf("the drained exchange reads as %+v, want a durable ending", v)
	}
}

// T2b. The drain's finalize needs the SLOT's sequence (the op returns the authority's), and an
// unsettled finalize goes to the completion worker even with a delivered entry standing.
//
// Mutation M16: the drain hands mqttOpOutcome.Seq to finalize -> (a)'s freshness fence refuses
// and no inline delete happens. M17: the drain's indeterminate finalize goes to
// retainAndReconcileTerminal -> (c) never retries over the delivered entry.
func TestF105DrainCompletionFinalizesFromTheSlotSequence(t *testing.T) {
	for _, tc := range []struct {
		name string
		// failReads is how many reads of the slot, after the drain's batch, fail.
		failPostBatchRead bool
		failDisposal      bool
	}{
		{name: "inline"},
		{name: "post-batch read fails", failPostBatchRead: true},
		{name: "finalize fails after a good read", failDisposal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-drain-seq-"+strings.ReplaceAll(tc.name, " ", "-"))
			seams := testF105ObservePubComp(t, sess)
			act := sess.mqttAcquirePubRelAction(c, ref)
			if act == nil {
				t.Fatalf("the PUBREL action was not acquired")
			}
			live := testF105SlotRefAt(t, sess, pi, mqttInflightPubRelReady, true)
			if res := sess.mqttTerminalizeOnPubComp(c, live); !res.recorded || res.outcome != mqttTerminalHeldByAction {
				t.Fatalf("PUBCOMP over a live action concluded %+v", res)
			}
			// A DELIVERED entry stands for the identifier, as a redelivered PUBREL leaves it: the
			// shape retainAndReconcileTerminal refuses to enrol over.
			sess.mu.Lock()
			sess.pendingPubRel[pi] = &mqttPending{jsAckSubject: "$JS.ACK.test.delivered", slot: live}
			sess.mu.Unlock()

			var armed atomic.Bool
			var failed atomic.Int32
			asm := sess.asm
			asm.mu.Lock()
			asm.testDrainComplete = func(req *mqttCompleteExchangeDrainRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				out, err := write()
				armed.Store(err == nil)
				if err == nil && tc.failDisposal {
					// The finalize's complete PUBREL scan fails ONCE after the batch landed, so
					// the first finalize is Indeterminate after a good slot read.
					asm.mu.Lock()
					asm.testInflightPubRelScan = func(string) ([]mqttStoredPubRel, int, bool) {
						asm.mu.Lock()
						asm.testInflightPubRelScan = nil
						asm.mu.Unlock()
						return nil, 0, false
					}
					asm.mu.Unlock()
				}
				return out, err
			}
			asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
				if tc.failPostBatchRead && armed.Load() && failed.Add(1) == 1 {
					return nil, nil, errors.New("test: the post-batch read fails")
				}
				return asm.jsa.loadInflightRecord(subject)
			}
			asm.mu.Unlock()
			t.Cleanup(func() {
				asm.mu.Lock()
				asm.testDrainComplete, asm.testInflightLoad, asm.testInflightPubRelScan = nil, nil, nil
				asm.mu.Unlock()
			})

			sess.startActionDrain(c, act)
			testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
			if !tc.failPostBatchRead && !tc.failDisposal {
				testF105SlotGone(t, sess, pi, mqttActionReconcileDelay/2)
				if _, _, enrolled, _ := seams.counts(); enrolled != 0 {
					t.Fatalf("an inline drain completion enrolled %d workers", enrolled)
				}
				return
			}
			if _, _, enrolled, _ := seams.counts(); enrolled != 1 {
				t.Fatalf("the unsettled drain cleanup enrolled %d completion workers, want 1", enrolled)
			}
			testF105SlotGone(t, sess, pi, 3*mqttActionReconcileDelay)
		})
	}
}

// T3. PUBCOMP and the PUBREL drain in every commit order end the exchange at Terminal.
func TestF105PubCompAndTheDrainInEveryOrder(t *testing.T) {
	t.Run("the drain commits first", func(t *testing.T) {
		t.Parallel()
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-order-drain-first")
		act := sess.mqttAcquirePubRelAction(c, ref)
		if act == nil {
			t.Fatalf("the PUBREL action was not acquired")
		}
		live := testF105SlotRefAt(t, sess, pi, mqttInflightPubRelReady, true)
		var calls atomic.Int32
		var firstRefused atomic.Bool
		testF105SetAckAdvance(t, sess, func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if !testF105IsPubCompAdvance(req, nonce) {
				return write()
			}
			if calls.Add(1) == 1 {
				if req.To != mqttExchangeTerminalPendingDrain {
					t.Errorf("the first PUBCOMP attempt derived phase %d over a live token", req.To)
				}
				// THE DRAIN LANDS FIRST, between PUBCOMP's read and its commit.
				sess.startActionDrain(c, act)
				out, err := write()
				firstRefused.Store(err != nil)
				return out, err
			}
			if req.To != mqttExchangeTerminal {
				t.Errorf("after the drain, PUBCOMP derived phase %d, want Terminal", req.To)
			}
			return write()
		})
		res := sess.mqttTerminalizeOnPubComp(c, live)
		if !firstRefused.Load() || calls.Load() < 2 {
			t.Fatalf("the forced order did not happen (first refused=%v, attempts=%d)", firstRefused.Load(), calls.Load())
		}
		if !res.recorded {
			t.Fatalf("PUBCOMP after the drain is unrecorded: %+v", res)
		}
		testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
		testF105SlotGone(t, sess, pi, time.Second)
	})

	t.Run("PUBCOMP commits while the drain holds a stale read", func(t *testing.T) {
		t.Parallel()
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-order-pubcomp-first")
		act := sess.mqttAcquirePubRelAction(c, ref)
		if act == nil {
			t.Fatalf("the PUBREL action was not acquired")
		}
		live := testF105SlotRefAt(t, sess, pi, mqttInflightPubRelReady, true)
		var pubcompRan, drainRefused, paired atomic.Bool
		asm := sess.asm
		asm.mu.Lock()
		asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
			if subject == live.name.subject && r.State == mqttInflightPubRelReady && r.ActionToken == _EMPTY_ &&
				pubcompRan.CompareAndSwap(false, true) {
				// The drain read pubrel_ready and is about to clear its token there: PUBCOMP
				// commits first.
				if res := sess.mqttTerminalizeOnPubComp(c, live); !res.recorded || res.outcome != mqttTerminalHeldByAction {
					t.Errorf("the interleaved PUBCOMP concluded %+v", res)
				}
				seq, err := asm.jsa.storeInflightCAS(subject, expectSeq, r)
				drainRefused.Store(err != nil)
				return seq, err
			}
			return asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		asm.testDrainComplete = func(req *mqttCompleteExchangeDrainRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			paired.Store(true)
			return write()
		}
		asm.mu.Unlock()
		t.Cleanup(func() {
			asm.mu.Lock()
			asm.testInflightStore, asm.testDrainComplete = nil, nil
			asm.mu.Unlock()
		})

		sess.startActionDrain(c, act)
		if !pubcompRan.Load() || !drainRefused.Load() || !paired.Load() {
			t.Fatalf("the forced order did not happen (pubcomp=%v stale drain refused=%v paired drain=%v)",
				pubcompRan.Load(), drainRefused.Load(), paired.Load())
		}
		testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
		testF105SlotGone(t, sess, pi, time.Second)
	})
}

// T3b. An acquisition racing PUBCOMP, in both commit orders.
func TestF105PubRelAcquisitionAgainstPubComp(t *testing.T) {
	t.Run("the acquisition commits first", func(t *testing.T) {
		t.Parallel()
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-acq-first")
		var act *mqttSendAction
		var calls atomic.Int32
		var firstRefused atomic.Bool
		testF105SetAckAdvance(t, sess, func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if !testF105IsPubCompAdvance(req, nonce) {
				return write()
			}
			if calls.Add(1) == 1 {
				act = sess.mqttAcquirePubRelAction(c, ref)
				out, err := write()
				firstRefused.Store(err != nil)
				return out, err
			}
			return write()
		})
		res := sess.mqttTerminalizeOnPubComp(c, ref)
		if act == nil || !firstRefused.Load() {
			t.Fatalf("the forced order did not happen (acquired=%v, first refused=%v)", act != nil, firstRefused.Load())
		}
		if !res.recorded || res.outcome != mqttTerminalHeldByAction || res.heldToken != act.token {
			t.Fatalf("PUBCOMP after the acquisition concluded %+v, want pending-drain over the new token", res)
		}
		testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminalPendingDrain)
		sess.startActionDrain(c, act)
		testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
		testF105SlotGone(t, sess, pi, time.Second)
	})

	t.Run("PUBCOMP commits first", func(t *testing.T) {
		t.Parallel()
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-pubcomp-before-acq")
		var pubcompRan, acqRefused atomic.Bool
		asm := sess.asm
		asm.mu.Lock()
		asm.testInflightStore = func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
			if subject == ref.name.subject && r.ActionToken != _EMPTY_ && pubcompRan.CompareAndSwap(false, true) {
				if res := sess.mqttTerminalizeOnPubComp(c, ref); !res.recorded {
					t.Errorf("the interleaved PUBCOMP is unrecorded: %+v", res)
				}
				seq, err := asm.jsa.storeInflightCAS(subject, expectSeq, r)
				acqRefused.Store(err != nil)
				return seq, err
			}
			return asm.jsa.storeInflightCAS(subject, expectSeq, r)
		}
		asm.mu.Unlock()
		t.Cleanup(func() {
			asm.mu.Lock()
			asm.testInflightStore = nil
			asm.mu.Unlock()
		})
		if act := sess.mqttAcquirePubRelAction(c, ref); act != nil {
			t.Fatalf("a PUBREL action was acquired over a completed exchange")
		}
		if !pubcompRan.Load() || !acqRefused.Load() {
			t.Fatalf("the forced order did not happen (pubcomp=%v acquisition refused=%v)", pubcompRan.Load(), acqRefused.Load())
		}
		testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
		testF105SlotGone(t, sess, pi, time.Second)
		sess.mu.Lock()
		h := sess.sendsInFlight[pi]
		liveHolder := h != nil && h.action != nil && !h.action.drained
		sess.mu.Unlock()
		if liveHolder {
			t.Fatalf("the refused acquisition left a live local holder")
		}
	})
}

// T3c. Two completers of one exchange: exactly one transition lands, the other re-reads the
// terminal slot and reports the completion recorded.
func TestF105TwoCompletersOfOneExchange(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-two-completers")
	var calls, landed atomic.Int32
	var nested atomic.Bool
	testF105SetAckAdvance(t, sess, func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		if !testF105IsPubCompAdvance(req, nonce) {
			return write()
		}
		if calls.Add(1) == 1 {
			// THE SECOND COMPLETER runs to completion between the first one's read and commit.
			nested.Store(true)
			if res := sess.mqttTerminalizeOnPubComp(c, ref); !res.recorded {
				t.Errorf("the second completer is unrecorded: %+v", res)
			}
		}
		out, err := write()
		if err == nil {
			landed.Add(1)
		}
		return out, err
	})
	res := sess.mqttTerminalizeOnPubComp(c, ref)
	if !nested.Load() {
		t.Fatalf("the second completer never ran")
	}
	if !res.recorded {
		t.Fatalf("the first completer, having lost, is unrecorded: %+v", res)
	}
	if n := landed.Load(); n != 1 {
		t.Fatalf("%d PUBCOMP transitions landed for one exchange, want exactly 1", n)
	}
	testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
	testF105SlotGone(t, sess, pi, time.Second)
}

// T4. A lost response — on the first attempt, or on the final one — is resolved INLINE; the
// three-attempt budget completes inline when only the third attempt lands.
//
// Mutation M12: the exhaustion re-read deleted -> "final attempt lost" enrols a worker. M9: a
// single attempt -> "third attempt lands" is unrecorded inline and enrols a worker.
func TestF105PubCompResolvesALostResponseInline(t *testing.T) {
	for _, tc := range []struct {
		name string
		// plan per attempt: "refuse" (no write), "lost" (write, then report an error), "ok".
		plan []string
	}{
		{"first attempt lost", []string{"lost"}},
		{"final attempt lost", []string{"refuse", "refuse", "lost"}},
		{"third attempt lands", []string{"refuse", "refuse", "ok"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			_, _, sess, c, pi, nonce, ref := testF105PubRelReady(t, o, s, "f105-lost-"+strings.ReplaceAll(tc.name, " ", "-"))
			seams := testF105ObservePubComp(t, sess)
			var calls atomic.Int32
			testF105SetAckAdvance(t, sess, func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				if !testF105IsPubCompAdvance(req, nonce) {
					return write()
				}
				n := int(calls.Add(1))
				step := "ok"
				if n <= len(tc.plan) {
					step = tc.plan[n-1]
				}
				switch step {
				case "refuse":
					return mqttOpOutcome{}, errors.New("test: refused without writing")
				case "lost":
					if _, err := write(); err != nil {
						t.Errorf("the write behind the lost response failed: %v", err)
					}
					return mqttOpOutcome{}, errors.New("test: response lost")
				}
				return write()
			})
			res := sess.mqttTerminalizeOnPubComp(c, ref)
			if int(calls.Load()) != len(tc.plan) {
				t.Fatalf("PUBCOMP made %d attempts, want %d", calls.Load(), len(tc.plan))
			}
			if !res.recorded {
				t.Fatalf("the completion is unrecorded inline: %+v", res)
			}
			if _, _, enrolled, _ := seams.counts(); enrolled != 0 {
				t.Fatalf("an inline completion enrolled %d workers", enrolled)
			}
			testF105ExchangePhaseIs(t, sess, nonce, mqttExchangeTerminal)
			testF105SlotGone(t, sess, pi, time.Second)
		})
	}
}

// T8. The retired PUBREL-owed phase value is refused, never read as PUBREC-observed.
//
// Mutation M7: re-admit 3 to mqttValidExchangePhase and the raw record decodes.
func TestF105RetiredPubRelOwedPhaseIsRefused(t *testing.T) {
	x := &mqttOutboundExchange{
		ID: "c", Epoch: "e", Gen: "g1", Nonce: "n1", PI: 7, QoS: 2,
		Phase: mqttExchangePubRecObserved, AdmitTxn: "t1", Tenure: "ten1",
	}
	good, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	if _, derr := mqttDecodeOutboundExchange(good, "c", "e", "g1", "n1"); derr != nil {
		t.Skipf("the fixture exchange is not otherwise valid, so the refusal below would prove nothing: %v", derr)
	}
	x.Phase = mqttExchangePhase(3)
	bad, _ := json.Marshal(x)
	if _, derr := mqttDecodeOutboundExchange(bad, "c", "e", "g1", "n1"); derr == nil {
		t.Fatalf("a persisted exchange at the retired phase 3 decoded")
	}
	if mqttValidExchangePhase(mqttExchangePhase(3)) {
		t.Fatalf("the retired phase 3 is a valid phase")
	}
	if err := mqttActionPermittedAtPhase(2, mqttExchangePhase(3), mqttActionPubRel); err == nil {
		t.Fatalf("the retired phase 3 permits a release")
	}
}
