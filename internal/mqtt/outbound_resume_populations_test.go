// Copyright 2025 The mqtt-nats-broker Authors
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
	"strings"
	"sync/atomic"
	"testing"
)

// The resume's populations beyond the inventory root (F-105 7b, plan-7b §3.2, §3.6): terminal
// duties still count, a PUBREL record alone authorizes nothing, a resume whose entitlement is
// superseded mid-walk is refused, and a withdrawal cut before its register delete is finished.

// T2. An abandoned exchange whose duty is TERMINAL keeps its identifier (its Receive Maximum
// charge was released at the abandonment, Spec5 §4.9, and must not come back).
//
// Mutation M2: terminal duties skipped by the traversal -> the identifier is handed out.
func TestF105ResumeKeepsAnAbandonedExchangesIdentifier(t *testing.T) {
	t.Parallel()
	f := testAbandonAdmitted(t, "t2-abandoned", 1)
	if r := f.deny(); r != mqttAbandonDone {
		t.Fatalf("fixture: the deny must abandon: %v", r)
	}
	f.requireAbandoned(t)
	e := f.e
	ord := testDutyOrdinalOf(t, e, f.gen, f.lin)
	d, _, err := e.asm.jsa.loadDutyEntryAt(e.sess.id, e.asm.sessStreamEpoch, f.gen, ord)
	if err != nil || d == nil {
		t.Fatalf("loading the duty: %v", err)
	}
	if d.State != mqttDutyTerminal {
		next := *d
		next.State, next.Outcome, next.Claim, next.LeaseUntil = mqttDutyTerminal, "test-terminal", _EMPTY_, 0
		body, _ := json.Marshal(&next)
		if _, err := e.asm.jsa.storeMsg(mqttDutySubject(e.sess.idHash, f.gen, ord), -1, body); err != nil {
			t.Fatalf("terminating the duty: %v", err)
		}
	}
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	held, charged := e.sess.custody.holdsPI(f.pi), e.sess.custody.chargesPI(f.pi)
	e.sess.mu.Unlock()
	if !held {
		t.Fatalf("an abandoned exchange behind a terminal duty must keep identifier %d", f.pi)
	}
	if charged {
		t.Fatal("the abandoned exchange's Receive Maximum charge must stay released")
	}
	if got := testCustodyMints(e.sess, f.pi); got == f.pi {
		t.Fatalf("identifier %d was handed out", f.pi)
	}
}

// T6. An ordinary PUBREL record visible only to the PUBREL scan — no register, no exchange —
// authorizes nothing: no actionable PUBREL entry is installed from it.
//
// Mutation M5: the PUBREL scan installs ordinary work.
func TestF105ResumePubRelRecordAloneAuthorizesNothing(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "t6-pubrel-alone")
	testReadyForRealPubRelDelivery(t, e)
	const pi = uint16(5)
	testStoreCanonical(t, e, pi, mqttNewInflightNonce(), testRealPubRelSrcSeq)
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	e.sess.mu.Lock()
	rel := e.sess.pendingPubRel[pi]
	e.sess.mu.Unlock()
	if rel != nil && !rel.regRecoveryPlaceholder && !rel.regRecoveryOpaque && !rel.regForeign {
		t.Fatalf("a PUBREL record alone must install no actionable ordinary work: %+v", rel)
	}
}

// T13 (M17a): a takeover on another broker that lands between the resume's walk and its apply —
// its notification withheld — refuses the resume: no sendable entry is installed.
func TestF105ResumeRefusedWhenTheEntitlementIsSupersededMidWalk(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "t13-remote", 1)
	testRestart(f.e)
	var loads atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testResumeAuthorityLoad = func(id string) mqttAuthorityLoad {
		if loads.Add(1) == 2 {
			testSupersedeTenure(t, f.e)
		}
		return f.e.asm.jsa.loadOutboundAuthority(id, f.e.asm.sessStreamEpoch)
	}
	f.e.asm.mu.Unlock()
	t.Cleanup(func() { f.e.asm.mu.Lock(); f.e.asm.testResumeAuthorityLoad = nil; f.e.asm.mu.Unlock() })
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err == nil {
		t.Fatal("a resume whose entitlement was superseded before apply must be refused")
	}
	if testProjectionPresent(f) {
		t.Fatal("a refused resume installs no sendable entry")
	}
	if cur, _ := f.register(t); cur == nil || cur.Cancelled {
		t.Fatalf("a refused resume makes no owner-dependent durable write: %+v", cur)
	}
}

// T13 (M17b): the local binding check. A same-object rebind during the walk — the session now
// bound to another connection — refuses the stale resume's apply. (A production CONNECT cannot
// schedule this; the client-ID gate holds through restoration. It is the check's unit test.)
func TestF105ResumeRefusedAfterALocalRebindMidWalk(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "t13-local", 1)
	testRestart(f.e)
	var loads atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testResumeAuthorityLoad = func(id string) mqttAuthorityLoad {
		if loads.Add(1) == 2 {
			c2 := &client{mqtt: &mqtt{asm: f.e.asm, sess: f.e.sess}}
			f.e.sess.mu.Lock()
			f.e.sess.c = c2
			f.e.sess.mu.Unlock()
		}
		return f.e.asm.jsa.loadOutboundAuthority(id, f.e.asm.sessStreamEpoch)
	}
	f.e.asm.mu.Unlock()
	t.Cleanup(func() { f.e.asm.mu.Lock(); f.e.asm.testResumeAuthorityLoad = nil; f.e.asm.mu.Unlock() })
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err == nil {
		t.Fatal("a resume whose session was rebound to another connection must be refused")
	}
	if testProjectionPresent(f) {
		t.Fatal("a refused resume installs no sendable entry")
	}
}

// T19, the withdrawal cut: the withdrawal (exchange WithdrawnUnsent, register tombstone) committed
// and the broker died before the register delete. The resume finishes it: the identifier is free
// and nothing is restored.
func TestF105ResumeFinishesAWithdrawalCutBeforeItsDelete(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "t19-withdraw-cut", 1)
	testEndExchangeRemotely(t, f)
	testRestart(f.e)
	if _, err := f.e.sess.recoverInflightRegisters(f.e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if testProjectionPresent(f) {
		t.Fatal("a withdrawn obligation must not be restored")
	}
	f.e.sess.mu.Lock()
	_, reserved := f.e.sess.pendingPublish[f.pi]
	f.e.sess.mu.Unlock()
	if reserved {
		t.Fatal("the withdrawn obligation's identifier must not stay reserved")
	}
	f.e.waitRegisterCount(t, 0)
	if got := testCustodyMints(f.e.sess, f.pi); got != f.pi {
		t.Fatalf("the identifier must be free, got %d", got)
	}
}

// T11. Owner-INDEPENDENT work still lands after a takeover: the client's own PUBACK, captured by
// the old connection, completes the exchange whatever tenure serves, because its justification
// is the exchange's evidence (identity, phase, acquisition), not the connection's right to decide.
// The negative fixture exercises ONE fence: a PUBACK naming the wrong acquisition is refused.
//
// Mutations M14 (entitlement equality added to opAdvanceExchangeAck -> the captured PUBACK is
// refused) and M15a (the ack advance's Acq fence removed -> the wrong acquisition lands).
func TestF105OwnerIndependentAckLandsAfterATakeover(t *testing.T) {
	t.Parallel()
	t.Run("captured PUBACK completes", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "t11-puback", 1)
		f.e.transmit(t, f.pi, testRealPubRelSrcDur, f.sseq, true)
		testSupersedeTenure(t, f.e)
		if err := f.e.c.mqttProcessPubAck(f.pi); err != nil {
			t.Fatalf("PUBACK: %v", err)
		}
		if x := f.exchange(t); x == nil || !mqttExchangePhaseEnded(x.Phase) {
			t.Fatalf("the client's PUBACK must end the exchange after a takeover: %+v", x)
		}
	})
	t.Run("a wrong acquisition is refused", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "t11-wrong-acq", 1)
		f.e.transmit(t, f.pi, testRealPubRelSrcDur, f.sseq, true)
		testSupersedeTenure(t, f.e)
		cur, seq := f.register(t)
		if _, err := f.e.asm.jsa.opAdvanceExchangeAck(&mqttAdvanceExchangeAckRequest{
			ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack",
			Gen: f.gen, Nonce: f.lin, To: mqttExchangeTerminal, Acq: cur.Acq + 1,
			Register: cur, RegisterSeq: seq,
		}); err == nil {
			t.Fatal("a PUBACK naming another acquisition must be refused")
		}
		if x := f.exchange(t); x == nil || mqttExchangePhaseEnded(x.Phase) {
			t.Fatalf("the refused advance must leave the exchange outstanding: %+v", x)
		}
	})
}

// T13 (M30): a refused resume leaves the drains exactly as they stood. The drain recomputation is
// COMPUTED during the walk and APPLIED only after apply's authority and binding checks, so a
// resume superseded before apply installs no drain observation.
func TestF105RefusedResumeInstallsNoDrainObservation(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "t13-drain")
	testReadyForRealPubRelDelivery(t, e)
	const sid, pi = "foo", uint16(3)
	e.landAdmission(t, pi, 2, testRealPubRelSrcDur, testRealPubRelSrcSeq, mqttNewInflightNonce())
	cc := &ConsumerConfig{Durable: testRealPubRelSrcDur, DeliverSubject: mqttSubPrefix + "t13.drain"}
	e.sess.mu.Lock()
	e.sess.drains = map[string]*mqttDrain{sid: {
		cc: cc, qos: 2,
		seqs: map[uint64]*drainSeq{testRealPubRelSrcSeq: {claimed: true, pi: pi, regSeq: 999}},
	}}
	e.sess.drainByDur = map[string]*mqttDrain{testRealPubRelSrcDur: e.sess.drains[sid]}
	e.sess.mu.Unlock()
	lostSourceHook(e, mqttInflightSourceLive)
	e.forgetInMemory()
	var loads atomic.Int32
	e.asm.mu.Lock()
	e.asm.testResumeAuthorityLoad = func(id string) mqttAuthorityLoad {
		if loads.Add(1) == 2 {
			testSupersedeTenure(t, e)
		}
		return e.asm.jsa.loadOutboundAuthority(id, e.asm.sessStreamEpoch)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testResumeAuthorityLoad = nil; e.asm.mu.Unlock() })
	if _, err := e.sess.recoverInflightRegisters(e.c); err == nil {
		t.Fatal("fixture: the superseded resume must be refused")
	}
	e.sess.mu.Lock()
	regSeq := e.sess.drains[sid].seqs[testRealPubRelSrcSeq].regSeq
	e.sess.mu.Unlock()
	if regSeq != 999 {
		t.Fatalf("a refused resume must not install its drain observation: regSeq 999 -> %d", regSeq)
	}
}

// T9. A DELAYED ADMISSION and the successor's finalize (its accept, which moves the serving
// tenure), in every reachable order. The admission batch carries its authority CAS, so it lands
// either BEFORE the finalize — below the ledger's Hi, where the successor's inventory reaches it —
// or not at all; and a landing whose response was lost is still found. The (b) order, a landing
// between an early Hi read and finalize, is the CONNECT path's ordering (the traversal runs only
// after driveAcceptToServingAuthority) and is recorded in the plan as verified by reading.
//
// Mutation M8: the admission batch's authority CAS dropped -> (c) lands under a superseded tenure.
func TestF105DelayedAdmissionAgainstTheSuccessorsFinalize(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		wantLanded bool
		around     func(t *testing.T, e *testInflightEnv, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error)
	}{
		{"lands before finalize", true, func(t *testing.T, e *testInflightEnv, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			out, err := write()
			testSupersedeTenure(t, e)
			return out, err
		}},
		{"lands after finalize", false, func(t *testing.T, e *testInflightEnv, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			testSupersedeTenure(t, e)
			return write()
		}},
		{"response lost", true, func(t *testing.T, e *testInflightEnv, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			out, err := write()
			if err != nil {
				return out, err
			}
			testSupersedeTenure(t, e)
			return out, errors.New("test: the admission's response was lost")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := testNewInflightEnv(t, "t9-"+strings.ReplaceAll(tc.name, " ", "-"))
			testReadyForRealPubRelDelivery(t, e)
			e.admissionPrerequisites(t, testRealPubRelSrcDur, 1)
			e.acceptUnderServingAuthority(t)
			var once atomic.Bool
			e.asm.mu.Lock()
			e.asm.testAdmitWrite = func(sess *mqttSession, registerOnly bool, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				if once.CompareAndSwap(false, true) {
					return tc.around(t, e, write)
				}
				return write()
			}
			e.asm.mu.Unlock()
			e.sess.mu.Lock()
			pi, isNew := e.sess.trackPublish(testRealPubRelSrcDur, testInflightAckSubject(testRealPubRelSrcDur, testRealPubRelSrcSeq, 1))
			nonce := e.sess.pendingPublish[pi].regNonce
			e.sess.mu.Unlock()
			if pi == 0 || !isNew {
				t.Fatalf("allocate: pi=%d isNew=%v", pi, isNew)
			}
			e.sess.admitInflightPublish(e.c, pi, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, nonce, _EMPTY_, _EMPTY_)
			e.asm.mu.Lock()
			e.asm.testAdmitWrite = nil
			e.asm.mu.Unlock()
			if !once.Load() {
				t.Fatal("fixture: the admission's write was never reached")
			}
			x, _, _ := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, e.sess.gen, nonce)
			if (x != nil) != tc.wantLanded {
				t.Fatalf("the admission batch: landed=%v, want %v", x != nil, tc.wantLanded)
			}
			// The successor's resume, from the store alone.
			c2 := testServingClient(t, e)
			testRestart(e)
			if _, err := e.sess.recoverInflightRegisters(c2); err != nil {
				t.Fatalf("the successor's resume: %v", err)
			}
			e.sess.mu.Lock()
			held := e.sess.custody.hasExchange(e.sess.gen, nonce)
			e.sess.mu.Unlock()
			if held != tc.wantLanded {
				t.Fatalf("the successor's inventory holds the admission=%v, want %v", held, tc.wantLanded)
			}
		})
	}
}
