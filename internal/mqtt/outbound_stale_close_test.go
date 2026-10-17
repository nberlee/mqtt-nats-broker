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
	"errors"
	"sync/atomic"
	"testing"
)

// A STALE CLOSE DECIDES NOTHING FOR ITS SUCCESSOR (F-105 7b, plan-7b §3.4, T10/T10b/T10d).
// Every cancellation a close decides — the void, the withdrawal, the fallback tombstone — is
// owner-dependent, so it commits only under the entitlement the deciding connection was accepted
// with, compared inside the committing batch. A close whose connection was superseded is refused
// with no destructive fallback, and the registers it would have ended stay for the successor's
// resume, in either order.

// testSuccessor supersedes the serving tenure and returns the successor's connection, accepted
// under it and bound to the session object (the same-object rebind). The env's own connection
// keeps the superseded entitlement: it is the stale closer.
func testSuccessor(t *testing.T, e *testInflightEnv) *client {
	t.Helper()
	testSupersedeTenure(t, e)
	return testServingClient(t, e)
}

// testServingClient returns a connection accepted under the authority as it stands, bound to the
// session object.
func testServingClient(t *testing.T, e *testInflightEnv) *client {
	t.Helper()
	al := e.asm.jsa.loadOutboundAuthority(e.sess.id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil {
		t.Fatalf("no successor authority: %+v", al)
	}
	ent := mqttOutboundEntitlement{Gen: al.rec.Gen, Tenure: al.rec.Tenure}
	c2 := &client{mqtt: &mqtt{asm: e.asm, sess: e.sess}}
	c2.setOutboundEntitlement(ent)
	e.sess.mu.Lock()
	e.sess.c = c2
	e.sess.boundEnt, e.sess.boundEntOK = ent, true
	e.sess.mu.Unlock()
	return c2
}

// testStaleCloses runs every owner-dependent cancellation route under the STALE connection's
// licence and asserts each is refused with nothing destroyed.
func testStaleCloses(t *testing.T, f *testWithdrawFix, stale mqttDestroyLicence) {
	t.Helper()
	e := f.e
	cur, seq := f.register(t)
	if cur == nil {
		t.Fatal("fixture: the register must stand before the stale close")
	}
	if e.sess.voidRegistersOnStream(nil, []mqttVoidedReg{{seq: seq, nonce: f.nonce, pi: f.pi,
		dur: testRealPubRelSrcDur, sseq: f.sseq}}, stale) {
		t.Fatal("a stale close's void must not report the registers ended")
	}
	if out, _ := e.sess.markInflightRegisterCancelled(nil, f.pi, testRealPubRelSrcDur, f.sseq, seq, f.nonce, stale); out == mqttRegCancelStored {
		t.Fatal("a stale close's fallback tombstone must not land")
	}
	if r := e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, stale, nil); r == mqttWithdrawLanded {
		t.Fatal("a stale close's withdrawal must not land")
	}
	got, _ := f.register(t)
	if got == nil || got.Cancelled || got.Nonce != f.nonce {
		t.Fatalf("no destructive fallback: the register must stand live, got %+v", got)
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("no destructive fallback: the exchange must stay at Publish, got %+v", x)
	}
	e.sess.mu.Lock()
	_, verdict := e.sess.cancelledRegNonces[f.nonce]
	e.sess.mu.Unlock()
	if verdict {
		t.Fatal("a stale close must not record a cancellation verdict for the successor's register")
	}
}

// testSuccessorRestores asserts the successor's resume — after its own crash — restores the
// delivery as work: the identifier stays recoverable whatever the stale close attempted.
func testSuccessorRestores(t *testing.T, f *testWithdrawFix, c2 *client) {
	t.Helper()
	e := f.e
	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(c2); err != nil {
		t.Fatalf("the successor's resume: %v", err)
	}
	e.sess.mu.Lock()
	ack := e.sess.pendingPublish[f.pi]
	var sendOK bool
	if ack != nil {
		_, sendOK = e.sess.beginSendLocked(c2, f.pi, testRealPubRelSrcDur, f.sseq)
	}
	e.sess.mu.Unlock()
	if ack == nil || ack.regNonce != f.nonce || !sendOK {
		t.Fatalf("the successor must restore the delivery as work: entry=%+v sendable=%v", ack, sendOK)
	}
}

// T10, both orders. Mutations M10 (the void without its entitlement comparison), M13 (entitlement
// loss falling back to a destructive retry).
func TestF105StaleCloseDecidesNothingForTheSuccessor(t *testing.T) {
	t.Parallel()
	t.Run("close first", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stale-close-first", 1)
		stale := mqttLicenceOf(f.e.c)
		c2 := testSuccessor(t, f.e)
		testStaleCloses(t, f, stale)
		testSuccessorRestores(t, f, c2)
	})
	t.Run("resume first", func(t *testing.T) {
		t.Parallel()
		f := testWithdrawAdmitted(t, "stale-resume-first", 1)
		stale := mqttLicenceOf(f.e.c)
		c2 := testSuccessor(t, f.e)
		testRestart(f.e)
		if _, err := f.e.sess.recoverInflightRegisters(c2); err != nil {
			t.Fatalf("the successor's resume: %v", err)
		}
		testStaleCloses(t, f, stale)
		testSuccessorRestores(t, f, c2)
	})
}

// T10, the load/commit pause: the entitlement is compared inside the committing batch, so a
// successor that takes over BETWEEN the close's authority read and its commit still defeats it.
//
// Mutation M11: the entitlement checked, then committed in a separate write.
func TestF105StaleCloseLosesToATakeoverBetweenLoadAndCommit(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "stale-close-pause", 1)
	lic := mqttLicenceOf(f.e.c)
	var took bool
	r := f.e.sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, lic, func() {
		if !took {
			took = true
			testSupersedeTenure(t, f.e)
		}
	})
	if !took {
		t.Fatal("fixture: the pause between the evidence reads and the commit was never reached")
	}
	if r == mqttWithdrawLanded {
		t.Fatal("a withdrawal whose entitlement was superseded before its commit must not land")
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the exchange must stay at Publish: %+v", x)
	}
}

// T10b. The nonce verdict is staged only under the entitlement the session is BOUND to: a stale
// connection's staging after a same-object rebind is refused (the successor's save would
// otherwise persist it), while a verdict staged before the rebind is the then-serving
// connection's decision and is carried.
//
// Mutation M13b: verdict staging without the binding check.
func TestF105VerdictStagingNeedsTheBoundEntitlement(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "verdict-staging", 1)
	e := f.e
	stale := mqttLicenceOf(e.c)
	before := mqttNewInflightNonce()
	e.sess.mu.Lock()
	stagedBefore := e.sess.stageCancellationVerdictLocked(stale, before)
	e.sess.mu.Unlock()
	if !stagedBefore {
		t.Fatal("the serving connection's own verdict must stage")
	}
	testSuccessor(t, e)
	after := mqttNewInflightNonce()
	e.sess.mu.Lock()
	stagedAfter := e.sess.stageCancellationVerdictLocked(stale, after)
	_, keptBefore := e.sess.cancelledRegNonces[before]
	_, keptAfter := e.sess.cancelledRegNonces[after]
	e.sess.mu.Unlock()
	if stagedAfter || keptAfter {
		t.Fatal("a stale connection's verdict must be refused after the rebind")
	}
	if !keptBefore {
		t.Fatal("a verdict staged before the rebind is the then-serving connection's decision and is carried")
	}
}

// T10d (consult 92). Duplicate-source ambiguity survives a stale close and another recovery: two
// registers naming one source keep both identifiers reserved and the identifier space incomplete,
// whatever the stale close attempted.
func TestF105DuplicateSourceAmbiguitySurvivesAStaleClose(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "stale-close-duplicate")
	testReadyForRealPubRelDelivery(t, e)
	for _, pi := range []uint16{3, 4} {
		e.landAdmission(t, pi, 1, testRealPubRelSrcDur, testRealPubRelSrcSeq, mqttNewInflightNonce())
	}
	stale := mqttLicenceOf(e.c)
	c2 := testSuccessor(t, e)
	for _, pi := range []uint16{3, 4} {
		rec, smsg := e.loadRegister(t, pi)
		if rec == nil || smsg == nil {
			t.Fatalf("fixture: register %d must stand", pi)
		}
		if e.sess.voidRegistersOnStream(nil, []mqttVoidedReg{{seq: smsg.Sequence, nonce: rec.Nonce, pi: pi,
			dur: rec.Dur, sseq: rec.Seq}}, stale) {
			t.Fatalf("a stale close must not end register %d", pi)
		}
	}
	for run := 0; run < 2; run++ {
		testRestart(e)
		if _, err := e.sess.recoverInflightRegisters(c2); err != nil {
			t.Fatalf("recovery %d: %v", run+1, err)
		}
		e.sess.mu.Lock()
		_, kept3 := e.sess.pendingPublish[3]
		_, kept4 := e.sess.pendingPublish[4]
		incomplete := e.sess.piSpaceIncomplete
		e.sess.mu.Unlock()
		if !kept3 || !kept4 || !incomplete {
			t.Fatalf("recovery %d: the ambiguity must survive: 3=%v 4=%v incomplete=%v", run+1, kept3, kept4, incomplete)
		}
	}
	e.waitRegisterCount(t, 2)
}

// T10b, the detached close: a NORMAL close clears sess.c before its purge, so its verdict must
// stage under the entitlement the session is still BOUND to, not a sess.c check. With the
// register-stream rungs (withdrawal, confirmed delete, tombstone) all unavailable, the verdict
// fallback is the only durable representation left, and it stages and persists.
//
// Mutation M-detach: boundEnt cleared on detach -> the detached close's verdict is refused.
func TestF105DetachedCloseStagesItsVerdict(t *testing.T) {
	// NOT parallel: installs a process-global delete hook.
	f := testWithdrawAdmitted(t, "detached-close", 1)
	e := f.e
	lic := mqttLicenceOf(e.c)
	_, seq := f.register(t)
	e.sess.mu.Lock()
	e.sess.c = nil // the normal close detaches before it purges
	e.sess.mu.Unlock()
	fail := errors.New("test: the register stream is unavailable")
	e.asm.mu.Lock()
	e.asm.testWithdrawUnsent = func(*mqttWithdrawUnsentRequest, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		return mqttOpOutcome{}, fail
	}
	e.asm.testInflightStore = func(string, uint64, *mqttInflightRecord) (uint64, error) { return 0, fail }
	e.asm.mu.Unlock()
	restore := setMQTTTestDeleteMsgHook(func(stream string, _ uint64) error {
		if stream == mqttInflightStreamName {
			return fail
		}
		return nil
	})
	t.Cleanup(restore)
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testWithdrawUnsent, e.asm.testInflightStore = nil, nil
		e.asm.mu.Unlock()
	})
	if !e.sess.voidRegistersOnStream(nil, []mqttVoidedReg{{seq: seq, nonce: f.nonce, pi: f.pi,
		dur: testRealPubRelSrcDur, sseq: f.sseq}}, lic) {
		t.Fatal("with every register rung unavailable the void must fall back to the verdict")
	}
	e.sess.mu.Lock()
	_, staged := e.sess.cancelledRegNonces[f.nonce]
	e.sess.mu.Unlock()
	if !staged {
		t.Fatal("the detached close's verdict must stage under the entitlement the session is bound to")
	}
	if !e.sess.persistVoidedRegs(nil) {
		t.Fatal("the staged verdict must persist with the session record")
	}
}

// H7b-2 finding 1: an UNREADABLE exchange is not a missing one. A stale close whose exchange read
// fails must not destroy the successor's live register through the door's direct delete: the
// read that would have fenced it by the deciding connection's entitlement never happened.
//
// Mutation M-unreadable: the unreadable arm proceeds as the missing one does -> the register is
// deleted.
func TestF105StaleCloseDoesNotDestroyOverAnUnreadableExchange(t *testing.T) {
	// NOT parallel: installs a process-global load hook.
	f := testWithdrawAdmitted(t, "stale-close-unreadable", 1)
	e := f.e
	stale := mqttLicenceOf(e.c)
	testSuccessor(t, e)
	xsubj := mqttExchangeSubject(e.sess.idHash, f.gen, f.lin)
	restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if subject == xsubj {
			return errors.New("test: the exchange read could not be completed")
		}
		return nil
	})
	t.Cleanup(restore)
	_, seq := f.register(t)
	e.sess.voidRegistersOnStream(nil, []mqttVoidedReg{{seq: seq, nonce: f.nonce, pi: f.pi,
		dur: testRealPubRelSrcDur, sseq: f.sseq}}, stale)
	restore()
	got, _ := f.register(t)
	if got == nil || got.Cancelled || got.Nonce != f.nonce {
		t.Fatalf("a stale close must not destroy or cancel the successor's register over an unreadable exchange: %+v", got)
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the obligation must stay outstanding: %+v", x)
	}
}

// H7b-3: no destruction without a committed, entitlement-fenced disposition. The close's
// withdrawal passes its entitlement check, then a takeover lands and the withdrawal's marker read
// fails, so the op answers "no marker" having committed NOTHING. The door must not fall back to a
// direct delete: its tombstone, written under the now-stale licence, is refused, and the
// successor's register stays live.
//
// Mutation M-nomarker: the no-marker arm deletes directly again -> the register is gone.
func TestF105NoMarkerWithdrawalNeverDestroysWithoutAFence(t *testing.T) {
	// NOT parallel: installs a process-global load hook.
	f := testWithdrawAdmitted(t, "stale-close-nomarker", 1)
	e := f.e
	lic := mqttLicenceOf(e.c)
	markerSubject := mqttInflightMarkerSubject(e.sess.idHash, f.gen, f.pi)
	var reads atomic.Int32
	restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if subject != markerSubject {
			return nil
		}
		// The door's own marker read proves the obligation unsent; the withdrawal's read, after
		// its entitlement check, meets the takeover and cannot be completed.
		if reads.Add(1) == 2 {
			testSupersedeTenure(t, e)
			return errors.New("test: the marker read could not be completed")
		}
		return nil
	})
	t.Cleanup(restore)
	_, seq := f.register(t)
	e.sess.voidRegistersOnStream(nil, []mqttVoidedReg{{seq: seq, nonce: f.nonce, pi: f.pi,
		dur: testRealPubRelSrcDur, sseq: f.sseq}}, lic)
	restore()
	if reads.Load() < 2 {
		t.Fatalf("fixture: the withdrawal's marker read was never reached (%d marker reads)", reads.Load())
	}
	got, _ := f.register(t)
	if got == nil || got.Cancelled || got.Nonce != f.nonce {
		t.Fatalf("a no-marker withdrawal after a takeover must not destroy the successor's register: %+v", got)
	}
	if x := f.exchange(t); x == nil || x.Phase != mqttExchangePublish {
		t.Fatalf("the obligation must stay outstanding: %+v", x)
	}
}
