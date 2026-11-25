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

//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// RECEIPT BATCHES, END TO END (issue #192): a real broker, real deliveries, the account's receipt
// driver doing the work.

// testReceiptSubjects counts client cid's generation receipts and actions on the stream.
// Lifecycle-scoped receipts (L.<L>) are the stated residual, not counted.
func testReceiptSubjects(t *testing.T, jsa *mqttJSA, cid string) (txn, act int) {
	t.Helper()
	pre := mqttOutboundRecordSubjectPrefix()
	h := getHash(cid)
	for _, f := range []struct {
		filter string
		n      *int
	}{{pre + mqttOutcomeClassTk + "." + h + ".>", &txn}, {pre + mqttActionClassTk + "." + h + ".>", &act}} {
		subs, complete, err := jsa.streamSubjectsComplete(mqttSessStreamName, f.filter)
		if err != nil || !complete {
			t.Fatalf("listing %q: complete %v, %v", f.filter, complete, err)
		}
		for s := range subs {
			if f.n == &txn && strings.Contains(s, "."+h+"."+mqttLifecycleScopePrefix+".") {
				continue
			}
			*f.n++
		}
	}
	return txn, act
}

// mqttTestFloorStallWindow is how long the at-rest wait below tolerates no forward milestone. It is a
// test watchdog with scheduling and I/O headroom over the floor's periodic re-arm (mqttFloorTick,
// +20%) and its backoff cap, not a protocol bound: the floor promises reclamation at rest, never a
// drain time.
const mqttTestFloorStallWindow = 90 * time.Second

// testFloorMilestones is the forward progress the at-rest wait credits: a new maximum of the floor or
// the deletion watermark, a scan reaching further from the same floor, the first qualification of a
// horizon, or fewer receipts left. A horizon that resets or rescans renews nothing.
type testFloorMilestones struct {
	lo, del, scanBase, scanNext, qualifiedEnd uint64
	txn                                       int
}

// advance folds one observation in and reports whether it reached a milestone.
func (m *testFloorMilestones) advance(lo, del uint64, hz *mqttFloorHorizon, txn int) bool {
	moved := false
	if lo > m.lo {
		m.lo, moved = lo, true
	}
	if del > m.del {
		m.del, moved = del, true
	}
	if hz != nil {
		if hz.base != m.scanBase {
			m.scanBase, m.scanNext, m.qualifiedEnd = hz.base, hz.next, 0
		} else if hz.next > m.scanNext {
			m.scanNext, moved = hz.next, true
		}
		if hz.executing && hz.end > m.qualifiedEnd {
			m.qualifiedEnd, moved = hz.end, true
		}
	}
	if txn < m.txn {
		m.txn, moved = txn, true
	}
	return moved
}

// testReclaimVerdict decides one observation of the at-rest wait. Identity comes first: a session
// replaced, or a generation that changed, may have had its records purged by its retirement, and
// that is not the live generation's reclamation, so it is fatal even when the counts are satisfied.
func testReclaimVerdict(wantGen, curGen string, resident bool, txn, act, maxTxn, maxAct int) (bool, error) {
	switch {
	case !resident:
		return false, fmt.Errorf("the session object was replaced while waiting for reclamation")
	case curGen != wantGen:
		return false, fmt.Errorf("the generation changed from %q to %q while waiting for reclamation", wantGen, curGen)
	}
	return txn <= maxTxn && act <= maxAct, nil
}

// TestReclaimVerdictRefusesAReplacedGeneration: counts satisfied by a different generation, or a
// replaced session object, never end the wait as a pass.
func TestReclaimVerdictRefusesAReplacedGeneration(t *testing.T) {
	if done, err := testReclaimVerdict("g1", "g2", true, 0, 0, 0, 96); err == nil || done {
		t.Fatalf("a changed generation with satisfied counts must fail: done=%v err=%v", done, err)
	}
	if done, err := testReclaimVerdict("g1", "g1", false, 0, 0, 0, 96); err == nil || done {
		t.Fatalf("a replaced session with satisfied counts must fail: done=%v err=%v", done, err)
	}
	if done, err := testReclaimVerdict("g1", "g1", true, 0, 97, 0, 96); err != nil || done {
		t.Fatalf("unsatisfied counts keep waiting: done=%v err=%v", done, err)
	}
	if done, err := testReclaimVerdict("g1", "g1", true, 0, 96, 0, 96); err != nil || !done {
		t.Fatalf("satisfied counts of the same generation end the wait: done=%v err=%v", done, err)
	}
}

// testWaitReceiptsReclaimed waits until client cid holds at most maxTxn generation receipts and maxAct
// actions, and returns what it last read. It waits for as long as reclamation keeps reaching forward
// milestones, because at rest the floor drains at the store's pace (32 positions a unit, due again at
// once while it progresses), and fails when nothing moves for mqttTestFloorStallWindow: a floor that
// stops is the defect, a slow one is not.
func testWaitReceiptsReclaimed(t *testing.T, asm *mqttAccountSessionManager, cid string, maxTxn, maxAct int) (int, int) {
	t.Helper()
	asm.mu.RLock()
	sess := asm.sessions[cid]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("%q is not resident", cid)
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	hardCap := time.Now().Add(20 * time.Minute)
	if d, ok := t.Deadline(); ok && d.Add(-time.Minute).Before(hardCap) {
		hardCap = d.Add(-time.Minute)
	}
	m := testFloorMilestones{txn: math.MaxInt}
	lastMove := time.Now()
	for {
		txn, act := testReceiptSubjects(t, &asm.jsa, cid)
		// The identity is read AFTER the counts, so a count can only be credited to the generation
		// it was waited for. The horizon is copied under the lock; the ledger is read outside it,
		// so the wait never holds the session the floor unit needs.
		asm.mu.RLock()
		resident := asm.sessions[cid] == sess
		asm.mu.RUnlock()
		sess.mu.Lock()
		curGen := sess.gen
		var hz *mqttFloorHorizon
		if sess.floorHorizon != nil {
			h := *sess.floorHorizon
			hz = &h
		}
		sess.mu.Unlock()
		done, err := testReclaimVerdict(gen, curGen, resident, txn, act, maxTxn, maxAct)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return txn, act
		}
		led, _, lerr := asm.jsa.loadGenLedger(cid, asm.sessStreamEpoch, gen)
		now := time.Now()
		if lerr == nil && led != nil && m.advance(led.Lo, led.Del, hz, txn) {
			lastMove = now
		}
		if now.Sub(lastMove) > mqttTestFloorStallWindow {
			t.Fatalf("reclamation stopped for %v at %d receipts and %d actions: ledger lo=%d del=%d, horizon %+v (read error %v)",
				mqttTestFloorStallWindow, txn, act, m.lo, m.del, hz, lerr)
		}
		if now.After(hardCap) {
			t.Fatalf("reclamation was still moving at the hard cap with %d receipts and %d actions", txn, act)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// A long-lived persistent QoS 1 subscriber's receipts and actions stay bounded while every delivery
// completes: its receipts are purged batch by batch by the run that wrote them, and its actions by
// the floor.
func TestALongLivedSubscriberKeepsItsReceiptsBounded(t *testing.T) {
	const n = 1500
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testSubscribe(t, s, o, "receipts-long", "receipts/long", false)
	defer sub.c.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "receipts-long")

	testFloorDeliver(t, o, sub, "receipts/long", n)

	// Before reclamation the receipts are there to be reclaimed: the fixture is not vacuous.
	if txn, _ := testReceiptSubjects(t, &asm.jsa, "receipts-long"); txn == 0 {
		t.Fatalf("after %d deliveries the subscriber holds no generation receipt to reclaim", n)
	}
	// At rest no batch is left unpurged once the open batch has aged, and the floor retires the
	// history down to its slack, deleting the reclaimed positions' actions. It does that at the store's
	// pace, not to a deadline, so the wait follows its progress and fails on a stop.
	txn, act := testWaitReceiptsReclaimed(t, asm, "receipts-long", 0, mqttFloorSlack+64)
	if txn != 0 {
		t.Fatalf("after %d deliveries and the batch age the subscriber still holds %d generation receipts", n, txn)
	}
	if act > mqttFloorSlack+64 {
		t.Fatalf("after %d deliveries and the floor at rest the subscriber holds %d actions", n, act)
	}
	if batches, unsettled := asm.jsa.receipts().pending(asm.sessStreamEpoch); batches != 0 || unsettled != 0 {
		t.Fatalf("the ledger still tracks %d batch(es) and %d unsettled record(s)", batches, unsettled)
	}
}

// A graceful shutdown purges every receipt batch of its run, the open one included, for a
// low-traffic session whose batch has not aged.
func TestAGracefulShutdownPurgesItsReceiptBatches(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testSubscribe(t, s, o, "receipts-shutdown", "receipts/shutdown", false)
	defer sub.c.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "receipts-shutdown")
	testFloorDeliver(t, o, sub, "receipts/shutdown", 3)
	if txn, _ := testReceiptSubjects(t, &asm.jsa, "receipts-shutdown"); txn == 0 {
		t.Fatal("the fixture must hold receipts in an open batch")
	}
	s.settleReceiptsAtShutdown()
	if txn, _ := testReceiptSubjects(t, &asm.jsa, "receipts-shutdown"); txn != 0 {
		t.Fatalf("after the shutdown settlement %d generation receipts remain", txn)
	}
	if _, err := asm.jsa.receipts().prepare(&mqttAuthorityTransition{Attempt: &mqttTransitionAttempt{
		ID: "receipts-shutdown", Epoch: asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Scope: sub.sess.gen,
		Kind: mqttTransitionAdmitExchange}, Auth: &mqttOutboundAuthority{}, PrevSeq: 1}); !errors.Is(err, errMQTTReceiptLedgerClosed) {
		t.Fatalf("after the shutdown settlement a new receipted attempt was admitted: %v", err)
	}
}

// testAmbiguousAttempt registers a receipted attempt of client cid's current generation over its
// standing authority, as a commit whose acknowledgement was lost and which never reached the store.
func testAmbiguousAttempt(t *testing.T, asm *mqttAccountSessionManager, cid string) (*mqttReceiptAttempt, uint64) {
	rec, seq, _ := testAmbiguousTransition(t, asm, cid)
	return rec, seq
}

// testAmbiguousTransition is testAmbiguousAttempt, also returning the authority the attempt would
// have installed.
func testAmbiguousTransition(t *testing.T, asm *mqttAccountSessionManager, cid string) (*mqttReceiptAttempt, uint64, *mqttOutboundAuthority) {
	t.Helper()
	jsa := &asm.jsa
	al := jsa.loadOutboundAuthority(cid, asm.sessStreamEpoch)
	if al.status != mqttOutboundValid {
		t.Fatalf("the authority of %q reads %v", cid, al.status)
	}
	auth := al.rec
	txn := mqttNewInflightNonce()
	next := *auth
	next.Rev, next.Txn = auth.Rev+1, txn
	tr := &mqttAuthorityTransition{
		Attempt: &mqttTransitionAttempt{
			ID: cid, Epoch: asm.sessStreamEpoch, Txn: txn, Scope: auth.Gen, Op: "op-" + txn,
			Kind: mqttTransitionAdmitExchange, PrevTxn: auth.OwnerTxn, Rev: auth.Rev + 1,
			Gen: auth.Gen, Tenure: auth.Tenure, OwnerTxn: auth.OwnerTxn,
		},
		Prev: auth, PrevSeq: al.seq, Auth: &next,
	}
	rec, err := jsa.receipts().prepare(tr)
	if err != nil {
		t.Fatalf("preparing: %v", err)
	}
	if err := jsa.receipts().register(rec, al.seq); err != nil {
		t.Fatalf("registering: %v", err)
	}
	jsa.receipts().returned(rec, true, mqttSubmissionAmbiguous, al.seq, 0)
	return rec, al.seq, &next
}

// An attempt whose commit was lost with the authority still at its expectation is settled by the
// driver alone, with no holder and no unrelated write: its own fenced abort decides it never
// landed, and the batch's ABORTED receipt is then purged.
func TestTheDriverSettlesAnAmbiguousCommitByItsOwnAbort(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testSubscribe(t, s, o, "receipts-abort", "receipts/abort", false)
	defer sub.c.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "receipts-abort")
	jsa := &asm.jsa
	rec, fence := testAmbiguousAttempt(t, asm, "receipts-abort")
	a := rec.attempt
	jsa.receipts().kickDriver()
	deadline := time.Now().Add(mqttReceiptBatchAge + 10*time.Second)
	for {
		if v, ok := jsa.receipts().verdict(rec); ok {
			if v != mqttTransitionNeverLanded {
				t.Fatalf("the driver decided %v for an attempt that never reached the store", v)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the driver never decided the ambiguous attempt")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if al := jsa.loadOutboundAuthority("receipts-abort", asm.sessStreamEpoch); al.seq <= fence {
		t.Fatalf("the abort settled the attempt without moving the authority past %d", fence)
	}
	for {
		_, _, st := jsa.loadTransitionOutcome(a.ID, a.Epoch, a.Scope, a.Txn)
		if st == mqttOutboundAbsent {
			if b, _ := jsa.receipts().pending(asm.sessStreamEpoch); b == 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the settled attempt's batch was never purged: its receipt reads %v", st)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A decided attempt whose last abort was lost, on a session nothing else writes, is settled by the
// driver's progress write: the authority rewritten as read moves it past the fence, and the batch
// is purged.
func TestADecidedAttemptOnAnIdleSessionSettlesByAProgressWrite(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testSubscribe(t, s, o, "receipts-progress", "receipts/progress", false)
	defer sub.c.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "receipts-progress")
	jsa := &asm.jsa
	rec, fence := testAmbiguousAttempt(t, asm, "receipts-progress")
	// The holder decided it, then an abort it had sent at the same expectation lost its answer.
	if err := jsa.receipts().register(rec, fence); err != nil {
		t.Fatal(err)
	}
	jsa.receipts().returned(rec, false, mqttSubmissionAmbiguous, fence, 0)
	jsa.receipts().decide(rec, mqttTransitionNeverLanded)
	before := jsa.loadOutboundAuthority("receipts-progress", asm.sessStreamEpoch)
	jsa.receipts().kickDriver()
	deadline := time.Now().Add(mqttReceiptBatchAge + 10*time.Second)
	for {
		jsa.receipts().mu.Lock()
		settled := rec.settled
		jsa.receipts().mu.Unlock()
		if settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the decided attempt never settled on an idle session")
		}
		time.Sleep(100 * time.Millisecond)
	}
	after := jsa.loadOutboundAuthority("receipts-progress", asm.sessStreamEpoch)
	if after.seq <= fence || after.rec.Rev != before.rec.Rev || after.rec.Txn != before.rec.Txn {
		t.Fatalf("the progress write must move the authority's sequence past %d and nothing else: before %d/%d/%q, after %d/%d/%q",
			fence, before.seq, before.rec.Rev, before.rec.Txn, after.seq, after.rec.Rev, after.rec.Txn)
	}
	if v, _ := jsa.receipts().verdict(rec); v != mqttTransitionNeverLanded {
		t.Fatalf("the progress write changed the published verdict to %v", v)
	}
}

// A spent receipt budget refuses a new delivery's admission before anything is sent and puts the
// generation under pressure: no fresh delivery is allocated, and the refused identifier is freed.
func TestASpentReceiptBudgetPutsTheGenerationUnderPressure(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testSubscribe(t, s, o, "receipts-budget", "receipts/budget", false)
	defer sub.c.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "receipts-budget")
	jsa := &asm.jsa
	testFloorDeliver(t, o, sub, "receipts/budget", 1)
	sub.sess.mu.Lock()
	gen := sub.sess.gen
	sub.sess.mu.Unlock()
	l := jsa.receipts()
	l.mu.Lock()
	ks := l.keys[mqttReceiptKey{epoch: asm.sessStreamEpoch, id: "receipts-budget", gen: gen}]
	if ks == nil {
		l.mu.Unlock()
		t.Fatal("the fixture's delivery left no receipt batch")
	}
	spent := mqttReceiptBudget - ks.charged
	ks.charged = mqttReceiptBudget
	l.mu.Unlock()

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "receipts-budget-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "receipts/budget", 1, []byte("y"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		sub.sess.mu.Lock()
		under := sub.sess.receiptPressureLocked(time.Now())
		pending := len(sub.sess.pendingPublish)
		sub.sess.mu.Unlock()
		if under {
			if pending != 0 {
				t.Fatalf("the refused delivery's identifier is still held: %d pending", pending)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a delivery refused at a spent budget did not put the generation under pressure")
		}
		time.Sleep(50 * time.Millisecond)
	}
	l.mu.Lock()
	ks.charged -= spent
	l.mu.Unlock()
}

// A resolution returns the record's cell, not what it read: another participant decides the
// attempt Landed while this resolver proves it never landed from a moved authority and an absent
// receipt, and the resolver returns Landed.
func TestAResolverReturnsTheVerdictDecidedWhileItRead(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testSubscribe(t, s, o, "receipts-cell", "receipts/cell", false)
	defer sub.c.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "receipts-cell")
	jsa := &asm.jsa
	rec, _ := testAmbiguousAttempt(t, asm, "receipts-cell")
	// The authority moves past the attempt's expectation, so the store alone proves NeverLanded.
	if err := jsa.progressAuthority(rec); err != nil {
		t.Fatalf("moving the authority: %v", err)
	}
	hook := func(id string) {
		if id == "receipts-cell" {
			jsa.receipts().decide(rec, mqttTransitionLanded)
		}
	}
	mqttTestStaleProofPointV.Store(&hook)
	defer mqttTestStaleProofPointV.Store(nil)
	if v := jsa.resolveTransitionHandle(mqttReceiptHandle(rec)); v != mqttTransitionLanded {
		t.Fatalf("the resolver returned %v, its own read, over the Landed decided while it read", v)
	}
}

// An attempt whose commit LANDED but whose acknowledgement was lost, with nothing else writing, is
// decided Landed by the driver from its receipt (resolved first, whatever the authority's
// position), settled by the commit's own authority sequence, and its batch purged.
func TestTheDriverDecidesALandedCommitFromItsReceipt(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testSubscribe(t, s, o, "receipts-landed", "receipts/landed", false)
	defer sub.c.Close()
	asm := testMQTTGetAccountSessionManager(t, s, "receipts-landed")
	jsa := &asm.jsa
	rec, fence, next := testAmbiguousTransition(t, asm, "receipts-landed")
	a := rec.attempt
	// The commit lands behind the lost acknowledgement: its receipt and its authority, one batch.
	out, _ := json.Marshal(&mqttTransitionOutcome{
		ID: a.ID, Epoch: a.Epoch, Txn: a.Txn, Scope: a.Scope, Op: a.Op, Kind: a.Kind,
		Verdict: mqttOutcomeCommitted, PrevTxn: a.PrevTxn, Intent: a.Intent,
		Rev: a.Rev, Gen: a.Gen, Tenure: a.Tenure, OwnerTxn: a.OwnerTxn, Digest: rec.digest,
	})
	auth, _ := json.Marshal(next)
	batchID := mqttNewInflightNonce()
	om, oh := mqttFrameBatchWrite(0, out, batchID, 1, false)
	am, ah := mqttFrameBatchWrite(fence, auth, batchID, 2, true)
	if _, err := jsa.sendTransitionBatch([]mqttBatchMsg{
		{subj: mqttTransitionOutcomeSubject(getHash(a.ID), a.Scope, a.Txn), hdr: oh, msg: om},
		{subj: mqttOutboundAuthoritySubject(getHash(a.ID)), hdr: ah, msg: am},
	}, batchID, 2, a.Txn); err != nil {
		t.Fatalf("landing the commit: %v", err)
	}
	jsa.receipts().kickDriver()
	deadline := time.Now().Add(mqttReceiptBatchAge + 10*time.Second)
	for {
		if v, ok := jsa.receipts().verdict(rec); ok {
			if v != mqttTransitionLanded && v != mqttTransitionSuperseded {
				t.Fatalf("the driver decided %v for a commit that landed", v)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the driver never decided the landed commit")
		}
		time.Sleep(100 * time.Millisecond)
	}
	for {
		if b, _ := jsa.receipts().pending(asm.sessStreamEpoch); b == 0 {
			if _, _, st := jsa.loadTransitionOutcome(a.ID, a.Epoch, a.Scope, a.Txn); st != mqttOutboundAbsent {
				t.Fatalf("the batch was forgotten with its receipt reading %v", st)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the landed commit's batch was never purged")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
