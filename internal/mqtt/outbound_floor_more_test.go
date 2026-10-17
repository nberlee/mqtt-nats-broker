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

//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// T12. NO EXCHANGE ENUMERATION REACHES THE PAGED SUBJECT WALK (M6). streamSubjectsComplete is
// sound only over filters whose records are never deleted; since F-112 exchange records are
// deleted behind the floor, so no production call may pass it the exchange walk filter, directly
// or through a variable.
func TestF112NoExchangeFilterReachesThePagedSubjectWalk(t *testing.T) {
	t.Parallel()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	walks := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, rerr := os.ReadFile(name)
		if rerr != nil {
			t.Fatal(rerr)
		}
		f, perr := parser.ParseFile(fset, name, src, 0)
		if perr != nil {
			t.Fatalf("parsing %s: %v", name, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			body := string(src[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset])
			if strings.Contains(body, "streamSubjectsComplete(") {
				walks++
				if strings.Contains(body, "mqttExchangeWalkFilter(") {
					t.Errorf("%s: %s passes an exchange filter to the paged subject walk: exchange records are "+
						"deleted behind the floor, so the inventory [Lo, Hi] is the only sound enumeration (F-112)",
						name, fn.Name.Name)
				}
			}
			return true
		})
	}
	if walks == 0 {
		t.Fatal("fixture: no caller of streamSubjectsComplete was found, so this check examined nothing")
	}
}

// T2d. The amortization rule (M9). Twenty long-lived obligations pin the prefix; reclaimable
// history arrives ten positions at a time, and the horizon is driven to idle after each arrival.
// Without the rule every arrival would rewrite the twenty pins; with it, total relocation writes
// stay within half the reclaimed positions plus one batch.
func TestF112RelocationIsAmortizedOverReclaimedHistory(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-t2d", 1)
	e := f.e
	sess := e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	gen := f.gen
	admit := func(pi uint16, end bool) {
		t.Helper()
		al := jsa.loadOutboundAuthority(sess.id, epoch)
		led, ledSeq, _ := jsa.loadGenLedger(sess.id, epoch, gen)
		txn := mqttNewInflightNonce()
		x := &mqttOutboundExchange{ID: sess.id, Epoch: epoch, Gen: gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure}
		tr, err := mqttAdmitExchangeTransition(al.rec, al.seq, txn, "op-"+txn, x, led, ledSeq)
		if err != nil {
			t.Fatalf("composing: %v", err)
		}
		if _, err := jsa.commitAuthorityTransition(tr); err != nil {
			t.Fatalf("admitting: %v", err)
		}
		if !end {
			return
		}
		etxn := mqttNewInflightNonce()
		if _, err := jsa.opAdvanceExchange(sess.id, epoch, etxn, "op-"+etxn, gen, x.Nonce, mqttExchangeTerminal); err != nil {
			t.Fatalf("ending: %v", err)
		}
	}
	start, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
	for i := 0; i < 20; i++ {
		admit(uint16(1000+i), false)
	}
	admissions := uint64(20)
	reclaimed := uint64(0)
	for round := 0; round < 12; round++ {
		for i := 0; i < 10; i++ {
			admit(1500, true)
		}
		admissions += 10
		for step := 0; step < 64; step++ {
			led, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
			progressed, more := sess.floorHorizonStep(gen, epoch, led)
			if !progressed && !more {
				break
			}
		}
	}
	led, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
	relocations := led.Hi - start.Hi - admissions
	reclaimed = led.Lo - 1 - relocations
	if relocations > reclaimed/2+mqttFloorBatchCarry {
		t.Fatalf("the floor wrote %d relocations while reclaiming %d positions: relocation is not amortized", relocations, reclaimed)
	}
	if led.Lo == 1 {
		t.Fatal("fixture: the floor never advanced, so the amortization was never exercised")
	}
}

// T6a. Deletion owed below the floor is discovered from a resident ledger read (M18): the floor is
// advanced and NOT deleted, the process forgets everything, and the census it rebuilds from arms
// the floor unit, which finishes the deletions with no ending, no admission and no manual kick.
func TestF112DeletionOwedBelowTheFloorIsFinishedFromALedgerRead(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-t6a", 1)
	e := f.e
	sess := e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	gen := f.gen
	for i := 0; i < 40; i++ {
		al := jsa.loadOutboundAuthority(sess.id, epoch)
		led, ledSeq, _ := jsa.loadGenLedger(sess.id, epoch, gen)
		txn := mqttNewInflightNonce()
		x := &mqttOutboundExchange{ID: sess.id, Epoch: epoch, Gen: gen, Nonce: mqttNewInflightNonce(),
			PI: 1600, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure}
		tr, _ := mqttAdmitExchangeTransition(al.rec, al.seq, txn, "op-"+txn, x, led, ledSeq)
		if _, err := jsa.commitAuthorityTransition(tr); err != nil {
			t.Fatalf("admitting: %v", err)
		}
		etxn := mqttNewInflightNonce()
		if _, err := jsa.opAdvanceExchange(sess.id, epoch, etxn, "op-"+etxn, gen, x.Nonce, mqttExchangeTerminal); err != nil {
			t.Fatalf("ending: %v", err)
		}
		ord, _, _ := jsa.findDutyInInventory(sess.id, epoch, gen, func(d *mqttDutyEntry) bool { return d.Ref == x.Nonce })
		d, _, _ := jsa.loadDutyEntryAt(sess.id, epoch, gen, ord)
		root := &mqttRecoveryRoot{ID: sess.id, Epoch: epoch, Gen: gen}
		if err := jsa.opResolveDuty(root, mqttSweptDuty{Ord: ord, Entry: d}, mqttNewInflightNonce()); err != nil {
			t.Fatalf("resolving: %v", err)
		}
	}
	before, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
	fb, err := jsa.composeFloorBatch(sess.id, epoch, gen, before.Hi+1)
	if err != nil || fb == nil || len(fb.Positions) == 0 {
		t.Fatalf("composing: %v", err)
	}
	if err := jsa.opAdvanceFloor(fb, mqttNewInflightNonce()); err != nil {
		t.Fatalf("advancing: %v", err)
	}
	mid, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
	if mid.Del >= mid.Lo {
		t.Fatalf("fixture: deletion must be owed: %+v", mid)
	}
	testRestart(e)
	sess.reconstructCustody(gen)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		now, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
		if now != nil && now.Del == now.Lo {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	now, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
	t.Fatalf("deletion owed below the floor was not finished after a resident ledger read: %+v", now)
}

// A reclaimed lineage is released as the ending it proves: its custody holder, and its binding
// with the ended-and-deleted proof (a shared HandBack needs exactly that).
func TestF112AReclaimedLineageReleasesItsBindingAndItsCustody(t *testing.T) {
	t.Parallel()
	sess := testLateAckUnitSess()
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.custody.ensureExchangeLocked("lin-r", 6)
	b := testLateAckUnitBinding(sess, mqttAckRoleHandBack, 6, "n6", "lin-r", mqttSharedConsumerDurablePrefix+"grp_inc")
	if !sess.installAbandonedAckLocked(b) {
		t.Fatal("fixture: the HandBack was not installed")
	}
	if !sess.acceptReclaimedLineageLocked(sess.gen, "lin-r") {
		t.Fatal("the reclaimed lineage's charge was not released")
	}
	if sess.custody.holdsPI(6) {
		t.Fatal("the reclaimed lineage still holds its identifier")
	}
	if sess.abandonedAcks[b.index()] != nil {
		t.Fatal("the reclaimed lineage's HandBack still stands")
	}
}

// Holistic round 2, finding 1. An EXCHANGE uncertainty — a census decoded a duty naming the
// exchange but could not read the exchange — whose lineage is since reclaimed behind the floor is
// discharged by its individual re-read, and allocation reopens. Before, only duty-subject
// uncertainty was discharged and this one withheld allocation for the generation's life.
func TestF112AReclaimedExchangesUncertaintyIsDischarged(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-uncx", 1)
	e := f.e
	sess := e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	gen := f.gen
	al := jsa.loadOutboundAuthority(sess.id, epoch)
	led, ledSeq, _ := jsa.loadGenLedger(sess.id, epoch, gen)
	txn := mqttNewInflightNonce()
	x := &mqttOutboundExchange{ID: sess.id, Epoch: epoch, Gen: gen, Nonce: mqttNewInflightNonce(),
		PI: 1700, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure}
	tr, _ := mqttAdmitExchangeTransition(al.rec, al.seq, txn, "op-"+txn, x, led, ledSeq)
	if _, err := jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("admitting: %v", err)
	}
	xsubj := mqttExchangeSubject(sess.idHash, gen, x.Nonce)
	sess.mu.Lock()
	if sess.custody.uncertainSubjects == nil {
		sess.custody.uncertainSubjects = map[string]uint64{}
	}
	sess.custody.uncertainSubjects[xsubj] = 1
	withheld := sess.custody.withholdsAllocation()
	sess.mu.Unlock()
	if !withheld {
		t.Fatal("fixture: an uncertain exchange subject must withhold allocation")
	}
	etxn := mqttNewInflightNonce()
	if _, err := jsa.opAdvanceExchange(sess.id, epoch, etxn, "op-"+etxn, gen, x.Nonce, mqttExchangeTerminal); err != nil {
		t.Fatalf("ending: %v", err)
	}
	ord, _, _ := jsa.findDutyInInventory(sess.id, epoch, gen, func(d *mqttDutyEntry) bool { return d.Ref == x.Nonce })
	d, _, _ := jsa.loadDutyEntryAt(sess.id, epoch, gen, ord)
	root := &mqttRecoveryRoot{ID: sess.id, Epoch: epoch, Gen: gen}
	if err := jsa.opResolveDuty(root, mqttSweptDuty{Ord: ord, Entry: d}, mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving: %v", err)
	}
	for i := 0; i < 64; i++ {
		fb, err := jsa.composeFloorBatch(sess.id, epoch, gen, ord+1)
		if err != nil || fb == nil || len(fb.Positions) == 0 {
			break
		}
		if err := jsa.opAdvanceFloor(fb, mqttNewInflightNonce()); err != nil {
			t.Fatalf("advancing: %v", err)
		}
	}
	for i := 0; i < 64; i++ {
		more, err := jsa.deleteRetiredPositionsStep(sess.id, epoch, gen)
		if err != nil {
			t.Fatalf("deleting: %v", err)
		}
		if !more {
			break
		}
	}
	if _, _, xerr := jsa.loadOutboundExchange(sess.id, epoch, gen, x.Nonce); xerr == nil || !mqttExchangeReadAbsent(xerr) {
		t.Fatalf("fixture: the exchange was not reclaimed: %v", xerr)
	}
	sess.observeCustodyTarget(gen, mqttCustodyTarget{kind: mqttCustodyTargetUncertain, key: xsubj})
	sess.mu.Lock()
	_, still := sess.custody.uncertainSubjects[xsubj]
	withheld = sess.custody.withholdsAllocation()
	sess.mu.Unlock()
	if still || withheld {
		t.Fatalf("a reclaimed exchange's uncertainty was not discharged (uncertain %v, withholds %v)", still, withheld)
	}
}

// Holistic round 2, finding 2. The recovery sweep accounts a sealed generation's retired prefix in
// constant time: with the floor near the top of a long sealed inventory, its FIRST batch reaches
// the surviving duties, charging only the positions it reads.
func TestF112TheRecoverySweepJumpsTheRetiredPrefix(t *testing.T) {
	e, done := testF112Env(t, "f112-sweep")
	defer done()
	jsa := &e.asm.jsa
	const hi = 5000
	idHash := getHash(e.id)
	for _, ord := range []uint64{hi - 1, hi} {
		d := &mqttDutyEntry{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, Ord: ord, Kind: mqttDutyExchange,
			Ref: mqttNewInflightNonce(), State: mqttDutyPending}
		body, _ := json.Marshal(d)
		if _, err := jsa.storeMsg(mqttDutySubject(idHash, testF105Gen, ord), -1, body); err != nil {
			t.Fatalf("planting duty %d: %v", ord, err)
		}
	}
	led := &mqttGenLedger{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, State: mqttGenLedgerSealed,
		Hi: hi, SealedHi: hi, Lo: hi - 1, Del: hi - 1}
	lbody, _ := json.Marshal(led)
	if _, err := jsa.storeMsg(mqttGenLedgerSubject(idHash, testF105Gen), -1, lbody); err != nil {
		t.Fatalf("planting the ledger: %v", err)
	}
	root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: testF105Gen, SweepStart: 1, Resume: 1, Bound: hi}
	charged := 0
	swept, out, err := jsa.sweepOneBatchCharged(root, func() bool { charged++; return true }, false)
	if err != nil {
		t.Fatalf("sweeping: %v", err)
	}
	if out.Examined != hi || len(swept) != 2 || out.LowestUnresolved != hi-1 {
		t.Fatalf("the first batch examined %d (want %d) and returned %d surviving duties from %d (want 2 from %d)",
			out.Examined, hi, len(swept), out.LowestUnresolved, hi-1)
	}
	if charged != 2 {
		t.Fatalf("the first batch charged %d reads, want the 2 it made", charged)
	}
}

// Holistic round 3. An ordinary delivery admission is a FOREGROUND authority operation for its
// whole ladder, so a background authority attempt — the floor unit's — that starts while it runs
// waits until it returns and cannot take one of its bounded attempts. Deterministic: the
// background attempt is started from inside the admission's own announcement.
func TestF112AnAdmissionHoldsTheFloorsAuthorityAttemptsBack(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-fg", 1)
	sess := f.e.sess
	var (
		mu                    sync.Mutex
		started, waiting      bool
		admitted              bool
		admittedBeforeRelease bool
		done                  = make(chan struct{})
	)
	waitHook := func() {
		mu.Lock()
		waiting = true
		mu.Unlock()
	}
	sess.testBackgroundWait.Store(&waitHook)
	announce := func(on bool) {
		mu.Lock()
		first := on && !started
		if first {
			started = true
		}
		if !on && started && !admittedBeforeRelease {
			admittedBeforeRelease = admitted
		}
		mu.Unlock()
		if !first {
			return
		}
		go func() {
			defer close(done)
			ok := sess.admitBackgroundAuthority()
			mu.Lock()
			admitted = ok
			mu.Unlock()
		}()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			w, a := waiting, admitted
			mu.Unlock()
			if w || a {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	sess.testForegroundAnnounce.Store(&announce)
	t.Cleanup(func() {
		sess.testForegroundAnnounce.Store(nil)
		sess.testBackgroundWait.Store(nil)
	})
	testWithdrawAdmitMore(t, f.e, testRealPubRelSrcSeq+50, 1)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the background attempt was never admitted after the admission returned")
	}
	mu.Lock()
	defer mu.Unlock()
	if !started {
		t.Fatal("fixture: the admission announced no foreground operation, so nothing here is tested")
	}
	if !waiting || admittedBeforeRelease {
		t.Fatalf("a background authority attempt was admitted while a delivery admission was in its ladder "+
			"(waited %v, admitted before the release %v)", waiting, admittedBeforeRelease)
	}
}
