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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// THE FLOOR, SESSION-SIDE (F-112): what a resident session reads, keeps and prunes once a live
// generation's history is retired behind its floor.

// T1. A resume reads [Lo, Hi] and nothing below it (M5): with the floor near the top of a long
// history, the traversal's duty reads are exactly the span, and none names a position below Lo.
func TestF112ResumeReadsFromTheFloor(t *testing.T) {
	t.Parallel()
	const hi = 3000
	const span = 120
	f := testWithdrawAdmitted(t, "f112-t1", 1)
	e := f.e
	led, _, err := e.asm.jsa.loadGenLedger(e.sess.id, e.asm.sessStreamEpoch, f.gen)
	if err != nil || led == nil || led.Hi >= hi-span {
		t.Fatalf("fixture ledger: %+v (%v)", led, err)
	}
	for ord := hi - span + 1; ord <= hi; ord++ {
		d := &mqttDutyEntry{ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Gen: f.gen, Ord: uint64(ord),
			Kind: mqttDutyCleanup, Ref: mqttNewInflightNonce(), State: mqttDutyTerminal, Outcome: "test-terminal"}
		body, _ := json.Marshal(d)
		if _, err := e.asm.jsa.storeMsg(mqttDutySubject(e.sess.idHash, f.gen, uint64(ord)), -1, body); err != nil {
			t.Fatalf("store duty %d: %v", ord, err)
		}
	}
	next := *led
	next.Hi, next.Lo, next.Del = hi, hi-span+1, hi-span+1
	lbody, _ := json.Marshal(&next)
	if _, err := e.asm.jsa.storeMsg(mqttGenLedgerSubject(e.sess.idHash, f.gen), -1, lbody); err != nil {
		t.Fatalf("advance the ledger: %v", err)
	}
	var reads, below atomic.Int64
	dutyPrefix := mqttF105SubjectPrefix() + mqttDutyClassTk + "."
	e.asm.mu.Lock()
	e.asm.testInflightLoadMulti = func(subjects []string) ([]*JSApiMsgGetResponse, error) {
		for _, s := range subjects {
			if !strings.HasPrefix(s, dutyPrefix) {
				continue
			}
			reads.Add(1)
			if _, ord, perr := mqttParseDutySubject(s, e.sess.idHash); perr == nil && ord < next.Lo {
				below.Add(1)
			}
		}
		return e.asm.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
	}
	e.asm.mu.Unlock()
	t.Cleanup(func() { e.asm.mu.Lock(); e.asm.testInflightLoadMulti = nil; e.asm.mu.Unlock() })

	testRestart(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatalf("recoverInflightRegisters: %v", err)
	}
	if got := reads.Load(); got != span {
		t.Fatalf("the resume read %d duty positions, want exactly the span %d", got, span)
	}
	if n := below.Load(); n != 0 {
		t.Fatalf("the resume read %d positions below the floor", n)
	}
}

// T10. The ended-lineage veto's exit (M13a/b/c). An entry is pruned only with no projection
// naming it, no resume in flight, and its register no longer naming it; each condition alone keeps
// it, and a kept entry still vetoes an AckRoute.
func TestF112TheEndedLineageVetoExitsOnlyWhenNothingCanReinstallIt(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-t10", 1)
	e := f.e
	sess := e.sess
	gen := f.gen
	const freePI = 999
	prune := func() {
		t.Helper()
		sess.pruneEndedLineagesStep(gen, e.asm.sessStreamEpoch)
	}
	kept := func(lin string) bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		_, ok := sess.custody.endedLineages[lin]
		return ok
	}

	// (c) THE REGISTER STILL NAMES IT. The fixture's own projection of f.lin is dropped first, so
	// the register is the ONLY thing keeping the entry.
	sess.mu.Lock()
	delete(sess.pendingPublish, f.pi)
	sess.custody.noteEnded(f.lin, f.pi)
	sess.mu.Unlock()
	prune()
	if !kept(f.lin) {
		t.Fatal("an ended lineage whose register still names it was pruned (M13c)")
	}

	// (b) A RESUME IN FLIGHT.
	gone := "lin-gone-" + mqttNewInflightNonce()
	sess.mu.Lock()
	sess.custody.noteEnded(gone, freePI)
	sess.custody.beginResume()
	sess.mu.Unlock()
	prune()
	if !kept(gone) {
		t.Fatal("an ended lineage was pruned while a resume was in flight (M13b)")
	}
	sess.mu.Lock()
	sess.custody.endResume()
	sess.mu.Unlock()

	// (a) A PROJECTION NAMES IT.
	sess.mu.Lock()
	sess.pendingPublish[freePI] = &mqttPending{ev: mqttPendingEvidence{gen: gen, lin: gone}}
	sess.mu.Unlock()
	prune()
	if !kept(gone) {
		t.Fatal("an ended lineage a projection still names was pruned (M13a)")
	}
	// (e) A KEPT ENTRY STILL VETOES AN ACKROUTE.
	sess.mu.Lock()
	route := testLateAckUnitBinding(sess, mqttAckRoleAckRoute, freePI, "n-gone", gone, "dur")
	installed := sess.installAbandonedAckLocked(route)
	delete(sess.pendingPublish, freePI)
	sess.mu.Unlock()
	if installed {
		t.Fatal("an AckRoute was installed for a kept ended lineage")
	}

	// (d) ALL THREE HOLD: pruned.
	prune()
	if kept(gone) {
		t.Fatal("an ended lineage with no projection, no resume and no register was not pruned")
	}
	if !kept(f.lin) {
		t.Fatal("the lineage whose register still names it was pruned with the other")
	}
}

// T16. The floor's demand is a latch that survives a closing incarnation (M16): its tick fires
// into the closing incarnation, the start is refused at the lease, and the reopen re-arms it. The
// floor is the only owed work.
func TestF112TheFloorLatchSurvivesACloseAndReopen(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-t16", 1)
	sess := f.e.sess
	srv := f.e.s
	testDriverWait(t, "quiescent", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return !sess.custodyObserving
	})
	rec := testDriverInstall(sess)
	sess.mu.Lock()
	leases := sess.leases
	owedElse := len(sess.custodyHints) > 0 || sess.custodyPassWanted || len(sess.custodyPassQueue) > 0 ||
		sess.custodyDemand || len(sess.custodyRetry) > 0 || len(sess.withdrawOwed) > 0 ||
		len(sess.admissionContinuity) > 0 || sess.custody.censusWanted
	tick := sess.custodyTickKey
	sess.floorWanted, sess.floorDueAt = true, time.Now().Add(300*time.Millisecond)
	gen := sess.gen
	sess.mu.Unlock()
	if owedElse || tick != _EMPTY_ {
		t.Fatalf("fixture: the floor must be the only owed work, with no tick (other work %v, tick %q)", owedElse, tick)
	}
	floorRuns := func() int {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		n := 0
		for _, u := range rec.units {
			if u.kind == mqttCustodyUnitFloor && u.gen == gen {
				n++
			}
		}
		return n
	}
	sess.startCustodyDispatcher()
	var key string
	testDriverWait(t, "the run ends with the floor's tick armed", 5*time.Second, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		key = sess.custodyTickKey
		return !sess.custodyObserving && key != _EMPTY_
	})
	leases.close()
	if n := floorRuns(); n != 0 {
		t.Fatalf("fixture: the floor unit ran (%d) before the incarnation closed", n)
	}
	testDriverWait(t, "the floor's tick fires into the closing incarnation", 5*time.Second, func() bool {
		sess.mu.Lock()
		cur := sess.custodyTickKey
		sess.mu.Unlock()
		return cur != key && !testDriverTimerRegistered(srv, key)
	})
	sess.mu.Lock()
	owed := sess.floorWanted
	sess.mu.Unlock()
	if !owed || floorRuns() != 0 {
		t.Fatalf("the closing incarnation must refuse the tick's start and keep the floor owed (owed %v, runs %d)",
			owed, floorRuns())
	}
	leases.reopen()
	testCustodyEventually(t, "the reopen re-arms the owed floor and it runs", func() bool {
		return floorRuns() > 0
	})
	// M17: a delayed unit of a generation that no longer stands changes nothing: this generation's
	// latch, deadline and horizon stay exactly as they were.
	far := time.Now().Add(time.Hour)
	sess.mu.Lock()
	sess.floorWanted, sess.floorDueAt, sess.floorDelay = true, far, 0
	sess.mu.Unlock()
	sess.runFloorUnit("0000000000000000000000", f.e.asm.sessStreamEpoch)
	sess.mu.Lock()
	stray := !sess.floorWanted || !sess.floorDueAt.Equal(far) || sess.floorDelay != 0
	sess.floorWanted, sess.floorDueAt = false, time.Time{}
	sess.mu.Unlock()
	if stray {
		t.Fatal("a floor unit of another generation changed this generation's floor state")
	}
}

// T13. A retired position's uncertainty waits for a census (M15). A duty subject held uncertain
// by this session is retired and deleted behind the floor; its individual re-read does not
// discharge it, and the next complete census does.
func TestF112ARetiredPositionsUncertaintyWaitsForACensus(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-t13", 1)
	e := f.e
	sess := e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	ord := testDutyOrdinalOf(t, e, f.gen, f.lin)
	if got := sess.withdrawUnsent(f.gen, f.subject, f.nonce, f.lin, mqttTestLicence(sess, nil), nil); got != mqttWithdrawLanded {
		t.Fatalf("withdrawing the fixture: %v", got)
	}
	d, _, derr := jsa.loadDutyEntryAt(sess.id, epoch, f.gen, ord)
	if derr != nil || d == nil {
		t.Fatalf("reading the duty: %v", derr)
	}
	root := &mqttRecoveryRoot{ID: sess.id, Epoch: epoch, Gen: f.gen}
	if err := jsa.opResolveDuty(root, mqttSweptDuty{Ord: ord, Entry: d}, mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving the duty: %v", err)
	}
	subj := mqttDutySubject(sess.idHash, f.gen, ord)
	sess.mu.Lock()
	if sess.custody.uncertainSubjects == nil {
		sess.custody.uncertainSubjects = map[string]uint64{}
	}
	sess.custody.uncertainSubjects[subj] = 1
	sess.mu.Unlock()

	for i := 0; i < 64; i++ {
		fb, err := jsa.composeFloorBatch(sess.id, epoch, f.gen, ord+1)
		if err != nil || fb == nil || len(fb.Positions) == 0 {
			break
		}
		if err := jsa.opAdvanceFloor(fb, mqttNewInflightNonce()); err != nil {
			t.Fatalf("advancing the floor: %v", err)
		}
	}
	for i := 0; i < 64; i++ {
		more, err := jsa.deleteRetiredPositionsStep(sess.id, epoch, f.gen)
		if err != nil {
			t.Fatalf("deleting: %v", err)
		}
		if !more {
			break
		}
	}
	led, _, _ := jsa.loadGenLedger(sess.id, epoch, f.gen)
	if led == nil || led.Lo <= ord || led.Del != led.Lo {
		t.Fatalf("fixture: the position was not retired and deleted: %+v", led)
	}

	sess.observeCustodyTarget(f.gen, mqttCustodyTarget{kind: mqttCustodyTargetUncertain, key: subj})
	sess.mu.Lock()
	_, still := sess.custody.uncertainSubjects[subj]
	wanted := sess.custody.censusWanted
	sess.mu.Unlock()
	if !still || !wanted {
		t.Fatalf("an individual re-read of a retired position must keep the uncertainty and demand a census "+
			"(uncertain %v, census %v) (M15)", still, wanted)
	}
	sess.reconstructCustody(f.gen)
	sess.mu.Lock()
	_, still = sess.custody.uncertainSubjects[subj]
	sess.mu.Unlock()
	if still {
		t.Fatal("a complete census did not discharge the uncertainty of a retired position")
	}
}

// T5 / T5b. Admission continuity across a reclamation (M7, M11). A continuity record whose
// admission LANDED and whose exchange was since ended and reclaimed behind the floor retires
// WITHOUT a NAK — the client already completed the message, so redelivering its source would
// duplicate it. A record whose attempt was positively ABORTED (nothing landed) still NAKs. One
// whose committed lineage is still named inside the floor is retained.
func TestF112AdmissionContinuityDoesNotNakAReclaimedAdmission(t *testing.T) {
	t.Parallel()
	f := testWithdrawAdmitted(t, "f112-t5", 1)
	e := f.e
	sess := e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	gen := f.gen

	compose := func(pi uint16) (*mqttAuthorityTransition, *mqttTransitionHandle, string) {
		t.Helper()
		al := jsa.loadOutboundAuthority(sess.id, epoch)
		txn := mqttNewInflightNonce()
		x := &mqttOutboundExchange{
			ID: sess.id, Epoch: epoch, Gen: gen, Nonce: mqttNewInflightNonce(),
			PI: pi, QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, err := testAdmitPublishTr(t, jsa, al.rec, al.seq, txn, "op-"+txn, x)
		if err != nil {
			t.Fatalf("composing: %v", err)
		}
		h, herr := mqttNewTransitionHandle(tr)
		if herr != nil {
			t.Fatalf("freezing: %v", herr)
		}
		return tr, h, x.Nonce
	}
	// The shim's own NATS connection: the NAK is published through it, and a subscription on it
	// sees it (no echo suppression in this rig).
	nc := e.s.nrc
	run := func(lin string, pi uint16, h *mqttTransitionHandle) (retired, nakked bool) {
		t.Helper()
		reply := "f112.t5.nak." + mqttNewInflightNonce()
		sub := natsSubSync(t, nc, reply)
		defer func() { _ = sub.Unsubscribe() }() // cleanup; the connection closes with the test
		nc.Flush()
		c := &mqttAdmissionContinuity{gen: gen, epoch: epoch, dur: "f112-dur", seq: uint64(pi), nonce: "alloc-" + lin,
			lin: lin, pi: pi, reply: reply, attempts: []mqttCustodyAttempt{{handle: h, lin: lin}}}
		sess.mu.Lock()
		if sess.admissionContinuity == nil {
			sess.admissionContinuity = map[mqttDeferredKey]*mqttAdmissionContinuity{}
		}
		sess.admissionContinuity[mqttDeferredKey{c.dur, c.seq}] = c
		sess.mu.Unlock()
		sess.runAdmissionContinuityUnit(gen, c)
		sess.mu.Lock()
		retired = sess.admissionContinuity[mqttDeferredKey{c.dur, c.seq}] != c
		delete(sess.admissionContinuity, mqttDeferredKey{c.dur, c.seq})
		sess.mu.Unlock()
		_, nerr := sub.NextMsg(300 * time.Millisecond)
		return retired, nerr == nil
	}

	// LANDED, ENDED, RECLAIMED.
	tr, h, lin := compose(777)
	if _, err := jsa.commitAuthorityTransition(tr); err != nil {
		t.Fatalf("admitting: %v", err)
	}
	if _, err := testAdvanceLive(t, jsa, sess.id, epoch, gen, lin, mqttExchangeTerminal); err != nil {
		t.Fatalf("ending: %v", err)
	}
	testDropEndedSlot(t, jsa, sess.id, epoch, gen, lin)
	ord, found, ferr := jsa.findDutyInInventory(sess.id, epoch, gen, func(d *mqttDutyEntry) bool { return d.Ref == lin })
	if ferr != nil || !found {
		t.Fatalf("finding the duty: %v", ferr)
	}
	d, _, _ := jsa.loadDutyEntryAt(sess.id, epoch, gen, ord)
	root := &mqttRecoveryRoot{ID: sess.id, Epoch: epoch, Gen: gen}
	if err := jsa.opResolveDuty(root, mqttSweptDuty{Ord: ord, Entry: d}, mqttNewInflightNonce()); err != nil {
		t.Fatalf("resolving: %v", err)
	}
	// STILL NAMED INSIDE THE FLOOR: a second landed lineage, admitted after the first (so above
	// it), whose exchange record is made absent while its duty stands. Retained, nothing sent.
	tr2, h2, lin2 := compose(779)
	if _, err := jsa.commitAuthorityTransition(tr2); err != nil {
		t.Fatalf("admitting the second: %v", err)
	}
	if err := jsa.deleteReclaimedExchange(sess.id, epoch, gen, lin2); err != nil {
		t.Fatalf("removing the second exchange: %v", err)
	}
	if retired, nak := run(lin2, 779, h2); retired || nak {
		t.Fatalf("an absence still named inside the floor must be retained without a NAK (retired %v, nak %v)", retired, nak)
	}
	for i := 0; i < 64; i++ {
		fb, err := jsa.composeFloorBatch(sess.id, epoch, gen, ord+1)
		if err != nil || fb == nil || len(fb.Positions) == 0 {
			break
		}
		if err := jsa.opAdvanceFloor(fb, mqttNewInflightNonce()); err != nil {
			t.Fatalf("advancing the floor: %v", err)
		}
	}
	if led, _, _ := jsa.loadGenLedger(sess.id, epoch, gen); led == nil || led.Lo <= ord {
		t.Fatalf("fixture: the position was not retired: %+v", led)
	}
	for i := 0; i < 64; i++ {
		more, err := jsa.deleteRetiredPositionsStep(sess.id, epoch, gen)
		if err != nil {
			t.Fatalf("deleting the retired positions: %v", err)
		}
		if !more {
			break
		}
	}
	if _, _, xerr := jsa.loadOutboundExchange(sess.id, epoch, gen, lin); xerr == nil || !mqttExchangeReadAbsent(xerr) {
		t.Fatalf("fixture: the reclaimed exchange still reads: %v", xerr)
	}
	if retired, nak := run(lin, 777, h); !retired || nak {
		t.Fatalf("a reclaimed admission must retire WITHOUT a NAK (retired %v, nak %v) (M7)", retired, nak)
	}

	// UNRESOLVED (holistic round 1): an attempt whose outcome does not exist yet — a batch still in
	// flight, or a read that failed — proves neither direction: the record is RETAINED and nothing
	// is NAKed.
	_, uh, ulin := compose(780)
	if retired, nak := run(ulin, 780, uh); retired || nak {
		t.Fatalf("an unresolved landing proof must retain the record without a NAK (retired %v, nak %v)", retired, nak)
	}

	// ABORTED: nothing landed, the NAK stands (M11).
	_, ah, alin := compose(778)
	if err := jsa.abortTransition(&ah.attempt); err != nil {
		t.Fatalf("aborting: %v", err)
	}
	if retired, nak := run(alin, 778, ah); !retired || !nak {
		t.Fatalf("a never-landed admission must retire WITH a NAK (retired %v, nak %v) (M11)", retired, nak)
	}
}

// T14. A process that crashes between a completion and its resolution (M-fresh). Three hundred
// obligations complete while this broker's floor unit is held back, so none of their duties is
// resolved; then the process is replaced — every in-memory projection and the custody ledger
// forgotten, as a restart does, without restarting the store (whose upstream version revival,
// MAXMSGSPER-REVIVAL, would bring back registers the floor then rightly carries). The fresh
// state's census arms the floor, and its unit resolves the duties FROM THE INVENTORY — this
// process never held those exchanges — and reclaims them rather than carrying them.
func TestF112AFreshProcessResolvesWhatItsPredecessorCompleted(t *testing.T) {
	t.Parallel()
	const n = 300
	f := testWithdrawAdmitted(t, "f112-t14", 1)
	e := f.e
	sess := e.sess
	jsa := &e.asm.jsa
	epoch := e.asm.sessStreamEpoch
	gen := f.gen
	var held atomic.Bool
	held.Store(true)
	sess.mu.Lock()
	if sess.testCustodyDriver == nil {
		sess.testCustodyDriver = &mqttCustodyDriverSeams{}
	}
	sess.testCustodyDriver.floorHeld = func(string) bool { return held.Load() }
	sess.mu.Unlock()

	for i := 0; i < n; i++ {
		al := jsa.loadOutboundAuthority(sess.id, epoch)
		txn := mqttNewInflightNonce()
		x := &mqttOutboundExchange{
			ID: sess.id, Epoch: epoch, Gen: gen, Nonce: mqttNewInflightNonce(),
			PI: 900 + uint16(i), QoS: 1, Phase: mqttExchangePublish, AdmitTxn: txn, Tenure: al.rec.Tenure,
		}
		tr, err := testAdmitPublishTr(t, jsa, al.rec, al.seq, txn, "op-"+txn, x)
		if err != nil {
			t.Fatalf("composing %d: %v", i, err)
		}
		if _, err := jsa.commitAuthorityTransition(tr); err != nil {
			t.Fatalf("admitting %d: %v", i, err)
		}
		if _, err := testAdvanceLive(t, jsa, sess.id, epoch, gen, x.Nonce, mqttExchangeTerminal); err != nil {
			t.Fatalf("ending %d: %v", i, err)
		}
		testDropEndedSlot(t, jsa, sess.id, epoch, gen, x.Nonce)
	}
	before, _, _ := jsa.loadGenLedger(sess.id, epoch, gen)
	if before == nil || before.Lo != 1 || before.Hi < n {
		t.Fatalf("fixture: the held floor must not have moved: %+v", before)
	}

	// THE FRESH PROCESS.
	testRestart(e)
	held.Store(false)
	sess.reconstructCustody(gen)

	deadline := time.Now().Add(30 * time.Second)
	var led *mqttGenLedger
	for time.Now().Before(deadline) {
		led, _, _ = jsa.loadGenLedger(sess.id, epoch, gen)
		if led != nil && led.Lo > n-mqttFloorSlack {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if led == nil || led.Lo <= n-mqttFloorSlack {
		t.Fatalf("the fresh process did not resolve and reclaim its predecessor's completions: %+v", led)
	}
	// RECLAIMED, NOT CARRIED: what moved above the old high-water is only the fixture's own
	// long-lived duties (its source consumer's and its outstanding obligation's), never the
	// completed exchanges.
	if carried := led.Hi - before.Hi; carried > 8 {
		t.Fatalf("the floor carried %d positions above the old high-water: completed exchanges were "+
			"relocated instead of resolved and reclaimed", carried)
	}
}
