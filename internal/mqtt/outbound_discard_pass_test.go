//go:build mqttport_fulltests

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

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// F-105 7d: the discard census, the executor's discard work, the release gate's root arm and the
// prompt worker's scheduling (plan-7d T3, T4d-f, T7, T9, T12, T13, T15).

// testDiscardClose closes the environment's SERVING generation DISCARDING through the production
// route — a Clean-Start handoff's prepare, metadata commit and finalize — after the test admitted
// what it needs into it, and returns the closed generation.
func testDiscardClose(t *testing.T, e *testF105Env) string {
	t.Helper()
	jsa := &e.asm.jsa
	al := jsa.loadOutboundAuthority(e.id, e.epoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.State != mqttAuthorityServing {
		t.Fatalf("fixture: no serving authority to close: %v", al.status)
	}
	first := al.rec
	ps, seq := testF105StandingSession(t, e)
	intent := &mqttHandoffIntent{
		ID: "intent-discard", PrevGen: first.Gen, PrevTenure: first.Tenure,
		PrevOwnerTxn: first.OwnerTxn, PrevSessWID: ps.WID, PrevSessSeq: seq,
		Gen: testF105Gen2, Tenure: "tenure-discard", CleanReq: true,
		Retire: &mqttRetirementRequest{
			Gen: first.Gen, Tenure: first.Tenure, OwnerTxn: first.OwnerTxn,
			SessWID: ps.WID, SessSeq: seq, Reason: mqttRetireCleanStart, Discard: true,
		},
	}
	if _, err := jsa.opPrepareHandoff(e.id, e.epoch, "txn-prep-discard", intent); err != nil {
		t.Fatalf("preparing the clean start: %v", err)
	}
	if _, _, err := jsa.opCommitMetadata(&mqttCommitMetadataRequest{
		ID: e.id, Epoch: e.epoch, IntentID: intent.ID, SessWID: mqttNewSessGen(),
		Record:   &mqttPersistedSession{Origin: "o"},
		AcceptID: mqttNewAcceptID(), AcceptAt: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("the clean start's metadata half: %v", err)
	}
	if _, err := jsa.opFinalizeHandoff(e.id, e.epoch, "txn-fin-discard", intent.ID); err != nil {
		t.Fatalf("finalizing the discarding handoff: %v", err)
	}
	if err := jsa.requireDiscardingClosure(e.id, e.epoch, first.Gen); err != nil {
		t.Fatalf("fixture: the generation was not closed discarding: %v", err)
	}
	return first.Gen
}

// testDiscardDutyOrd finds the ordinal of an obligation's exchange duty.
func testDiscardDutyOrd(t *testing.T, e *testF105Env, gen, nonce string) uint64 {
	t.Helper()
	led, _, err := e.asm.jsa.loadGenLedger(e.id, e.epoch, gen)
	if err != nil || led == nil {
		t.Fatalf("the ledger of %q: %v", gen, err)
	}
	for ord := uint64(1); ord <= led.Hi; ord++ {
		d, _, derr := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, ord)
		if derr == nil && d != nil && d.Kind == mqttDutyExchange && d.Ref == nonce {
			return ord
		}
	}
	t.Fatalf("no exchange duty names %q", nonce)
	return 0
}

// testDiscardResolveDutyRaw plants an obligation's exchange duty as already RESOLVED — the shape a
// delivery the broker abandoned, or an ending whose cleanup crashed, leaves before the discard —
// written raw, because what is under test is the census reading it, not the act that resolves it.
func testDiscardResolveDutyRaw(t *testing.T, e *testF105Env, gen, nonce string) {
	t.Helper()
	ord := testDiscardDutyOrd(t, e, gen, nonce)
	d, seq, err := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, ord)
	if err != nil || d == nil {
		t.Fatalf("loading duty %d: %v", ord, err)
	}
	d.State, d.Outcome = mqttDutyTerminal, "resolved"
	b, _ := json.Marshal(d)
	msg, hdr := mqttFrameBatchWrite(seq, b, _EMPTY_, 0, false)
	if _, serr := e.asm.jsa.storeMsg(mqttDutySubject(getHash(e.id), gen, ord), hdr, msg); serr != nil {
		t.Fatalf("planting the resolved duty: %v", serr)
	}
}

// testDiscardCertified reports whether the generation certified complete.
func testDiscardCertified(e *testF105Env, gen string) bool {
	cert, err := e.asm.jsa.loadGenCompletionCert(e.id, e.epoch, gen)
	return err == nil && cert != nil
}

// testDiscardSweepUntilCertified runs passes, checking after EACH that a certificate never stands
// while inv fails, until the generation certifies or the bound runs out.
func testDiscardSweepUntilCertified(t *testing.T, e *testF105Env, gen string, inv func() error) {
	t.Helper()
	for i := 0; i < 8*mqttDirShards; i++ {
		e.asm.sweepOutboundRecovery(e.asm.server(), nil)
		certified := testDiscardCertified(e, gen)
		if err := inv(); err != nil && certified {
			t.Fatalf("pass %d: the generation CERTIFIED while %v", i, err)
		}
		if certified {
			if err := inv(); err != nil {
				t.Fatalf("certified with work left: %v", err)
			}
			return
		}
	}
	t.Fatalf("the generation never certified: %v", inv())
}

// T3a: in a discarded generation an abandoned delivery is owed its discard, not resolved as a duty
// that "owes no further delivery" (M10).
func TestF105DiscardedAbandonedDeliveryOwesItsDiscard(t *testing.T) {
	f := testAbandonAdmitted(t, "f105-discard-t3a", 1)
	f.deny()
	f.requireAbandoned(t)
	jsa, id, epoch := &f.e.asm.jsa, f.e.sess.id, f.e.asm.sessStreamEpoch
	d := &mqttDutyEntry{Kind: mqttDutyExchange, Ref: f.lin}
	if v := jsa.classifyOutstandingDutyIn(id, epoch, f.gen, false, d); v.Disposition != mqttDutyMayResolve {
		t.Fatalf("fixture: a live generation's abandoned delivery classifies %+v, want may-resolve", v)
	}
	if v := jsa.classifyOutstandingDutyIn(id, epoch, f.gen, true, d); v.Disposition != mqttDutyOwesWork ||
		v.Work != mqttDutyWorkDiscardEnd {
		t.Fatalf("a discarded generation's abandoned delivery classifies %+v, want owes discard-end", v)
	}
}

// T3b: an obligation whose duty already RESOLVED while it still holds is found by the census, ended,
// and only then does the generation certify (M15).
func TestF105DiscardCensusFindsAResolvedObligation(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-census")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, _, _ := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
	gen := testDiscardClose(t, e)
	testDiscardResolveDutyRaw(t, e, gen, nonce)
	testDiscardSweepUntilCertified(t, e, gen, func() error {
		x, _ := testF105LoadExchange(t, e, gen, nonce)
		if x.Phase != mqttExchangeDiscarded {
			return fmt.Errorf("the resolved obligation stands at phase %d", x.Phase)
		}
		testRequireHolds(t, &e.asm.jsa, x, false)
		return nil
	})
}

// T4d / T4e: an obligation that ENDED, whose duty resolved, still owes its canonical PUBREL (rel
// only) or its register (register only) in a discarded generation; neither certifies past it, and
// each is deleted by the census (M7a, M7b). The ending itself is preserved.
func TestF105DiscardCensusDeletesWhatAnEndedObligationLeft(t *testing.T) {
	for _, which := range []string{"rel only", "register only"} {
		t.Run(which, func(t *testing.T) {
			e, done := testF105NewEnv(t, "f105-discard-left-"+map[string]string{"rel only": "rel", "register only": "reg"}[which])
			defer done()
			e.bootstrap(t, "txn-1")
			testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
			nonce, regSubj, _, regSeq := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
			if _, err := e.asm.jsa.opAdvanceExchange(e.id, e.epoch, "txn-end", "op-end", testF105Gen,
				nonce, mqttExchangeTerminal); err != nil {
				t.Fatalf("ending the obligation: %v", err)
			}
			relSubj := mqttRetainedRelSubject(getHash(e.id), testF105Gen, nonce)
			if which == "rel only" {
				if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, regSeq, true); err != nil {
					t.Fatalf("removing the register: %v", err)
				}
				b, _ := json.Marshal(&mqttRetainedRel{ID: e.id, Epoch: e.epoch, Gen: testF105Gen,
					Nonce: nonce, PI: 7, Identity: mqttNewInflightNonce()})
				msg, hdr := mqttFrameBatchWrite(0, b, _EMPTY_, 0, false)
				if _, err := e.asm.jsa.storeMsg(relSubj, hdr, msg); err != nil {
					t.Fatalf("planting the canonical PUBREL: %v", err)
				}
			}
			gen := testDiscardClose(t, e)
			testDiscardResolveDutyRaw(t, e, gen, nonce)
			standing := func(subj string) bool {
				_, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj)
				return err == nil
			}
			testDiscardSweepUntilCertified(t, e, gen, func() error {
				if standing(relSubj) {
					return errors.New("the canonical PUBREL still stands")
				}
				if standing(regSubj) {
					return errors.New("the register still stands")
				}
				if x, _ := testF105LoadExchange(t, e, gen, nonce); x.Phase != mqttExchangeTerminal {
					return fmt.Errorf("the ending was rewritten to %d", x.Phase)
				}
				return nil
			})
		})
	}
}

// T4f: a root whose prefix holds more resolved, edge-free obligations than one visit's budget can
// classify still certifies — every visit classifies at least one entry — and a SHUTDOWN at any point
// of a visit never lets the cursor pass an entry it did not classify (M7c, M20).
func TestF105DiscardCensusSurvivesBudgetAndShutdown(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-budget")
	defer done()
	e.bootstrap(t, "txn-1")
	const n = 140
	nonces := make([]string, 0, n)
	for i := 0; i < n; i++ {
		inc := fmt.Sprintf("inc-%03d", i)
		testF105PlantResource(t, e, inc, mqttResourceSourceConsumer)
		nonce, _, _, _ := testF105AdmitQoS(t, e, fmt.Sprintf("txn-adm-%03d", i), uint16(i+1), 1, inc)
		nonces = append(nonces, nonce)
	}
	gen := testDiscardClose(t, e)
	for _, nonce := range nonces {
		testDiscardResolveDutyRaw(t, e, gen, nonce)
	}
	allDiscarded := func() error {
		for _, nonce := range nonces {
			if x, _ := testF105LoadExchange(t, e, gen, nonce); x.Phase != mqttExchangeDiscarded {
				return fmt.Errorf("obligation %q stands at phase %d", nonce, x.Phase)
			}
		}
		return nil
	}
	// SHUTDOWNS FIRST: visits stopped after k charged operations, for many k, each followed by the
	// invariant — a certificate never stands while an obligation is undiscarded.
	closed, _, _ := e.asm.jsa.loadGenClosure(e.id, e.epoch, gen)
	for k := 1; k <= 60; k += 3 {
		root, rootSeq, err := e.asm.jsa.loadRecoveryRoot(closed.RootShard, closed.RootOrd)
		if err != nil || root == nil {
			break
		}
		calls := 0
		e.asm.visitRecoveryRoot(e.asm.server(), root, rootSeq, newMqttRecoveryBudget(), func() bool {
			calls++
			return calls > k
		})
		if testDiscardCertified(e, gen) {
			if err := allDiscarded(); err != nil {
				t.Fatalf("after a visit stopped at %d: CERTIFIED while %v", k, err)
			}
		}
	}
	testDiscardSweepUntilCertified(t, e, gen, allDiscarded)
}

// T9: a reachable root is durable recovery ownership only under a DISCARDING closure (M9).
func TestF105ReleaseGateRootArmNeedsADiscard(t *testing.T) {
	sealed := &mqttGenLedger{State: mqttGenLedgerSealed, Hi: 3, SealedHi: 3}
	base := func(discard mqttFactState) mqttAdoptedReleaseFacts {
		return mqttAdoptedReleaseFacts{Closed: &mqttGenClosure{}, Ledger: sealed, Discard: discard, RootEnrolled: true}
	}
	if blocks := mqttAdoptedReleaseBlockedBy("c", "g", base(mqttFactApplies)); len(blocks) != 0 {
		t.Fatalf("a discarding generation with a reachable root was refused: %v", blocks)
	}
	if blocks := mqttAdoptedReleaseBlockedBy("c", "g", base(mqttFactDoesNotApply)); len(blocks) != 1 ||
		blocks[0].Fact != "durable recovery ownership" {
		t.Fatalf("a preserving generation with only a root was released: %v", blocks)
	}
	empty := base(mqttFactDoesNotApply)
	empty.Ledger = &mqttGenLedger{State: mqttGenLedgerSealed}
	if blocks := mqttAdoptedReleaseBlockedBy("c", "g", empty); len(blocks) != 0 {
		t.Fatalf("a preserving generation with an EMPTY sealed inventory was refused: %v", blocks)
	}
	certified := base(mqttFactDoesNotApply)
	certified.Cert = &mqttGenCompletionCertificate{SealedHi: 3}
	if blocks := mqttAdoptedReleaseBlockedBy("c", "g", certified); len(blocks) != 0 {
		t.Fatalf("a preserving generation that certified was refused: %v", blocks)
	}
	retired := base(mqttFactApplies)
	retired.RootEnrolled, retired.Cert = false, &mqttGenCompletionCertificate{SealedHi: 3}
	if blocks := mqttAdoptedReleaseBlockedBy("c", "g", retired); len(blocks) != 0 {
		t.Fatalf("a discarded generation whose root retired behind its certificate was refused: %v", blocks)
	}
}

// T12: a discarded generation's PUBREL consumer is not disposed of while an enrolled affine
// consumer's readiness — the PUBREC proofs secured — does not stand (M8).
func TestF105DiscardedPubRelWaitsForAffineReadiness(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-affine")
	defer done()
	const affGen = "0123456789abcdefghijkq"
	_, _, durable := mqttQ2AffineNames(getHash(e.id), affGen)
	gen, aff := testF105DiscardingCloseWithAffine(t, e, affGen, durable)
	if gen != testF105Gen {
		t.Fatalf("fixture: closed %q, the planting helpers write %q", gen, testF105Gen)
	}
	testF105PlantResource(t, e, "inc-rel", mqttResourcePubRelConsumer)
	root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: gen}
	if ok, _ := e.asm.pubRelDisposalPrerequisites(root); ok {
		t.Fatal("the PUBREL consumer may be disposed of before the affine readiness stands")
	}
	d := mqttSweptDuty{Entry: &mqttDutyEntry{Kind: mqttDutyCleanup, Ref: "inc-rel"}}
	v := mqttDutyVerdict{Disposition: mqttDutyOwesWork, Work: mqttDutyWorkDispose}
	e.asm.performDiscardWork(e.asm.server(), root, d, v, newMqttRecoveryBudget())
	if r, _ := testF105LoadResource(t, e, "inc-rel"); r.Disposal != nil {
		t.Fatalf("the PUBREL consumer was disposed of before the readiness: %+v", r.Disposal)
	}
	if err := e.asm.jsa.publishAffineReadiness(&mqttAffineReadiness{ID: e.id, Epoch: e.epoch, Gen: gen,
		AffGen: aff.AffGen, Durable: aff.Name, PubRelDur: aff.PubRelDur}); err != nil {
		t.Fatalf("asserting the readiness: %v", err)
	}
	e.asm.performDiscardWork(e.asm.server(), root, d, v, newMqttRecoveryBudget())
	if r, _ := testF105LoadResource(t, e, "inc-rel"); r.Disposal == nil || r.Disposal.Reason != mqttDisposalGenerationDiscarded {
		t.Fatalf("with the readiness standing the PUBREL consumer was not disposed of: %+v", r.Disposal)
	}
}

// T13: the prompt worker's allowance, quantum and order, measured apart from what a visit does
// (M18, M19).
func TestF105RecoveryKickIsBoundedAndFair(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-kick")
	defer done()
	var mu sync.Mutex
	var order []string
	visits := map[string]int{}
	restore := setMQTTTestRecoveryKickVisit(func(id, gen string) bool {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, gen)
		visits[gen]++
		return gen == "actionable" && visits[gen] == 12
	})
	defer restore()
	e.asm.kickRecoveryRoot(e.id, e.epoch, "withheld")
	e.asm.kickRecoveryRoot(e.id, e.epoch, "actionable")
	checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
		e.asm.recoveryKick.mu.Lock()
		defer e.asm.recoveryKick.mu.Unlock()
		if e.asm.recoveryKick.running || len(e.asm.recoveryKick.queue) != 0 {
			return errors.New("the worker still has roots queued")
		}
		return nil
	})
	mu.Lock()
	defer mu.Unlock()
	if visits["withheld"] != mqttRecoveryKickMaxVisits {
		t.Fatalf("the withheld root was visited %d times, want exactly the allowance %d",
			visits["withheld"], mqttRecoveryKickMaxVisits)
	}
	if visits["actionable"] != 12 {
		t.Fatalf("the actionable root was visited %d times, want 12 (it concluded there)", visits["actionable"])
	}
	// ONE QUANTUM PER TURN: until the actionable root concluded, the withheld one never ran more
	// than a quantum in a row, and the actionable one had the second turn.
	lastActionable := 0
	for i, g := range order {
		if g == "actionable" {
			lastActionable = i
		}
	}
	run := 0
	for _, g := range order[:lastActionable] {
		if g == "withheld" {
			run++
			if run > mqttRecoveryKickQuantum {
				t.Fatalf("the withheld root ran %d visits in a row while the actionable one waited: %v", run, order)
			}
		} else {
			run = 0
		}
	}
	if order[mqttRecoveryKickQuantum] != "actionable" {
		t.Fatalf("the actionable root did not get the second turn: %v", order[:mqttRecoveryKickQuantum+1])
	}

	// FULL: a kick beyond the cap is dropped, and the set never grows past it.
	park := setMQTTTestRecoveryKickHook(func(string, string) bool { return false })
	defer park()
	for i := 0; i < mqttRecoveryKickCap+5; i++ {
		e.asm.kickRecoveryRoot(e.id, e.epoch, fmt.Sprintf("g%03d", i))
	}
	e.asm.recoveryKick.mu.Lock()
	n := len(e.asm.recoveryKick.queue)
	e.asm.recoveryKick.mu.Unlock()
	if n != mqttRecoveryKickCap {
		t.Fatalf("the set holds %d roots, want the cap %d", n, mqttRecoveryKickCap)
	}
}

// T7: an AMBIGUOUS physical delete records nothing — the resource stands DeleteAuthorized and its
// duty open — and the next visit repeats it and completes (continuation 4).
func TestF105DiscardAmbiguousDeleteIsRepeated(t *testing.T) {
	release := testDiscardParkAll(t)
	f := testDiscardTearDown(t, "discard-ambiguous", 1, false)
	var mu sync.Mutex
	failed := false
	restore := setMQTTTestRecoveryConsumerDeleteHook(func(stream, name string) error {
		mu.Lock()
		first := !failed
		failed = true
		mu.Unlock()
		if first {
			_, _ = f.asm.jsa.deleteConsumer(stream, name, false)
			return errors.New("test: the delete's response was lost")
		}
		_, err := f.asm.jsa.deleteConsumer(stream, name, false)
		return err
	})
	defer restore()
	release()
	f.asm.kickRecoveryRoot(f.id, f.epoch(), f.gen)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		return f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, f.src)
	})
	own := f.ownership(t, mqttOwnedStreamMsgs, f.src)
	if r := f.resource(t, own.OwnerInc); r.Outcome != mqttCleanupGone {
		t.Fatalf("the repeated delete was classified %q, want gone", r.Outcome)
	}
}

// T8: a late CREATE resurrecting a name the discard retired is the RETIRED-name orphan the plain
// sweep reaps.
func TestF105DiscardRetiredNameOrphanIsSwept(t *testing.T) {
	f := testDiscardTearDown(t, "discard-orphan", 1, false)
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		return f.retired(t, mqttOwnedStreamMsgs, mqttStreamName, f.src)
	})
	if _, err := f.asm.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttStreamName,
		Config: ConsumerConfig{Durable: f.src, DeliverSubject: mqttSubPrefix + "late",
			AckPolicy: AckExplicit, FilterSubject: mqttStreamSubjectPrefix + "discard.topic"}}); err != nil {
		t.Fatalf("the late CREATE: %v", err)
	}
	f.asm.sweepOrphanedConsumersBefore(f.s, time.Now().Add(time.Hour))
	if f.consumerExists(t, mqttStreamName, f.src) {
		t.Fatalf("the resurrected consumer %q under a RETIRED name was not swept", f.src)
	}
}

// T15: a discarding generation's named durable that no resource names is left standing and
// reported, never deleted unauthorized.
func TestF105DiscardLeavesAnUnenrolledDurable(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-unenrolled")
	defer done()
	e.bootstrap(t, "txn-1")
	gen := testDiscardClose(t, e)
	target := &mqttTeardownTarget{id: e.id, idHash: getHash(e.id), gen: gen, durs: []string{"never-claimed"}}
	own, err := e.asm.classifyTeardownGeneration(e.id, e.epoch, gen, target)
	if err != nil {
		t.Fatalf("classifying: %v", err)
	}
	if own.Class != mqttTeardownOwnerDiscarding || len(own.Unenrolled) != 1 || own.Unenrolled[0] != "never-claimed" {
		t.Fatalf("the unenrolled durable was not reported: %+v", own)
	}
	own.transfers(target)
	if !target.transferLogical {
		t.Fatal("a discarding classification did not transfer the logical-use targets")
	}
}

// T4 at the reap: a discarding teardown's register reap SPARES the retained register — its
// exchange's discard cleanup deletes it after the canonical PUBREL — and still reaps an ordinary
// one; the census then retires the retained obligation and its register before certifying (M6).
func TestF105DiscardReapSparesRetainedRegisters(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-retained")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	m, mSeq := testRetainedMaterialize(t, e, auth, "pay-d", "inc-pay", []byte("retained-discard"), 2)
	ret := testRetainedAdmit(t, e, auth, 11, m, mSeq)
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	_, ordSubj, _, _ := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
	idHash := getHash(e.id)
	retSubj := mqttInflightSubject(idHash, auth.Gen, 11)
	standing := func(subj string) bool {
		_, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, subj)
		return err == nil
	}
	if err := e.asm.reapIncarnationRegisters(nil, idHash, auth.Gen, true); err != nil {
		t.Fatalf("the sparing reap: %v", err)
	}
	if !standing(retSubj) {
		t.Fatal("the discarding reap deleted the retained register its exchange's cleanup owns")
	}
	if standing(ordSubj) {
		t.Fatal("the discarding reap spared an ordinary register")
	}
	gen := testDiscardClose(t, e)
	testDiscardSweepUntilCertified(t, e, gen, func() error {
		if standing(retSubj) {
			return errors.New("the retained register still stands")
		}
		if x, _ := testF105LoadExchange(t, e, gen, ret.Lin); x.Phase != mqttExchangeDiscarded {
			return fmt.Errorf("the retained obligation stands at phase %d", x.Phase)
		}
		return nil
	})
}

// The unit arithmetic: a budget holding exactly one entry unit above the reserve performs the
// costliest discard step of a census entry — its classification with the canonical PUBREL and the
// register, and the delete (M7c).
func TestF105RecoveryEntryUnitCoversTheCostliestEntry(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-unit")
	defer done()
	e.bootstrap(t, "txn-1")
	testF105PlantResource(t, e, "inc-src", mqttResourceSourceConsumer)
	nonce, _, _, _ := testF105AdmitQoS(t, e, "txn-adm", 7, 1, "inc-src")
	if _, err := e.asm.jsa.opAdvanceExchange(e.id, e.epoch, "txn-end", "op-end", testF105Gen,
		nonce, mqttExchangeTerminal); err != nil {
		t.Fatalf("ending the obligation: %v", err)
	}
	gen := testDiscardClose(t, e)
	testDiscardResolveDutyRaw(t, e, gen, nonce)
	ord := testDiscardDutyOrd(t, e, gen, nonce)
	d, _, _ := e.asm.jsa.loadDutyEntryAt(e.id, e.epoch, gen, ord)
	regSubj := mqttInflightSubject(getHash(e.id), gen, 7)
	b := &mqttRecoveryBudget{left: mqttRecoveryOpReserve + mqttRecoveryMinimumUnit, reserve: mqttRecoveryOpReserve}
	if !b.fitsAUnit() {
		t.Fatal("fixture: the budget does not fit one unit")
	}
	root := &mqttRecoveryRoot{ID: e.id, Epoch: e.epoch, Gen: gen}
	e.asm.executeRecoveryDuty(e.asm.server(), root, mqttSweptDuty{Ord: ord, Entry: d, Census: true}, b, nil, true)
	if _, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, regSubj); err == nil {
		t.Fatalf("one unit did not cover the census entry's delete (budget left %d)", b.left)
	}
}

// A consumer a route already DECIDED about while the generation served keeps that decision: the
// discard retires it from its record and never asks for another (M13). Its name is a real
// provisioned one, so the pass reaches it through its own cleanup duty.
func TestF105DiscardRetiresADecidedConsumer(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-decided")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	if _, err := e.asm.jsa.opProvisionResource(e.id, e.epoch, "txn-prov", "op-prov", auth.Gen,
		auth.Tenure, "inc-dec", "src-decided", mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning: %v", err)
	}
	if _, err := e.asm.jsa.opRequestDisposal(&mqttRequestDisposalRequest{ID: e.id, Epoch: e.epoch,
		Txn: "txn-route", Gen: auth.Gen, Inc: "inc-dec", Reason: mqttDisposalDrainRetired,
		Entitlement: mqttOutboundEntitlement{Gen: auth.Gen, Tenure: auth.Tenure}}); err != nil {
		t.Fatalf("the route's decision: %v", err)
	}
	gen := testDiscardClose(t, e)
	testDiscardSweepUntilCertified(t, e, gen, func() error {
		r, _, err := e.asm.jsa.loadOutboundResource(e.id, e.epoch, gen, "inc-dec")
		if err != nil || r == nil || r.State != mqttResourceCleanupComplete {
			return fmt.Errorf("the decided consumer is not retired: %+v (%v)", r, err)
		}
		if r.Disposal == nil || r.Disposal.Txn != "txn-route" || r.Disposal.Reason != mqttDisposalDrainRetired {
			return fmt.Errorf("the route's decision was not the one carried: %+v", r.Disposal)
		}
		return nil
	})
}

// T6: three drivers on one discarded root at once — passes and prompt visits interleaving on the
// same obligations and consumers — converge: every obligation discarded exactly once, every
// consumer retired, one certificate. Each act is a CAS'd transition or an act on a RETIRED name or
// an exact sequence, so a duplicate is refused or harmless (run under -race).
func TestF105DiscardConcurrentDriversConverge(t *testing.T) {
	e, done := testF105NewEnv(t, "f105-discard-concurrent")
	defer done()
	auth, _ := e.bootstrap(t, "txn-1")
	if _, err := e.asm.jsa.opProvisionResource(e.id, e.epoch, "txn-prov", "op-prov", auth.Gen,
		auth.Tenure, "inc-src", "src-concurrent", mqttResourceSourceConsumer); err != nil {
		t.Fatalf("provisioning: %v", err)
	}
	const n = 12
	nonces := make([]string, 0, n)
	for i := 0; i < n; i++ {
		nonce, _, _, _ := testF105AdmitQoS(t, e, fmt.Sprintf("txn-adm-%02d", i), uint16(i+1), 1, "inc-src")
		nonces = append(nonces, nonce)
	}
	gen := testDiscardClose(t, e)
	var wg sync.WaitGroup
	for d := 0; d < 3; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			for i := 0; i < 4*mqttDirShards && !testDiscardCertified(e, gen); i++ {
				if d == 0 {
					e.asm.kickRecoveryRoot(e.id, e.epoch, gen)
				}
				e.asm.sweepOutboundRecovery(e.asm.server(), nil)
			}
		}(d)
	}
	wg.Wait()
	if !testDiscardCertified(e, gen) {
		t.Fatal("concurrent drivers never certified the generation")
	}
	for _, nonce := range nonces {
		if x, _ := testF105LoadExchange(t, e, gen, nonce); x.Phase != mqttExchangeDiscarded {
			t.Fatalf("obligation %q stands at phase %d", nonce, x.Phase)
		}
	}
	r, _ := testF105LoadResource(t, e, "inc-src")
	if r.State != mqttResourceCleanupComplete || len(r.Deps) != 0 || r.Disposal == nil {
		t.Fatalf("the consumer was not retired once: %+v", r)
	}
}
