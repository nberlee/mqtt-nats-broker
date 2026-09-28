//go:build mqttport_fulltests

package mqtt

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Sequential: recovery and record-save fixtures include process-wide seams.
func testC78SourceUncertainty(t *testing.T, e *testInflightEnv) func(bool) {
	t.Helper()
	e.asm.mu.Lock()
	prior := e.asm.testInflightSourceState
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testInflightSourceState = prior
		e.asm.mu.Unlock()
	})
	return func(unknown bool) {
		e.asm.mu.Lock()
		e.asm.testInflightSourceState = nil
		if unknown {
			e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return mqttInflightSourceUnknown
			}
		}
		e.asm.mu.Unlock()
	}
}

// Allocate through trackPublish/bumpPI and admitInflightPublish. Neither the
// allocation nor its binding, source owner, or output hold is planted in a map.
func testC78Successor(t *testing.T, e *testInflightEnv, pi uint16) (*testAbandonFix, func()) {
	t.Helper()
	sess := e.sess
	dur := sess.idHash + "_" + sess.gen + "_c78"
	seq, reply := e.publishOfferedSource(t, dur)
	sess.mu.Lock()
	sess.last_pi = pi - 1
	sess.mu.Unlock()
	got := e.trackAdmitted(t, dur, seq, 2, reply)
	if got != pi {
		t.Fatalf("real allocator returned %d, want reused identifier %d", got, pi)
	}
	r, msg := e.loadRegister(t, pi)
	if r == nil || msg == nil || r.State != mqttInflightPublish {
		t.Fatal("successor admission did not create an ordinary register")
	}
	f := &testAbandonFix{testWithdrawFix: &testWithdrawFix{
		e: e, pi: pi, subject: msg.Subject, gen: sess.gen,
		lin: r.Lin, nonce: r.Nonce, sseq: seq,
	}, reply: reply}
	ml := e.asm.jsa.loadOccupantMarkerLoad(f.subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, f.subject, ml.seq, true); err != nil {
		t.Fatal(err)
	}
	w := testUnitCInstallDisposal(t, f)
	w.step()
	f.requireAbandoned(t)
	binding := testUnitCAbandonedBinding(t, f)
	if sess.intendPubRelSlot(e.c, binding.pending()) != mqttPubRelIntentConfirmed {
		t.Fatal("successor PUBREC did not establish durable intent")
	}
	r, regSeq := f.register(t)
	sess.attemptOwedMaterialization(e.c, &mqttOwedMaterialization{
		slot: *mqttSlotRefFromRecord(f.subject, regSeq, r),
	})
	r, _ = f.register(t)
	if r.State != mqttInflightPubRelReady || r.PubRelStreamSeq == 0 {
		t.Fatal("successor did not materialize canonical PUBREL")
	}
	testUnitCRecoverySuccessor(t, f, false)
	testPersistAcceptedRecord(t, e)
	if _, err := sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	r, regSeq = f.register(t)
	act := sess.testAcquirePubRel(e.c, *mqttSlotRefFromRecord(f.subject, regSeq, r))
	if act == nil {
		t.Fatal("successor did not acquire a real output action")
	}
	t.Cleanup(func() { sess.startActionDrain(e.c, act) })
	before, beforeSeq := f.register(t)
	x := f.exchange(t)
	fact := *x.Abandoned
	sess.mu.Lock()
	entry := sess.pendingPubRel[pi]
	hold := sess.sendsInFlight[pi]
	source := mqttDeferredKey{dur, seq}
	suppression := sess.srcAckOwed[source]
	sess.mu.Unlock()
	wire := testRow2Wire(e)
	check := func() {
		t.Helper()
		testUnitCDisposalOwned(t, w)
		sess.mu.Lock()
		_, indexed := sess.disposalsBySource[source][w]
		ok := entry != nil && sess.pendingPubRel[pi] == entry &&
			// Scanned PUBREL placeholders omit lineage; the durable slot retains it.
			entry.regNonce == f.nonce && entry.lin == _EMPTY_ &&
			before.Lin == f.lin && binding.matchesRecord(before) &&
			sess.pendingPublish[pi] == nil &&
			sess.abandonedAcks[binding.index()] == binding &&
			suppression != nil && sess.srcAckOwed[source] == suppression &&
			suppression.nonce == f.nonce && sess.sourceRetiredLocked(dur, seq) &&
			indexed && hold != nil && sess.sendsInFlight[pi] == hold &&
			hold.action == act && sess.custody.holdsPI(pi) &&
			!sess.custody.chargesPI(pi) && sess.applyProvenanceCompleteLocked() &&
			!sess.piSpaceIncomplete
		sess.mu.Unlock()
		after, afterSeq := f.register(t)
		afterX := f.exchange(t)
		if !ok || after == nil || *after != *before || afterSeq != beforeSeq ||
			after.ActionToken != act.token || afterX.Phase != x.Phase ||
			!mqttExchangeAbandonmentEqual(afterX.Abandoned, &fact) ||
			len(wire.written) != 0 || testCustodyMints(sess, pi) == pi {
			t.Fatal("old work altered successor entry, binding, suppression, source index or output hold")
		}
		// Inspect the physical source without the uncertainty seam. No worker
		// can run here: fixture admission is closed and its workers have joined.
		e.asm.mu.Lock()
		hook := e.asm.testInflightSourceState
		e.asm.testInflightSourceState = nil
		e.asm.mu.Unlock()
		state := e.asm.inflightSourceState(after)
		e.asm.mu.Lock()
		e.asm.testInflightSourceState = hook
		e.asm.mu.Unlock()
		if state != mqttInflightSourceLive {
			t.Fatalf("successor source was settled: %v", state)
		}
	}
	check()
	return f, check
}

// Reach resolvePubRelIntent's terminal-carrier arm and deferred exact binding
// removal after a recovery handoff and actual same-generation PI reuse.
func TestConsult78Successor(t *testing.T) {
	f, old := testUnitCOfferedDisposal(t, 2)
	e, sess := f.e, f.e.sess
	e.s.willWorkerWG.Wait()
	uncertain := testC78SourceUncertainty(t, e)
	uncertain(true)
	old.step()
	f.requireAbandoned(t)
	binding := testUnitCAbandonedBinding(t, f)
	r, seq := f.register(t)
	testAdvanceTerminalPair(t, e, f.subject, seq, r)

	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan mqttPubRelIntentOutcome, 1)
	var once sync.Once
	var first atomic.Bool
	e.asm.mu.Lock()
	prior := e.asm.testInflightLoad
	e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		r, msg, err := e.asm.jsa.loadInflightRecord(subject)
		if subject == f.subject && first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return r, msg, err
	}
	e.asm.mu.Unlock()
	joined := false
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		if !joined {
			<-done
		}
		e.asm.mu.Lock()
		e.asm.testInflightLoad = prior
		e.asm.mu.Unlock()
	})
	go func() { done <- sess.resolvePubRelIntent(e.c, binding.pending()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old terminal read did not reach the off-lock pause")
	}
	testUnitCRecoverySuccessor(t, f, false)
	testPersistAcceptedRecord(t, e)
	if _, err := sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	r, seq = f.register(t)
	ref := *mqttSlotRefFromRecord(f.subject, seq, r)
	sess.mu.Lock()
	owner := sess.terminalSources[mqttTerminalSourceKeyOf(ref)]
	sess.mu.Unlock()
	if owner == nil {
		t.Fatal("recovery did not install the terminal source successor")
	}
	old.step() // C4 receives ownership before the disposal restriction goes.
	sess.mu.Lock()
	handed := !sess.disposalCurrentLocked(old) &&
		sess.terminalSources[mqttTerminalSourceKeyOf(ref)] == owner
	sess.mu.Unlock()
	if !handed {
		t.Fatal("recovery-to-terminal handoff was not reached")
	}
	uncertain(false)
	e.s.finishOwedTerminals()
	testCustodyEventually(t, "old terminal source and slot finish", func() bool {
		cur, _, err := e.asm.jsa.loadInflightRecord(f.subject)
		return err == nil && cur == nil
	})
	if f.exchange(t).Phase != mqttExchangeRejected || e.asm.inflightSourceState(r) != mqttInflightSourceComplete {
		t.Fatal("old MQTT ending/source settlement was not established")
	}
	if _, err := sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	uncertain(true)
	next, check := testC78Successor(t, e, f.pi)
	if next.gen != f.gen || next.nonce == f.nonce || next.lin == f.lin || next.sseq == f.sseq {
		t.Fatal("fixture did not allocate a distinct same-generation successor")
	}
	once.Do(func() { close(release) })
	select {
	case out := <-done:
		joined = true
		if out != mqttPubRelIntentEnded {
			t.Fatalf("old completion classified %v, want its captured terminal ending", out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old completion did not return")
	}
	check()
}

// Reach clearInner -> retireIncarnationLocked -> destroy -> resetForReuseLocked,
// then the old disposal's claimStep fence and the queued intent's generation exit.
func TestConsult78Generation(t *testing.T) {
	f, old := testUnitCOfferedDisposal(t, 2)
	e, sess := f.e, f.e.sess
	e.s.willWorkerWG.Wait()
	uncertain := testC78SourceUncertainty(t, e)
	uncertain(true)
	old.step()
	f.requireAbandoned(t)
	binding := testUnitCAbandonedBinding(t, f)
	sess.scheduleIntentResolution(e.c, f.pi, binding.pending())
	e.s.owedIntentsMu.Lock()
	var queued *mqttIntentResolution
	for _, w := range e.s.owedIntents {
		if w.key.sess == sess && w.key.nonce == f.nonce {
			queued = w
		}
	}
	e.s.owedIntentsMu.Unlock()
	if queued == nil {
		t.Fatal("admission-refused old PUBREC is not owned")
	}
	testUnitCDisposalOwned(t, old)
	ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, old.key.dur)
	if err != nil || ci == nil {
		t.Fatal("old source consumer is absent before retirement")
	}
	// Model the subscription that owns the real offered source consumer.
	sess.mu.Lock()
	if sess.cons == nil {
		sess.cons = make(map[string]*ConsumerConfig)
	}
	sess.cons["foo.bar"] = ci.Config
	if sess.subs == nil {
		sess.subs = make(map[string]byte)
	}
	sess.subs["foo.bar"] = 2
	sess.mu.Unlock()
	testPersistAcceptedRecord(t, e)
	sess.mu.Lock()
	sess.c = nil // Production detaches before retiring the incarnation.
	sess.mu.Unlock()
	if err := sess.clear(false); err != nil {
		t.Fatal(err)
	}
	watermark, _, status := e.asm.jsa.loadGenRetirementRecord(e.asm.domainTk, sess.idHash)
	if status != mqttRetirementValid || watermark.Gen != f.gen || sess.gen == f.gen {
		t.Fatal("clear did not durably retire the old generation")
	}
	ci, err = e.asm.jsa.lookupConsumer(mqttStreamName, old.key.dur)
	if (err != nil && !IsNatsErr(err, JSConsumerNotFoundErr)) || ci != nil {
		t.Fatal("retired source consumer survived completed teardown")
	}
	sess.mu.Lock()
	clean := len(sess.abandonedAcks) == 0 && len(sess.disposals) == 0 &&
		len(sess.disposalsBySource) == 0 && len(sess.disposalsByPI) == 0 &&
		len(sess.srcAckOwed) == 0 && len(sess.terminalSources) == 0
	sess.mu.Unlock()
	e.s.owedDisposalsMu.Lock()
	_, stillOwed := e.s.owedDisposals[old]
	e.s.owedDisposalsMu.Unlock()
	if !clean || stillOwed {
		t.Fatal("real retirement retained old binding/source/disposal obligations")
	}
	e.s.owedIntentsMu.Lock()
	retained := e.s.owedIntents[queued.key] == queued
	e.s.owedIntentsMu.Unlock()
	if !retained {
		t.Fatal("queued old resolver vanished before its generation classification")
	}
	// Retirement leaves CLOSED authority. The bootstrap helper only creates at
	// sequence zero, so model the next accept against the existing authority head.
	testRewriteAuthority(t, e, func(a *mqttOutboundAuthority) {
		if a.State != mqttAuthorityClosed || a.Gen != f.gen {
			t.Fatal("retirement did not close the old generation's authority")
		}
		*a = mqttOutboundAuthority{
			ID: a.ID, Epoch: a.Epoch, Gen: sess.gen, Tenure: mqttNewInflightNonce(),
			State: mqttAuthorityServing, Rev: a.Rev, Txn: a.Txn, OwnerTxn: a.Txn,
		}
	})
	// A new accept on the reused object has a fresh set-once entitlement.
	e.c.mu.Lock()
	e.c.mqtt.outEnt = mqttOutboundEntitlement{}
	e.c.outBind = nil
	e.c.mu.Unlock()
	testReadyForRealPubRelDelivery(t, e)
	testPersistAcceptedRecord(t, e)
	next, check := testC78Successor(t, e, f.pi)
	if next.gen == f.gen || next.subject == f.subject || next.nonce == f.nonce {
		t.Fatal("successor did not allocate in the new generation")
	}
	old.step()
	e.s.finishOwedIntents()
	check()
	e.s.owedIntentsMu.Lock()
	_, retained = e.s.owedIntents[queued.key]
	e.s.owedIntentsMu.Unlock()
	if retained {
		t.Fatal("retired generation's queued PUBREC was not discharged")
	}
}
