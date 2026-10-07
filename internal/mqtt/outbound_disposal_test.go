//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func testUnitCInstallDisposal(t *testing.T, f *testAbandonFix) *mqttDisposal {
	t.Helper()
	f.e.s.closeWillDispatch()
	ev := testAbandonEvidence(t, f.e, f.pi, f.subject)
	ev.Reply = f.reply
	f.e.sess.mu.Lock()
	w := f.e.sess.installDisposalLocked(f.e.c, ev)
	f.e.sess.mu.Unlock()
	if w == nil {
		t.Fatal("disposal installation refused")
	}
	return w
}

func testUnitCDisposalOwned(t *testing.T, w *mqttDisposal) {
	t.Helper()
	sess := w.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	_, source := sess.disposalsBySource[mqttDeferredKey{w.key.dur, w.key.seq}][w]
	_, pi := sess.disposalsByPI[w.key.pi][w]
	w.srv.owedDisposalsMu.Lock()
	_, owed := w.srv.owedDisposals[w]
	w.srv.owedDisposalsMu.Unlock()
	if !sess.disposalCurrentLocked(w) || !source || !pi || !owed {
		t.Fatalf("ownership gap: current=%v source=%v pi=%v owed=%v",
			sess.disposalCurrentLocked(w), source, pi, owed)
	}
}

func testUnitCFailedRecovery(t *testing.T, f *testAbandonFix, why string) {
	t.Helper()
	sess := f.e.sess
	sess.mu.Lock()
	incomplete, completed := sess.piSpaceIncomplete, sess.inflightApplyCompletedWalk
	sess.mu.Unlock()
	if incomplete {
		t.Fatal("fixture already has an incomplete identifier space")
	}
	var enumerated atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testInflightEnumerate = func(string) (map[string]uint64, error) {
		enumerated.Add(1)
		return nil, errors.New(why)
	}
	f.e.asm.mu.Unlock()
	staged, err := sess.recoverInflightRegisters(f.e.c)
	if err != nil || staged != nil || enumerated.Load() == 0 {
		t.Fatalf("failed enumeration route: calls=%d staged=%v err=%v", enumerated.Load(), staged, err)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.piSpaceIncomplete || sess.inflightApplyCompletedWalk != completed ||
		sess.inflightApplyCompletedWalk == sess.inflightWalks {
		t.Fatal("failed enumeration did not leave recovery incomplete without applying the walk")
	}
	if pi := sess.bumpPI(); pi != 0 {
		t.Fatalf("failed enumeration allowed identifier allocation: %d", pi)
	}
}

func TestUnitCDisposalLifecycle(t *testing.T) {
	for _, exit := range []string{"empty handles", "entry removal", "disconnect", "failed recovery",
		"failed final", "settled final", "stale instance", "source successor", "discard"} {
		t.Run(exit, func(t *testing.T) {
			f := testAbandonAdmitted(t, "c3-life", 1)
			w := testUnitCInstallDisposal(t, f)
			sess := f.e.sess
			testUnitCDisposalOwned(t, w)
			switch exit {
			case "empty handles", "failed final":
				f.e.asm.mu.Lock()
				f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
					return nil, nil, errors.New("read unavailable after positive non-landing")
				}
				f.e.asm.mu.Unlock()
				f.e.s.finishOwedDisposals()
			case "entry removal":
				sess.mu.Lock()
				sess.untrackPublishEntry(f.pi)
				sess.mu.Unlock()
			case "disconnect":
				sess.mu.Lock()
				sess.c = nil
				sess.mu.Unlock()
			case "failed recovery":
				testUnitCFailedRecovery(t, f, "recovery inventory unavailable")
			case "settled final":
				f.e.asm.mu.Lock()
				f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
					return mqttInflightSourceComplete
				}
				f.e.asm.mu.Unlock()
				f.e.s.finishOwedDisposals()
				f.requireAbandoned(t)
				sess.mu.Lock()
				left := len(sess.disposals) + len(sess.disposalsBySource) + len(sess.disposalsByPI)
				sess.mu.Unlock()
				f.e.s.owedDisposalsMu.Lock()
				left += len(f.e.s.owedDisposals)
				f.e.s.owedDisposalsMu.Unlock()
				if left != 0 {
					t.Fatal("settled final attempt leaked ownership")
				}
				return
			case "stale instance":
				sess.mu.Lock()
				next := *w
				next.busy = true
				sess.disposals[w.key] = &next
				removed := sess.removeDisposalLocked(w)
				current := sess.disposals[w.key]
				sess.mu.Unlock()
				if removed || current != &next || !next.busy {
					t.Fatal("stale completion changed its same-allocation replacement")
				}
				return
			case "source successor":
				sess.mu.Lock()
				next := *w
				next.key.nonce = mqttNewInflightNonce()
				sess.disposals[next.key] = &next
				source := mqttDeferredKey{w.key.dur, w.key.seq}
				sess.disposalsBySource[source][&next] = struct{}{}
				sess.disposalsByPI[w.key.pi][&next] = struct{}{}
				sess.removeDisposalLocked(w)
				_, sourceHeld := sess.disposalsBySource[source][&next]
				_, piHeld := sess.disposalsByPI[w.key.pi][&next]
				sess.mu.Unlock()
				if !sourceHeld || !piHeld {
					t.Fatal("cleanup erased a successor's index membership")
				}
				return
			case "discard":
				sess.mu.Lock()
				sess.resetForReuseLocked(mqttRetirementPermit{tr: sess.leases}, mqttNewInflightNonce(), 0)
				left := len(sess.disposals) + len(sess.disposalsBySource) + len(sess.disposalsByPI)
				sess.mu.Unlock()
				f.e.s.owedDisposalsMu.Lock()
				left += len(f.e.s.owedDisposals)
				f.e.s.owedDisposalsMu.Unlock()
				if left != 0 {
					t.Fatal("permit-bearing reset retained old disposal metadata")
				}
				return
			}
			testUnitCDisposalOwned(t, w)
		})
	}
}

// Complete provenance and a normal resident entry leave only the target disposal
// predicates to decide. Removal of the scheduling entry must not remove D.
func TestUnitCDisposalRestrictions(t *testing.T) {
	f := testAbandonAdmitted(t, "c3-restrictions", 1)
	w := testUnitCInstallDisposal(t, f)
	sess := f.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !sess.applyProvenanceCompleteLocked() || unresolvedReservationLocked(sess.pendingPublish[f.pi]) {
		t.Fatal("another restriction masks the disposal predicates")
	}
	if !sess.sourceRestrictedLocked(w.key.dur, w.key.seq) ||
		sess.sourceRestrictedLocked(w.key.dur, w.key.seq+1) ||
		!sess.durableRestrictedLocked(w.key.dur) || sess.durableRestrictedLocked("unrelated") {
		t.Fatal("disposal source/durable attribution is wrong")
	}
	if _, eligible, _ := sess.drainClaimEligibleLocked(f.pi); eligible {
		t.Fatal("pending disposal became an actionable claim")
	}
	cur := sess.pendingPublish[f.pi]
	cur.transmitted = true
	if _, ok := sess.beginSendLocked(f.e.c, f.pi, w.key.dur, w.key.seq); ok {
		t.Fatal("allowing re-offer opened a PUBLISH section")
	}
	sess.untrackPublishEntry(f.pi)
	if !sess.disposalCurrentLocked(w) || !sess.sourceRestrictedLocked(w.key.dur, w.key.seq) {
		t.Fatal("entry removal removed disposal ownership")
	}
	// Isolate PI reservation from custody and terminal/send holds.
	sess.custody = newMQTTCustodyLedger(sess.gen)
	sess.last_pi = f.pi - 1
	if got := sess.bumpPI(); got == 0 || got == f.pi {
		t.Fatal("pending disposal's identifier was reused")
	}
}

func TestUnitCDisposalHandlesPrecedeSubmission(t *testing.T) {
	f := testAbandonAdmitted(t, "c3-handles", 1)
	w := testUnitCInstallDisposal(t, f)
	var submissions atomic.Int32
	var unresolved atomic.Bool
	unresolved.Store(true)
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAbandonDelivery {
			submissions.Add(1)
			return 0, errors.New("submission outcome unavailable")
		}
		return send()
	}
	f.e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
	f.e.asm.mu.Lock()
	f.e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
		if unresolved.Load() {
			return mqttTransitionUnresolved, true
		}
		return mqttTransitionNeverLanded, true
	}
	f.e.asm.mu.Unlock()
	w.step()
	if len(w.handles) == 0 || submissions.Load() != 1 {
		t.Fatal("fixture failed to retain the submitted handle")
	}
	evidence, handle := w.evidence, w.handles[0]
	w.step()
	if submissions.Load() != 1 || len(w.handles) != 1 || w.handles[0] != handle {
		t.Fatal("resubmitted before settling the retained handle")
	}
	unresolved.Store(false)
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, errors.New("fresh evidence unavailable")
	}
	f.e.asm.mu.Unlock()
	w.step()
	testUnitCDisposalOwned(t, w)
	if len(w.handles) != 0 || w.evidence != evidence {
		t.Fatal("positive non-landing lost the original decision")
	}
	f.e.asm.jsa.testTransitionCommit.Store(nil)
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = nil
	f.e.asm.testCustodyResolve = nil
	f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	f.e.asm.mu.Unlock()
	f.e.s.finishOwedDisposals()
	f.requireAbandoned(t)
}

func TestUnitCDisposalSingleFlight(t *testing.T) {
	f := testAbandonAdmitted(t, "c3-single", 1)
	w := testUnitCInstallDisposal(t, f)
	if !w.claimStep() {
		t.Fatal("initial single-flight acquisition refused")
	}
	if w.claimStep() {
		t.Fatal("two workers acquired the same disposal")
	}
}

// A failed global walk with unchanged authority does not park authorized local
// work. No redelivery, ACK, resume or explicit owner wake follows fault removal.
func TestUnitCDisposalRetryWithoutTraffic(t *testing.T) {
	f := testAbandonAdmitted(t, "c3-idle", 1)
	var failed atomic.Bool
	var attempts atomic.Int32
	failed.Store(true)
	f.e.asm.mu.Lock()
	f.e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		attempts.Add(1)
		if failed.Load() {
			return mqttOpOutcome{}, errors.New("pre-submission outage")
		}
		return write()
	}
	f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	f.e.asm.mu.Unlock()
	t.Cleanup(func() {
		f.e.s.closeWillDispatch()
		f.e.s.willWorkerWG.Wait()
	})
	auth, _, err := f.e.asm.jsa.servingAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if f.deny() != mqttAbandonPending || attempts.Load() == 0 {
		t.Fatal("pre-submission failure route was not reached")
	}
	testUnitCFailedRecovery(t, f, "failed global inventory")
	f.e.sess.mu.Lock()
	walks, completed, incomplete := f.e.sess.inflightWalks, f.e.sess.inflightApplyCompletedWalk, f.e.sess.piSpaceIncomplete
	f.e.sess.mu.Unlock()
	failed.Store(false)
	testCustodyEventually(t, "idle disposal finishes", func() bool {
		f.e.sess.mu.Lock()
		defer f.e.sess.mu.Unlock()
		return len(f.e.sess.disposals) == 0
	})
	f.requireAbandoned(t)
	after, _, err := f.e.asm.jsa.servingAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
	if err != nil || after.Gen != auth.Gen || after.Tenure != auth.Tenure {
		t.Fatal("fixture displaced authority")
	}
	f.e.sess.mu.Lock()
	defer f.e.sess.mu.Unlock()
	if walks != f.e.sess.inflightWalks || completed != f.e.sess.inflightApplyCompletedWalk ||
		incomplete != f.e.sess.piSpaceIncomplete {
		t.Fatal("local settlement altered global recovery completeness")
	}
}

// Install against a real offered source. A syntactic reply alone cannot witness
// premature source ACK: JetStream rejects ACKs ahead of its delivery pointer.
func testUnitCOfferedDisposal(t *testing.T, qos byte) (*testAbandonFix, *mqttDisposal) {
	t.Helper()
	e := testNewInflightEnv(t, "c3-offered")
	testReadyForRealPubRelDelivery(t, e)
	e.c.mqtt.proto = mqttProtoLevel5
	dur := e.sess.idHash + "_c3"
	seq, reply := e.publishOfferedSource(t, dur)
	pi := e.trackAdmitted(t, dur, seq, qos, reply)
	subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
	r, _, err := e.asm.loadInflightRecord(subject)
	if err != nil || r == nil {
		t.Fatal("missing admitted source register")
	}
	ml := e.asm.jsa.loadOccupantMarkerLoad(subject)
	if err := e.asm.deleteInflightMarker(mqttMarkerCallerSender, subject, ml.seq, true); err != nil {
		t.Fatal(err)
	}
	f := &testAbandonFix{testWithdrawFix: &testWithdrawFix{
		e: e, pi: pi, subject: subject, gen: e.sess.gen, lin: r.Lin, nonce: r.Nonce, sseq: seq,
	}, reply: reply}
	w := testUnitCInstallDisposal(t, f)
	return f, w
}

// Enqueue an in-progress marker through the SAME publisher as sendAck. Receiving
// that marker proves all earlier source ACK publications have crossed the observer.
// This is a FIFO witness, not a negative sleep assertion.
func testUnitCNoSourceAck(t *testing.T, f *testAbandonFix, action func()) {
	t.Helper()
	sub, err := f.e.s.nrc.SubscribeSync(f.reply)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if err := f.e.s.nrc.Flush(); err != nil {
		t.Fatal(err)
	}
	action()
	f.e.sess.jsa.sendMsg(f.reply, []byte("+WPI"))
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil || string(msg.Data) != "+WPI" {
		t.Fatalf("source ACK preceded the observation barrier: msg=%v err=%v", msg, err)
	}
}

func TestUnitCDisposalCallback(t *testing.T) {
	for _, contract := range []string{"entrance", "source"} {
		t.Run(contract, func(t *testing.T) {
			f, w := testUnitCOfferedDisposal(t, 1)
			sess := f.e.sess
			notified, dispatched := 0, 0
			sess.mu.Lock()
			sess.untrackPublishEntry(f.pi)
			sess.drainByDur = map[string]*mqttDrain{w.key.dur: {cc: &ConsumerConfig{Durable: w.key.dur}}}
			sess.testDisposalNotify = func(got *mqttDisposal) {
				if got != w {
					t.Error("notification targeted a different instance")
				}
				notified++
			}
			sess.testDisposalDispatch = func() { dispatched++ }
			if !sess.applyProvenanceCompleteLocked() {
				t.Fatal("incomplete provenance masks the disposal guard")
			}
			sess.mu.Unlock()
			if sess.isSeqUncertain() {
				t.Fatal("uncertain record masks the unclaimed drain route")
			}
			pc := &client{kind: JETSTREAM}
			hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
			pc.pa.hdr = len(hdr)
			sub := &subscription{client: f.e.c, mqtt: &mqttSub{qos: 1, jsDur: w.key.dur}}
			testUnitCNoSourceAck(t, f, func() {
				mqttDeliverMsgCbQoS12(sub, pc, nil, mqttStreamSubjectPrefix+"f092.source", f.reply, append(hdr, 'x'))
			})
			if contract == "entrance" && (notified != 1 || dispatched != 0 || w.reply != f.reply) {
				t.Fatalf("entrance contract: notified=%d dispatched=%d reply=%q", notified, dispatched, w.reply)
			}
			testUnitCDisposalOwned(t, w)
		})
	}
}

// M-C22: a pending disposal with a real offered source. The ACK may record its
// protocol ending, but neither its handler nor C4 can settle this source yet.
func TestUnitCDisposalPendingAckSource(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			f, w := testUnitCOfferedDisposal(t, qos)
			var unresolved atomic.Bool
			unresolved.Store(true)
			hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				if tr.Attempt.Kind == mqttTransitionAbandonDelivery {
					return 0, errors.New("paused disposal outcome")
				}
				return send()
			}
			f.e.asm.jsa.testTransitionCommit.Store(&hook)
			t.Cleanup(func() { f.e.asm.jsa.testTransitionCommit.Store(nil) })
			f.e.asm.mu.Lock()
			f.e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
				if unresolved.Load() {
					return mqttTransitionUnresolved, true
				}
				return mqttTransitionNeverLanded, true
			}
			f.e.asm.mu.Unlock()
			w.step()
			if len(w.handles) == 0 {
				t.Fatal("the pending disposal route has no retained attempt")
			}
			reason := byte(0)
			if qos == 2 {
				reason = 0x80
			}
			testUnitCNoSourceAck(t, f, func() {
				if err := f.e.c.mqttProcessPublishReceived(f.pi, qos == 2, reason); err != nil {
					t.Fatal(err)
				}
			})
			r, seq := f.register(t)
			x := f.exchange(t)
			if r == nil || r.State != mqttInflightPublishTerminal || x.Abandoned != nil ||
				(qos == 1 && x.Phase != mqttExchangeTerminal) || (qos == 2 && x.Phase != mqttExchangeRejected) ||
				f.e.asm.inflightSourceState(r) != mqttInflightSourceLive {
				t.Fatal("ACK-first did not retain the no-fact terminal source carrier")
			}
			if f.e.sess.settleTerminalSource(f.subject, seq, r) {
				t.Fatal("C4 settled through a pending disposal")
			}
			testUnitCDisposalOwned(t, w)
			handed := false
			f.e.sess.mu.Lock()
			h := f.e.sess.sendsInFlight[f.pi]
			if h == nil || h.token != _EMPTY_ || h.terminalOwed != w.key.nonce {
				f.e.sess.mu.Unlock()
				t.Fatal("ACK-first fixture lacks the terminal-only identifier hold")
			}
			f.e.sess.testDisposalHandoff = func(old *mqttDisposal, next *mqttTerminalSource) {
				handed = true
				if old != w || !f.e.sess.disposalCurrentLocked(w) ||
					f.e.sess.terminalSources[mqttTerminalSourceKeyOf(next.ref)] != next ||
					f.e.sess.sendsInFlight[f.pi] != h || h.terminalOwed != w.key.nonce {
					t.Error("source handoff has an ownership gap")
				}
			}
			f.e.sess.mu.Unlock()
			unresolved.Store(false)
			// A co-resident open send still postpones the handoff. Ending that
			// send leaves C4's identifier reservation intact.
			f.e.sess.mu.Lock()
			h.token = mqttNewInflightNonce()
			f.e.sess.mu.Unlock()
			f.e.s.finishOwedDisposals()
			testUnitCDisposalOwned(t, w)
			if handed {
				t.Fatal("terminal handoff crossed an open send")
			}
			f.e.sess.mu.Lock()
			h.token = _EMPTY_
			f.e.sess.mu.Unlock()
			f.e.s.finishOwedDisposals()
			if !handed {
				t.Fatal("ACK-first cleanup was not handed to C4")
			}
			f.e.sess.mu.Lock()
			retained := f.e.sess.disposalCurrentLocked(w)
			f.e.sess.mu.Unlock()
			if retained {
				t.Fatal("C4 handoff retained the disposal restriction")
			}
			f.e.s.finishOwedTerminals()
			testCustodyEventually(t, "terminal source cleanup finishes", func() bool {
				cur, _, err := f.e.asm.loadInflightRecord(f.subject)
				return err == nil && cur == nil
			})
		})
	}
}

func TestUnitCDisposalDisplacedRecovery(t *testing.T) {
	f := testAbandonAdmitted(t, "c3-displaced", 1)
	w := testUnitCInstallDisposal(t, f)
	evidence := w.evidence
	msg, err := f.e.asm.jsa.loadSessionMsg(f.e.asm.domainTk, f.e.sess.idHash)
	if err != nil || msg == nil {
		t.Fatal("missing predecessor session metadata")
	}
	var record mqttPersistedSession
	if err := json.Unmarshal(msg.Data, &record); err != nil {
		t.Fatal(err)
	}
	gen, tenure := testF105ResumeHandoff(t, &f.e.asm.jsa, f.e.asm.domainTk, f.e.sess.id,
		f.e.asm.sessStreamEpoch, &record, msg.Sequence)
	if gen != w.key.gen || tenure == evidence.Tenure {
		t.Fatalf("successor tenure was not established: gen=%s tenure=%s", gen, tenure)
	}
	// B's accept on this connection object: its entitlement, and the session bound to it, as the
	// accept loop records (F-105 7b) — B's recovery runs under B's entitlement.
	f.e.c.mu.Lock()
	f.e.c.mqtt.outEnt = mqttOutboundEntitlement{}
	f.e.c.mu.Unlock()
	f.e.c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: gen, Tenure: tenure})
	f.e.sess.mu.Lock()
	f.e.sess.boundEnt, f.e.sess.boundEntOK = mqttOutboundEntitlement{Gen: gen, Tenure: tenure}, true
	f.e.sess.mu.Unlock()
	var submitted atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		submitted.Add(1)
		return write()
	}
	f.e.asm.mu.Unlock()
	testUnitCFailedRecovery(t, f, "B's recovery fails")
	w.step()
	w.step()
	testUnitCDisposalOwned(t, w)
	if w.stage != mqttDisposalRecovery || w.evidence != evidence || submitted.Load() != 0 {
		t.Fatal("displacement discarded or reauthorized the old denial")
	}
	if f.exchange(t).Phase != mqttExchangePublish {
		t.Fatal("displaced unlanded obligation was changed")
	}
	f.e.sess.mu.Lock()
	if !f.e.sess.disposalSourceLocked(w.key.dur, w.key.seq) ||
		!f.e.sess.disposalPILocked(w.key.pi) {
		t.Error("failed recovery released source or identifier restrictions")
	}
	f.e.sess.mu.Unlock()
}

func TestUnitCDisposalStaleReadCompletion(t *testing.T) {
	f := testAbandonAdmitted(t, "c3-stale-read", 1)
	w := testUnitCInstallDisposal(t, f)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
		r, msg, err := f.e.asm.jsa.loadInflightRecord(subject)
		if subject == f.subject && first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return r, msg, err
	}
	f.e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceComplete
	}
	f.e.asm.mu.Unlock()
	go func() { w.step(); close(done) }()
	<-entered
	sess := f.e.sess
	sess.mu.Lock()
	next := *w
	next.busy = false
	next.stage = mqttDisposalRecovery
	sess.disposals[w.key] = &next
	source := mqttDeferredKey{w.key.dur, w.key.seq}
	delete(sess.disposalsBySource[source], w)
	delete(sess.disposalsByPI[w.key.pi], w)
	sess.disposalsBySource[source][&next] = struct{}{}
	sess.disposalsByPI[w.key.pi][&next] = struct{}{}
	w.srv.owedDisposalsMu.Lock()
	delete(w.srv.owedDisposals, w)
	w.srv.owedDisposals[&next] = struct{}{}
	w.srv.owedDisposalsMu.Unlock()
	sess.mu.Unlock()
	close(release)
	<-done
	testUnitCDisposalOwned(t, &next)
	if next.busy || next.stage != mqttDisposalRecovery || next.submitted {
		t.Fatal("old I/O completion changed its replacement")
	}
	next.step()
	f.requireAbandoned(t)
}

func TestUnitCDisposalTerminalHandoff(t *testing.T) {
	f, w := testUnitCOfferedDisposal(t, 1)
	r, seq := f.register(t)
	testAdvanceTerminalPair(t, f.e, f.subject, seq, r)
	sess := f.e.sess
	sess.mu.Lock()
	if len(sess.terminalSources) != 0 || len(sess.terminalRetries) != 0 {
		t.Fatal("an existing C4 owner masks the handoff")
	}
	observed := false
	sess.testDisposalHandoff = func(old *mqttDisposal, next *mqttTerminalSource) {
		observed = true
		if old != w || !sess.disposalCurrentLocked(w) ||
			sess.terminalSources[mqttTerminalSourceKeyOf(next.ref)] != next {
			t.Error("old restriction released before successor installation")
		}
	}
	sess.mu.Unlock()
	f.e.s.finishOwedDisposals()
	if !observed {
		t.Fatal("terminal handoff was not reached")
	}
	f.e.s.finishOwedTerminals()
}

// H7a-3. An ORDINARY abandonment's disposal reads the abandoned pair and pauses before settling;
// a PUBACK captures the still-tracked entry (its terminal owner carries no binding) and ends the
// exchange Terminal. The disposal then installs the binding from its earlier read and settles. Once
// the terminal cleanup deletes the register, the binding must be gone: a stale one keeps its
// identifier index, and a later abandonment of another allocation on that identifier could never
// install its own.
//
// Mutation: finishTerminalSource resolves the binding from the carried refs only — the binding
// outlives the register.
func TestUnitCTerminalOwnerCapturedBeforeTheOrdinaryBindingReleasesIt(t *testing.T) {
	f, w := testUnitCOfferedDisposal(t, 1)
	sess := f.e.sess
	var raced int
	sess.mu.Lock()
	sess.testDisposalBeforeSettle = func(d *mqttDisposal) {
		if d != w || raced > 0 {
			return
		}
		raced++
		if err := f.e.c.mqttProcessPublishReceived(f.pi, false, 0); err != nil {
			t.Errorf("the racing PUBACK: %v", err)
		}
	}
	sess.mu.Unlock()
	defer func() {
		sess.mu.Lock()
		sess.testDisposalBeforeSettle = nil
		sess.mu.Unlock()
	}()
	w.step()
	if raced != 1 {
		t.Fatalf("fixture error: the settle window was reached %d time(s)", raced)
	}
	testCustodyEventually(t, "the abandoned exchange ended Terminal with its fact", func() bool {
		x := f.exchange(t)
		return x != nil && x.Phase == mqttExchangeTerminal && x.Abandoned != nil
	})
	sess.mu.Lock()
	settled := !sess.disposalCurrentLocked(w)
	installed := sess.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}] != nil
	sess.mu.Unlock()
	if !settled || !installed {
		t.Fatalf("fixture error: the disposal must have settled (%v) after installing the binding (%v)",
			settled, installed)
	}
	// The terminal owner's retry driver runs its cleanup now that the source is settled.
	f.e.s.finishOwedTerminals()
	idx := mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}
	testCustodyEventually(t, "the register deleted and the binding released", func() bool {
		if r, _ := f.register(t); r != nil {
			return false
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.abandonedAcks[idx] == nil
	})
}
