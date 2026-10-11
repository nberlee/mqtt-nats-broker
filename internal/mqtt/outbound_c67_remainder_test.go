//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests deliberately run serially: recovery and save seams may be global.
// Every cold branch uses a different manager/server, sharing only durable data.
func TestUnitCSourceAckCrashCuts(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		for _, fact := range []bool{false, true} {
			for _, cut := range []string{"unapplied", "applied-lost", "confirmed"} {
				for _, cold := range []bool{false, true} {
					t.Run(fmt.Sprintf("qos%d/fact%v/%s/cold%v", qos, fact, cut, cold), func(t *testing.T) {
						f, w := testUnitCOfferedDisposal(t, qos)
						e := f.e
						testPersistAcceptedRecord(t, e)
						e.s.willWorkerWG.Wait()
						e.asm.mu.Lock()
						e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
							return mqttInflightSourceUnknown
						}
						e.asm.mu.Unlock()
						if fact {
							w.step()
							f.requireAbandoned(t)
						}
						reason, phase := byte(0), mqttExchangeTerminal
						if qos == 2 {
							reason, phase = 0x80, mqttExchangeRejected
						}
						if err := e.c.mqttProcessPublishReceived(f.pi, qos == 2, reason); err != nil {
							t.Fatal(err)
						}
						w.step() // Transfer terminal source ownership without settling it.
						r, seq := f.register(t)
						x := f.exchange(t)
						if r == nil || r.State != mqttInflightPublishTerminal || x.Phase != phase ||
							(x.Abandoned != nil) != fact || (r.AbandonReason != "") != fact ||
							!mqttSourceAckReplyBinds(r.SourceAckReply, mqttStreamName, r.Dur, r.Seq) {
							t.Fatal("wrong durable terminal crash carrier")
						}
						var calls atomic.Int32
						e.asm.mu.Lock()
						e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
							return mqttInflightSourceLive
						}
						e.asm.testInflightSourceAck = func(reply string) error {
							calls.Add(1)
							if reply != r.SourceAckReply {
								t.Errorf("ACK uses %q, want persisted reply %q", reply, r.SourceAckReply)
							}
							if cut != "unapplied" {
								if err := e.asm.jsa.testConfirmAckRaw(reply); err != nil {
									return err
								}
							}
							if cut != "confirmed" {
								return errors.New("source ACK confirmation lost")
							}
							return nil
						}
						e.asm.mu.Unlock()
						// Execute the source stage alone: even positive confirmation is
						// followed by a crash BEFORE the deletion continuation runs.
						settled := e.sess.settleTerminalSource(f.subject, seq, r)
						if calls.Load() != 1 || settled != (cut == "confirmed") {
							t.Fatalf("cut not reached: calls=%d settled=%v", calls.Load(), settled)
						}
						if got, gotSeq := f.register(t); got == nil || *got != *r || gotSeq != seq {
							t.Fatal("source stage changed/deleted the durable crash carrier")
						}
						if cold {
							e = testUnitCColdPeer(t, e)
							f.e = e
						}
						// Unknown after restart must retain the carrier even if the ACK
						// actually applied. Same-object resume may retain positive proof.
						e.asm.mu.Lock()
						e.asm.testInflightSourceAck = nil
						e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
							return mqttInflightSourceUnknown
						}
						e.asm.mu.Unlock()
						wire := testRow2Wire(e)
						if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
							t.Fatal(err)
						}
						if cold || cut != "confirmed" {
							if got, _ := f.register(t); got == nil || *got != *r {
								t.Fatal("Unknown source lost its durable carrier")
							}
							ref := mqttSlotRefFromRecord(f.subject, seq, r)
							e.sess.mu.Lock()
							owner := e.sess.terminalSources[mqttTerminalSourceKeyOf(*ref)]
							held := e.sess.terminalSourceHeldLocked(r.Dur, r.Seq)
							publish := e.sess.pendingPublish[f.pi]
							coldExact := owner != nil && owner.record == *r && owner.reply == "" && owner.proof.seq == 0
							e.sess.mu.Unlock()
							if owner == nil || !held || publish != nil {
								t.Fatal("resume lacks exact source suppression/cleanup ownership")
							}
							if cold && !coldExact {
								t.Fatal("cold owner inherited reply/proof instead of durable metadata")
							}
							if testCustodyMints(e.sess, f.pi) == f.pi {
								t.Fatal("identifier reused before source classification")
							}
						}
						if len(wire.written) != 0 || testPubRelRecordStored(e) {
							t.Fatal("terminal recovery emitted output or invented PUBREL")
						}
						// No packet, re-offer or further recovery below this point.
						e.asm.mu.Lock()
						e.asm.testInflightSourceState = nil
						e.asm.mu.Unlock()
						testCustodyEventually(t, "post-ACK crash source/terminal convergence", func() bool {
							if !cold {
								e.s.finishOwedTerminals()
							}
							cur, _, err := e.asm.loadInflightRecord(f.subject)
							return err == nil && cur == nil
						})
						if e.asm.inflightSourceState(r) != mqttInflightSourceComplete {
							t.Fatal("terminal disappeared before real source completion")
						}
						end := f.exchange(t)
						if end.Phase != phase || !mqttExchangeAbandonmentEqual(end.Abandoned, x.Abandoned) {
							t.Fatal("cleanup changed protocol ending or historical fact")
						}
					})
				}
			}
		}
	}
}

func TestUnitCPreIntentProcessLoss(t *testing.T) {
	f, w := testUnitCOfferedDisposal(t, 2)
	e := f.e
	testPersistAcceptedRecord(t, e)
	e.s.willWorkerWG.Wait()
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceUnknown
	}
	e.asm.mu.Unlock()
	w.step()
	f.requireAbandoned(t)
	before, _ := f.register(t)
	fact := *f.exchange(t).Abandoned
	var attempts atomic.Int32
	e.asm.mu.Lock()
	e.asm.testAuthorityContentionWait = func(time.Duration) {}
	e.asm.testAckAdvance = func(*mqttAdvanceExchangeAckRequest, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		attempts.Add(1)
		return mqttOpOutcome{}, errors.New("positive pre-intent failure")
	}
	e.asm.mu.Unlock()
	if err := e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	e.s.owedIntentsMu.Lock()
	owned := false
	for _, owner := range e.s.owedIntents {
		if owner.key.sess == e.sess && owner.key.nonce == f.nonce && owner.key.lin == f.lin {
			owned = true
		}
	}
	e.s.owedIntentsMu.Unlock()
	if !owned || attempts.Load() == 0 {
		t.Fatal("accepted packet never reached its retained pre-intent owner")
	}
	if r, _ := f.register(t); r == nil || *r != *before || testPubRelRecordStored(e) {
		t.Fatal("pre-intent cut already contains durable intent")
	}
	e = testUnitCColdPeer(t, e)
	f.e = e
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceUnknown
	}
	e.asm.mu.Unlock()
	wire := testRow2Wire(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	e.s.finishOwedIntents()
	r, _ := f.register(t)
	e.s.owedIntentsMu.Lock()
	intents := len(e.s.owedIntents)
	e.s.owedIntentsMu.Unlock()
	e.sess.mu.Lock()
	publish := e.sess.pendingPublish[f.pi]
	rel := e.sess.pendingPubRel[f.pi]
	charged := e.sess.custody.chargesPI(f.pi)
	e.sess.mu.Unlock()
	testUnitCAbandonedBinding(t, f)
	if r == nil || r.State != mqttInflightDeliveryAbandoned ||
		!mqttExchangeAbandonmentEqual(f.exchange(t).Abandoned, &fact) ||
		intents != 0 || publish != nil || rel != nil || charged ||
		testPubRelRecordStored(e) || len(wire.written) != 0 || testCustodyMints(e.sess, f.pi) == f.pi {
		t.Fatal("process loss invented PUBREC/PUBREL or lost unanswered abandonment")
	}
	// Positive control: only a NEW packet may recreate the lost intent.
	if err := e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
		t.Fatal(err)
	}
	testCustodyEventually(t, "new PUBREC creates durable intent", func() bool {
		cur, _, err := e.asm.loadInflightRecord(f.subject)
		return err == nil && cur != nil && cur.State == mqttInflightPubRelReady
	})
}

func TestUnitCUnansweredColdSourceConvergence(t *testing.T) {
	f, w := testUnitCOfferedDisposal(t, 1)
	e := f.e
	testPersistAcceptedRecord(t, e)
	e.s.willWorkerWG.Wait()
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceUnknown
	}
	e.asm.mu.Unlock()
	w.step()
	f.requireAbandoned(t)
	r, _ := f.register(t)
	fact := *f.exchange(t).Abandoned
	e = testUnitCColdPeer(t, e)
	f.e = e
	var confirmations atomic.Int32
	var healthy atomic.Bool
	e.asm.mu.Lock()
	e.asm.testInflightSourceAck = func(reply string) error {
		confirmations.Add(1)
		if reply != fact.Reply {
			t.Errorf("cold source owner used %q, want fact reply %q", reply, fact.Reply)
		}
		if !healthy.Load() {
			return errors.New("confirmation unavailable")
		}
		return e.asm.jsa.testConfirmAckRaw(reply)
	}
	e.asm.mu.Unlock()
	wire := testRow2Wire(e)
	if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	testUnitCAbandonedBinding(t, f)
	healthy.Store(true)
	testCustodyEventually(t, "unanswered cold source settles without re-offer", func() bool {
		e.sess.mu.Lock()
		left := len(e.sess.disposals)
		e.sess.mu.Unlock()
		return confirmations.Load() != 0 && left == 0 &&
			e.asm.inflightSourceState(r) == mqttInflightSourceComplete
	})
	testUnitCAbandonedBinding(t, f)
	got, _ := f.register(t)
	e.sess.mu.Lock()
	charged := e.sess.custody.chargesPI(f.pi)
	publish := e.sess.pendingPublish[f.pi]
	e.sess.mu.Unlock()
	if got == nil || got.State != mqttInflightDeliveryAbandoned || charged || publish != nil ||
		!mqttExchangeAbandonmentEqual(f.exchange(t).Abandoned, &fact) ||
		testCustodyMints(e.sess, f.pi) == f.pi || len(wire.written) != 0 {
		t.Fatal("source completion answered the client, recharged, or republished")
	}
}

func TestUnitCPubRecOverInstalledDisposal(t *testing.T) {
	for _, landed := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost-landed=%v", landed), func(t *testing.T) {
			f, w := testUnitCOfferedDisposal(t, 2)
			e := f.e
			e.s.willWorkerWG.Wait()
			var submissions atomic.Int32
			hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				if tr.Attempt.Kind != mqttTransitionAbandonDelivery {
					return send()
				}
				submissions.Add(1)
				if _, err := send(); err != nil {
					return 0, err
				}
				return 0, errors.New("landed abandonment response lost")
			}
			e.asm.jsa.testTransitionCommit.Store(&hook)
			t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
			e.asm.mu.Lock()
			e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return mqttInflightSourceUnknown
			}
			e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
				return mqttTransitionUnresolved, true
			}
			e.asm.mu.Unlock()
			if landed {
				w.step()
				if len(w.handles) != 1 || f.exchange(t).Abandoned == nil {
					t.Fatal("lost-response landed cut not reached")
				}
			}
			testUnitCNoSourceAck(t, f, func() {
				if err := e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
					t.Fatal(err)
				}
			})
			testCustodyEventually(t, "successful PUBREC materializes", func() bool {
				r, _ := f.register(t)
				return r != nil && r.State == mqttInflightPubRelReady
			})
			if x := f.exchange(t); x.Phase != mqttExchangePubRecObserved || (x.Abandoned != nil) != landed {
				t.Fatal("PUBREC changed the abandonment commit order")
			}
			e.asm.mu.Lock()
			e.asm.testCustodyResolve = nil
			e.asm.testInflightSourceState = nil
			e.asm.mu.Unlock()
			w.step()
			e.sess.mu.Lock()
			owned, settled := e.sess.disposalCurrentLocked(w), w.settled
			e.sess.mu.Unlock()
			wantSubmissions := int32(0)
			if landed {
				wantSubmissions = 1
			}
			if owned || !settled || len(w.handles) != 0 || submissions.Load() != wantSubmissions {
				t.Fatal("ACK successor failed to settle the old handle/source without a new denial")
			}
			r, _ := f.register(t)
			if e.asm.inflightSourceState(r) != mqttInflightSourceComplete {
				t.Fatal("source did not actually settle")
			}
			wire := testRow2Wire(e)
			writes := testCountPubRelWrites(t, e, func() {
				testDeliverPubRelCorrelated(t, e, f.pi,
					testInflightAckSubject(r.PubRelDurable, r.PubRelStreamSeq, 1), r.Nonce, r.Dur, r.Seq)
			})
			if writes != 1 || !bytes.Equal(wire.written, testRow2PubRelFrame(f.pi)) {
				t.Fatal("successful PUBREC did not produce exactly one wire PUBREL")
			}
			e.s.finishOwedActions()
			testCustodyEventually(t, "PUBREL action drains", func() bool {
				cur, _ := f.register(t)
				return cur != nil && cur.ActionToken == ""
			})
			e.c.mqttProcessPubComp(f.pi)
			e.s.finishOwedTerminals()
			testCustodyEventually(t, "PUBCOMP ends exchange", func() bool {
				return f.exchange(t).Phase == mqttExchangeTerminal
			})
			if (f.exchange(t).Abandoned != nil) != landed {
				t.Fatal("PUBCOMP invented or removed historical abandonment")
			}
		})
	}
}

// M-C13 and M-C37: actual acquire/drain history, with no live-token veto.
func TestUnitCCapturedAcquisitionAndCurrentOutput(t *testing.T) {
	e, subject, tok, pi := testAcquireEnv(t, "c67-acquisition")
	e.s.closeWillDispatch()
	e.s.willWorkerWG.Wait()
	captured := tok.slot
	if !e.sess.acquirePublishAction(e.c, pi, &tok) || !e.sess.drainPublishAction(e.c, tok) {
		t.Fatal("acquire/drain history did not land")
	}
	r, msg, err := e.asm.loadInflightRecord(subject)
	if err != nil || r == nil || msg == nil || r.Acq == captured.acq || r.ActionToken != "" {
		t.Fatal("fixture must contain a newer, drained acquisition")
	}
	ev := testAbandonEvidence(t, e, pi, subject)
	if _, err := e.sess.abandonCurrentOutputVeto(ev); !errors.Is(err, errMQTTAbandonCurrentOutput) {
		t.Fatalf("positive current-tenure evidence was not vetoed: %v", err)
	}
	var advances atomic.Int32
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceUnknown
	}
	e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		advances.Add(1)
		return write()
	}
	e.asm.mu.Unlock()
	// Direct captured-ACK continuation: the caller's earlier lookup cannot mask
	// a refresh here, and the newer record is otherwise eligible to terminalize.
	e.sess.attemptTerminalRegisterRelease(e.c, pi, captured, true)
	x, _, err := e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, captured.name.gen, r.Lin)
	if err != nil || x == nil || x.Phase != mqttExchangePublish || advances.Load() != 0 {
		t.Fatal("old terminal ACK was retargeted to the newer acquisition")
	}
	fresh := mqttSlotRefFromRecord(subject, msg.Sequence, r)
	e.sess.attemptTerminalRegisterRelease(e.c, pi, *fresh, true)
	x, _, err = e.asm.jsa.loadOutboundExchange(e.sess.id, e.asm.sessStreamEpoch, captured.name.gen, r.Lin)
	if err != nil || x == nil || x.Phase != mqttExchangeRejected || advances.Load() != 1 {
		t.Fatal("valid fresh failed-PUBREC control did not terminalize")
	}
}

func TestUnitCAbandonmentCASAndVettedAcq(t *testing.T) {
	f := testAbandonAdmitted(t, "c67-cas", 1)
	f.e.s.closeWillDispatch()
	f.e.s.willWorkerWG.Wait()
	ev := testAbandonEvidence(t, f.e, f.pi, f.subject)
	ev.Reply = f.reply
	r, seq := f.register(t)
	jsa := &f.e.asm.jsa
	auth, authSeq, err := jsa.requireAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
	if err != nil {
		t.Fatal(err)
	}
	x, xSeq, err := jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.lin)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := mqttAbandonDeliveryTransition(auth, authSeq, mqttNewInflightNonce(), "op-abandon-delivery",
		ev, r, seq, x, xSeq)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, member := range tr.Extra {
		if member.subj == f.subject {
			found++
			if member.expect != seq {
				t.Fatalf("register CAS envelope=%d, want exact read=%d", member.expect, seq)
			}
		}
	}
	if found != 1 || r.ActionToken != "" {
		t.Fatal("fixture lacks one exact token-free register member")
	}
	readPassed := false
	_, err = jsa.opAbandonDeniedDelivery(&mqttAbandonDeliveryRequest{
		ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(),
		Op: "op-abandon-delivery", Evidence: ev, VettedAcq: r.Acq + 1,
		afterEvidence: func() { readPassed = true },
	})
	if !errors.Is(err, errMQTTAbandonNoEvidence) || readPassed {
		t.Fatalf("stale veto reached composer/commit: passed=%v err=%v", readPassed, err)
	}
	if cur, curSeq := f.register(t); cur == nil || *cur != *r || curSeq != seq {
		t.Fatal("stale vetted acquisition rewrote the register")
	}
	// The same exact evidence, with the vetted ordinal corrected, must commit.
	if _, err := jsa.opAbandonDeniedDelivery(&mqttAbandonDeliveryRequest{
		ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(),
		Op: "op-abandon-delivery", Evidence: ev, VettedAcq: r.Acq,
	}); err != nil {
		t.Fatal(err)
	}
	f.requireAbandoned(t)
}

func TestUnitCDrainProposalExclusion(t *testing.T) {
	for _, only := range []bool{false, true} {
		for _, unconfirmed := range []bool{false, true} {
			t.Run(fmt.Sprintf("drains-only%v/unconfirmed%v", only, unconfirmed), func(t *testing.T) {
				f := testAbandonAdmitted(t, "c67-drain", 1)
				e := f.e
				e.s.closeWillDispatch()
				e.s.willWorkerWG.Wait()
				ev := testAbandonEvidence(t, e, f.pi, f.subject)
				ev.Reply = f.reply
				e.sess.mu.Lock()
				e.sess.restoreDrainLocked("c67", mqttDrainFromPersisted(&mqttPersistedDrain{
					Cons: &ConsumerConfig{Durable: ev.Dur}, QoS: 1,
					Started: []mqttPersistedDrainSeq{{Seq: ev.Seq, PI: ev.PI}},
				}))
				st := e.sess.drainSeqLocked(ev.Dur, ev.Seq)
				if st == nil {
					e.sess.mu.Unlock()
					t.Fatal("drain sequence was not restored")
				}
				st.claimed, st.unclaimUnconfirmed = !unconfirmed, unconfirmed
				if only {
					delete(e.sess.drainByDur, ev.Dur)
				}
				// Call installation directly: releaseTransmittedIdentifier's own
				// earlier drain guard cannot mask either installation mutation.
				w := e.sess.installDisposalLocked(e.c, ev)
				e.sess.mu.Unlock()
				if w != nil {
					t.Fatal("claimed/unconfirmed drain admitted an abandonment proposal")
				}
				e.sess.mu.Lock()
				st.claimed, st.unclaimUnconfirmed = false, false
				control := e.sess.installDisposalLocked(e.c, ev)
				e.sess.mu.Unlock()
				if control == nil {
					t.Fatal("another premise masks the drain exclusion")
				}
				if f.exchange(t).Phase != mqttExchangePublish {
					t.Fatal("proposal test changed durable exchange")
				}
			})
		}
	}
}

func TestUnitCDisposalSubmissionConcurrency(t *testing.T) {
	f, w := testUnitCOfferedDisposal(t, 1)
	f.e.s.willWorkerWG.Wait()
	entered, release := make(chan struct{}, 32), make(chan struct{})
	var calls, active, peak atomic.Int32
	f.e.asm.mu.Lock()
	f.e.asm.testAbandonDelivery = func(*mqttAbandonDeliveryRequest, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		calls.Add(1)
		n := active.Add(1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		active.Add(-1)
		return mqttOpOutcome{}, errors.New("definite pre-submission failure")
	}
	f.e.asm.mu.Unlock()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() { defer workers.Done(); w.step() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		workers.Wait()
		t.Fatal("first submission did not enter")
	}
	// All challengers finish claimStep while the first submission is blocked.
	var challengers sync.WaitGroup
	for i := 0; i < 16; i++ {
		challengers.Add(1)
		go func() {
			defer challengers.Done()
			w.step()
		}()
	}
	done := make(chan struct{})
	go func() { challengers.Wait(); close(done) }()
	select {
	case <-done:
	case <-entered: // A second concurrent submission is already a contract failure.
	case <-time.After(5 * time.Second):
	}
	close(release)
	challengers.Wait()
	workers.Wait()
	if peak.Load() != 1 {
		t.Fatalf("overlapping wakes submitted concurrently: peak=%d calls=%d", peak.Load(), calls.Load())
	}
	testUnitCDisposalOwned(t, w)
}

// T-C25b: use the ownership protocol to establish B, not a fixture-only tenure
// rewrite. The landed fact remains A's even when B's inventory pass fails.
func TestUnitCLandedSuccessorAfterTenureDisplacement(t *testing.T) {
	for _, ending := range []string{"puback", "failed-pubrec", "pubrec"} {
		t.Run(ending, func(t *testing.T) {
			qos := byte(2)
			if ending == "puback" {
				qos = 1
			}
			f, w := testUnitCOfferedDisposal(t, qos)
			e := f.e
			testPersistAcceptedRecord(t, e)
			e.s.willWorkerWG.Wait()
			e.asm.mu.Lock()
			e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return mqttInflightSourceUnknown
			}
			e.asm.mu.Unlock()
			w.step()
			f.requireAbandoned(t)
			fact := *f.exchange(t).Abandoned
			if ending == "pubrec" {
				if err := e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
					t.Fatal(err)
				}
			} else {
				reason := byte(0)
				if qos == 2 {
					reason = 0x80
				}
				if err := e.c.mqttProcessPublishReceived(f.pi, qos == 2, reason); err != nil {
					t.Fatal(err)
				}
			}
			phase := f.exchange(t).Phase
			if phase == mqttExchangeDeliveryAbandoned || phase == mqttExchangePublish {
				t.Fatal("ACK successor did not land before displacement")
			}
			msg, err := e.asm.jsa.loadSessionMsg(e.asm.domainTk, e.sess.idHash)
			if err != nil || msg == nil {
				t.Fatal("missing accepted session metadata")
			}
			var record mqttPersistedSession
			if err := json.Unmarshal(msg.Data, &record); err != nil {
				t.Fatal(err)
			}
			gen, tenure := testF105ResumeHandoff(t, &e.asm.jsa, e.asm.domainTk, e.sess.id,
				e.asm.sessStreamEpoch, &record, msg.Sequence)
			if gen != f.gen || tenure == fact.Tenure {
				t.Fatalf("B did not displace A: gen=%s tenure=%s", gen, tenure)
			}
			// B's accept on this connection object: its entitlement, and the session bound to
			// it (F-105 7b) — B's recovery runs under B's entitlement.
			e.c.mu.Lock()
			e.c.mqtt.outEnt = mqttOutboundEntitlement{}
			e.c.mu.Unlock()
			e.c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: gen, Tenure: tenure})
			e.sess.mu.Lock()
			e.sess.boundEnt, e.sess.boundEntOK = mqttOutboundEntitlement{Gen: gen, Tenure: tenure}, true
			e.sess.mu.Unlock()
			testUnitCFailedRecovery(t, f, "B inventory unavailable")
			var submissions atomic.Int32
			e.asm.mu.Lock()
			e.asm.testAbandonDelivery = func(*mqttAbandonDeliveryRequest, func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				submissions.Add(1)
				return mqttOpOutcome{}, errors.New("old denial must not resubmit")
			}
			e.asm.testInflightSourceState = nil
			e.asm.mu.Unlock()
			r, _ := f.register(t)
			testCustodyEventually(t, "landed cleanup after real displacement", func() bool {
				e.s.finishOwedDisposals()
				e.s.finishOwedTerminals()
				e.sess.mu.Lock()
				left := len(e.sess.disposals) + len(e.sess.terminalSources)
				e.sess.mu.Unlock()
				return left == 0 && e.asm.inflightSourceState(r) == mqttInflightSourceComplete
			})
			e.sess.mu.Lock()
			incomplete := e.sess.piSpaceIncomplete && !e.sess.applyProvenanceCompleteLocked()
			e.sess.mu.Unlock()
			x := f.exchange(t)
			if submissions.Load() != 0 || !incomplete || x.Phase != phase ||
				!mqttExchangeAbandonmentEqual(x.Abandoned, &fact) {
				t.Fatal("cleanup reauthorized denial, changed history, or completed failed recovery")
			}
		})
	}
}

func TestUnitCEntryLossIsNotDiscard(t *testing.T) {
	f, w := testUnitCOfferedDisposal(t, 1)
	f.e.s.willWorkerWG.Wait()
	evidence := w.evidence
	f.e.sess.mu.Lock()
	f.e.sess.untrackPublishEntry(f.pi, testUntrackNoBinding)
	f.e.sess.mu.Unlock()
	f.e.asm.mu.Lock()
	f.e.asm.testInflightLoad = func(string) (*mqttInflightRecord, *StoredMsg, error) {
		return nil, nil, errors.New("no durable discard proof")
	}
	f.e.asm.mu.Unlock()
	w.step()
	testUnitCDisposalOwned(t, w)
	if w.evidence != evidence {
		t.Fatal("entry loss discarded the retained decision")
	}
	f.e.sess.mu.Lock()
	restricted := f.e.sess.disposalSourceLocked(w.key.dur, w.key.seq) &&
		f.e.sess.disposalPILocked(w.key.pi)
	f.e.sess.mu.Unlock()
	if !restricted {
		t.Fatal("entry absence released source or identifier without teardown")
	}
}
