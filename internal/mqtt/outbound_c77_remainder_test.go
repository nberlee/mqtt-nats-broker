//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"sync/atomic"
	"testing"
)

// C76-WINDOW / C76-UNREADABLE-CUT. The two histories share the same
// displaced window; they do not multiply packet and allocation permutations.
// Reach mqtt.go:21933 (callback owner), unsub_drain.go:1209 (rollback),
// :2192/:3835 (empty-drain removal refusal), outbound_disposal.go:483
// (unresolved handles), and outbound_disposal_recovery.go:179/:199
// (classified save and activation). No process-wide hook may escape this test.
func TestConsult77DisplacedWindow(t *testing.T) {
	for _, cut := range []string{"allow", "unreadable-landed"} {
		t.Run(cut, func(t *testing.T) {
			f, old := testUnitCOfferedDisposal(t, 1)
			e, sess := f.e, f.e.sess
			e.s.willWorkerWG.Wait()
			testPersistAcceptedRecord(t, e)
			wire := testRow2Wire(e)
			evidence := old.evidence
			var submissions, resolutions atomic.Int32
			e.asm.mu.Lock()
			priorSubmit := e.asm.testAbandonDelivery
			priorResolve := e.asm.testCustodyResolve
			priorEnumerate := e.asm.testInflightEnumerate
			priorDrop := e.asm.testSessPersistDrop
			priorBackstop := e.asm.testSessRecordBackstopOff
			e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
				submissions.Add(1)
				return write()
			}
			e.asm.mu.Unlock()
			priorCommit := e.asm.jsa.testTransitionCommit.Load()
			sess.mu.Lock()
			priorNotify, priorDispatch := sess.testDisposalNotify, sess.testDisposalDispatch
			sess.pendingPublish[f.pi].markerSeq = 0
			sess.pendingPublish[f.pi].transmitted = true
			sess.mu.Unlock()
			t.Cleanup(func() {
				e.asm.jsa.testTransitionCommit.Store(priorCommit)
				e.asm.mu.Lock()
				e.asm.testAbandonDelivery = priorSubmit
				e.asm.testCustodyResolve = priorResolve
				e.asm.testInflightEnumerate = priorEnumerate
				e.asm.testSessPersistDrop = priorDrop
				e.asm.testSessRecordBackstopOff = priorBackstop
				e.asm.mu.Unlock()
				sess.mu.Lock()
				sess.testDisposalNotify, sess.testDisposalDispatch = priorNotify, priorDispatch
				sess.mu.Unlock()
			})

			var handle *mqttTransitionHandle
			if cut == "unreadable-landed" {
				hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
					if tr.Attempt.Kind != mqttTransitionAbandonDelivery {
						return send()
					}
					if _, err := send(); err != nil {
						return 0, err
					}
					return 0, errors.New("C77 lost committed response")
				}
				e.asm.jsa.testTransitionCommit.Store(&hook)
				e.asm.mu.Lock()
				e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
					resolutions.Add(1)
					return mqttTransitionUnresolved, true
				}
				e.asm.mu.Unlock()
				testUnitCNoSourceAck(t, f, old.step)
				if len(old.handles) != 1 || resolutions.Load() == 0 || submissions.Load() != 1 {
					t.Fatal("did not retain the genuinely submitted unreadable outcome")
				}
				handle = old.handles[0]
				f.requireAbandoned(t) // Durable truth differs from the owner's knowledge.
			}
			testUnitCRecoverySuccessor(t, f, false)
			testUnitCFailedRecovery(t, f, "C77 displaced inventory failure")
			beforeSubmissions := submissions.Load()

			d := addTestDrain(sess, "c77-source", old.key.dur, "f092.source", 1, nil)
			otherDur := sess.idHash + "_c77_other"
			otherSeq, otherReply := e.publishOfferedSource(t, otherDur)
			otherDrain := addTestDrain(sess, "c77-other", otherDur, "f092.source", 1, nil)
			notified, dispatched := 0, 0
			sess.mu.Lock()
			sess.testDisposalNotify = func(w *mqttDisposal) {
				if w != old {
					t.Error("callback notified a different owner")
				}
				notified++
			}
			sess.testDisposalDispatch = func() { dispatched++ }
			sess.mu.Unlock()
			deliver := func(dur, reply string) {
				pc := &client{kind: JETSTREAM}
				hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
				pc.pa.hdr = len(hdr)
				sub := &subscription{client: e.c, mqtt: &mqttSub{qos: 1, jsDur: dur}}
				mqttDeliverMsgCbQoS12(sub, pc, nil, mqttStreamSubjectPrefix+"f092.source", reply, append(hdr, 'x'))
			}
			testUnitCNoSourceAck(t, f, func() {
				for i := 0; i < 3; i++ {
					deliver(old.key.dur, f.reply)
					old.step()
				}
			})
			if notified != 3 || dispatched != 0 || submissions.Load() != beforeSubmissions {
				t.Fatal("repeated callbacks bypassed the displaced owner")
			}
			// A real unrelated offer must pass the exact-source disposal entrance.
			// Failed global provenance may still withhold its downstream ACK.
			deliver(otherDur, otherReply)
			if otherSeq == old.key.seq || notified != 3 || dispatched != 1 {
				t.Fatal("unrelated callback was swallowed by disposal suppression")
			}
			testUnitCNoSourceAck(t, f, func() { sess.finishDrainIfEmpty(e.c, d) })
			sess.mu.Lock()
			retained := sess.drains["c77-source"] == d && !d.removalMarked && !d.retired && len(d.seqs) == 0
			d.staged, otherDrain.staged = true, true
			sess.rollbackStagedDrains()
			rolledBack := sess.drainByDur[old.key.dur] == nil && sess.cons["c77-source"] == d.cc
			_, admitted := sess.beginSendLocked(e.c, f.pi, old.key.dur, old.key.seq)
			walk, completed := sess.inflightWalks, sess.inflightApplyCompletedWalk
			sess.mu.Unlock()
			if !retained || !rolledBack || admitted {
				t.Fatal("empty-drain teardown or allowing rollback escaped the owner")
			}
			testUnitCNoSourceAck(t, f, func() { deliver(old.key.dur, f.reply) })
			if notified != 4 || dispatched != 1 || len(wire.written) != 0 {
				t.Fatal("allowing rollback enabled source dispatch or output")
			}
			testUnitCDisposalOwned(t, old)
			if old.evidence != evidence || (handle != nil && (len(old.handles) != 1 || old.handles[0] != handle)) {
				t.Fatal("failed displaced recovery replaced evidence or the unresolved attempt")
			}

			e.asm.jsa.testTransitionCommit.Store(priorCommit)
			e.asm.mu.Lock()
			e.asm.testCustodyResolve = priorResolve
			e.asm.mu.Unlock()
			if cut == "unreadable-landed" {
				// No offer or new deny: real resolver readback and the installed
				// owner classify the landed pair and settle the offered source.
				old.step()
				f.requireAbandoned(t)
				r, _ := f.register(t)
				if e.asm.inflightSourceState(r) != mqttInflightSourceComplete || submissions.Load() != 1 {
					t.Fatal("durable classification did not settle exactly the old source")
				}
				binding := testUnitCAbandonedBinding(t, f)
				sess.mu.Lock()
				ok := old.settled && !sess.disposalCurrentLocked(old) &&
					sess.pendingPublish[f.pi] == nil && binding != nil &&
					sess.custody.holdsPI(f.pi) && !sess.custody.chargesPI(f.pi) &&
					sess.piSpaceIncomplete && sess.inflightWalks == walk &&
					sess.inflightApplyCompletedWalk == completed
				sess.mu.Unlock()
				if !ok || len(old.handles) != 0 || len(wire.written) != 0 {
					t.Fatal("local classification lost the ACK owner or completed global recovery")
				}
				return
			}

			e.asm.mu.Lock()
			e.asm.testInflightEnumerate = priorEnumerate
			e.asm.testSessPersistDrop = func(string) bool { return true }
			e.asm.testSessRecordBackstopOff = true
			e.asm.mu.Unlock()
			recoverOwner := func() *mqttDisposal {
				t.Helper()
				if _, err := sess.recoverInflightRegisters(e.c); err != nil {
					t.Fatal(err)
				}
				sess.mu.Lock()
				next := sess.disposals[old.key]
				sess.mu.Unlock()
				if next == nil || next == old || next.recovery == nil || next.recovery.deny {
					t.Fatal("healthy allow classification did not install a fresh recovery owner")
				}
				return next
			}
			next := recoverOwner()
			restore := setMQTTTestSessSaveHook(func(*mqttSession) error { return errors.New("C77 refused allow save") })
			t.Cleanup(restore)
			err := testSessRecordTx(t, sess, func(tok recordTx) error { return sess.saveDisposalRecovery(e.c, tok) })
			restore()
			if err == nil || next.recovery.saved {
				t.Fatal("failed allow save authorized activation")
			}
			sess.commitDisposalRecovery(e.c)
			testUnitCDisposalOwned(t, next)
			testUnitCNoSourceAck(t, f, next.step)
			next = recoverOwner()
			if err := testSessRecordTx(t, sess, func(tok recordTx) error { return sess.saveDisposalRecovery(e.c, tok) }); err != nil {
				t.Fatal(err)
			}
			if !next.recovery.saved {
				t.Fatal("successful classified save was not recorded")
			}
			sess.commitDisposalRecovery(e.c)
			sess.mu.Lock()
			tok, allowed := sess.beginSendLocked(e.c, f.pi, old.key.dur, old.key.seq)
			left := sess.disposals[old.key]
			sess.mu.Unlock()
			if !allowed || left != nil || f.exchange(t).Abandoned != nil || submissions.Load() != 0 {
				t.Fatal("durably saved allow did not restore ordinary send admission")
			}
			sess.endSend(e.c, f.pi, tok.token)
		})
	}
}

// C76-OUTPUT-EVIDENCE. Reach outbound_abandon_delivery.go:345 (missing
// action), outbound_disposal.go:368 (open section), :445 (submission after
// closure), and :431 (positive veto removes only the proposal).
func TestConsult77OutputEvidence(t *testing.T) {
	t.Run("missing-action", func(t *testing.T) {
		e, subject, tok, pi := testAcquireEnv(t, "c77-missing-action")
		e.s.closeWillDispatch()
		e.s.willWorkerWG.Wait()
		if !e.sess.acquirePublishAction(e.c, pi, &tok) || !e.sess.drainPublishAction(e.c, tok) {
			t.Fatal("real acquisition and drain failed")
		}
		r, msg, err := e.asm.loadInflightRecord(subject)
		if err != nil || r == nil || msg == nil || r.Acq == 0 || r.ActionToken != "" {
			t.Fatal("missing nonzero drained acquisition")
		}
		actionSubject := mqttActionSubject(e.sess.idHash, e.sess.gen, r.Lin, r.Acq)
		action, err := e.asm.jsa.loadLastMsgFor(mqttInflightStreamName, actionSubject)
		if err != nil || action == nil {
			t.Fatal("missing positive action control")
		}
		ev := testAbandonEvidence(t, e, pi, subject)
		if _, err := e.sess.abandonCurrentOutputVeto(ev); !errors.Is(err, errMQTTAbandonCurrentOutput) {
			t.Fatal("the real action did not prove current output")
		}
		if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, action.Sequence, true); err != nil {
			t.Fatal(err)
		}
		e.sess.mu.Lock()
		w := e.sess.installDisposalLocked(e.c, ev)
		e.sess.mu.Unlock()
		if w == nil {
			t.Fatal("proposal did not install")
		}
		w.step()
		testUnitCDisposalOwned(t, w)
		after, afterMsg, err := e.asm.loadInflightRecord(subject)
		if err != nil || after == nil || afterMsg == nil || *after != *r || afterMsg.Sequence != msg.Sequence || w.submitted {
			t.Fatal("missing evidence changed or submitted the delivery")
		}
		if _, err := e.sess.abandonCurrentOutputVeto(ev); !errors.Is(err, errMQTTAbandonNoEvidence) {
			t.Fatal("missing action was treated as positive evidence")
		}
		// Restore the exact evidence; the same owner must now observe the
		// positive veto and retire without another deny or source offer.
		if _, err := e.asm.jsa.storeMsg(actionSubject, 0, action.Data); err != nil {
			t.Fatal(err)
		}
		w.step()
		e.sess.mu.Lock()
		ok := !e.sess.disposalCurrentLocked(w) && e.sess.pendingPublish[pi] != nil &&
			e.sess.custody.holdsPI(pi) && e.sess.custody.chargesPI(pi)
		e.sess.mu.Unlock()
		if !ok || w.submitted {
			t.Fatal("positive current-output evidence did not retire only the proposal")
		}
	})
	t.Run("closed-without-acquisition", func(t *testing.T) {
		f := testAbandonAdmitted(t, "c77-closed-section", 1)
		e, sess := f.e, f.e.sess
		e.s.closeWillDispatch()
		e.s.willWorkerWG.Wait()
		sess.mu.Lock()
		tok, opened := sess.beginSendLocked(e.c, f.pi, testRealPubRelSrcDur, f.sseq)
		sess.mu.Unlock()
		if !opened {
			t.Fatal("real send section did not open")
		}
		t.Cleanup(func() { sess.endSend(e.c, f.pi, tok.token) })
		w := testUnitCInstallDisposal(t, f)
		e.asm.mu.Lock()
		priorSource := e.asm.testInflightSourceState
		e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return mqttInflightSourceComplete }
		e.asm.mu.Unlock()
		t.Cleanup(func() {
			e.asm.mu.Lock()
			e.asm.testInflightSourceState = priorSource
			e.asm.mu.Unlock()
		})
		w.step()
		testUnitCDisposalOwned(t, w)
		r, _ := f.register(t)
		if r.Acq != 0 || r.ActionToken != "" || r.State != mqttInflightPublish || w.submitted {
			t.Fatal("open unacquired section was abandoned")
		}
		sess.endSend(e.c, f.pi, tok.token)
		e.s.finishOwedDisposals() // Existing registry, no second deny/offer.
		f.requireAbandoned(t)
		// WP4: no dispatcher work starts after the shutdown gate closes; the fixture's
		// closeWillDispatch is a setup shortcut, not shutdown. The charge release below is the
		// custody dispatcher's, so the gate reopens before it is asserted (ruling R-impl-3).
		testReopenDispatchForCustody(t, e)
		r, _ = f.register(t)
		sess.mu.Lock()
		ok := w.settled && !sess.disposalCurrentLocked(w) && sess.pendingPublish[f.pi] == nil &&
			sess.custody.holdsPI(f.pi) && !sess.custody.chargesPI(f.pi)
		sess.mu.Unlock()
		if !ok || r.Acq != 0 {
			t.Fatal("closure without acquisition did not converge through the installed owner")
		}
	})
}
