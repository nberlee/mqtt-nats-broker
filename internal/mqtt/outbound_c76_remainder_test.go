//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"testing"
)

// Build each crash cut through production transitions, never state assignment.
// Source uncertainty prevents the disposal owner from settling during setup.
func testC76History(t *testing.T, cut string) (*testAbandonFix, *mqttSendAction) {
	t.Helper()
	f, w := testUnitCOfferedDisposal(t, 2)
	e := f.e
	e.s.willWorkerWG.Wait()
	testPersistAcceptedRecord(t, e)
	e.asm.mu.Lock()
	e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
		return mqttInflightSourceUnknown
	}
	e.asm.mu.Unlock()
	w.step()
	f.requireAbandoned(t)
	ack := testUnitCAbandonedBinding(t, f).pending()
	if e.sess.intendPubRelSlot(e.c, ack) != mqttPubRelIntentConfirmed {
		t.Fatal("real abandoned-to-intended transition failed")
	}
	r, seq := f.register(t)
	if r.State != mqttInflightPubRelIntended || testPubRelRecordStored(e) {
		t.Fatal("intended cut already materialized")
	}
	if cut == "intended" {
		return f, nil
	}
	e.sess.attemptOwedMaterialization(e.c, &mqttOwedMaterialization{
		slot: *mqttSlotRefFromRecord(f.subject, seq, r),
	})
	r, seq = f.register(t)
	if r.State != mqttInflightPubRelReady || r.PubRelStreamSeq == 0 {
		t.Fatal("real canonical materialization/binding failed")
	}
	if cut == "ready" {
		return f, nil
	}
	act := e.sess.testAcquirePubRel(e.c, *mqttSlotRefFromRecord(f.subject, seq, r))
	if act == nil {
		t.Fatal("real PUBREL acquisition failed")
	}
	r, seq = f.register(t)
	res := e.sess.mqttTerminalizeOnPubComp(e.c, *mqttSlotRefFromRecord(f.subject, seq, r))
	cur, _ := f.register(t)
	if !res.recorded || cur == nil || cur.ActionToken != act.token ||
		f.exchange(t).Phase != mqttExchangeTerminalPendingDrain {
		t.Fatal("PUBCOMP did not preserve the live action")
	}
	return f, act
}

// T-C7/T-C34. Must reach outbound_disposal.go:224 retireSourceLocked,
// inflight_register.go:11130 scheduleOwedMaterialization (intended), and
// pubrel_action.go:2096's live-token guard (pending-drain).
func TestConsult76RecoveryHistories(t *testing.T) {
	for _, cut := range []string{"intended", "ready", "pending-drain"} {
		for _, mode := range []string{"same", "cold"} {
			t.Run(cut+"/"+mode, func(t *testing.T) {
				f, act := testC76History(t, cut)
				e := f.e
				fact := *f.exchange(t).Abandoned
				before, _ := f.register(t)
				if mode == "cold" {
					e = testUnitCColdPeer(t, e)
					f.e = e
					e.asm.mu.Lock()
					e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
						return mqttInflightSourceUnknown
					}
					e.asm.mu.Unlock()
				}
				// Cold intended recovery must enroll its real timer. Other
				// cuts keep workers closed so the snapshot stays observable.
				if cut != "intended" {
					e.s.closeWillDispatch()
				}
				wire := testRow2Wire(e)
				if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
					t.Fatal(err)
				}
				e.sess.mu.Lock()
				rel := e.sess.pendingPubRel[f.pi]
				ok := rel != nil && e.sess.pendingPublish[f.pi] == nil &&
					e.sess.sourceRetiredLocked(before.Dur, before.Seq) &&
					e.sess.disposalSourceLocked(before.Dur, before.Seq) &&
					!e.sess.custody.chargesPI(f.pi) &&
					!e.sess.piSpaceIncomplete && e.sess.applyProvenanceCompleteLocked()
				e.sess.mu.Unlock()
				if !ok || len(wire.written) != 0 ||
					!mqttExchangeAbandonmentEqual(f.exchange(t).Abandoned, &fact) ||
					testCustodyMints(e.sess, f.pi) == f.pi {
					t.Fatal("recovery lost ownership, recharged, transmitted or reused PI")
				}
				if cut == "pending-drain" {
					cur, seq := f.register(t)
					if cur.ActionToken != before.ActionToken ||
						f.exchange(t).Phase != mqttExchangeTerminalPendingDrain {
						t.Fatal("recovery completed a live output token")
					}
					out, token := e.sess.finalizeTerminalSlotObserved(e.c,
						*mqttSlotRefFromRecord(f.subject, seq, cur))
					if out != mqttTerminalHeldByAction || token != act.token {
						t.Fatal("recovered cleanup bypassed the exact holder")
					}
					return
				}
				if cut == "intended" && mode == "same" {
					// Exercise the enrolled work's production step with worker
					// admission closed; cold below independently tests enrollment.
					cur, seq := f.register(t)
					e.sess.attemptOwedMaterialization(e.c, &mqttOwedMaterialization{
						slot: *mqttSlotRefFromRecord(f.subject, seq, cur),
					})
				}
				testCustodyEventually(t, "recovered canonical PUBREL", func() bool {
					cur, _, err := e.asm.loadInflightRecord(f.subject)
					return err == nil && cur != nil && cur.State == mqttInflightPubRelReady
				})
				cur, _ := f.register(t)
				if cut == "ready" && cur.PubRelStreamSeq != before.PubRelStreamSeq {
					t.Fatal("recovery replaced an existing canonical record")
				}
				e.sess.mu.Lock()
				disposals := make([]*mqttDisposal, 0, len(e.sess.disposals))
				for _, w := range e.sess.disposals {
					disposals = append(disposals, w)
				}
				e.sess.mu.Unlock()
				e.asm.mu.Lock()
				e.asm.testInflightSourceState = nil
				e.asm.mu.Unlock()
				for _, w := range disposals {
					w.step()
				}
				if e.asm.inflightSourceState(cur) != mqttInflightSourceComplete {
					t.Fatal("restored source owner did not settle its real source")
				}
				testDeliverPubRelCorrelated(t, e, f.pi,
					testInflightAckSubject(cur.PubRelDurable, cur.PubRelStreamSeq, 1),
					cur.Nonce, cur.Dur, cur.Seq)
				if !bytes.Equal(wire.written, testRow2PubRelFrame(f.pi)) ||
					!mqttExchangeAbandonmentEqual(f.exchange(t).Abandoned, &fact) {
					t.Fatal("recovered continuation did not emit exactly one fact-bearing PUBREL")
				}
				e.s.finishOwedActions()
			})
		}
	}
}

// T-C18/M-C41a/b. Must reach pubrel_action.go:2096 and :2120,
// then :2134 disposeTerminalPubRelRecords and its exact record deletes.
// Assert the guard's own result: the later delete boundary cannot mask a kill.
func TestConsult76TerminalGuards(t *testing.T) {
	for _, mode := range []string{"unknown", "live", "token", "both"} {
		t.Run(mode, func(t *testing.T) {
			cut := "ready"
			if mode == "token" || mode == "both" {
				cut = "pending-drain"
			}
			f, act := testC76History(t, cut)
			e := f.e
			fact := *f.exchange(t).Abandoned
			state := mqttInflightSourceUnknown
			if mode == "live" {
				state = mqttInflightSourceLive
			} else if mode == "token" {
				state = mqttInflightSourceComplete
			}
			e.asm.mu.Lock()
			e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return state
			}
			e.asm.mu.Unlock()
			if mode == "token" {
				if err := e.asm.jsa.confirmAck(f.reply); err != nil {
					t.Fatal(err)
				}
			}
			// Every matching record must survive either guard; another nonce
			// must also survive the eventual successful cleanup.
			r, seq := f.register(t)
			storeRecord := func(nonce string) uint64 {
				t.Helper()
				body, hdr := mqttNewCorrelatedPubRel(f.pi, nonce, r.Dur, r.Seq, "", "", "")
				msg, err := e.asm.jsa.storeMsg(e.sess.pubRelSubject, hdr, body)
				if err != nil || msg == nil {
					t.Fatalf("store correlated record: %v", err)
				}
				return msg.Sequence
			}
			duplicate := storeRecord(f.nonce)
			foreign := storeRecord(mqttNewInflightNonce())
			canonical := r.PubRelStreamSeq
			if act == nil {
				res := e.sess.mqttTerminalizeOnPubComp(e.c,
					*mqttSlotRefFromRecord(f.subject, seq, r))
				if !res.recorded || res.outcome != mqttTerminalRetainedReoffer {
					t.Fatal("source guard failed at PUBCOMP")
				}
			}
			r, seq = f.register(t)
			ref := *mqttSlotRefFromRecord(f.subject, seq, r)
			out, token := e.sess.finalizeTerminalSlotObserved(e.c, ref)
			if act == nil {
				if r.ActionToken != "" || out != mqttTerminalRetainedReoffer || token != "" {
					t.Fatal("source-only guard bypassed")
				}
			} else if out != mqttTerminalHeldByAction || token != act.token {
				t.Fatal("token guard bypassed")
			}
			for _, record := range []uint64{canonical, duplicate, foreign} {
				msg, err := e.asm.jsa.loadMsg(mqttOutStreamName, record)
				if err != nil || msg == nil {
					t.Fatal("guard allowed canonical/duplicate disposal")
				}
			}
			if after, afterSeq := f.register(t); after == nil || *after != *r || afterSeq != seq {
				t.Fatal("guard changed the terminal carrier")
			}
			if act != nil {
				e.sess.startActionDrain(e.c, act)
				if mode == "both" {
					r, seq = f.register(t)
					if r == nil || r.ActionToken != "" ||
						e.sess.finalizeTerminalSlot(e.c,
							*mqttSlotRefFromRecord(f.subject, seq, r)) != mqttTerminalRetainedReoffer {
						t.Fatal("token drain bypassed the independent source hold")
					}
				}
			}
			// No more client packets or source re-offers. The installed source
			// owner and the existing terminal finalizer finish the obligation.
			e.asm.mu.Lock()
			e.asm.testInflightSourceState = nil
			e.asm.mu.Unlock()
			e.s.finishOwedDisposals()
			r, seq = f.register(t)
			if r != nil && e.sess.finalizeTerminalSlot(e.c,
				*mqttSlotRefFromRecord(f.subject, seq, r)) != mqttTerminalDone {
				t.Fatal("healthy source/token cleanup did not finish")
			}
			if r, _ := f.register(t); r != nil {
				t.Fatal("completed terminal slot survived")
			}
			for _, record := range []uint64{canonical, duplicate} {
				msg, err := e.asm.jsa.loadMsg(mqttOutStreamName, record)
				if (err != nil && !IsNatsErr(err, JSNoMessageFoundErr)) || msg != nil {
					t.Fatal("matching canonical/duplicate survived cleanup")
				}
			}
			msg, err := e.asm.jsa.loadMsg(mqttOutStreamName, foreign)
			if err != nil || msg == nil ||
				!mqttExchangeAbandonmentEqual(f.exchange(t).Abandoned, &fact) ||
				f.exchange(t).Phase != mqttExchangeTerminal {
				t.Fatal("cleanup consumed foreign material or changed historical fact")
			}
		})
	}
}
