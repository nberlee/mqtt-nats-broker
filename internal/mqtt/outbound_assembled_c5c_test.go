//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A distinct server/manager/session has no access to the crashed process's
// registries or hooks. Only JetStream and the client ID cross this boundary.
// This models process-memory loss; it is not a graceful session teardown.
func testUnitCColdPeer(t *testing.T, old *testInflightEnv) *testInflightEnv {
	t.Helper()
	old.s.closeWillDispatch()
	old.s.willWorkerWG.Wait()
	old.sess.mu.Lock()
	old.sess.disposals, old.sess.disposalsBySource, old.sess.disposalsByPI = nil, nil, nil
	old.sess.terminalSources, old.sess.terminalRetries, old.sess.abandonedAcks = nil, nil, nil
	old.sess.pendingPublish, old.sess.pendingPubRel, old.sess.cpending = nil, nil, nil
	old.sess.srcAckOwed, old.sess.sendsInFlight = nil, nil
	old.sess.mu.Unlock()
	old.s.owedDisposalsMu.Lock()
	old.s.owedDisposals = nil
	old.s.owedDisposalsMu.Unlock()
	old.s.owedTerminalsMu.Lock()
	old.s.owedTerminals = nil
	old.s.owedTerminalsMu.Unlock()
	old.s.owedIntentsMu.Lock()
	old.s.owedIntents = nil
	old.s.owedIntentsMu.Unlock()
	as, sess, c := old.peer(t, "c5c-cold", old.sess.id)
	e := &testInflightEnv{s: c.srv, asm: as, sess: sess, c: c}
	c.mqtt = &mqtt{asm: as, sess: sess, proto: mqttProtoLevel5}
	testConnIdentity(t, e, c)
	sess.mu.Lock()
	sess.c, sess.maxp, sess.pubRelSubscribed = c, 100, true
	empty := len(sess.disposals) == 0 && len(sess.terminalSources) == 0 &&
		len(sess.abandonedAcks) == 0 && len(sess.terminalRetries) == 0
	sess.mu.Unlock()
	if as == old.asm || sess == old.sess || e.s == old.s || !empty {
		t.Fatal("cold recovery inherited process-local ownership")
	}
	e.installServingAuthority(t)
	testPersistAcceptedRecord(t, e)
	t.Cleanup(func() {
		e.s.closeWillDispatch()
		e.s.willWorkerWG.Wait()
	})
	return e
}

// T-C3/T-C4/T-C34: real ACK-first atomic ending while an abandonment handle is
// unresolved, deletion entrance, then a different process's reconstruction.
// Safety re-offers and no-traffic convergence use different fixtures.
func TestUnitCAckFirstCrash(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		for _, source := range []mqttInflightSourceState{mqttInflightSourceLive, mqttInflightSourceUnknown} {
			for _, mode := range []string{"safety", "not-applied", "applied-lost"} {
				for _, refused := range []bool{false, true} {
					t.Run(fmt.Sprintf("qos%d/source%d/%s/refused%v", qos, source, mode, refused), func(t *testing.T) {
						f, disposal := testUnitCOfferedDisposal(t, qos)
						old := f.e
						hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
							if tr.Attempt.Kind == mqttTransitionAbandonDelivery {
								return 0, errors.New("unresolved abandonment response")
							}
							return send()
						}
						old.asm.jsa.testTransitionCommit.Store(&hook)
						t.Cleanup(func() { old.asm.jsa.testTransitionCommit.Store(nil) })
						old.asm.mu.Lock()
						old.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
							return mqttTransitionUnresolved, true
						}
						old.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState { return source }
						old.asm.mu.Unlock()
						disposal.step()
						if len(disposal.handles) != 1 {
							t.Fatal("did not enter the unresolved-attempt cut")
						}
						before, _ := f.register(t)
						reason, phase := byte(0), mqttExchangeTerminal
						if qos == 2 {
							reason, phase = 0x80, mqttExchangeRejected
						}
						testUnitCNoSourceAck(t, f, func() {
							if err := old.c.mqttProcessPublishReceived(f.pi, qos == 2, reason); err != nil {
								t.Fatal(err)
							}
						})
						r, seq := f.register(t)
						x := f.exchange(t)
						if r == nil || r.State != mqttInflightPublishTerminal ||
							r.Nonce != before.Nonce || r.Lin != before.Lin || r.Acq != before.Acq ||
							r.Dur != before.Dur || r.Seq != before.Seq || r.SourceAckReply != before.SourceAckReply ||
							r.AbandonReason != "" || r.AbandonTenure != "" || x.Abandoned != nil || x.Phase != phase {
							t.Fatal("ACK did not durably retain the exact no-fact terminal pair")
						}
						if old.sess.recoverPublishTerminalSlot(old.c, f.subject, seq, r) {
							t.Fatal("deletion entrance crossed unresolved source settlement")
						}
						old.s.finishOwedTerminals()
						if cur, _ := f.register(t); cur == nil {
							t.Fatal("terminal continuation deleted the crash carrier")
						}
						testUnitCDisposalOwned(t, disposal)
						e := testUnitCColdPeer(t, old)
						f.e = e
						// No old owner, callback reply or hook is passed to recovery.
						// The old dispatch gate is closed and joined, and no final pass
						// is run on that process after this cut.
						disposal = nil
						var observed, confirmations atomic.Int32
						var fail atomic.Bool
						fail.Store(true)
						var state atomic.Int32
						state.Store(int32(source))
						if refused {
							e.s.closeWillDispatch()
						}
						e.asm.mu.Lock()
						e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
							return mqttInflightSourceState(state.Load())
						}
						e.asm.testInflightSourceAck = func(reply string) error {
							confirmations.Add(1)
							// Compare with bytes read from storage; never supply this
							// value to the reconstructed owner.
							if reply != r.SourceAckReply {
								t.Errorf("cold owner used %q instead of its persisted reply", reply)
							}
							if fail.Load() {
								if mode == "applied-lost" {
									if err := e.asm.jsa.testConfirmAckRaw(reply); err != nil {
										return err
									}
								}
								return errors.New("ACK confirmation unavailable")
							}
							return e.asm.jsa.testConfirmAckRaw(reply)
						}
						e.asm.testInflightEnumerate = func(filter string) (map[string]uint64, error) {
							if filter == mqttInflightMarkerWalkFilter(e.sess.idHash, e.sess.gen) {
								ref := mqttSlotRefFromRecord(f.subject, seq, r)
								e.sess.mu.Lock()
								w := e.sess.terminalSources[mqttTerminalSourceKeyOf(*ref)]
								h := e.sess.sendsInFlight[f.pi]
								if w == nil || w.record != *r || w.reply != "" || h == nil ||
									h.terminalOwed != r.Nonce || len(e.sess.terminalRetries) == 0 ||
									e.sess.pendingPublish[f.pi] != nil || !e.sess.terminalSourceHeldLocked(r.Dur, r.Seq) {
									t.Error("cold source/identifier ownership missing before dispatch")
								}
								e.sess.mu.Unlock()
								observed.Add(1)
							}
							subjects, _, err := e.asm.jsa.streamSubjects(mqttInflightStreamName, filter)
							return subjects, err
						}
						e.asm.mu.Unlock()
						if _, err := e.sess.recoverInflightRegisters(e.c); err != nil || observed.Load() == 0 {
							t.Fatalf("cold recovery did not reach the ownership cut: %v", err)
						}
						if cur, _ := f.register(t); cur == nil || *cur != *r {
							t.Fatal("Live/Unknown recovery lost or rewrote the source carrier")
						}
						if f.exchange(t).Abandoned != nil || testPubRelRecordStored(e) {
							t.Fatal("no-fact ending invented abandonment or PUBREL")
						}
						if got := testCustodyMints(e.sess, f.pi); got == f.pi {
							t.Fatal("cold terminal identifier reused before source completion")
						}
						e.sess.mu.Lock()
						charged := e.sess.custody.chargesPI(f.pi)
						complete := !e.sess.piSpaceIncomplete && e.sess.applyProvenanceCompleteLocked()
						e.sess.mu.Unlock()
						if charged || !complete {
							t.Fatal("terminal charge/provenance is wrong or masks local suppression")
						}
						if mode == "safety" {
							wire := testRow2Wire(e)
							var dispatched atomic.Int32
							d := &mqttDrain{cc: &ConsumerConfig{Durable: r.Dur}}
							e.sess.mu.Lock()
							e.sess.drainByDur = map[string]*mqttDrain{r.Dur: d}
							e.sess.testDisposalDispatch = func() { dispatched.Add(1) }
							e.sess.mu.Unlock()
							pc := &client{kind: JETSTREAM}
							hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
							pc.pa.hdr = len(hdr)
							sub := &subscription{client: e.c, mqtt: &mqttSub{qos: qos, jsDur: r.Dur}}
							testUnitCNoSourceAck(t, f, func() {
								mqttDeliverMsgCbQoS12(sub, pc, nil, mqttStreamSubjectPrefix+"f092.source", r.SourceAckReply, append(hdr, 'x'))
							})
							e.sess.mu.Lock()
							claimed := d.seqs[r.Seq] != nil && d.seqs[r.Seq].claimed
							published := len(e.sess.pendingPublish) != 0 || len(e.sess.pendingPubRel) != 0
							w := e.sess.terminalSources[mqttTerminalSourceKeyOf(*mqttSlotRefFromRecord(f.subject, seq, r))]
							notified := w != nil && w.reply == r.SourceAckReply
							e.sess.mu.Unlock()
							if !notified || dispatched.Load() != 0 || claimed || published || len(wire.written) != 0 {
								t.Fatal("terminal source re-offer produced a claim or wire output")
							}
							return
						}
						// No callbacks, another client ACK or resume below this line.
						state.Store(int32(mqttInflightSourceLive))
						if refused {
							e.s.finishOwedTerminals()
						}
						testCustodyEventually(t, "failed exact source confirmation", func() bool {
							return confirmations.Load() != 0
						})
						if cur, _ := f.register(t); cur == nil {
							t.Fatal("ACK timeout was treated as positive completion")
						}
						e.sess.mu.Lock()
						owner := e.sess.terminalSources[mqttTerminalSourceKeyOf(*mqttSlotRefFromRecord(f.subject, seq, r))]
						e.sess.mu.Unlock()
						if owner == nil {
							t.Fatal("failed final/timer attempt lost source ownership")
						}
						fail.Store(false)
						e.asm.mu.Lock()
						e.asm.testInflightSourceState = nil
						e.asm.mu.Unlock()
						if refused {
							e.s.finishOwedTerminals()
						}
						testCustodyEventually(t, "cold no-traffic source and terminal cleanup", func() bool {
							cur, _, err := e.asm.loadInflightRecord(f.subject)
							e.sess.mu.Lock()
							left := len(e.sess.terminalSources) + len(e.sess.terminalRetries)
							e.sess.mu.Unlock()
							return err == nil && cur == nil && left == 0
						})
						if e.asm.inflightSourceState(r) != mqttInflightSourceComplete ||
							f.exchange(t).Abandoned != nil || f.exchange(t).Phase != phase {
							t.Fatal("cleanup lost source completion or changed crash truth")
						}
					})
				}
			}
		}
	}
}

// T-C33: three durable outcomes (the unresolved row has two branches) crossed
// with admission open/refused. Faults remain active until exact ownership and
// the actual durable snapshot have both been checked.
func TestUnitCIntentAdmissionMatrix(t *testing.T) {
	for _, outcome := range []string{"pre-intent", "unresolved-unlanded", "unresolved-landed"} {
		for _, drive := range []string{"timer", "final", "shutdown"} {
			refused := drive != "timer"
			t.Run(outcome+"/"+drive, func(t *testing.T) {
				f := testAbandonAdmitted(t, "c5c-intent", 2)
				// The base fixture saves before provisioning its PUBREL consumer.
				// Persist the complete setup so a cold peer restores that consumer.
				testPersistAcceptedRecord(t, f.e)
				if f.deny() != mqttAbandonDone {
					t.Fatal("abandonment did not land")
				}
				e := f.e
				t.Cleanup(func() {
					e.s.closeWillDispatch()
					e.s.willWorkerWG.Wait()
				})
				if refused {
					e.s.closeWillDispatch()
				}
				var healthy, unreadable atomic.Bool
				var attempts atomic.Int32
				e.asm.mu.Lock()
				e.asm.testAuthorityContentionWait = func(time.Duration) {}
				e.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
					if subject == f.subject && !healthy.Load() && unreadable.Load() {
						return nil, nil, errors.New("intent readback unavailable")
					}
					return e.asm.jsa.loadInflightRecord(subject)
				}
				e.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
					attempts.Add(1)
					if healthy.Load() {
						return write()
					}
					if outcome == "unresolved-landed" {
						if _, err := write(); err != nil {
							return mqttOpOutcome{}, err
						}
					}
					if outcome != "pre-intent" {
						unreadable.Store(true)
					}
					return mqttOpOutcome{}, errors.New("intent response unavailable")
				}
				e.asm.mu.Unlock()
				if err := e.c.mqttProcessPubRec(f.pi, mqttReasonSuccess); err != nil {
					t.Fatal(err)
				}
				e.s.owedIntentsMu.Lock()
				var owner *mqttIntentResolution
				for _, candidate := range e.s.owedIntents {
					if candidate.key.sess == e.sess && candidate.key.nonce == f.nonce &&
						candidate.key.lin == f.lin && candidate.key.name.pi == f.pi {
						owner = candidate
					}
				}
				owned := owner != nil && (!refused || !owner.running)
				e.s.owedIntentsMu.Unlock()
				if !owned || attempts.Load() == 0 {
					t.Fatal("accepted PUBREC has no exact resolver before return")
				}
				r, _, err := e.asm.jsa.loadInflightRecord(f.subject)
				want := mqttInflightDeliveryAbandoned
				if outcome == "unresolved-landed" {
					want = mqttInflightPubRelIntended
				}
				if err != nil || r == nil || r.State != want || testPubRelRecordStored(e) {
					t.Fatal("fixture has the wrong durable intent branch or an alternate materializer")
				}
				e.sess.mu.Lock()
				publish := e.sess.pendingPublish[f.pi]
				e.sess.mu.Unlock()
				if publish != nil {
					t.Fatal("failed successful-PUBREC advance rolled back to actionable PUBLISH")
				}
				if refused {
					e.s.finishOwedIntents()
					e.s.owedIntentsMu.Lock()
					retained := e.s.owedIntents[owner.key] == owner && !owner.running
					e.s.owedIntentsMu.Unlock()
					if !retained {
						t.Fatal("failed shutdown-final attempt lost packet ownership")
					}
				}
				healthy.Store(true)
				if drive == "shutdown" {
					// Exercise production shutdown ordering, not just its helper.
					e.s.running.Store(false)
					e.s.shutdownDone = make(chan struct{})
					e.s.runShutdown()
					// testNewInflightEnv's cleanup unconditionally closes quitCh.
					// The real channel is now closed and all its workers joined.
					e.s.quitCh = make(chan struct{})
					e = testUnitCColdPeer(t, e)
					f.e = e
					durable, _, err := e.asm.jsa.loadInflightRecord(f.subject)
					if err != nil || durable == nil || durable.State != mqttInflightPubRelReady {
						t.Fatal("real shutdown failed to materialize its owed intent")
					}
					if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
						t.Fatal(err)
					}
				} else if refused {
					e.s.finishOwedIntents()
				}
				testCustodyEventually(t, "owned intent materializes without a second PUBREC", func() bool {
					cur, _, err := e.asm.jsa.loadInflightRecord(f.subject)
					return err == nil && cur != nil && cur.State == mqttInflightPubRelReady
				})
				r, _, err = e.asm.jsa.loadInflightRecord(f.subject)
				if err != nil || r == nil || r.PubRelStreamSeq == 0 || !testPubRelRecordStored(e) ||
					f.exchange(t).Phase != mqttExchangePubRecObserved || f.exchange(t).Abandoned == nil {
					t.Fatal("retained packet did not produce the canonical fact-bearing successor")
				}
				wire := testRow2Wire(e)
				writes := testCountPubRelWrites(t, e, func() {
					testDeliverPubRelCorrelated(t, e, f.pi,
						testInflightAckSubject(r.PubRelDurable, r.PubRelStreamSeq, 1), r.Nonce, r.Dur, r.Seq)
				})
				if writes != 1 || !bytes.Equal(wire.written, testRow2PubRelFrame(f.pi)) {
					t.Fatalf("canonical continuation wrote %x, want exactly one PUBREL", wire.written)
				}
				if refused && drive != "shutdown" {
					e.s.finishOwedActions()
				}
				testCustodyEventually(t, "PUBREL output drains", func() bool {
					cur, _, err := e.asm.jsa.loadInflightRecord(f.subject)
					return err == nil && cur != nil && cur.ActionToken == ""
				})
				e.c.mqttProcessPubComp(f.pi)
				if refused && drive != "shutdown" {
					e.s.finishOwedTerminals()
				}
				testCustodyEventually(t, "fact-bearing PUBCOMP terminalizes", func() bool {
					x := f.exchange(t)
					return x.Phase == mqttExchangeTerminal && x.Abandoned != nil
				})
				e.sess.mu.Lock()
				publish = e.sess.pendingPublish[f.pi]
				charged := e.sess.custody.chargesPI(f.pi)
				e.sess.mu.Unlock()
				if publish != nil || charged {
					t.Fatal("PUBREL/PUBCOMP recreated PUBLISH or recharged abandonment")
				}
			})
		}
	}
}

// T-C1: compose a real ACK successor, test the immutable-fact boundary directly
// with otherwise-valid changed/removed facts, then commit the clean batch.
func TestUnitCImmutableFactAtAckBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		qos  byte
		to   mqttExchangePhase
	}{
		{"puback", 1, mqttExchangeTerminal},
		{"failed-pubrec", 2, mqttExchangeRejected},
		{"pubrec", 2, mqttExchangePubRecObserved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := testAbandonAdmitted(t, "c5c-carry", tc.qos)
			if f.deny() != mqttAbandonDone {
				t.Fatal("abandonment did not land")
			}
			f.e.s.closeWillDispatch()
			f.e.s.willWorkerWG.Wait()
			jsa := &f.e.asm.jsa
			auth, authSeq, err := jsa.requireAuthority(f.e.sess.id, f.e.asm.sessStreamEpoch)
			if err != nil {
				t.Fatal(err)
			}
			r, seq := f.register(t)
			x, xseq, err := jsa.loadOutboundExchange(f.e.sess.id, f.e.asm.sessStreamEpoch, f.gen, f.lin)
			if err != nil {
				t.Fatal(err)
			}
			tr, err := mqttAdvanceExchangeAckTransition(auth, authSeq, &mqttAdvanceExchangeAckRequest{
				ID: f.e.sess.id, Epoch: f.e.asm.sessStreamEpoch, Gen: f.gen,
				Txn: mqttNewInflightNonce(), Op: "op-ack-advance", Nonce: f.lin,
				To: tc.to, Acq: r.Acq, Register: r, RegisterSeq: seq,
			}, x, xseq)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for i := range tr.Extra {
				member := &tr.Extra[i]
				if member.subj != mqttExchangeSubject(f.e.sess.idHash, f.gen, f.lin) {
					continue
				}
				found = true
				clean := member.body
				next, err := mqttDecodeOutboundExchange(clean, x.ID, x.Epoch, x.Gen, x.Nonce)
				if err != nil || next == nil || !mqttExchangeAbandonmentEqual(next.Abandoned, x.Abandoned) {
					t.Fatal("ACK composer dropped or changed the fact")
				}
				for _, remove := range []bool{false, true} {
					forged := *next
					if remove {
						forged.Abandoned = nil
					} else {
						fact := *next.Abandoned
						fact.Tenure = mqttNewInflightNonce()
						forged.Abandoned = &fact
					}
					member.body, err = json.Marshal(&forged)
					if err != nil || testFactDecode(t, &forged) != nil {
						t.Fatal("forged successor is not otherwise schema-valid")
					}
					if _, err := jsa.checkExchangeOverwrites(tr.Attempt, tr.Extra); err == nil ||
						!strings.Contains(err.Error(), "beyond its phase") {
						t.Fatalf("immutable-fact boundary returned %v", err)
					}
				}
				member.body = clean
			}
			if !found {
				t.Fatal("ACK batch has no exchange member")
			}
			if _, err := jsa.commitAuthorityTransition(tr); err != nil {
				t.Fatal(err)
			}
			if got := f.exchange(t); got.Phase != tc.to || !mqttExchangeAbandonmentEqual(got.Abandoned, x.Abandoned) {
				t.Fatal("durable ACK successor failed to carry the immutable fact")
			}
		})
	}
}

// T-C3/T-C4's cold abandoned-first leg: recovery's binding must route an
// actual packet, independently of any restored PUBLISH scheduling entry.
func TestUnitCColdAbandonedTerminalAck(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			f := testAbandonAdmitted(t, "c5c-cold-ack", qos)
			if f.deny() != mqttAbandonDone {
				t.Fatal("abandonment did not land")
			}
			fact := *f.exchange(t).Abandoned
			e := testUnitCColdPeer(t, f.e)
			f.e = e
			e.s.closeWillDispatch()
			e.asm.mu.Lock()
			e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return mqttInflightSourceUnknown
			}
			e.asm.mu.Unlock()
			if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
				t.Fatal(err)
			}
			b := testUnitCAbandonedBinding(t, f)
			e.sess.mu.Lock()
			publish := e.sess.pendingPublish[f.pi]
			charged := e.sess.custody.chargesPI(f.pi)
			e.sess.mu.Unlock()
			if publish != nil || charged || b.nonce != f.nonce || b.ev.lin != f.lin {
				t.Fatal("cold binding has the wrong identity or scheduling state")
			}
			reason, phase := byte(0), mqttExchangeTerminal
			if qos == 2 {
				reason, phase = 0x80, mqttExchangeRejected
			}
			if err := e.c.mqttProcessPublishReceived(f.pi, qos == 2, reason); err != nil {
				t.Fatal(err)
			}
			x := f.exchange(t)
			if x.Phase != phase || !mqttExchangeAbandonmentEqual(x.Abandoned, &fact) ||
				testPubRelRecordStored(e) {
				t.Fatal("cold bound terminal ACK did not preserve its fact/packet meaning")
			}
		})
	}
}
