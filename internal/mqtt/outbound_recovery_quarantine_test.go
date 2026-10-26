//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"testing"
)

func TestUnitCRecoveryQuarantineTwoPass(t *testing.T) {
	for _, kind := range []string{"publish", "pubrel", "pubrel-delivered"} {
		for _, outcome := range []string{"healthy", "unreadable", "invalid", "conflict-publish", "conflict-pubrel"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				f := testAbandonAdmitted(t, "c70-quarantine", 2)
				e := f.e
				e.s.closeWillDispatch()
				e.asm.mu.Lock()
				e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
					return mqttInflightSourceUnknown
				}
				e.asm.mu.Unlock()
				ev := testAbandonEvidence(t, e, f.pi, f.subject)
				ev.Reply = f.reply
				acq, err := e.sess.abandonCurrentOutputVeto(ev)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := e.sess.submitAbandonDeniedDelivery(ev, acq); err != nil {
					t.Fatal(err)
				}
				r, seq := f.register(t)
				x := f.exchange(t)
				store := func(subject string, value any) uint64 {
					t.Helper()
					body, err := json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
					msg, err := e.asm.jsa.storeMsg(subject, 0, body)
					if err != nil {
						t.Fatal(err)
					}
					return msg.Sequence
				}
				xsubject := mqttExchangeSubject(e.sess.idHash, f.gen, f.lin)
				if kind != "publish" {
					r.State, x.Phase = mqttInflightPubRelReady, mqttExchangePubRecObserved
					_, _, r.PubRelDurable = mqttPubRelNames(e.sess.idHash, f.gen)
					r.PubRelStreamSeq = testStoreCanonical(t, e, f.pi, f.nonce, f.sseq)
					if err := mqttValidateInflightShape(r); err != nil {
						t.Fatal(err)
					}
					seq = store(f.subject, r)
					store(xsubject, x)
				}
				testPersistAcceptedRecord(t, e)
				// First resume is cold. Every later resume uses this exact object.
				restored, _, err := e.asm.createOrRestoreSession(e.sess.id, e.s.getOpts())
				if err != nil || restored == e.sess {
					t.Fatalf("cold session: %v", err)
				}
				e.sess, e.c.mqtt.sess = restored, restored
				restored.mu.Lock()
				restored.c = e.c
				var resident, conflict *mqttPending
				if kind != "publish" {
					resident = mqttRecoveredPending(restored.idHash, f.gen, f.pi,
						mqttRecoveredInflightReg{rec: r, seq: seq})
					restored.pendingPubRel = map[uint16]*mqttPending{f.pi: resident}
					if kind == "pubrel-delivered" {
						restored.trackAsPubRel(f.pi, testInflightAckSubject(r.PubRelDurable, r.PubRelStreamSeq, 1))
						resident = restored.pendingPubRel[f.pi]
						if resident.jsAckSubject == _EMPTY_ || resident.jsDur != r.PubRelDurable {
							restored.mu.Unlock()
							t.Fatal("fixture did not install a delivered PUBREL")
						}
					}
				}
				var residentDur, residentReply string
				var residentSeq uint64
				if resident != nil {
					residentDur, residentSeq, residentReply = resident.jsDur, resident.sseq, resident.jsAckSubject
				}
				if outcome == "conflict-publish" || outcome == "conflict-pubrel" {
					conflict = &mqttPending{regSeq: seq, regNonce: "another-allocation",
						ev: testPendingEv(restored.gen, r.Lin), jsDur: r.Dur, sseq: r.Seq, qos: r.QoS}
					if outcome == "conflict-publish" {
						restored.pendingPublish = map[uint16]*mqttPending{f.pi: conflict}
					} else {
						restored.pendingPubRel = map[uint16]*mqttPending{f.pi: conflict}
					}
				}
				// Snapshots of the resident and the conflict as installed. Quarantine DEMOTES and a
				// healthy pass REACTIVATES by replacement (a new pointer each time, so an alias of the
				// old one fails closed), so the obligation these assert — not displaced, not rebound,
				// restored whole — is CONTENT identity against these snapshots.
				var residentSnap, conflictSnap mqttPending
				if resident != nil {
					residentSnap = *resident
				}
				if conflict != nil {
					conflictSnap = *conflict
				}
				restored.mu.Unlock()
				testPersistAcceptedRecord(t, e)
				// Force loadOutboundExchange to fail decoding, then restore healthy
				// storage. No read hook or session reset can hide a sticky reservation.
				if _, err := e.asm.jsa.storeMsg(xsubject, 0, []byte("{")); err != nil {
					t.Fatal(err)
				}
				recover := func() {
					t.Helper()
					if _, err := restored.recoverInflightRegisters(e.c); err != nil {
						t.Fatal(err)
					}
				}
				assertQuarantined := func() {
					t.Helper()
					restored.mu.Lock()
					defer restored.mu.Unlock()
					pub, rel := restored.pendingPublish[f.pi], restored.pendingPubRel[f.pi]
					if !restored.piSpaceIncomplete || pub == nil || !pub.regForeign ||
						(rel != nil && !rel.regForeign) || restored.bumpPI() != 0 ||
						len(restored.disposals) != 0 || len(restored.terminalSources) != 0 ||
						restored.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}] != nil {
						t.Fatal("quarantine became actionable or lost occupancy")
					}
					if conflict != nil {
						cur := pub
						if outcome == "conflict-pubrel" {
							cur = rel
						}
						// Neither displaced by this pass's own install nor rebound to another
						// exchange: the same allocation, evidence, source and QoS, demoted.
						if cur == nil || cur.regNonce != conflictSnap.regNonce || cur.ev != conflictSnap.ev ||
							cur.jsDur != conflictSnap.jsDur || cur.sseq != conflictSnap.sseq || cur.qos != conflictSnap.qos ||
							cur.regSeq != conflictSnap.regSeq || cur.kind != mqttPendingHold || !cur.regForeign {
							t.Fatalf("resident conflict was replaced or rebound: %+v, was %+v", cur, conflictSnap)
						}
					}
				}
				recover()
				assertQuarantined()
				if outcome != "unreadable" {
					if outcome == "invalid" {
						x.Abandoned.Alloc = "another-allocation"
					}
					store(xsubject, x)
				}
				recover()
				if outcome != "healthy" {
					assertQuarantined()
					return
				}
				restored.mu.Lock()
				defer restored.mu.Unlock()
				if restored.piSpaceIncomplete ||
					restored.inflightApplyCompletedWalk != restored.inflightWalks ||
					restored.pendingPublish[f.pi] != nil ||
					!restored.disposalSourceLocked(r.Dur, r.Seq) ||
					!restored.sourceRetiredLocked(r.Dur, r.Seq) {
					t.Fatal("healthy second pass did not restore source ownership and complete apply")
				}
				binding := restored.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}]
				if kind == "publish" {
					if !binding.matchesRecord(r) || restored.pendingPubRel[f.pi] != nil {
						t.Fatal("healthy second pass lost ACK-only ownership")
					}
				} else if rel := restored.pendingPubRel[f.pi]; rel == nil || rel.kind != residentSnap.kind ||
					rel.ev != residentSnap.ev || rel.slot != residentSnap.slot || rel.custody != residentSnap.custody ||
					rel.regForeign || rel.regRecoveryDemoted || rel.regNonce != r.Nonce ||
					rel.ev.lin != r.Lin || rel.jsDur != residentDur || rel.sseq != residentSeq ||
					rel.jsAckSubject != residentReply || binding != nil {
					t.Fatal("healthy second pass did not restore the resident PUBREL")
				}
			})
		}
	}
}

func TestUnitCRecoveryOpaqueOrdinaryTwoPass(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		name := "unreadable"
		if healthy {
			name = "healthy"
		}
		t.Run(name, func(t *testing.T) {
			e := testNewInflightEnv(t, "c70-ordinary")
			e.s.closeWillDispatch()
			dur := e.sess.idHash + "_ordinary"
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 1)
			r, _ := e.loadRegister(t, pi)
			_, marker := e.loadMarker(t, pi)
			body, err := json.Marshal(r)
			if err != nil || r == nil || marker == nil {
				t.Fatalf("ordinary fixture: %v", err)
			}
			subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
			if _, err := e.asm.jsa.storeMsg(subject, 0, []byte("{")); err != nil {
				t.Fatal(err)
			}
			e.forgetInMemory()
			for pass := 0; pass != 2; pass++ {
				if pass == 1 && healthy {
					if _, err := e.asm.jsa.storeMsg(subject, 0, body); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
					t.Fatal(err)
				}
				e.sess.mu.Lock()
				p := e.sess.pendingPublish[pi]
				incomplete := e.sess.piSpaceIncomplete
				mapped := e.sess.cpending[dur][sseq] == pi
				good := p != nil && !p.regForeign && !p.regRecoveryOpaque &&
					!p.markerOpaque && !p.transmitted && p.regNonce == r.Nonce &&
					p.markerSeq == marker.Sequence && mapped
				held := p != nil && p.regForeign && p.regRecoveryOpaque && !mapped
				e.sess.mu.Unlock()
				if pass == 1 && healthy {
					if incomplete || !good {
						t.Fatal("healthy ordinary walk did not replace opaque occupancy")
					}
				} else if !incomplete || !held {
					t.Fatal("unreadable ordinary walk escaped quarantine")
				}
			}
		})
	}
}
