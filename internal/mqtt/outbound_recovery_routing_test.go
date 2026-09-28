//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

// Routing snapshots start with a real admitted pair. The abandonment row uses
// the real atomic commit, with memory deliberately left at the pre-commit cut.
// Successor snapshots isolate routing, not the ACK/action write boundaries.
func TestUnitCRecoveryRouting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   mqttInflightState
		phase   mqttExchangePhase
		qos     byte
		fact    bool
		source  mqttInflightSourceState
		bad     string
		cleaned bool
	}{
		{"abandoned", mqttInflightDeliveryAbandoned, mqttExchangeDeliveryAbandoned, 2, true, mqttInflightSourceUnknown, "", false},
		{"intended", mqttInflightPubRelIntended, mqttExchangePubRecObserved, 2, true, mqttInflightSourceUnknown, "", false},
		{"ready", mqttInflightPubRelReady, mqttExchangePubRecObserved, 2, true, mqttInflightSourceUnknown, "", false},
		{"pending-drain", mqttInflightPubRelTerminal, mqttExchangeTerminalPendingDrain, 2, true, mqttInflightSourceUnknown, "", false},
		{"fact-terminal", mqttInflightPublishTerminal, mqttExchangeTerminal, 1, true, mqttInflightSourceUnknown, "", false},
		{"fact-rejected", mqttInflightPublishTerminal, mqttExchangeRejected, 2, true, mqttInflightSourceUnknown, "", false},
		{"terminal-live", mqttInflightPublishTerminal, mqttExchangeTerminal, 1, false, mqttInflightSourceLive, "", false},
		{"rejected-unknown", mqttInflightPublishTerminal, mqttExchangeRejected, 2, false, mqttInflightSourceUnknown, "", false},
		{"terminal-complete", mqttInflightPublishTerminal, mqttExchangeTerminal, 1, false, mqttInflightSourceComplete, "", false},
		{"rejected-lost", mqttInflightPublishTerminal, mqttExchangeRejected, 2, false, mqttInflightSourceLost, "", false},
		{"ended-cleaned", mqttInflightPublishTerminal, mqttExchangeTerminal, 1, true, mqttInflightSourceComplete, "", true},
		{"pre-landing", mqttInflightPublish, mqttExchangePublish, 2, false, mqttInflightSourceLive, "", false},
		{"missing-slot", mqttInflightDeliveryAbandoned, mqttExchangeDeliveryAbandoned, 2, true, mqttInflightSourceUnknown, "missing", false},
		{"exchange-only", mqttInflightPubRelIntended, mqttExchangePubRecObserved, 2, true, mqttInflightSourceUnknown, "exchange-only", false},
		{"register-only", mqttInflightPubRelIntended, mqttExchangePubRecObserved, 2, true, mqttInflightSourceUnknown, "register-only", false},
		{"inconsistent", mqttInflightDeliveryAbandoned, mqttExchangeDeliveryAbandoned, 2, true, mqttInflightSourceUnknown, "allocation", false},
		{"unreadable", mqttInflightDeliveryAbandoned, mqttExchangeDeliveryAbandoned, 2, true, mqttInflightSourceUnknown, "unreadable", false},
		{"unsupported", mqttInflightDeliveryAbandoned, mqttExchangeDeliveryAbandoned, 2, true, mqttInflightSourceUnknown, "unsupported", false},
	} {
		for _, cold := range []bool{false, true} {
			mode := "same"
			if cold {
				mode = "cold"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				f := testAbandonAdmitted(t, "c5a-routing", tc.qos)
				e := f.e
				initial, _ := f.register(t)
				if tc.state != mqttInflightPubRelIntended {
					e.s.closeWillDispatch()
				}
				var source atomic.Int32
				var confirmations atomic.Int32
				var allow atomic.Bool
				source.Store(int32(tc.source))
				e.asm.mu.Lock()
				e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
					return mqttInflightSourceState(source.Load())
				}
				e.asm.testInflightSourceAck = func(reply string) error {
					confirmations.Add(1)
					if !mqttSourceAckReplyBinds(reply, mqttStreamName, initial.Dur, initial.Seq) ||
						(!tc.fact && reply != initial.SourceAckReply) {
						t.Errorf("lost durable reply: %q", reply)
					}
					if allow.Load() {
						return nil
					}
					return errors.New("confirmation unavailable")
				}
				e.asm.mu.Unlock()
				ev := testAbandonEvidence(t, e, f.pi, f.subject)
				ev.Reply = f.reply
				var old *mqttDisposal
				if !cold && !tc.cleaned && (tc.fact || tc.state == mqttInflightPublish) {
					e.sess.mu.Lock()
					old = e.sess.installDisposalLocked(e.c, ev)
					if old != nil {
						old.busy = true
					}
					e.sess.mu.Unlock()
					if old == nil {
						t.Fatal("fixture: no pre-landing owner")
					}
				}
				if tc.fact {
					acq, err := e.sess.abandonCurrentOutputVeto(ev)
					if err != nil {
						t.Fatal(err)
					}
					out, err := e.sess.submitAbandonDeniedDelivery(ev, acq)
					if err != nil {
						t.Fatal(err)
					}
					if old != nil && out.Handle != nil {
						e.sess.mu.Lock()
						old.handles = append(old.handles, out.Handle)
						e.sess.mu.Unlock()
					}
					f.requireAbandoned(t)
				}
				r, seq := f.register(t)
				x := f.exchange(t)
				if tc.cleaned {
					e.sess.mu.Lock()
					bound := e.sess.settleAbandonedDeliveryLocked(ev, r)
					e.sess.mu.Unlock()
					if !bound {
						t.Fatal("fixture: no historical ACK binding")
					}
				}
				if tc.state == mqttInflightPublishTerminal {
					seq = testAdvanceTerminalPair(t, e, f.subject, seq, r)
					x = f.exchange(t)
				} else if tc.state != r.State {
					r.State, x.Phase = tc.state, tc.phase
					if tc.state == mqttInflightPubRelReady || tc.state == mqttInflightPubRelTerminal {
						_, _, r.PubRelDurable = mqttPubRelNames(e.sess.idHash, f.gen)
						r.PubRelStreamSeq = testStoreCanonical(t, e, f.pi, f.nonce, f.sseq)
					}
					if tc.state == mqttInflightPubRelTerminal {
						r.ActionToken = mqttNewInflightNonce()
						r.ActionClaim, r.ActionClaimSeq = r.Claim, r.ClaimSeq
					}
					if err := mqttValidateInflightShape(r); err != nil {
						t.Fatal(err)
					}
					data, _ := json.Marshal(r)
					resp, err := e.asm.jsa.storeMsg(f.subject, 0, data)
					if err != nil {
						t.Fatal(err)
					}
					seq = resp.Sequence
				}
				if err := mqttValidateOutboundExchange(x, e.sess.id, e.asm.sessStreamEpoch, f.gen, f.lin); err != nil {
					t.Fatal(err)
				}
				if tc.bad == "exchange-only" {
					r.AbandonReason, r.AbandonTenure = _EMPTY_, _EMPTY_
					body, _ := json.Marshal(r)
					resp, err := e.asm.jsa.storeMsg(f.subject, 0, body)
					if err != nil {
						t.Fatal(err)
					}
					seq = resp.Sequence
				}
				if tc.bad == "register-only" {
					x.Abandoned = nil
				}
				if tc.bad == "allocation" {
					x.Abandoned.Alloc = mqttNewInflightNonce()
				}
				data, _ := json.Marshal(x)
				if tc.bad == "unreadable" {
					data = []byte("{")
				}
				if tc.bad == "unsupported" {
					data = append(data[:len(data)-1], []byte(`,"unsupported":true}`)...)
				}
				if _, err := e.asm.jsa.storeMsg(mqttExchangeSubject(e.sess.idHash, f.gen, f.lin), 0, data); err != nil {
					t.Fatal(err)
				}
				ref := *mqttSlotRefFromRecord(f.subject, seq, r)
				if tc.bad == "missing" {
					if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true); err != nil {
						t.Fatal(err)
					}
				}
				var terminal *mqttTerminalSource
				if !cold && !tc.fact && tc.state == mqttInflightPublishTerminal {
					e.sess.mu.Lock()
					terminal = e.sess.holdTerminalSourceLocked(ref)
					e.sess.mu.Unlock()
				}
				if tc.cleaned {
					if !e.sess.recoverPublishTerminalSlot(e.c, f.subject, seq, r) {
						t.Fatal("fixture: source-complete cleanup failed")
					}
					e.forgetInMemory()
				}
				if cold {
					restored, _, err := e.asm.createOrRestoreSession(e.sess.id, e.s.getOpts())
					if err != nil || restored == e.sess {
						t.Fatalf("fresh object: %v", err)
					}
					e.sess = restored
					e.c.mqtt.sess = restored
					restored.mu.Lock()
					restored.c = e.c
					restored.mu.Unlock()
					testPersistAcceptedRecord(t, e)
				}
				if !cold && tc.state == mqttInflightPubRelIntended {
					e.sess.mu.Lock()
					e.sess.pendingPubRel = map[uint16]*mqttPending{
						f.pi: mqttRecoveredPending(e.sess.idHash, f.gen, f.pi, mqttRecoveredInflightReg{rec: r, seq: seq}),
					}
					e.sess.mu.Unlock()
				}
				var handle *mqttTransitionHandle
				if old != nil && len(old.handles) != 0 {
					handle = old.handles[0]
				}
				if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
					t.Fatal(err)
				}
				e.sess.mu.Lock()
				pub, rel := e.sess.pendingPublish[f.pi], e.sess.pendingPubRel[f.pi]
				binding := e.sess.abandonedAcks[mqttAbandonedAckIndex{gen: f.gen, pi: f.pi}]
				owner := e.sess.terminalSources[mqttTerminalSourceKeyOf(ref)]
				var disposal *mqttDisposal
				for _, w := range e.sess.disposals {
					if w.key.nonce == f.nonce {
						disposal = w
					}
				}
				incomplete := e.sess.piSpaceIncomplete
				suppressed := e.sess.sourceRetiredLocked(r.Dur, r.Seq) ||
					e.sess.terminalSourceHeldLocked(r.Dur, r.Seq) || e.sess.disposalSourceLocked(r.Dur, r.Seq)
				holds, charges := e.sess.custody.holdsPI(f.pi), e.sess.custody.chargesPI(f.pi)
				e.sess.mu.Unlock()
				if old != nil && (disposal != old || (handle != nil &&
					(len(old.handles) == 0 || old.handles[0] != handle))) {
					t.Fatal("recovery replaced the exact disposal instance/handle")
				}
				if tc.bad != "" {
					if !incomplete || confirmations.Load() != 0 || binding != nil || owner != nil ||
						(pub != nil && pub.regNonce != "" && !pub.regForeign) {
						t.Fatal("inconsistent/unreadable pair became actionable")
					}
					if cur, _ := f.register(t); cur == nil && tc.bad != "missing" {
						t.Fatal("opaque slot deleted")
					}
					return
				}
				if tc.state == mqttInflightPublish {
					if pub == nil || pub.regNonce != f.nonce || pub.regForeign || binding != nil || owner != nil || (cold && disposal != nil) ||
						f.exchange(t).Abandoned != nil || f.exchange(t).Phase != mqttExchangePublish {
						t.Fatal("pre-landing recovery invented abandonment")
					}
					return
				}
				if tc.state == mqttInflightPublishTerminal && !tc.cleaned &&
					(tc.source == mqttInflightSourceLive || tc.source == mqttInflightSourceUnknown) && owner == nil {
					t.Fatal("terminal source owner was not reconstructed")
				}
				if pub != nil {
					t.Fatal("recovery restored a PUBLISH entry")
				}
				if tc.cleaned || tc.source == mqttInflightSourceComplete || tc.source == mqttInflightSourceLost {
					if binding != nil || owner != nil {
						t.Fatal("cleaned slot gained an ACK/source owner")
					}
					if cur, _ := f.register(t); cur != nil {
						t.Fatal("completed source did not permit cleanup")
					}
					if tc.fact && (f.exchange(t).Abandoned == nil || charges) {
						t.Fatal("historical charge fact lost")
					}
					return
				}
				if !suppressed {
					t.Fatal("recovery omitted source suppression")
				}
				if tc.fact && charges {
					t.Fatal("abandonment was recharged")
				}
				if tc.state == mqttInflightDeliveryAbandoned {
					if binding == nil || disposal == nil || !holds || rel != nil ||
						f.exchange(t).Phase != mqttExchangeDeliveryAbandoned || testPubRelRecordStored(e) {
						t.Fatal("post-landing/pre-intent cut lost ownership or invented PUBREC")
					}
				}
				if tc.state == mqttInflightDeliveryAbandoned {
					allow.Store(true)
					source.Store(int32(mqttInflightSourceLive))
					if old != nil {
						e.sess.mu.Lock()
						old.busy = false
						e.sess.mu.Unlock()
					}
					e.s.finishOwedDisposals()
					e.sess.mu.Lock()
					left := e.sess.disposals[disposal.key]
					kept := e.sess.abandonedAcks[binding.index()] == binding
					e.sess.mu.Unlock()
					if left != nil || !kept || confirmations.Load() == 0 {
						t.Fatal("post-landing owner did not settle without another offer or ACK")
					}
					if cur, _ := f.register(t); cur == nil || cur.State != mqttInflightDeliveryAbandoned {
						t.Fatal("source settlement ended the unanswered MQTT exchange")
					}
				}
				if tc.state == mqttInflightPubRelIntended || tc.state == mqttInflightPubRelReady ||
					tc.state == mqttInflightPubRelTerminal {
					if disposal == nil || rel == nil {
						t.Fatal("fact successor skipped PUBREL/source recovery")
					}
				}
				if tc.state == mqttInflightPubRelIntended {
					testCustodyEventually(t, "recovery materializes the owed PUBREL", func() bool {
						cur, _ := f.register(t)
						return cur != nil && cur.State == mqttInflightPubRelReady && testPubRelRecordStored(e)
					})
				}
				if tc.state == mqttInflightPubRelTerminal {
					cur, _ := f.register(t)
					if cur == nil || cur.ActionToken != r.ActionToken ||
						f.exchange(t).Phase != mqttExchangeTerminalPendingDrain {
						t.Fatal("live token was completed")
					}
				}
				if tc.state == mqttInflightPublishTerminal {
					if owner == nil || owner.record.SourceAckReply != r.SourceAckReply ||
						(terminal != nil && owner != terminal) {
						t.Fatal("terminal source owner not reconciled")
					}
					if !tc.fact {
						if tc.source == mqttInflightSourceLive && confirmations.Load() == 0 {
							t.Fatal("durable reply was not used")
						}
						allow.Store(true)
						source.Store(int32(mqttInflightSourceLive))
						e.s.finishOwedTerminals()
						if cur, _ := f.register(t); cur != nil || confirmations.Load() == 0 {
							t.Fatal("no-traffic source/terminal cleanup failed")
						}
						if f.exchange(t).Abandoned != nil {
							t.Fatal("terminal cleanup invented abandonment")
						}
					}
				}
			})
		}
	}
}

func TestUnitCRecoveryCensusDoesNotOwnOrdinaryWalk(t *testing.T) {
	for _, mode := range []string{"missing-slot", "failed-census", "uncertain-census"} {
		t.Run(mode, func(t *testing.T) {
			e := testNewInflightEnv(t, "c69-census")
			var census *mqttCustodyCensus
			var missingPI uint16
			if mode == "missing-slot" {
				// A separate no-fact exchange lacks its slot; the ordinary
				// pair below must still be recovered in this same walk.
				testReadyForRealPubRelDelivery(t, e)
				f := testWithdrawAdmitMore(t, e, testRealPubRelSrcSeq, 1)
				_, seq := f.register(t)
				census = &mqttCustodyCensus{live: []*mqttOutboundExchange{f.exchange(t)}}
				missingPI = f.pi
				if err := e.asm.jsa.deleteMsg(mqttInflightStreamName, seq, true); err != nil {
					t.Fatal(err)
				}
			}
			e.s.closeWillDispatch()

			// As in marker recovery row 4, keep a real, unacknowledged source.
			// A synthetic absent source instead permits proven-unsent release.
			dur := e.sess.idHash + "_ordinary"
			sseq := e.publishSource(t, dur)
			pi := e.track(t, dur, sseq, 1)
			r, _ := e.loadRegister(t, pi)
			_, marker := e.loadMarker(t, pi)
			if r == nil || marker == nil || marker.Sequence == 0 {
				t.Fatal("ordinary fixture lacks its register/marker pair")
			}
			if mode == "uncertain-census" {
				census = &mqttCustodyCensus{uncertain: []string{
					mqttExchangeSubject(e.sess.idHash, e.sess.gen, r.Lin),
				}}
			}

			assertRecovered := func(stage string, wantWalk uint64) {
				t.Helper()
				e.sess.mu.Lock()
				defer e.sess.mu.Unlock()
				if e.sess.piSpaceIncomplete ||
					e.sess.inflightWalks != wantWalk ||
					e.sess.inflightApplyCompletedWalk != wantWalk ||
					!e.sess.custody.reconstructionIncomplete {
					t.Fatalf("%s: ordinary register apply inherited exchange-census uncertainty", stage)
				}
				p := e.sess.pendingPublish[pi]
				if p == nil || p.regNonce != r.Nonce || p.regForeign ||
					p.transmitted || p.markerOpaque || p.markerSeq != marker.Sequence ||
					e.sess.cpending[dur][sseq] != pi {
					t.Fatalf("%s: ordinary pair did not retain its unsent marker and source mapping", stage)
				}
				if missingPI != 0 && e.sess.pendingPublish[missingPI] != nil {
					t.Fatalf("%s: no-fact census invented an opaque register reservation", stage)
				}
			}

			e.sess.mu.Lock()
			e.sess.custody.beginReconstruction(e.sess.gen)
			before := e.sess.inflightWalks
			e.sess.mu.Unlock()

			// Prove this fixture reaches the ordinary install route using
			// the pre-C5 call shape, with no census argument at all.
			e.forgetInMemory()
			if _, err := e.sess.recoverInflightRegistersOnce(e.c); err != nil {
				t.Fatal(err)
			}
			assertRecovered("no-census baseline", before+1)

			// Reconstruct again from durable state, so an existing pending
			// entry cannot hide a census-dependent failure to install.
			e.forgetInMemory()
			if _, err := e.sess.recoverInflightRegistersOnce(e.c, census); err != nil {
				t.Fatal(err)
			}
			assertRecovered(mode, before+2)
		})
	}
}

// Successful exact reconciliation is not a successful recovery walk.
func TestUnitCRecoveryLocalReadKeepsIncompleteness(t *testing.T) {
	f := testAbandonAdmitted(t, "c5a-local", 2)
	w := testUnitCInstallDisposal(t, f)
	acq, err := f.e.sess.abandonCurrentOutputVeto(w.evidence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.sess.submitAbandonDeniedDelivery(w.evidence, acq); err != nil {
		t.Fatal(err)
	}
	r, seq := f.register(t)
	x := f.exchange(t)
	sess := f.e.sess
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.piSpaceIncomplete = true
	sess.custody.beginReconstruction(sess.gen)
	sess.inflightWalks, sess.inflightApplyCompletedWalk = 7, 6
	sess.pendingPublish[f.pi] = mqttRecoveryOpaquePending(seq)
	if !sess.recoverExchangeRouteLocked(f.e.c, f.subject, seq, r, x) {
		t.Fatal("exact pair refused")
	}
	if sess.disposals[w.key] != w || !sess.piSpaceIncomplete ||
		!sess.custody.reconstructionIncomplete || sess.inflightWalks != 7 || sess.inflightApplyCompletedWalk != 6 {
		t.Fatal("local reconciliation cleared incompleteness, provenance or instance identity")
	}
	sess.mu.Unlock()
	_, err = sess.recoverInflightRegistersOnce(f.e.c, nil)
	sess.mu.Lock()
	if err != nil {
		t.Fatal(err)
	}
	if sess.piSpaceIncomplete || !sess.custody.reconstructionIncomplete ||
		sess.inflightApplyCompletedWalk != sess.inflightWalks {
		t.Fatal("register apply and custody census lost their independent provenance")
	}
}
