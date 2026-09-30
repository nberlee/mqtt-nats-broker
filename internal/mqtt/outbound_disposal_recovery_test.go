//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

func testUnitCRecoverySuccessor(t *testing.T, f *testAbandonFix, deny bool) string {
	t.Helper()
	tenure := mqttNewInflightNonce()
	testRewriteAuthority(t, f.e, func(a *mqttOutboundAuthority) { a.Tenure = tenure })
	// The entitlement is set once per accept, so model B's accept on the same
	// connection object explicitly; a plain set would be refused and leave A's.
	f.e.c.mu.Lock()
	f.e.c.mqtt.outEnt = mqttOutboundEntitlement{}
	f.e.c.mu.Unlock()
	f.e.c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: f.gen, Tenure: tenure})
	if ent, ok := f.e.c.outboundEntitlement(); !ok || ent.Tenure != tenure {
		t.Fatal("successor entitlement was not installed")
	}
	// And the session is bound to it, as the accept loop records beside the connection's
	// entitlement (F-105 7b): a resume applies only for the entitlement its session is bound to.
	f.e.sess.mu.Lock()
	f.e.sess.boundEnt, f.e.sess.boundEntOK = mqttOutboundEntitlement{Gen: f.gen, Tenure: tenure}, true
	f.e.sess.mu.Unlock()
	// A real, independently compiled successor snapshot, including the service
	// account and live identity connection checks used by ordinary delivery.
	f.e.s.dialClient = func(string, string, string) (*nats.Conn, error) { return nil, nil }
	setServiceAccount(f.e.s, "C5b", true)
	var denied []string
	if deny {
		denied = []string{">"}
	}
	perms, err := mqttCompilePermissions(&SubjectPermission{Allow: []string{">"}, Deny: denied}, true)
	if err != nil {
		t.Fatal(err)
	}
	f.e.c.mu.Lock()
	// The env's client is an internal captured-decision continuation, which no
	// identity governs; B is a real MQTT connection whose snapshot decides.
	f.e.c.origin = mqttOriginClient
	f.e.c.nrc = f.e.s.nrc
	f.e.c.authzValid, f.e.c.authzSvcAccount, f.e.c.mperms = true, "C5b", perms
	f.e.c.mu.Unlock()
	want := mqttSubDeliverAllow
	if deny {
		want = mqttSubDeliverDenied
	}
	if got := f.e.c.checkDenySubVerdict("foo.bar", _EMPTY_); got != want {
		t.Fatalf("successor snapshot decides %v, want %v", got, want)
	}
	return tenure
}

// T-C23, T-C25d-f, T-C32 and proof 4. No tests run in parallel: save seams
// are process-wide. Every cut observes the exact restriction independently of
// the CONNECT restore gate, and failures exercise real classified record saves.
func TestUnitCRecoveryHandoff(t *testing.T) {
	for _, route := range []string{"allow", "deny", "drain"} {
		for _, result := range []string{"landed", "definite", "ambiguous"} {
			t.Run(route+"/"+result, func(t *testing.T) {
				f, old := testUnitCOfferedDisposal(t, 1)
				e, sess := f.e, f.e.sess
				evidence := old.evidence
				sess.mu.Lock()
				// The fixture deleted the real transmission marker; mirror the
				// completed send in the surviving same-object scheduling entry.
				sess.pendingPublish[f.pi].markerSeq = 0
				sess.pendingPublish[f.pi].transmitted = true
				sess.mu.Unlock()
				tenure := testUnitCRecoverySuccessor(t, f, route == "deny")
				var d *mqttDrain
				if route == "drain" {
					d = addTestDrain(sess, "f092.source", old.key.dur, "f092.source", 1, nil)
				}
				testUnitCFailedRecovery(t, f, "successor inventory unavailable")
				old.step()
				testUnitCDisposalOwned(t, old)
				e.asm.mu.Lock()
				e.asm.testInflightEnumerate = nil
				e.asm.testSessPersistDrop = func(string) bool { return true }
				e.asm.testSessRecordBackstopOff = true
				e.asm.mu.Unlock()

				cuts := make(map[string]int)
				sess.mu.Lock()
				sess.testDisposalRecovery = func(stage string, prior, next *mqttDisposal) {
					cuts[stage]++
					if !sess.disposalSourceLocked(old.key.dur, old.key.seq) ||
						!sess.disposalPILocked(f.pi) || !sess.custody.holdsPI(f.pi) {
						t.Error("ownership gap at " + stage)
					}
					if stage != "before-transfer" && !sess.disposalCurrentLocked(next) {
						t.Error("successor not installed at " + stage)
					}
					if stage == "transfer" {
						_, indexed := sess.disposalsBySource[mqttDeferredKey{old.key.dur, old.key.seq}][next]
						if !indexed {
							t.Error("successor source ownership missing at transfer")
						}
					}
				}
				sess.mu.Unlock()
				if _, err := sess.recoverInflightRegisters(e.c); err != nil {
					t.Fatal(err)
				}
				sess.mu.Lock()
				next := sess.disposals[old.key]
				complete := sess.applyProvenanceCompleteLocked() && !sess.piSpaceIncomplete
				sess.mu.Unlock()
				if !complete || next == nil || next == old || next.recovery == nil ||
					next.recovery.deny != (route == "deny") || old.evidence != evidence {
					t.Fatal("successful resume failed to replace the old exact owner")
				}
				testUnitCDisposalOwned(t, next)
				sess.mu.Lock()
				preSaveToken, actionable := sess.beginSendLocked(e.c, f.pi, old.key.dur, old.key.seq)
				sess.mu.Unlock()
				if actionable {
					sess.endSend(e.c, f.pi, preSaveToken.token)
					t.Fatal("source became actionable before persistence")
				}
				if cuts["before-transfer"] != 1 || cuts["transfer"] != 1 || cuts["after-transfer"] != 1 {
					t.Fatalf("transfer route not witnessed: %v", cuts)
				}
				if route == "deny" && (next.evidence.Tenure != tenure || next.evidence == evidence) {
					t.Fatal("successor denial borrowed predecessor evidence")
				}
				if d != nil && (d.seqs[f.sseq] == nil || !d.seqs[f.sseq].claimed) {
					t.Fatal("old disposal still blocked floor enrollment")
				}
				// A normal callback must stop at the new owner before drain
				// dispatch. A local worker cannot submit even the fresh deny yet.
				notified, dispatched, submitted := 0, 0, 0
				sess.mu.Lock()
				sess.testDisposalNotify = func(w *mqttDisposal) { notified++ }
				sess.testDisposalDispatch = func() { dispatched++ }
				sess.mu.Unlock()
				e.asm.mu.Lock()
				e.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
					submitted++
					return write()
				}
				e.asm.mu.Unlock()
				pc := &client{kind: JETSTREAM}
				hdr := []byte("NATS/1.0\r\n" + mqttNatsHeader + ":1\r\n\r\n")
				pc.pa.hdr = len(hdr)
				sub := &subscription{client: e.c, mqtt: &mqttSub{qos: 1, jsDur: old.key.dur}}
				testUnitCNoSourceAck(t, f, func() {
					mqttDeliverMsgCbQoS12(sub, pc, nil, mqttStreamSubjectPrefix+"f092.source", f.reply, append(hdr, 'x'))
					next.step()
				})
				if notified != 1 || dispatched != 0 || submitted != 0 {
					t.Fatal("callback/worker bypassed staged recovery ownership")
				}
				restore := func() {}
				switch result {
				case "definite":
					restore = setMQTTTestSessSaveHook(func(*mqttSession) error { return errors.New("pre-store failure") })
				case "ambiguous":
					restore = setMQTTTestSessStoreResultHook(func(_ *mqttSession, r *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
						if err == nil && r != nil {
							return nil, errMQTTTestLostResponse
						}
						return r, err
					})
				}
				t.Cleanup(restore)
				err := testSessRecordTx(t, sess, func(tok recordTx) error { return sess.saveDisposalRecovery(e.c, tok) })
				restore()
				if cuts["before-persistence"] != 1 {
					t.Fatal("persistence pause was not reached")
				}
				if result != "landed" {
					if err == nil || next.recovery.saved {
						t.Fatal("failed/ambiguous save authorized activation")
					}
					sess.commitDisposalRecovery(e.c)
					testUnitCDisposalOwned(t, next)
					if d != nil && ((d.seqs[f.sseq] == nil) != (result == "definite")) {
						t.Fatal("classified floor rollback/retention is wrong")
					}
					if result == "ambiguous" {
						if !sess.isSeqUncertain() {
							t.Fatal("ambiguous save lost its existing resolver ownership")
						}
						sess.resolveUncertainNow(e.c)
						testUnitCDisposalOwned(t, next)
						if next.recovery.saved {
							t.Fatal("local readback activated a refused resume")
						}
					}
					// Only another successful synchronous resume may transfer
					// the retained restriction and attempt the transaction again.
					if _, err := sess.recoverInflightRegisters(e.c); err != nil {
						t.Fatal(err)
					}
					sess.mu.Lock()
					next = sess.disposals[old.key]
					sess.mu.Unlock()
					err = testSessRecordTx(t, sess, func(tok recordTx) error { return sess.saveDisposalRecovery(e.c, tok) })
				}
				if err != nil || next == nil || next.recovery == nil || !next.recovery.saved {
					t.Fatalf("successful floor/disposition save: %v", err)
				}
				// A fresh restore reads the floor from the actual record before
				// this object's owner is allowed to activate it.
				if d != nil {
					cold, _, err := e.asm.createOrRestoreSession(sess.id, e.s.getOpts())
					if err != nil || cold == sess || cold.drainByDur[old.key.dur] == nil ||
						cold.drainByDur[old.key.dur].seqs[f.sseq] == nil ||
						!cold.drainByDur[old.key.dur].seqs[f.sseq].claimed {
						t.Fatalf("floor was not durable before activation: %v", err)
					}
				}
				sess.commitDisposalRecovery(e.c)
				if route == "deny" {
					if next.recovery != nil {
						t.Fatal("fresh deny was not activated")
					}
					next.step()
					f.requireAbandoned(t)
				} else {
					sess.mu.Lock()
					left := sess.disposals[old.key]
					held := sess.custody.holdsPI(f.pi)
					tok, admitted := sess.beginSendLocked(e.c, f.pi, old.key.dur, old.key.seq)
					sess.mu.Unlock()
					if left != nil || !held || !admitted || f.exchange(t).Abandoned != nil {
						t.Fatal("ordinary recovery lost custody or normal output admission")
					}
					sess.endSend(e.c, f.pi, tok.token)
				}
			})
		}
	}
}

// The classifier's own contract: no downstream gate can mask M-C49.
func TestUnitCRecoveryNonLandingCertificate(t *testing.T) {
	f := testAbandonAdmitted(t, "c5b-certificate", 1)
	w := testUnitCInstallDisposal(t, f)
	r, _ := f.register(t)
	x := f.exchange(t)
	if !mqttDisposalNeverLandedPair(w, r, x, true) {
		t.Fatal("ordinary exact pair refused")
	}
	if mqttDisposalNeverLandedPair(w, r, x, false) {
		t.Fatal("unreadable pair classified as never-landed")
	}
	r.State, x.Phase = mqttInflightPublishTerminal, mqttExchangeTerminal
	if mqttDisposalNeverLandedPair(w, r, x, true) {
		t.Fatal("ACK-first terminal pair was treated as an ordinary predecessor")
	}
}

func TestUnitCRecoveryRetainsUnresolvedAttempt(t *testing.T) {
	f, old := testUnitCOfferedDisposal(t, 1)
	e, sess := f.e, f.e.sess
	hook := func(tr *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		if tr.Attempt.Kind == mqttTransitionAbandonDelivery {
			return 0, errors.New("unknown abandonment response")
		}
		return send()
	}
	e.asm.jsa.testTransitionCommit.Store(&hook)
	t.Cleanup(func() { e.asm.jsa.testTransitionCommit.Store(nil) })
	e.asm.mu.Lock()
	e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
		return mqttTransitionUnresolved, true
	}
	e.asm.mu.Unlock()
	old.step()
	if len(old.handles) == 0 {
		t.Fatal("fixture did not retain an actual submission handle")
	}
	handle, evidence := old.handles[0], old.evidence
	testUnitCRecoverySuccessor(t, f, false)
	if _, err := sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	testUnitCDisposalOwned(t, old)
	sess.mu.Lock()
	incomplete := sess.piSpaceIncomplete && !sess.applyProvenanceCompleteLocked()
	sess.mu.Unlock()
	if !incomplete || len(old.handles) == 0 || old.handles[0] != handle || old.evidence != evidence {
		t.Fatal("unresolved attempt lost ownership or falsely completed recovery")
	}
	e.asm.mu.Lock()
	e.asm.testCustodyResolve = func(*mqttTransitionHandle, string) (mqttTransitionVerdict, bool) {
		return mqttTransitionNeverLanded, true
	}
	e.asm.mu.Unlock()
	if _, err := sess.recoverInflightRegisters(e.c); err != nil {
		t.Fatal(err)
	}
	sess.mu.Lock()
	next := sess.disposals[old.key]
	sess.mu.Unlock()
	if next == nil || next == old || next.recovery == nil {
		t.Fatal("positive settlement followed by a successful resume did not transfer")
	}
}
