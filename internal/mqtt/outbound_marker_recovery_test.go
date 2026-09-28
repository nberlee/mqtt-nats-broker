//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestConsult75UnconfirmedMarkerTerminalHandoff(t *testing.T) {
	for _, tc := range []struct {
		name     string
		qos      byte
		occupant bool
	}{
		{"landed-qos1", 1, false},
		{"landed-qos2", 2, false},
		{"occupant-qos2", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewInflightEnv(t, "c75-marker-terminal")
			e.s.closeWillDispatch()
			dur := e.sess.idHash + "_ordinary"
			sseq := e.publishSource(t, dur)
			if tc.occupant {
				// A lower-QoS proposal must reserve the existing QoS 2 obligation.
				e.track(t, dur, sseq, 2)
				e.forgetInMemory()
			}
			e.bind()
			e.asm.mu.Lock()
			e.asm.testInflightMarkerLoad = func(string) (*StoredMsg, error) {
				return nil, errors.New("test: unreadable marker")
			}
			e.asm.mu.Unlock()
			e.sess.mu.Lock()
			pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
			e.sess.mu.Unlock()
			if pi == 0 || !isNew {
				t.Fatalf("allocate: pi=%d isNew=%v", pi, isNew)
			}
			if e.sess.storeInflightRegister(e.c, pi, tc.qos, dur, sseq,
				e.nonceOf(pi), _EMPTY_, _EMPTY_) {
				t.Fatal("unreadable marker permitted sending")
			}
			e.asm.mu.Lock()
			e.asm.testInflightMarkerLoad = nil
			// Keep source completion unresolved, so the successor must retain PI exclusion.
			e.asm.testInflightSourceState = func(*mqttInflightRecord) mqttInflightSourceState {
				return mqttInflightSourceUnknown
			}
			e.asm.mu.Unlock()
			e.sess.mu.Lock()
			p := e.sess.pendingPublish[pi]
			_, sendOK := e.sess.beginSendLocked(e.c, pi, dur, sseq)
			restricted := p != nil && p.markerOpaque && !p.regPending &&
				e.sess.piSpaceIncomplete && e.sess.cpending[dur][sseq] == 0 &&
				!sendOK && e.sess.bumpPI() == 0
			e.sess.mu.Unlock()
			if !restricted {
				t.Fatal("actual unconfirmed marker constructor did not retain a restrictive reservation")
			}

			// Durable progress by another writer leaves this same session object resident.
			r, _ := e.loadRegister(t, pi)
			r.State = mqttInflightPublishTerminal
			x := &mqttOutboundExchange{
				ID: e.sess.id, Epoch: e.asm.sessStreamEpoch, Gen: e.sess.gen,
				Nonce: r.Lin, PI: pi, QoS: r.QoS, Phase: mqttExchangeTerminal,
				AdmitTxn: mqttNewInflightNonce(), Tenure: mqttNewInflightNonce(),
			}
			if r.QoS == 2 {
				x.Phase = mqttExchangeRejected
			}
			if err := mqttValidateInflightShape(r); err != nil {
				t.Fatal(err)
			}
			if err := mqttValidateOutboundExchange(x, x.ID, x.Epoch, x.Gen, r.Lin); err != nil {
				t.Fatal(err)
			}
			subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
			store := func(subject string, value any) {
				t.Helper()
				body, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := e.asm.jsa.storeMsg(subject, 0, body); err != nil {
					t.Fatal(err)
				}
			}
			store(mqttExchangeSubject(e.sess.idHash, e.sess.gen, r.Lin), x)
			store(subject, r)
			if !mqttRecoveryOwnsExchangeRoute(r, x) {
				t.Fatal("fixture does not select the C5 route")
			}

			for pass := 0; pass < 2; pass++ {
				if _, err := e.sess.recoverInflightRegisters(e.c); err != nil {
					t.Fatal(err)
				}
				e.sess.mu.Lock()
				// recoverExchangeRouteLocked must reach next.record = *r and
				// remove the old PUBLISH only after the terminal owner exists.
				owned := false
				for _, w := range e.sess.terminalSources {
					owned = owned || (w.record.Nonce == r.Nonce &&
						w.record.Lin == r.Lin && w.record.QoS == r.QoS &&
						w.record.Dur == dur && w.record.Seq == sseq)
				}
				ok := owned && e.sess.sendReservedLocked(pi) &&
					e.sess.pendingPublish[pi] == nil && e.sess.pendingPubRel[pi] == nil &&
					e.sess.cpending[dur][sseq] == 0 && !e.sess.piSpaceIncomplete &&
					e.sess.inflightApplyCompletedWalk == e.sess.inflightWalks
				allocated := e.sess.bumpPI()
				e.sess.mu.Unlock()
				if !ok || allocated == 0 || allocated == pi {
					t.Fatalf("pass %d: handoff/allocation failed: ok=%v allocated=%d", pass, ok, allocated)
				}
			}
		})
	}
}
