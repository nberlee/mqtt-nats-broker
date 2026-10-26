//go:build mqtt_fulltests

// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The retained abandonment's continuation is the DRIVER's (consult 103 2f): an unresolved attempt
// is settled before anything is re-submitted, the durable abandonment is applied to memory only
// once it reads back, and a decision whose entitlement no longer serves is passed on, never
// submitted under a successor.

// awaitAbandonmentApplied waits for memory to carry the abandonment: the ACK-only binding of the
// exact allocation installed and the scheduling entry gone.
func (rig *testRetainedAbandonRig) awaitAbandonmentApplied(t *testing.T, reg *mqttInflightRecord) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		rig.sess.mu.Lock()
		b := rig.sess.abandonedAcks[mqttAbandonedAckIndex{gen: rig.gen, pi: rig.pi}]
		_, entry := rig.sess.pendingPublish[rig.pi]
		rig.sess.mu.Unlock()
		if b != nil && b.nonce == reg.Nonce && b.payload == reg.Payload && !entry {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the abandonment was never applied to memory: binding %+v, entry %v", b, entry)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestF105RetainedAbandonLostResponseIsSettledNotResubmitted: the abandonment commits but its
// response is lost, and the attempt's settlement stays UNRESOLVED for two passes. The driver keeps
// the handle, submits nothing further, and applies the abandonment once the settlement lands.
func TestF105RetainedAbandonLostResponseIsSettledNotResubmitted(t *testing.T) {
	var submits, unresolved atomic.Int32
	rig := testRetainedAbandonUpExpiring(t, "ret-ab-lost", 1, 60, func(rig *testRetainedAbandonRig) {
		rig.asm.mu.Lock()
		rig.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			submits.Add(1)
			out, err := write()
			if err == nil {
				return out, errors.New("the response was lost")
			}
			return out, err
		}
		rig.asm.testCustodyResolve = func(h *mqttTransitionHandle, step string) (mqttTransitionVerdict, bool) {
			if h.attempt.Op == "op-abandon-delivery" && unresolved.Add(1) <= 4 {
				return mqttTransitionUnresolved, true
			}
			return 0, false
		}
		rig.asm.mu.Unlock()
	})
	defer rig.done()
	reg, x := rig.abandoned(t)
	assertRetainedArm(t, reg, x, mqttAbandonReasonPacketTooLarge)
	rig.awaitAbandonmentApplied(t, reg)
	if n := submits.Load(); n != 1 {
		t.Fatalf("the abandonment was submitted %d times, want once: an unresolved attempt is settled, never re-submitted", n)
	}
	if unresolved.Load() < 4 {
		t.Fatalf("the settlement seam ran %d times: the fixture did not hold the attempt unresolved", unresolved.Load())
	}
}

// TestF105RetainedAbandonSurvivesTransientReadbackFailures: after the abandonment commits, the
// register's readback fails three times. The driver neither gives up nor re-decides: it applies the
// abandonment once the register reads again.
func TestF105RetainedAbandonSurvivesTransientReadbackFailures(t *testing.T) {
	var committed atomic.Bool
	var failed atomic.Int32
	rig := testRetainedAbandonUpExpiring(t, "ret-ab-readback", 1, 60, func(rig *testRetainedAbandonRig) {
		rig.asm.mu.Lock()
		rig.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			out, err := write()
			if err == nil {
				committed.Store(true)
			}
			return out, err
		}
		as := rig.asm
		rig.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
			if committed.Load() && strings.HasSuffix(subject, ".1") && failed.Add(1) <= 3 {
				return nil, nil, errors.New("the register read failed")
			}
			return as.jsa.loadInflightRecord(subject)
		}
		rig.asm.mu.Unlock()
	})
	defer rig.done()
	reg, _ := rig.abandoned(t)
	rig.awaitAbandonmentApplied(t, reg)
	if failed.Load() < 3 {
		t.Fatalf("the readback failed %d times: the fixture did not reach the post-commit window", failed.Load())
	}
}

// TestF105RetainedAbandonPassesToATakeover: the abandonment decision is made, then a takeover
// resumes the session on a new connection (a new tenure, a limit the replay fits) before the
// decision is submitted. The decision is passed on, never committed under the successor; the
// successor's own resume replays the PUBLISH.
func TestF105RetainedAbandonPassesToATakeover(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var submits atomic.Int32
	rig := testRetainedAbandonUpExpiring(t, "ret-ab-takeover", 1, 60, func(rig *testRetainedAbandonRig) {
		rig.asm.mu.Lock()
		rig.asm.testAbandonDelivery = func(req *mqttAbandonDeliveryRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
			if submits.Add(1) == 1 {
				entered <- struct{}{}
				<-release
			}
			return write()
		}
		rig.asm.mu.Unlock()
	})
	defer rig.done()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the abandonment was never submitted")
	}
	takeover := &mqttV5ConnInfo{clientID: rig.id, cleanStart: false, props: mqttV5ConnPropsSessionExpiry(60)}
	c2, r2 := testMQTTConnectV5(t, takeover, rig.o.MQTT.Host, rig.o.MQTT.Port)
	defer c2.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r2); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the takeover: rc=%x present=%v", rc, present)
	}
	close(release)
	flags, pi, _ := testMQTTReadPubV5Raw(t, r2, "ab/big", []byte(strings.Repeat("x", 200)))
	if pi != rig.pi || flags&mqttPubFlagDup == 0 {
		t.Fatalf("the successor's replay went out as pi %d flags %x, want pi %d with DUP", pi, flags, rig.pi)
	}
	subject := mqttInflightSubject(rig.idHash, rig.gen, rig.pi)
	time.Sleep(500 * time.Millisecond)
	cur, _, _ := rig.asm.loadInflightRecord(subject)
	if cur == nil || cur.State == mqttInflightDeliveryAbandoned {
		t.Fatalf("the displaced decision was committed under the successor: register %+v", cur)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, pi)
}

// Holistic round 1, finding 1: an ABANDONED retained QoS 2 exchange answered by a late PUBREC while
// the register cannot be read. The handler leaves the intent to the asynchronous resolver and the
// connection open; once reads recover, the resolver's confirmation must still install and emit the
// owed PUBREL to the silent, connected client.
func TestF105RetainedAbandonedLatePubRecResolvedAsynchronouslyEmitsThePubRel(t *testing.T) {
	var armed atomic.Bool
	var failed atomic.Int32
	rig := testRetainedAbandonUpExpiring(t, "ret-ab-async", 2, 60, func(rig *testRetainedAbandonRig) {
		as := rig.asm
		rig.asm.mu.Lock()
		rig.asm.testInflightLoad = func(subject string) (*mqttInflightRecord, *StoredMsg, error) {
			if armed.Load() && strings.HasSuffix(subject, ".1") && failed.Add(1) <= 4 {
				return nil, nil, errors.New("the register read failed")
			}
			return as.jsa.loadInflightRecord(subject)
		}
		rig.asm.mu.Unlock()
	})
	defer rig.done()
	reg, _ := rig.abandoned(t)
	rig.awaitAbandonmentApplied(t, reg)
	armed.Store(true)
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, rig.pi)
	if failed.Load() < 4 {
		t.Fatalf("only %d register reads failed: the fixture never forced the asynchronous resolution", failed.Load())
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, rig.pi)
	testMQTTFlush(t, rig.c, nil, rig.r)
}

// Holistic round 3, finding 2 (consult 106): a confirmed late PUBREC on an ABANDONED retained QoS 2
// exchange owes the connected client its PUBREL, and an installation that is refused is OWNED —
// retried by the continuation worker — never a silent completion. (a) A transient refusal: the
// PUBREL still arrives on the open connection. (b) A cold resume whose inventory could not decide
// the exchange (its duty unreadable): the abandoned arm seeds custody provisionally, as every
// restored entry does, so the installation is not refused for want of the lineage.
func TestF105RetainedAbandonedPubRelInstallationIsOwned(t *testing.T) {
	t.Run("transient-refusal", func(t *testing.T) {
		rig := testRetainedAbandonUp(t, "ret-ab-install", 2)
		defer rig.done()
		reg, _ := rig.abandoned(t)
		rig.awaitAbandonmentApplied(t, reg)
		var refusals atomic.Int32
		refuse := func() bool { return refusals.Add(1) <= 2 }
		rig.sess.testRetainedInstallRefused.Store(&refuse)
		defer rig.sess.testRetainedInstallRefused.Store(nil)
		testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
		for deadline := time.Now().Add(5 * time.Second); refusals.Load() < 1 && time.Now().Before(deadline); {
			time.Sleep(5 * time.Millisecond)
		}
		testRetainedOwnershipHolds(t, &rig.testRetainedRig, false, 4, 2*time.Second)
		testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, rig.pi)
		if refusals.Load() < 3 {
			t.Fatalf("the installation was attempted %d time(s): the refusal never forced a retry", refusals.Load())
		}
		testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, rig.pi)
		testMQTTFlush(t, rig.c, nil, rig.r)
	})
	t.Run("cold-resume-undecided-inventory", func(t *testing.T) {
		rig := testRetainedAbandonUp(t, "ret-ab-coldseed", 2)
		defer func() { rig.done() }()
		reg, _ := rig.abandoned(t)
		rig.awaitAbandonmentApplied(t, reg)
		// The broker restarts and the resume's custody census cannot read the exchange (the custody
		// read faults; the register and the exchange still read cleanly to the classification), so
		// the inventory cannot decide the lineage — and keeps not deciding it for the whole test.
		xsubj := mqttExchangeSubject(rig.idHash, rig.gen, reg.Lin)
		dir := strings.TrimSuffix(rig.s.JetStreamConfig().StoreDir, JetStreamStoreDir)
		rig.s.Shutdown()
		rig.c.Close()
		rig.o.MQTT.Port = -1
		testSetStoreDir(rig.o, dir)
		rig.s = testMQTTRunServer(t, rig.o)
		rig.cleanup = append(rig.cleanup, func() { testMQTTShutdownServer(rig.s) })
		probe, probeR := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: rig.id + "-probe", cleanStart: true},
			rig.o.MQTT.Host, rig.o.MQTT.Port)
		defer probe.Close()
		testMQTTReadConnAckV5(t, probeR)
		as := testMQTTGetAccountSessionManager(t, rig.s, rig.id+"-probe")
		as.mu.Lock()
		as.testCustodyDecodeFault = func(subject string) bool { return subject == xsubj }
		as.mu.Unlock()
		rig.c, rig.r = testMQTTConnectV5(t, rig.ci, rig.o.MQTT.Host, rig.o.MQTT.Port)
		if present, rc, _ := testMQTTReadConnAckV5(t, rig.r); rc != mqttConnAckRCConnectionAccepted || !present {
			t.Fatalf("the resume after the restart: rc=%x present=%v", rc, present)
		}
		rig.attach(t)
		rig.sess.mu.Lock()
		undecided := rig.sess.custody.subjectUncertain(rig.gen, xsubj)
		rig.sess.mu.Unlock()
		if !undecided {
			t.Fatal("the fixture never left the resume's inventory undecided")
		}
		testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, rig.pi)
		testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, rig.pi)
		testMQTTFlush(t, rig.c, nil, rig.r)
	})
}

// TestF105RetainedStaleResolverDoesNotActOverAQuarantine (holistic H-10): a PUBREC intent resolver of
// the PREVIOUS connection that confirms after a same-object resume quarantined the exchange (its
// canonical PUBREL absent) must install nothing: the quarantine authorizes no action, and the
// exchange is not that connection's any more. The resolver's confirmation is driven directly with
// the previous connection and the ACK-only snapshot it captured.
func TestF105RetainedStaleResolverDoesNotActOverAQuarantine(t *testing.T) {
	rig := testRetainedAbandonUp(t, "ret-ab-stale", 2)
	defer rig.done()
	reg, _ := rig.abandoned(t)
	rig.awaitAbandonmentApplied(t, reg)
	rig.sess.mu.Lock()
	oldC := rig.sess.c
	b := rig.sess.abandonedAcks[mqttAbandonedAckIndex{gen: rig.gen, pi: rig.pi}]
	rig.sess.mu.Unlock()
	if b == nil || oldC == nil {
		t.Fatal("no abandoned binding or connection")
	}
	ack := mqttAckSnapshot(b)
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, rig.pi)
	subject := mqttInflightSubject(rig.idHash, rig.gen, rig.pi)
	cur, _, _ := rig.asm.loadInflightRecord(subject)
	if cur == nil || cur.State != mqttInflightPubRelReady || cur.RelSubj == _EMPTY_ {
		t.Fatalf("the late PUBREC did not advance to pubrel_ready: %+v", cur)
	}
	rig.c.Close()
	// The unresolved-intent shape: no PUBREL entry was installed on the previous connection.
	rig.sess.mu.Lock()
	delete(rig.sess.pendingPubRel, rig.pi)
	rig.sess.mu.Unlock()
	raw, err := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, cur.RelSubj)
	if err != nil || raw == nil {
		t.Fatalf("loading the canonical: %v", err)
	}
	if err := rig.asm.jsa.deleteMsg(mqttInflightStreamName, raw.Sequence, true); err != nil {
		t.Fatalf("removing the canonical: %v", err)
	}
	rig.c, rig.r = testMQTTConnectV5(t, rig.ci, rig.o.MQTT.Host, rig.o.MQTT.Port)
	if present, rc, _ := testMQTTReadConnAckV5(t, rig.r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the same-object resume: rc=%x present=%v", rc, present)
	}
	rig.sess.mu.Lock()
	quarantined := rig.sess.retainedQuarantinedLocked(cur.Nonce)
	rig.sess.mu.Unlock()
	if !quarantined {
		t.Fatal("the fixture never quarantined the exchange")
	}
	// THE PREVIOUS CONNECTION'S RESOLVER CONFIRMS NOW — through its own attempt, and through the
	// confirmation's continuation directly.
	r := &mqttIntentResolution{key: mqttIntentResolutionKey{sess: rig.sess, name: ack.slot.name, epoch: rig.epoch,
		nonce: ack.regNonce, lin: ack.ev.lin}, srv: rig.s, c: oldC, ack: *ack}
	if !r.attempt(false) {
		t.Fatal("the stale resolver did not stand down over a quarantined exchange")
	}
	rig.sess.continueAbandonedRetainedPubRel(oldC, rig.pi, ack)
	time.Sleep(200 * time.Millisecond)
	rig.sess.mu.Lock()
	rel := rig.sess.pendingPubRel[rig.pi]
	rig.sess.mu.Unlock()
	if rel != nil && rel.acts() {
		t.Fatalf("a stale resolver installed an acting PUBREL entry over a quarantine: %+v", rel)
	}
	rig.sess.retainedDriver.mu.Lock()
	for _, o := range rig.sess.retainedDriver.owed {
		if o.pi == rig.pi && o.nonce == cur.Nonce {
			rig.sess.retainedDriver.mu.Unlock()
			t.Fatalf("a stale resolver left a continuation for a quarantined exchange: %+v", o)
		}
	}
	rig.sess.retainedDriver.mu.Unlock()
	testMQTTExpectNothing(t, rig.r)
	testRetainedOwnershipHolds(t, &rig.testRetainedRig, false, 4, 2*time.Second)
}

// TestF105RetainedCloseRefusesAContradictedAbandonment (consult 108, row 9): the manifest close's
// abandoned arm admits a copy only on POSITIVE agreement between the exchange's retained
// abandonment fact and its register. A delivery_abandoned register whose (valid) abandonment
// reason disagrees with the fact is the contradiction a resume quarantines; the close must refuse
// it as material still needed, and the copy stays. Reclamation is held off (the copy's owner
// unreadable) until the contradiction is in place, then one step is driven directly.
func TestF105RetainedCloseRefusesAContradictedAbandonment(t *testing.T) {
	for _, tc := range []struct {
		name string
		// forge rewrites the register or the exchange into a contradiction a resume quarantines.
		forge func(t *testing.T, rig *testRetainedAbandonRig, subject string, cur *mqttInflightRecord)
	}{
		{"register-reason-disagrees", func(t *testing.T, rig *testRetainedAbandonRig, subject string, cur *mqttInflightRecord) {
			other := mqttAbandonReasonACLDeny
			if cur.AbandonReason == other {
				other = mqttAbandonReasonPacketTooLarge
			}
			testRewriteRecordField(t, rig.asm, subject, "abandon", other)
		}},
		{"exchange-past-pubrec-register-abandoned", func(t *testing.T, rig *testRetainedAbandonRig, subject string, cur *mqttInflightRecord) {
			testRewriteRecordField(t, rig.asm, mqttExchangeSubject(rig.idHash, rig.gen, cur.Lin), "phase",
				int(mqttExchangePubRecObserved))
		}},
		{"register-pubrel-ready-exchange-abandoned", func(t *testing.T, rig *testRetainedAbandonRig, subject string, cur *mqttInflightRecord) {
			testRewriteRecordField(t, rig.asm, subject, "state", string(mqttInflightPubRelReady))
		}},
		// A LATE PUBREC advances the abandoned exchange (PubRecObserved over pubrel_ready, the fact
		// riding on), then its canonical goes missing or names another exchange: the pair recovery
		// quarantines on the canonical, which the close must refuse by the same check.
		{"late-pubrec-canonical-absent", func(t *testing.T, rig *testRetainedAbandonRig, subject string, cur *mqttInflightRecord) {
			rel := testRetainedLatePubRec(t, rig, subject)
			raw, err := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, rel)
			if err != nil || raw == nil {
				t.Fatalf("loading the canonical: %v", err)
			}
			if err := rig.asm.jsa.deleteMsg(mqttInflightStreamName, raw.Sequence, true); err != nil {
				t.Fatalf("removing the canonical: %v", err)
			}
		}},
		{"late-pubrec-canonical-mismatched", func(t *testing.T, rig *testRetainedAbandonRig, subject string, cur *mqttInflightRecord) {
			rel := testRetainedLatePubRec(t, rig, subject)
			testRewriteRecordField(t, rig.asm, rel, "identity", mqttNewInflightNonce())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hold atomic.Bool
			hold.Store(true)
			var ownerSubj atomic.Value
			restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
				if os, _ := ownerSubj.Load().(string); hold.Load() && os != _EMPTY_ && subject == os {
					return errors.New("the owner read is held")
				}
				return nil
			})
			defer restore()
			rig := testRetainedAbandonUpExpiring(t, "ret-ab-contra", 2, 60, func(rig *testRetainedAbandonRig) {
				rig.asm.mu.RLock()
				sess := rig.asm.sessions[rig.id]
				rig.asm.mu.RUnlock()
				sess.mu.Lock()
				idHash, gen := sess.idHash, sess.gen
				sess.mu.Unlock()
				reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(idHash, gen, rig.pi))
				if reg == nil {
					t.Fatal("no retained register before the resume")
				}
				m, _, merr := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, gen, reg.Payload)
				if merr != nil {
					t.Fatalf("no copy before the resume: %v", merr)
				}
				// The reclaimer's FIRST read is the copy's owner resource: holding it holds
				// reclamation without touching any read the resume or the abandonment makes.
				ownerSubj.Store(mqttResourceSubject(idHash, gen, m.Inc))
			})
			defer rig.done()
			reg, _ := rig.abandoned(t)
			rig.awaitAbandonmentApplied(t, reg)
			subject := mqttInflightSubject(rig.idHash, rig.gen, rig.pi)
			cur, _, _ := rig.asm.loadInflightRecord(subject)
			if cur == nil || cur.State != mqttInflightDeliveryAbandoned {
				t.Fatalf("the register is not delivery_abandoned: %+v", cur)
			}
			tc.forge(t, rig, subject, cur)
			hold.Store(false)
			man, _, merr := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, rig.gen, cur.Payload)
			if merr != nil {
				t.Fatalf("the copy was reclaimed before the contradiction was in place: %v", merr)
			}
			_, err := rig.asm.jsa.reclaimRetainedPayloadStep(rig.asm, rig.id, rig.epoch, rig.gen, man.Inc)
			if !errors.Is(err, errMQTTRetainedMaterialNeeded) {
				t.Fatalf("the close over a contradicted abandonment answered %v, want material still needed", err)
			}
			if after, _, aerr := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, rig.gen, cur.Payload); aerr != nil ||
				after.State == mqttManifestClosed {
				t.Fatalf("the contradicted abandonment's copy was closed: %+v %v", after, aerr)
			}
		})
	}
}

// testRewriteRecordField rewrites one JSON field of the record at subject on $MQTT_inflight.
func testRewriteRecordField(t *testing.T, as *mqttAccountSessionManager, subject, field string, value any) {
	t.Helper()
	raw, err := as.jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil || raw == nil {
		t.Fatalf("loading %q: %v", subject, err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw.Data, &m); err != nil {
		t.Fatalf("decoding %q: %v", subject, err)
	}
	m[field] = value
	body, _ := json.Marshal(m)
	if _, err := as.jsa.storeMsg(subject, -1, body); err != nil {
		t.Fatalf("rewriting %q: %v", subject, err)
	}
}

// testRetainedLatePubRec answers an abandoned retained QoS 2 exchange with a late PUBREC, reads the
// PUBREL it owes, and returns the canonical's subject once the register stands at pubrel_ready.
func testRetainedLatePubRec(t *testing.T, rig *testRetainedAbandonRig, subject string) string {
	t.Helper()
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, rig.pi)
	for deadline := time.Now().Add(5 * time.Second); ; {
		cur, _, _ := rig.asm.loadInflightRecord(subject)
		if cur != nil && cur.State == mqttInflightPubRelReady && cur.RelSubj != _EMPTY_ {
			return cur.RelSubj
		}
		if time.Now().After(deadline) {
			t.Fatalf("the late PUBREC never advanced the register: %+v", cur)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestF105RetainedAckAdvanceRefusesAContradictedPair (consult 109, obligation 3): the
// acknowledgement advance is tenure-agnostic by design, so an advance already in flight is fenced
// only by its boundary: the standing register and the exchange's predecessor must agree by the one
// consistency definition. A late PUBREC's advance on an abandoned retained QoS 2 exchange is held
// at its write while the exchange's abandonment reason is rewritten (a contradiction a resume
// quarantines); released, the advance is refused — the register stays delivery_abandoned and no
// canonical PUBREL is created.
func TestF105RetainedAckAdvanceRefusesAContradictedPair(t *testing.T) {
	rig := testRetainedAbandonUp(t, "ret-ab-ackpair", 2)
	defer rig.done()
	reg, _ := rig.abandoned(t)
	rig.awaitAbandonmentApplied(t, reg)
	subject := mqttInflightSubject(rig.idHash, rig.gen, rig.pi)
	cur, _, _ := rig.asm.loadInflightRecord(subject)
	if cur == nil || cur.State != mqttInflightDeliveryAbandoned {
		t.Fatalf("the register is not delivery_abandoned: %+v", cur)
	}
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var outcome atomic.Value
	rig.asm.mu.Lock()
	rig.asm.testAckAdvance = func(req *mqttAdvanceExchangeAckRequest, write func() (mqttOpOutcome, error)) (mqttOpOutcome, error) {
		once.Do(func() { close(held); <-release })
		out, err := write()
		if err != nil {
			outcome.Store(err.Error())
		} else {
			outcome.Store("landed")
		}
		return out, err
	}
	rig.asm.mu.Unlock()
	defer func() {
		rig.asm.mu.Lock()
		rig.asm.testAckAdvance = nil
		rig.asm.mu.Unlock()
	}()
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the late PUBREC never reached its advance")
	}
	x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, cur.Lin)
	other := mqttAbandonReasonACLDeny
	if x.Abandoned.Reason == other {
		other = mqttAbandonReasonPacketTooLarge
	}
	raw, _ := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttExchangeSubject(rig.idHash, rig.gen, cur.Lin))
	var m map[string]any
	if err := json.Unmarshal(raw.Data, &m); err != nil {
		t.Fatalf("decoding the exchange: %v", err)
	}
	ab, _ := m["abandoned"].(map[string]any)
	if ab == nil {
		t.Fatalf("the exchange carries no abandonment fact: %s", raw.Data)
	}
	ab["reason"] = other
	body, _ := json.Marshal(m)
	if _, err := rig.asm.jsa.storeMsg(mqttExchangeSubject(rig.idHash, rig.gen, cur.Lin), -1, body); err != nil {
		t.Fatalf("rewriting the exchange: %v", err)
	}
	close(release)
	for deadline := time.Now().Add(5 * time.Second); outcome.Load() == nil && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if o, _ := outcome.Load().(string); o == "landed" || o == _EMPTY_ {
		t.Fatalf("the advance over a contradicted pair was not refused: %q", o)
	}
	after, _, _ := rig.asm.loadInflightRecord(subject)
	if after == nil || after.State != mqttInflightDeliveryAbandoned {
		t.Fatalf("the contradicted pair was advanced: register %+v", after)
	}
	if _, err := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName,
		mqttRetainedRelSubject(rig.idHash, rig.gen, cur.Nonce)); err == nil {
		t.Fatal("a canonical PUBREL was created over a contradicted pair")
	}
}

// TestF105RetainedCloseLocatesTheRegisterByItsAdmission (consult 109, obligations 2 and 4): the
// manifest close locates the allocation's register by the copy's ADMISSION ADDRESS, never by the
// exchange's PI. An exchange whose PI was rewritten — to an empty identifier or to one another
// allocation's register occupies — is refused on BOTH arms; the ended arm without the rewrite closes
// (its register legitimately gone or terminal). Reclamation is held off at the copy's owner read.
func TestF105RetainedCloseLocatesTheRegisterByItsAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ended   bool // a late PUBACK ends the abandoned QoS 1 exchange first
		wrongPI bool
		foreign bool // another allocation's register occupies the wrong identifier
	}{
		{"abandoned-wrong-pi-empty", false, true, false},
		{"abandoned-wrong-pi-foreign", false, true, true},
		{"ended-wrong-pi-empty", true, true, false},
		{"ended-wrong-pi-foreign", true, true, true},
		{"ended-control-closes", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hold atomic.Bool
			hold.Store(true)
			var ownerSubj atomic.Value
			restore := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
				if os, _ := ownerSubj.Load().(string); hold.Load() && os != _EMPTY_ && subject == os {
					return errors.New("the owner read is held")
				}
				return nil
			})
			defer restore()
			rig := testRetainedAbandonUpExpiring(t, "ret-ab-addr", 1, 60, func(rig *testRetainedAbandonRig) {
				rig.asm.mu.RLock()
				sess := rig.asm.sessions[rig.id]
				rig.asm.mu.RUnlock()
				sess.mu.Lock()
				idHash, gen := sess.idHash, sess.gen
				sess.mu.Unlock()
				reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(idHash, gen, rig.pi))
				m, _, merr := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, gen, reg.Payload)
				if merr != nil {
					t.Fatalf("no copy before the resume: %v", merr)
				}
				ownerSubj.Store(mqttResourceSubject(idHash, gen, m.Inc))
			})
			defer rig.done()
			reg, _ := rig.abandoned(t)
			rig.awaitAbandonmentApplied(t, reg)
			if tc.ended {
				testMQTTSendPIPacket(mqttPacketPubAck, t, rig.c, rig.pi)
				for deadline := time.Now().Add(5 * time.Second); ; {
					x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, reg.Lin)
					if x != nil && mqttExchangePhaseEnded(x.Phase) {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("the late PUBACK never ended the exchange: %+v", x)
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			const wrong = 9
			if tc.foreign {
				foreign := *reg
				foreign.PI, foreign.Nonce, foreign.Lin = wrong, mqttNewInflightNonce(), mqttNewInflightNonce()
				body, _ := json.Marshal(&foreign)
				if _, err := rig.asm.jsa.storeMsg(mqttInflightSubject(rig.idHash, rig.gen, wrong), -1, body); err != nil {
					t.Fatalf("placing the foreign register: %v", err)
				}
			}
			if tc.wrongPI {
				testRewriteRecordField(t, rig.asm, mqttExchangeSubject(rig.idHash, rig.gen, reg.Lin), "pi", wrong)
			}
			hold.Store(false)
			man, _, merr := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, rig.gen, reg.Payload)
			if merr != nil {
				t.Fatalf("the copy was reclaimed before the step: %v", merr)
			}
			if man.AdmittedPI != rig.pi {
				t.Fatalf("the copy's admission address is %d, want %d", man.AdmittedPI, rig.pi)
			}
			_, err := rig.asm.jsa.reclaimRetainedPayloadStep(rig.asm, rig.id, rig.epoch, rig.gen, man.Inc)
			after, _, _ := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, rig.gen, reg.Payload)
			if !tc.wrongPI {
				if err != nil || after == nil || after.State != mqttManifestClosed {
					t.Fatalf("the ended exchange's copy did not close: %v, manifest %+v", err, after)
				}
				return
			}
			if !errors.Is(err, errMQTTRetainedMaterialNeeded) {
				t.Fatalf("the close over a wrong-PI exchange answered %v, want material still needed", err)
			}
			if after == nil || after.State == mqttManifestClosed {
				t.Fatalf("the wrong-PI exchange's copy was closed: %+v", after)
			}
		})
	}
}
