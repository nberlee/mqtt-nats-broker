//go:build mqttport_fulltests

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
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// T24 — a retained replay the resumed connection can no longer carry (F-105 7c, plan §3.6.1).

// testRetainedAbandonRig is a v5 server with one retained message delivered once to a persistent
// subscriber that never answered, and the subscriber then resumed with a Maximum Packet Size the
// replay no longer fits.
type testRetainedAbandonRig struct {
	testRetainedRig
	c  net.Conn
	r  *mqttReader
	pi uint16
	ci *mqttV5ConnInfo
}

func testRetainedAbandonUp(t *testing.T, id string, qos byte) *testRetainedAbandonRig {
	t.Helper()
	return testRetainedAbandonUpExpiring(t, id, qos, 60)
}

// testRetainedAbandonUpExpiring is testRetainedAbandonUp with the resumed connection's Session
// Expiry Interval, so a test can end the generation by expiry (a Discard closure).
func testRetainedAbandonUpExpiring(t *testing.T, id string, qos byte, resumeExpiry uint32,
	beforeResume ...func(*testRetainedAbandonRig)) *testRetainedAbandonRig {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	rig := &testRetainedAbandonRig{testRetainedRig: testRetainedRig{s: s, o: o, id: id}}
	rig.cleanup = append(rig.cleanup, func() { testMQTTShutdownServer(s) })
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	rig.cleanup = append(rig.cleanup, func() { cp.Close() })
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, qos, true, 1, "ab/big", []byte(strings.Repeat("x", 200)), nil)

	rig.ci = &mqttV5ConnInfo{clientID: id, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
	c, r := testMQTTConnectV5(t, rig.ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "ab/big", opts: qos}})
	gotQoS, pi := testMQTTReadPublishV5(t, r, "ab/big", []byte(strings.Repeat("x", 200)))
	if gotQoS != qos {
		t.Fatalf("the first retained send went out at QoS %d, want %d", gotQoS, qos)
	}
	rig.pi = pi
	c.Close()

	if len(beforeResume) > 0 {
		rig.asm = testMQTTGetAccountSessionManager(t, s, id)
		rig.epoch = rig.asm.sessStreamEpoch
		for _, f := range beforeResume {
			f(rig)
		}
	}
	// RESUME with a Maximum Packet Size the 200-byte replay does not fit.
	rig.ci.cleanStart = false
	rig.ci.props = append(mqttV5ConnPropsMaxPacketSize(64), mqttV5ConnPropsSessionExpiry(resumeExpiry)...)
	rig.c, rig.r = testMQTTConnectV5(t, rig.ci, o.MQTT.Host, o.MQTT.Port)
	rig.cleanup = append(rig.cleanup, func() { rig.c.Close() })
	if present, rc, _ := testMQTTReadConnAckV5(t, rig.r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the resume must be accepted with the session present, rc=%x present=%v", rc, present)
	}
	rig.attach(t)
	return rig
}

// abandoned waits for the retained register and exchange to read as the retained arm of the
// abandonment, and returns them with the copy's incarnation.
func (rig *testRetainedAbandonRig) abandoned(t *testing.T) (*mqttInflightRecord, *mqttOutboundExchange) {
	t.Helper()
	subject := mqttInflightSubject(rig.idHash, rig.gen, rig.pi)
	deadline := time.Now().Add(10 * time.Second)
	for {
		reg, _, _ := rig.asm.loadInflightRecord(subject)
		var x *mqttOutboundExchange
		if reg != nil {
			x, _, _ = rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, reg.Lin)
		}
		if reg != nil && reg.State == mqttInflightDeliveryAbandoned && x != nil &&
			x.Phase == mqttExchangeDeliveryAbandoned {
			return reg, x
		}
		if time.Now().After(deadline) {
			t.Fatalf("the unservable replay was not abandoned: register %+v, exchange %+v", reg, x)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// assertRetainedArm checks the durable fact is the retained arm, bound to this register's copy,
// with no source coordinate — so no source settlement was possible.
func assertRetainedArm(t *testing.T, reg *mqttInflightRecord, x *mqttOutboundExchange, reason string) {
	t.Helper()
	f := x.Abandoned
	if f == nil || !f.retained() || f.Payload != reg.Payload || f.Reason != reason || reg.AbandonReason != reason ||
		f.Dur != _EMPTY_ || f.Seq != 0 || f.Reply != _EMPTY_ || f.Alloc != reg.Nonce || f.Acq != reg.Acq {
		t.Fatalf("the abandonment fact is %+v for register %+v; want the retained arm, reason %q", f, reg, reason)
	}
}

// awaitRetainedCopyReclaimed waits for the copy's material to be gone and its owner complete.
func (rig *testRetainedAbandonRig) awaitRetainedCopyReclaimed(t *testing.T, payload string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, gone := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(rig.idHash, rig.gen, payload))
		if gone != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the abandoned copy %q was not reclaimed", payload)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// chargeOf reports whether the ledger holds the identifier, and whether its charge is released.
func (rig *testRetainedAbandonRig) chargeOf(lin string) (held, released bool) {
	rig.sess.mu.Lock()
	defer rig.sess.mu.Unlock()
	if rig.sess.custody == nil {
		return false, false
	}
	e := rig.sess.custody.heldExchanges[lin]
	return rig.sess.custody.holdsPI(rig.pi), e != nil && e.chargeReleased
}

// TestF105RetainedReplayTooLargeIsAbandonedQoS1: a possibly-sent retained QoS 1 replay that no longer
// fits is never sent, is abandoned with the retained arm (packet-too-large), its charge released and
// its identifier held, its copy reclaimed; a late PUBACK then completes the exchange and frees the
// identifier. The late PUBACK settles no source: it installs no source owner, holds no identifier
// past the exchange's ending, and the abandonment's own binding goes with the ending.
func TestF105RetainedReplayTooLargeIsAbandonedQoS1(t *testing.T) {
	var sourceless *atomic.Int64
	rig := testRetainedAbandonUpExpiring(t, "ret-ab-q1", 1, 60, func(rig *testRetainedAbandonRig) {
		sourceless = testAnswerSourcelessAsUnknown(t, rig.asm)
	})
	defer rig.done()
	before := mqttRetainedAtOrdinarySink.Load()
	reg, x := rig.abandoned(t)
	assertRetainedArm(t, reg, x, mqttAbandonReasonPacketTooLarge)
	testMQTTExpectNothing(t, rig.r)
	rig.awaitRetainedCopyReclaimed(t, reg.Payload)
	if held, released := rig.chargeOf(reg.Lin); !held || !released {
		t.Fatalf("after the abandonment: identifier held %v (want true), charge released %v (want true)", held, released)
	}
	rig.sess.mu.Lock()
	_, entry := rig.sess.pendingPublish[rig.pi]
	rig.sess.mu.Unlock()
	if entry {
		t.Fatal("the abandoned exchange kept a scheduling entry")
	}
	rig.sess.mu.Lock()
	idx := mqttAbandonedAckIndex{gen: rig.gen, pi: rig.pi}
	binding := rig.sess.abandonedAcks[idx]
	rig.sess.mu.Unlock()
	if binding == nil {
		t.Fatal("the abandoned retained exchange installed no ACK binding")
	}

	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.c, rig.pi)
	deadline := time.Now().Add(10 * time.Second)
	for {
		x, _, _ = rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, reg.Lin)
		held, _ := rig.chargeOf(reg.Lin)
		if x != nil && x.Phase == mqttExchangeTerminal && !held {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a late PUBACK did not complete the abandoned exchange: %+v, identifier held %v", x, held)
		}
		time.Sleep(50 * time.Millisecond)
	}
	testAwaitNoSourceOwner(t, rig.sess, mqttInflightSubject(rig.idHash, rig.gen, rig.pi), rig.pi)
	deadline = time.Now().Add(10 * time.Second)
	for {
		rig.sess.mu.Lock()
		still := rig.sess.abandonedAcks[idx] == binding
		rig.sess.mu.Unlock()
		if !still {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the abandonment's ACK binding outlived the exchange's ending")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := sourceless.Load(); n != 0 {
		t.Fatalf("%d source classification(s) were asked with no source coordinates", n)
	}
	if n := mqttRetainedAtOrdinarySink.Load() - before; n != 0 {
		t.Fatalf("%d sourceless input(s) reached an ordinary-source sink", n)
	}
}

// TestF105RetainedReplayTooLargeIsAbandonedQoS2: the QoS 2 variant, then a late PUBREC after the copy
// is reclaimed advances through the material-free abandoned arrow — the canonical PUBREL created
// without the manifest — the PUBREL is emitted, and PUBCOMP removes the canonical and the register.
func TestF105RetainedReplayTooLargeIsAbandonedQoS2(t *testing.T) {
	rig := testRetainedAbandonUp(t, "ret-ab-q2", 2)
	defer rig.done()
	reg, x := rig.abandoned(t)
	assertRetainedArm(t, reg, x, mqttAbandonReasonPacketTooLarge)
	testMQTTExpectNothing(t, rig.r)
	rig.awaitRetainedCopyReclaimed(t, reg.Payload)

	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, rig.pi)
	subject := mqttInflightSubject(rig.idHash, rig.gen, rig.pi)
	after, _, _ := rig.asm.loadInflightRecord(subject)
	if after == nil || after.State != mqttInflightPubRelReady || after.RelSubj == _EMPTY_ {
		t.Fatalf("the abandoned register after the late PUBREC is %+v", after)
	}
	if _, released := rig.chargeOf(reg.Lin); !released {
		t.Fatal("a late PUBREC re-charged an abandoned exchange")
	}

	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, rig.pi)
	deadline := time.Now().Add(10 * time.Second)
	for {
		x, _, _ = rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, reg.Lin)
		cur, _, _ := rig.asm.loadInflightRecord(subject)
		_, relGone := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, after.RelSubj)
		if x != nil && x.Phase == mqttExchangeTerminal && cur == nil && relGone != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after PUBCOMP: exchange %+v, register %+v, rel gone %v", x, cur, relGone != nil)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestF105RetainedAbandonBoundaryRefusesForgedBatches: direct submissions of a retained abandonment
// against a live retained register. The unmutated batch COMMITS — the control without which every
// refusal below could be a composer that never builds a valid batch — and each forgery is refused.
func TestF105RetainedAbandonBoundaryRefusesForgedBatches(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		mutate     func(reg *mqttInflightRecord, x *mqttOutboundExchange) (regBody, xBody bool)
	}{
		{name: "control"},
		{name: "retained fact carrying dur", want: "a retained delivery was never a source delivery",
			mutate: func(_ *mqttInflightRecord, x *mqttOutboundExchange) (bool, bool) {
				x.Abandoned.Dur, x.Abandoned.Seq = "SRC", 7
				return false, true
			}},
		{name: "fact naming another copy", want: "without the abandonment of its obligation",
			mutate: func(_ *mqttInflightRecord, x *mqttOutboundExchange) (bool, bool) {
				x.Abandoned.Payload = mqttNewInflightNonce()
				return false, true
			}},
		{name: "ordinary-arm fact on a retained register", want: "without the abandonment of its obligation",
			mutate: func(_ *mqttInflightRecord, x *mqttOutboundExchange) (bool, bool) {
				x.Abandoned.Payload = _EMPTY_
				x.Abandoned.Dur, x.Abandoned.Seq = "SRC", 7
				x.Abandoned.Reply = testInflightAckSubject("SRC", 7, 1)
				x.Abandoned.Reason = mqttAbandonReasonACLDeny
				return false, true
			}},
		{name: "register copy rewritten", want: "changes more than its disposition",
			mutate: func(reg *mqttInflightRecord, _ *mqttOutboundExchange) (bool, bool) {
				reg.Digest = strings.Repeat("0", len(reg.Digest))
				return true, false
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := testRetainedRigUp(t, "ret-ab-forged-"+strings.ReplaceAll(tc.name, " ", "-"), "rt/f", 1, []byte("forged"))
			defer r.done()
			sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
			defer sub.Close()
			testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/f", qos: 1}}, []byte{1})
			pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/f", mqttPubQos1|mqttPubFlagRetain, []byte("forged"))
			r.attach(t)
			subject := mqttInflightSubject(r.idHash, r.gen, pi)
			jsa := &r.asm.jsa
			var reg *mqttInflightRecord
			var regSeq uint64
			deadline := time.Now().Add(5 * time.Second)
			for {
				cur, smsg, _ := r.asm.loadInflightRecord(subject)
				if cur != nil && cur.ActionToken == _EMPTY_ && cur.State == mqttInflightPublish {
					reg, regSeq = cur, smsg.Sequence
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the retained register never settled tokenless at publish: %+v", cur)
				}
				time.Sleep(20 * time.Millisecond)
			}
			auth, authSeq, err := jsa.servingAuthority(r.id, r.epoch)
			if err != nil {
				t.Fatalf("no serving authority: %v", err)
			}
			x, xSeq, _ := jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
			ev := mqttDenyEvidence{Gen: auth.Gen, Tenure: auth.Tenure, Subject: subject, Nonce: reg.Nonce, Lin: reg.Lin,
				PI: reg.PI, Reason: mqttAbandonReasonPacketTooLarge, Payload: reg.Payload}
			txn := mqttNewInflightNonce()
			tr, terr := mqttAbandonDeliveryTransition(auth, authSeq, txn, "op-"+txn, ev, reg, regSeq, x, xSeq)
			if terr != nil {
				t.Fatalf("composing: %v", terr)
			}
			if tc.mutate != nil {
				var next mqttInflightRecord
				if err := json.Unmarshal(tr.Extra[0].body, &next); err != nil {
					t.Fatal(err)
				}
				var nx mqttOutboundExchange
				if err := json.Unmarshal(tr.Extra[1].body, &nx); err != nil {
					t.Fatal(err)
				}
				rb, xb := tc.mutate(&next, &nx)
				if rb {
					tr.Extra[0].body, _ = json.Marshal(&next)
				}
				if xb {
					tr.Extra[1].body, _ = json.Marshal(&nx)
				}
			}
			_, cerr := jsa.commitAuthorityTransition(tr)
			if tc.mutate == nil {
				if cerr != nil {
					t.Fatalf("the unmutated retained abandonment was refused: %v", cerr)
				}
				return
			}
			if cerr == nil || !strings.Contains(cerr.Error(), tc.want) {
				t.Fatalf("the forged batch was answered %v, want a refusal naming %q", cerr, tc.want)
			}
		})
	}
}

// TestF105RetainedAbandonedSurvivesARestart: after the abandonment and the copy's reclamation, a
// broker restart restores the ACK-only identity — no PUBLISH replay, the identifier still held — and
// a late PUBACK (QoS 1) or a late PUBREC then PUBCOMP (QoS 2) reaches the right allocation and
// completes it with no material.
func TestF105RetainedAbandonedSurvivesARestart(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(map[byte]string{1: "qos1", 2: "qos2"}[qos], func(t *testing.T) {
			rig := testRetainedAbandonUp(t, "ret-ab-restart-"+string('0'+qos), qos)
			defer func() { rig.done() }()
			reg, _ := rig.abandoned(t)
			rig.awaitRetainedCopyReclaimed(t, reg.Payload)

			dir := strings.TrimSuffix(rig.s.JetStreamConfig().StoreDir, JetStreamStoreDir)
			rig.s.Shutdown()
			rig.c.Close()
			rig.o.Port, rig.o.MQTT.Port, rig.o.StoreDir = -1, -1, dir
			rig.s = testMQTTRunServer(t, rig.o)
			rig.cleanup = append(rig.cleanup, func() { testMQTTShutdownServer(rig.s) })
			rig.c, rig.r = testMQTTConnectV5(t, rig.ci, rig.o.MQTT.Host, rig.o.MQTT.Port)
			if present, rc, _ := testMQTTReadConnAckV5(t, rig.r); rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("the resume after the restart: rc=%x present=%v", rc, present)
			}
			rig.attach(t)
			testMQTTExpectNothing(t, rig.r)
			if held, released := rig.chargeOf(reg.Lin); !held || !released {
				t.Fatalf("after the restart: identifier held %v (want true), charge released %v (want true)", held, released)
			}

			subject := mqttInflightSubject(rig.idHash, rig.gen, rig.pi)
			if qos == 1 {
				testMQTTSendPIPacket(mqttPacketPubAck, t, rig.c, rig.pi)
			} else {
				testMQTTSendPIPacket(mqttPacketPubRec, t, rig.c, rig.pi)
				testMQTTReadPIPacket(mqttPacketPubRel, t, rig.r, rig.pi)
				testMQTTSendPIPacket(mqttPacketPubComp, t, rig.c, rig.pi)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, reg.Lin)
				cur, _, _ := rig.asm.loadInflightRecord(subject)
				if x != nil && x.Phase == mqttExchangeTerminal && cur == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the late answer after the restart: exchange %+v, register %+v", x, cur)
				}
				time.Sleep(50 * time.Millisecond)
			}
		})
	}
}

// TestF105RetainedUnservableProvenUnsentIsWithdrawn: an admitted retained delivery whose first send
// never happened (the broker stopped between the admission and the transmission, so the unsent
// marker stands) is restored after a restart with a Maximum Packet Size it no longer fits. It is
// WITHDRAWN, not abandoned: nothing is sent, the exchange reads WithdrawnUnsent with no
// abandonment fact, the identifier is released with the register, and the copy is reclaimed.
func TestF105RetainedUnservableProvenUnsentIsWithdrawn(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer func() { testMQTTShutdownServer(s) }()
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-wd-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, true, 1, "wd/big", []byte(strings.Repeat("x", 200)), nil)
	cp.Close()

	ci := &mqttV5ConnInfo{clientID: "ret-wd", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r)
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	admitted := make(chan uint16, 1)
	hold := make(chan struct{})
	rig.sess.mu.Lock()
	rig.sess.testRetainedAfterAdmit = func(pi uint16) {
		admitted <- pi
		<-hold
	}
	rig.sess.mu.Unlock()
	defer close(hold)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "wd/big", opts: 1}})
	var pi uint16
	select {
	case pi = <-admitted:
	case <-time.After(10 * time.Second):
		t.Fatal("the retained delivery was never admitted")
	}
	subject := mqttInflightSubject(rig.idHash, rig.gen, pi)
	reg, _, err := rig.asm.loadInflightRecord(subject)
	if err != nil || reg == nil {
		t.Fatalf("no admitted retained register: %v", err)
	}
	if !mqttMarkerNamesAllocation(rig.asm.jsa.loadOccupantMarkerLoad(subject), reg.Nonce) {
		t.Fatal("the admitted retained delivery has no unsent marker: the fixture does not reach the proven-unsent arm")
	}

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()
	o.Port, o.MQTT.Port, o.StoreDir = -1, -1, dir
	s = testMQTTRunServer(t, o)
	ci.cleanStart = false
	ci.props = append(mqttV5ConnPropsMaxPacketSize(64), mqttV5ConnPropsSessionExpiry(60)...)
	checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
	c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
		t.Fatalf("the resume after the restart: rc=%x present=%v", rc, present)
	}
	rig.s = s
	rig.attach(t)
	deadline := time.Now().Add(10 * time.Second)
	for {
		x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, reg.Lin)
		cur, _, _ := rig.asm.loadInflightRecord(subject)
		_, manifestGone := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(rig.idHash, rig.gen, reg.Payload))
		rig.sess.mu.Lock()
		held := rig.sess.custody != nil && rig.sess.custody.holdsPI(pi)
		_, entry := rig.sess.pendingPublish[pi]
		rig.sess.mu.Unlock()
		if x != nil && x.Phase == mqttExchangeWithdrawnUnsent && x.Abandoned == nil && cur == nil && !held && !entry &&
			manifestGone != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the proven-unsent replay: exchange %+v, register %+v, identifier held %v, entry %v, copy gone %v",
				x, cur, held, entry, manifestGone != nil)
		}
		time.Sleep(50 * time.Millisecond)
	}
	testMQTTExpectNothing(t, r)
}

// T24(e): after its generation is DISCARDED, an abandoned retained exchange takes no new canonical
// PUBREL. The material-free abandoned arrow is fenced by a closure read at the boundary, and that
// read is itself fenced by the authority CAS (consult 103 2c): (a) a Discard standing before the
// advance is refused by the read; (b) a Discard landing AFTER the read — in the presend window —
// fails the advance's authority CAS atomically. Either way no `rel` is created. An advance may name
// an earlier generation (mqttKindEndsEarlierGeneration), so the route is real; the Discard is the
// production discarding close (opCloseTeardownGenerationWith), applied in the window a teardown
// leaves between its closure and its register reap. (A clean start over a QoS 2 session cannot
// reach it: F-113.)
func TestF105RetainedAbandonedArrowRefusedAfterDiscard(t *testing.T) {
	for _, order := range []string{"discard-before-read", "discard-in-presend"} {
		t.Run(order, func(t *testing.T) {
			rig := testRetainedAbandonUp(t, "ret-ab-discard-"+order, 2)
			defer rig.done()
			reg0, _ := rig.abandoned(t)
			gen1 := rig.gen
			subject := mqttInflightSubject(rig.idHash, gen1, rig.pi)
			jsa := &rig.asm.jsa
			discard := func() {
				// Re-attempted from a fresh read: the session's own background writes (the copy's
				// reclamation after the abandonment) may still be landing on the authority.
				var cerr error
				for attempt := 0; attempt < 20; attempt++ {
					prev, prevSeq, err := jsa.requireAuthority(rig.id, rig.epoch)
					if err != nil || prev == nil {
						t.Errorf("no authority to discard: %v", err)
						return
					}
					rig.sess.mu.Lock()
					wid, seq := rig.sess.wid, rig.sess.seq
					rig.sess.mu.Unlock()
					txn := mqttNewInflightNonce()
					if _, cerr = jsa.opCloseTeardownGenerationWith(rig.id, rig.epoch, txn, gen1, prev, prevSeq,
						&mqttRetirementRequest{Gen: gen1, Tenure: prev.Tenure, OwnerTxn: prev.OwnerTxn, SessWID: wid,
							SessSeq: seq, Reason: mqttRetireSessionEnd, Discard: true}); cerr == nil {
						return
					}
					time.Sleep(20 * time.Millisecond)
				}
				t.Errorf("the discarding close: %v", cerr)
			}
			if order == "discard-before-read" {
				discard()
			}
			reg, smsg, err := rig.asm.loadInflightRecord(subject)
			if err != nil || smsg == nil || reg == nil || reg.State != mqttInflightDeliveryAbandoned || reg.Nonce != reg0.Nonce {
				t.Fatalf("the fixture does not reach the arrow: the abandoned register is %+v (%v)", reg, err)
			}
			x, xSeq, xerr := jsa.loadOutboundExchange(rig.id, rig.epoch, gen1, reg.Lin)
			if xerr != nil || x == nil || x.Phase != mqttExchangeDeliveryAbandoned {
				t.Fatalf("the fixture does not reach the arrow: the exchange is %+v (%v)", x, xerr)
			}
			prev, prevSeq, aerr := jsa.requireAuthority(rig.id, rig.epoch)
			if aerr != nil || prev == nil {
				t.Fatalf("no authority: %v", aerr)
			}
			txn := mqttNewInflightNonce()
			req := &mqttAdvanceExchangeAckRequest{ID: rig.id, Epoch: rig.epoch, Txn: txn, Op: "op-" + txn, Gen: gen1,
				Nonce: reg.Lin, To: mqttExchangePubRecObserved, Acq: reg.Acq, Register: reg, RegisterSeq: smsg.Sequence,
				RelID: mqttNewInflightNonce()}
			tr, terr := mqttAdvanceExchangeAckTransition(prev, prevSeq, req, x, xSeq)
			if terr != nil {
				t.Fatalf("composing the late PUBREC advance: %v", terr)
			}
			want := "a discarded generation takes no new canonical PUBREL"
			if order == "discard-in-presend" {
				// The advance's boundary read sees no closure; the Discard lands before its batch.
				var fired atomic.Bool
				hook := func(at *mqttAuthorityTransition) {
					if at.Attempt != nil && at.Attempt.Txn == txn && fired.CompareAndSwap(false, true) {
						discard()
					}
				}
				jsa.testTransitionPresend.Store(&hook)
				defer jsa.testTransitionPresend.Store(nil)
				want = _EMPTY_
			}
			_, cerr := jsa.commitAuthorityTransition(tr)
			if cerr == nil || (want != _EMPTY_ && !strings.Contains(cerr.Error(), want)) {
				t.Fatalf("the late PUBREC advance of a discarded generation was answered %v, want a refusal %q", cerr, want)
			}
			if clo, _, cst := jsa.loadGenClosure(rig.id, rig.epoch, gen1); cst != mqttOutboundValid || clo == nil || !clo.Discard {
				t.Fatalf("the fixture's Discard did not land: %+v (status %v)", clo, cst)
			}
			if _, gone := jsa.loadLastMsgFor(mqttInflightStreamName, mqttRetainedRelSubject(rig.idHash, gen1, reg.Lin)); gone == nil {
				t.Fatal("a canonical PUBREL was created in a discarded generation")
			}
		})
	}
}

// TestF105RetainedAbandonACLDenyThroughTheProducer (consult 103 2d): an acl-deny abandonment of a
// live retained register passes through the real producer (opAbandonDeniedDelivery's retained arm)
// and the boundary, and reads back with its reason and its copy: the register at
// delivery_abandoned with acl-deny, the exchange's fact the retained arm naming the same payload.
// The permission refresh that would produce the decision is outside this coverage; the verdict
// that reads it is TestF105RetainedReplayVerdict.
func TestF105RetainedAbandonACLDenyThroughTheProducer(t *testing.T) {
	r := testRetainedRigUp(t, "ret-ab-acl", "rt/acl", 1, []byte("denied"))
	defer r.done()
	sub, subr := testMQTTConnect(t, &mqttConnInfo{clientID: r.id, cleanSess: false}, r.o.MQTT.Host, r.o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subr, []*mqttFilter{{filter: "rt/acl", qos: 1}}, []byte{1})
	pi := testMQTTCheckPubMsgNoAck(t, sub, subr, "rt/acl", mqttPubQos1|mqttPubFlagRetain, []byte("denied"))
	r.attach(t)
	subject := mqttInflightSubject(r.idHash, r.gen, pi)
	var reg *mqttInflightRecord
	deadline := time.Now().Add(5 * time.Second)
	for {
		cur, _, _ := r.asm.loadInflightRecord(subject)
		if cur != nil && cur.ActionToken == _EMPTY_ && cur.State == mqttInflightPublish {
			reg = cur
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the retained register never settled tokenless at publish: %+v", cur)
		}
		time.Sleep(20 * time.Millisecond)
	}
	jsa := &r.asm.jsa
	auth, _, err := jsa.servingAuthority(r.id, r.epoch)
	if err != nil {
		t.Fatalf("no serving authority: %v", err)
	}
	ev := mqttDenyEvidence{Gen: auth.Gen, Tenure: auth.Tenure, Subject: subject, Nonce: reg.Nonce, Lin: reg.Lin,
		PI: reg.PI, Reason: mqttAbandonReasonACLDeny, Payload: reg.Payload}
	txn := mqttNewInflightNonce()
	if _, oerr := jsa.opAbandonDeniedDelivery(&mqttAbandonDeliveryRequest{ID: r.id, Epoch: r.epoch, Txn: txn,
		Op: "op-" + txn, Evidence: ev, VettedAcq: reg.Acq}); oerr != nil {
		t.Fatalf("the acl-deny retained abandonment was refused: %v", oerr)
	}
	after, _, _ := r.asm.loadInflightRecord(subject)
	x, _, _ := jsa.loadOutboundExchange(r.id, r.epoch, r.gen, reg.Lin)
	if after == nil || after.State != mqttInflightDeliveryAbandoned || x == nil || x.Phase != mqttExchangeDeliveryAbandoned {
		t.Fatalf("after the acl-deny abandonment: register %+v, exchange %+v", after, x)
	}
	assertRetainedArm(t, after, x, mqttAbandonReasonACLDeny)
}

// TestF105RetainedLiveFirstSendUnservableIsWithdrawn (consult 103 2b): a LIVE first send judged
// unservable after its admission — here its frame no longer fits; on the live route the change a
// production connection can actually see is a permission refresh, and the verdict reads both — is
// WITHDRAWN: no PUBLISH, the exchange WithdrawnUnsent with no abandonment fact, the identifier and
// the charge released, the copy reclaimed, and the replay queue goes on to deliver the next item.
// The seam changes the connection's limit while the readLoop is idle, between the admission and
// the transmission, so the snapshot's own size check passed.
func TestF105RetainedLiveFirstSendUnservableIsWithdrawn(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-live-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, true, 1, "lv/big", []byte(strings.Repeat("x", 200)), nil)
	testMQTTPubV5Props(t, cp, rp, 1, true, 2, "lv/small", []byte("ok"), nil)

	ci := &mqttV5ConnInfo{clientID: "ret-live", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
	rig.attach(t)
	type admitted struct {
		pi  uint16
		reg *mqttInflightRecord
	}
	bigCh := make(chan admitted, 1)
	rig.sess.mu.Lock()
	rig.sess.testRetainedAfterAdmit = func(pi uint16) {
		subject := mqttInflightSubject(rig.idHash, rig.gen, pi)
		reg, _, _ := rig.asm.loadInflightRecord(subject)
		if reg == nil {
			return
		}
		m, _, _ := rig.asm.jsa.loadRetainedManifest(rig.id, rig.epoch, rig.gen, reg.Payload)
		if m == nil || m.Topic != "lv/big" {
			return
		}
		rig.sess.mu.Lock()
		cc := rig.sess.c
		rig.sess.mu.Unlock()
		cc.mu.Lock()
		cc.mqtt.maxPacketSize = 64
		cc.mu.Unlock()
		bigCh <- admitted{pi: pi, reg: reg}
	}
	rig.sess.mu.Unlock()
	// Two SUBSCRIBE packets, so the oversized copy's replay object is queued FIRST and the small
	// one's runs only after it was disposed of.
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/big", opts: 1}})
	if rc, sp := testMQTTSubV5PropsEx(t, c, r, 2, nil, []mqttV5SubFilter{{topic: "lv/small", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
		reason := _EMPTY_
		if sp != nil {
			reason = sp.reasonString
		}
		t.Fatalf("the SUBSCRIBE behind a materializing replay was answered %v (%q), want [1]", rc, reason)
	}
	// The queue goes on: the small copy is delivered, and nothing else is.
	testMQTTReadPubV5Props(t, c, r, "lv/small", []byte("ok"))
	var big admitted
	select {
	case big = <-bigCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the fixture never admitted the oversized copy")
	}
	testMQTTExpectNothing(t, r)
	deadline := time.Now().Add(10 * time.Second)
	for {
		x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, big.reg.Lin)
		cur, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, big.pi))
		_, gone := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(rig.idHash, rig.gen, big.reg.Payload))
		rig.sess.mu.Lock()
		held := rig.sess.custody != nil && rig.sess.custody.holdsPI(big.pi)
		_, entry := rig.sess.pendingPublish[big.pi]
		rig.sess.mu.Unlock()
		if x != nil && x.Phase == mqttExchangeWithdrawnUnsent && x.Abandoned == nil && cur == nil && !held && !entry && gone != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the unservable live first send: exchange %+v, register %+v, identifier held %v, entry %v, copy gone %v",
				x, cur, held, entry, gone != nil)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestF105RetainedIndeterminateVerdictIsOwed (consult 103 2e, re-ruled by consult 106): a restored
// retained PUBLISH whose verdict is INDETERMINATE (no usable authorization snapshot) decides
// nothing — no acquisition, no output, no withdrawal or abandonment, the reservation and the
// restored entry kept — and it is OWED, not withheld: an indeterminate verdict proves no
// settlement. (a) Authorization recovering on the same open connection delivers it there. (b) A
// verdict that stays indeterminate past the bound closes the connection, and the usable resume
// replays it. The verdict is forced through the seam; the verdict's own indeterminate arm is
// TestF105RetainedReplayVerdict's.
func TestF105RetainedIndeterminateVerdictIsOwed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		recover bool
	}{{"recovers-on-the-open-connection", true}, {"stays-indeterminate-closes", false}} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptionsV5()
			s := testMQTTRunServer(t, o)
			defer func() { testMQTTShutdownServer(s) }()
			cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-indet-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, rp)
			testMQTTPubV5Props(t, cp, rp, 1, true, 1, "id/t", []byte("held"), nil)
			cp.Close()
			ci := &mqttV5ConnInfo{clientID: "ret-indet", cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)}
			c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			testMQTTReadConnAckV5(t, r)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "id/t", opts: 1}})
			_, pi := testMQTTReadPublishV5(t, r, "id/t", []byte("held"))
			rig := &testRetainedRig{s: s, o: o, id: ci.clientID}
			rig.attach(t)
			subject := mqttInflightSubject(rig.idHash, rig.gen, pi)
			var before *mqttInflightRecord
			deadline := time.Now().Add(5 * time.Second)
			for {
				cur, _, _ := rig.asm.loadInflightRecord(subject)
				if cur != nil && cur.ActionToken == _EMPTY_ {
					before = cur
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("the register never settled tokenless: %+v", cur)
				}
				time.Sleep(20 * time.Millisecond)
			}
			c.Close()

			var judged atomic.Int32
			forced := func(mqttRetainedReplayVerdict, string) (mqttRetainedReplayVerdict, string) {
				judged.Add(1)
				return mqttRetainedReplayIndeterminate, _EMPTY_
			}
			rig.sess.testRetainedVerdict.Store(&forced)
			ci.cleanStart = false
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("the resume: rc=%x present=%v", rc, present)
			}
			testMQTTExpectNothing(t, r)
			cur, _, _ := rig.asm.loadInflightRecord(subject)
			if cur == nil || cur.State != mqttInflightPublish || cur.Acq != before.Acq || cur.ActionToken != _EMPTY_ || cur.Cancelled {
				t.Fatalf("an indeterminate verdict acquired or disposed: register %+v, before %+v", cur, before)
			}
			x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, cur.Lin)
			if x == nil || x.Phase != mqttExchangePublish || x.Abandoned != nil {
				t.Fatalf("an indeterminate verdict ended the exchange: %+v", x)
			}
			rig.sess.mu.Lock()
			p := rig.sess.pendingPublish[pi]
			held := rig.sess.custody != nil && rig.sess.custody.holdsPI(pi)
			rig.sess.mu.Unlock()
			if p == nil || p.kind != mqttPendingRetained || !held {
				t.Fatalf("an indeterminate verdict released the reservation: entry %+v, held %v", p, held)
			}
			testRetainedOwnershipHolds(t, rig, false, 4, 2*time.Second)
			// OWED: the verdict is judged again on the open connection.
			for deadline := time.Now().Add(5 * time.Second); judged.Load() < 2; {
				if time.Now().After(deadline) {
					t.Fatalf("an indeterminate verdict was judged %d time(s): the PUBLISH was not owed", judged.Load())
				}
				time.Sleep(20 * time.Millisecond)
			}
			if tc.recover {
				rig.sess.testRetainedVerdict.Store(nil)
				flags, got, _ := testMQTTReadPubV5Raw(t, r, "id/t", []byte("held"))
				if got != pi || flags&mqttPubFlagDup == 0 {
					t.Fatalf("the recovered verdict delivered pi %d flags %x, want pi %d with DUP", got, flags, pi)
				}
				testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
				return
			}
			c.SetReadDeadline(time.Now().Add(30 * time.Second))
			if _, err := c.Read(make([]byte, 1)); err == nil {
				t.Fatal("a PUBLISH was sent under an indeterminate verdict")
			} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("the connection was never closed: the owed PUBLISH was withheld on it")
			}
			rig.sess.testRetainedVerdict.Store(nil)
			checkClientsCount(t, s, 0) // the previous connection is gone server-side before the reconnect
			c, r = testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if present, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttConnAckRCConnectionAccepted || !present {
				t.Fatalf("the usable resume: rc=%x present=%v", rc, present)
			}
			flags, got, _ := testMQTTReadPubV5Raw(t, r, "id/t", []byte("held"))
			if got != pi || flags&mqttPubFlagDup == 0 {
				t.Fatalf("the usable resume replayed pi %d flags %x, want pi %d with DUP", got, flags, pi)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		})
	}
}

// T13 — CAPACITY. An admitted retained QoS 1 delivery is a LEDGER charge (custody holds its
// identifier and its entry carries the charge), so with Receive Maximum 1 a live QoS 1 publication
// to the same session is withheld until the retained one is acknowledged, then delivered.
func TestF105RetainedAdmissionIsALedgerCharge(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-cap-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	testMQTTPubV5Props(t, cp, rp, 1, true, 1, "cap/ret", []byte("first"), nil)

	props := append(mqttV5ConnPropsReceiveMax(1), mqttV5ConnPropsSessionExpiry(60)...)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "ret-cap", cleanStart: true, props: props}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "cap/#", opts: 1}})
	_, pi := testMQTTReadPublishV5(t, r, "cap/ret", []byte("first"))
	rig := &testRetainedRig{s: s, o: o, id: "ret-cap"}
	rig.attach(t)
	rig.sess.mu.Lock()
	p := rig.sess.pendingPublish[pi]
	charged := rig.sess.custody != nil && rig.sess.custody.holdsPI(pi)
	rig.sess.mu.Unlock()
	if p == nil || p.kind != mqttPendingRetained || !p.custody || !charged {
		t.Fatalf("the admitted retained delivery is not a ledger charge: entry %+v, custody holds %v", p, charged)
	}

	testMQTTPubV5Props(t, cp, rp, 1, false, 2, "cap/live", []byte("second"), nil)
	testMQTTExpectNothing(t, r)
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTReadPubV5Props(t, c, r, "cap/live", []byte("second"))
}

// T14 — DUPLICATE AND NEGATIVE PUBREC. A duplicate PUBREC at pubrel_ready re-emits the PUBREL from
// the canonical; a PUBREC carrying a failure reason code rejects the retained exchange: no PUBREL,
// the register removed, and the copy reclaimed.
func TestF105RetainedPubRecDuplicateAndNegative(t *testing.T) {
	setup := func(t *testing.T, id string) (*testRetainedRig, net.Conn, *mqttReader, uint16, *mqttInflightRecord) {
		o := testMQTTDefaultOptionsV5()
		s := testMQTTRunServer(t, o)
		t.Cleanup(func() { testMQTTShutdownServer(s) })
		cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		t.Cleanup(func() { cp.Close() })
		testMQTTReadConnAckV5(t, rp)
		testMQTTPubV5Props(t, cp, rp, 2, true, 1, "pr/q2", []byte("q2"), nil)
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(60)},
			o.MQTT.Host, o.MQTT.Port)
		t.Cleanup(func() { c.Close() })
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "pr/q2", opts: 2}})
		_, pi := testMQTTReadPublishV5(t, r, "pr/q2", []byte("q2"))
		rig := &testRetainedRig{s: s, o: o, id: id}
		rig.attach(t)
		reg, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, pi))
		if reg == nil || !reg.retainedSource() {
			t.Fatalf("no retained register: %+v", reg)
		}
		return rig, c, r, pi, reg
	}
	t.Run("duplicate", func(t *testing.T) {
		_, c, r, pi, _ := setup(t, "ret-pubrec-dup")
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
		testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
		testMQTTFlush(t, c, nil, r)
	})
	t.Run("negative", func(t *testing.T) {
		rig, c, r, pi, reg := setup(t, "ret-pubrec-neg")
		if _, err := testMQTTWrite(c, []byte{mqttPacketPubRec, 3, byte(pi >> 8), byte(pi), 0x80}); err != nil {
			t.Fatalf("writing the failed PUBREC: %v", err)
		}
		testMQTTExpectNothing(t, r)
		deadline := time.Now().Add(10 * time.Second)
		for {
			x, _, _ := rig.asm.jsa.loadOutboundExchange(rig.id, rig.epoch, rig.gen, reg.Lin)
			cur, _, _ := rig.asm.loadInflightRecord(mqttInflightSubject(rig.idHash, rig.gen, pi))
			_, gone := rig.asm.jsa.loadLastMsgFor(mqttInflightStreamName, mqttManifestSubject(rig.idHash, rig.gen, reg.Payload))
			if x != nil && x.Phase == mqttExchangeRejected && cur == nil && gone != nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("after a failed PUBREC: exchange %+v, register %+v, copy gone %v", x, cur, gone != nil)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

// T21 — SAME-SID REPLACEMENT. The first replay is held at its reservation, then: a same-filter
// re-SUBSCRIBE with Retain Handling 2 inherits the outstanding item, which is delivered exactly
// once; with Retain Handling 0 the replacement adds its own snapshot, delivered after the inherited
// one; an UNSUBSCRIBE then a fresh SUBSCRIBE departs the held item (never delivered) and the fresh
// incarnation's own snapshot is the one copy that arrives.
func TestF105RetainedSameSidReplacementInheritsTheReplay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		unsub   bool
		opts    byte
		publish int
	}{
		{name: "rh2 inherits", opts: 1 | 2<<4, publish: 1},
		{name: "rh0 adds a snapshot", opts: 1, publish: 2},
		{name: "unsubscribe then subscribe", unsub: true, opts: 1 | 1<<4, publish: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig, c, r := testForegroundServer(t, "ret-sid-"+strings.ReplaceAll(tc.name, " ", "-"),
				map[string][]byte{"sid/t": []byte("copy")})
			jsa := &rig.asm.jsa
			parked := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			presend := func(at *mqttAuthorityTransition) {
				if at.Attempt != nil && at.Attempt.ID == rig.id && at.Attempt.Op == "op-retained-reserve" {
					once.Do(func() {
						close(parked)
						<-release
					})
				}
			}
			jsa.testTransitionPresend.Store(&presend)
			defer jsa.testTransitionPresend.Store(nil)
			testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "sid/t", opts: 1}})
			select {
			case <-parked:
			case <-time.After(10 * time.Second):
				t.Fatal("the first replay never reached its reservation")
			}
			if tc.unsub {
				testMQTTUnsubV5(t, c, r, 2, []string{"sid/t"})
			}
			if rc := testMQTTSubV5(t, c, r, 3, []mqttV5SubFilter{{topic: "sid/t", opts: tc.opts}}); len(rc) != 1 || rc[0] != 1 {
				t.Fatalf("the replacing SUBSCRIBE: %v", rc)
			}
			close(release)
			for i := 0; i < tc.publish; i++ {
				flags, pi, _ := testMQTTReadPubV5Raw(t, r, "sid/t", []byte("copy"))
				if flags&mqttPubFlagRetain == 0 {
					t.Fatalf("delivery %d carried flags %x, want RETAIN", i, flags)
				}
				testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
			}
			testMQTTExpectNothing(t, r)
		})
	}
}
