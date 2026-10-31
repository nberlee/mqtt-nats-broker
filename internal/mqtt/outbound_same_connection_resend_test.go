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
	"net"
	"sync"
	"testing"
	"time"
)

// A transmission is not repeated on the connection that may hold it ([MQTT-4.4.0-1]): a JetStream
// re-offer of an unacknowledged PUBLISH or PUBREL is absorbed there, and resent after a reconnect.
//
// Every test here drives the process-wide refusal seam, so none is parallel.

const testResendAckWait = 300 * time.Millisecond

// testResendClient is a bare client with connection identity cid that negotiated proto.
func testResendClient(cid uint64, proto byte) *client {
	return &client{cid: cid, mqtt: &mqtt{proto: proto}}
}

// testResendRig is a v5 subscriber with a persistent session on a broker with a short AckWait, and
// a publisher. refused counts the re-offers refused as resends, per packet type.
type testResendRig struct {
	o      *Options
	s      *Server
	id     string
	qos    byte
	sc     net.Conn
	sr     *mqttReader
	pc     net.Conn
	pr     *mqttReader
	mu     sync.Mutex
	counts map[byte]int
}

func newTestResendRig(t *testing.T, id string, qos byte) *testResendRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = testResendAckWait
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &testResendRig{o: o, s: s, id: id, qos: qos, counts: map[byte]int{}}
	mqttTestSameConnectionResendRefusedHook.Set(func(packet byte, _ uint16) {
		rig.mu.Lock()
		rig.counts[packet]++
		rig.mu.Unlock()
	})
	t.Cleanup(func() { mqttTestSameConnectionResendRefusedHook.Set(nil) })
	rig.connect(t, false)
	testMQTTSubV5(t, rig.sc, rig.sr, 1, []mqttV5SubFilter{{topic: "rs/t", opts: qos}})
	rig.pc, rig.pr = testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.pc.Close() })
	testMQTTCheckConnAck(t, rig.pr, mqttConnAckRCConnectionAccepted, false)
	return rig
}

// connect (re)connects the subscriber with Clean Start 0 and a session that outlives it.
func (rig *testResendRig) connect(t *testing.T, wantPresent bool) {
	t.Helper()
	rig.sc, rig.sr = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: rig.id, props: mqttV5ConnPropsSessionExpiry(300)},
		rig.o.MQTT.Host, rig.o.MQTT.Port)
	sc := rig.sc
	t.Cleanup(func() { sc.Close() })
	present, rc, _ := testMQTTReadConnAckV5(t, rig.sr)
	if rc != mqttConnAckRCConnectionAccepted || present != wantPresent {
		t.Fatalf("CONNACK: reason %x session present %v, want accepted and %v", rc, present, wantPresent)
	}
}

func (rig *testResendRig) refused(packet byte) int {
	rig.mu.Lock()
	defer rig.mu.Unlock()
	return rig.counts[packet]
}

// awaitRefusals waits until n more re-offers of packet were refused than before.
func (rig *testResendRig) awaitRefusals(t *testing.T, packet byte, n int) {
	t.Helper()
	want := rig.refused(packet) + n
	deadline := time.Now().Add(time.Duration(n+10) * testResendAckWait * 4)
	for rig.refused(packet) < want {
		if time.Now().After(deadline) {
			t.Fatalf("%d re-offers of packet %x refused, want %d: no re-offer reached the opener", rig.refused(packet), packet, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// publish publishes payload at the rig's QoS and returns the subscriber's first PUBLISH.
func (rig *testResendRig) publish(t *testing.T, payload string) (flags byte, pi uint16) {
	t.Helper()
	testMQTTPublish(t, rig.pc, rig.pr, rig.qos, false, false, "rs/t", 1, []byte(payload))
	return rig.readPublish(t, payload)
}

func (rig *testResendRig) readPublish(t *testing.T, payload string) (flags byte, pi uint16) {
	t.Helper()
	b, topic, got, pi, _ := testMQTTReadPubV5Packet(t, rig.sr)
	if topic != "rs/t" || string(got) != payload {
		t.Fatalf("PUBLISH %q on %q, want %q on rs/t", got, topic, payload)
	}
	return b & mqttPacketFlagMask, pi
}

// awaitSettled waits until the session tracks nothing in flight.
func (rig *testResendRig) awaitSettled(t *testing.T) {
	t.Helper()
	sess := testMQTTGetClient(t, rig.s, rig.id).mqtt.sess
	deadline := time.Now().Add(10 * time.Second)
	for {
		sess.mu.Lock()
		n := len(sess.pendingPublish) + len(sess.pendingPubRel)
		sess.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d exchanges still tracked after the acknowledgements", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestReofferOnTheSameConnectionIsNotResent: a QoS 1 PUBLISH the v5 client has not acknowledged is
// re-offered at AckWait and not written again; the late PUBACK still completes it.
func TestReofferOnTheSameConnectionIsNotResent(t *testing.T) {
	rig := newTestResendRig(t, "rs-q1", 1)
	flags, pi := rig.publish(t, "m0")
	if flags&mqttPubFlagDup != 0 {
		t.Fatalf("first transmission carries DUP: flags %x", flags)
	}
	rig.awaitRefusals(t, mqttPacketPub, 2)
	testMQTTExpectNothingFor(t, rig.sr, 2*testResendAckWait)
	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// TestQoS2ReofferBeforePubRecIsNotResent: the same for a QoS 2 PUBLISH awaiting PUBREC, and the
// exchange still completes when the client answers.
func TestQoS2ReofferBeforePubRecIsNotResent(t *testing.T) {
	rig := newTestResendRig(t, "rs-q2", 2)
	_, pi := rig.publish(t, "m0")
	rig.awaitRefusals(t, mqttPacketPub, 2)
	testMQTTExpectNothingFor(t, rig.sr, 2*testResendAckWait)
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.sr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// TestReofferAfterReconnectIsResent: the absorbed delivery is resent, DUP set and under the same
// identifier of the same resident entry, as soon as the client reconnects with its session.
func TestReofferAfterReconnectIsResent(t *testing.T) {
	rig := newTestResendRig(t, "rs-reconnect", 1)
	_, pi := rig.publish(t, "m0")
	rig.awaitRefusals(t, mqttPacketPub, 1)
	rig.sc.Close()
	rig.connect(t, true)
	flags, pi2 := rig.readPublish(t, "m0")
	if flags&mqttPubFlagDup == 0 || pi2 != pi {
		t.Fatalf("resend after reconnect: flags %x identifier %d, want DUP and %d", flags, pi2, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// TestPubRelReofferOnTheSameConnectionIsNotResent: a PUBREL whose PUBCOMP has not arrived is
// re-offered at AckWait and not written again; the late PUBCOMP still completes the exchange.
func TestPubRelReofferOnTheSameConnectionIsNotResent(t *testing.T) {
	rig := newTestResendRig(t, "rs-rel", 2)
	_, pi := rig.publish(t, "m0")
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.sr, pi)
	rig.awaitRefusals(t, mqttPacketPubRel, 2)
	testMQTTExpectNothingFor(t, rig.sr, 2*testResendAckWait)
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// TestDuplicatePubRecIsStillAnswered: the client's own duplicate PUBREC is answered by one PUBREL
// on the same connection ([MQTT-4.3.3]), which is not a resend; nothing follows it.
func TestDuplicatePubRecIsStillAnswered(t *testing.T) {
	rig := newTestResendRig(t, "rs-duprec", 2)
	_, pi := rig.publish(t, "m0")
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.sr, pi)
	rig.awaitRefusals(t, mqttPacketPubRel, 1)
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.sr, pi)
	rig.awaitRefusals(t, mqttPacketPubRel, 1)
	testMQTTExpectNothingFor(t, rig.sr, 2*testResendAckWait)
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// TestPubRelAfterReconnectIsResent: the absorbed PUBREL is resent on the reconnected session.
func TestPubRelAfterReconnectIsResent(t *testing.T) {
	rig := newTestResendRig(t, "rs-rel-reconnect", 2)
	_, pi := rig.publish(t, "m0")
	testMQTTSendPIPacket(mqttPacketPubRec, t, rig.sc, pi)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.sr, pi)
	rig.awaitRefusals(t, mqttPacketPubRel, 1)
	rig.sc.Close()
	rig.connect(t, true)
	testMQTTReadPIPacket(mqttPacketPubRel, t, rig.sr, pi)
	testMQTTSendPIPacket(mqttPacketPubComp, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// testResendAbortFirstSection makes the first send section of the next delivery stop at window,
// so nothing is written on its first attempt.
func testResendAbortFirstSection(t *testing.T, window string) {
	t.Helper()
	var once sync.Once
	restore := setMQTTTestMarkerSendWindowHook(func(_ *mqttSession, _ uint16, w string) bool {
		if w != window {
			return true
		}
		aborted := false
		once.Do(func() { aborted = true })
		return !aborted
	})
	t.Cleanup(restore)
}

// TestPreIntentPreventedWriteIsSentOnTheSameConnection: a first attempt stopped before its intent
// wrote nothing, so the re-offer on the same connection is the first transmission (DUP=0).
func TestPreIntentPreventedWriteIsSentOnTheSameConnection(t *testing.T) {
	rig := newTestResendRig(t, "rs-pre-intent", 1)
	testResendAbortFirstSection(t, mqttMarkerSendReserved)
	flags, pi := rig.publish(t, "m0")
	if flags&mqttPubFlagDup != 0 {
		t.Fatalf("the first written attempt carries DUP: flags %x", flags)
	}
	rig.awaitRefusals(t, mqttPacketPub, 1)
	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// TestPostIntentPreventedWriteIsSentOnTheSameConnection: a first attempt stopped after its intent
// is possibly-attempted (DUP=1) but reached no connection, so the re-offer on the same connection
// still writes it.
func TestPostIntentPreventedWriteIsSentOnTheSameConnection(t *testing.T) {
	rig := newTestResendRig(t, "rs-post-intent", 1)
	testResendAbortFirstSection(t, mqttMarkerSendIntent)
	flags, pi := rig.publish(t, "m0")
	if flags&mqttPubFlagDup == 0 {
		t.Fatalf("the written attempt after a confirmed intent carries no DUP: flags %x", flags)
	}
	rig.awaitRefusals(t, mqttPacketPub, 1)
	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	rig.awaitSettled(t)
}

// TestPubRecRollbackHoldsThePublishOnItsConnection: a rolled-back PUBREC reinstates the PUBLISH as
// held by the connection the PUBREC arrived on, even when the write's end never stamped it, so a
// re-offer is not resent there and is sent to any other connection. A 3.1.1 connection keeps the
// retry its §4.4 permits.
func TestPubRecRollbackHoldsThePublishOnItsConnection(t *testing.T) {
	pubrec, other := testResendClient(11, mqttProtoLevel5), testResendClient(12, mqttProtoLevel5)
	legacy := testResendClient(11, mqttProtoLevel) // the same connection identity, at 3.1.1
	const pi = uint16(7)
	// An Affine entry: an acting kind whose reinstatement needs no custody lineage, which this
	// bare session has none of. Unstamped, as a PUBREC that overtook its write's end leaves it.
	removed := &mqttPending{kind: mqttPendingAffine, jsDur: "DUR", sseq: 42, jsAckSubject: "$JS.ACK.x", qos: 2}
	sess := &mqttSession{
		id:             "rollback-holds",
		pendingPublish: map[uint16]*mqttPending{},
		pendingPubRel:  map[uint16]*mqttPending{},
		cpending:       map[string]map[uint64]uint16{},
	}
	sess.mu.Lock()
	sess.retrackPublishLocked(pubrec, pi, removed)
	got := sess.pendingPublish[pi]
	sess.mu.Unlock()
	if got == nil || got.transmittedOn != pubrec.cid {
		t.Fatalf("the reinstated entry is held by %v, want the PUBREC's connection %d", got, pubrec.cid)
	}
	for _, tc := range []struct {
		c    *client
		want bool
	}{{pubrec, false}, {other, true}, {legacy, true}} {
		sess.mu.Lock()
		sess.c = tc.c
		_, ok := sess.beginSendLocked(tc.c, pi, "DUR", 42)
		sess.mu.Unlock()
		if ok != tc.want {
			t.Fatalf("opener for connection %d admitted %v, want %v", tc.c.cid, ok, tc.want)
		}
		if ok {
			sess.mu.Lock()
			got.sendNonce = _EMPTY_
			delete(sess.sendsInFlight, pi)
			sess.mu.Unlock()
		}
	}
}

// TestPredecessorPubRelDebtIsNotTheSuccessors: a PUBREL debt owed to a predecessor connection says
// nothing about what the successor holds; a reconnect sweep must not be what makes this true.
func TestPredecessorPubRelDebtIsNotTheSuccessors(t *testing.T) {
	pred, succ := testResendClient(3, mqttProtoLevel5), testResendClient(4, mqttProtoLevel5)
	const pi = uint16(9)
	sess := &mqttSession{sendsInFlight: map[uint16]*mqttSendHold{pi: {owner: pred.cid, pubCompOwed: 1}}}
	sess.c = succ
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.pubRelHeldOnLocked(succ, pi) != nil {
		t.Fatal("the successor is judged to hold its predecessor's PUBREL")
	}
	if resend, _ := sess.pubRelResendLocked(succ, pi); resend {
		t.Fatal("the successor's re-offer is refused as a resend")
	}
	if sess.pubRelHeldOnLocked(pred, pi) == nil {
		t.Fatal("the predecessor's own debt is not held by it")
	}
}

// TestUnemittedAnswerIsOwedAgain: the PUBREL owed for a duplicate PUBREC is consumed by its
// emission, not by its admission; an answer requested during the section survives its end; and the
// PUBCOMP that ends the debt ends the answer too.
func TestUnemittedAnswerIsOwedAgain(t *testing.T) {
	cc := testResendClient(21, mqttProtoLevel5)
	const pi = uint16(5)
	open := func(sess *mqttSession) string {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		resend, answer := sess.pubRelResendLocked(cc, pi)
		if resend || !answer {
			t.Fatalf("an owed answer judged resend=%v answer=%v", resend, answer)
		}
		h := sess.sendsInFlight[pi]
		h.token = "attempt"
		h.pubCompOwed++
		sess.bindPubRelAnswerLocked(cc, pi)
		return h.token
	}
	newSess := func() *mqttSession {
		sess := &mqttSession{sendsInFlight: map[uint16]*mqttSendHold{pi: {owner: cc.cid, pubCompOwed: 1}}}
		sess.c = cc
		sess.mu.Lock()
		sess.owePubRelAnswerLocked(cc, pi)
		sess.mu.Unlock()
		return sess
	}

	sess := newSess()
	tok := open(sess)
	sess.endPubRelSend(cc, pi, tok, false)
	sess.mu.Lock()
	if resend, answer := sess.pubRelResendLocked(cc, pi); resend || !answer {
		t.Fatalf("after an unemitted answer: resend=%v answer=%v, want the answer owed again", resend, answer)
	}
	sess.mu.Unlock()

	sess = newSess()
	tok = open(sess)
	sess.endPubRelSend(cc, pi, tok, true)
	sess.mu.Lock()
	if resend, _ := sess.pubRelResendLocked(cc, pi); !resend {
		t.Fatal("after an emitted answer a further re-offer is not refused")
	}
	sess.mu.Unlock()

	sess = newSess()
	tok = open(sess)
	sess.mu.Lock()
	sess.owePubRelAnswerLocked(cc, pi) // a duplicate PUBREC during the open section
	sess.mu.Unlock()
	sess.endPubRelSend(cc, pi, tok, true)
	sess.mu.Lock()
	if _, answer := sess.pubRelResendLocked(cc, pi); !answer {
		t.Fatal("an answer requested during the section was lost at its end")
	}
	sess.consumePubRelAckLocked(cc, pi)
	sess.consumePubRelAckLocked(cc, pi)
	if h := sess.sendsInFlight[pi]; h != nil && h.answerOwed {
		t.Fatal("the answer outlived the PUBCOMP that ended the debt")
	}
	sess.mu.Unlock()
}
