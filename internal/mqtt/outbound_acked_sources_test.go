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
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// A re-offer of a source this session already acknowledged is not delivered again: under a backlog
// queued longer than AckWait, JetStream re-offers messages whose first offer is still queued, and
// each would otherwise go out a second time under a fresh identifier with DUP=0.

// TestAckedSourceSetBounds pins the set's keying and its bounds.
func TestAckedSourceSetBounds(t *testing.T) {
	var a mqttAckedSources
	// KEYED BY DURABLE AND SEQUENCE.
	a.record("A", 5)
	if !a.has("A", 5) {
		t.Fatalf("(A, 5) was not recorded")
	}
	if a.has("B", 5) {
		t.Fatalf("(B, 5) reads recorded after only (A, 5) was: the key is not the durable's")
	}
	// A DURABLE IS EVICTED WHOLE, oldest first, and another durable's entries survive it.
	a.record("B", 5)
	for i := 0; i < mqttAckedSourcesDurables-1; i++ {
		a.record(fmt.Sprintf("D%d", i), 1)
	}
	if a.has("A", 5) {
		t.Fatalf("(A, 5) survived the durable cap: the oldest durable was not evicted")
	}
	if !a.has("B", 5) {
		t.Fatalf("(B, 5) was evicted with A")
	}
	if len(a.durs) != mqttAckedSourcesDurables || len(a.order) != mqttAckedSourcesDurables {
		t.Fatalf("%d durables (%d in order) remembered, want %d", len(a.durs), len(a.order), mqttAckedSourcesDurables)
	}
	// SEQUENCES PER DURABLE ARE BOUNDED, oldest first, and a repeat takes no second slot.
	var b mqttAckedSources
	for seq := uint64(1); seq <= mqttAckedSourcesPerDurable+1; seq++ {
		b.record("S", seq)
	}
	d := b.durs["S"]
	if b.has("S", 1) || !b.has("S", mqttAckedSourcesPerDurable+1) {
		t.Fatalf("the oldest sequence was not the one evicted")
	}
	if len(d.seqs) != mqttAckedSourcesPerDurable || len(d.fifo) != mqttAckedSourcesPerDurable {
		t.Fatalf("%d sequences (%d in order) remembered, want %d", len(d.seqs), len(d.fifo), mqttAckedSourcesPerDurable)
	}
	var c mqttAckedSources
	c.record("S", 7)
	c.record("S", 7)
	if n := len(c.durs["S"].fifo); n != 1 {
		t.Fatalf("recording a remembered sequence again took another slot (%d)", n)
	}
}

// testAckedReofferRig is a broker with a short AckWait, a persistent QoS 1 subscriber on "ar/t"
// and a publisher.
type testAckedReofferRig struct {
	s       *Server
	sess    *mqttSession
	sc, pc  net.Conn
	sr, pr  *mqttReader
	mu      sync.Mutex
	decline []uint64
	acks    []string
}

func newTestAckedReofferRig(t *testing.T, id string, ackWait time.Duration) *testAckedReofferRig {
	t.Helper()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = ackWait
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &testAckedReofferRig{s: s}
	rig.sc, rig.sr = testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.sc.Close() })
	testMQTTCheckConnAck(t, rig.sr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, rig.sc, rig.sr, []*mqttFilter{{filter: "ar/t", qos: 1}}, []byte{1})
	testMQTTFlush(t, rig.sc, nil, rig.sr)
	rig.pc, rig.pr = testMQTTConnect(t, &mqttConnInfo{clientID: id + "-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.pc.Close() })
	testMQTTCheckConnAck(t, rig.pr, mqttConnAckRCConnectionAccepted, false)
	rig.sess = testMQTTGetClient(t, s, id).mqtt.sess
	mqttTestAckedReofferDeclinedHook.Set(func(_ string, seq uint64) {
		rig.mu.Lock()
		rig.decline = append(rig.decline, seq)
		rig.mu.Unlock()
	})
	obs := func(reply string, positive bool) {
		if positive {
			rig.mu.Lock()
			rig.acks = append(rig.acks, reply)
			rig.mu.Unlock()
		}
	}
	mqttTestSourceAckObserver.Store(&obs)
	t.Cleanup(func() {
		mqttTestAckedReofferDeclinedHook.Set(nil)
		mqttTestSourceAckObserver.Store(nil)
	})
	return rig
}

func (rig *testAckedReofferRig) declined() []uint64 {
	rig.mu.Lock()
	defer rig.mu.Unlock()
	return append([]uint64(nil), rig.decline...)
}

// reAcked reports whether a positive acknowledgement of stream sequence seq was submitted on a reply
// whose delivered count is above 1: the re-offer's own reply.
func (rig *testAckedReofferRig) reAcked(seq uint64) bool {
	rig.mu.Lock()
	defer rig.mu.Unlock()
	for _, r := range rig.acks {
		if sseq, _, dc, _, _ := ackReplyInfo(r); sseq == seq && dc > 1 {
			return true
		}
	}
	return false
}

// consumer reads the subscriber's source consumer: delivered count and acknowledgement floor.
func (rig *testAckedReofferRig) consumer(t *testing.T) (delivered, ackFloor uint64, ackPending int) {
	t.Helper()
	nc, js := jsClientConnect(t, rig.s)
	defer nc.Close()
	for ci := range js.ConsumersInfo(mqttStreamName) {
		if strings.HasPrefix(ci.Name, rig.sess.idHash+"_") {
			return ci.Delivered.Consumer, ci.AckFloor.Stream, ci.NumAckPending
		}
	}
	t.Fatalf("the subscriber's source consumer was not found")
	return 0, 0, 0
}

// readInOrder reads n PUBLISHes and PUBACKs each, failing on any payload out of order, and then
// asserts no further PUBLISH arrives within quiet.
func (rig *testAckedReofferRig) readInOrder(t *testing.T, n int, quiet time.Duration) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, pi, _, payload := testMQTTGetPubMsgEx(t, rig.sc, rig.sr, "ar/t", nil)
		if want := fmt.Sprintf("m%d", i); string(payload) != want {
			t.Fatalf("delivery %d carried %q, want %q", i, payload, want)
		}
		testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
	}
	if b, _, ok := testMQTTReadPacketReady(rig.sr, quiet); ok {
		t.Fatalf("a further packet %x arrived after the %d unique deliveries", b, n)
	}
}

// TestQueuedReofferOfAcknowledgedSourceIsDeclined: a backlog queued longer than AckWait is
// re-offered while its first offers are still queued; the client receives every message once, in
// order, and each declined re-offer's own reply is acknowledged.
func TestQueuedReofferOfAcknowledgedSourceIsDeclined(t *testing.T) {
	rig := newTestAckedReofferRig(t, "ar-backlog", 500*time.Millisecond)
	const n = 30
	for i := 0; i < n; i++ {
		testMQTTPublish(t, rig.pc, rig.pr, 1, false, false, "ar/t", uint16(i+1), []byte(fmt.Sprintf("m%d", i)))
	}
	rig.readInOrder(t, n, 3*time.Second)
	delivered, floor, _ := rig.consumer(t)
	if delivered <= n {
		t.Fatalf("fixture: JetStream re-offered nothing (%d deliveries of %d messages): the backlog "+
			"did not outlast AckWait", delivered, n)
	}
	if floor != n {
		t.Fatalf("the source consumer's floor is %d after %d messages were acknowledged", floor, n)
	}
	d := rig.declined()
	if len(d) == 0 {
		t.Fatalf("JetStream re-offered %d time(s) and no re-offer was declined", delivered-n)
	}
	for _, seq := range d {
		if !rig.reAcked(seq) {
			t.Fatalf("the declined re-offer of sequence %d was not acknowledged on its own reply", seq)
		}
	}
}

// TestFailedSourceAckConfirmationStaysRecorded: a confirmed acknowledgement that is lost still
// leaves its submission recorded — the record states the session's decision about the source, not
// the server's receipt. (On the ordinary completion route the terminal-source suppression already
// covers a lost confirmation, so this is pinned at the gate.)
func TestFailedSourceAckConfirmationStaysRecorded(t *testing.T) {
	rig := newTestAckedReofferRig(t, "ar-lost-ack", 500*time.Millisecond)
	testMQTTPublish(t, rig.pc, rig.pr, 1, false, false, "ar/t", 1, []byte("m0"))
	rig.readInOrder(t, 1, time.Second)
	var dur string
	nc, js := jsClientConnect(t, rig.s)
	for ci := range js.ConsumersInfo(mqttStreamName) {
		if strings.HasPrefix(ci.Name, rig.sess.idHash+"_") {
			dur = ci.Name
		}
	}
	nc.Close()
	if dur == _EMPTY_ {
		t.Fatalf("fixture: the subscriber's source durable was not found")
	}
	reply := testInflightAckSubject(dur, 99, 1)
	mqttTestSourceAckDropHook.Set(func(r string) bool { return r == reply })
	t.Cleanup(func() { mqttTestSourceAckDropHook.Set(nil) })
	if res, err := rig.sess.submitSourceAck(reply, true, nil); res != mqttSourceAckFailed || err == nil {
		t.Fatalf("fixture: a lost confirmed acknowledgement returned %v, %v", res, err)
	}
	rig.sess.mu.Lock()
	recorded := rig.sess.ackedSources.has(dur, 99)
	rig.sess.mu.Unlock()
	if !recorded {
		t.Fatalf("the acknowledgement whose confirmation failed is not recorded")
	}
}

// TestReofferOfUnacknowledgedDeliveryIsRetransmitted: a re-offer of a delivery the client never
// acknowledged is not this session's acknowledged source, and is retransmitted.
func TestReofferOfUnacknowledgedDeliveryIsRetransmitted(t *testing.T) {
	rig := newTestAckedReofferRig(t, "ar-unacked", 500*time.Millisecond)
	testMQTTPublish(t, rig.pc, rig.pr, 1, false, false, "ar/t", 1, []byte("m0"))
	flags, pi, _, payload := testMQTTGetPubMsgEx(t, rig.sc, rig.sr, "ar/t", nil)
	if string(payload) != "m0" || flags&mqttPubFlagDup != 0 {
		t.Fatalf("first transmission: %q flags %x", payload, flags)
	}
	// No PUBACK: JetStream re-offers at AckWait, and the session owes the client a retransmission.
	flags, pi2, _, payload := testMQTTGetPubMsgEx(t, rig.sc, rig.sr, "ar/t", nil)
	if string(payload) != "m0" || pi2 != pi || flags&mqttPubFlagDup == 0 {
		t.Fatalf("retransmission: %q identifier %d (first %d) flags %x", payload, pi2, pi, flags)
	}
	if d := rig.declined(); len(d) != 0 {
		t.Fatalf("a re-offer of an unacknowledged delivery was declined: %v", d)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, rig.sc, pi)
}

// TestHandedBackSourceIsNotRecorded: a hand-back is not a decision about the source's fate, so the
// gate records nothing for it.
func TestHandedBackSourceIsNotRecorded(t *testing.T) {
	f := testSharedTransmitted(t, "ar-handback")
	sess := f.e.sess
	if r := sess.releaseTransmittedIdentifier(f.e.c, f.dur, f.reply); r != mqttAbandonDone {
		t.Fatalf("deny: %v", r)
	}
	if !f.held(sess) {
		t.Fatalf("fixture: no hand-back holds the source")
	}
	sess.mu.Lock()
	res := sess.submitSourceAckLocked(f.reply, nil)
	recorded := sess.ackedSources.has(f.dur, f.sseq)
	sess.mu.Unlock()
	if res != mqttSourceAckHandedBack {
		t.Fatalf("fixture: the gate returned %v, want a hand-back", res)
	}
	if recorded {
		t.Fatalf("a handed-back source was recorded as acknowledged")
	}
}
