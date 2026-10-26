//go:build mqtt_fulltests

// Copyright 2025 The mqtt-nats-broker Authors
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
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// THE REDRIVE WAITER ON THE WIRE (F-098).
//
// The unit gates in pubrel_redrive_test.go prove what the waiter does with each blocker.
// These prove the windows are REACHED by an ordinary client, that nothing escapes while a
// refusal stands, and — the point of the whole mechanism — that the PUBREL arrives on a
// timescale AckWait cannot explain.
//
// AckWait is 30 s in both fixtures and every deadline here is single-digit seconds, so a
// PUBREL observed inside them was produced by the wake and by nothing else. That is the
// discrimination the gate needs: the same tests pass on a broker with no waiter at all if
// the deadline is allowed to exceed AckWait.

// mqttRedriveAckWait is long enough that no assertion below can be satisfied by JetStream's
// own redelivery.
const mqttRedriveAckWait = 30 * time.Second

// testFailPublishDrainWrites returns a store hook that fails EXACTLY the write that drains
// a PUBLISH action, and counts how often it did.
//
// Selecting that write by "an in-flight subject whose body carries no action token" was too
// wide, and a fixture proved it: the register's FIRST store and the unsent marker match that
// too, so the hook refused writes the test was not about and never provably reached the drain
// at all. A drain is the one register REWRITE that clears the token of a transmission that
// was already counted: the counter is at least one (C-I), the token is empty, and the subject
// is the register rather than its marker sibling.
func testFailPublishDrainWrites(blocking *atomic.Bool, hits *atomic.Int32) func(subject string, hdrLen int, msg []byte) error {
	return func(subject string, hdrLen int, msg []byte) error {
		if !blocking.Load() || !strings.HasPrefix(subject, mqttInflightStreamSubjectPrefix) ||
			strings.HasSuffix(subject, "."+mqttInflightMarkerSuffix) {
			return nil
		}
		var rec mqttInflightRecord
		if err := json.Unmarshal(msg[hdrLen:], &rec); err != nil || rec.ActionToken != _EMPTY_ || rec.Acq == 0 {
			return nil
		}
		if hits != nil {
			hits.Add(1)
		}
		return errors.New("test: the drain's write could not be confirmed")
	}
}

// testRedriveSlotRecord reads the durable slot for one identifier, so a fixture can prove
// WHICH half of a transmission a refusal was about rather than inferring it.
func testRedriveSlotRecord(t *testing.T, s *Server, clientID string, pi uint16) *mqttInflightRecord {
	t.Helper()
	sess := testMQTTResidentSession(t, s, clientID)
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	asm := sess.asm
	sess.mu.Unlock()
	cur, smsg, err := asm.loadInflightRecord(mqttInflightSubject(idHash, gen, pi))
	if err != nil || smsg == nil || cur == nil {
		t.Fatalf("the slot for packet identifier %d must be readable: rec=%v msg=%v err=%v",
			pi, cur, smsg, err)
	}
	return cur
}

// testRedriveNakObserver watches every acknowledgement this server sends on its own JS API
// connection, so a fixture can require that NOTHING answered a refused delivery while the
// blocker stood. "The PUBREL eventually arrived" does not prove the ordering; the absence
// of a NAK during the window does.
func testRedriveNakObserver(t *testing.T, s *Server) *nats.Subscription {
	t.Helper()
	obs, err := s.nrc.SubscribeSync("$JS.ACK.>")
	if err != nil {
		t.Fatalf("subscribing to the acknowledgement subjects: %v", err)
	}
	t.Cleanup(func() { _ = obs.Unsubscribe() })
	if ferr := s.nrc.Flush(); ferr != nil {
		t.Fatalf("flush: %v", ferr)
	}
	return obs
}

// testRedriveRequireNoNak requires that no redrive was issued during d.
func testRedriveRequireNoNak(t *testing.T, obs *nats.Subscription, d time.Duration, why string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return
		}
		msg, err := obs.NextMsg(left)
		if err != nil {
			return
		}
		if bytes.Equal(msg.Data, AckNak) {
			t.Fatalf("%s: a redrive was issued on %q while the blocker still stood, so the "+
				"waiter was consumed and the transmission's own end has nothing left to wake",
				why, msg.Subject)
		}
	}
}

// testRedriveRequireNak requires that a redrive WAS issued, on the same observer the
// negative assertions use.
//
// Without this the negatives are vacuous: a subscription that never sees this server's
// acknowledgements at all would satisfy every "no NAK" check while proving nothing. The
// positive is what makes the observer a measuring instrument rather than a formality.
func testRedriveRequireNak(t *testing.T, obs *nats.Subscription, d time.Duration, why string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			t.Fatalf("%s: no redrive was ever observed, so every 'no redrive' assertion above "+
				"was vacuous", why)
		}
		msg, err := obs.NextMsg(left)
		if err != nil {
			continue
		}
		if bytes.Equal(msg.Data, AckNak) {
			return
		}
	}
}

// testRedriveWaitWaiters waits until exactly want records are waiting on this identifier.
//
// Registration happens on the stored record's own delivery callback, which runs on its own
// goroutine: the PUBREC that causes it returns to the client long before that callback has
// been reached, so reading the set straight after sending one is a race, not a check.
func testRedriveWaitWaiters(t *testing.T, s *Server, clientID string, pi uint16, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := testRedriveWaiterCount(t, s, clientID, pi)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d refused records are waiting on packet identifier %d, want %d",
				got, pi, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// testRedriveWaiterCount reports how many stored records are waiting on this identifier.
func testRedriveWaiterCount(t *testing.T, s *Server, clientID string, pi uint16) int {
	t.Helper()
	sess := testMQTTResidentSession(t, s, clientID)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.pubRelRedrive[pi])
}

// W1: THE DURABLE HALF, and its continuation is the DRAIN RECONCILER rather than the send
// section — which is what makes this the window the section's own wake cannot cover.
//
// The action drain fails while the client's PUBREC is already in flight, so the section
// ends with the token still standing: endPublishSend's wake runs and finds the blocker in
// place, correctly declining to consume the waiter. Nothing else was left. The reconciler
// is the only event that ends this action, so the wake belongs there too, and its absence
// left the client waiting out AckWait after a fault that cleared in seconds.
//
// Mutation M77: drop the reconciler's wake (or wake unconditionally on an unresolved
// attempt). The PUBREL then cannot arrive before AckWait and this fails.
func TestMQTTPubRelRedriveWakesFromTheDrainReconciler(t *testing.T) {
	// NOT parallel: the store hook is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = mqttRedriveAckWait
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "redrive-w1"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "w1/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	// THE DRAIN, AND ONLY THE DRAIN, FAILS. The acquisition and both phase advances carry
	// the live token, so keying on its ABSENCE in the body selects the one write that ends
	// the action — failing every in-flight store instead would stop the PUBLISH ever going
	// out and the fixture would never reach the window it names.
	var blocking atomic.Bool
	blocking.Store(true)
	var drainAttempts atomic.Int32
	restore := setMQTTTestStoreMsgHook(testFailPublishDrainWrites(&blocking, &drainAttempts))
	t.Cleanup(restore)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "redrive-w1-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, pc, 2, false, false, "w1/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "w1/topic", mqttPubQoS2, []byte("payload"))
	naks := testRedriveNakObserver(t, s)
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)

	// NOTHING ESCAPES WHILE THE REFUSAL STANDS. A PUBREL here would mean admission
	// transmitted over a live action, which is the exclusion the refusal exists to provide.
	testMQTTExpectNothingFor(t, r, 1500*time.Millisecond)
	// AND NOTHING WAS WOKEN EITHER. The section's own wake ran in this window — the write
	// returned long ago — and it must have declined, because the token it could not drain
	// still stands. A NAK here would prove the wake is unconditional, which spends the
	// one-shot waiter on a refusal and leaves the reconciler with nothing to release.
	testRedriveRequireNoNak(t, naks, 500*time.Millisecond, "the drain has not landed")
	testRedriveWaitWaiters(t, s, "redrive-w1", pi, 1)
	if cur := testRedriveSlotRecord(t, s, "redrive-w1", pi); cur.ActionToken == _EMPTY_ {
		t.Fatal("the fixture must be blocked on the DURABLE half: the slot carries no action token")
	}
	if drainAttempts.Load() == 0 {
		t.Fatal("the fixture never reached the drain: nothing was blocking")
	}

	// THE FAULT CLEARS. The next reconciler attempt drains the action and wakes the waiter.
	blocking.Store(false)
	start := time.Now()
	testRedriveRequireNak(t, naks, 10*time.Second, "the reconciler drained the action")
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	if elapsed := time.Since(start); elapsed >= mqttRedriveAckWait {
		t.Fatalf("the PUBREL took %v, which AckWait alone explains: this proves no wake", elapsed)
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	testMQTTFlush(t, c, nil, r)
}

// W2: THE LOCAL HALF. The durable action is already drained and the record shows no token
// at all, yet admission still refuses — the opener cannot undo a PUBLISH whose write has
// not returned. That refusal was invisible to the caller, so it left no waiter and the
// client waited out AckWait for a window a few microseconds wide.
//
// Mutation M78: return mqttAdmissionRefused from the local-hold check instead of
// mqttAdmissionPublishInFlight, so the caller cannot tell it from a permanent refusal and
// registers nothing.
func TestMQTTPubRelRedriveWakesFromTheLocalSendSection(t *testing.T) {
	// NOT parallel: the section pause is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = mqttRedriveAckWait
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "redrive-w2"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "w2/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	// HOLD THE SECTION OPEN AFTER THE DURABLE DRAIN. Exactly one section is held — the
	// one delivering this message — and the release is explicit, so the window is
	// deterministic rather than raced.
	paused := make(chan struct{}, 1)
	release := make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	restore := setMQTTTestPublishDrainedPause(func(uint16) {
		if !armed.CompareAndSwap(true, false) {
			return
		}
		select {
		case paused <- struct{}{}:
		default:
		}
		<-release
	})
	t.Cleanup(restore)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "redrive-w2-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, pc, 2, false, false, "w2/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "w2/topic", mqttPubQoS2, []byte("payload"))
	naks := testRedriveNakObserver(t, s)
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("the send section never reached the post-drain window")
	}
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)

	// The durable half is GONE — this is what makes the refusal the local one — and
	// nothing is transmitted over it.
	testMQTTExpectNothingFor(t, r, 1500*time.Millisecond)
	testRedriveRequireNoNak(t, naks, 500*time.Millisecond, "the send section is still open")
	testRedriveWaitWaiters(t, s, "redrive-w2", pi, 1)
	// THE REFUSAL WAS THE LOCAL ONE, AND IT COST NO ACTION. Both halves are asserted
	// because either alone is ambiguous: an empty token could mean the durable-token
	// branch simply had nothing to see, and a live section could coexist with an action
	// admission had already acquired — which is the outcome shape (b) exists to avoid,
	// since an acquired action must itself drain before its own waiter can proceed.
	if cur := testRedriveSlotRecord(t, s, "redrive-w2", pi); cur.ActionToken != _EMPTY_ {
		t.Fatalf("the durable half must be drained and no action acquired over it, got token %q "+
			"in state %v", cur.ActionToken, cur.State)
	}
	sess := testMQTTResidentSession(t, s, "redrive-w2")
	sess.mu.Lock()
	h := sess.sendsInFlight[pi]
	sess.mu.Unlock()
	if h == nil || h.pubIdent == _EMPTY_ {
		t.Fatalf("the fixture must be inside a real PUBLISH send section, got hold %+v", h)
	}

	close(release)
	start := time.Now()
	testRedriveRequireNak(t, naks, 10*time.Second, "the send section closed")
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	if elapsed := time.Since(start); elapsed >= mqttRedriveAckWait {
		t.Fatalf("the PUBREL took %v, which AckWait alone explains: this proves no wake", elapsed)
	}
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	testMQTTFlush(t, c, nil, r)
}

// W3: TWO STORED RECORDS FOR ONE IDENTIFIER, both refused in the same window and both
// promptly re-admitted.
//
// Several PUBREL records under one identifier is a SUPPORTED stale-broker shape. A waiter
// set keyed by the identifier alone kept one of them and silently abandoned the other,
// which is the defect the whole mechanism exists to remove, reached one level down: the
// abandoned record waits out AckWait exactly as an unregistered refusal does.
//
// THE WINDOW IS THE DURABLE ONE, and the choice is forced rather than incidental. Admission
// tests the action token BEFORE it classifies a record as a duplicate, so a live token
// refuses — and registers — both records. In the LOCAL window the duplicate is already
// past that point and is discharged and acknowledged on the spot, which is correct and
// leaves exactly one waiter: a two-waiter assertion there would be a fixture asserting the
// wrong mechanism (the first draft of this test did exactly that and failed).
//
// Both outcomes must then DIFFER: the slot names one canonical record, so exactly one
// PUBREL may reach the client and the other must be discharged rather than transmitted.
// One PUBREL on the wire is therefore not enough by itself — it is the pair of NAKs plus
// the single transmission that proves both were re-offered and correctly routed.
//
// Mutation M83: key the waiter set by the identifier alone (M74's wire form). The second
// registration replaces the first and one of the two records is left to AckWait.
func TestMQTTPubRelRedriveWakesEveryStoredRecord(t *testing.T) {
	// NOT parallel: the store hook is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = mqttRedriveAckWait
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "redrive-w3"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "w3/topic", qos: 2}}, []byte{2})
	testMQTTFlush(t, c, nil, r)

	var blocking atomic.Bool
	blocking.Store(true)
	var drainAttempts atomic.Int32
	restore := setMQTTTestStoreMsgHook(testFailPublishDrainWrites(&blocking, &drainAttempts))
	t.Cleanup(restore)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "redrive-w3-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSendPublishPacket(t, pc, 2, false, false, "w3/topic", 1, []byte("payload"))
	testMQTTReadPIPacket(mqttPacketPubRec, t, pr, 1)
	testQoS2SendPubRel(t, pc, 1)
	testMQTTReadPIPacket(mqttPacketPubComp, t, pr, 1)

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "w3/topic", mqttPubQoS2, []byte("payload"))
	naks := testRedriveNakObserver(t, s)
	// The client's PUBREC stores the CANONICAL record, whose delivery the live action
	// refuses.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	testRedriveWaitWaiters(t, s, "redrive-w3", pi, 1)

	// A SECOND, structurally valid record for the same exchange — what a stale broker
	// leaves behind. Built from the slot itself, so it carries the exchange's own owner
	// nonce and source correlation and is a genuine duplicate rather than a stray.
	cur := testRedriveSlotRecord(t, s, "redrive-w3", pi)
	sess := testMQTTResidentSession(t, s, "redrive-w3")
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	pubRelSubject, _, _ := mqttPubRelNames(idHash, gen)
	dupMsg, dupHdr := mqttNewCorrelatedPubRel(pi, cur.Nonce, cur.Dur, cur.Seq, _EMPTY_, _EMPTY_, _EMPTY_)
	if _, err := sess.jsa.storeMsg(pubRelSubject, dupHdr, dupMsg); err != nil {
		t.Fatalf("storing the duplicate PUBREL record: %v", err)
	}
	testRedriveWaitWaiters(t, s, "redrive-w3", pi, 2)
	testMQTTExpectNothingFor(t, r, 500*time.Millisecond)
	testRedriveRequireNoNak(t, naks, 300*time.Millisecond, "the action is still live")

	blocking.Store(false)
	// BOTH records are re-offered: one NAK per stored record, not one per identifier.
	testRedriveRequireNak(t, naks, 15*time.Second, "the drain landed (first record)")
	testRedriveRequireNak(t, naks, 15*time.Second, "the drain landed (second record)")

	// EXACTLY ONE reaches the client. The slot names one canonical record; the other is a
	// duplicate and is discharged, never transmitted.
	start := time.Now()
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
	if elapsed := time.Since(start); elapsed >= mqttRedriveAckWait {
		t.Fatalf("the PUBREL took %v, which AckWait alone explains: this proves no wake", elapsed)
	}
	testMQTTExpectNothingFor(t, r, 1500*time.Millisecond)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	testMQTTFlush(t, c, nil, r)
}
