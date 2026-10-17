//go:build mqttport_fulltests

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
	"sync/atomic"
	"testing"
	"time"
)

// THE PUBLISH SEND BARRIER (F-098).
//
// A QoS 1/2 PUBLISH send section takes a DURABLE action on its exchange's slot and ends it
// by draining that action, which is a JetStream write. A graceful shutdown that tore
// JetStream down while a section was open left a live token on the record — and no later
// start can tell that token from a peer's, because nothing durable says the holder's
// process died. That is not a residual an ordinary restart may pay: the identifier, its
// Receive Maximum capacity and the session's whole outbound flow stay blocked.
//
// So sections are admitted through a gate and JOINED before the JSA teardown, and each
// HOLDER confirms its own drain. Nothing here clears a token it does not hold.

// B1: SHUTDOWN WAITS FOR AN OPEN SECTION, and the token is confirmed drained.
//
// The section is held open inside its own window — after the durable action is acquired
// and before anything is written — and shutdown is started underneath it. The barrier must
// not let the JSA teardown run: a shutdown that completed here would be the defect, and
// the drain that follows the release would fail with the request path already gone.
//
// Mutation M85: skip admitPublishSend/publishSendDone, or close the barrier after Phase 4.
// Shutdown then returns while the section is open and the token survives into the restart.
func TestMQTTPublishSendBarrierWaitsForAnOpenSection(t *testing.T) {
	// NOT parallel: the send-window hook is process-global.
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 30 * time.Second
	s := testMQTTRunServer(t, o)
	shutdownOnce := false
	defer func() {
		if !shutdownOnce {
			testMQTTShutdownServer(s)
		}
	}()

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "barrier-b1"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "b1/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	// HELD INSIDE THE SECTION, with the durable action already acquired: that is the
	// state whose escape into the next process this barrier exists to prevent.
	opened := make(chan uint16, 1)
	release := make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	restore := setMQTTTestMarkerSendWindowHook(func(_ *mqttSession, pi uint16, window string) bool {
		if window != mqttMarkerSendReserved || !armed.CompareAndSwap(true, false) {
			return true
		}
		select {
		case opened <- pi:
		default:
		}
		<-release
		return true
	})
	t.Cleanup(restore)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "barrier-b1-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "b1/topic", 1, []byte("payload"))

	var pi uint16
	select {
	case pi = <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("no send section ever opened")
	}
	// The durable half is REALLY held: without this the barrier could be waiting for a
	// section that has nothing to drain, and the test would prove nothing.
	sess := testMQTTResidentSession(t, s, "barrier-b1")
	sess.mu.Lock()
	idHash, gen, asm := sess.idHash, sess.gen, sess.asm
	sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	cur, _, err := asm.loadInflightRecord(subject)
	if err != nil || cur == nil || cur.ActionToken == _EMPTY_ {
		t.Fatalf("the section must hold a durable action: rec=%+v err=%v", cur, err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Shutdown()
	}()
	select {
	case <-done:
		t.Fatal("shutdown completed while a PUBLISH send section was still open: its drain " +
			"would then run against a torn-down request path and the token would survive " +
			"into the next process")
	case <-time.After(500 * time.Millisecond):
	}

	// RELEASED: the holder finishes its own section, drains its own action, and only then
	// does shutdown proceed.
	close(release)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("shutdown never completed after the section was released")
	}
	shutdownOnce = true

	// THE TOKEN IS GONE, read from a server restarted on the same store. A drain that
	// merely ran proves nothing; the absence is what the next process needs, and it is
	// read from the record rather than inferred from a redelivery — a redelivery has its
	// own pacing and would make this test about AckWait instead.
	o2 := testMQTTDefaultOptions()
	o2.StoreDir = o.StoreDir
	o2.MQTT.AckWait = 30 * time.Second
	s2 := testMQTTRunServer(t, o2)
	defer testMQTTShutdownServer(s2)
	asm2 := testMQTTGetGlobalASM(t, s2)
	after, smsg2, aerr := asm2.loadInflightRecord(subject)
	switch {
	case aerr != nil:
		t.Fatalf("reading the slot after the restart: %v", aerr)
	case smsg2 == nil, after == nil:
		// The exchange completed and its register was deleted, which is a stronger
		// outcome than a drained token.
	case after.ActionToken != _EMPTY_:
		t.Fatalf("a durable PUBLISH action survived a graceful shutdown: %q on %q — no later "+
			"start can tell it from a peer's, so the identifier is blocked for the life of "+
			"the session", after.ActionToken, subject)
	}
}

// B2: A DRAIN THAT FAILED WHILE THE STORAGE WAS UNAVAILABLE IS STILL FINISHED, as long as
// the storage recovers before the teardown.
//
// The reconciler's timer cannot cover this: its admission closes at Phase 2, so a fault
// that clears after that would leave the token standing with nothing left to retry. The
// obligation is therefore held by IDENTITY on the server, independently of the entry maps
// and of any timer, and shutdown makes one last attempt while requests still work.
//
// Mutation M86: drop the owed-drain registry (or skip finishOwedDrains). The redelivery
// after the restart then never arrives.
func TestMQTTPublishSendBarrierFinishesADrainTheTimerCouldNotRetry(t *testing.T) {
	// NOT parallel: the store hook is process-global.
	o := testMQTTDefaultOptions()
	// SHORT, and on the FIRST server. A source consumer keeps the AckWait it was created
	// with across a restart — TestMQTTAckWaitConfigChange asserts exactly that — so the
	// redelivery the restarted server owes arrives on THIS clock. A long value here made
	// a no-failure control fail identically, which is how the fixture was caught.
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	shutdownOnce := false
	defer func() {
		if !shutdownOnce {
			testMQTTShutdownServer(s)
		}
	}()

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "barrier-b2"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "b2/topic", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	// ONLY THE DRAIN FAILS: its write is the one carrying an empty action token.
	// THE STORAGE STAYS DOWN UNTIL NO TIMER CAN RUN, and recovers exactly when the final
	// pass begins.
	//
	// Keyed on the SHUTDOWN PHASE, not on a count of failed writes. A first version counted
	// writes and let storage back after "one more bounded attempt" past the dispatch gate —
	// but casInflightSlot retries internally, so one drain attempt spent that whole budget
	// and the reconciler's NEXT attempt drained the token itself. Mutation M86 (no final
	// pass) survived, and a stack capture showed schedulePublishDrainReconcile doing the
	// work the test attributed to shutdown.
	//
	// Phase 3b starts by closing the PUBLISH send barrier, and Phase 3 has already waited
	// for every admitted worker, the reconciler's timers included. So while the barrier is
	// open every drain fails, and once it is closed the only thing left that can drain is the
	// final pass.
	var blocking atomic.Bool
	blocking.Store(true)
	var drainFailures, afterGate atomic.Int32
	failDrain := testFailPublishDrainWrites(&blocking, &drainFailures)
	restore := setMQTTTestStoreMsgHook(func(subject string, hdrLen int, msg []byte) error {
		s.publishSendMu.Lock()
		finalPass := s.publishSendClosed
		s.publishSendMu.Unlock()
		if finalPass {
			blocking.Store(false)
		}
		err := failDrain(subject, hdrLen, msg)
		if err == nil {
			return nil
		}
		s.willDispatchMu.Lock()
		gateClosed := s.willDispatchClosed
		s.willDispatchMu.Unlock()
		if gateClosed {
			afterGate.Add(1)
		}
		return err
	})
	t.Cleanup(restore)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "barrier-b2-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, 1, false, false, "b2/topic", 1, []byte("payload"))
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "b2/topic", mqttPubQos1, []byte("payload"))
	for deadline := time.Now().Add(10 * time.Second); drainFailures.Load() < mqttInflightStoreAttempts; {
		if time.Now().After(deadline) {
			t.Fatalf("the drain failed %d times, want its whole bounded budget (%d): the obligation "+
				"this test is about never existed", drainFailures.Load(), mqttInflightStoreAttempts)
		}
		time.Sleep(10 * time.Millisecond)
	}
	sess := testMQTTResidentSession(t, s, "barrier-b2")
	sess.mu.Lock()
	idHash, gen, asm := sess.idHash, sess.gen, sess.asm
	sess.mu.Unlock()
	subject := mqttInflightSubject(idHash, gen, pi)
	if cur, _, err := asm.loadInflightRecord(subject); err != nil || cur == nil ||
		cur.ActionToken == _EMPTY_ {
		t.Fatalf("the unresolved drain must leave its token standing: rec=%+v err=%v", cur, err)
	}

	s.Shutdown()
	shutdownOnce = true
	// THE CROSSING HAPPENED: a reconciler attempt ran after the dispatch gate closed and
	// failed, so its reschedule was refused and no timer was left to retry. Without this the
	// fixture could be satisfied by a drain that never needed the final pass at all.
	if afterGate.Load() == 0 {
		t.Fatal("no drain attempt failed after the dispatch gate closed: the obligation never " +
			"crossed the point where the reconciler could no longer retry it")
	}

	o2 := testMQTTDefaultOptions()
	o2.StoreDir = o.StoreDir
	// SHORT, because the property here is that the identifier is USABLE again, and the
	// redelivery below must arrive on the source's own clock rather than being withheld
	// forever by a token the shutdown failed to drain.
	o2.MQTT.AckWait = 500 * time.Millisecond
	s2 := testMQTTRunServer(t, o2)
	defer testMQTTShutdownServer(s2)
	asm2 := testMQTTGetGlobalASM(t, s2)
	if after, smsg2, aerr := asm2.loadInflightRecord(subject); aerr != nil {
		t.Fatalf("reading the slot after the restart: %v", aerr)
	} else if smsg2 != nil && after != nil && after.ActionToken != _EMPTY_ {
		t.Fatalf("the drain that failed on unavailable storage was never finished: token %q "+
			"survived the shutdown", after.ActionToken)
	}
	c2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: "barrier-b2"}, o2.MQTT.Host, o2.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	gotPI := testMQTTCheckPubMsgNoAck(t, c2, r2, "b2/topic", mqttPubQos1|mqttPubFlagDup, []byte("payload"))
	if gotPI != pi {
		t.Fatalf("the redelivery used packet identifier %d, want the held %d", gotPI, pi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c2, gotPI)
	testMQTTFlush(t, c2, nil, r2)
}

// B3: A PEER'S TOKEN IS NEVER TOUCHED by the shutdown pass.
//
// The pass re-attempts a drain by its own captured identity, and the drain re-reads and
// re-proves that exact token before writing anything. A shutdown that cleared whatever it
// found would be the steal F-098 exists to refuse — and it would do it at the one moment
// nobody is watching.
//
// Mutation M87: have finishOwedDrains clear the slot's action fields directly instead of
// re-attempting the identity-bound drain.
func TestMQTTPublishSendBarrierLeavesAPeersToken(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "barrier-b3")
	testReadyForPubRelDelivery(t, e)
	const pi = uint16(3)
	subject := testRedriveSlot(t, e, pi, "a-peers-live-action-token")

	cur, smsg, err := e.asm.loadInflightRecord(subject)
	if err != nil || cur == nil || smsg == nil {
		t.Fatalf("loading the seeded slot: rec=%+v err=%v", cur, err)
	}
	ref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur)
	if ref == nil {
		t.Fatal("the seeded slot must yield a reference")
	}
	// OUR obligation names a DIFFERENT token: the shape a holder leaves when its own
	// action was replaced while its drain was unresolved.
	ours := *ref
	ours.actionToken = "our-own-abandoned-token"
	e.s.noteOwedDrain(e.sess, mqttSendToken{token: "our-own-abandoned-token", actionHeld: true, slot: ours})

	e.s.finishOwedDrains()

	after, _, aerr := e.asm.loadInflightRecord(subject)
	if aerr != nil || after == nil {
		t.Fatalf("reloading the slot: rec=%+v err=%v", after, aerr)
	}
	if after.ActionToken != "a-peers-live-action-token" {
		t.Fatalf("the shutdown pass cleared a token it does not hold: %q", after.ActionToken)
	}
}
