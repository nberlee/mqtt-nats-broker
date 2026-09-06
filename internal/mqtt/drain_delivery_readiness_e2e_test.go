// Copyright 2025 The NATS Authors
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

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testStripDrainDelivery puts the session's single drain into the R2 state: present in
// sess.drains, absent from drainByDur, and with its delivery subscription actually GONE.
//
// It models R2 ALONE. R1 — the blanket QoS 2 withholding — leaves the drain INDEXED
// with no subscription, because restoreDrains skips the durable and the entry
// restoreDrainLocked made at record restore survives. That asymmetry is what the guard
// keyed on the subscription exists for, and it is exercised by R1's own producer test.
//
// Deleting only the reverse-index entry would not even be R2. The subscription would
// still be installed, so the reclaim would take its "already delivering" exit and do
// nothing — and the test would pass without ever reaching the defect.
func testStripDrainDelivery(t *testing.T, sess *mqttSession) (dur, drainSid string) {
	t.Helper()
	var deliverSubject string
	sess.mu.Lock()
	for sid, d := range sess.drains {
		dur, drainSid, deliverSubject = d.cc.Durable, sid, d.cc.DeliverSubject
		delete(sess.drainByDur, dur)
	}
	sess.mu.Unlock()
	if dur == "" {
		t.Fatal("no drained durable to work with")
	}
	c := sess.c
	c.mu.Lock()
	sub := c.subs[deliverSubject]
	c.mu.Unlock()
	if sub != nil {
		c.processUnsub(sub.sid)
	}
	c.mu.Lock()
	stillSubscribed := c.subs[deliverSubject] != nil
	c.mu.Unlock()
	if stillSubscribed {
		t.Fatal("the fixture must end with NO delivery subscription for the drained durable")
	}
	if drainRegistered(sess, dur) {
		t.Fatal("the fixture must end un-indexed")
	}
	return dur, drainSid
}

// testAwaitProducedState is the precondition every producer-driven test waits for, and
// the FIRST thing it waits on is proof the producer actually FIRED.
//
// Waiting on "no delivery subscription" alone is not that proof, and a review round
// caught why: a drain restored from the record has no subscription until restoreDrains
// installs one, so that state is already true BEFORE the resume reaches the failure
// being injected. A test that cleared its hook there would let the resume succeed and
// then pass through ordinary adoption, exercising nothing.
//
// wantIndexed distinguishes the two producers, and they differ:
//   - R2, a failed restoreDrains: the registration is rolled back, so the drain is
//     UNINDEXED with no subscription.
//   - R1, the blanket QoS 2 withholding: restoreDrains skips the durable entirely, so
//     the entry restoreDrainLocked made at record restore STAYS — INDEXED, with no
//     subscription. That asymmetry is the production bug this repair had to fix.
func testAwaitProducedState(t *testing.T, sess *mqttSession, dur string, fired *atomic.Bool, wantIndexed bool) {
	t.Helper()
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if !fired.Load() {
			return errors.New("the injected producer has not fired yet")
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		for sid, d := range sess.drains {
			if d.cc.Durable != dur {
				continue
			}
			if h := sess.drainSidHoldLocked(sid); h != mqttDrainNotHeld {
				t.Fatalf("the drain must be UNHELD, or F-094 refuses it and this tests nothing, got %v", h)
			}
			if indexed := sess.drainByDur[dur] != nil; indexed != wantIndexed {
				return fmt.Errorf("drainByDur entry: want %v, got %v", wantIndexed, indexed)
			}
			c := sess.c
			if c == nil {
				return errors.New("the session has no connection yet")
			}
			c.mu.Lock()
			sub := c.subs[d.cc.DeliverSubject]
			c.mu.Unlock()
			if sub != nil {
				return errors.New("the resume still has a delivery subscription for this drain")
			}
			return nil
		}
		return fmt.Errorf("the resume has not restored drain %q yet", dur)
	})
}

// testMQTTSubAwaitingSubAck sends a SUBSCRIBE and reads until the SUBACK, DISCARDING any
// PUBLISH that arrives first.
//
// The interleaving is a consequence of the fix, not noise: the reclaim installs delivery
// interest inside processSubs, so JetStream can push the drain's outstanding redelivery
// before the handler has written the SUBACK. testMQTTSub expects the SUBACK to be the
// very next packet and fails on the PUBLISH.
func testMQTTSubAwaitingSubAck(t *testing.T, pi uint16, c net.Conn, r *mqttReader, filter string, qos byte) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2 + len(filter) + 1)
	w.WriteUint16(pi)
	w.WriteBytes([]byte(filter))
	w.WriteByte(qos)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}
	for i := 0; i < 10; i++ {
		b, pl := testMQTTReadPacket(t, r)
		if b&mqttPacketMask != mqttPacketSubAck {
			// Only a PUBLISH may legitimately overtake the SUBACK here.
			if b&mqttPacketMask != mqttPacketPub {
				t.Fatalf("unexpected packet %v while waiting for the SUBACK", b&mqttPacketMask)
			}
			r.pos += pl
			continue
		}
		// The SUBACK's own assertions are kept: tolerating an interleaved PUBLISH must
		// not become tolerating any answer at all.
		rpi, err := r.readUint16("packet identifier")
		if err != nil {
			t.Fatalf("read SUBACK identifier: %v", err)
		}
		if rpi != pi {
			t.Fatalf("SUBACK identifier: expected %v, got %v", pi, rpi)
		}
		if pl-2 != 1 {
			t.Fatalf("SUBACK payload: expected one grant, got %d bytes", pl-2)
		}
		granted, err := r.readByte("granted qos")
		if err != nil {
			t.Fatalf("read SUBACK grant: %v", err)
		}
		if granted != qos {
			t.Fatalf("SUBACK grant: expected %v, got %v", qos, granted)
		}
		return
	}
	t.Fatal("no SUBACK arrived within 10 packets")
}

// TestMQTTReSubscribeOverASubscriptionLessDrainKeepsDelivering is the defect end to
// end, through a real client, and it is the test that has to fail on the DAMAGE.
//
// THE ASSERTION THAT DISCRIMINATES is the delivery of a message published AFTER the
// re-SUBSCRIBE. A broker that merely answers the SUBACK correctly satisfies nothing
// here: at HEAD the SUBACK already says success, and it is the silence afterwards
// that is the bug. The drain's durable holds interest, so the message is stored and
// pending forever with nothing pushing it anywhere.
//
// AND IT CARRIES A POSITIVE DELIVERY CONTROL. "No PUBLISH arrived" is satisfied by a
// dead connection or a stalled transmission gate just as well as by the defect, so a
// second filter — subscribed before the drain existed, and an ordinary live consumer
// throughout — must be shown delivering in the same window.
func TestMQTTReSubscribeOverASubscriptionLessDrainKeepsDelivering(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "readiness/topic"
	const control = "readiness/control"
	conn, sess, _ := testDrainSetup(t, s, o, "readiness", topic, 1)
	defer conn.c.Close()

	// Subscribed before the drain exists, so its delivery says nothing about the
	// drained filter and everything about the connection being alive.
	testMQTTSub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: control, qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	// The delivery is on the wire and unacknowledged, so the UNSUBSCRIBE drains it.
	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %d", n)
	}

	// THE FIXTURE. This one is SYNTHETIC — it strips the delivery subscription on the
	// live connection rather than going through a resume — which is deliberate: it
	// isolates the reclaim from CONNECT recovery. The two REAL producers, a failed
	// restoreDrains and the blanket QoS 2 withholding, are driven end to end by the
	// two tests below.
	testStripDrainDelivery(t, sess)

	// The re-SUBSCRIBE that adopts it. At HEAD this succeeds and the filter then
	// delivers nothing.
	testMQTTSub(t, 3, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "readiness-pub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// The positive control first, so a failure below cannot be blamed on the connection.
	testMQTTPublish(t, pc, pr, 1, false, false, control, 20, []byte("control"))
	cpi := testMQTTCheckPubMsg(t, conn.c, conn.r, control, mqttPubQos1, []byte("control"))
	_ = cpi

	// THE DISCRIMINATING ASSERTION.
	testMQTTPublish(t, pc, pr, 1, false, false, topic, 21, []byte("after-resub"))
	testMQTTCheckPubMsg(t, conn.c, conn.r, topic, mqttPubQos1, []byte("after-resub"))
}

// TestMQTTAQoS0ReSubscribeDoesNotReclaimASubscriptionLessDrain pins the gate. A QoS 0
// re-SUBSCRIBE deliberately does not adopt — the drain keeps draining independently
// while the raw subscription delivers new messages at QoS 0 — so the reclaim, which
// adopts, must not run for it either.
func TestMQTTAQoS0ReSubscribeDoesNotReclaimASubscriptionLessDrain(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "readiness0/topic"
	conn, sess, _ := testDrainSetup(t, s, o, "readiness0", topic, 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	dur, drainSid := testStripDrainDelivery(t, sess)

	testMQTTSub(t, 3, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 0}}, []byte{0})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	stillDraining, adopted := len(sess.drains), sess.cons[drainSid] != nil
	sess.mu.Unlock()
	if stillDraining != 1 {
		t.Fatalf("a QoS 0 re-SUBSCRIBE must leave the drain draining, got %d drains", stillDraining)
	}
	if adopted {
		t.Fatal("a QoS 0 re-SUBSCRIBE must adopt nothing, so the reclaim must not run for it")
	}
	if drainRegistered(sess, dur) {
		t.Fatal("a QoS 0 re-SUBSCRIBE must install no delivery interest for the drain")
	}
}

// TestMQTTAFailedReclaimFailsTheFilterAndCleansUp is the failure integration: the
// error must reach the filter's existing consumer-error arm, not a bare continue that
// leaves the raw QoS 0 subscription this filter already installed behind.
func TestMQTTAFailedReclaimFailsTheFilterAndCleansUp(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "readinessfail/topic"
	conn, sess, _ := testDrainSetup(t, s, o, "readinessfail", topic, 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	dur, drainSid := testStripDrainDelivery(t, sess)

	restore := setMQTTTestQOS12SubFailHook(func(durable string) error {
		if durable == dur {
			return errors.New("delivery subscription refused")
		}
		return nil
	})
	defer restore()

	// 0x80, because a filter whose delivery path cannot be installed is refused rather
	// than granted and then silent.
	testMQTTSub(t, 3, conn.c, conn.r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{mqttSubAckFailure})
	testMQTTFlush(t, conn.c, nil, conn.r)

	sess.mu.Lock()
	stillDraining, adopted := sess.drains[drainSid] != nil, sess.cons[drainSid] != nil
	sess.mu.Unlock()
	if !stillDraining {
		t.Fatal("a failed reclaim must leave the drain where it found it")
	}
	if adopted {
		t.Fatal("a failed reclaim must adopt nothing")
	}
	// The raw QoS 0 subscription is installed BEFORE the reclaim runs, so a bare
	// continue would strand it. The cleanup arm is what removes it.
	c := sess.c
	c.mu.Lock()
	rawSurvived := c.subs[drainSid] != nil
	c.mu.Unlock()
	if rawSurvived {
		t.Fatal("a failed filter must not leave its raw subscription behind: the reclaim " +
			"error has to enter the existing cleanup arm, not skip it")
	}
}

// TestMQTTASubscribeRollbackKeepsTheReclaimedDrainRouted is the snapshot retention.
//
// snapshotSubs copies both drain maps at packet start, and rollbackSubscribePacket
// restores them wholesale. THIS TEST'S fixture is R2, whose drain is absent from the
// pre-packet drainByDur because a failed restore rolled the registration back — that
// absence is not what defines a repair in general, since R1 leaves the entry in
// place — so
// without recording the reclaim into the snapshot the rollback would do two things
// wrong at once: count the adopted durable among the packet's own consumers and delete
// it once the rolled-back record confirms, and leave the live subscription over a
// durable the reverse index no longer names, delivering a drained source as an
// ordinary PUBLISH for the rest of the connection.
//
// The save hook fails the packet's FINAL save definitely and lets the rollback's own
// save succeed. Both are required: without a confirmed rollback save
// finishSubscribeRollback never runs and the deletion path is never exercised.
func TestMQTTASubscribeRollbackKeepsTheReclaimedDrainRouted(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "readinessrb/topic"
	conn, sess, _ := testDrainSetup(t, s, o, "readinessrb", topic, 1)
	defer conn.c.Close()

	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	dur, drainSid := testStripDrainDelivery(t, sess)
	deliverSubject := mqttSubPrefix
	sess.mu.Lock()
	if d := sess.drains[drainSid]; d != nil {
		deliverSubject = d.cc.DeliverSubject
	}
	// Captured NOW: the failed packet closes the connection, so sess.c is already nil
	// by the time the assertions below run.
	sc := sess.c
	sess.mu.Unlock()

	// Fail the packet's final save ONCE, definitely; the rollback's own save then
	// succeeds, which is what lets finishSubscribeRollback run.
	// The ROLLBACK'S OWN SAVE is the observation point, and it has to be, because the
	// failed packet closes the connection immediately afterwards: checking the delivery
	// subscription later cannot tell "the rollback removed it" from "connection teardown
	// removed it". By the second hook call rollbackSubscribePacket has already restored
	// both drain maps — it does that before its save — and the connection is still up.
	var saves atomic.Int32
	var indexedAtRollback, routedAtRollback, sampled atomic.Bool
	restore := setMQTTTestSessSaveHook(func(hs *mqttSession) error {
		if hs != sess {
			return nil
		}
		n := saves.Add(1)
		if n == 1 {
			return errors.New("packet save refused")
		}
		if n == 2 {
			hs.mu.Lock()
			indexedAtRollback.Store(hs.drainByDur[dur] != nil)
			hs.mu.Unlock()
			sc.mu.Lock()
			sub := sc.subs[deliverSubject]
			sc.mu.Unlock()
			// closed is part of "routed": a present-but-closed subscription delivers
			// nothing, so presence and durable identity alone do not prove the routing
			// this retention exists to preserve.
			routedAtRollback.Store(sub != nil && sub.mqtt != nil && sub.mqtt.jsDur == dur && !sub.mqtt.closed)
			// Published LAST, so a waiter can never read the flags above while this
			// hook is still filling them in. Waiting on the save COUNT would do exactly
			// that: it reaches two on entry, before either flag is stored.
			sampled.Store(true)
		}
		return nil
	})
	defer restore()

	// The SUBSCRIBE reclaims, adopts, then fails its save and rolls back. The
	// connection closes, so everything below is asserted on the session object.
	// Written directly rather than through testMQTTSub: there is no SUBACK to read,
	// because the failed save closes the connection instead of answering.
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(2 + 2 + len(topic) + 1)
	w.WriteUint16(3)
	w.WriteBytes([]byte(topic))
	w.WriteByte(1)
	if _, err := testMQTTWrite(conn.c, w.Bytes()); err != nil {
		t.Fatalf("write SUBSCRIBE: %v", err)
	}

	// Wait for EVIDENCE THE PACKET RAN, not for a condition that is already true. The
	// drain is present before the SUBSCRIBE as well as after the rollback, so waiting
	// on it returns instantly and asserts against a packet that was never processed.
	// The sampling flag, not the save count: the count reaches two on ENTRY to the
	// hook, before the observations below have been taken.
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		if !sampled.Load() {
			return fmt.Errorf("the rollback has not been sampled yet: %d save(s)", saves.Load())
		}
		return nil
	})

	sess.mu.Lock()
	restored, adopted, indexed := sess.drains[drainSid], sess.cons[drainSid], sess.drainByDur[dur]
	sess.mu.Unlock()
	if restored == nil {
		t.Fatal("the rollback must restore the drain")
	}
	if adopted != nil {
		t.Fatal("the rollback must undo the adoption")
	}
	if indexed != restored {
		t.Fatal("the rollback must restore the drain WITH the delivery subscription this " +
			"packet installed: without the retention the reverse index is empty and the " +
			"live subscription delivers a drained source as an ordinary PUBLISH")
	}
	// The routing claim has two halves and the restored map is only one: sampled at the
	// rollback itself, the subscription this packet installed must still be installed
	// and still name the drain's durable, or "restored to drainByDur" names a durable
	// nothing delivers for.
	if !indexedAtRollback.Load() {
		t.Fatal("at the rollback the drain must already be back in drainByDur")
	}
	if !routedAtRollback.Load() {
		t.Fatal("at the rollback the delivery subscription this packet installed must still " +
			"be present and still name the drain's durable")
	}

	// The durable must survive: the rollback must not count it among the packet's own
	// consumers, or finishSubscribeRollback deletes one that still owes started
	// deliveries.
	//
	// Synchronized on the connection actually CLOSING — not on the save-hook count,
	// which only proves the rollback's write started, while finishSubscribeRollback
	// (what would issue the deletion) runs after that write returns. A read TIMEOUT is
	// explicitly not closure: accepting any error would let this assert against a
	// rollback still in flight.
	conn.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 1)
	_, rerr := conn.c.Read(buf)
	if rerr == nil {
		t.Fatal("the failed packet must close the connection")
	}
	if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
		t.Fatalf("timed out waiting for the failed packet to close the connection: %v", rerr)
	}

	// The durable must survive: the rollback must not count it among the packet's own
	// consumers, or finishSubscribeRollback deletes one that still owes started
	// deliveries.
	//
	// BOUNDED, and worth saying so: the deletion is a no-wait request, so closure does
	// not acknowledge its execution and no single lookup can. The check is therefore
	// repeated across a settle window, and the DISCRIMINATING assertion for the
	// retention is the routing sampled at the rollback above — which a mutation
	// removing retainRepairedDrain fails outright.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := sess.jsa.lookupConsumer(mqttStreamName, dur); err != nil {
			t.Fatalf("the rollback must not delete the adopted drain's durable: %v", err)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestMQTTAFailedCompanionReclaimCleansUpBothHalves is the SECOND insertion point, and
// it exists because one does not establish the other.
//
// A multi-level filter owns two consumer identities: the main sid and the level-up
// companion. The companion's reclaim runs after the main half has already installed
// its raw subscription AND created its consumer, so a bare continue there strands more
// than the main insertion point ever could. "foo/#" is the fixture that has a
// companion at all; "#" alone does not.
func TestMQTTAFailedCompanionReclaimCleansUpBothHalves(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const filter = "readinesswc/#"
	// Published to the EXACT level, which is what the level-up companion serves.
	const exact = "readinesswc"

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "readinesswc"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: filter, qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "readinesswc-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// Unacknowledged, so the UNSUBSCRIBE drains the COMPANION's consumer.
	testMQTTPublish(t, pc, pr, 1, false, false, exact, 1, []byte("payload"))
	testMQTTCheckPubMsgNoAck(t, c, r, exact, mqttPubQos1, []byte("payload"))

	sess := testMQTTResidentSession(t, s, "readinesswc")
	testMQTTUnsub(t, 2, c, r, []*mqttFilter{{filter: filter}})
	testMQTTFlush(t, c, nil, r)

	dur, drainSid := testStripDrainDelivery(t, sess)
	// The premise: the drain must be the COMPANION's, or this exercises the main
	// insertion point again under a different name.
	if !strings.HasSuffix(drainSid, mqttMultiLevelSidSuffix) {
		t.Fatalf("the fixture must drain the LEVEL-UP companion sid, got %q", drainSid)
	}

	restore := setMQTTTestQOS12SubFailHook(func(durable string) error {
		if durable == dur {
			return errors.New("companion delivery subscription refused")
		}
		return nil
	})
	defer restore()

	testMQTTSub(t, 3, c, r, []*mqttFilter{{filter: filter, qos: 1}}, []byte{mqttSubAckFailure})
	testMQTTFlush(t, c, nil, r)

	sess.mu.Lock()
	stillDraining := sess.drains[drainSid] != nil
	companionAdopted := sess.cons[drainSid] != nil
	mainSid := strings.TrimSuffix(drainSid, mqttMultiLevelSidSuffix) + ".>"
	mainConsumer := sess.cons[mainSid]
	sess.mu.Unlock()
	if !stillDraining {
		t.Fatal("a failed companion reclaim must leave the companion drain where it found it")
	}
	if companionAdopted {
		t.Fatal("a failed companion reclaim must adopt nothing")
	}
	// The main half's consumer was CREATED before the companion failed. A bare continue
	// at the companion insertion point leaves it behind, referenced by a membership the
	// failed filter never gained.
	if mainConsumer != nil {
		t.Fatalf("a failed filter must clean up the main consumer it created, got %v", mainConsumer)
	}
	sc := sess.c
	sc.mu.Lock()
	mainRaw, companionRaw := sc.subs[mainSid] != nil, sc.subs[drainSid] != nil
	sc.mu.Unlock()
	if mainRaw || companionRaw {
		t.Fatalf("a failed filter must clean up BOTH raw subscriptions, got main=%v companion=%v",
			mainRaw, companionRaw)
	}
}

// TestMQTTAReSubscribeAfterAFailedDrainRestoreDelivers is producer R2 END TO END, and
// the reason it exists rather than another synthetic fixture: the roadmap now marks
// this route closed, and a hand-built fixture cannot establish that a RESUME produces
// the state the reclaim is being credited with repairing.
//
// The whole path runs: disconnect, reconnect, restoreDrains' own processQOS12Sub
// fails for this durable so it drops the reverse-index entry and keeps the drain,
// then a re-SUBSCRIBE reclaims it.
func TestMQTTAReSubscribeAfterAFailedDrainRestoreDelivers(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "r2route/topic"
	const control = "r2route/control"
	conn, sess, _ := testDrainSetup(t, s, o, "r2route", topic, 1)

	testMQTTSub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: control, qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)
	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)

	var dur string
	sess.mu.Lock()
	for _, d := range sess.drains {
		dur = d.cc.Durable
	}
	sess.mu.Unlock()
	if dur == "" {
		t.Fatal("the UNSUBSCRIBE must have produced a drain")
	}
	conn.c.Close()

	// THE PRODUCER: restoreDrains cannot install this durable's delivery subscription,
	// so it rolls back the registration and keeps sess.drains[sid] — the R2 state.
	var restoreFailed atomic.Bool
	restore := setMQTTTestQOS12SubFailHook(func(durable string) error {
		if durable == dur {
			restoreFailed.Store(true)
			return errors.New("restore refused")
		}
		return nil
	})
	// Deferred the moment it is installed: a t.Fatal in the reconnect or the
	// precondition below would otherwise leave a PROCESS-GLOBAL hook armed for every
	// test that follows. Restoring twice is harmless.
	defer restore()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "r2route"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess2 := testMQTTResidentSession(t, s, "r2route")
	// The hook stays armed until the producer has DEMONSTRABLY fired and left its state.
	testAwaitProducedState(t, sess2, dur, &restoreFailed, false)
	restore()

	// The re-SUBSCRIBE that reclaims it. At HEAD it is granted and then silent.
	testMQTTSubAwaitingSubAck(t, 3, c, r, topic, 1)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "r2route-pub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// Positive control first, so a failure below cannot be blamed on the connection.
	testMQTTPublish(t, pc, pr, 1, false, false, control, 20, []byte("control"))
	testMQTTCheckPubMsg(t, c, r, control, mqttPubQos1, []byte("control"))

	testMQTTPublish(t, pc, pr, 1, false, false, topic, 21, []byte("after-resub"))
	testMQTTCheckPubMsg(t, c, r, topic, mqttPubQos1, []byte("after-resub"))
}

// TestMQTTAQoS2DrainWithheldByPubRelSetupIsReclaimedOnceItRecovers is producer R1 END
// TO END, and it is the route the blanket withholding creates.
//
// When ensurePubRelConsumerSubscription fails during a resume, qos2DrainDurables puts
// EVERY QoS 2 drain into the skip set — including one holding only PUBLISH-phase
// claims, which no hold covers. That drain is then adoptable with no delivery
// subscription. PUBREL setup retries per SUBSCRIBE, so once it succeeds the filter
// reaches the reclaim.
func TestMQTTAQoS2DrainWithheldByPubRelSetupIsReclaimedOnceItRecovers(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	const topic = "r1route/topic"
	const control = "r1route/control"
	// QoS 2, and NO PUBREC is sent, so the drain carries a PUBLISH-phase claim with no
	// intent — the subset F-094's hold does NOT cover, which is what makes it adoptable.
	conn, sess, _ := testDrainSetup(t, s, o, "r1route", topic, 2)

	testMQTTSub(t, 1, conn.c, conn.r, []*mqttFilter{{filter: control, qos: 1}}, []byte{1})
	testMQTTFlush(t, conn.c, nil, conn.r)
	testMQTTUnsub(t, 2, conn.c, conn.r, []*mqttFilter{{filter: topic}})
	testMQTTFlush(t, conn.c, nil, conn.r)
	if n := testDrainCount(sess); n != 1 {
		t.Fatalf("expected one draining consumer, got %d", n)
	}
	var dur string
	sess.mu.Lock()
	for _, d := range sess.drains {
		dur = d.cc.Durable
	}
	sess.mu.Unlock()
	if dur == "" {
		t.Fatal("the UNSUBSCRIBE must have produced a drain")
	}
	conn.c.Close()

	// THE PRODUCER: PUBREL delivery cannot be established on this resume, so every
	// QoS 2 drain is withheld — this one included, despite owing no PUBREL.
	var pubRelFailed atomic.Bool
	restore := setMQTTTestPubRelSubFailHook(func() error {
		pubRelFailed.Store(true)
		return errors.New("pubrel subscription refused")
	})
	// Deferred immediately, and it matters more here than for R2: this hook is not
	// keyed by durable, so leaking it would refuse PUBREL setup for EVERY QoS 2 test
	// that runs afterwards.
	defer restore()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "r1route"}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	sess2 := testMQTTResidentSession(t, s, "r1route")
	// INDEXED with no subscription: R1 skips the durable in restoreDrains, so the entry
	// restoreDrainLocked made at record restore survives. Keying the reclaim's guard on
	// that entry is the production bug this test caught.
	testAwaitProducedState(t, sess2, dur, &pubRelFailed, true)
	// PUBREL setup recovers, which is what lets a QoS 2 filter past the first pass.
	restore()

	testMQTTSubAwaitingSubAck(t, 3, c, r, topic, 2)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "r1route-pub2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, pc, pr, 1, false, false, control, 20, []byte("control"))
	testMQTTCheckPubMsg(t, c, r, control, mqttPubQos1, []byte("control"))

	testMQTTPublish(t, pc, pr, 2, false, false, topic, 21, []byte("after-resub"))
	testMQTTCheckPubMsg(t, c, r, topic, mqttPubQoS2, []byte("after-resub"))
}
