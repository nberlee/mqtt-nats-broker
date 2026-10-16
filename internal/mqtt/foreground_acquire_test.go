//go:build mqttport_fulltests

// Copyright 2026 The mqtt-nats-bridge Authors
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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A delivery's send-section acquisition is a FOREGROUND authority operation
// (outbound_foreground_priority.go). Before it announced itself, this broker's own background
// writers — the retained reclaim chain after a retained replay, the source retirement owner after a
// resume — won all three of its CAS attempts and the live delivery waited out AckWait
// (TestMQTTv5RetainHandling, TestMQTTReconcileChargeAccounting). These tests force the schedules
// instead of waiting for them.

// testAcquireRig is a v5 persistent subscriber on topic with the given AckWait, plus a publisher.
type testAcquireRig struct {
	s     *Server
	o     *Options
	sess  *mqttSession
	asm   *mqttAccountSessionManager
	id    string
	topic string
	c, cp net.Conn
	r, rp *mqttReader
}

func testAcquireRigUp(t *testing.T, id string, ackWait time.Duration) *testAcquireRig {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.AckWait = ackWait
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	rig := &testAcquireRig{s: s, o: o, id: id, topic: id + "/live"}
	rig.c, rig.r = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id, cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.c.Close() })
	testMQTTReadConnAckV5(t, rig.r)
	rig.cp, rig.rp = testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: id + "-pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { rig.cp.Close() })
	testMQTTReadConnAckV5(t, rig.rp)
	if rc := testMQTTSubV5(t, rig.c, rig.r, 1, []mqttV5SubFilter{{topic: rig.topic, opts: 1}}); len(rc) != 1 || rc[0] != 1 {
		t.Fatalf("SUBSCRIBE %s: %v", rig.topic, rc)
	}
	rig.sess = testMQTTResidentSession(t, s, id)
	rig.asm = rig.sess.asm
	return rig
}

// TestMQTTForegroundAcquireAnnouncesAcrossItsAttempts: while a live delivery's acquisition is in an
// attempt, a background admission waits, and it is granted once the acquisition returns.
func TestMQTTForegroundAcquireAnnouncesAcrossItsAttempts(t *testing.T) {
	rig := testAcquireRigUp(t, "fgacq-ann", 30*time.Second)

	parked := make(chan struct{})
	release := make(chan struct{})
	var parkOnce sync.Once
	presend := func(at *mqttAuthorityTransition) {
		if at.Attempt == nil || at.Attempt.ID != rig.id || at.Attempt.Op != "op-acquire-publish" {
			return
		}
		parkOnce.Do(func() {
			close(parked)
			<-release
		})
	}
	rig.asm.jsa.testTransitionPresend.Store(&presend)
	defer rig.asm.jsa.testTransitionPresend.Store(nil)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	waiting := make(chan struct{}, 1)
	wait := func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}
	rig.sess.testBackgroundWait.Store(&wait)
	defer rig.sess.testBackgroundWait.Store(nil)

	testMQTTPubV5Props(t, rig.cp, rig.rp, 1, false, 1, rig.topic, []byte("live"), nil)
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("the delivery's acquisition never reached its attempt")
	}

	admitted := make(chan struct{})
	go func() {
		rig.sess.admitBackgroundAuthority()
		close(admitted)
	}()
	select {
	case <-waiting:
	case <-admitted:
		t.Fatal("a background attempt was admitted while the foreground acquisition was in its attempt")
	case <-time.After(5 * time.Second):
		t.Fatal("the background admission neither waited nor returned")
	}
	select {
	case <-admitted:
		t.Fatal("a background attempt was admitted while the foreground acquisition was in its attempt")
	case <-time.After(300 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("the background admission was never granted after the acquisition returned")
	}
	testMQTTReadPubV5Props(t, rig.c, rig.r, rig.topic, []byte("live"))
}

// TestMQTTForegroundAcquireBudgetOutlastsTheBackgroundWriters: the acquisition survives one lost
// attempt per background writer and transmits at once, and one more loss than that is left to the
// redelivery. The losses are the production CAS rejection, injected at the commit, so the count is
// exact; each re-attempt is preceded by the shared contention wait.
func TestMQTTForegroundAcquireBudgetOutlastsTheBackgroundWriters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lose  int
		inBud bool
	}{
		{name: "one loss per background writer", lose: mqttBackgroundAuthorityWriters, inBud: true},
		{name: "one loss more", lose: mqttBackgroundAuthorityWriters + 1, inBud: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := testAcquireRigUp(t, "fgacq-bud", 2*time.Second)
			var attempts, waits atomic.Int32
			commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
				if at.Attempt == nil || at.Attempt.ID != rig.id || at.Attempt.Op != "op-acquire-publish" {
					return send()
				}
				if int(attempts.Add(1)) <= tc.lose {
					return 0, ApiErrors[JSStreamWrongLastSequenceErrF]
				}
				return send()
			}
			rig.asm.jsa.testTransitionCommit.Store(&commit)
			defer rig.asm.jsa.testTransitionCommit.Store(nil)
			rig.asm.mu.Lock()
			rig.asm.testAuthorityContentionWait = func(time.Duration) { waits.Add(1) }
			rig.asm.mu.Unlock()
			defer func() {
				rig.asm.mu.Lock()
				rig.asm.testAuthorityContentionWait = nil
				rig.asm.mu.Unlock()
			}()

			start := time.Now()
			testMQTTPubV5Props(t, rig.cp, rig.rp, 1, false, 1, rig.topic, []byte("live"), nil)
			if tc.inBud {
				testMQTTReadPubV5Props(t, rig.c, rig.r, rig.topic, []byte("live"))
				if el := time.Since(start); el >= rig.o.MQTT.AckWait {
					t.Fatalf("the delivery took %v, which is the redelivery's (AckWait %v), not the first offer's",
						el, rig.o.MQTT.AckWait)
				}
				if n := attempts.Load(); int(n) != tc.lose+1 {
					t.Fatalf("the acquisition made %d attempts, want %d (%d lost, then one landed)", n, tc.lose+1, tc.lose)
				}
				if w := waits.Load(); int(w) != tc.lose {
					t.Fatalf("%d contention waits before %d re-attempts", w, tc.lose)
				}
				return
			}
			if _, _, got := testMQTTReadPacketReady(rig.r, time.Second); got {
				t.Fatal("a delivery was transmitted although every attempt of its budget was lost")
			}
			if n := attempts.Load(); int(n) != mqttForegroundAcquireAttempts {
				t.Fatalf("the acquisition made %d attempts on the first offer, want its budget %d",
					n, mqttForegroundAcquireAttempts)
			}
			testMQTTReadPubV5Props(t, rig.c, rig.r, rig.topic, []byte("live"))
		})
	}
}

// TestMQTTForegroundAcquireRetainedStaysBackground: the retained driver's own acquisition is a
// BACKGROUND attempt — it waits at its admission while a foreground operation is announced, and it
// does not announce itself (if it did, it would then wait on its own announcement forever).
func TestMQTTForegroundAcquireRetainedStaysBackground(t *testing.T) {
	rig, c, r := testForegroundServer(t, "fgacq-bg", map[string][]byte{"lv/bg": []byte("kept")})

	parked := make(chan struct{})
	release := make(chan struct{})
	var parkOnce, releaseOnce sync.Once
	var acquiredWhileAnnounced, announced atomic.Bool
	commit := func(at *mqttAuthorityTransition, send func() (uint64, error)) (uint64, error) {
		seq, err := send()
		if at.Attempt == nil || at.Attempt.ID != rig.id {
			return seq, err
		}
		switch at.Attempt.Op {
		case "op-admit-retained":
			if err == nil {
				parkOnce.Do(func() {
					close(parked)
					<-release
				})
			}
		case "op-acquire-publish":
			if announced.Load() {
				acquiredWhileAnnounced.Store(true)
			}
		}
		return seq, err
	}
	rig.asm.jsa.testTransitionCommit.Store(&commit)
	defer rig.asm.jsa.testTransitionCommit.Store(nil)
	defer releaseOnce.Do(func() { close(release) })

	// The wait under test is the ACQUISITION's own admission, told apart by its stack.
	acquisitionWaited := make(chan struct{}, 1)
	wait := func() {
		buf := make([]byte, 64<<10)
		if strings.Contains(string(buf[:runtime.Stack(buf, false)]), "acquirePublishAction") {
			select {
			case acquisitionWaited <- struct{}{}:
			default:
			}
		}
	}
	rig.sess.testBackgroundWait.Store(&wait)
	defer rig.sess.testBackgroundWait.Store(nil)

	testMQTTWriteSubV5(t, c, 1, []mqttV5SubFilter{{topic: "lv/bg", opts: 1}})
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the retained replay never committed its admission")
	}
	announced.Store(true)
	releaseFg := rig.sess.announceForegroundAuthority()
	releaseOnce.Do(func() { close(release) })
	select {
	case <-acquisitionWaited:
	case <-time.After(5 * time.Second):
		releaseFg()
		t.Fatal("the retained acquisition never waited at its admission while a foreground operation was announced")
	}
	time.Sleep(200 * time.Millisecond)
	announced.Store(false)
	releaseFg()
	if acquiredWhileAnnounced.Load() {
		t.Fatal("the retained acquisition committed while a foreground operation was announced")
	}
	// Released, it proceeds: the SUBACK and the retained PUBLISH both arrive, in either order.
	var sawSubAck, sawPub bool
	deadline := time.Now().Add(10 * time.Second)
	for !(sawSubAck && sawPub) && time.Now().Before(deadline) {
		b, pl := testMQTTReadPacket(t, r)
		switch b & mqttPacketMask {
		case mqttPacketSubAck:
			testMQTTParseSubAckV5(t, r, pl, 1)
			sawSubAck = true
		case mqttPacketPub:
			tp, _, pi := testMQTTParsePubV5(t, r, b, pl)
			if tp != "lv/bg" {
				t.Fatalf("unexpected PUBLISH on %q", tp)
			}
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
			sawPub = true
		default:
			t.Fatalf("unexpected packet %x", b)
		}
	}
	if !sawPub {
		t.Fatal("the retained PUBLISH never arrived after the foreground released")
	}
}

// TestMQTTForegroundAcquireUnderTheRetainedReclaim is the RetainHandling schedule, forced: a retained
// copy's reclaim chain is mid-burst when a live delivery on the same subscription acquires. The
// background attempt admitted before the acquisition announced costs it exactly one attempt; the
// next background attempt waits at its admission; no other background attempt is admitted or
// commits inside the announced window; and the live PUBLISH arrives on its first offer.
//
// The admitted attempt is parked AT ITS ADMISSION, before its reads ("admit"), not at its batch:
// the delivery's own admission commits between the park and the acquisition, so an attempt parked
// past its reads would lose its CAS on release and collide with nothing.
func TestMQTTForegroundAcquireUnderTheRetainedReclaim(t *testing.T) {
	rig, c, r := testForegroundServer(t, "fgacq-reclaim", map[string][]byte{"lv/a": []byte("aa")})
	if rc := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "lv/a", opts: 1}}); len(rc) != 1 || rc[0] != 1 {
		t.Fatalf("SUBSCRIBE lv/a: %v", rc)
	}
	_, pi, _ := testMQTTReadPubV5Raw(t, r, "lv/a", []byte("aa"))

	sch := testForegroundScheduleUp(t, rig.id, rig.sess, &rig.asm.jsa, rig.asm, "admit")
	sch.mu.Lock()
	sch.fgOp, sch.armOn, sch.pinned = "op-acquire-publish", "op-admit-publish", false
	sch.mu.Unlock()
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	sch.awaitParked()

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: rig.id + "-live", cleanStart: true}, rig.o.MQTT.Host, rig.o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	start := time.Now()
	testMQTTPubV5Props(t, cp, rp, 1, false, 1, "lv/a", []byte("live"), nil)
	testMQTTReadPubV5Props(t, c, r, "lv/a", []byte("live"))
	if el := time.Since(start); el >= 10*time.Second {
		t.Fatalf("the live delivery took %v: it waited for a redelivery", el)
	}
	sch.verify(true)
}
