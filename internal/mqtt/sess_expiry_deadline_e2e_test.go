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
	"sync/atomic"
	"testing"
	"time"
)

// testWaitEarlyInSecond waits until the wall clock is within the first `within` of a second.
func testWaitEarlyInSecond(within time.Duration) {
	for {
		now := time.Now()
		frac := time.Duration(now.Nanosecond())
		if frac < within {
			return
		}
		time.Sleep(time.Second - frac + time.Millisecond)
	}
}

// testClosedSessionArmed waits until a disconnected client's session has its expiry timer armed —
// the close's last step, after it stamped the disconnect — and returns the session.
func testClosedSessionArmed(t *testing.T, s *Server, probe, cid string) *mqttSession {
	t.Helper()
	asm := testMQTTGetClient(t, s, probe).mqtt.asm
	var es *mqttSession
	checkFor(t, 5*time.Second, 5*time.Millisecond, func() error {
		asm.mu.Lock()
		defer asm.mu.Unlock()
		if _, ok := asm.pendingExpiries[getHash(cid)]; !ok {
			return fmt.Errorf("no expiry armed for %q yet", cid)
		}
		es = asm.sessByHash[getHash(cid)]
		return nil
	})
	if es == nil {
		t.Fatalf("fixture: no resident session for %q", cid)
	}
	return es
}

// TestMQTTv5SessionResumedJustBeforeItsIntervalElapses: a session whose disconnect stamp reads
// exactly its interval ago may have disconnected less than its interval ago, and is resumed; one
// whose stamp is well past its interval is not.
func TestMQTTv5SessionResumedJustBeforeItsIntervalElapses(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "sbprobe", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	asm := testMQTTGetClient(t, s, "sbprobe").mqtt.asm

	// disconnectStamped connects a 5 s session, closes it, waits for its close to finish, cancels
	// its timer and stamps its disconnect `ago` whole seconds before now.
	disconnectStamped := func(cid string, ago int64) int64 {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
			props: mqttV5ConnPropsSessionExpiry(5)}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "sb/" + cid, opts: 1}})
		c.Close()
		es := testClosedSessionArmed(t, s, "sbprobe", cid)
		testWaitEarlyInSecond(300 * time.Millisecond)
		stamp := time.Now().Unix() - ago
		asm.mu.Lock()
		asm.cancelSessionExpiryLocked(getHash(cid))
		asm.mu.Unlock()
		es.mu.Lock()
		es.disconnectedAt = stamp
		es.mu.Unlock()
		return stamp
	}
	reconnect := func(cid string) bool {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid,
			props: mqttV5ConnPropsSessionExpiry(5)}, o.MQTT.Host, o.MQTT.Port)
		sp, _, _ := testMQTTReadConnAckV5(t, r)
		if _, err := c.Write(mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
			t.Fatal(err)
		}
		c.Close()
		return sp
	}

	conclusive := false
	for attempt := 0; attempt < 5 && !conclusive; attempt++ {
		cid := fmt.Sprintf("sbnd%d", attempt)
		stamp := disconnectStamped(cid, 5)
		sp := reconnect(cid)
		// CONCLUSIVE only if the decision fell inside the stamp's fifth second: one second later the
		// conservative deadline has passed and discarding is correct.
		if time.Now().Unix() != stamp+5 {
			continue
		}
		conclusive = true
		if !sp {
			t.Fatalf("a session whose stamp reads exactly its interval ago was discarded: it may have " +
				"disconnected less than its interval ago")
		}
	}
	if !conclusive {
		t.Fatalf("fixture: no attempt reconnected within the stamp's interval second")
	}

	disconnectStamped("sbpast", 7)
	if reconnect("sbpast") {
		t.Fatalf("a session whose interval elapsed two seconds ago was resumed")
	}
}

// TestMQTTv5SessionExpiryTimerFiresAtTheConservativeDeadline: the close arms a session's expiry
// timer to the deadline its fire re-reads, so the fire is never early: it never takes the not-due
// return, and the session is cleaned up with no reconnect and no sweep.
func TestMQTTv5SessionExpiryTimerFiresAtTheConservativeDeadline(t *testing.T) {
	swept := make(chan struct{}, 1)
	restore := setMQTTTestSessExpirySweepPassHook(func(*mqttAccountSessionManager) {
		select {
		case swept <- struct{}{}:
		default:
		}
	})
	defer restore()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "sfprobe", cleanStart: true,
		props: mqttV5ConnPropsSessionExpiry(0)}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)
	// THE STARTUP SWEEP HAS RUN, so it cannot re-arm a timer this test relies on not being there.
	select {
	case <-swept:
	case <-time.After(30 * time.Second):
		t.Fatal("fixture: the startup expiry sweep never ran")
	}

	var notDue atomic.Int32
	var target atomic.Value
	h := func(idHash string) {
		if v, _ := target.Load().(string); v == idHash {
			notDue.Add(1)
		}
	}
	mqttTestExpiryNotDueHookV.Store(&h)
	defer mqttTestExpiryNotDueHookV.Store(nil)

	conclusive := false
	for attempt := 0; attempt < 5 && !conclusive; attempt++ {
		cid := fmt.Sprintf("sfire%d", attempt)
		target.Store(getHash(cid))
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true,
			props: mqttV5ConnPropsSessionExpiry(1)}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "sf/" + cid, opts: 1}})
		testWaitEarlyInSecond(300 * time.Millisecond)
		c.Close()
		es := testClosedSessionArmed(t, s, "sfprobe", cid)
		armedBy := time.Now()
		es.mu.Lock()
		stamp := es.disconnectedAt
		es.mu.Unlock()
		// CONCLUSIVE only if the timer was armed in the first half of the stamp's second: a relative
		// one-second timer would then fire at least half a second before the conservative deadline.
		early := armedBy.Before(time.Unix(stamp, int64(500*time.Millisecond)))
		deadline := mqttSessionExpiryDeadline(stamp, 1)
		// THE PREMATURE RETURN IS THE FAILURE, reported the moment it happens: a fire earlier than
		// the deadline it re-reads returns not due and leaves the cleanup to the periodic sweep.
		for limit := deadline.Add(1500 * time.Millisecond); ; {
			if k := notDue.Load(); k != 0 {
				t.Fatalf("the expiry of %q fired before its deadline and returned not due %d time(s)", cid, k)
			}
			if !testMQTTSessionRecordExists(t, s, "sfprobe", cid) && !testMQTTHasPendingExpiry(t, s, "sfprobe", cid) {
				break
			}
			if time.Now().After(limit) {
				t.Fatalf("the session of %q was not cleaned up within 1.5 s of its deadline", cid)
			}
			time.Sleep(20 * time.Millisecond)
		}
		conclusive = early
	}
	if !conclusive {
		t.Fatalf("fixture: no attempt armed its timer within the first half of its stamp's second")
	}
}
