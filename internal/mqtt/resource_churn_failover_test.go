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

//go:build mqtt_fulltests

package mqtt

// A session object a broker removed without clearing it — a takeover through another broker
// — may keep working while the work it holds is unresolved (docs/CONFORMANCE.md states that
// retention and its conditional end). What the residual does NOT allow is an object still
// reachable once its work has completed and its generation's ledger is quiescent: that is a
// leak. These tests create removed objects that really hold work, complete the work, and then
// require every one of them to become unreachable.

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"
)

// removedSessions captures, weakly, every object of one client ID (every object, for an
// empty ID) that leaves an account's maps, with the generation it served.
type removedSessions struct {
	mu   sync.Mutex
	id   string
	ptrs []weak.Pointer[mqttSession]
	gens []string
	// onCapture runs for each capture (outside the hook's critical section is not
	// guaranteed: it must not block or take locks).
	onCapture func(*mqttSession)
}

func captureRemovedSessions(t *testing.T, id string) *removedSessions {
	t.Helper()
	rs := &removedSessions{id: id}
	hook := func(sess *mqttSession) {
		if id != "" && sess.id != id {
			return
		}
		rs.mu.Lock()
		rs.ptrs = append(rs.ptrs, weak.Make(sess))
		// The caller may hold as.mu but never sess.mu here (removeSession's callers do not
		// hold it), so the generation is read under the session's own lock.
		sess.mu.Lock()
		rs.gens = append(rs.gens, sess.gen)
		sess.mu.Unlock()
		cb := rs.onCapture
		rs.mu.Unlock()
		if cb != nil {
			cb(sess)
		}
	}
	mqttTestSessionRemovedV.Store(&hook)
	t.Cleanup(func() { mqttTestSessionRemovedV.Store(nil) })
	return rs
}

// awaitCollected requires every captured object to become unreachable within d.
func (rs *removedSessions) awaitCollected(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		runtime.GC()
		runtime.GC()
		rs.mu.Lock()
		alive := 0
		for _, p := range rs.ptrs {
			if p.Value() != nil {
				alive++
			}
		}
		n := len(rs.ptrs)
		rs.mu.Unlock()
		if alive == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d removed session object(s) of %q are still reachable after their work completed",
				alive, n, rs.id)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// rawSession is a persistent MQTT 3.1.1 client the test drives packet by packet, so it can
// leave exchanges open on purpose.
type rawSession struct {
	t  *testing.T
	c  net.Conn
	r  *mqttReader
	id string
}

func dialRawSession(t *testing.T, o *Options, id string, present bool) *rawSession {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, present)
	return &rawSession{t: t, c: c, r: r, id: id}
}

// completeAll answers everything that arrives until the connection is quiet for `quiet`:
// PUBACK a QoS 1 PUBLISH, PUBREC a QoS 2 PUBLISH, PUBCOMP a PUBREL. Returns how many packets
// it handled.
func (rs *rawSession) completeAll(quiet time.Duration) int {
	n := 0
	for {
		b, _, ok := testMQTTReadPacketReady(rs.r, quiet)
		if !ok {
			return n
		}
		n++
		switch b & mqttPacketMask {
		case mqttPacketPub:
			qos := (b & mqttPubFlagQoS) >> 1
			if _, err := rs.r.readString("topic"); err != nil {
				rs.t.Fatal(err)
			}
			var pi uint16
			if qos > 0 {
				pi, _ = rs.r.readUint16("pi")
			}
			rs.r.pos = len(rs.r.buf) // the payload is not inspected
			switch qos {
			case 1:
				testMQTTSendPIPacket(mqttPacketPubAck, rs.t, rs.c, pi)
			case 2:
				testMQTTSendPIPacket(mqttPacketPubRec, rs.t, rs.c, pi)
			}
		case mqttPacketPubRel:
			pi, _ := rs.r.readUint16("pi")
			testMQTTSendPIPacket(mqttPacketPubComp, rs.t, rs.c, pi)
		default:
			rs.r.pos = len(rs.r.buf)
		}
	}
}

// openExchanges leaves one QoS 1 delivery unacknowledged and one QoS 2 exchange held after
// PUBREC (its PUBREL unanswered). Returns the two identifiers.
func (rs *rawSession) openExchanges(pub net.Conn, pr *mqttReader, k int) (q1, q2 uint16) {
	t := rs.t
	testMQTTPublish(t, pub, pr, 1, false, false, "fo/q1", uint16(2*k+1), []byte(fmt.Sprint(k)))
	testMQTTPublish(t, pub, pr, 2, false, false, "fo/q2", uint16(2*k+2), []byte(fmt.Sprint(k)))
	for q1 == 0 || q2 == 0 {
		b, _, ok := testMQTTReadPacketReady(rs.r, 10*time.Second)
		if !ok {
			t.Fatalf("cycle %d: the deliveries did not arrive (q1=%d q2=%d)", k, q1, q2)
		}
		switch b & mqttPacketMask {
		case mqttPacketPub:
			qos := (b & mqttPubFlagQoS) >> 1
			topic, _ := rs.r.readString("topic")
			pi, _ := rs.r.readUint16("pi")
			rs.r.pos = len(rs.r.buf)
			if qos == 1 && topic == "fo/q1" {
				q1 = pi // left unacknowledged
			} else if qos == 2 && topic == "fo/q2" {
				q2 = pi
				testMQTTSendPIPacket(mqttPacketPubRec, t, rs.c, pi)
			}
		case mqttPacketPubRel:
			rs.r.pos = len(rs.r.buf) // left unanswered: the exchange stays held after PUBREC
		default:
			rs.r.pos = len(rs.r.buf)
		}
	}
	return q1, q2
}

func residentSessionOn(t *testing.T, s *Server, id string) *mqttSession {
	t.Helper()
	var sess *mqttSession
	checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
		as := testMQTTGetGlobalASM(t, s)
		as.mu.RLock()
		sess = as.sessions[id]
		as.mu.RUnlock()
		if sess == nil {
			return fmt.Errorf("no resident %q", id)
		}
		return nil
	})
	return sess
}

func TestResourceChurnFailover(t *testing.T) {
	requireResourceSoak(t)
	if os.Getenv("MQTTNB_CLUSTER_FAILOVER_TESTS") != "1" {
		t.Skip("set MQTTNB_CLUSTER_FAILOVER_TESTS=1 to run cluster failover tests")
	}
	const id = "fo-client"
	c := testNewJetStreamCluster(t, 3)
	ackWait := func(o *Options) { o.MQTT.AckWait = 2 * time.Second }
	brokers := []*testClusterBroker{c.startBroker(t, 0, ackWait), c.startBroker(t, 1, ackWait)}

	// The floor is held on every object of this session until phase B releases it, so the
	// inventory the test builds is observed before the floor reclaims it.
	var floorHeld atomic.Bool
	floorHeld.Store(true)
	holdFloor := func(sess *mqttSession) {
		sess.mu.Lock()
		sess.testCustodyDriver = &mqttCustodyDriverSeams{floorHeld: func(string) bool { return floorHeld.Load() }}
		sess.mu.Unlock()
	}
	removed := captureRemovedSessions(t, id)

	var gen string
	ticks := 0
	const cycles = 20
	for k := 0; k < cycles; k++ {
		b := brokers[k%2]
		cs := dialRawSession(t, b.opts, id, k > 0)
		if k == 0 {
			testMQTTSub(t, 1, cs.c, cs.r, []*mqttFilter{{filter: "fo/q1", qos: 1}, {filter: "fo/q2", qos: 2}}, []byte{1, 2})
		}
		sess := residentSessionOn(t, b.s, id)
		holdFloor(sess)
		// What the previous broker's object owed is completed here, on its successor.
		cs.completeAll(2 * time.Second)

		pub, pr := testMQTTConnect(t, &mqttConnInfo{clientID: fmt.Sprintf("fo-pub-%d", k), cleanSess: true},
			b.opts.MQTT.Host, b.opts.MQTT.Port)
		testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
		q1, q2 := cs.openExchanges(pub, pr, k)
		pub.Close()

		// THE RETAINING ROOTS, asserted rather than inferred: the object the next takeover
		// displaces holds a pending PUBLISH and a pending PUBREL.
		// The PUBREC is processed asynchronously, so the PUBREL entry is awaited.
		checkFor(t, 10*time.Second, 25*time.Millisecond, func() error {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			gen = sess.gen
			_, hasQ1 := sess.pendingPublish[q1]
			_, hasQ2 := sess.pendingPubRel[q2]
			if !hasQ1 || !hasQ2 {
				return fmt.Errorf("cycle %d: the object holds no open work (QoS 1 pending %v, QoS 2 PUBREL pending %v); "+
					"its displacement would prove nothing", k, hasQ1, hasQ2)
			}
			return nil
		})
		// A custody tick is armed only while a retry, withdrawal, demand or floor item is due,
		// not merely because exchanges are held, so it is recorded rather than required.
		b.s.willDispatchMu.Lock()
		for key := range b.s.willTimers {
			if strings.HasPrefix(key, "$MQTT_custody_tick."+id+".") {
				ticks++
				break
			}
		}
		b.s.willDispatchMu.Unlock()
		cs.c.Close()

		if k == cycles/2 {
			leader := c.waitStreamLeader(t, testGlobalAccount, mqttSessStreamName)
			c.stopNode(leader)
			c.waitStreamLeader(t, testGlobalAccount, mqttSessStreamName)
			c.restartNode(t, leader)
			c.waitStreamCurrent(t, testGlobalAccount, mqttSessStreamName)
		}
	}

	// Phase A ends on brokers[0]: complete the last cycle's work, then build floor inventory
	// with the floor held.
	final := brokers[0]
	cs := dialRawSession(t, final.opts, id, true)
	sess := residentSessionOn(t, final.s, id)
	holdFloor(sess)
	cs.completeAll(3 * time.Second)
	pub, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "fo-pub-floor", cleanSess: true}, final.opts.MQTT.Host, final.opts.MQTT.Port)
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	for i := 0; i < 600; i++ {
		testMQTTPublish(t, pub, pr, 1, false, false, "fo/q1", uint16(1+i%60000), []byte("floor"))
		pi := testMQTTCheckPubMsgNoAck(t, cs.c, cs.r, "fo/q1", mqttPubQos1, []byte("floor"))
		testMQTTSendPIPacket(mqttPacketPubAck, t, cs.c, pi)
	}
	pub.Close()
	as := testMQTTGetGlobalASM(t, final.s)
	readLedger := func() *mqttGenLedger {
		led, _, err := as.jsa.loadGenLedger(id, as.sessStreamEpoch, gen)
		if err != nil || led == nil {
			t.Fatalf("reading the generation ledger: %+v %v", led, err)
		}
		return led
	}
	if led := readLedger(); mqttLedgerSpan(led) <= mqttFloorSlack {
		t.Fatalf("with the floor held the ledger spans %d, not above the slack %d: the floor is not exercised",
			mqttLedgerSpan(led), mqttFloorSlack)
	}

	// Phase B: release, and require floor quiescence by direct ledger reads. Quiescence is
	// not reclamation of every exchange (up to the slack legitimately remains), which is why
	// the client side completed every exchange above.
	floorHeld.Store(false)
	cs.completeAll(2 * time.Second)
	checkFor(t, 180*time.Second, time.Second, func() error {
		led := readLedger()
		if led.Del != led.Lo || mqttLedgerSpan(led) > mqttFloorSlack {
			return fmt.Errorf("ledger not quiescent: Lo=%d Hi=%d Del=%d", led.Lo, led.Hi, led.Del)
		}
		return nil
	})

	// Phase C: every removed object, all of this generation, unreachable; nothing owed on
	// either broker.
	removed.mu.Lock()
	for i, g := range removed.gens {
		if g != gen {
			t.Errorf("removed object %d served generation %q, not the session's %q", i, g, gen)
		}
	}
	captured := len(removed.ptrs)
	removed.mu.Unlock()
	if captured < cycles-1 {
		t.Fatalf("only %d removed objects captured over %d takeovers", captured, cycles)
	}
	t.Logf("%d of %d displaced objects had a custody tick armed when displaced", ticks, cycles)
	removed.awaitCollected(t, 120*time.Second)
	for i, b := range brokers {
		rc := countResources(b.s, testMQTTGetGlobalASM(t, b.s))
		if rc.owedDrains+rc.owedIntents+rc.owedDisposals+rc.owedActions+rc.owedTerminals+rc.owedRetained != 0 {
			t.Errorf("broker %d still owes work for completed exchanges: %+v", i, rc)
		}
	}
	cs.c.Close()
}
