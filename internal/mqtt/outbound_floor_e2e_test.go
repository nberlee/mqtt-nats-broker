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

//go:build mqttport_fulltests

package mqtt

import (
	"net"
	"testing"
	"time"
)

// THE FLOOR, END TO END (F-112): real clients, real deliveries, the custody dispatcher's floor
// unit doing the work.

type testF112Sub struct {
	c    net.Conn
	r    *mqttReader
	sess *mqttSession
}

// testF112Subscribe connects a persistent QoS 1 subscriber and returns it with its resident session.
func testF112Subscribe(t *testing.T, s *Server, o *Options, cid, topic string, resume bool) *testF112Sub {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: cid, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, resume)
	if !resume {
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: topic, qos: 1}}, []byte{1})
	}
	testMQTTFlush(t, c, nil, r)
	asm := testMQTTGetAccountSessionManager(t, s, cid)
	asm.mu.RLock()
	sess := asm.sessions[cid]
	asm.mu.RUnlock()
	if sess == nil {
		t.Fatalf("no resident session for %q", cid)
	}
	return &testF112Sub{c: c, r: r, sess: sess}
}

// testF112Deliver publishes n QoS 1 messages and has the subscriber acknowledge each.
func testF112Deliver(t *testing.T, o *Options, sub *testF112Sub, topic string, n int) {
	t.Helper()
	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "f112-pub-" + mqttNewInflightNonce(), cleanSess: true},
		o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	for i := 0; i < n; i++ {
		testMQTTPublish(t, pc, pr, 1, false, false, topic, uint16(i%60000+1), []byte("x"))
		_, pi := testMQTTGetPubMsg(t, sub.c, sub.r, topic, []byte("x"))
		testMQTTSendPIPacket(mqttPacketPubAck, t, sub.c, pi)
	}
}

// testF112SessLedger reads the session's generation ledger.
func testF112SessLedger(t *testing.T, sess *mqttSession) *mqttGenLedger {
	t.Helper()
	sess.mu.Lock()
	gen, id, jsa, asm := sess.gen, sess.id, sess.jsa, sess.asm
	sess.mu.Unlock()
	led, _, err := jsa.loadGenLedger(id, asm.sessStreamEpoch, gen)
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	return led
}

// T15. A healthy, continuously connected subscriber advances its floor with no reconnect and no
// census requested: its own completions arm the floor unit, which resolves the duties from the
// inventory, reclaims and deletes them, and prunes the ended-lineage veto.
func TestF112AHealthySessionAdvancesItsFloorWithoutAReconnect(t *testing.T) {
	const n = 600
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	sub := testF112Subscribe(t, s, o, "f112-t15", "f112/t15", false)
	defer sub.c.Close()

	testF112Deliver(t, o, sub, "f112/t15", n)

	deadline := time.Now().Add(30 * time.Second)
	var led *mqttGenLedger
	for time.Now().Before(deadline) {
		led = testF112SessLedger(t, sub.sess)
		if led != nil && led.Lo > n-mqttFloorSlack && led.Del == led.Lo {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if led == nil || led.Lo <= n-mqttFloorSlack || led.Del != led.Lo {
		t.Fatalf("after %d completed deliveries the floor did not advance and delete on its own: %+v", n, led)
	}
	if span := mqttLedgerSpan(led); span > mqttFloorSlack+8 {
		t.Fatalf("the inventory's span stayed %d after the floor advanced", span)
	}
	sub.sess.mu.Lock()
	vetoed := len(sub.sess.custody.endedLineages)
	sub.sess.mu.Unlock()
	if vetoed >= n/2 {
		t.Fatalf("the ended-lineage veto still holds %d of %d lineages", vetoed, n)
	}
}

// testF112LogPositions logs the first k positions of the inventory and how the floor classifies
// them, for a failure report.
func testF112LogPositions(t *testing.T, sess *mqttSession, led *mqttGenLedger, k int) {
	t.Helper()
	if led == nil {
		return
	}
	sess.mu.Lock()
	gen, id, jsa, asm := sess.gen, sess.id, sess.jsa, sess.asm
	sess.mu.Unlock()
	for ord := led.Lo; ord < led.Lo+uint64(k) && ord <= led.Hi; ord++ {
		d, _, derr := jsa.loadDutyEntryAt(id, asm.sessStreamEpoch, gen, ord)
		if derr != nil || d == nil {
			t.Logf("position %d: unreadable %v", ord, derr)
			continue
		}
		cls, cerr := jsa.classifyFloorPosition(id, asm.sessStreamEpoch, gen, d)
		var phase mqttExchangePhase
		if d.Kind == mqttDutyExchange {
			if x, _, _ := jsa.loadOutboundExchange(id, asm.sessStreamEpoch, gen, d.Ref); x != nil {
				phase = x.Phase
				if rec, _, _ := jsa.loadInflightRecord(mqttInflightSubject(getHash(id), gen, x.PI)); rec != nil {
					t.Logf("position %d register at %d: lin %q state %q", ord, x.PI, rec.Lin, rec.State)
				}
			}
		}
		t.Logf("position %d: kind %s state %d phase %d class %d (%v)", ord, d.Kind, d.State, phase, cls, cerr)
	}
}
