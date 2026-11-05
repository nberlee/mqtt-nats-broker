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

// A disconnected client is collected while its persistent session stays resident: nothing the
// session keeps — a resume cutoff, a pending exchange's origin or allocation fence, a finished
// retained replay — may keep the connection, its read buffer or its per-client NATS connection
// reachable. See https://github.com/nberlee/mqtt-nats-broker/issues/174.

import (
	"fmt"
	"net"
	"testing"
	"time"
	"weak"
)

const disconnectedClientConf = `
port: -1
host: "127.0.0.1"
jetstream: enabled
accounts {
  APP: {
    jetstream: enabled
    users: [
      { user: svc, password: svcpw, permissions: { publish: ">", subscribe: ">" } }
      { user: alice, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$MQTT.>"] }
          subscribe: { allow: [">"], deny: ["$MQTT.>"] }
      } }
    ]
  }
}
`

// collectedBroker is one broker under test and how a client reaches it.
type collectedBroker struct {
	s    *Server
	host string
	port int
}

func (b *collectedBroker) connect(t *testing.T, id string, clean bool) (net.Conn, *mqttReader) {
	t.Helper()
	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: id, cleanSess: clean, user: "alice", pass: "pw"}, b.host, b.port)
	return c, r
}

// collectedSharedBroker serves every client on the shared service connection.
func collectedSharedBroker(t *testing.T) *collectedBroker {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	return &collectedBroker{s: s, host: o.MQTT.Host, port: o.MQTT.Port}
}

// collectedPassthroughBroker dials a per-client NATS connection for each client.
func collectedPassthroughBroker(t *testing.T, url, node string) *collectedBroker {
	b := startPassthroughBroker(t, url, node)
	return &collectedBroker{s: b.s, host: b.o.MQTT.Host, port: b.o.MQTT.Port}
}

// clientRef names a connected client by weak pointer only, so the oracle cannot keep it.
func clientRef(t *testing.T, s *Server, id string) weak.Pointer[client] {
	t.Helper()
	return weak.Make(testMQTTGetClient(t, s, id))
}

// connectedClient reports whether s still lists a client with the given identifier.
func connectedClient(s *Server, ref weak.Pointer[client]) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clients {
		if weak.Make(c) == ref {
			return true
		}
	}
	return false
}

// requireResidentSession requires the session to stay in the account's maps: the point is
// that the client goes while the session is kept.
func requireResidentSession(t *testing.T, s *Server, id string) {
	t.Helper()
	as := testMQTTGetGlobalASM(t, s)
	as.mu.RLock()
	_, ok := as.sessions[id]
	as.mu.RUnlock()
	if !ok {
		t.Fatalf("the session of %q is not resident: the witness proves nothing", id)
	}
}

// awaitClientCollected waits for the client to leave the server, then requires it collected.
func awaitClientCollected(t *testing.T, s *Server, ref weak.Pointer[client]) {
	t.Helper()
	checkFor(t, 20*time.Second, 50*time.Millisecond, func() error {
		if connectedClient(s, ref) {
			return fmt.Errorf("the client is still registered")
		}
		return nil
	})
	awaitWeakCollected(t, "disconnected client", func() int {
		if ref.Value() != nil {
			return 1
		}
		return 0
	})
}

func subscribeQoS(t *testing.T, c net.Conn, r *mqttReader, qos byte, filters ...string) {
	t.Helper()
	var fs []*mqttFilter
	var want []byte
	for _, f := range filters {
		fs = append(fs, &mqttFilter{filter: f, qos: qos})
		want = append(want, qos)
	}
	testMQTTSub(t, 1, c, r, fs, want)
	testMQTTFlush(t, c, nil, r)
}

func publishFrom(t *testing.T, b *collectedBroker, id string, qos byte, retain bool, topic string, payload []byte) {
	t.Helper()
	pc, pr := b.connect(t, id, true)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pc, pr, qos, false, retain, topic, 1, payload)
}

func TestDisconnectedClientIsCollected(t *testing.T) {
	for _, mode := range []string{"shared", "passthrough"} {
		for _, graceful := range []bool{false, true} {
			name := fmt.Sprintf("%s/drop", mode)
			if graceful {
				name = fmt.Sprintf("%s/disconnect", mode)
			}
			t.Run(name, func(t *testing.T) {
				var b *collectedBroker
				if mode == "shared" {
					b = collectedSharedBroker(t)
				} else {
					b = collectedPassthroughBroker(t, serviceReleaseNATS(t, disconnectedClientConf), "coll-"+mode)
				}
				id := "coll-plain"
				c, r := b.connect(t, id, false)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				subscribeQoS(t, c, r, 1, "a/b", "c/d")
				ref := clientRef(t, b.s, id)
				if graceful {
					testMQTTDisconnect(t, c, nil)
				}
				c.Close()
				awaitClientCollected(t, b.s, ref)
				requireResidentSession(t, b.s, id)
			})
		}
	}

	t.Run("unacknowledged QoS 1 delivery", func(t *testing.T) {
		b := collectedPassthroughBroker(t, serviceReleaseNATS(t, disconnectedClientConf), "coll-q1")
		id := "coll-q1"
		c, r := b.connect(t, id, false)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		subscribeQoS(t, c, r, 1, "a/b")
		ref := clientRef(t, b.s, id)
		publishFrom(t, b, id+"-pub", 1, false, "a/b", []byte("held"))
		testMQTTCheckPubMsgNoAck(t, c, r, "a/b", mqttPubQos1, []byte("held"))
		c.Close()
		awaitClientCollected(t, b.s, ref)
		requireResidentSession(t, b.s, id)
	})

	t.Run("QoS 2 delivery awaiting PUBCOMP", func(t *testing.T) {
		b := collectedPassthroughBroker(t, serviceReleaseNATS(t, disconnectedClientConf), "coll-q2")
		id := "coll-q2"
		c, r := b.connect(t, id, false)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		subscribeQoS(t, c, r, 2, "a/b")
		ref := clientRef(t, b.s, id)
		publishFrom(t, b, id+"-pub", 2, false, "a/b", []byte("held"))
		pi := testMQTTCheckPubMsgNoAck(t, c, r, "a/b", mqttPubQoS2, []byte("held"))
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
		c.Close()
		awaitClientCollected(t, b.s, ref)
		requireResidentSession(t, b.s, id)
	})

	t.Run("completed retained replay", func(t *testing.T) {
		b := collectedPassthroughBroker(t, serviceReleaseNATS(t, disconnectedClientConf), "coll-ret")
		id := "coll-ret"
		publishFrom(t, b, id+"-pub", 1, true, "r/a", []byte("kept-a"))
		publishFrom(t, b, id+"-pub", 1, true, "r/b", []byte("kept-b"))
		c, r := b.connect(t, id, false)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		subscribeQoS(t, c, r, 1, "r/+")
		ref := clientRef(t, b.s, id)
		for range 2 {
			pi := testMQTTCheckPubMsgNoAck(t, c, r, "", mqttPubQos1|mqttPubFlagRetain, nil)
			testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
		}
		testMQTTFlush(t, c, nil, r)
		c.Close()
		awaitClientCollected(t, b.s, ref)
		requireResidentSession(t, b.s, id)
	})

	t.Run("takeover on the same broker", func(t *testing.T) {
		b := collectedPassthroughBroker(t, serviceReleaseNATS(t, disconnectedClientConf), "coll-take")
		id := "coll-take"
		c, r := b.connect(t, id, false)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		subscribeQoS(t, c, r, 1, "a/b")
		pred := clientRef(t, b.s, id)
		c2, r2 := b.connect(t, id, false)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
		succ := clientRef(t, b.s, id)
		if succ == pred {
			t.Fatal("the successor was not registered")
		}
		c.Close()
		awaitClientCollected(t, b.s, pred)
		if !connectedClient(b.s, succ) {
			t.Fatal("the successor left the server")
		}
	})

	t.Run("takeover across brokers", func(t *testing.T) {
		url := serviceReleaseNATS(t, disconnectedClientConf)
		a := collectedPassthroughBroker(t, url, "coll-xa")
		b := collectedPassthroughBroker(t, url, "coll-xb")
		id := "coll-cross"
		c, r := a.connect(t, id, false)
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		subscribeQoS(t, c, r, 1, "a/b")
		pred := clientRef(t, a.s, id)
		c2, r2 := b.connect(t, id, false)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
		c.Close()
		awaitClientCollected(t, a.s, pred)
	})
}

// A PUBLISH larger than the read buffer's ceiling arrives across reads of a growing buffer and
// is delivered intact.
func TestPublishLargerThanTheReadBufferCeiling(t *testing.T) {
	b := collectedSharedBroker(t)
	c, r := b.connect(t, "big-sub", true)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	subscribeQoS(t, c, r, 1, "big/t")
	payload := make([]byte, mqttReadBufMax*3+17)
	for i := range payload {
		payload[i] = byte(i)
	}
	publishFrom(t, b, "big-pub", 1, false, "big/t", payload)
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "big/t", mqttPubQos1, payload)
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
}
