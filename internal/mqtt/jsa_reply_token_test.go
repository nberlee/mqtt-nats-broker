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
	"strings"
	"testing"
)

// A JetStream reply is routed by the kind token at a fixed position after the reply
// prefix, so the token between "$MQTT.JSA." and the kind must be exactly one subject
// token. The node name is a host name, and a host name may carry dots: a broker on a
// host named like "a.b.c" read its reply kind from the wrong token and timed out every
// JetStream request. The reply token is the run's own identity instead, and the node
// name keeps its other job, the stable identity records are stamped with.
func TestADottedNodeNameServesJetStream(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	testSetServerName(o, "a.b.c")
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account session manager: %v", err)
	}
	if asm.jsa.id != "a.b.c" {
		t.Fatalf("the records' origin is %q, want the node name %q", asm.jsa.id, "a.b.c")
	}
	token := strings.TrimSuffix(strings.TrimPrefix(asm.jsa.rplyr, mqttJSARepliesPrefix), ".")
	if token == _EMPTY_ || strings.ContainsAny(token, ".*> \t") {
		t.Fatalf("the reply prefix %q does not end in exactly one subject token", asm.jsa.rplyr)
	}

	other := testMQTTDefaultOptions()
	testSetServerName(other, "a.b.c")
	s2 := testMQTTRunServer(t, other)
	defer testMQTTShutdownServer(s2)
	asm2, err := s2.getOrCreateMQTTAccountSessionManager(s2.createInternalAccountClient())
	if err != nil {
		t.Fatalf("second account session manager: %v", err)
	}
	if asm2.jsa.rplyr == asm.jsa.rplyr {
		t.Fatalf("two runs on one node name share the reply prefix %q", asm.jsa.rplyr)
	}

	// A stored QoS 1 message and a resumed session both need JetStream answers.
	sub, subR := testMQTTConnect(t, &mqttConnInfo{clientID: "dotted-sub"}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, subR, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, sub, subR, []*mqttFilter{{filter: "dotted/a", qos: 1}}, []byte{1})
	testMQTTFlush(t, sub, nil, subR)
	sub.Close()

	pub, pubR := testMQTTConnect(t, &mqttConnInfo{clientID: "dotted-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pub.Close()
	testMQTTCheckConnAck(t, pubR, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, pub, pubR, 1, false, false, "dotted/a", 1, []byte("held"))

	sub, subR = testMQTTConnect(t, &mqttConnInfo{clientID: "dotted-sub"}, o.MQTT.Host, o.MQTT.Port)
	defer sub.Close()
	testMQTTCheckConnAck(t, subR, mqttConnAckRCConnectionAccepted, true)
	pi := testMQTTCheckPubMsg(t, sub, subR, "dotted/a", mqttPubQos1, []byte("held"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, sub, pi)
}
