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

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

// consumerMaxAckPending reads the push window of the $MQTT_msgs durable filtering subject.
func consumerMaxAckPending(t *testing.T, js nats.JetStreamContext, subject string) int {
	t.Helper()
	for ci := range js.ConsumersInfo("$MQTT_msgs") {
		if ci.Config.FilterSubject == subject {
			return ci.Config.MaxAckPending
		}
	}
	t.Fatalf("no $MQTT_msgs consumer filters %s", subject)
	return 0
}

// A broker restarted with a lower MaxAckPending keeps the existing durable's window, paces the
// restored session at its own lower cap, and creates new consumers with the new value. Nothing
// is migrated.
func TestMaxAckPendingLoweredAtRestartKeepsTheDurableWindow(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 4
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cisub := &mqttConnInfo{clientID: "lowered-sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	s.Shutdown()
	o.MQTT.Port = -1
	o.MQTT.MaxAckPending = 2
	testSetStoreDir(o, dir)
	s = testMQTTRunServer(t, o)
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "lowered-pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	for i := 1; i <= 3; i++ {
		testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, fmt.Appendf(nil, "m%d", i))
	}

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	if got := consumerMaxAckPending(t, js, "$MQTT.msgs.foo"); got != 4 {
		t.Fatalf("the existing durable's MaxAckPending is %d after the restart, want its own 4", got)
	}
	mc := testMQTTGetClient(t, s, cisub.clientID)
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()
	sess.mu.Lock()
	maxp := sess.maxp
	sess.mu.Unlock()
	if maxp != 2 {
		t.Fatalf("the restored session's in-flight cap is %d, want the running broker's 2", maxp)
	}

	pi1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("m1"))
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("m2"))
	testMQTTExpectNothing(t, r)
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi1)
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("m3"))

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	if got := consumerMaxAckPending(t, js, "$MQTT.msgs.bar"); got != 2 {
		t.Fatalf("a consumer created after the restart has MaxAckPending %d, want 2", got)
	}
}
