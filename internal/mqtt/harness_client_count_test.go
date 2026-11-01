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
	"net"
	"strconv"
	"testing"
)

// TestClientCountIgnoresASocketThatNeverPresentedConnect: the "previous connection is gone"
// precondition counts the MQTT clients a test connected, not every accepted socket. A stranger's
// dial, such as another process's late dial to a port this listener reused, is a socket that
// never presents a CONNECT; it must neither hold a count of zero open nor be counted in place of
// a client that did connect.
func TestClientCountIgnoresASocketThatNeverPresentedConnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	stranger, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, strconv.Itoa(o.MQTT.Port)))
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer stranger.Close()

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "count-me", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	checkClientsCount(t, s, 1)
	c.Close()
	checkClientsCount(t, s, 0)
}
