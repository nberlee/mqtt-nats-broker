//go:build mqttport_wstests

// Copyright 2020-2026 The NATS Authors
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

// Upstream MQTT-over-WebSocket tests kept as an unbuilt reference: this
// transport is not implemented and the production accept path has no WebSocket
// hook, and these tests also depend on the unported config-file
// machinery (createConfFile, RunServerWithConfig, reloadUpdateConfig). See
// docs/PORTING.md.

package mqtt

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
)

type mqttWrapAsWs struct {
	net.Conn
	t   testing.TB
	br  *bufio.Reader
	tmp []byte
}

func (c *mqttWrapAsWs) Write(p []byte) (int, error) {
	proto := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, p)
	return c.Conn.Write(proto)
}

func (c *mqttWrapAsWs) Read(p []byte) (int, error) {
	for {
		if n := len(c.tmp); n > 0 {
			if len(p) < n {
				n = len(p)
			}
			copy(p, c.tmp[:n])
			c.tmp = c.tmp[n:]
			return n, nil
		}
		c.tmp = testWSReadFrame(c.t, c.br)
	}
}

func TestMQTTWebsocketToMQTTPort(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	ci := &mqttConnInfo{cleanSess: true, ws: true}
	if _, _, err := testMQTTConnectRetryWithError(t, ci, o.MQTT.Host, o.MQTT.Port, 0); err == nil {
		t.Fatal("Expected error during connect")
	}
	select {
	case e := <-l.errCh:
		if !strings.Contains(e, errMQTTHTTPRequestOnMQTTListener.Error()) {
			t.Fatalf("Unexpected error: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("No error regarding wrong port")
	}
}

func TestMQTTWebsocket(t *testing.T) {
	tdir := t.TempDir()
	template := `
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		accounts {
			MQTT {
				jetstream: enabled
				users [
					{user: "mqtt", pass: "pwd", connection_types: ["%s"%s]}
				]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`
	s, o, conf := runReloadServerWithContent(t, []byte(fmt.Sprintf(template, tdir, jwt.ConnectionTypeMqtt, "")))
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", user: "mqtt", pass: "pwd", ws: true}
	c, r := testMQTTConnect(t, cisub, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCNotAuthorized, false)
	c.Close()

	ws := fmt.Sprintf(`, "%s"`, jwt.ConnectionTypeMqttWS)
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, tdir, jwt.ConnectionTypeMqtt, ws))

	cisub = &mqttConnInfo{clientID: "sub", user: "mqtt", pass: "pwd", ws: true}
	c, r = testMQTTConnect(t, cisub, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	cipub := &mqttConnInfo{clientID: "pub", user: "mqtt", pass: "pwd", ws: true}
	cp, rp := testMQTTConnect(t, cipub, o.Websocket.Host, o.Websocket.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
}

func TestMQTTWebsocketTLS(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir = %q
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: '../test/configs/certs/server-cert.pem'
				key_file: '../test/configs/certs/server-key.pem'
				ca_file: '../test/configs/certs/ca.pem'
			}
		}
	`, t.TempDir())))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", ws: true, tls: true}, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", ws: true, tls: true}, o.Websocket.Host, o.Websocket.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
}
