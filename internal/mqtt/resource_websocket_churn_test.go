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
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

// wsChurnCycle runs one connection of each WebSocket shape the listener sees: a client that
// connects and disconnects cleanly, one that ends with a peer Close frame and no DISCONNECT, a
// refused upgrade, and a peer that stalls in the upgrade until the handshake timeout.
func wsChurnCycle(t *testing.T, wsAddr string, b, i int) {
	t.Helper()
	id := fmt.Sprintf("ws-churn-%d-%d", b, i)

	c := wsTestDial(t, wsAddr)
	_, _ = c.Write(wsConnect311(id, 30, "", ""))
	wsExpectConnAck(t, c)
	_, _ = c.Write([]byte{0xE0, 0x00})
	_ = c.Raw.Close()

	c2 := wsTestDial(t, wsAddr)
	_, _ = c2.Write(wsConnect311(id+"-closed", 30, "", ""))
	wsExpectConnAck(t, c2)
	_, _ = c2.Raw.Write(wsClose(wsStatus(1001)))
	_ = c2.Raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.Copy(io.Discard, c2.Raw)
	_ = c2.Raw.Close()

	if r, err := wsTestDialErr(wsAddr, [2]string{"Origin", "https://elsewhere.example"}); err == nil {
		r.Raw.Close()
		t.Fatal("a foreign origin was admitted")
	}

	raw, err := net.Dial("tcp", wsAddr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = raw.Write([]byte("GET /mqtt HTTP/1.1\r\n"))
	_ = raw.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.Copy(io.Discard, raw) // returns when the handshake timeout closes it
	_ = raw.Close()
}

// TestResourceWebSocketChurn: WebSocket connections of every shape, upgrades refused and
// upgrades abandoned leave the broker's goroutines at their idle set and its heap and
// descriptors without sustained growth. The adapter owns no goroutine and no timer outliving a
// call, so anything that grows here is a connection the teardown did not release.
func TestResourceWebSocketChurn(t *testing.T) {
	requireResourceSoak(t)
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = 4096
	t.Cleanup(func() { runtime.MemProfileRate = prevRate })
	prevJail, prevFlap, prevTTL := mqttSessJailDur.Get(), mqttFlapCleanItvl.Get(), mqttRetainedCacheTTL.Get()
	mqttSessJailDur.Set(50 * time.Millisecond)
	mqttFlapCleanItvl.Set(250 * time.Millisecond)
	mqttRetainedCacheTTL.Set(500 * time.Millisecond)
	t.Cleanup(func() {
		mqttSessJailDur.Set(prevJail)
		mqttFlapCleanItvl.Set(prevFlap)
		mqttRetainedCacheTTL.Set(prevTTL)
	})

	o := testMQTTDefaultOptions()
	o.MQTT.Websocket = WebsocketOpts{Host: "127.0.0.1", Port: -1, HandshakeTimeout: 200 * time.Millisecond}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	wsAddr := s.WSAddr().String()

	as := testMQTTGetGlobalASM(t, s)
	quiesce(t, s, as, nil)
	baseline := awaitIdleGoroutines(t)

	const blocks, perBlock = 4, 25 // block 0 is the warm-up
	var brokerHeaps []int64
	var fds []int
	for b := 0; b < blocks; b++ {
		for i := 0; i < perBlock; i++ {
			wsChurnCycle(t, wsAddr, b, i)
		}
		quiesce(t, s, as, nil)
		if b == 0 {
			continue
		}
		deadline := time.Now().Add(resourceSoakQuiesce)
		for {
			d := goroutineDiff(baseline, brokerGoroutines())
			if d == "" {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("block %d: broker goroutines did not return to the idle set:\n%s", b, d)
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		brokerHeaps = append(brokerHeaps, brokerHeapInuse())
		fds = append(fds, openFDs(t))
		t.Logf("block %d: broker heap in use ~%d bytes, %d descriptors", b, brokerHeaps[len(brokerHeaps)-1], fds[len(fds)-1])
	}
	const heapAllowance, fdAllowance = 8 << 20, 16
	if grew := brokerHeaps[len(brokerHeaps)-1] - brokerHeaps[0]; grew > heapAllowance {
		t.Errorf("broker-allocated heap in use grew ~%d bytes across %d blocks of churn (allowance %d)", grew, blocks-2, heapAllowance)
	}
	if grew := fds[len(fds)-1] - fds[0]; grew > fdAllowance {
		t.Errorf("open descriptors grew by %d across %d blocks (allowance %d)", grew, blocks-2, fdAllowance)
	}
}
