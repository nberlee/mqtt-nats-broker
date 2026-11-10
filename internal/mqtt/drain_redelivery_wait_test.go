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
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// testFatalRecorder stands in for a test so a helper's refusal can be observed: Fatal and
// Fatalf record the message and end the calling goroutine, as the real ones do.
type testFatalRecorder struct {
	testing.TB
	mu     sync.Mutex
	failed string
}

func (r *testFatalRecorder) Helper() {}

func (r *testFatalRecorder) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.failed = fmt.Sprintf(format, args...)
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *testFatalRecorder) Fatal(args ...any) {
	r.mu.Lock()
	r.failed = fmt.Sprint(args...)
	r.mu.Unlock()
	runtime.Goexit()
}

// testRunRecorded runs fn against a recorder on its own goroutine and reports what it refused,
// or "" when it returned normally.
func testRunRecorded(t *testing.T, fn func(tb testing.TB)) string {
	t.Helper()
	rec := &testFatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(rec)
	}()
	<-done
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.failed
}

// testDrainWaitPeer is the broker side of a pipe: it writes the given bytes and then either
// closes or stays open.
func testDrainWaitPeer(t *testing.T, script []byte, closeAfter bool) *mqttTestConn {
	t.Helper()
	client, broker := net.Pipe()
	t.Cleanup(func() { client.Close(); broker.Close() })
	go func() {
		buf := make([]byte, 64)
		_, _ = broker.Read(buf) // the PINGREQ or DISCONNECT the helper sends
		if len(script) > 0 {
			_, _ = broker.Write(script)
		}
		if closeAfter {
			broker.Close()
		}
	}()
	return &mqttTestConn{c: client, r: &mqttReader{reader: client}}
}

// testDrainRedeliveryBytes encodes a QoS 1 PUBLISH with the given flags.
func testDrainRedeliveryBytes(flags byte, pi uint16, topic, payload string) []byte {
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub | flags)
	w.WriteVarInt(2 + len(topic) + 2 + len(payload))
	w.WriteString(topic)
	w.WriteUint16(pi)
	w.Write([]byte(payload))
	return w.Bytes()
}

// TestDrainRedeliveryWaitsRefuseAnythingButTheRedelivery pins what the drain tests' waits
// accept while a started delivery is outstanding: exactly its DUP redelivery, and otherwise
// the reply or the close they wait for, intact and on time.
func TestDrainRedeliveryWaitsRefuseAnythingButTheRedelivery(t *testing.T) {
	t.Parallel()
	d := testDrainRedelivery{pi: 7, topic: "drain/topic", payload: "payload"}
	dup := testDrainRedeliveryBytes(mqttPubQos1|mqttPubFlagDup, d.pi, d.topic, d.payload)

	for _, tc := range []struct {
		name    string
		ping    bool
		script  []byte
		close   bool
		refused bool
	}{
		{"close after a DUP redelivery", false, dup, true, false},
		{"a clean close", false, nil, true, false},
		{"a malformed packet before the close", false, []byte{0xC0, 0x01, 0x00}, true, true},
		{"a truncated redelivery before the close", false, dup[:len(dup)-2], true, true},
		{"a lone PUBLISH header before the close", false, []byte{mqttPacketPub | mqttPubQos1 | mqttPubFlagDup, 0x16}, true, true},
		{"a redelivery without DUP", false, testDrainRedeliveryBytes(mqttPubQos1, d.pi, d.topic, d.payload), true, true},
		{"a redelivery with RETAIN", false, testDrainRedeliveryBytes(mqttPubQos1|mqttPubFlagDup|mqttPubFlagRetain, d.pi, d.topic, d.payload), true, true},
		{"another identifier", false, testDrainRedeliveryBytes(mqttPubQos1|mqttPubFlagDup, d.pi+1, d.topic, d.payload), true, true},
		{"no close within the deadline", false, nil, false, true},
		{"PINGRESP after a DUP redelivery", true, append(append([]byte(nil), dup...), mqttPacketPingResp, 0), false, false},
		{"PINGRESP with a body", true, []byte{mqttPacketPingResp, 2, 0, 0}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := testDrainWaitPeer(t, tc.script, tc.close)
			failed := testRunRecorded(t, func(tb testing.TB) {
				if tc.ping {
					testDrainPingAmidRedeliveries(tb, conn, d)
					return
				}
				testDrainDisconnectAmidRedeliveriesWithin(tb, conn, d, time.Second)
			})
			if tc.refused && failed == "" {
				t.Fatalf("the wait accepted %q", tc.name)
			}
			if !tc.refused && failed != "" {
				t.Fatalf("the wait refused %q: %s", tc.name, failed)
			}
		})
	}
}

// TestDrainExpectNothingAcceptsOnlyALateRedelivery pins the wait after the started delivery
// is acknowledged: a late DUP redelivery of it may still arrive, and nothing else may.
func TestDrainExpectNothingAcceptsOnlyALateRedelivery(t *testing.T) {
	t.Parallel()
	d := testDrainRedelivery{pi: 7, topic: "drain/topic", payload: "payload"}
	for _, tc := range []struct {
		name    string
		script  []byte
		refused bool
	}{
		{"nothing", nil, false},
		{"a late DUP redelivery", testDrainRedeliveryBytes(mqttPubQos1|mqttPubFlagDup, d.pi, d.topic, d.payload), false},
		{"a fresh message", testDrainRedeliveryBytes(mqttPubQos1, d.pi+1, d.topic, "fresh"), true},
		{"the same message without DUP", testDrainRedeliveryBytes(mqttPubQos1, d.pi, d.topic, d.payload), true},
		{"a malformed PUBACK header", []byte{mqttPacketPubAck, 0x00}, true},
		{"a lone PUBLISH header", []byte{mqttPacketPub | mqttPubQos1 | mqttPubFlagDup, 0x16}, true},
		{"a late redelivery then a partial one", append(testDrainRedeliveryBytes(mqttPubQos1|mqttPubFlagDup, d.pi, d.topic, d.payload), mqttPacketPub|mqttPubQos1|mqttPubFlagDup), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, broker := net.Pipe()
			t.Cleanup(func() { client.Close(); broker.Close() })
			go func() {
				if len(tc.script) > 0 {
					_, _ = broker.Write(tc.script)
				}
			}()
			r := &mqttReader{reader: client}
			failed := testRunRecorded(t, func(tb testing.TB) { testDrainExpectNothingButLateRedeliveries(tb, r, d) })
			if tc.refused && failed == "" {
				t.Fatalf("the wait accepted %q", tc.name)
			}
			if !tc.refused && failed != "" {
				t.Fatalf("the wait refused %q: %s", tc.name, failed)
			}
		})
	}
}
