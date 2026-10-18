//go:build mqttport_fulltests

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

package mqtt

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

var testMQTTTimeout = 10 * time.Second

func testMQTTReadPacket(t testing.TB, r *mqttReader) (byte, int) {
	t.Helper()

	var b byte
	var pl int
	var err error

	rd := r.reader

	fill := func() []byte {
		t.Helper()
		var buf [512]byte

		n, err := rd.Read(buf[:])
		if err != nil {
			t.Fatalf("Error reading data: %v", err)
		}
		return copyBytes(buf[:n])
	}

	rd.SetReadDeadline(time.Now().Add(testMQTTTimeout))
	for {
		r.pstart = r.pos
		if !r.hasMore() {
			r.reset(fill())
			continue
		}
		b, err = r.readByte("packet type")
		if err != nil {
			t.Fatalf("Error reading packet: %v", err)
		}
		var complete bool
		// The client side frames server packets under the most permissive
		// remaining-length rules (MQTT 5.0's): this reader frames, it does not police.
		pl, complete, err = r.readPacketLen(b&mqttPacketMask, mqttProtoLevel5, MAX_PAYLOAD_SIZE)
		if err != nil {
			t.Fatalf("Error reading packet: %v", err)
		}
		if !complete {
			r.reset(fill())
			continue
		}
		break
	}
	rd.SetReadDeadline(time.Time{})
	return b, pl
}

// testMQTTReadPacketReady is testMQTTReadPacket but returns ok=false on a read
// timeout instead of failing the test, for polling loops (e.g. waiting for an
// asynchronously-stored retained message to become replayable).
func testMQTTReadPacketReady(r *mqttReader, timeout time.Duration) (byte, int, bool) {
	rd := r.reader
	fill := func() ([]byte, bool) {
		var buf [512]byte
		n, err := rd.Read(buf[:])
		if err != nil {
			return nil, false
		}
		return copyBytes(buf[:n]), true
	}
	rd.SetReadDeadline(time.Now().Add(timeout))
	defer rd.SetReadDeadline(time.Time{})
	for {
		r.pstart = r.pos
		if !r.hasMore() {
			b, ok := fill()
			if !ok {
				return 0, 0, false
			}
			r.reset(b)
			continue
		}
		b, err := r.readByte("packet type")
		if err != nil {
			return 0, 0, false
		}
		pl, complete, err := r.readPacketLen(b&mqttPacketMask, mqttProtoLevel5, MAX_PAYLOAD_SIZE)
		if err != nil {
			return 0, 0, false
		}
		if !complete {
			fb, ok := fill()
			if !ok {
				return 0, 0, false
			}
			r.reset(fb)
			continue
		}
		return b, pl, true
	}
}

func testMQTTReadPIPacket(expectedType byte, t testing.TB, r *mqttReader, expectedPI uint16) {
	t.Helper()
	b, _ := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != expectedType {
		t.Fatalf("Expected packet %x, got %x", expectedType, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != expectedPI {
		t.Fatalf("Expected PI %v got: %v, err=%v", expectedPI, rpi, err)
	}
}

func TestMQTTReader(t *testing.T) {
	t.Parallel()
	r := &mqttReader{}
	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err := r.readBytes("", false)
	if err != nil {
		t.Fatal(err)
	}
	sbs := string(bs)
	if sbs != "ab" {
		t.Fatalf(`expected "ab", got %q`, sbs)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err = r.readBytes("", true)
	if err != nil {
		t.Fatal(err)
	}
	bs[0], bs[1] = 'c', 'd'
	if bytes.Equal(bs, r.buf[2:]) {
		t.Fatal("readBytes should have returned a copy")
	}

	r.reset([]byte{'a', 'b'})
	if b, err := r.readByte(""); err != nil || b != 'a' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if !r.hasMore() {
		t.Fatal("expected to have more, did not")
	}
	if b, err := r.readByte(""); err != nil || b != 'b' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if r.hasMore() {
		t.Fatal("expected to not have more")
	}
	if _, err := r.readByte("test"); err == nil || !strings.Contains(err.Error(), "error reading test") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	if s, err := r.readString(""); err != nil || s != "ab" {
		t.Fatalf("Error reading string: s=%q err=%v", s, err)
	}

	r.reset([]byte{10})
	if _, err := r.readUint16("uint16"); err == nil || !strings.Contains(err.Error(), "error reading uint16") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0x82, 0xff, 0x3})
	l, _, err := r.readVarInt()
	if err != nil {
		t.Fatal("error getting packet len")
	}
	if l != 0xff82 {
		t.Fatalf("expected length 0xff82 got 0x%x", l)
	}
	r.reset([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	if _, _, err := r.readVarInt(); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0, 2, 'a', 'b', mqttPacketPub, 0x82, 0xff, 0x3})
	r.readString("")
	for i := 0; i < 2; i++ {
		r.pstart = r.pos
		b, err := r.readByte("")
		if err != nil {
			t.Fatalf("Error reading byte: %v", err)
		}
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			t.Fatalf("Unexpected byte: %v", b)
		}
		pl, complete, err := r.readPacketLen(mqttPacketPub, mqttProtoLevel, MAX_PAYLOAD_SIZE)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if complete {
			t.Fatal("Expected to be incomplete")
		}
		if pl != 0 {
			t.Fatalf("Expected pl to be 0, got %v", pl)
		}
		if i > 0 {
			break
		}
		if !bytes.Equal(r.pbuf, []byte{mqttPacketPub, 0x82, 0xff, 0x3}) {
			t.Fatalf("Invalid recorded partial: %v", r.pbuf)
		}
		r.reset([]byte{'a', 'b', 'c'})
		if !bytes.Equal(r.buf, []byte{mqttPacketPub, 0x82, 0xff, 0x3, 'a', 'b', 'c'}) {
			t.Fatalf("Invalid buffer: %v", r.buf)
		}
		if r.pbuf != nil {
			t.Fatalf("Partial buffer should have been reset, got %v", r.pbuf)
		}
		if r.pos != 0 {
			t.Fatalf("Pos should have been reset, got %v", r.pos)
		}
		if r.pstart != 0 {
			t.Fatalf("Pstart should have been reset, got %v", r.pstart)
		}
	}
	// On second pass, the pbuf should have been extended with 'abc'
	if !bytes.Equal(r.pbuf, []byte{mqttPacketPub, 0x82, 0xff, 0x3, 'a', 'b', 'c'}) {
		t.Fatalf("Invalid recorded partial: %v", r.pbuf)
	}
}

func TestMQTTWriter(t *testing.T) {
	t.Parallel()
	w := newMQTTWriter(0)
	w.WriteUint16(1234)

	r := &mqttReader{}
	r.reset(w.Bytes())
	if v, err := r.readUint16(""); err != nil || v != 1234 {
		t.Fatalf("unexpected value: v=%v err=%v", v, err)
	}

	w.Reset()
	w.WriteString("test")
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before string, got %v", r.buf)
	}

	w.Reset()
	w.WriteBytes([]byte("test"))
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before bytes, got %v", r.buf)
	}

	ints := []int{
		0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455,
	}
	lens := []int{
		1, 1, 1, 2, 2, 3, 3, 4, 4,
	}

	tl := 0
	w.Reset()
	for i, v := range ints {
		w.WriteVarInt(v)
		tl += lens[i]
		if tl != w.Len() {
			t.Fatalf("expected len %d, got %d", tl, w.Len())
		}
	}

	r.reset(w.Bytes())
	for _, v := range ints {
		x, _, _ := r.readVarInt()
		if v != x {
			t.Fatalf("expected %d, got %d", v, x)
		}
	}
}

func TestMQTTIncompleteConnectMaxPayloadViolationDisconnects(t *testing.T) {
	t.Parallel()
	const maxPayload = 1024
	const remainingLength = maxPayload + 1

	o := testMQTTDefaultOptions()
	o.MaxPayload = maxPayload
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	require_NoError(t, err)
	defer c.Close()

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(remainingLength)
	w.Write(bytes.Repeat([]byte{'A'}, maxPayload))

	_, err = testMQTTWrite(c, w.Bytes())
	require_NoError(t, err)

	testMQTTExpectDisconnect(t, c)
}

func TestMQTTPacketLenMaxPayloadViolation(t *testing.T) {
	t.Parallel()
	const maxPayload = 1024

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(maxPayload)
	w.Write(bytes.Repeat([]byte{'A'}, maxPayload))

	r := &mqttReader{}
	r.reset(w.Bytes())
	r.pstart = r.pos

	_, err := r.readByte("packet type")
	require_NoError(t, err)

	packetLen, complete, err := r.readPacketLen(mqttPacketConnect, 0, maxPayload)
	require_Error(t, err, ErrMaxPayload)
	require_False(t, complete)
	require_Equal(t, packetLen, w.Len())
	require_Equal(t, len(r.pbuf), 0)
}

// A connected client's claimed length is judged at the fixed header (F-120):
// a packet over the bound is refused after a few body bytes, not held until the
// claimed body arrives. Ported from upstream nats-server (#8692).
func TestMQTTIncompletePacketMaxPayloadViolationDisconnects(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MaxPayload = 1024
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		header byte
	}{
		{"publish", mqttPacketPub},
		{"subscribe", mqttPacketSub | mqttSubscribeFlags},
		{"unsubscribe", mqttPacketUnsub | mqttUnsubscribeFlags},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			// Declare the maximum remaining length but only send a few bytes.
			w := newMQTTWriter(0)
			w.WriteByte(test.header)
			w.WriteVarInt(mqttMaxPayloadSize)
			w.WriteString("foo")
			w.Write(bytes.Repeat([]byte{'A'}, 100))

			_, err := testMQTTWrite(mc, w.Bytes())
			require_NoError(t, err)

			testMQTTExpectDisconnect(t, mc)
		})
	}
}

// A fixed-length packet type claiming a body is malformed at its fixed header:
// it is refused there, not after the claimed body arrives (F-120).
func TestMQTTIncompleteFixedLengthPacketDisconnects(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		header byte
	}{
		{"pingreq", mqttPacketPing},
		{"puback", mqttPacketPubAck},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			w := newMQTTWriter(0)
			w.WriteByte(test.header)
			w.WriteVarInt(1000)
			_, err := testMQTTWrite(mc, w.Bytes())
			require_NoError(t, err)

			testMQTTExpectDisconnect(t, mc)
		})
	}
}

// The reserved-namespace branch of mqttProcessPub discards a PUBLISH before its
// payload is checked, so a complete oversized one used to be discarded with the
// connection kept. Over the connected 3.1.1 bound it is now refused at its fixed
// header like any other packet: a reserved topic earns no exemption (F-120).
func TestMQTTOversizedReservedTopicPublishDisconnects(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MaxPayload = 1024
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	_, header := mqttMakePublishHeader(0, 0, false, false, false, []byte("$MQTT/foo"), nil, 70000)
	// The server may close while the body is still being written; the verdict is
	// the disconnect below, not the write.
	testMQTTWrite(mc, append(header, bytes.Repeat([]byte{'A'}, 70000)...))

	testMQTTExpectDisconnect(t, mc)
}

// max_payload bounds the NATS message, and a QoS 0 PUBLISH's topic is not part
// of it, so the connected 3.1.1 bound leaves room for a topic beyond max_payload
// (F-120). Ported from upstream nats-server (#8692).
func TestMQTTMaxPayloadDoesNotCapPublishTopic(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MaxPayload = 1024
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "cid", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSendPublishPacket(t, mc, 0, false, false, strings.Repeat("a", 1100), 0, nil)
	testMQTTFlush(t, mc, nil, r)
}

// A trickled packet is appended to, not copied in full on every read: buffer
// growth is amortized (F-120). Ported from upstream nats-server (#8692).
func TestMQTTPartialPacketNotCopiedOnEveryRead(t *testing.T) {
	const payloadLen = 64 * 1024
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub)
	w.WriteVarInt(2 + 3 + payloadLen)
	w.WriteString("foo")
	w.Write(bytes.Repeat([]byte{'A'}, payloadLen))
	packet := w.Bytes()

	r := &mqttReader{}
	var reallocs int
	var last *byte
	for i, b := range packet {
		r.reset([]byte{b})
		r.pstart = r.pos
		_, err := r.readByte("packet type")
		require_NoError(t, err)
		pl, complete, err := r.readPacketLen(mqttPacketPub, mqttProtoLevel, MAX_PAYLOAD_SIZE)
		require_NoError(t, err)
		if i == len(packet)-1 {
			require_True(t, complete)
			require_Equal(t, pl, 2+3+payloadLen)
			require_True(t, bytes.Equal(r.buf, packet))
		} else {
			require_False(t, complete)
			require_Equal(t, len(r.pbuf), i+1)
			if p := &r.pbuf[0]; p != last {
				reallocs++
				last = p
			}
		}
	}
	require_True(t, reallocs < 100)
}

// An owned buffer is kept as the partial's buffer only while it holds nothing
// but the partial. Once a packet has been parsed out of it, the partial after
// that packet is copied out, so the parsed packet is never parsed again (F-120).
func TestMQTTPartialPacketAfterCompletePacketIsCopied(t *testing.T) {
	ping := []byte{mqttPacketPing, 0}
	pub := newMQTTWriter(0)
	pub.WriteByte(mqttPacketPub)
	pub.WriteVarInt(2 + 3 + 10)
	pub.WriteString("foo")
	pub.Write(bytes.Repeat([]byte{'A'}, 10))
	pubBytes := pub.Bytes()

	r := &mqttReader{}
	// First read: only the PINGREQ's type byte, kept as a partial.
	r.reset(ping[:1])
	r.pstart = r.pos
	_, err := r.readByte("packet type")
	require_NoError(t, err)
	_, complete, err := r.readPacketLen(mqttPacketPing, mqttProtoLevel, MAX_PAYLOAD_SIZE)
	require_NoError(t, err)
	require_False(t, complete)

	// Second read completes the PINGREQ and starts the PUBLISH; the reader now
	// works in a buffer it owns, with a parsed packet before the partial.
	r.reset(append([]byte{ping[1]}, pubBytes[:4]...))
	require_True(t, r.owned)
	r.pstart = r.pos
	_, err = r.readByte("packet type")
	require_NoError(t, err)
	pl, complete, err := r.readPacketLen(mqttPacketPing, mqttProtoLevel, MAX_PAYLOAD_SIZE)
	require_NoError(t, err)
	require_True(t, complete)
	require_Equal(t, pl, 0)
	r.pstart = r.pos
	_, err = r.readByte("packet type")
	require_NoError(t, err)
	_, complete, err = r.readPacketLen(mqttPacketPub, mqttProtoLevel, MAX_PAYLOAD_SIZE)
	require_NoError(t, err)
	require_False(t, complete)

	// Third read: the buffer starts at the PUBLISH, not at the parsed PINGREQ.
	r.reset(pubBytes[4:])
	require_True(t, bytes.Equal(r.buf, pubBytes))
}

// The remaining length a packet type fixes is judged at the fixed header, before
// any body is buffered, under the rules of the connection's protocol level
// (F-120).
func TestMQTTReadPacketLenRefusesRemainingLengthBeforeBuffering(t *testing.T) {
	for _, test := range []struct {
		name   string
		header byte
		proto  byte
		remLen int
		body   int
		ok     bool
	}{
		{"pingreq claiming a body", mqttPacketPing, mqttProtoLevel, 1000, 0, false},
		{"v5 pingreq claiming a body", mqttPacketPing, mqttProtoLevel5, 1000, 0, false},
		{"3.1.1 puback with a reason byte", mqttPacketPubAck, mqttProtoLevel, 3, 3, false},
		{"v5 puback with a reason byte", mqttPacketPubAck, mqttProtoLevel5, 3, 3, true},
		{"v5 disconnect with a body", mqttPacketDisconnect, mqttProtoLevel5, 5, 5, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := newMQTTWriter(0)
			w.WriteByte(test.header)
			w.WriteVarInt(test.remLen)
			w.Write(make([]byte, test.body))

			r := &mqttReader{}
			r.reset(w.Bytes())
			r.pstart = r.pos
			_, err := r.readByte("packet type")
			require_NoError(t, err)
			pl, complete, err := r.readPacketLen(test.header, test.proto, MAX_PAYLOAD_SIZE)
			if !test.ok {
				if !errors.Is(err, errMQTTMalformedPacket) {
					t.Fatalf("Expected a malformed packet error, got %v", err)
				}
				require_False(t, complete)
				require_Equal(t, len(r.pbuf), 0)
				return
			}
			require_NoError(t, err)
			require_True(t, complete)
			require_Equal(t, pl, test.remLen)
		})
	}
}

func testMQTTDefaultOptions() *Options {
	o := DefaultOptions()
	o.ServerName = nuid.Next()
	o.MQTT.Host = "127.0.0.1"
	o.MQTT.Port = -1
	// The required suites use the production boundary. The separate
	// mqttport_experimental_shared_qos2 build tag opts the parked suite into the
	// preserved affine implementation.
	o.MQTT.ExperimentalSharedQoS2 = mqttTestExperimentalSharedQoS2
	o.JetStream = true
	return o
}

func testMQTTRunServer(t testing.TB, o *Options) *Server {
	t.Helper()
	if o.StoreDir == _EMPTY_ {
		o.StoreDir = t.TempDir()
	}
	// One embedded server per StoreDir, started once however many tests arrive
	// together: two nats-servers on one store corrupt JetStream state.
	entry, err := testEmbedded.acquire(o.StoreDir, func() (*natsserver.Server, error) {
		ns, err := natsserver.NewServer(&natsserver.Options{
			Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: o.StoreDir,
		})
		if err != nil {
			return nil, fmt.Errorf("creating embedded nats-server: %w", err)
		}
		go ns.Start()
		if !ns.ReadyForConnections(10 * time.Second) {
			ns.Shutdown()
			return nil, errors.New("embedded nats-server not ready")
		}
		return ns, nil
	})
	if err != nil {
		t.Fatalf("Error starting embedded nats-server: %v", err)
	}
	// acquire took a reference for the pairing below. Every failure between here
	// and pair leaves via t.Fatalf, which unwinds only THIS goroutine — the other
	// parallel tests keep running — so without this the entry and its embedded
	// server stay registered for the rest of the run. Deferred functions do run on
	// Goexit, which is what makes this the right shape.
	paired := false
	defer func() {
		if !paired {
			testEmbedded.releaseEntry(entry)
		}
	}()
	ns := entry.server()
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to embedded nats-server: %v", err)
	}
	s := NewServer(nc, o, o.ServerName)
	if err := s.Start(); err != nil {
		nc.Close()
		t.Fatalf("Error starting MQTT server: %v", err)
	}
	testEmbedded.pair(s, entry)
	paired = true
	if err := s.readyForConnections(3 * time.Second); err != nil {
		testMQTTShutdownServer(s)
		t.Fatal(err)
	}
	return s
}

func testMQTTShutdownRestartedServer(s **Server) {
	srv := *s
	testMQTTShutdownServer(srv)
	*s = nil
}

func testMQTTShutdownServer(s *Server) {
	s.Shutdown()
	// One critical section: forgetting the pairing and forgetting the StoreDir
	// entries that named the same embedded server have to be atomic together, or
	// a restart could find a dir entry pointing at a server this call is about to
	// shut down.
	testEmbedded.release(s)
}

func TestMQTTStart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	nc.Close()

	// Check failure to start due to port in use
	o2 := testMQTTDefaultOptions()
	o2.MQTT.Port = o.MQTT.Port
	s2, err := testMQTTNewServerFromOptions(t, o2)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s2.Shutdown()
	l := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s2.SetLogger(l, false, false)
	s2.Start()

	select {
	case e := <-l.fatalCh:
		if !strings.Contains(e, "Unable to listen for MQTT connections") {
			t.Fatalf("Unexpected error: %q", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Should have gotten a fatal error")
	}
}

type mqttConnInfo struct {
	clientID  string
	cleanSess bool
	keepAlive uint16
	will      *mqttWill
	user      string
	pass      string
	ws        bool
	tls       bool
	tlsc      *tls.Config
}

func testMQTTGetClient(t testing.TB, s *Server, clientID string) *client {
	t.Helper()
	var mc *client
	s.mu.Lock()
	for _, c := range s.clients {
		c.mu.Lock()
		if c.isMqtt() && c.mqtt.cid == clientID {
			mc = c
		}
		c.mu.Unlock()
		if mc != nil {
			break
		}
	}
	s.mu.Unlock()
	if mc == nil {
		t.Fatalf("Did not find client %q", clientID)
	}
	return mc
}

func testMQTTGetAccountSessionManager(t *testing.T, s *Server, cid string) *mqttAccountSessionManager {
	t.Helper()
	c := testMQTTGetClient(t, s, cid)
	require_NotNil(t, c)
	asm := c.mqtt.asm
	require_NotNil(t, asm)
	return asm
}

func testMQTTRead(c net.Conn) ([]byte, error) {
	var buf [512]byte
	// Make sure that test does not block
	c.SetReadDeadline(time.Now().Add(testMQTTTimeout))
	n, err := c.Read(buf[:])
	if err != nil {
		return nil, err
	}
	c.SetReadDeadline(time.Time{})
	return copyBytes(buf[:n]), nil
}

func testMQTTWrite(c net.Conn, buf []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(testMQTTTimeout))
	n, err := c.Write(buf)
	c.SetWriteDeadline(time.Time{})
	return n, err
}

func testMQTTConnect(t testing.TB, ci *mqttConnInfo, host string, port int) (net.Conn, *mqttReader) {
	t.Helper()
	return testMQTTConnectRetry(t, ci, host, port, 0)
}

func testMQTTConnectRetry(t testing.TB, ci *mqttConnInfo, host string, port int, retryCount int) (net.Conn, *mqttReader) {
	t.Helper()
	c, r, err := testMQTTConnectRetryWithError(t, ci, host, port, retryCount)
	if err != nil {
		t.Fatal(err)
	}
	return c, r
}

func testMQTTConnectRetryWithError(t testing.TB, ci *mqttConnInfo, host string, port int, retryCount int) (net.Conn, *mqttReader, error) {
	retry := func(c net.Conn) bool {
		if c != nil {
			c.Close()
		}
		if retryCount == 0 {
			return false
		}
		time.Sleep(time.Second)
		retryCount--
		return true
	}

	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	var c net.Conn
	var err error
RETRY:
	if ci.ws {
		// MQTT-over-WebSocket is not built under this tag; the WS-dependent
		// tests that set ci.ws live in mqtt_ws_ref_test.go (mqttport_wstests).
		t.Fatal("ci.ws requested but WebSocket transport is not built under mqttport_fulltests")
	} else {
		c, err = net.Dial("tcp", addr)
		if err == nil && ci.tls {
			tc := ci.tlsc
			if tc == nil {
				tc = &tls.Config{InsecureSkipVerify: true}
			}
			c = tls.Client(c, tc)
			c.SetDeadline(time.Now().Add(time.Second))
			err = c.(*tls.Conn).Handshake()
			c.SetDeadline(time.Time{})
		}
	}
	if err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error creating mqtt connection: %v", err)
	}

	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c, proto); err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error writing connect: %v", err)
	}

	buf, err := testMQTTRead(c)
	if err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error reading: %v", err)
	}
	mr := &mqttReader{reader: c}
	mr.reset(buf)

	return c, mr, nil
}

func mqttCreateConnectProto(ci *mqttConnInfo) []byte {
	flags := byte(0)
	if ci.cleanSess {
		flags |= mqttConnFlagCleanSession
	}
	if ci.will != nil {
		flags |= mqttConnFlagWillFlag | (ci.will.qos << 3)
		if ci.will.retain {
			flags |= mqttConnFlagWillRetain
		}
	}
	if ci.user != _EMPTY_ {
		flags |= mqttConnFlagUsernameFlag
	}
	if ci.pass != _EMPTY_ {
		flags |= mqttConnFlagPasswordFlag
	}

	pkLen := 2 + len(mqttProtoName) +
		1 + // proto level
		1 + // flags
		2 + // keepAlive
		2 + len(ci.clientID)

	if ci.will != nil {
		pkLen += 2 + len(ci.will.topic)
		pkLen += 2 + len(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		pkLen += 2 + len(ci.user)
	}
	if ci.pass != _EMPTY_ {
		pkLen += 2 + len(ci.pass)
	}

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x4)
	w.WriteByte(flags)
	w.WriteUint16(ci.keepAlive)
	w.WriteString(ci.clientID)
	if ci.will != nil {
		w.WriteBytes(ci.will.topic)
		w.WriteBytes(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		w.WriteString(ci.user)
	}
	if ci.pass != _EMPTY_ {
		w.WriteBytes([]byte(ci.pass))
	}
	return w.Bytes()
}

func testMQTTCheckConnAck(t testing.TB, r *mqttReader, rc byte, sessionPresent bool) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	pt := b & mqttPacketMask
	if pt != mqttPacketConnectAck {
		t.Fatalf("Expected ConnAck (%x), got %x", mqttPacketConnectAck, pt)
	}
	if pl != 2 {
		t.Fatalf("ConnAck packet length should be 2, got %v", pl)
	}
	caf, err := r.readByte("connack flags")
	if err != nil {
		t.Fatalf("Error reading packet length: %v", err)
	}
	if caf&0xfe != 0 {
		t.Fatalf("ConnAck flag bits 7-1 should all be 0, got %x", caf>>1)
	}
	if sp := caf == 1; sp != sessionPresent {
		t.Fatalf("Expected session present flag=%v got %v", sessionPresent, sp)
	}
	carc, err := r.readByte("connack return code")
	if err != nil {
		t.Fatalf("Error reading returned code: %v", err)
	}
	if carc != rc {
		t.Fatalf("Expected return code to be %v, got %v", rc, carc)
	}
}

func testMQTTCheckPubAck(t testing.TB, r *mqttReader, packetType byte) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	pt := b & mqttPacketMask
	if pt != packetType {
		t.Fatalf("Expected %x, got %x", packetType, pt)
	}
	r.pos += pl
}

func TestMQTTAuthTimeout(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		at   float64
		mat  float64
		ok   bool
	}{
		{"use top-level auth timeout", 0.5, 0.0, true},
		{"use mqtt auth timeout", 0.5, 0.05, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.AuthTimeout = test.at
			o.MQTT.Username = "mqtt"
			o.MQTT.Password = "client"
			o.MQTT.AuthTimeout = test.mat
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			mc, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
			if err != nil {
				t.Fatalf("Error connecting: %v", err)
			}
			defer mc.Close()

			time.Sleep(100 * time.Millisecond)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "mqtt",
				pass:      "client",
			}
			proto := mqttCreateConnectProto(ci)
			if _, err := testMQTTWrite(mc, proto); err != nil {
				if test.ok {
					t.Fatalf("Error sending connect: %v", err)
				}
				// else it is ok since we got disconnected due to the connect deadline
				return
			}
			buf, err := testMQTTRead(mc)
			if !test.ok {
				// The window elapsed before this CONNECT, so the broker must NOT have
				// acknowledged it. Requiring the absence of a CONNACK is what makes
				// this case test the bound: accepting either outcome passed against a
				// deadline that was never armed at all.
				if err == nil {
					t.Fatalf("Expected no CONNACK after the connect deadline expired, got %v", buf)
				}
				return
			}
			if err != nil {
				t.Fatalf("Error reading: %v", err)
			}
			r := &mqttReader{reader: mc}
			r.reset(buf)
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			time.Sleep(500 * time.Millisecond)
			testMQTTPublish(t, mc, r, 1, false, false, "foo", 1, []byte("msg"))
		})
	}
}

func TestMQTTConnectNotFirstPacket(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	c, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	testMQTTSendPublishPacket(t, c, 0, false, false, "foo", 0, []byte("hello"))
	testMQTTExpectDisconnect(t, c)

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "should be a CONNECT") {
			t.Fatalf("Expected error about first packet being a CONNECT, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not log any error")
	}
}

func TestMQTTSecondConnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true})
	if _, err := testMQTTWrite(mc, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTParseConnect(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		proto   []byte
		err     string
		wantErr bool
	}{
		{"packet in buffer error", []byte{0}, io.ErrUnexpectedEOF.Error(), false},
		{"bad proto name", []byte{0, 4, 'B', 'A', 'D'}, "protocol name", false},
		{"invalid proto name", []byte{0, 3, 'B', 'A', 'D'}, "expected connect packet with protocol name", false},
		{"old proto not supported", []byte{0, 6, 'M', 'Q', 'I', 's', 'd', 'p'}, "older protocol", false},
		{"error on protocol level", []byte{0, 4, 'M', 'Q', 'T', 'T'}, "protocol level", false},
		{"unacceptable protocol version", []byte{0, 4, 'M', 'Q', 'T', 'T', 10}, "unacceptable protocol version", false},
		{"error on flags", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel}, "flags", false},
		{"reserved flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1}, errMQTTConnFlagReserved.Error(), false},
		{"will qos without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 3}, "if Will flag is set to 0, Will QoS must be 0 too", false},
		{"will retain without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 5}, errMQTTWillAndRetainFlag.Error(), false},
		{"will qos", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 3<<3 | 1<<2}, "if Will flag is set to 1, Will QoS can be 0, 1 or 2", false},
		{"no user but password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagPasswordFlag}, errMQTTPasswordFlagAndNoUser.Error(), false},
		{"missing keep alive", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0}, "keep alive", false},
		{"missing client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1}, "client ID", false},
		{"empty client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 0}, errMQTTCIDEmptyNeedsCleanFlag.Error(), false},
		{"invalid utf8 client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 1, 241}, "invalid utf8 for client ID", false},
		{"client ID with null byte", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 3, 'a', 0, 'b'}, "", true},
		{"missing will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, "Will topic", false},
		{"empty will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, errMQTTEmptyWillTopic.Error(), false},
		{"invalid utf8 will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, "invalid utf8 for Will topic", false},
		{"invalid wildcard will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, '#'}, "wildcards not allowed", false},
		{"error on will message", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a', 0, 3}, "Will message", false},
		{"error on username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, "user name", false},
		{"empty username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, errMQTTEmptyUsername.Error(), false},
		{"invalid utf8 username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, "invalid utf8 for user name", false},
		{"username with null byte", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 3, 'a', 0, 'b'}, "", true},
		{"error on password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagPasswordFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a'}, "password", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			_, _, err := c.mqttParseConnect(r, false)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTConnectFailsOnParse(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}

	pkLen := 2 + len(mqttProtoName) +
		1 + // proto level
		1 + // flags
		2 + // keepAlive
		2 + len("mqtt")

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x7)
	w.WriteByte(mqttConnFlagCleanSession)
	w.WriteUint16(0)
	w.WriteString("mqtt")
	c.Write(w.Bytes())

	buf, err := testMQTTRead(c)
	if err != nil {
		t.Fatalf("Error reading: %v", err)
	}
	r := &mqttReader{reader: c}
	r.reset(buf)
	testMQTTCheckConnAck(t, r, mqttConnAckRCUnacceptableProtocolVersion, false)
}

func TestMQTTConnKeepAlive(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, keepAlive: 1}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, []byte("msg"))

	time.Sleep(2 * time.Second)
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTMalformedFixedHeaderFlagsCauseDisconnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		packet func(t *testing.T) (net.Conn, []byte)
	}{
		{
			name: "connect",
			packet: func(t *testing.T) (net.Conn, []byte) {
				addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
				c, err := net.Dial("tcp", addr)
				if err != nil {
					t.Fatalf("Error creating mqtt connection: %v", err)
				}
				proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true})
				proto[0] |= 0x1
				return c, proto
			},
		},
		{
			name: "pingreq",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPing | 0x1, 0}
			},
		},
		{
			name: "puback",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubAck | 0x1, 2, 0, 1}
			},
		},
		{
			name: "pubrec",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRec | 0x1, 2, 0, 1}
			},
		},
		{
			name: "pubrel",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRel | 0x1, 2, 0, 1}
			},
		},
		{
			name: "pubcomp",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubComp | 0x1, 2, 0, 1}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, packet := test.packet(t)
			defer c.Close()

			if _, err := testMQTTWrite(c, packet); err != nil {
				t.Fatalf("Error writing malformed %s packet: %v", test.name, err)
			}
			testMQTTExpectDisconnect(t, c)
		})
	}
}

func TestMQTTMalformedRemainingLengthCausesDisconnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		packet func(t *testing.T) (net.Conn, []byte)
	}{
		{
			name: "pingreq",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPing, 1, 0}
			},
		},
		{
			name: "puback",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubAck, 3, 0, 1, 0}
			},
		},
		{
			name: "pubrec",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRec, 3, 0, 1, 0}
			},
		},
		{
			name: "pubrel",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubRel | 0x2, 3, 0, 1, 0}
			},
		},
		{
			name: "pubcomp",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
				return mc, []byte{mqttPacketPubComp, 3, 0, 1, 0}
			},
		},
		{
			name: "publish",
			packet: func(t *testing.T) (net.Conn, []byte) {
				mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
				testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

				// Declare a remaining length that only covers the topic length field,
				// then write a much larger topic.
				w := newMQTTWriter(0)
				w.WriteByte(mqttPacketPub)
				w.WriteVarInt(2)
				w.WriteBytes(bytes.Repeat([]byte("a"), 64))
				return mc, w.Bytes()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, packet := test.packet(t)
			defer c.Close()

			if _, err := testMQTTWrite(c, packet); err != nil {
				t.Fatalf("Error writing malformed %s packet: %v", test.name, err)
			}
			testMQTTExpectDisconnect(t, c)
		})
	}
}

func TestMQTTTopicAndSubjectConversion(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		mqttTopic   string
		natsSubject string
		err         string
	}{
		{"/", "/", "/./", ""},
		{"//", "//", "/././", ""},
		{"///", "///", "/./././", ""},
		{"////", "////", "/././././", ""},
		{"foo", "foo", "foo", ""},
		{"/foo", "/foo", "/.foo", ""},
		{"//foo", "//foo", "/./.foo", ""},
		{"///foo", "///foo", "/././.foo", ""},
		{"///foo/", "///foo/", "/././.foo./", ""},
		{"///foo//", "///foo//", "/././.foo././", ""},
		{"///foo///", "///foo///", "/././.foo./././", ""},
		{"//.foo.//", "//.foo.//", "/././/foo//././", ""},
		{"foo/bar", "foo/bar", "foo.bar", ""},
		{"/foo/bar", "/foo/bar", "/.foo.bar", ""},
		{"/foo/bar/", "/foo/bar/", "/.foo.bar./", ""},
		{"foo/bar/baz", "foo/bar/baz", "foo.bar.baz", ""},
		{"/foo/bar/baz", "/foo/bar/baz", "/.foo.bar.baz", ""},
		{"/foo/bar/baz/", "/foo/bar/baz/", "/.foo.bar.baz./", ""},
		{"bar", "bar/", "bar./", ""},
		{"bar//", "bar//", "bar././", ""},
		{"bar///", "bar///", "bar./././", ""},
		{"foo//bar", "foo//bar", "foo./.bar", ""},
		{"foo///bar", "foo///bar", "foo././.bar", ""},
		{"foo////bar", "foo////bar", "foo./././.bar", ""},
		{".", ".", "//", ""},
		{"..", "..", "////", ""},
		{"...", "...", "//////", ""},
		{"./", "./", "//./", ""},
		{".//.", ".//.", "//././/", ""},
		{"././.", "././.", "//.//.//", ""},
		{"././/.", "././/.", "//.//././/", ""},
		{".foo", ".foo", "//foo", ""},
		{"foo.", "foo.", "foo//", ""},
		{".foo.", ".foo.", "//foo//", ""},
		{"foo../bar/", "foo../bar/", "foo////.bar./", ""},
		{"foo../bar/.", "foo../bar/.", "foo////.bar.//", ""},
		{"/foo/", "/foo/", "/.foo./", ""},
		{"./foo/.", "./foo/.", "//.foo.//", ""},
		{"foo.bar/baz", "foo.bar/baz", "foo//bar.baz", ""},
		// These should produce errors
		{"foo/+", "foo/+", "", "wildcards not allowed in publish"},
		{"foo/#", "foo/#", "", "wildcards not allowed in publish"},
		{"foo bar", "foo bar", "", "not supported"},
		{"foo\x7fbar", "foo\x7fbar", "", "not supported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			res, err := mqttTopicToNATSPubSubject([]byte(test.mqttTopic))
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("Expected error %q, got %v", test.err, err)
				}
				return
			}
			toNATS := string(res)
			if toNATS != test.natsSubject {
				t.Fatalf("Expected subject %q got %q", test.natsSubject, toNATS)
			}

			res = natsSubjectToMQTTTopic(res)
			backToMQTT := string(res)
			if backToMQTT != test.mqttTopic {
				t.Fatalf("Expected topic %q got %q (NATS conversion was %q)", test.mqttTopic, backToMQTT, toNATS)
			}
		})
	}
}

func TestMQTTFilterConversion(t *testing.T) {
	t.Parallel()
	// Similar to TopicConversion test except that wildcards are OK here.
	// So testing only those.
	for _, test := range []struct {
		name        string
		mqttTopic   string
		natsSubject string
	}{
		{"single level wildcard", "+", "*"},
		{"single level wildcard", "/+", "/.*"},
		{"single level wildcard", "+/", "*./"},
		{"single level wildcard", "/+/", "/.*./"},
		{"single level wildcard", "foo/+", "foo.*"},
		{"single level wildcard", "foo/+/", "foo.*./"},
		{"single level wildcard", "foo/+/bar", "foo.*.bar"},
		{"single level wildcard", "foo/+/+", "foo.*.*"},
		{"single level wildcard", "foo/+/+/", "foo.*.*./"},
		{"single level wildcard", "foo/+/+/bar", "foo.*.*.bar"},
		{"single level wildcard", "foo//+", "foo./.*"},
		{"single level wildcard", "foo//+/", "foo./.*./"},
		{"single level wildcard", "foo//+//", "foo./.*././"},
		{"single level wildcard", "foo//+//bar", "foo./.*./.bar"},
		{"single level wildcard", "foo///+///bar", "foo././.*././.bar"},
		{"single level wildcard", "foo.bar///+///baz", "foo//bar././.*././.baz"},

		{"multi level wildcard", "#", ">"},
		{"multi level wildcard", "/#", "/.>"},
		{"multi level wildcard", "/foo/#", "/.foo.>"},
		{"multi level wildcard", "foo/#", "foo.>"},
		{"multi level wildcard", "foo//#", "foo./.>"},
		{"multi level wildcard", "foo///#", "foo././.>"},
		{"multi level wildcard", "foo/bar/#", "foo.bar.>"},
		{"multi level wildcard", "foo/bar.baz/#", "foo.bar//baz.>"},

		// NATS wildcards in MQTT filters are generally
		// not escaped, and get interpreted as wildcards
		// when converted to NATS subjects
		{"* literal", "foo*/bar", "foo*.bar"},
		{"> literal", "foo>/bar", "foo>.bar"},
		{"* wildcard middle", "foo/*/bar", "foo.*.bar"},
		{"* wildcard end", "foo/*/bar/*", "foo.*.bar.*"},
		{"> wildcard", "foo/>", "foo.>"},
		{"wildcard mix", "foo/*/bar/>", "foo.*.bar.>"},
	} {
		t.Run(test.name, func(t *testing.T) {
			res, err := mqttFilterToNATSSubject([]byte(test.mqttTopic))
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if string(res) != test.natsSubject {
				t.Fatalf("Expected subject %q got %q", test.natsSubject, res)
			}
		})
	}
}

func TestMQTTParseSub(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		proto   []byte
		b       byte
		pl      int
		err     string
		wantErr bool
	}{
		{"reserved flag", nil, 3, 0, "wrong subscribe reserved flags", false},
		{"ensure packet loaded", []byte{1, 2}, mqttSubscribeFlags, 10, io.ErrUnexpectedEOF.Error(), false},
		{"error reading packet id", []byte{1}, mqttSubscribeFlags, 1, "reading packet identifier", false},
		{"packet id cannot be zero", []byte{0, 0}, mqttSubscribeFlags, 2, errMQTTPacketIdentifierIsZero.Error(), false},
		{"missing filters", []byte{0, 1}, mqttSubscribeFlags, 2, "subscribe protocol must contain at least 1 topic filter", false},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttSubscribeFlags, 5, "topic filter", false},
		{"empty topic", []byte{0, 1, 0, 0}, mqttSubscribeFlags, 4, errMQTTTopicFilterCannotBeEmpty.Error(), false},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttSubscribeFlags, 5, "invalid utf8 for topic filter", false},
		{"topic with null byte", []byte{0, 1, 0, 3, 'a', 0, 'b', 0}, mqttSubscribeFlags, 8, "", true},
		{"missing qos", []byte{0, 1, 0, 1, 'a'}, mqttSubscribeFlags, 5, "QoS", false},
		{"invalid qos", []byte{0, 1, 0, 1, 'a', 3}, mqttSubscribeFlags, 6, "subscribe QoS value must be 0, 1 or 2", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			_, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, true)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTSub(t testing.TB, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter, expected []byte) {
	t.Helper()
	w := newMQTTWriter(0)
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter) + 1
	}
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes([]byte(f.filter))
		w.WriteByte(f.qos)
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing SUBSCRIBE protocol: %v", err)
	}
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketSubAck {
		t.Fatalf("Expected SUBACK packet %x, got %x", mqttPacketSubAck, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
	for i, rem := 0, pl-2; rem > 0; rem-- {
		qos, err := r.readByte("filter qos")
		if err != nil {
			t.Fatal(err)
		}
		if expected != nil && qos != expected[i] {
			t.Fatalf("For topic filter %q expected qos of %v, got %v",
				filters[i].filter, expected[i], qos)
		}
		i++
	}
}

func TestMQTTSubAck(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "user",
			Password: "pass",
			Permissions: &Permissions{
				Subscribe: &SubjectPermission{Allow: []string{"foo", "bar", "baz", "foo.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, user: "user", pass: "pass"}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	subs := []*mqttFilter{
		{filter: "foo", qos: 0},
		{filter: "bar", qos: 1},
		{filter: "baz", qos: 2},
		{filter: "foo/#/bar", qos: 0}, // Invalid sub, so we should receive a result of mqttSubAckFailure
	}
	expected := []byte{
		0,
		1,
		2,
		mqttSubAckFailure,
	}
	testMQTTSub(t, 1, mc, r, subs, expected)
}

func TestMQTTQoS2SubDowngrade(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.downgradeQoS2Sub = true
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	subs := []*mqttFilter{
		{filter: "bar", qos: 1},
		{filter: "baz", qos: 2},
	}
	expected := []byte{
		1,
		1,
	}
	testMQTTSub(t, 1, mc, r, subs, expected)
}

func testMQTTFlush(t testing.TB, c net.Conn, bw *bufio.Writer, r *mqttReader) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPing)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	ab, l := testMQTTReadPacket(t, r)
	if pt := ab & mqttPacketMask; pt != mqttPacketPingResp {
		t.Fatalf("Expected ping response got %x", pt)
	}
	if l != 0 {
		t.Fatalf("Expected PINGRESP length to be 0, got %v", l)
	}
}

func testMQTTExpectNothing(t testing.TB, r *mqttReader) {
	t.Helper()
	// First, check that we don't have buffered data.
	if r.hasMore() {
		t.Fatalf("Expected nothing, got %v", r.buf[r.pos:])
	}
	// Then, try to read from the reader with some timeout.
	var buf [128]byte
	r.reader.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := r.reader.Read(buf[:]); err == nil {
		t.Fatalf("Expected nothing, got %v", buf[:n])
	}
	r.reader.SetReadDeadline(time.Time{})
}

func testMQTTCheckPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, expectedFlags byte, payload []byte) uint16 {
	t.Helper()
	pi := testMQTTCheckPubMsgNoAck(t, c, r, topic, expectedFlags, payload)
	if pi == 0 {
		return 0
	}
	qos := mqttGetQoS(expectedFlags)
	switch qos {
	case 1:
		testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	case 2:
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
	}
	return pi
}

func testMQTTCheckPubMsgNoAck(t testing.TB, c net.Conn, r *mqttReader, topic string, expectedFlags byte, payload []byte) uint16 {
	t.Helper()
	pflags, pi := testMQTTGetPubMsg(t, c, r, topic, payload)
	if pflags != expectedFlags {
		t.Fatalf("Expected flags to be %x, got %x", expectedFlags, pflags)
	}
	return pi
}

func testMQTTGetPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, payload []byte) (byte, uint16) {
	t.Helper()
	flags, pi, _, _ := testMQTTGetPubMsgEx(t, c, r, topic, payload)
	return flags, pi
}

func testMQTTGetPubMsgEx(t testing.TB, _ net.Conn, r *mqttReader, topic string, payload []byte) (byte, uint16, string, []byte) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
	}
	return testMQTTGetPubMsgExEx(t, nil, r, b, pl, topic, payload)
}

func testMQTTGetPubMsgExEx(t testing.TB, _ net.Conn, r *mqttReader, b byte, pl int, topic string, payload []byte) (byte, uint16, string, []byte) {
	t.Helper()
	pflags := b & mqttPacketFlagMask
	qos := (pflags & mqttPubFlagQoS) >> 1
	start := r.pos
	ptopic, err := r.readString("topic name")
	if err != nil {
		t.Fatal(err)
	}
	if topic != _EMPTY_ && ptopic != topic {
		t.Fatalf("Expected topic %q, got %q", topic, ptopic)
	}
	var pi uint16
	if qos > 0 {
		pi, err = r.readUint16("packet identifier")
		if err != nil {
			t.Fatal(err)
		}
	}
	msgLen := pl - (r.pos - start)
	if r.pos+msgLen > len(r.buf) {
		t.Fatalf("computed message length goes beyond buffer: ml=%v pos=%v lenBuf=%v",
			msgLen, r.pos, len(r.buf))
	}
	ppayload := r.buf[r.pos : r.pos+msgLen]
	if payload != nil && !bytes.Equal(payload, ppayload) {
		t.Fatalf("Expected payload %q, got %q", payload, ppayload)
	}
	r.pos += msgLen
	return pflags, pi, ptopic, ppayload
}

func testMQTTReadPubPacket(t testing.TB, r *mqttReader) (flags byte, pi uint16, topic string, payload []byte) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
	}
	flags = b & mqttPacketFlagMask
	start := r.pos
	topic, err := r.readString("topic name")
	if err != nil {
		t.Fatal(err)
	}
	qos := (flags & mqttPubFlagQoS) >> 1
	if qos > 0 {
		pi, err = r.readUint16("packet identifier")
		if err != nil {
			t.Fatal(err)
		}
	}
	msgLen := pl - (r.pos - start)
	if r.pos+msgLen > len(r.buf) {
		t.Fatalf("computed message length goes beyond buffer: ml=%v pos=%v lenBuf=%v",
			msgLen, r.pos, len(r.buf))
	}
	payload = r.buf[r.pos : r.pos+msgLen]
	r.pos += msgLen
	return flags, pi, topic, payload
}

func testMQTTSendPIPacket(packetType byte, t testing.TB, c net.Conn, pi uint16) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(packetType)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing packet type %v: %v", packetType, err)
	}
}

func testMQTTWritePublishPacket(t testing.TB, w *mqttWriter, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	t.Helper()
	w.WritePublishHeader(pi, qos, dup, retain, false, []byte(topic), nil, len(payload))
	if _, err := w.Write(payload); err != nil {
		t.Fatalf("Error writing PUBLISH proto: %v", err)
	}
}

func testMQTTSendPublishPacket(t testing.TB, c net.Conn, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	t.Helper()
	c.SetWriteDeadline(time.Now().Add(testMQTTTimeout))
	_, header := mqttMakePublishHeader(pi, qos, dup, retain, false, []byte(topic), nil, len(payload))
	if _, err := c.Write(header); err != nil {
		t.Fatalf("Error writing PUBLISH header: %v", err)
	}
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("Error writing PUBLISH payload: %v", err)
	}
	c.SetWriteDeadline(time.Time{})
}

func testMQTTPublish(t testing.TB, c net.Conn, r *mqttReader, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	t.Helper()
	testMQTTSendPublishPacket(t, c, qos, dup, retain, topic, pi, payload)
	switch qos {
	case 1:
		b, _ := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPubAck {
			t.Fatalf("Expected PUBACK packet %x, got %x", mqttPacketPubAck, pt)
		}
		rpi, err := r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}

	case 2:
		b, _ := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPubRec {
			t.Fatalf("Expected PUBREC packet %x, got %x", mqttPacketPubRec, pt)
		}
		rpi, err := r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}

		testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, c, pi)

		b, _ = testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPubComp {
			t.Fatalf("Expected PUBCOMP packet %x, got %x", mqttPacketPubComp, pt)
		}
		rpi, err = r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}

		testMQTTFlush(t, c, nil, r)
	}
}

func TestMQTTParsePub(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		flags   byte
		proto   []byte
		pl      int
		err     string
		wantErr bool
	}{
		{"qos not supported", (3 << 1), nil, 0, "QoS=3 is invalid in MQTT", false},
		{"packet in buffer error", 0, nil, 10, io.ErrUnexpectedEOF.Error(), false},
		{"error on topic", 0, []byte{0, 3, 'f', 'o'}, 4, "topic", false},
		{"empty topic", 0, []byte{0, 0}, 2, errMQTTTopicIsEmpty.Error(), false},
		{"wildcards topic", 0, []byte{0, 1, '#'}, 3, "wildcards not allowed", false},
		{"invalid utf8 topic", 0, []byte{0, 1, 241}, 3, "", true},
		{"topic with null byte", 0, []byte{0, 3, 'f', 0, 'o'}, 5, "", true},
		{"error on packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o'}, 5, "packet identifier", false},
		{"invalid packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o', 0, 0}, 7, errMQTTPacketIdentifierIsZero.Error(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			pp := &mqttPublish{flags: test.flags}
			err := c.mqttParsePub(r, test.pl, pp, false)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTParsePIMsg(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		proto []byte
		err   string
	}{
		{"packet in buffer error", nil, io.ErrUnexpectedEOF.Error()},
		{"error reading packet identifier", []byte{0}, "packet identifier"},
		{"invalid packet identifier", []byte{0, 0}, errMQTTPacketIdentifierIsZero.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			if _, err := mqttParsePIPacket(r); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTPublish(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mcp, mpr, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 2, false, false, "foo", 2, []byte("msg"))
}

func TestMQTTQoS2PubReject(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.rejectQoS2Pub = true
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mcp, mpr, 1, false, false, "foo", 1, []byte("msg"))

	testMQTTSendPublishPacket(t, mcp, 2, false, false, "foo", 2, []byte("msg"))
	testMQTTExpectDisconnect(t, mcp)
}

func TestMQTTSub(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	for _, test := range []struct {
		name           string
		mqttSubTopic   string
		natsPubSubject string
		mqttPubTopic   string
		ok             bool
	}{
		{"1 level match", "foo", "foo", "foo", true},
		{"1 level no match", "foo", "bar", "bar", false},
		{"2 levels match", "foo/bar", "foo.bar", "foo/bar", true},
		{"2 levels no match", "foo/bar", "foo.baz", "foo/baz", false},
		{"3 levels match", "/foo/bar", "/.foo.bar", "/foo/bar", true},
		{"3 levels no match", "/foo/bar", "/.foo.baz", "/foo/baz", false},

		{"single level wc", "foo/+", "foo.bar.baz", "foo/bar/baz", false},
		{"single level wc", "foo/+", "foo.bar./", "foo/bar/", false},
		{"single level wc", "foo/+", "foo.bar", "foo/bar", true},
		{"single level wc", "foo/+", "foo./", "foo/", true},
		{"single level wc", "foo/+", "foo", "foo", false},
		{"single level wc", "foo/+", "/.foo", "/foo", false},

		{"multiple level wc", "foo/#", "foo.bar.baz./", "foo/bar/baz/", true},
		{"multiple level wc", "foo/#", "foo.bar.baz", "foo/bar/baz", true},
		{"multiple level wc", "foo/#", "foo.bar./", "foo/bar/", true},
		{"multiple level wc", "foo/#", "foo.bar", "foo/bar", true},
		{"multiple level wc", "foo/#", "foo./", "foo/", true},
		{"multiple level wc", "foo/#", "foo", "foo", true},
		{"multiple level wc", "foo/#", "/.foo", "/foo", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: test.mqttSubTopic, qos: 0}}, []byte{0})
			testMQTTFlush(t, mc, nil, r)

			natsPub(t, nc, test.natsPubSubject, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}

			testMQTTPublish(t, mcp, mpr, 0, false, false, test.mqttPubTopic, 0, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}
		})
	}
}

func TestMQTTSubQoS2(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	topic := "foo/bar/baz"
	mqttTopic0 := "foo/#"
	mqttTopic1 := "foo/bar/#"
	mqttTopic2 := topic
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic0, qos: 0}}, []byte{0})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic1, qos: 1}}, []byte{1})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic2, qos: 2}}, []byte{2})
	testMQTTFlush(t, mc, nil, r)

	for pubQoS, expectedCounts := range map[byte]map[byte]int{
		0: {0: 3},
		1: {0: 1, 1: 2},
		2: {0: 1, 1: 1, 2: 1},
	} {
		t.Run(fmt.Sprintf("pubQoS %v", pubQoS), func(t *testing.T) {
			pubPI := uint16(456)

			testMQTTPublish(t, mcp, mpr, pubQoS, false, false, topic, pubPI, []byte("msg"))

			qosCounts := map[byte]int{}
			delivered := map[uint16]byte{}

			// We have 3 subscriptions, each should receive the message, with the
			// QoS that maybe "trimmed" to that of the subscription.
			for i := 0; i < 3; i++ {
				flags, pi := testMQTTGetPubMsg(t, mc, r, topic, []byte("msg"))
				delivered[pi] = flags
				qosCounts[mqttGetQoS(flags)]++
			}

			for pi, flags := range delivered {
				switch mqttGetQoS(flags) {
				case 1:
					testMQTTSendPIPacket(mqttPacketPubAck, t, mc, pi)

				case 2:
					testMQTTSendPIPacket(mqttPacketPubRec, t, mc, pi)
					testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
					testMQTTSendPIPacket(mqttPacketPubComp, t, mc, pi)
				}
			}

			if !reflect.DeepEqual(qosCounts, expectedCounts) {
				t.Fatalf("Expected QoS %#v, got %#v", expectedCounts, qosCounts)
			}
		})
	}
}

func TestMQTTSubQoS1(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mqttTopic := "foo/bar"

	// Subscribe with QoS 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic, qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish from NATS, which means QoS 0
	natsPub(t, nc, "foo.bar", []byte("NATS"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))

	// Publish from MQTT with QoS 0
	testMQTTPublish(t, mcp, mpr, 0, false, false, mqttTopic, 0, []byte("msg"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))

	// Publish from MQTT with QoS 1
	testMQTTPublish(t, mcp, mpr, 1, false, false, mqttTopic, 1, []byte("msg"))
	pflags1, pi1 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags1 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags1)
	}
	pflags2, pi2 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags2 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags2)
	}
	if pi1 == pi2 {
		t.Fatalf("packet identifier for message 1: %v should be different from message 2", pi1)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, mc, pi1)
	testMQTTSendPIPacket(mqttPacketPubAck, t, mc, pi2)
}

func getSubQoS(sub *subscription) int {
	if sub.mqtt != nil {
		return int(sub.mqtt.qos)
	}
	return -1
}

func TestMQTTSubDups(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Test with single SUBSCRIBE protocol but multiple filters
	filters := []*mqttFilter{
		{filter: "foo", qos: 1},
		{filter: "foo", qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{1, 0})
	testMQTTFlush(t, mc, nil, r)

	// And also with separate SUBSCRIBE protocols
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	// Ask for QoS 1 but server will downgrade to 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received only once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Check that the QoS for subscriptions have been updated to the latest received filter
	var err error
	subc := testMQTTGetClient(t, s, "sub")
	subc.mu.Lock()
	if subc.opts.Username != "sub" {
		err = fmt.Errorf("wrong user name")
	}
	if err == nil {
		if sub := subc.subs["foo"]; sub == nil || getSubQoS(sub) != 0 {
			err = fmt.Errorf("subscription foo QoS should be 0, got %v", getSubQoS(sub))
		}
	}
	if err == nil {
		if sub := subc.subs["bar"]; sub == nil || getSubQoS(sub) != 1 {
			err = fmt.Errorf("subscription bar QoS should be 1, got %v", getSubQoS(sub))
		}
	}
	subc.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Now subscribe on "foo/#" which means that a PUBLISH on "foo" will be received
	// by this subscription and also the one on "foo".
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	checkWCSub := func(expectedQoS int) {
		t.Helper()

		subc.mu.Lock()
		defer subc.mu.Unlock()

		// When invoked with expectedQoS==1, we have the following subs:
		// foo (QoS-0), bar (QoS-1), foo.> (QoS-1)
		// which means (since QoS-1 have a JS consumer + sub for delivery
		// and foo.> causes a "foo fwc") that we should have the following
		// number of NATS subs: foo (1), bar (2), foo.> (2) and "foo fwc" (2),
		// so total=7.
		// When invoked with expectedQoS==0, it means that we have replaced
		// foo/# QoS-1 to QoS-0, so we should have 2 less NATS subs,
		// so total=5
		expected := 7
		if expectedQoS == 0 {
			expected = 5
		}
		if lenmap := len(subc.subs); lenmap != expected {
			t.Fatalf("Subs map should have %v entries, got %v", expected, lenmap)
		}
		if sub, ok := subc.subs["foo.>"]; !ok {
			t.Fatal("Expected sub foo.> to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo.> QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		if sub, ok := subc.subs["foo fwc"]; !ok {
			t.Fatal("Expected sub foo fwc to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo fwc QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		// Make sure existing sub on "foo" qos was not changed.
		if sub, ok := subc.subs["foo"]; !ok {
			t.Fatal("Expected sub foo to be present but was not")
		} else if getSubQoS(sub) != 0 {
			t.Fatalf("Expected sub foo QoS to be 0, got %v", getSubQoS(sub))
		}
	}
	checkWCSub(1)

	// Sub again on same subject with lower QoS
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	checkWCSub(0)
}

func TestMQTTSubWithSpaces(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo bar", qos: 0}}, []byte{mqttSubAckFailure})
}

// Filters containing characters that would corrupt the NATS wire protocol
// (\t, \n, \r, \f) must result in a per-filter SUBACK failure, not a
// connection drop. mqttFilterToNATSSubject rejects them; the SUBSCRIBE
// parser logs and continues so the failure surfaces as mqttSubAckFailure.
func TestMQTTSubWithControlChars(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name   string
		filter string
	}{
		{"tab", "foo\tbar"},
		{"line feed", "foo\nbar"},
		{"carriage return", "foo\rbar"},
		{"form feed", "foo\fbar"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			// Mix a bad filter with a good one to prove the good one still
			// succeeds (the connection is not torn down by the bad filter).
			testMQTTSub(t, 1, mc, r,
				[]*mqttFilter{{filter: test.filter, qos: 0}, {filter: "good/topic", qos: 0}},
				[]byte{mqttSubAckFailure, 0})
		})
	}
}

func TestMQTTSubCaseSensitive(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "Foo/Bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "Foo/Bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("msg"))

	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "Foo.Bar", []byte("nats"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("nats"))

	natsPub(t, nc, "foo.bar", []byte("nats"))
	testMQTTExpectNothing(t, r)
}

func TestMQTTPubSubMatrix(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		natsPub     bool
		mqttPub     bool
		mqttPubQoS  byte
		natsSub     bool
		mqttSubQoS0 bool
		mqttSubQoS1 bool
	}{
		{"NATS to MQTT sub QoS-0", true, false, 0, false, true, false},
		{"NATS to MQTT sub QoS-1", true, false, 0, false, false, true},
		{"NATS to MQTT sub QoS-0 and QoS-1", true, false, 0, false, true, true},

		{"MQTT QoS-0 to NATS sub", false, true, 0, true, false, false},
		{"MQTT QoS-0 to MQTT sub QoS-0", false, true, 0, false, true, false},
		{"MQTT QoS-0 to MQTT sub QoS-1", false, true, 0, false, false, true},
		{"MQTT QoS-0 to NATS sub and MQTT sub QoS-0", false, true, 0, true, true, false},
		{"MQTT QoS-0 to NATS sub and MQTT sub QoS-1", false, true, 0, true, false, true},
		{"MQTT QoS-0 to all subs", false, true, 0, true, true, true},

		{"MQTT QoS-1 to NATS sub", false, true, 1, true, false, false},
		{"MQTT QoS-1 to MQTT sub QoS-0", false, true, 1, false, true, false},
		{"MQTT QoS-1 to MQTT sub QoS-1", false, true, 1, false, false, true},
		{"MQTT QoS-1 to NATS sub and MQTT sub QoS-0", false, true, 1, true, true, false},
		{"MQTT QoS-1 to NATS sub and MQTT sub QoS-1", false, true, 1, true, false, true},
		{"MQTT QoS-1 to all subs", false, true, 1, true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()

			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			mc1, r1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc1.Close()
			testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

			mc2, r2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc2.Close()
			testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

			// First setup subscriptions based on test options.
			var ns *nats.Subscription
			if test.natsSub {
				ns = natsSubSync(t, nc, "foo")
			}
			if test.mqttSubQoS0 {
				testMQTTSub(t, 1, mc1, r1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
				testMQTTFlush(t, mc1, nil, r1)
			}
			if test.mqttSubQoS1 {
				testMQTTSub(t, 1, mc2, r2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
				testMQTTFlush(t, mc2, nil, r2)
			}

			// Just as a barrier
			natsFlush(t, nc)

			// Now publish
			if test.natsPub {
				natsPub(t, nc, "foo", []byte("msg"))
			} else {
				testMQTTPublish(t, mc, r, test.mqttPubQoS, false, false, "foo", 1, []byte("msg"))
			}

			// Check message received
			if test.natsSub {
				natsNexMsg(t, ns, time.Second)
				// Make sure no other is received
				if msg, err := ns.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Should not have gotten a second message, got %v", msg)
				}
			}
			if test.mqttSubQoS0 {
				testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, []byte("msg"))
				testMQTTExpectNothing(t, r1)
			}
			if test.mqttSubQoS1 {
				var expectedFlag byte
				if test.mqttPubQoS > 0 {
					expectedFlag = test.mqttPubQoS << 1
				}
				testMQTTCheckPubMsg(t, mc2, r2, "foo", expectedFlag, []byte("msg"))
				testMQTTExpectNothing(t, r2)
			}
		})
	}
}

func TestMQTTPreventSubWithMQTTSubPrefix(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r,
		[]*mqttFilter{{filter: strings.ReplaceAll(mqttSubPrefix, ".", "/") + "foo/bar", qos: 1}},
		[]byte{mqttSubAckFailure})
}

func TestMQTTTrackPendingOverrun(t *testing.T) {
	t.Parallel()
	sess := mqttSession{}
	// Real sessions always have a positive max pending (mqttSessionCreate uses
	// the configured MaxAckPending or the default); use the maximum here so this
	// PI-allocation test exercises the full packet-identifier range. With the
	// zero value the effective in-flight cap would be 0 and nothing could be
	// published.
	sess.maxp = 0xFFFF

	sess.last_pi = 0xFFFF
	pi := sess.trackPublishRetained(1)
	if pi != 1 {
		t.Fatalf("Expected 1, got %v", pi)
	}

	p := &mqttPending{}
	for i := 1; i <= 0xFFFF; i++ {
		sess.pendingPublish[uint16(i)] = p
	}
	pi, _ = sess.trackPublish("test", "test")
	if pi != 0 {
		t.Fatalf("Expected 0, got %v", pi)
	}

	delete(sess.pendingPublish, 1234)
	pi = sess.trackPublishRetained(1)
	if pi != 1234 {
		t.Fatalf("Expected 1234, got %v", pi)
	}
}

func TestMQTTSubRestart(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Start an MQTT subscription QoS=1 on "foo"
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Now start a NATS subscription on ">" (anything that would match the JS consumer delivery subject)
	natsSubSync(t, nc, ">")
	natsFlush(t, nc)

	// Restart the MQTT client
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	mc, r = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Restart an MQTT subscription QoS=1 on "foo"
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// Publish a message QoS1
	testMQTTPublish(t, pc, pr, 1, false, false, "foo", 1, []byte("msg1"))
	// Make sure we receive it
	testMQTTCheckPubMsg(t, mc, r, "foo", mqttPubQos1, []byte("msg1"))

	// Now "restart" the subscription but as a Qos0
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish a message QoS
	testMQTTPublish(t, pc, pr, 1, false, false, "foo", 1, []byte("msg2"))
	// Make sure we receive but as a QoS0
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg2"))
}

func testMQTTConnectDisconnect(t *testing.T, o *Options, clientID string, clean bool, found bool) {
	t.Helper()
	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: clean}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, found)
	testMQTTDisconnectEx(t, mc, nil, false)
	mc.Close()
}

func TestMQTTRetainedNoMsgBodyCorruption(t *testing.T) {
	t.Parallel()
	f := func() {
		o := testMQTTDefaultOptions()
		s := testMQTTRunServer(t, o)
		defer testMQTTShutdownServer(s)

		// Send a retained message.
		c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTPublish(t, c, r, 0, false, true, "foo/bar", 0, []byte("retained 1"))
		testMQTTFlush(t, c, nil, r)

		checkRetained := func(msg string) {
			t.Helper()
			c, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
			testMQTTCheckPubMsg(t, c, r, "foo/bar", mqttPubFlagRetain, []byte(msg))
		}
		// Subscribe to make it load into the cache.
		checkRetained("retained 1")

		// Now send another one.
		testMQTTPublish(t, c, r, 0, false, true, "foo/bar", 0, []byte("retained 2"))
		testMQTTFlush(t, c, nil, r)

		// Check it is updated
		checkRetained("retained 2")

		// Now we will simulate an update coming from another server
		// if we were in cluster mode.
		nc := natsConnect(t, s.ClientURL())
		defer nc.Close()

		msg := nats.NewMsg("$MQTT.rmsgs.foo.bar")
		msg.Header.Set(mqttNatsRetainedMessageOrigin, "XXXXXXXX")
		msg.Header.Set(mqttNatsRetainedMessageTopic, "foo/bar")
		msg.Header.Set(mqttNatsRetainedMessageFlags, "1")
		msg.Data = []byte("retained 3")
		nc.PublishMsg(msg)
		natsFlush(t, nc)

		// Have a continuous flow of updates coming in
		wg := sync.WaitGroup{}
		wg.Add(1)
		ch := make(chan struct{})
		go func() {
			defer wg.Done()
			for {
				nc.PublishMsg(msg)
				select {
				case <-ch:
					return
				default:
				}
			}
		}()

		// Retrieve the account session manager using the "pub" client we have.
		as := testMQTTGetAccountSessionManager(t, s, "pub")
		as.mu.RLock()
		cache := as.rmsCache
		as.mu.RUnlock()

		// Wait to make sure at least the first update occurs
		checkFor(t, 5*time.Second, 10*time.Millisecond, func() error {
			v, ok := cache.Load("foo.bar")
			if !ok {
				return errors.New("not in the cache")
			}
			rm := v.(*mqttRetainedMsg)
			if !bytes.Equal(rm.Msg, []byte("retained 3")) {
				return fmt.Errorf("Retained message not updated, got %q", rm.Msg)
			}
			return nil
		})
		// Repeat starting a subscription to check the retained message and
		// make sure it is not corrupted. With the bug, the payload will at
		// the very least contain trailing "\r\n" and possibly be corrupted
		// (and the race detector would report a race).
		for range 50 {
			checkRetained("retained 3")
		}
		close(ch)
		wg.Wait()
	}
	for range 5 {
		f()
	}
}

type remoteConnSameClientIDLogger struct {
	DummyLogger
	warn chan string
}

func (l *remoteConnSameClientIDLogger) Warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "remote connection has started with the same client ID") {
		l.warn <- msg
	}
}

func TestMQTTParseUnsub(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		proto   []byte
		b       byte
		pl      int
		err     string
		wantErr bool
	}{
		{"reserved flag", nil, 3, 0, "wrong unsubscribe reserved flags", false},
		{"ensure packet loaded", []byte{1, 2}, mqttUnsubscribeFlags, 10, io.ErrUnexpectedEOF.Error(), false},
		{"error reading packet id", []byte{1}, mqttUnsubscribeFlags, 1, "reading packet identifier", false},
		{"packet id cannot be zero", []byte{0, 0}, mqttUnsubscribeFlags, 2, errMQTTPacketIdentifierIsZero.Error(), false},
		{"missing filters", []byte{0, 1}, mqttUnsubscribeFlags, 2, "subscribe protocol must contain at least 1 topic filter", false},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttUnsubscribeFlags, 5, "topic filter", false},
		{"empty topic", []byte{0, 1, 0, 0}, mqttUnsubscribeFlags, 4, errMQTTTopicFilterCannotBeEmpty.Error(), false},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttUnsubscribeFlags, 5, "invalid utf8 for topic filter", false},
		{"topic with null byte", []byte{0, 1, 0, 3, 'a', 0, 'b'}, mqttUnsubscribeFlags, 7, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			_, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, false)
			if test.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got none")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTUnsub(t *testing.T, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter) {
	t.Helper()
	w := newMQTTWriter(0)
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter)
	}
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes([]byte(f.filter))
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing UNSUBSCRIBE protocol: %v", err)
	}
	b, _ := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketUnsubAck {
		t.Fatalf("Expected UNSUBACK packet %x, got %x", mqttPacketUnsubAck, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
}

func TestMQTTUnsub(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	// Unsubscribe
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo"}})

	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Use of wildcards subs
	filters := []*mqttFilter{
		{filter: "foo/bar", qos: 0},
		{filter: "foo/#", qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{0, 0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and check that message received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))

	// Unsub the wildcard one
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#"}})
	// Publish and check that message received once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Unsub last
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo/bar"}})
	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)
}

func testMQTTExpectDisconnect(t testing.TB, c net.Conn) {
	t.Helper()
	buf, err := testMQTTRead(c)
	if err == nil {
		t.Fatalf("Expected connection to be disconnected, got %s", buf)
	}
	// Distinguish real disconnection (EOF, connection reset) from timeout
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("Expected a disconnect but got a timeout error")
	}
}

func TestMQTTPublishTopicErrors(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name  string
		topic string
	}{
		{"empty", ""},
		{"with single level wildcard", "foo/+"},
		{"with multiple level wildcard", "foo/#"},
		{"with tab", "foo\tbar"},
		{"with line feed", "foo\nbar"},
		{"with carriage return", "foo\rbar"},
		{"with form feed", "foo\fbar"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTPublish(t, mc, r, 0, false, false, test.topic, 0, []byte("msg"))
			testMQTTExpectDisconnect(t, mc)
		})
	}
}

func testMQTTDisconnect(t testing.TB, c net.Conn, bw *bufio.Writer) {
	t.Helper()
	testMQTTDisconnectEx(t, c, bw, true)
}

func testMQTTDisconnectEx(t testing.TB, c net.Conn, bw *bufio.Writer, wait bool) {
	t.Helper()
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketDisconnect)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	if wait {
		testMQTTExpectDisconnect(t, c)
	}
}

func TestMQTTWill(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub := natsSubSync(t, nc, "will.topic")
	natsFlush(t, nc)

	willMsg := []byte("bye")

	for _, test := range []struct {
		name         string
		willExpected bool
		willQoS      byte
	}{
		{"will qos 0", true, 0},
		{"will qos 1", true, 1},
		{"will qos 2", true, 2},
		{"proper disconnect no will", false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcs.Close()
			testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: 2}}, []byte{2})
			testMQTTFlush(t, mcs, nil, rs)

			mc, r := testMQTTConnect(t,
				&mqttConnInfo{
					cleanSess: true,
					will: &mqttWill{
						topic:   []byte("will/topic"),
						message: willMsg,
						qos:     test.willQoS,
					},
				}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			if test.willExpected {
				mc.Close()
				testMQTTCheckPubMsg(t, mcs, rs, "will/topic", test.willQoS<<1, willMsg)
				wm := natsNexMsg(t, sub, time.Second)
				if !bytes.Equal(wm.Data, willMsg) {
					t.Fatalf("Expected will message to be %q, got %q", willMsg, wm.Data)
				}
			} else {
				testMQTTDisconnect(t, mc, nil)
				testMQTTExpectNothing(t, rs)
				if wm, err := sub.NextMsg(100 * time.Millisecond); err == nil {
					t.Fatalf("Should not have receive a message, got subj=%q data=%q",
						wm.Subject, wm.Data)
				}
			}
		})
	}
}

func TestMQTTQoS2WillReject(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.rejectQoS2Pub = true
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: 2}}, []byte{2})
	testMQTTFlush(t, mcs, nil, rs)

	mc, r := testMQTTConnect(t,
		&mqttConnInfo{
			cleanSess: true,
			will: &mqttWill{
				topic:   []byte("will/topic"),
				message: []byte("bye"),
				qos:     2,
			},
		}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCQoS2WillRejected, false)
}

func TestMQTTWillRetain(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		pubQoS byte
		subQoS byte
	}{
		{"pub QoS0 sub QoS0", 0, 0},
		{"pub QoS0 sub QoS1", 0, 1},
		{"pub QoS1 sub QoS0", 1, 0},
		{"pub QoS1 sub QoS1", 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			mces, res := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mces.Close()
			testMQTTCheckConnAck(t, res, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mces, res, []*mqttFilter{{filter: "will/#", qos: test.subQoS}}, []byte{test.subQoS})

			willTopic := []byte("will/topic")
			willMsg := []byte("bye")

			mc, r := testMQTTConnect(t,
				&mqttConnInfo{
					cleanSess: true,
					will: &mqttWill{
						topic:   willTopic,
						message: willMsg,
						qos:     test.pubQoS,
						retain:  true,
					},
				}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			// Disconnect the client
			mc.Close()

			// Wait for the server to process the connection close, which will
			// cause the "will" message to be published (and retained).
			checkClientsCount(t, s, 1)

			// Create subscription on will topic and expect will message.
			mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcs.Close()
			testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: test.subQoS}}, []byte{test.subQoS})
			pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "will/topic", willMsg)
			if !mqttIsRetained(pflags) {
				t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
			}
			// Expected QoS will be the lesser of the pub/sub QoS.
			expectedQoS := test.pubQoS
			if test.subQoS == 0 {
				expectedQoS = 0
			}
			if qos := mqttGetQoS(pflags); qos != expectedQoS {
				t.Fatalf("expected qos to be %v, got %v", expectedQoS, qos)
			}

			// The existing subscription (prior to sending the will) should receive
			// the will but the retain flag should not be set.
			pflags, _ = testMQTTGetPubMsg(t, mces, res, "will/topic", willMsg)
			if mqttIsRetained(pflags) {
				t.Fatalf("expected retain flag to not be set, it was: %v", pflags)
			}
			// Expected QoS will be the lesser of the pub/sub QoS.
			if qos := mqttGetQoS(pflags); qos != expectedQoS {
				t.Fatalf("expected qos to be %v, got %v", expectedQoS, qos)
			}
		})
	}
}

func TestMQTTPublishRetain(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	buf := strings.Builder{}
	for i := 0; i < 128; i++ { // 128Kb
		for j := 0; j < 64; j++ { // 16 * 64 = 1Kb
			buf.Write([]byte("0123456789abcdef"))
		}
	}
	large := buf.String()

	for _, test := range []struct {
		name          string
		retained      bool
		sentValue     string
		expectedValue string
		subGetsIt     bool
	}{
		{"publish large retained", true, large, large, true},
		{"publish retained", true, "retained", "retained", true},
		{"publish not retained", false, "not retained", "retained", true},
		{"remove retained", true, "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc1.Close()
			testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, mc1, rs1, 0, false, test.retained, "foo", 0, []byte(test.sentValue))
			testMQTTFlush(t, mc1, nil, rs1)

			mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc2.Close()
			testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})

			if test.subGetsIt {
				pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "foo", []byte(test.expectedValue))
				if !mqttIsRetained(pflags) {
					t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
				}
			} else {
				testMQTTExpectNothing(t, rs2)
			}

			testMQTTDisconnect(t, mc1, nil)
			testMQTTDisconnect(t, mc2, nil)
		})
	}
}

func TestMQTTQoS2RetainedReject(t *testing.T) {
	t.Parallel()
	// Start the server with QOS2 enabled, and submit retained messages with QoS
	// 1 and 2.
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc1, rs1, 2, false, true, "foo", 1, []byte("qos2 failed"))
	testMQTTPublish(t, mc1, rs1, 1, false, true, "bar", 2, []byte("qos1 retained"))
	testMQTTFlush(t, mc1, nil, rs1)
	testMQTTDisconnect(t, mc1, nil)
	mc1.Close()
	s.Shutdown()

	// Restart the server with QOS2 disabled; we should be using the same
	// JetStream store, so the retained message should still be there.
	o.MQTT.rejectQoS2Pub = true
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "bar/#", qos: 2}}, []byte{2})
	pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "bar", []byte("qos1 retained"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
	}

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/#", qos: 2}}, []byte{2})
	testMQTTExpectNothing(t, rs2)
	testMQTTDisconnect(t, mc2, nil)
}

func TestMQTTRetainFlag(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/0", 0, []byte("flag set"))
	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/1", 0, []byte("flag set"))
	testMQTTFlush(t, mc1, nil, rs1)

	mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/0", qos: 0}}, []byte{0})
	pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "foo/0", []byte("flag set"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
	}

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/1", qos: 1}}, []byte{1})
	pflags, _ = testMQTTGetPubMsg(t, mc2, rs2, "foo/1", []byte("flag set"))
	if !mqttIsRetained(pflags) {
		t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
	}

	// For existing subscriptions, RETAIN flag should not be set: [MQTT-3.3.1-9].
	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/0", 0, []byte("flag not set"))
	testMQTTFlush(t, mc1, nil, rs1)

	pflags, _ = testMQTTGetPubMsg(t, mc2, rs2, "foo/0", []byte("flag not set"))
	if mqttIsRetained(pflags) {
		t.Fatalf("retain flag should not have been set, it was: flags=%v", pflags)
	}

	testMQTTPublish(t, mc1, rs1, 0, false, true, "foo/1", 0, []byte("flag not set"))
	testMQTTFlush(t, mc1, nil, rs1)

	pflags, _ = testMQTTGetPubMsg(t, mc2, rs2, "foo/1", []byte("flag not set"))
	if mqttIsRetained(pflags) {
		t.Fatalf("retain flag should not have been set, it was: flags=%v", pflags)
	}
}

func TestMQTTCleanSession(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		clientID:  "me",
		cleanSess: false,
	}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	ci.cleanSess = true
	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
}

func TestMQTTDuplicateClientID(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		clientID:  "me",
		cleanSess: false,
	}
	c1, r1 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The old client should be disconnected.
	testMQTTExpectDisconnect(t, c1)
}

func TestMQTTPersistedSession(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}

	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, c, r,
		[]*mqttFilter{
			{filter: "foo/#", qos: 1},
			{filter: "bar", qos: 1},
			{filter: "baz", qos: 0},
		},
		[]byte{1, 1, 0})
	testMQTTFlush(t, c, nil, r)

	// Shutdown server, close connection and restart server. It should
	// have restored the session and consumers.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Create a publisher that will send qos1 so we verify that messages
	// are stored for the persisted sessions.
	c, r = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c, r, 1, false, false, "foo/bar", 1, []byte("msg0"))
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Recreate session
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Since consumers have been recovered, messages should be received
	// (MQTT does not need client to recreate consumers for a recovered
	// session)

	// Check that qos1 publish message is received.
	testMQTTCheckPubMsg(t, c, r, "foo/bar", mqttPubQos1, []byte("msg0"))

	// Flush before publishing: the CONNACK's arrival at the client does not order this
	// publish against the tail of the resume the broker is still running behind it.
	testMQTTFlush(t, c, nil, r)

	// Now publish some messages to all subscriptions.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo.bar", []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg1"))

	natsPub(t, nc, "foo", []byte("msg2"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg2"))

	natsPub(t, nc, "bar", []byte("msg3"))
	testMQTTCheckPubMsg(t, c, r, "bar", 0, []byte("msg3"))

	natsPub(t, nc, "baz", []byte("msg4"))
	testMQTTCheckPubMsg(t, c, r, "baz", 0, []byte("msg4"))

	// Now unsub "bar" and verify that message published on this topic
	// is not received.
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar"}})
	natsPub(t, nc, "bar", []byte("msg5"))
	testMQTTExpectNothing(t, r)

	nc.Close()
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Recreate a client
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	nc = natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo.bar", []byte("msg6"))
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg6"))

	natsPub(t, nc, "foo", []byte("msg7"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg7"))

	// Make sure that we did not recover bar.
	natsPub(t, nc, "bar", []byte("msg8"))
	testMQTTExpectNothing(t, r)

	natsPub(t, nc, "baz", []byte("msg9"))
	testMQTTCheckPubMsg(t, c, r, "baz", 0, []byte("msg9"))

	// Have the sub client send a subscription downgrading the qos1 subscription.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)

	nc.Close()
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Recreate the sub client
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Publish as a qos1
	c2, r2 := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, c2, r2, 1, false, false, "foo/bar", 1, []byte("msg10"))

	// Verify that it is received as qos0 which is the qos of the subscription.
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg10"))

	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTDisconnect(t, c2, nil)
	c2.Close()

	// Finally, recreate the sub with clean session and ensure that all is gone
	cisub.cleanSess = true
	for i := 0; i < 2; i++ {
		c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

		nc = natsConnect(t, s.ClientURL())
		defer nc.Close()

		natsPub(t, nc, "foo.bar", []byte("msg11"))
		testMQTTExpectNothing(t, r)

		natsPub(t, nc, "foo", []byte("msg12"))
		testMQTTExpectNothing(t, r)

		// Make sure that we did not recover bar.
		natsPub(t, nc, "bar", []byte("msg13"))
		testMQTTExpectNothing(t, r)

		natsPub(t, nc, "baz", []byte("msg14"))
		testMQTTExpectNothing(t, r)

		testMQTTDisconnect(t, c, nil)
		c.Close()
		nc.Close()

		s.Shutdown()
		o.Port = -1
		o.MQTT.Port = -1
		o.StoreDir = dir
		s = testMQTTRunServer(t, o)
		// There is already the defer for shutdown at top of function
	}
}

func TestMQTTRejectsHashCollidingClientID(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// These IDs were found to collide under getHash()
	victimID := "cid-03579431"
	attackerID := "cid-09191235"
	require_True(t, getHash(victimID) == getHash(attackerID))

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: victimID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	attacker, r := testMQTTConnect(t, &mqttConnInfo{clientID: attackerID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer attacker.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCIdentifierRejected, false)
	testMQTTExpectDisconnect(t, attacker)
	attacker.Close()

	c, r = testMQTTConnect(t, &mqttConnInfo{clientID: victimID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTFlush(t, c, nil, r)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo", []byte("msg"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg"))
}

func TestMQTTRecoverSessionAndAddNewSub(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub := &mqttConnInfo{clientID: "sub1", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Shutdown server, close connection and restart server. It should
	// have restored the session and consumers.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// No need for defer since it is done top of function

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// Now add sub and make sure it does not crash
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	// Now repeat with a new client but without server restart.
	cisub2 := &mqttConnInfo{clientID: "sub2", cleanSess: false}
	c2, r2 := testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c2, nil)
	c2.Close()

	c2, r2 = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)
}

func TestMQTTRecoverSessionWithSubAndClientResendSub(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub1 := &mqttConnInfo{clientID: "sub1", cleanSess: false}
	c, r := testMQTTConnect(t, cisub1, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Have a client send a SUBSCRIBE protocol for foo, QoS1
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Restart the server now.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// No need for defer since it is done top of function

	// Now restart the client. Since the client was created with cleanSess==false,
	// the server will have recorded the subscriptions for this client.
	c, r = testMQTTConnect(t, cisub1, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// At this point, the server has recreated the subscription on foo, QoS1.

	// For applications that restart, it is possible (likely) that they
	// will resend their SUBSCRIBE protocols, so do so now:
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	checkNumSub := func(clientID string) {
		t.Helper()

		// Find the MQTT client...
		mc := testMQTTGetClient(t, s, clientID)

		// Check how many NATS subscriptions are registered.
		var fooSub int
		var otherSub int
		mc.mu.Lock()
		for _, sub := range mc.subs {
			switch string(sub.subject) {
			case "foo":
				fooSub++
			default:
				otherSub++
			}
		}
		mc.mu.Unlock()

		// We should have 2 subscriptions, one on "foo", and one for the JS durable
		// consumer's delivery subject.
		if fooSub != 1 {
			t.Fatalf("Expected 1 sub on 'foo', got %v", fooSub)
		}
		if otherSub != 1 {
			t.Fatalf("Expected 1 subscription for JS durable, got %v", otherSub)
		}
	}
	checkNumSub("sub1")

	c.Close()

	// Now same but without the server restart in-between.
	cisub2 := &mqttConnInfo{clientID: "sub2", cleanSess: false}
	c, r = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, c, nil)
	c.Close()
	// Restart client
	c, r = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	// Check client subs
	checkNumSub("sub2")
}

func TestMQTTFlappingSession(t *testing.T) {
	// The jail must outlive the route it is observed through: the takeover alone takes 200-330 ms
	// under -race, and the jailed reconnect and its read come after it. At 250 ms the entry was
	// gone (the cleanup tick reaps it) before the check below could see it. The cleanup interval
	// stays longer than the jail, as the final absence check requires.
	mqttSessJailDur = time.Second
	mqttFlapCleanItvl = 1200 * time.Millisecond
	defer func() {
		mqttSessJailDur = mqttSessFlappingJailDur
		mqttFlapCleanItvl = mqttSessFlappingCleanupInterval
	}()

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "flapper", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Let's get a handle on the asm to check things later.
	asm := testMQTTGetAccountSessionManager(t, s, "flapper")

	// Start a new connection with the same clientID, which should replace
	// the old one and put it in the flappers map.
	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Should be disconnected...
	testMQTTExpectDisconnect(t, c)

	// Now try to reconnect "c" and we should fail. We have to do this manually,
	// since we expect it to fail.
	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c.Close()
	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c, proto); err != nil {
		t.Fatalf("Error writing protocols: %v", err)
	}
	// Misbehave and send a SUB protocol without waiting for the CONNACK
	w := newMQTTWriter(0)
	pkLen := 2 // for pi
	// Topic "foo"
	pkLen += 2 + 3 + 1
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(1)
	w.WriteBytes([]byte("foo"))
	w.WriteByte(1)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing protocols: %v", err)
	}
	// Now read the CONNACK and we should have been disconnected.
	if _, err := testMQTTRead(c); err == nil {
		t.Fatal("Expected connection to fail")
	}

	// This should be in the flappers map, but after 250ms should be cleared.
	for i := 0; i < 2; i++ {
		asm.mu.RLock()
		_, present := asm.flappers["flapper"]
		asm.mu.RUnlock()
		if i == 0 {
			if !present {
				t.Fatal("Did not find the client ID in the flappers map")
			}
			// Wait for more than the cleanup interval
			time.Sleep(mqttFlapCleanItvl + 100*time.Millisecond)
		} else if present {
			t.Fatal("The client ID should have been cleared from the map")
		}
	}
}

func TestMQTTLockedSession(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	subClientID := "sub"
	ci := &mqttConnInfo{clientID: subClientID, cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	sm := &s.mqtt.sessmgr
	sm.mu.Lock()
	asm := sm.sessions[globalAccountName]
	sm.mu.Unlock()
	if asm == nil {
		t.Fatalf("account session manager not found")
	}

	// It is possible, however unlikely, to have received CONNACK while
	// mqttProcessConnect is still running, and the session remains locked. Wait
	// for it to finish.
	checkFor(t, 250*time.Millisecond, 10*time.Millisecond, func() error {
		asm.mu.RLock()
		defer asm.mu.RUnlock()
		if _, stillLocked := asm.sessLocked[subClientID]; stillLocked {
			return fmt.Errorf("session still locked")
		}
		return nil
	})

	// Get the session for "sub"
	cli := testMQTTGetClient(t, s, subClientID)
	sess := cli.mqtt.sess

	// Pretend that the session above is locked.
	if err := asm.lockSession(sess, cli); err != nil {
		t.Fatalf("Unable to lock session: %v", err)
	}
	defer asm.unlockSession(sess)

	// Now try to connect another client that wants to use "sub".
	// We can't use testMQTTConnect() because it is going to fail.
	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c2.Close()
	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c2, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	if _, err := testMQTTRead(c2); err == nil {
		t.Fatal("Expected connection to fail")
	}

	// Now try again, but this time release the session while waiting
	// to connect and it should succeed.
	time.AfterFunc(250*time.Millisecond, func() { asm.unlockSession(sess) })
	c3, r3 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
}

func TestMQTTPersistRetainedMsg(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	c, r := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c, r, 1, false, true, "moo/foo", 1, []byte("foo1"))
	testMQTTPublish(t, c, r, 1, false, true, "moo/foo", 1, []byte("foo2"))
	testMQTTPublish(t, c, r, 1, false, true, "moo/bar", 1, []byte("bar1"))
	testMQTTPublish(t, c, r, 0, false, true, "moo/baz", 1, []byte("baz1"))
	// Remove bar
	testMQTTPublish(t, c, r, 1, false, true, "moo/bar", 1, nil)
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	t.Run("many subs one topic", func(t *testing.T) {
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "moo/foo", qos: 1}}, []byte{1})
		testMQTTCheckPubMsg(t, c, r, "moo/foo", mqttPubFlagRetain|mqttPubQos1, []byte("foo2"))

		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "moo/baz", qos: 1}}, []byte{1})
		testMQTTCheckPubMsg(t, c, r, "moo/baz", mqttPubFlagRetain, []byte("baz1"))

		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "moo/bar", qos: 1}}, []byte{1})
		testMQTTExpectNothing(t, r)
	})

	runSingleSubscribe := func(filters []*mqttFilter) func(t *testing.T) {
		return func(t *testing.T) {
			testMQTTSub(t, 1, c, r, filters, nil)

			for i := 0; i < 2; i++ {
				flags, pi, topic, payload := testMQTTReadPubPacket(t, r)
				if (flags & mqttPubFlagRetain) == 0 {
					t.Fatalf("Expected flags to have retain set, got %v", flags)
				}
				if (flags & mqttPubFlagDup) != 0 {
					t.Fatalf("Expected flags to not have Dup set, got %v", flags)
				}
				var expQOS byte
				var expPayload []byte
				switch topic {
				case "moo/foo":
					expQOS = mqttPubQos1
					expPayload = []byte("foo2")
					testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
				case "moo/baz":
					expQOS = 0
					expPayload = []byte("baz1")
				default:
					t.Fatalf("Unexpected topic: %v", topic)
				}
				if (flags & mqttPubFlagQoS) != expQOS {
					t.Fatalf("Expected flags to have QOS %x set, got %x", expQOS, flags)
				}
				if string(payload) != string(expPayload) {
					t.Fatalf("Expected payload to be %q, got %v", expPayload, string(payload))
				}
			}
			testMQTTExpectNothing(t, r)
		}
	}

	t.Run("one sub many topics", runSingleSubscribe([]*mqttFilter{
		{filter: "moo/foo", qos: 1},
		{filter: "moo/baz", qos: 1},
		{filter: "moo/bar", qos: 1},
	}))

	t.Run("one sub wildcard plus", runSingleSubscribe([]*mqttFilter{
		{filter: "moo/+", qos: 1},
	}))

	t.Run("one sub wildcard hash", runSingleSubscribe([]*mqttFilter{
		{filter: "moo/#", qos: 1},
	}))

	testMQTTDisconnect(t, c, nil)
	c.Close()
}

func TestMQTTRetainedMsgCleanup(t *testing.T) {
	mqttRetainedCacheTTL = 250 * time.Millisecond
	defer func() { mqttRetainedCacheTTL = mqttDefaultRetainedCacheTTL }()

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "cache", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Send a retained message.
	testMQTTPublish(t, c, r, 1, false, true, "foo", 1, []byte("msg"))
	testMQTTFlush(t, c, nil, r)

	// Start a subscription.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1|mqttPubFlagRetain, []byte("msg"))

	time.Sleep(2 * mqttRetainedCacheTTL)

	// Make sure not in cache anymore
	asm := testMQTTGetAccountSessionManager(t, s, "cache")
	if v, ok := asm.rmsCache.Load("foo"); ok {
		t.Fatalf("Should not be in cache, got %+v", v)
	}
}

func TestMQTTRestoreRetainedMsgs(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "retain", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Send several retained messages on different topic
	testMQTTPublish(t, c, r, 1, false, true, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, c, r, 1, false, true, "bar", 1, []byte("msg2"))
	testMQTTPublish(t, c, r, 1, false, true, "baz", 1, []byte("msg3"))

	// Remove the two last ones (by sending empty body)
	testMQTTPublish(t, c, r, 1, false, true, "bar", 1, []byte(""))
	testMQTTPublish(t, c, r, 1, false, true, "baz", 1, []byte(""))
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Now restart the server. We had a bug where we would wait to restore retained
	// messages based on stream last sequence, which was wrong.
	s.Shutdown()
	s = testMQTTRestartServerSameStore(t, s, o)
	l := &captureWarnLogger{warn: make(chan string, 10)}
	s.SetLogger(l, false, false)
	require_NoError(t, s.Start())
	defer testMQTTShutdownServer(s)

	time.Sleep(500 * time.Millisecond)
	start := time.Now()
	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	dur := time.Since(start)
	if dur > 2*time.Second {
		var warnMsg string
		select {
		case warnMsg = <-l.warn:
		default:
		}
		t.Fatalf("Likely timing out restoring retained messages (%s)", warnMsg)
	}
}

func TestMQTTConnAckFirstPacket(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.NoLog, o.Debug, o.Trace = true, false, false
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTDisconnect(t, c, nil)
	c.Close()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{}, 1)
	ready := make(chan struct{})
	go func() {
		defer wg.Done()

		close(ready)
		for {
			nc.Publish("foo", []byte("msg"))
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	<-ready
	for i := 0; i < 100; i++ {
		c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketDisconnect)
		w.WriteByte(0)
		c.Write(w.Bytes())
		// Wait to be disconnected, we can't use testMQTTDisconnect() because
		// it would fail because we may still receive some NATS messages.
		var b [10]byte
		for {
			if _, err := c.Read(b[:]); err != nil {
				break
			}
		}
		c.Close()
	}
	close(ch)
	wg.Wait()
}

func TestMQTTRedeliveryAckWait(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("foo1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 2, []byte("foo2"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		pi1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo1"))
		pi2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo2"))

		if pi1 != 1 || pi2 != 2 {
			t.Fatalf("Unexpected pi values: %v, %v", pi1, pi2)
		}
	}
	// Ack first message
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, 1)
	// Redelivery should only be for second message now
	for i := 0; i < 2; i++ {
		flags := mqttPubQos1 | mqttPubFlagDup
		pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo2"))
		if pi != 2 {
			t.Fatalf("Unexpected pi to be 2, got %v", pi)
		}
	}

	// Restart client, should receive second message with pi==2
	c.Close()
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// Check that message is received with proper pi
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("foo2"))
	if pi != 2 {
		t.Fatalf("Unexpected pi to be 2, got %v", pi)
	}
	// Now ack second message
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, 2)
	// Flush to make sure it is processed before checking client's maps
	testMQTTFlush(t, c, nil, r)

	// Look for the sub client
	mc := testMQTTGetClient(t, s, "sub")
	// The flush orders the READ LOOP, and this acknowledgement arrived while the
	// redelivery's send section was still open, so its removal is parked on that section
	// and is discharged by the delivery callback. Wait for the section, not for the maps.
	mc.mu.Lock()
	sendSess := mc.mqtt.sess
	mc.mu.Unlock()
	testMQTTWaitSendSectionClosed(t, sendSess, 2)
	mc.mu.Lock()
	sess := mc.mqtt.sess
	sess.mu.Lock()
	lpi := len(sess.pendingPublish)
	var lsseq int
	for _, sseqToPi := range sess.cpending {
		lsseq += len(sseqToPi)
	}
	sess.mu.Unlock()
	mc.mu.Unlock()
	if lpi != 0 || lsseq != 0 {
		t.Fatalf("Maps should be empty, got %v, %v", lpi, lsseq)
	}
}

// [MQTT-4.4.0-1] On reconnect with CleanSession=0, the server must re-send
// unacknowledged QoS 1/2 PUBLISH messages (with DUP set). AckWait is kept well
// above the client read deadline (testMQTTTimeout) so a redelivery arriving
// proves it was triggered by the reconnect, not by the AckWait timer.
func TestMQTTReconnectForcesRedeliveryWithDUP(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 3 * testMQTTTimeout
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Publish a QoS 1 message; receive it (DUP=0) but do not ack.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg"))

	// Drop the subscriber's connection without acking.
	c.Close()

	// Reconnect the same (persistent) session.
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// The unacknowledged message must be re-delivered promptly with DUP set,
	// well before the 30s AckWait could fire. Same-server reconnect keeps the
	// session's pending maps, so the original packet identifier is reused.
	rpi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg"))
	if rpi != pi {
		t.Fatalf("Expected redelivery to reuse original packet identifier %v, got %v", pi, rpi)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, rpi)
}

// Re-subscribing a QoS 1/2 filter at QoS 0 deletes its JS consumer; the
// consumer's pending (unacknowledged) deliveries must be purged too, otherwise
// their packet identifiers leak and permanently count against the in-flight cap
// for the life of the session (as mqttProcessUnsubs already does).
// TestMQTTQoS0DowngradeDrainsPendingInsteadOfPurging replaces
// TestMQTTQoS0DowngradePurgesPending, whose assertion encoded the DEFECT rather than the
// intended behavior: it required a QoS 0 re-SUBSCRIBE to purge the pending maps, which
// destroys an exchange the broker has already transmitted.
//
// [MQTT-3.8.4-3] requires the replacement to be accepted without interrupting the flow of
// publications, so the downgrade now converts the consumer into a DRAIN and the exchange
// stays tracked and completable — the same obligation
// TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe below has always honored for
// UNSUBSCRIBE under [MQTT-3.10.4-3]. The contrast between those two tests was the defect
// in miniature.
func TestMQTTQoS0DowngradeDrainsPendingInsteadOfPurging(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Deliver a QoS 1 message and receive it without acking: it is now tracked
	// as pending against the "foo" consumer.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg"))

	countPending := func() (int, int) {
		mc := testMQTTGetClient(t, s, "sub")
		mc.mu.Lock()
		sess := mc.mqtt.sess
		mc.mu.Unlock()
		sess.mu.Lock()
		defer sess.mu.Unlock()
		var np, nc int
		np = len(sess.pendingPublish)
		for _, m := range sess.cpending {
			nc += len(m)
		}
		return np, nc
	}

	if np, nc := countPending(); np != 1 || nc != 1 {
		t.Fatalf("Expected 1 pending publish/cpending before downgrade, got %v/%v", np, nc)
	}

	// Re-subscribe the same filter at QoS 0. The consumer is not destroyed: it becomes a
	// drain that owes this exchange, and the QoS 0 filter is served by the raw
	// subscription instead.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)

	if np, nc := countPending(); np != 1 || nc != 1 {
		t.Fatalf("the downgrade must KEEP the started exchange, got pendingPublish=%v cpending=%v: "+
			"purging it destroys a PUBLISH the broker has already sent", np, nc)
	}
	// And it is the drain's now, not a live consumer's (I7).
	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()
	sess.mu.Lock()
	_, stillCons := sess.cons["foo"]
	drains := len(sess.drains)
	sess.mu.Unlock()
	if stillCons {
		t.Fatal("the downgraded consumer must leave sess.cons: the drain owns the sid")
	}
	if drains != 1 {
		t.Fatalf("the downgrade must convert its consumer into a drain, got %d", drains)
	}
}

// - [MQTT-3.10.4-3] If a Server deletes a Subscription It MUST complete the
// delivery of any QoS 1 or QoS 2 messages which it has started to send to the
// Client.
//
// Test flow:
//   - Subscribe to foo, publish 3 QoS2 messages.
//   - After one is PUBCOMP-ed, and one is PUBREC-ed, Unsubscribe.
//   - See that the remaining 2 are fully delivered.
func TestMQTTQoS2InflightMsgsDeliveredAfterUnsubscribe(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	// AckWait is this fixture's SETUP BUDGET: everything before the UNSUBSCRIBE —
	// publishing and reading the three-message batch, completing PI 1, PI 2's
	// PUBREC answered by its PUBREL, and the UNSUBACK — must finish before the
	// first unacknowledged PUBLISH (PI 3) becomes eligible for redelivery, or its
	// legitimate retransmission overtakes PUBREL 2 and the strict packet order
	// below fails for a timing reason. The old 10ms window raced the batch read;
	// 250ms still raced PUBREL 2, which can wait for a concurrent PI 2 send
	// section to drain (F-115). The retries the loop below relies on still come
	// well inside the 10s packet-read deadline.
	o.MQTT.AckWait = 2 * time.Second
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// send 3 messages
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", 441, []byte("data1"))
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", 442, []byte("data2"))
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", 443, []byte("data3"))

	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	subPI1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))
	subPI2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data2"))
	subPI3 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data3"))

	// fully receive first message
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI1)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI1)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, subPI1)

	// Do not PUBCOMP the 2nd message yet.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI2)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI2)

	// Unsubscribe
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}})

	// PI2 and PI3 both continue from their respective states, which is what
	// [MQTT-3.10.4-3] requires: PI2 is past PUBREC so only its PUBREL replays,
	// while PI3 was transmitted but never acknowledged, so its PUBLISH is
	// redelivered and the whole exchange completes. The consumer survives the
	// UNSUBSCRIBE as a DRAIN precisely so this can happen; before that it was torn
	// down and PI3 was silently lost. See internal/mqtt/unsub_drain.go.
	gotPI2PubRel := false
	gotPI3Publish := false
	gotPI3PubRel := false
	for !gotPI2PubRel || !gotPI3Publish || !gotPI3PubRel {
		b, pl := testMQTTReadPacket(t, r)
		switch b & mqttPacketMask {
		case mqttPacketPubRel:
			pi, err := r.readUint16("packet identifier")
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}
			switch pi {
			case subPI2:
				testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
				gotPI2PubRel = true
			case subPI3:
				if !gotPI3Publish {
					t.Fatalf("got the PUBREL for PI %v before its PUBLISH was redelivered", pi)
				}
				testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
				gotPI3PubRel = true
			default:
				t.Fatalf("Expected PI %v or %v got: %v", subPI2, subPI3, pi)
			}

		case mqttPacketPub:
			_, pi, _, _ := testMQTTGetPubMsgExEx(t, c, r, b, pl, "foo", []byte("data3"))
			if pi != subPI3 {
				t.Fatalf("Expected PI %v got: %v", subPI3, pi)
			}
			gotPI3Publish = true
			testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI3)

		default:
			t.Fatalf("Unexpected packet type: %v", b&mqttPacketMask)
		}
	}

	testMQTTExpectNothing(t, r)
}

func TestMQTTQoS2RejectPublishDuplicates(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Publish 3 different with same PI before we get any PUBREC back, then
	// complete the PUBREL/PUBCOMP flow as needed. Only one message (first
	// payload) should be delivered. PUBRECs,
	var pubPI uint16 = 444
	testMQTTSendPublishPacket(t, cp, qos2, false, false, "foo", pubPI, []byte("data1"))
	testMQTTSendPublishPacket(t, cp, qos2, true, false, "foo", pubPI, []byte("data2"))
	testMQTTSendPublishPacket(t, cp, qos2, false, false, "foo", pubPI, []byte("data3"))

	for i := 0; i < 3; i++ {
		// [MQTT-4.3.3-1] The receiver
		//
		// - MUST respond with a PUBREC containing the Packet Identifier from
		// the incoming PUBLISH Packet, having accepted ownership of the
		// Application Message.
		//
		// - Until it has received the corresponding PUBREL packet, the Receiver
		// MUST acknowledge any subsequent PUBLISH packet with the same Packet
		// Identifier by sending a PUBREC. It MUST NOT cause duplicate messages
		// to be delivered to any onward recipients in this case.
		testMQTTReadPIPacket(mqttPacketPubRec, t, rp, pubPI)
	}
	for i := 0; i < 3; i++ {
		testMQTTSendPIPacket(mqttPacketPubRel|0x2, t, cp, pubPI)
	}
	for i := 0; i < 3; i++ {
		// [MQTT-4.3.3-1] MUST respond to a PUBREL packet by sending a PUBCOMP
		// packet containing the same Packet Identifier as the PUBREL.
		testMQTTReadPIPacket(mqttPacketPubComp, t, rp, pubPI)
	}

	// [MQTT-4.3.3-1] After it has sent a PUBCOMP, the receiver MUST treat any
	// subsequent PUBLISH packet that contains that Packet Identifier as being a
	// new publication.
	//
	// Publish another message, identical to the first one. Since the server
	// already sent us a PUBCOMP, it will deliver this message, for a total of 2
	// delivered.
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", pubPI, []byte("data5"))

	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// Verify we got a total of 2 messages.
	subPI1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))
	subPI2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data5"))
	for _, pi := range []uint16{subPI1, subPI2} {
		testMQTTSendPIPacket(mqttPacketPubRec, t, c, pi)
		testMQTTReadPIPacket(mqttPacketPubRel, t, r, pi)
		testMQTTSendPIPacket(mqttPacketPubComp, t, c, pi)
	}
	testMQTTExpectNothing(t, r)
}

func TestMQTTQoS2RetriesPublish(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 100 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Publish a message and close the pub connection.
	var pubPI uint16 = 444
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", pubPI, []byte("data1"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// See that we got the message delivered to the sub, but don't PUBREC it
	// yet.
	subPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))

	// See that the message is redelivered again 2 times, with the DUP on, before we PUBREC it.
	for i := 0; i < 2; i++ {
		expectedFlags := mqttPubQoS2 | mqttPubFlagDup
		pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", expectedFlags, []byte("data1"))
		if pi != subPI {
			t.Fatalf("Expected pi to be %v, got %v", subPI, pi)
		}
	}

	// Finish the exchange and make sure there are no more attempts.
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, subPI)
	testMQTTExpectNothing(t, r)
}

func TestMQTTQoS2RetriesPubRel(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	// AckWait governs how long the server waits before REDELIVERING an unacknowledged
	// outbound packet. This test asserts that a PUBREL is redelivered repeatedly until
	// PUBCOMP and then ceases — the redelivery behaviour, not a teardown-latency budget. A
	// concurrent clean-session disconnect now closes its outbound authority (F-105 row 8),
	// which adds a few JetStream round trips the 50ms window could not absorb, so a stray
	// PUBLISH redelivery raced ahead of the first PUBREL. 250ms keeps the redelivery
	// assertions exactly and stops timing the teardown instead of the retry.
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	var qos2 byte = 2
	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: qos2}}, []byte{qos2})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Publish a message and close the pub connection.
	var pubPI uint16 = 444
	testMQTTPublish(t, cp, rp, qos2, false, false, "foo", pubPI, []byte("data1"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	// See that we got the message delivered to the sub, PUBREC it and expect a
	// PUBREL from the server.
	subPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQoS2, []byte("data1"))
	testMQTTSendPIPacket(mqttPacketPubRec, t, c, subPI)

	// See that we get PUBREL redelivered several times, there's no DUP flag to
	// check.
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)
	testMQTTReadPIPacket(mqttPacketPubRel, t, r, subPI)

	// Finish the exchange and make sure there are no more attempts.
	testMQTTSendPIPacket(mqttPacketPubComp, t, c, subPI)
	testMQTTExpectNothing(t, r)
}

func TestMQTTAckWaitConfigChange(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sendMsg := func(topic, payload string) {
		t.Helper()
		cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
		cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
		defer cp.Close()
		testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

		testMQTTPublish(t, cp, rp, 1, false, false, topic, 1, []byte(payload))
		testMQTTDisconnect(t, cp, nil)
		cp.Close()
	}
	sendMsg("foo", "msg1")

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("msg1"))
	}

	// Restart the server with a different AckWait option value.
	// Verify that MQTT sub restart succeeds. It will keep the
	// original value.
	c.Close()
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.MQTT.AckWait = 10 * time.Millisecond
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	start := time.Now()
	fooPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	if dur := time.Since(start); dur < 200*time.Millisecond {
		t.Fatalf("AckWait seem to have changed for existing subscription: %v", dur)
	}
	// Complete msg1 now that its redelivery interval is measured: left unacknowledged it keeps
	// being redelivered every 250 ms on "foo", and under -race that redelivery lands before the
	// "bar" delivery the next assertions read.
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, fooPI)
	testMQTTFlush(t, c, nil, r)

	// Create new subscription
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	sendMsg("bar", "msg2")
	testMQTTCheckPubMsgNoAck(t, c, r, "bar", mqttPubQos1, []byte("msg2"))
	start = time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "bar", mqttPubQos1|mqttPubFlagDup, []byte("msg2"))
	// The bound only has to tell the new AckWait (10 ms) from the preserved one (250 ms): a
	// redelivery's own round trip is 50-85 ms under -race, so 50 ms measured the race detector,
	// not the value in force.
	if dur := time.Since(start); dur > 150*time.Millisecond {
		t.Fatalf("AckWait new value not used by new sub: %v", dur)
	}
	c.Close()
}

func TestMQTTUnsubscribeWithPendingAcks(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("msg"))
	}

	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo"}})
	testMQTTFlush(t, c, nil, r)

	// DIVERGENCE FROM UPSTREAM, deliberately inverting this assertion: upstream
	// expects the pending ack map to be EMPTY here, i.e. the started QoS 1 PUBLISH
	// was dropped by the UNSUBSCRIBE. [MQTT-3.10.4-3] forbids that — the server
	// must complete the delivery of a QoS 1/2 message it has already started
	// sending — so the consumer survives as a DRAIN instead and the delivery is
	// still owed. See internal/mqtt/unsub_drain.go and docs/ROADMAP.md.
	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()
	sess.mu.Lock()
	pal := len(sess.pendingPublish)
	draining := len(sess.drainByDur)
	sess.mu.Unlock()
	if pal != 1 {
		t.Fatalf("Expected the started QoS 1 delivery to still be pending, got %v", pal)
	}
	if draining != 1 {
		t.Fatalf("Expected the consumer to be draining, got %v drain(s)", draining)
	}

	// It really is still deliverable: it is redelivered (DUP set) and PUBACK
	// completes it.
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)

	// Completing the last claim retires the drain and its durable: the record no
	// longer references it, so nothing is left behind.
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if n := len(sess.drainByDur); n != 0 {
			return fmt.Errorf("drain still present (%d)", n)
		}
		if n := len(sess.pendingPublish); n != 0 {
			return fmt.Errorf("delivery still pending (%d)", n)
		}
		return nil
	})
}

func TestMQTTMaxAckPending(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 1
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	// Check that we don't receive the second one due to max ack pending
	testMQTTExpectNothing(t, r)

	// Now ack first message
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	// Now we should receive message 2
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg2"))
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Give a chance to the server to "close" the consumer
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for ci := range js.ConsumersInfo("$MQTT_msgs") {
			if ci.PushBound {
				return fmt.Errorf("Consumer still connected")
			}
		}
		return nil
	})

	// Send 2 messages while sub is offline
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg3"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg4"))

	// Restart consumer
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Should receive only message 3
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg3"))
	testMQTTExpectNothing(t, r)

	// Ack and get the next
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg4"))

	// Make sure this message gets ack'ed
	mcli := testMQTTGetClient(t, s, cisub.clientID)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		mcli.mu.Lock()
		sess := mcli.mqtt.sess
		sess.mu.Lock()
		np := len(sess.pendingPublish)
		sess.mu.Unlock()
		mcli.mu.Unlock()
		if np != 0 {
			return fmt.Errorf("Still %v pending messages", np)
		}
		return nil
	})

	// Check that change to config does not prevent restart of sub.
	cp.Close()
	c.Close()
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.MQTT.MaxAckPending = 2
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	cp, rp = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg5"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg6"))

	// Restart consumer
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Should receive only message 5
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg5"))
	testMQTTExpectNothing(t, r)

	// Ack and get the next
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg6"))
}

func TestMQTTMaxAckPendingForMultipleSubs(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	o.MQTT.MaxAckPending = 1
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))

	// Now send a second message but on topic bar
	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg2"))

	// JS allows us to limit per consumer, but we apply the limit to the
	// session, so although JS will attempt to delivery this message,
	// the MQTT code will suppress it.
	testMQTTExpectNothing(t, r)

	// Ack the first message.
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)

	// Now we should get the second message. It was WITHHELD at the cap and never put on
	// the wire (testMQTTExpectNothing above), so its arrival is a FIRST transmission and
	// carries DUP=0 — even though JetStream's delivery count is above one. Deriving DUP
	// from that count was the defect F-072 removes (N-1.1); the marker records that this
	// identifier's PUBLISH was never sent.
	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1, []byte("msg2"))
}

func TestMQTTMaxAckPendingOverLimit(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 20000
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	checkTMax := func(sess *mqttSession, expected int) {
		t.Helper()
		sess.mu.Lock()
		tmax := sess.tmaxack
		sess.mu.Unlock()
		if tmax != expected {
			t.Fatalf("Expected current tmax to be %v, got %v", expected, tmax)
		}
	}

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()

	// After this one, total would be 20000
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	checkTMax(sess, 20000)
	// This one will count for 2, so total will be 60000
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar/#", qos: 1}}, []byte{1})
	checkTMax(sess, 60000)

	// This should fail
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{mqttSubAckFailure})
	checkTMax(sess, 60000)

	// Remove the one with wildcard
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar/#"}})
	checkTMax(sess, 20000)

	// Now we could add 2 more without wildcards
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	checkTMax(sess, 40000)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "baz", qos: 1}}, []byte{1})
	checkTMax(sess, 60000)

	// Again, this one should fail
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bat", qos: 1}}, []byte{mqttSubAckFailure})
	checkTMax(sess, 60000)

	// Now remove all and check that we are at 0
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo"}})
	checkTMax(sess, 40000)
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar"}})
	checkTMax(sess, 20000)
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "baz"}})
	checkTMax(sess, 0)
}

func TestMQTTStreamInfoReturnsNonEmptySubject(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	// Check that we can query all MQTT streams. MQTT streams are
	// created without subject filter, however, if we return them like this,
	// the 'nats' utility will fail to display them due to some xml validation.
	for _, sname := range []string{
		mqttStreamName,
		mqttRetainedMsgsStreamName,
	} {
		t.Run(sname, func(t *testing.T) {
			resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, sname), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var bResp JSApiStreamInfoResponse
			if err = json.Unmarshal(resp.Data, &bResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if len(bResp.Config.Subjects) == 0 {
				t.Fatalf("No subject returned, which will cause nats tooling to fail: %+v", bResp.Config)
			}
		})
	}
}

type chunkWriteConn struct {
	net.Conn
}

func (cwc *chunkWriteConn) Write(p []byte) (int, error) {
	max := len(p)
	cs := rand.Intn(max) + 1
	if cs < max {
		if pn, perr := cwc.Conn.Write(p[:cs]); perr != nil {
			return pn, perr
		}
		time.Sleep(10 * time.Millisecond)
		if pn, perr := cwc.Conn.Write(p[cs:]); perr != nil {
			return pn, perr
		}
		return len(p), nil
	}
	return cwc.Conn.Write(p)
}

func TestMQTTClientIDInLogStatements(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	l := &captureDebugLogger{dbgCh: make(chan string, 10)}
	s.SetLogger(l, true, false)

	cisub := &mqttConnInfo{clientID: "my_client_id", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	tm := time.NewTimer(2 * time.Second)
	var connected bool
	var disconnected bool
	for {
		select {
		case dl := <-l.dbgCh:
			if strings.Contains(dl, "my_client_id") {
				if strings.Contains(dl, "Client connected") {
					connected = true
				} else if strings.Contains(dl, "Client connection closed") {
					disconnected = true
				}
				if connected && disconnected {
					// OK!
					return
				}
			}
		case <-tm.C:
			t.Fatal("Did not get the debug statements or client_id in them")
		}
	}
}

// This was ill-advised since the messages stream is currently interest policy.
// Interest policy streams require consumers match the replica count.
// Will leave her for now to make sure we do not override.

// Test for auto-cleanup of consumers.

// Issue https://github.com/nats-io/nats-server/issues/3924
// The MQTT Server MUST NOT match Topic Filters starting with a wildcard character (# or +),
// with Topic Names beginning with a $ character [MQTT-4.7.2-1]

func TestMQTTTopicWithDot(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("mqtt", "pwd"))
	defer nc.Close()

	sub := natsSubSync(t, nc, "*.*")

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "spBv1.0/plant1", qos: 0}}, []byte{0})
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "spBv1.0/plant2", qos: 1}}, []byte{1})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c2, r2, 0, false, false, "spBv1.0/plant1", 0, []byte("msg1"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant1", 0, []byte("msg1"))
	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant1")

	testMQTTPublish(t, c2, r2, 1, false, false, "spBv1.0/plant2", 1, []byte("msg2"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant2", mqttPubQos1, []byte("msg2"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant2")

	natsPub(t, nc, "spBv1//0.plant1", []byte("msg3"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant1", 0, []byte("msg3"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant1")

	natsPub(t, nc, "spBv1//0.plant2", []byte("msg4"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant2", 0, []byte("msg4"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant2")
}

// Issue https://github.com/nats-io/nats-server/issues/4291

func TestMQTTSparkbDeathHandling(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// protoMetrics is protobuf-encoded test data, with no timestamp.
	//
	//	p := &sproto.Payload{
	// 		Metrics: []*sproto.Payload_Metric{
	// 			newSparkbMetric_Uint64(sparkbBDSeqMetric, uint64(sparkbBDSeq)),
	// 			newSparkbMetric(sparkbDeviceMetric1, "value-at-death-1"),
	// 			newSparkbMetric(sparkbDeviceMetric2, "value-at-death-2"),
	// 		},
	// 		Timestamp: nil,
	// 	}
	protoMetrics := []byte{0x12, 0x0b, 0x0a, 0x05, 0x62, 0x64, 0x53, 0x65, 0x71, 0x20, 0x04, 0x58, 0x02, 0x12, 0x24, 0x0a, 0x0e, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x2d, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x31, 0x20, 0x0c, 0x7a, 0x10, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2d, 0x61, 0x74, 0x2d, 0x64, 0x65, 0x61, 0x74, 0x68, 0x2d, 0x31, 0x12, 0x24, 0x0a, 0x0e, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x2d, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x32, 0x20, 0x0c, 0x7a, 0x10, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2d, 0x61, 0x74, 0x2d, 0x64, 0x65, 0x61, 0x74, 0x68, 0x2d, 0x32}

	proto0Timestamp := []byte{0x08, 0x00}

	protoDeadbeefTimestamp := []byte{0x08, 0xef, 0xfd, 0xb6, 0xf5, 0xfd, 0xde, 0xef, 0xd6, 0xde, 0x01}

	mcPub, rPub := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcPub.Close()
	testMQTTCheckConnAck(t, rPub, mqttConnAckRCConnectionAccepted, false)

	mcSub, rSub := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcSub.Close()
	testMQTTCheckConnAck(t, rSub, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mcSub, rSub, []*mqttFilter{{filter: "spBv1.0/ggg/#", qos: 0}}, []byte{0})

	for _, test := range []*struct {
		name                  string
		payload               []byte
		expectTimestampOffset int
		expectUnchanged       bool
	}{
		{"append", protoMetrics, len(protoMetrics), false},
		{"replace at the end", append(protoMetrics, proto0Timestamp...), len(protoMetrics), false},
		{"replace at the start", append(protoDeadbeefTimestamp, protoMetrics...), 0, false},
		{"invalid", []byte{0xde, 0xad, 0xbe, 0xef}, 0, true}, // invalid payload
		{"invalid fixed32", []byte{0x0d, 0x01}, 0, true},
		{"invalid fixed64", []byte{0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, 0, true},
	} {
		var sendPI uint16
		for _, topic := range []string{"NDEATH/nnn", "DDEATH/nnn/ddd"} {
			sendPI++
			t.Run(test.name+" "+topic, func(t *testing.T) {
				testMQTTPublish(t, mcPub, rPub, 1, false, false, "spBv1.0/ggg/"+topic, sendPI, test.payload)

				flags, pi, _, received := testMQTTGetPubMsgEx(t, mcSub, rSub, "", nil)
				if mqttGetQoS(flags) == 1 {
					testMQTTSendPIPacket(mqttPacketPubAck, t, mcSub, pi)
				}

				if test.expectUnchanged {
					if !bytes.Equal(test.payload, received) {
						t.Fatalf("Expected payload to be unchanged, got %q", received)
					}
					return
				}

				if test.expectTimestampOffset > len(received) {
					t.Fatalf("Expected timestamp offset %v to be less than %v", test.expectTimestampOffset, len(received))
				}
				if test.expectTimestampOffset > 0 {
					if !bytes.Equal(protoMetrics, received[:test.expectTimestampOffset]) {
						t.Fatalf("Expected payload to be unchanged up to %v, got %q", test.expectTimestampOffset, received)
					}
				}

				tsBytes := received[test.expectTimestampOffset:]
				if tsBytes[0] != 0x08 {
					t.Fatalf("Expected timestamp to start with 0x08, got %x", tsBytes)
				}
				v, _, err := protoScanVarint(tsBytes[1:])
				if err != nil {
					t.Fatalf("Error scanning varint: %v", err)
				}

				ts := time.UnixMilli(int64(v))
				if time.Now().Before(ts) {
					t.Fatalf("Expected timestamp to be in the past, got %v", ts)
				}
				if time.Since(ts) > time.Second {
					t.Fatalf("Expected timestamp to be within a second, got %v", time.Since(ts))
				}
			})
		}
	}
}

func TestMQTTSparkbBirthHandling(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Publish an NBIRTH message. Make sure it is received as both the original
	// subject, and as a $sparkplug/certificates one.

	const NBORN = "NODE BORN FAKE MESSAGE"
	const DBORN = "DEVICE BORN FAKE MESSAGE"

	tests := []*struct {
		topic   string
		payload string
		flags   byte
		mc      net.Conn
		r       *mqttReader
	}{
		{
			topic:   "spBv1.0/ggg/NBIRTH/nnn",
			payload: NBORN,
			flags:   0,
		},
		{
			topic:   "spBv1.0/ggg/DBIRTH/nnn/ddd",
			payload: DBORN,
			flags:   0,
		},
		{
			topic:   "$sparkplug/certificates/spBv1.0/ggg/NBIRTH/nnn",
			payload: NBORN,
			flags:   mqttPubFlagRetain,
		},
		{
			topic:   "$sparkplug/certificates/spBv1.0/ggg/DBIRTH/nnn/ddd",
			payload: DBORN,
			flags:   mqttPubFlagRetain,
		},
	}

	t.Run("$sparkplug messages delivered", func(t *testing.T) {
		// Connect the subscribers, unique for each topic we monitor.
		for i, test := range tests {
			test.mc, test.r = testMQTTConnect(t, &mqttConnInfo{cleanSess: true, clientID: fmt.Sprintf("sub-%v", i)}, o.MQTT.Host, o.MQTT.Port)
			defer test.mc.Close()
			testMQTTCheckConnAck(t, test.r, mqttConnAckRCConnectionAccepted, false)

			// Subscribne at QoS2 to make sure the messages are posted at QoS0 and
			// not truncated to sub QoS.
			testMQTTSub(t, 1, test.mc, test.r, []*mqttFilter{{filter: test.topic, qos: 2}}, []byte{2})
		}

		// connect the publisher
		mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, clientID: "pub"}, o.MQTT.Host, o.MQTT.Port)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

		// {Publish} [ND]BIRTH messages. Note that IRL these are protobuf payloads.
		testMQTTPublish(t, mc, r, 0, false, false, "spBv1.0/ggg/NBIRTH/nnn", 0, []byte("NODE BORN FAKE MESSAGE"))
		testMQTTPublish(t, mc, r, 0, false, false, "spBv1.0/ggg/DBIRTH/nnn/ddd", 0, []byte("DEVICE BORN FAKE MESSAGE"))

		// Check that the subscribers got the messages.
		for _, test := range tests {
			testMQTTCheckPubMsg(t, test.mc, test.r, test.topic, test.flags, []byte(test.payload))
		}
	})

	t.Run("$sparkplug messages retained", func(t *testing.T) {
		// Connect/subscribe again, and make sure that only retained messages are there.
		for i, test := range tests {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, clientID: fmt.Sprintf("sub-%v", i+100)}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: test.topic, qos: 2}}, []byte{2})
			if test.flags&mqttPubFlagRetain != 0 {
				testMQTTCheckPubMsg(t, mc, r, test.topic, test.flags, []byte(test.payload))
			} else {
				testMQTTExpectNothing(t, r)
			}
		}
	})
}

func TestMQTTSliceHeadersAndDecodeRetainedMessage(t *testing.T) {
	t.Parallel()
	// First check low level mqttSliceHeaders
	for _, test := range []struct {
		name     string
		hdr      string
		expected []string
	}{
		// Valid cases
		{"one key and some random", hdrLine + "key1:val1\r\nsomeotherkey:someval\r\n", []string{"val1", _EMPTY_, _EMPTY_}},
		{"two keys", hdrLine + "key2:val2\r\nthisisnotkey1:someval\r\nkey1:val1\r\n", []string{"val1", "val2", _EMPTY_}},
		{"three keys", hdrLine + "key2:val2\r\nkey3:val3\r\nkey1:val1\r\n", []string{"val1", "val2", "val3"}},
		{"space before value", hdrLine + "somekey:someval\r\nkey2:  val2withspacebefore\r\n", []string{_EMPTY_, "val2withspacebefore", _EMPTY_}},
		{"space between key and colon sign", hdrLine + "key1:val1\r\nkey2 :val2\r\nkey3  :  val3\r\n", []string{"val1", "val2", "val3"}},
		// A key whose LENGTH equals the scan's current absolute position must not
		// end the scan: the delimiter search returns an offset into the remaining
		// slice, and confusing the two silently dropped this header and every one
		// after it. The first header begins at len(hdrLine) == 10, so a
		// ten-character key is the case that fires there — "Nmqtt-RExp" is exactly
		// ten, which is how this reached retained messages carrying an MQTT 5.0
		// expiry. The second line repeats the trap at the next position.
		{"key length equals the scan position", hdrLine + "0123456789:valA\r\nkey1:val1\r\nkey2:val2\r\nkey3:val3\r\n", []string{"val1", "val2", "val3"}},
		// Error cases
		{"no hdr line", "key1:val1\r\n", []string{_EMPTY_, _EMPTY_, _EMPTY_}},
		{"key length 0", hdrLine + "key1:val1\r\n:val2\r\nkey3:val3\r\n", []string{"val1", _EMPTY_, _EMPTY_}},
		{"key is only spaces", hdrLine + "key1:val1\r\nkey2:val2\r\n     :val3\r\n", []string{"val1", "val2", _EMPTY_}},
		{"value no crlf", hdrLine + "key1:val1\r\nkey2:val2", []string{"val1", _EMPTY_, _EMPTY_}},
	} {
		t.Run(test.name, func(t *testing.T) {
			headers := map[string][]byte{
				"key1": nil,
				"key2": nil,
				"key3": nil,
			}
			mqttSliceHeaders(headers, []byte(test.hdr))
			for i := range len(headers) {
				key := fmt.Sprintf("key%d", i+1)
				val := string(headers[key])
				if ev := test.expected[i]; ev != val {
					t.Fatalf("For key %q, expected value to be %q, got %q", key, ev, val)
				}
			}
		})
	}

	// Now test mqttDecodeRetainedMessage() itself.
	t.Run("flag not a number", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:bad\r\n\r\n", hdrLine)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("flag too big", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:15\r\n\r\n", hdrLine)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("flag invalid qos", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:7\r\n\r\n", hdrLine)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	// The development-era delete marker ('-' prefixed to the hex flags) is gone
	// from both the encoder and the decoder. A record still carrying it must be
	// rejected, never silently accepted as flags=1.
	t.Run("flag with legacy delete marker rejected", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RFlags:-1\r\n\r\n", hdrLine)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, nil)
		require_Error(t, err, errMQTTInvalidRetainFlags)
	})
	t.Run("decode retained msg with space before header value", func(t *testing.T) {
		msg, hdrLen := mqttEncodeRetainedMessage(&mqttRetainedMsg{
			Topic:  "foo/x",
			Origin: "  Origin", // Add spaces in front on purpose
			Source: "Source",
			Flags:  1,
			Msg:    []byte("msg1"),
		})
		hdr := msg[:hdrLen]
		msg = msg[hdrLen:]
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.foo.x", hdr, msg)
		require_NoError(t, err)
		require_Equal(t, rm.Topic, "foo/x")
		require_Equal(t, rm.Subject, "foo.x")
		require_Equal(t, rm.Origin, "Origin")
		require_Equal(t, rm.Source, "Source")
		require_Equal(t, rm.Flags, 1)
		require_Equal(t, string(rm.Msg), "msg1")
	})
	t.Run("decode retained msg with subject transformed", func(t *testing.T) {
		msg, hdrLen := mqttEncodeRetainedMessage(&mqttRetainedMsg{
			Topic:  "foo/x",
			Origin: "Origin",
			Source: "Source",
			Flags:  1,
			Msg:    []byte("msg2"),
		})
		hdr := msg[:hdrLen]
		msg = msg[hdrLen:]
		// Use different subject when calling the function. Make sure the
		// topic is properly reflecting the subject.
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, msg)
		require_NoError(t, err)
		require_Equal(t, rm.Topic, "bar/x")
		require_Equal(t, rm.Subject, "bar.x")
		require_Equal(t, rm.Origin, "Origin")
		require_Equal(t, rm.Source, "Source")
		require_Equal(t, rm.Flags, 1)
		require_Equal(t, string(rm.Msg), "msg2")
	})
	t.Run("decode deleted retained message", func(t *testing.T) {
		msg, hdrLen := mqttEncodeRetainedMessage(&mqttRetainedMsg{
			Topic:  "foo/x",
			Origin: "Origin",
			Source: "Source",
			Flags:  1,
			Msg:    nil,
		})
		hdr := msg[:hdrLen]
		msg = msg[hdrLen:]
		// Use different subject too
		rm, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, msg)
		require_NoError(t, err)
		require_Equal(t, rm.Topic, "bar/x")
		require_Equal(t, rm.Subject, "bar.x")
		require_Equal(t, rm.Origin, "Origin")
		require_Equal(t, rm.Source, "Source")
		require_Equal(t, rm.Flags, 1)
		require_Len(t, len(rm.Msg), 0)
	})
	// The Nmqtt-RFlags header is what identifies a record as one this broker
	// wrote. Without it the stored bytes are rejected as an unsupported format —
	// never interpreted as some other format, which is what the development-era
	// JSON fallback used to do.
	t.Run("no header at all is unsupported", func(t *testing.T) {
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.foo.x", nil, []byte("msg"))
		require_Error(t, err, errMQTTRetainedMsgUnsupportedFormat)
	})
	t.Run("JSON body without header is not decoded as JSON", func(t *testing.T) {
		msg, err := json.Marshal(map[string]any{
			"topic":  "foo/x",
			"origin": "Origin",
			"source": "Source",
			"flags":  1,
			"msg":    []byte("hello"),
		})
		require_NoError(t, err)
		_, err = mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", nil, msg)
		require_Error(t, err, errMQTTRetainedMsgUnsupportedFormat)
	})
	t.Run("other retained headers without flags is unsupported", func(t *testing.T) {
		hdr := fmt.Appendf(nil, "%sNmqtt-RTopic:foo/x\r\nNmqtt-ROrigin:Origin\r\n\r\n", hdrLine)
		_, err := mqttDecodeRetainedMessage("$MQTT.rmsgs.bar.x", hdr, []byte("msg"))
		require_Error(t, err, errMQTTRetainedMsgUnsupportedFormat)
	})
}

func TestMQTTRetainedMsgRemovedFromMapIfNotInStream(t *testing.T) {
	mqttRetainedCacheTTL = 250 * time.Millisecond
	defer func() { mqttRetainedCacheTTL = mqttDefaultRetainedCacheTTL }()

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, c, r, 0, false, true, "foo", 0, []byte("msg1"))
	testMQTTFlush(t, c, nil, r)

	checkRetained := func(expected string) {
		t.Helper()
		c, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
		if expected == _EMPTY_ {
			testMQTTExpectNothing(t, r)
		} else {
			testMQTTCheckPubMsg(t, c, r, "foo", mqttPubFlagRetain, []byte(expected))
		}
	}
	checkRetained("msg1")

	testMQTTPublish(t, c, r, 0, false, true, "foo", 0, []byte("msg2"))
	testMQTTFlush(t, c, nil, r)

	checkRetained("msg2")

	// Now we will get the current sequence for the retained message and
	// remove it from the stream. We expect to get a warning that indicates
	// that the load failed. Restarting the subscription should not longer
	// cause this warning and the retained message should have been removed
	// from the map/cache.
	l := &captureWarnLogger{warn: make(chan string, 10)}
	s.SetLogger(l, false, false)

	asm := testMQTTGetAccountSessionManager(t, s, "pub")
	// Make sure it is in the cache
	rm := asm.getCachedRetainedMsg("foo")
	require_NotNil(t, rm)
	// Get the retained message sequence from the tree.
	asm.mu.RLock()
	rf, ok := asm.retmsgs.Find([]byte("foo"))
	asm.mu.RUnlock()
	require_True(t, ok)
	nc, js := jsClientConnect(t, s)
	defer nc.Close()
	err := js.DeleteMsg(mqttRetainedMsgsStreamName, rf.sseq)
	require_NoError(t, err)

	// Wait for more than the cache TTL
	time.Sleep(2 * mqttRetainedCacheTTL)

	checkRetained(_EMPTY_)

	// A missing stored message is an expected outcome (per-message TTL reaps
	// expired retained messages routinely), so no warning is emitted; the
	// stale index entry is silently removed instead.
	checkRetained(_EMPTY_)

	select {
	case w := <-l.warn:
		if strings.Contains(w, ApiErrors[JSNoMessageFoundErr].Description) {
			t.Fatalf("Got the warning: %q", w)
		}
	case <-time.After(250 * time.Millisecond):
		// OK
	}

	// Finally, check that the retmsgs map is empty.
	asm.mu.RLock()
	ok = asm.retmsgs.Size() == 0
	asm.mu.RUnlock()
	require_True(t, ok)
}

// TestMQTTSessionPersistSpoofPrevented verifies that an MQTT client cannot publish
// into the broker's reserved internal namespace to forge session state and evict
// other sessions.
//
// The target moved with the mechanism: ownership is observed by WATCHING the session
// stream now, so the subject that matters is $MQTT.sess.<hash> — where an unguarded
// publish would land a durable record rather than a transient notification.
func TestMQTTSessionPersistSpoofPrevented(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "victim",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.>"}},
				Subscribe: &SubjectPermission{Allow: []string{"foo.>"}},
			},
		},
		{
			Username: "attacker",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.>"}},
				Subscribe: &SubjectPermission{Allow: []string{"foo.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Connect the victim MQTT client with a persistent session.
	vc, vr := testMQTTConnect(t, &mqttConnInfo{
		clientID:  "victim",
		user:      "victim",
		pass:      "pass",
		cleanSess: true,
	}, o.MQTT.Host, o.MQTT.Port)
	defer vc.Close()
	testMQTTCheckConnAck(t, vr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, vc, vr, []*mqttFilter{{filter: "foo/+", qos: 1}}, []byte{1})
	testMQTTFlush(t, vc, nil, vr)

	// Connect the attacker MQTT client.
	ac, ar := testMQTTConnect(t, &mqttConnInfo{
		clientID:  "attacker",
		user:      "attacker",
		pass:      "pass",
		cleanSess: true,
	}, o.MQTT.Host, o.MQTT.Port)
	defer ac.Close()
	testMQTTCheckConnAck(t, ar, mqttConnAckRCConnectionAccepted, false)

	// Craft a forged SESSION RECORD targeting the victim's session. MQTT topic "/"
	// maps to NATS ".", so this becomes $MQTT.sess.<victimHash> — a subject the
	// session stream itself owns.
	//
	// This is the subject the guard has to cover now that ownership is observed by
	// watching $MQTT_sess rather than by listening for JetStream replies. It is also
	// a strictly worse target than the old one: an unguarded publish here does not
	// merely forge a notification, it lands a RECORD, corrupting the victim's durable
	// session state and waking every broker's watch.
	victimHash := getHash("victim")
	spoofTopic := fmt.Sprintf("$MQTT/sess/%s", victimHash)
	forgedPayload := []byte(`{"id":"victim","origin":"fakenode","clean":false}`)

	// Attempt the spoofed publish. The broker rejects any client PUBLISH whose
	// mapped subject falls in the reserved internal namespace ("$MQTT.*") in
	// mqttProcessPub, so the forged record never reaches the stream — independent of
	// NATS account permissions.
	testMQTTPublish(t, ac, ar, 0, false, false, spoofTopic, 0, forgedPayload)
	testMQTTFlush(t, ac, nil, ar)

	// Give the server time to process if the message leaked through.
	time.Sleep(250 * time.Millisecond)

	// Verify the victim session was NOT evicted by round-tripping a message.
	testMQTTPublish(t, ac, ar, 0, false, false, "foo/bar", 0, []byte("still-alive"))
	testMQTTCheckPubMsg(t, vc, vr, "foo/bar", 0, []byte("still-alive"))

	// And that nothing was written where the guard is supposed to have stopped it.
	// Surviving the round trip alone would also hold if the record HAD landed and the
	// watch simply had not evicted yet; the stream is where the damage would be.
	asm := testMQTTGetAccountSessionManager(t, s, "victim")
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, victimHash)
	if err != nil {
		t.Fatalf("load the victim's session record: %v", err)
	}
	if smsg == nil {
		t.Fatal("fixture: the victim must have a session record for this to prove anything")
	}
	var ps mqttPersistedSession
	if uerr := json.Unmarshal(smsg.Data, &ps); uerr != nil {
		t.Fatalf("the victim's record must still be the broker's own: %v", uerr)
	}
	if ps.Origin == "fakenode" {
		t.Fatal("the forged record reached the session stream: the reserved-namespace publish guard did not hold")
	}
}

//////////////////////////////////////////////////////////////////////////
//
// Benchmarks
//
//////////////////////////////////////////////////////////////////////////

const (
	mqttPubSubj     = "a"
	mqttBenchBufLen = 32768
)

func mqttBenchPubQoS0(b *testing.B, subject, payload string, numSubs int) {
	b.StopTimer()
	b.ReportAllocs()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(b, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "pub", cleanSess: true}
	c, br := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(b, br, mqttConnAckRCConnectionAccepted, false)
	w := newMQTTWriter(0)
	testMQTTWritePublishPacket(b, w, 0, false, false, subject, 0, []byte(payload))
	sendOp := w.Bytes()

	dch := make(chan error, 1)
	totalSize := int64(len(sendOp))
	cdch := 0

	createSub := func(i int) {
		ci := &mqttConnInfo{clientID: fmt.Sprintf("sub%d", i), cleanSess: true}
		cs, brs := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(b, brs, mqttConnAckRCConnectionAccepted, false)

		testMQTTSub(b, 1, cs, brs, []*mqttFilter{{filter: subject, qos: 0}}, []byte{0})
		testMQTTFlush(b, cs, nil, brs)

		w := newMQTTWriter(0)
		varHeaderAndPayload := 2 + len(subject) + len(payload)
		w.WriteVarInt(varHeaderAndPayload)
		size := 1 + w.Len() + varHeaderAndPayload
		totalSize += int64(size)

		go func() {
			mqttBenchConsumeMsgQoS0(cs, int64(b.N)*int64(size), dch)
			cs.Close()
		}()
	}
	for i := 0; i < numSubs; i++ {
		createSub(i + 1)
		cdch++
	}

	bw := bufio.NewWriterSize(c, mqttBenchBufLen)
	b.SetBytes(totalSize)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	testMQTTFlush(b, c, bw, br)
	for i := 0; i < cdch; i++ {
		if e := <-dch; e != nil {
			b.Fatal(e.Error())
		}
	}
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func mqttBenchConsumeMsgQoS0(c net.Conn, total int64, dch chan<- error) {
	var buf [mqttBenchBufLen]byte
	var err error
	var n int
	for size := int64(0); size < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		size += int64(n)
	}
	dch <- err
}

func mqttBenchPubQoS1(b *testing.B, subject, payload string, numSubs int) {
	b.StopTimer()
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 0xFFFF
	s := testMQTTRunServer(b, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{cleanSess: true}
	c, br := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(b, br, mqttConnAckRCConnectionAccepted, false)

	w := newMQTTWriter(0)
	testMQTTWritePublishPacket(b, w, 1, false, false, subject, 1, []byte(payload))
	// For reported bytes we will count the PUBLISH + PUBACK (4 bytes)
	totalSize := int64(w.Len() + 4)
	w.Reset()

	pi := uint16(1)
	maxpi := uint16(60000)
	ppich := make(chan error, 10)
	dch := make(chan error, 1+numSubs)
	cdch := 1
	// Start go routine to consume PUBACK for published QoS 1 messages.
	go mqttBenchConsumePubAck(c, b.N, dch, ppich)

	createSub := func(i int) {
		ci := &mqttConnInfo{clientID: fmt.Sprintf("sub%d", i), cleanSess: true}
		cs, brs := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(b, brs, mqttConnAckRCConnectionAccepted, false)

		testMQTTSub(b, 1, cs, brs, []*mqttFilter{{filter: subject, qos: 1}}, []byte{1})
		testMQTTFlush(b, cs, nil, brs)

		w := newMQTTWriter(0)
		varHeaderAndPayload := 2 + len(subject) + 2 + len(payload)
		w.WriteVarInt(varHeaderAndPayload)
		size := 1 + w.Len() + varHeaderAndPayload
		// Add to the bytes reported the size of message sent to subscriber + PUBACK (4 bytes)
		totalSize += int64(size + 4)

		go func() {
			mqttBenchConsumeMsgQos1(cs, b.N, size, dch)
			cs.Close()
		}()
	}
	for i := 0; i < numSubs; i++ {
		createSub(i + 1)
		cdch++
	}

	flush := func() {
		b.Helper()
		if _, err := c.Write(w.Bytes()); err != nil {
			b.Fatalf("Error on write: %v", err)
		}
		w.Reset()
	}

	b.SetBytes(totalSize)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if pi <= maxpi {
			testMQTTWritePublishPacket(b, w, 1, false, false, subject, pi, []byte(payload))
			pi++
			if w.Len() >= mqttBenchBufLen {
				flush()
			}
		} else {
			if w.Len() > 0 {
				flush()
			}
			if pi > 60000 {
				pi = 1
				maxpi = 0
			}
			if e := <-ppich; e != nil {
				b.Fatal(e.Error())
			}
			maxpi += 10000
			i--
		}
	}
	if w.Len() > 0 {
		flush()
	}
	for i := 0; i < cdch; i++ {
		if e := <-dch; e != nil {
			b.Fatal(e.Error())
		}
	}
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func mqttBenchConsumeMsgQos1(c net.Conn, total, size int, dch chan<- error) {
	var buf [mqttBenchBufLen]byte
	pubAck := [4]byte{mqttPacketPubAck, 0x2, 0, 0}
	var err error
	var n int
	var pi uint16
	var prev int
	for i := 0; i < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		n += prev
		for ; n >= size; n -= size {
			i++
			pi++
			pubAck[2] = byte(pi >> 8)
			pubAck[3] = byte(pi)
			if _, err = c.Write(pubAck[:4]); err != nil {
				dch <- err
				return
			}
			if pi == 60000 {
				pi = 0
			}
		}
		prev = n
	}
	dch <- err
}

func mqttBenchConsumePubAck(c net.Conn, total int, dch, ppich chan<- error) {
	var buf [mqttBenchBufLen]byte
	var err error
	var n int
	var pi uint16
	var prev int
	for i := 0; i < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		n += prev
		for ; n >= 4; n -= 4 {
			i++
			pi++
			if pi%10000 == 0 {
				ppich <- nil
			}
			if pi == 60001 {
				pi = 0
			}
		}
		prev = n
	}
	ppich <- err
	dch <- err
}

func BenchmarkMQTT_QoS0_Pub_______0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 0)
}

func BenchmarkMQTT_QoS0_Pub_______8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 0)
}

func BenchmarkMQTT_QoS0_Pub______32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 0)
}

func BenchmarkMQTT_QoS0_Pub_____128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 0)
}

func BenchmarkMQTT_QoS0_Pub_____256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 0)
}

func BenchmarkMQTT_QoS0_Pub_______1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 0)
}

func BenchmarkMQTT_QoS0_PubSub1___0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 1)
}

func BenchmarkMQTT_QoS0_PubSub1___8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 1)
}

func BenchmarkMQTT_QoS0_PubSub1__32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 1)
}

func BenchmarkMQTT_QoS0_PubSub1_128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 1)
}

func BenchmarkMQTT_QoS0_PubSub1_256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 1)
}

func BenchmarkMQTT_QoS0_PubSub1___1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 1)
}

func BenchmarkMQTT_QoS0_PubSub2___0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 2)
}

func BenchmarkMQTT_QoS0_PubSub2___8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 2)
}

func BenchmarkMQTT_QoS0_PubSub2__32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 2)
}

func BenchmarkMQTT_QoS0_PubSub2_128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 2)
}

func BenchmarkMQTT_QoS0_PubSub2_256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 2)
}

func BenchmarkMQTT_QoS0_PubSub2___1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 2)
}

func BenchmarkMQTT_QoS1_Pub_______0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 0)
}

func BenchmarkMQTT_QoS1_Pub_______8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 0)
}

func BenchmarkMQTT_QoS1_Pub______32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 0)
}

func BenchmarkMQTT_QoS1_Pub_____128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 0)
}

func BenchmarkMQTT_QoS1_Pub_____256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 0)
}

func BenchmarkMQTT_QoS1_Pub_______1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 0)
}

func BenchmarkMQTT_QoS1_PubSub1___0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 1)
}

func BenchmarkMQTT_QoS1_PubSub1___8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 1)
}

func BenchmarkMQTT_QoS1_PubSub1__32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 1)
}

func BenchmarkMQTT_QoS1_PubSub1_128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 1)
}

func BenchmarkMQTT_QoS1_PubSub1_256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 1)
}

func BenchmarkMQTT_QoS1_PubSub1___1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 1)
}

func BenchmarkMQTT_QoS1_PubSub2___0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 2)
}

func BenchmarkMQTT_QoS1_PubSub2___8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 2)
}

func BenchmarkMQTT_QoS1_PubSub2__32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 2)
}

func BenchmarkMQTT_QoS1_PubSub2_128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 2)
}

func BenchmarkMQTT_QoS1_PubSub2_256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 2)
}

func BenchmarkMQTT_QoS1_PubSub2___1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 2)
}

// ---------------------------------------------------------------------------
// A duplicate plain filter that downgrades and then upgrades in ONE SUBSCRIBE
// packet. docs/ROADMAP.md's "closed-but-present delivery subscription under a
// live cons[sid]"; mechanism in docs/ARCHITECTURE.md under the deferred
// teardown.
//
// Every membership and consumer assertion below uses NATS SUBJECTS, not wire
// filters: the parser converts before processSubs sees the packet, so a
// wildcard's sids are "foo.>" and "foo fwc". An assertion on Cons["foo/#"]
// would fail against correct code.
// ---------------------------------------------------------------------------

// testSubRevokeWrittenRecords collects the RECORD AS WRITTEN for every landed session-record
// write of one session, in order.
//
// The oracle is the marshalled payload, never sess.voidedRegNonces or sess.subs:
// saveWithDetail marshals and RELEASES sess.mu before it stores, so the live maps read from
// the result hook are not what was written. Reading the record back instead is not an
// option — a JetStream read issued from inside the write's own completion blocks the packet
// making it, which this fixture demonstrated before it took this shape.
//
// The two hooks are a pair: the body hook says what these bytes are, the result hook says
// they landed. Both run on broker goroutines, so nothing here calls t.Fatal.
type testSubRevokeWrittenRecords struct {
	mu      sync.Mutex
	sess    *mqttSession
	pending []byte
	records []*mqttPersistedSession
	err     error
	stop    func() bool // when it returns true, later writes are ignored
}

// armWrittenRecords, not "install": TestParallelTestsTouchNoProcessGlobalState resolves
// call chains BY FUNCTION NAME, so a helper sharing a name with one another package file
// already uses makes that file's parallel tests look like they reach this file's global
// seams. The distinct name is what keeps that guard's report about real reachability.
func (w *testSubRevokeWrittenRecords) armWrittenRecords(sess *mqttSession) func() {
	w.sess = sess
	restoreBody := setMQTTTestSessRecordBodyHook(func(ss *mqttSession, body []byte) {
		if ss != w.sess {
			return
		}
		w.mu.Lock()
		w.pending = append([]byte(nil), body...)
		w.mu.Unlock()
	})
	restoreResult := setMQTTTestSessStoreResultHook(func(ss *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if ss != w.sess || err != nil {
			return resp, err
		}
		if w.stop != nil && w.stop() {
			return resp, err
		}
		w.mu.Lock()
		body := w.pending
		w.pending = nil
		if len(body) == 0 {
			w.err = fmt.Errorf("a session write landed with no marshalled body observed")
		} else {
			ps := &mqttPersistedSession{}
			if uerr := json.Unmarshal(body, ps); uerr != nil {
				w.err = uerr
			} else {
				w.records = append(w.records, ps)
			}
		}
		w.mu.Unlock()
		return resp, err
	})
	return func() { restoreResult(); restoreBody() }
}

func (w *testSubRevokeWrittenRecords) landed() ([]*mqttPersistedSession, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.records, w.err
}

// carriesNonce reports whether a written record names one register nonce as cancelled.
func carriesNonce(ps *mqttPersistedSession, nonce string) bool {
	for _, n := range ps.VoidedRegs {
		if n == nonce {
			return true
		}
	}
	return false
}

// testSubRevokeSession returns the live session behind a connected client id.
func testSubRevokeSession(t testing.TB, s *Server, clientID string) *mqttSession {
	t.Helper()
	mc := testMQTTGetClient(t, s, clientID)
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()
	if sess == nil {
		t.Fatalf("no session for %q", clientID)
	}
	return sess
}

// testSubRevokeConsumerLives reports whether a plain durable is still on the messages
// stream. The record can name a durable that no longer exists, which is half of this
// item's defect, so the two are asserted separately and never one from the other.
func testSubRevokeConsumerLives(t testing.TB, sess *mqttSession, durable string) bool {
	t.Helper()
	ci, err := sess.jsa.lookupConsumer(mqttStreamName, durable)
	if err != nil {
		if isErrorOtherThan(err, JSConsumerNotFoundErr) {
			t.Fatalf("lookup consumer %q: %v", durable, err)
		}
		return false
	}
	return ci != nil
}

// TestMQTTSubDuplicateFilterDowngradeThenUpgradeKeepsDelivering is the headline case.
//
// One packet carries "foo" at QoS 0 and then at QoS 1. [MQTT-3.8.4-4] makes that a
// sequence of subscriptions with one SUBACK, and [MQTT-3.8.4-3] makes the second entry
// replace the first, so the required end state is one subscription at QoS 1 with its
// consumer intact and delivering. Before the revocation the client was granted QoS 1 and
// then received nothing for the rest of the connection, while the record named a durable
// the packet went on to delete.
func TestMQTTSubDuplicateFilterDowngradeThenUpgradeKeepsDelivering(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// A TRANSMITTED, unacknowledged QoS 1 delivery. The staging records a void verdict
	// on its register rather than cancelling it, and that verdict — durable, in
	// VoidedRegs — is what the revocation must withdraw: it says the client was never
	// sent this message, and licenses reuse of an identifier the client is holding.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("held"))
	heldPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("held"))

	sess := testSubRevokeSession(t, s, "sub")
	sess.mu.Lock()
	preCfg := sess.cons["foo"]
	var heldNonce string
	var heldTransmitted bool
	if ack := sess.pendingPublish[heldPI]; ack != nil {
		heldNonce, heldTransmitted = ack.regNonce, !ack.regPending
	}
	sess.mu.Unlock()
	if preCfg == nil {
		t.Fatal("the QoS 1 subscribe must leave a consumer in sess.cons")
	}
	if heldNonce == _EMPTY_ || !heldTransmitted {
		t.Fatalf("the held delivery must carry a CONFIRMED register: without one the staging "+
			"cancels rather than voids and the un-void assertion below is vacuous "+
			"(nonce=%q transmitted=%v)", heldNonce, heldTransmitted)
	}

	// One packet, two occurrences of the same filter. The SUBACK reports each occurrence.
	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 0}, {filter: "foo", qos: 1}}, []byte{0, 1})
	testMQTTFlush(t, c, nil, r)

	sess.mu.Lock()
	postCfg := sess.cons["foo"]
	sess.mu.Unlock()
	if postCfg != preCfg {
		t.Fatalf("the pre-packet consumer must survive the packet, got %v", postCfg)
	}
	if !testSubRevokeConsumerLives(t, sess, preCfg.Durable) {
		t.Fatalf("durable %q was deleted: the packet granted QoS 1 and then destroyed the "+
			"consumer that serves it", preCfg.Durable)
	}

	asm := testMQTTGetAccountSessionManager(t, s, "sub")
	ps, _ := testReconcileRecord(t, asm, "sub")
	if ps.Subs["foo"] != 1 {
		t.Fatalf("the record must end at the LAST occurrence's QoS, got %v", ps.Subs["foo"])
	}
	if rc := ps.Cons["foo"]; rc == nil || rc.Durable != preCfg.Durable {
		t.Fatalf("the record must name the surviving durable, got %v", rc)
	}
	for _, n := range ps.VoidedRegs {
		if n == heldNonce {
			t.Fatal("the staged void verdict on a TRANSMITTED delivery must be withdrawn " +
				"durably: left standing it says the client never saw a message it is " +
				"holding, and licenses reuse of that identifier")
		}
	}

	// Delivery, under the last occurrence's terms.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 2, []byte("after"))
	afterPI := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("after"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, afterPI)

	// And the held exchange still completes. Its COMPLETION is the assertion, not an
	// AckWait redelivery: the default AckWait is 30s, and shortening it to force a
	// redelivery here would make the schedule timing-dependent for no extra claim — the
	// message was never lost, so acknowledging it is what proves the exchange survived.
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, heldPI)
	testMQTTFlush(t, c, nil, r)
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if len(sess.pendingPublish) != 0 {
			return fmt.Errorf("still %d pending publishes", len(sess.pendingPublish))
		}
		return nil
	})
}

// TestMQTTSubDuplicateFilterDowngradeThenUpgradeWildcardBothHalves is the same packet with
// a multi-level wildcard, which owns TWO sids and therefore reproduces the defect twice:
// the main "foo.>" and the level-up companion "foo fwc" each have their own staging site
// and their own refresh arm.
func TestMQTTSubDuplicateFilterDowngradeThenUpgradeWildcardBothHalves(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	sess := testSubRevokeSession(t, s, "sub")
	sess.mu.Lock()
	mainCfg, fwcCfg := sess.cons["foo.>"], sess.cons["foo fwc"]
	sess.mu.Unlock()
	if mainCfg == nil || fwcCfg == nil {
		t.Fatalf("a multi-level filter owns two consumers, got main=%v fwc=%v", mainCfg, fwcCfg)
	}

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}, {filter: "foo/#", qos: 1}}, []byte{0, 1})
	testMQTTFlush(t, c, nil, r)

	sess.mu.Lock()
	postMain, postFwc := sess.cons["foo.>"], sess.cons["foo fwc"]
	sess.mu.Unlock()
	if postMain != mainCfg || postFwc != fwcCfg {
		t.Fatalf("both halves' consumers must survive, got main=%v fwc=%v", postMain, postFwc)
	}
	for _, cfg := range []*ConsumerConfig{mainCfg, fwcCfg} {
		if !testSubRevokeConsumerLives(t, sess, cfg.Durable) {
			t.Fatalf("durable %q was deleted", cfg.Durable)
		}
	}

	asm := testMQTTGetAccountSessionManager(t, s, "sub")
	ps, _ := testReconcileRecord(t, asm, "sub")
	if ps.Subs["foo.>"] != 1 {
		t.Fatalf("the record must end at QoS 1, got %v", ps.Subs["foo.>"])
	}
	if rc := ps.Cons["foo.>"]; rc == nil || rc.Durable != mainCfg.Durable {
		t.Fatalf("the record must name the main durable, got %v", rc)
	}
	if rc := ps.Cons["foo fwc"]; rc == nil || rc.Durable != fwcCfg.Durable {
		t.Fatalf("the record must name the companion durable, got %v", rc)
	}

	// Both routes deliver at QoS 1: "foo/bar" through the main sid, "foo" through the
	// level-up companion.
	testMQTTPublish(t, cp, rp, 1, false, false, "foo/bar", 1, []byte("deep"))
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo/bar", mqttPubQos1, []byte("deep"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 2, []byte("top"))
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("top"))
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)
}

// TestMQTTSubDuplicateFilterUpgradeThenDowngradeStillDeletes is the regression guard in the
// other direction: [{foo,1},{foo,0}] ends at QoS 0 and the durable must go.
//
// It also asserts Cons["foo"] is ABSENT from the record. This assertion used to demand the
// opposite, as pinned evidence of a filed defect: the record named a consumer the same
// packet then deleted. The fix is not in the record BUILDER — a cons entry at a QoS 0
// membership can be a live adopted durable, and omitting those lets a peer's orphan sweep
// delete a started exchange — but at the packet's final-update boundary, which is where a
// DESTROYED downgrade now gives its entry up. See releaseDowngradedConsumersLocked.
func TestMQTTSubDuplicateFilterUpgradeThenDowngradeStillDeletes(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sess := testSubRevokeSession(t, s, "sub")
	sess.mu.Lock()
	preCfg := sess.cons["foo"]
	sess.mu.Unlock()
	if preCfg == nil {
		t.Fatal("the QoS 1 subscribe must leave a consumer in sess.cons")
	}

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 1}, {filter: "foo", qos: 0}}, []byte{1, 0})
	testMQTTFlush(t, c, nil, r)

	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if testSubRevokeConsumerLives(t, sess, preCfg.Durable) {
			return fmt.Errorf("durable %q still exists", preCfg.Durable)
		}
		return nil
	})
	sess.mu.Lock()
	live := sess.cons["foo"]
	sess.mu.Unlock()
	if live != nil {
		t.Fatalf("a downgrade that STANDS must drop the cons entry, got %v", live)
	}

	asm := testMQTTGetAccountSessionManager(t, s, "sub")
	ps, _ := testReconcileRecord(t, asm, "sub")
	// Two-value form: a MISSING key also reads as zero, and a missing key is a removal,
	// which is a different fact and a different code path.
	if qos, ok := ps.Subs["foo"]; !ok || qos != 0 {
		t.Fatalf("the record must end with the membership PRESENT at QoS 0, got %v (present %v)", qos, ok)
	}
	if ps.Cons["foo"] != nil {
		t.Fatalf("a QoS 0 membership whose consumer was DESTROYED must not be named in "+
			"the record, got %+v", ps.Cons["foo"])
	}
}

// TestMQTTSubDuplicateFilterRepeatedDowngradeDeletesOnce pins the at-most-one-live-staging
// invariant. [{foo,0},{foo,0}] appended TWO entries for one config at HEAD and ran the
// irreversible teardown twice; the second run is silent in every observable state, which is
// why the assertion counts executions.
func TestMQTTSubDuplicateFilterRepeatedDowngradeDeletesOnce(t *testing.T) {
	// NOT parallel: it installs a PROCESS-GLOBAL seam. A parallel sibling installing the
	// same one replaces the pointer, and the loss is silent — the counter simply reads
	// zero, which is indistinguishable from the behaviour this test would fail on.
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sess := testSubRevokeSession(t, s, "sub")
	sess.mu.Lock()
	preCfg := sess.cons["foo"]
	sess.mu.Unlock()
	if preCfg == nil {
		t.Fatal("the QoS 1 subscribe must leave a consumer in sess.cons")
	}

	var mu sync.Mutex
	execs := 0
	restore := setMQTTTestDeferredDeleteHook(func(_, durable string) {
		if durable != preCfg.Durable {
			return
		}
		mu.Lock()
		execs++
		mu.Unlock()
	})
	defer restore()

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo", qos: 0}, {filter: "foo", qos: 0}}, []byte{0, 0})
	testMQTTFlush(t, c, nil, r)

	mu.Lock()
	got := execs
	mu.Unlock()
	if got != 1 {
		t.Fatalf("one consumer, one irreversible teardown; ran %d times", got)
	}

	t.Run("downgrade upgrade downgrade", func(t *testing.T) {
		ci2 := &mqttConnInfo{clientID: "sub2", cleanSess: true}
		c2, r2 := testMQTTConnect(t, ci2, o.MQTT.Host, o.MQTT.Port)
		defer c2.Close()
		testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})

		sess2 := testSubRevokeSession(t, s, "sub2")
		sess2.mu.Lock()
		cfg2 := sess2.cons["bar"]
		sess2.mu.Unlock()
		if cfg2 == nil {
			t.Fatal("the QoS 1 subscribe must leave a consumer in sess.cons")
		}

		var mu2 sync.Mutex
		execs2 := 0
		restore2 := setMQTTTestDeferredDeleteHook(func(_, durable string) {
			if durable != cfg2.Durable {
				return
			}
			mu2.Lock()
			execs2++
			mu2.Unlock()
		})
		defer restore2()

		testMQTTSub(t, 2, c2, r2, []*mqttFilter{
			{filter: "bar", qos: 0}, {filter: "bar", qos: 1}, {filter: "bar", qos: 0},
		}, []byte{0, 1, 0})
		testMQTTFlush(t, c2, nil, r2)

		mu2.Lock()
		got2 := execs2
		mu2.Unlock()
		if got2 != 1 {
			t.Fatalf("a revoked staging that is staged again is still ONE teardown; ran %d times", got2)
		}
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if testSubRevokeConsumerLives(t, sess2, cfg2.Durable) {
				return fmt.Errorf("durable %q still exists", cfg2.Durable)
			}
			return nil
		})
		asm := testMQTTGetAccountSessionManager(t, s, "sub2")
		ps, _ := testReconcileRecord(t, asm, "sub2")
		if ps.Subs["bar"] != 0 {
			t.Fatalf("the record must end at the LAST occurrence's QoS 0, got %v", ps.Subs["bar"])
		}
	})
}

// TestMQTTDeferredDelRevokeRefusesUnreadyDelivery is a UNIT test on the readiness
// predicate, under a SYNTHESIZED state. It claims nothing about production reachability:
// for a live entry this packet created, "cons[sid] present and its delivery subscription
// closed" has no producer other than that packet's own staging. Refusing is the point —
// granting a QoS the broker cannot serve is the defect, not a lesser form of it.
func TestMQTTDeferredDelRevokeRefusesUnreadyDelivery(t *testing.T) {
	_, c, sess, _ := drainTestSession(t, "revoke-unready")

	const sid = "foo"
	cfg := &ConsumerConfig{Durable: "dur-revoke-unready", DeliverSubject: mqttSubPrefix + "dur-revoke-unready"}
	staged := &subscription{mqtt: &mqttSub{closed: true, jsDur: cfg.Durable}}

	// The internal account client this session is built on has no subscription map
	// until it takes one, and the predicate reads c.subs.
	c.mu.Lock()
	if c.subs == nil {
		c.subs = make(map[string]*subscription)
	}
	c.mu.Unlock()

	install := func(inCons *ConsumerConfig, inSubs *subscription) {
		sess.mu.Lock()
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		if inCons == nil {
			delete(sess.cons, sid)
		} else {
			sess.cons[sid] = inCons
		}
		sess.mu.Unlock()
		c.mu.Lock()
		if inSubs == nil {
			delete(c.subs, cfg.DeliverSubject)
		} else {
			c.subs[cfg.DeliverSubject] = inSubs
		}
		c.mu.Unlock()
	}

	for _, tc := range []struct {
		name   string
		cons   *ConsumerConfig
		subs   *subscription
		closed *subscription
	}{
		{
			name:   "the installed subscription is no longer the one whose barrier we hold",
			cons:   cfg,
			subs:   &subscription{mqtt: &mqttSub{}},
			closed: staged,
		},
		{
			name:   "no barrier was ever lowered",
			cons:   cfg,
			subs:   staged,
			closed: nil,
		},
		{
			name:   "the session's consumer for this sid moved",
			cons:   &ConsumerConfig{Durable: "dur-other", DeliverSubject: cfg.DeliverSubject},
			subs:   staged,
			closed: staged,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			install(tc.cons, tc.subs)
			dd := &mqttDeferredDel{sid: sid, cc: cfg, closed: tc.closed}
			sess.mu.Lock()
			err := dd.revokeLocked(c, sess, 1, false, false, 0)
			sess.mu.Unlock()
			if err == nil {
				t.Fatal("readiness that cannot be established must REFUSE, so the filter " +
					"fails 0x80 and the deferral stands")
			}
			if dd.revoked {
				t.Fatal("a refused revocation must leave the staging live, or the packet " +
					"ends with neither a teardown nor a delivering consumer")
			}
			if tc.closed != nil && !tc.closed.mqtt.closed {
				t.Fatal("a refused revocation must not reopen the barrier")
			}
		})
	}
}

// testMQTTSendSubOnly writes a SUBSCRIBE and does NOT read its SUBACK. A test whose packet
// transmits a PUBLISH before it completes must read the wire itself, in arrival order.
func testMQTTSendSubOnly(t testing.TB, pi uint16, c net.Conn, filters []*mqttFilter) {
	t.Helper()
	w := newMQTTWriter(0)
	pkLen := 2
	for _, f := range filters {
		pkLen += 2 + len(f.filter) + 1
	}
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for _, f := range filters {
		w.WriteBytes([]byte(f.filter))
		w.WriteByte(f.qos)
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing SUBSCRIBE protocol: %v", err)
	}
}

// testMQTTReadSubAckTolerantOfPubs reads until the SUBACK for pi, acknowledging any
// PUBLISH that arrives first.
//
// Whether a delivery started inside the packet reaches the wire at all is NOT determined:
// a re-staged downgrade withdraws a registration that is still pending, so the same
// message is transmitted on one run and cancelled before transmission on another. Both are
// correct, and both leave the assertion this test makes — the nonce in the packet's own
// membership-removal write — unchanged, so the reader must not demand either.
func testMQTTReadSubAckTolerantOfPubs(t testing.TB, c net.Conn, r *mqttReader, pi uint16) []byte {
	t.Helper()
	var codes []byte
	for {
		b, pl := testMQTTReadPacket(t, r)
		switch b & mqttPacketMask {
		case mqttPacketPub:
			_, ppi, _, _ := testMQTTGetPubMsgExEx(t, c, r, b, pl, _EMPTY_, nil)
			if mqttGetQoS(b&mqttPacketFlagMask) > 0 {
				testMQTTSendPIPacket(mqttPacketPubAck, t, c, ppi)
			}
		case mqttPacketSubAck:
			rpi, err := r.readUint16("packet identifier")
			if err != nil || rpi != pi {
				t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
			}
			for rem := pl - 2; rem > 0; rem-- {
				qos, err := r.readByte("filter qos")
				if err != nil {
					t.Fatal(err)
				}
				codes = append(codes, qos)
			}
			return codes
		default:
			t.Fatalf("unexpected packet %x while waiting for the SUBACK", b&mqttPacketMask)
		}
	}
}

// testMQTTReadPubAndSubAck reads until it has seen BOTH one PUBLISH and the SUBACK for pi,
// in whichever order they arrive.
//
// A packet that transmits a PUBLISH from inside its own processing puts two writes on the
// wire with no ordering between them: the delivery is written by the callback's goroutine
// while the SUBACK is written by the read loop once the packet completes. Asserting either
// order is a flake, and it showed up as one — the first form of these tests read the
// PUBLISH first and failed under -race, which merely made the read loop slower.
func testMQTTReadPubAndSubAck(t testing.TB, c net.Conn, r *mqttReader, pi uint16,
	topic string, payload []byte) (byte, uint16, []byte) {
	t.Helper()
	var pflags byte
	var ppi uint16
	var codes []byte
	gotPub, gotAck := false, false
	for !gotPub || !gotAck {
		b, pl := testMQTTReadPacket(t, r)
		switch b & mqttPacketMask {
		case mqttPacketPub:
			if gotPub {
				t.Fatalf("a second PUBLISH arrived; the schedule expects exactly one")
			}
			pflags, ppi, _, _ = testMQTTGetPubMsgExEx(t, c, r, b, pl, topic, payload)
			gotPub = true
		case mqttPacketSubAck:
			rpi, err := r.readUint16("packet identifier")
			if err != nil || rpi != pi {
				t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
			}
			for rem := pl - 2; rem > 0; rem-- {
				qos, err := r.readByte("filter qos")
				if err != nil {
					t.Fatal(err)
				}
				codes = append(codes, qos)
			}
			gotAck = true
		default:
			t.Fatalf("unexpected packet %x while waiting for the PUBLISH and the SUBACK", b&mqttPacketMask)
		}
	}
	return pflags, ppi, codes
}

// TestMQTTSubDuplicateFilterRevocationInstallsTheTermsWithTheReopen pins the one ordering
// constraint the design makes: the barrier, the terms and the un-void move in ONE critical
// section. Splitting them would let a delivery dispatched in between go out at the
// PRE-PACKET QoS — here a QoS 2 PUBLISH under a QoS 1 grant [MQTT-3.8.4-8].
//
// A post-SUBACK publication cannot see that, because the existing-consumer refresh arm
// re-applies the same four fields immediately afterwards. So the schedule runs INSIDE the
// packet, between the revocation and that refresh.
func TestMQTTSubDuplicateFilterRevocationInstallsTheTermsWithTheReopen(t *testing.T) {
	// NOT parallel: process-global seams, see the note on the repeated-downgrade test.
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	// QoS 2 before the packet, so a stale-terms transmission is visible as a QoS 2
	// PUBLISH rather than as a missing one.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 2}}, []byte{2})

	sess := testSubRevokeSession(t, s, "sub")
	sess.mu.Lock()
	preCfg := sess.cons["foo"]
	sess.mu.Unlock()
	if preCfg == nil {
		t.Fatal("the QoS 2 subscribe must leave a consumer in sess.cons")
	}

	var mu sync.Mutex
	var skippedReply string
	var sawSkip bool
	restoreSkip := setMQTTTestDeliverySkippedHook(func(sub *subscription, reply string) {
		if sub == nil || sub.mqtt == nil || sub.mqtt.jsDur != preCfg.Durable {
			return
		}
		mu.Lock()
		if !sawSkip {
			sawSkip, skippedReply = true, reply
		}
		mu.Unlock()
	})
	defer restoreSkip()

	// storeErr and scheduleErr are written inside the pause, which runs on the client's
	// READ LOOP, and read on the test goroutine afterwards. The socket traffic between
	// them is not a happens-before edge, so they take the same mutex as sawSkip.
	var storeErr, scheduleErr error
	fail := func(err error, schedule bool) {
		mu.Lock()
		if schedule {
			scheduleErr = err
		} else {
			storeErr = err
		}
		mu.Unlock()
	}
	restorePause := setMQTTTestSubPause(func(at string) {
		switch at {
		case "staged:foo":
			// The barrier is down and the packet is still running. An ordinary MQTT
			// publication cannot reach the callback here — processSubs holds the subs
			// barrier for the whole packet and a concurrent store waits on it — so the
			// source is stored directly, with the MQTT publish header the delivery path
			// requires.
			hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s:%d\r\n\r\n", mqttNatsHeader, 2)
			full := append(append([]byte(nil), hdr...), []byte("q2")...)
			if _, err := sess.jsa.storeMsg(mqttStreamSubjectPrefix+"foo", len(hdr), full); err != nil {
				fail(err, false)
				return
			}
			// Wait for the skip POSITIVELY. Its absence would be indistinguishable from
			// a delivery that never arrived.
			deadline := time.Now().Add(5 * time.Second)
			for {
				mu.Lock()
				got := sawSkip
				mu.Unlock()
				if got {
					return
				}
				if time.Now().After(deadline) {
					fail(fmt.Errorf("the delivery was never skipped by the closed arm"), true)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		case "revoked:foo":
			// The revocation has installed its terms; the refresh arm has NOT run yet.
			// Re-offer the skipped sequence by NAKing it explicitly, so the re-delivery
			// is scheduled by this test rather than by an AckWait expiry.
			mu.Lock()
			reply := skippedReply
			mu.Unlock()
			if reply == _EMPTY_ {
				fail(fmt.Errorf("no ack reply subject was captured for the skipped delivery"), true)
				return
			}
			sess.jsa.sendMsg(reply, AckNak)
			// The transmission must happen while this pause is HELD, or a mutation that
			// moves the term application after it would be masked by the refresh arm.
			// trackPublish allocating a packet identifier is that transmission.
			deadline := time.Now().Add(5 * time.Second)
			for {
				sess.mu.Lock()
				n := len(sess.pendingPublish)
				sess.mu.Unlock()
				if n > 0 {
					return
				}
				if time.Now().After(deadline) {
					fail(fmt.Errorf("the re-offered delivery was never transmitted inside the pause"), true)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	})
	defer restorePause()

	testMQTTSendSubOnly(t, 2, c, []*mqttFilter{{filter: "foo", qos: 0}, {filter: "foo", qos: 1}})

	// The re-offer is transmitted inside the packet, so it reaches the wire first.
	// Receipt is REQUIRED: a timeout here is a failure of the test, and the comparison
	// below is between two deliveries that both happened, never between one and silence.
	pflags, pi, codes := testMQTTReadPubAndSubAck(t, c, r, 2, "foo", []byte("q2"))
	mu.Lock()
	serr, sched := storeErr, scheduleErr
	mu.Unlock()
	if serr != nil {
		t.Fatalf("storing the QoS 2 source: %v", serr)
	}
	if sched != nil {
		t.Fatalf("the schedule did not reach the state it names: %v", sched)
	}
	if got := mqttGetQoS(pflags); got != 1 {
		t.Fatalf("the delivery must go out at THIS occurrence's QoS 1, got %d: the terms "+
			"and the reopen are one critical section, and a split sends under the "+
			"pre-packet grant", got)
	}
	testMQTTSendPIPacket(mqttPacketPubAck, t, c, pi)

	if len(codes) != 2 || codes[0] != 0 || codes[1] != 1 {
		t.Fatalf("expected SUBACK [0x00 0x01], got %v", codes)
	}
}

// TestMQTTSubDuplicateFilterRevivedFilterFailureRestagesTheDowngrade covers the reviver's
// own failure. sess.update skips a failed filter, so the earlier QoS 0 occurrence's
// membership stands and the downgrade is owed AGAIN.
//
// Re-staging goes through the same helper rather than restoring the saved barrier and
// verdicts, because the interval between the revocation and the failure had LIVE delivery:
// a registration allocated there is withdrawn only by a fresh snapshot, and its
// cancellation belongs in the packet's OWN membership-removal write. That write is the
// assertion — a post-execution check is worthless, because the teardown's own purge would
// re-find and secure the same registration.
func TestMQTTSubDuplicateFilterRevivedFilterFailureRestagesTheDowngrade(t *testing.T) {
	// NOT parallel: process-global seams, see the note on the repeated-downgrade test.
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sess := testSubRevokeSession(t, s, "sub")
	asm := testMQTTGetAccountSessionManager(t, s, "sub")
	sess.mu.Lock()
	preCfg := sess.cons["foo"]
	sess.mu.Unlock()
	if preCfg == nil {
		t.Fatal("the QoS 1 subscribe must leave a consumer in sess.cons")
	}

	var mu sync.Mutex
	var intervalNonce string
	var intervalPI uint16
	var sawPending bool
	// Written on the read loop inside the pause, read on the test goroutine: guarded,
	// for the same reason as test 3's.
	var scheduleErr error
	// The point the deferred teardown starts running. Only a record written BEFORE it is
	// evidence about the packet's own write, because the teardown's own purge secures the
	// same registration again and would mask a re-stage that lost it.
	execStarted := false

	// Hold the interval registration's STORE, so the allocation is still regPending when
	// the re-stage runs. That is the branch the plan names: the cancellation must remove
	// the allocation, park its tombstone in the packet's write, and survive the store
	// completing afterwards. A confirmed register would exercise only the weaker
	// transmitted-void branch.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseStore := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseStore()
	// Released the moment the re-stage has CANCELLED the allocation, not later: the
	// packet's own commit path waits on the register store to settle before it can
	// secure that cancellation, so holding it past the re-stage wedges the packet
	// rather than testing it. Watching for regCancelled makes the ordering exact — the
	// cancellation is observed first, the store completes afterwards — where a sleep
	// would only make it likely.
	watchDone := make(chan struct{})
	defer func() { <-watchDone }()
	go func() {
		defer close(watchDone)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			pi, seen := intervalPI, sawPending
			mu.Unlock()
			if seen {
				sess.mu.Lock()
				ack := sess.pendingPublish[pi]
				cancelled := ack != nil && ack.regCancelled
				sess.mu.Unlock()
				if cancelled {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
		releaseStore()
	}()
	asm.mu.Lock()
	asm.testInflightStore = func(subject string, expectSeq uint64, rec *mqttInflightRecord) (uint64, error) {
		if strings.Contains(subject, sess.idHash) {
			<-release
		}
		return asm.jsa.storeInflightCAS(subject, expectSeq, rec)
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testInflightStore = nil
		asm.mu.Unlock()
	}()

	restorePause := setMQTTTestSubPause(func(at string) {
		if at != "revoked:foo" {
			return
		}
		// The barrier is open again. Put a message through it, so a FRESH registration
		// exists that the saved snapshot cannot know about.
		hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s:%d\r\n\r\n", mqttNatsHeader, 1)
		full := append(append([]byte(nil), hdr...), []byte("interval")...)
		if _, err := sess.jsa.storeMsg(mqttStreamSubjectPrefix+"foo", len(hdr), full); err != nil {
			mu.Lock()
			scheduleErr = err
			mu.Unlock()
			return
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			sess.mu.Lock()
			for pi, ack := range sess.pendingPublish {
				// regPending is REQUIRED, not incidental: it is what makes this the
				// allocated-but-not-yet-durable branch rather than the transmitted one.
				if ack != nil && ack.regNonce != _EMPTY_ && ack.regPending {
					mu.Lock()
					intervalNonce, intervalPI, sawPending = ack.regNonce, pi, true
					mu.Unlock()
				}
			}
			sess.mu.Unlock()
			mu.Lock()
			got := intervalNonce
			mu.Unlock()
			if got != _EMPTY_ {
				return
			}
			if time.Now().After(deadline) {
				mu.Lock()
				scheduleErr = fmt.Errorf("no PENDING registration was allocated in the reopened interval")
				mu.Unlock()
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	})
	defer restorePause()

	written := &testSubRevokeWrittenRecords{stop: func() bool {
		mu.Lock()
		defer mu.Unlock()
		return execStarted
	}}
	defer written.armWrittenRecords(sess)()

	restoreExec := setMQTTTestDeferredDeleteHook(func(_, durable string) {
		if durable != preCfg.Durable {
			return
		}
		mu.Lock()
		execStarted = true
		mu.Unlock()
	})
	defer restoreExec()

	// Fail the "consumer" exit for sid "foo" on its SECOND call: that exit is reached by
	// EVERY occurrence, and failing the first would cancel the downgrade before any
	// revival exists.
	calls := 0
	asm.mu.Lock()
	asm.testSubFilterFail = func(exit, sid string) error {
		if exit != "consumer" || sid != "foo" {
			return nil
		}
		calls++
		if calls < 2 {
			return nil
		}
		return fmt.Errorf("test: the reviving occurrence fails after adopting")
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testSubFilterFail = nil
		asm.mu.Unlock()
	}()

	// The interval message may or may not reach the wire, and if it does its order against
	// the SUBACK is not determined, so the reader tolerates both.
	testMQTTSendSubOnly(t, 2, c, []*mqttFilter{{filter: "foo", qos: 0}, {filter: "foo", qos: 1}})
	codes := testMQTTReadSubAckTolerantOfPubs(t, c, r, 2)
	if len(codes) != 2 || codes[0] != 0 || codes[1] != mqttSubAckFailure {
		t.Fatalf("expected SUBACK [0x00 0x80], got %v", codes)
	}
	testMQTTFlush(t, c, nil, r)

	mu.Lock()
	sched := scheduleErr
	nonce, pendingSeen, pi := intervalNonce, sawPending, intervalPI
	mu.Unlock()
	packetSaves, werr := written.landed()
	if werr != nil {
		t.Fatalf("collecting the written records: %v", werr)
	}
	if sched != nil {
		t.Fatalf("the schedule did not reach the state it names: %v", sched)
	}
	if nonce == _EMPTY_ || !pendingSeen {
		t.Fatal("no PENDING registration was allocated in the reopened interval: the " +
			"assertion below would be vacuous, and the branch it exists for unreached")
	}
	if len(packetSaves) == 0 {
		t.Fatal("the packet issued no session record write before its deferred teardown ran")
	}
	last := packetSaves[len(packetSaves)-1]
	if !carriesNonce(last, nonce) {
		t.Fatalf("the registration allocated while the barrier was OPEN must be withdrawn "+
			"into the packet's own membership-removal write: a re-stage that restores "+
			"the saved snapshot cannot know about it, and the identifier it holds has "+
			"nothing to recover it. Stored VoidedRegs=%v", last.VoidedRegs)
	}
	if last.Subs["foo"] != 0 {
		t.Fatalf("that same write must carry the QoS 0 membership the failed reviver left "+
			"standing, got %v", last.Subs["foo"])
	}

	// The held store was released the moment the cancellation was observed, so its
	// completion lands on an allocation that is already cancelled. It must not resurrect
	// it: a settled-but-cancelled entry is either dropped or left parked, never handed
	// back as sendable.
	releaseStore()
	checkFor(t, 3*time.Second, 15*time.Millisecond, func() error {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		ack := sess.pendingPublish[pi]
		if ack == nil {
			return nil
		}
		if ack.regPending {
			return fmt.Errorf("the held register store has not settled yet")
		}
		if !ack.regCancelled {
			return fmt.Errorf("a late store completion resurrected a CANCELLED allocation")
		}
		return nil
	})

	// And the end state: the downgrade stands, so the durable goes.
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		if testSubRevokeConsumerLives(t, sess, preCfg.Durable) {
			return fmt.Errorf("durable %q still exists: the re-stage never happened", preCfg.Durable)
		}
		return nil
	})
	ps, _ := testReconcileRecord(t, asm, "sub")
	if ps.Subs["foo"] != 0 {
		t.Fatalf("a failed reviving occurrence leaves the QoS 0 membership standing, got %v", ps.Subs["foo"])
	}
	sess.mu.Lock()
	live := sess.cons["foo"]
	sess.mu.Unlock()
	if live != nil {
		t.Fatalf("the re-staged teardown must drop the cons entry, got %v", live)
	}
}

// TestMQTTSubDuplicateFilterCompanionFailureRestagesBothHalves is the multi-half form: a
// wildcard revokes on BOTH sids, and a companion-half failure after the main half revoked
// owes a fresh staging for each.
func TestMQTTSubDuplicateFilterCompanionFailureRestagesBothHalves(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})

	sess := testSubRevokeSession(t, s, "sub")
	asm := testMQTTGetAccountSessionManager(t, s, "sub")
	sess.mu.Lock()
	mainCfg, fwcCfg := sess.cons["foo.>"], sess.cons["foo fwc"]
	sess.mu.Unlock()
	if mainCfg == nil || fwcCfg == nil {
		t.Fatalf("a multi-level filter owns two consumers, got main=%v fwc=%v", mainCfg, fwcCfg)
	}

	calls := 0
	asm.mu.Lock()
	asm.testSubFilterFail = func(exit, sid string) error {
		if exit != "consumer-fwc" || sid != "foo fwc" {
			return nil
		}
		calls++
		if calls < 2 {
			return nil
		}
		return fmt.Errorf("test: the reviving occurrence's companion half fails")
	}
	asm.mu.Unlock()
	defer func() {
		asm.mu.Lock()
		asm.testSubFilterFail = nil
		asm.mu.Unlock()
	}()

	testMQTTSub(t, 2, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}, {filter: "foo/#", qos: 1}},
		[]byte{0, mqttSubAckFailure})
	testMQTTFlush(t, c, nil, r)

	// BOTH sids were revoked by the reviving filter, so both are owed a fresh staging.
	for _, cfg := range []*ConsumerConfig{mainCfg, fwcCfg} {
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			if testSubRevokeConsumerLives(t, sess, cfg.Durable) {
				return fmt.Errorf("durable %q still exists", cfg.Durable)
			}
			return nil
		})
	}
	ps, _ := testReconcileRecord(t, asm, "sub")
	if ps.Subs["foo.>"] != 0 {
		t.Fatalf("a failed reviving occurrence leaves the QoS 0 membership standing, got %v", ps.Subs["foo.>"])
	}
}

// TestMQTTSubDuplicateFilterEarlySharedSaveRecordsNoVoidVerdict pins the
// early-shared-save window in BOTH directions.
//
// A shared filter between the two occurrences persists the session record MID-LOOP,
// beside a still-pre-packet plain membership. This test used to assert that the record
// carried occurrence 1's void verdict on a TRANSMITTED register — a record saying a
// delivery the client is holding was never transmitted — and it pinned that as a
// PRE-EXISTING exposure, with its own instruction that "if this fires, that window was
// closed and this assertion is what must be updated".
//
// **It fired, and the window is closed.** The QoS 0 downgrade now stages a conversion
// CANDIDATE, whose staging cancels the not-yet-transmitted allocations but records no
// verdict on the transmitted ones — because those exchanges are about to be drained
// rather than abandoned, and a verdict any mid-packet save makes durable would be a lie
// that unvoiding could not retract. So the assertion is inverted rather than deleted: the
// early save must carry NO such verdict, and a future change reintroducing one fails
// here.
//
// The second direction is unchanged and still asserted: the packet's own save must not
// carry the verdict either, so nothing survives to the next resume.
//
// MQTT 5 throughout, because shared subscriptions are a v5 feature: a 3.1.1 client's
// "$share/g/bar" is an ordinary topic filter, which stages no early save at all and would
// leave this test asserting nothing.
func TestMQTTSubDuplicateFilterEarlySharedSaveRecordsNoVoidVerdict(t *testing.T) {
	// NOT parallel: process-global seams, see the note on the repeated-downgrade test.
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "sub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	if codes := testMQTTSubV5(t, c, r, 1, []mqttV5SubFilter{{topic: "foo", opts: 1}}); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("expected SUBACK [1], got %x", codes)
	}

	cp, rp := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTReadConnAckV5(t, rp)

	testMQTTPublishV5(t, cp, 1, 1, "foo", []byte("held"))
	testMQTTReadPubAck(t, rp, 1)
	_, heldPI, _ := testMQTTReadPubV5Raw(t, r, "foo", []byte("held"))

	sess := testSubRevokeSession(t, s, "sub")
	sess.mu.Lock()
	var heldNonce string
	var heldTransmitted bool
	if ack := sess.pendingPublish[heldPI]; ack != nil {
		heldNonce, heldTransmitted = ack.regNonce, !ack.regPending
	}
	sess.mu.Unlock()
	if heldNonce == _EMPTY_ || !heldTransmitted {
		t.Fatalf("the held delivery must carry a CONFIRMED register (nonce=%q transmitted=%v)",
			heldNonce, heldTransmitted)
	}

	written := &testSubRevokeWrittenRecords{}
	restoreSave := written.armWrittenRecords(sess)

	// The shared filter between the two occurrences is what forces the mid-loop save.
	codes := testMQTTSubV5(t, c, r, 2, []mqttV5SubFilter{
		{topic: "foo", opts: 0}, {topic: "$share/g/bar", opts: 1}, {topic: "foo", opts: 1},
	})
	restoreSave()
	if len(codes) != 3 || codes[0] != 0 || codes[1] != 1 || codes[2] != 1 {
		t.Fatalf("expected SUBACK [0 1 1], got %x", codes)
	}

	got, werr := written.landed()
	if werr != nil {
		t.Fatalf("collecting the written records: %v", werr)
	}
	if len(got) < 2 {
		t.Fatalf("expected an early shared-filter save and the packet's own save, got %d writes", len(got))
	}
	early, final := got[0], got[len(got)-1]
	// The early save still lands mid-loop beside the PRE-PACKET membership — that half is
	// unchanged, and without it this test would not be pinning the window it names.
	if early.Subs["foo"] != 1 {
		t.Fatalf("the early shared save must still carry the PRE-PACKET plain membership, "+
			"got %v — without that the window this pins is not the one described", early.Subs["foo"])
	}
	// And it must carry NO void verdict for the held delivery. A record asserting that a
	// delivery the client is holding was never transmitted is incoherent beside a
	// membership that still grants QoS 1, and a crash there used to leave exactly that.
	if carriesNonce(early, heldNonce) {
		t.Fatal("the early shared save must record NO never-transmitted verdict for a held " +
			"delivery: the downgrade stages a conversion candidate, whose exchanges are to " +
			"be drained rather than abandoned, so no verdict is recorded for a mid-packet " +
			"save to make durable")
	}
	if final.Subs["foo"] != 1 {
		t.Fatalf("the packet's own save must end at the last occurrence's QoS 1, got %v", final.Subs["foo"])
	}
	if carriesNonce(final, heldNonce) {
		t.Fatal("the packet's own save must not carry the verdict either: with the " +
			"revocation there is nothing to withdraw, and nothing may survive to the " +
			"next resume claiming a held delivery was never sent")
	}
}
