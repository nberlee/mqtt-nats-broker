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
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The server's error DISCONNECT reaches a client that is still sending: the reader keeps reading
// and discarding what the client sends, within bounds, before it closes.

// testLingerReadPacket reads one raw MQTT packet from c.
func testLingerReadPacket(c net.Conn) (byte, []byte, error) {
	h := make([]byte, 1)
	if _, err := io.ReadFull(c, h); err != nil {
		return 0, nil, err
	}
	n, mul := 0, 1
	for {
		b := make([]byte, 1)
		if _, err := io.ReadFull(c, b); err != nil {
			return 0, nil, err
		}
		n += int(b[0]&0x7f) * mul
		if b[0]&0x80 == 0 {
			break
		}
		mul *= 128
	}
	body := make([]byte, n)
	_, err := io.ReadFull(c, body)
	return h[0], body, err
}

// testLingerPubRel frames a PUBREL for pi.
func testLingerPubRel(pi uint16) []byte {
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPubRel | 0x2)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	return w.Bytes()
}

// testLingerServer runs a v5 server with a small Receive Maximum.
func testLingerServer(t *testing.T, recvMax int) (*Server, *Options) {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	o.MQTT.ReceiveMaximum = recvMax
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	return s, o
}

// testLingerViolate connects cid and publishes n QoS 2 messages without completing any, one past
// a Receive Maximum of n-1.
func testLingerViolate(t *testing.T, o *Options, cid string, n int, props []byte) net.Conn {
	t.Helper()
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true, props: props}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	testMQTTReadConnAckV5(t, r)
	for i := 1; i <= n; i++ {
		testMQTTPublishV5(t, c, 2, uint16(i), "lg/"+cid, []byte(fmt.Sprintf("message %d", i)))
	}
	return c
}

// testLingerAcknowledgeToDisconnect reads PUBRECs, answering each with a PUBREL after pace, until
// the DISCONNECT; it reports the DISCONNECT's reason, or the error that ended the walk first.
func testLingerAcknowledgeToDisconnect(c net.Conn, pace time.Duration) (byte, error) {
	for {
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		b, body, err := testLingerReadPacket(c)
		if err != nil {
			return 0, fmt.Errorf("read: %w", err)
		}
		switch b & mqttPacketMask {
		case mqttPacketPubRec:
			time.Sleep(pace)
			if _, err := c.Write(testLingerPubRel(uint16(body[0])<<8 | uint16(body[1]))); err != nil {
				return 0, fmt.Errorf("write: %w", err)
			}
		case mqttPacketDisconnect:
			if len(body) == 0 {
				return 0, nil
			}
			return body[0], nil
		}
	}
}

// TestMQTTv5ErrorDisconnectReachesAClientStillAcknowledging: a client that exceeds Receive Maximum
// and then walks its 1024 acknowledgements, answering each with a PUBREL at about a millisecond
// apiece (the Eclipse Paho interoperability client's shape), reads the server's DISCONNECT 0x93
// with no send failing on the way. Closing as soon as the DISCONNECT was written failed every
// iteration with "connection reset by peer" (measured 10 of 10 on the tree before this change).
func TestMQTTv5ErrorDisconnectReachesAClientStillAcknowledging(t *testing.T) {
	_, o := testLingerServer(t, 1024)
	for it := 0; it < 10; it++ {
		c := testLingerViolate(t, o, fmt.Sprintf("lgack%d", it), 1025, nil)
		reason, err := testLingerAcknowledgeToDisconnect(c, time.Millisecond)
		c.Close()
		if err != nil {
			t.Fatalf("iteration %d: the client lost the server's DISCONNECT: %v", it, err)
		}
		if reason != mqttReasonReceiveMaxExceeded {
			t.Fatalf("iteration %d: DISCONNECT reason %#x, want %#x", it, reason, mqttReasonReceiveMaxExceeded)
		}
	}
}

// TestMQTTv5ErrorDisconnectLingerEndsAtTheClientsClose: a client that closes once it has read the
// DISCONNECT ends the linger at once; its connection is gone well before the bound.
func TestMQTTv5ErrorDisconnectLingerEndsAtTheClientsClose(t *testing.T) {
	s, o := testLingerServer(t, 4)
	c := testLingerViolate(t, o, "lgclose", 5, nil)
	if _, err := testLingerAcknowledgeToDisconnect(c, 0); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	closed := time.Now()
	c.Close()
	checkFor(t, 5*time.Second, 5*time.Millisecond, func() error {
		if testLingerClientRegistered(s, "lgclose") {
			return fmt.Errorf("still registered")
		}
		return nil
	})
	if d := time.Since(closed); d > time.Second {
		t.Fatalf("the connection outlived the client's close by %v", d)
	}
}

func testLingerClientRegistered(s *Server, cid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cl := range s.clients {
		cl.mu.Lock()
		live := cl.isMqtt() && cl.mqtt.cid == cid
		cl.mu.Unlock()
		if live {
			return true
		}
	}
	return false
}

// testLingerAwaitServerClose reads (and discards) until the server closes, and reports how long
// after start that was.
func testLingerAwaitServerClose(t *testing.T, c net.Conn, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	_ = c.SetReadDeadline(start.Add(within))
	buf := make([]byte, 4096)
	for {
		if _, err := c.Read(buf); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("the server had not closed the connection after %v", within)
			}
			return time.Since(start)
		}
	}
}

// TestMQTTv5ErrorDisconnectLingerIsBounded: a peer that reads the DISCONNECT, sends a little more
// and then stays silent with its side open is closed once the bound passes.
func TestMQTTv5ErrorDisconnectLingerIsBounded(t *testing.T) {
	mqttDisconnectLinger.Set(time.Second)
	defer mqttDisconnectLinger.Set(5 * time.Second)
	_, o := testLingerServer(t, 4)
	c := testLingerViolate(t, o, "lgbound", 5, nil)
	if _, err := testLingerAcknowledgeToDisconnect(c, 0); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if _, err := c.Write(testLingerPubRel(1)); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if d := testLingerAwaitServerClose(t, c, 5*time.Second); d > 1500*time.Millisecond {
		t.Fatalf("the server closed %v after the DISCONNECT, past its 1 s bound", d)
	}
}

// TestMQTTv5ErrorDisconnectDrainIsNotParsed: what a client sends after the server's DISCONNECT has
// no effect: a DISCONNECT ending its session (expiry 0) does not end it, and a PUBLISH is not
// published.
func TestMQTTv5ErrorDisconnectDrainIsNotParsed(t *testing.T) {
	s, o := testLingerServer(t, 4)
	ws, wr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "lgwatch", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer ws.Close()
	testMQTTReadConnAckV5(t, wr)
	testMQTTSubV5(t, ws, wr, 1, []mqttV5SubFilter{{topic: "lg/watched", opts: 0}})
	c := testLingerViolate(t, o, "lgparse", 5, mqttV5ConnPropsSessionExpiry(60))
	if _, err := testLingerAcknowledgeToDisconnect(c, 0); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	testMQTTPublishV5(t, c, 0, 0, "lg/watched", []byte("after the DISCONNECT"))
	if _, err := c.Write(mqttV5DisconnectSessionExpiry(mqttReasonSuccess, 0)); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	c.Close()
	if _, _, ok := testMQTTReadPacketReady(wr, 500*time.Millisecond); ok {
		t.Fatalf("a PUBLISH sent after the server's DISCONNECT was published")
	}
	checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
		if testLingerClientRegistered(s, "lgparse") {
			return fmt.Errorf("still registered")
		}
		return nil
	})
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "lgparse",
		props: mqttV5ConnPropsSessionExpiry(60)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	if sp, _, _ := testMQTTReadConnAckV5(t, r2); !sp {
		t.Fatalf("the session ended: a DISCONNECT sent after the server's DISCONNECT was acted on")
	}
}

// TestMQTTv5ErrorDisconnectIsTheLastFrame: a writer that reaches the socket's write lock while the
// DISCONNECT is being written is refused: nothing follows the DISCONNECT on the wire.
func TestMQTTv5ErrorDisconnectIsTheLastFrame(t *testing.T) {
	_, o := testLingerServer(t, 4)
	var staged atomic.Int32
	h := func(c *client) {
		if c.mqtt == nil || !strings.HasPrefix(c.mqtt.cid, "lglast") || staged.Add(1) != 1 {
			return
		}
		go c.queueOutbound([]byte{mqttPacketPingResp, 0})
		time.Sleep(100 * time.Millisecond) // the staged writer is waiting on the write lock
	}
	mqttTestAfterSealingWriteV.Store(&h)
	defer mqttTestAfterSealingWriteV.Store(nil)
	c := testLingerViolate(t, o, "lglast", 5, nil)
	if _, err := testLingerAcknowledgeToDisconnect(c, 0); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	// The hook runs after the DISCONNECT's bytes are handed to the socket, so this client can read
	// them before the server's writer reaches it: wait for the staging rather than sample it.
	checkFor(t, 2*time.Second, 5*time.Millisecond, func() error {
		if staged.Load() == 0 {
			return fmt.Errorf("fixture: no writer was staged at the DISCONNECT's write")
		}
		return nil
	})
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if b, _, err := testLingerReadPacket(c); err == nil {
		t.Fatalf("a packet of type %#x followed the server's DISCONNECT", b&mqttPacketMask)
	}
}

// TestMQTTv5ErrorDisconnectPreventedDoesNotLinger: a DISCONNECT the sealed output gate prevented is
// not followed by a linger: the connection closes at once.
func TestMQTTv5ErrorDisconnectPreventedDoesNotLinger(t *testing.T) {
	s, o := testLingerServer(t, 4)
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "lgprev", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)
	sealOutput(testMQTTGetClient(t, s, "lgprev").outBind)
	for i := 1; i <= 5; i++ {
		testMQTTPublishV5(t, c, 2, uint16(i), "lg/prev", []byte("x"))
	}
	if d := testLingerAwaitServerClose(t, c, 4*time.Second); d > time.Second {
		t.Fatalf("a connection whose DISCONNECT was prevented lingered %v", d)
	}
}

// TestMQTTv5ErrorDisconnectLingerStopsAtItsCeiling: a client that keeps sending fast after the
// DISCONNECT is closed once the ceiling is read, well before the time bound.
func TestMQTTv5ErrorDisconnectLingerStopsAtItsCeiling(t *testing.T) {
	_, o := testLingerServer(t, 4)
	c := testLingerViolate(t, o, "lgcap", 5, nil)
	if _, err := testLingerAcknowledgeToDisconnect(c, 0); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	start := time.Now()
	chunk := make([]byte, 64*1024)
	for time.Since(start) < 4*time.Second {
		if _, err := c.Write(chunk); err != nil {
			break
		}
	}
	if d := time.Since(start); d >= 4*time.Second {
		t.Fatalf("the server was still reading after %v of continuous input: no ceiling", d)
	}
}

// TestMQTTv5PacketTooLargeDisconnectReachesAClientStillSending: the 0x95 arm lingers too: a
// client that sent an oversized packet and keeps sending small ones reads DISCONNECT 0x95 with no
// send failing.
func TestMQTTv5PacketTooLargeDisconnectReachesAClientStillSending(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	o.MaxPayload = 1024
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	for it := 0; it < 5; it++ {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: fmt.Sprintf("lgbig%d", it), cleanStart: true},
			o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		done := make(chan error, 1)
		go func() {
			_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
			for {
				b, body, err := testLingerReadPacket(c)
				if err != nil {
					done <- fmt.Errorf("read: %w", err)
					return
				}
				if b&mqttPacketMask == mqttPacketDisconnect {
					if len(body) == 0 || body[0] != mqttReasonPacketTooLarge {
						done <- fmt.Errorf("DISCONNECT body %v", body)
						return
					}
					done <- nil
					return
				}
			}
		}()
		testMQTTPublishV5(t, c, 0, 0, "lg/big", make([]byte, 4096))
		var werr error
		for i := 0; i < 300 && werr == nil; i++ {
			_, werr = c.Write([]byte{mqttPacketPing, 0})
			time.Sleep(time.Millisecond)
		}
		err := <-done
		c.Close()
		if werr != nil {
			t.Fatalf("iteration %d: a send failed while the DISCONNECT was owed: %v", it, werr)
		}
		if err != nil {
			t.Fatalf("iteration %d: %v", it, err)
		}
	}
}
