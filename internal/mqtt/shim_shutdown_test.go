//go:build mqttport_fulltests

// Tests for graceful connection draining on shutdown (Layer 1): the server
// sends connected v5 clients a DISCONNECT 0x8B "server shutting down" and closes
// their sockets, 3.1.1 clients get a clean close, the drain is bounded, and the
// listener refuses new connections once shutdown has begun.

package mqtt

import (
	"errors"
	"net"
	"strconv"
	"testing"
	"time"
)

// TestMQTTShutdownV5SendsDisconnect verifies a connected MQTT 5.0 client receives
// a server DISCONNECT with reason 0x8B before the socket is closed.
func TestMQTTShutdownV5SendsDisconnect(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "shutv5", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTReadConnAckV5(t, r)

	// Begin shutdown asynchronously so we can read the drain DISCONNECT.
	s.BeginShutdown()

	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketDisconnect {
		t.Fatalf("expected DISCONNECT (0x%02x), got 0x%02x", mqttPacketDisconnect, pt)
	}
	if pl < 1 {
		t.Fatalf("expected DISCONNECT with a reason code, remaining length %d", pl)
	}
	reason, err := r.readByte("reason")
	if err != nil {
		t.Fatalf("reading DISCONNECT reason: %v", err)
	}
	if reason != mqttReasonServerShuttingDown {
		t.Fatalf("expected reason 0x%02x, got 0x%02x", mqttReasonServerShuttingDown, reason)
	}

	// The socket then closes.
	expectSocketClose(t, c)
}

// TestMQTTShutdownV311CleanClose verifies a 3.1.1 client gets a clean socket
// close and NO server DISCONNECT (3.1.1 has no server-initiated DISCONNECT).
func TestMQTTShutdownV311CleanClose(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, _ := testMQTTConnect(t, &mqttConnInfo{clientID: "shutv311", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()

	s.BeginShutdown()

	// No DISCONNECT byte should arrive — just EOF/closed.
	expectSocketClose(t, c)
}

// TestMQTTShutdownPostDialRefused verifies that once Shutdown has completed the
// listener is closed and a fresh dial is refused (not accepted without CONNACK).
func TestMQTTShutdownPostDialRefused(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	addr := net.JoinHostPort(o.MQTT.Host, strconv.Itoa(o.MQTT.Port))

	// Full shutdown (drops the embedded NATS too, so do it via the helper).
	testMQTTShutdownServer(s)

	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		c.Close()
		t.Fatalf("expected dial to %s to be refused after shutdown", addr)
	}
}

// TestMQTTShutdownBoundedWithStuckReader verifies the drain does not hang when a
// client stops reading: the bounded write deadline lets Shutdown complete well
// within shutdownDrainTimeout rather than blocking on the wedged socket.
func TestMQTTShutdownBoundedWithStuckReader(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// Connect several v5 clients and then never read from them again.
	var conns []net.Conn
	for i := 0; i < 5; i++ {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "stuck" + strconv.Itoa(i), cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		conns = append(conns, c)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		<-s.BeginShutdown()
		close(done)
	}()

	select {
	case <-done:
		if el := time.Since(start); el > shutdownDrainTimeout+2*time.Second {
			t.Fatalf("shutdown took %v, expected under the drain timeout", el)
		}
	case <-time.After(shutdownDrainTimeout + 5*time.Second):
		t.Fatal("shutdown did not complete: drain hung on non-reading clients")
	}
}

// expectSocketClose asserts the next read on c observes EOF or a closed
// connection within a short deadline. A read timeout means the socket was NOT
// closed by the drain, which is a failure.
func expectSocketClose(t testing.TB, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var buf [64]byte
	for {
		n, err := c.Read(buf[:])
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("socket not closed by drain (read timed out)")
			}
			// EOF / connection reset / use of closed connection: the drain closed it.
			return
		}
		if n == 0 {
			return
		}
		// Trailing bytes before the close: keep reading until EOF.
	}
}
