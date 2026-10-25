//go:build mqttport_fulltests

package mqtt

import (
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// A plaintext listener beside TLS: each TCP connection takes its listener's mode, so the
// plaintext port never handshakes and never asks for a client certificate, and the TLS port
// stays TLS-only. Admission (MaxConn), the CONNECT deadline and shutdown are shared.

// testMQTTPlainBesideTLSOptions is a TLS listener on an ephemeral Port with a plaintext one
// on an ephemeral PlainPort.
func testMQTTPlainBesideTLSOptions(m *testTLSMaterial) *Options {
	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	o.MQTT.PlainPort = -1
	return o
}

func testMQTTPlainPort(t testing.TB, s *Server) int {
	t.Helper()
	a := s.PlainAddr()
	if a == nil {
		t.Fatal("no plaintext listener")
	}
	return a.(*net.TCPAddr).Port
}

// TestMQTTPlainListenerBesideTLS: plaintext CONNECT is accepted on the plaintext port, TLS on
// the TLS port, and a plaintext CONNECT on the TLS port is still refused. With a WebSocket
// listener as well, all three ephemeral requests get three distinct ports.
func TestMQTTPlainListenerBesideTLS(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTPlainBesideTLSOptions(m)
	o.MQTT.Websocket = WebsocketOpts{Port: -1}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	plain := testMQTTPlainPort(t, s)
	ws := s.WSAddr().(*net.TCPAddr).Port
	if plain == o.MQTT.Port || plain == ws || ws == o.MQTT.Port {
		t.Fatalf("listener ports MQTT %d, plaintext %d, websocket %d: want three distinct", o.MQTT.Port, plain, ws)
	}

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "plain", cleanSess: true}, o.MQTT.Host, plain)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	if _, ok := c.(*tls.Conn); ok {
		t.Fatal("the plaintext listener's client is a TLS connection")
	}

	tc, tr := testMQTTConnect(t, &mqttConnInfo{clientID: "tls", cleanSess: true, tls: true, tlsc: m.clientCfg}, o.MQTT.Host, o.MQTT.Port)
	defer tc.Close()
	testMQTTCheckConnAck(t, tr, mqttConnAckRCConnectionAccepted, false)

	raw, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, strconv.Itoa(o.MQTT.Port)))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := testMQTTWrite(raw, mqttCreateConnectProto(&mqttConnInfo{clientID: "plain-on-tls", cleanSess: true})); err == nil {
		testMQTTExpectClosed(t, raw, "plaintext CONNECT on the TLS port beside a plaintext listener")
	}
}

// TestMQTTPlainListenerSkipsClientCertificates: with client certificates required, the TLS
// port refuses a client that presents none, and the plaintext port, which never handshakes,
// admits it.
func TestMQTTPlainListenerSkipsClientCertificates(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	m.requireClientCerts()
	o := testMQTTPlainBesideTLSOptions(m)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	tc, err := tls.Dial("tcp", net.JoinHostPort(o.MQTT.Host, strconv.Itoa(o.MQTT.Port)), m.clientCfg)
	if err == nil {
		// TLS 1.3 reports the server's refusal on the first read, not in the handshake.
		_, _ = testMQTTWrite(tc, mqttCreateConnectProto(&mqttConnInfo{clientID: "nocert", cleanSess: true}))
		var buf [16]byte
		_ = tc.SetReadDeadline(time.Now().Add(5 * time.Second))
		if n, rerr := tc.Read(buf[:]); rerr == nil {
			t.Fatalf("the TLS port answered a client without a certificate: %v", buf[:n])
		}
		tc.Close()
	}

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "plain", cleanSess: true}, o.MQTT.Host, testMQTTPlainPort(t, s))
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
}

// TestMQTTPlainListenerBindFailureReleasesEarlierListeners: every listener is bound before
// any accepts, so when the plaintext port is taken Start fails and the MQTT and WebSocket
// ports it had already bound are free again.
func TestMQTTPlainListenerBindFailureReleasesEarlierListeners(t *testing.T) {
	_, nc := runNATS(t)
	free := func() int {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		l.Close()
		return p
	}
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	mqttPort, wsPort := free(), free()

	m := testMQTTTLSMaterial(t)
	o := &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: mqttPort, TLSConfig: m.serverCfg,
		PlainPort: taken.Addr().(*net.TCPAddr).Port, Websocket: WebsocketOpts{Host: "127.0.0.1", Port: wsPort}}}
	s := NewServer(nc, o, "plain-bind-failure")
	if err := s.Start(); err == nil {
		s.Shutdown()
		t.Fatal("Start() = nil with the plaintext port taken")
	}
	for name, p := range map[string]int{"MQTT": mqttPort, "WebSocket": wsPort} {
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(p)))
		if err != nil {
			t.Errorf("the %s port %d is still bound after the failed Start: %v", name, p, err)
			continue
		}
		l.Close()
	}
	s.Shutdown()
}

// TestMQTTPlainListenerClosedByBeginShutdown: BeginShutdown closes the plaintext listener
// before it returns. The asynchronous lifecycle is held at its entry while the port is dialed,
// so a close moved into it would leave the port accepting here whatever the scheduling.
func TestMQTTPlainListenerClosedByBeginShutdown(t *testing.T) {
	m := testMQTTTLSMaterial(t)
	o := testMQTTPlainBesideTLSOptions(m)
	s := testMQTTRunServer(t, o)
	addr := s.PlainAddr().String()

	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	hook := func(srv *Server) {
		if srv == s {
			<-gate
		}
	}
	mqttTestShutdownEntryV.Store(&hook)
	// Registered before BeginShutdown: a failing assertion still releases the worker, and
	// the lifecycle (and with it the hook) has returned before the pointer is cleared.
	t.Cleanup(func() {
		release()
		testMQTTShutdownServer(s)
		<-s.BeginShutdown()
		mqttTestShutdownEntryV.Store(nil)
	})

	s.BeginShutdown()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		c.Close()
		t.Fatalf("the plaintext listener %s accepts after BeginShutdown returned", addr)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial %s after BeginShutdown: %v, want connection refused", addr, err)
	}
	release()
	<-s.BeginShutdown()
}

// TestMQTTPlainListenerSharesMaxConn: MaxConn counts the clients of every listener, in
// either admission order.
func TestMQTTPlainListenerSharesMaxConn(t *testing.T) {
	t.Parallel()
	for _, plainFirst := range []bool{false, true} {
		t.Run("plainFirst="+strconv.FormatBool(plainFirst), func(t *testing.T) {
			m := testMQTTTLSMaterial(t)
			o := testMQTTPlainBesideTLSOptions(m)
			o.MaxConn = 1
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			plain := testMQTTPlainPort(t, s)

			plainInfo := &mqttConnInfo{clientID: "plain", cleanSess: true}
			tlsInfo := &mqttConnInfo{clientID: "tls", cleanSess: true, tls: true, tlsc: m.clientCfg}
			first, firstPort, secondTLS := tlsInfo, o.MQTT.Port, false
			if plainFirst {
				first, firstPort, secondTLS = plainInfo, plain, true
			}
			c, r := testMQTTConnect(t, first, o.MQTT.Host, firstPort)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			if secondTLS {
				tc, err := tls.Dial("tcp", net.JoinHostPort(o.MQTT.Host, strconv.Itoa(o.MQTT.Port)), m.clientCfg)
				if err == nil {
					defer tc.Close()
					if _, err := testMQTTWrite(tc, mqttCreateConnectProto(tlsInfo)); err == nil {
						testMQTTExpectClosed(t, tc, "a TLS client over MaxConn")
					}
				}
				return
			}
			raw, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, strconv.Itoa(plain)))
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			if _, err := testMQTTWrite(raw, mqttCreateConnectProto(plainInfo)); err == nil {
				testMQTTExpectClosed(t, raw, "a plaintext client over MaxConn")
			}
		})
	}
}

// TestMQTTPlainListenerConnectDeadline: a plaintext peer that sends nothing, or only part of
// a CONNECT, is closed by the CONNECT deadline (not the much longer TLS timeout: the plaintext
// listener never handshakes), and the admission slot it held is released.
func TestMQTTPlainListenerConnectDeadline(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTPlainBesideTLSOptions(m)
	o.MaxConn = 1
	o.MQTT.AuthTimeout = 0.5
	o.MQTT.TLSTimeout = 8
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	addr := net.JoinHostPort(o.MQTT.Host, strconv.Itoa(testMQTTPlainPort(t, s)))

	for _, tc := range []struct {
		name string
		send []byte
	}{
		{"silent", nil},
		{"partial CONNECT", mqttCreateConnectProto(&mqttConnInfo{clientID: "partial", cleanSess: true})[:3]},
	} {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if tc.send != nil {
			if _, err := testMQTTWrite(c, tc.send); err != nil {
				t.Fatal(err)
			}
		}
		start := time.Now()
		_ = c.SetReadDeadline(time.Now().Add(6 * time.Second))
		var buf [16]byte
		_, rerr := c.Read(buf[:])
		var ne net.Error
		if rerr == nil || (errors.As(rerr, &ne) && ne.Timeout()) {
			t.Fatalf("%s peer: not closed (%v)", tc.name, rerr)
		}
		if el := time.Since(start); el > 3*time.Second {
			t.Fatalf("%s peer closed after %v: the CONNECT deadline is 0.5s, so it waited on something longer (the TLS sniff?)", tc.name, el)
		}
		c.Close()
	}

	// Both slots were released: under MaxConn 1 a real client is admitted.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := testMQTTWrite(c, mqttCreateConnectProto(&mqttConnInfo{clientID: "after", cleanSess: true})); err == nil {
			if b, err := testMQTTRead(c); err == nil && len(b) > 0 && b[0]&0xF0 == mqttPacketConnectAck {
				c.Close()
				return
			}
		}
		c.Close()
		if time.Now().After(deadline) {
			t.Fatal("no client admitted after the deadline-closed peers: their admission slots were not released")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
