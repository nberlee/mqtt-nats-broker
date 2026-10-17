//go:build mqttport_fulltests

package mqtt

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
)

// testTLSMaterial mints a CA, a listener certificate for 127.0.0.1/localhost,
// and a client certificate signed by that same CA. The repo ships no certificate
// fixtures, so TLS coverage generates its own in-process.
type testTLSMaterial struct {
	ca        *testcerts.Authority
	serverCfg *tls.Config // listener config (no client auth until requireClientCerts)
	clientCfg *tls.Config // client config trusting the CA
	clientCrt tls.Certificate
}

func testMQTTTLSMaterial(t testing.TB) *testTLSMaterial {
	t.Helper()
	ca, err := testcerts.NewAuthority("mqtt-test-ca")
	if err != nil {
		t.Fatalf("Error creating test CA: %v", err)
	}
	serverPEM, serverKeyPEM, err := ca.Issue("mqtt-server", "127.0.0.1", "localhost")
	if err != nil {
		t.Fatalf("Error issuing server certificate: %v", err)
	}
	serverCrt, err := tls.X509KeyPair(serverPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("Error loading server keypair: %v", err)
	}
	clientPEM, clientKeyPEM, err := ca.Issue("mqtt-client")
	if err != nil {
		t.Fatalf("Error issuing client certificate: %v", err)
	}
	clientCrt, err := tls.X509KeyPair(clientPEM, clientKeyPEM)
	if err != nil {
		t.Fatalf("Error loading client keypair: %v", err)
	}
	return &testTLSMaterial{
		ca: ca,
		serverCfg: &tls.Config{
			Certificates: []tls.Certificate{serverCrt},
			MinVersion:   tls.VersionTLS12,
		},
		clientCfg: &tls.Config{
			RootCAs:    ca.Pool(),
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
		},
		clientCrt: clientCrt,
	}
}

// requireClientCerts turns the listener config into a verifying mTLS config,
// trusting only the CA that issued m.clientCrt.
func (m *testTLSMaterial) requireClientCerts() {
	m.serverCfg.ClientCAs = m.ca.Pool()
	m.serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
}

// TestMQTTTLSHandshake is the core regression test: with a TLS config on the
// listener the connection must actually be upgraded to TLS and then speak MQTT.
// Before the handshake was implemented this listener served plaintext (and
// panicked on the *tls.Conn assertion made after client setup).
func TestMQTTTLSHandshake(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "tlsconn", cleanSess: true, tls: true, tlsc: m.clientCfg}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	if _, ok := c.(*tls.Conn); !ok {
		t.Fatalf("Expected a TLS connection, got %T", c)
	}
	cs := c.(*tls.Conn).ConnectionState()
	if !cs.HandshakeComplete {
		t.Fatal("Expected the TLS handshake to be complete")
	}
}

// TestMQTTTLSRejectsPlaintext proves there is no plaintext fallback on a TLS
// listener: a client that skips the handshake and sends CONNECT in the clear
// gets no CONNACK.
func TestMQTTTLSRejectsPlaintext(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error dialing: %v", err)
	}
	defer c.Close()

	if _, err := testMQTTWrite(c, mqttCreateConnectProto(&mqttConnInfo{clientID: "plain", cleanSess: true})); err != nil {
		// A write failure is an acceptable rejection: the server may have closed
		// the connection as soon as the bytes failed to parse as a TLS record.
		return
	}
	testMQTTExpectClosed(t, c, "plaintext CONNECT on a TLS listener")
}

// testMQTTExpectClosed asserts the server actively closed the connection. It
// deliberately treats a read TIMEOUT as a failure: accepting any read error
// would let a regression that leaves the socket open indefinitely pass on the
// test's own deadline. Only EOF or a reset (i.e. the peer really went away)
// proves the server rejected us.
func testMQTTExpectClosed(t testing.TB, c net.Conn, what string) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("Error setting read deadline: %v", err)
	}
	var buf [128]byte
	n, err := c.Read(buf[:])
	if err == nil {
		t.Fatalf("Expected the server to close the connection (%s), got %v", what, buf[:n])
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("Read timed out waiting for the server to close the connection (%s): "+
			"the socket was left open instead of being rejected", what)
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("Expected EOF or connection reset (%s), got %T %v", what, err, err)
	}
}

// TestMQTTMutualTLS checks that a client certificate signed by the configured
// client CA is accepted.
func TestMQTTMutualTLS(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	m.requireClientCerts()

	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	clientCfg := m.clientCfg.Clone()
	clientCfg.Certificates = []tls.Certificate{m.clientCrt}

	ci := &mqttConnInfo{clientID: "mtlsconn", cleanSess: true, tls: true, tlsc: clientCfg}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
}

// TestMQTTTLSBadCertRejected checks that a client certificate from an untrusted
// CA cannot connect to an mTLS listener.
func TestMQTTTLSBadCertRejected(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	m.requireClientCerts()

	// A second, unrelated CA issues the certificate the client will present.
	rogue, err := testcerts.NewAuthority("rogue-ca")
	if err != nil {
		t.Fatalf("Error creating rogue CA: %v", err)
	}
	roguePEM, rogueKeyPEM, err := rogue.Issue("rogue-client")
	if err != nil {
		t.Fatalf("Error issuing rogue certificate: %v", err)
	}
	rogueCrt, err := tls.X509KeyPair(roguePEM, rogueKeyPEM)
	if err != nil {
		t.Fatalf("Error loading rogue keypair: %v", err)
	}

	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	clientCfg := m.clientCfg.Clone()
	// GetClientCertificate rather than Certificates: the server's
	// CertificateRequest advertises only its own CA, and the Go client would
	// silently send no certificate at all, which would make this test pass
	// without ever exercising chain verification.
	clientCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return &rogueCrt, nil
	}

	ci := &mqttConnInfo{clientID: "rogueconn", cleanSess: true, tls: true, tlsc: clientCfg}
	// The rejection surfaces either as a handshake failure or, under TLS 1.3
	// where the client finishes first, as a failure to read the CONNACK.
	c, _, err := testMQTTConnectRetryWithError(t, ci, o.MQTT.Host, o.MQTT.Port, 0)
	if c != nil {
		defer c.Close()
	}
	if err == nil {
		t.Fatal("Expected the connection to be rejected for an untrusted client certificate")
	}
}

// TestMQTTTLSHandshakeTimeout checks that a client which opens a socket and then
// stalls is dropped once the handshake bound elapses. It uses an explicit short
// timeout: the lower bound catches a regression that closes immediately (which
// would make any "eventually closed" assertion pass vacuously), the upper bound
// is generous enough for -race.
func TestMQTTTLSHandshakeTimeout(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	o.MQTT.TLSTimeout = 0.25
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error dialing: %v", err)
	}
	defer c.Close()

	// Send nothing at all, then wait for the server to give up on us. The closure
	// is proven by the error type (EOF/reset, never our own timeout); the elapsed
	// bounds then check that it happened at the configured deadline rather than
	// immediately.
	start := time.Now()
	testMQTTExpectClosed(t, c, "stalled TLS handshake")
	elapsed := time.Since(start)
	if elapsed < 100*time.Millisecond {
		t.Fatalf("Connection closed after %v, want at least the ~250ms handshake bound "+
			"(an immediate close means the timeout is not being applied)", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Connection still open after %v, want it closed at the handshake bound", elapsed)
	}
}

// TestMQTTTLSTimeoutResolver pins the default handshake bound, which the
// timeout integration test deliberately does not exercise (timing assertions
// around a 2s default are flaky).
func TestMQTTTLSTimeoutResolver(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in, want float64
	}{
		{0, mqttDefaultTLSTimeout},
		{-1, mqttDefaultTLSTimeout},
		{0.25, 0.25},
		{10, 10},
	} {
		if got := mqttTLSTimeout(tc.in); got != tc.want {
			t.Errorf("mqttTLSTimeout(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if mqttDefaultTLSTimeout != 2 {
		t.Errorf("mqttDefaultTLSTimeout = %v, want 2", mqttDefaultTLSTimeout)
	}
}

// TestMQTTTLSStalledHandshakeDoesNotBlockAccept is a denial-of-service
// regression test. The accept loop must not perform the TLS handshake inline: a
// single unauthenticated client that connects and then sends nothing would
// otherwise stall every subsequent accept for the whole handshake timeout, and a
// handful of idle sockets would keep the listener unavailable indefinitely.
// MaxConn is no defense: it bounds how many clients are registered, not how long
// one of them can hold up the accept loop (and it is unlimited by default).
func TestMQTTTLSStalledHandshakeDoesNotBlockAccept(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	// A long handshake bound makes the window unambiguous: if the accept loop
	// serializes handshakes, the healthy client below cannot connect for 5s.
	o.MQTT.TLSTimeout = 5
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))

	// Stall several sockets: connected, but never sending a ClientHello.
	for i := 0; i < 3; i++ {
		stalled, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Error dialing stalled connection %d: %v", i, err)
		}
		defer stalled.Close()
	}

	// A healthy client must still get through promptly.
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		ci := &mqttConnInfo{clientID: "healthy", cleanSess: true, tls: true, tlsc: m.clientCfg}
		c, r, err := testMQTTConnectRetryWithError(t, ci, o.MQTT.Host, o.MQTT.Port, 0)
		if err != nil {
			close(done)
			return
		}
		defer c.Close()
		if b, _ := testMQTTReadPacket(t, r); b&mqttPacketMask != mqttPacketConnectAck {
			close(done)
			return
		}
		done <- time.Since(start)
	}()

	select {
	case el, ok := <-done:
		if !ok {
			t.Fatal("Healthy TLS client failed to connect while other handshakes were pending")
		}
		t.Logf("healthy client connected in %v with 3 stalled handshakes pending", el)
	case <-time.After(3 * time.Second):
		t.Fatal("Healthy TLS client could not connect within 3s while stalled handshakes were " +
			"pending: the accept loop is serializing TLS handshakes (remote listener starvation)")
	}
}

// TestMQTTTLSShutdownInterruptsParkedHandshake verifies that shutdown actively
// interrupts a client parked in its TLS handshake rather than waiting it out.
// Registration precedes the handshake, so such a connection is a registered
// client: drainClients must close its socket, which is what makes the handshake
// return. The handshake bound is set far beyond the assertion window and the test
// closes nothing itself, so prompt completion can only come from the drain.
//
// With the connection-setup wait ordered before the drain, this deadlocks until
// the handshake deadline instead — the drain cannot reach the socket it needs to
// close.
func TestMQTTTLSShutdownInterruptsParkedHandshake(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	// Far longer than the assertion window below: if shutdown only completes
	// because the handshake expired, the test fails.
	o.MQTT.TLSTimeout = 60
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	// An ordinary stalled socket: connected, never sends a ClientHello.
	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))
	stalled, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error dialing: %v", err)
	}
	defer stalled.Close()

	// Wait until the server has registered it, i.e. setup has reached the
	// handshake and is parked there.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		n := len(s.clients)
		s.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Stalled connection never registered (clients=%d)", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		<-s.BeginShutdown()
		close(done)
	}()

	select {
	case <-done:
		t.Logf("shutdown completed in %v, interrupting the parked handshake", time.Since(start))
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not complete within 10s while a client was parked in its TLS " +
			"handshake: the drain never closed that socket, so shutdown is waiting out the " +
			"handshake deadline instead of interrupting it")
	}
}

// TestMQTTTLSHandshakeCompletionRacesShutdown overlaps successful handshake
// completion with the drain. The handshake replaces the client's connection with
// the TLS wrapper, while the drain closes that client's socket from another
// goroutine; both must be safe. Run under -race, where an unsynchronized
// read/write of the connection field is reported.
func TestMQTTTLSHandshakeCompletionRacesShutdown(t *testing.T) {
	t.Parallel()
	m := testMQTTTLSMaterial(t)
	o := testMQTTDefaultOptionsV5()
	o.MQTT.TLSConfig = m.serverCfg
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port))

	// Keep handshakes completing continuously so some land inside the drain.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c, err := net.DialTimeout("tcp", addr, time.Second)
				if err != nil {
					return // listener closed: shutdown is done
				}
				tc := tls.Client(c, m.clientCfg)
				_ = tc.Handshake()
				tc.Close()
			}
		}()
	}

	// Let the churn get going, then drain underneath it.
	time.Sleep(100 * time.Millisecond)
	<-s.BeginShutdown()
	close(stop)
	wg.Wait()
}
