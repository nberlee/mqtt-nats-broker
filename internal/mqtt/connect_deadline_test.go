package mqtt

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests are deliberately untagged: the CONNECT deadline is race-shaped
// (an AfterFunc goroutine against the readLoop), so it belongs in the default
// tree that `make test` runs under -race, not in the tagged suite that does not.

// startConnectDeadlineBroker brings up a broker whose Options the caller tunes.
// The window is per server rather than a package-level var, which is what keeps
// every test here parallel.
func startConnectDeadlineBroker(t *testing.T, tune func(*Options)) (*Server, string) {
	t.Helper()
	_, nc := runNATS(t)
	opts := &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}
	if tune != nil {
		tune(opts)
	}
	s := NewServer(nc, opts, "connect-deadline-node")
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	addr := s.Addr()
	if addr == nil {
		t.Fatal("no listener address")
	}
	return s, addr.String()
}

// dialSilent opens a TCP connection to the MQTT listener and sends nothing.
// Every caller is testing a connection that never identified itself, so the
// absent CONNECT is the point.
func dialSilent(t testing.TB, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// testConnectPacket builds a minimal MQTT 3.1.1 CONNECT (clean session, no
// credentials, no Will). Hand-built rather than borrowed from the upstream suite's
// helpers, which live behind the mqtt_fulltests tag.
func testConnectPacket(clientID string, keepAlive uint16) []byte {
	body := newMQTTWriter(0)
	body.WriteBytes(mqttProtoName)
	body.WriteByte(mqttProtoLevel)
	body.WriteByte(mqttConnFlagCleanSession)
	body.WriteUint16(keepAlive)
	body.WriteString(clientID)

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(body.Len())
	w.Write(body.Bytes())
	return w.Bytes()
}

// testPublishQoS1Packet builds an MQTT 3.1.1 QoS 1 PUBLISH.
func testPublishQoS1Packet(topic string, pi uint16, payload string) []byte {
	body := newMQTTWriter(0)
	body.WriteString(topic)
	body.WriteUint16(pi)
	body.Write([]byte(payload))

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub | 0x2) // QoS 1
	w.WriteVarInt(body.Len())
	w.Write(body.Bytes())
	return w.Bytes()
}

// expectPacket reads one packet off the socket and requires its type. Used where
// the assertion is that the connection is still serving, so the packet's body
// does not matter.
func expectPacket(t testing.TB, c net.Conn, want byte) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [64]byte
	n, err := c.Read(buf[:])
	if err != nil {
		t.Fatalf("reading packet 0x%02x: %v", want, err)
	}
	if n < 1 {
		t.Fatalf("empty read waiting for packet 0x%02x", want)
	}
	if pt := buf[0] & mqttPacketMask; pt != want {
		t.Fatalf("expected packet 0x%02x, got 0x%02x", want, pt)
	}
	_ = c.SetReadDeadline(time.Time{})
}

// expectConnAckAccepted reads one CONNACK off the socket and requires a success
// return code.
func expectConnAckAccepted(t testing.TB, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [8]byte
	n, err := c.Read(buf[:4])
	if err != nil {
		t.Fatalf("reading CONNACK: %v", err)
	}
	if n < 4 {
		t.Fatalf("short CONNACK: %d bytes", n)
	}
	if pt := buf[0] & mqttPacketMask; pt != mqttPacketConnectAck {
		t.Fatalf("expected CONNACK (%x), got %x", mqttPacketConnectAck, pt)
	}
	if rc := buf[3]; rc != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNACK return code %x, want %x", rc, mqttConnAckRCConnectionAccepted)
	}
	_ = c.SetReadDeadline(time.Time{})
}

// expectClosedBetween asserts the broker closed the connection, and that it did
// not close before min. The lower bound is the load-bearing half: an
// upper-bound-only assertion passes against a window resolved in the wrong unit
// and against an AfterFunc(0) that fires before any peer could have sent a
// CONNECT.
func expectClosedBetween(t testing.TB, c net.Conn, min, max time.Duration) {
	t.Helper()
	start := time.Now()
	_ = c.SetReadDeadline(time.Now().Add(max))
	var buf [64]byte
	for {
		n, err := c.Read(buf[:])
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("connection still open after %v: the CONNECT deadline did not close it", time.Since(start))
			}
			break
		}
		if n == 0 {
			break
		}
		// Trailing bytes before the close: keep reading until the socket ends.
	}
	if el := time.Since(start); el < min {
		t.Fatalf("connection closed after %v, inside the %v the peer was owed to send its CONNECT", el, min)
	}
}

// TestMQTTConnectDeadlineClosesSilentConnection is the base case: a peer that
// opens a socket and sends nothing is closed once the window elapses. Before this
// bound existed such a connection held its socket, its registration, its clientWG
// token and both goroutines until the peer hung up.
func TestMQTTConnectDeadlineClosesSilentConnection(t *testing.T) {
	t.Parallel()
	_, addr := startConnectDeadlineBroker(t, func(o *Options) { o.MQTT.AuthTimeout = 0.3 })
	expectClosedBetween(t, dialSilent(t, addr), 200*time.Millisecond, 5*time.Second)
}

// TestMQTTConnectDeadlineClosesPartialConnect pins that the window is absolute
// rather than restarted by each byte received. A CONNECT whose fixed header
// arrives but whose body never completes leaves mqttParse buffering with no error
// and no read deadline of its own -- cp is nil, so the keep-alive rd is 0 -- which
// is the shape a per-read bound would miss.
func TestMQTTConnectDeadlineClosesPartialConnect(t *testing.T) {
	t.Parallel()
	_, addr := startConnectDeadlineBroker(t, func(o *Options) { o.MQTT.AuthTimeout = 0.3 })
	c := dialSilent(t, addr)

	proto := testConnectPacket("partial-connect", 0)
	if len(proto) <= 3 {
		t.Fatalf("CONNECT is %d bytes; the test needs a fixed header plus a body byte", len(proto))
	}
	// The fixed header (type + one-byte remaining length) and a single body byte,
	// then nothing more.
	if _, err := c.Write(proto[:3]); err != nil {
		t.Fatalf("writing partial CONNECT: %v", err)
	}
	expectClosedBetween(t, c, 200*time.Millisecond, 5*time.Second)
}

// TestMQTTConnectDeadlineDefaultWindow is the only test that exercises the
// default through the arming call site: neither Options.AuthTimeout nor
// MQTT.AuthTimeout is set, so the arm must go through mqttConnectTimeout. A call
// site that passed the raw option straight to time.AfterFunc would arm a zero
// window and close this connection before its CONNECT.
func TestMQTTConnectDeadlineDefaultWindow(t *testing.T) {
	t.Parallel()
	s, addr := startConnectDeadlineBroker(t, nil)
	o := s.getOpts()
	if o.AuthTimeout != 0 || o.MQTT.AuthTimeout != 0 {
		t.Fatalf("this test needs both windows unset, got %v / %v", o.AuthTimeout, o.MQTT.AuthTimeout)
	}
	c := dialSilent(t, addr)

	// Far outside a zero window, well inside mqttDefaultConnectTimeout.
	time.Sleep(time.Second)

	if _, err := c.Write(testConnectPacket("default-window", 0)); err != nil {
		t.Fatalf("writing CONNECT after 1s: %v", err)
	}
	expectConnAckAccepted(t, c)
}

// TestMQTTConnectDeadlineSparesConnectedClient is the admission side: once a
// CONNECT is accepted the deadline must never close the connection, even for a
// client that negotiated Keep Alive 0 and is therefore bounded by nothing
// afterwards. Driven well past the window, so a callback that ignored the
// admission would already have fired.
//
// Deliberately raw rather than driven by a client library. A paho client
// reconnects on its own by default, and `IsConnectionOpen` reports whatever
// connection it currently holds — so a deadline that closed the admitted socket
// could be masked by a replacement, and this test is the only witness for M3.
// A socket the test owns cannot be substituted underneath the assertion.
func TestMQTTConnectDeadlineSparesConnectedClient(t *testing.T) {
	t.Parallel()
	_, addr := startConnectDeadlineBroker(t, func(o *Options) { o.MQTT.AuthTimeout = 0.3 })

	c := dialSilent(t, addr)
	if _, err := c.Write(testConnectPacket("connect-deadline-spared", 0)); err != nil {
		t.Fatalf("writing CONNECT: %v", err)
	}
	expectConnAckAccepted(t, c)

	// More than three windows after the CONNECT was admitted.
	time.Sleep(time.Second)

	// PINGREQ on the same socket: a connection the deadline closed cannot answer.
	if _, err := c.Write([]byte{mqttPacketPing, 0}); err != nil {
		t.Fatalf("writing PINGREQ after the window: %v", err)
	}
	expectPacket(t, c, mqttPacketPingResp)

	// And the session still works, not just the socket.
	if _, err := c.Write(testPublishQoS1Packet("connect/deadline/spared", 1, "msg")); err != nil {
		t.Fatalf("writing PUBLISH after the window: %v", err)
	}
	expectPacket(t, c, mqttPacketPubAck)
}

// awaitClients waits until the server has registered n connections and returns
// them. A pre-CONNECT connection has no client id, so s.clients is the only
// handle a test has on one.
func awaitClients(t testing.TB, s *Server, n int) []*client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		out := make([]*client, 0, len(s.clients))
		for _, c := range s.clients {
			out = append(out, c)
		}
		s.mu.Unlock()
		if len(out) >= n {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d connections registered", len(out), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMQTTConnectDeadlineRacesShutdown puts a deadline callback and the shutdown
// drain in flight at the same time, which is the interaction the design reasons
// about: closeConnection stops the timer under c.mu while a callback that has
// already fired is blocked on that same lock.
//
// Every step of the overlap is forced, none of it timed. The callback is parked
// by c.testConnectDeadlinePause, which runs inside c.mu before the arbitration,
// so "the callback has fired and is holding the lock" is a fact the test observes
// rather than infers from a sleep -- time.AfterFunc gives no upper bound on when
// its goroutine starts, and sync.Mutex grants waiters no ordering. That the drain
// reached this connection is likewise observed, as EOF on the socket, before the
// callback is released.
//
// It waits on BeginShutdown's completion channel, not on Shutdown(), which
// returns after its own bound whether or not the lifecycle finished.
func TestMQTTConnectDeadlineRacesShutdown(t *testing.T) {
	t.Parallel()
	// A long window: this callback is released by the test's lever, not by the
	// clock, and the second connection's deadline must not fire on its own.
	s, addr := startConnectDeadlineBroker(t, func(o *Options) { o.MQTT.AuthTimeout = 5 })

	held := dialSilent(t, addr)
	hc := awaitClients(t, s, 1)[0]

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCallback := func() { releaseOnce.Do(func() { close(release) }) }
	// A parked callback must never outlive this test.
	t.Cleanup(releaseCallback)

	hc.mu.Lock()
	hc.testConnectDeadlinePause = func() {
		close(entered)
		<-release
	}
	// Fire this connection's deadline now. Reset re-arms the same AfterFunc, so
	// what parks at the lever is the production callback on its real path.
	hc.connectTimer.Reset(time.Millisecond)
	hc.mu.Unlock()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the deadline callback never reached the pause lever")
	}
	// The callback now holds c.mu and has not arbitrated.

	// A second connection whose deadline is still armed: its window is 5s and it
	// was dialed just now, so nothing has fired for it.
	armed := dialSilent(t, addr)
	var ac *client
	for _, c := range awaitClients(t, s, 2) {
		if c.cid != hc.cid {
			ac = c
		}
	}
	if ac == nil {
		t.Fatal("the second connection did not register")
	}
	ac.mu.Lock()
	stillArmed := ac.connectAdmission == mqttConnectPending && ac.connectTimer != nil
	ac.mu.Unlock()
	if !stillArmed {
		t.Fatal("the second connection's deadline must still be armed when shutdown starts, or the drain never meets an armed timer")
	}

	done := s.BeginShutdown()

	// The drain has reached this connection: only shutdownDisconnect can have
	// closed it, since its own callback is parked and has closed nothing. From
	// here closeConnection is contending for the c.mu the callback holds.
	expectClosedBetween(t, held, 0, 30*time.Second)

	releaseCallback()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("the shutdown lifecycle did not complete with a deadline callback in flight")
	}

	// The parked callback really ran, and really won its arbitration -- which is
	// what makes this test a witness for the callback-versus-teardown interaction
	// rather than for shutdown alone.
	hc.mu.Lock()
	adm := hc.connectAdmission
	hc.mu.Unlock()
	if adm != mqttConnectExpired {
		t.Fatalf("the held connection's callback must have won the arbitration, got admission %d", adm)
	}

	expectClosedBetween(t, armed, 0, 10*time.Second)
}

// TestMQTTConnectTimeoutResolution covers the precedence directly, including the
// values that must never reach time.AfterFunc.
func TestMQTTConnectTimeoutResolution(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		top  float64
		mqtt float64
		want float64
	}{
		{"both unset", 0, 0, mqttDefaultConnectTimeout},
		{"top-level only", 7, 0, 7},
		{"mqtt wins over top-level", 7, 3, 3},
		{"mqtt only", 0, 3, 3},
		{"negative mqtt falls back", 7, -1, mqttDefaultConnectTimeout},
		{"negative top-level falls back", -1, 0, mqttDefaultConnectTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			o := &Options{AuthTimeout: test.top, MQTT: MQTTOpts{AuthTimeout: test.mqtt}}
			if got := mqttConnectTimeout(o); got != test.want {
				t.Fatalf("mqttConnectTimeout(top=%v, mqtt=%v) = %v, want %v", test.top, test.mqtt, got, test.want)
			}
		})
	}
	if mqttDefaultConnectTimeout <= 0 {
		t.Fatalf("mqttDefaultConnectTimeout = %v; the bound is fail-closed and must be positive", mqttDefaultConnectTimeout)
	}
}

// TestMQTTConnectAdmissionArbitration drives the arbitration in both orders with
// no server, no timer and no wall clock, which is what makes it the witness the
// timing-based tests cannot be. Two of the three cases call the deadline callback
// itself rather than only the locked helper, so a callback that closes without
// consulting the admission state is caught here.
func TestMQTTConnectAdmissionArbitration(t *testing.T) {
	t.Parallel()

	// The callback needs no server: c.clog() and c.String() are both nil-safe
	// with srv nil, so it runs unmodified against a bare client.
	newPipedClient := func(t *testing.T) (*client, net.Conn) {
		broker, peer := net.Pipe()
		t.Cleanup(func() { broker.Close(); peer.Close() })
		return &client{rawConn: broker}, peer
	}
	// net.Pipe delivers EOF to the peer when its counterpart is closed, so a read
	// that times out means the broker side is still open.
	pipeClosed := func(peer net.Conn) bool {
		_ = peer.SetReadDeadline(time.Now().Add(time.Second))
		var buf [1]byte
		_, err := peer.Read(buf[:])
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return false
		}
		return err != nil
	}

	t.Run("admitted then expired", func(t *testing.T) {
		t.Parallel()
		c, _ := newPipedClient(t)
		c.mu.Lock()
		admitted := c.mqttAdmitConnectLocked()
		expired := c.mqttExpireConnectLocked()
		c.mu.Unlock()
		if !admitted {
			t.Fatal("the CONNECT was not admitted")
		}
		if expired {
			t.Fatal("the deadline expired a connection whose CONNECT had already been admitted")
		}
	})

	t.Run("deadline first, then CONNECT", func(t *testing.T) {
		t.Parallel()
		c, peer := newPipedClient(t)
		c.connectDeadlineExpired(300 * time.Millisecond)
		if !pipeClosed(peer) {
			t.Fatal("the deadline won but did not close the socket")
		}
		c.mu.Lock()
		admitted := c.mqttAdmitConnectLocked()
		c.mu.Unlock()
		if admitted {
			t.Fatal("a CONNECT was admitted after the deadline had already condemned the connection")
		}
	})

	// The clock arms (F-074). A deadline that was never armed has nothing to compare, so a
	// bare fixture is admitted; a recorded deadline already passed refuses even though no
	// callback ran, and makes the expiry transition itself; one still ahead admits.
	t.Run("never armed is admitted", func(t *testing.T) {
		t.Parallel()
		c, _ := newPipedClient(t)
		c.mu.Lock()
		admitted := c.mqttAdmitConnectLocked()
		c.mu.Unlock()
		if !admitted {
			t.Fatal("a connection whose deadline was never armed was refused: a zero deadline is not a passed one")
		}
	})

	t.Run("window elapsed by the clock, callback not run", func(t *testing.T) {
		t.Parallel()
		c, _ := newPipedClient(t)
		c.mu.Lock()
		c.connectDeadline = time.Now().Add(-time.Millisecond)
		admitted := c.mqttAdmitConnectLocked()
		adm := c.connectAdmission
		expiredAgain := c.mqttExpireConnectLocked()
		c.mu.Unlock()
		if admitted {
			t.Fatal("a CONNECT was admitted after the window had elapsed, because the callback had not run")
		}
		if adm != mqttConnectExpired {
			t.Fatalf("a clock refusal must make the expiry transition itself, got admission %d", adm)
		}
		if expiredAgain {
			t.Fatal("the late callback would expire the connection a second time")
		}
	})

	t.Run("window still open by the clock", func(t *testing.T) {
		t.Parallel()
		c, _ := newPipedClient(t)
		c.mu.Lock()
		c.connectDeadline = time.Now().Add(time.Hour)
		admitted := c.mqttAdmitConnectLocked()
		c.mu.Unlock()
		if !admitted {
			t.Fatal("a CONNECT inside the window was refused")
		}
	})

	t.Run("CONNECT first, then deadline", func(t *testing.T) {
		t.Parallel()
		c, peer := newPipedClient(t)
		c.mu.Lock()
		admitted := c.mqttAdmitConnectLocked()
		c.mu.Unlock()
		if !admitted {
			t.Fatal("the CONNECT was not admitted")
		}
		c.connectDeadlineExpired(300 * time.Millisecond)
		if pipeClosed(peer) {
			t.Fatal("a late deadline callback closed a connection whose CONNECT had been admitted")
		}
	})
}

// testConnectPacketWithCredentials is testConnectPacket with a user name and password:
// the CONNECT a client under auth passthrough sends.
func testConnectPacketWithCredentials(clientID, user, pass string) []byte {
	body := newMQTTWriter(0)
	body.WriteBytes(mqttProtoName)
	body.WriteByte(mqttProtoLevel)
	body.WriteByte(mqttConnFlagCleanSession | mqttConnFlagUsernameFlag | mqttConnFlagPasswordFlag)
	body.WriteUint16(30)
	body.WriteString(clientID)
	body.WriteString(user)
	body.WriteBytes([]byte(pass))

	pkt := newMQTTWriter(0)
	pkt.WriteByte(mqttPacketConnect)
	pkt.WriteVarInt(body.Len())
	pkt.Write(body.Bytes())
	return pkt.Bytes()
}

// parkDeadlineCallbacks installs a hook on s that parks every CONNECT deadline callback
// BEFORE it takes c.mu, which is how a late callback looks to the CONNECT path. Installed
// before the test dials, so no timer can fire ahead of it. entered receives each parked
// client; closing the returned release lets them all go, and the cleanup closes it too, so
// no callback outlives the test.
func parkDeadlineCallbacks(t *testing.T, s *Server) (entered <-chan *client, release func()) {
	t.Helper()
	ch := make(chan *client, 4)
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	hook := func(c *client) {
		ch <- c
		<-gate
	}
	s.testConnectDeadlineBeforeLock.Store(&hook)
	t.Cleanup(func() { s.testConnectDeadlineBeforeLock.Store(nil) })
	return ch, release
}

// expectClosedWithoutBytes requires the broker to close c without sending anything: EOF
// or a reset, never a read timeout, and never a byte first (a CONNACK would be one).
func expectClosedWithoutBytes(t testing.TB, c net.Conn, what string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [16]byte
	n, err := c.Read(buf[:])
	if n > 0 {
		t.Fatalf("%s: the broker sent % x before closing; nothing was owed", what, buf[:n])
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("%s: the connection is still open", what)
	}
	if err == nil {
		t.Fatalf("%s: empty read without an error", what)
	}
}

// awaitNoClients waits until s has no registered connection: the ordered teardown ran.
func awaitNoClients(t testing.TB, s *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		n := len(s.clients)
		s.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d connection(s) still registered: the refused connection was not torn down", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMQTTConnectRefusedOnceTheWindowElapsedBeforeTheCallbackRan is F-074's witness: the
// deadline's callback is LATE -- parked before it takes c.mu, so the CONNECT path cannot
// know -- and a CONNECT arriving after the window must still be refused. Before the fix
// admission only asked whether the callback had already run, so this CONNECT was served.
//
// The refused connection gets no CONNACK (it lost to the deadline before authenticating),
// dials no per-client NATS connection, is closed, and is torn down. The control, on a
// second broker with a 5 s window, parks its callback the same way, fired early by
// Timer.Reset so the recorded deadline is still ahead: that CONNECT is admitted and the
// connection keeps working after the callback is released.
func TestMQTTConnectRefusedOnceTheWindowElapsedBeforeTheCallbackRan(t *testing.T) {
	t.Parallel()

	// Every CONNECT after the window is owed nothing, including the ones the parser itself
	// would refuse with a CONNACK: an unsupported protocol level (3.1.1 return code 0x01)
	// and a malformed 5.0 CONNECT (reserved flag set, which 5.0 answers with a reason code).
	badLevel := testConnectPacket("late-level", 30)
	badLevel[2+2+len(mqttProtoName)] = 9 // fixed header (2), name length (2), name, level
	malformedV5 := testConnectPacketV5WithCredentials("late-v5", "alice", "pw")
	malformedV5[2+2+len(mqttProtoName)+1] |= 0x01 // the reserved CONNECT flag
	for _, late := range []struct {
		name   string
		packet []byte
	}{
		{"a valid CONNECT", testConnectPacketWithCredentials("late", "alice", "pw")},
		{"a CONNECT the parser refuses with a return code", badLevel},
		{"a malformed 5.0 CONNECT", malformedV5},
	} {
		t.Run("after the window: "+late.name, func(t *testing.T) {
			t.Parallel()
			var dials atomic.Int64
			s, addr := startAuthBrokerTuned(t, func(o *Options) { o.MQTT.AuthTimeout = 0.1 }, &dials)
			entered, release := parkDeadlineCallbacks(t, s)

			conn := dialSilent(t, addr)
			var parked *client
			select {
			case parked = <-entered:
			case <-time.After(30 * time.Second):
				t.Fatal("the deadline callback never fired")
			}
			parked.mu.Lock()
			deadline := parked.connectDeadline
			parked.mu.Unlock()
			if deadline.IsZero() {
				t.Fatal("the armed deadline was not recorded")
			}
			if wait := time.Until(deadline); wait > 0 {
				time.Sleep(wait)
			}
			time.Sleep(10 * time.Millisecond)

			if _, err := conn.Write(late.packet); err != nil {
				t.Fatalf("writing CONNECT: %v", err)
			}
			expectClosedWithoutBytes(t, conn, "a CONNECT after the window, with the callback late")
			awaitNoClients(t, s)
			parked.mu.Lock()
			adm := parked.connectAdmission
			parked.mu.Unlock()
			if adm != mqttConnectExpired {
				t.Fatalf("admission state %d, want expired", adm)
			}
			if n := dials.Load(); n != 0 {
				t.Fatalf("the late CONNECT dialed %d per-client NATS connection(s); it must be refused first", n)
			}
			release()
		})
	}

	t.Run("inside the window, callback parked (control)", func(t *testing.T) {
		t.Parallel()
		s, addr := startAuthBrokerTuned(t, func(o *Options) { o.MQTT.AuthTimeout = 5 }, nil)
		entered, release := parkDeadlineCallbacks(t, s)

		conn := dialSilent(t, addr)
		hc := awaitClients(t, s, 1)[0]
		hc.mu.Lock()
		hc.connectTimer.Reset(time.Millisecond)
		hc.mu.Unlock()
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
			t.Fatal("the deadline callback never reached the hook")
		}
		if _, err := conn.Write(testConnectPacketWithCredentials("early", "alice", "pw")); err != nil {
			t.Fatalf("writing CONNECT: %v", err)
		}
		expectConnAckAccepted(t, conn)
		release()
		if _, err := conn.Write(testPublishQoS1Packet("control/topic", 1, "x")); err != nil {
			t.Fatalf("writing PUBLISH: %v", err)
		}
		expectPacket(t, conn, mqttPacketPubAck)
	})
}

// TestMQTTConnectDeadlineUnderLoad interleaves both outcomes on one server so the
// field accesses are exercised under -race: half the connections are admitted and
// must survive the window, half stay silent and must be closed by it. It then
// shuts down and requires the admitted half to drain.
//
// It is deliberately NOT the witness for the callback-versus-shutdown race: it
// waits for every silent connection to have been closed before shutting down, so
// by that point no timer is still armed. TestMQTTConnectDeadlineRacesShutdown
// covers that.
func TestMQTTConnectDeadlineUnderLoad(t *testing.T) {
	t.Parallel()
	s, addr := startConnectDeadlineBroker(t, func(o *Options) { o.MQTT.AuthTimeout = 0.3 })

	const pairs = 8
	var wg sync.WaitGroup
	for i := 0; i < pairs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", addr)
			if err != nil {
				t.Errorf("silent dial %d: %v", i, err)
				return
			}
			defer c.Close()
			expectClosedBetween(t, c, 200*time.Millisecond, 10*time.Second)
		}(i)
	}

	admitted := make([]net.Conn, 0, pairs)
	for i := 0; i < pairs; i++ {
		c := dialSilent(t, addr)
		if _, err := c.Write(testConnectPacket(fmt.Sprintf("connect-deadline-load-%d", i), 0)); err != nil {
			t.Fatalf("writing CONNECT %d: %v", i, err)
		}
		expectConnAckAccepted(t, c)
		admitted = append(admitted, c)
	}
	wg.Wait()

	// Every admitted connection outlived the window the silent ones died on, and
	// is still there to be torn down by the shutdown drain.
	s.Shutdown()
	for i, c := range admitted {
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		var buf [64]byte
		for {
			n, err := c.Read(buf[:])
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					t.Fatalf("admitted connection %d was not closed by shutdown", i)
				}
				break
			}
			if n == 0 {
				break
			}
		}
	}
}
