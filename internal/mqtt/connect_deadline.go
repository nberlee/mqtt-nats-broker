package mqtt

import (
	"errors"
	"time"
)

// The CONNECT deadline bounds how long an accepted connection may hold broker
// resources without having identified itself.
//
// A connection that has not sent CONNECT has no client id, no session and no
// subscriptions, so nothing else in the broker can name it, evict it or bound
// it: the keep-alive read deadline is armed from cp.rd AFTER CONNECT, and the
// TLS handshake deadline is cleared the moment the handshake succeeds. Until
// this bound existed such a connection held its socket, its s.clients
// registration, its clientWG token, a readLoop goroutine and a writeLoop
// goroutine for as long as the peer kept the TCP connection open.
//
// MQTT 3.1.1 / 5.0 [3.1.4]: if the Server does not receive a CONNECT Packet
// within a reasonable amount of time after the Network Connection is
// established, the Server SHOULD close the connection.
//
// The bound is unconditional and fail-closed. It is not a DoS defense: a peer
// can still hold roughly (accept rate x window) connections open. It makes an
// idle connection's lifetime finite, which is a different fact; MaxConn remains
// the absolute cap.

// mqttDefaultConnectTimeout bounds, in seconds, how long a connection may hold a
// socket, a registration and a readLoop without having sent a CONNECT. It
// matches config.Default()'s ConnectTimeout so the compiled-in default and the
// configured one state the same number. There is deliberately no value that
// disables the bound.
const mqttDefaultConnectTimeout = 5.0

// mqttConnectTimeout resolves the effective CONNECT bound in seconds. The
// precedence is the inherited one: the MQTT-specific value wins when set,
// otherwise the top-level one, otherwise the default. Resolved at the point of
// use, as mqttTLSTimeout and mqttReceiveMax are, rather than by mutating Options
// at startup.
//
// A zero or negative resolved value falls back to the default rather than
// reaching time.AfterFunc, where it would fire immediately and close every
// connection before its CONNECT could arrive.
//
// The option field is named AuthTimeout because it is upstream's, where the
// timer is armed only when an authentication mechanism is configured. This
// broker arms it unconditionally and names the mechanism after the protocol fact
// it encodes, which is why the two names differ.
func mqttConnectTimeout(o *Options) float64 {
	timeout := o.AuthTimeout
	if o.MQTT.AuthTimeout != 0 {
		timeout = o.MQTT.AuthTimeout
	}
	if timeout <= 0 {
		return mqttDefaultConnectTimeout
	}
	return timeout
}

// mqttConnectAdmission records which of two racing facts won for a connection:
// the CONNECT arrived, or the deadline expired first. Exactly one wins, and the
// transition is made under c.mu.
//
// The state exists because time.Timer.Stop() cannot arbitrate this. Stop
// reports that the timer had already fired; it says nothing about whether the
// callback has already decided to close the connection, and it cannot order
// itself against a callback that is about to.
type mqttConnectAdmission uint8

const (
	// mqttConnectPending is the state of a connection whose CONNECT has not
	// arrived and whose deadline has not expired. Every teardown that is neither
	// of those two facts -- a malformed first packet, a non-CONNECT first packet,
	// a peer hang-up, shutdown -- leaves the state here, because on those paths
	// neither racer won.
	mqttConnectPending mqttConnectAdmission = iota
	mqttConnectAdmitted
	mqttConnectExpired
)

// errMQTTConnectDeadlineExpired is returned by mqttProcessConnect for a CONNECT
// that arrived after the deadline had already condemned the connection. It
// surfaces through mqttParse as an ordinary parse error; the socket is already
// closed by then.
var errMQTTConnectDeadlineExpired = errors.New("CONNECT arrived after the connect deadline expired")

// armConnectDeadline starts the CONNECT deadline for this connection. Called
// from createMQTTClient with c.mu held, after the TLS handshake (so the two
// bounds compose rather than overlap) and before the readLoop goroutine starts
// (so an expired deadline always has a reader that will observe the close).
func (c *client) armConnectDeadline(d time.Duration) {
	c.connectTimer = time.AfterFunc(d, func() { c.connectDeadlineExpired(d) })
}

// mqttAdmitConnectLocked records that this connection's CONNECT won the race,
// and stops the deadline. It returns false only when the deadline already won,
// which is the caller's signal to refuse the CONNECT.
//
// Admission is idempotent: a second call for a connection that already won
// returns true, so "false" means the deadline expired and never means "asked
// twice". c.mu held.
func (c *client) mqttAdmitConnectLocked() bool {
	if c.connectAdmission == mqttConnectExpired {
		return false
	}
	c.connectAdmission = mqttConnectAdmitted
	if c.connectTimer != nil {
		c.connectTimer.Stop()
	}
	return true
}

// mqttExpireConnectLocked records that the deadline won. It returns true only
// for the transition out of pending, so exactly one caller may act on the
// expiry, and a callback that fires after a CONNECT was admitted leaves the
// connection alone. c.mu held.
func (c *client) mqttExpireConnectLocked() bool {
	if c.connectAdmission != mqttConnectPending {
		return false
	}
	c.connectAdmission = mqttConnectExpired
	return true
}

// connectDeadlineExpired is the deadline callback. It closes c.rawConn only,
// never closeConnection: the ordered teardown belongs to readLoop's disconnect(),
// which runs mqttHandleClosedClient (nil-safe pre-CONNECT) before
// closeConnection. Closing the accepted socket also tears down any TLS layer
// above it, which is why shutdownDisconnect has the same shape.
//
// Logged at Debug rather than Warn: an unfinished CONNECT is the ordinary
// behavior of a port scanner or a health probe, and the connection close is the
// observable.
func (c *client) connectDeadlineExpired(d time.Duration) {
	c.mu.Lock()
	// Held here, under c.mu and before the arbitration, a test can pin this
	// callback against a concurrent teardown. Nil in production.
	if pause := c.testConnectDeadlinePause; pause != nil {
		pause()
	}
	expired := c.mqttExpireConnectLocked()
	c.mu.Unlock()
	if !expired {
		return
	}
	c.Debugf("No CONNECT within %v; closing connection", d)
	c.closeTransport()
}
