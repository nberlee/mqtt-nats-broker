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

// Connection lifecycle: starting the listener, accepting a client connection, validating the
// MQTT options, and handling a closed client.

package mqtt

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"time"
)

func (s *Server) startMQTT() {
	if s.isShuttingDown() {
		return
	}

	sopts := s.getOpts()
	o := &sopts.MQTT

	var hl net.Listener
	var err error

	port := o.Port
	if port == -1 {
		port = 0
	}
	hp := net.JoinHostPort(o.Host, strconv.Itoa(port))
	s.mu.Lock()
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	// Per-server secret keying the No Local origin HMAC. Best-effort: if the read
	// fails the key stays nil and No Local simply never suppresses (fail-open).
	if s.mqtt.originKey == nil {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err == nil {
			s.mqtt.originKey = key
		}
	}
	hl, err = natsListen("tcp", hp)
	s.mqtt.listenerErr = err
	if err != nil {
		s.mu.Unlock()
		s.Fatalf("Unable to listen for MQTT connections: %v", err)
		return
	}
	if port == 0 {
		o.Port = hl.Addr().(*net.TCPAddr).Port
	}
	s.mqtt.listener = hl
	scheme := "mqtt"
	if o.TLSConfig != nil {
		scheme = "tls"
	}
	s.Noticef("Listening for MQTT clients on %s://%s:%d", scheme, o.Host, o.Port)
	go s.acceptConnections(hl, "MQTT", func(conn net.Conn) { s.createMQTTClient(conn) }, nil)
	s.mu.Unlock()
}

// This is similar to createClient() but has some modifications specifi to MQTT clients.
// The comments have been kept to minimum to reduce code size. Check createClient() for
// more details.
func (s *Server) createMQTTClient(conn net.Conn) *client {
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	now := time.Now()

	mqtt := &mqtt{
		experimentalSharedQoS2: opts.MQTT.ExperimentalSharedQoS2,
		// The abort latch, made before the client exists anywhere another goroutine can
		// close it (inbound_pipeline.go).
		inAbort: make(chan struct{}),
	}
	c := &client{srv: s, nc: conn, rawConn: conn, mpay: maxPay, start: now, last: now, mqtt: mqtt}
	c.headers = true
	c.mqtt.pp = &mqttPublish{}
	// MQTT clients don't send NATS CONNECT protocols. So make it an "echo"
	// client, but disable verbose and pedantic (by not setting them).
	c.opts.Echo = true

	c.registerWithAccount(s.globalAccount())

	s.mu.Lock()
	authRequired := s.info.AuthRequired
	s.totalClients++
	s.mu.Unlock()

	c.mu.Lock()
	if authRequired {
		c.flags.set(expectConnect)
	}
	c.initClient()
	c.Debugf("Client connection created")
	c.mu.Unlock()

	s.mu.Lock()
	if !s.isRunning() || s.ldm {
		if s.isShuttingDown() {
			conn.Close()
		}
		s.mu.Unlock()
		return c
	}

	if opts.MaxConn < 0 || (opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn) {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c
	// Reserve the shutdown-drain lifecycle token under the same s.mu that makes
	// the client visible to a Shutdown snapshot; released via c.finish() from
	// readLoop's defer or abortSetup. Every early return below goes through
	// abortSetup so the token is never leaked.
	s.clientWG.Add(1)

	tlsRequired := opts.MQTT.TLSConfig != nil
	s.mu.Unlock()

	c.mu.Lock()

	// In case connection has already been closed
	if c.isClosed() {
		c.mu.Unlock()
		c.abortSetup(WriteError)
		return nil
	}

	var pre []byte
	if tlsRequired && opts.AllowNonTLS {
		pre = make([]byte, 4)
		if derr := c.nc.SetReadDeadline(time.Now().Add(secondsToDuration(mqttTLSTimeout(opts.MQTT.TLSTimeout)))); derr != nil {
			// The peek is bounded by this deadline alone: without it a silent peer would hold
			// this goroutine for as long as it kept the socket open. A connection that refuses
			// a deadline is unusable, so the setup ends here.
			c.mu.Unlock()
			c.abortSetup(ReadError)
			return nil
		}
		n, _ := io.ReadFull(c.nc, pre[:])
		// A failed clear is superseded before the next read: the TLS handshake and the
		// pre-CONNECT bound below each arm their own deadline first.
		_ = c.nc.SetReadDeadline(time.Time{})
		pre = pre[:n]
		if n > 0 && pre[0] == 0x16 {
			tlsRequired = true
		} else {
			tlsRequired = false
		}
	}

	if tlsRequired {
		if len(pre) > 0 {
			c.nc = &tlsMixConn{c.nc, bytes.NewBuffer(pre)}
			pre = nil
		}

		// Perform server-side TLS handshake.
		if err := c.doTLSServerHandshake(tlsHandshakeMQTT, opts.MQTT.TLSConfig, opts.MQTT.TLSTimeout, opts.MQTT.TLSPinnedCerts); err != nil {
			c.mu.Unlock()
			c.abortSetup(TLSHandshakeError)
			return nil
		}
	}

	// A connection that has not sent CONNECT has no identity and no session, and
	// the keep-alive read deadline that bounds a connected client is armed only
	// from cp.rd, after CONNECT. Without this bound such a connection holds its
	// socket, its s.clients registration, its clientWG token and its readLoop for
	// as long as the peer keeps the TCP connection open. MQTT 3.1.1 / 5.0 [3.1.4]:
	// the Server SHOULD close a Network Connection that does not deliver a CONNECT
	// in reasonable time. Unconditional, because the cost is the same whether or
	// not an authentication mechanism is configured. Armed after the TLS handshake
	// so the two bounds compose rather than overlap, and before the readLoop below
	// so an expired deadline always has a reader that observes the close.
	c.armConnectDeadline(secondsToDuration(mqttConnectTimeout(opts)))

	// No Ping timer for MQTT clients...

	// readLoop is tracked by clientWG (token reserved at registration, released
	// by its defer via c.finish), so Shutdown Phase 1 can wait for client cleanup
	// independently of the background grWG goroutines. writeLoop stays on grWG.
	go c.readLoop(pre)
	s.startGoRoutine(func() { c.writeLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tls.CipherSuiteName(cs.CipherSuite))
	}

	c.mu.Unlock()

	return c
}

// ValidateOptions is the standalone broker's one validation boundary for Options. Start runs it
// before anything else, so an invalid configuration is refused with nothing dialed, no listener
// bound and no goroutine started; the command runs the same function before its own side effects
// (the NATS connection and the health server), so each constraint is stated once.
//
// It checks this broker's own interpretation and encoding of each value - sentinels read two ways,
// conversions that truncate or overflow, strings written to the wire, features it does not
// implement - and refuses one whose effect would otherwise be a SILENT change of behavior. It does
// not duplicate the backend's validation or arithmetic on a value forwarded to it: those limits are
// stated in docs/CONFORMANCE.md ("Programmatic AckWait near the duration limit"). Upstream's checks
// about an embedded server - its JetStream flag, its cluster and gateway ports, its users and
// nkeys - describe a process this is not, and are gone (F-052).
func ValidateOptions(o *Options) error {
	mo := &o.MQTT
	if mo.AckWait < 0 {
		return errMQTTAckWaitMustBePositive
	}
	if mo.JSAPITimeout < 0 {
		return errMQTTJSAPITimeoutMustBePositive
	}
	// Topic Alias Maximum: 0 = use default, -1 = disabled, 1..65535 explicit.
	if mo.TopicAliasMaximum < -1 || mo.TopicAliasMaximum > 0xFFFF {
		return fmt.Errorf("mqtt topic_alias_maximum must be in [-1..%d] (0 = default %d, -1 disables), got %d",
			0xFFFF, mqttDefaultTopicAliasMax, mo.TopicAliasMaximum)
	}
	// Receive Maximum: 0 = use default, -1 = disabled, 1..65535 explicit.
	if mo.ReceiveMaximum < -1 || mo.ReceiveMaximum > 0xFFFF {
		return fmt.Errorf("mqtt receive_maximum must be in [-1..%d] (0 = default %d, -1 disables), got %d",
			0xFFFF, mqttDefaultReceiveMax, mo.ReceiveMaximum)
	}
	// Keep Alive Maximum: 0 = no override, 1..65535 explicit. No -1 sentinel is needed (unlike
	// topic_alias_maximum): there is no non-zero default, so unset and disabled are the same thing.
	if mo.KeepAliveMaximum < 0 || mo.KeepAliveMaximum > 0xFFFF {
		return fmt.Errorf("mqtt keep_alive_maximum must be in [0..%d] (0 = no override), got %d", 0xFFFF, mo.KeepAliveMaximum)
	}
	// The accept path reads a negative bound as "refuse every connection".
	if o.MaxConn < 0 {
		return fmt.Errorf("mqtt max_conn must be >= 0 (0 = unlimited), got %d", o.MaxConn)
	}
	// A negative value is read as automatic when the replica count is chosen and as an explicit
	// requirement when the streams are checked, so it has no single meaning. The upper bound is the
	// server's, which refuses it loudly.
	if mo.StreamReplicas < 0 {
		return fmt.Errorf("mqtt stream_replicas must be >= 0 (0 = automatic), got %d", mo.StreamReplicas)
	}
	for _, t := range []struct {
		name string
		v    float64
	}{
		{"auth_timeout", o.AuthTimeout},
		{"mqtt auth_timeout", mo.AuthTimeout},
		{"mqtt tls_timeout", mo.TLSTimeout},
	} {
		if err := validateTimeoutSeconds(t.name, t.v); err != nil {
			return err
		}
	}
	// Response Information is written verbatim into a v5 CONNACK as an MQTT UTF-8 string: a
	// 16-bit length prefix, valid UTF-8, no NUL [MQTT-1.5.4-1, 1.5.4-2]. Past 65,535 bytes the
	// prefix wraps and the CONNACK is malformed.
	if ri := mo.ResponseInformation; ri != _EMPTY_ {
		if len(ri) > 0xFFFF {
			return fmt.Errorf("mqtt response_information must be at most %d bytes, got %d", 0xFFFF, len(ri))
		}
		if err := mqttValidateString(ri, "response_information"); err != nil {
			return fmt.Errorf("mqtt response_information is not a valid MQTT string: %v", err)
		}
	}
	// Pinning is not implemented, so a pin set would be accepted and never enforced: fail-open.
	if len(mo.TLSPinnedCerts) > 0 {
		return errors.New("mqtt TLS certificate pinning is not supported")
	}
	return nil
}

// validateTimeoutSeconds refuses a timeout in float seconds that cannot become the duration it
// names. Nonpositive keeps its meaning - the default - so mqttConnectTimeout and mqttTLSTimeout
// are unchanged for every value they handled. A positive value must survive secondsToDuration's
// own float64 product: below one nanosecond it truncates to zero, and at or above 2^63 it
// overflows, and either way the CONNECT timer or TLS deadline would fire at once.
// float64(math.MaxInt64) rounds to 2^63, so the bound is >= 2^63, not > MaxInt64.
func validateTimeoutSeconds(name string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("%s must be a finite number of seconds, got %v", name, v)
	}
	if v <= 0 {
		return nil
	}
	if ns := v * float64(time.Second); ns < 1 || ns >= 0x1p63 {
		return fmt.Errorf("%s must be between 1ns and %v when positive, got %v seconds", name, time.Duration(math.MaxInt64), v)
	}
	return nil
}

// Returns true if this connection is from a MQTT client.
// Lock held on entry.
func (c *client) isMqtt() bool {
	return c.mqtt != nil
}

// The MQTT client connection has been closed, or the DISCONNECT packet was received.
// For a "clean" session, we will delete the session, otherwise, simply removing
// the binding. We will also send the "will" message if applicable.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttHandleClosedClient(c *client) {
	if h := mqttTestClosedClientEntryV.Load(); h != nil {
		(*h)(c)
	}
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	// If asm or sess are nil, it means that we have failed a client
	// before it was associated with a session, so nothing more to do.
	if asm == nil || sess == nil {
		return
	}

	// Whatever closed this connection, classify what its Will is owed BEFORE
	// contending for the session gate below. A same-client-ID CONNECT may be
	// taking the session over right now, and both sides decide the Will's fate;
	// this is the transition that settles which of them observed the close first.
	// Already terminal (a normal DISCONNECT, or a takeover that discarded it) is
	// a no-op. Spec5 [3.1.2.5].
	c.mqttOweWill(c.mqttSessionExpiry())
	asm.beforeTeardownLock(sess.id)

	// Add this session to the locked map for the rest of the execution.
	//
	// A failed acquisition is CLASSIFIED, never assumed: lockSession fails both
	// for a rebind (a takeover owns everything below) and for plain contention —
	// and an uncertain-write resolver now holds this gate across a JetStream
	// readback, which can outlast the bounded retry. Reading that as a takeover
	// silently dropped the whole teardown of a session that was still ours: no
	// detach, no persisted disconnect time, no expiry, no Will. So while this
	// connection is still the owner, contention parks the teardown when the
	// session is unresolved — exactly what winning the gate would have done, and
	// the resolver holding it discharges the parked action before releasing —
	// and otherwise simply keeps trying: every holder is bounded (a CONNECT
	// rebinds early, flipping the owner check; resolvers are bounded by their
	// JetStream timeouts), so the loop terminates.
	for {
		err := asm.lockSessionForTeardown(sess, c)
		if err == nil {
			break
		}
		sess.mu.Lock()
		stillOurs := sess.c == c
		sess.mu.Unlock()
		if !stillOurs {
			// The session is no longer bound to THIS connection — a replacement CONNECT,
			// or a disposal that detached it — so none of the teardown below is ours to
			// do. An owed Will still has to be discharged, and nothing else will do it.
			// See mqttPublishOwedWill.
			s.mqttPublishOwedWill(c, asm)
			asm.afterTeardownOwnerLost(sess.id)
			return
		}
		// Still ours and possibly unresolved: the holder is (or is about to be)
		// a resolver. Park the obligations with their original deadlines, as the
		// gated path below would; resolution discharges them. Parking needs no
		// gate — it only writes session memory — but it must be ATOMIC with the
		// uncertainty check: the gate-holding resolver can clear the uncertainty
		// and run its final discharge scan at any instant, and an action parked
		// after that scan would be stranded forever. deferDisconnectIfUncertain
		// makes check and park one critical section; a refusal because the
		// session resolved meanwhile falls through to retaking the gate (the
		// resolver is finishing), with the Will restored for the ordinary
		// teardown that follows.
		if s.mqttDeferDisconnectUngated(c, asm, sess) {
			return
		}
	}
	defer asm.unlockSession(sess)

	// An unresolved record write makes every teardown decision below unsafe: a
	// stale sess.seq would drive a second failing disconnect save, a clear() that
	// deletes the durables and the record before its CAS could fail, or a Will
	// takeover fence derived from a sequence nobody has classified. Record the MQTT
	// obligations with their ORIGINAL deadlines, detach the client, and let
	// resolution discharge them. Unresolved is NOT takeover, so nothing may be
	// dropped and nothing may be deleted. See sess_uncertain.go.
	if sess.isSeqUncertain() {
		s.mqttDeferDisconnectOwned(c, asm, sess)
		return
	}

	// THE DISCONNECT OVERRIDE, made durable before any teardown decision reads the
	// lifetime. A v5 DISCONNECT may replace the Session Expiry Interval (Spec5
	// [3.14.2.2.2]), and until F-041 that replacement reached memory only: the
	// record kept the pre-override interval AND its old Clean, so a discard that
	// did not complete left a record a later CONNECT resumes -- reporting Session
	// Present true for a session the DISCONNECT ended -- while an override RAISING
	// the interval to never-expire left a finite record the expiry sweep stamps and
	// deletes. Opposite directions, one cause, which is why this is not conditioned
	// on the override being zero.
	//
	// Clean and the interval are ONE fact (mqttSessionIsCleanForExpiry): this is
	// the writer that used to move the interval without Clean, and persisting the
	// interval alone would be strictly worse than not persisting at all -- the
	// resume decision reads Clean, `overdue` needs a positive interval, and the
	// expiry sweep skips zero, so the record would be both resumable and
	// unreapable.
	//
	// It runs in its OWN record transaction, closed before the stamp block below
	// opens its own: the fence is not re-entrant. And it uses the blocking
	// beginRecordTx, which the stamp block deliberately does not -- that block's
	// non-blocking acquisition is the record of two defects a wait there caused,
	// and this transaction is closed before it, so it does not convert one into
	// the other.
	if ov := c.mqttLifetimeOverride(); ov != nil {
		tok := sess.beginRecordTx()
		// Re-checked WITH THE FENCE HELD. The entry check above is a check-then-act:
		// an off-readLoop writer can arm uncertainty while this waits, after which
		// the classifier sees wasUncertain already true and reports a definite
		// failure for a write that was never issued -- concluding "nothing was
		// stored" about somebody else's outstanding proposal.
		if sess.isSeqUncertain() {
			if err := sess.endRecordTx(tok); err != nil {
				c.Errorf("MQTT: %v", err)
			}
			s.mqttDeferDisconnectOwned(c, asm, sess)
			return
		}
		g := sess.applyRequestedLifetime(tok, *ov,
			mqttSessionIsCleanForExpiry(*ov), mqttLifetimeFinal)
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
		switch g.outcome {
		case mqttPreConnackProceed:
			// Applied, or unchanged, or contention resolved as landed. Continue on
			// the applied terms -- and note equality alone proves no new durability,
			// which is why the classifier and not a comparison decides this.
		case mqttPreConnackUnresolved:
			// Nothing below may run on a sequence nobody has classified. The park
			// carries the override (see mqttDeferDisconnectEx), so the obligation
			// survives even though the session is back on its confirmed terms.
			s.mqttDeferDisconnectOwned(c, asm, sess)
			return
		case mqttPreConnackDisposingNonForked, mqttPreConnackDisposingForked:
			// An OWNERSHIP verdict, not a persistence failure. Continuing would run
			// purgeSharedPending -- which performs register operations independently
			// of the later teardown CAS -- and a second finite save, against a record
			// that is no longer ours. Terminate through the existing disposal, which
			// discharges the owed Will, and keep the classifier's minted grant and
			// vocabulary on the forked arm. Nonforked does NOT prove a successor
			// exists (F-084).
			sess.disposeForeignSessionLocked(asm, c, g.voc)
			return
		default: // mqttPreConnackLocal
			// Under Final the terms STAND, so the teardown below does what the client
			// asked; only the durable backstop is lost. Whether the record has moved
			// is not concluded here -- that is the disposing arms' verdict -- and
			// clearInner's own CAS still arbitrates (errMQTTClearSuperseded).
			c.Warnf("MQTT: unable to persist the lifetime session %q ends on; "+
				"a discard that does not complete will leave a resumable record: %v",
				sess.id, g.err)
		}
	}

	// A NEVER-EXPIRING session makes no disconnect save below, so nothing else ever
	// records that this connection ended — and the delayed-Will record fence reads a
	// newer record with DisconnectedAt == 0 as a live newer connection and CANCELS a Will
	// this client is owed. Every later write on this session would publish exactly that
	// shape: the purge below, a void-verdict release handoff, a rejection probe.
	//
	// The RECORD FENCE is what makes the stamp effective, and `sess.mu` alone is not: an
	// off-readLoop writer (a register store's `persistVoidedRegs`, which takes the fence
	// without the ownership gate) can already have MARSHALLED `DisconnectedAt = 0` and be
	// mid-store, and that body landing after the origin sequence is captured below is a
	// newer record reading as connected. Holding the fence means every earlier marshal has
	// completed AND advanced `sess.seq` — so the capture below is at or past it — while
	// every later marshal needs this fence and therefore carries the stamp.
	//
	// Only for never-expire: the finite branch keeps its own in-transaction stamp, whose
	// save corrects the record immediately, and a clean close discards the record outright.
	sess.mu.Lock()
	neverExpire := sess.expiryInterval == mqttSessionNeverExpire
	sess.mu.Unlock()
	if neverExpire {
		sess.rejectionWindowHook(mqttRejectionWindowStamp)
		// NEVER the blocking form. Waiting here is what generated two defects rather than
		// one fix: the wait is exactly long enough for the writer being waited on to go
		// AMBIGUOUS — publishing seqUncertain before it releases, with its own immediate
		// resolver failing against the ownership gate this close holds — and long enough
		// (a JetStream timeout) that a park taken afterwards timestamps the close seconds
		// late, which can re-open an already-elapsed Will Delay for a reconnect to cancel.
		// A refusal does not skip the stamp — see the else branch, which stamps anyway,
		// unordered — it only gives up the ORDERING against a body already marshalled inside
		// an open transaction, when there is one. Same non-blocking reasoning as every other
		// off-path record write (see persistVoidedRegs). This is the ONE non-blocking site
		// that does not retry: it takes its degraded arm immediately, so waiter priority (see
		// sess_recordtx.go) gives that arm more occasions — including with the fence FREE but
		// reserved for an announced blocking writer. That arm is not cheaper: no transaction
		// is open at the refusal, but the prioritized writer can then take sess.mu, open its
		// transaction and marshal DisconnectedAt = 0 before this fallback stamps, which is
		// the same one-body residual reached from the other side. More occasions of a stated
		// residual, not a new behaviour; docs/CONFORMANCE.md carries it.
		if tok, ok := sess.tryBeginRecordTx(); ok {
			// Re-checked WITH THE FENCE HELD even so: acquiring it proves every earlier
			// marshal completed, but not that none of them armed uncertainty between the
			// entry check above and this instant. Stamping and detaching on a sequence
			// nobody has classified is what that check exists to withhold.
			unresolved := sess.isSeqUncertain()
			if !unresolved {
				sess.mu.Lock()
				sess.disconnectedAt = time.Now().Unix()
				sess.mu.Unlock()
			}
			if err := sess.endRecordTx(tok); err != nil {
				c.Errorf("MQTT: %v", err)
			}
			if unresolved {
				// Park after the release: the deferred discharge takes the fence itself.
				s.mqttDeferDisconnectOwned(c, asm, sess)
				return
			}
		} else {
			// An unavailable fence still stamps, just without the ordering. Skipping it entirely
			// would leave the REJECTION PROBE — a writer this change adds, and the reason
			// the stamp exists at all — free to persist current memory with
			// DisconnectedAt = 0 at a newer sequence, which the Will fence reads as a
			// reconnect: the holder we could not order against can be rejected at the
			// unchanged base, arming the marker, and the probe then lands after the origin
			// capture. So every marshal from HERE on carries the disconnect time, and the
			// residual is one body this stamp could not order against, and which waiting
			// for would re-open the two defects the wait caused. On the OPEN-transaction
			// refusal that body was marshalled before the refusal; on the waiter-priority
			// refusal nothing is open yet, but the prioritized writer can open and marshal
			// in the interval before this stamp takes sess.mu, so the exposure is the same
			// one body rather than none.
			//
			// No uncertainty re-check on this arm: without the fence there is no sound one
			// to make, and an unsound sample acted on is check-then-act. The stamp is a
			// memory field, and every other teardown decision below already runs with that
			// same pre-existing exposure.
			sess.mu.Lock()
			sess.disconnectedAt = time.Now().Unix()
			sess.mu.Unlock()
		}
	}

	asm.mu.Lock()
	// Clear the client from the session, but session may stay.
	sess.mu.Lock()
	sess.c = nil
	// The Session Expiry Interval (set at CONNECT, possibly updated by a v5
	// DISCONNECT) is authoritative here: 0 means the session ends at connection
	// close. For 3.1.1 this is 0 iff the session is clean, so behavior is unchanged.
	expiry := sess.expiryInterval
	idHash := sess.idHash
	// Captured BEFORE the clear() below, which re-mints: a delayed Will this
	// connection enqueues belongs to the incarnation that is ending, and the
	// displaced-broker cancel scopes itself by exactly that. (The clean path never
	// enqueues one, but reading the generation after a re-mint would still be wrong.)
	sessGen := sess.gen
	sess.mu.Unlock()
	doClean := expiry == 0
	// If the session ends at connection close, then we remove from the account
	// manager, and we will call clear() outside of any lock.
	if doClean {
		asm.removeSession(sess, false)
	}
	// Remove in case it was in the flappers map.
	asm.removeSessFromFlappersLocked(sess.id)
	asm.mu.Unlock()

	// This needs to be done outside of any lock.
	if doClean {
		sess.mu.Lock()
		ceilingTracker := sess.leases
		sess.mu.Unlock()
		if err := sess.clearInner(true, nil,
			s.pendingDiscardFinisher(asm, sess, ceilingTracker),
			s.armPendingDiscardWorker); err != nil {
			// A LATE ambiguity — armed after the entry check the discard already passed —
			// deleted nothing, so the discard is recoverable: park it and restore the
			// residency removeSession took above. Without that the record, the consumers,
			// the shared group durables and this generation's registers survive with NO
			// resident object, and nothing reaps them — reconcileResidentRecordState and CONNECT
			// both scan as.sessions, and sweepSessionExpiries skips an ExpiryInterval == 0
			// record — so only a reconnect on this client ID ever would.
			//
			// The Will is NOT parked: mqttHandleWill below still runs and publishes it
			// immediately, exactly as it does today after a failed clear(). See
			// reparkRefusedDiscard.
			//
			// FALL THROUGH rather than return. The !doClean block is skipped anyway, and
			// this connection's Will is still owed.
			switch {
			case errors.Is(err, errMQTTClearSuperseded):
				// TERMINAL, and a normal outcome: the base this discard claimed is not
				// current, so the session is not ours to discard (it may be held by
				// somebody, or already gone — the sentinel does not say which) and nothing
				// here was ours to delete. Its own branch rather than the fall-through
				// below, because logging a normal outcome at error level is what sends an
				// operator hunting a teardown that never went wrong. Explicitly NOT
				// re-parked either — nothing was refused, and re-parking would resurrect a
				// discard obligation this object no longer holds.
				c.Debugf("MQTT: discard of session %q stopped: %v", sess.id, err)
			case errors.Is(err, errMQTTClearDrainTimeout):
				// An admitted route or queued effect of this incarnation is still
				// outstanding, so nothing was deleted — but the clean-session discard is
				// OWED ([MQTT-3.1.2-6]) and the expiry sweep skips expiry-zero records,
				// so the object may not be left ordinary-resident. The obligation is
				// parked on the tracker (still CLOSING, which is what makes the session
				// non-resumable), residency is restored so a CONNECT can find the mark,
				// and the drain trigger or that CONNECT completes the discard. The Will
				// falls through to mqttHandleWill below, exactly as on the refusal arm.
				// See sess_lease.go and finishPendingDiscard.
				asm.addSession(sess, true)
				c.Warnf("MQTT: clean-session discard of %q deferred until its incarnation drains", sess.id)
			case errors.Is(err, errMQTTClearRefusedUnresolved) && s.reparkRefusedDiscard(asm, sess, time.Now().Unix()):
			default:
				c.Errorf("%s", err.Error())
			}
		}
	} else {
		// Session survives the disconnect: drop pending QoS 1/2 state for shared
		// group consumers without acking, so a resume does not NAK-steal messages
		// that AckWait redelivers to other members (and so PIs are freed). Spec5 [4.8.2].
		// Under the CLOSING connection's licence (F-105 7b): sess.c is already cleared, but c's
		// accepted entitlement is immutable, so a successor that has since accepted makes every
		// owner-dependent write of this purge fail rather than borrow the successor's.
		sess.purgeSharedPending(mqttLicenceOf(c))
	}
	if !doClean && expiry != mqttSessionNeverExpire {
		// Finite Session Expiry Interval: keep the session (so a reconnect can
		// resume it) and arm a cleanup timer. Persist the disconnect time so the
		// deadline survives a restart; a failed save only loses restart-survival.
		// Spec5 [3.1.2.11.2].
		//
		// The transaction opens BEFORE the disconnect time is set, not around the
		// save alone: a transaction that begins after its own mutation cannot make
		// that mutation invisible to another writer. See sess_recordtx.go.
		tok := sess.beginRecordTx()
		sess.mu.Lock()
		sess.disconnectedAt = time.Now().Unix()
		sess.mu.Unlock()
		if err := sess.save(tok); err != nil {
			c.Debugf("Unable to persist session expiry deadline: %v", err)
		}
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
		asm.scheduleSessionExpiry(s, idHash, time.Duration(expiry)*time.Second)
	}

	// Capture the persisted session-record sequence as the delayed-Will takeover
	// fence baseline. For a finite-expiry session the disconnect save above bumped
	// it; for a never-expire session no disconnect save runs, so it is the last
	// prior save's sequence — still a valid baseline, since any reconnect/takeover
	// saves again with a strictly higher sequence. Captured here (post-save, before
	// any reconnect) so a later higher session sequence means reconnect/takeover or
	// a sweep, never an ordinary in-connection save. The epoch names the incarnation
	// so the fence only compares this sequence within it. See mqttWillOutbox.
	sess.mu.Lock()
	originSessionSeq := sess.seq
	sess.mu.Unlock()
	originSessEpoch := asm.sessStreamEpoch

	// Now handle the "will". This function will be a no-op if there is no "will" to
	// send. The effective Session Expiry Interval bounds any Will Delay.
	s.mqttHandleWill(c, &expiry, sessGen, originSessionSeq, originSessEpoch)
}
