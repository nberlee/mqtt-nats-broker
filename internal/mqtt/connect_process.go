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

// CONNECT: keep-alive, parsing the CONNECT packet, and processing it (mqttProcessConnect).

package mqtt

import (
	"bytes"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nuid"
)

// mqttKeepAliveRD returns the read deadline for a keep alive: one and a half
// times the keep alive interval. Computed in milliseconds to avoid truncating
// the half second for odd keep alives (e.g. 1s => 1.5s). Spec [MQTT-3.1.2-24].
func mqttKeepAliveRD(ka uint16) time.Duration {
	return time.Duration(ka) * 1500 * time.Millisecond
}

// Parse the MQTT connect protocol
func (c *client) mqttParseConnect(r *mqttReader, hasMappings bool) (byte, *mqttConnectProto, error) {
	// Protocol name
	proto, err := r.readBytes("protocol name", false)
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-1]
	if !bytes.Equal(proto, mqttProtoName) {
		// Check proto name against v3.1 to report better error
		if bytes.Equal(proto, mqttOldProtoName) {
			return 0, nil, fmt.Errorf("older protocol %q not supported", proto)
		}
		return 0, nil, fmt.Errorf("expected connect packet with protocol name %q, got %q", mqttProtoName, proto)
	}

	// Protocol level
	level, err := r.readByte("protocol level")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.2-2], spec5 [3.1.2.2]. This switch is the one definition of the
	// accepted versions: 3.1.1 (0x4) and 5.0 (0x5), on every broker. Any other level
	// is refused in 3.1.1 framing, since c.mqtt.proto is not yet set.
	switch level {
	case mqttProtoLevel, mqttProtoLevel5:
	default:
		return mqttConnAckRCUnacceptableProtocolVersion, nil, fmt.Errorf("unacceptable protocol version of %v", level)
	}
	// Record the negotiated version; it drives wire framing for the rest of the
	// connection's lifetime (CONNACK, ACKs, DISCONNECT).
	c.mqtt.proto = level

	cp := &mqttConnectProto{}
	// Connect flags
	cp.flags, err = r.readByte("flags")
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-3]
	if cp.flags&mqttConnFlagReserved != 0 {
		return 0, nil, errMQTTConnFlagReserved
	}

	var hasWill bool
	wqos := (cp.flags & mqttConnFlagWillQoS) >> 3
	wretain := cp.flags&mqttConnFlagWillRetain != 0
	// Spec [MQTT-3.1.2-11]
	if cp.flags&mqttConnFlagWillFlag == 0 {
		// Spec [MQTT-3.1.2-13]
		if wqos != 0 {
			return 0, nil, fmt.Errorf("if Will flag is set to 0, Will QoS must be 0 too, got %v", wqos)
		}
		// Spec [MQTT-3.1.2-15]
		if wretain {
			return 0, nil, errMQTTWillAndRetainFlag
		}
	} else {
		// Spec [MQTT-3.1.2-14]
		if wqos == 3 {
			return 0, nil, fmt.Errorf("if Will flag is set to 1, Will QoS can be 0, 1 or 2, got %v", wqos)
		}
		hasWill = true
	}

	// Spec [MQTT-3.1.2-19]
	hasUser := cp.flags&mqttConnFlagUsernameFlag != 0
	// Spec [MQTT-3.1.2-21]
	hasPassword := cp.flags&mqttConnFlagPasswordFlag != 0
	// Spec [MQTT-3.1.2-22]
	if !hasUser && hasPassword {
		return 0, nil, errMQTTPasswordFlagAndNoUser
	}

	// Keep alive
	var ka uint16
	ka, err = r.readUint16("keep alive")
	if err != nil {
		return 0, nil, err
	}
	cp.ka = ka
	// Spec [MQTT-3.1.2-24]
	if ka > 0 {
		cp.rd = mqttKeepAliveRD(ka)
	}

	// MQTT 5.0: the CONNECT variable header ends with a properties block, just
	// before the payload. Spec5 [3.1.2.11].
	if level == mqttProtoLevel5 {
		cp.props, err = r.readProperties(mqttPacketConnect)
		if err != nil {
			return 0, nil, err
		}
		// Capture at parse time the two properties that gate optional outbound
		// content, so even a CONNACK that rejects this CONNECT honors them:
		// Maximum Packet Size bounds every server-generated packet (an optional
		// Reason String is dropped, and a packet that still overflows is not sent),
		// and Request Problem Information 0 forbids Reason String/User Property on
		// packets other than CONNACK/DISCONNECT. If readProperties itself failed
		// above, cp.props is nil and the client communicated no limit. Spec5
		// [3.1.2.11.4], [3.1.2.11.7].
		if cp.props != nil {
			c.mqtt.maxPacketSize = cp.props.maxPacketSize
			c.mqtt.noProblemInfo = cp.props.present[mqttPropRequestProblemInfo] && cp.props.requestProblem == 0
		}
		// Enhanced authentication (AUTH exchange) is not implemented. If the
		// client requests an authentication method, reject the connection with
		// the v5 "bad authentication method" reason. Spec5 [4.12].
		if cp.props != nil && cp.props.authMethod != _EMPTY_ {
			return mqttReasonBadAuthMethod, nil, errMQTTAuthMethodNotSupported
		}
		// Spec5 [3.1.2.11.10]: Authentication Data without an Authentication
		// Method is a Protocol Error.
		if cp.props != nil && cp.props.authData != nil && cp.props.authMethod == _EMPTY_ {
			return mqttReasonProtocolError, nil, fmt.Errorf("%w: authentication data without authentication method", errMQTTProtocolError)
		}
	}

	// Payload starts here and order is mandated by:
	// Spec [MQTT-3.1.3-1]: client ID, will topic, will message, username, password

	// Client ID
	c.mqtt.cid, err = r.readString("client ID")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.3-7]
	if c.mqtt.cid == _EMPTY_ {
		if cp.flags&mqttConnFlagCleanSession == 0 {
			return mqttConnAckRCIdentifierRejected, nil, errMQTTCIDEmptyNeedsCleanFlag
		}
		// Spec [MQTT-3.1.3-6]
		c.mqtt.cid = nuid.Next()
		c.mqtt.cidGenerated = true
	}
	// Spec [MQTT-3.1.3-4] and [MQTT-3.1.3-9]
	//
	// mqtt-nats-broker divergence from upstream NATS: we accept any valid-UTF-8
	// client ID (MQTT 5.0 [3.1.3.1] permits it, and mainstream brokers do too),
	// rather than requiring a NATS-safe name. The client ID is only used as a
	// NATS subject token via getHash() (sessions, QoS2 dedup — see
	// mqttQoS2InternalSubject), so arbitrary characters (spaces, '.', wildcards)
	// are safe. mqttValidateString still enforces UTF-8 and rejects NUL.
	if err := mqttValidateString(c.mqtt.cid, "client ID"); err != nil {
		return mqttConnAckRCIdentifierRejected, nil, err
	}

	if hasWill {
		cp.will = &mqttWill{
			qos:    wqos,
			retain: wretain,
		}
		// MQTT 5.0: Will properties precede the Will topic. Spec5 [3.1.3.2].
		if level == mqttProtoLevel5 {
			cp.will.props, err = r.readProperties(mqttPropsContextWill)
			if err != nil {
				return 0, nil, err
			}
		}
		var topic []byte
		// Need to make a copy since we need to hold to this topic after the
		// parsing of this protocol.
		topic, err = r.readBytes("Will topic", true)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, errMQTTEmptyWillTopic
		}
		if err := mqttValidateTopic(topic, "Will topic"); err != nil {
			return 0, nil, err
		}
		// Convert MQTT topic to NATS subject
		cp.will.subject, err = mqttTopicToNATSPubSubject(topic)
		if err != nil {
			return 0, nil, err
		}
		// Check for subject mapping.
		if hasMappings {
			// For selectMappedSubject to work, we need to have c.pa.subject set.
			// If there is a change, c.pa.mapped will be set after the call.
			c.pa.subject = cp.will.subject
			if changed := c.selectMappedSubject(); changed {
				// We need to keep track of the NATS subject/mapped in the `cp` structure.
				cp.will.subject = c.pa.subject
				cp.will.mapped = c.pa.mapped
				// We also now need to map the original MQTT topic to the new topic
				// based on the new subject.
				topic = natsSubjectToMQTTTopic(cp.will.subject)
			}
			// Reset those now.
			c.pa.subject, c.pa.mapped = nil, nil
		}
		// A Will may not target the broker's reserved namespace. This is the same
		// rule mqttProcessPub enforces for a client PUBLISH, applied here because
		// a Will never goes through that path: the immediate Will is delivered by
		// mqttInitiateMsgDelivery, and a delayed Will is replayed from the outbox
		// over the *service* connection, where the client's own NATS permissions
		// no longer apply. Without this check a Will topic is the one remaining
		// way for a client to publish into $MQTT.>.
		//
		// Checked after subject mapping so a mapping cannot land in the namespace
		// either. Rejecting the CONNECT (rather than dropping the Will silently)
		// tells the client its Will will not be honoured; 0x05 in 3.1.1, mapped to
		// 0x87 Not authorized for v5 by mqttConnAckReasonFromRC.
		if bytes.HasPrefix(cp.will.subject, []byte(mqttPrefix)) {
			return mqttConnAckRCNotAuthorized, nil, fmt.Errorf("the Will topic %q targets the reserved %s namespace", topic, mqttPrefix)
		}
		cp.will.topic = topic
		// Now "will" message.
		// Ask for a copy since we need to hold to this after parsing of this protocol.
		cp.will.message, err = r.readBytes("Will message", true)
		if err != nil {
			return 0, nil, err
		}
		// MQTT 5.0 Payload Format Indicator on the Will: if the client declares the
		// Will payload is UTF-8 (PFI=1) but it is not well-formed, reject the CONNECT
		// with CONNACK 0x99. Spec5 [3.1.3.2.2], [3.3.2.3.2].
		if level == mqttProtoLevel5 && cp.will.props != nil &&
			cp.will.props.present[mqttPropPayloadFormat] && cp.will.props.payloadFormat == 1 &&
			!utf8.Valid(cp.will.message) {
			return 0, nil, fmt.Errorf("%w: Will payload is not valid UTF-8 but the Payload Format Indicator is 1", errMQTTPayloadFormatInvalid)
		}
	}

	if hasUser {
		c.opts.Username, err = r.readString("user name")
		if err != nil {
			return 0, nil, err
		}
		if c.opts.Username == _EMPTY_ {
			return mqttConnAckRCBadUserOrPassword, nil, errMQTTEmptyUsername
		}
		// Spec [MQTT-3.1.3-11]
		if err := mqttValidateString(c.opts.Username, "user name"); err != nil {
			return mqttConnAckRCBadUserOrPassword, nil, err
		}
	}

	if hasPassword {
		c.opts.Password, err = r.readString("password")
		if err != nil {
			return 0, nil, err
		}
		c.opts.Token = c.opts.Password
	}
	return 0, cp, nil
}

func (c *client) mqttConnectTrace(cp *mqttConnectProto) string {
	trace := fmt.Sprintf("clientID=%s", c.mqtt.cid)
	if cp.rd > 0 {
		trace += fmt.Sprintf(" keepAlive=%v", cp.rd)
	}
	if cp.will != nil {
		trace += fmt.Sprintf(" will=(topic=%s QoS=%v retain=%v)",
			cp.will.topic, cp.will.qos, cp.will.retain)
	}
	if cp.flags&mqttConnFlagCleanSession != 0 {
		trace += " clean"
	}
	if c.opts.Username != _EMPTY_ {
		trace += fmt.Sprintf(" username=%s", c.opts.Username)
	}
	if c.opts.Password != _EMPTY_ {
		trace += " password=****"
	}
	return trace
}

// Process the CONNECT packet.
//
// For the first session on the account, an account session manager will be created,
// along with the JetStream streams/consumer necessary for the working of MQTT.
//
// The session, identified by a client ID, will be registered, or if already existing,
// will be resumed. If the session exists but is associated with an existing client,
// the old client is evicted, as per the specifications.
//
// Due to specific locking requirements around JS API requests, we cannot hold some
// locks for the entire duration of processing of some protocols, therefore, we use
// a map that registers the client ID in a "locked" state. If a different client tries
// to connect and the server detects that the client ID is in that map, it will try
// a little bit until it is not, or fail the new client, since we can't protect
// processing of protocols in the original client. This is not expected to happen often.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessConnect(c *client, cp *mqttConnectProto, trace bool) (connErr error) {
	sendConnAck := func(rc byte, sessp bool, reasonStr string) bool {
		// A delivered success CONNACK is recorded by mqttWriteConnAckFrame itself,
		// under the same c.mu hold as the write, so a concurrent takeover
		// notification cannot read the gate in between (see
		// mqttRecordConnAckWrittenLocked).
		sent, werr := c.mqttEnqueueConnAck(rc, sessp, reasonStr)
		// Traced only when it reached the socket: a write that failed sent nothing.
		if sent && werr == nil && trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
		}
		// The write error is part of the verdict: "a success CONNACK was written"
		// must be a checked fact, because every post-bind failure path decides its
		// abort discipline by exactly that. A failed write closed the socket, so
		// nothing was acknowledged.
		return sent && werr == nil
	}

	c.mu.Lock()
	cid := c.mqtt.cid
	admitted := c.mqttAdmitConnectLocked()
	c.mu.Unlock()
	if !admitted {
		// The CONNECT deadline already won, by its callback or by the clock: this
		// connection is condemned. The socket is closed HERE because a clock refusal
		// runs before the callback, which will now find the connection expired and close
		// nothing; when the callback won, this is a second, harmless close. Refuse BEFORE
		// authorizeClient, so a connection that lost the race dials no per-client NATS
		// connection and binds no session state -- there is nothing to unwind -- and
		// silently: it never authenticated, so it is owed no CONNACK.
		c.closeTransport()
		return errMQTTConnectDeadlineExpired
	}
	// The ownership budget is anchored HERE, at the moment F-051's deadline stopped
	// (mqttAdmitConnectLocked stopped the timer just above), so the whole interval between
	// that stop and a servable authority — the reads, the recovery, and every re-drive — is
	// bounded by one absolute allowance. Captured before the restoreSession label so a
	// `goto restoreSession` re-drive cannot re-anchor it: a budget re-created per cycle would
	// let a retry loop extend the total without bound, which is the defect the deadline exists
	// to prevent (Codex consult 10). See outbound_connect_budget.go.
	ownBudgetAnchor := time.Now()
	// THE OWED ABANDONMENT of this CONNECT's own aborted proposal, if a refusal leaves one
	// (F-105 8a). Registered BEFORE the certifier below so it runs AFTER it — defers run last
	// first — and therefore after the accept transaction's own deferred release as well: the
	// worker starts only once this connection is certified and holds nothing.
	var owedAbandon func()
	// certAsm is the account the refusal certifier reports to a test seam through, once known.
	var certAsm *mqttAccountSessionManager
	defer func() {
		if owedAbandon != nil && connErr != nil {
			owedAbandon()
		}
	}()
	// A REFUSED ACCEPTANCE NEVER ACTIVATES, whatever lands afterwards (F-105 8a). Every refusal
	// that wrote no success CONNACK certifies this connection closed through the external
	// protocol — seal, close outside the writer lock, cross the barrier, record — so a
	// finalization another acceptance completes later cannot make this socket emit. It runs at
	// return, after whatever failure CONNACK the exit wrote. A failure AFTER the success CONNACK
	// is left to its caller, which owes the client a v5 DISCONNECT with the reason and then
	// closes through closeConnection, which certifies; sealing here would suppress that DISCONNECT.
	defer func() {
		if connErr == nil {
			return
		}
		c.mu.Lock()
		connected := c.flags.isSet(connectReceived)
		c.mu.Unlock()
		if !connected {
			cert := mqttCertifyClosed(c, c.outBind, "connect refused: "+connErr.Error())
			if certAsm != nil {
				certAsm.mu.RLock()
				hook := certAsm.testCertified
				certAsm.mu.RUnlock()
				if hook != nil {
					hook(c, cert)
				}
			}
		}
	}()
	if refusal, ok := s.authorizeClient(c); !ok {
		// Tell the client which refusal it was before the close (F-061): 3.1.1 [3.1.4] says
		// the Server SHOULD, 5.0 [3.1.4] that it MAY. sendConnAck traces it only if written.
		if h := mqttTestRefusalBeforeConnAckV.Load(); h != nil {
			(*h)(c)
		}
		sendConnAck(refusal.code(c.mqtt.proto), false, refusal.reasonStr)
		c.closeConnection(WriteError)
		return ErrAuthentication
	}
	// Authenticated AND account-bound: authorizeClient refused the CONNECT
	// unless the identity's own NATS account is the service account, which is the
	// v1 same-account constraint docs/ARCHITECTURE.md states. F-080.
	//
	// Capture the Will's authorization HERE, while the snapshot is provably valid
	// and the connection still exists. Every site that discharges a Will -- the
	// delayed and takeover gates, the uncertainty-parked gate, and the immediate
	// path -- runs inside mqttHandleClosedClient, long after the connection has
	// gone and therefore after the snapshot has been invalidated. Asking at those
	// sites would suppress an AUTHORIZED Will on every NATS blip, which is the
	// opposite of what an authorization fix should do. Recording the answer once,
	// at the only moment it is well defined, is also why the durable outbox record
	// can be read as evidence: it now embodies a decision that was actually taken.
	//
	// Over the FULL effective-subject set, so a Will on a Sparkplug birth topic is
	// judged on its derived certificate subject too. Spec5 [3.1.3.2].
	c.mu.Lock()
	if cp.will != nil {
		c.willAuthorized = c.mqttPublicationAuthorizedLocked(&mqttPublish{
			topic:   cp.will.topic,
			subject: cp.will.subject,
			mapped:  cp.will.mapped,
		})
		if !c.willAuthorized {
			c.Warnf("Will on %q is not authorized for this identity; it will not be published", cp.will.topic)
		}
	}
	c.mu.Unlock()
	// Get the account's level MQTT sessions manager. If it does not exists yet,
	// this will create it along with the streams where sessions and messages
	// are stored.
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	certAsm = asm
	if err != nil {
		return err
	}

	// Resolve every input the success CONNACK's size depends on, then verify the
	// frame fits the client's Maximum Packet Size BEFORE touching any session
	// state. If it cannot fit, the server MUST NOT send it [MQTT-3.1.2-24]; there
	// is no useful connection to establish, so fail now — before any session
	// lookup, clear, rebind, live-client eviction or Will handling — so nothing
	// needs rolling back and no side effects leak. v5 only. Spec5 [3.2.2.3.6].
	if c.mqtt.proto == mqttProtoLevel5 {
		// Server Keep Alive: enforce the configured maximum when the client asked
		// for none (0) or more. Reads only Options and cp.ka. Spec5 [3.1.2.10],
		// [3.2.2.3.14].
		if kam := s.getOpts().MQTT.KeepAliveMaximum; kam > 0 && (cp.ka == 0 || cp.ka > uint16(kam)) {
			c.mqtt.serverKeepAlive = uint16(kam)
			cp.rd = mqttKeepAliveRD(uint16(kam))
		}
		// Response Information: when the client sets Request Response Information = 1
		// and the server is configured with a base response topic, return it in the
		// CONNACK so the client can build a request/response topic. Spec5
		// [3.1.2.11.6], [3.2.2.3.15].
		if ri := s.getOpts().MQTT.ResponseInformation; ri != _EMPTY_ &&
			cp.props != nil && cp.props.present[mqttPropRequestResponseInfo] && cp.props.requestResponse == 1 {
			c.mqtt.responseInfo = ri
		}
		// Topic Alias Maximum, both directions. Inbound: the server-side cap for
		// aliases the client may send us, advertised in the CONNACK. Outbound: the
		// client's CONNECT Topic Alias Maximum (absent => 0) caps aliases we may
		// send it, bounded by the same server-side cap to limit per-connection
		// memory. Spec5 [3.2.2.3.2], [3.1.2.11.2].
		c.mqtt.topicAliasMax = mqttTopicAliasMax(s.getOpts())
		if cp.props != nil {
			c.mqtt.outTopicAliasMax = min(cp.props.topicAliasMax, c.mqtt.topicAliasMax)
		}
		// Maximum Packet Size we advertise and enforce on inbound; captured once so
		// the encoded and enforced values are identical even across a reload.
		c.mqtt.maxInPacketSize = c.mqttServerMaxPacketSize()
		// Receive Maximum we advertise and enforce on inbound QoS 1/2 PUBLISH;
		// captured before the fit check below so its property bytes count toward
		// the CONNACK size. Spec5 [3.2.2.3.3], [4.9].
		c.mqtt.recvMaxIn = mqttReceiveMax(s.getOpts())
		// Frame size is independent of the session present flag, so a placeholder
		// (0) is fine for the fit check.
		if frame := c.mqttMakeConnAckV5Frame(mqttConnAckRCConnectionAccepted, 0, _EMPTY_); !c.mqttFitsClientMax(len(frame)) {
			c.Errorf("Success CONNACK of %d bytes exceeds client Maximum Packet Size %d; closing", len(frame), c.mqtt.maxPacketSize)
			return fmt.Errorf("CONNACK exceeds client maximum packet size")
		}
	}

	// Most of the session state is altered only in the readLoop so does not
	// need locking. For things that can be access in the readLoop and in
	// callbacks, we will use explicit locking.
	// To prevent other clients to connect with the same client ID, we will
	// add the client ID to a "locked" map so that the connect somewhere else
	// is put on hold.
	// This keep track of how many times this client is detecting that its
	// client ID is in the locked map. After a short amount, the server will
	// fail this inbound client.
	locked := 0

CHECK:
	asm.mu.Lock()
	// Check if different applications keep trying to connect with the same
	// client ID at the same time.
	if tm, ok := asm.flappers[cid]; ok {
		// If the last time it tried to connect was more than 1 sec ago,
		// then accept and remove from flappers map.
		if time.Since(tm) > mqttSessJailDur {
			asm.removeSessFromFlappersLocked(cid)
		} else {
			// Will hold this client for a second and then close it. We
			// do this so that if the client has a reconnect feature we
			// don't end-up with very rapid flapping between apps.
			// We need to wait in place and not schedule the connection
			// close because if this is a misbehaved client that does
			// not wait for the CONNACK and sends other protocols, the
			// server would not have a fully setup client and may panic.
			asm.mu.Unlock()
			select {
			case <-s.quitCh:
			case <-time.After(mqttSessJailDur):
			}
			c.closeConnection(DuplicateClientID)
			return ErrConnectionClosed
		}
	}
	// If an existing session is in the process of processing some packet, we can't
	// evict the old client just yet. So try again to see if the state clears, but
	// if it does not, then we have no choice but to fail the new client instead of
	// the old one.
	if _, ok := asm.sessLocked[cid]; ok {
		asm.mu.Unlock()
		if locked++; locked == 10 {
			return fmt.Errorf("other session with client ID %q is in the process of connecting", cid)
		}
		time.Sleep(100 * time.Millisecond)
		goto CHECK
	}

	// The disposing refusals' notify/close tail, registered BEFORE the gate-release defer
	// below so LIFO runs it LAST — after endAcceptTx, after the Will phase, and after the
	// client-ID gate is released. Notifying or closing while the gate is held is what
	// disposeForeignSessionLocked has to dodge by dispatching its close to another
	// goroutine; here it simply runs at the end, on this readLoop. F-084.
	var connTail func()
	defer func() {
		if connTail != nil {
			connTail()
		}
	}()

	// Register this client ID the "locked" map for the duration if this function.
	asm.sessLocked[cid] = struct{}{}
	// And remove it on exit, regardless of error or not.
	defer func() {
		asm.mu.Lock()
		delete(asm.sessLocked, cid)
		// The resident session, read in the same critical section as the release, for the
		// void-verdict re-check below: this is a gate release like unlockSession's and owes
		// the same handoff, and reading the map here is what keeps it to a RESIDENT object
		// however this CONNECT ended. A successful CONNECT leaves the session bound, which
		// the predicate excludes — a bound session converges through its own packet tails,
		// and firing here (right after the success CONNACK, before any tail exists) could
		// refuse the resume work behind an already-accepted connection.
		released := asm.sessions[cid]
		asm.mu.Unlock()
		asm.handoffVoidVerdictsOnGateRelease(released)
	}()

	// Is the client requesting a clean session (v5: Clean Start) or not.
	cleanSess := cp.flags&mqttConnFlagCleanSession != 0
	// MQTT 5.0 Session Expiry Interval: how long the session survives after the
	// connection closes; orthogonal to Clean Start. 3.1.1 maps clean => 0,
	// persistent => never. A session ends at connection close iff the interval is
	// 0 — exactly the legacy meaning of `clean` — so sess.clean is derived from the
	// interval and all existing clean-based branches work unchanged. Spec5 [3.1.2.11.2].
	var expiry uint32
	if !cleanSess {
		expiry = mqttSessionNeverExpire
	}
	if c.mqtt.proto == mqttProtoLevel5 {
		expiry = 0
		if cp.props != nil {
			expiry = cp.props.sessionExpiry
		}
	}
	sessClean := mqttSessionIsCleanForExpiry(expiry)
	// Session present? Assume false, will be set to true only when applicable.
	sessp := false
	// The QoS 1/2 deliveries this session already had in flight when it was bound
	// to this connection, filled in by the resume path below and consumed by
	// resendPendingRedelivery. Empty for a session that was not resumed.
	var resumePubs, resumeRels map[mqttPendingRef]struct{}
	// Do we have an existing session for this client ID
	es, exists := asm.sessions[cid]
	asm.mu.Unlock()
	formatError := func(err error) error {
		return fmt.Errorf("%v for account %q, session %q", err, c.acc.GetName(), cid)
	}

	// This CONNECT's re-drive budget, and the ACCEPT TRANSACTION's identity — both
	// declared ahead of the restore label because both must SURVIVE a re-drive. A
	// budget re-created per pass would bound nothing, and a deferred close that
	// captured `es` would, after a T2 object switch, end the old token against the new
	// object and leak the discarded one's fence. So the transaction is pinned to its
	// own session variable, which the handover updates explicitly.
	// See sess_takeover_redrive.go.
	rd := mqttNewRecordReDrive()
	var (
		acceptSess    *mqttSession
		acceptTok     recordTx
		acceptTokOpen bool
	)
	endAcceptTx := func() {
		if !acceptTokOpen {
			return
		}
		acceptTokOpen = false
		if err := acceptSess.endRecordTx(acceptTok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
	}
	// The disposing refusals' Will action. Registered AFTER the gate-release defer and
	// BEFORE endAcceptTx, so LIFO gives exactly
	//
	//	endAcceptTx -> this -> gate release -> connTail
	//
	// which is the order the discharge needs: the accept transaction must be CLOSED (the
	// fence cannot be acquired twice on one goroutine) and the ownership gate must still be
	// HELD, so nothing rebinds between the disposal and the Will it owes. F-084.
	var connWillPhase func()
	defer func() {
		if connWillPhase != nil {
			connWillPhase()
		}
	}()

	// disposeThroughPhases is the ONE way any arm of this CONNECT disposes a session, and it
	// exists so the ordering lives in a single place rather than being restated per arm —
	// which is exactly how three forced-leave arms came to bypass it.
	//
	// It runs phase 1 inline, because the identity check and the extraction must happen
	// while this frame still knows what it is disposing, and hands phase 2 and the report to
	// the two defers above. LIFO then gives exactly
	//
	//	endAcceptTx -> Will -> gate release -> notify/close
	//
	// which is what the Will action requires: the accept transaction CLOSED, because
	// mqttConsumeWillUnderGate does JetStream work and holding the record fence across an
	// enqueue's retries would block every other writer on this subject; and the ownership
	// gate STILL HELD, so nothing rebinds between the disposal and the Will it owes. F-084.
	//
	// owner is the identity the disposal checks itself against, and it is a PARAMETER
	// because one caller must waive it: the teardown-incomplete arm disposes an object this
	// CONNECT has already wiped, whatever is bound to it, and passing c there could only
	// decline the disposal it needs.
	//
	// Declared with the defers rather than beside its first caller so that every disposing
	// arm of this function can reach it, including the ones that run long before the accept
	// transaction is opened.
	//
	// The phases it hands off CHAIN rather than replace, through registerDisposalPhases: since
	// F-105 8a a CONNECT can dispose a stale resident object at its top and go on to reach
	// another disposing arm, and every disposal's Will and close are owed whatever happens
	// after it.
	//
	// registerDisposalPhases is the ONLY writer of connWillPhase and connTail (a source guard,
	// TestF105DisposalPhasesOnlyAppend, holds that): each registration runs after the ones
	// before it, so no later disposal can drop an earlier one's Will or close.
	registerDisposalPhases := func(will, tail func()) {
		prevWill := connWillPhase
		connWillPhase = func() {
			if prevWill != nil {
				prevWill()
			}
			will()
		}
		if tail != nil {
			prevTail := connTail
			connTail = func() {
				if prevTail != nil {
					prevTail()
				}
				tail()
			}
		}
	}
	disposeThroughPhases := func(owner *client, voc mqttDisposalVocabulary, fence *mqttWillFence) {
		target := es
		d := target.disposeForeignStateFenced(asm, owner, false, fence)
		var tail func()
		if d.applied && d.stale != nil {
			stale := d.stale
			tail = func() {
				stale.mqttNotifyForeignDisposal(target.id, voc)
				stale.closeConnection(voc.closeReason)
			}
		}
		registerDisposalPhases(func() { d.applyWill(target, asm) }, tail)
	}
	// disposeCapturedThroughPhases is disposeThroughPhases for the retirement path,
	// where the displaced client was detached (and its parked action taken) BEFORE the
	// re-mint: the disposition is built from the capsule instead of from the session,
	// which by now names a fresh incarnation. F-036 unit 1.
	disposeCapturedThroughPhases := func(capsule *mqttRetiredBinding, voc mqttDisposalVocabulary) {
		target := es
		d := asm.disposeCaptured(target, capsule, false)
		var tail func()
		if d.stale != nil {
			stale := d.stale
			tail = func() {
				stale.mqttNotifyForeignDisposal(target.id, voc)
				stale.closeConnection(voc.closeReason)
			}
		}
		registerDisposalPhases(func() { d.applyWill(target, asm) }, tail)
	}

	defer endAcceptTx()

	// One absolute ownership budget for this CONNECT, anchored at F-051's stop and constructed
	// ABOVE the restoreSession label so it survives every re-drive unchanged: retries and cycles
	// consume it, none resets it. See outbound_connect_budget.go and ownBudgetAnchor above.
	ownBudgetTotal := mqttConnectOwnershipBudget
	asm.mu.RLock()
	if asm.testOwnershipBudgetTotal > 0 {
		ownBudgetTotal = asm.testOwnershipBudgetTotal
	}
	asm.mu.RUnlock()
	ownBudget, _ := mqttNewOwnershipBudget(ownBudgetAnchor, ownBudgetTotal)
	// The authority-step re-attempt allowance, per CONNECT and for the same reason: every drive
	// and every re-drive of this frame draws on one allowance, so a restoration never replenishes
	// it (see reconcileOwnershipStep).
	stepReattempts := mqttOwnershipStepReattempts
	// stepAllow admits ONE re-attempt of an ownership authority step whose attempt positively
	// never landed. It bounds STARTING another attempt — the absolute deadline and the
	// per-CONNECT allowance above; the requests already running are bounded by the same
	// deadline through the protocol's deadline view (outbound_connect_deadline.go).
	stepAllow := func() error {
		if berr := mqttOwnershipStepAllowed(ownBudget, time.Now(), "authority step re-attempt"); berr != nil {
			return berr
		}
		if stepReattempts <= 0 {
			return fmt.Errorf("the ownership authority steps of this CONNECT have spent their %d re-attempts",
				mqttOwnershipStepReattempts)
		}
		stepReattempts--
		return nil
	}
	// The CONNECT's own ownership state across every drive and re-drive: the intents its drives
	// proposed, and the ONE foreign proposal it settles (F-105 8a). Above the label for the same
	// reason as the budget: a re-drive that reset it would be a fresh acceptance in all but name.
	ownState := newMQTTConnectOwnershipState()

	// R1's re-entry point for this frame: the AUTHORITATIVE restore, from the top. Every
	// transition that learns its base is not current arrives back here having already
	// disposed of whatever it held, with `exists` false so the record — not the map —
	// decides again. Session Present and the resumed in-flight sets are recomputed from
	// that read too: carried over, they would report a session present that the second
	// pass discarded, and re-send deliveries belonging to a dead incarnation.
restoreSession:
	// SETTLE ANY STANDING OWNERSHIP PROPOSAL FIRST, before the restore, the uncertain-record
	// resolution, the retirement and the clean-start block (F-105 8a). Each of those may destroy
	// or re-propose over what a crashed or concurrent acceptance left mid-flight — a committed
	// successor record, a predecessor a cancelled handoff would restore — and none of them reads
	// the authority's proposal. Settling it here is the CONNECT's half; every discard settles at
	// its own boundary too (prepareClear), because this read is not a fence.
	{
		sres, sload, serr := asm.jsa.settleStandingProposal(cid, asm.sessStreamEpoch, asm.domainTk,
			ownBudget, time.Now, stepAllow, ownState)
		if serr != nil {
			return formatError(fmt.Errorf("settling the standing ownership proposal: %w", serr))
		}
		if sres.Outcome != mqttOwnershipSettled {
			return formatError(fmt.Errorf("refusing CONNECT for %q: a standing ownership proposal is "+
				"not settled (%s)", cid, sres.Reason))
		}
		// A RESIDENT OBJECT THE SETTLED STATE HAS LEFT BEHIND. If the authority now serves a
		// generation other than the resident object's, a committed proposal moved the record
		// past it — whoever finalized it, this CONNECT or another settler first. The object is
		// stale, and the existing disposal for exactly that fact applies: the record moved and
		// nothing establishes who holds it, so the Will stays ours to discharge (F-084). An
		// object whose retirement is pending is left to the join below, which drains its owners
		// and finishes its discard; nothing here may declare them drained.
		if exists && es != nil && sload.status == mqttOutboundValid && sload.rec != nil &&
			sload.rec.State == mqttAuthorityServing {
			es.mu.Lock()
			staleGen := sload.rec.Gen != es.gen
			pending := es.leases.isRetirementPending()
			es.mu.Unlock()
			if staleGen && !pending {
				// Through the phased helper, like every disposing arm of this CONNECT: the Will
				// runs after the accept transaction closes and before the gate releases.
				disposeThroughPhases(nil, mqttDisposalForLineage(false), nil)
				es, exists, sessp = nil, false, false
				resumePubs, resumeRels = nil, nil
			}
		}
	}
	asm.mu.RLock()
	restoreHook := asm.testRestoreEntry
	asm.mu.RUnlock()
	if restoreHook != nil {
		restoreHook(cid)
	}
	// The session is not in the map, but may be on disk, so try to recover
	// or create the stream if not.
	if !exists {
		es, exists, err = asm.createOrRestoreSessionWithin(cid, s.getOpts(), rd)
		if err != nil {
			if err == errMQTTSessionCollision {
				sendConnAck(mqttConnAckRCIdentifierRejected, false, err.Error())
			}
			return formatError(err)
		}
	}
	// A resident session with an UNRESOLVED record write must be resolved BEFORE
	// Session Present is computed and before the accept save, or the reconnect
	// would be decided (and CAS'd) against a sequence nobody has classified. A
	// reconnect routinely beats the periodic retry pass here, and both go through
	// resolveUncertainSession so exactly one of them acts; the client ID is already
	// in sessLocked, which is what excludes the retry pass. See sess_uncertain.go.
	if exists && es.isSeqUncertain() {
		switch s.resolveUncertainSession(asm, es) {
		case mqttUncertainForeign, mqttUncertainForked:
			// A record exists that is not ours and that nothing established an owner for,
			// on either arm, and the local copy has been evicted either way. Rebuild from
			// the record and take over normally: the
			// surviving record is what the session's durable state IS, whoever wrote
			// it, and this CONNECT is a legitimate new claim on it. Named explicitly
			// rather than left to the default, which means "ours" and would bind a
			// stale object whose CAS base is no longer the record's.
			es, exists, err = asm.createOrRestoreSessionWithin(cid, s.getOpts(), rd)
			if err != nil {
				if err == errMQTTSessionCollision {
					sendConnAck(mqttConnAckRCIdentifierRejected, false, err.Error())
				}
				return formatError(err)
			}
		case mqttUncertainUnresolved:
			// Nothing may be concluded about this session's durable state, so it must
			// not be resumed, discarded, or overwritten. Reject and let the client
			// retry. Two states arrive here and the rejection is right for both: the
			// readback failed, in which case the deferred close obligations are still
			// parked; or the resolution's own work — a discharged deferred close, a
			// continuation's re-save — left a NEW write outstanding, in which case
			// binding anyway would have the accept save refused and the session evicted
			// with that write's obligations. See sess_uncertain.go.
			return formatError(fmt.Errorf("session record state is unresolved"))
		default:
			// Resolved as ours, and the deferred close (if any) has been applied. If
			// that discharged a deferred discard — a clean disconnect, or a deadline
			// that passed while the record was unresolved — the session is gone from
			// the maps and this CONNECT must start fresh.
			//
			// It RE-READS rather than reusing the object it left behind, and that is R1.
			// The discharge runs clear(), whose outcome this frame never sees: the
			// deferred-discard site consumes it and removes the session either way. So a
			// SUPERSEDED discard is indistinguishable here from a completed one — and
			// reusing the object after a superseded one would write at the sequence
			// clear() reset to 0 having already learned the base was stale. That is the
			// exact shape this item exists to close, reached through the one door that
			// does not report it.
			//
			// The cost is one record read on a CONNECT that discharged a deferred
			// discard. It cannot loop: the restore below yields an object that is not
			// uncertain, so this arm is not re-entered.
			asm.mu.RLock()
			_, resident := asm.sessions[cid]
			asm.mu.RUnlock()
			if !resident {
				// Admitted as a CYCLE, because by the definition this item bounds it is
				// one: the pass it starts performs a fresh authoritative restore and may
				// run a whole teardown and accept save off it. R3 admits nothing past the
				// deadline, and an arm that jumped straight to the restore would begin
				// exactly that work after it.
				//
				// The cycle CAP can never refuse here — this arm is reachable at most once
				// per CONNECT, since the restore below yields an object that is not
				// uncertain — so what this call really enforces is the deadline. Refusing
				// is clean: the discharge only runs on an UNBOUND session and has already
				// removed it, so nothing is resident and nothing is bound to leak.
				if aerr := rd.admitCycle(); aerr != nil {
					return formatError(aerr)
				}
				es, exists, sessp = nil, false, false
				resumePubs, resumeRels = nil, nil
				goto restoreSession
			}
		}
	}

	// The clean-start HANDOFF's captured predecessor, declared at the restoreSession scope so it
	// reaches both the discard branch (inside `if exists`) and the ownership drive (outside it),
	// and re-zeroed on every re-drive because a goto restoreSession re-runs this declaration. A
	// clean start over a LIVE serving authority does NOT destroy the record before acquiring
	// ownership — that premature destruction is what F-105's row-9 ordering corrects (Codex
	// consult 6): the accepted batch displaces the predecessor record in place through a
	// discarding handoff, so its identity (cshWID/cshSeq) is captured before the reset, its
	// backing-resource targets ride the closing batch, and its held durables are cleaned only
	// once the successor is serving. Left false, this is a plain discard or a resume.
	var (
		cleanStartHandoff bool
		cshWID            string
		cshSeq            uint64
		cshRetiringGen    string
		cshTargets        mqttCleanStartRetireTargets
		cshDurs           []string
		cshQ2AffGen       string
		cshQ2AffDur       string
	)

	if exists {
		// A RETIREMENT-PENDING session owes a clean-session discard whose drain
		// ceiling fired ([MQTT-3.1.2-6]; F-036 unit 1). It is non-resumable, so
		// finish the discard before deciding anything about this CONNECT: JOIN the
		// close cycle already in progress — do NOT close it again — and claim the
		// parked obligation under the gate this CONNECT holds, which is what
		// arbitrates against the drain-triggered worker.
		es.mu.Lock()
		pendingTracker := es.leases
		es.mu.Unlock()
		if pendingTracker.isRetirementPending() {
			ceiling := time.NewTimer(mqttIncarnationDrainTimeout)
			joined := pendingTracker.waitDrained(ceiling.C)
			ceiling.Stop()
			if !joined {
				// Still not drained: the mark and the closing tracker stay exactly as
				// they are, and this CONNECT is refused rather than resuming a session
				// whose discard is owed. A v5 client is TOLD 0x89 rather than just
				// dropped, because the whole disposition is "retry" — a bare close
				// leaves that to the client's own classification of a transport error.
				// 3.1.1 has no such code and keeps the close. Codes >= 0x80 pass
				// through the CONNACK mapping unchanged.
				if c.mqtt.proto == mqttProtoLevel5 {
					sendConnAck(mqttReasonServerBusy, false, "session is still completing in-flight work")
				}
				return formatError(fmt.Errorf("session is still completing in-flight work"))
			}
			if s.finishPendingDiscard(asm, es, pendingTracker) == mqttPendingDiscardReparked {
				// STILL OWED. prepareClear refused on an unclassified write, so the
				// object stays resident carrying both that write and the obligation.
				// Restoring over it would install a replacement in both indexes and
				// orphan them — and for a session whose disconnect required a discard,
				// the CONNECT could then report Session Present and resume state the
				// client asked to have destroyed. Reject without evicting, which is the
				// same answer the ordinary unresolved-record refusal gives, and let the
				// resolution re-drive the discard.
				return formatError(fmt.Errorf("session record state is unresolved"))
			}
			// Completed, or superseded by another claimant: this object is not the
			// client ID's session any more, so start over from the record.
			es, exists, sessp = nil, false, false
			resumePubs, resumeRels = nil, nil
			goto restoreSession
		}
		// A SESSION ITS BOUND CONNECTION HAS ALREADY ENDED (F-040). The connection still
		// bound here may have sent a v5 DISCONNECT setting the Session Expiry Interval to
		// 0 (Spec5 [3.14.2.2.2]), so the session ends when that connection closes
		// ([3.1.2.11]) and must not be resumed. Applying that instruction is the close's
		// job, but the close needs the ownership gate this CONNECT holds, and once this
		// CONNECT rebinds, the close finds the session no longer its own and does nothing.
		// So the takeover performs the close's override write ON ITS BEHALF, here, and
		// the decision below then reads `es.clean` exactly as for any other ended session.
		//
		// It is a DURABLE write, before anything detaches, not a flag in memory: the
		// retirement below treats a discard as refusable, and a handoff cancelled or a
		// teardown refused after the detach would otherwise leave the predecessor record
		// persistent and resumable while the displaced close has already given up. The
		// instruction is unrefusable, so it must outlive every one of those arms.
		//
		// The override is recorded under c.mu by the packet handler before it starts the
		// close. Recorded before this sample: honored here. Recorded after it: that
		// DISCONNECT was sent on a connection this takeover had already displaced, and it
		// does nothing. See docs/ARCHITECTURE.md, "The DISCONNECT lifetime override is a
		// connection's OBLIGATION".
		es.mu.Lock()
		bound := es.c
		// An UNGATED park (parkClose) detaches a closing connection under this CONNECT's
		// gate, but only on an uncertain session, so an unbound object does not prove that
		// nothing is owed: the parked action may carry the ending. Its resolution
		// discharges it; nothing here may decide over it.
		parkedUncertain := bound == nil && es.deferredDisconnect != nil && es.seqUncertain
		es.mu.Unlock()
		if parkedUncertain {
			return formatError(fmt.Errorf("session record state is unresolved"))
		}
		if bound != nil {
			if ov := bound.mqttLifetimeOverride(); ov != nil && mqttSessionIsCleanForExpiry(*ov) {
				asm.beforeBoundDisconnectHonor(cid)
				tok := es.beginRecordTx()
				// Re-checked WITH THE FENCE HELD, as the close does: an off-readLoop writer
				// can have armed uncertainty since the entry check above, and the write
				// below must not be classified against a sequence nobody has classified.
				if es.isSeqUncertain() {
					if err := es.endRecordTx(tok); err != nil {
						c.Errorf("MQTT: %v", err)
					}
					return formatError(fmt.Errorf("session record state is unresolved"))
				}
				g := es.applyRequestedLifetime(tok, *ov, true, mqttLifetimeFinal)
				if err := es.endRecordTx(tok); err != nil {
					c.Errorf("MQTT: %v", err)
				}
				switch g.outcome {
				case mqttPreConnackProceed:
					// Durable (or already so): es.clean is true, and the decision below
					// discards.
				case mqttPreConnackUnresolved:
					// Nothing was concluded and the incumbent is still bound: its close
					// retakes the gate after this refusal and parks with the override.
					return formatError(fmt.Errorf("session record state is unresolved"))
				case mqttPreConnackDisposingNonForked, mqttPreConnackDisposingForked:
					// The record is not this object's: dispose of it as the close would
					// have, and let the record decide again.
					disposeThroughPhases(nil, g.voc, nil)
					if aerr := rd.admitCycle(); aerr != nil {
						return formatError(aerr)
					}
					es, exists, sessp = nil, false, false
					resumePubs, resumeRels = nil, nil
					goto restoreSession
				default: // mqttPreConnackLocal
					// Under Final the terms stand in memory, so the decision below still
					// discards; only the durable backstop is lost, as on the close's path.
					c.Warnf("MQTT: unable to persist the lifetime session %q ends on; "+
						"a discard that does not complete will leave a resumable record: %v",
						es.id, g.err)
				}
			}
		}
		// An overdue session must not be resumed even when this reconnect beat
		// a late expiry timer to the in-memory copy (the record-restore path
		// has the same check). Spec5 [3.1.2.11.2].
		es.mu.Lock()
		overdue := es.disconnectedAt > 0 && es.expiryInterval > 0 &&
			es.expiryInterval != mqttSessionNeverExpire &&
			time.Now().Unix() >= es.disconnectedAt+int64(es.expiryInterval)
		es.mu.Unlock()
		// The retirement capsule of the clean-start path, if that is the path taken:
		// it outlives the retirement because the SUCCESS arm still owes the displaced
		// client its takeover tail and still owes its parked action a disposition, and
		// by then es.c is nil (F-036 unit 1).
		var capsule *mqttRetiredBinding
		// Clear the session if client wants a clean session.
		// Also, Spec [MQTT-3.2.2-1]: don't report session present
		if cleanSess || es.clean || overdue {
			// Spec [MQTT-3.1.2-6]: If CleanSession is set to 1, the Client and
			// Server MUST discard any previous Session and start a new one.
			// This Session lasts as long as the Network Connection. State data
			// associated with this Session MUST NOT be reused in any subsequent
			// Session.
			// BEFORE the clear. Every post-reset failure arm below disposes this object,
			// and by then its generation is re-minted and its sequence 0 — so the Will
			// baseline has to be taken here, while it still names the incarnation the Will
			// was armed under. See mqttWillFence.
			// THE TWO-PHASE RETIREMENT (F-036 unit 1). Today's single clear() call
			// re-minted this object's generation while the displaced client was still
			// bound and still admitted, so its in-flight routes derived subjects under
			// the FRESH generation — losing an acknowledged QoS 2 message with one
			// broker, no crash and no peer. The order below is what property P needs:
			// close admission, drain the displaced incarnation's owners, capture what
			// every failure arm will need, prepare (the non-mutating refusals), detach,
			// mint the permit, and only then destroy and re-mint.
			//
			// Nothing here holds a lock across the drain: not es.mu, not the record
			// fence, not the QoS fences. The client-ID gate IS held, and that is safe
			// because no owner acquires it — the contenders all fail fast or
			// bounded-yield. See sess_lease.go and docs/ARCHITECTURE.md.
			var (
				prepared *mqttPreparedClear
				cerr     error
			)
			// P2 (F-105 8b): a clean start that will hand off from a SERVING predecessor names
			// the retirement targets of the record the accepted batch DISPLACES, read here under
			// the record fence and before the detach, never from this object's memory; and a
			// clean start never discards where a marker built from memory could be continued into
			// a receipt for a generation that may still serve. ONE disposition, defaulting to
			// refuse with the incumbent intact (consult 123): see displacedRetireTargets.
			var displaced *mqttCleanStartRetireTargets
			capsule, prepared, cerr = asm.retireBoundIncarnationChecked(es, func(s *mqttSession, p *mqttPreparedClear) error {
				t, ok, derr := s.displacedRetireTargets(p)
				if ok {
					displaced = &t
				}
				return derr
			})
			if cerr != nil {
				// NOTHING was mutated on either arm, and the incumbent is still bound,
				// still holding its Will, its socket, its expiry and its disconnect
				// state — so the answer is to refuse THIS CONNECT and let the client
				// retry, exactly as the unresolved-record refusal has always done.
				// A drain ceiling means an admitted route or a queued effect of the
				// incumbent is still outstanding: disposing under it is what every
				// prior F-036 attempt died on.
				if errors.Is(cerr, errMQTTClearDrainTimeout) {
					// Same as the JOIN arm above: the answer is RETRY, so a v5 client
					// is told 0x89 instead of being left to infer it from a socket
					// close. docs/CONFORMANCE.md states this reason.
					if c.mqtt.proto == mqttProtoLevel5 {
						sendConnAck(mqttReasonServerBusy, false,
							"session is still completing in-flight work")
					}
					return formatError(fmt.Errorf("session is still completing in-flight work"))
				}
				if errors.Is(cerr, errMQTTClearRefusedUnresolved) {
					return formatError(fmt.Errorf("session record state is unresolved"))
				}
				return formatError(cerr)
			}
			// The capsule carries the RETIRING incarnation's Will baseline, the
			// displaced client and its parked disconnect action: after the detach
			// nothing may rediscover them through es.c. See mqttWillFence, F-084.
			//
			// DISCARD versus HANDOFF. A clean start over a client with a LIVE serving outbound
			// authority must NOT destroy its record and then acquire ownership — that premature
			// destruction leaves the authority serving with no record to hand off from, and the
			// next transition refuses (F-105 row-9 ordering, Codex consult 6). Instead the accepted
			// batch displaces the predecessor record IN PLACE through a discarding handoff: its
			// identity (cshWID/cshSeq) and its backing-resource targets are captured HERE, before
			// the reset, and its held durables are cleaned only AFTER the successor is serving.
			// Absent or CLOSED authority — including a record already gone — keeps the ordinary
			// destroy-then-create path.
			es.mu.Lock()
			s1WID, s1Seq, s1Gen := es.wid, es.seq, es.gen
			es.mu.Unlock()
			// The choice was made ONCE, before the detach, from the discard boundary's own load
			// (F-105 8a; see mqttPreparedClear.auth): the disposition proved this object's record
			// at s1Seq under an authority SERVING at s1Gen, or chose the discard arm, or refused
			// (F-105 8b; displacedRetireTargets). Nothing here re-decides it.
			if displaced != nil {
				// Quiesce the QoS 2 move and release fences exactly as destroy does — a move or a
				// release admitted before this point performs JetStream round trips, so draining
				// them here is what stops one landing after the record has moved. Capture the
				// retirement targets, then re-mint the object as a FRESH incarnation while KEEPING
				// seq at the predecessor's, because the handoff CAS is conditioned on it. No record
				// and no resource is deleted on this path.
				es.closeQ2AffAndDrain()
				es.closeQoS2ReleaseAndDrain()
				es.mu.Lock()
				var durs []string
				for sid, cc := range es.cons {
					delete(es.cons, sid)
					// A shared group consumer is not this session's to delete; the generation's
					// acquisitions of it are sealed and released by the recovery owner the
					// finalization's discarding closure plants (F-105 7e).
					if cc.DeliverGroup != _EMPTY_ {
						continue
					}
					durs = append(durs, cc.Durable)
				}
				durs = append(durs, es.drainDurablesLocked()...)
				es.resetForReuseLocked(capsule.permit, mqttNewSessGen(), s1Seq)
				es.mu.Unlock()
				es.reopenQoS2ReleaseFence()
				// The retiring incarnation's parked ingress is resolved now that the object has
				// been re-minted, exactly as destroy's deferred resolveRetired does.
				capsule.permit.tr.resolveRetired()
				// Release the discard's record fence WITH a generation bump: it kills any writer
				// minted before it and, unlike a plain release, runs no void-verdict handoff — so it
				// cannot advance the very record sequence the handoff CAS is about to name. The
				// accept transaction opens its own fence below.
				if terr := es.endRecordTxNewGeneration(prepared.tok); terr != nil {
					es.jsa.c.Errorf("MQTT: %v", terr)
				}
				cleanStartHandoff = true
				cshWID, cshSeq, cshRetiringGen = s1WID, s1Seq, s1Gen
				cshDurs, cshTargets = durs, *displaced
				cshQ2AffGen, cshQ2AffDur = displaced.AffineGen, displaced.AffineDurable
			} else if err := prepared.destroy(capsule.permit, false); err != nil {
				if errors.Is(err, errMQTTClearSuperseded) {
					// R1 at transition T1. The base this discard claimed is not current,
					// and clear() has already reset this object to sequence 0 without
					// detaching its client — so proceeding would bind and then CREATE
					// against the record that actually won, and returning would leave a
					// seq == 0 object resident that no reaper reaches.
					//
					// Dispose FIRST, before the budget check and before the fallible
					// restore below: that ordering is what makes the reset-resident state
					// unreachable rather than merely unlikely. On exhaustion the CONNECT
					// is refused with nothing resident and nothing bound, which is the
					// state a fresh CONNECT starts from.
					// The capsule's client, not es.c: the detach already happened, so
					// rediscovering the displaced connection through the session would
					// find nil and silently drop its close, its notification, its Will,
					// its disconnect stamp and its expiry (F-036 unit 1, and the fourth
					// attempt's finding 4).
					asm.abandonReDrivenSessionCaptured(c, es, capsule)
					if aerr := rd.admitCycle(); aerr != nil {
						return formatError(aerr)
					}
					es, exists, sessp = nil, false, false
					resumePubs, resumeRels = nil, nil
					goto restoreSession
				}
				// EVERY REMAINING ERROR IS POST-RESET, and since F-036 unit 1 that is
				// structural rather than merely provable: BOTH uncertainty refusals live
				// in prepareClear, which ran (and returned above) before this destroy
				// could start, so no errMQTTClearRefusedUnresolved can arrive here at
				// all. By this point the object's subscriptions, consumers and registers
				// are erased, its sequence is 0 and its generation re-minted — an
				// indeterminate claim, a failed target deletion, an unclassifiable
				// release or a shared durable error all arrive in this state.
				//
				// removeSession alone was NOT enough, and the gap was silent: it deletes
				// map entries and never touches sess.c, so an old client stayed BOUND to a
				// wiped, evicted object. Its readLoop's gate checks the binding, not
				// residency, so it went on serving packets against a session that no
				// longer existed anywhere — and no watch, backstop or sweep reaches an
				// object that is not in the maps. Detach and close it instead, with the
				// one instruction this state licenses: try again later.
				// The fence, not the session: clear() has already re-minted this object's
				// generation and set its sequence to 0, so consuming against it would hand
				// enqueueDelayedWill a zero origin sequence, which its validation refuses —
				// sacrificing the delay and publishing a delayed Will immediately for a
				// session that only failed to tear itself down. willFence carries the
				// RETIRING incarnation, which is the one the Will belongs to. F-084.
				// The capsule carries the displaced client, its parked action and the
				// retiring incarnation's Will fence, so this arm discharges all five
				// obligations without reading es.c — which the detach nilled. F-084: no
				// successor is established here, so the Will is preserved or discharged
				// deferred, never discarded.
				disposeCapturedThroughPhases(capsule, mqttDisposalTeardownIncomplete)
				return err
			}
		} else {
			// Report to the client that the session was present
			sessp = true
		}
		// Spec [MQTT-3.1.4-2]. If the ClientId represents a Client already
		// connected to the Server then the Server MUST disconnect the existing
		// client.
		// Bind with the new client. This needs to be protected because can be
		// accessed outside of the readLoop.
		es.mu.Lock()
		ec := es.c
		// On the clean-start retirement path the displaced client was detached before
		// the re-mint, so es.c is nil by now and the capsule is the only thing that
		// still names it. The takeover tail below is owed to it either way — an
		// ordinary resume takeover finds it here, a retirement finds it there
		// (F-036 unit 1).
		if ec == nil && capsule != nil {
			ec = capsule.client
		}
		es.c = c
		// Ownership has just moved, so every hold scoped to the PREDECESSOR — an
		// unanswerable PUBCOMP debt, an unwritten retained frame — is now inert by the
		// shared predicate and can be dropped. This is the only place a hold's owner
		// changes, and clear()'s sweep runs strictly before it, so without this a
		// never-flushed retained frame's entry survives every rebind.
		//
		// HYGIENE, and nothing rests on it: an inert hold is invisible to
		// sendReservedLocked, so it blocks no allocation and no transmission either way.
		// It is also not the "dropped at a named transition site" F-089 forbids: that rule
		// is about a LIVE debt, while this deletes only what the predicate has already
		// proved reserves nothing — a write token is owner-independent and survives here.
		es.sweepDeadSendHoldsLocked()
		// And every released delivery whose PUBCOMP only the predecessor could have answered.
		es.retireReleasedDeliveriesLocked()
		// The connection's EXPECTED incarnation, captured in the same critical section
		// as the binding: every packet, delivery and effect of this connection owns
		// against THIS tracker for the rest of its life. Ordered after the sweep so the
		// sweep stays the statement immediately following the ownership move, which is
		// what the rebind guard requires; the capture only needs the same critical
		// section, not a position within it (F-036 unit 1).
		c.mqtt.tracker = es.leases
		// The requested Session Expiry Interval and Clean flag are NOT applied
		// here. This CONNECT has not been granted yet — the reconciliation phase
		// below can still refuse it — and the accept save a few lines down would
		// persist them regardless, leaving the record describing a session the
		// client was never told it has: with a requested interval of 0 the close
		// path would then discard a persistent session outright (doClean). They
		// are applied, and persisted, at the grant point below instead, so a
		// refused resume changes no lifetime and has nothing to repair.
		// Spec5 [3.1.2.11.2].
		// A session that was CLEARED just above is not being resumed: it has no
		// prior lifetime worth protecting, clear() does not reset these two
		// fields, and the accept save below is about to create its record from
		// scratch. So it takes the requested terms immediately; only a genuine
		// RESUME defers them to the grant point.
		if !sessp {
			es.clean, es.expiryInterval = sessClean, expiry
		}
		es.disconnectedAt = 0
		// Clear these flags so we resubscribe to the PUBREL and session-affine QoS 2
		// delivery subjects if needed: both subscriptions belonged to the previous
		// connection and went with it, while the consumers behind them (and their
		// pending deliveries) are session state that survives.
		es.pubRelSubscribed = false
		es.q2AffSubscribed = false
		// A resumed session must re-prove its affine delivery path before any move
		// may use it, and its PUBREL path before it stores a PUBREL: in both cases
		// the persisted consumer config is not evidence that the consumer still
		// exists, and $MQTT_out's interest retention silently drops what it stores
		// when it does not. See ensureQ2AffineConsumerSubscription and
		// ensurePubRelConsumerSubscription.
		es.q2AffReady = false
		es.pubRelVerified = false
		// A resumed session's retained (QoS 1/2) entries are deliberately NOT dropped here,
		// and this is where they used to be. They cannot be redelivered — a retained send has
		// no JetStream backing at all (R6) — but that is a fact about what this broker can
		// still do, never evidence that the client completed the exchange, and dropping them
		// freed both the identifier and a Receive Maximum slot while a QoS 2 client could
		// still be holding the identifier awaiting PUBREL. The release now happens at the one
		// site that PROVES the frame was never written, its abandonment
		// (releaseRetainedSendAbandonedLocked), so resume has nothing left to decide and reads
		// no transmission state at all (F-101).
		// What this session already had in flight, captured before any subscription
		// is restored — the only point at which "pending" unambiguously means
		// "pending from before the disconnect". resendPendingRedelivery NAKs exactly
		// these; anything the restored consumers push from here on is a first
		// transmission that must not be redelivered on top of itself.
		resumePubs, resumeRels = mqttPendingRefs(es.pendingPublish), mqttPendingRefs(es.pendingPubRel)
		es.mu.Unlock()
		if capsule != nil && capsule.action != nil {
			// A parked disconnect obligation OUTRANKS a live Will, because parking is
			// what moved the Will off the connection: the action carries it along with
			// the deadline, generation, sequence and epoch it was parked with. The
			// retirement took it into the capsule with the detach, so this is the one
			// place the successful takeover can still discharge it — dropping it here
			// would silently lose that Will (F-036 unit 1; F-084).
			//
			// A successor IS held in hand here, which is what licenses discarding a
			// LIVE Will below; a parked action is discharged rather than discarded
			// because it is no longer this connection's to suppress.
			d := mqttForeignDisposition{
				applied: true,
				stale:   capsule.client,
				action:  capsule.action,
				fence:   capsule.fence,
				will:    mqttWillActionDischargeDeferred,
			}
			d.applyWill(es, asm)
		}
		if ec != nil {
			// The one takeover tail, shared with the re-drive's T1b so the two cannot
			// drift apart: a client displaced by a re-drive is entitled to exactly the
			// same explanation as one displaced by an ordinary reconnect.
			// See sess_takeover_redrive.go.
			// The capsule's fence when the object was re-minted under this CONNECT (a clean
			// start), else nil: a resumed object still carries the displaced incarnation.
			var tailFence *mqttWillFence
			if capsule != nil {
				tailFence = capsule.fence
			}
			asm.discardTakenOverClient(c, ec, cid, es, tailFence)
		}
	} else {
		// Spec [MQTT-3.2.2-3]: if the Server does not have stored Session state,
		// it MUST set Session Present to 0 in the CONNACK packet.
		es.mu.Lock()
		es.c, es.clean = c, sessClean
		// Same capture as the resident-session bind above (F-036 unit 1).
		c.mqtt.tracker = es.leases
		es.expiryInterval = expiry
		es.disconnectedAt = 0
		es.mu.Unlock()
		// Now add this new session into the account sessions
		asm.addSession(es, true)
	}
	// MQTT 5.0 Receive Maximum: cap the number of in-flight unacknowledged
	// QoS1/2 PUBLISH we will send to this client. Re-evaluated on every CONNECT
	// since a resumed session may be taken over by a client with a different
	// (or no) limit. 0 means no client-imposed limit.
	var rmax uint16
	if cp.props != nil {
		rmax = cp.props.receiveMax
	}
	// The accept transaction. It opens BEFORE the mutations the accept save
	// persists (the Receive Maximum and the Will drop just below), because a
	// transaction that begins after its own mutation cannot make that mutation
	// invisible to another writer. See sess_recordtx.go.
	//
	// It is opened on THIS pass's object and recorded as such: a re-drive ends this
	// transaction explicitly before switching objects and then arrives back here to open
	// a fresh one on the replacement, which is what keeps the deferred close matched to
	// the token it is closing.
	acceptSess, acceptTok, acceptTokOpen = es, es.beginRecordTx(), true

	// Merge a recognised fork's candidate delta against the surviving record and re-assert
	// what that comparison accepts, before the mutations below and
	// before the save that persists them — the merge edits subs, the options, cons, the drains
	// and the nonce set, so it belongs inside this transaction rather than at the restore that
	// produced the object. This placement is also what makes one call serve both entry states,
	// a session restored this pass and a resident one, and it is strictly AFTER the CONNECT's
	// own uncertainty resolution on every path including the re-driven ones. A clean start
	// needs no special case: clear() has already re-minted the generation above, so the
	// capsule's anchor cannot match. See sess_fork_repair.go.
	//
	// A repair that could not read whether a consumer it would reinstate still serves refuses
	// the CONNECT before its accept, exactly as an unsettled ownership does: nothing has landed,
	// the capsule is kept, and the client's retry reads again (F-105 7f).
	if ferr := asm.reassertForkedGrantInto(es, acceptTok); ferr != nil {
		asm.removeSession(es, true)
		return fmt.Errorf("refusing CONNECT for %q: %v", cid, ferr)
	}

	es.mu.Lock()
	es.rmax = rmax
	// A new connection cancels any pending delayed Will (cancelWill below);
	// drop its persisted copy with the save that follows. Spec5 [MQTT-3.1.2-8].
	es.will = nil
	es.mu.Unlock()

	// Server Keep Alive, Topic Alias Maximum and the advertised Maximum Packet
	// Size were resolved on entry (before any session state), so the CONNACK could
	// be size-checked against the client's Maximum Packet Size before committing.

	// We would need to save only if it did not exist previously, but we save
	// always in case we are running in cluster mode. This will notify other
	// running servers that this session is being used.
	//
	// This is the ONE save in a connection's life that records the permanent fact of the
	// acceptance, committed in the same atomic batch as the record. It is therefore also
	// the last place a delayed-Will suppression has to be arranged: there is no watermark
	// to advance, no CONNACK gate on a second write, and no rollback for that write to
	// fail — the fact either commits with the record or neither exists. Every later save
	// on this CONNECT, the grant point's included, is an ordinary one. See
	// sess_accept_fact.go.
	//
	// THE ACCEPTED CONNECT MINTS A SERVING OUTBOUND AUTHORITY (F-105 cutover, row 9). The
	// ownership protocol drives
	// reserve -> commit-metadata(fenced) -> finalize, and the metadata commit is
	// saveAcceptWithOwnership, which writes the acceptance fact, the COMMITTED receipt AND the
	// session record as ONE fenced batch through persistRecord — so the record's lineage, the
	// void-register accounting, the ambiguous-write refusal and the record-tx fence all hold.
	// The authority generation IS the session generation (es.gen): a resume preserves it, a
	// clean start carries a fresh one; the metadata commit refuses a record whose generation
	// disagrees with the proposal, which is what makes them one.
	//
	// The budget is one absolute allowance the whole protocol shares; retries below consume it
	// rather than resetting it. It is constructed ABOVE the restoreSession label (ownBudget) and
	// anchored at F-051's stop, so a re-drive never re-anchors it.
	// retryOK admits ONE metadata-commit resend inside the ownership executor (Codex consult
	// 10): a same-subject CAS contention (10164) on the metadata half decided nothing, so the
	// SAME frozen attempt is re-sent rather than the whole drive re-run with a fresh proposal.
	// It draws on the same two allowances every other retry does — the shared ownership budget
	// (so a resend cannot extend the absolute deadline) and the redrive create allowance — so
	// the metadata resends and the reserve/finalize re-drives below share one budget.
	retryOK := func() error {
		if berr := mqttOwnershipStepAllowed(ownBudget, time.Now(), "metadata resend"); berr != nil {
			return berr
		}
		return rd.admitCreateRetry()
	}
	for {
		prevWID, prevSeq := es.wid, es.seq
		var driveTargets mqttCleanStartRetireTargets
		if cleanStartHandoff {
			// The predecessor identity was captured BEFORE the reset zeroed es.wid, and its
			// backing-resource targets ride the discarding handoff's closing batch (F-105 row 9).
			prevWID, prevSeq, driveTargets = cshWID, cshSeq, cshTargets
		}
		ogen, otenure, ores, oerr := es.driveAcceptToServingAuthority(acceptTok, cid, asm.sessStreamEpoch,
			!sessp, prevWID, prevSeq, driveTargets, ownBudget, time.Now, retryOK, stepAllow, ownState)
		// A REFUSAL THAT LEAVES THIS ACCEPTANCE'S OWN ABORTED PROPOSAL STANDING owes its
		// abandonment, whatever arm below ends the CONNECT: the in-place cleanup may have failed,
		// or the deadline may have cut it off. The worker runs once this connection is refused,
		// certified and holding nothing, on exactly this proposal (F-105 8a).
		if o := ores.Obs; o.Own != nil && o.Verdict == mqttMetadataAborted && !o.AbandonConfirmed {
			owedAbandon = func() { asm.abandonOwnProposal(cid, o) }
		} else {
			owedAbandon = nil
		}
		// THE RE-DRIVE IS LICENSED BY WHAT THE DRIVE ESTABLISHED, never by the identity of its
		// error (F-105 8a, consult 120). Checked before the success arm and every retry arm
		// below: an own proposal that is not COMMITTED and was not provably aborted and
		// abandoned by this acceptance itself forbids a fresh proposal, so the CONNECT refuses —
		// whatever error a handler returned. See mqttOwnObservation.forbidsRedrive.
		if ores.Outcome != mqttOwnershipServable && ores.Obs.forbidsRedrive() {
			asm.removeSession(es, true)
			if oerr == nil {
				oerr = fmt.Errorf("outbound ownership was lost (%s)", ores.Reason)
			}
			return fmt.Errorf("refusing CONNECT for %q: this acceptance's own ownership proposal forbids "+
				"proposing again: %v", cid, oerr)
		}
		if oerr == nil && ores.Outcome == mqttOwnershipServable {
			// THE CONNECTION RECORDS WHAT IT WAS ACCEPTED UNDER. The drive reports servable only for
			// the tenure it supplied, so this pair is exactly this connection's entitlement.
			c.setOutboundEntitlement(mqttOutboundEntitlement{Gen: ogen, Tenure: otenure})
			es.mu.Lock()
			es.boundEnt, es.boundEntOK = mqttOutboundEntitlement{Gen: ogen, Tenure: otenure}, true
			es.mu.Unlock()
			break
		}
		// The metadata commit's store error travels WRAPPED: persistRecord returns it as
		// "unable to persist session ownership metadata (seq=%v): %w", and the ownership
		// executor and drive pass it through unchanged. IsNatsErr does a bare type assertion
		// and cannot see a JetStream code through that wrap, so the CAS arms below classify the
		// UNWRAPPED ApiError (jsAPIError, errors.As) — otherwise a record-CAS contention on the
		// accept batch would fall through to the refusal arm rather than retrying or re-driving.
		// This is the same wrap-aware discipline the predecessor-stale arm uses via errors.Is.
		casErr := jsAPIError(oerr)
		switch {
		case casErr != nil && IsNatsErr(casErr, JSStreamWrongLastSequenceConstantErr):
			// 10164 at a RESERVE or FINALIZE step: same-subject contention on the authority
			// record, nothing decided. Re-drive the SAME protocol within the redrive budget — the
			// authority steps are idempotent re-reads, and a standing committed-but-unfinalized
			// proposal is recovered (finalized as proposed), never re-minted. The METADATA-step
			// 10164 no longer reaches here: the executor consumes it in place, re-sending the same
			// frozen attempt (retryOK) so a mere contention never re-runs the drive with a fresh
			// proposal — which would strand the reservation the first attempt already opened.
			if aerr := rd.admitCreateRetry(); aerr != nil {
				asm.detachConnectingClientForReDrive(es, c)
				return formatError(aerr)
			}
			time.Sleep(mqttRecordCreateRetryWait)
			continue
		case casErr != nil && mqttIsRecordCASRejection(casErr):
			// 10071 on the metadata commit's record CAS: a peer's record won. The executor has
			// ALREADY resolved this attempt's permanent receipt slot and settled the proposal
			// before returning here (Codex consult 10, consult-5 step 4): a resolved-COMMITTED
			// verdict never reaches this arm — it is adopted and finalized in place — and an
			// UNRESOLVED slot was aborted and its reservation closed (creation) or its handoff
			// cancelled, so no own-ABORTED proposal is left standing to be minted over. Re-read
			// from the authoritative restore, SILENTLY, and re-drive against the winner's record:
			// this is redrive arbitration against a SETTLED authority, not a fresh proposal over
			// an unresolved half.
			endAcceptTx()
			asm.detachConnectingClientForReDrive(es, c)
			if aerr := rd.admitCycle(); aerr != nil {
				return formatError(aerr)
			}
			es, exists, sessp = nil, false, false
			resumePubs, resumeRels = nil, nil
			goto restoreSession
		case oerr != nil && errors.Is(oerr, errMQTTForkFenceMoved):
			// The handoff preparation found the authority past the sequence fork repair read its
			// reinstated consumers under (F-105 7f): a write since — a disposal among the
			// possibilities — means the merge's reads no longer prove what it installed. Nothing
			// landed. NOT the inner 10164 retry, which re-drives the protocol over the same merged
			// state with the same fence and can only exhaust: the FULL re-drive discards the
			// merge with the object and re-runs fork repair on fresh reads.
			if h := mqttTestForkFenceRedriveV.Load(); h != nil {
				(*h)(cid)
			}
			endAcceptTx()
			asm.detachConnectingClientForReDrive(es, c)
			if aerr := rd.admitCycle(); aerr != nil {
				return formatError(aerr)
			}
			es, exists, sessp = nil, false, false
			resumePubs, resumeRels = nil, nil
			goto restoreSession
		case oerr != nil && errors.Is(oerr, errMQTTHandoffPredecessorStale):
			// The predecessor session record the handoff proposed to displace moved, was torn
			// down, or changed identity under the acceptance before its metadata half committed
			// — a rival winning the clear pre-fence window, most often. Nothing landed: the
			// three-member batch is atomic and its predecessor pre-check refuses before the send,
			// so sess.seq and the authority are exactly as they were. RE-DRIVE against what now
			// stands, the same disposition a record-CAS rejection gets: re-read the authoritative
			// record, re-decide resume/discard/creation, and re-drive the ownership protocol
			// against the winner. A first attempt that already reserved or prepared has aborted
			// its own metadata slot and undone that transition before returning here (the
			// executor cleans up its failed attempt), so the re-drive meets a settled authority
			// rather than a stalled proposal of its own.
			endAcceptTx()
			asm.detachConnectingClientForReDrive(es, c)
			if aerr := rd.admitCycle(); aerr != nil {
				return formatError(aerr)
			}
			es, exists, sessp = nil, false, false
			resumePubs, resumeRels = nil, nil
			goto restoreSession
		case oerr == nil:
			// The protocol resolved without a store error but did not reach a servable
			// authority — the loser bound (this acceptance's own proposal was aborted, its
			// generation closed), an expiry, or an unresolved verdict. The CONNECT is refused;
			// nothing is aborted that a later acceptance cannot settle.
			asm.removeSession(es, true)
			return fmt.Errorf("refusing CONNECT for %q: outbound ownership not established (%s)", cid, ores.Reason)
		default:
			asm.removeSession(es, true)
			return oerr
		}
	}
	// A clean-start HANDOFF has now installed the successor SERVING authority; its
	// finalization closed the predecessor generation with Discard, sealed its inventory ledger
	// and enrolled its affine target for recovery, all in the batch that displaced the record;
	// its source and PUBREL consumers were enrolled when they were provisioned. Its SHARED
	// acquisition ledger, on $MQTT_shared, is not in that batch: the recovery executor seals and
	// releases it under the same closure and certifies only once it reads sealed and empty
	// (F-105 7e). The classification transfers the rest to the recovery executor (F-105 7d) and what remains
	// here is the generation's inbound QoS 2 holds and ordinary registers, best-effort: anything
	// left is the enrolled recovery's to finish, so a cleanup error never fails a CONNECT whose
	// successor is already serving. Cleanup ownership stays partitioned; classification runs before any
	// physical delete (Codex consult 6). See sess_teardown_partition.go.
	if cleanStartHandoff {
		// The finalization committed the predecessor's discarding closure: its retirement is the
		// recovery executor's, promptly (F-105 7d).
		asm.kickRecoveryRoot(cid, asm.sessStreamEpoch, cshRetiringGen)
		target := &mqttTeardownTarget{
			id: cid, idHash: es.idHash, gen: cshRetiringGen,
			durs: cshDurs, pubRelDur: cshTargets.PubRelDurable,
			q2AffGen: cshQ2AffGen, q2AffDur: cshQ2AffDur,
		}
		if own, cerr := asm.classifyTeardownGeneration(cid, asm.sessStreamEpoch, cshRetiringGen, target); cerr != nil {
			es.jsa.c.Errorf("MQTT: clean-start predecessor classification for %q: %v", cid, cerr)
		} else {
			own.transfers(target)
			asm.reportUnenrolledTargets(cid, cshRetiringGen, own)
			if derr := es.deleteTeardownTargets(target, false); derr != nil {
				es.jsa.c.Errorf("MQTT: clean-start predecessor cleanup for %q: %v", cid, derr)
			}
		}
	}
	// THE TENURE is this acceptance's ONE tenure (F-105 8b): the authority tenure it was just
	// finalized under, ordered by the stream sequence of its own acceptance fact, read back.
	// Every shared acquisition this connection makes is composed under it, and nothing else
	// ever borrows it.
	//
	// EVERY ACCEPTANCE then projects that tenure onto the generation's acquisition ledger BEFORE
	// it serves or reconciles anything (the plan's 3.7): the record CAS that landed decides the
	// SESSION, and the installation carries that decision onto $MQTT_shared, where no $MQTT_sess
	// write reaches. A fresh generation installs too — the installation over absence is the ONLY
	// creator of a ledger, so a delayed acceptance whose successor already installed learns here
	// that it was superseded, and no join can create a ledger behind an acceptance that never
	// installed. Then the verdicts the record carries are executed, an acquisition the record
	// does not name is cancelled by a subsequent record CAS, and every committed acquisition is
	// checked against its group. A refusal — superseded by a newer tenure, a generation being
	// closed, an unreadable fact or ledger — refuses the CONNECT before the CONNACK, as every
	// other pre-CONNACK disposition does.
	es.mu.Lock()
	servingTenure := es.boundEnt.Tenure
	es.mu.Unlock()
	if terr := es.mintAcceptanceTenure(es.jsa.withDeadline(ownBudget.deadline()), servingTenure); terr != nil {
		asm.removeSession(es, true)
		return fmt.Errorf("refusing CONNECT for %q: %w", cid, terr)
	}
	if ierr := es.installAndReconcileTenure(c, acceptTok); ierr != nil {
		asm.removeSession(es, true)
		return ierr
	}
	// The connection is now accepted, and the permanent fact of that acceptance is
	// durable with it. NOTHING further is done here to suppress a prior delayed Will,
	// and the two writes that used to be are both gone:
	//
	//   - the cancellation watermark, because a single-slot marker is regressible and
	//     an erased acceptance is unrecoverable — F-031;
	//   - the direct outbox cancel, because it suppressed on reconnect ALONE, with no
	//     term for whether the Will had already become due. A reconnect after the Will
	//     Delay has elapsed does not suppress: the Will is owed ([MQTT-3.1.2-8]).
	//
	// The claimant reads the acceptance facts when the Will becomes due and applies
	// the dated predicate. That is later than a direct cancel, and it is the only
	// point at which the question "was this acceptance before the Will was owed?" can
	// be answered at all. See sess_accept_fact.go and will_outbox.go.
	//
	// Cancel any pending session-expiry timer: this connection has resumed
	// (or replaced) the session before it expired. Spec5 [3.1.2.11.2].
	asm.cancelSessionExpiry(getHash(cid))
	c.mu.Lock()
	c.flags.set(connectReceived)
	c.mqtt.cp = cp
	c.mqtt.asm = asm
	c.mqtt.sess = es
	// This connection's own copy of the interval, set in the same critical section
	// as the Will it classifies. See mqtt.sessionExpiry.
	c.mqtt.sessionExpiry = expiry
	c.mu.Unlock()

	// Rebuild the outbound in-flight identifier space from durable state (R3,
	// inflight_register.go), BEFORE the CONNACK. Two orderings depend on it: no source
	// interest may exist yet (processSubs below installs it), and recoverDrainPubRels
	// stores PUBRELs under recorded identifiers that must already be reserved.
	//
	// Before the CONNACK rather than beside those steps, for a reason that is not
	// aesthetic: a client may publish the instant its CONNACK lands, so every JetStream
	// round trip the resume still owes at that moment widens the window in which a
	// QoS 0 subscription does not yet exist and a message is simply dropped. Doing this
	// work early shortens that window. (An earlier version of this comment described the
	// window as "after the CONNACK but before processSubs", which F-050 made impossible:
	// restoration now precedes the CONNACK.) It also reads better as a rule — the
	// identifier space is reconstructed before the session is declared usable at all.
	//
	// A failure REFUSES the CONNECT: unlike a drain, whose barrier can withhold one
	// durable's interest, the identifier space is a single shared resource and half of
	// it cannot be used safely. Only reads JetStream and writes session memory, so it
	// takes no record transaction and is safe inside the accept transaction.
	//
	// From here to the success CONNACK, every failure exit goes through
	// abortAcceptedResume: the client is bound with connectReceived set and its
	// CONNECT (Will included) installed, so a bare error return would have the
	// readLoop send a v5 DISCONNECT as the FIRST packet ([MQTT-3.2.0-1]) and
	// close handling could publish the NEW connection's Will for a session that
	// was never granted. See sess_resume_reconcile.go.
	//
	// It does NOT put the session lifetime back, and does not need to: the requested
	// interval and Clean flag are not applied until the grant point below, so a resume
	// refused BEFORE that point still holds — and its record still describes — the
	// lifetime it had when this CONNECT arrived. A grant that was successfully
	// persisted deliberately stands even if the success CONNACK is then refused,
	// because it is durable and the record describes it. Spec5 [3.1.2.11.2].
	//
	// (An earlier version of this comment described lifetime restoration here and a
	// `foreign` parameter that told it whether another owner held the session. Neither
	// exists: the parameter was retained unused for the reconciliation verdicts' benefit
	// and has since been deleted, because the fact it was reserved for now arrives in a
	// typed result instead.)
	//
	// It also ABANDONS this CONNECT's restore verdict: every delivery withheld
	// because nothing may precede the CONNACK is woken so it can record itself as
	// never transmitted, and none of them is allowed out, because no CONNACK was
	// written. The verdict stays installed — a delivery arriving after this and
	// before the client detaches must still be blocked, and this helper
	// deliberately leaves c.mqtt.sess in place. See sess_restore_gate.go.
	abortAcceptedResume := func() {
		es.abandonRestoreGate()
		c.mqttAbortAcceptedConnect()
	}
	// refusePreConnack tells a v5 client WHY a pre-CONNACK refusal happened, instead of
	// closing in silence. F-050 did this for one site — a permanently unrestorable filter,
	// where an automatic client would otherwise see only an endless silent close with no hint
	// that a clean start is the recovery — and the same argument covers every other refusal on
	// this side of the acknowledgement.
	//
	// v5 ONLY, and that is a protocol limit rather than a choice: a 3.1.1 CONNACK has no reason
	// code above 5, so that connection gets the bare close it gets today.
	//
	// A failure CONNACK does not open the connection: mqttRecordConnAckWrittenLocked stores the
	// [MQTT-3.2.0-1] flag only for an accepted code with no write error, so no later takeover
	// notification is unblocked by this and the outer caller sends no DISCONNECT (the abort
	// clears connectReceived). Suppression when the frame exceeds the client's Maximum Packet
	// Size is tolerated, exactly as it already was here: the refusal proceeds regardless.
	//
	// SCOPE, and it is narrower than "every refusal": this covers the POST-ACCEPT-SAVE,
	// pre-success-CONNACK window, which is where this closure is defined and where every
	// call to it sits. Exits EARLIER in the CONNECT — an unresolved record state, a
	// post-clear failure that disposes the old client — return without a signal and are
	// deliberately out of scope, as they were for the F-050 half of this work.
	//
	// Within that window the ONE exit still silent is the failed success-CONNACK write,
	// which has no channel left to explain itself on, because the channel is what just
	// failed. The reconciliation verdicts and the lifetime grant used to be silent too,
	// for a reason that was about their own code rather than a choice: both disposed the
	// session internally before returning, so a CONNACK the caller enqueued had already
	// lost the race with its own socket close. Both now report their verdict instead.
	// See refuseDisposing.
	refusePreConnack := func(reason byte, reasonStr string) {
		if c.mqtt.proto == mqttProtoLevel5 {
			sendConnAck(reason, false, reasonStr)
		}
	}
	// refuseDisposing is the ONE pre-CONNACK refusal sequence for an outcome that
	// DISPOSES, and the order is the whole point:
	//
	//  1. abort   — discards this connection's Will and clears connectReceived, so the
	//     close scheduled in (3) cannot publish the NEW CONNECT's Will and the parser
	//     cannot send a v5 DISCONNECT as the first packet ([MQTT-3.2.0-1]);
	//  2. signal  — the failure CONNACK, enqueued BEFORE any close is scheduled;
	//  3. dispose — last, because it schedules closeConnection on another goroutine, and
	//     a CONNACK written after that has lost a race with its own socket close.
	//
	// The two arguments are two DIFFERENT facts and are not interchangeable: outcome
	// decides what the CLIENT is told, voc decides what the DISPOSAL claims. The
	// vocabulary's own Reason String never reaches the client — that separation was
	// introduced because the vocabulary then asserted a takeover the classifier never
	// established, and it is KEPT because the two are different claims on different
	// surfaces even now that F-084 has stopped every arm reaching here from asserting one.
	// See mqttPreConnackRefusal.
	//
	// mqttAbortAcceptedConnect touches only c.mqtt (the Will and connectReceived), never
	// sess.c, so the disposal's owner recheck still sees this connection as the session's.

	// The sequence is abort -> signal -> dispose, and then, through the two defers above,
	// endAcceptTx -> Will -> gate release -> notify/close.
	//
	// The head is unchanged and load-bearing: abortAcceptedResume discards THIS (arriving)
	// connection's Will, which is right because no session was ever granted to it, and it
	// must precede any scheduled close; the signal must precede the disposal. What F-084
	// adds is the DISPLACED session's Will, which this site used to drop. At this site that
	// can only ever be a parked action's discharge — the bound client is the arriving
	// connection whose Will the abort has already taken, and any previously bound client was
	// displaced upstream — so there is no live consume here.
	//
	// Nothing is added before the failure CONNACK's own unbounded socket write returns:
	// phase 1 and both defers run strictly after it.
	refuseDisposing := func(outcome mqttPreConnackOutcome, voc mqttDisposalVocabulary) {
		abortAcceptedResume()
		if r, ok := preConnackRefusalFor(outcome); ok {
			refusePreConnack(r.code, r.reasonStr)
		} else {
			// An outcome nobody gave an answer to. Say NOTHING rather than guess: a wrong
			// ownership claim is the defect this whole mechanism is about, and a bare close
			// is exactly the pre-existing behavior. Loud, because it is a programming
			// error and never a protocol state.
			c.Errorf("MQTT: refusing to resume session %q with no pre-CONNACK signal:"+
				" unhandled refusal outcome %v", cid, outcome)
		}
		disposeThroughPhases(c, voc, nil)
	}
	// ARM the verdict before the first subscription this CONNECT installs, and
	// unconditionally — a clean start and a resume naming no filters included.
	// An abandoned verdict is left installed on purpose, so the next arm is the
	// only thing that replaces it: arming conditionally would leave a reused
	// session object's later live subscriptions behind a dead verdict forever.
	es.armRestoreGate()
	// The recovery-reservation releases this resume's register walk staged, held FRAME-LOCAL
	// until the resume has actually succeeded. Recovery runs long before the success
	// CONNACK and every refusal after it preserves the session object and its maps, so
	// applying a release at the walk would free an identifier on a resume that never
	// completed. Committed below, past every refusal return (F-087).
	var stagedReleases []mqttStagedRelease
	if sessp {
		staged, err := es.recoverInflightRegisters(c)
		if err != nil {
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, err)
			abortAcceptedResume()
			refusePreConnack(mqttReasonImplementationSpecificError, err.Error())
			return formatError(err)
		}
		stagedReleases = staged
	}

	// Reconcile the plain QoS 1/2 durables the record names BEFORE the CONNACK:
	// a durable reaped by an inactivity threshold must be repaired (under a
	// fresh identity) or the CONNECT refused — never silently trusted into a
	// subscription that will not deliver. After register recovery, so every
	// in-flight register is classified against the OLD durable names before any
	// repair exists; before the CONNACK, so a failure refuses the CONNECT and
	// nothing has been transmitted when it fails. The repair save is, by
	// construction, the first record write after the accept save. See
	// sess_resume_reconcile.go.
	//
	// The phase's subscriptions are installed CLOSED and phase-owned;
	// restoration below adopts them in place. The sweep of whatever restoration
	// did not adopt has an owner on EVERY exit: it always runs right after
	// processSubs (before its error is even tested), and the deferred fallback
	// covers the exits that never reach it (the CONNACK failure and the 3.1.1
	// forced-leave failure returns). Idempotent, so the double run is harmless.
	var phaseSubs []string
	if sessp {
		r := asm.reconcileResumedConsumers(es, c, acceptTok)
		if r.outcome != mqttPreConnackProceed {
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, r.err)
			if r.outcome.disposes() {
				refuseDisposing(r.outcome, r.voc)
			} else {
				// Local and Unresolved are not ownership statements: the client gets the
				// phase's own diagnostic, and nothing is disposed.
				abortAcceptedResume()
				refusePreConnack(mqttReasonImplementationSpecificError, r.err.Error())
			}
			return formatError(r.err)
		}
		phaseSubs = r.installed
		if len(phaseSubs) > 0 {
			defer es.sweepUnadoptedPhaseSubs(c, phaseSubs)
		}
	}

	// RESTORE the subscriptions the record names, still BEFORE the CONNACK. A
	// per-filter restore failure used to be dropped in silence — no SUBACK exists
	// on this path, sess.update is skipped, and the error never reached the caller
	// — so the client got Session Present = 1 on a session whose membership was
	// durable and whose delivery path was gone, forever (F-050). Restoration is
	// therefore one more thing that can REFUSE the CONNECT, which puts it on this
	// side of the acknowledgement.
	//
	// Nothing here transmits: the restore path never serialises retained messages
	// (rmSubjects is populated only under fromSubProto), and every asynchronous
	// delivery route consults this connection's restore verdict, armed above.
	//
	// A resuming MQTT 3.1.1 client cannot express, observe or UNSUBSCRIBE a
	// "$share/..." filter, and the forced leave below removes its persisted shared
	// memberships outright — so they are not restored here either. The old code
	// achieved that by ordering (the forced leave ran first); now that restoration
	// comes earlier, the exclusion is EXPLICIT. Spec5 [4.8.2].
	restoreShared := c.mqtt.proto == mqttProtoLevel5
	// refuseRestore is refusePreConnack at the restore path's own reason code, kept as a
	// name because the restore sites read better for it. It was once the ONLY refusal in
	// this window that told the client why; every other one does now too, so what is left
	// here is the specific argument for this site. It is not decoration: a filter that can
	// never be restored — a subscribe
	// permission revoked since it was saved is the reachable case — refuses every
	// resume, and without a reason an automatic client sees only an endless silent
	// close. Its recovery, a clean start, is one the client can only choose if it
	// knows there is something to recover from. v5 only: a 3.1.1 CONNACK has no
	// reason code above 5, so that connection gets the bare close it gets today.
	// Every other refusal in the post-accept-save window now carries a reason too — that
	// generalisation has landed and is no longer an open roadmap item. Earlier exits in
	// the CONNECT are outside that window and stay silent; see refusePreConnack.
	// refuseRestore is refusePreConnack at the restore path's own reason code. Kept as a name
	// because the restore sites read better for it, not as a second mechanism.
	refuseRestore := func(rerr error) {
		refusePreConnack(mqttReasonImplementationSpecificError, rerr.Error())
	}
	if l := len(es.subs) + len(es.sharedSubs); l > 0 {
		filters := make([]*mqttFilter, 0, l)
		for subject, qos := range es.subs {
			_, noLocal := es.noLocalSubs[subject]
			_, rap := es.rapSubs[subject]
			filters = append(filters, &mqttFilter{filter: subject, qos: qos, noLocal: noLocal, retainAsPublished: rap, subID: es.subIDs[subject]})
		}
		// The persisted key is the sole authority; parse it back into group +
		// underlying subject. Spec5 [4.8.2].
		//
		// A key that does NOT parse is carried as an already-failed filter rather
		// than skipped. Skipping it is the finding this whole change is about in
		// miniature: the membership stays in the record, nothing reports a failure,
		// the aggregation below sees nothing wrong, and the client is told Session
		// Present for a subscription that will never deliver. Corruption or schema
		// drift is the way in — `mqttReservedMembershipInRecord` deliberately skips
		// malformed shared keys rather than refusing the record — so this path must
		// refuse rather than trust. A pre-failed filter reaches
		// mqttFirstUnrestoredFilter through processSubs untouched (both of its loops
		// skip a filter already at or above mqttSubAckFailure), which keeps ONE
		// refusal path rather than adding a second.
		//
		// Not needed for 3.1.1: shared filters are not restored there at all, and the
		// forced leave below removes the membership outright.
		if restoreShared {
			for key, m := range es.sharedSubs {
				group, subject, ok := mqttParseSharedKey(key)
				if !ok {
					// `reason`, not just `qos`: processSubs' first loop clamps any
					// qos above 2 down to 2 BEFORE it tests the failure sentinel, so
					// a filter marked only through qos is silently converted into a
					// QoS 2 subscription. `reason` is the field that loop reads to
					// carry a pre-decided failure through untouched.
					filters = append(filters, &mqttFilter{
						filter:    key,
						qos:       mqttSubAckFailure,
						reason:    mqttSubAckFailure,
						reasonStr: "persisted shared-subscription key is malformed",
					})
					continue
				}
				_, rap := es.rapSubs[key]
				// The RESTORE arm reuses the standing acquisition references: recreating an
				// absent consumer materializes the same incarnation and adds no acquisition.
				restored := m
				filters = append(filters, &mqttFilter{shared: true, group: group, filter: subject, qos: m.QoS, retainAsPublished: rap, subID: es.subIDs[key], sharedM: &restored})
			}
		}
		// PRE-PASS. Prove every group durable a RESIDENT shared membership is about to
		// be restored into actually exists, and carries a configuration this broker
		// recognises, before processSubs subscribes to its deliver subject (F-076).
		//
		// One batched lookup rather than one per filter: a session may hold thousands,
		// and this runs before the CONNACK on every reconnect.
		//
		// Only resident sids are in scope. A filter with no resident cons entry takes
		// the join path, which creates the durable and thereby proves it.
		sharedTargets := es.collectResidentSharedGroups(filters)
		sharedVerdicts := asm.classifySharedGroups(sharedTargets, mqttSharedGroupVerdictIsTerminal)
		// STOP HERE if the pre-pass already refused, and do not restore at all.
		//
		// The same rule classifySharedGroups applies internally, applied at the level it
		// actually governs. A refused filter makes the aggregation below refuse the
		// CONNECT no matter what happens in between, so running processSubs would install
		// subscriptions and enter create paths for non-resident filters on a connection
		// that is already lost — and would then pay a second classification pass whose
		// result cannot change the outcome either. Both are work a client can amplify by
		// holding more filters.
		//
		// The sweep still runs: the reconciliation phase installed phase-owned
		// subscriptions BEFORE this point, and they hold consumer interest until something
		// reclaims them. That is why this shares the refusal shape below rather than
		// returning early on its own.
		// The PRE-pass: a REPAIRABLE verdict — an authoritative absence — does not fail its
		// filter and does not stop the restore, because F-077's resume repair recreates the
		// durable under a membership the ownership CAS then proves is still ours. Every
		// other error is terminal exactly as before. See
		// mqttSharedGroupVerdictIsTerminal.
		failSharedGroupFilters(filters, sharedVerdicts, mqttSharedGroupVerdictIsTerminal)
		if f := mqttFirstUnrestoredFilter(filters); f != nil {
			if len(phaseSubs) > 0 {
				es.sweepUnadoptedPhaseSubs(c, phaseSubs)
			}
			rerr := fmt.Errorf("subscription %q could not be restored: %s", f.sessionKey(), f.reasonStr)
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
			abortAcceptedResume()
			refuseRestore(rerr)
			return formatError(rerr)
		}
		_, restoreFacts, serr := asm.processSubs(es, c, acceptTok, filters, false, trace, sharedVerdicts)
		// The sweep runs BEFORE the error is tested: a subscription restoration
		// skipped stays closed and phase-owned, holding consumer interest while
		// delivering nothing, and nothing later would remove it. The deferred
		// fallback above stays armed for the exits that never reach here.
		if len(phaseSubs) > 0 {
			es.sweepUnadoptedPhaseSubs(c, phaseSubs)
		}
		if serr != nil {
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, serr)
			abortAcceptedResume()
			refuseRestore(serr)
			return formatError(serr)
		}
		// A record write the restore made was refused by its per-subject CAS. Until this
		// path wrote nothing there was nothing to report and the bit was never read here;
		// the restore-side drain conversion and the teardowns behind it are record writes,
		// so their rejection is now this connection's to act on.
		//
		// It says this broker may no longer speak for the record — NOT that a successor
		// exists, which neither 10071 nor 10164 establishes on its own — so the
		// disposition is to refuse this resume rather than to dispose of anything. The
		// conversion, if it was already confirmed, is durable and is deliberately left
		// standing: it is a fact about the record, not about this connection, and the
		// client's exchanges are owed whether or not this CONNECT succeeds.
		if restoreFacts.recordCASRejected {
			rerr := fmt.Errorf("a record write of the restore of session %q was rejected by its CAS, "+
				"so this broker cannot confirm the record is still ours", cid)
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
			abortAcceptedResume()
			refuseRestore(rerr)
			return formatError(rerr)
		}
		// POST-PASS. The pre-pass observed each durable BEFORE this connection's queue
		// interest existed, so a delete or a configuration change landing in between
		// would have been accepted. Repeat the FULL classification now that interest is
		// up — not a bare presence check, because a consumer can become non-canonical
		// between the two reads — and adopt the second reading, since a member-variable
		// field may legitimately have moved.
		//
		// It sits here deliberately: after processSubs and its sweep, and immediately
		// before the aggregation below, which is the existing machinery that turns a
		// failed filter into a pre-CONNACK refusal naming it. Nothing between the two
		// transmits an MQTT packet or mutates durable state, so a refusal here costs the
		// connection and nothing else.
		//
		// This NARROWS the window; it does not close it. CONSUMER.DELETE and
		// CONSUMER.CREATE are both unconditioned, so a durable deleted or altered after
		// this reading gets no further check on this CONNECT. Stated in
		// docs/CONFORMANCE.md rather than implied away.
		if len(sharedTargets) > 0 {
			asm.mu.RLock()
			between := asm.testSharedGroupBetweenPasses
			asm.mu.RUnlock()
			if between != nil {
				between()
			}
			// The post-pass classifies STRICTLY and does it itself: every error ends the
			// restore, absence included. There is deliberately no predicate to pass here —
			// see applySharedGroupPostPass.
			applySharedGroupPostPass(asm, es, filters, sharedTargets)
		}
		// ARBITRATE. Everything above proved the durables' CONFIGURATION; nothing proved
		// this broker still OWNS the membership they were restored into (F-077). One
		// classified record write, CAS'd at sess.seq under the accept transaction, does —
		// and only a write can, because a load cannot distinguish "the record did not move"
		// from "a contender's proposal has not committed yet".
		//
		// Conditioned on interest having actually been restored, not on a durable having
		// been created: a resume that finds every durable present-and-canonical creates
		// nothing and still serves. And not on the record merely naming a shared filter: if
		// every shared filter failed before installing anything, the aggregation below is
		// already refusing this CONNECT and a write here would arbitrate nothing.
		//
		// It runs AFTER the last group-durable write of the restore — the join create and
		// the post-subscribe verify create both sit inside processSubs — and BEFORE the
		// CONNACK, so every delivery in the window is still withheld by the restore gate.
		if restoreFacts.interestRestored {
			v := es.proveSharedMembershipOwnership(acceptTok)
			mqttTestSharedArbitratePause("after-ownership-cas")
			switch v.outcome {
			case mqttGroupOwnershipOurs:
				// Still ours at the write's linearization point. Proceed.
			case mqttGroupOwnershipForeign, mqttGroupOwnershipForked:
				// A record this broker could not confirm it still owns, or a sibling that
				// erased our write. Either way this connection must not serve the group.
				//
				// It goes through refuseDisposing, the one sequence every pre-CONNACK
				// disposing refusal shares — abort, signal, dispose — so this site no longer
				// states the ordering itself and no fourth site can get it wrong. The
				// displacement stays silent (mqttNotifyDisplaced is gated on
				// c.connAckWritten, false for every pre-CONNACK refusal), so the REFUSAL is
				// what carries the reason.
				//
				// Its own verdict is mapped rather than shared: mqttGroupOwnership stays the
				// vocabulary of the shared-membership write, because F-078's live-SUBSCRIBE
				// consumer switches on it and its Foreign arm's Will handling is a different
				// question. See F-084.
				rerr := fmt.Errorf("shared subscription ownership could not be confirmed for session %q: %v", cid, v.err)
				c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
				outcome := mqttPreConnackDisposingNonForked
				if v.outcome == mqttGroupOwnershipForked {
					outcome = mqttPreConnackDisposingForked
				}
				refuseDisposing(outcome, v.voc)
				return formatError(rerr)
			case mqttGroupOwnershipUnresolved:
				// The write may have landed. The restored state is KEPT — that is what the
				// resolver re-attempts and proves durable — and no further write is
				// permitted until it settles. No SUCCESS CONNACK, so nothing is served
				// meanwhile; the failure CONNACK below opens nothing.
				rerr := fmt.Errorf("shared subscription ownership is unresolved for session %q: %v", cid, v.err)
				c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
				abortAcceptedResume()
				refusePreConnack(mqttReasonImplementationSpecificError, rerr.Error())
				return formatError(rerr)
			default: // mqttGroupOwnershipLocal
				rerr := fmt.Errorf("shared subscription ownership could not be established for session %q: %v", cid, v.err)
				c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
				abortAcceptedResume()
				refusePreConnack(mqttReasonImplementationSpecificError, rerr.Error())
				return formatError(rerr)
			}
		}
		// AGGREGATE the per-filter outcomes. This is local failure aggregation and
		// nothing more — it does not arbitrate ownership, does not prove this broker
		// still owns the record, and does not prove an installed subscription yields
		// end-to-end delivery. What it proves is that no step of this restoration
		// reported failure, which is exactly what the CONNACK is about to claim.
		//
		// One unrestored filter refuses the whole CONNECT: there is no packet on
		// this path that could tell the client WHICH filter died, which is the
		// finding. The v5 failure CONNACK is what stops a persistently unrestorable
		// filter from becoming an unexplained silent-close loop; it is legal here
		// precisely because no CONNACK has been written, so it is the first packet
		// ([MQTT-3.2.0-1]). A 3.1.1 connection gets the bare close it gets today.
		if f := mqttFirstUnrestoredFilter(filters); f != nil {
			// sessionKey rather than filter: for a shared subscription the two differ, and
			// the topic alone is ambiguous — the same client may hold both a plain and a
			// shared subscription on it, and the operator reading this needs to know which
			// one refused. For every other filter the two are identical.
			rerr := fmt.Errorf("subscription %q could not be restored: %s", f.sessionKey(), f.reasonStr)
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
			abortAcceptedResume()
			refuseRestore(rerr)
			return formatError(rerr)
		}
	}
	mqttTestRestorePauseHook("before-connack")

	// PREFLIGHT the shared memberships a 3.1.1 resume is about to force-leave, BEFORE the
	// CONNACK and before any void is staged.
	//
	// mqttSharedDurablesFor returns nil for a key it cannot parse, and a membership whose
	// acquisition references are malformed names no ledger entry: the leave would remove the
	// membership from the record while its verdicts named nothing releasable, so an
	// acquisition — and with it a holder count on some group — would be stranded with no
	// diagnostic.
	//
	// So an unparseable key REFUSES THE RESUME, and it does so here rather than at the leave:
	// this is the side of the CONNACK where refusing costs nothing the client can observe. The
	// pre-CONNACK abort discards the Will and leaves the lifetime ungranted, no acknowledged
	// session is left unusable, and being pre-CONNACK it is a site the refusal signal can
	// explain to a v5 client.
	//
	// A broker-minted key always parses (mqttFilter.sessionKey writes the shape
	// mqttParseSharedKey accepts), so this is corrupt, unsupported or out-of-band state — which
	// the compatibility boundary says to REFUSE rather than migrate or repair.
	if c.mqtt.proto != mqttProtoLevel5 {
		es.mu.Lock()
		var badKey string
		var foundBadKey bool
		for key, m := range es.sharedSubs {
			if len(mqttSharedDurablesFor(key, m)) == 0 || mqttValidateSharedMembership(key, m) != nil {
				badKey, foundBadKey = key, true
				break
			}
		}
		es.mu.Unlock()
		// A SEPARATE found flag, never `badKey != _EMPTY_`: the empty string is itself an
		// unparseable key and a shape JSON permits, so using it as the sentinel would let
		// exactly the corrupt record this preflight exists to refuse through to a success
		// CONNACK and on into destructive processing.
		if foundBadKey {
			rerr := fmt.Errorf("shared subscription %q cannot be parsed, so it cannot be released "+
				"for a 3.1.1 resume of session %q", badKey, cid)
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
			abortAcceptedResume()
			refuseRestore(rerr)
			return formatError(rerr)
		}
	}

	// GRANT the requested Session Expiry Interval and Clean flag. Everything that
	// could refuse this CONNECT has now passed, so this is the first moment the
	// session may take on what the client asked for — and it is persisted here
	// too, because the accept save deliberately carried the OLD lifetime and a
	// session whose record disagrees with its memory would be resumed, or
	// expired, on the wrong terms after a restart. Only when something actually
	// changed: an ordinary reconnect that keeps its interval writes nothing.
	//
	// A failure refuses the CONNECT and puts the two fields back. Nothing else
	// needs unwinding, because the record was never made to describe the new
	// lifetime — which is precisely what the earlier restore-after-the-fact
	// machinery existed to repair. Spec5 [3.1.2.11.2].
	//
	// The grant used to dispose the session ITSELF and return a bare error, so this
	// caller signalled 0x83 after the close was already scheduled and could not tell a
	// forked record — where 0x89 "try again" is both legal and exactly right — from
	// anything else. It now reports its verdict and this frame runs the one sequence.
	if sessp {
		if g := es.applyRequestedLifetime(acceptTok, expiry, sessClean, mqttLifetimeSpeculative); g.outcome != mqttPreConnackProceed {
			c.Errorf("MQTT: refusing to resume session %q: could not grant the requested session lifetime: %v", cid, g.err)
			if g.outcome.disposes() {
				refuseDisposing(g.outcome, g.voc)
			} else {
				abortAcceptedResume()
				refusePreConnack(mqttReasonImplementationSpecificError, g.err.Error())
			}
			return formatError(g.err)
		}
	}

	// Persist C5b's staged floor/disposition through the existing accept
	// transaction, before a success CONNACK or any source/drain activation.
	if err := es.saveDisposalRecovery(c, acceptTok); err != nil {
		abortAcceptedResume()
		refusePreConnack(mqttReasonImplementationSpecificError, err.Error())
		return formatError(err)
	}

	// Spec [MQTT-3.2.0-1]: CONNACK must be the first protocol sent to the session.
	// The entry fit check used the identical frame builder and captured values, so
	// this send is guaranteed to fit; the guard is defensive — but the WRITE can
	// still fail, and an unacknowledged session must not proceed as accepted.
	if !sendConnAck(mqttConnAckRCConnectionAccepted, sessp, _EMPTY_) {
		abortAcceptedResume()
		return fmt.Errorf("unable to deliver the success CONNACK")
	}
	mqttTestRestorePauseHook("before-open")

	// The restore verdict does NOT open here, and the delay is deliberate.
	//
	// [MQTT-3.2.0-1] forbids opening it before the CONNACK, which is why it cannot move
	// earlier. What moved is the other end: the 3.1.1 forced leave below can still REFUSE this
	// connection — a record that has demonstrably moved, or one nothing can read — and an
	// opened gate is IMMEDIATE permission for the delivery callbacks, which run on their own
	// goroutines. A transmission cannot be retracted by a refusal that comes afterwards, so the
	// gate opens only once the leave has settled.
	//
	// The cost is bounded and paid by QoS 0 alone: it has no redelivery to wait for, so
	// mqttDeliverMsgCbQoS0 DROPS while the verdict is unopened, whereas QoS 1/2, PUBREL and
	// affine deliveries wait. A dropped QoS 0 message is what QoS 0 means; serving one under a
	// membership whose ownership is still unproven is not recoverable.
	//
	// Every exit below therefore owes the verdict a terminal: open on success, abandon on
	// refusal. See sess_restore_gate.go.

	// A resuming MQTT 3.1.1 client cannot express, observe, or UNSUBSCRIBE a
	// "$share/..." filter (shared-subscription parsing is v5-only), so restoring
	// shared membership would create deliveries it can never manage and a phantom
	// holder no UNSUBSCRIBE could clear. Force-leave any persisted shared
	// subscriptions instead: remove them (transactionally) and release the group
	// consumers. Spec5 [4.8.2].
	//
	// This is broker-side hygiene the client neither asked for nor can observe, so its FAILURE
	// owes the client nothing — and the connection is refused only where refusing is the right
	// answer, never merely because a write did not succeed. The definite bucket cannot contain
	// a transient store failure at all: persistRecord arms uncertainty for every non-CAS store
	// error, so an ordinary "JetStream was briefly unavailable" is AMBIGUOUS and is resolved in
	// line below. What is left definite is 10071 (the subject moved), 10164 (contention), and
	// refusals that never reached JetStream.
	if c.mqtt.proto != mqttProtoLevel5 && len(es.sharedSubs) > 0 {
		es.mu.Lock()
		prior := es.sharedSubs
		es.sharedSubs = nil
		// In the same breath as the membership removal, and before the save that
		// persists it: see voidRegistrationsLocked.
		var leaveCancels []mqttRegCancellation
		var leaveVoided []string
		voidLeave := func(dur string) {
			cancels, sent := es.voidRegistrationsLocked(dur, mqttLicenceOf(c))
			leaveCancels = append(leaveCancels, cancels...)
			leaveVoided = append(leaveVoided, sent...)
		}
		for _, cc := range es.cons {
			if cc.DeliverGroup != _EMPTY_ {
				voidLeave(cc.Durable)
			}
		}
		for key, m := range prior {
			for _, dur := range mqttSharedDurablesFor(key, m) {
				voidLeave(dur)
			}
		}
		// And every shared durable the PENDING MAPS name, which neither view above can reach.
		// A record-restored session has membership without configs, so the cons walk names
		// nothing; a durable whose membership some earlier operation already removed is in no
		// key either. Its register is still live, and staging is the only place its verdict can
		// be written: the write that removes the membership is the write that must say "these
		// allocations were never transmitted" (voidRegistrationsLocked). Left to the commit, a
		// crash in between would leave a live register with no VoidedRegs entry and NO
		// membership able to trigger another leave — recovery would then reserve it forever.
		//
		// srcAckOwed retirements are NOT voided, and voidRegistrationsLocked skips them for its
		// own stated reason: a retirement is the durable suppression of a completed exchange,
		// not a claim on an identifier, and voiding it would have recovery drop the suppression
		// and re-offer a completed payload as a QoS 2 duplicate.
		for _, dur := range es.sharedPendingDurablesLocked() {
			voidLeave(dur)
		}
		es.mu.Unlock()
		// Everything the forced leave does once its record write is CONFIRMED. A
		// closure, because that confirmation can also arrive later from the resolver:
		// skipping it then would leave the record without the shared memberships while
		// the runtime kept their cons entries, their MaxAckPending charge and their
		// group durables — stale state a v5 re-subscribe would trip over, and leaked
		// consumers nothing releases.
		//
		// The record token is a PARAMETER, never the captured acceptTok: as a landed
		// continuation this runs under the RESOLVER'S transaction, and the CONNECT's
		// own is long closed by then — every write under it would be refused at entry.
		// Reports whether any write it performed was rejected by its per-subject CAS. The
		// caller does not act on that bool alone — recordStateBlocksServing covers the whole
		// class, ambiguity included — but returning it keeps the diagnostic honest.
		// settleBeforeRelease answers "is this session's record state still ours?" using the
		// mechanisms that can actually settle it, and it is called from inside the commit —
		// after the cleanup writes, before the first CONSUMER.DELETE.
		//
		// Refusing on an unsettled state is NOT enough, and a revision that only refused
		// recreated this item's own defect one level deeper: a cleanup write whose response was
		// merely lost would close an already-acknowledged connection and publish its Will, which
		// is exactly what the forced leave stopped doing. So an ambiguity is RESOLVED and a
		// rejection is PROBED; only a state neither can settle refuses.
		settleBeforeRelease := func(tok recordTx) mqttForcedLeaveOwnership {
			es.mu.Lock()
			uncertain := es.seqUncertain
			rejected := es.recordRejectionGatesServingLocked()
			es.mu.Unlock()
			if uncertain {
				switch res, _ := es.resolveUncertainWriteInTx(tok); res {
				case mqttUncertainOurs:
					// Settled as ours. Re-sample below: the resolution's own continuations can
					// have written again.
				case mqttUncertainForeign, mqttUncertainForked:
					return mqttForcedLeaveOwnershipForeign
				default:
					return mqttForcedLeaveOwnershipUnresolved
				}
				es.mu.Lock()
				rejected = es.recordRejectionGatesServingLocked()
				es.mu.Unlock()
			}
			if rejected {
				// The probe settles a rejection by RE-ATTEMPTING the write, never by reading the
				// record back: a readback cannot tell "somebody else moved it" from "our own
				// write landed and we lost the response".
				switch v := es.probeRecordRejectionInTx(tok); v {
				case mqttRejectionOurs:
					return mqttForcedLeaveOwnershipOurs
				case mqttRejectionForeign, mqttRejectionForked:
					return mqttForcedLeaveOwnershipForeign
				default:
					return mqttForcedLeaveOwnershipUnresolved
				}
			}
			return mqttForcedLeaveOwnershipOurs
		}
		commitLeave := func(tok recordTx, settle func(recordTx) mqttForcedLeaveOwnership) mqttForcedLeaveOwnership {
			// TWO AUTHORITIES, because "this config is doomed" and "this durable is owed a
			// release" are different questions and only one of them can be answered by either
			// structural view.
			//
			// STRUCTURAL (membership keys + cons entries) decides the doomed configs and the
			// candidates for a consumer release. For this caller the two questions collapse:
			// every membership is going, so nothing local survives to hold anything — which is
			// what licenses one set here where UNSUBSCRIBE needs a surviving-key fence.
			//
			// PENDING (the pending maps) decides the purge, and only the purge. A target found
			// only there is purged and NEVER offered for deletion: pending state is not
			// evidence that this session was the last holder of a group durable.
			es.mu.Lock()
			structural := make(map[string]string)
			for sid, cc := range es.cons {
				if cc.DeliverGroup == _EMPTY_ {
					continue
				}
				// The doomed config goes whatever happens to its durable.
				delete(es.cons, sid)
				if _, dup := structural[cc.Durable]; !dup {
					structural[cc.Durable] = cc.DeliverSubject
				}
			}
			for key, m := range prior {
				for _, t := range mqttSharedGroupNamesFor(key, m) {
					if _, dup := structural[t.durable]; !dup {
						structural[t.durable] = t.deliver
					}
				}
			}
			var regs []mqttVoidedReg
			var cancels []mqttRegCancellation
			for _, dur := range es.sharedPendingDurablesLocked() {
				if _, isStructural := structural[dur]; isStructural {
					continue
				}
				pregs, pcancels := es.purgeConsumerPendingLocked(dur, mqttLicenceOf(c))
				regs = append(regs, pregs...)
				cancels = append(cancels, pcancels...)
			}
			// Every STRUCTURAL target: CLOSE its delivery, then purge, then refund — all in this
			// one critical section, and the close strictly before the purge.
			//
			// A delivery callback takes sess.mu too, so any gap between closing and purging lets
			// one observe an open subscription, reinstate a pending entry, and head toward storing
			// a register this collection has already walked past — absent from regs, from cancels
			// and from voidedRegNonces, after which the consumer may be deleted. That is what
			// makes the durability predicate's completeness an OPERATION-level property rather
			// than a per-nonce one, and a holistic round caught it as a regression of the split.
			durs := make([]string, 0, len(structural))
			subs := make([]*subscription, 0, len(structural))
			for dur := range structural {
				durs = append(durs, dur)
			}
			es.mu.Unlock()
			// The subscription pointers are resolved under c.mu, so they cannot be taken while
			// sess.mu is held.
			for _, dur := range durs {
				deliver := structural[dur]
				var sub *subscription
				if c != nil && deliver != _EMPTY_ {
					c.mu.Lock()
					sub = c.subs[deliver]
					c.mu.Unlock()
				}
				subs = append(subs, sub)
			}
			es.mu.Lock()
			for i, dur := range durs {
				es.closeSharedGroupTargetDeliveryLocked(subs[i])
				pregs, pcancels := es.purgeConsumerPendingLocked(dur, mqttLicenceOf(c))
				regs = append(regs, pregs...)
				cancels = append(cancels, pcancels...)
				es.refundSharedGroupChargeLocked(dur)
			}
			es.mu.Unlock()
			// The network unsubscribe stays outside the lock.
			for i := range durs {
				if c != nil && subs[i] != nil {
					if uerr := c.processUnsub(subs[i].sid); uerr != nil {
						c.Warnf("MQTT: unable to remove subscription %q: %v", subs[i].sid, uerr)
					}
				}
			}

			// ONE aggregate write pair, never one per target. A per-target pair cannot be made
			// safe and was tried twice: persistRecord refuses every write AT ENTRY while the
			// session is unresolved, so if target A's fallback goes ambiguous then target B's is
			// never sent — and resolving A credits only the revision A marshalled, leaving B's
			// verdict owed while the resolution legitimately reports "ours". One write cannot
			// have that shape. This is also the form collectSharedPendingPurge already uses, so
			// it deletes a variant rather than adding a mechanism.
			casRejected := es.deleteInflightRegistersClassified(c, tok, regs, mqttLicenceOf(c))
			// The staged cancellations are already in this CONNECT's record; anything the purge
			// just found arrived after that write, and the group durables below stay while such a
			// cancellation is unaccounted for — an unexplained register costs an identifier for
			// the life of the session, a surviving durable only costs resources.
			// ONE secure call, so "one aggregate pair" is literally true: the staging's own
			// cancellations and anything the purge just found go together. Two calls was the
			// remnant of the per-target shape and a holistic round pointed out that it made the
			// singular claim false.
			secured, cancelCAS := es.secureRegCancellationsClassified(c, tok,
				append(append([]mqttRegCancellation(nil), leaveCancels...), cancels...), mqttLicenceOf(c))
			if cancelCAS {
				casRejected = true
			}
			if !secured {
				c.Errorf("MQTT: the cancelled in-flight registration(s) of client %q are not "+
					"durably recorded yet on a 3.1.1 forced leave", es.id)
			}
			// SETTLE BEFORE ANYTHING IRREVERSIBLE. Every write above is a session-record write
			// that can leave this session unresolved or arm the rejection marker, and what
			// follows is a CONSUMER.DELETE on an interest-retained stream — which destroys the
			// backlog of every member that still holds the group.
			//
			// The ownership question must be answered BEFORE that delete, not after it: noticing
			// afterwards and closing the connection is no remedy, because the delete has already
			// happened. The release verdicts this leave wrote are executable by any reader, so
			// the arbitrated count cannot be moved twice — but whether this session was
			// ENTITLED to write them is the record's question, and it is settled here.
			// ALWAYS, including on the resolver-continuation path. Passing nil there was wrong,
			// and the holistic review caught it: the resolver proved the MEMBERSHIP write, but
			// this commit then performs its own fresh cleanup writes, and
			// resolutionAfterContinuations can only downgrade the resolver's verdict AFTER the
			// commit returns — by which time the deletes have happened.
			{
				if h := mqttTestForcedLeavePreSettleHookV.Load(); h != nil {
					(*h)(es)
				}
				if v := settle(tok); v != mqttForcedLeaveOwnershipOurs {
					c.Errorf("MQTT: the shared-membership release of session %q stopped before "+
						"releasing any group consumer: its own cleanup left the record state "+
						"unsettled (%v), and a CONSUMER.DELETE here could destroy a durable "+
						"this session can no longer speak for", es.id, v)
					return v
				}
			}
			// AND every staged void verdict must be DURABLY CREDITED, which is a different
			// question from "the record is still ours" — and conflating the two is one defect
			// stated three ways across three review rounds.
			//
			// They come apart exactly when a write is refused at entry: if an earlier fallback
			// went ambiguous, persistRecord refuses the later ones, and resolving the earlier
			// one credits only the revision IT marshalled. The resolution then legitimately
			// reports "ours" while a cancellation is still owed — and deleting the consumer
			// there strands a live register with no verdict, no membership and no consumer,
			// reserving its identifier and its Receive Maximum slot for good.
			//
			// Naming the requirement is what makes this complete, where enumerating the writes
			// was not: the predicate holds whatever the commit wrote, including writes a later
			// change adds.
			if h := mqttTestForcedLeavePostSettleHookV.Load(); h != nil {
				(*h)(es)
			}
			if !es.voidVerdictsDurable() {
				c.Errorf("MQTT: the shared-membership release of session %q stopped before "+
					"releasing any group consumer: a staged in-flight void verdict is not yet "+
					"durably credited, so a delete here would strand a live register no future "+
					"leave can reach", es.id)
				return mqttForcedLeaveOwnershipUnresolved
			}
			if h := mqttTestForcedLeaveReleaseHookV.Load(); h != nil {
				(*h)()
			}
			// PHASE 3, and the only irreversible part: execute the release VERDICTS the
			// leave's record write carried, one arbitrated ledger+group CAS per acquisition,
			// retiring an incarnation whose count reaches zero. Reached only on an `ours`
			// verdict, and never a scan's answer.
			es.executeSharedReleases(c)
			if casRejected {
				c.Errorf("MQTT: a record write of the 3.1.1 forced leave of session %q was "+
					"rejected by its CAS; the session's ownership is in question", es.id)
			}
			c.Warnf("MQTT: dropped %d shared subscription(s) not supported for a 3.1.1 session resume", len(prior))
			if h := mqttTestForcedLeaveCommitHookV.Load(); h != nil {
				(*h)()
			}
			return mqttForcedLeaveOwnershipOurs
		}
		// Put the memberships back only if the removal DEFINITELY did not land. An ambiguous
		// write may already have stored the record without them, and the resolver settles it
		// forward by re-attempting exactly those bytes; restoring here would leave memory
		// holding entries the record has dropped, and the next save would resurrect a shared
		// membership the client cannot see, observe or UNSUBSCRIBE. See sess_uncertain.go.
		//
		// saveDefinitelyFailed REQUIRES this rollback, inside the still-open transaction, or
		// another writer can persist the state being undone. Un-voiding is right on these arms
		// for the same reason it always was: they all close, and the close's own
		// purgeSharedPending frees the identifiers.
		undoLeave := func() {
			es.mu.Lock()
			es.sharedSubs = prior
			es.mu.Unlock()
			es.unvoidRegNonces(leaveVoided)
		}
		// refuseLeave is the ONE exit for every arm below that cannot proceed. It owes the
		// restore verdict its terminal — and specifically abandonRestoreGate, NOT
		// abortAcceptedResume: the CONNACK is already written, so the Will may not be discarded
		// ([MQTT-3.1.2-8]) and acceptance may not be cleared. Waking the withheld deliveries is
		// what lets them record themselves as never transmitted; left parked they wait out
		// their bound, find the session detached, and their redelivery reaches the client
		// marked DUP.
		refuseLeave := func(rerr error) error {
			es.abandonRestoreGate()
			c.Errorf("MQTT: refusing the resume of session %q after its shared-membership "+
				"release: %v", cid, rerr)
			return formatError(rerr)
		}
		// The detail, not just the error: save() re-formats the store error and destroys its
		// NATS identity, and the two CAS rejections mean different things. 10071 is positive
		// evidence that the subject MOVED; 10164 is contention and decides nothing on its own.
		outcome, detail, err := es.saveClassifiedDetail(acceptTok)
		switch {
		case err == nil:
			if v := commitLeave(acceptTok, settleBeforeRelease); v != mqttForcedLeaveOwnershipOurs {
				return refuseLeave(fmt.Errorf("the shared-membership release of session %q could not "+
					"confirm the record is still ours (%v)", cid, v))
			}

		case outcome == saveMaybeLanded:
			// The write may be durable. Register the continuation FIRST — a settlement that
			// lands runs it inside settleLandedAttempt, which drains the continuations before
			// returning — then resolve in line under THIS transaction. resolveUncertainNow
			// cannot be used here and never could: it takes the ownership gate this CONNECT
			// already holds and needs a fence this frame already owns, so it returned
			// "unresolved" without attempting anything.
			es.retainOnLanded(func(rtok recordTx) {
				// The verdict is reported rather than discarded. On this path there is no
				// connection left to refuse, so the diagnostic IS the disposition — and a
				// silently dropped verdict was how a withheld release became invisible.
				if v := commitLeave(rtok, settleBeforeRelease); v != mqttForcedLeaveOwnershipOurs {
					c.Errorf("MQTT: the resolver-driven shared-membership release of session %q "+
						"withheld its group-consumer release (%v); the periodic pass retries it",
						es.id, v)
				}
			})
			res, rerr := es.resolveUncertainWriteInTx(acceptTok)
			switch res {
			case mqttUncertainOurs:
				// Settled as ours and the continuation has already run. Never call commitLeave
				// again here: the continuation is the single run path.
			case mqttUncertainForeign, mqttUncertainForked:
				// The record is not ours to write. NOT "owned elsewhere": the foreign arm found
				// a record merely not ours, and the forked arm one claiming our parent — the
				// verdict that establishes nobody demonstrably owns it. Neither names a
				// successor. F-084. The Locked form, because this frame holds
				// the ownership gate and the wrapper fails fast on a gate it finds held, which
				// would silently skip the disposition.
				es.abandonRestoreGate()
				disposeThroughPhases(c, mqttDisposalForLineage(res == mqttUncertainForked), nil)
				return formatError(fmt.Errorf("the session record of %q is no longer ours; "+
					"ownership is unsettled: %v", cid, err))
			default:
				// Unresolved: the record could not be read, so nothing further may be
				// persisted. The continuation stays parked and the periodic pass converges it.
				return refuseLeave(fmt.Errorf("the shared-membership release of session %q is "+
					"unresolved: %v (%v)", cid, err, rerr))
			}

		case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceConstantErr):
			// 10164: CONTENTION, not a verdict. The bounded re-read/retry classifies it by the
			// attempt's own identity rather than guessing from the rejection.
			switch v := es.resolveContendedRecordSave(&detail); v {
			case mqttRepairSaveLanded:
				if v := commitLeave(acceptTok, settleBeforeRelease); v != mqttForcedLeaveOwnershipOurs {
					return refuseLeave(fmt.Errorf("the shared-membership release of session %q could "+
						"not confirm the record is still ours (%v)", cid, v))
				}
			case mqttRepairSaveForeign, mqttRepairSaveForked:
				es.abandonRestoreGate()
				disposeThroughPhases(c, mqttDisposalForLineage(v == mqttRepairSaveForked), nil)
				return formatError(fmt.Errorf("the session record of %q is no longer ours; "+
					"ownership is unsettled: %v", cid, err))
			case mqttRepairSaveLocalDefinite:
				// Retries exhausted with the base sequence still ours: nothing was stored, so
				// the rollback is owed exactly as on any definite failure.
				undoLeave()
				return refuseLeave(err)
			default: // mqttRepairSaveUnresolved
				// Unclassifiable: the retry established a pending write, so the readback
				// machinery decides later. The continuation must be parked BEFORE returning or
				// a settlement that proves the removal durable would run no commit at all,
				// stranding the durables with no membership left to reach them by.
				es.retainOnLanded(func(rtok recordTx) {
					// The verdict is reported rather than discarded. On this path there is no
					// connection left to refuse, so the diagnostic IS the disposition — and a
					// silently dropped verdict was how a withheld release became invisible.
					if v := commitLeave(rtok, settleBeforeRelease); v != mqttForcedLeaveOwnershipOurs {
						c.Errorf("MQTT: the resolver-driven shared-membership release of session %q "+
							"withheld its group-consumer release (%v); the periodic pass retries it",
							es.id, v)
					}
				})
				return refuseLeave(fmt.Errorf("contention on the shared-membership release of "+
					"session %q is unresolved: %v", cid, err))
			}

		case IsNatsErr(detail.rawErr, JSStreamWrongLastSequenceErrF):
			// 10071: the subject MOVED. Classify the lineage so the client is disposed of with
			// the right vocabulary — a sibling's fork, or ownership unsettled — rather than
			// closed unclassified. Never a takeover: a moved subject says we may not write it
			// and says nothing about who may. F-084.
			undoLeave()
			es.abandonRestoreGate()
			dvoc, _ := es.disposalForDirectRejection(&detail)
			disposeThroughPhases(c, dvoc, nil)
			return formatError(fmt.Errorf("the session record of %q moved; ownership is unsettled: %v", cid, err))

		default:
			// A pre-store refusal: an earlier write already unresolved, a rejected
			// transaction, or the test hook. None reached JetStream and none is evidence about
			// ownership — but none can proceed either, so fail closed.
			undoLeave()
			return refuseLeave(err)
		}
		// The commit ran on some arm above. Its own register cleanup performs record writes, so
		// ask the SESSION whether anything it did leaves this connection unable to serve:
		// an unresolved write, or an armed rejection whose ownership question is outstanding.
		// One property rather than a result threaded up through three helpers, because it also
		// covers writes this code never named.
		if es.recordStateBlocksServing() {
			return refuseLeave(fmt.Errorf("a record write of the shared-membership release of "+
				"session %q left its state unsettled", cid))
		}
	}

	// Past every refusal this CONNECT can still make — the success CONNACK, the lifetime
	// grant, the subscription and consumer work, the forced leave's own arms and its
	// record-state check — so a recovery reservation released here is released on a resume
	// that SUCCEEDED, which is what its release rule requires (F-087).
	es.commitStagedReleases(c, stagedReleases)
	es.commitDisposalRecovery(c)

	// The leave has settled, so the restore verdict may OPEN: every delivery it withheld goes
	// out now, as the first transmission it is. Never skipped on a success, or the session's
	// backlog would sit until AckWait. See sess_restore_gate.go.
	es.openRestoreGate()

	// Subscription restoration ran BEFORE the CONNACK above, which is F-050's fix:
	// a filter whose delivery path could not be installed refuses the CONNECT
	// rather than being dropped in silence behind a Session-Present acknowledgement.
	//
	// The CONNECT's record transaction ends HERE, before the resume work below.
	// resendPendingRedelivery re-establishes the affine QoS 2 path under a
	// transaction of its own, and the fence's blocking form would deadlock against
	// one still held by this frame. Idempotent, so the deferred release is a no-op.
	endAcceptTx()

	// Restore the drains this session carries, so their started deliveries can be
	// redelivered and completed. Each of the steps below opens a record transaction
	// of its own, so they run only after the accept transaction is released, and
	// their ORDER is load-bearing (see unsub_drain.go):
	//
	//  1. make PUBREL delivery ready — needed whenever a PUBREL consumer is
	//     persisted or a QoS 2 drain was restored, and BEFORE the barrier, because
	//     $MQTT_out is interest-retained and a recreate with no consumer returns
	//     success while keeping nothing. Once per resume, not per drain: the
	//     barrier can finalize the last drain before a per-drain loop would run;
	//  2. if that fails, withhold every QoS 2 drain — without a delivery path an
	//     exchange cannot complete, so the barrier must not finalize one;
	//  3. run the past-PUBREC barrier, before any source interest exists;
	//  4. reconcile the rest against consumer state, skipping what the barrier
	//     could not resolve — a claim past PUBREC is owed a PUBREL, not a source
	//     message, and must not be pruned by the fate of its consumer;
	//  5. install source interest, then the NAKs, so a redelivery lands on a
	//     subscription that exists.
	skipDrains, pubRelReady := make(map[string]bool), true
	if es.needsPubRelDelivery() {
		if err := es.ensurePubRelConsumerSubscription(c); err != nil {
			c.Errorf("Unable to re-establish PUBREL delivery on resume: %v", err)
			pubRelReady = false
			for _, dur := range es.qos2DrainDurables() {
				skipDrains[dur] = true
			}
		}
	}
	if pubRelReady {
		skipDrains = es.recoverDrainPubRels(c)
	}
	es.recoverDrains(c, skipDrains)
	es.restoreDrains(c, skipDrains)
	// And every release request the record carried: its obligation is to the client's
	// identifier, so it may not wait for a source delivery that no longer has to come.
	es.enrolRecoveredDrainReleases(c)

	asm.beforeResumeNaks(cid)
	// Re-send unacked QoS 1/2 messages (and reconcile any that expired while
	// offline) on resume, whether or not saved subscriptions were restored: a
	// session can carry unacknowledged PUBRELs with no active subscription.
	es.resendPendingRedelivery(c, resumePubs, resumeRels)
	// And the restored RETAINED exchanges (F-105 7c): replayed from their verified copies and
	// canonicals, since no JetStream redelivery exists to carry them.
	es.replayRecoveredRetained(c)
	// A RESUMED generation settles what its displaced tenures left undecided or unretired
	// (F-105 7f): this accept is what fenced them out.
	if sessp {
		es.startTenureSettlement(c)
	}
	return nil
}
