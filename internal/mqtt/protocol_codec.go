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

// MQTT packet framing and parsing: the reader and writer, the per-connection parse loop, the
// fixed-header and remaining-length checks, packet-identifier packets, QoS and retain bits,
// Sparkplug B topic parsing, PINGRESP, and trace helpers.

package mqtt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt/v2"
)

type mqttConnectProto struct {
	rd    time.Duration
	ka    uint16 // client-requested keep alive (seconds), 0 = none
	will  *mqttWill
	flags byte
	// MQTT 5.0 CONNECT properties (nil for 3.1.1).
	props *mqttProperties
}

type mqttIOReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

type mqttReader struct {
	reader mqttIOReader
	buf    []byte
	pos    int
	pstart int
	pbuf   []byte
	owned  bool // buf is backed by pbuf, not the caller's read buffer
}

type mqttWriter struct {
	bytes.Buffer
}

// Parse protocols inside the given buffer.
// This is invoked from the readLoop.
func (c *client) mqttParse(buf []byte) error {
	c.mu.Lock()
	s := c.srv
	trace := c.trace
	connected := c.flags.isSet(connectReceived)
	mqtt := c.mqtt
	r := mqtt.r
	var rd time.Duration
	if mqtt.cp != nil {
		rd = mqtt.cp.rd
		if rd > 0 {
			r.reader.SetReadDeadline(time.Time{})
		}
	}
	hasMappings := c.in.flags.isSet(hasMappings)
	c.mu.Unlock()

	r.reset(buf)

	var err error
	var b byte
	var pl int
	var complete bool

	// This packet's completion lease (F-036 unit 1). Reassigned per iteration and
	// released at the end of the iteration; the deferred release below covers every
	// early exit out of the middle of an iteration — the rejection gate's error and
	// displaced arms, the SUBSCRIBE live-arbitration disposal, and DISCONNECT — since
	// release is idempotent and only the CURRENT iteration's lease can still be held.
	var pktLease *mqttLease
	defer func() {
		if pktLease != nil {
			pktLease.release()
		}
	}()

	for err == nil && r.hasMore() {

		// Keep track of the starting of the packet, in case we have a partial
		r.pstart = r.pos

		// Read packet type and flags
		if b, err = r.readByte("packet type"); err != nil {
			break
		}

		// Packet type
		pt := b & mqttPacketMask

		// If client was not connected yet, the first packet must be
		// a mqttPacketConnect otherwise we fail the connection.
		if !connected && pt != mqttPacketConnect {
			// If the buffer indicates that it may be a websocket handshake
			// but the client is not websocket, it means that the client
			// connected to the MQTT port instead of the Websocket port.
			if bytes.HasPrefix(buf, []byte("GET ")) && !c.isWebsocket() {
				err = errMQTTNotWebsocketPort
			} else {
				err = fmt.Errorf("the first packet should be a CONNECT (%v), got %v", mqttPacketConnect, pt)
			}
			break
		}
		if err = mqttCheckFixedHeaderFlags(pt, b&mqttPacketFlagMask); err != nil {
			break
		}

		maxLen := c.mqttInboundPacketBound(connected)
		pl, complete, err = r.readPacketLen(pt, c.mqtt.proto, maxLen)
		if err != nil || !complete {
			if err == ErrMaxPayload {
				// Tell a connected v5 client why before the close (no-op for 3.1.1
				// and pre-CONNECT); maxPayloadViolation then logs and closes.
				if connected {
					c.mqttEnqueueDisconnect(mqttReasonPacketTooLarge,
						fmt.Sprintf("packet size %d exceeds maximum packet size %d", pl, maxLen))
				}
				c.maxPayloadViolation(pl, maxLen)
			}
			break
		}

		// ACQUIRE THIS PACKET'S COMPLETION LEASE on the incarnation this connection
		// was bound to, before the rejection gate and before any mutable session
		// observation (F-036 unit 1). A displaced connection whose session object has
		// since been re-minted holds a RETIRED tracker, so it is refused here rather
		// than serving a packet under a generation that is not the one it was admitted
		// under — which is how an acknowledged QoS 2 message was silently lost.
		//
		// Per PACKET, not per read and not per connection: the readLoop holds no
		// standing ownership, so a quiet displaced connection cannot hold a takeover
		// off. While the tracker is CLOSING the packet parks on that cycle's decision —
		// a reopened transition (drain ceiling, or an unresolved-state refusal) resumes
		// serving the incumbent, a completed one refuses.
		if connected {
			if pktLease != nil {
				// Defensive: the previous iteration's release is idempotent, so this
				// only matters if a future edit adds a path that skips the epilogue.
				pktLease.release()
			}
			var ok bool
			if pktLease, ok = c.acquirePacketLease(); !ok {
				err = errMQTTTrackerRetired
				break
			}
		}

		// A definitely rejected record CAS means the session subject MOVED, so this
		// broker's ownership belief may be obsolete — and BEFORE DISPATCH is the only
		// place that can act on it: a QoS 1 PUBLISH routed and acked from a displaced
		// connection cannot be retracted afterwards. Per packet rather than per read,
		// because a save the PREVIOUS packet in this same buffer triggered (an
		// UNSUBSCRIBE's voided-register flush) can arm the marker between the two.
		if connected {
			if proceed, serr := c.mqttGateDispatchOnRejection(pt); !proceed {
				if serr != nil {
					// No verdict: fail closed through the ordinary parse-error path, so
					// the connection is really torn down and the close bookkeeping runs.
					err = serr
					break
				}
				// Foreign or displaced: this connection has been disposed, its one
				// DISCONNECT attempted (or deliberately suppressed — pre-CONNACK, or
				// already claimed) and closed. Return as the graceful DISCONNECT branch
				// does: an error here would put the footer in the race for the REASON
				// the client is left with — the claim would refuse whichever attempt is
				// second, so the cost is a wrong reason rather than a second packet.
				// The footer's own pre-CONNACK exposure is closed elsewhere, by
				// mqttAbortAcceptedConnect clearing connectReceived.
				return nil
			}
		}

		switch pt {
		// Packets that we receive back when we act as the "sender": PUBACK,
		// PUBREC, PUBCOMP.
		case mqttPacketPubAck:
			var pi uint16
			pi, _, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBACK", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubAck(pi)
			}

		case mqttPacketPubRec:
			var pi uint16
			var reason byte
			pi, reason, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBREC", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubRec(pi, reason)
			}

		case mqttPacketPubComp:
			var pi uint16
			pi, _, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBCOMP", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				c.mqttProcessPubComp(pi)
			}

		// Packets where we act as the "receiver": PUBLISH, PUBREL, SUBSCRIBE, UNSUBSCRIBE.
		case mqttPacketPub:
			pp := c.mqtt.pp
			pp.flags = b & mqttPacketFlagMask
			err = c.mqttParsePub(r, pl, pp, hasMappings)
			if trace {
				c.traceInOp("PUBLISH", errOrTrace(err, mqttPubTrace(pp)))
				if err == nil {
					c.mqttTraceMsg(pp.msg)
				}
			}
			if err == nil {
				err = s.mqttProcessPub(c, pp, trace)
			}

		case mqttPacketPubRel:
			var pi uint16
			pi, _, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBREL", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = s.mqttProcessPubRel(c, pi, trace)
			}

		case mqttPacketSub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			var subs []*subscription
			pi, filters, err = c.mqttParseSubs(r, b, pl)
			if trace {
				c.traceInOp("SUBSCRIBE", errOrTrace(err, mqttSubscribeTrace(pi, filters)))
			}
			if err == nil {
				var arb mqttSubArbitration
				subs, arb, err = c.mqttProcessSubs(filters)
				if arb.disposed {
					// The live arbitration proved this connection foreign: it has
					// already been notified and closed, on this readLoop, by the
					// packet's own tail. Return as the graceful DISCONNECT branch
					// does — an error here would put the footer in the race for the
					// REASON the client is left with, and the connection's one
					// DISCONNECT claim refuses whichever attempt is second, so the
					// cost would be a WRONG reason rather than a second packet. The
					// same shape mqttGateDispatchOnRejection uses above. See
					// sub_operation_gate.go (F-078).
					return nil
				}
				// NOT called when the live tail owned this packet's ambiguity: it
				// resolved under the ownership gate it was already holding, and a
				// second attempt here would either be a no-op or hand the generic
				// resolver — whose forked arm DISPOSES — a session the live matrix
				// says must not be disposed.
				if !arb.settledOwnAmbiguity {
					c.mqttResolveAfterPacket()
				}
			}
			if err == nil {
				// Trace only if the SUBACK was actually sent (a tiny client
				// Maximum Packet Size can suppress it).
				if c.mqttEnqueueSubAck(pi, filters) && trace {
					c.traceOutOp("SUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
				c.mqttSendRetainedMsgsToNewSubs(subs)
			}

		case mqttPacketUnsub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			pi, filters, err = c.mqttParseUnsubs(r, b, pl)
			if trace {
				c.traceInOp("UNSUBSCRIBE", errOrTrace(err, mqttUnsubscribeTrace(pi, filters)))
			}
			var unsubAckRestore *mqttUnsubAckRestore
			if err == nil {
				unsubAckRestore, err = c.mqttProcessUnsubs(filters)
				c.mqttResolveAfterPacket()
				// Unconditional: an UNSUBSCRIBE that cancelled a register-pending
				// delivery leaves the cleanup obligation with the delivery goroutine,
				// which was refused the fence while this packet held it — and that is
				// true whether the packet itself succeeded.
				c.mqttFlushVoidedRegs()
			}
			if err == nil {
				// After the resolution above, because an ambiguous write proven
				// durable by it turns this packet's 0x83 reasons back into the true
				// ones. Trace only if the UNSUBACK was actually sent (a tiny client
				// Maximum Packet Size can suppress it, and a displaced connection is
				// not acknowledged at all).
				if c.mqttAckUnsubscribe(pi, filters, unsubAckRestore) && trace {
					c.traceOutOp("UNSUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
			}

		// Packets that we get both as a receiver and sender: PING, CONNECT, DISCONNECT
		case mqttPacketPing:
			if trace {
				c.traceInOp("PINGREQ", nil)
			}
			c.mqttEnqueuePingResp()
			if trace {
				c.traceOutOp("PINGRESP", nil)
			}

		case mqttPacketConnect:
			// It is an error to receive a second connect packet
			if connected {
				err = errMQTTSecondConnectPacket
				break
			}
			var rc byte
			var cp *mqttConnectProto
			var sessp bool
			start := r.pos
			rc, cp, err = c.mqttParseConnect(r, hasMappings)
			// CONNECT has no trailing length check of its own; a lying
			// properties length would silently desync the packet stream.
			if err == nil && rc == 0 && r.pos-start != pl {
				err = fmt.Errorf("connect packet length mismatch: consumed %v bytes, remaining length is %v", r.pos-start, pl)
			}
			// Add the client id to the client's string, regardless of error.
			// We may still get the client_id if the call above fails somewhere
			// after parsing the client ID itself.
			c.ncs.Store(fmt.Sprintf("%s - %q", c, c.mqtt.cid))
			if trace && cp != nil {
				c.traceInOp("CONNECT", errOrTrace(err, c.mqttConnectTrace(cp)))
			}
			if rc != 0 {
				var rs string
				if err != nil {
					rs = err.Error()
				}
				sent, _ := c.mqttEnqueueConnAck(rc, sessp, rs)
				if sent && trace {
					c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
				}
			} else if err != nil {
				// A malformed CONNECT has no return code. v5 expects a CONNACK
				// with the failure reason before the close; 3.1.1 just closes.
				// Spec5 [3.1.4.1], [MQTT-3.1.2-19].
				if c.mqtt.proto == mqttProtoLevel5 {
					reason := mqttConnAckReasonFromConnectErr(err)
					sent, _ := c.mqttEnqueueConnAck(reason, false, err.Error())
					if sent && trace {
						c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", false, reason)))
					}
				}
			} else {
				if err = s.mqttProcessConnect(c, cp, trace); err != nil {
					err = fmt.Errorf("unable to connect: %v", err)
					// If the success CONNACK already went out, mark connected so
					// the v5 error path below sends a DISCONNECT with the reason
					// instead of a bare close. Read under the client lock: the
					// writeLoop mutates c.flags while flushing that CONNACK.
					c.mu.Lock()
					connected = c.flags.isSet(connectReceived)
					c.mu.Unlock()
				} else {
					// Add this debug statement so users running in Debug mode
					// will have the client id printed here for the first time.
					c.Debugf("Client connected")
					connected = true
					rd = cp.rd
				}
			}

		case mqttPacketDisconnect:
			// By default a DISCONNECT discards the Will. Spec [MQTT-3.1.2-8].
			discardWill := true
			if c.mqtt.proto == mqttProtoLevel5 && pl > 0 {
				// v5 DISCONNECT carries a reason code and optional properties.
				// Spec5 [3.14]. Reason 0x04 ("Disconnect with Will Message")
				// instructs the server to publish the Will instead.
				var rcode byte
				if rcode, err = r.readByte("disconnect reason code"); err != nil {
					break
				}
				var props *mqttProperties
				if pl > 1 {
					if props, err = r.readProperties(mqttPacketDisconnect); err != nil {
						break
					}
				}
				if rcode == mqttReasonDisconnectWithWill {
					discardWill = false
				}
				// Spec5 [3.14.2.2.2]: a DISCONNECT may update the Session Expiry
				// Interval. Raising it from 0 is a Protocol Error; ignore that
				// override rather than fail an already-closing connection.
				if props != nil && props.present[mqttPropSessionExpiry] {
					c.mu.Lock()
					// The connection's own copy moves with it, whether or not it still
					// owns the session below: it is what classifies this connection's
					// Will. Same Protocol Error rule, applied to its own value.
					if c.mqtt.sessionExpiry != 0 || props.sessionExpiry == 0 {
						c.mqtt.sessionExpiry = props.sessionExpiry
						// The client's explicit instruction for how this session ends,
						// recorded as an OBLIGATION of this accepted connection rather
						// than applied to the session here -- which is what the code
						// this replaced did, under sess.c == c, leaving sess.clean
						// behind and reaching $MQTT_sess never (F-041). The close
						// applies it under the ownership gate, inside a record
						// transaction, and every park carries it. nil means this
						// connection ends on whatever the session was granted.
						//
						// Nothing session-scoped is written here any more, so no
						// ownership test is needed: a late DISCONNECT from a displaced
						// connection records an obligation on ITS OWN client struct,
						// and its close then finds the session rebound and does nothing
						// with it. Spec5 [3.14.2.2.2].
						overridden := props.sessionExpiry
						c.mqtt.lifetimeOverride = &overridden
					}
					c.mu.Unlock()
				}
			}
			if trace {
				c.traceInOp("DISCONNECT", nil)
			}
			if discardWill {
				c.mqttDiscardWill()
			} else {
				// Reason 0x04 makes the Will owed from this point, recorded before
				// the teardown below runs and before any same-client-ID CONNECT can
				// take the session over: from here the Will is no longer a takeover's
				// to discard. Spec5 [3.14.4].
				c.mqttOweWill(c.mqttSessionExpiry())
			}
			// RELEASE BEFORE THE TEARDOWN. mqttHandleClosedClient runs the discard,
			// which closes this incarnation's tracker and waits for it to drain — so
			// holding this packet's own lease into it would make the graceful
			// DISCONNECT wait for itself until the ceiling (F-036 unit 1). Idempotent,
			// so the deferred release stays correct.
			if pktLease != nil {
				pktLease.release()
				pktLease = nil
			}
			s.mqttHandleClosedClient(c)
			c.closeConnection(ClientClosed)
			return nil

		default:
			err = fmt.Errorf("%w: received unknown packet type %d", errMQTTMalformedPacket, pt>>4)
		}

		// The packet's synchronous extent — its route, its helper tails and its
		// response enqueue — is over, so its ownership ends here. An effect it
		// handed to the transport is NOT an owner: its target was frozen while this
		// lease was held, and its safety is a property of that target rather than of
		// this ledger (F-036 unit 1; see sess_lease.go).
		if pktLease != nil {
			pktLease.release()
			pktLease = nil
		}
	}
	if err == nil && rd > 0 {
		r.reader.SetReadDeadline(time.Now().Add(rd))
	} else if err != nil && err != ErrMaxPayload && connected && c.mqtt.proto == mqttProtoLevel5 {
		// Best-effort: tell the v5 client why we are about to close. The read
		// loop closes the connection right after this returns. ErrMaxPayload is
		// excluded: it already sent a DISCONNECT 0x95 and closed above.
		c.mqttEnqueueDisconnect(mqttDisconnectReasonFromErr(err), err.Error())
	}
	return err
}

func mqttCheckFixedHeaderFlags(packetType, flags byte) error {
	var expected byte
	switch packetType {
	case mqttPacketConnect, mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubComp,
		mqttPacketPing, mqttPacketDisconnect:
		expected = 0
	case mqttPacketPubRel, mqttPacketSub, mqttPacketUnsub:
		expected = 0x2
	case mqttPacketPub:
		return nil
	default:
		return nil
	}
	if flags != expected {
		return fmt.Errorf("%w: invalid fixed header flags %x for packet type %x", errMQTTMalformedPacket, flags, packetType)
	}
	return nil
}

// mqttInboundPacketBound is the largest full packet this connection may claim in
// a fixed header; a larger claim is refused before its body is buffered, so a
// client cannot make the broker hold a packet it will never accept (F-120).
// Before CONNECT it is max_payload. A connected MQTT 5.0 client is held to the
// Maximum Packet Size its CONNACK advertised, and to nothing else: the enforced
// value IS the advertised one [MQTT-3.2.2-15]; 0 (nothing advertised) occurs only
// when max_payload exceeds what an MQTT packet can encode, where the variable
// byte maximum is already the tighter bound. A connected MQTT 3.1.1 client has no
// Maximum Packet Size, so it is held to max_payload plus what a packet carries
// that is not payload (mqttConnectedPacketOverhead): no PUBLISH whose payload fits
// max_payload is refused here, and the complete-message max_payload check in
// mqttProcessPub remains the payload's own bound. jwt.NoLimit stays unlimited.
func (c *client) mqttInboundPacketBound(connected bool) int32 {
	mpay := atomic.LoadInt32(&c.mpay)
	if !connected {
		return mpay
	}
	if c.mqtt.proto == mqttProtoLevel5 {
		if mps := c.mqtt.maxInPacketSize; mps > 0 {
			return int32(mps)
		}
		return jwt.NoLimit
	}
	if mpay == jwt.NoLimit || mpay > math.MaxInt32-mqttConnectedPacketOverhead {
		return jwt.NoLimit
	}
	return mpay + mqttConnectedPacketOverhead
}

func mqttCheckRemainingLength(packetType, proto byte, pl int) error {
	v5 := proto == mqttProtoLevel5
	var expected int
	switch packetType {
	case mqttPacketConnect, mqttPacketPub, mqttPacketSub, mqttPacketUnsub:
		return nil
	case mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubRel, mqttPacketPubComp:
		if v5 {
			// v5: packet identifier (2) + optional reason code + optional
			// properties. Spec5 [3.4.2.1].
			if pl < 2 {
				return fmt.Errorf("%w: invalid remaining length %d for packet type %x", errMQTTMalformedPacket, pl, packetType)
			}
			return nil
		}
		expected = 2
	case mqttPacketDisconnect:
		if v5 {
			// v5: 0 (normal disconnect) or reason code + optional properties.
			// Spec5 [3.14.2.1].
			return nil
		}
		expected = 0
	case mqttPacketPing:
		expected = 0
	default:
		return nil
	}
	if pl != expected {
		return fmt.Errorf("%w: invalid remaining length %d for packet type %x", errMQTTMalformedPacket, pl, packetType)
	}
	return nil
}

func (c *client) mqttTraceMsg(msg []byte) {
	maxTrace := c.srv.getOpts().MaxTracedMsgLen
	if maxTrace > 0 && len(msg) > maxTrace {
		c.Tracef("<<- MSG_PAYLOAD: [\"%s...\"]", msg[:maxTrace])
	} else {
		c.Tracef("<<- MSG_PAYLOAD: [%q]", msg)
	}
}

func mqttParsePIPacket(r *mqttReader) (uint16, error) {
	pi, err := r.readUint16("packet identifier")
	if err != nil {
		return 0, err
	}
	if pi == 0 {
		return 0, errMQTTPacketIdentifierIsZero
	}
	return pi, nil
}

// mqttParsePubResponsePacket parses an inbound PUBACK/PUBREC/PUBREL/PUBCOMP. In
// MQTT 5.0 these packets may carry a reason code and a properties block after
// the packet identifier. The reason code is returned so the caller can honor
// failure codes (e.g. a PUBREC >= 0x80 must not be answered with a PUBREL,
// Spec5 [4.3.3]); an absent reason code (short form, or 3.1.1) is Success
// (0x00). It consumes exactly pl bytes so the reader is positioned at the start
// of the next packet. Spec5 [3.4.2.1] and following.
func mqttParsePubResponsePacket(r *mqttReader, proto byte, pl int) (uint16, byte, error) {
	end := r.pos + pl
	pi, err := mqttParsePIPacket(r)
	if err != nil {
		return 0, 0, err
	}
	reason := mqttReasonSuccess
	if proto == mqttProtoLevel5 {
		// Optional reason code (1 byte).
		if r.pos < end {
			if reason, err = r.readByte("reason code"); err != nil {
				return 0, 0, err
			}
		}
		// Optional properties. The allowed property set (Reason String, User
		// Property) is identical across the four pub-response packets, so the
		// PUBACK context is sufficient for validation.
		if r.pos < end {
			if _, err = r.readProperties(mqttPacketPubAck); err != nil {
				return 0, 0, err
			}
		}
	}
	if r.pos != end {
		return 0, 0, fmt.Errorf("%w: invalid remaining length %d for pub-response packet", errMQTTMalformedPacket, pl)
	}
	return pi, reason, nil
}

// Return the QoS from the given PUBLISH protocol's flags
func mqttGetQoS(flags byte) byte {
	return flags & mqttPubFlagQoS >> 1
}

func mqttIsRetained(flags byte) bool {
	return flags&mqttPubFlagRetain != 0
}

func sparkbParseBirthDeathTopic(topic []byte) (isBirth, isDeath, isCertificate bool) {
	if bytes.HasPrefix(topic, sparkbCertificatesTopicPrefix) {
		isCertificate = true
		topic = topic[len(sparkbCertificatesTopicPrefix):]
	}
	if !bytes.HasPrefix(topic, sparkbNamespaceTopicPrefix) {
		return false, false, false
	}
	topic = topic[len(sparkbNamespaceTopicPrefix):]

	parts := bytes.Split(topic, []byte{'/'})
	if len(parts) < 3 || len(parts) > 4 {
		return false, false, false
	}
	typ := bytesToString(parts[1])
	switch typ {
	case sparkbNBIRTH, sparkbDBIRTH:
		isBirth = true
	case sparkbNDEATH, sparkbDDEATH:
		isDeath = true
	default:
		return false, false, false
	}
	return isBirth, isDeath, isCertificate
}
func (c *client) mqttEnqueuePingResp() {
	// 2 bytes; always fits a connected client's Maximum Packet Size, checked for
	// uniformity. [MQTT-3.1.2-24].
	c.mqttEnqueueFittingProto(mqttPingResponse)
}
func errOrTrace(err error, trace string) []byte {
	if err != nil {
		return []byte(err.Error())
	}
	return []byte(trace)
}
func (r *mqttReader) reset(buf []byte) {
	r.owned = len(r.pbuf) > 0
	if r.owned {
		// Append to the partial packet, so a trickled packet is not copied in full
		// on every read: growth is amortized, total copying linear (F-120).
		buf = append(r.pbuf, buf...)
		r.pbuf = nil
	}
	r.buf = buf
	r.pos = 0
	r.pstart = 0
}

func (r *mqttReader) hasMore() bool {
	return r.pos != len(r.buf)
}

func (r *mqttReader) readByte(field string) (byte, error) {
	if r.pos == len(r.buf) {
		return 0, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.EOF)
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// readPacketLen reads a packet's Remaining Length and reports whether the whole
// packet is in the buffer, keeping a partial one in pbuf. A claimed length is
// judged before anything is buffered (F-120): first against maxLen, so a v5
// packet over the advertised Maximum Packet Size is Packet Too Large (0x95) even
// when its length is also wrong for its type, then against the length its type
// fixes (mqttCheckRemainingLength), so a PINGREQ claiming a body is refused at
// its fixed header rather than after the body arrives.
func (r *mqttReader) readPacketLen(pt, proto byte, maxLen int32) (int, bool, error) {
	v, complete, err := r.readVarInt()
	if err != nil {
		return 0, false, err
	}
	if complete {
		packetEnd := r.pos + v
		packetLen := packetEnd - r.pstart
		if maxLen != jwt.NoLimit && int64(packetLen) > int64(maxLen) {
			return packetLen, false, ErrMaxPayload
		}
		if err = mqttCheckRemainingLength(pt, proto, v); err != nil {
			return 0, false, err
		}
		if packetEnd <= len(r.buf) {
			return v, true, nil
		}
	}
	// Keep our own buffer only while it holds nothing but this partial packet: the
	// caller's buffer is the readLoop's, recycled on the next read, and a buffer
	// with a parsed packet before the partial would be parsed again and may back
	// slices readBytes already handed out.
	if r.owned && r.pstart == 0 {
		r.pbuf = r.buf
	} else {
		r.pbuf = copyBytes(r.buf[r.pstart:])
	}
	return 0, false, nil
}

func (r *mqttReader) readVarInt() (int, bool, error) {
	m := 1
	v := 0
	for {
		var b byte
		if r.pos != len(r.buf) {
			b = r.buf[r.pos]
			r.pos++
		} else {
			break
		}
		v += int(b&0x7f) * m
		if (b & 0x80) == 0 {
			return v, true, nil
		}
		m *= 0x80
		if m > 0x200000 {
			return 0, false, errMQTTMalformedVarInt
		}
	}
	return 0, false, nil
}

func (r *mqttReader) readString(field string) (string, error) {
	var s string
	bs, err := r.readBytes(field, false)
	if err == nil {
		s = string(bs)
	}
	return s, err
}

func (r *mqttReader) readBytes(field string, cp bool) ([]byte, error) {
	luint, err := r.readUint16(field)
	if err != nil {
		return nil, err
	}
	l := int(luint)
	if l == 0 {
		return nil, nil
	}
	start := r.pos
	if start+l > len(r.buf) {
		return nil, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.ErrUnexpectedEOF)
	}
	r.pos += l
	b := r.buf[start:r.pos]
	if cp {
		b = copyBytes(b)
	}
	return b, nil
}

func (r *mqttReader) readUint16(field string) (uint16, error) {
	if len(r.buf)-r.pos < 2 {
		return 0, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.ErrUnexpectedEOF)
	}
	start := r.pos
	r.pos += 2
	return binary.BigEndian.Uint16(r.buf[start:r.pos]), nil
}

func (r *mqttReader) readUint32(field string) (uint32, error) {
	if len(r.buf)-r.pos < 4 {
		return 0, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.ErrUnexpectedEOF)
	}
	start := r.pos
	r.pos += 4
	return binary.BigEndian.Uint32(r.buf[start:r.pos]), nil
}
func (w *mqttWriter) WriteUint16(i uint16) {
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func (w *mqttWriter) WriteUint32(i uint32) {
	w.WriteByte(byte(i >> 24))
	w.WriteByte(byte(i >> 16))
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func (w *mqttWriter) WriteString(s string) {
	w.WriteBytes([]byte(s))
}

func (w *mqttWriter) WriteBytes(bs []byte) {
	w.WriteUint16(uint16(len(bs)))
	w.Write(bs)
}

func (w *mqttWriter) WriteVarInt(value int) {
	for {
		b := byte(value & 0x7f)
		value >>= 7
		if value > 0 {
			b |= 0x80
		}
		w.WriteByte(b)
		if value == 0 {
			break
		}
	}
}

func newMQTTWriter(cap int) *mqttWriter {
	w := &mqttWriter{}
	w.Grow(cap)
	return w
}
