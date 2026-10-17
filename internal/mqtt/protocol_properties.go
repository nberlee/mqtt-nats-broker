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

// MQTT 5.0 properties: the property set, its codec and per-packet allowance, the forwarded
// properties carried in NATS headers (message expiry, subscription identifiers, topic alias),
// and the negotiated receive and topic-alias maxima.
//
// References to "spec5" here are from
// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html

package mqtt

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// mqttProperties holds the parsed MQTT 5.0 variable-header properties for a
// packet. Only the fields needed by the current implementation are tracked;
// recognized-but-unused properties are validated then discarded. Spec
// [MQTT-2.2.2].
type mqttProperties struct {
	sessionExpiry    uint32
	receiveMax       uint16
	maxPacketSize    uint32
	topicAliasMax    uint16
	topicAlias       uint16
	willDelay        uint32
	messageExpiry    uint32
	serverKeepAlive  uint16
	maxQoS           byte
	payloadFormat    byte
	requestProblem   byte
	requestResponse  byte
	retainAvailable  byte
	wildcardSubAvail byte
	subIDAvail       byte
	sharedSubAvail   byte
	subIDs           []int
	assignedClientID string
	authMethod       string
	contentType      string
	responseTopic    string
	responseInfo     string
	reasonString     string
	correlationData  []byte
	authData         []byte
	user             []mqttUserProperty
	// Tracks which scalar properties were present, so we can reject illegal
	// duplicates and tell "absent" apart from "explicit zero".
	present map[byte]bool
}

type mqttUserProperty struct {
	key   string
	value string
}

// mqttValidateForwardProps returns raw only if it is a well-formed MQTT 5.0
// properties block that is safe to forward to a subscriber: it must parse
// cleanly with no trailing bytes and must not contain a Topic Alias (hop-by-hop,
// never forwarded) or a Subscription Identifier (per-hop and server-generated at
// delivery time; a publisher-supplied block must never smuggle one, so it is
// disallowed in the inbound PUBLISH context). Otherwise it returns nil so the
// message is delivered without properties. This protects against a crafted
// Nmqtt-Props header from a non-MQTT publisher making the server emit a malformed
// or forbidden PUBLISH.
func mqttValidateForwardProps(raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	r := &mqttReader{}
	r.reset(raw)
	props, err := r.readProperties(mqttPacketPub)
	if err != nil || r.hasMore() {
		return nil
	}
	// Subscription Identifier is already rejected by readProperties (not allowed
	// in the PUBLISH context); Topic Alias parses but must not be forwarded.
	if props != nil && props.present[mqttPropTopicAlias] {
		return nil
	}
	return raw
}

// mqttVarIntFromSlice decodes an MQTT variable byte integer from the front of b.
// It returns the value and the number of bytes consumed, or (0, -1) if b does
// not begin with a complete, valid varint. Spec5 [1.5.5].
func mqttVarIntFromSlice(b []byte) (int, int) {
	m, v := 1, 0
	for n := 0; n < len(b) && n < 4; n++ {
		d := b[n]
		v += int(d&0x7f) * m
		if d&0x80 == 0 {
			return v, n + 1
		}
		m *= 0x80
	}
	return 0, -1
}

// mqttPropValueOffset walks a well-formed MQTT 5.0 properties block (length
// prefix + body) and returns the byte offset of the value of the first
// occurrence of property target, or -1 if the block carries no such property or
// cannot be walked. The block is expected to have already been validated (by
// readProperties / mqttValidateForwardProps), so property lengths are trusted;
// anything unexpected returns -1 rather than risk a bad rewrite.
func mqttPropValueOffset(props []byte, target byte) int {
	plen, n := mqttVarIntFromSlice(props)
	if n < 0 {
		return -1
	}
	i, end := n, n+plen
	if end > len(props) {
		return -1
	}
	for i < end {
		prop := props[i]
		i++
		valueOff := i
		switch prop {
		case mqttPropPayloadFormat, mqttPropRequestProblemInfo, mqttPropRequestResponseInfo,
			mqttPropMaxQoS, mqttPropRetainAvailable, mqttPropWildcardSubAvailable,
			mqttPropSubIDAvailable, mqttPropSharedSubAvailable:
			i++
		case mqttPropServerKeepAlive, mqttPropReceiveMaximum, mqttPropTopicAliasMax,
			mqttPropTopicAlias:
			i += 2
		case mqttPropMessageExpiry, mqttPropSessionExpiry, mqttPropWillDelay, mqttPropMaxPacketSize:
			i += 4
		case mqttPropContentType, mqttPropResponseTopic, mqttPropAssignedClientID,
			mqttPropAuthMethod, mqttPropResponseInfo, mqttPropServerReference,
			mqttPropReasonString, mqttPropCorrelationData, mqttPropAuthData:
			if i+2 > end {
				return -1
			}
			i += 2 + int(binary.BigEndian.Uint16(props[i:]))
		case mqttPropUserProperty:
			// Two length-prefixed strings (key then value).
			for k := 0; k < 2; k++ {
				if i+2 > end {
					return -1
				}
				i += 2 + int(binary.BigEndian.Uint16(props[i:]))
			}
		case mqttPropSubscriptionID:
			_, m := mqttVarIntFromSlice(props[i:])
			if m < 0 {
				return -1
			}
			i += m
		default:
			return -1
		}
		if i > end {
			return -1
		}
		if prop == target {
			return valueOff
		}
	}
	return -1
}

// mqttMessageExpiry returns the Message Expiry Interval (seconds) carried in a
// raw MQTT 5.0 properties block and whether the property was present. The two
// differ semantically: an absent property means the message never expires,
// whereas an explicit value of 0 means it expires immediately. Spec5 [3.3.2.3.3].
func mqttMessageExpiry(props []byte) (uint32, bool) {
	off := mqttPropValueOffset(props, mqttPropMessageExpiry)
	if off < 0 {
		return 0, false
	}
	return binary.BigEndian.Uint32(props[off:]), true
}

// mqttMessageExpiryTTL returns the JetStream per-message TTL (seconds) to apply
// to a stored message given its properties: 0 when the message has no expiry
// (never reaped by TTL), otherwise at least 1 (JetStream's minimum). Flooring an
// explicit expiry of 0 to 1s ensures an "expire immediately" message is still
// reaped rather than kept forever; onward delivery is additionally gated by
// mqttForwardExpiry. Spec5 [3.3.2.3.3].
func mqttMessageExpiryTTL(props []byte) uint32 {
	if me, ok := mqttMessageExpiry(props); ok {
		if me < 1 {
			return 1
		}
		return me
	}
	return 0
}

// mqttSetMessageExpiry returns a copy of the raw MQTT 5.0 properties block with
// the Message Expiry Interval value replaced by rem. If the block carries no
// Message Expiry Interval, props is returned unchanged (no copy), preserving the
// verbatim fast path for messages that do not use expiry. Spec5 [3.3.2.3.3].
func mqttSetMessageExpiry(props []byte, rem uint32) []byte {
	off := mqttPropValueOffset(props, mqttPropMessageExpiry)
	if off < 0 {
		return props
	}
	out := make([]byte, len(props))
	copy(out, props)
	binary.BigEndian.PutUint32(out[off:], rem)
	return out
}

// mqttForwardExpiry computes the outbound properties for a stored v5 PUBLISH,
// honoring the MQTT 5.0 Message Expiry Interval given the message's JetStream
// store time (storeUnixNano). It returns the (possibly rewritten) properties and
// whether the message has already expired. Spec5 [3.3.2.3.3]: the PUBLISH sent
// onward MUST carry the interval reduced by the time waited in the server; if
// the interval has elapsed before onward delivery the server MUST NOT deliver
// the message and MUST drop its stored copy. Returns props unchanged when there
// is no expiry to adjust. Elapsed time is measured on this server's wall clock;
// as with JetStream per-message TTLs, accuracy across a cluster assumes
// synchronized clocks.
func mqttForwardExpiry(props []byte, storeUnixNano int64) (out []byte, expired bool) {
	off := mqttPropValueOffset(props, mqttPropMessageExpiry)
	if off < 0 {
		return props, false
	}
	orig := int64(binary.BigEndian.Uint32(props[off:]))
	// An explicit interval of 0 means the message expires immediately: it is
	// already expired for any onward (stored) delivery, whatever the elapsed
	// time. Checked before the elapsed/store-time guards so a same-second
	// delivery cannot forward it. Spec5 [3.3.2.3.3].
	if orig == 0 {
		return props, true
	}
	if storeUnixNano <= 0 {
		return props, false
	}
	now := time.Now()
	if h := mqttTestExpiryNowV.Load(); h != nil {
		now = (*h)()
	}
	elapsed := (now.UnixNano() - storeUnixNano) / int64(time.Second)
	if elapsed <= 0 {
		return props, false
	}
	if elapsed >= orig {
		return props, true
	}
	out = make([]byte, len(props))
	copy(out, props)
	binary.BigEndian.PutUint32(out[off:], uint32(orig-elapsed))
	return out, false
}

// mqttInjectSubID returns a raw v5 properties block (variable-int length prefix
// + body) equal to props with a Subscription Identifier property (0x0B, varint
// id) appended to the body and the length prefix re-encoded, synthesizing a
// fresh block when props is empty. Offset-based rewrites (mqttSetMessageExpiry /
// mqttForwardExpiry) must run on the original block first; only a Topic Alias
// (mqttInjectTopicAlias) may be appended after this. props is returned unchanged
// when the id is out of range, the block is unsafe to forward (see
// mqttValidateForwardProps), or the injected block would exceed the maximum
// varint length. Spec5 [3.8.2.1.2].
func mqttInjectSubID(props []byte, id int) []byte {
	if id <= 0 || id > mqttMaxVarInt {
		return props
	}
	// The Subscription Identifier property: id 0x0B followed by its varint value.
	suffix := newMQTTWriter(5)
	suffix.WriteByte(mqttPropSubscriptionID)
	suffix.WriteVarInt(id)
	sb := suffix.Bytes()

	// An absent (nil) or empty (single 0 length byte) block: synthesize one that
	// carries just the identifier. Checked before validation so an empty input is
	// not mistaken for an invalid block.
	var body []byte
	if n := len(props); n > 1 {
		// A publisher-supplied block must be safe to forward verbatim before we
		// extend it (WritePublishHeader writes the result unchanged); a valid
		// length prefix can still wrap an invalid or unsafe body.
		if mqttValidateForwardProps(props) == nil {
			return props
		}
		plen, off := mqttVarIntFromSlice(props)
		if off < 0 || off+plen != n {
			return props
		}
		body = props[off:]
	}

	newLen := len(body) + len(sb)
	if newLen > mqttMaxVarInt {
		return props
	}
	w := newMQTTWriter(5 + newLen)
	w.WriteVarInt(newLen)
	w.Write(body)
	w.Write(sb)
	return w.Bytes()
}

// mqttInjectTopicAlias returns a raw v5 properties block (varint length prefix +
// body) equal to props with a Topic Alias property (0x23, 2-byte value) appended
// and the length prefix re-encoded, synthesizing a fresh block when props is
// nil/empty. ok is false (props returned unchanged) when the block cannot be
// safely extended; the caller must then send the full topic unaliased rather
// than an empty-topic frame with no alias. Unlike mqttInjectSubID it performs
// only the structural prefix/length check, NOT mqttValidateForwardProps: it runs
// after mqttInjectSubID, whose output legitimately carries a Subscription
// Identifier the forward validator rejects, and every block reaching here was
// already validated upstream. Caller precondition: props must not already carry
// a Topic Alias (this appends blindly) — inbound aliases are stripped before
// forwarding and mqttValidateForwardProps rejects NATS-carried blocks bearing
// one, so no current caller violates this. Spec5 [3.3.2.3.4].
func mqttInjectTopicAlias(props []byte, alias uint16) ([]byte, bool) {
	// The Topic Alias property: id 0x23 followed by its 2-byte value.
	sb := []byte{mqttPropTopicAlias, byte(alias >> 8), byte(alias)}

	// An absent (nil) or empty (single 0 length byte) block synthesizes one that
	// carries just the alias; a longer block must have a length prefix consistent
	// with its size before we extend it.
	var body []byte
	if n := len(props); n > 1 {
		plen, off := mqttVarIntFromSlice(props)
		if off < 0 || off+plen != n {
			return props, false
		}
		body = props[off:]
	}

	newLen := len(body) + len(sb)
	if newLen > mqttMaxVarInt {
		return props, false
	}
	w := newMQTTWriter(5 + newLen)
	w.WriteVarInt(newLen)
	w.Write(body)
	w.Write(sb)
	return w.Bytes(), true
}

// mqttStripTopicAlias returns a copy of the raw properties block (varint length
// prefix + body, the pp.props format) with the Topic Alias property (id +
// 2-byte value) removed and the length prefix re-encoded. It returns props
// unchanged when no alias is present, and nil when the alias was the only
// property (an empty block is never captured). Topic aliases are hop-local and
// MUST NOT be forwarded to subscribers or into retained storage. The result is
// always a fresh allocation, which doubles as the required copy out of the
// transient reader buffer. Spec5 [3.3.2.3.4].
func mqttStripTopicAlias(props []byte) []byte {
	off := mqttPropValueOffset(props, mqttPropTopicAlias)
	if off < 0 {
		return props
	}
	_, n := mqttVarIntFromSlice(props)
	// Drop the 3 alias bytes: the id byte at off-1 and its 2-byte value. The
	// body is everything after the length prefix minus those 3 bytes.
	newLen := len(props) - n - 3
	if newLen <= 0 {
		return nil
	}
	w := newMQTTWriter(5 + newLen)
	w.WriteVarInt(newLen)
	w.Write(props[n : off-1])
	w.Write(props[off+2:])
	return w.Bytes()
}

// mqttTopicAliasMax returns the effective MQTT 5.0 Topic Alias Maximum from
// Options: 0 (unset) => mqttDefaultTopicAliasMax, negative (explicitly
// disabled) => 0, otherwise the configured value. Spec5 [3.2.2.3.2].
func mqttTopicAliasMax(opts *Options) uint16 {
	tam := opts.MQTT.TopicAliasMaximum
	switch {
	case tam == 0:
		return mqttDefaultTopicAliasMax
	case tam < 0:
		return 0
	default:
		return uint16(tam)
	}
}

// mqttReceiveMax returns the effective MQTT 5.0 Receive Maximum from Options: 0
// (unset) => mqttDefaultReceiveMax, negative (explicitly disabled) => 0,
// otherwise the configured value. Spec5 [3.2.2.3.3].
func mqttReceiveMax(opts *Options) uint16 {
	rm := opts.MQTT.ReceiveMaximum
	switch {
	case rm == 0:
		return mqttDefaultReceiveMax
	case rm < 0:
		return 0
	default:
		return uint16(rm)
	}
}

// mqttPayloadFormatIsUTF8 reports whether a raw MQTT 5.0 properties block carries
// a Payload Format Indicator of 1 (the payload is declared UTF-8). Spec5
// [3.3.2.3.2].
func mqttPayloadFormatIsUTF8(props []byte) bool {
	off := mqttPropValueOffset(props, mqttPropPayloadFormat)
	return off >= 0 && off < len(props) && props[off] == 1
}

// mqttPropertyKnown reports whether prop is a property identifier defined by
// MQTT 5.0. Used to give a clearer error for genuinely unknown identifiers.
func mqttPropertyKnown(prop byte) bool {
	switch prop {
	case mqttPropPayloadFormat, mqttPropMessageExpiry, mqttPropContentType,
		mqttPropResponseTopic, mqttPropCorrelationData, mqttPropSubscriptionID,
		mqttPropSessionExpiry, mqttPropAssignedClientID, mqttPropServerKeepAlive,
		mqttPropAuthMethod, mqttPropAuthData, mqttPropRequestProblemInfo,
		mqttPropWillDelay, mqttPropRequestResponseInfo, mqttPropResponseInfo,
		mqttPropServerReference, mqttPropReasonString, mqttPropReceiveMaximum,
		mqttPropTopicAliasMax, mqttPropTopicAlias, mqttPropMaxQoS,
		mqttPropRetainAvailable, mqttPropUserProperty, mqttPropMaxPacketSize,
		mqttPropWildcardSubAvailable, mqttPropSubIDAvailable, mqttPropSharedSubAvailable:
		return true
	}
	return false
}

// mqttPropsContextWill is a pseudo "packet type" used to validate Will
// properties, which travel inside a CONNECT payload but have their own allowed
// property set. It cannot collide with real packet types (multiples of 0x10).
const mqttPropsContextWill = byte(0x01)

// mqttPropsContextPubOut is a pseudo "packet type" for a server-to-client
// (outbound) PUBLISH, which unlike an inbound PUBLISH may carry a server-injected
// Subscription Identifier and a server-assigned Topic Alias. Used to
// parse/validate delivered PUBLISH blocks (e.g. in tests). It cannot collide
// with real packet types (multiples of 0x10).
const mqttPropsContextPubOut = byte(0x02)

// mqttPropertyAllowed reports whether property prop may appear in the given
// context (a packet type, or mqttPropsContextWill / mqttPropsContextPubOut).
// Spec5 [2.2.2.2] table of which properties belong to which packets.
func mqttPropertyAllowed(prop, ctx byte) bool {
	switch prop {
	case mqttPropPayloadFormat, mqttPropMessageExpiry, mqttPropContentType,
		mqttPropResponseTopic, mqttPropCorrelationData:
		return ctx == mqttPacketPub || ctx == mqttPropsContextWill || ctx == mqttPropsContextPubOut
	case mqttPropSubscriptionID:
		// A Subscription Identifier is valid on a SUBSCRIBE and, from the server,
		// on a delivered PUBLISH (mqttPropsContextPubOut). A client-to-server
		// PUBLISH MUST NOT carry one (spec5 [MQTT-3.3.4-6]); inbound PUBLISH
		// properties and forwarded blocks are validated with the mqttPacketPub
		// context, so a client-supplied identifier is still rejected.
		return ctx == mqttPacketSub || ctx == mqttPropsContextPubOut
	case mqttPropSessionExpiry:
		return ctx == mqttPacketConnect || ctx == mqttPacketConnectAck || ctx == mqttPacketDisconnect
	case mqttPropAssignedClientID, mqttPropServerKeepAlive, mqttPropResponseInfo,
		mqttPropMaxQoS, mqttPropRetainAvailable, mqttPropWildcardSubAvailable,
		mqttPropSubIDAvailable, mqttPropSharedSubAvailable:
		return ctx == mqttPacketConnectAck
	case mqttPropAuthMethod, mqttPropAuthData:
		return ctx == mqttPacketConnect || ctx == mqttPacketConnectAck || ctx == mqttPacketAuth
	case mqttPropRequestProblemInfo, mqttPropRequestResponseInfo:
		return ctx == mqttPacketConnect
	case mqttPropWillDelay:
		return ctx == mqttPropsContextWill
	case mqttPropServerReference:
		return ctx == mqttPacketConnectAck || ctx == mqttPacketDisconnect
	case mqttPropReasonString:
		switch ctx {
		case mqttPacketConnectAck, mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubRel,
			mqttPacketPubComp, mqttPacketSubAck, mqttPacketUnsubAck, mqttPacketDisconnect,
			mqttPacketAuth:
			return true
		}
		return false
	case mqttPropReceiveMaximum, mqttPropTopicAliasMax, mqttPropMaxPacketSize:
		return ctx == mqttPacketConnect || ctx == mqttPacketConnectAck
	case mqttPropTopicAlias:
		// Valid on a client PUBLISH and, from the server, on a delivered PUBLISH
		// (mqttPropsContextPubOut). mqttValidateForwardProps still rejects a
		// publisher-smuggled alias via the mqttPacketPub context.
		return ctx == mqttPacketPub || ctx == mqttPropsContextPubOut
	case mqttPropUserProperty:
		// Allowed in every packet that carries a properties section.
		return true
	}
	return false
}

// readProperties parses an MQTT 5.0 properties block from the reader for the
// given context (packet type or mqttPropsContextWill). The whole packet is
// already buffered by the dispatch loop, so the var-int length must complete.
// It validates length, identifiers, packet-context, and illegal duplicates.
// Returns nil (no error) when the block is present but empty.
// readPropString reads a UTF-8 Encoded String property value and enforces the
// well-formedness rules of Spec5 [MQTT-1.5.4-1]/[MQTT-1.5.4-2] (valid UTF-8, no
// U+0000). Ill-formed values are a Malformed Packet; without this check the
// server would relay protocol-violating bytes on to subscribers.
func (r *mqttReader) readPropString(field string) (string, error) {
	s, err := r.readString(field)
	if err != nil {
		return _EMPTY_, err
	}
	if err = mqttValidateString(s, field); err != nil {
		return _EMPTY_, fmt.Errorf("%w: %v", errMQTTMalformedProperties, err)
	}
	return s, nil
}

func (r *mqttReader) readProperties(ctx byte) (*mqttProperties, error) {
	plen, complete, err := r.readVarInt()
	if err != nil {
		return nil, err
	}
	if !complete {
		return nil, errMQTTMalformedProperties
	}
	if plen == 0 {
		return nil, nil
	}
	end := r.pos + plen
	if end > len(r.buf) {
		return nil, errMQTTMalformedProperties
	}
	props := &mqttProperties{present: make(map[byte]bool)}
	for r.pos < end {
		prop, err := r.readByte("property id")
		if err != nil {
			return nil, err
		}
		if !mqttPropertyAllowed(prop, ctx) {
			if !mqttPropertyKnown(prop) {
				return nil, fmt.Errorf("%w: 0x%x", errMQTTUnknownProperty, prop)
			}
			return nil, fmt.Errorf("%w: 0x%x", errMQTTPropertyNotAllowed, prop)
		}
		// All properties are non-repeatable except User Property and
		// Subscription Identifier. Spec5 [2.2.2.1].
		if prop != mqttPropUserProperty && prop != mqttPropSubscriptionID {
			if props.present[prop] {
				return nil, fmt.Errorf("%w: 0x%x", errMQTTDuplicateProperty, prop)
			}
			props.present[prop] = true
		}
		switch prop {
		case mqttPropPayloadFormat:
			props.payloadFormat, err = r.readByte("payload format indicator")
			if err == nil && props.payloadFormat > 1 {
				// Spec5 [3.3.2.3.2]: only 0 or 1 are defined.
				err = fmt.Errorf("%w: payload format indicator %d", errMQTTProtocolError, props.payloadFormat)
			}
		case mqttPropRequestProblemInfo:
			props.requestProblem, err = r.readByte("request problem information")
			if err == nil && props.requestProblem > 1 {
				// Spec5 [3.1.2.11.7]: a value other than 0 or 1 is a Protocol Error.
				err = fmt.Errorf("%w: request problem information %d", errMQTTProtocolError, props.requestProblem)
			}
		case mqttPropRequestResponseInfo:
			props.requestResponse, err = r.readByte("request response information")
			if err == nil && props.requestResponse > 1 {
				// Spec5 [3.1.2.11.6]: a value other than 0 or 1 is a Protocol Error.
				err = fmt.Errorf("%w: request response information %d", errMQTTProtocolError, props.requestResponse)
			}
		case mqttPropMaxQoS:
			props.maxQoS, err = r.readByte("maximum qos")
		case mqttPropRetainAvailable:
			props.retainAvailable, err = r.readByte("retain available")
		case mqttPropWildcardSubAvailable:
			props.wildcardSubAvail, err = r.readByte("wildcard subscription available")
		case mqttPropSubIDAvailable:
			props.subIDAvail, err = r.readByte("subscription identifier available")
		case mqttPropSharedSubAvailable:
			props.sharedSubAvail, err = r.readByte("shared subscription available")
		case mqttPropServerKeepAlive:
			props.serverKeepAlive, err = r.readUint16("server keep alive")
		case mqttPropReceiveMaximum:
			if props.receiveMax, err = r.readUint16("receive maximum"); err == nil && props.receiveMax == 0 {
				// Spec5 [3.1.2.11.3]: a Receive Maximum value of 0 is a Protocol
				// Error. (Absent means no property, i.e. the default, which we
				// treat as no client-imposed limit.)
				err = fmt.Errorf("%w: receive maximum of 0", errMQTTProtocolError)
			}
		case mqttPropTopicAliasMax:
			props.topicAliasMax, err = r.readUint16("topic alias maximum")
		case mqttPropTopicAlias:
			props.topicAlias, err = r.readUint16("topic alias")
		case mqttPropMessageExpiry:
			props.messageExpiry, err = r.readUint32("message expiry interval")
		case mqttPropSessionExpiry:
			props.sessionExpiry, err = r.readUint32("session expiry interval")
		case mqttPropWillDelay:
			props.willDelay, err = r.readUint32("will delay interval")
		case mqttPropMaxPacketSize:
			// The client is telling us the largest packet it will accept. We honor
			// this on every server-generated packet: an outbound PUBLISH exceeding
			// it is discarded (mqttEnqueuePublishMsgTo, serializeRetainedMsgsForSub),
			// and any other packet drops its Reason String or, failing that, is not
			// sent (mqttEnqueueFittingProto). Stored as c.mqtt.maxPacketSize. Spec5
			// [3.1.2.11.4].
			if props.maxPacketSize, err = r.readUint32("maximum packet size"); err == nil && props.maxPacketSize == 0 {
				// Spec5 [3.1.2.11.4]: a Maximum Packet Size of 0 is a Protocol
				// Error. (Absent means no property, i.e. no client-imposed limit.)
				err = fmt.Errorf("%w: maximum packet size of 0", errMQTTProtocolError)
			}
		case mqttPropContentType:
			props.contentType, err = r.readPropString("content type")
		case mqttPropResponseTopic:
			props.responseTopic, err = r.readPropString("response topic")
			if err == nil && strings.ContainsAny(props.responseTopic, "+#") {
				// Spec5 [3.3.2.3.5]: the Response Topic must not contain
				// wildcard characters; treat it as a Protocol Error.
				err = fmt.Errorf("%w: wildcard in response topic %q", errMQTTProtocolError, props.responseTopic)
			}
		case mqttPropAssignedClientID:
			props.assignedClientID, err = r.readPropString("assigned client identifier")
		case mqttPropAuthMethod:
			props.authMethod, err = r.readPropString("authentication method")
		case mqttPropResponseInfo:
			props.responseInfo, err = r.readPropString("response information")
		case mqttPropServerReference:
			_, err = r.readPropString("server reference")
		case mqttPropReasonString:
			props.reasonString, err = r.readPropString("reason string")
		case mqttPropCorrelationData:
			props.correlationData, err = r.readBytes("correlation data", true)
		case mqttPropAuthData:
			props.authData, err = r.readBytes("authentication data", true)
		case mqttPropSubscriptionID:
			var v int
			var complete bool
			v, complete, err = r.readVarInt()
			if err == nil && !complete {
				err = errMQTTMalformedProperties
			}
			if err == nil {
				// Spec5 [3.3.2.3.8]/[3.8.2.1.2]: a value of 0 is a Protocol Error.
				if v == 0 {
					return nil, fmt.Errorf("%w: subscription identifier of 0", errMQTTProtocolError)
				}
				props.subIDs = append(props.subIDs, v)
			}
		case mqttPropUserProperty:
			var k, val string
			if k, err = r.readPropString("user property key"); err == nil {
				val, err = r.readPropString("user property value")
			}
			if err == nil {
				props.user = append(props.user, mqttUserProperty{key: k, value: val})
			}
		}
		if err != nil {
			return nil, err
		}
	}
	// A property that lied about its length will have advanced past the
	// declared end; reject such packets as malformed. Spec5 [2.2.2.1].
	if r.pos != end {
		return nil, errMQTTMalformedProperties
	}
	return props, nil
}

// encode writes the set properties as a properties body (without the leading
// length). Scalar/byte/int properties are written only when flagged present;
// strings/slices when non-empty. Used by the server to emit properties.
func (p *mqttProperties) encode(w *mqttWriter) {
	if p == nil {
		return
	}
	wb := func(id, v byte) {
		w.WriteByte(id)
		w.WriteByte(v)
	}
	w16 := func(id byte, v uint16) {
		w.WriteByte(id)
		w.WriteUint16(v)
	}
	w32 := func(id byte, v uint32) {
		w.WriteByte(id)
		w.WriteUint32(v)
	}
	if p.present[mqttPropPayloadFormat] {
		wb(mqttPropPayloadFormat, p.payloadFormat)
	}
	if p.present[mqttPropMessageExpiry] {
		w32(mqttPropMessageExpiry, p.messageExpiry)
	}
	if p.present[mqttPropContentType] {
		w.WriteByte(mqttPropContentType)
		w.WriteString(p.contentType)
	}
	if p.present[mqttPropResponseTopic] {
		w.WriteByte(mqttPropResponseTopic)
		w.WriteString(p.responseTopic)
	}
	if p.present[mqttPropCorrelationData] {
		w.WriteByte(mqttPropCorrelationData)
		w.WriteBytes(p.correlationData)
	}
	if p.present[mqttPropSessionExpiry] {
		w32(mqttPropSessionExpiry, p.sessionExpiry)
	}
	if p.present[mqttPropReceiveMaximum] {
		w16(mqttPropReceiveMaximum, p.receiveMax)
	}
	if p.present[mqttPropMaxQoS] {
		wb(mqttPropMaxQoS, p.maxQoS)
	}
	if p.present[mqttPropRetainAvailable] {
		wb(mqttPropRetainAvailable, p.retainAvailable)
	}
	if p.present[mqttPropMaxPacketSize] {
		w32(mqttPropMaxPacketSize, p.maxPacketSize)
	}
	if p.assignedClientID != _EMPTY_ {
		w.WriteByte(mqttPropAssignedClientID)
		w.WriteString(p.assignedClientID)
	}
	if p.responseInfo != _EMPTY_ {
		w.WriteByte(mqttPropResponseInfo)
		w.WriteString(p.responseInfo)
	}
	if p.present[mqttPropTopicAliasMax] {
		w16(mqttPropTopicAliasMax, p.topicAliasMax)
	}
	if p.present[mqttPropServerKeepAlive] {
		w16(mqttPropServerKeepAlive, p.serverKeepAlive)
	}
	if p.reasonString != _EMPTY_ {
		w.WriteByte(mqttPropReasonString)
		w.WriteString(p.reasonString)
	}
	if p.present[mqttPropWildcardSubAvailable] {
		wb(mqttPropWildcardSubAvailable, p.wildcardSubAvail)
	}
	if p.present[mqttPropSubIDAvailable] {
		wb(mqttPropSubIDAvailable, p.subIDAvail)
	}
	if p.present[mqttPropSharedSubAvailable] {
		wb(mqttPropSharedSubAvailable, p.sharedSubAvail)
	}
	for _, up := range p.user {
		w.WriteByte(mqttPropUserProperty)
		w.WriteString(up.key)
		w.WriteString(up.value)
	}
}

// writeProperties writes a length-prefixed MQTT 5.0 properties block. A nil
// (or empty) set is encoded as a single 0 length byte, which is mandatory in
// every v5 packet that has a properties section. Spec5 [2.2.2.1].
func (w *mqttWriter) writeProperties(p *mqttProperties) {
	if p == nil {
		w.WriteVarInt(0)
		return
	}
	body := newMQTTWriter(0)
	p.encode(body)
	b := body.Bytes()
	w.WriteVarInt(len(b))
	w.Write(b)
}
