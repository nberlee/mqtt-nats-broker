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

// CONNACK: its reason codes, the v5 CONNACK frame, writing and recording it, and the
// pre-CONNACK refusals.

package mqtt

import (
	"errors"
	"time"
)

// mqttConnAckReasonFromRC maps a 3.1.1 CONNACK return code to the equivalent
// MQTT 5.0 reason code. Codes already in the v5 failure range (>= 0x80) are
// passed through, which lets callers inject v5-only reasons (e.g. bad auth
// method) without a 3.1.1 equivalent. Spec5 [3.2.2.2].
func mqttConnAckReasonFromRC(rc byte) byte {
	switch rc {
	case mqttConnAckRCConnectionAccepted:
		return mqttReasonSuccess
	case mqttConnAckRCUnacceptableProtocolVersion:
		return mqttReasonUnsupportedProtocolVersion
	case mqttConnAckRCIdentifierRejected:
		return mqttReasonClientIDNotValid
	case mqttConnAckRCServerUnavailable:
		return mqttReasonServerUnavailable
	case mqttConnAckRCBadUserOrPassword:
		return mqttReasonBadUserOrPassword
	case mqttConnAckRCNotAuthorized:
		return mqttReasonNotAuthorized
	}
	if rc >= 0x80 {
		return rc
	}
	return mqttReasonUnspecifiedError
}

// mqttEnqueueConnAck enqueues a CONNACK and reports whether it was sent. A v5
// CONNACK is suppressed (returns false) when it cannot fit the client's Maximum
// Packet Size even after dropping its Reason String; the 3.1.1 path always sends.
// It returns whether the CONNACK was sent (false only when a v5 frame exceeds
// the client's Maximum Packet Size even after dropping its Reason String) and
// the socket write error, so the caller can decide its abort discipline on a
// write failure.
//
// A clean success CONNACK records connAckWritten HERE, inside the same c.mu
// critical section as the write, not in the caller after the lock is released.
// The flag is the [MQTT-3.2.0-1] gate every takeover notification consults
// (mqttNotifyTakenOver reads it under c.mu): storing it after the unlock left a
// window in which a notifier could observe false for a client that had already
// received its CONNACK, and silently drop the DISCONNECT it is owed. Under the
// mutex, a notifier entering the critical section after the write completed is
// guaranteed to see the flag.
func (c *client) mqttEnqueueConnAck(rc byte, sessionPresent bool, reasonStr string) (bool, error) {
	if h := mqttTestConnAckAttemptHookV.Load(); h != nil {
		(*h)(rc)
	}
	// Spec [MQTT-3.2.2-4], spec5 [3.2.2.1.1]: when the (reason) code indicates
	// failure, the session present flag must be 0.
	sp := byte(0)
	if rc == 0 && sessionPresent {
		sp = 1
	}

	if c.mqtt.proto == mqttProtoLevel5 {
		frame := c.mqttMakeConnAckV5Frame(rc, sp, reasonStr)
		if !c.mqttFitsClientMax(len(frame)) {
			c.Debugf("Suppressing MQTT CONNACK of %d bytes exceeding client Maximum Packet Size %d", len(frame), c.mqtt.maxPacketSize)
			return false, nil
		}
		return true, c.mqttWriteConnAckFrame(rc, frame)
	}

	proto := [4]byte{mqttPacketConnectAck, 2, sp, rc}
	return true, c.mqttWriteConnAckFrame(rc, proto[:])
}

// mqttWriteConnAckFrame writes a built CONNACK through the actionless boundary and records it,
// both in ONE c.mu section: the record gates every later takeover notification, and the lock
// held across write-then-store is what orders it against those readers.
//
// A refusal is impossible by construction (a locally built type-0x20 frame), but it is handled
// as the write failure it would be: nothing is recorded as written, and the socket is closed so
// "a failed CONNACK write closed the socket" stays true for every abort discipline that reads
// this error.
func (c *client) mqttWriteConnAckFrame(rc byte, frame []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.writeActionlessFrameBy(frame, time.Time{})
	var refusal mqttActionlessRefusal
	if errors.As(err, &refusal) && c.rawConn != nil {
		_ = c.rawConn.Close()
	}
	c.mqttRecordConnAckWrittenLocked(rc, err)
	return err
}

// mqttRecordConnAckWrittenLocked records that this connection has been told its
// CONNECT succeeded. Only a clean write of an accepted CONNACK counts: a failed
// write closed the socket and nothing was acknowledged, and a failure CONNACK
// never opens the connection for further packets. c.mu must be held — the flag
// gates every later takeover notification [MQTT-3.2.0-1], and holding the lock
// across write-then-store is what orders it against those readers.
func (c *client) mqttRecordConnAckWrittenLocked(rc byte, err error) {
	if err == nil && rc == mqttConnAckRCConnectionAccepted {
		c.connAckWritten.Store(true)
	}
}

// mqttMakeConnAckV5Frame builds an MQTT 5.0 CONNACK frame: a reason code, the
// session present flag, and a properties block advertising the server's
// capabilities. On a failure reason, reasonStr (when set and it fits) is added
// as a Reason String. The frame size is independent of sp (one byte either way),
// so the accept path can size it before knowing session present. Spec5 [3.2.2].
func (c *client) mqttMakeConnAckV5Frame(rc, sp byte, reasonStr string) []byte {
	reason := mqttConnAckReasonFromRC(rc)
	success := reason == mqttReasonSuccess

	props := &mqttProperties{present: map[byte]bool{}}
	// Advertise the capabilities of the current implementation. Properties left
	// absent take their spec defaults, which already describe us correctly, so
	// we MUST NOT send them with an explicit value:
	//   - Maximum QoS: absent => QoS 2 supported. (A value of 2 is illegal in
	//     this property: spec5 [3.2.2.3.4] allows only 0 or 1.)
	//   - Retain Available: absent => retained messages supported.
	//   - Wildcard Subscription Available: absent => wildcards supported.
	// We only emit the properties whose truthful value differs from the default.
	//   - Shared Subscription Available: absent => available. We implement them,
	//     so we emit nothing. Spec5 [3.2.2.3.11], [4.8.2].
	//   - Subscription Identifiers: absent => available (implemented), emit nothing.
	// Spec5 [3.2.2.3].
	// Topic Alias Maximum: advertise the (configured) number of inbound topic
	// aliases we accept. Success only (resolved on the accept path); omitted when
	// disabled: absent => 0, i.e. the client must not send any alias. Spec5
	// [3.2.2.3.2].
	if tam := c.mqtt.topicAliasMax; success && tam > 0 {
		props.topicAliasMax = tam
		props.present[mqttPropTopicAliasMax] = true
	}
	// Server Keep Alive: sent only when the server overrides the client's
	// requested keep alive (resolved on the accept path); absent => the client's
	// value applies. Spec5 [3.2.2.3.14].
	if ska := c.mqtt.serverKeepAlive; success && ska > 0 {
		props.serverKeepAlive = ska
		props.present[mqttPropServerKeepAlive] = true
	}
	// Maximum Packet Size: advertise the server's inbound limit (captured on the
	// accept path). Truthful: a packet over it gets a DISCONNECT 0x95 at the
	// framing layer, and an in-range PUBLISH whose NATS re-encoding exceeds the
	// account max payload gets a non-fatal 0x97 rather than being rejected here.
	// Omitted (absent => no limit) when there is none. Spec5 [3.2.2.3.6].
	if mps := c.mqtt.maxInPacketSize; success && mps > 0 {
		props.maxPacketSize = mps
		props.present[mqttPropMaxPacketSize] = true
	}
	// Receive Maximum: advertise how many inbound QoS 1/2 PUBLISH the server
	// processes concurrently (resolved on the accept path). Omitted when disabled:
	// absent => 65535, which the 16-bit packet identifier space can never exceed,
	// so not enforcing is truthful. Spec5 [3.2.2.3.3].
	if rm := c.mqtt.recvMaxIn; success && rm > 0 {
		props.receiveMax = rm
		props.present[mqttPropReceiveMaximum] = true
	}
	// Echo a server-assigned client identifier. Spec5 [3.2.2.3.7].
	if success && c.mqtt.cidGenerated {
		props.assignedClientID = c.mqtt.cid
	}
	// Response Information: returned only when the client requested it and the
	// server is configured with a base response topic (resolved on the accept
	// path). Spec5 [3.2.2.3.15].
	if success && c.mqtt.responseInfo != _EMPTY_ {
		props.responseInfo = c.mqtt.responseInfo
	}
	// On a failure reason, explain why with a Reason String (CONNACK is exempt
	// from Request Problem Information). Spec5 [3.2.2.3.9].
	if reason >= 0x80 {
		props.reasonString = c.mqttReasonString(mqttPacketConnectAck, reasonStr)
	}

	// Variable header = session present (1) + reason code (1) + properties.
	frame := func() []byte {
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketConnectAck)
		var vh mqttWriter
		vh.WriteByte(sp)
		vh.WriteByte(reason)
		vh.writeProperties(props)
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		return w.Bytes()
	}
	buf := frame()
	// Drop the Reason String (only) if it pushed the packet past the client's
	// Maximum Packet Size. Spec5 [3.1.2.11.4].
	if props.reasonString != _EMPTY_ && !c.mqttFitsClientMax(len(buf)) {
		props.reasonString = _EMPTY_
		buf = frame()
	}
	return buf
}

// Reason String sent with the DISCONNECT to a client whose session is taken
// over by a new connection with the same client ID. Spec5 [3.1.4].
const mqttReasonStrSessionTakenOver = "session taken over by a new connection"

// Reason String sent with the 0x89 DISCONNECT to a client whose session record was
// found to have FORKED — a record write CLAIMING the same parent as ours survived, so
// our own was erased without anything establishing another owner. Deliberately not
// mqttReasonStrSessionTakenOver: pairing that text with 0x89 would make exactly the
// claim the fork verdict exists to withhold. See sess_fork_lineage.go.
const mqttReasonStrOwnershipUnsettled = "session ownership could not be established; try again"

// Reason String sent with the DISCONNECT to every connected client when the
// broker is draining connections for shutdown. Spec5 [3.14.2.1].
const mqttReasonStrServerShuttingDown = "server shutting down"

// Reason String sent with the 0x83 pre-CONNACK refusal of a resume whose record CAS
// was rejected and whose evidence established nothing beyond that. Deliberately NOT
// mqttReasonStrSessionTakenOver: the rejection is also reached when the record could
// not be read, was absent, did not decode, was released, or sat at or below our own
// base, none of which demonstrates a successor. See mqttPreConnackRefusal.
const mqttReasonStrOwnershipUnconfirmed = "session ownership could not be confirmed"

// mqttPreConnackRefusal is what a pre-CONNACK refusal is licensed to tell a v5
// client, and it is DELIBERATELY NOT the disposal vocabulary.
//
// That bundle's contract governs the DISCONNECT, its Reason String, the log line and
// the close state, and its takeover text asserts a successor that most arms reaching
// it never established — the class F-084 owns. The CONNACK is a separate projection
// onto a code [3.2.2.2] permits, and this type is the whole of it: no disposal
// vocabulary is ever read on the way to the wire.
type mqttPreConnackRefusal struct {
	code      byte
	reasonStr string
}

var (
	// A body claiming our parent IS a positive fact — a non-empty, distinct identity
	// naming the same parent as our acknowledged record — and 0x89 "try again later"
	// states exactly what it licenses and nothing more. It does not claim a fork was
	// PROVED: provenance is not validated (sess_fork_lineage.go).
	mqttRefusalOwnershipUnsettled = mqttPreConnackRefusal{
		code: mqttReasonServerBusy, reasonStr: mqttReasonStrOwnershipUnsettled,
	}
	// Every other disposing outcome. 0x83 because 0x8E "Session taken over" is
	// DISCONNECT-only ([3.2.2.2]) and no legal CONNACK code says "stop reconnecting";
	// the text says only what is true on every one of these arms — the CAS was
	// rejected and this broker could not confirm it still owns the session.
	mqttRefusalOwnershipUnconfirmed = mqttPreConnackRefusal{
		code: mqttReasonImplementationSpecificError, reasonStr: mqttReasonStrOwnershipUnconfirmed,
	}
)

// preConnackRefusalFor maps an outcome onto what the client is told, and reports
// whether that outcome HAS an answer.
//
// EXHAUSTIVE over every outcome, with NO default arm: a default is exactly how a new
// outcome would silently inherit an ownership claim it has no evidence for, which is
// the defect class this mechanism exists to stop making worse.
//
// THREE outcomes are non-disposing and are listed rather than assumed. Proceed does
// not refuse at all; Local and Unresolved refuse through refusePreConnack directly,
// carrying the step's own error, because nothing about either is an ownership
// statement. They report false, so a caller reaching here with one sends NOTHING
// rather than a 0x00 CONNACK.
func preConnackRefusalFor(o mqttPreConnackOutcome) (mqttPreConnackRefusal, bool) {
	switch o {
	case mqttPreConnackDisposingForked:
		return mqttRefusalOwnershipUnsettled, true
	case mqttPreConnackDisposingNonForked:
		return mqttRefusalOwnershipUnconfirmed, true
	case mqttPreConnackProceed, mqttPreConnackLocal, mqttPreConnackUnresolved:
		return mqttPreConnackRefusal{}, false
	}
	return mqttPreConnackRefusal{}, false
}

// mqttConnAckReasonFromConnectErr picks a v5 CONNACK reason code for a CONNECT
// that failed to parse without a return code: property errors are Protocol
// Error (0x82), everything else Malformed Packet (0x81). Spec5 [3.1.4.1].
func mqttConnAckReasonFromConnectErr(err error) byte {
	switch {
	case errors.Is(err, errMQTTPayloadFormatInvalid):
		return mqttReasonPayloadFormatInvalid
	case errors.Is(err, errMQTTUnknownProperty), errors.Is(err, errMQTTPropertyNotAllowed),
		errors.Is(err, errMQTTDuplicateProperty), errors.Is(err, errMQTTProtocolError):
		return mqttReasonProtocolError
	}
	return mqttReasonMalformedPacket
}
