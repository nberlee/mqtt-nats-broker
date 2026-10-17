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

// DISCONNECT: claiming and enqueueing the server's DISCONNECT, the takeover and displacement
// notices, building the frame, and the reason code for a closing error.

package mqtt

import (
	"errors"
	"time"
)

// mqttClaimDisconnectLocked consumes this connection's one server-initiated
// DISCONNECT notification and reports whether the caller may write it. The
// second and every later would-be sender is refused: after sending a DISCONNECT
// the sender must not send any more MQTT Control Packets on that Network
// Connection [MQTT-3.14.4-1], and the displacement paths race, so which reason
// the client is told is decided here rather than by whoever reaches the socket
// first. Every server-DISCONNECT writer claims through this one predicate; the
// enqueue-path writers do it in the same c.mu critical section as their write,
// so for them there is no check-then-act window at all. shutdownDisconnect is
// the one exception, and there the claim is a RESERVATION: it claims under the
// lock and writes through the capped writeActionlessFrameBy after releasing it, so it can spend the
// claim — suppressing a competing notification — and then write nothing, or
// fail. That is why the guarantee is at most one ATTEMPT and not one delivered
// packet. c.mu must be held.
func (c *client) mqttClaimDisconnectLocked() bool {
	if c.mqtt == nil || c.mqtt.disconnectSent {
		return false
	}
	c.mqtt.disconnectSent = true
	return true
}

// mqttEnqueueDisconnect sends an MQTT 5.0 DISCONNECT with the given reason code
// (and, when allowed and it fits, a Reason String) to the client, before the
// connection is closed on a protocol/processing error. It is a no-op for 3.1.1
// connections, which have no server-initiated DISCONNECT, and for a connection
// whose one DISCONNECT another path already claimed. Spec5 [3.14].
func (c *client) mqttEnqueueDisconnect(reason byte, reasonStr string) {
	pkt := c.mqttBuildDisconnect(reason, reasonStr)
	if pkt == nil {
		// Nothing was sent, so [MQTT-3.14.4-1] never triggered: the claim stays
		// available to a later sender rather than being spent on no packet.
		return
	}
	c.mu.Lock()
	if c.mqttClaimDisconnectLocked() {
		// Claim and write in this one section. A refusal spends the claim and writes nothing,
		// which "at most one ATTEMPT" already permits.
		_ = c.writeActionlessFrameBy(pkt, time.Time{})
	}
	c.mu.Unlock()
}

// mqttNotifyTakenOver tells a displaced connection that its session was taken over (0x8E),
// and it has exactly ONE caller: discardTakenOverClient, the tail of an in-process
// same-client-ID replacement this broker holds both ends of.
//
// That is the F-084 licence, and the single caller is how it is enforced rather than
// asserted — TestF084OnlyOneReaderOfTheTakeoverClaim fails if a second appears. It used to
// be the shared tail for all four displacement routes; the other three (a remote persist
// observation, a resolver's foreign disposal, the bound rejection settlement) reach their
// verdict from a RECORD, which proves no successor, so they report ownership-unsettled
// through mqttNotifyDisplaced instead.
//
// The shared machinery is one level down: mqttNotifyDisplaced still owns the CONNACK gate
// and the one-DISCONNECT claim for every route, so "displace this connection" still has one
// notification owner rather than four writers racing to the socket.
//
// Two conditions, both required. The connection must already have been told its
// CONNECT succeeded: CONNACK must be the first packet a client sees
// [MQTT-3.2.0-1], and mqttAbortAcceptedConnect leaves a refused connection bound
// until its readLoop detaches it, so a displacement can find a bound but
// pre-CONNACK connection — which is then closed silently. And no server
// DISCONNECT may have been attempted yet ([MQTT-3.14.4-1],
// mqttClaimDisconnectLocked).
//
// The gate is read under c.mu, which orders it against the CONNACK writer's
// store in mqttWriteConnAckFrame's critical section: a notifier entering after
// the CONNACK write completed is guaranteed to see it. The packet is built
// before the lock (mqttBuildDisconnect reads only fields immutable after CONNECT
// parsing) so the critical section holds only the claim and the write.
//
// No lock held on entry.
func (c *client) mqttNotifyTakenOver() { c.mqttNotifyDisplaced(mqttDisposalTakenOver) }

// mqttNotifyDisplaced is mqttNotifyTakenOver's body, with WHAT is reported passed in.
//
// The vocabulary travels as one value because it is one claim made four ways: a caller
// that could pass a reason code alone would emit 0x89 "server busy" alongside "session
// taken over by a new connection", which is exactly the assertion a fork verdict exists
// to withhold. Both gates, the DISCONNECT claim and the 3.1.1 no-op are inherited
// unchanged — they belong to the displacement, not to its reason. See
// sess_fork_lineage.go.
func (c *client) mqttNotifyDisplaced(voc mqttDisposalVocabulary) {
	pkt := c.mqttBuildDisconnect(voc.reason, voc.reasonStr)
	if pkt == nil {
		return
	}
	c.mu.Lock()
	if c.connAckWritten.Load() && c.mqttClaimDisconnectLocked() {
		_ = c.writeActionlessFrameBy(pkt, time.Time{})
	}
	c.mu.Unlock()
}

// mqttBuildDisconnect returns the wire bytes for a server-initiated MQTT 5.0
// DISCONNECT with the given reason code (and, when allowed and it fits, a Reason
// String), or nil for a 3.1.1 connection (which has no server DISCONNECT) or
// when even the 3-byte short form would exceed the client's Maximum Packet Size.
// It builds only; the caller enqueues (mqttEnqueueDisconnect) or writes it
// directly under a deadline (shutdownDisconnect). Spec5 [3.14].
func (c *client) mqttBuildDisconnect(reason byte, reasonStr string) []byte {
	if c.mqtt == nil || c.mqtt.proto != mqttProtoLevel5 {
		return nil
	}
	if rs := c.mqttReasonString(mqttPacketDisconnect, reasonStr); rs != _EMPTY_ {
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketDisconnect)
		var vh mqttWriter
		vh.WriteByte(reason)
		vh.writeProperties(&mqttProperties{reasonString: rs})
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if c.mqttFitsClientMax(w.Len()) {
			return w.Bytes()
		}
		// Falls through to the short form when the Reason String would overflow.
	}
	// Remaining length of 1: just the reason code, properties omitted (allowed
	// when there are no properties). Suppressed if even this doesn't fit (only
	// possible before a success CONNACK bounds the client's tiny limit); the
	// connection closes regardless. Spec5 [3.14.2.2.1].
	proto := [3]byte{mqttPacketDisconnect, 1, reason}
	if !c.mqttFitsClientMax(len(proto)) {
		return nil
	}
	return append([]byte(nil), proto[:]...)
}

// mqttDisconnectReasonFromErr picks a v5 DISCONNECT reason code for a parse or
// processing error. Spec5 [3.14.2.1].
func mqttDisconnectReasonFromErr(err error) byte {
	switch {
	case errors.Is(err, errMQTTMalformedVarInt), errors.Is(err, errMQTTMalformedProperties),
		errors.Is(err, errMQTTMalformedSubOption), errors.Is(err, errMQTTMalformedSharedSub),
		errors.Is(err, errMQTTMalformedPacket), errors.Is(err, errMQTTTopicFilterCannotBeEmpty),
		errors.Is(err, errMQTTUnsupportedCharacters), errors.Is(err, errMQTTInvalidPublishLength):
		return mqttReasonMalformedPacket
	case errors.Is(err, errMQTTUnknownProperty), errors.Is(err, errMQTTPropertyNotAllowed),
		errors.Is(err, errMQTTDuplicateProperty), errors.Is(err, errMQTTProtocolError),
		errors.Is(err, errMQTTSecondConnectPacket), errors.Is(err, errMQTTTopicIsEmpty):
		return mqttReasonProtocolError
	case errors.Is(err, errMQTTTopicAliasInvalid):
		return mqttReasonTopicAliasInvalid
	case errors.Is(err, errMQTTReceiveMaxExceeded):
		return mqttReasonReceiveMaxExceeded
	case errors.Is(err, errMQTTPayloadFormatInvalid):
		return mqttReasonPayloadFormatInvalid
	case errors.Is(err, errMQTTPublishNotAuthorized):
		return mqttReasonNotAuthorized
	case errors.Is(err, errMQTTSessionOwnershipUnsettled):
		// Nothing arbitrated the ownership question, so this is "try again later", not
		// 0x8E: a takeover nobody proved must not be announced as one. See
		// sess_rejected_write.go.
		return mqttReasonServerBusy
	}
	return mqttReasonUnspecifiedError
}
