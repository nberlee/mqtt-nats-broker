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

// Generic protocol output: reason strings and reason properties, the client's maximum packet
// size, and enqueueing a protocol frame fitted to it.

package mqtt

import (
	"sync/atomic"
)

// mqttReasonString returns reasonStr if it may accompany a server-generated
// packet of type pktType, or empty otherwise: v5 only; a client that sent
// Request Problem Information 0 gets one only on CONNACK/DISCONNECT (where the
// server MAY send it regardless); and it must be a valid MQTT UTF-8 string
// ([MQTT-1.5.4-1]) no longer than an MQTT UTF-8 string can encode (the writer
// lays down a uint16 length, so an over-long string would wrap it and corrupt
// the block). Callers may pass client-derived error text, so both checks matter.
// Spec5 [1.5.4], [3.1.2.11.7].
func (c *client) mqttReasonString(pktType byte, reasonStr string) string {
	if reasonStr == _EMPTY_ || c.mqtt == nil || c.mqtt.proto != mqttProtoLevel5 {
		return _EMPTY_
	}
	if c.mqtt.noProblemInfo && pktType != mqttPacketConnectAck && pktType != mqttPacketDisconnect {
		return _EMPTY_
	}
	if len(reasonStr) > 0xFFFF || mqttValidateString(reasonStr, "reason string") != nil {
		return _EMPTY_
	}
	return reasonStr
}

// mqttFitsClientMax reports whether a framed packet of n bytes fits the client's
// MQTT 5.0 Maximum Packet Size (0 = no limit). An oversized packet must drop its
// Reason String rather than be sent. Spec5 [3.1.2.11.4].
func (c *client) mqttFitsClientMax(n int) bool {
	return c.mqtt == nil || c.mqtt.maxPacketSize == 0 || uint32(n) <= c.mqtt.maxPacketSize
}

// mqttEnqueueFittingProto enqueues buf unless it would exceed the client's
// Maximum Packet Size; the server MUST NOT send such a packet [MQTT-3.1.2-24].
// Returns whether it was enqueued.
func (c *client) mqttEnqueueFittingProto(buf []byte) bool {
	if !c.mqttFitsClientMax(len(buf)) {
		c.Debugf("Suppressing MQTT packet of %d bytes exceeding client Maximum Packet Size %d", len(buf), c.mqtt.maxPacketSize)
		return false
	}
	return c.mqttEnqueueProtoLocked(buf, false)
}

// mqttEnqueueFittingProtoUnlessDisconnected is mqttEnqueueFittingProto for a packet
// that must not follow a server-initiated DISCONNECT on this connection.
func (c *client) mqttEnqueueFittingProtoUnlessDisconnected(buf []byte) bool {
	if !c.mqttFitsClientMax(len(buf)) {
		c.Debugf("Suppressing MQTT packet of %d bytes exceeding client Maximum Packet Size %d", len(buf), c.mqtt.maxPacketSize)
		return false
	}
	return c.mqttEnqueueProtoLocked(buf, true)
}

// mqttEnqueueProtoLocked enqueues buf under c.mu, optionally refusing when this
// connection's one server-initiated DISCONNECT has already been ATTEMPTED.
//
// The refusal has to happen in the SAME critical section as the write, for the same
// reason every enqueue-path DISCONNECT writer claims in its own: a check made and
// released first is a check-then-act, and the interval between them is exactly when a
// displacement — the watch's eviction worker needs no lock of ours — notifies and detaches.
// A packet appended after that notification violates [MQTT-3.14.4-1].
//
// It gates only the packets whose callers ask for it. Nothing else about the
// connection's outbound traffic changes: an in-flight PUBLISH or a PINGRESP can still
// trail a DISCONNECT, which is the stated residual of the claim mechanism.
func (c *client) mqttEnqueueProtoLocked(buf []byte, unlessDisconnected bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if unlessDisconnected && (c.mqtt == nil || c.mqtt.disconnectSent) {
		return false
	}
	mqttTestObserveMixedSite(c, mqttMixedSiteProtoLocked, buf)
	c.enqueueProto(buf)
	return true
}

// mqttServerMaxPacketSize returns the server's MQTT 5.0 Maximum Packet Size for
// this connection, derived from the account max payload, or 0 when there is no
// effective limit (unlimited, or a limit at/above the largest legal control
// packet). Spec5 [3.2.2.3.6].
func (c *client) mqttServerMaxPacketSize() uint32 {
	mpay := atomic.LoadInt32(&c.mpay)
	if mpay <= 0 || mpay > mqttMaxControlPacketSize {
		return 0
	}
	return uint32(mpay)
}

// mqttReasonProps returns a properties block carrying reasonStr as a Reason
// String for a server-generated packet of type pktType, or nil (which encodes as
// a single zero length byte, byte-identical to no properties) when the string is
// empty or not permitted. Spec5 [3.9.2.2.2].
func (c *client) mqttReasonProps(pktType byte, reasonStr string) *mqttProperties {
	if rs := c.mqttReasonString(pktType, reasonStr); rs != _EMPTY_ {
		return &mqttProperties{reasonString: rs}
	}
	return nil
}
