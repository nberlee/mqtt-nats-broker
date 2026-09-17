// Copyright 2026 The mqtt-nats-bridge Authors
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

package mqtt

import "fmt"

// The ACTIONLESS half of the output boundary (F-105, row 2; plan §7.1).
//
// §7.1's property is about what reaches the writer, not about who calls it:
//
//	QoS 1/2 frame output cannot reach the raw writer without its action. Actionless output —
//	control packets and QoS 0 — takes an explicitly constrained path of its own.
//
// `writeAuthorizedFrame` is the first half. This is the second, and without it the property does
// not hold: every design that only guarded the AUTHORIZED path left a way to the socket that
// simply never touched it.
//
// # Three bypasses this has to refuse, and why the obvious checks miss them
//
// A review round refused two earlier designs of mine, and the reasons are the specification:
//
//  1. INLINE CONSTRUCTION. A caller that assembles PUBLISH bytes itself never touches the
//     authorized type, so a wrapper type alone proves nothing. The refusal has to be on what the
//     BYTES ARE.
//  2. A TRAILING PACKET. Inspecting the first byte and passing the write admits a valid control
//     packet followed by a QoS 1/2 PUBLISH in the same buffer: the first byte checks out and the
//     second frame goes unauthorized. So the frame's own length is decoded and the buffer must
//     end exactly there.
//  3. FRAGMENTATION. A write invocation is not a frame boundary, which §7.1.1 already says. This
//     API therefore takes a COMPLETE LOGICAL FRAME and validates it whole, rather than
//     validating whatever slice happened to arrive.
//
// # A caller-supplied label is not evidence
//
// There is deliberately no "this is actionless, trust me" parameter. The packet type is read
// from the bytes, and a PUBLISH with QoS > 0 or a PUBREL is refused however it was labelled.

// mqttActionlessRefusal explains why a frame may not take the actionless path.
type mqttActionlessRefusal struct {
	Why string
}

func (r mqttActionlessRefusal) Error() string { return r.Why }

// mqttValidateActionlessFrame reports whether these bytes are ONE complete MQTT packet that
// carries no outbound obligation.
//
// Returns the packet type on success, so a caller can log what it sent without re-parsing.
func mqttValidateActionlessFrame(frame []byte) (byte, error) {
	if len(frame) < 2 {
		return 0, mqttActionlessRefusal{Why: fmt.Sprintf(
			"an actionless frame of %d byte(s) is not a packet: a fixed header is at least two",
			len(frame))}
	}
	pt := frame[0] & mqttPacketMask
	flags := frame[0] & mqttPacketFlagMask

	// THE TWO KINDS THAT CARRY AN OBLIGATION ARE REFUSED HERE, by what they ARE.
	//
	// A QoS 1/2 PUBLISH is a delivery the client must acknowledge, and a PUBREL is the release
	// of one. Both must consume an acquired action, so both are the authorized path's business
	// however the caller described them.
	switch pt {
	case mqttPacketPub:
		if q := mqttGetQoS(flags); q > 0 {
			return 0, mqttActionlessRefusal{Why: fmt.Sprintf(
				"a QoS %d PUBLISH carries an outbound obligation and may not take the "+
					"actionless path: it consumes an acquired action or it does not go out", q)}
		}
	case mqttPacketPubRel:
		return 0, mqttActionlessRefusal{Why: "a PUBREL is the release of an obligation and may " +
			"not take the actionless path: it consumes an acquired action or it does not go out"}
	}

	if err := mqttCheckFixedHeaderFlags(pt, flags); err != nil {
		return 0, mqttActionlessRefusal{Why: fmt.Sprintf(
			"an actionless frame carries malformed fixed-header flags: %v", err)}
	}

	// THE FRAME MUST END WHERE IT SAYS IT DOES. This is the trailing-packet refusal, and it is
	// the one a first-byte check cannot make: a valid PINGRESP followed by a QoS 2 PUBLISH in
	// one buffer passes every check that looks only at the front.
	rl, n, ok := mqttDecodeRemainingLength(frame[1:])
	if !ok {
		return 0, mqttActionlessRefusal{Why: "an actionless frame carries a malformed remaining " +
			"length, so this broker cannot say where it ends or what follows it"}
	}
	total := 1 + n + rl
	switch {
	case total > len(frame):
		return 0, mqttActionlessRefusal{Why: fmt.Sprintf(
			"an actionless frame declares %d byte(s) but only %d were supplied: a partial frame "+
				"is not a frame, and a write invocation is not a frame boundary", total, len(frame))}
	case total < len(frame):
		return 0, mqttActionlessRefusal{Why: fmt.Sprintf(
			"an actionless frame declares %d byte(s) with %d supplied, so %d trailing byte(s) "+
				"follow it: a second packet behind a permitted one is how an unauthorized "+
				"PUBLISH reaches the socket unchecked", total, len(frame), len(frame)-total)}
	}
	return pt, nil
}

// mqttDecodeRemainingLength decodes MQTT's variable-length integer.
//
// Returns the value, how many bytes it occupied, and whether it decoded at all. A malformed
// length is NOT treated as zero: the length is what says where the frame ends, so failing to
// read it means this broker cannot tell whether anything follows.
func mqttDecodeRemainingLength(b []byte) (int, int, bool) {
	var value, multiplier int
	for i := 0; i < 4; i++ {
		if i >= len(b) {
			return 0, 0, false
		}
		d := b[i]
		value += int(d&0x7f) << multiplier
		if d&0x80 == 0 {
			return value, i + 1, true
		}
		multiplier += 7
	}
	// FOUR BYTES IS THE WHOLE ENCODING. A fifth continuation bit is not a larger number, it is
	// a malformed packet, and reading further would let a caller describe a frame length this
	// broker's own encoder could never produce.
	return 0, 0, false
}

// writeActionlessFrame sends one complete packet that carries no outbound obligation.
//
// The counterpart of writeAuthorizedFrame: between them they are the only two ways a frame
// reaches this connection's transport, and each refuses what belongs to the other.
func (c *client) writeActionlessFrame(frame []byte) error {
	if _, err := mqttValidateActionlessFrame(frame); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enqueueProto(frame)
	return nil
}
