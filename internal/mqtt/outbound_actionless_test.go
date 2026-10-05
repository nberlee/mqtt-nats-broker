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

//go:build mqttport_fulltests

package mqtt

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// testFrame builds one MQTT packet: a fixed header byte, a remaining length, and a body.
func testFrame(first byte, body ...byte) []byte {
	out := []byte{first}
	n := len(body)
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		out = append(out, d)
		if n == 0 {
			break
		}
	}
	return append(out, body...)
}

// TestF105TheActionlessPathRefusesWhatCarriesAnObligation is §7.1's other half.
//
// Guarding only the AUTHORIZED path leaves a way to the socket that never touches it. This path
// therefore refuses by what the BYTES ARE — not by a caller-supplied label, of which there is
// deliberately none.
func TestF105TheActionlessPathRefusesWhatCarriesAnObligation(t *testing.T) {
	// THE POSITIVE CONTROLS. Without these a validator that refused everything would satisfy
	// every refusal below.
	for name, f := range map[string][]byte{
		"a PINGRESP":      testFrame(mqttPacketPingResp),
		"a PUBACK":        testFrame(mqttPacketPubAck, 0, 7),
		"a QoS 0 PUBLISH": testFrame(mqttPacketPub, 0, 3, 'a', '/', 'b'),
		"a DISCONNECT":    testFrame(mqttPacketDisconnect),
	} {
		if _, err := mqttValidateActionlessFrame(f); err != nil {
			t.Fatalf("%s was refused the actionless path: %v", name, err)
		}
	}

	// THE TWO KINDS THAT CARRY AN OBLIGATION, refused however they are dressed.
	for name, tc := range map[string]struct {
		frame []byte
		want  string
	}{
		"a QoS 1 PUBLISH": {testFrame(mqttPacketPub|0x02, 0, 1, 'a', 0, 7), "carries an outbound obligation"},
		"a QoS 2 PUBLISH": {testFrame(mqttPacketPub|0x04, 0, 1, 'a', 0, 7), "carries an outbound obligation"},
		"a PUBREL":        {testFrame(mqttPacketPubRel|0x02, 0, 7), "release of an obligation"},
	} {
		_, err := mqttValidateActionlessFrame(tc.frame)
		if err == nil {
			t.Fatalf("%s took the actionless path", name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s was refused for the wrong reason: %v", name, err)
		}
	}
}

// TestF105TheActionlessPathRefusesATrailingPacket is the bypass a first-byte check cannot catch,
// and the one that defeated an earlier design of this boundary.
//
// A valid control packet followed by a QoS 1/2 PUBLISH in the SAME buffer passes every check
// that looks only at the front: the first byte is a PINGRESP, and the PUBLISH behind it reaches
// the socket having consumed no action at all.
func TestF105TheActionlessPathRefusesATrailingPacket(t *testing.T) {
	ping := testFrame(mqttPacketPingResp)
	pub := testFrame(mqttPacketPub|0x04, 0, 1, 'a', 0, 7)

	// The concatenation. Its FIRST byte is a perfectly good PINGRESP.
	both := append(append([]byte{}, ping...), pub...)
	if _, err := mqttValidateActionlessFrame(ping); err != nil {
		t.Fatalf("the fixture's PINGRESP is not itself valid: %v", err)
	}
	_, err := mqttValidateActionlessFrame(both)
	if err == nil {
		t.Fatalf("a QoS 2 PUBLISH rode into the socket behind a PINGRESP")
	}
	if !strings.Contains(err.Error(), "trailing byte") {
		t.Fatalf("the refusal did not come from the trailing-packet rule: %v", err)
	}

	// AND A PARTIAL FRAME IS NOT A FRAME. A write invocation is not a frame boundary, so a
	// buffer that stops short of what its own header declares is refused rather than sent.
	if _, err := mqttValidateActionlessFrame(pub[:len(pub)-2]); err == nil {
		t.Fatalf("a truncated frame was accepted")
	}
}

// TestF105RemainingLengthIsDecodedOrRefused pins the decoder the trailing-packet rule rests on.
//
// A malformed length is NOT read as zero. The length is what says where the frame ends, so
// failing to read it means this broker cannot tell whether anything follows — and "cannot tell"
// must refuse rather than assume nothing does.
func TestF105RemainingLengthIsDecodedOrRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		in  []byte
		val int
		n   int
		ok  bool
	}{
		"zero":           {[]byte{0x00}, 0, 1, true},
		"one byte":       {[]byte{0x7f}, 127, 1, true},
		"two bytes":      {[]byte{0x80, 0x01}, 128, 2, true},
		"four bytes":     {[]byte{0xff, 0xff, 0xff, 0x7f}, 268435455, 4, true},
		"five continues": {[]byte{0xff, 0xff, 0xff, 0xff, 0x7f}, 0, 0, false},
		"truncated":      {[]byte{0x80}, 0, 0, false},
		"empty":          {nil, 0, 0, false},
	} {
		v, n, ok := mqttDecodeRemainingLength(tc.in)
		if ok != tc.ok {
			t.Fatalf("%s decoded ok=%v, want %v", name, ok, tc.ok)
		}
		if ok && (v != tc.val || n != tc.n) {
			t.Fatalf("%s decoded (%d,%d), want (%d,%d)", name, v, n, tc.val, tc.n)
		}
	}
}

// TestF105TheActionlessWriterRefusesConcatenatedFrames is B1 at the WRITER, not the validator.
//
// The trailing-packet test above proves the validator refuses the bytes. What the cutover plan
// asked of B1 is that the concatenated frame actually REACH the actionless boundary and leave
// nothing on the transport; a validator nobody calls on the write path would pass the first test
// and fail this one. Both entry points are submitted, because the emitters call the lock-agnostic
// core and the retained QoS 0 replay calls the locking wrapper.
func TestF105TheActionlessWriterRefusesConcatenatedFrames(t *testing.T) {
	pingResp := testFrame(mqttPacketPingResp)
	qos2Publish := testFrame(mqttPacketPub|0x04, 0, 1, 'a', 0, 7, 'x')
	smuggled := append(append([]byte{}, pingResp...), qos2Publish...)

	for _, tc := range []struct {
		name  string
		write func(c *client, frame []byte) error
	}{
		{"writeActionlessFrameBy", func(c *client, f []byte) error { return c.writeActionlessFrameBy(f, time.Time{}) }},
		{"writeActionlessFrame", func(c *client, f []byte) error { return c.writeActionlessFrame(f) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, sc := testStalledClient(t)
			sc.setWritable(true)

			err := tc.write(c, smuggled)
			var refusal mqttActionlessRefusal
			if !errors.As(err, &refusal) {
				t.Fatalf("a QoS 2 PUBLISH trailing a PINGRESP was not refused at the writer: err=%v", err)
			}
			if got := sc.bytesWritten(); len(got) != 0 {
				t.Fatalf("the refused frame put %d byte(s) on the transport: %x", len(got), got)
			}

			// REACH CONTROL: the same writer on the same connection does write a lone
			// actionless packet, so the zero above is a refusal and not a dead transport.
			if err := tc.write(c, pingResp); err != nil {
				t.Fatalf("a lone PINGRESP was refused: %v", err)
			}
			if got := sc.bytesWritten(); string(got) != string(pingResp) {
				t.Fatalf("the transport carries %x, want exactly the PINGRESP %x", got, pingResp)
			}
		})
	}
}

// TestF105QoS12FrameRequiresActionByClass pins bindSection's rule: a QoS 1/2 frame may take the
// raw branch only on a slot-only (experimental affine) slot, whatever else its section carries.
//
// The empty-subject case is the one this rule closes. acquirePublishAction already refuses such a
// section, but the raw branch must not depend on a precondition three calls away: a QoS 1/2 frame
// with no slot name and no acquisition is refused at the writer, never written raw.
func TestF105QoS12FrameRequiresActionByClass(t *testing.T) {
	affine := mqttQ2AffineConsumerDurablePrefix + "grp"
	for _, tc := range []struct {
		name    string
		qos     byte
		dur     string
		subject string
		want    bool
	}{
		{"ordinary QoS 1, named slot", 1, "$MQTT_ord", "$MQTT.inflight.x", true},
		{"ordinary QoS 2, EMPTY slot subject", 2, "$MQTT_ord", "", true},
		{"ordinary QoS 1, EMPTY slot subject", 1, "", "", true},
		{"affine QoS 2 (chapter P)", 2, affine, "$MQTT.inflight.y", false},
		{"QoS 0", 0, "$MQTT_ord", "$MQTT.inflight.z", false},
	} {
		var tok mqttSendToken
		tok.slot.dur = tc.dur
		tok.slot.name.subject = tc.subject
		p := mqttPreparedPublish{qos: tc.qos}
		p.bindSection(tok)
		if p.requireAction != tc.want {
			t.Errorf("%s: requireAction=%v, want %v", tc.name, p.requireAction, tc.want)
		}
	}
}
