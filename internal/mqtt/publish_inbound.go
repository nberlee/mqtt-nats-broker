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

// Inbound PUBLISH: the publish shape and its NATS header encoding, parsing and processing a
// client's PUBLISH, the QoS 2 store, and an inbound PUBREL.

package mqtt

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nuid"
)

type mqttPublish struct {
	topic   []byte
	subject []byte
	mapped  []byte
	msg     []byte
	sz      int
	pi      uint16
	flags   byte
	// props is the raw MQTT 5.0 properties block (variable-int length prefix +
	// body, exactly as received on the wire), captured so it can be forwarded
	// unaltered to v5 subscribers. nil when absent or empty (3.1.1, or a v5
	// PUBLISH with no properties). Spec5 [3.3.2.3].
	props []byte
}

// When we re-encode incoming MQTT PUBLISH messages for NATS delivery, we add
// the following headers:
//   - "Nmqtt-Pub" (*always) indicates that the message originated from MQTT, and
//     contains the original message QoS.
//   - "Nmqtt-Subject" contains the original MQTT subject from mqttParsePub.
//   - "Nmqtt-Mapped" contains the mapping during mqttParsePub.
//
// When we submit a PUBREL for delivery, we add a "Nmqtt-PubRel" header that
// contains the PI.
const (
	// NATS header that indicates that the message originated from MQTT and
	// stores the published message QOS.
	mqttNatsHeader = "Nmqtt-Pub"

	// NATS headers to store retained message metadata (along with the original
	// message as binary).
	mqttNatsRetainedMessageTopic  = "Nmqtt-RTopic"
	mqttNatsRetainedMessageOrigin = "Nmqtt-ROrigin"
	mqttNatsRetainedMessageFlags  = "Nmqtt-RFlags"
	mqttNatsRetainedMessageSource = "Nmqtt-RSource"
	// Authenticated No Local origin marker for the session that published the
	// retained message (see mqttOriginMarker). Absent for older stored
	// messages or when v5 is disabled.
	mqttNatsRetainedMessageSessionHash = "Nmqtt-RSession"
	// Raw MQTT 5.0 properties block for a retained message (base64), so v5
	// properties are replayed to subscribers. Absent for older stored messages.
	mqttNatsRetainedMessageProps = "Nmqtt-RProps"

	// Absolute expiry deadline (Unix nanoseconds, decimal) for a retained message
	// that was published with an MQTT 5.0 Message Expiry Interval. Carried with
	// the stored message so the remaining interval can be computed on replay
	// regardless of which server (or cache) serves it. Absent when the retained
	// message has no expiry. Spec5 [3.3.2.3.3].
	mqttNatsRetainedMessageExpiry = "Nmqtt-RExp"

	// NATS header that indicates that the message is an MQTT PubRel and stores
	// the PI.
	mqttNatsPubRelHeader = "Nmqtt-PubRel"

	// NATS headers to store the original MQTT subject and the subject mapping.
	mqttNatsHeaderSubject = "Nmqtt-Subject"
	mqttNatsHeaderMapped  = "Nmqtt-Mapped"

	// NATS header carrying the raw MQTT 5.0 PUBLISH properties block
	// (base64-encoded, since the block is binary and NATS header values are
	// CRLF-delimited text) so end-to-end properties survive the NATS/JetStream
	// hop and can be re-emitted to v5 subscribers.
	mqttNatsHeaderProps = "Nmqtt-Props"

	// NATS header carrying the No Local STAMP taken when the server took ownership of
	// the message: one authenticated tag per filter of the publishing session that had
	// No Local set and could receive the subject (see nolocal_stamp.go). The delivery
	// callbacks decide No Local from it alone, so a later re-SUBSCRIBE never re-judges
	// an owned message (F-117). Absent when no such filter existed, the publisher has no
	// session (e.g. an internal Will client) or v5 is off. Spec5 [3.8.3.1],
	// [MQTT-4.5.0-1].
	mqttNatsHeaderNoLocal = "Nmqtt-NoLocal"

	// NATS header (value "1") marking that the message was published with the
	// RETAIN flag set, so the v5 Retain As Published subscription option can keep
	// that flag on a live forward after the original flag is cleared for normal
	// delivery. Present only for retained publishes when v5 is enabled. Spec5
	// [3.8.3.1].
	mqttNatsHeaderRetain = "Nmqtt-Ret"
)

type mqttParsedPublishNATSHeader struct {
	qos      byte
	subject  []byte
	mapped   []byte
	retained bool   // published with RETAIN set (from Nmqtt-Ret), for Retain As Published
	props    []byte // raw MQTT 5.0 properties block (decoded from Nmqtt-Props)
	noLocal  []byte // No Local stamp taken at ownership (from Nmqtt-NoLocal)
}

func mqttParsePublishNATSHeader(headerBytes []byte) *mqttParsedPublishNATSHeader {
	if len(headerBytes) == 0 {
		return nil
	}

	pubValue := getHeader(mqttNatsHeader, headerBytes)
	if len(pubValue) == 0 {
		return nil
	}
	h := &mqttParsedPublishNATSHeader{
		qos:     pubValue[0] - '0',
		subject: getHeader(mqttNatsHeaderSubject, headerBytes),
		mapped:  getHeader(mqttNatsHeaderMapped, headerBytes),
		noLocal: getHeader(mqttNatsHeaderNoLocal, headerBytes),
	}
	// Retain As Published marker: the contract is exactly "1"; anything else
	// (absent, or a forged Nmqtt-Ret:0 / junk from a NATS publisher) is not
	// retained. Spec5 [3.8.3.1].
	if ret := getHeader(mqttNatsHeaderRetain, headerBytes); len(ret) == 1 && ret[0] == '1' {
		h.retained = true
	}
	// MQTT 5.0 properties block, carried base64-encoded. This header can come
	// from an untrusted source (a NATS publisher can set Nmqtt-Pub/Nmqtt-Props
	// on a message an MQTT client is subscribed to), so the decoded block is
	// re-validated before it may be forwarded verbatim into an outbound PUBLISH
	// (see mqttValidateForwardProps). Best effort: anything invalid is simply
	// dropped, never a delivery failure and never a malformed outbound packet.
	if enc := getHeader(mqttNatsHeaderProps, headerBytes); len(enc) > 0 {
		if raw, err := base64.StdEncoding.DecodeString(string(enc)); err == nil {
			h.props = mqttValidateForwardProps(raw)
		}
	}
	return h
}

func mqttParsePubRelNATSHeader(headerBytes []byte) uint16 {
	if len(headerBytes) == 0 {
		return 0
	}

	pubrelValue := getHeader(mqttNatsPubRelHeader, headerBytes)
	if len(pubrelValue) == 0 {
		return 0
	}
	pi, _ := strconv.ParseUint(string(pubrelValue), 10, 16)
	return uint16(pi)
}
func (c *client) mqttParsePub(r *mqttReader, pl int, pp *mqttPublish, hasMappings bool) error {
	qos := mqttGetQoS(pp.flags)
	if qos > 2 {
		return fmt.Errorf("%w: QoS=%v is invalid in MQTT", errMQTTMalformedPacket, qos)
	}

	if c.mqtt.rejectQoS2Pub && qos == 2 {
		return fmt.Errorf("QoS=2 is disabled for PUBLISH messages")
	}

	// Keep track of where we are when starting to read the variable header
	start := r.pos

	var err error
	pp.topic, err = r.readBytes("topic", false)
	if err != nil {
		return err
	}
	v5 := c.mqtt.proto == mqttProtoLevel5
	// For v5 an empty topic is legal when a previously bound Topic Alias
	// resolves it, so defer the check until after properties are read. 3.1.1
	// has no aliases, so reject an empty topic immediately.
	if len(pp.topic) == 0 && !v5 {
		return errMQTTTopicIsEmpty
	}

	if qos > 0 {
		pp.pi, err = r.readUint16("packet identifier")
		if err != nil {
			return err
		}
		if pp.pi == 0 {
			return fmt.Errorf("with QoS=%v, packet identifier cannot be 0", qos)
		}
	} else {
		pp.pi = 0
	}

	// MQTT 5.0: PUBLISH properties follow the packet identifier (or topic, for
	// QoS 0) and precede the payload. Spec5 [3.3.2.3]. Reading here also
	// advances the reader so the payload-size computation below is correct.
	// pp is reused across packets (c.mqtt.pp), so pp.props is assigned on every
	// path below, never left carrying the previous packet's block.
	pp.props = nil
	if v5 {
		propStart := r.pos
		props, perr := r.readProperties(mqttPacketPub)
		if perr != nil {
			return perr
		}
		// Raw block (length prefix + body) as received: forwarded verbatim to v5
		// subscribers unless it carries a Topic Alias, which is hop-local and
		// stripped below. An inbound client PUBLISH can otherwise only carry
		// forwardable properties (Subscription Identifier is server->client
		// only), so verbatim re-emit is spec-correct. Spec5 [3.3.2.3].
		raw := r.buf[propStart:r.pos]
		if props != nil && props.present[mqttPropTopicAlias] {
			// A Topic Alias of 0, or above the maximum we advertised, is
			// invalid; this also covers the disabled case (maximum 0). Spec5
			// [3.3.2.3.4].
			alias := props.topicAlias
			if alias == 0 || alias > c.mqtt.topicAliasMax {
				return fmt.Errorf("%w: %d (maximum %d)", errMQTTTopicAliasInvalid, alias, c.mqtt.topicAliasMax)
			}
			if len(pp.topic) > 0 {
				// Bind (or re-bind, which the spec allows) the alias to this
				// topic. Copy: the reader buffer is transient.
				if c.mqtt.topicAliases == nil {
					c.mqtt.topicAliases = make(map[uint16][]byte)
				}
				c.mqtt.topicAliases[alias] = copyBytes(pp.topic)
			} else {
				// Alias-only PUBLISH: resolve to the bound topic. An unknown
				// alias with an empty topic is a Protocol Error. Spec5 [3.3.2.3.4].
				topic, ok := c.mqtt.topicAliases[alias]
				if !ok {
					return fmt.Errorf("%w: unknown topic alias %d", errMQTTProtocolError, alias)
				}
				pp.topic = topic
			}
			// Strip the alias so it is never forwarded or stored (hop-local).
			pp.props = mqttStripTopicAlias(raw)
		} else {
			if len(pp.topic) == 0 {
				return errMQTTTopicIsEmpty
			}
			// Keep the block only if it carries something (more than the single
			// zero length byte). Copy: the reader buffer is transient.
			if len(raw) > 1 {
				pp.props = copyBytes(raw)
			}
		}
	}

	if err := mqttValidateTopic(pp.topic, "topic"); err != nil {
		return err
	}
	// Convert the topic to a NATS subject. This call will also check that
	// there is no MQTT wildcards (Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1])
	// Note that this may not result in a copy if there is no conversion.
	// It is good because after the message is processed we won't have a
	// reference to the buffer and we save a copy.
	pp.subject, err = mqttTopicToNATSPubSubject(pp.topic)
	if err != nil {
		return err
	}

	// Check for subject mapping. pp is reused across packets, so reset mapped
	// unconditionally: it is only set when a mapping applies, and a stale value
	// from a prior mapped publish must not leak into this one.
	pp.mapped = nil
	if hasMappings {
		// For selectMappedSubject to work, we need to have c.pa.subject set.
		// If there is a change, c.pa.mapped will be set after the call.
		c.pa.subject = pp.subject
		if changed := c.selectMappedSubject(); changed {
			// We need to keep track of the NATS subject/mapped in the `pp` structure.
			pp.subject = c.pa.subject
			pp.mapped = c.pa.mapped
			// We also now need to map the original MQTT topic to the new topic
			// based on the new subject.
			pp.topic = natsSubjectToMQTTTopic(pp.subject)
		}
		// Reset those now.
		c.pa.subject, c.pa.mapped = nil, nil
	}

	// The message payload will be the total packet length minus
	// what we have consumed for the variable header
	payloadSize := pl - (r.pos - start)
	if payloadSize < 0 {
		return fmt.Errorf("%w: invalid remaining length %d for PUBLISH packet", errMQTTMalformedPacket, pl)
	}
	pp.sz = payloadSize
	if pp.sz > 0 {
		start = r.pos
		r.pos += pp.sz
		pp.msg = r.buf[start:r.pos]
	} else if pp.sz == 0 {
		pp.msg = nil
	} else {
		return errMQTTInvalidPublishLength
	}
	return nil
}

func mqttValidateTopic(topic []byte, field string) error {
	if !utf8.Valid(topic) {
		return fmt.Errorf("%w: invalid utf8 for %s %q", errMQTTMalformedPacket, field, topic)
	}
	if bytes.IndexByte(topic, 0) >= 0 {
		return fmt.Errorf("%w: invalid null character in %s %q", errMQTTMalformedPacket, field, topic)
	}
	return nil
}

func mqttValidateString(value string, field string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: invalid utf8 for %s %q", errMQTTMalformedPacket, field, value)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: invalid null character in %s %q", errMQTTMalformedPacket, field, value)
	}
	return nil
}

func mqttPubTrace(pp *mqttPublish) string {
	dup := pp.flags&mqttPubFlagDup != 0
	qos := mqttGetQoS(pp.flags)
	retain := mqttIsRetained(pp.flags)
	var piStr string
	if pp.pi > 0 {
		piStr = fmt.Sprintf(" pi=%v", pp.pi)
	}
	return fmt.Sprintf("%s dup=%v QoS=%v retain=%v size=%v%s",
		pp.topic, dup, qos, retain, pp.sz, piStr)
}

// mqttOriginMarker returns the authenticated No Local origin marker for a
// message published by session idHash on subject with the given payload: an
// HMAC of (idHash, subject, payload) under the server's per-run secret.
//
// It authenticates only a narrow claim used by the No Local decision — "this
// subscriber's own session published this subject+payload" — not that the whole
// stored message is server-created or fresh. Because the secret never leaves the
// server, a client cannot forge a marker for a session it does not control. The
// marker is deterministic (hence emitted in a readable header and observable),
// so it can be replayed; that is not a weakness here, because the marker is
// keyed by the origin idHash: replaying a victim's marker can only re-assert the
// victim's own origin for that same subject+payload, which No Local suppresses
// anyway, while an independently-authored message carries a different origin's
// marker and is delivered. Empty when there is no key or no session (nothing to
// authenticate). idHash and subject contain no NUL, so the NUL separators
// delimit the fields unambiguously. Spec5 [3.8.3.1].
func mqttOriginMarker(key []byte, idHash, subject string, payload []byte) string {
	if len(key) == 0 || idHash == _EMPTY_ {
		return _EMPTY_
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(idHash))
	mac.Write([]byte{0})
	mac.Write([]byte(subject))
	mac.Write([]byte{0})
	mac.Write(payload)
	// 16 bytes of tag is ample against forgery and keeps the header small.
	return base64.RawStdEncoding.EncodeToString(mac.Sum(nil)[:16])
}

// mqttStampedOrigin is the plain origin marker for a message this client is
// publishing on subject with payload, recorded only on the retained slot (retained
// replay judges it under the subscribing filter's current option; owned messages
// carry the No Local stamp instead, see nolocal_stamp.go), or empty when the server does not run v5
// (no No Local subscription can exist) or the client has no session (e.g. an
// internal Will-delivery client, so a Will is never treated as self-published).
// Spec5 [3.8.3.1].
func (c *client) mqttStampedOrigin(subject string, payload []byte) string {
	if c.mqtt == nil || c.mqtt.sess == nil || c.srv == nil || !c.srv.mqtt.v5Enabled {
		return _EMPTY_
	}
	return mqttOriginMarker(c.srv.mqtt.originKey, c.mqtt.sess.idHash, subject, payload)
}

// mqttOriginRetained reports whether pp was published with the RETAIN flag and
// v5 is enabled on this server, so its original RETAIN should be recorded (in
// the Nmqtt-Ret header) for the Retain As Published subscription option. Must be
// called while pp.flags still carries the original RETAIN (before fan-out clears
// it). A 3.1.1-only server records nothing. Spec5 [3.8.3.1].
func (c *client) mqttOriginRetained(pp *mqttPublish) bool {
	return c.srv != nil && c.srv.mqtt.v5Enabled && mqttIsRetained(pp.flags)
}

// mqttNoLocalSuppress reports whether retained replay must skip this retained
// message for a No Local subscription: the slot's origin marker matches the one
// recomputed for the subscribing session. Retained replay alone uses it, because a
// replay is a fresh forwarding judged under the subscription being created; owned
// messages are decided by their ownership-time stamp instead (nolocal_stamp.go). An
// unknown origin (empty marker) never suppresses. Spec5 [3.8.3.1].
func mqttNoLocalSuppress(noLocal bool, originHash, subHash string) bool {
	return noLocal && originHash != _EMPTY_ && originHash == subHash
}

// mqttComputeNatsMsgSize computes the size the NATS message to be delivered
// based on a MQTT PUBLISH packet.
// encodePP: whether to encode complete MQTT PUBLISH packet header information
//   - false: initial delivery (QoS 0/1) needs only base header
//   - true: QoS2 storage needs to encode Nmqtt-Subject and Nmqtt-Mapped
//
// msgId and q2Gen are the inbound-QoS 2 move protocol's two headers and are
// mutually exclusive by construction: msgId (Nats-Msg-Id) rides the RELEASE form
// stored onto $MQTT_msgs, q2Gen (Nmqtt-Q2Gen) rides the HOLD form stored onto
// $MQTT_qos2in. Every other call site passes _EMPTY_ for both. This function must
// stay byte-exact with mqttNewDeliverableMessage below: a disagreement makes
// headerLen wrong and the stored message corrupt.
func mqttComputeNatsMsgSize(pp *mqttPublish, encodePP bool, ttl uint32, noLocal string, retained bool, msgId, q2Gen string) int {
	size := len(hdrLine) +
		len(mqttNatsHeader) + 2 + 2 + // 2 for ':<qos>', and 2 for CRLF
		2 + // end-of-header CRLF
		pp.sz
	if len(noLocal) > 0 {
		size += len(mqttNatsHeaderNoLocal) + 1 + // +1 for ':'
			len(noLocal) + 2 // 2 for CRLF
	}
	if retained {
		size += len(mqttNatsHeaderRetain) + 1 + 1 + 2 // ':' + "1" + CRLF
	}
	if encodePP {
		size += len(mqttNatsHeaderSubject) + 1 + // +1 for ':'
			len(pp.subject) + 2 // 2 for CRLF

		if len(pp.mapped) > 0 {
			size += len(mqttNatsHeaderMapped) + 1 + // +1 for ':'
				len(pp.mapped) + 2 // 2 for CRLF
		}
	}
	// MQTT 5.0 properties are carried (base64) so they can be forwarded to v5
	// subscribers. Independent of encodePP: needed for both delivery and storage.
	if len(pp.props) > 0 {
		size += len(mqttNatsHeaderProps) + 1 + // +1 for ':'
			base64.StdEncoding.EncodedLen(len(pp.props)) + 2 // 2 for CRLF
	}
	// MQTT 5.0 Message Expiry Interval maps to a JetStream per-message TTL so the
	// stored copy is dropped once its lifetime elapses. Spec5 [3.3.2.3.3].
	if ttl > 0 {
		size += len(JSMessageTTL) + 1 + // +1 for ':'
			len(strconv.FormatUint(uint64(ttl), 10)) + 1 + 2 // +1 for 's', 2 for CRLF
	}
	// Inbound QoS 2 move protocol. See qos2_move.go: msgId makes a repeated
	// RELEASE store non-duplicating for stream-backed subscribers, q2Gen is the
	// hold record's immutable identity. Both are bounded by named constants that
	// the max_payload preflight uses (mqttQoS2MsgIdHeaderMax /
	// mqttQoS2GenHeaderMax), so a value longer than its bound would be accepted
	// here and then rejected by the broker — only ever pass the fixed forms.
	if len(msgId) > 0 {
		size += len(JSMsgId) + 1 + // +1 for ':'
			len(msgId) + 2 // 2 for CRLF
	}
	if len(q2Gen) > 0 {
		size += len(mqttQoS2GenHeader) + 1 + // +1 for ':'
			len(q2Gen) + 2 // 2 for CRLF
	}
	return size
}

// mqttMaxQoS2FormSize returns the largest NATS message a QoS 2 PUBLISH will ever
// be encoded as. delivered is the size of the ordinary delivered/stored form
// (already computed by the caller, since it is the QoS-independent baseline).
//
// Every form must fit the account's max payload, and the check has to happen at
// admission: accepting a PUBLISH at PUBREC time that a LATER form then pushes
// past max_payload leaves the client retrying an identical packet forever with
// its hold record never released — worse than the losses these protocols exist
// to fix. Bounds come from named constants shared with the encoders, never from
// live values, because the preflight runs before any generation, claim or stream
// sequence exists.
//
// Extracted from mqttProcessPub so each form can be compared against the others
// directly in a test: several of them do not scale with the payload, so which one
// dominates is header-size dependent, and a form that is only reachable for
// (say) a tiny payload with a short topic is exactly the one an implementation
// forgets to budget.
func mqttMaxQoS2FormSize(pp *mqttPublish, delivered int, retained bool, noLocal string) int {
	total := 0
	for _, n := range mqttQoS2FormCandidates(pp, delivered, retained, noLocal) {
		if n > total {
			total = n
		}
	}
	return total
}

// mqttQoS2FormCandidates is every alternative form a QoS 2 PUBLISH can later be stored
// as, keyed by name. mqttMaxQoS2FormSize is the maximum over it and nothing else.
//
// The candidates are a NAMED SET rather than a chain of `if bigger { total = bigger }`
// for one reason: which form dominates is header-size dependent and mostly accidental, so
// a candidate that is currently smaller than another is invisible to any assertion about
// the maximum. Enumerating them makes "this form is budgeted" a checkable fact
// independent of whether it happens to win today.
//
// ALTERNATIVES, not components. Each is a different record the same PUBLISH may become;
// none of them coexists with another in one stored message, so they are compared and
// never summed.
//
// noLocal is the No Local stamp the HOLD will carry (nolocal_stamp.go); delivered must already
// include it, since the release form is the delivered form re-stored. No other form carries it.
func mqttQoS2FormCandidates(pp *mqttPublish, delivered int, retained bool, noLocal string) map[string]int {
	return map[string]int{
		// RELEASE form: the delivered form plus the Nats-Msg-Id that makes a repeated
		// onward store non-duplicating. See qos2_move.go.
		"release": delivered + mqttQoS2MsgIdHeaderMax,
		// HOLD form: the subject-encoded form plus the immutable generation header and
		// the create-only expected-sequence header. The CAS header is NOT stripped
		// before storage, so it counts.
		"hold": mqttComputeNatsMsgSize(pp, true, 0, noLocal, retained, _EMPTY_, _EMPTY_) +
			mqttQoS2GenHeaderMax + mqttQoS2HoldCASHeaderMax,
		// TERMINAL form: an empty body, so its size does not depend on pp at all — for
		// a small payload with a short topic it can be the largest. Its failure mode is
		// the worst available: the onward store succeeds, the commit is rejected for
		// size, and every retry repeats the delivery.
		"terminal": mqttQoS2TerminalRecordMax(),
		// CLAIM form: the release claim written on a sibling of the hold subject before
		// the onward delivery. Payload-independent like the terminal form, so it too can
		// dominate for a small payload with a short topic, and the same eternal wedge if
		// it is not budgeted: the PUBLISH is accepted at PUBREC time and its PUBREL can
		// then never write the claim it must hold before delivering. See
		// qos2_release_claim.go.
		"release claim": mqttQoS2ClaimRecordMax(),
		// ASSIGNMENT CLAIM form: the shared-subscription election record on
		// $MQTT_qos2assign. Payload-independent like the two above, and unbudgeted it
		// wedges the group durable from the other side — no election can be won, so
		// nothing is ever acked. See qos2_shared_affine.go.
		"assignment claim": mqttQ2AffClaimRecordMax(),
		// AFFINE COPY form: the copy written when this message is delivered to a shared
		// subscription at effective QoS 2 and moved onto the chosen session's own
		// subject. Same class of eternal wedge: the claim never materializes, the group
		// delivery is never acked, and AckWait retries forever. The subscription
		// identifier and Retain As Published option that size it belong to a SUBSCRIBE
		// that has not happened yet, so both are budgeted at their maximum.
		"affine copy": mqttQ2AffineFormMax(pp, retained),
		// AFFINE PUBREL form: the stored PUBREL that completes an affine copy, which
		// since the PUBREC proof also NAMES the assignment it completes. Its own
		// alternative, never folded into the affine copy's bound: the two never coexist
		// in one stored message, so summing them would reject PUBLISHes no real later
		// form justifies. See qos2_shared_affine.go.
		"affine pubrel": mqttQ2AffPubRelFormMax(),
	}
}

// Composes a NATS message from a MQTT PUBLISH packet. The message includes an
// internal header containint the original packet's QoS, and for QoS2 packets
// the original subject.
//
// Example (QoS2, subject: "foo.bar"):
//
//	NATS/1.0\r\n
//	Nmqtt-Pub:2foo.bar\r\n
//	\r\n
func mqttNewDeliverableMessage(pp *mqttPublish, encodePP bool, ttl uint32, noLocal string, retained bool, msgId, q2Gen string) (natsMsg []byte, headerLen int) {
	size := mqttComputeNatsMsgSize(pp, encodePP, ttl, noLocal, retained, msgId, q2Gen)

	buf := bytes.NewBuffer(make([]byte, 0, size))

	qos := mqttGetQoS(pp.flags)

	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsHeader)
	buf.WriteByte(':')
	buf.WriteByte(qos + '0')
	buf.WriteString(_CRLF_)

	// No Local stamp taken at ownership. Absent when no No Local filter of the
	// publisher reached the subject, the publisher has no session, or v5 is off. The
	// QoS 2 HOLD form carries it too, so the release reuses the PUBREC-time stamp.
	// Spec5 [3.8.3.1], [MQTT-4.5.0-1].
	if len(noLocal) > 0 {
		buf.WriteString(mqttNatsHeaderNoLocal)
		buf.WriteByte(':')
		buf.WriteString(noLocal)
		buf.WriteString(_CRLF_)
	}

	// Original RETAIN flag, for the Retain As Published subscription option.
	// Spec5 [3.8.3.1].
	if retained {
		buf.WriteString(mqttNatsHeaderRetain)
		buf.WriteByte(':')
		buf.WriteByte('1')
		buf.WriteString(_CRLF_)
	}

	if encodePP {
		buf.WriteString(mqttNatsHeaderSubject)
		buf.WriteByte(':')
		buf.Write(pp.subject)
		buf.WriteString(_CRLF_)

		if len(pp.mapped) > 0 {
			buf.WriteString(mqttNatsHeaderMapped)
			buf.WriteByte(':')
			buf.Write(pp.mapped)
			buf.WriteString(_CRLF_)
		}
	}

	if len(pp.props) > 0 {
		buf.WriteString(mqttNatsHeaderProps)
		buf.WriteByte(':')
		buf.WriteString(base64.StdEncoding.EncodeToString(pp.props))
		buf.WriteString(_CRLF_)
	}

	// MQTT 5.0 Message Expiry Interval: attach a JetStream per-message TTL so the
	// stored copy expires after the requested lifetime. This is server-internal
	// metadata (not one of the forwarded Nmqtt-* headers). Spec5 [3.3.2.3.3].
	if ttl > 0 {
		buf.WriteString(JSMessageTTL)
		buf.WriteByte(':')
		buf.WriteString(strconv.FormatUint(uint64(ttl), 10))
		buf.WriteByte('s')
		buf.WriteString(_CRLF_)
	}

	// Inbound QoS 2 move protocol headers, sized identically above. Nats-Msg-Id
	// on the RELEASE form gives the $MQTT_msgs store a deduplication key that is
	// stable across every PUBREL retry of one hold generation; Nmqtt-Q2Gen on the
	// HOLD form is that generation. This function is the SOLE writer of
	// Nmqtt-Q2Gen onto a held record — storeQoS2HoldCAS adds only the expected-
	// sequence header, and emitting the generation in both places would double it
	// and invalidate every size bound. See qos2_move.go.
	if len(msgId) > 0 {
		buf.WriteString(JSMsgId)
		buf.WriteByte(':')
		buf.WriteString(msgId)
		buf.WriteString(_CRLF_)
	}
	if len(q2Gen) > 0 {
		buf.WriteString(mqttQoS2GenHeader)
		buf.WriteByte(':')
		buf.WriteString(q2Gen)
		buf.WriteString(_CRLF_)
	}

	// End of header
	buf.WriteString(_CRLF_)

	headerLen = buf.Len()

	buf.Write(pp.msg)
	return buf.Bytes(), headerLen
}

// Process the PUBLISH packet.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPub(c *client, pp *mqttPublish, trace bool) error {
	qos := mqttGetQoS(pp.flags)

	// Reject a client PUBLISH into the broker's internal subject namespace
	// ("$MQTT.*"). An MQTT client can map a "$MQTT/..." topic onto e.g.
	// "$MQTT.JSA.<...>.SP.<hash>.<uuid>" and forge session-persist / JSA traffic
	// to evict or corrupt other sessions. The broker's own internal publishes go
	// through the internal client's processInboundClientMsg, never this client
	// PUBLISH path, so this only blocks external clients from the reserved
	// namespace. Defense in depth: production should ALSO have the NATS account
	// used for auth passthrough deny publish to "$MQTT.>", but the broker must
	// not depend on that config being correct. Drop the message (do not deliver
	// or store); ack QoS>0 with Not authorized (0x87) so the client's flow stays
	// consistent, and do not disconnect on QoS0. Spec5 [3.3.5], reason 0x87.
	if bytes.HasPrefix(pp.subject, []byte(mqttPrefix)) {
		c.Warnf("Rejecting PUBLISH to reserved internal subject %q", pp.subject)
		switch qos {
		case 1:
			c.mqttEnqueuePubResponseReason(mqttPacketPubAck, pp.pi, mqttReasonNotAuthorized, "publish to reserved topic not allowed", trace)
		case 2:
			c.mqttEnqueuePubResponseReason(mqttPacketPubRec, pp.pi, mqttReasonNotAuthorized, "publish to reserved topic not allowed", trace)
		}
		return nil
	}

	// MQTT 5.0 flow control [MQTT-4.9]: the server enforces the portion of the
	// client's send quota it can observe — the QoS 2 messages held between PUBREC
	// and PUBREL. Any QoS>0 PUBLISH arriving while those fill the quota is a
	// violation => DISCONNECT 0x93 via the read loop's error exit. Pure QoS 1
	// overrun is not reliably observable (the slot is returned when the client
	// receives the PUBACK, which the server cannot see) and is not policed here.
	// A DUP re-send of an already-held QoS 2 PI is not a new send and must not
	// double-count. Checked before the message is examined so a flow-control
	// violation trumps any per-message reason.
	if qos > 0 && c.mqtt.recvMaxIn > 0 {
		if _, held := c.mqtt.qos2InFlight[pp.pi]; !(qos == 2 && held) &&
			len(c.mqtt.qos2InFlight) >= int(c.mqtt.recvMaxIn) {
			return fmt.Errorf("%w: %d QoS 2 messages already await PUBREL, receive maximum is %d",
				errMQTTReceiveMaxExceeded, len(c.mqtt.qos2InFlight), c.mqtt.recvMaxIn)
		}
	}

	// No Local is decided at OWNERSHIP (F-117): the stamp is taken here, on the read loop,
	// so it reflects every SUBSCRIBE this connection sent before this PUBLISH and none
	// after it. QoS 0 and 1 are owned by this function; QoS 2 at PUBREC, whose hold
	// carries the stamp to the release. Computed for every QoS, because this standalone
	// broker's QoS 0 fan-out also travels over a real NATS subscription whose callback
	// never carries the publisher's identity. See nolocal_stamp.go. Spec5 [3.8.3.1],
	// [MQTT-4.5.0-1].
	noLocal := c.mqttNoLocalStamp(string(pp.subject), pp.msg)
	// The plain origin marker, used only if this publication updates the retained slot:
	// retained replay is a fresh forwarding judged under the subscribing filter's current
	// option, not an owned message. Empty for 3.1.1 or when v5 is off.
	origin := c.mqttStampedOrigin(string(pp.subject), pp.msg)

	// Enforce max_payload using existing client max payload logic (mpay) by
	// checking the largest NATS message size that would be processed, including
	// the Nats-TTL header that a Message Expiry Interval adds to the stored
	// message. QoS2 is first held with the PUBLISH subject headers (no TTL) and
	// later delivered with the TTL header (no subject headers); the two header
	// sets never coexist, so we compare the actual forms rather than sum them.
	//
	// For QoS 2 there are FOUR forms, and every one of them must fit; see
	// mqttMaxQoS2FormSize.
	//
	// QoS 2 budgets the stamp LATER. The stamp is variable-length and is only ever
	// written by a NEW hold, but this check runs before anyone knows whether the
	// identifier is already held: a DUP of an exchange the broker owns, retransmitted
	// after its publisher added No Local filters, must be answered from the hold rather
	// than refused for a stamp it will never carry. So the forms are budgeted here
	// without it, and mqttStoreQoS2MsgOnce applies the stamped budget on the new-hold
	// path only — the placement F-079 chose for QoS 2 authorization, for the same reason.
	var q2StampRefusal func() error
	if maxPayload := atomic.LoadInt32(&c.mpay); maxPayload != jwt.NoLimit {
		// Both forms carry the Nmqtt-Ret marker for a retained publish (the hold copy so
		// it survives to PUBREL).
		retained := c.mqttOriginRetained(pp)
		ttl := c.mqttStoredMsgTTL(pp)
		var total int
		if qos == 2 {
			total = mqttMaxQoS2FormSize(pp, mqttComputeNatsMsgSize(pp, false, ttl, _EMPTY_, retained, _EMPTY_, _EMPTY_), retained, _EMPTY_)
			if stamped := mqttMaxQoS2FormSize(pp, mqttComputeNatsMsgSize(pp, false, ttl, noLocal, retained, _EMPTY_, _EMPTY_), retained, noLocal); stamped > int(maxPayload) {
				q2StampRefusal = func() error { return c.mqttRefuseOversizedPublish(pp, qos, stamped, maxPayload, false) }
			}
		} else {
			total = mqttComputeNatsMsgSize(pp, false, ttl, noLocal, retained, _EMPTY_, _EMPTY_)
		}
		if total > int(maxPayload) {
			return c.mqttRefuseOversizedPublish(pp, qos, total, maxPayload, trace)
		}
	}

	// MQTT 5.0 Payload Format Indicator: when the publisher declares the payload
	// is UTF-8 (PFI=1) but it is not well-formed, this is a Payload Format Invalid
	// error. Like Quota Exceeded above it is non-fatal for QoS>0 (the ack carries
	// reason 0x99 and the message is discarded); a QoS0 violation cannot be acked,
	// so the connection is closed with DISCONNECT 0x99. Spec5 [3.3.2.3.2], reason
	// 0x99. Only v5 carries the property; 3.1.1 has no PFI.
	if c.mqtt.proto == mqttProtoLevel5 && mqttPayloadFormatIsUTF8(pp.props) && !utf8.Valid(pp.msg) {
		rs := "payload is not valid UTF-8 but the Payload Format Indicator is 1"
		switch qos {
		case 1:
			c.mqttEnqueuePubResponseReason(mqttPacketPubAck, pp.pi, mqttReasonPayloadFormatInvalid, rs, trace)
			return nil
		case 2:
			c.mqttEnqueuePubResponseReason(mqttPacketPubRec, pp.pi, mqttReasonPayloadFormatInvalid, rs, trace)
			return nil
		default:
			return fmt.Errorf("%w: %s", errMQTTPayloadFormatInvalid, rs)
		}
	}

	// AUTHORIZATION, and it goes here for two reasons. It is ahead of every
	// durable effect -- the $MQTT_msgs store, the retained store and the QoS 2
	// hold all hang off the switch below -- and it is over the publication's FULL
	// effective-subject set, so a Sparkplug birth cannot smuggle its derived
	// certificate subject past a check that only looked at what the client named.
	//
	// QoS 2 is excluded here on purpose. An occupied packet identifier is not a new
	// publication at all: MQTT transfers ownership at PUBREC, so a later
	// same-identifier PUBLISH before PUBREL must be answered with PUBREC and no
	// second delivery, WHATEVER subject it names. Checking its subject here would
	// let a deliberately-denied subject reject an exchange the broker already owns,
	// so the QoS 2 gate lives inside mqttStoreQoS2MsgOnce, on the path that has
	// established the hold is new. F-079.
	// Test seam. Parks a PUBLISH that has been PARSED but not yet authorized, which
	// is the only window in which "the snapshot was revoked while an operation was
	// already in hand" can be observed. Without it, closing the socket and revoking
	// the snapshot are indistinguishable -- the socket close alone satisfies every
	// other assertion, so a broker that only closed would pass. Nil in production.
	if h := mqttTestPublishGatePauseV.Load(); h != nil {
		(*h)()
	}
	if qos != 2 && !c.mqttPublicationAuthorized(pp) {
		return c.mqttRefuseUnauthorizedPublish(pp, qos, trace)
	}

	switch qos {
	case 0:
		return s.mqttInitiateMsgDelivery(c, pp, noLocal, origin)

	case 1:
		// [MQTT-4.3.2-2]. Initiate onward delivery of the Application Message,
		// Send PUBACK.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBACK. When its original sender receives
		// the PUBACK packet, ownership of the Application Message is
		// transferred to the receiver.
		err := s.mqttInitiateMsgDelivery(c, pp, noLocal, origin)
		if err == nil {
			c.mqttEnqueuePubResponse(mqttPacketPubAck, pp.pi, trace)
		}
		return err

	case 2:
		// [MQTT-4.3.3-2]. Method A, Store message, send PUBREC.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBREC or PUBCOMP. When its original
		// sender receives the PUBREC packet, ownership of the Application
		// Message is transferred to the receiver.
		err := s.mqttStoreQoS2MsgOnce(c, pp, noLocal, q2StampRefusal)
		if errors.Is(err, errMQTTQoS2PublishRefused) {
			// Already answered with PUBREC 0x87, no hold created, no quota charged.
			return nil
		}
		if err == nil {
			// A success PUBREC leaves the client's quota slot occupied until the
			// matching PUBREL frees it. A DUP re-store returns nil too, so re-adding
			// the PI is idempotent. Only tracked when a Receive Maximum applies.
			// Spec5 [4.9].
			if c.mqtt.recvMaxIn > 0 {
				if c.mqtt.qos2InFlight == nil {
					c.mqtt.qos2InFlight = make(map[uint16]struct{})
				}
				c.mqtt.qos2InFlight[pp.pi] = struct{}{}
			}
			c.mqttEnqueuePubResponse(mqttPacketPubRec, pp.pi, trace)
		}
		return err

	default:
		return fmt.Errorf("unreachable: invalid QoS in mqttProcessPub: %v", qos)
	}
}

// mqttStoredMsgTTL returns the JetStream per-message TTL to attach to pp's
// stored copy: its MQTT 5.0 Message Expiry Interval. AllowMsgTTL is a startup
// prerequisite of the messages stream (validateMsgsStream), so there is no
// stream that would reject the header.
func (c *client) mqttStoredMsgTTL(pp *mqttPublish) uint32 {
	if mqttTestSuppressMsgTTLV.Load() {
		// Test seam. The Message Expiry Interval drives BOTH the delivery-side
		// expiry check and this stored-copy TTL, so a message that is expired for
		// delivery is normally also being reaped from the stream — the two coexist
		// only during JetStream's reaping lag. Dropping just the TTL keeps the
		// stored copy addressable while its MQTT lifetime elapses, which is what
		// makes the "started deliveries are never dropped on expiry" rule testable
		// without racing a reaper.
		return 0
	}
	return mqttMessageExpiryTTL(pp.props)
}

// mqttRefuseOversizedPublish answers a PUBLISH whose NATS encoding (total bytes) exceeds the
// account max payload. A v5 PUBLISH within the advertised Maximum Packet Size can still
// exceed it once re-encoded as a NATS message (header overhead). Rather than close the
// connection, discard the message and report a non-fatal Quota Exceeded so the advertised
// packet-size limit stays truthful; QoS0 has no ack, so drop silently, and return nil. A
// 3.1.1 client has no reason code, so the violation closes the connection.
// Spec5 [3.4.2.1], [3.5.2.1], reason 0x97.
func (c *client) mqttRefuseOversizedPublish(pp *mqttPublish, qos byte, total int, maxPayload int32, trace bool) error {
	if c.mqtt.proto == mqttProtoLevel5 {
		rs := fmt.Sprintf("message size %d exceeds server maximum of %d after encoding", total, maxPayload)
		switch qos {
		case 1:
			c.mqttEnqueuePubResponseReason(mqttPacketPubAck, pp.pi, mqttReasonQuotaExceeded, rs, trace)
		case 2:
			c.mqttEnqueuePubResponseReason(mqttPacketPubRec, pp.pi, mqttReasonQuotaExceeded, rs, trace)
		default:
			c.Debugf("Discarding QoS0 PUBLISH: %s", rs)
		}
		return nil
	}
	c.maxPayloadViolation(total, maxPayload)
	return ErrMaxPayload
}

func (s *Server) mqttInitiateMsgDelivery(c *client, pp *mqttPublish, noLocal, origin string) error {
	return s.mqttInitiateMsgDeliveryJSA(c, c.mqtt.sess.jsa, pp, noLocal, origin, _EMPTY_)
}

// mqttInitiateMsgDeliveryDedup is mqttInitiateMsgDelivery for the inbound QoS 2
// PUBREL release: the stored copy carries a Nats-Msg-Id derived from the hold
// record's immutable generation, so a PUBREL retried after an unconfirmed move
// does not write a second copy onto $MQTT_msgs. Best effort by design — the move
// protocol's durable terminal state is what actually prevents re-delivery, and
// the deduplication window only narrows the unresolved interval. See qos2_move.go.
func (s *Server) mqttInitiateMsgDeliveryDedup(c *client, pp *mqttPublish, noLocal, origin, msgId string) error {
	return s.mqttInitiateMsgDeliveryJSA(c, c.mqtt.sess.jsa, pp, noLocal, origin, msgId)
}

// mqttInitiateMsgDeliveryJSA is mqttInitiateMsgDelivery with an explicit
// JetStream access handle for the QoS1/2 store step, instead of taking it from
// c.mqtt.sess. This lets a delayed Will (MQTT 5.0 Will Delay Interval,
// [MQTT-3.1.2-8]) be published after its originating client is gone, using the
// account's internal client and JSA.
//
// noLocal is the No Local stamp taken when the server took ownership of this message
// (see nolocal_stamp.go): the delivered and stored forms carry it and both delivery
// callbacks decide No Local from it. origin is the plain authenticated origin marker,
// used ONLY for a retained store (retained replay is a fresh forwarding judged under the
// subscribing filter's current option). Both are passed explicitly so they do not depend
// on which client (real or internal) drives delivery: a Will is server-originated and
// passes _EMPTY_ for both. Spec5 [3.8.3.1], [MQTT-4.5.0-1].
//
// msgId, when non-empty, is a Nats-Msg-Id for the $MQTT_msgs store so a repeated
// store of the same logical message is deduplicated by JetStream. Only the
// inbound QoS 2 PUBREL release passes one (see mqttInitiateMsgDeliveryDedup); it
// is NOT set on the live fan-out's behalf, which is at-least-once regardless.
//
// The retained update is part of the publication's outcome (F-060): an error from it is
// returned even when the $MQTT_msgs store succeeded, so no caller gives final success to
// a publication whose retained update the store did not confirm.
func (s *Server) mqttInitiateMsgDeliveryJSA(c *client, jsa *mqttJSA, pp *mqttPublish, noLocal, origin, msgId string) (err error) {
	// Test seam: observes what this publication reported, which is the only way to see
	// the reporting half of a fence exit -- the revocation that trips the fence closes
	// the socket anyway. Nil in production.
	if h := mqttTestDeliveryResultV.Load(); h != nil {
		defer func() { (*h)(pp, err) }()
	}
	// A stored (QoS 1/2) PUBLISH carrying an MQTT 5.0 Message Expiry Interval is
	// given a matching JetStream per-message TTL so it is dropped from the
	// delivery stream once its lifetime elapses. Spec5 [3.3.2.3.3]. Degraded
	// mode drops the header; delivery-side expiry checks still apply.
	//
	// pp.flags still carries the original RETAIN flag here (mqttHandlePubRetain clears
	// it after the fan-out); record it for the Retain As Published option when v5 is
	// enabled. Spec5 [3.8.3.1].
	retained := c.mqttOriginRetained(pp)
	natsMsg, headerLen := mqttNewDeliverableMessage(pp, false, c.mqttStoredMsgTTL(pp), noLocal, retained, msgId, _EMPTY_)

	// Set the client's pubarg for processing.
	c.pa.subject = pp.subject
	c.pa.mapped = pp.mapped
	c.pa.reply = nil
	c.pa.hdr = headerLen
	c.pa.hdb = []byte(strconv.FormatInt(int64(c.pa.hdr), 10))
	c.pa.size = len(natsMsg)
	c.pa.szb = []byte(strconv.FormatInt(int64(c.pa.size), 10))
	// Expose the plain origin marker so mqttHandlePubRetain (invoked below for a
	// retained PUBLISH) records the retained slot's publishing session. Spec5 [3.8.3.1].
	c.mqtt.pubOrigin = origin
	defer func() {
		c.pa.subject = nil
		c.pa.mapped = nil
		c.pa.reply = nil
		c.pa.hdr = -1
		c.pa.hdb = nil
		c.pa.size = 0
		c.pa.szb = nil
		c.mqtt.pubOrigin = _EMPTY_
	}()

	fannedOut, permIssue := c.processInboundClientMsg(natsMsg)
	if permIssue {
		return nil
	}

	qos := mqttGetQoS(pp.flags)
	if qos == 0 && !fannedOut {
		// A QoS 0 publication has NO durable copy, so the fan-out above is the only
		// thing that happened -- and its failure has to reach the caller, because
		// one caller acts on the answer. A delayed Will's worker finalizes and
		// DELETES its outbox record when this returns nil, so reporting success for
		// a fan-out that never left the socket loses the Will permanently. The
		// reconnect buffer covers a connection that is merely reconnecting; a
		// terminally closed one (repeated reconnect authentication failures, say)
		// fails outright, and that is the case this reports.
		return errMQTTWillFanOutFailed
	}

	// The retained update: it stores or clears the retained copy in $MQTT_rmsgs, and
	// for a Sparkplug birth it also derives, publishes and stores a
	// "$sparkplug/certificates/..." subject. It no-ops for a non-retained publish.
	//
	// It runs whether or not the live publish left the socket for QoS 1/2: a failed
	// core-NATS send says nothing about the retained stream, and the PUBACK or PUBCOMP
	// that follows a successful $MQTT_msgs store must not cover a retained update that
	// was never attempted. F-060.
	//
	// pp is passed IN rather than read from c.mqtt.pp, and that is a fix, not a
	// refactor. c.mqtt.pp is the shared PARSER object, overwritten by every packet
	// the client sends. mqttProcessPubRel reconstructs the released publication
	// into a LOCAL pp and hands it down, so the hook used to retain whatever the
	// parser had touched most recently instead. It happened to be right only
	// because nothing usually intervenes between a retained QoS 2 PUBLISH and its
	// PUBREL; with an intervening PUBLISH it retained the wrong message, and with
	// an intervening DENIED Sparkplug birth it manufactured that birth's derived
	// certificate effect on the back of an unrelated authorized exchange.
	//
	// Authorization is already settled by the time we get here: the gate in
	// mqttProcessPub (or, for QoS 2, in mqttStoreQoS2MsgOnce) covered this
	// publication's FULL effective-subject set, derived certificate subject
	// included. That is why there is no second check here, and why adding one
	// would double-refuse a replayed Will. F-079.
	retainErr := c.mqttHandlePubRetain(pp)

	// If QoS 0 messages don't need to be stored, other (1 and 2) do. Store them
	// JetStream under "$MQTT.msgs.<delivery-subject>"
	if qos == 0 {
		// Its retained update is the only durable effect, so its failure is the
		// publication's: the read loop closes the connection (a best-effort DISCONNECT
		// 0x80 under v5), and a Will worker leaves its record to be re-fired.
		return retainErr
	}

	// A retained failure does NOT skip the $MQTT_msgs store below. The live fan-out
	// has already happened and cannot be retracted, so skipping the store would only
	// take the stream-backed subscribers' copy away from a publisher whose session
	// does not survive to re-send. The retained error is returned regardless of how
	// the store goes, joined with the store's own, so it is never erased by a success.

	// We need to call flushClients now since this we may have called c.addToPCD
	// with destination clients (possibly a route). Without calling flushClients
	// the following call may then be stuck waiting for a reply that may never
	// come because the destination is not flushed (due to c.out.fsp > 0,
	// see addToPCD and writeLoop for details).
	c.flushClients(0)

	// Wait out any SUBSCRIBE currently being processed in this account before
	// storing: $MQTT_msgs uses interest-based retention, and a message stored
	// before a racing SUBSCRIBE's consumer registers interest would be
	// discarded outright — permanently invisible to the new subscription. See
	// mqttSubsBarrier. The abort channel lets a delayed-Will worker unblock on
	// shutdown so it cannot hang Phase 3 (nil-safe: a nil server means no abort).
	var barrierAbort <-chan struct{}
	if c.srv != nil {
		barrierAbort = c.srv.willClaimStopCh
	}
	jsa.subsBarrier.wait(barrierAbort)

	// Re-checked immediately before the durable write, not only at the gate. The
	// gate ran before the live fan-out and the subs barrier, and the service epoch
	// can have moved in between -- a write authorized against one incarnation of the
	// service account must not be handed to another. This narrows the window to the
	// store's own round trip; what remains is stated in docs/CONFORMANCE.md, because
	// nats.go can buffer a publish across a reconnect and no check here can unsend
	// it.
	// Test seam. Parks a publication that has ALREADY passed the authorization gate
	// and is about to be written durably, which is the only window the final fence
	// covers and the gate does not. Without it a mutation that drops the fence's
	// validity half survives, because the gate refuses first. Nil in production.
	if h := mqttTestDurableWritePauseV.Load(); h != nil {
		(*h)()
	}
	if !c.serviceEpochStillOurs() {
		return errors.Join(retainErr, errMQTTServiceEpochMoved)
	}
	_, serr := jsa.storeMsg(mqttStreamSubjectPrefix+string(c.pa.subject), headerLen, natsMsg)

	return errors.Join(retainErr, serr)
}

// mqttStoreQoS2MsgOnce stores an inbound QoS 2 PUBLISH as a HELD record on
// $MQTT_qos2in, so it is not broadcast until the sender releases it with PUBREL.
// Returning nil means "the client may be sent PUBREC": either this payload is now
// durably held, or an identical held record for this packet identifier already is
// (a DUP PUBLISH).
//
// This is a bounded create-and-reconcile loop, mirroring enqueueDelayedWill. The
// create is a create-only CAS (expected last subject sequence 0), so a rejection
// means "the subject is occupied" or "transient contention" — never "the write
// landed". Only a re-read can tell those apart, and the re-read's classification
// then decides:
//
//   - absent: the record raced away between the CAS and the read; retry the create.
//   - corrupt: unusable, and checked BEFORE held — see below. Reap it and retry.
//   - delivered: a tombstone from a completed earlier exchange on this identifier
//     that terminal cleanup has not reaped yet. Reap it and retry; misreading it as
//     a duplicate would silently lose THIS payload.
//   - held: a genuine DUP PUBLISH; answer PUBREC without storing again.
//
// The held case is safe to treat as a duplicate because no path lets a client
// believe an exchange finished while a held record survives (the corrupt, malformed
// and expired branches all confirm their cleanup before PUBCOMP — invariant 3) and
// clear() purges hold state on session discard. So a held record on this subject
// always belongs to the exchange the client is currently retrying.
//
// Reaping is what must NOT be assumed: an unconfirmed reap leaves a nonterminal or
// unidentifiable record in place, so it withholds PUBREC entirely (invariant 3, one
// packet earlier) rather than proceeding.
//
// noLocal is the No Local stamp taken in mqttProcessPub, written into the HOLD so the
// release reuses the stamp of the moment ownership transferred, which for QoS 2 is the
// PUBREC ([MQTT-4.3.3], [MQTT-4.5.0-1]). stampRefusal is non-nil when the forms only
// exceed max_payload BECAUSE of that stamp; it refuses a NEW publication, and is only
// reached once the identifier is known not to be held (see mqttProcessPub).
func (s *Server) mqttStoreQoS2MsgOnce(c *client, pp *mqttPublish, noLocal string, stampRefusal func() error) error {
	asm := c.mqtt.asm
	subject := c.mqttQoS2InternalSubject(pp.pi)

	// AUTHORIZATION for QoS 2, and the ordering below is forced by the protocol
	// rather than chosen.
	//
	// MQTT transfers ownership of the message to the receiver at PUBREC, so a
	// later PUBLISH on the SAME identifier before PUBREL must be answered with
	// PUBREC and must not deliver again -- whatever subject it carries. An
	// unconditional subject check ahead of everything would therefore let a
	// deliberately-denied subject reject an exchange the broker already owns.
	//
	// So: the incoming subject is evaluated LOCALLY first, with no I/O. Allowed,
	// and the ordinary CAS-and-reconcile path below runs untouched. Denied, and
	// only then is the identifier's hold read -- solely to ask whether this is an
	// already-accepted exchange rather than a new publication.
	//
	// The cost is one bounded read per denied QoS 2 PUBLISH from an authenticated
	// client. It buys the protocol rule; it is stated in docs/CONFORMANCE.md rather
	// than hidden.
	if !c.mqttPublicationAuthorized(pp) {
		rec, _, lerr := asm.loadQoS2Record(subject)
		switch {
		case lerr != nil:
			// Indeterminate. Never guessed either way: no authorization, no CAS, no
			// PUBREC. The client retries and the exchange is still whatever it was.
			return lerr
		case rec != nil && rec.state == mqttQoS2Held:
			// An exchange this broker already accepted. Answer it from the HOLD; the
			// incoming subject is not used, not stored, and not authorized.
			return nil
		default:
			// Absent, done or corrupt: there is no accepted exchange to protect, so
			// this is a new publication and it is refused. Nothing is mutated and
			// nothing is reaped -- a denied publish must not be able to reap another
			// exchange's record.
			if rerr := c.mqttRefuseUnauthorizedPublish(pp, 2, false); rerr != nil {
				return rerr // 3.1.1: closes the connection
			}
			// v5: PUBREC 0x87 has been sent by the refusal. The sentinel stops the
			// caller sending a SECOND, success-shaped PUBREC and stops it charging the
			// Receive Maximum slot for an exchange that does not exist.
			return errMQTTQoS2PublishRefused
		}
	}

	// THE STAMP'S BUDGET, ordered exactly like authorization above and for the same
	// reason. Only a new hold writes the stamp, so only a new publication can be too
	// large because of it; a DUP of an exchange this broker already owns is answered from
	// its hold, whose stamp was budgeted when it was accepted. Occupancy is otherwise
	// learned from the create CAS below, which would have to attempt the oversized store
	// to learn it — so the hold is read first, with the same bounded read the denied
	// path uses, and only when the stamp is what overflows.
	if stampRefusal != nil {
		rec, _, lerr := asm.loadQoS2Record(subject)
		switch {
		case lerr != nil:
			// Indeterminate: no PUBREC either way; the client retries.
			return lerr
		case rec != nil && rec.state == mqttQoS2Held:
			return nil // an exchange this broker already accepted; PUBREC from the hold
		default:
			if rerr := stampRefusal(); rerr != nil {
				return rerr // 3.1.1: closes the connection
			}
			// v5: PUBREC 0x97 has been sent. The sentinel stops a second, success-shaped
			// PUBREC and the Receive Maximum charge for an exchange that does not exist.
			return errMQTTQoS2PublishRefused
		}
	}

	// The record's immutable identity for the whole exchange: it gates the terminal
	// CAS (invariant 1) and becomes the onward store's Nats-Msg-Id. Generated once
	// per PUBLISH, never rewritten.
	gen := nuid.Next()

	// `true` means encode the MQTT PUBLISH packet in the NATS message header. No
	// TTL here: this is the QoS2 dedup hold stream (no AllowMsgTTL); the expiry
	// TTL is applied when the message is moved to the delivery stream on PUBREL.
	// The original RETAIN flag is recorded so Retain As Published survives the
	// hold and is restored on PUBREL. Spec5 [3.8.3.1].
	//
	// mqttNewDeliverableMessage is the sole writer of Nmqtt-Q2Gen onto the held
	// record; storeQoS2HoldCAS adds only the expected-sequence header. Emitting the
	// generation in both places would double the header and invalidate the
	// max_payload bounds computed in mqttProcessPub.
	// The No Local stamp rides the hold: it was taken at this PUBREC, and the release
	// must reuse it rather than re-sample options a later SUBSCRIBE may have replaced.
	natsMsg, headerLen := mqttNewDeliverableMessage(pp, true, 0, noLocal, c.mqttOriginRetained(pp), _EMPTY_, gen)

	var lastErr error
	for attempt := 0; attempt < mqttQoS2StoreAttempts; attempt++ {
		// Re-checked on every attempt, not once: the loop can span several round trips and
		// the service account must still be the one this publication was authorized
		// against at the moment the hold is actually created.
		if !c.serviceEpochStillOurs() {
			return errMQTTServiceEpochMoved
		}
		_, err := asm.storeQoS2HoldCAS(subject, 0, headerLen, natsMsg)
		if err == nil {
			return nil // freshly held; PUBREC
		}
		lastErr = err
		if !isQoS2CASRejection(err) {
			// A real store failure, not a contested subject. No PUBREC; the client
			// retries the PUBLISH.
			return err
		}
		rec, seq, lerr := asm.loadQoS2Record(subject)
		if lerr != nil {
			// Indeterminate: never guessed into an absence, because retrying the
			// create-only CAS against a subject that actually holds a record would
			// just fail again, and treating it as a duplicate would answer PUBREC for
			// a payload we did not store.
			return lerr
		}
		if rec == nil {
			continue // raced away; retry the create
		}
		switch rec.state {
		case mqttQoS2Corrupt:
			// Checked before held, and it must never fall through to held: a record we
			// cannot identify is not this PUBLISH's payload, and answering PUBREC for
			// it means the following PUBREL releases the OLD record and reports 0x92
			// while this payload is lost.
			c.Errorf("Reaping unusable QoS2 hold record (%s)",
				mqttQoS2CorruptDetail(subject, rec.stored, "no valid generation, or a malformed done marker"))
			if st, rerr := asm.releaseQoS2Record(seq); st != mqttQoS2Released {
				return rerr
			}
		case mqttQoS2Done:
			// A leftover tombstone whose asynchronous cleanup has not landed yet
			// (invariant 4 permits that). Reap it so this identifier is usable again.
			if st, rerr := asm.releaseQoS2Record(seq); st != mqttQoS2Released {
				return rerr
			}
		default: // mqttQoS2Held
			return nil // genuine DUP PUBLISH; PUBREC
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("QoS2 hold not confirmed after %d attempts", mqttQoS2StoreAttempts)
	}
	return lastErr
}

// mqttQoS2InternalSubject is the $MQTT_qos2in hold subject for one of this
// client's inbound packet identifiers.
//
// The client ID is hashed into a NATS-safe token (as sessions do): the raw client
// ID may contain characters invalid in a NATS subject now that arbitrary UTF-8
// client IDs are accepted. mqtt-nats-broker divergence from upstream.
//
// The session's incarnation generation is part of the subject, so a hold belongs
// to one incarnation and a discard purge naming a RETIRED generation cannot reach
// it. Naming was never the whole protection, because a resume preserves the
// generation: a teardown for THIS incarnation, won against a peer still serving it,
// purges by subject and would reach a hold accepted meanwhile. What stops it is the bound
// every hold purge now carries. Which bound each arm uses, and what that does and does not
// license, are stated once in F-062 (docs/findings/ha-arbitration.md) — it is not one
// watermark for all four arms, so do not summarise it as one here.
// Both callers run on the inbound QoS 2 path, well after processConnect installed
// c.mqtt.sess (which the delivery path already dereferences unguarded), and a
// re-mint only ever happens on a discard, which ends the connection.
func (c *client) mqttQoS2InternalSubject(pi uint16) string {
	sess := c.mqtt.sess
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	return mqttQoS2HoldSubject(idHash, gen, pi)
}

// Process a PUBREL packet (QoS2, acting as Receiver).
//
// This is the recoverable MOVE half of the inbound QoS 2 exchange: the held record
// is delivered onward FIRST and only then replaced by a terminal tombstone, so no
// window IN THIS EXCHANGE deletes the message and then fails to deliver it. (A
// teardown running for an incarnation a peer still serves destroys only what it can
// prove it was entitled to destroy: F-062.) The branch order is
// load, corrupt, delivered, malformed, expired, claim, prove, store, commit; see
// qos2_move.go for the state machine, the four design invariants it obeys, and the
// honest guarantee (no silent loss on this exchange's own paths; exactly-once for
// durable subscribers only while deduplication survives the unresolved store..commit
// interval).
//
// The claim (R) and the proof (P) are the two fences that serialize this release
// against a concurrent session discard, and neither substitutes for the other: the
// claim fences a DISCARD, which a re-read cannot because read-then-fan-out is
// check-then-act, and the proof fences an ALREADY-COMPLETED exchange, which the
// claim cannot because a completed exchange leaves nothing to contend with. Both
// must precede the onward store, whose fan-out cannot be taken back. See
// qos2_release_claim.go.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPubRel(c *client, pi uint16, trace bool) error {
	// A PUBCOMP (with any reason, including 0x92) ends the exchange and returns
	// the client's send-quota slot, so the two travel together: sendPubComp
	// gates both. It stays true for every outcome that answers the client —
	// success, an unknown PI, a released corrupt or malformed record, an
	// expiry-dropped hold, a release the discard won, and a superseded finalize — and
	// is cleared ONLY where a broker-side JetStream failure means the exchange is
	// unfinished (an indeterminate lookup, an unresolved claim or proof, a failed
	// onward store, an unconfirmed terminal commit, or an unconfirmed nonterminal
	// release), because MQTT 5
	// gives PUBCOMP no reason code for "the broker failed" (only 0x00 and 0x92
	// are defined) and answering anyway would tell the client an exactly-once
	// exchange completed for a message still held. Spec5 [4.9], [MQTT-4.3.3-1].
	//
	// Freeing the identifier is exactly what makes unconfirmed cleanup unsafe, so
	// the corrupt, malformed and expired branches below clear sendPubComp when their
	// delete cannot be confirmed (invariant 3), while the terminal tombstone's
	// delete is allowed to be asynchronous (invariant 4).
	sendPubComp := true
	reason := mqttReasonSuccess
	var reasonStr string
	defer func() {
		if !sendPubComp {
			return
		}
		delete(c.mqtt.qos2InFlight, pi)
		c.mqttEnqueuePubResponseReason(mqttPacketPubComp, pi, reason, reasonStr, trace)
	}()

	subject := c.mqttQoS2InternalSubject(pi)
	asm := c.mqtt.asm

	// releaseNonterminal disposes of a record that is NOT a terminal tombstone
	// (corrupt, malformed, or expiry-dropped) and reports whether the client may be
	// answered. An unconfirmed delete must not be reported as done: PUBCOMP frees
	// the packet identifier, and the client's next PUBLISH on it would then be
	// matched against the surviving record and silently lost. Invariant 3.
	releaseNonterminal := func(seq uint64, what string) error {
		st, rerr := asm.releaseQoS2Record(seq)
		if st == mqttQoS2Released {
			return nil
		}
		sendPubComp = false
		c.Errorf("Unable to release %s QoS2 hold record for packet identifier %v: %v", what, pi, rerr)
		return rerr
	}

	// L: load and classify the current record on this identifier's hold subject.
	rec, seq, err := asm.loadQoS2Record(subject)
	switch {
	case err != nil:
		// A broker-side lookup failure, NOT a missing PI. Nothing has been
		// delivered or removed yet, so the record is still there: suppress the
		// PUBCOMP and close the connection so the client retries PUBREL with its
		// session state intact.
		sendPubComp = false
		c.Errorf("Unable to look up QoS2 hold entry for packet identifier %v: %v", pi, err)
		return err
	case rec == nil:
		// No record found: no session state for this PI, which a v5 PUBCOMP
		// reports as 0x92. This includes an exchange that DID complete whose
		// tombstone has since been reaped — absence is indistinguishable from
		// never-existed, and Spec5 sanctions 0x92 during recovery. Spec5
		// [MQTT-4.3.3-1].
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	}

	// K: the record carries no usable identity (missing/duplicated/malformed
	// generation, or a done marker we cannot attribute). It can be neither released
	// as ours nor delivered as a message, so dispose of it and admit the identifier's
	// state is gone. Metadata only in the log line: the body is an application
	// payload and can be as large as max_payload.
	if rec.state == mqttQoS2Corrupt {
		c.Errorf("Discarding unusable QoS2 hold record (%s)",
			mqttQoS2CorruptDetail(subject, rec.stored, "no valid generation, or a malformed done marker"))
		if rerr := releaseNonterminal(seq, "unusable"); rerr != nil {
			return rerr
		}
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	}

	// T: the terminal tombstone. This exchange already delivered onward and
	// committed, so answer success WITHOUT re-delivering — indefinitely, and
	// independently of any deduplication window. This branch is what the whole
	// protocol buys. Reap the tombstone asynchronously (invariant 4): a surviving
	// tombstone is inert, since this branch answers from it and a later PUBLISH on
	// this identifier reaps it.
	if rec.state == mqttQoS2Done {
		_ = asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, seq, false)
		return nil
	}

	stored := rec.stored
	gen := rec.gen

	// V: only MQTT QoS2 messages should be here, and they must have a subject. The
	// generation IS valid here (K already handled the case where it is not), so this
	// is a record we can prove is ours but cannot deliver. Release it before
	// answering, for the same reason as K. Metadata only, again.
	h := mqttParsePublishNATSHeader(stored.Header)
	if h == nil || h.qos != 2 || len(h.subject) == 0 {
		c.Errorf("Discarding QoS2 hold record with malformed publish headers (%s)",
			mqttQoS2CorruptDetail(subject, stored, "missing or invalid Nmqtt-Pub/Nmqtt-Subject"))
		if rerr := releaseNonterminal(seq, "malformed"); rerr != nil {
			return rerr
		}
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	}

	pp := &mqttPublish{
		topic:   natsSubjectToMQTTTopic(h.subject),
		subject: h.subject,
		mapped:  h.mapped,
		msg:     stored.Data,
		sz:      len(stored.Data),
		pi:      pi,
		flags:   h.qos << 1,
		props:   h.props, // forward v5 properties captured at store time
	}
	// Restore the original RETAIN flag recorded at store time so Retain As
	// Published works for a QoS2 retained publish released on PUBREL. Spec5
	// [3.8.3.1].
	if h.retained {
		pp.flags |= mqttPubFlagRetain
	}
	// The No Local stamp taken at PUBREC, when ownership transferred, and carried by the
	// hold. Reused verbatim, never re-sampled: a SUBSCRIBE between PUBREC and PUBREL
	// replaced a subscription for messages owned after it, not this one. Every retried
	// release reads the same hold, so every copy it fans out or stores is judged alike.
	// Spec5 [MQTT-4.3.3], [MQTT-4.5.0-1].
	holdNoLocal := string(h.noLocal)

	// X: MQTT 5.0 Message Expiry Interval. A QoS2 message ages while it waits in the
	// server for the PUBREL. Measure the interval from the original PUBLISH receipt
	// (the hold-stream store time) rather than restarting it here: age the carried
	// interval by the time already elapsed, and drop the message if it has expired
	// before release. The drop is this path's own responsibility — release the record
	// before answering, again per invariant 3. Spec5 [3.3.2.3.3].
	var expired bool
	if pp.props, expired = mqttForwardExpiry(pp.props, stored.Time.UnixNano()); expired {
		if rerr := releaseNonterminal(seq, "expired"); rerr != nil {
			return rerr
		}
		return nil
	}

	// R: the release claim — fence 1, and the last point at which a concurrent
	// session discard can still win. It is a create-only CAS on a sibling of the hold
	// subject, so for the exchange occupancy a discard actually enumerated and competed
	// for, exactly one of "release it" and "discard it" succeeds and the loser does
	// nothing irreversible. It must precede everything below: the
	// fan-out inside the onward store cannot be taken back. See qos2_release_claim.go.
	//
	// This fence decides only the occupancy it competed for. What that is worth is bounded
	// by the marker's fence, which makes an occupancy the discard was never entitled to
	// provable here (F-062), and by one open blocker that is not decidable here at all:
	// F-036 (docs/findings/ha-arbitration.md). Do not restate their boundary in this file.
	if sess := c.mqtt.sess; sess != nil {
		if !sess.beginQoS2ReleaseOp() {
			// This session is being discarded on this broker. Deliver nothing and claim
			// nothing: the discard is about to destroy this hold, and the client's
			// session state with it. Answering 0x92 is the same answer the durable claim
			// would have produced had the discard been on another broker.
			reason = mqttReasonPacketIDNotFound
			reasonStr = "no message found for the packet identifier"
			return nil
		}
		defer sess.endQoS2ReleaseOp()
	}

	claimStatus, claimSeq, cerr := asm.claimQoS2Release(s, subject, gen, seq)
	switch claimStatus {
	case mqttQoS2ClaimClaimed, mqttQoS2ClaimAlreadyClaimed:
		// Ours. AlreadyClaimed is not an error and not a conflict: the generation
		// identifies the EXCHANGE, not the broker, so it means our own earlier attempt
		// or a peer releasing the same exchange. The claim is discard-exclusive, never
		// releaser-exclusive.
	case mqttQoS2ClaimDiscarded, mqttQoS2ClaimUnreadable:
		// A discard won for THIS occupancy of the claim subject, or a record we cannot
		// attribute holds it. Nothing may be delivered here. A marker left for an EARLIER
		// occupancy no longer reaches this branch: it records the fence its writer was
		// entitled to destroy, so claimQoS2Release reaps it and retries the create when our
		// hold sits above that fence (F-062 mechanism 2). The Unreadable arm shares this
		// branch and is by definition not a legitimate marker, which is why the claim being
		// made here is about the Discarded VERDICT and not about the branch.
		// Release the hold record before answering
		// (invariant 3) and report the identifier as gone. The CLAIM is deliberately
		// not reaped: a `discarded` claim is the only durable proof that a later
		// releaser must lose, and an unreadable one may be a corrupted instance of one.
		if rerr := releaseNonterminal(seq, "discarded"); rerr != nil {
			return rerr
		}
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	default: // mqttQoS2ClaimIndeterminate
		// We cannot prove we hold the claim, so we must not deliver, and the exchange
		// is unfinished: suppress the PUBCOMP and let the client retry.
		sendPubComp = false
		c.Errorf("Unable to claim the QoS2 release for packet identifier %v: %v", pi, cerr)
		return cerr
	}

	// P: prove the exchange is still the one we loaded — fence 2. The claim cannot
	// catch a peer that completed this exchange and reaped both records while we were
	// between the load and the claim: our create-only claim then SUCCEEDS against an
	// empty subject and we would fan out a second time from the payload we still hold
	// in memory. Every accepting outcome is generation-qualified.
	vStatus, vSeq, verr := asm.verifyQoS2HoldForRelease(subject, gen, seq)
	switch vStatus {
	case mqttQoS2VerifyHeld:
		// Our record, at our sequence. Holding the claim fences a discard that has not yet
		// committed to this subject's purge, and a discard that already committed to one
		// could not have covered this record anyway: its purge was bounded to what it
		// enumerated (F-062). See verifyQoS2HoldForRelease.
	case mqttQoS2VerifyDelivered:
		// Our generation's own tombstone: a peer completed this exchange. Answer from
		// it without re-delivering, exactly as branch T does, and reap both records
		// asynchronously (invariant 4).
		asm.reapQoS2RelClaim(claimSeq)
		_ = asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, vSeq, false)
		return nil
	case mqttQoS2VerifyGone:
		// Terminated and already reaped. Absence means terminated rather than
		// discarded precisely because a discard claims before it purges, and we hold
		// the claim.
		//
		// One further case reaches here and answers 0x00 for an exchange that was in
		// fact DISCARDED: the orphan sweep retires an aged `discarded` marker, but only
		// after destroying the hold and confirming it gone, so a releaser that preloaded
		// the payload and resumed later finds an empty claim subject and then an absent
		// record. For the exchange that marker was written for, nothing is delivered and
		// nothing is lost — only the reason code is more optimistic than the marker-backed
		// 0x92. A LATER exchange's hold is not reachable this way: the sweep's purge is
		// bounded to the exact record it observed and re-confirmed, so a hold accepted
		// after that observation is spared rather than destroyed (F-062). See
		// orphan_qos2_sweep.go and the residual in docs/CONFORMANCE.md.
		asm.reapQoS2RelClaim(claimSeq)
		return nil
	case mqttQoS2VerifySuperseded:
		// A foreign generation holds the subject and was NOT touched. Our exchange's
		// state is no longer ours to close, so 0x92 is the honest answer; our own claim
		// is stale and can go.
		asm.reapQoS2RelClaim(claimSeq)
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	default: // mqttQoS2VerifyIndeterminate
		sendPubComp = false
		c.Errorf("Unable to verify the QoS2 hold record for packet identifier %v before release: %v", pi, verr)
		return verr
	}

	// W: the retirement classifier — F-036 unit 2, and the last fence before the
	// irreversible act. Neither the claim nor fence 2 can see the two populations it
	// answers for: a hold a discard's one-shot enumeration never named, and one its
	// F-062 cutoff SPARED above its own watermark. For both of those the session was
	// durably discarded and this broker was never told, so without this the release
	// fans an acknowledged payload out for a session that no longer exists and answers
	// the client 0x00.
	//
	// It sits AFTER the proof so it classifies only the hold just re-proved at its own
	// sequence, and BEFORE the store because the fan-out inside the store cannot be
	// taken back. See qos2_retire_classify.go for the two-dimensional classification,
	// the identity term the packet lease supplies, and why taking the record fence here
	// cannot deadlock against a local discard.
	if sess := c.mqtt.sess; sess != nil {
		switch verdict, werr := sess.classifyQoS2ReleaseAgainstRetirement(asm); verdict {
		case mqttQoS2RetireProceed:
		case mqttQoS2RetireRetired:
			// The session this hold belongs to was durably discarded elsewhere. Deliver
			// nothing, and release the hold NONTERMINALLY before answering — the same
			// invariant-3 ordering the discarded-claim arm above uses, and for the same
			// reason: PUBCOMP frees the packet identifier, so an unconfirmed cleanup
			// would let the client's next PUBLISH on it be matched against a surviving
			// record and silently lost.
			//
			// The CLAIM stays until that release is CONFIRMED, and only then may it be
			// reaped at the exact sequence we hold. Reaping it first would empty the
			// claim subject while the payload is still verifiably held, which is what
			// lets a paused releaser win a fresh claim and re-read this hold as Held.
			if rerr := releaseNonterminal(seq, "retired"); rerr != nil {
				return rerr
			}
			asm.reapQoS2RelClaim(claimSeq)
			reason = mqttReasonPacketIDNotFound
			reasonStr = "no message found for the packet identifier"
			return nil
		default: // mqttQoS2RetireUnresolved
			// The question could not be decided, and no verdict is not permission. The
			// exchange is unfinished: keep the hold AND the claim, and suppress the
			// PUBCOMP so the client retries.
			sendPubComp = false
			c.Errorf("Unable to classify the QoS2 release for packet identifier %v against the retirement watermark: %v", pi, werr)
			return werr
		}
	}

	// S: the onward store, carrying a Nats-Msg-Id derived from the immutable
	// generation. A failure here means the exchange is unfinished and the record is
	// still held: suppress the PUBCOMP so the client retries. (Before this fix the
	// failure was returned with sendPubComp still true, telling the client an
	// exactly-once exchange had completed over a message that existed nowhere.)
	// The same holds for a retained update the store did not confirm (F-060): the
	// error is returned even when the onward store succeeded, so the PUBCOMP is
	// withheld and the re-sent PUBREL re-attempts the whole release.
	//
	// Note that the live core-NATS fan-out happens INSIDE this call, before the
	// stream store. A retry therefore repeats the fan-out: QoS 0 subscribers, plain
	// NATS subscribers and the retained store may see the message twice. That
	// duplicate is irreducible (see residual 1 in qos2_move.go); the msgId is what
	// keeps the stored copy single for stream-backed subscribers.
	if err := s.mqttInitiateMsgDeliveryDedup(c, pp, holdNoLocal,
		c.mqttStampedOrigin(string(pp.subject), pp.msg), mqttQoS2ReleaseMsgId(gen)); err != nil {
		sendPubComp = false
		c.Errorf("Unable to deliver released QoS2 message for packet identifier %v: %v", pi, err)
		return err
	}

	// C: commit the terminal state. Only now is it safe to leave `held`, and only
	// against our own generation (invariant 1).
	status, doneSeq, ferr := asm.finalizeQoS2Move(s, subject, gen, seq)
	switch status {
	case mqttQoS2FinalizeCommitted:
		// Terminal state is durable, so the tombstone's own removal is inert
		// (invariant 4): push it without waiting. The claim goes the same way, and only
		// now — reaping it before the commit would let a peer stalled since before our
		// claim win a fresh one (fence 2 would still stop it delivering, but the claim
		// must not be the thing that stops working).
		asm.reapQoS2RelClaim(claimSeq)
		_ = asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, doneSeq, false)
		return nil
	case mqttQoS2FinalizeGone:
		// Our generation is already absent and the onward store succeeded, so the
		// exchange is complete. Only our claim may be left.
		asm.reapQoS2RelClaim(claimSeq)
		return nil
	case mqttQoS2FinalizeSuperseded:
		// A later exchange on this identifier owns this subject and was NOT touched. Our
		// message was delivered, but this identifier's state is no longer ours to close, so
		// 0x92 is the honest answer (residual 5). Our own claim is stale and can go.
		asm.reapQoS2RelClaim(claimSeq)
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	default: // mqttQoS2FinalizeIndeterminate
		// Delivery happened but the terminal state is unconfirmed. Suppress the
		// PUBCOMP: the client retries, and the retry either finds the tombstone (T,
		// answered 0x00 with no re-delivery) or re-delivers with the same msgId. The
		// claim STAYS: the retry adopts it, and while it stands a discard still loses.
		sendPubComp = false
		c.Errorf("Unable to commit QoS2 move for packet identifier %v: %v", pi, ferr)
		return ferr
	}
}
