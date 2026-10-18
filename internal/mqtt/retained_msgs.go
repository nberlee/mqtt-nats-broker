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

// Retained messages: their shape, processing and cache, encoding and decoding, loading,
// serializing for a new subscription, an inbound retained PUBLISH, and the retained frames
// sent to new subscriptions.

package mqtt

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// mqttRetainedMsg is stored and read back through the NATS header format that
// mqttEncodeRetainedMessage writes, never as a marshalled struct — so the fields
// carry no serialization tags.
type mqttRetainedMsg struct {
	Origin  string
	Subject string
	Topic   string
	Msg     []byte
	Flags   byte
	Source  string
	// Props is the raw MQTT 5.0 properties block captured from the retained
	// PUBLISH, forwarded to v5 subscribers when the message is replayed. Empty
	// for messages published by 3.1.1 clients or without properties.
	Props []byte

	// SessionHash is the authenticated No Local origin marker (HMAC of the
	// publishing session hash, subject, and payload under a per-server secret)
	// for the session that published this retained message, so No Local can
	// suppress replaying it to that same session without trusting a forgeable
	// header. Empty when there is no publishing session (a Will) or no origin key.
	// Spec5 [3.8.3.1].
	SessionHash string

	// expires is the absolute deadline after which this retained message must no
	// longer be delivered, when it was published with an MQTT 5.0 Message Expiry
	// Interval. Zero when the message has no expiry. Persisted with the stored
	// message via the mqttNatsRetainedMessageExpiry header (not JSON). Spec5
	// [3.3.2.3.3].
	expires time.Time

	expiresFromCache time.Time
}

type mqttRetainedMsgRef struct {
	sseq uint64
	// deleted marks a PLACEHOLDER: a deletion at sseq that a writer other than the
	// retained observer saw ahead of it. Invisible to every reader; it exists only so a
	// delayed older application cannot walk back over the deletion. See
	// retained_index.go.
	deleted bool
}

// processRetainedMsg is the retained observer's callback: it loads all retained messages
// at startup and applies every later change, each in stream order, through the arbitrated
// index (retained_index.go). Classification and the test gate run here, outside any lock;
// commitRetainedDelivery re-validates the generation and the delivery sequence and applies
// the record in one critical section.
//
// Run from the observer subscription's nats.go dispatch goroutine, one per generation.
// No lock held on entry.
func (as *mqttAccountSessionManager) processRetainedMsg(sub *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	sm := as.retainedSeams()
	if sm != nil && sm.testRetainedDeliveryDrop != nil && sm.testRetainedDeliveryDrop(subject) {
		return
	}
	// Unlike upstream, the shim's msgParts (shim_engine.go) returns the pure
	// payload with no trailing wire-protocol "\r\n" to strip (frameNATSMsg
	// builds rmsg from nats.go's already-decoded m.Data, which carries no such
	// terminator) — stripping here would truncate the last 2 payload bytes.
	h, m := c.msgParts(rmsg)
	if reply == _EMPTY_ && mqttRetainedIsHeartbeat(h) {
		last, _ := strconv.ParseUint(string(sliceHeader(JSLastConsumerSeq, h)), 10, 64)
		as.commitRetainedHeartbeat(sub, last)
		return
	}
	// Get the stream and consumer delivery sequences for this message. A message with no
	// ACK reply has neither, and the commit treats it as a gap.
	seq, dseq, _, _, _ := ackReplyInfo(reply)

	// The index is keyed by the LOGICAL retained subject, not by the stream subject the
	// callback carries.
	logical := retainedLogicalSubject(subject)
	var rm *mqttRetainedMsg
	apply := true
	if len(sliceHeader(JSMarkerReason, h)) > 0 {
		// A JetStream subject delete marker — sourced in from an upstream that has them,
		// since this stream refuses SubjectDeleteMarkerTTL itself. It records that the
		// subject's record is gone: a deletion at its own sequence.
	} else if decoded, err := mqttDecodeRetainedMessage(subject, h, m); err != nil {
		// A record this broker cannot decode is an anomaly, not a message meant
		// for someone else: the only format written is the header format, and no
		// mechanism relies any more on servers silently ignoring what they cannot
		// parse. Fail CLOSED — leaving a valid predecessor indexed and cached for a
		// subject whose authoritative stream record is unreadable would be fail-open —
		// so it is a deletion AT this record's sequence, ordered like any other: a
		// delayed corrupt record cannot evict a newer valid one. This matches
		// loadRetainedMessages, so the live and the post-restart views agree.
		c.Warnf("Rejecting retained message on %q: %v", subject, err)
	} else if strings.IndexByte(decoded.Subject, 0x7f) >= 0 {
		c.Warnf("Skipping retained message for subject %q: unsupported character 0x7f", decoded.Subject)
		apply = false
	} else {
		logical = decoded.Subject
		if len(m) > 0 {
			// The `rm.Msg` references some buffer that we don't own; the cache write
			// copies it if `rm` ends up cached.
			rm = decoded
		}
		// An empty payload is the tombstone: it IS the deletion, and the store has
		// already evicted the record it replaces (MaxMsgsPer=1 enforcement runs on every
		// store, empty body or not). Spec [MQTT-3.3.1-11].
	}
	if apply && sm != nil && sm.testRetainedApplyGate != nil {
		sm.testRetainedApplyGate(logical, rm == nil)
	}
	as.commitRetainedDelivery(sub, seq, dseq, logical, rm, apply)
	if sm != nil && sm.testRetainedCommitObserve != nil {
		sm.testRetainedCommitObserve(logical)
	}
}

// A timer loop to cleanup up expired cached retained messages for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) cleanupRetainedMessageCache(s *Server, closeCh chan struct{}) {
	tt := time.NewTicker(mqttRetainedCacheTTL)
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
			// Set a limit to the number of retained messages to scan since we
			// lock as for it. Since the map enumeration gives random order we
			// should eventually clean up everything.
			i, maxScan := 0, 10*1000
			now := time.Now()
			as.rmsCache.Range(func(key, value any) bool {
				rm := value.(*mqttRetainedMsg)
				// Evict when the cache entry aged out, or when the message
				// itself reached its MQTT 5.0 Message Expiry deadline (its
				// stored copy is TTL-reaped by JetStream at the same time).
				if now.After(rm.expiresFromCache) ||
					(!rm.expires.IsZero() && now.After(rm.expires)) {
					as.rmsCache.Delete(key)
				}
				i++
				return i < maxScan
			})

		case <-closeCh:
			return
		case <-s.quitCh:
			return
		}
	}
}

// Retained publish messages matching this subscription are serialized in the
// subscription's `prm` mqtt writer. This buffer will be queued for outbound
// after the subscription is processed and SUBACK is sent or possibly when
// server processes an incoming published message matching the newly
// registered subscription.
//
// Runs from the client's readLoop.
// Account session manager lock held on entry.
// Session lock held on entry.
func (as *mqttAccountSessionManager) serializeRetainedMsgsForSub(rms map[string]*mqttRetainedMsg, sess *mqttSession, c *client, sub *subscription, trace bool) {
	if as.retmsgs.Size() == 0 || len(rms) == 0 {
		return
	}
	toTrace := []mqttPublish{}
	// The entitlement this SUBSCRIBE runs under, captured once: every durable act of the replay is
	// fenced by it (F-105 7c).
	ent, _ := c.outboundEntitlement()
	as.retmsgs.Match(sub.subject, func(subj []byte, ref *mqttRetainedMsgRef) {
		if ref.deleted {
			return // a placeholder is a deletion, never a candidate
		}
		rm := rms[string(subj)]
		if rm == nil {
			// This should not happen since we pre-load messages into rms before
			// calling serialize.
			return
		}
		// MQTT 5.0 Message Expiry Interval: an expired retained message must not be
		// delivered. The stored copy is reaped by its per-message TTL; this guards
		// the window where the in-memory ref/cache still points at it. Spec5
		// [3.3.2.3.3].
		if !rm.expires.IsZero() && !time.Now().Before(rm.expires) {
			return
		}
		// No Local: do not replay a retained message this same session published.
		// The stored SessionHash is the authenticated origin marker; recompute it
		// for this session+subject+payload and suppress only on an authentic
		// match. Spec5 [3.8.3.1].
		if sub.mqtt.noLocal {
			expected := mqttOriginMarker(c.srv.mqtt.originKey, sess.idHash, string(subj), rm.Msg)
			if mqttNoLocalSuppress(true, rm.SessionHash, expected) {
				return
			}
		}
		// A broad wildcard subscription can overlap a subscribe deny clause, and
		// retained replay rides the SERVICE connection, so NATS does not filter it
		// -- this guard is the only thing that does. F-081.
		//
		// Both non-allow verdicts return, and here they need no other disposal:
		// nothing has been allocated yet (the packet identifier is taken below), so
		// there is nothing to release and no JetStream delivery to acknowledge.
		if c.checkDenySubVerdict(string(subj), string(sub.queue)) != mqttSubDeliverAllow {
			return
		}
		var pi uint16
		// nonce is THIS allocation's identity, read back under the same lock that minted
		// it. The frame carries it so its eventual disposition can name the exchange it
		// disposes of rather than an identifier a later allocation may hold (F-101).
		var nonce string
		qos := min(mqttGetQoS(rm.Flags), sub.mqtt.qos)
		if c.mqtt.rejectQoS2Pub && qos == 2 {
			c.Warnf("Rejecting retained message with QoS2 for subscription %q, as configured", sub.subject)
			return
		}
		if qos > 0 {
			pi = sess.trackPublishRetained(qos)

			// The identifier has to stay reserved for the WHOLE span from here to
			// whichever later queueOutbound writes this frame, and the pendingPublish
			// entry trackPublishRetained just installed cannot do that job: clear()
			// nils it and resets last_pi while a flush is still to come, and the
			// displaced connection's write outlives the call. Taken in this same
			// sess.mu acquisition so the identifier is never allocated-and-unreserved
			// (F-091).
			if pi != 0 {
				if ack := sess.pendingPublish[pi]; ack != nil {
					nonce = ack.regNonce
				}
			}
			if pi != 0 && !sess.beginRetainedSendLocked(c, pi) {
				// SKIPPED, not downgraded. Every clause of the refusal is unreachable
				// behind bumpPI and the ownership gate, so this is defensive — and the
				// existing pi == 0 downgrade below already deviates from the exact
				// minimum-QoS rule ([MQTT-3.8.4-6], Spec5 [3.8.4-8]: the delivered QoS
				// must EQUAL min(published, granted), not merely not exceed it). One
				// route into that deviation is pre-existing; a second one added here
				// would be this change's own.
				sess.untrackPublish(pi, mqttUntrackUnanswered) // never sent: no binding
				return
			}

			// If we failed to get a PI for this message, send it as a QoS0, the
			// best we can do?
			if pi == 0 {
				qos = 0
			}
		}

		// MQTT 5.0 Message Expiry Interval: a v5 subscriber must receive the
		// interval reduced by the time the retained message has been waiting in the
		// server. Spec5 [3.3.2.3.3]. 3.1.1 subscribers get no properties section.
		v5 := c.mqtt.proto == mqttProtoLevel5
		props := rm.Props
		if v5 && !rm.expires.IsZero() {
			rem := uint32(time.Until(rm.expires).Seconds())
			if rem < 1 {
				rem = 1
			}
			props = mqttSetMessageExpiry(rm.Props, rem)
		}
		// MQTT 5.0 Subscription Identifier: inject the subscription's identifier
		// (this path builds the header directly, bypassing mqttEnqueuePublishMsgTo).
		// After the expiry rewrite and before the size check below. Spec5 [3.8.2.1.2].
		if v5 && sub.mqtt.subID > 0 {
			props = mqttInjectSubID(props, sub.mqtt.subID)
		}

		// Need to use the subject for the retained message, not the `sub` subject.
		// We can find the published retained message in rm.sub.subject.
		// Set the RETAIN flag: [MQTT-3.3.1-8].
		// Retained frames are intentionally not aliased: they are buffered into
		// sub.mqtt.prm here (under the session lock) and flushed later under cc.mu,
		// so binding an alias here could order the binding after live frames already
		// queued. A full topic is always legal; the first live message binds instead.
		flags, headerBytes := mqttMakePublishHeader(pi, qos, false, true, v5, []byte(rm.Topic), props, len(rm.Msg))

		// MQTT 5.0 Maximum Packet Size: skip a retained message that would exceed
		// the client's limit rather than send an oversized packet (which a strict
		// client rejects as a protocol error). Discarding is spec-correct: the
		// message is treated as delivered. Release the packet identifier if one
		// was allocated (the session lock is held on entry). Spec5 [3.1.2.11.4].
		if mp := c.mqtt.maxPacketSize; mp != 0 && len(headerBytes)+len(rm.Msg) > int(mp) {
			if pi != 0 {
				// The one place a RESERVED retained frame is provably never written: it
				// was never even buffered. Abandoned rather than written, so the
				// generation fence does not apply — no bytes reached the client (F-091).
				// The abandonment untracks the entry itself, so the separate
				// untrackPublish this site used to issue is gone rather than left beside
				// it: two mechanisms freeing one identifier is one too many (F-101).
				sess.releaseRetainedSendAbandonedLocked(c, pi, nonce)
			}
			return
		}
		// STAGED, not framed for a later raw write (F-105 7c). A QoS 0 item keeps its frame and is
		// written by the driver through the actionless writer; a QoS 1/2 item carries the immutable
		// SNAPSHOT the driver materializes, admits and transmits through the authorized boundary.
		f := mqttRetainedFrame{header: headerBytes, msg: rm.Msg, pi: pi, nonce: nonce, qos: qos}
		if qos > 0 {
			var expiresAt int64
			if !rm.expires.IsZero() {
				expiresAt = rm.expires.UnixNano()
			}
			f.mat = &mqttRetainedMaterial{Topic: rm.Topic, QoS: qos, Props: rm.Props, ExpiresAt: expiresAt,
				SessionHash: rm.SessionHash, SubID: uint32(max(sub.mqtt.subID, 0)),
				Data: append([]byte(nil), rm.Msg...)}
			f.ent = ent
		}
		c.mu.Lock()
		stageRetainedFrameLocked(sub, f)
		c.mu.Unlock()
		if trace {
			toTrace = append(toTrace, mqttPublish{
				topic: []byte(rm.Topic),
				flags: flags,
				pi:    pi,
				sz:    len(rm.Msg),
			})
		}
	})
	for _, pp := range toTrace {
		c.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
	}
}

// Appends the stored message subjects for all retained message records that
// match the given subscription's `subject` (which could have wildcards).
//
// Account session manager NOT lock held on entry.
func (as *mqttAccountSessionManager) addRetainedSubjectsForSubject(list map[string]uint64, topSubject string) {
	as.mu.RLock()
	defer as.mu.RUnlock()

	if as.retmsgs.Size() == 0 {
		return
	}

	as.retmsgs.Match(stringToBytes(topSubject), func(subj []byte, ret *mqttRetainedMsgRef) {
		subject := string(subj)
		if _, ok := list[subject]; ok {
			return
		}
		if seq := ret.sseq; seq > 0 && !ret.deleted {
			list[subject] = seq
		}
	})
}

type warner interface {
	Warnf(format string, v ...any)
}

// Loads a list of retained messages given a list of stored message subjects.
func (as *mqttAccountSessionManager) loadRetainedMessages(subjects map[string]uint64, w warner) map[string]*mqttRetainedMsg {
	rms := make(map[string]*mqttRetainedMsg, len(subjects))
	ss := []string{}
	for s := range subjects {
		if rm := as.getCachedRetainedMsg(s); rm != nil {
			rms[s] = rm
		} else {
			ss = append(ss, mqttRetainedMsgsStreamSubject+s)
		}
	}

	if len(ss) == 0 {
		return rms
	}

	// Although we have the stream sequence for a given subject, we still use
	// the load with "last for subject" because it will cover the cases where a
	// new retained message has arrived since we collected the subject/seq pair.
	// If we were doing a load "by seq" and the message is not found, we would
	// incorrectly remove the retained message from our map.
	results, err := as.jsa.loadLastMsgForMulti(mqttRetainedMsgsStreamName, ss)
	// If an error occurred, warn, but then proceed with what we got.
	if err != nil {
		w.Warnf("error loading retained messages: %v", err)
	}
	if sm := as.retainedSeams(); sm != nil && sm.testRetainedLoadGate != nil {
		sm.testRetainedLoadGate(ss)
	}
	for i, result := range results {
		if result == nil {
			continue // skip requests that timed out
		}
		// Skip the "$MQTT.rmsgs." prefix...
		subj := ss[i][len(mqttRetainedMsgsStreamSubject):]
		if err := result.ToError(); err != nil {
			if IsNatsErr(err, JSNoMessageFoundErr) {
				// No record for the subject: expected when a per-message TTL reaped an
				// expired retained message, so no warning. The answer carries no
				// sequence, so only the sequence the index recorded may be acted on; a
				// retained message that arrived during the load has moved it.
				as.retainedNoMessage(subj, subjects[subj])
				continue
			}
			w.Warnf("failed to load retained message for subject %q: %v", subj, err)
			continue
		}
		seq := result.Message.Sequence
		if len(sliceHeader(JSMarkerReason, result.Message.Header)) > 0 {
			// A sourced JetStream subject delete marker: the record is gone, at this
			// sequence. See processRetainedMsg.
			as.applyRetained(subj, seq, nil, mqttRetainedFromLoad)
			continue
		}
		rm, err := mqttDecodeRetainedMessage(result.Message.Subject, result.Message.Header, result.Message.Data)
		if err != nil {
			// Unlikely that we can recover from that: a deletion at this record's
			// sequence, as in processRetainedMsg.
			as.applyRetained(subj, seq, nil, mqttRetainedFromLoad)
			w.Warnf("failed to decode retained message for subject %q: %v", subj, err)
			continue
		}
		if len(rm.Msg) == 0 {
			// An empty body is a delete tombstone (see mqttHandlePubRetain): the
			// "last message for subject" query can return one directly on a cold
			// load, before this broker's observer has applied it, so it must be
			// treated as "no retained message" here, not delivered as an empty PUBLISH.
			as.applyRetained(subj, seq, nil, mqttRetainedFromLoad)
			continue
		}

		// The loaded record is applied like any other writer's, so a literal filter's
		// forced load INDEXES a subject the index was missing, and the cache is written
		// only for the record the index names. What is replayed is the record read,
		// either way: it was the stream's current record when it was read. We don't need
		// the cache write to clone the `rm.Msg` bytes slice since we own it.
		as.applyRetained(rm.Subject, seq, rm, mqttRetainedFromLoad)
		rms[rm.Subject] = rm
	}
	return rms
}

// Composes a NATS message for a storeable mqttRetainedMsg.
// An empty body is itself the deletion tombstone [MQTT-3.3.1-11]; the flags are
// encoded plainly either way, and every reader is expected to decode both.
func mqttEncodeRetainedMessage(rm *mqttRetainedMsg) (natsMsg []byte, headerLen int) {
	delRM := len(rm.Msg) == 0

	// MQTT 5.0 Message Expiry Interval: for a live retained message with an
	// expiry, carry the absolute deadline (so the remaining interval can be
	// recomputed on replay) plus a JetStream per-message TTL for the remaining
	// lifetime (so the stored copy is dropped once it expires). Tombstones
	// (empty payload) never carry expiry. Spec5 [3.3.2.3.3].
	var expStr, ttlStr string
	if !delRM && !rm.expires.IsZero() {
		expStr = strconv.FormatInt(rm.expires.UnixNano(), 10)
		// Round the remaining lifetime up so JetStream never deletes the retained
		// copy before its deadline (truncating a 1.99s remainder to 1s would drop
		// it almost a second early). At least 1s, JetStream's minimum.
		secs := int64((time.Until(rm.expires) + time.Second - 1) / time.Second)
		if secs < 1 {
			secs = 1
		}
		ttlStr = strconv.FormatInt(secs, 10)
	}

	// No need to encode the subject, we can restore it from topic.
	l := len(hdrLine)
	l += len(mqttNatsRetainedMessageTopic) + 1 + len(rm.Topic) + 2 // 1 byte for ':', 2 bytes for CRLF
	if rm.Origin != _EMPTY_ {
		l += len(mqttNatsRetainedMessageOrigin) + 1 + len(rm.Origin) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if rm.Source != _EMPTY_ {
		l += len(mqttNatsRetainedMessageSource) + 1 + len(rm.Source) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if rm.SessionHash != _EMPTY_ {
		l += len(mqttNatsRetainedMessageSessionHash) + 1 + len(rm.SessionHash) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if len(rm.Props) > 0 {
		l += len(mqttNatsRetainedMessageProps) + 1 + base64.StdEncoding.EncodedLen(len(rm.Props)) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if expStr != _EMPTY_ {
		l += len(mqttNatsRetainedMessageExpiry) + 1 + len(expStr) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if ttlStr != _EMPTY_ {
		l += len(JSMessageTTL) + 1 + len(ttlStr) + 1 + 2 // 1 byte for ':', 1 for 's', 2 bytes for CRLF
	}
	l += len(mqttNatsRetainedMessageFlags) + 1 + 2 + 2 // 1 byte for ':', 2 bytes for the flags, 2 bytes for CRLF
	l += 2                                             // 2 bytes for the extra CRLF after the header
	if !delRM {
		l += len(rm.Msg)
	}

	buf := bytes.NewBuffer(make([]byte, 0, l))

	buf.WriteString(hdrLine)

	buf.WriteString(mqttNatsRetainedMessageTopic)
	buf.WriteByte(':')
	buf.WriteString(rm.Topic)
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttNatsRetainedMessageFlags)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatUint(uint64(rm.Flags), 16))
	buf.WriteString(_CRLF_)

	if rm.Origin != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageOrigin)
		buf.WriteByte(':')
		buf.WriteString(rm.Origin)
		buf.WriteString(_CRLF_)
	}
	if rm.Source != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageSource)
		buf.WriteByte(':')
		buf.WriteString(rm.Source)
		buf.WriteString(_CRLF_)
	}
	if rm.SessionHash != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageSessionHash)
		buf.WriteByte(':')
		buf.WriteString(rm.SessionHash)
		buf.WriteString(_CRLF_)
	}
	if len(rm.Props) > 0 {
		buf.WriteString(mqttNatsRetainedMessageProps)
		buf.WriteByte(':')
		buf.WriteString(base64.StdEncoding.EncodeToString(rm.Props))
		buf.WriteString(_CRLF_)
	}

	if expStr != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageExpiry)
		buf.WriteByte(':')
		buf.WriteString(expStr)
		buf.WriteString(_CRLF_)
	}
	if ttlStr != _EMPTY_ {
		buf.WriteString(JSMessageTTL)
		buf.WriteByte(':')
		buf.WriteString(ttlStr)
		buf.WriteByte('s')
		buf.WriteString(_CRLF_)
	}

	// End of header, finalize
	buf.WriteString(_CRLF_)
	headerLen = buf.Len()
	buf.Write(rm.Msg)
	return buf.Bytes(), headerLen
}

func mqttSliceHeaders(headers map[string][]byte, hdr []byte) {
	// Skip the hdrLine
	if !bytes.HasPrefix(hdr, stringToBytes(hdrLine)) {
		return
	}
	crLFAsBytes := stringToBytes(CR_LF)
	for i := len(hdrLine); i < len(hdr); {
		// Search for key/val delimiter. `del` is an offset INTO hdr[i:], not an
		// absolute index — comparing it against `i` (as this once did) aborts the
		// scan whenever a key's length happens to equal the current position, and
		// silently discards every header from there on. The first header sits at
		// i == len(hdrLine) == 10, so a ten-character key such as "Nmqtt-RExp"
		// triggered it; NATS header order is map order, so it bit roughly one
		// retained message in seven that carried an MQTT 5.0 expiry. A zero-length
		// key is `del == 0`, and it is caught by the empty-key check below in any
		// case.
		del := bytes.IndexByte(hdr[i:], ':')
		// Not found or key is length 0, we stop.
		if del < 0 || del == 0 {
			break
		}
		keyStart := i
		// Walk back to remove spaces between the key and ':' if applicable.
		index := keyStart + del - 1
		for index > keyStart && hdr[index] == ' ' {
			index--
		}
		key := hdr[keyStart : index+1]
		// If what we had is only spaces, we stop.
		if len(key) == 0 {
			break
		}
		i += del + 1
		valStart := i
		// Search for `\r\n`.
		nl := bytes.Index(hdr[valStart:], crLFAsBytes)
		// If we don't find, we stop.
		if nl < 0 {
			break
		}
		// Look if the caller is interested in this key.
		if _, ok := headers[bytesToString(key)]; ok {
			index := valStart
			// Remove possible spaces between the ':' and the value.
			for index < valStart+nl && hdr[index] == ' ' {
				index++
			}
			// Create a slice and limit capacity to the value range.
			val := hdr[index : valStart+nl : valStart+nl]
			// Record in the caller's map the value for this key.
			headers[bytesToString(key)] = val
		}
		// Reposition to past the `\r\n`.
		i += nl + 2
	}
}

// Decodes a retained message based on the content of the header `h`.
// The returned `*mqttRetainedMsg` object will hold a reference to `m`.
// If the buffer `m` is not owned by the caller, it is the caller
// responsibility to make a copy of the byte slice.
func mqttDecodeRetainedMessage(subject string, h, m []byte) (*mqttRetainedMsg, error) {
	headers := map[string][]byte{
		mqttNatsRetainedMessageOrigin:      nil,
		mqttNatsRetainedMessageFlags:       nil,
		mqttNatsRetainedMessageSource:      nil,
		mqttNatsRetainedMessageProps:       nil,
		mqttNatsRetainedMessageExpiry:      nil,
		mqttNatsRetainedMessageSessionHash: nil,
	}
	// Retrieve the values for the above headers.
	mqttSliceHeaders(headers, h)
	// Get the flag header. It is the one header that identifies the record as
	// one this broker wrote: without it the stored bytes are in no format this
	// broker produces, and are rejected rather than interpreted as another one.
	fHeader := headers[mqttNatsRetainedMessageFlags]
	if len(fHeader) == 0 {
		return nil, errMQTTRetainedMsgUnsupportedFormat
	}
	flagsUint, err := strconv.ParseUint(bytesToString(fHeader), 16, 8)
	if err != nil {
		// Since the error is currently not reported in the server, we
		// will simply replace with this one.
		return nil, errMQTTInvalidRetainFlags
	}
	rm := &mqttRetainedMsg{
		Flags:       byte(flagsUint),
		Origin:      string(headers[mqttNatsRetainedMessageOrigin]),
		Source:      string(headers[mqttNatsRetainedMessageSource]),
		SessionHash: string(headers[mqttNatsRetainedMessageSessionHash]),
		Msg:         m,
	}
	// MQTT 5.0 properties block (base64). Absent for messages published without
	// properties. Validate before keeping it: the retained stream could contain a
	// crafted Nmqtt-RProps from a non-MQTT publisher, and rm.Props is later
	// written verbatim into outbound PUBLISH packets.
	if enc := headers[mqttNatsRetainedMessageProps]; len(enc) > 0 {
		if raw, err := base64.StdEncoding.DecodeString(bytesToString(enc)); err == nil {
			rm.Props = mqttValidateForwardProps(raw)
		}
	}
	// MQTT 5.0 Message Expiry Interval: absolute deadline (Unix nanoseconds).
	if exp := headers[mqttNatsRetainedMessageExpiry]; len(exp) > 0 {
		if ns, err := strconv.ParseInt(bytesToString(exp), 10, 64); err == nil && ns > 0 {
			rm.expires = time.Unix(0, ns)
		}
	}
	// Now check that the values are correct.
	//
	// For "Flags", anything at or above binary (1111) is too big.
	if rm.Flags >= mqttPacketFlagMask {
		return nil, errMQTTInvalidRetainFlags
	}
	if qos := mqttGetQoS(rm.Flags); qos > 2 {
		return nil, errMQTTInvalidRetainFlags
	}
	// We store `Topic` in the retained message because we used to store
	// all retained messages under the same subject `$MQTT_rmsgs` in
	// the retained messages stream. That is no longer the case, and to
	// cover setups where the retained message stream is sourced from another
	// account and has some subject transforms, simply reconstruct the
	// topic/subject based on the `subject` passed to this function.
	rm.Subject = strings.TrimPrefix(subject, mqttRetainedMsgsStreamSubject)
	rm.Topic = bytesToString(natsSubjectStrToMQTTTopic(rm.Subject))
	return rm, nil
}

func (as *mqttAccountSessionManager) getCachedRetainedMsg(subject string) *mqttRetainedMsg {
	v, ok := as.rmsCache.Load(subject)
	if !ok {
		return nil
	}
	rm := v.(*mqttRetainedMsg)
	if rm.expiresFromCache.Before(time.Now()) {
		as.rmsCache.Delete(subject)
		return nil
	}
	// MQTT 5.0 Message Expiry Interval: drop an expired retained message rather
	// than serve a stale copy from the cache. Spec5 [3.3.2.3.3].
	if !rm.expires.IsZero() && !time.Now().Before(rm.expires) {
		as.rmsCache.Delete(subject)
		return nil
	}
	return rm
}

// If cache is enabled, the expiration for the `rm` is bumped by
// `mqttRetainedCacheTTL` seconds.
// If `onlyReplace` is true, then the `rm` object is stored in the cache using
// the `subject` key only if there was already an object stored under that key.
// If `copyMsgBytes` is true, then the `rm.Msg` bytes are copied (because it
// references some buffer that is not owned by the caller).
//
// Note: currently `onlyReplace` and `cloneMsgBytes` always have the same
// value (all `true` or all `false`) however we use different booleans to
// better express the intent.
func (as *mqttAccountSessionManager) setCachedRetainedMsg(subject string, rm *mqttRetainedMsg, onlyReplace, copyMsgBytes bool) {
	if rm == nil {
		return
	}
	rm.expiresFromCache = time.Now().Add(mqttRetainedCacheTTL)
	if onlyReplace {
		if _, ok := as.rmsCache.Load(subject); !ok {
			return
		}
	}
	if copyMsgBytes {
		rm.Msg = copyBytes(rm.Msg)
	}
	as.rmsCache.Store(subject, rm)
}

// Invoked when processing an inbound client message. If the "retain" flag is
// set, the message is stored so it can be later resent to (re)starting
// subscriptions that match the subject.
//
// Invoked from the MQTT publisher's readLoop. No client lock is held on entry.
//
// It returns an error whenever the retained update it was asked for is not confirmed --
// a store error (the write may still have landed) or a fence that refused to write --
// so that mqttInitiateMsgDeliveryJSA can withhold final success. Nil when there was
// nothing to retain or the store confirmed the update. F-060.
func (c *client) mqttHandlePubRetain(pp *mqttPublish) error {
	retainMQTT := mqttIsRetained(pp.flags)
	isBirth, _, isCertificate := sparkbParseBirthDeathTopic(pp.topic)
	retainSparkbBirth := isBirth && !isCertificate

	// [tck-id-topics-nbirth-mqtt] NBIRTH messages MUST be published with MQTT
	// QoS equal to 0 and retain equal to false.
	//
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware MQTT
	// Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with the
	// MQTT retain flag set to true.
	if retainMQTT == retainSparkbBirth {
		// (retainSparkbBirth && retainMQTT) : not valid, so ignore altogether.
		// (!retainSparkbBirth && !retainMQTT) : nothing to do.
		return nil
	}

	asm := c.mqtt.asm
	key := string(pp.subject)

	// Always clear the retain flag to deliver a normal published message.
	defer func() {
		pp.flags &= ^mqttPubFlagRetain
	}()

	// Spec [MQTT-3.3.1-11]. Payload of size 0 removes the retained message, but
	// should still be delivered as a normal message.
	//
	// A deletion is stored, not signalled: we write the message with an empty
	// body and every broker's retained-stream consumer removes the subject when
	// it processes that record. The STREAM is what orders adds and removes
	// durably — the empty-body tombstone is the only deletion authority, and
	// MaxMsgsPer=1 evicts the record it replaces at store time, so no explicit
	// delete request is involved. The volatile INDEX is ordered by that same
	// stream sequence: every writer, the local seed below included, goes through
	// applyRetainedLocked (retained_index.go).

	rm := &mqttRetainedMsg{
		Origin: asm.jsa.id,
		Msg:    pp.msg, // will copy these bytes later as we process rm.
		Flags:  pp.flags,
		Source: c.opts.Username,
		Props:  pp.props, // v5 properties to replay to subscribers
		// Same No Local origin the live delivery used (empty for a Will), so
		// retained replay is suppressed under No Local consistently. Spec5 [3.8.3.1].
		SessionHash: c.mqtt.pubOrigin,
	}

	// MQTT 5.0 Message Expiry Interval on a retained message: record the absolute
	// deadline so the remaining interval can be recomputed on replay and the
	// stored message dropped once it expires. A present interval of 0 means the
	// message expires immediately, so the deadline is "now" (distinct from an
	// absent interval, which never expires and leaves the deadline zero). Spec5
	// [3.3.2.3.3].
	if me, ok := mqttMessageExpiry(pp.props); ok {
		rm.expires = time.Now().Add(time.Duration(me) * time.Second)
	}

	if retainSparkbBirth {
		// [tck-id-conformance-mqtt-aware-store] A Sparkplug Aware MQTT Server
		// MUST store NBIRTH and DBIRTH messages as they pass through the MQTT
		// Server.
		//
		// [tck-id-conformance-mqtt-aware-nbirth-mqtt-topic]. A Sparkplug Aware
		// MQTT Server MUST make NBIRTH messages available on a topic of the
		// form: $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id
		//
		// [tck-id-conformance-mqtt-aware-dbirth-mqtt-topic] A Sparkplug Aware
		// MQTT Server MUST make DBIRTH messages available on a topic of the
		// form:
		// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
		topic := append(sparkbCertificatesTopicPrefix, pp.topic...)
		subject, _ := mqttTopicToNATSPubSubject(topic)
		rm.Topic = string(topic)
		rm.Subject = string(subject)

		// will use to save the retained message.
		key = string(subject)

		// Store the retained message with the RETAIN flag set.
		rm.Flags |= mqttPubFlagRetain

		if pp.sz > 0 {
			// The derived certificate publication is an application-visible effect
			// in its own right, and it happens BEFORE the retained store below, so
			// the store's fence cannot retract it. Fenced here for the same reason
			// and on the same terms.
			// Test seam, for the same reason the durable-write one exists: this fence
			// covers the window between the authorization gate and THIS send, and
			// the gate refuses first, so without a pause here a mutation removing
			// the fence survives. Nil in production.
			if h := mqttTestDerivedSendPauseV.Load(); h != nil {
				(*h)()
			}
			if !c.serviceEpochStillOurs() {
				c.Warnf("Abandoning the derived Sparkplug publication on %q: the authorizing identity is no longer valid for this account", key)
				return fmt.Errorf("%w: derived Sparkplug publication on %q", errMQTTServiceEpochMoved, key)
			}
			// Copy the payload out of pp since we will be sending the message
			// asynchronously.
			msg := make([]byte, pp.sz)
			copy(msg, pp.msg[:pp.sz])
			asm.jsa.sendMsg(key, msg)
		}

	} else { // isRetained
		// Spec [MQTT-3.3.1-5]. Store the retained message with its QoS.
		//
		// When coming from a publish protocol, `pp` is referencing a stack
		// variable that itself possibly references the read buffer.
		rm.Topic = string(pp.topic)
	}

	// Set the key to the subject of the message for retained, or the composed
	// $sparkplug subject for sparkB.
	rm.Subject = key
	rmBytes, hdr := mqttEncodeRetainedMessage(rm) // will copy the payload bytes
	// The retained store is its own durable write, and for a QoS 0 retained
	// publish it is the ONLY one -- that path returns before the $MQTT_msgs store
	// and its check. Fenced here for the same reason: a copy authorized against one
	// service account must not be written into another.
	// Test seam, for the same reason as its two siblings: the gate refuses first, so
	// without a pause between it and this fence a mutation removing the fence survives.
	// Nil in production.
	if h := mqttTestRetainedFencePauseV.Load(); h != nil {
		(*h)()
	}
	if !c.serviceEpochStillOurs() {
		c.Warnf("Abandoning the retained store for %q: the service account changed after it was authorized", key)
		return fmt.Errorf("%w: retained store for %q", errMQTTServiceEpochMoved, key)
	}
	resp, err := asm.jsa.storeMsg(mqttRetainedMsgsStreamSubject+key, hdr, rmBytes)
	if err != nil {
		c.mu.Lock()
		acc := c.acc
		c.mu.Unlock()
		c.Errorf("unable to store retained message for account %q, subject %q: %v",
			acc.GetName(), key, err)
		return fmt.Errorf("%w: subject %q: %w", errMQTTRetainedStoreUnconfirmed, key, err)
	}

	// Seed the local retained index synchronously with the just-stored message.
	// The observer's application of the same record is a NATS round trip away in
	// this standalone broker (unlike upstream's in-process hop), so a SUBSCRIBE
	// arriving right after this PUBLISH could otherwise miss it. Seeding here makes
	// retained-then-subscribe deterministic on the publishing node. Both writers
	// are ordered by the record's stream sequence, so the observer's later
	// application of the same record is a no-op, and a seed delayed past a newer
	// record changes nothing. An empty body is a tombstone [MQTT-3.3.1-11].
	if sm := asm.retainedSeams(); sm != nil && sm.testRetainedSeedGate != nil {
		sm.testRetainedSeedGate(key)
	}
	var seed *mqttRetainedMsg
	if len(rm.Msg) > 0 {
		seed = rm
	}
	asm.applyRetained(key, resp.Sequence, seed, mqttRetainedFromSeed)
	return nil
}

// Queues the published retained messages for each subscription and signals
// the writeLoop.
func (c *client) mqttSendRetainedMsgsToNewSubs(subs []*subscription) {
	// THE SEAM stays where the window is: after the frames are staged and before anything of them
	// leaves the readLoop. See mqttTestRetainedFlushBlockV.
	if h := mqttTestRetainedFlushBlockV.Load(); h != nil {
		c.mu.Lock()
		(*h)(subs)
		c.mu.Unlock()
	}
	// TRANSFERRED to the session's driver, never written here (F-105 7c): the readLoop returns to
	// reading at once, and the driver materializes, admits and transmits in order.
	c.transferRetainedReplays(subs)
}

// detachRetainedFramesLocked takes a departing subscription's buffered retained frames and
// hands them to the caller, which owes them either a NEW HOME or an ABANDONED release.
//
// This is the whole of the departure property: *every subscription leaving c.subs DETACHES
// its retained frames*, after which the caller either gives them a new home or abandons them.
// Stated as a property over the sites that remove one rather than as a list of teardown paths.
//
// DETACHING is not releasing, and the distinction is load-bearing: a same-sid replacement
// transfers the frames and their reservations to the replacement, because a duplicate-filter
// SUBSCRIBE still owes that replay, while a teardown abandons them because nothing will ever
// write them and the identifier would otherwise stay reserved against reallocation for the
// session's life (F-091).
//
// The detach shares the c.mu section that mutates c.subs, which is what makes a flusher and
// a departure contend on ONE mutex: exactly one of them takes the slice, so each frame gets
// exactly one disposition — written by the flusher, or detached here and then TRANSFERRED to
// a replacement or ABANDONED by a teardown. Never both, never neither.
//
// Nil-tolerant by requirement, not by habit: this runs from processUnsub, which is generic
// and is called on the internal account client during session-manager startup rollback —
// a client constructed with mqtt == nil.
//
// Connection lock held on entry.
func (c *client) detachRetainedFramesLocked(sub *subscription) []mqttRetainedFrame {
	if sub == nil || sub.mqtt == nil {
		return nil
	}
	// A DEPARTURE of the replay lineage (F-105 7c): the driver gives its remaining items their
	// departure dispositions; an object the driver does not hold retires here, so no live callback
	// is left waiting on a gate nothing will close.
	if r := sub.mqtt.replay; r != nil && !r.retired {
		r.departLocked()
		if !r.queued {
			r.retireLocked()
		}
	}
	if sub.mqtt.prm == nil {
		return nil
	}
	frames := sub.mqtt.prm
	sub.mqtt.prm = nil
	return frames
}

// mqttDisplacedRetained is what a same-sid replacement takes over from the subscription it
// displaces: its staged frames and its outstanding replay object.
type mqttDisplacedRetained struct {
	frames []mqttRetainedFrame
	replay *mqttRetainedReplay
}

// detachDisplacedRetainedLocked is the REPLACEMENT's detach: nothing departs, so the replay object
// is handed on intact rather than marked departed.
//
// Connection lock held on entry.
func (c *client) detachDisplacedRetainedLocked(sub *subscription) mqttDisplacedRetained {
	if sub == nil || sub.mqtt == nil {
		return mqttDisplacedRetained{}
	}
	d := mqttDisplacedRetained{frames: sub.mqtt.prm}
	sub.mqtt.prm = nil
	if r := sub.mqtt.replay; r != nil && !r.retired {
		r.handing = true
		d.replay = r
		sub.mqtt.replay = nil
	}
	return d
}

// releaseDetachedRetainedFrames abandons the reservations of frames that will never be
// written. Split from the detach because the release needs sess.mu and the detach ran under
// c.mu.
//
// Connection lock NOT held on entry.
func (c *client) releaseDetachedRetainedFrames(frames []mqttRetainedFrame) {
	refs := mqttRetainedFrameRefs(frames)
	// Retained frames on a client with no MQTT session is an impossible shape; it is
	// refused here rather than dereferenced, because the detach above is reachable from a
	// generic unsubscribe path.
	if len(refs) == 0 || c.mqtt == nil || c.mqtt.sess == nil {
		return
	}
	c.mqtt.sess.abandonRetainedSends(c, refs)
}

// mqttRetainedFrameRefs reports the RESERVATIONS a frame list holds, each as the
// (identifier, allocation) pair its disposition has to name. A QoS 0 frame reserves nothing
// and contributes nothing.
//
// It replaced an identifier-only helper rather than sitting beside one: the abandonment
// releases the pending entry as well as the hold, and only the allocation identifies which
// exchange that entry is, so a bare identifier is not enough for any caller (F-101).
func mqttRetainedFrameRefs(frames []mqttRetainedFrame) []mqttRetainedRef {
	var refs []mqttRetainedRef
	for _, f := range frames {
		if f.pi != 0 {
			refs = append(refs, mqttRetainedRef{pi: f.pi, nonce: f.nonce})
		}
	}
	return refs
}

// mqttRetainedRef is one buffered retained frame's reservation: the identifier it holds
// and the allocation that holds it.
type mqttRetainedRef struct {
	pi    uint16
	nonce string
}
