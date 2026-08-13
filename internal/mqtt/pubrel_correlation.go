// Durable correlation between a stored PUBREL and the source delivery it
// completes.
//
// A stored PUBREL used to carry only the packet identifier. After a restart
// nothing linked it back to the delivery it was completing: cpending is
// memory-only, so the usedForPubRel guard could not fire, a redelivery of the
// source allocated a FRESH identifier and RESENT the payload of an exchange
// already past PUBREC, and nothing could remove the source's claim, because the
// completing path (PUBCOMP on the PUBREL) knew nothing about (durable, sequence).
// Both a duplicate exactly-once delivery and a permanent wedge.
//
// So a PUBREL names its source: the durable that delivered it and that delivery's
// stream sequence. Absent headers mean "no correlation", and that is a real,
// current shape rather than a legacy one — a retained QoS 2 send is tracked with a
// wholly empty pending entry (trackPublishRetained), so its PUBREC names no source
// and the PUBREL is stored uncorrelated. Safe, because a delivery with no source
// durable is not one a drain can be waiting on.
//
// The deterministic Nats-Msg-Id exists for a different hazard. A superseded broker
// can still perform its own (non-CAS) PUBREL store after a peer took over and
// recreated the exchange, so the two stores are collapsed by deduplication when
// they fall inside the window. Deduplication is NOT trusted as a completion signal
// — a dedupe entry outlives its message, so a recreate under the same id can be
// accepted as a duplicate that stores nothing — which is why the id carries an
// ATTEMPT counter that a verified-absent recreate bumps, and why presence is
// always checked at the returned sequence rather than inferred from the response.

package mqtt

import (
	"bytes"
	"strconv"
)

const (
	// mqttPubRelSrcDurHeader / mqttPubRelSrcSeqHeader name the source delivery a
	// stored PUBREL completes: the JetStream durable it was delivered by, and its
	// stream sequence. Together with the client hash they identify one exchange
	// across restarts, which the packet identifier alone cannot do (identifiers are
	// per-connection and reallocated).
	mqttPubRelSrcDurHeader = "Nmqtt-Q2SrcDur"
	mqttPubRelSrcSeqHeader = "Nmqtt-Q2SrcSeq"
)

// mqttPubRelCorrelation is the source delivery a PUBREL completes. Ok is false for
// an uncorrelated record — a retained or otherwise unbacked QoS 2 send names no
// source durable, and a missing or malformed header parses the same way — which
// callers must treat as it says: no correlation, no source or drain interaction.
type mqttPubRelCorrelation struct {
	SrcDur string
	SrcSeq uint64
	Ok     bool
}

// mqttPubRelMsgId is the deterministic deduplication identity of one PUBREL store
// attempt: the exchange (client hash, source durable, source sequence) plus the
// attempt counter.
//
// The attempt is part of the identity on purpose. Deduplication cannot be trusted
// as proof that a record exists — an entry outlives its message, so re-storing
// under the same id inside the window is accepted as a duplicate that stores
// nothing — so a recreate that has VERIFIED its record absent bumps the attempt and
// retries under a fresh id. Without that, a poisoned id could never be recovered
// from.
func mqttPubRelMsgId(idHash, srcDur string, srcSeq, attempt uint64) string {
	var b bytes.Buffer
	b.WriteString("q2rel.")
	b.WriteString(idHash)
	b.WriteByte('.')
	b.WriteString(srcDur)
	b.WriteByte('.')
	b.WriteString(strconv.FormatUint(srcSeq, 10))
	b.WriteByte('.')
	b.WriteString(strconv.FormatUint(attempt, 10))
	return b.String()
}

// mqttNewCorrelatedPubRel builds a deliverable PUBREL naming the source delivery
// it completes, and (when msgId is non-empty) carrying a deterministic
// deduplication identity so a superseded broker's late store collapses into this
// one rather than producing a second PUBREL.
//
// An empty srcDur produces exactly the uncorrelated form mqttNewDeliverablePubRel
// produces, so the ordinary QoS 2 path is byte-for-byte unchanged.
func mqttNewCorrelatedPubRel(pi uint16, srcDur string, srcSeq uint64, msgId string) (natsMsg []byte, headerLen int) {
	if srcDur == _EMPTY_ {
		return mqttNewDeliverablePubRel(pi)
	}
	seqStr := strconv.FormatUint(srcSeq, 10)
	size := len(hdrLine) +
		len(mqttNatsPubRelHeader) + 6 + 2 +
		len(mqttPubRelSrcDurHeader) + 1 + len(srcDur) + 2 +
		len(mqttPubRelSrcSeqHeader) + 1 + len(seqStr) + 2 +
		len(JSMsgId) + 1 + len(msgId) + 2 +
		2
	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsPubRelHeader)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatInt(int64(pi), 10))
	buf.WriteString(_CRLF_)
	buf.WriteString(mqttPubRelSrcDurHeader)
	buf.WriteByte(':')
	buf.WriteString(srcDur)
	buf.WriteString(_CRLF_)
	buf.WriteString(mqttPubRelSrcSeqHeader)
	buf.WriteByte(':')
	buf.WriteString(seqStr)
	buf.WriteString(_CRLF_)
	if msgId != _EMPTY_ {
		buf.WriteString(JSMsgId)
		buf.WriteByte(':')
		buf.WriteString(msgId)
		buf.WriteString(_CRLF_)
	}
	buf.WriteString(_CRLF_)
	return buf.Bytes(), buf.Len()
}

// mqttParsePubRelCorrelation reads the source-delivery headers off a stored
// PUBREL. Ok is false when either header is missing or malformed: a partially
// correlated record is treated as uncorrelated rather than as a correlation to
// some sequence 0, which no delivery has.
func mqttParsePubRelCorrelation(headerBytes []byte) mqttPubRelCorrelation {
	if len(headerBytes) == 0 {
		return mqttPubRelCorrelation{}
	}
	dur := getHeader(mqttPubRelSrcDurHeader, headerBytes)
	if len(dur) == 0 {
		return mqttPubRelCorrelation{}
	}
	seqStr := getHeader(mqttPubRelSrcSeqHeader, headerBytes)
	if len(seqStr) == 0 {
		return mqttPubRelCorrelation{}
	}
	seq, err := strconv.ParseUint(string(seqStr), 10, 64)
	if err != nil || seq == 0 {
		return mqttPubRelCorrelation{}
	}
	return mqttPubRelCorrelation{SrcDur: string(dur), SrcSeq: seq, Ok: true}
}
