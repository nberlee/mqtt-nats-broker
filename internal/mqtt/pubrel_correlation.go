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

	// mqttPubRelAffClaimHeader / mqttPubRelAffCopyHeader name the session-affine
	// shared QoS 2 assignment this PUBREL proves the client answered PUBREC for:
	// the claim record's subject on $MQTT_qos2assign, and the copy subject the
	// delivery actually carried.
	//
	// The PAIR is the reference, never the claim subject alone. A claim subject is
	// reused by a later incarnation of the same assignment, so a bare subject would
	// credit a SUCCESSOR claim with a delivery it never made and let its own copy be
	// acknowledged away unpublished — the loss F-065 exists to prevent. The copy
	// subject carries the incarnation nonce, which is what distinguishes them.
	mqttPubRelAffClaimHeader = "Nmqtt-Q2AffClaim"
	mqttPubRelAffCopyHeader  = "Nmqtt-Q2AffCopy"
)

// mqttPubRelAffineRefState classifies what a stored PUBREL's affine reference is.
// Absent and malformed are DIFFERENT facts and must not be collapsed: absent is the
// current, permanent shape of every non-affine PUBREL (ordinary QoS 2, retained,
// drained, uncorrelated), while malformed is a persisted shape this broker does not
// write and therefore REFUSES — it is never read as "no reference".
type mqttPubRelAffineRefState int

const (
	// mqttPubRelAffineRefAbsent: neither header is present. No affine reference, and
	// nothing is owed.
	mqttPubRelAffineRefAbsent mqttPubRelAffineRefState = iota
	// mqttPubRelAffineRefValid: both headers present and both structurally exact.
	mqttPubRelAffineRefValid
	// mqttPubRelAffineRefMalformed: one header without the other, or either value is
	// not one this broker's generators produce. Refused: no claim is advanced from it,
	// and the PUBREL it rides is not acknowledged away, because the record it names
	// cannot be resolved and a loss verdict must not be inferred around it.
	mqttPubRelAffineRefMalformed
)

// mqttPubRelAffineRef is the assignment a stored PUBREL proves PUBREC for.
type mqttPubRelAffineRef struct {
	Claim string
	Copy  string
	State mqttPubRelAffineRefState
}

// valid reports whether this reference may be resolved against a claim record.
func (r mqttPubRelAffineRef) valid() bool { return r.State == mqttPubRelAffineRefValid }

// refused reports whether this reference is a shape the broker does not write, which
// blocks both the loss inference and the PUBREL's acknowledgement.
func (r mqttPubRelAffineRef) refused() bool { return r.State == mqttPubRelAffineRefMalformed }

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
// An empty srcDur AND an empty affine reference produce exactly the uncorrelated form
// mqttNewDeliverablePubRel produces, so the ordinary QoS 2 path is byte-for-byte
// unchanged.
//
// The affine reference is emitted INDEPENDENTLY of srcDur. It is not a detail of the
// source correlation and must survive on a PUBREL that carries no source at all, so
// the two are not nested.
func mqttNewCorrelatedPubRel(pi uint16, srcDur string, srcSeq uint64, msgId string,
	affClaim, affCopy string) (natsMsg []byte, headerLen int) {

	affine := affClaim != _EMPTY_ && affCopy != _EMPTY_
	if srcDur == _EMPTY_ && !affine {
		return mqttNewDeliverablePubRel(pi)
	}
	seqStr := strconv.FormatUint(srcSeq, 10)
	size := len(hdrLine) +
		len(mqttNatsPubRelHeader) + 6 + 2 +
		len(mqttPubRelSrcDurHeader) + 1 + len(srcDur) + 2 +
		len(mqttPubRelSrcSeqHeader) + 1 + len(seqStr) + 2 +
		len(JSMsgId) + 1 + len(msgId) + 2 +
		len(mqttPubRelAffClaimHeader) + 1 + len(affClaim) + 2 +
		len(mqttPubRelAffCopyHeader) + 1 + len(affCopy) + 2 +
		2
	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsPubRelHeader)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatInt(int64(pi), 10))
	buf.WriteString(_CRLF_)
	if srcDur != _EMPTY_ {
		buf.WriteString(mqttPubRelSrcDurHeader)
		buf.WriteByte(':')
		buf.WriteString(srcDur)
		buf.WriteString(_CRLF_)
		buf.WriteString(mqttPubRelSrcSeqHeader)
		buf.WriteByte(':')
		buf.WriteString(seqStr)
		buf.WriteString(_CRLF_)
	}
	if msgId != _EMPTY_ {
		buf.WriteString(JSMsgId)
		buf.WriteByte(':')
		buf.WriteString(msgId)
		buf.WriteString(_CRLF_)
	}
	if affine {
		buf.WriteString(mqttPubRelAffClaimHeader)
		buf.WriteByte(':')
		buf.WriteString(affClaim)
		buf.WriteString(_CRLF_)
		buf.WriteString(mqttPubRelAffCopyHeader)
		buf.WriteByte(':')
		buf.WriteString(affCopy)
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

// mqttParsePubRelAffineRef reads the affine assignment reference off a stored PUBREL,
// as a TRI-STATE.
//
// Neither header present is `absent`, and that is the current, permanent shape of every
// PUBREL that does not complete a session-affine shared QoS 2 copy — not a legacy form.
// Both present and both structurally exact is `valid`. Anything else is `malformed`, and
// it is deliberately NOT folded into `absent`: a half-written or unrecognised reference
// names an assignment this broker cannot resolve, so reading it as "no reference" would
// let a loss be inferred around a record that may hold the proof, and would let the
// PUBREL carrying it be acknowledged away.
//
// The validation here is STRUCTURAL, against the same generators that produce the two
// subjects, because "it starts with the right prefix" is not enough to earn the
// distinction: a loose value would parse valid, resolve to an absent claim, and take the
// acknowledging arm. The AUTHORITATIVE binding is still q2AffClaimUsableForDecision,
// which re-derives both subjects from the claim record's own fields.
func mqttParsePubRelAffineRef(headerBytes []byte) mqttPubRelAffineRef {
	if len(headerBytes) == 0 {
		return mqttPubRelAffineRef{}
	}
	nClaim := mqttCountHeader(mqttPubRelAffClaimHeader, headerBytes)
	nCopy := mqttCountHeader(mqttPubRelAffCopyHeader, headerBytes)
	if nClaim == 0 && nCopy == 0 {
		return mqttPubRelAffineRef{}
	}
	claim := getHeader(mqttPubRelAffClaimHeader, headerBytes)
	copySubj := getHeader(mqttPubRelAffCopyHeader, headerBytes)
	ref := mqttPubRelAffineRef{
		Claim: string(claim),
		Copy:  string(copySubj),
		State: mqttPubRelAffineRefMalformed,
	}
	// EXACTLY one of each. getHeader returns the first occurrence, so a record carrying
	// two claim headers would be read as naming only the first — silently choosing one
	// of two assignments a resolution might then act on. The builder emits each header
	// once, so more than one is a shape this broker does not write.
	if nClaim != 1 || nCopy != 1 || len(claim) == 0 || len(copySubj) == 0 {
		return ref
	}
	if !mqttValidQ2AffClaimSubject(ref.Claim) || !mqttValidQ2AffCopySubject(ref.Copy) {
		return ref
	}
	ref.State = mqttPubRelAffineRefValid
	return ref
}
