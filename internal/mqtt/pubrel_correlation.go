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
	"fmt"
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

	// mqttPubRelOwnerHeader names the OUTBOUND QoS 2 EXCHANGE this record belongs to:
	// the exchange nonce its durable owner slot on $MQTT_inflight carries.
	//
	// MANDATORY on every stored PUBREL, and that is what turns the record from a CLAIM on
	// the packet identifier into EVIDENCE about one exchange. Absent or malformed is an
	// unsupported persisted shape and is REFUSED — never interpreted, never transmitted,
	// never acknowledged away.
	//
	// What this header does NOT yet do, stated here because an earlier revision of this
	// comment claimed otherwise and a holistic round caught it: admission checks only that
	// the owner is SYNTACTICALLY valid. The rule the design is heading for — a record may
	// act under an identifier only while the slot for that identifier is in its PUBREL
	// phase and names this same nonce — needs PUBREL admission, which is F-092's unbuilt
	// half. Until then a valid-looking owner is enough to be transmitted.
	//
	// The retained case is where that gap bites hardest, and it is a stated residual rather
	// than a property of this header. A retained QoS 2 send has no slot at all (R6): it
	// mints its exchange nonce into the in-memory reservation and carries it here, so its
	// authority is that reservation and nothing durable. After a restart the reservation is
	// gone and the record's owner is still syntactically valid, so nothing refuses it — see
	// docs/CONFORMANCE.md, which says so in those terms.
	mqttPubRelOwnerHeader = "Nmqtt-Q2Owner"
)

// mqttPubRelOwnerState classifies a stored PUBREL's owner identity, as a TRI-STATE for
// the same reason mqttPubRelAffineRefState is one: absent and malformed are DIFFERENT
// facts. Neither is admissible here — the header is mandatory — and the distinction exists
// so a refusal can say which it was, and so the shape tests assert each separately rather
// than sharing one outcome and hiding a fold between them.
type mqttPubRelOwnerState int

const (
	// mqttPubRelOwnerAbsent: no header at all. Under this schema an unsupported record,
	// NOT a legacy form — every path that creates a PUBREL writes it, retained included.
	mqttPubRelOwnerAbsent mqttPubRelOwnerState = iota
	// mqttPubRelOwnerValid: exactly one header carrying a well-formed exchange nonce.
	mqttPubRelOwnerValid
	// mqttPubRelOwnerMalformed: present but not a shape this broker writes — more than
	// one occurrence, or a value that is not a nonce.
	mqttPubRelOwnerMalformed
)

// mqttPubRelOwner is the exchange identity a stored PUBREL carries.
type mqttPubRelOwner struct {
	Nonce string
	State mqttPubRelOwnerState
}

// valid reports whether this identity may be matched against an owner slot.
func (o mqttPubRelOwner) valid() bool { return o.State == mqttPubRelOwnerValid }

// mqttParsePubRelOwner reads the owner identity off a stored PUBREL as a tri-state.
//
// EXACTLY one header, and the value validated by the same rule the register's own nonce
// uses: a token of the right length from a foreign alphabet is not one of ours, and an
// empty one must not collapse "no identity" into the identity space. getHeader returns
// the first occurrence, so two headers would silently pick one of two identities — the
// kind of quiet choice this whole design exists to remove.
func mqttParsePubRelOwner(headerBytes []byte) mqttPubRelOwner {
	if len(headerBytes) == 0 {
		return mqttPubRelOwner{State: mqttPubRelOwnerAbsent}
	}
	n := mqttCountHeader(mqttPubRelOwnerHeader, headerBytes)
	if n == 0 {
		return mqttPubRelOwner{State: mqttPubRelOwnerAbsent}
	}
	v := string(getHeader(mqttPubRelOwnerHeader, headerBytes))
	o := mqttPubRelOwner{Nonce: v, State: mqttPubRelOwnerMalformed}
	if n != 1 || !mqttValidInflightNonce(v) {
		return o
	}
	o.State = mqttPubRelOwnerValid
	return o
}

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

// mqttNewCorrelatedPubRel builds a deliverable PUBREL naming the OUTBOUND EXCHANGE it
// belongs to and the source delivery it completes, and (when msgId is non-empty) carrying
// a deterministic deduplication identity so a superseded broker's late store collapses
// into this one rather than producing a second PUBREL.
//
// owner is MANDATORY. There is deliberately no longer a shortcut to the bare uncorrelated
// form: every stored PUBREL carries its exchange identity, which is what lets the delivery
// callback ask the durable owner slot whether this record may act under its identifier at
// all. A caller with no owner nonce has nothing legitimate to store — that is a bug rather
// than a legacy shape — so this refuses instead of writing an unattributable record.
//
// The affine reference is emitted INDEPENDENTLY of srcDur. It is not a detail of the
// source correlation and must survive on a PUBREL that carries no source at all, so
// the two are not nested.
func mqttNewCorrelatedPubRel(pi uint16, owner, srcDur string, srcSeq uint64, msgId string,
	affClaim, affCopy string) (natsMsg []byte, headerLen int) {

	if !mqttValidInflightNonce(owner) {
		return nil, 0
	}
	affine := affClaim != _EMPTY_ && affCopy != _EMPTY_
	seqStr := strconv.FormatUint(srcSeq, 10)
	size := len(hdrLine) +
		len(mqttNatsPubRelHeader) + 6 + 2 +
		len(mqttPubRelOwnerHeader) + 1 + len(owner) + 2 +
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
	buf.WriteString(mqttPubRelOwnerHeader)
	buf.WriteByte(':')
	buf.WriteString(owner)
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

// mqttExactUint16Header reports whether key's single value parses EXACTLY as a 16-bit
// unsigned integer.
//
// Separate from mqttParsePubRelNATSHeader because that function's contract — return 0 when
// there is nothing usable — is right for its own callers and cannot distinguish "absent"
// from "out of range": strconv.ParseUint returns the SATURATED value alongside its error,
// and that function drops the error. So `65536` reaches it as 65535, a perfectly plausible
// identifier that is not the one the record names.
func mqttExactUint16Header(hdr []byte, key string) bool {
	v := getHeader(key, hdr)
	if len(v) == 0 {
		return false
	}
	_, err := strconv.ParseUint(string(v), 10, 16)
	return err == nil
}

// mqttStoredPubRelShape is a stored PUBREL's decoded header set, and the error a decode
// returns is the whole point: no field of it authorizes anything until every field is
// legal.
//
// This is the PUBREL half of what mqttDecodeInflightRecord is for the register, and it
// arrived by the same route — three review rounds, each finding one more reader that had
// parsed a persisted body field by field and then trusted the fields it happened to look
// at. The specific failure that forced it: the drain recovery scan parsed each record's
// packet identifier and then DISCARDED it, confirming an intent on source and owner alone,
// so a record with the right source and owner but a zero or foreign `Nmqtt-PubRel` cleared
// the drain claim as durable proof — after which the client's original exchange could never
// complete and its recoverable source evidence was gone.
//
// So the four headers are one shape, decided in one place:
//
//   - the packet identifier must be present and nonzero, because it is what the record is
//     transmitted under;
//   - the exchange owner must be valid, because the record is EVIDENCE about one exchange
//     rather than a claim on an identifier;
//   - the source correlation may be ABSENT — a retained send has no source at all — but a
//     PARTIAL one is refused rather than silently read as uncorrelated, because
//     uncorrelated IS the retained shape and admitting a partial one under that reading
//     would give it a retained record's licence.
//
// The affine reference is deliberately NOT a clause here, and the boundary matters. A
// MALFORMED reference does not stop the record being transmitted: the client is owed the
// PUBREL and answering it is harmless. What it stops is the record being ACKNOWLEDGED
// AWAY, because a half-written reference names an assignment nothing can resolve and
// reading it as "no reference" would let a loss be inferred around the record that holds
// the proof. So it is carried here as the tri-state it is and decided by the caller that
// needs it. TestMQTTInflightWithheldPubCompStillConsumesItsDebt is the fixture that
// distinguishes the two, and it went red when this decode refused the shape outright.
type mqttStoredPubRelShape struct {
	pi     uint16
	owner  mqttPubRelOwner
	affRef mqttPubRelAffineRef
	srcDur string
	srcSeq uint64
}

// mqttDecodeStoredPubRel is the ONE way a stored PUBREL's headers become a decision.
//
// EVERY field is filled before the verdict is returned, and a first version that returned
// early on the first bad clause was wrong in a way a red test caught. A refused record is
// still handed to the walks that must account for what a teardown is about to destroy, and
// an early return left those walks looking at a zeroed affine reference — reading "no
// affine proof here" off a record that carried one. A verdict describes a record; it does
// not license leaving the record undescribed.
func mqttDecodeStoredPubRel(hdr []byte) (mqttStoredPubRelShape, error) {
	var out mqttStoredPubRelShape
	out.pi = mqttParsePubRelNATSHeader(hdr)
	out.owner = mqttParsePubRelOwner(hdr)
	out.affRef = mqttParsePubRelAffineRef(hdr)
	corr := mqttParsePubRelCorrelation(hdr)
	if corr.Ok {
		out.srcDur, out.srcSeq = corr.SrcDur, corr.SrcSeq
	}
	partialCorr := !corr.Ok && (mqttCountHeader(mqttPubRelSrcDurHeader, hdr) > 0 ||
		mqttCountHeader(mqttPubRelSrcSeqHeader, hdr) > 0)

	// CARDINALITY and PARSE STRICTNESS, because the underlying parsers are deliberately
	// lenient for their own callers and this boundary must not be. A review round found
	// both holes and both were reproduced before being fixed:
	//
	//   - mqttParsePubRelNATSHeader discards ParseUint's error, so `Nmqtt-PubRel:65536`
	//     SATURATES to 65535 and is accepted as an ordinary identifier. A record would then
	//     be transmitted under an identifier that is not the one it names.
	//   - every one of these headers takes the FIRST occurrence with no count, so a record
	//     carrying two identifiers, or two source-correlation pairs, was accepted and the
	//     reader silently picked one. That is the same defect as a duplicate JSON member on
	//     the register, and it gets the same answer: a record this broker never writes is
	//     refused rather than resolved by an arbitrary rule.
	piCount := mqttCountHeader(mqttNatsPubRelHeader, hdr)
	durCount := mqttCountHeader(mqttPubRelSrcDurHeader, hdr)
	seqCount := mqttCountHeader(mqttPubRelSrcSeqHeader, hdr)
	// Deliberately independent of piCount, so the two refusals below are two facts rather
	// than one wearing two labels. Folding the count in here made the cardinality arm
	// unreachable — a mutation disabling it changed nothing observable, because a record
	// with two identifier headers also failed this one.
	piExact := mqttExactUint16Header(hdr, mqttNatsPubRelHeader)

	switch {
	case piCount != 1:
		return out, fmt.Errorf("stored PUBREL carries %d packet-identifier headers, not one", piCount)
	case !piExact:
		return out, fmt.Errorf("stored PUBREL's packet-identifier header is not an exact 16-bit value")
	case durCount > 1 || seqCount > 1:
		return out, fmt.Errorf("stored PUBREL carries %d source-durable and %d source-sequence headers; "+
			"at most one of each is a shape this broker writes", durCount, seqCount)
	case out.pi == 0:
		return out, fmt.Errorf("stored PUBREL carries no packet identifier")
	case !out.owner.valid():
		why := "absent"
		if out.owner.State == mqttPubRelOwnerMalformed {
			why = "not a shape this broker writes"
		}
		return out, fmt.Errorf("stored PUBREL for packet identifier %d has an exchange identity that is %s",
			out.pi, why)
	case partialCorr:
		// One correlation header without a usable pair. The parser folds this into
		// "uncorrelated", which is right for ITS caller and wrong as an admission: an
		// uncorrelated record is the RETAINED shape, and admitting a partial one under
		// that reading would give it a retained record's licence.
		return out, fmt.Errorf("stored PUBREL for packet identifier %d carries a partial source correlation",
			out.pi)
	}
	return out, nil
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
