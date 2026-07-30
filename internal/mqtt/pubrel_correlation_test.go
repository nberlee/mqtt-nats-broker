package mqtt

import (
	"strings"
	"testing"
)

// TestPubRelCorrelationRoundTrip pins that a stored PUBREL names the source
// delivery it completes, and that the identifier itself still parses exactly as
// before.
//
// Regression: a PUBREL carried only the packet identifier, so after a restart
// nothing linked it back to the delivery it completed — cpending is memory-only.
// A redelivery of that source then allocated a FRESH identifier and resent the
// payload of an exchange already past PUBREC.
func TestPubRelCorrelationRoundTrip(t *testing.T) {
	const (
		pi     = uint16(4242)
		srcDur = "abcdef0123456789_XYZ"
		srcSeq = uint64(987654321)
	)
	msg, hdrLen := mqttNewCorrelatedPubRel(pi, srcDur, srcSeq, _EMPTY_)
	hdr := msg[:hdrLen]

	if got := mqttParsePubRelNATSHeader(hdr); got != pi {
		t.Fatalf("packet identifier = %v, want %v", got, pi)
	}
	corr := mqttParsePubRelCorrelation(hdr)
	if !corr.Ok {
		t.Fatal("a correlated PUBREL must parse as correlated")
	}
	if corr.SrcDur != srcDur || corr.SrcSeq != srcSeq {
		t.Fatalf("correlation = (%q, %v), want (%q, %v)", corr.SrcDur, corr.SrcSeq, srcDur, srcSeq)
	}

	// The header length must cover exactly the headers, and the body must be empty
	// — a PUBREL record carries no payload.
	if hdrLen != len(msg) {
		t.Errorf("headerLen = %v, want the whole message (%v)", hdrLen, len(msg))
	}
}

// TestPubRelUncorrelatedIsUnchanged pins backward compatibility in both
// directions: producing a PUBREL with no source is byte-identical to the old
// form, and a record written by a broker that predates correlation parses as
// uncorrelated rather than as a correlation to some sequence 0.
func TestPubRelUncorrelatedIsUnchanged(t *testing.T) {
	const pi = uint16(7)
	legacy, legacyLen := mqttNewDeliverablePubRel(pi)
	got, gotLen := mqttNewCorrelatedPubRel(pi, _EMPTY_, 0, _EMPTY_)
	if string(got) != string(legacy) || gotLen != legacyLen {
		t.Fatalf("an uncorrelated PUBREL must be byte-identical to the legacy form:\n got %q\nwant %q", got, legacy)
	}
	if corr := mqttParsePubRelCorrelation(legacy[:legacyLen]); corr.Ok {
		t.Errorf("a legacy PUBREL must parse as uncorrelated, got %+v", corr)
	}
	if got := mqttParsePubRelNATSHeader(legacy[:legacyLen]); got != pi {
		t.Errorf("packet identifier = %v, want %v", got, pi)
	}
}

// TestPubRelCorrelationRejectsPartialRecords: a half-correlated record must be
// treated as uncorrelated, never as a correlation to a sequence no delivery has.
func TestPubRelCorrelationRejectsPartialRecords(t *testing.T) {
	for name, hdr := range map[string]string{
		"no headers at all": hdrLine + _CRLF_,
		"durable only":      hdrLine + mqttPubRelSrcDurHeader + ":DUR" + _CRLF_ + _CRLF_,
		"sequence only":     hdrLine + mqttPubRelSrcSeqHeader + ":5" + _CRLF_ + _CRLF_,
		"zero sequence":     hdrLine + mqttPubRelSrcDurHeader + ":DUR" + _CRLF_ + mqttPubRelSrcSeqHeader + ":0" + _CRLF_ + _CRLF_,
		"malformed sequence": hdrLine + mqttPubRelSrcDurHeader + ":DUR" + _CRLF_ +
			mqttPubRelSrcSeqHeader + ":notanumber" + _CRLF_ + _CRLF_,
	} {
		if corr := mqttParsePubRelCorrelation([]byte(hdr)); corr.Ok {
			t.Errorf("%s: parsed as correlated (%+v); a partial record must be uncorrelated", name, corr)
		}
	}
	if corr := mqttParsePubRelCorrelation(nil); corr.Ok {
		t.Error("a nil header block must parse as uncorrelated")
	}
}

// TestPubRelMsgIdIdentity pins what the deduplication identity distinguishes. It
// must separate different exchanges — otherwise one exchange's store would
// suppress another's — and it must separate ATTEMPTS of the same exchange, which
// is what lets a recreate escape a poisoned identity.
//
// Deduplication entries outlive their messages, so re-storing under the same id
// inside the window is accepted as a duplicate that stores nothing. A recreate
// that has verified its record absent therefore bumps the attempt rather than
// retrying under an id the server will swallow.
func TestPubRelMsgIdIdentity(t *testing.T) {
	base := mqttPubRelMsgId("hash", "DUR", 5, 0)
	for name, other := range map[string]string{
		"different client":   mqttPubRelMsgId("other", "DUR", 5, 0),
		"different durable":  mqttPubRelMsgId("hash", "OTHER", 5, 0),
		"different sequence": mqttPubRelMsgId("hash", "DUR", 6, 0),
		"different attempt":  mqttPubRelMsgId("hash", "DUR", 5, 1),
	} {
		if other == base {
			t.Errorf("%s produced the same identity %q", name, other)
		}
	}
	if again := mqttPubRelMsgId("hash", "DUR", 5, 0); again != base {
		t.Errorf("the identity must be deterministic: %q vs %q", again, base)
	}
	// It has to be usable as a NATS header value.
	if strings.ContainsAny(base, "\r\n :") {
		t.Errorf("identity %q contains characters that cannot appear in a header value", base)
	}
}

// TestCorrelatedPubRelCarriesMsgIdOnlyWhenAsked: the deduplication identity is
// opt-in per store, because it is only safe where a presence check backs it up.
// The ordinary PUBREC path deliberately omits it — there, the only legitimate
// re-store of the same (durable, sequence) is the post-restart one that follows a
// redelivery, and a surviving dedupe entry would silently drop it, leaving the
// client waiting for a PUBREL that will never exist.
func TestCorrelatedPubRelCarriesMsgIdOnlyWhenAsked(t *testing.T) {
	without, wl := mqttNewCorrelatedPubRel(9, "DUR", 3, _EMPTY_)
	if got := getHeader(JSMsgId, without[:wl]); len(got) != 0 {
		t.Errorf("an unasked-for deduplication identity was written: %q", got)
	}
	id := mqttPubRelMsgId("hash", "DUR", 3, 2)
	with, l := mqttNewCorrelatedPubRel(9, "DUR", 3, id)
	if got := string(getHeader(JSMsgId, with[:l])); got != id {
		t.Errorf("deduplication identity = %q, want %q", got, id)
	}
	// The correlation must survive alongside it.
	if corr := mqttParsePubRelCorrelation(with[:l]); !corr.Ok || corr.SrcDur != "DUR" || corr.SrcSeq != 3 {
		t.Errorf("correlation was disturbed by the deduplication identity: %+v", corr)
	}
	if got := mqttParsePubRelNATSHeader(with[:l]); got != 9 {
		t.Errorf("packet identifier = %v, want 9", got)
	}
}
