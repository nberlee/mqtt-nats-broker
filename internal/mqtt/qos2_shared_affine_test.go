//go:build mqttport_experimental_shared_qos2

package mqtt

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The affine encoder and its size calculator must agree BYTE-EXACTLY. They are
// also the sole source of the max_payload bound, so a disagreement is not a
// cosmetic mismatch: it makes headerLen wrong (a corrupt stored copy) and the
// admission budget wrong (an unwritable copy admitted at PUBREC).
func TestQ2AffineEncoderMatchesSizeCalculator(t *testing.T) {
	t.Parallel()
	base := func() *mqttQ2AffineEnvelope {
		return &mqttQ2AffineEnvelope{
			natsSubject:  "foo.bar",
			msg:          []byte("payload"),
			claimSubject: mqttQ2AffClaimSubject("$MQTT_SHARED_0123456789abcdef_1", 4242),
			gen:          mqttNewQ2AffGen(),
			msgID:        mqttQ2AffMsgId("$MQTT_SHARED_0123456789abcdef_1", 4242, testQ2AffFixtureNonce),
		}
	}
	for _, tc := range []struct {
		name string
		mut  func(env *mqttQ2AffineEnvelope)
	}{
		{"minimal", func(*mqttQ2AffineEnvelope) {}},
		{"retained", func(e *mqttQ2AffineEnvelope) { e.retained = true }},
		{"props", func(e *mqttQ2AffineEnvelope) { e.props = []byte{0x02, 0x01, 0x01} }},
		{"sub id", func(e *mqttQ2AffineEnvelope) { e.subID = 12345 }},
		{"max sub id", func(e *mqttQ2AffineEnvelope) { e.subID = mqttMaxVarInt }},
		{"empty payload", func(e *mqttQ2AffineEnvelope) { e.msg = nil }},
		{"non-zero expectation", func(e *mqttQ2AffineEnvelope) { e.expectSeq = 1 << 40 }},
		{"everything", func(e *mqttQ2AffineEnvelope) {
			e.retained = true
			e.props = bytes.Repeat([]byte{0x26}, 40)
			e.subID = 7
			e.natsSubject = strings.Repeat("a.", 30) + "z"
			e.msg = bytes.Repeat([]byte("x"), 4096)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := base()
			tc.mut(env)
			want := mqttComputeQ2AffineSize(env)
			msg, hdrLen := mqttNewQ2AffineRecord(env)
			if len(msg) != want {
				t.Fatalf("encoder wrote %d bytes, size calculator said %d", len(msg), want)
			}
			if hdrLen != len(msg)-len(env.msg) {
				t.Fatalf("header length %d, want %d", hdrLen, len(msg)-len(env.msg))
			}
			if !bytes.Equal(msg[hdrLen:], env.msg) {
				t.Fatal("payload was not written verbatim after the header")
			}
			if !bytes.HasSuffix(msg[:hdrLen], []byte(_CRLF_+_CRLF_)) && len(msg[:hdrLen]) > 0 {
				// The last header line's CRLF plus the end-of-header CRLF.
				t.Fatalf("header block does not end in a blank line: %q", msg[:hdrLen])
			}
		})
	}
}

// The copy is built from FRESH headers, never spliced from the source record. A
// spliced envelope would inherit an origin marker (meaningless for a shared
// subscription, which is never No Local), a per-message TTL, and — worst — the
// inbound move protocol's Nats-Msg-Id, which would make the copies of one
// $MQTT_msgs message moved into two different groups dedup each other away.
func TestQ2AffineRecordCarriesOnlyItsOwnIdentity(t *testing.T) {
	t.Parallel()
	env := &mqttQ2AffineEnvelope{
		natsSubject:  "foo.bar",
		msg:          []byte("payload"),
		props:        []byte{0x02, 0x01, 0x01},
		retained:     true,
		subID:        9,
		claimSubject: mqttQ2AffClaimSubject("$MQTT_SHARED_dead", 7),
		gen:          mqttNewQ2AffGen(),
		msgID:        mqttQ2AffMsgId("$MQTT_SHARED_dead", 7, testQ2AffFixtureNonce),
	}
	msg, hdrLen := mqttNewQ2AffineRecord(env)
	hdr := msg[:hdrLen]

	for _, forbidden := range []string{mqttNatsHeaderOrigin, JSMessageTTL, mqttQoS2GenHeader, mqttNatsHeaderMapped} {
		if mqttCountHeader(forbidden, hdr) != 0 {
			t.Fatalf("affine copy must not carry %s", forbidden)
		}
	}
	for _, required := range []string{
		JSExpectedLastSubjSeq, JSMsgId, mqttNatsHeader, mqttNatsHeaderSubject,
		mqttQ2AffClaimHeader, mqttQ2AffGenHeader, mqttNatsHeaderRetain,
		mqttNatsHeaderProps, mqttQ2AffSubIDHeader,
	} {
		if n := mqttCountHeader(required, hdr); n != 1 {
			t.Fatalf("expected exactly one %s header, got %d", required, n)
		}
	}
	if got := string(getHeader(JSMsgId, hdr)); got != env.msgID {
		t.Fatalf("msg id is %q, want %q", got, env.msgID)
	}
	if got := string(getHeader(mqttQ2AffClaimHeader, hdr)); got != env.claimSubject {
		t.Fatalf("claim header is %q, want %q", got, env.claimSubject)
	}
	if got := string(getHeader(JSExpectedLastSubjSeq, hdr)); got != "0" {
		t.Fatalf("the copy store must be create-only, expectation header is %q", got)
	}

	// The generic publish-header parser must read it back as a QoS 2 message on
	// the ORIGINAL topic: that is how the affine delivery callback recovers the
	// topic the copy's own subject no longer carries.
	h := mqttParsePublishNATSHeader(hdr)
	if h == nil || h.qos != 2 {
		t.Fatalf("copy did not parse as a QoS 2 publish: %+v", h)
	}
	if string(h.subject) != env.natsSubject {
		t.Fatalf("parsed subject %q, want %q", h.subject, env.natsSubject)
	}
	if !h.retained {
		t.Fatal("Retain As Published marker was not read back")
	}
	if !bytes.Equal(h.props, env.props) {
		t.Fatalf("parsed props %v, want %v", h.props, env.props)
	}
}

// The deduplication key namespaces by GROUP and source sequence. It is an
// optimization (see the file header), but a key that collided across groups
// would turn that optimization into silent loss for the second group.
func TestQ2AffMsgIdNamespacing(t *testing.T) {
	t.Parallel()
	a := mqttQ2AffMsgId("$MQTT_SHARED_aaa", 10, testQ2AffFixtureNonce)
	b := mqttQ2AffMsgId("$MQTT_SHARED_bbb", 10, testQ2AffFixtureNonce)
	c := mqttQ2AffMsgId("$MQTT_SHARED_aaa", 11, testQ2AffFixtureNonce)
	if a == b {
		t.Fatal("two groups moving one source message must not share a dedup key")
	}
	if a == c {
		t.Fatal("two source messages in one group must not share a dedup key")
	}
	if a != mqttQ2AffMsgId("$MQTT_SHARED_aaa", 10, testQ2AffFixtureNonce) {
		t.Fatal("the dedup key must be stable across retries")
	}
	if !strings.HasPrefix(a, mqttQ2AffMsgIdPrefix) {
		t.Fatalf("dedup key %q is not namespaced", a)
	}
}

// Every affine name carries the session generation, and the claim-specific token
// makes the copy subject unique per claim. Without the generation, a later clean
// session of the same client ID would inherit the previous generation's subject,
// durable and deliver subject.
func TestQ2AffineNamesAreGenerationScopedAndClaimSpecific(t *testing.T) {
	t.Parallel()
	const idHash = "abcdef0123456789"
	g1, g2 := mqttNewQ2AffGen(), mqttNewQ2AffGen()
	if g1 == g2 {
		t.Fatal("generations must be unique")
	}
	f1, d1, dur1 := mqttQ2AffineNames(idHash, g1)
	f2, d2, dur2 := mqttQ2AffineNames(idHash, g2)
	if f1 == f2 || d1 == d2 || dur1 == dur2 {
		t.Fatalf("names for two generations collide: (%s,%s,%s) vs (%s,%s,%s)", f1, d1, dur1, f2, d2, dur2)
	}
	if !strings.HasPrefix(f1, mqttQ2AffineSubjectPrefix) || !strings.HasSuffix(f1, ".>") {
		t.Fatalf("filter subject %q is not the generation prefix", f1)
	}

	ch1 := mqttQ2AffClaimHash("$MQTT_SHARED_aaa", 1)
	ch2 := mqttQ2AffClaimHash("$MQTT_SHARED_aaa", 2)
	if ch1 == ch2 {
		t.Fatal("two claims in one group must not share a copy subject")
	}
	if ch1 != mqttQ2AffClaimHash("$MQTT_SHARED_aaa", 1) {
		t.Fatal("the claim hash must be deterministic: the winner and an on-behalf contender must compute the same subject")
	}
	copy1 := mqttQ2AffCopySubject(idHash, g1, ch1, testQ2AffFixtureNonce)
	if !strings.HasPrefix(copy1, strings.TrimSuffix(f1, ">")) {
		t.Fatalf("copy subject %q is not covered by the consumer filter %q", copy1, f1)
	}
	if !strings.HasPrefix(copy1, mqttOutSubjectPrefix) {
		t.Fatalf("copy subject %q is not on the $MQTT_out stream's subject space", copy1)
	}
}

// A persisted affine consumer whose identity does not match the session
// generation must FAIL CLOSED. The config we cannot account for may belong to a
// consumer still holding an in-flight PUBLISH; re-provisioning over it would
// strand or delete that message.
func TestValidateQ2AffineNames(t *testing.T) {
	t.Parallel()
	const idHash = "abcdef0123456789"
	gen := mqttNewQ2AffGen()
	filter, deliver, durable := mqttQ2AffineNames(idHash, gen)
	good := func() *ConsumerConfig {
		return &ConsumerConfig{Durable: durable, FilterSubject: filter, DeliverSubject: deliver}
	}
	if err := mqttValidateQ2AffineNames(good(), idHash, gen); err != nil {
		t.Fatalf("the provisioning path's own names were rejected: %v", err)
	}
	if err := mqttValidateQ2AffineNames(nil, idHash, gen); err == nil {
		t.Fatal("expected a nil consumer config to be rejected")
	}
	for _, tc := range []struct {
		name string
		mut  func(cc *ConsumerConfig)
		gen  string
	}{
		{"foreign durable", func(cc *ConsumerConfig) { cc.Durable = "$MQTT_Q2AFF_other" }, gen},
		{"foreign filter", func(cc *ConsumerConfig) { cc.FilterSubject = mqttQ2AffineSubjectPrefix + "other.>" }, gen},
		{"foreign deliver subject", func(cc *ConsumerConfig) { cc.DeliverSubject = mqttQ2AffineDeliverPrefix + "other" }, gen},
		{"generation not an identity", func(*ConsumerConfig) {}, "short"},
		{"generation out of alphabet", func(*ConsumerConfig) {}, strings.Repeat("-", mqttQ2AffGenLen)},
		{"empty generation", func(*ConsumerConfig) {}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := good()
			tc.mut(cc)
			if err := mqttValidateQ2AffineNames(cc, idHash, tc.gen); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}
}

// Claim transitions must PRESERVE facts already proven, and the phase must never
// go backwards. The specific bug this guards is clear()'s discard erasing a
// SourceAcked a contender had proven: the claim would then look reapable while
// its group ack was in fact unconfirmed, and the source redelivery would elect a
// fresh winner — the central violation, through the discard side-door.
func TestQ2AffClaimTransitionsPreserveProvenFacts(t *testing.T) {
	t.Parallel()
	prev := &mqttQ2AffClaim{Phase: mqttQ2AffMaterialized, SourceAcked: true}

	discard := &mqttQ2AffClaim{Phase: mqttQ2AffClaimed, Discarded: true}
	discard.mergeProven(prev)
	if !discard.SourceAcked {
		t.Fatal("a discard erased a proven source ack")
	}
	if discard.Phase != mqttQ2AffMaterialized {
		t.Fatalf("phase went backwards to %q", discard.Phase)
	}

	// An unknown phase from a future version must not demote a phase we
	// understand, and must not be treated as authorizing delivery.
	future := &mqttQ2AffClaim{Phase: mqttQ2AffPhase("someday")}
	future.mergeProven(prev)
	if future.Phase != mqttQ2AffMaterialized {
		t.Fatalf("an unknown phase demoted a materialized claim to %q", future.Phase)
	}
	if mqttQ2AffPhase("someday").rank() >= mqttQ2AffClaimed.rank() {
		t.Fatal("an unknown phase must rank below claimed")
	}

	// Facts only ever accumulate.
	acc := &mqttQ2AffClaim{Phase: mqttQ2AffClaimed}
	acc.mergeProven(&mqttQ2AffClaim{AffineDone: true})
	acc.mergeProven(&mqttQ2AffClaim{Discarded: true})
	acc.mergeProven(&mqttQ2AffClaim{SourceAcked: true})
	if !acc.AffineDone || !acc.Discarded || !acc.SourceAcked {
		t.Fatalf("a merge dropped a proven fact: %+v", acc)
	}

	// ...except the two that are mutually exclusive. A record must never end up
	// asserting both that the message was delivered and that it was lost.
	delivered := &mqttQ2AffClaim{Lost: true}
	delivered.mergeProven(&mqttQ2AffClaim{AffineDone: true})
	if delivered.Lost || !delivered.AffineDone {
		t.Fatalf("a lost marker was added on top of a completed delivery: %+v", delivered)
	}
	lost := &mqttQ2AffClaim{AffineDone: true}
	lost.mergeProven(&mqttQ2AffClaim{Lost: true})
	if lost.AffineDone || !lost.Lost {
		t.Fatalf("a completed delivery was added on top of a recorded loss: %+v", lost)
	}
	// mergeProven with no previous record is a no-op, not a reset.
	before := *acc
	acc.mergeProven(nil)
	if *acc != before {
		t.Fatal("merging a nil previous record changed the claim")
	}
}

// Deletion needs BOTH sides proven AND the copy actually gone. SourceAcked alone
// deletes a claim whose copy is still pending, stranding it behind the
// fail-closed delivery gate; a terminal fact alone deletes a claim whose group
// ack never committed, reopening ownership; and a live copy always keeps its
// claim, because the affine side is recorded BEFORE the copy is acked and
// deleting the claim in that window would strand the copy with nothing left able
// to authorize or dispose of it.
func TestQ2AffClaimReapRule(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		claim      mqttQ2AffClaim
		copyAbsent bool
		want       bool
	}{
		{"fresh claim", mqttQ2AffClaim{Phase: mqttQ2AffClaimed}, true, false},
		{"source acked, affine pending", mqttQ2AffClaim{SourceAcked: true}, false, false},
		{"affine done, source unproven", mqttQ2AffClaim{AffineDone: true}, true, false},
		{"discarded, source unproven", mqttQ2AffClaim{Discarded: true}, true, false},
		{"discarded and acked, copy still present", mqttQ2AffClaim{Discarded: true, SourceAcked: true}, false, false},
		{"both proven but the copy is still there", mqttQ2AffClaim{SourceAcked: true, AffineDone: true}, false, false},
		{"both proven and the copy is gone", mqttQ2AffClaim{SourceAcked: true, AffineDone: true}, true, true},
		{"discarded, acked, copy gone", mqttQ2AffClaim{Discarded: true, SourceAcked: true}, true, true},
		{"lost is never reapable", mqttQ2AffClaim{SourceAcked: true, Lost: true}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.claim
			if got := c.reapable(tc.copyAbsent); got != tc.want {
				t.Fatalf("reapable(copyAbsent=%v) = %v, want %v", tc.copyAbsent, got, tc.want)
			}
		})
	}
	var nilClaim *mqttQ2AffClaim
	if nilClaim.reapable(true) {
		t.Fatal("an absent claim must never be reapable: absence is not authorization")
	}
	if nilClaim.terminal() {
		t.Fatal("an absent claim must not read as terminal")
	}
	if (&mqttQ2AffClaim{Winner: "a", Gen: "g"}).ownedBy("a", "h") {
		t.Fatal("ownership must match the generation, not just the client hash")
	}
}

// Every field validateQoS2AssignStream rejects, rejected independently — plus a
// positive row for the exact configuration the create path produces, so the
// validator cannot reject its own output.
func TestValidateQoS2AssignStream(t *testing.T) {
	t.Parallel()
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:       mqttQoS2AssignStreamName,
			Subjects:   []string{mqttQoS2AssignStreamSubjectPrefix + ">"},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
			Replicas:   3,
			Metadata:   map[string]string{mqttSessEpochMetaKey: testBundleEpoch},
		}}
	}
	if err := validateQoS2AssignStream(good(), 3, testBundleEpoch); err != nil {
		t.Fatalf("the create path's own configuration was rejected: %v", err)
	}
	if err := validateQoS2AssignStream(good(), 0, testBundleEpoch); err != nil {
		t.Fatalf("unexpected rejection with no replica expectation: %v", err)
	}
	if err := validateQoS2AssignStream(nil, 0, testBundleEpoch); err == nil {
		t.Fatal("expected nil stream info to be rejected")
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"wrong subject", func(c *StreamConfig) { c.Subjects = []string{"other.>"} }},
		{"extra subject", func(c *StreamConfig) { c.Subjects = append(c.Subjects, "other.>") }},
		{"no subjects", func(c *StreamConfig) { c.Subjects = nil }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"interest retention", func(c *StreamConfig) { c.Retention = InterestPolicy }},
		{"discard new", func(c *StreamConfig) { c.Discard = DiscardNew }},
		{"max msgs per 0", func(c *StreamConfig) { c.MaxMsgsPer = 0 }},
		{"max msgs per 2", func(c *StreamConfig) { c.MaxMsgsPer = 2 }},
		{"global max msgs", func(c *StreamConfig) { c.MaxMsgs = 100 }},
		{"global max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"global max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} }},
		{"sources", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} }},
		{"wrong replicas", func(c *StreamConfig) { c.Replicas = 1 }},
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) { c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"} }},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"allow msg counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
		{"allow rollup", func(c *StreamConfig) { c.AllowRollup = true }},
		{"async persist mode", func(c *StreamConfig) { c.PersistMode = AsyncPersistMode }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateQoS2AssignStream(si, 3, testBundleEpoch); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}
}

// validateOutStream's list is DIFFERENT from the claim stream's, because this
// owner does different things to $MQTT_out: append-only stores, consumer
// delivery, interest-based drop. It never deletes or purges messages there, so
// DenyDelete/DenyPurge are deliberately NOT prerequisites — a positive row
// asserts that, so a future copy-paste from the claim validator is caught.
func TestValidateOutStream(t *testing.T) {
	t.Parallel()
	good := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Name:      mqttOutStreamName,
			Subjects:  []string{mqttOutSubjectPrefix + ">"},
			Storage:   FileStorage,
			Retention: InterestPolicy,
			Replicas:  3,
			Metadata:  map[string]string{mqttSessEpochMetaKey: testBundleEpoch},
		}}
	}
	if err := validateOutStream(good(), 3, mqttDefaultMaxAckPending, testBundleEpoch); err != nil {
		t.Fatalf("the create path's own configuration was rejected: %v", err)
	}
	if err := validateOutStream(good(), 0, mqttDefaultMaxAckPending, testBundleEpoch); err != nil {
		t.Fatalf("unexpected rejection with no replica expectation: %v", err)
	}
	if err := validateOutStream(nil, 0, mqttDefaultMaxAckPending, testBundleEpoch); err == nil {
		t.Fatal("expected nil stream info to be rejected")
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"deny purge is not a prerequisite", func(c *StreamConfig) { c.DenyPurge = true }},
		// The explicit boundary of the MaxAckPending threshold form: a stream
		// limit AT the broker's effective cap changes nothing, so it must start.
		// Only a limit BELOW the cap makes JetStream reject the consumer creates.
		{"consumer max_ack_pending at the cap is not a prerequisite", func(c *StreamConfig) {
			c.ConsumerLimits.MaxAckPending = mqttDefaultMaxAckPending
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateOutStream(si, 3, mqttDefaultMaxAckPending, testBundleEpoch); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}
	for _, tc := range []struct {
		name string
		mut  func(cfg *StreamConfig)
	}{
		{"wrong subject", func(c *StreamConfig) { c.Subjects = []string{"other.>"} }},
		{"extra subject", func(c *StreamConfig) { c.Subjects = append(c.Subjects, "other.>") }},
		{"memory storage", func(c *StreamConfig) { c.Storage = MemoryStorage }},
		{"limits retention", func(c *StreamConfig) { c.Retention = LimitsPolicy }},
		{"max msgs per subject", func(c *StreamConfig) { c.MaxMsgsPer = 1 }},
		{"global max msgs", func(c *StreamConfig) { c.MaxMsgs = 100 }},
		{"global max bytes", func(c *StreamConfig) { c.MaxBytes = 1 << 20 }},
		{"global max age", func(c *StreamConfig) { c.MaxAge = time.Hour }},
		{"mirror", func(c *StreamConfig) { c.Mirror = &StreamSource{Name: "X"} }},
		{"sources", func(c *StreamConfig) { c.Sources = []*StreamSource{{Name: "X"}} }},
		{"wrong replicas", func(c *StreamConfig) { c.Replicas = 1 }},
		{"max msg size", func(c *StreamConfig) { c.MaxMsgSize = 1024 }},
		{"no ack", func(c *StreamConfig) { c.NoAck = true }},
		// Since F-065 this owner DELETES an exact sequence here, to withdraw a copy no
		// claim of its incarnation can authorize. DenyDelete is therefore a CHANGED
		// PREREQUISITE, not a weakened test: the fact the accepted table above used to
		// assert — "this owner issues no delete on $MQTT_out" — has stopped being true, so
		// the assertion moves tables rather than relaxing. Denied, such a copy stays
		// behind the fail-closed delivery gate, NAK-looping for the session's whole life.
		{"deny delete", func(c *StreamConfig) { c.DenyDelete = true }},
		{"sealed", func(c *StreamConfig) { c.Sealed = true }},
		{"subject transform", func(c *StreamConfig) { c.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"} }},
		{"republish", func(c *StreamConfig) { c.RePublish = &RePublish{Source: "a", Destination: "b"} }},
		{"subject delete marker ttl", func(c *StreamConfig) { c.SubjectDeleteMarkerTTL = time.Minute }},
		{"allow msg counter", func(c *StreamConfig) { c.AllowMsgCounter = true }},
		{"allow rollup", func(c *StreamConfig) { c.AllowRollup = true }},
		{"async persist mode", func(c *StreamConfig) { c.PersistMode = AsyncPersistMode }},
		{"stream-forced consumer inactivity", func(c *StreamConfig) { c.ConsumerLimits.InactiveThreshold = time.Minute }},
		// Both durables here are per session (PUBREL + affine), so a finite cap
		// eventually rejects a legitimate session's resume.
		{"finite max consumers", func(c *StreamConfig) { c.MaxConsumers = 5 }},
		// Below the effective cap JetStream rejects the durable creates outright,
		// failing every QoS 2 resume. (At the cap it must be accepted; see above.)
		{"consumer max_ack_pending below the cap", func(c *StreamConfig) { c.ConsumerLimits.MaxAckPending = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := good()
			tc.mut(&si.Config)
			if err := validateOutStream(si, 3, mqttDefaultMaxAckPending, testBundleEpoch); err == nil {
				t.Fatalf("expected %s to be rejected", tc.name)
			}
		})
	}
}

// The admission bound must dominate every affine copy the exchange can actually
// write. It is budgeted at PUBLISH time, before the winning subscription (and so
// its Subscription Identifier and Retain As Published option) exists, which is
// why those are budgeted at their maximum rather than at their actual value.
func TestQ2AffineFormMaxBoundsEveryRealRecord(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("x"), 512)
	subj, err := mqttTopicToNATSPubSubject([]byte("shared/topic/deep"))
	if err != nil {
		t.Fatalf("subject conversion: %v", err)
	}
	props := bytes.Repeat([]byte{0x26}, 64)
	pp := &mqttPublish{topic: []byte("shared/topic/deep"), subject: subj, msg: payload, sz: len(payload), props: props}
	bound := mqttQ2AffineFormMax(pp, false)

	// A worst-case real record: the widest group durable, the widest source
	// sequence, a full-width generation, the largest subscription identifier and
	// the RETAIN marker present.
	widestDur := mqttSharedConsumerDurablePrefix + strings.Repeat("f", 16)
	const widestSeq = ^uint64(0)
	env := &mqttQ2AffineEnvelope{
		natsSubject:  string(subj),
		msg:          payload,
		props:        props,
		retained:     true,
		subID:        mqttMaxVarInt,
		claimSubject: mqttQ2AffClaimSubject(widestDur, widestSeq),
		gen:          mqttNewQ2AffGen(),
		msgID:        mqttQ2AffMsgId(widestDur, widestSeq, testQ2AffFixtureNonce),
	}
	if got := mqttComputeQ2AffineSize(env); got > bound {
		t.Fatalf("worst-case real record is %d bytes, but the admission bound is %d", got, bound)
	}
	// And it must not be wildly loose either: the bound exists to admit messages,
	// not to reject them. One generation's worth of slack is the tolerance.
	if got := mqttComputeQ2AffineSize(env); bound-got > mqttQ2AffGenLen {
		t.Fatalf("admission bound %d is %d bytes above the worst-case record %d", bound, bound-got, got)
	}

	// A create-only expectation is what the real store writes; the bound budgets
	// the full uint64 width, so the real record can only be smaller.
	env.expectSeq = 0
	if mqttComputeQ2AffineSize(env) > bound {
		t.Fatal("the create-only form must fit the bound")
	}

	// The runtime backstop for a message stored without the admission bound, which
	// a QoS 1/2 Will bypasses.
	if !mqttQ2AffineRecordFits(env, int32(bound)) {
		t.Fatal("a record at the bound must be accepted by the runtime check")
	}
	if mqttQ2AffineRecordFits(env, int32(mqttComputeQ2AffineSize(env)-1)) {
		t.Fatal("a record one byte over must be rejected by the runtime check")
	}
	if !mqttQ2AffineRecordFits(env, 0) {
		t.Fatal("no configured max payload must not reject anything")
	}
}

// The ASSIGNMENT CLAIM record is budgeted against the REAL encoder, at the widest
// identity and delivery values it can carry, and over every reachable terminal shape
// — not just the initial election form.
//
// It is budgeted for the same reason the copy is, and the wedge is the mirror image:
// a PUBLISH admitted at PUBREC time whose CLAIM cannot be stored means no election
// can ever be won, so the group delivery is never acked and AckWait retries forever.
// Nothing checked this before F-065 widened the record.
//
// Paired mutation M15: omit the nonce from mqttQ2AffMaxIdentityWidths, or drop the
// terminal shapes and budget only the election form. Either makes the bound fall
// below what the encoder actually emits.
func TestQ2AffClaimRecordMaxCoversEveryReachableShape(t *testing.T) {
	t.Parallel()
	bound := mqttQ2AffClaimRecordMax()
	claimW, _, genW := mqttQ2AffMaxIdentityWidths()
	maxDur := len(mqttSharedConsumerDurablePrefix) + 16
	pad := strings.Repeat("x", claimW)
	base := mqttQ2AffClaim{
		Winner:       strings.Repeat("x", genW),
		Gen:          strings.Repeat("x", genW),
		Nonce:        strings.Repeat("x", genW),
		GroupDurable: strings.Repeat("x", maxDur),
		SourceSeq:    math.MaxUint64,
		ClaimHash:    strings.Repeat("x", mqttQ2AffClaimHashLen),
		CopySubject:  pad,
		AffDurable:   pad,
		SubID:        mqttMaxVarInt,
		Retained:     true,
	}
	// Every shape a transition can actually reach, including the initial election
	// form. mergeProven keeps AffineDone and Lost mutually exclusive, so those two are
	// separate shapes rather than one record carrying both.
	shapes := map[string]func(*mqttQ2AffClaim){
		"elected": func(c *mqttQ2AffClaim) { c.Phase = mqttQ2AffClaimed },
		"materialized": func(c *mqttQ2AffClaim) {
			c.Phase = mqttQ2AffMaterialized
		},
		"source acked": func(c *mqttQ2AffClaim) {
			c.Phase, c.SourceAcked = mqttQ2AffMaterialized, true
		},
		"complete": func(c *mqttQ2AffClaim) {
			c.Phase, c.SourceAcked, c.AffineDone = mqttQ2AffMaterialized, true, true
		},
		"complete and discarded": func(c *mqttQ2AffClaim) {
			c.Phase, c.SourceAcked, c.AffineDone, c.Discarded = mqttQ2AffMaterialized, true, true, true
		},
		"lost and discarded": func(c *mqttQ2AffClaim) {
			c.Phase, c.SourceAcked, c.Lost, c.Discarded = mqttQ2AffMaterialized, true, true, true
		},
		// The PUBREC-proof shapes. PubRecProven implies AffineDone and excludes Lost
		// (mergeProven), so the proven form is the complete form plus one more member and
		// is therefore the LARGEST reachable record. A bound that compares only the two
		// older terminal shapes leaves a record this broker writes outside the admission
		// preflight, which is the eternal-retry wedge the preflight exists to prevent.
		"pubrec proven": func(c *mqttQ2AffClaim) {
			c.Phase, c.SourceAcked, c.AffineDone, c.PubRecProven =
				mqttQ2AffMaterialized, true, true, true
		},
		"pubrec proven and discarded": func(c *mqttQ2AffClaim) {
			c.Phase, c.SourceAcked, c.AffineDone, c.PubRecProven, c.Discarded =
				mqttQ2AffMaterialized, true, true, true, true
		},
	}
	for name, mut := range shapes {
		t.Run(name, func(t *testing.T) {
			rec := base
			mut(&rec)
			body, err := json.Marshal(&rec)
			if err != nil {
				t.Fatalf("marshalling: %v", err)
			}
			// The create-only CAS header is not stripped before storage, so it counts
			// toward max_payload exactly as the record body does.
			hdr := len(hdrLine) + len(JSExpectedLastSubjSeq) + 1 +
				len(strconv.FormatUint(math.MaxUint64, 10)) + 2 + 2
			if got := hdr + len(body); got > bound {
				t.Fatalf("the %s record encodes to %d bytes but the admission bound is %d: a PUBLISH would be accepted whose claim can never be stored, and its group delivery would retry forever", name, got, bound)
			}
		})
	}
}
