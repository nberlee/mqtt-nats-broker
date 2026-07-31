// Session-affine ownership of a STARTED shared-subscription QoS 2 delivery.
//
// A shared-subscription QoS 2 message is pending on the queue-delivered GROUP
// durable from the moment its PUBLISH is sent until the client's PUBREC. In that
// window the group durable is the only thing that owns it, so a member leaving
// (UNSUBSCRIBE, disconnect, session end) made AckWait redeliver the started
// message to a DIFFERENT member — or dropped it when the leaver was the last
// holder. Spec5 [MQTT-4.8.2-5]/[MQTT-4.8.2-6] require the opposite: once the
// server has begun delivering a shared message to one client it must complete
// that delivery with that client, and it may only die with that client's session.
//
// # The two durable pieces
//
//  1. An assignment CLAIM record on $MQTT_qos2assign, one per
//     (group durable, source stream sequence), naming the winning session
//     generation. A create-only CAS elects the winner; every other contender
//     LOADS the claim and honors it INDEFINITELY. This is the arbitration
//     mechanism, and it is durable on purpose — see "Dedup is not arbitration".
//  2. An affine COPY of the message on the winner's own generation-scoped
//     $MQTT_out subject, delivered by a durable owned by that session generation.
//     The whole PUBLISH -> PUBREC -> PUBREL -> PUBCOMP exchange then runs off the
//     winner's own consumer, where no group member can reach it.
//
// The group delivery is acked only after the claim says the copy exists. So the
// message is never simultaneously "released to the group" and "not yet owned".
//
// # Dedup is not arbitration
//
// Nats-Msg-Id deduplication has a FINITE window. Using it to decide who owns a
// started delivery reintroduces the exact violation this file exists to remove:
// crash after the PUBLISH but before the group ack commits, let the window
// expire, and the source redelivers to another member which believes it is the
// first writer. Every ownership decision here therefore reads a durable claim.
// The msg-id on the copy store is an optimization that narrows a duplicate-copy
// window; correctness does not rest on it. (Same rule as qos2_move.go's header.)
//
// # Why the copy subject carries the CLAIM, not just the session
//
// Two writers can legitimately try to materialize one claim at the same time: the
// winner (on its own redelivery) and a contender materializing ON BEHALF of an
// absent winner. "Scan for an existing copy, then store" is not atomic, so the
// copy subject ends in a per-claim token and the store is a create-only CAS
// (JSExpectedLastSubjSeq: 0). Exactly one store can succeed, whatever the dedup
// window is doing. A rejected CAS is resolved by an authoritative RE-READ of that
// subject, never by assuming "it already exists" — the copy can be consumed and
// acked away between the rejection and the handling.
//
// # Why every affine name carries a session GENERATION
//
// idHash is a hash of the CLIENT ID, so a later clean session of the same client
// reuses it. The copy subject is append-only and needs no CAS generation, but it
// absolutely needs a session-generation NAMESPACE, and so do the consumer durable
// and its deliver subject: a leaked consumer from a previous generation must not
// inject into a new session's internal subscription, and a delayed copy store
// racing terminate-and-recreate must land where only the dead generation could
// have read it. All three names are validated together on restore and fail CLOSED
// on a mismatch.
//
// # Facts are orthogonal and monotonic, not a linear state
//
// A claim tracks THREE independent completion facts plus a phase:
//
//   - phase (claimed -> materialized): does a copy provably exist?
//   - SourceAcked: has the group delivery's ack been PROVEN committed?
//   - AffineDone: has the copy's own delivery reached a terminal state?
//   - Discarded: the winner's session ended; the delivery dies with it.
//
// A linear enum would let one fact stand in for another. In particular Discarded
// does NOT prove SourceAcked: reaping a discarded claim whose group ack never
// committed reopens ownership through the discard side-door, which is the central
// violation again. Every CAS PRESERVES facts already proven. A claim may be
// deleted only when SourceAcked AND (AffineDone OR (Discarded AND the copy is
// authoritatively absent)).
//
// Claim ABSENCE is never authorization: the affine delivery callback NAKs on a
// missing claim, which is safe precisely because nothing may delete a claim whose
// copy is still pending.
//
// # Residuals
//
//   - Packet-identifier continuity across a broker restart or failover. A
//     same-process reconnect reuses the original PI; after a restart the message
//     is redelivered to the SAME session with DUP=1 but a freshly allocated PI.
//     The message is never lost and never redistributed; HA exactly-once
//     (identifier continuity) remains a documented deviation. See
//     docs/CONFORMANCE.md.
//   - Mixed-version operation. The storage schema is rolling-compatible, but the
//     delivery PROTOCOL is not: an older broker serving the same shared group
//     never consults $MQTT_qos2assign and will redistribute. Shared QoS 2
//     affinity holds only once every broker that can serve the group is
//     q2aff-aware.

package mqtt

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nuid"
)

const (
	// mqttQ2AffClaimHeader carries, on the affine copy, the subject of the claim
	// that authorizes it. The delivery callback loads THAT claim and refuses to
	// PUBLISH unless it is durably materialized — the copy can become visible
	// before the claim's materialized CAS commits, and a PUBLISH under an
	// unresolved claim would let recovery create a second copy.
	mqttQ2AffClaimHeader = "Nmqtt-Q2Claim"

	// mqttQ2AffGenHeader carries the winning session generation on the copy. It is
	// the copy's immutable identity; a consumer of a different generation never
	// sees the subject in the first place, so this is a cross-check, not the
	// primary isolation.
	mqttQ2AffGenHeader = "Nmqtt-Q2Aff"

	// mqttQ2AffSubIDHeader carries the v5 Subscription Identifier decided at move
	// time. It is NOT injected into the stored properties block: injection happens
	// per delivery (mqttInjectSubID), and baking it into the copy would double it.
	// Spec5 [3.8.2.1.2].
	mqttQ2AffSubIDHeader = "Nmqtt-SubId"

	// mqttQ2AffMsgIdPrefix namespaces the copy's deduplication key. A message on
	// $MQTT_msgs delivered to TWO shared groups is moved twice, and the two copies
	// must not dedup each other away, so the key carries the group durable and the
	// source sequence. Any msg-id inherited from the source record is stripped for
	// the same reason.
	mqttQ2AffMsgIdPrefix = "mqtt-q2aff-"

	// mqttQ2AffGenLen is the length of a nuid.Next() identity, the form every
	// session generation takes.
	mqttQ2AffGenLen = 22

	// mqttQ2AffClaimHashLen is the width of the per-claim token in the copy
	// subject. Wide enough that an accidental collision (which would let two
	// claims contend for one create-only-CAS subject) is negligible.
	mqttQ2AffClaimHashLen = 16

	// Bounded retry budget for the claim CAS transitions. Every loop re-reads and
	// reconciles rather than retrying blindly, so a small budget suffices;
	// exhaustion is reported as a failure and the source delivery is left unacked
	// for AckWait to re-offer.
	mqttQ2AffClaimAttempts = 3
)

// Fixed upper bounds for every element the affine envelope adds, used BOTH by
// mqttQ2AffineFormMax (the max_payload preflight in mqttProcessPub) and,
// indirectly, by the encoder they are derived from. The preflight must budget the
// LARGEST form the exchange will ever write: a QoS 2 PUBLISH accepted at PUBREC
// time whose affine form then exceeds max_payload can never be materialized, so
// its claim and its group delivery would retry forever. Deriving the bound by
// running the real encoder at maximal widths (see mqttQ2AffineFormMax) is what
// keeps bound and encoder from drifting.
const (
	// mqttQ2AffMaxSeqDigits is the decimal width of math.MaxUint64: the widest a
	// stream sequence can be, in the copy subject, the claim subject, the msg-id
	// and the expected-last-subject-sequence CAS header. The CAS header is always
	// written with 0 in practice (the copy store is create-only) but is budgeted at
	// full width so the bound cannot be invalidated by a future non-create write.
	mqttQ2AffMaxSeqDigits = 20
)

// mqttQ2AffPhase is the claim's monotonic progress marker: has a copy provably
// been created? It is deliberately NOT the whole state — completion facts are
// separate boolean fields, because they are independent and each has its own
// proof (see the file header).
type mqttQ2AffPhase string

const (
	// mqttQ2AffClaimed: ownership is decided, no copy is proven to exist yet. The
	// affine delivery callback must NOT publish under this phase.
	mqttQ2AffClaimed mqttQ2AffPhase = "claimed"
	// mqttQ2AffMaterialized: a copy provably exists on the claim-specific subject.
	// Only now may the group delivery be acked and the copy be published.
	mqttQ2AffMaterialized mqttQ2AffPhase = "materialized"
)

// rank orders the phase for the monotonic merge. An unknown phase (a record
// written by a future version) ranks below claimed so it can never DEMOTE a
// phase we understand, and never silently authorize a delivery.
func (p mqttQ2AffPhase) rank() int {
	switch p {
	case mqttQ2AffMaterialized:
		return 2
	case mqttQ2AffClaimed:
		return 1
	default:
		return 0
	}
}

// mqttQ2AffClaim is one durable assignment record, living on its own subject
// (MaxMsgsPer:1) so each transition is a CAS at an exact sequence.
//
// Winner+Gen is the immutable identity every transition is matched against
// (qos2_move.go invariant 1): the subject is keyed by group durable and source
// sequence, which say nothing about which session generation owns it, so a
// sequence is not an identity here either.
type mqttQ2AffClaim struct {
	Winner       string `json:"winner"` // winning session's client-ID hash
	Gen          string `json:"gen"`    // winning session's durable generation
	GroupDurable string `json:"gdur"`
	SourceSeq    uint64 `json:"sseq"`
	ClaimHash    string `json:"chash"`
	// CopySubject and AffDurable record the EXACT destination the winner was
	// elected with, so a contender materializing on the winner's behalf writes
	// where that generation (and only that generation) can read it, instead of
	// re-deriving names from a session record it may be racing.
	CopySubject string `json:"copy"`
	AffDurable  string `json:"adur"`
	// SubID and Retained are the winner's delivery decisions, baked in at claim
	// time. A contender materializing on the winner's behalf must write the
	// WINNER's copy, not one shaped by its own subscription options — two members
	// of one group can carry different Subscription Identifiers and different
	// Retain As Published settings. Spec5 [3.8.2.1.2], [3.8.3.1].
	SubID    int  `json:"subid,omitempty"`
	Retained bool `json:"ret,omitempty"`

	Phase mqttQ2AffPhase `json:"phase"`
	// SourceAcked: the group delivery's ack is PROVEN committed (a confirmed
	// double-ack, or authoritative group-consumer reconciliation). Never set on
	// the strength of a queued sendAck.
	SourceAcked bool `json:"sacked,omitempty"`
	// AffineDone: the copy's delivery reached a terminal state (PUBREC handed the
	// exchange to the PUBREL path, the copy was dropped as undeliverable, or the
	// copy is authoritatively absent).
	AffineDone bool `json:"adone,omitempty"`
	// Discarded: the winning session ended. The delivery dies with it, which
	// [MQTT-4.8.2-6] permits; it is NEVER released back to the group.
	Discarded bool `json:"disc,omitempty"`
	// Lost: mutually exclusive with AffineDone. The copy is gone and was NEVER
	// delivered — the affine consumer was destroyed out of band, and $MQTT_out is
	// interest-retained, so its pending
	// copies went with it. This is a QoS 2 message the broker accepted and did not
	// deliver. It is deliberately NOT AffineDone: recording it as a completed
	// delivery would launder a compliance failure into a clean reap, and the
	// evidence would then be deleted. A lost claim is never reapable, so the
	// durable record survives for an operator to find; it is terminal, so no
	// contender waits on it and the group delivery is still released.
	Lost bool `json:"lost,omitempty"`
}

// ownedBy reports whether this claim names the given session generation.
func (c *mqttQ2AffClaim) ownedBy(idHash, gen string) bool {
	return c != nil && c.Winner == idHash && c.Gen == gen
}

// mergeProven folds every fact already proven by prev into c, and never lets the
// phase go backwards. Called on the record about to be CAS-written, with prev the
// record just loaded at the CAS sequence.
//
// This is the mechanism behind "transitions preserve proven facts". Without it,
// clear()'s discard would erase a SourceAcked that a contender had proven, and
// the claim would become reapable while its group ack was in fact unconfirmed.
func (c *mqttQ2AffClaim) mergeProven(prev *mqttQ2AffClaim) {
	if prev == nil {
		return
	}
	c.SourceAcked = c.SourceAcked || prev.SourceAcked
	c.AffineDone = c.AffineDone || prev.AffineDone
	c.Discarded = c.Discarded || prev.Discarded
	c.Lost = c.Lost || prev.Lost
	// AffineDone and Lost are mutually exclusive — the message was either
	// delivered or it was not — so they are NOT independently OR-merged. Whichever
	// the durable record already proved stands; a later writer cannot add its
	// opposite on top and leave a record asserting both.
	if prev.AffineDone {
		c.Lost = false
	} else if prev.Lost {
		c.AffineDone = false
	}
	if prev.Phase.rank() > c.Phase.rank() {
		c.Phase = prev.Phase
	}
}

// reapable reports whether the claim may be deleted. copyAbsent must be the
// result of an AUTHORITATIVE lookup of the claim-specific copy subject (a load
// error is not an absence).
//
// Both sides must be proven. SourceAcked alone would delete a claim whose copy is
// still pending, stranding it behind the fail-closed delivery gate; AffineDone
// alone (or Discarded alone) would delete a claim whose group ack never
// committed, letting the source redelivery elect a fresh winner.
func (c *mqttQ2AffClaim) reapable(copyAbsent bool) bool {
	if c == nil || !c.SourceAcked {
		return false
	}
	if c.Lost {
		// A message the broker accepted and never delivered. Deleting the record
		// would destroy the only durable evidence that it happened.
		return false
	}
	if !copyAbsent {
		// A copy that still exists keeps its claim, whatever else is proven. The
		// affine side is recorded BEFORE the copy is acked, so a live copy under an
		// AffineDone claim is the crash window between those two steps — and
		// deleting the claim there would strand the copy behind the delivery gate,
		// NAK'd forever with nothing left able to authorize or dispose of it.
		return false
	}
	return c.AffineDone || c.Discarded
}

// terminal reports whether the claim can no longer produce an MQTT delivery, so a
// contender holding the source message may ack it (once SourceAcked is proven)
// and stop reasoning about it.
func (c *mqttQ2AffClaim) terminal() bool {
	return c != nil && (c.Discarded || c.AffineDone || c.Lost)
}

// mqttQ2AffMoveResult is the resolved disposition of one shared QoS 2 move
// attempt. It is an enum rather than an error because the interesting outcomes
// are not failures: "a foreign session owns this, I acked and walked away" and
// "I could not prove anything, so I left the delivery unacked" are both correct,
// and only distinguishable by intent.
type mqttQ2AffMoveResult int

const (
	// mqttQ2AffMoveOwned: we are the durable winner and the copy is materialized.
	mqttQ2AffMoveOwned mqttQ2AffMoveResult = iota
	// mqttQ2AffMoveForeign: a foreign session owns it; we acked the group delivery.
	mqttQ2AffMoveForeign
	// mqttQ2AffMoveOnBehalf: a foreign session owns it and was absent, so we
	// materialized the copy onto ITS durable and acked the group delivery.
	mqttQ2AffMoveOnBehalf
	// mqttQ2AffMoveTerminal: the claim is discarded or complete; we acked the group
	// delivery (after proving the ack where that was still owed).
	mqttQ2AffMoveTerminal
	// mqttQ2AffMoveDropped: the message can never be delivered (expired, or its
	// affine form exceeds max_payload); we acked the group delivery and stored
	// nothing.
	mqttQ2AffMoveDropped
	// mqttQ2AffMoveDeferred: nothing could be proven (readiness gate closed,
	// indeterminate load, failed CAS). Nothing was acked; AckWait re-offers.
	mqttQ2AffMoveDeferred
)

func (r mqttQ2AffMoveResult) String() string {
	switch r {
	case mqttQ2AffMoveOwned:
		return "owned"
	case mqttQ2AffMoveForeign:
		return "foreign"
	case mqttQ2AffMoveOnBehalf:
		return "on-behalf"
	case mqttQ2AffMoveTerminal:
		return "terminal"
	case mqttQ2AffMoveDropped:
		return "dropped"
	default:
		return "deferred"
	}
}

// mqttValidQ2AffGen reports whether s is a well-formed session generation:
// exactly 22 characters of the NUID base-62 alphabet. Same rule, and same
// reasoning, as mqttValidQoS2Gen — an out-of-alphabet body of the right length
// must not be admitted into the identity space, and empty must not collapse "no
// identity" into it.
func mqttValidQ2AffGen(s string) bool {
	if len(s) != mqttQ2AffGenLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// mqttNewQ2AffGen mints a session generation.
func mqttNewQ2AffGen() string { return nuid.Next() }

// mqttQ2AffClaimHash is the per-claim token that ends the copy subject. Derived
// deterministically from the claim's key so the winner and any contender
// materializing on its behalf compute the SAME subject — which is what makes the
// create-only CAS on it a mutual exclusion rather than two independent stores.
func mqttQ2AffClaimHash(groupDurable string, sseq uint64) string {
	return getHashSize(groupDurable+" "+strconv.FormatUint(sseq, 10), mqttQ2AffClaimHashLen)
}

// mqttQ2AffClaimSubject is the claim record's subject.
func mqttQ2AffClaimSubject(groupDurable string, sseq uint64) string {
	return mqttQoS2AssignStreamSubjectPrefix + groupDurable + "." + strconv.FormatUint(sseq, 10)
}

// mqttQ2AffCopySubject is the claim-specific copy subject on $MQTT_out. The
// generation namespaces it against a later session of the same client ID; the
// claim hash makes the store a create-only CAS with exactly one winner.
func mqttQ2AffCopySubject(idHash, gen, claimHash string) string {
	return mqttQ2AffineSubjectPrefix + idHash + "." + gen + "." + claimHash
}

// mqttQ2AffineNames returns the three generation-scoped names of a session's
// affine QoS 2 delivery path: the consumer's filter subject (the generation
// prefix, covering every claim's copy), its deliver subject, and its durable.
// They are always created, validated and torn down together.
func mqttQ2AffineNames(idHash, gen string) (filter, deliver, durable string) {
	filter = mqttQ2AffineSubjectPrefix + idHash + "." + gen + ".>"
	deliver = mqttQ2AffineDeliverPrefix + idHash + "." + gen
	durable = mqttQ2AffineConsumerDurablePrefix + idHash + "_" + gen
	return filter, deliver, durable
}

// mqttQ2AffMsgId returns the copy's deduplication key. Stable across every retry
// of one claim, and distinct per (group, source message) so two groups moving the
// same $MQTT_msgs message never dedup each other.
func mqttQ2AffMsgId(groupDurable string, sseq uint64) string {
	return mqttQ2AffMsgIdPrefix + groupDurable + "-" + strconv.FormatUint(sseq, 10)
}

// mqttValidateQ2AffineNames checks a persisted affine consumer config against the
// three names the given session generation must own.
//
// A mismatch is a HARD failure, never a "drop it and re-provision": the config we
// cannot account for may belong to a consumer that still holds an in-flight
// PUBLISH, and replacing its generation would strand or delete that message. The
// caller refuses the resume and leaves the state for diagnosis.
func mqttValidateQ2AffineNames(cc *ConsumerConfig, idHash, gen string) error {
	if cc == nil {
		return fmt.Errorf("no consumer config")
	}
	if !mqttValidQ2AffGen(gen) {
		return fmt.Errorf("session generation %q is not a valid identity", gen)
	}
	filter, deliver, durable := mqttQ2AffineNames(idHash, gen)
	if cc.Durable != durable {
		return fmt.Errorf("durable is %q (want %q)", cc.Durable, durable)
	}
	if cc.FilterSubject != filter {
		return fmt.Errorf("filter subject is %q (want %q)", cc.FilterSubject, filter)
	}
	if cc.DeliverSubject != deliver {
		return fmt.Errorf("deliver subject is %q (want %q)", cc.DeliverSubject, deliver)
	}
	return nil
}

// mqttQ2AffineEnvelope is everything the affine copy carries. Every decision it
// records (RETAIN-as-published, subscription identifier, forwarded properties) is
// baked at MOVE time from the subscription that won, because by delivery time the
// subscription may be gone.
type mqttQ2AffineEnvelope struct {
	// natsSubject is the ORIGINAL message's NATS subject with the $MQTT.msgs.
	// prefix stripped. The copy lives on a different subject, so the topic has to
	// travel in a header (the same trick QoS 2 storage uses, mqttNatsHeaderSubject).
	natsSubject string
	msg         []byte
	// props is the raw (decoded) v5 properties block, expiry-adjusted at move
	// time. Re-adjusted again at delivery against the copy's own store timestamp;
	// the two adjustments compose.
	props    []byte
	retained bool
	subID    int

	claimSubject string
	gen          string
	msgID        string
	// expectSeq is the create-only CAS expectation. Always 0 on the real store
	// path; parameterized so the size bound can be taken at full uint64 width.
	expectSeq uint64
}

// mqttComputeQ2AffineSize returns the exact encoded size of env. It must stay
// byte-exact with mqttNewQ2AffineRecord: a disagreement makes headerLen wrong and
// the stored copy corrupt. Both are also the SOLE source of the max_payload bound
// (mqttQ2AffineFormMax), so a header added to one of them and not the others
// would be silently unbudgeted.
func mqttComputeQ2AffineSize(env *mqttQ2AffineEnvelope) int {
	size := len(hdrLine) +
		len(JSExpectedLastSubjSeq) + 1 + len(strconv.FormatUint(env.expectSeq, 10)) + 2 +
		len(JSMsgId) + 1 + len(env.msgID) + 2 +
		len(mqttNatsHeader) + 1 + 1 + 2 + // ":2" + CRLF
		len(mqttNatsHeaderSubject) + 1 + len(env.natsSubject) + 2 +
		len(mqttQ2AffClaimHeader) + 1 + len(env.claimSubject) + 2 +
		len(mqttQ2AffGenHeader) + 1 + len(env.gen) + 2 +
		2 + // end-of-header CRLF
		len(env.msg)
	if env.retained {
		size += len(mqttNatsHeaderRetain) + 1 + 1 + 2
	}
	if len(env.props) > 0 {
		size += len(mqttNatsHeaderProps) + 1 + base64.StdEncoding.EncodedLen(len(env.props)) + 2
	}
	if env.subID > 0 {
		size += len(mqttQ2AffSubIDHeader) + 1 + len(strconv.Itoa(env.subID)) + 2
	}
	return size
}

// mqttNewQ2AffineRecord encodes the affine copy. The headers are written FRESH
// rather than spliced from the source record: the source carries an origin marker
// (meaningless for a shared subscription, which is never No Local), possibly a
// TTL, and possibly a msg-id from an inbound QoS 2 release — all of which must
// not be inherited. Stripping by construction is safer than stripping by
// enumeration.
func mqttNewQ2AffineRecord(env *mqttQ2AffineEnvelope) (natsMsg []byte, headerLen int) {
	buf := bytes.NewBuffer(make([]byte, 0, mqttComputeQ2AffineSize(env)))

	buf.WriteString(hdrLine)

	// Create-only CAS: exactly one materializer may store this claim's copy.
	buf.WriteString(JSExpectedLastSubjSeq)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatUint(env.expectSeq, 10))
	buf.WriteString(_CRLF_)

	// Optimization only (see the file header): narrows the window in which a
	// retry duplicates the copy. Never consulted for ownership.
	buf.WriteString(JSMsgId)
	buf.WriteByte(':')
	buf.WriteString(env.msgID)
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttNatsHeader)
	buf.WriteByte(':')
	buf.WriteByte('2')
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttNatsHeaderSubject)
	buf.WriteByte(':')
	buf.WriteString(env.natsSubject)
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttQ2AffClaimHeader)
	buf.WriteByte(':')
	buf.WriteString(env.claimSubject)
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttQ2AffGenHeader)
	buf.WriteByte(':')
	buf.WriteString(env.gen)
	buf.WriteString(_CRLF_)

	if env.retained {
		buf.WriteString(mqttNatsHeaderRetain)
		buf.WriteByte(':')
		buf.WriteByte('1')
		buf.WriteString(_CRLF_)
	}
	if len(env.props) > 0 {
		buf.WriteString(mqttNatsHeaderProps)
		buf.WriteByte(':')
		buf.WriteString(base64.StdEncoding.EncodeToString(env.props))
		buf.WriteString(_CRLF_)
	}
	if env.subID > 0 {
		buf.WriteString(mqttQ2AffSubIDHeader)
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(env.subID))
		buf.WriteString(_CRLF_)
	}

	buf.WriteString(_CRLF_)
	headerLen = buf.Len()

	buf.Write(env.msg)
	return buf.Bytes(), headerLen
}

// mqttQ2AffMaxIdentityWidths returns the worst-case widths of the identity
// elements the affine envelope adds. Split out so the bound below is assembled
// from named quantities rather than from arithmetic restated at the call site.
//
//   - the group durable is "$MQTT_SHARED_" + a fixed-width hash;
//   - a source sequence is at most math.MaxUint64;
//   - a generation is exactly mqttQ2AffGenLen.
func mqttQ2AffMaxIdentityWidths() (claimSubject, msgID, gen int) {
	maxDur := len(mqttSharedConsumerDurablePrefix) + 16
	maxSeq := mqttQ2AffMaxSeqDigits
	claimSubject = len(mqttQoS2AssignStreamSubjectPrefix) + maxDur + 1 + maxSeq
	msgID = len(mqttQ2AffMsgIdPrefix) + maxDur + 1 + maxSeq
	gen = mqttQ2AffGenLen
	return claimSubject, msgID, gen
}

// mqttQ2AffineFormMax is the upper bound on the affine copy of pp, computed by
// running the REAL encoder's size calculator at maximal identity widths. It is
// what the max_payload preflight in mqttProcessPub adds to the QoS 2 forms.
//
// The failure it prevents is unrecoverable in the same way the terminal QoS 2
// record's is: a PUBLISH admitted at PUBREC time, delivered to a shared group,
// whose copy cannot be stored — the claim never materializes, the group delivery
// is never acked, and AckWait retries it forever.
//
// The v5 Subscription Identifier is budgeted at its maximum because it comes from
// the SUBSCRIBE, which has not happened yet at PUBLISH admission time. Retained
// is budgeted unconditionally for the same reason (Retain As Published is a
// subscription option).
var mqttQ2AffineFormMax = func(pp *mqttPublish, retained bool) int {
	claimW, msgIDW, genW := mqttQ2AffMaxIdentityWidths()
	env := &mqttQ2AffineEnvelope{
		natsSubject:  strings.Repeat("x", len(pp.subject)),
		props:        pp.props,
		retained:     true,
		subID:        mqttMaxVarInt,
		claimSubject: strings.Repeat("x", claimW),
		gen:          strings.Repeat("x", genW),
		msgID:        strings.Repeat("x", msgIDW),
		expectSeq:    math.MaxUint64,
	}
	_ = retained // budgeted unconditionally; see the doc comment.
	// pp.sz is the payload size (pp.msg may not be populated on every path).
	return mqttComputeQ2AffineSize(env) - len(env.msg) + pp.sz
}

// mqttQ2AffineRecordFits reports whether an already-assembled envelope fits the
// account's max payload. Defensive backstop for a message stored by a broker
// that predates the affine bound above: those were admitted without it, so the
// runtime check is the only thing standing between them and an eternal
// materialize-retry wedge. The caller's policy on failure is to NOT claim, ack
// the group delivery, and log an oversize discard — the same disposal the client
// Maximum Packet Size path already uses.
func mqttQ2AffineRecordFits(env *mqttQ2AffineEnvelope, maxPayload int32) bool {
	if maxPayload <= 0 {
		return true
	}
	return mqttComputeQ2AffineSize(env) <= int(maxPayload)
}

// storeQ2AffClaimCAS writes rec to subject expecting expectSeq as that subject's
// current last sequence: 0 requires it to be empty (create-only, the ownership
// election), a non-zero value requires that exact record to still be current
// (every later transition). A concurrent writer that moved the subject wins and
// this write is rejected, which is the state machine's linearization point.
func (jsa *mqttJSA) storeQ2AffClaimCAS(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
	b, _ := json.Marshal(rec)
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(expectSeq, 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	bb.Write(b)
	resp, err := jsa.storeMsg(subject, hdr, bb.Bytes())
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// loadQ2AffClaim returns the current claim on subject and its sequence, or
// (nil, 0, nil) when the subject is empty. A non-nil error means the read was
// INDETERMINATE and must never be guessed into an absence: absence authorizes a
// fresh claim, which is the one decision that can violate affinity.
func (jsa *mqttJSA) loadQ2AffClaim(subject string) (*mqttQ2AffClaim, uint64, error) {
	smsg, err := jsa.loadLastMsgFor(mqttQoS2AssignStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, 0, err
		}
		return nil, 0, nil
	}
	if smsg == nil {
		return nil, 0, nil
	}
	rec := &mqttQ2AffClaim{}
	if err := json.Unmarshal(smsg.Data, rec); err != nil {
		return nil, 0, err
	}
	return rec, smsg.Sequence, nil
}

// storeQ2AffClaimCAS performs a claim transition, honoring an instance-scoped
// test seam. Only claim writes are routed through here so an injected fault never
// perturbs the copy store or unrelated loads.
func (as *mqttAccountSessionManager) storeQ2AffClaimCAS(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error) {
	as.mu.RLock()
	hook := as.testQ2AffClaimStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, rec)
	}
	return as.jsa.storeQ2AffClaimCAS(subject, expectSeq, rec)
}

// loadQ2AffClaim loads a claim, honoring an instance-scoped test seam.
func (as *mqttAccountSessionManager) loadQ2AffClaim(subject string) (*mqttQ2AffClaim, uint64, error) {
	as.mu.RLock()
	hook := as.testQ2AffClaimLoad
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject)
	}
	return as.jsa.loadQ2AffClaim(subject)
}

// storeQ2AffCopy performs the create-only copy store, honoring an
// instance-scoped test seam.
func (as *mqttAccountSessionManager) storeQ2AffCopy(subject string, hdrLen int, msg []byte) (uint64, error) {
	as.mu.RLock()
	hook := as.testQ2AffCopyStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, hdrLen, msg)
	}
	resp, err := as.jsa.storeMsg(subject, hdrLen, msg)
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// mqttQ2AffCopyState is the authoritative answer to "does this claim's copy
// exist?". Absent is a DECISION, not a default: only a confirmed no-message
// response produces it.
type mqttQ2AffCopyState int

const (
	mqttQ2AffCopyPresent mqttQ2AffCopyState = iota
	mqttQ2AffCopyAbsent
	mqttQ2AffCopyUnknown // load failed; the caller must fail closed
)

// loadQ2AffCopy answers whether the claim-specific copy subject holds a message,
// and at which sequence. Because the subject names exactly one claim, this is a
// direct lookup rather than a scan with header matching — which is what makes the
// store-succeeded/response-lost window resolvable without a dedup window.
func (as *mqttAccountSessionManager) loadQ2AffCopy(subject string) (mqttQ2AffCopyState, *StoredMsg) {
	as.mu.RLock()
	hook := as.testQ2AffCopyLoad
	as.mu.RUnlock()
	var (
		smsg *StoredMsg
		err  error
	)
	if hook != nil {
		smsg, err = hook(subject)
	} else {
		smsg, err = as.jsa.loadLastMsgFor(mqttOutStreamName, subject)
	}
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return mqttQ2AffCopyUnknown, nil
		}
		return mqttQ2AffCopyAbsent, nil
	}
	if smsg == nil {
		return mqttQ2AffCopyAbsent, nil
	}
	return mqttQ2AffCopyPresent, smsg
}

// validateQoS2AssignStream verifies a $MQTT_qos2assign stream can actually
// arbitrate assignments. Like validateQoS2InStream, the rule is to reject every
// setting that changes the write, CAS, subject, acknowledgement, retention or
// deletion semantics this owner depends on, rather than reason about which
// combinations happen to survive.
//
// The failure these checks prevent is the central one: a claim that disappears,
// or that a second writer can append beside, reopens the ownership of a started
// delivery. Retention is as load-bearing here as the per-subject singleton —
// claims must persist with ZERO consumer interest, indefinitely.
//
// Scope note: derived from what THIS owner does (create-only CAS, CAS at an exact
// sequence, delete of an exact sequence, per-subject singleton, permanent
// retention). DenyDelete is fatal because the reap deletes; DenyPurge is not
// listed because nothing here purges by subject.
func validateQoS2AssignStream(si *StreamInfo, wantReplicas int) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config

	wantSubj := mqttQoS2AssignStreamSubjectPrefix + ">"
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != wantSubj {
		return fmt.Errorf("subjects %v (want [%q])", cfg.Subjects, wantSubj)
	}
	if cfg.Storage != FileStorage {
		return fmt.Errorf("storage is %v (want file)", cfg.Storage)
	}
	if cfg.Retention != LimitsPolicy {
		// Interest or work-queue retention would drop a claim the moment no
		// consumer wanted it — and nothing ever consumes claims; they are read by
		// direct subject lookup. That is the arbitration record vanishing.
		return fmt.Errorf("retention is %v (want limits)", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		// DiscardNew would make every transition CAS a rejected write instead of a
		// replacement, so a claim could never advance past `claimed`.
		return fmt.Errorf("discard is %v (want old)", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		// With more than one record per subject a transition APPENDS instead of
		// replacing: loadLastMsgFor returns the new record while the old one
		// survives underneath, and deleting an exact sequence then re-exposes a
		// stale claim state.
		return fmt.Errorf("max_msgs_per_subject is %d (want 1)", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict live assignment state",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if cfg.Mirror != nil || len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has a mirror or sources (want none): writes would land elsewhere")
	}
	if wantReplicas > 0 && cfg.Replicas != wantReplicas {
		return fmt.Errorf("replicas is %d (want %d)", cfg.Replicas, wantReplicas)
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject a claim transition independently of any broker preflight", cfg.MaxMsgSize)
	}
	if cfg.NoAck {
		return fmt.Errorf("no_ack is set: it suppresses the acknowledgements every CAS here depends on")
	}
	if cfg.DenyDelete {
		return fmt.Errorf("deny_delete is set: it blocks the claim reap")
	}
	if cfg.Sealed {
		return fmt.Errorf("stream is sealed: it blocks all writes")
	}
	if cfg.SubjectTransform != nil {
		return fmt.Errorf("a subject transform is configured: it rewrites the per-claim subject that CAS identity and loadLastMsgFor both depend on")
	}
	if cfg.RePublish != nil {
		return fmt.Errorf("republish is configured: it would mirror internal assignment state onto a user-visible subject")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages on claim subjects would be parsed as records", cfg.SubjectDeleteMarkerTTL)
	}
	if cfg.AllowMsgCounter {
		return fmt.Errorf("allow_msg_counter is set: counter streams do not support the store semantics used here")
	}
	if cfg.PersistMode != DefaultPersistMode {
		return fmt.Errorf("persist_mode is %v (want the default): its durability semantics are unproven for this protocol", cfg.PersistMode)
	}
	return nil
}

// validateOutStream verifies $MQTT_out can carry the affine copies. It is a
// DIFFERENT list from validateQoS2AssignStream's, derived from what this owner
// does to $MQTT_out: append-only stores (one of them a create-only CAS), consumer
// delivery, and interest-based drop when a session's durable goes away. It never
// deletes or purges messages there, so DenyDelete/DenyPurge are not prerequisites.
//
// InterestPolicy is load-bearing in BOTH directions: it is how a discarded
// session's undelivered copies disappear when its durable is deleted, and it is
// why a copy must never be stored before its consumer exists — a store with no
// interest returns success and keeps nothing.
func validateOutStream(si *StreamInfo, wantReplicas int) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config

	wantSubj := mqttOutSubjectPrefix + ">"
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != wantSubj {
		return fmt.Errorf("subjects %v (want [%q])", cfg.Subjects, wantSubj)
	}
	if cfg.Storage != FileStorage {
		return fmt.Errorf("storage is %v (want file)", cfg.Storage)
	}
	if cfg.Retention != InterestPolicy {
		return fmt.Errorf("retention is %v (want interest): a discarded session's undelivered copies would not be dropped with its consumer", cfg.Retention)
	}
	if cfg.MaxMsgsPer > 0 {
		// Not part of this owner's semantics, and actively hostile to it: a
		// per-subject cap interacts with the create-only CAS on a copy subject and
		// could evict a pending copy that a claim still points at.
		return fmt.Errorf("max_msgs_per_subject is %d (want unlimited)", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict pending PUBREL or affine QoS 2 state",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if cfg.Mirror != nil || len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has a mirror or sources (want none): writes would land elsewhere")
	}
	if wantReplicas > 0 && cfg.Replicas != wantReplicas {
		return fmt.Errorf("replicas is %d (want %d)", cfg.Replicas, wantReplicas)
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject an affine copy the broker max-payload preflight already admitted", cfg.MaxMsgSize)
	}
	if cfg.NoAck {
		return fmt.Errorf("no_ack is set: it suppresses the acknowledgements the copy store depends on")
	}
	if cfg.Sealed {
		return fmt.Errorf("stream is sealed: it blocks all writes")
	}
	if cfg.SubjectTransform != nil {
		return fmt.Errorf("a subject transform is configured: it rewrites the generation-scoped subject that consumer isolation and the create-only CAS depend on")
	}
	if cfg.RePublish != nil {
		return fmt.Errorf("republish is configured: it would mirror in-flight QoS 2 payloads onto a user-visible subject")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages would be delivered to a session's affine consumer as if they were copies", cfg.SubjectDeleteMarkerTTL)
	}
	if cfg.AllowMsgCounter {
		return fmt.Errorf("allow_msg_counter is set: counter streams do not support the store semantics used here")
	}
	if cfg.PersistMode != DefaultPersistMode {
		return fmt.Errorf("persist_mode is %v (want the default): its durability semantics are unproven for this protocol", cfg.PersistMode)
	}
	if cfg.ConsumerLimits.InactiveThreshold != 0 {
		// A stream-forced inactivity threshold would give every session's affine
		// durable a delete timer, reaping resumable-session state while the client
		// is offline — exactly what this feature exists to preserve.
		return fmt.Errorf("stream imposes a consumer inactivity limit (%v): it would reap a disconnected session's affine QoS 2 consumer", cfg.ConsumerLimits.InactiveThreshold)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Session-side provisioning, and the move-operation lifecycle fence
// ---------------------------------------------------------------------------

// q2AffCondLocked returns the fence's condition variable, creating it on first
// use. sess.mu must be held. Lazily created because mqttSession is also built as
// a struct literal (transient sessions, tests), where a constructor-installed
// cond would be nil anyway.
func (sess *mqttSession) q2AffCondLocked() *sync.Cond {
	if sess.q2AffCond == nil {
		sess.q2AffCond = sync.NewCond(&sess.mu)
	}
	return sess.q2AffCond
}

// beginQ2AffOp admits one move operation and returns the generation it must run
// under. It is the ONLY entry point to the move: it enforces the readiness gate
// and the closing fence together, under one acquisition of sess.mu, and pins the
// generation for the operation's whole lifetime.
//
// ok=false means "prove nothing, write nothing, ack nothing" — the group
// delivery is left for AckWait to re-offer.
func (sess *mqttSession) beginQ2AffOp() (gen string, ok bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.q2AffClosing || !sess.q2AffReady || sess.q2AffGen == _EMPTY_ || sess.q2AffConsumer == nil {
		return _EMPTY_, false
	}
	sess.q2AffInFlight++
	return sess.q2AffGen, true
}

// endQ2AffOp releases a move operation admitted by beginQ2AffOp.
func (sess *mqttSession) endQ2AffOp() {
	sess.mu.Lock()
	if sess.q2AffInFlight > 0 {
		sess.q2AffInFlight--
	}
	if sess.q2AffInFlight == 0 && sess.q2AffClosing {
		sess.q2AffCondLocked().Broadcast()
	}
	sess.mu.Unlock()
}

// closeQ2AffAndDrain closes the fence and waits for every admitted move to
// finish, returning the generation and durable the session owned.
//
// This is what stops a delayed claim CAS from landing AFTER clear() has scanned
// and finished. Such a claim would name a generation that no longer exists and
// would sit at `claimed` forever, and contenders honoring it would stall the
// group delivery indefinitely — a live message parked on a dead session. The
// contender-side "winner's session record is gone ⇒ discard" rule is the
// defense in depth behind this, not a substitute for it.
//
// sess.mu not held on entry.
func (sess *mqttSession) closeQ2AffAndDrain() (gen, durable string) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.q2AffClosing = true
	sess.q2AffReady = false
	for sess.q2AffInFlight > 0 {
		sess.q2AffCondLocked().Wait()
	}
	gen = sess.q2AffGen
	if sess.q2AffConsumer != nil {
		durable = sess.q2AffConsumer.Durable
	} else if gen != _EMPTY_ {
		_, _, durable = mqttQ2AffineNames(sess.idHash, gen)
	}
	return gen, durable
}

// ensureQ2AffineConsumerSubscription provisions (or verifies, on resume) this
// session's affine QoS 2 delivery path and only then opens the readiness gate.
//
// The order is forced by two properties of $MQTT_out:
//
//   - it is interest-retained, so a copy stored before the consumer exists is
//     DISCARDED by a store that returns success; and
//   - a consumer whose name is not in the persisted session record cannot be
//     found again after a restart, so its copies would be undeliverable.
//
// Hence: subscribe, create the consumer, persist the record, and only then set
// q2AffReady. Any failure rolls the whole thing back rather than leaving a
// half-provisioned path a move could latch onto.
//
// On resume the persisted config proves only that a consumer once existed, so
// the durable is re-verified by create-or-update and the three names are checked
// against the generation. A mismatch FAILS CLOSED (the caller refuses the
// subscription) instead of re-provisioning: the config we cannot account for may
// belong to a consumer that still holds an in-flight PUBLISH.
//
// tok identifies the caller's open record transaction. This runs both inside a
// SUBSCRIBE packet (whose token it is handed) and standalone from
// resendPendingRedelivery (which opens its own first), so ownership is passed
// explicitly rather than inferred — see sess_recordtx.go.
//
// Runs from the client's readLoop. sess.mu not held on entry.
func (sess *mqttSession) ensureQ2AffineConsumerSubscription(c *client, tok recordTx) error {
	sess.mu.Lock()
	if sess.q2AffClosing {
		sess.mu.Unlock()
		return fmt.Errorf("session is terminating")
	}
	idHash, id := sess.idHash, sess.id
	gen := sess.q2AffGen
	existing := sess.q2AffConsumer
	subscribed := sess.q2AffSubscribed
	ready := sess.q2AffReady
	tmaxack := sess.tmaxack
	sess.mu.Unlock()

	if ready && subscribed && existing != nil {
		return nil
	}

	// A persisted consumer with no generation is unaccountable state: we can
	// neither derive the names it owns nor safely replace it. Refuse.
	if existing != nil && gen == _EMPTY_ {
		return fmt.Errorf("session %q has a persisted affine QoS 2 consumer %q with no generation; refusing to resume it", id, existing.Durable)
	}
	minted := gen == _EMPTY_
	if minted {
		gen = mqttNewQ2AffGen()
	} else if existing != nil {
		if err := mqttValidateQ2AffineNames(existing, idHash, gen); err != nil {
			return fmt.Errorf("session %q has an unaccountable affine QoS 2 consumer: %w", id, err)
		}
	}
	filter, deliver, durable := mqttQ2AffineNames(idHash, gen)

	opts := c.srv.getOpts()
	ackWait := opts.MQTT.AckWait
	if ackWait == 0 {
		ackWait = mqttDefaultAckWait
	}
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	// Charge the session's MaxAckPending budget only when this call is INSTALLING
	// a consumer the session did not already have. A same-process resume runs this
	// function again (the delivery subscription and the readiness gate belong to
	// the connection, not the session), and charging there would bill the same
	// consumer once per reconnect until shared QoS 2 subscribes started failing
	// against mqttMaxAckTotalLimit. This is the same rule the PUBREL consumer and
	// the per-subscription consumers follow: a consumer restored from the record is
	// not charged either.
	charge := existing == nil
	if charge {
		if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			return fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v", after, mqttMaxAckTotalLimit)
		}
	}

	// Track exactly what this call did, so a later failure undoes exactly that
	// and nothing else (a resume must not tear down state it merely verified).
	var (
		didSubscribe bool
		didCharge    bool
	)
	rollback := func() {
		if didSubscribe {
			if uerr := c.processUnsub([]byte(deliver)); uerr != nil {
				c.Warnf("Unable to remove the affine QoS 2 delivery subscription on %q: %v", deliver, uerr)
			}
		}
		sess.mu.Lock()
		sess.q2AffReady = false
		if didSubscribe {
			sess.q2AffSubscribed = false
		}
		if didCharge {
			sess.tmaxack -= maxAckPending
			if sess.tmaxack < 0 {
				sess.tmaxack = 0
			}
		}
		if minted {
			// Nothing durable ever referenced this generation, so dropping it leaves
			// no orphan; keeping it would pin the session to a half-built identity.
			sess.q2AffGen = _EMPTY_
			sess.q2AffConsumer = nil
			sess.q2AffDeliverySubject = _EMPTY_
			sess.q2AffDeliverySubjectB = nil
		}
		sess.mu.Unlock()
		if minted {
			if _, err := sess.jsa.deleteConsumer(mqttOutStreamName, durable, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
				c.Warnf("Unable to roll back affine QoS 2 consumer %q: %v", durable, err)
			}
		}
	}

	// (1) Install the identity with the gate still CLOSED.
	sess.mu.Lock()
	sess.q2AffGen = gen
	sess.q2AffDeliverySubject = deliver
	sess.q2AffDeliverySubjectB = []byte(deliver)
	sess.q2AffReady = false
	sess.mu.Unlock()

	// (2) Subscribe before the consumer exists so no delivery is missed. The
	// affine subscription needs a real mqttSub (its callback delivers PUBLISH
	// packets), which processQOS12Sub installs; the pubRel path's bare processSub
	// would leave sub.mqtt nil.
	if !subscribed {
		sess.mu.Lock()
		_, err := sess.processQOS12Sub(c, []byte(deliver), []byte(deliver), nil,
			false, 2, false, false, false, 0, durable, mqttDeliverQ2AffineCb)
		sess.mu.Unlock()
		if err != nil {
			c.Errorf("Unable to create subscription for the affine QoS 2 consumer on %q: %v", deliver, err)
			rollback()
			return err
		}
		didSubscribe = true
		sess.mu.Lock()
		sess.q2AffSubscribed = true
		sess.mu.Unlock()
	}

	// (3) Create-or-update the durable. This both creates it and VERIFIES a
	// pre-existing one against the configuration we expect, which is what a resume
	// needs: a persisted config is not evidence that the consumer still exists or
	// still looks like this.
	ccr := &CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			DeliverSubject: deliver,
			Durable:        durable,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  filter,
			AckWait:        ackWait,
			MaxAckPending:  maxAckPending,
			MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			// InactiveThreshold is deliberately left at zero, unconditionally and
			// independently of MQTT.ConsumerInactiveThreshold: an inactivity delete
			// would reap this durable while the client is offline, and with it the
			// interest that keeps its pending copies alive. That is precisely the
			// resumable-session state this feature exists to preserve.
		},
	}
	resp, err := sess.jsa.createDurableConsumer(ccr)
	if err != nil {
		c.Errorf("Unable to add the affine QoS 2 JetStream consumer for client %q: %v", id, err)
		rollback()
		return err
	}
	if resp != nil && resp.ConsumerInfo != nil {
		if verr := mqttValidateQ2AffineNames(resp.Config, idHash, gen); verr != nil {
			c.Errorf("The affine QoS 2 consumer for client %q does not match its expected identity: %v", id, verr)
			rollback()
			return verr
		}
	}
	sess.mu.Lock()
	sess.q2AffConsumer = &ccr.Config
	if charge {
		sess.tmaxack += maxAckPending
		didCharge = true
	}
	sess.mu.Unlock()

	// (4) Persist the record naming it, and only then open the gate. A consumer
	// that no record names is unfindable after a restart, so its copies would be
	// stored into a subject nothing will ever deliver from.
	if existing == nil {
		outcome, err := sess.saveClassified(tok)
		if err != nil {
			c.Errorf("Unable to persist the affine QoS 2 consumer for client %q: %v", id, err)
			// Undo only when the write DEFINITELY did not land. An ambiguous one may
			// have stored a record that names this consumer, and detaching it in
			// memory would leave that record pointing at something the session no
			// longer knows about. Which way memory reconciles is the readback's to
			// decide, not the failing writer's. See sess_uncertain.go.
			switch outcome {
			case saveDefinitelyFailed:
				rollback()
			case saveMaybeLanded:
				// The record may name the consumer, so keep the view — the reconciliation
				// depends on an outcome nobody knows yet — and let the readback detach it
				// if it proves the write never landed: a record that does not name this
				// consumer must not be paired with a session that does.
				sess.retainOnUncertain(func(recordTx) { rollback() }, nil)
			}
			return err
		}
	}
	sess.mu.Lock()
	sess.q2AffReady = true
	sess.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// Claim transitions
// ---------------------------------------------------------------------------

// errQ2AffUnresolved marks an outcome that could not be PROVEN either way. It is
// never a reason to guess: a caller that sees it writes nothing, acks nothing and
// lets AckWait re-offer the delivery.
var errQ2AffUnresolved = errors.New("affine QoS 2 state could not be resolved")

// advanceQ2AffClaim applies mutate to the claim on subject and commits it with a
// CAS at the sequence the claim was loaded at, retrying against the record's
// latest state. Facts already proven by the loaded record are preserved
// (mergeProven), so a transition can only ever ADD to what is known.
//
// wantWinner/wantGen, when set, assert whose claim this must be; a mismatch
// returns the loaded claim with errQ2AffUnresolved and mutates nothing, in the
// same spirit as finalizeQoS2Move's Superseded. (The claim subject is keyed by
// group durable and SOURCE SEQUENCE, and a stream sequence is never reused, so
// the subject already identifies one source message forever; the identity check
// guards against acting on a winner that is not the one the caller reasoned about.)
//
// A missing claim returns (nil, 0, nil): there is nothing to advance, and absence
// is never manufactured into a new claim here.
func (as *mqttAccountSessionManager) advanceQ2AffClaim(subject, wantWinner, wantGen string,
	mutate func(*mqttQ2AffClaim)) (*mqttQ2AffClaim, uint64, error) {

	var lastErr error
	for attempt := 0; attempt < mqttQ2AffClaimAttempts; attempt++ {
		cur, seq, err := as.loadQ2AffClaim(subject)
		if err != nil {
			lastErr = err
			continue
		}
		if cur == nil {
			return nil, 0, nil
		}
		if wantWinner != _EMPTY_ && !cur.ownedBy(wantWinner, wantGen) {
			return cur, seq, errQ2AffUnresolved
		}
		next := *cur
		mutate(&next)
		next.mergeProven(cur)
		if next == *cur {
			return cur, seq, nil // already true; no write needed
		}
		newSeq, serr := as.storeQ2AffClaimCAS(subject, seq, &next)
		if serr == nil {
			return &next, newSeq, nil
		}
		lastErr = serr
		if !isQoS2CASRejection(serr) {
			// Not a lost race: a real backend failure. Retrying the same CAS would
			// only repeat it, so re-read and try once more at the fresh sequence.
			continue
		}
	}
	if lastErr == nil {
		lastErr = errQ2AffUnresolved
	}
	return nil, 0, lastErr
}

// reapQ2AffClaim deletes the claim on subject if and only if BOTH sides are
// proven complete. It re-reads first: the caller's view is always older than the
// record, and this is the one operation that can, if wrong, reopen the ownership
// of a started delivery.
//
// EVERY writer that records one of the two proofs must call this afterwards. The
// two proofs are produced by different code paths (the source ack by the move,
// the affine completion by the delivery) with no ordering between them, and each
// reap re-reads, so whichever writer commits second necessarily observes the
// first one's fact and performs the deletion. Reaping from only one of them
// leaves a fully terminal claim behind whenever the other happens to finish last.
//
// It completes ONE proof on its own: a source side that can no longer be
// re-offered at all (see q2AffSourceUnofferable). That proves ARBITRATION is
// over, which is what SourceAcked means; it says nothing about the affine side,
// which is why it is not sufficient by itself.
//
// It deliberately completes NOTHING on the affine side. "Materialized with no copy
// left" is ambiguous — the copy was either delivered and acked, or dropped when
// its consumer was destroyed (interest retention) — and this function runs on
// callers that cannot tell the two apart, most importantly a CONTENDER on another
// broker handling a source redelivery. Concluding completion there would delete a
// lost message's only durable record, which is the laundering the Lost state
// exists to prevent. The one caller that CAN tell is recoverQ2AffClaims, on the
// owner's own resume, where the consumer's absence has actually been observed.
func (as *mqttAccountSessionManager) reapQ2AffClaim(s *Server, subject string) {
	claim, seq, err := as.loadQ2AffClaim(subject)
	if err != nil || claim == nil {
		return
	}

	copyState, _ := as.loadQ2AffCopy(claim.CopySubject)
	if copyState == mqttQ2AffCopyUnknown {
		return // fail closed: an unreadable copy is not an absent copy
	}
	copyAbsent := copyState == mqttQ2AffCopyAbsent

	if !claim.SourceAcked && as.q2AffSourceUnofferable(claim) {
		claim, seq, err = as.advanceQ2AffClaim(subject, _EMPTY_, _EMPTY_, func(c *mqttQ2AffClaim) { c.SourceAcked = true })
		if err != nil || claim == nil {
			return
		}
	}
	if !claim.reapable(copyAbsent) {
		return
	}
	// Delete the EXACT sequence loaded (qos2_move.go invariant 2): deleting a
	// sequence cannot reach a record written after it.
	if derr := as.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); derr != nil &&
		isErrorOtherThan(derr, JSSequenceNotFoundErrF) &&
		!isJSErrCausedBy(derr, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
		s.Warnf("MQTT: unable to reap the completed shared QoS 2 assignment %q: %v", subject, derr)
	}
}

// q2AffSourceUnofferable reports whether the group delivery behind this claim can
// be PROVEN never to be re-offered — which is exactly what SourceAcked asserts,
// and the only thing that has to be true for reaping the claim to be unable to
// let a redelivery elect a fresh winner.
//
// Two independent proofs, because the ack can be committed while the record of it
// is not. The group consumer being gone covers the whole group at once; the
// SOURCE MESSAGE being gone is the per-message proof, and it is the one that
// matters after an ordinary crash — the ack commits (which removes the message
// from the interest-retained stream) and the CAS that records SourceAcked then
// fails. Without the second proof such a claim is retained forever whenever the
// group consumer happens to survive, which is the common case.
func (as *mqttAccountSessionManager) q2AffSourceUnofferable(claim *mqttQ2AffClaim) bool {
	return as.q2AffGroupConsumerGone(claim.GroupDurable) || as.q2AffSourceGone(claim)
}

// q2AffGroupConsumerGone reports whether the group durable is AUTHORITATIVELY
// absent. Any other error answers false: "I could not tell" must never become
// "it is gone".
func (as *mqttAccountSessionManager) q2AffGroupConsumerGone(groupDurable string) bool {
	if groupDurable == _EMPTY_ {
		return false
	}
	_, err := as.jsa.lookupConsumer(mqttStreamName, groupDurable)
	return err != nil && !isErrorOtherThan(err, JSConsumerNotFoundErr)
}

// confirmSourceAckAndRecord acks the group delivery and, once JetStream has
// CONFIRMED the ack, records that fact on the claim.
//
// The confirmation is not decoration. sendAck is a queue push: if that ack is
// lost and the claim has already been reaped on the strength of it, the source
// redelivers with no claim in the way, a fresh contender claims it, and the
// started delivery is handed to a second client — the exact violation this file
// exists to remove.
func (as *mqttAccountSessionManager) confirmSourceAckAndRecord(claimSubject, reply string) error {
	if err := as.jsa.confirmAck(reply); err != nil {
		return err
	}
	_, _, err := as.advanceQ2AffClaim(claimSubject, _EMPTY_, _EMPTY_, func(c *mqttQ2AffClaim) { c.SourceAcked = true })
	return err
}

// ---------------------------------------------------------------------------
// The move: from a group delivery to a session-affine copy
// ---------------------------------------------------------------------------

// mqttQ2AffMoveInput is everything one group delivery contributes to a move.
type mqttQ2AffMoveInput struct {
	groupDurable string // the shared group's JS durable: half of the claim key
	natsSubject  string // the source subject with the $MQTT.msgs. prefix stripped
	reply        string // the group delivery's JetStream ack subject
	props        []byte
	msg          []byte
	retained     bool // Retain As Published applies to THIS member's subscription
	subID        int  // this member's v5 Subscription Identifier
	maxPayload   int32
}

// moveStartedSharedQoS2 runs on EVERY group delivery of an effective-QoS-2
// message to a shared subscription member — unconditionally, and instead of
// trackPublish. There is deliberately no "only if not already started" test:
// `started` is a property of the local in-memory tracking maps, which say
// nothing after a restart, a failover, or on another broker, and consulting them
// would make the decision non-durable exactly where it has to be durable.
//
// The result is never an error: "a foreign session owns this, so I acked and
// walked away" is a correct outcome, and so is "I could prove nothing, so I left
// the delivery for AckWait". Only the second writes nothing at all.
//
// Runs from a JetStream delivery callback. sess.mu not held on entry.
func (sess *mqttSession) moveStartedSharedQoS2(s *Server, as *mqttAccountSessionManager, in *mqttQ2AffMoveInput) mqttQ2AffMoveResult {
	if as == nil {
		// No account manager means no claim stream to arbitrate with. Ack nothing:
		// releasing the delivery here is what redistributes it.
		return mqttQ2AffMoveDeferred
	}
	sseq, _, _, ts, _ := ackReplyInfo(in.reply)
	claimSubject := mqttQ2AffClaimSubject(in.groupDurable, sseq)

	res := mqttQ2AffMoveDeferred
	defer func() {
		as.mu.RLock()
		done := as.testQ2AffMoveDone
		as.mu.RUnlock()
		if done != nil {
			done(claimSubject, res)
		}
	}()

	// The readiness gate and the closing fence, together, under one lock: from
	// here until endQ2AffOp this session's generation cannot be torn down out from
	// under the JetStream round trips below.
	gen, ok := sess.beginQ2AffOp()
	if !ok {
		return res
	}
	defer sess.endQ2AffOp()

	idHash := sess.idHash
	claimHash := mqttQ2AffClaimHash(in.groupDurable, sseq)
	copySubject := mqttQ2AffCopySubject(idHash, gen, claimHash)
	_, _, affDurable := mqttQ2AffineNames(idHash, gen)

	// A message whose Message Expiry Interval has elapsed was never STARTED to
	// anyone, so no assignment exists to protect: drop it exactly as the ordinary
	// delivery path does. Spec5 [3.3.2.3.3].
	props := in.props
	if len(props) > 0 {
		var expired bool
		if props, expired = mqttForwardExpiry(props, ts); expired {
			sess.jsa.sendAck(in.reply)
			res = mqttQ2AffMoveDropped
			return res
		}
	}

	env := &mqttQ2AffineEnvelope{
		natsSubject:  in.natsSubject,
		msg:          in.msg,
		props:        props,
		retained:     in.retained,
		subID:        in.subID,
		claimSubject: claimSubject,
		gen:          gen,
		msgID:        mqttQ2AffMsgId(in.groupDurable, sseq),
	}
	// Backstop for a message stored by a broker that predates the affine
	// admission bound: it was admitted without it, and a copy that can never be
	// stored would wedge the group durable in eternal retry. Drop it with a
	// logged error instead — the same disposal an undeliverable oversize PUBLISH
	// already gets. New PUBLISHes are bounded at admission (mqttMaxQoS2FormSize).
	if !mqttQ2AffineRecordFits(env, in.maxPayload) {
		s.Errorf("MQTT: discarding a shared QoS 2 message on %q: its session-affine form (%d bytes) exceeds the account max payload (%d); it was accepted by a broker without the affine admission bound",
			in.natsSubject, mqttComputeQ2AffineSize(env), in.maxPayload)
		sess.jsa.sendAck(in.reply)
		res = mqttQ2AffMoveDropped
		return res
	}

	// 1. Elect. A create-only CAS: exactly one session can win, forever.
	claim := &mqttQ2AffClaim{
		Winner:       idHash,
		Gen:          gen,
		GroupDurable: in.groupDurable,
		SourceSeq:    sseq,
		ClaimHash:    claimHash,
		CopySubject:  copySubject,
		AffDurable:   affDurable,
		SubID:        in.subID,
		Retained:     in.retained,
		Phase:        mqttQ2AffClaimed,
	}
	claimSeq, err := as.storeQ2AffClaimCAS(claimSubject, 0, claim)
	if err == nil {
		if sess.completeQ2AffOwnClaim(s, as, claim, claimSubject, claimSeq, env, in.reply) {
			res = mqttQ2AffMoveOwned
		}
		return res
	}
	if !isQoS2CASRejection(err) {
		return res // a real backend failure: prove nothing, ack nothing
	}

	// 2. Someone claimed it. Read the durable truth and honor it.
	cur, curSeq, lerr := as.loadQ2AffClaim(claimSubject)
	if lerr != nil || cur == nil {
		// Indeterminate, or the claim vanished between the rejection and this read.
		// Either way nothing is proven; AckWait re-offers.
		return res
	}

	switch {
	case cur.ownedBy(idHash, gen):
		// Our own claim, from an earlier attempt. Resume it idempotently.
		if sess.completeQ2AffOwnClaim(s, as, cur, claimSubject, curSeq, env, in.reply) {
			res = mqttQ2AffMoveOwned
		}

	case cur.terminal():
		// Discarded with its session, or already completed. Either way no delivery
		// can come of it — but the source ack still has to be PROVEN before the
		// claim may be reaped, or a lost ack would reopen the assignment through
		// the discard side-door.
		if cur.SourceAcked {
			sess.jsa.sendAck(in.reply)
		} else if aerr := as.confirmSourceAckAndRecord(claimSubject, in.reply); aerr != nil {
			return res
		}
		as.reapQ2AffClaim(s, claimSubject)
		res = mqttQ2AffMoveTerminal

	case cur.Phase == mqttQ2AffMaterialized:
		// A foreign session owns it and its copy exists: the delivery is safely on
		// that session's own durable, so release the group delivery.
		if aerr := as.confirmSourceAckAndRecord(claimSubject, in.reply); aerr != nil {
			return res
		}
		as.reapQ2AffClaim(s, claimSubject)
		res = mqttQ2AffMoveForeign

	default:
		// A foreign session owns it but no copy exists yet. Waiting for that
		// session to come back is not bounded by anything: a perfectly valid
		// persistent winner may simply never reconnect, and the group delivery
		// would stall forever. Materialize the copy ON ITS BEHALF, onto ITS
		// durable, and then release the group delivery.
		if as.materializeQ2AffOnBehalf(s, cur, claimSubject, curSeq, env, in) {
			res = mqttQ2AffMoveOnBehalf
		}
	}
	return res
}

// completeQ2AffOwnClaim finishes a move for a claim this session owns: store the
// copy, commit the materialized phase, then release the group delivery. Each step
// is idempotent, so a retry after any partial failure converges.
//
// Returns false when nothing could be proven — the caller then acks nothing.
func (sess *mqttSession) completeQ2AffOwnClaim(s *Server, as *mqttAccountSessionManager,
	claim *mqttQ2AffClaim, claimSubject string, claimSeq uint64, env *mqttQ2AffineEnvelope, reply string) bool {

	if !as.materializeQ2AffCopy(s, claim, claimSubject, claimSeq, env) {
		return false
	}
	if claim.SourceAcked {
		sess.jsa.sendAck(reply)
		return true
	}
	if err := as.confirmSourceAckAndRecord(claimSubject, reply); err != nil {
		// The copy exists and the claim is materialized, so the message is already
		// safe on our own durable. The unacked group delivery just redelivers, and
		// the next attempt takes the "already materialized" path.
		s.Warnf("MQTT: shared QoS 2 assignment %q is materialized but its source ack is unconfirmed: %v", claimSubject, err)
		return true
	}
	// Reap from HERE too, not only from the affine side. The materialized claim is
	// deliverable the instant it commits, so the client can PUBREC — and the
	// affine side can record AffineDone and try to reap — before this source ack
	// lands. That reap correctly declines (the source side was unproven), and
	// without this one nothing would ever look again. See reapQ2AffClaim.
	as.reapQ2AffClaim(s, claimSubject)
	return true
}

// materializeQ2AffCopy stores the affine copy (create-only CAS on the
// claim-specific subject) and commits the claim's claimed->materialized
// transition. Returns true only when the copy is PROVEN to exist and the claim
// PROVEN to say so.
//
// The copy is stored before the phase advances, never after: the phase is what
// authorizes delivery, and a materialized claim with no copy would let the
// delivery gate open on nothing.
func (as *mqttAccountSessionManager) materializeQ2AffCopy(s *Server,
	claim *mqttQ2AffClaim, claimSubject string, claimSeq uint64, env *mqttQ2AffineEnvelope) bool {

	if claim.Discarded {
		return false // the winner's session ended; nothing may be materialized for it
	}
	if claim.Phase != mqttQ2AffMaterialized {
		state, _ := as.loadQ2AffCopy(claim.CopySubject)
		switch state {
		case mqttQ2AffCopyUnknown:
			return false // fail closed: unreadable is not absent
		case mqttQ2AffCopyAbsent:
			// Absent under a not-yet-materialized claim proves the copy was never
			// stored: the delivery gate refuses to publish (and so to ack away) a
			// copy whose claim is not materialized, so it cannot have been consumed.
			msg, hdrLen := mqttNewQ2AffineRecord(env)
			if _, err := as.storeQ2AffCopy(claim.CopySubject, hdrLen, msg); err != nil {
				if !isQoS2CASRejection(err) {
					return false
				}
				// A rejected create-only CAS means "the subject is not empty", which
				// is NOT the same as "a copy is there now": it can have been consumed
				// and acked away in between. Re-read authoritatively.
				if st, _ := as.loadQ2AffCopy(claim.CopySubject); st != mqttQ2AffCopyPresent {
					return false
				}
			}
		}
	}

	as.mu.RLock()
	pre := as.testQ2AffPreMaterialize
	as.mu.RUnlock()
	if pre != nil {
		if err := pre(claimSubject, claimSeq); err != nil {
			return false
		}
	}

	updated, _, err := as.advanceQ2AffClaim(claimSubject, claim.Winner, claim.Gen,
		func(c *mqttQ2AffClaim) { c.Phase = mqttQ2AffMaterialized })
	if err != nil || updated == nil {
		return false
	}
	// A claim that turned discarded while we were storing keeps its copy dropped
	// by the delivery gate; the copy itself dies with the session's consumer.
	return updated.Phase == mqttQ2AffMaterialized
}

// materializeQ2AffOnBehalf writes the copy for a foreign winner that is not here
// to write it itself, and then releases the group delivery.
//
// The verification order is load-bearing. The winner's SESSION RECORD is checked
// FIRST, and for the exact generation: an orphaned consumer can outlive the
// record that named it, so "the consumer exists" would happily materialize onto a
// durable belonging to a session that is gone. Only once the record proves the
// generation is live is the consumer itself verified — without it the copy would
// be stored with no interest and silently discarded.
//
// Anything indeterminate leaves the claim untouched and acks nothing.
func (as *mqttAccountSessionManager) materializeQ2AffOnBehalf(s *Server, claim *mqttQ2AffClaim,
	claimSubject string, claimSeq uint64, env *mqttQ2AffineEnvelope, in *mqttQ2AffMoveInput) bool {

	// (1) Is the winning generation still a live session?
	ps, _, err := as.loadFenceSession(claim.Winner)
	if err != nil {
		return false // indeterminate: fail closed, a later contender resolves it
	}
	if ps == nil || ps.Q2AffGen != claim.Gen {
		// Authoritatively gone, or replaced by a later generation of the same client
		// ID. The delivery dies with the session it was assigned to, which
		// [MQTT-4.8.2-6] permits; it is never released back to the group.
		if _, _, derr := as.advanceQ2AffClaim(claimSubject, claim.Winner, claim.Gen,
			func(c *mqttQ2AffClaim) { c.Discarded = true }); derr != nil {
			return false
		}
		if aerr := as.confirmSourceAckAndRecord(claimSubject, in.reply); aerr != nil {
			return false
		}
		as.reapQ2AffClaim(s, claimSubject)
		return true
	}

	// (2) Does the winner's affine consumer actually exist, with the identity the
	// claim recorded? $MQTT_out is interest-retained: without it the copy store
	// would return success and keep nothing.
	ci, err := as.jsa.lookupConsumer(mqttOutStreamName, claim.AffDurable)
	if err != nil || ci == nil || ci.Config == nil {
		return false
	}
	if verr := mqttValidateQ2AffineNames(ci.Config, claim.Winner, claim.Gen); verr != nil {
		s.Warnf("MQTT: refusing to materialize shared QoS 2 assignment %q on behalf of %q: %v", claimSubject, claim.AffDurable, verr)
		return false
	}

	// (3) Write the WINNER's copy: its subject, its generation, and its delivery
	// decisions — not ours.
	onBehalf := *env
	onBehalf.gen = claim.Gen
	onBehalf.subID = claim.SubID
	onBehalf.retained = claim.Retained
	if !as.materializeQ2AffCopy(s, claim, claimSubject, claimSeq, &onBehalf) {
		return false
	}
	if aerr := as.confirmSourceAckAndRecord(claimSubject, in.reply); aerr != nil {
		return false
	}
	// Same reason as in completeQ2AffOwnClaim: the absent winner may have returned
	// and finished the exchange while this ack was in flight.
	as.reapQ2AffClaim(s, claimSubject)
	return true
}

// ---------------------------------------------------------------------------
// Affine delivery
// ---------------------------------------------------------------------------

// mqttDeliverQ2AffineCb delivers a session-affine QoS 2 copy to its owning
// client. It is the SOLE tracker of these deliveries: the group callback moves
// and acks, and never calls trackPublish for a shared QoS 2 message.
//
// Every delivery is gated on the durable claim. The copy becomes visible to this
// consumer as soon as it is stored, which is BEFORE the claimed->materialized CAS
// commits; publishing in that window would let the client PUBREC it, ack the copy
// away, and leave recovery looking at a `claimed` claim with no copy — which it
// would resolve by materializing a SECOND copy. So: no claim, or a claim that is
// not durably materialized, means NAK and wait.
func mqttDeliverQ2AffineCb(sub *subscription, pc *client, _ *Account, _, reply string, rmsg []byte) {
	if reply == _EMPTY_ {
		return
	}
	cc := sub.client
	if cc == nil || cc.mqtt == nil || cc.mqtt.sess == nil {
		return
	}
	sess := cc.mqtt.sess
	as := cc.mqtt.asm
	if as == nil {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	h := mqttParsePublishNATSHeader(hdr)
	if h == nil || h.qos != 2 || len(h.subject) == 0 {
		return
	}
	claimSubject := string(getHeader(mqttQ2AffClaimHeader, hdr))
	gen := string(getHeader(mqttQ2AffGenHeader, hdr))
	subID, _ := strconv.Atoi(string(getHeader(mqttQ2AffSubIDHeader, hdr)))
	if claimSubject == _EMPTY_ || gen == _EMPTY_ {
		return
	}

	sess.mu.Lock()
	if sess.c != cc || sub.mqtt == nil || sub.mqtt.closed || sess.q2AffConsumer == nil {
		sess.mu.Unlock()
		return
	}
	if sess.q2AffGen != gen {
		// A copy from a different generation of this client ID. The consumer's
		// filter subject should have made this unreachable; treat it as unreadable
		// rather than deliverable.
		sess.mu.Unlock()
		return
	}
	idHash := sess.idHash
	jsDur := sess.q2AffConsumer.Durable
	sess.mu.Unlock()

	claim, _, err := as.loadQ2AffClaim(claimSubject)
	switch {
	case err != nil:
		sess.jsa.sendMsg(reply, AckNak) // indeterminate: retry, never guess
		return
	case claim == nil:
		// FAIL CLOSED. Safe by construction: nothing may delete a claim whose
		// affine side is unproven, so a missing claim here is a transient read or
		// an operator action, not authorization to deliver.
		sess.jsa.sendMsg(reply, AckNak)
		return
	case !claim.ownedBy(idHash, gen):
		// The claim says this message belongs to another session. Our copy is an
		// artifact that must not be delivered; dropping it is right, because the
		// real owner has (or will have) its own.
		cc.Errorf("MQTT: dropping an affine QoS 2 copy on %q: its claim names a different owner", claimSubject)
		sess.jsa.sendAck(reply)
		return
	case claim.Discarded:
		// The session ended between the claim and the delivery. The message dies
		// with it [MQTT-4.8.2-6]; record the affine side complete so the claim can
		// be reaped once its source ack is proven.
		cc.mqttFinishQ2AffineDelivery(claimSubject, reply)
		return
	case claim.AffineDone || claim.Lost:
		// The affine side is already recorded complete, so this is a redelivery of
		// a copy whose exchange has moved on — either into the PUBREL phase, or
		// into a recorded loss. Publishing it again would duplicate a PUBLISH the
		// client has already answered. Dispose of it and let the reap proceed now
		// that the copy is actually going away.
		cc.mqttFinishQ2AffineDelivery(claimSubject, reply)
		return
	case claim.Phase != mqttQ2AffMaterialized:
		sess.jsa.sendMsg(reply, AckNak)
		return
	}

	sess.mu.Lock()
	pi, dup, started := sess.trackPublish(jsDur, reply)
	if pi != 0 {
		if p := sess.pendingPublish[pi]; p != nil {
			// Carry the claim into the pending entry: the terminal paths (PUBREC,
			// expiry, oversize discard) see a packet identifier and nothing else.
			p.q2AffClaim = claimSubject
		}
	}
	sess.mu.Unlock()
	if pi == 0 {
		return
	}

	props := h.props
	if len(props) > 0 && !started {
		_, _, _, ts, _ := ackReplyInfo(reply)
		var expired bool
		if props, expired = mqttForwardExpiry(props, ts); expired {
			sess.mu.Lock()
			sess.untrackPublish(pi)
			sess.mu.Unlock()
			cc.mqttFinishQ2AffineDelivery(claimSubject, reply)
			return
		}
	}
	topic := natsSubjectStrToMQTTTopic(string(h.subject))
	if !pc.mqttEnqueuePublishMsgTo(cc, sub, pi, 2, dup, h.retained, subID, topic, msg, props) {
		// Exceeds the client's MQTT 5.0 Maximum Packet Size: discard it and
		// complete the delivery. It dies with this session, never redistributed.
		// Spec5 [3.1.2.11.4].
		sess.mu.Lock()
		sess.untrackPublish(pi)
		sess.mu.Unlock()
		cc.mqttFinishQ2AffineDelivery(claimSubject, reply)
	}
}

// mqttFinishQ2AffineDelivery completes the affine side of one assignment: record
// AffineDone, then ack the copy WITH CONFIRMATION, then reap if the source side
// is proven too.
//
// The order is the whole point, and it is the opposite of the intuitive one.
// Acking first and recording after leaves a crash window in which the claim says
// `materialized` and the copy is simply GONE — which is indistinguishable from a
// copy that was dropped because its consumer was destroyed. That ambiguity is
// what previously required a per-broker "I saw the consumer disappear" flag, and
// an in-memory flag cannot survive the broker handoff that makes the ambiguity
// matter. Recording first removes the ambiguity from the data itself: an absent
// copy under a claim that never reached AffineDone was NEVER acked by a delivery,
// so it was lost — and every broker reads that the same way.
//
// The inverted window is benign and self-correcting: AffineDone recorded but the
// copy not yet acked leaves the copy live, the claim un-reapable (see reapable),
// and the redelivered copy disposed of by the delivery gate's terminal branch.
func (c *client) mqttFinishQ2AffineDelivery(claimSubject, ackSubject string) {
	as := c.mqtt.asm
	if as == nil || c.mqtt.sess == nil {
		return
	}
	if _, _, err := as.advanceQ2AffClaim(claimSubject, _EMPTY_, _EMPTY_,
		func(cl *mqttQ2AffClaim) { cl.AffineDone = true }); err != nil {
		// Not recorded: do NOT ack. AckWait re-offers the copy, the gate re-runs,
		// and nothing has been made ambiguous.
		c.Warnf("Unable to record the completion of affine QoS 2 assignment %q: %v", claimSubject, err)
		return
	}
	if err := as.jsa.confirmAck(ackSubject); err != nil {
		c.Warnf("Unable to confirm the acknowledgement of an affine QoS 2 delivery for %q: %v", claimSubject, err)
		return
	}
	as.reapQ2AffClaim(c.srv, claimSubject)
}

// ---------------------------------------------------------------------------
// Session termination and winner recovery
// ---------------------------------------------------------------------------

// server returns the Server behind this account manager's internal JetStream
// client, for logging from paths that are handed no other context. Nil only in
// hand-built test managers.
func (as *mqttAccountSessionManager) server() *Server {
	if as.jsa.c == nil {
		return nil
	}
	return as.jsa.c.srv
}

// mqttQ2AffClaimIdentityOK reports whether a decoded claim carries the identity
// every decision here keys on. A record missing any of it cannot be attributed to
// a session, and must therefore never be treated as "not mine".
func mqttQ2AffClaimIdentityOK(c *mqttQ2AffClaim) bool {
	return c != nil &&
		c.Winner != _EMPTY_ &&
		mqttValidQ2AffGen(c.Gen) &&
		c.GroupDurable != _EMPTY_ &&
		c.SourceSeq != 0 &&
		c.CopySubject != _EMPTY_
}

// scanQ2AffClaims walks every assignment claim in the account, calling fn with
// each. ANY record it cannot read or cannot attribute aborts the scan with an
// error, and so does a load failure: a partial walk must never be reported as a
// complete one, because both callers act on ABSENCE.
//
// Skipping an unreadable record would be the dangerous shortcut. A claim whose
// identity cannot be parsed may well be one of the caller's own, and clear()
// would then delete the session record and the affine consumer while a live claim
// still named that generation — contenders honor such a claim indefinitely, so its
// source delivery would stall forever with nothing left able to complete it.
func (as *mqttAccountSessionManager) scanQ2AffClaims(fn func(subject string, claim *mqttQ2AffClaim, seq uint64) error) error {
	return as.scanQ2AffClaimRecords(func(claim *mqttQ2AffClaim, smsg *StoredMsg) error {
		return fn(smsg.Subject, claim, smsg.Sequence)
	})
}

// scanQ2AffClaimRecords is scanQ2AffClaims with the stored message exposed, for the
// one caller that needs a claim's JetStream STORE TIME (the orphan sweep ages a
// claim by it, never by anything self-reported).
//
// It enforces mqttQ2AffClaimIdentityOK and nothing more. That predicate is the
// SHARED ATTRIBUTION FLOOR for every full scan, and it is deliberately not where
// stricter, derived-name checks belong: the production callers above are a session
// resume and clear(), where one failure anywhere in this account-wide walk already
// aborts the caller — and for clear() that fails the discard, strands the teardown
// marker and locks the client ID out of its next CONNECT. A derived-name mismatch
// must not be able to do that to unrelated sessions, so the orphan sweep layers its
// own validator (validateOrphanQ2AffClaim) over each decoded claim instead.
func (as *mqttAccountSessionManager) scanQ2AffClaimRecords(fn func(claim *mqttQ2AffClaim, smsg *StoredMsg) error) error {
	filter := mqttQoS2AssignStreamSubjectPrefix + ">"
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttQoS2AssignStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return err
			}
			return nil // walked to the end
		}
		if smsg == nil {
			return nil
		}
		seq = smsg.Sequence + 1
		claim := &mqttQ2AffClaim{}
		if uerr := json.Unmarshal(smsg.Data, claim); uerr != nil {
			return fmt.Errorf("assignment claim on %q at sequence %d is unreadable: %w", smsg.Subject, smsg.Sequence, uerr)
		}
		if !mqttQ2AffClaimIdentityOK(claim) {
			return fmt.Errorf("assignment claim on %q at sequence %d carries no usable identity (winner %q, generation %q, group %q, source sequence %d)",
				smsg.Subject, smsg.Sequence, claim.Winner, claim.Gen, claim.GroupDurable, claim.SourceSeq)
		}
		if ferr := fn(claim, smsg); ferr != nil {
			return ferr
		}
	}
}

// discardQ2AffClaimsFor marks every nonterminal assignment owned by one session
// generation as discarded, so its started deliveries die with the session rather
// than being released back to the group. [MQTT-4.8.2-6] permits the first and
// forbids the second.
//
// The discard PRESERVES facts already proven, and specifically does not imply
// SourceAcked: a discarded claim whose group ack never committed must survive
// until some contender proves that ack, or the source redelivery would find no
// claim and elect a fresh winner. That is the same violation, reached through the
// discard side-door.
//
// Called from clear() with the fence closed and every in-flight move drained, and
// BEFORE the session record is deleted. An error fails the clear: reporting a
// session as cleaned up while a live claim still names it is exactly the
// terminal-cleanup-before-completion mistake.
func (as *mqttAccountSessionManager) discardQ2AffClaimsFor(idHash, gen string) error {
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return nil
	}
	s := as.server()
	return as.scanQ2AffClaims(func(subject string, claim *mqttQ2AffClaim, _ uint64) error {
		if !claim.ownedBy(idHash, gen) || claim.Discarded {
			return nil
		}
		updated, _, err := as.advanceQ2AffClaim(subject, idHash, gen, func(c *mqttQ2AffClaim) { c.Discarded = true })
		if err != nil {
			return fmt.Errorf("discard assignment %q: %w", subject, err)
		}
		if updated != nil && updated.reapable(true) {
			// Only reachable when both sides were already proven; reapQ2AffClaim
			// re-reads and re-proves before deleting anything.
			as.reapQ2AffClaim(s, subject)
		}
		return nil
	})
}

// recoverQ2AffClaims reconciles the assignments a resuming session still owns.
// Its job is to leave no claim in a state that stalls something:
//
//   - materialized, not discarded, AffineDone never recorded, and the copy gone:
//     a LOST message. The affine side is always recorded BEFORE the copy is acked
//     (mqttFinishQ2AffineDelivery), so a copy that vanished without that record
//     was never acked by a delivery — the only other thing that removes a message
//     from $MQTT_out is deleting the consumer that held it, which is exactly the
//     loss. This reads the same on every broker and across a handoff, which a
//     "did I see the consumer disappear" flag could not: the broker that observed
//     it may be gone by the time anyone reconciles.
//   - claimed with the SOURCE MESSAGE authoritatively gone and no copy: no PUBLISH
//     can ever have passed the materialization gate, and no group delivery can
//     ever be re-offered. Mark it discarded and affine-complete, and let the reap
//     prove the source side from the group consumer's absence. Without this the
//     claim would sit at `claimed` forever, and contenders would keep honoring a
//     winner for a message that no longer exists.
//   - claimed with the source still present: leave it. The group's AckWait
//     re-offers the source, and whichever member receives it (this one included)
//     materializes from the payload it carries — which recovery does not have.
//
// Best effort: everything here is re-derivable, so a failure just leaves the work
// for the next contender or the next resume.
func (as *mqttAccountSessionManager) recoverQ2AffClaims(idHash, gen string) {
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return
	}
	s := as.server()
	err := as.scanQ2AffClaims(func(subject string, claim *mqttQ2AffClaim, _ uint64) error {
		if !claim.ownedBy(idHash, gen) || claim.Lost {
			// Lost is terminal, never reapable and mutually exclusive with
			// AffineDone: revisiting it on a later resume — when the consumer has
			// since been recreated and so looks intact — would only risk asserting
			// both that the message was delivered and that it was lost.
			return nil
		}
		if claim.AffineDone {
			// Completed but not reaped. The reap is the LAST step of
			// mqttFinishQ2AffineDelivery, so a broker that stopped after the copy's
			// ack committed — or whose ack response was merely lost — leaves a
			// finished assignment behind with nothing else that would ever revisit
			// it: the copy is gone so it cannot redeliver, and the source is already
			// acked so no contender looks at it either. Reaping here is safe in both
			// states because the reaper re-reads and requires the copy to be
			// AUTHORITATIVELY absent: a still-live copy keeps its claim, one whose
			// ack committed is deleted.
			as.reapQ2AffClaim(s, subject)
			return nil
		}
		copyState, _ := as.loadQ2AffCopy(claim.CopySubject)
		if copyState != mqttQ2AffCopyAbsent {
			return nil // present, or unreadable: nothing provable to do
		}
		switch claim.Phase {
		case mqttQ2AffMaterialized:
			if claim.Discarded {
				break // its session ended; the terminal path below reaps it
			}
			// The copy is gone and no delivery ever recorded disposing of it. That
			// is a QoS 2 message the broker accepted and never delivered. Record it
			// as LOST: terminal, so no contender waits on it and the group delivery
			// is released, but never reapable, so the evidence survives instead of
			// being erased by a clean reap.
			if s != nil {
				s.Errorf("MQTT: shared QoS 2 assignment %q for session %q was materialized, but its copy is gone with no record of it being delivered; recording it as a LOST QoS 2 delivery",
					subject, idHash)
			}
			if _, _, aerr := as.advanceQ2AffClaim(subject, idHash, gen,
				func(c *mqttQ2AffClaim) { c.Lost = true }); aerr != nil {
				return nil
			}
		default:
			if !as.q2AffSourceGone(claim) {
				return nil
			}
			// Claimed, no copy, and the SOURCE message itself is gone: no PUBLISH can
			// ever have passed the materialization gate, and none ever will. Mark it
			// discarded — not AffineDone, which would assert a delivery that never
			// happened — and record the source side too: a source message that no
			// longer exists cannot be re-offered to anybody, which is exactly what
			// SourceAcked asserts. Without that the claim could never become
			// reapable, since nothing would be left to ack the group delivery.
			if _, _, aerr := as.advanceQ2AffClaim(subject, idHash, gen, func(c *mqttQ2AffClaim) {
				c.Discarded = true
				c.SourceAcked = true
			}); aerr != nil {
				return nil
			}
		}
		as.reapQ2AffClaim(s, subject)
		return nil
	})
	if err != nil && s != nil {
		s.Warnf("MQTT: unable to reconcile the shared QoS 2 assignments of session %q: %v", idHash, err)
	}
}

// q2AffSourceGone reports whether the claim's SOURCE message is authoritatively
// absent from $MQTT_msgs. Only a confirmed no-message answer counts; an
// unreadable stream is not an empty one.
func (as *mqttAccountSessionManager) q2AffSourceGone(claim *mqttQ2AffClaim) bool {
	if claim.SourceSeq == 0 {
		return false
	}
	si, err := as.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		return false
	}
	if claim.SourceSeq < si.State.FirstSeq {
		return true
	}
	_, err = as.jsa.loadMsg(mqttStreamName, claim.SourceSeq)
	return err != nil && !isErrorOtherThan(err, JSNoMessageFoundErr)
}

// mqttQ2AffUnwind is a prepared, not-yet-executed teardown of an affine delivery
// path this SUBSCRIBE packet provisioned. Split in two because the rollback has a
// commit point: the restored session record must not NAME the consumer (so the
// session fields are cleared before the save), while the consumer itself must not
// be DELETED until that record is confirmed (so a failed confirm leaves the
// resources the record still describes). This is the same discipline
// rollbackSubscribePacket already applies to the packet's other consumers.
type mqttQ2AffUnwind struct {
	gen        string
	durable    string
	deliver    string
	subscribed bool
	charged    bool
	// prepared is false when there was nothing to unwind, or when the claim
	// cleanup failed and the path was deliberately left in place.
	prepared bool
}

// prepareQ2AffineUnwind fences the affine path, terminates the assignments it
// owns, and clears it from the session — but deletes nothing yet.
//
// Claim cleanup comes FIRST, and a failure aborts the whole unwind with the path
// left intact and usable. Clearing the session's view of a consumer that is still
// alive would orphan it: the record would stop naming it, nothing would delete
// it, and the assignments still pointing at it would have no owner able to act.
// Leaving a working path provisioned after a failed packet is the strictly better
// outcome — it is session-level, idempotent, and the next SUBSCRIBE reuses it.
//
// Runs from the client's readLoop. sess.mu not held on entry.
func (sess *mqttSession) prepareQ2AffineUnwind(c *client) mqttQ2AffUnwind {
	gen, durable := sess.closeQ2AffAndDrain()
	if gen == _EMPTY_ {
		sess.mu.Lock()
		sess.q2AffClosing = false
		sess.mu.Unlock()
		return mqttQ2AffUnwind{}
	}

	// Terminate this generation's assignments before anything is torn down. They
	// are marked DISCARDED, never released: a delivery already started to this
	// session dies with the subscription that was never established, which
	// [MQTT-4.8.2-6] permits, rather than being handed to another member.
	if sess.asm != nil {
		if err := sess.asm.discardQ2AffClaimsFor(sess.idHash, gen); err != nil {
			c.Errorf("MQTT: unable to terminate the shared QoS 2 assignments of a rolled-back SUBSCRIBE for %q, leaving its affine delivery path in place: %v", sess.id, err)
			sess.mu.Lock()
			sess.q2AffClosing = false
			sess.q2AffReady = sess.q2AffConsumer != nil && sess.q2AffSubscribed
			sess.mu.Unlock()
			return mqttQ2AffUnwind{}
		}
	}

	sess.mu.Lock()
	u := mqttQ2AffUnwind{
		gen:        gen,
		durable:    durable,
		deliver:    sess.q2AffDeliverySubject,
		subscribed: sess.q2AffSubscribed,
		charged:    sess.q2AffConsumer != nil,
		prepared:   true,
	}
	sess.q2AffConsumer = nil
	sess.q2AffGen = _EMPTY_
	sess.q2AffSubscribed = false
	sess.q2AffDeliverySubject = _EMPTY_
	sess.q2AffDeliverySubjectB = nil
	sess.q2AffReady = false
	// Reopen the fence: the session survives this failed packet and must be able
	// to provision again later. Safe for the same reason as in clear() — with no
	// generation and no readiness, beginQ2AffOp admits nothing.
	sess.q2AffClosing = false
	sess.mu.Unlock()
	return u
}

// finishQ2AffineUnwind deletes what prepareQ2AffineUnwind detached, once the
// record that no longer names it is confirmed.
func (sess *mqttSession) finishQ2AffineUnwind(c *client, u mqttQ2AffUnwind) {
	if !u.prepared {
		return
	}
	if u.subscribed && u.deliver != _EMPTY_ {
		if err := c.processUnsub([]byte(u.deliver)); err != nil {
			c.Warnf("Unable to remove the affine QoS 2 delivery subscription on %q: %v", u.deliver, err)
		}
	}
	if !u.charged {
		return
	}
	if _, err := sess.jsa.deleteConsumer(mqttOutStreamName, u.durable, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
		c.Errorf("Unable to delete the affine QoS 2 consumer %q of a rolled-back SUBSCRIBE: %v", u.durable, err)
	}
	opts := c.srv.getOpts()
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	sess.mu.Lock()
	sess.tmaxack -= maxAckPending
	if sess.tmaxack < 0 {
		sess.tmaxack = 0
	}
	sess.mu.Unlock()
}

// restoreQ2AffineUnwind puts a prepared-but-not-executed unwind back, for a
// rollback whose own confirming save failed. Nothing was deleted, so the session
// must go back to describing the resources that are still live.
func (sess *mqttSession) restoreQ2AffineUnwind(u mqttQ2AffUnwind) {
	if !u.prepared {
		return
	}
	filter, deliver, durable := mqttQ2AffineNames(sess.idHash, u.gen)
	sess.mu.Lock()
	sess.q2AffGen = u.gen
	sess.q2AffDeliverySubject = deliver
	sess.q2AffDeliverySubjectB = []byte(deliver)
	sess.q2AffSubscribed = u.subscribed
	if u.charged {
		sess.q2AffConsumer = &ConsumerConfig{Durable: durable, FilterSubject: filter, DeliverSubject: deliver}
	}
	// Readiness stays FALSE: the record naming this path could not be confirmed,
	// and a move must never store a copy the record cannot lead back to. The next
	// resume re-proves it through ensureQ2AffineConsumerSubscription.
	sess.q2AffReady = false
	sess.mu.Unlock()
}

// hasSharedQoS2Sub reports whether the session still holds any shared
// subscription at QoS 2 — the only thing the affine delivery path exists for.
//
// sess.mu not held on entry.
func (sess *mqttSession) hasSharedQoS2Sub() bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, qos := range sess.sharedSubs {
		if qos == 2 {
			return true
		}
	}
	return false
}
