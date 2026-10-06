// Session-affine ownership of a STARTED shared-subscription QoS 2 delivery.
//
// A shared-subscription QoS 2 message is pending on the queue-delivered GROUP
// durable from the moment its PUBLISH is sent until the client's PUBREC. In that
// window the group durable is the only thing that owns it, so a member leaving
// (UNSUBSCRIBE, disconnect, session end) made AckWait redeliver the started
// message to a DIFFERENT member — or dropped it when the leaver was the last
// holder. Spec5 [MQTT-4.8.2-4]/[MQTT-4.8.2-5] require the opposite: once the
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
// The group delivery is released only against evidence: a copy PROVEN PRESENT by a
// fresh lookup, or a terminal fact on the claim (Discarded, AffineDone, Lost) that
// says no delivery can come of it, or a SourceAcked already recorded. A
// materialized phase is not evidence on its own — see sourceAckAuthorized. So the
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
//   - phase (claimed -> materialized): did a copy provably exist at some instant?
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
//   - The source acknowledgement rests on a FRESH but non-atomic observation. The copy
//     is proven present by a lookup on its own subject before the group source is
//     released, but the claim and the copy live on different streams and
//     CONSUMER.DELETE carries no precondition, so a consumer destroyed between that
//     proof and the confirmed ack still loses the copy. The span includes any claim
//     load or CAS the materialization performs in between. F-063, and stated in
//     docs/CONFORMANCE.md rather than bounded here.
//   - Scope. Affinity is a property of the brokers that consult $MQTT_qos2assign,
//     which is every broker serving the account. Nothing gates it at runtime: a
//     broker serving the same shared group without consulting the claims would
//     redistribute a started delivery another broker had claimed.

package mqtt

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

// mqttQ2AffPhase is the claim's monotonic progress marker: was a copy provably
// created at some earlier instant? It is deliberately NOT the whole state —
// completion facts are separate boolean fields, because they are independent and
// each has its own proof (see the file header).
//
// It is a record of the past, not a statement about the present. $MQTT_out is
// interest-retained, so the copy a materialized claim names can be dropped with
// its consumer afterwards; only a fresh lookup of the copy subject says whether it
// is there now.
type mqttQ2AffPhase string

const (
	// mqttQ2AffClaimed: ownership is decided and no copy has been proven yet. The
	// affine delivery callback must NOT publish under this phase. It does not say a
	// copy is absent: the store completes BEFORE the materialized CAS, so a copy can
	// exist under this phase — the gate is what keeps it undeliverable.
	mqttQ2AffClaimed mqttQ2AffPhase = "claimed"
	// mqttQ2AffMaterialized: a copy was proven present on the claim-specific
	// subject. Only now may the copy be published — and the group delivery released
	// only against a copy proven present NOW, or a terminal fact (see
	// sourceAckAuthorized); the phase alone does not authorize that ack.
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
// Winner+Gen is the immutable identity a transition is matched against
// (qos2_move.go invariant 1): the subject is keyed by group durable and source
// sequence, which say nothing about which session generation owns it, so a
// sequence is not an identity here either.
//
// SEVEN of the ten transition call sites assert it; three deliberately do not,
// because the facts they record — SourceAcked and AffineDone — are owner-independent
// statements about the group delivery and about a copy the delivery gate has already
// attributed. F-033's audit states the split rather than glossing it as a universal
// check.
//
// Of those three, only the TWO SourceAcked writes rest on the per-subject CAS alone,
// which is sufficient for a fact about the source MESSAGE and would not be for an
// ownership decision. AffineDone is owner-independent but NOT unfenced: it states
// something about ONE incarnation's own copy, so F-065 fenced it on the claim's Nonce
// and on the copy subject the delivery carried. Owner-independent is not the same as
// unfenced, and the two questions are answered by different fields.
//
// The subject identifies one source message only within one incarnation of $MQTT_msgs,
// and a $MQTT_msgs whose sequences restart while these claims survive is exactly the
// state no per-subject CAS speaks to. That WAS reachable through a supported
// single-stream recreation, which is what made it F-066; the enforcement landed
// 2026-09-01 and the supported reset now takes all nine streams, so it clears these
// claims along with the source. What still produces the state is administrative
// deletion or a partial restore — unsupported rather than impossible, which is why the
// scoping below stays.
type mqttQ2AffClaim struct {
	Winner       string `json:"winner"` // winning session's client-ID hash
	Gen          string `json:"gen"`    // winning session's durable generation
	GroupDurable string `json:"gdur"`
	SourceSeq    uint64 `json:"sseq"`
	ClaimHash    string `json:"chash"`
	// Nonce identifies this INCARNATION of the claim subject, and is what stops a
	// stale actor from acting on a successor's claim. The subject is keyed by group
	// durable and source sequence, and its create-only election can be won AGAIN
	// after a reap — by a group delivery callback that was already dispatched when
	// the reap happened — so Winner+Gen+ClaimHash name the assignment but not the
	// attempt. Without it two incarnations share one copy subject, and a replacement
	// adopts the copy a superseded materializer is about to withdraw.
	//
	// Minted once, at the election, and NEVER rewritten: renaming it would rename a
	// copy the winner is already reading. Same form as Gen, so its width is an
	// accounting invariant rather than "some subject token". F-065.
	Nonce string `json:"nonce"`
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
	// [MQTT-4.8.2-5] permits; it is NEVER released back to the group.
	Discarded bool `json:"disc,omitempty"`
	// Lost: mutually exclusive with AffineDone. The copy is gone and its delivery
	// cannot be PROVEN — the affine consumer was destroyed out of band, and
	// $MQTT_out is interest-retained, so its pending copies went with it. It is
	// deliberately NOT AffineDone: recording it as a completed delivery would
	// launder an unproven outcome into a clean reap, and the evidence would then be
	// deleted. A lost claim is never reapable, so the durable record survives for
	// an operator to find; it is terminal, so no contender waits on it and the
	// group delivery is still released.
	//
	// NOT proof that the client missed the message. The write order that licenses
	// the inference — AffineDone before the copy ack — covers the JETSTREAM copy
	// acknowledgement only. mqttProcessPublishReceived stores the correlated PUBREL
	// before it reaches mqttFinishQ2AffineDelivery, so a client can have answered
	// PUBREC and still leave this shape behind. Nothing is redistributed and the
	// handshake still completes from the persisted PUBREL, which is why this is a
	// reporting defect rather than a conformance one; see docs/CONFORMANCE.md.
	Lost bool `json:"lost,omitempty"`
	// PubRecProven: a PUBREL bound to THIS claim incarnation is durable on
	// $MQTT_out, so the client answered PUBREC for this copy. It is the only fact in
	// this record that outranks Lost.
	//
	// Deliberately NOT AffineDone, and the distinction is the whole mechanism.
	// AffineDone means "terminal copy disposition" and is written by seven routes —
	// the session discarded, a redelivery under an already-terminal claim, a
	// subscribe denial, expiry, an oversize discard, retired-source settlement, and
	// the ordinary PUBREC. One of those is the delivery gate's own `AffineDone ||
	// Lost` arm, so if AffineDone outranked Lost then "this claim is already Lost"
	// would become the reason to clear Lost, confirm the copy acknowledgement and
	// reap. That is circular, and reachable: a delayed materializer holding a
	// `claimed` snapshot can store a copy while recovery is committing Lost.
	//
	// This field has exactly two writers, and both hold a resolved (claim subject,
	// copy subject) carrier read off a stored PUBREL: the owner's resume pass
	// (reconcileQ2AffPubRelProofs) and the PUBCOMP arm that must not acknowledge a
	// PUBREL away while the proof it carries is unrecorded.
	PubRecProven bool `json:"prec,omitempty"`
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
	c.PubRecProven = c.PubRecProven || prev.PubRecProven
	// A PROOF outranks an INFERENCE, and that asymmetry is why PubRecProven exists
	// as its own field. Lost concludes a loss from a copy it cannot find; the
	// broker's own comment on it says "could not prove", not "never delivered".
	// PubRecProven concludes a delivery from a PUBREL that exists, bound to this
	// exact claim incarnation. Resolving the two by arrival order — which is what
	// this function used to do — let a mislabelling become permanent, because
	// nothing could ever correct a Lost that landed first.
	//
	// Generic AffineDone is deliberately NOT given this power; see the field's own
	// comment for the circular path that would open.
	if c.PubRecProven {
		c.Lost = false
		c.AffineDone = true
	} else if prev.AffineDone {
		// AffineDone and Lost are mutually exclusive — a delivery is either
		// PROVEN or it is not — so they are NOT independently OR-merged. Whichever
		// the durable record already proved stands; a later writer cannot add its
		// opposite on top and leave a record asserting both.
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
		// A message the broker accepted and could not prove it delivered. Deleting
		// the record would destroy the only durable evidence that it happened.
		//
		// "Could not prove", not "never delivered": the writer of Lost infers a
		// missing delivery from a missing copy, and that inference is sound for the
		// JetStream copy ack but NOT for the client's PUBREC, which is answered
		// before the affine side is recorded. So a Lost claim may name a message the
		// client in fact received. The retention rule is unaffected — the evidence is
		// worth keeping either way — but the label is not proof of loss. See F-064's
		// residual 1 and docs/ARCHITECTURE.md.
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

// sourceAckAuthorized reports whether the group source may be released. copyPresent
// must come from a FRESH lookup of this claim's copy subject, taken by the same
// decision that acts on the answer.
//
// A materialized phase is deliberately not one of the disjuncts. It records that a
// copy was proven at some earlier instant, and $MQTT_out is interest-retained, so
// the copy may have been dropped with its consumer since — while the source ack is
// irreversible and releases the message from the shared group forever.
//
// Each disjunct is an independent authorization:
//
//   - copyPresent: the copy provably exists on the winner's own durable.
//   - AffineDone: recorded BEFORE the copy is acked, so an absent copy under it was
//     legitimately consumed by its own delivery.
//   - Discarded: the winner's session ended and the delivery dies with it
//     ([MQTT-4.8.2-5]). Required, not merely permitted: reapable refuses without
//     SourceAcked, so withholding the ack here would make every discarded claim
//     permanently unreapable.
//   - Lost: terminal by construction, and a lost claim still releases the group
//     delivery — the message was accepted and its delivery cannot be proven, which
//     withholding the ack does not undo. (Cannot be proven, not "was not
//     delivered": see the Lost field's own comment for the PUBREC window.)
//   - SourceAcked: the irreversible step already happened.
//
// The claim read and the copy read are separate JetStream round trips with no
// atomic snapshot across them, and CONSUMER.DELETE is unconditioned, so a copy
// proven present can still be gone when the ack lands. That residual is stated in
// docs/CONFORMANCE.md rather than claimed away.
func (c *mqttQ2AffClaim) sourceAckAuthorized(copyPresent bool) bool {
	if c == nil {
		return false
	}
	return copyPresent || c.AffineDone || c.Discarded || c.Lost || c.SourceAcked
}

// mqttQ2AffMoveResult is the resolved disposition of one shared QoS 2 move
// attempt. It is an enum rather than an error because the interesting outcomes
// are not failures: "a foreign session owns this, I acked and walked away" and
// "I could not prove anything, so I left the delivery unacked" are both correct,
// and only distinguishable by intent.
type mqttQ2AffMoveResult int

const (
	// mqttQ2AffMoveOwned: we are the durable winner and the source was released —
	// against a copy proven present on our own durable, or against a terminal fact
	// that arrived while we were materializing.
	mqttQ2AffMoveOwned mqttQ2AffMoveResult = iota
	// mqttQ2AffMoveForeign: a foreign session owns it, its copy was proven present,
	// and we acked the group delivery.
	mqttQ2AffMoveForeign
	// mqttQ2AffMoveOnBehalf: a foreign session owns it and was absent, so we proved a
	// copy onto ITS durable — or found a terminal fact — and acked the group delivery.
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
// generation namespaces it against a later session of the same client ID; the claim
// hash makes the store a create-only CAS with exactly one winner; and the claim's
// NONCE namespaces it against a later INCARNATION of the same claim subject, so a
// replacement claim can never read, adopt or lose the copy of the incarnation it
// replaced (F-065). It stays under the affine consumer's generation-prefixed filter,
// so the consumer's three names are unaffected.
func mqttQ2AffCopySubject(idHash, gen, claimHash, nonce string) string {
	return mqttQ2AffineSubjectPrefix + idHash + "." + gen + "." + claimHash + "." + nonce
}

// errQ2AffCarriedCopySuperseded reports the one carried-pair failure that is a POSITIVE
// fact rather than a refusal: the record on the subject is attributable and simply names a
// different copy, so nothing will ever authorize the copy this carrier was created for.
//
// It is separate from every other error out of q2AffClaimForCarriedCopy because the two
// licence opposite actions. An unattributable or unreadable record licenses nothing — no
// write, no acknowledgement. A superseded copy may be DISPOSED of, which is what keeps a
// retired source's redelivery from NAK-looping for the life of the session; the delivery
// gate draws the same distinction on the same evidence. The claim is returned alongside
// this error so a caller can check the copy IN HAND against the live record before
// disposing of anything, since the successor's own copy must never be destroyed.
//
// It is deliberately NARROWER than "the copy subjects differ". A carried copy belonging to
// a DIFFERENT assignment is not superseded by anything — the claim that owns it may be
// perfectly live — so it is an ordinary refusal, and only a copy of THIS assignment under
// another incarnation reaches this error. Review found the wider form reachable: a carrier
// naming a foreign claim survives the immediate transition (which refuses), and the
// retirement it leaves behind then acknowledged the foreign claim's live copy away on its
// next redelivery.
var errQ2AffCarriedCopySuperseded = errors.New("the claim on this subject names a different copy")

// q2AffClaimForCarriedCopy loads the claim a carrier names and returns it ONLY if it
// agrees with the copy that carrier was created for. It is the single place that turns a
// carried (claim subject, copy subject) pair back into a claim, so every consumer of such
// a pair gets the same binding.
//
// Three things have to hold, and none of them follows from the others:
//
//   - the record must be operationally attributable, which binds it to its own subject
//     and source (q2AffClaimUsableForDecision);
//   - it must name THIS copy. A pair naming some other claim's valid subject would
//     otherwise credit that claim with this delivery, and its own copy would then be
//     acknowledged away unpublished — losing that message. This is the check a bare
//     incarnation cannot make, because the incarnation says which ATTEMPT, not which COPY;
//   - the carried copy subject must itself be well formed, so an empty or malformed value
//     cannot pass by matching an equally malformed record.
//
// Returns nil with no error when the claim is simply absent, which every caller treats as
// "nothing to record" rather than as a failure. F-065.
func (as *mqttAccountSessionManager) q2AffClaimForCarriedCopy(claimSubject, carriedCopy string) (*mqttQ2AffClaim, error) {
	if mqttQ2AffCopyIncarnation(carriedCopy) == _EMPTY_ {
		return nil, fmt.Errorf("carried copy subject %q is not one this broker emits", carriedCopy)
	}
	claim, _, err := as.loadQ2AffClaim(claimSubject)
	if err != nil {
		return nil, err
	}
	if claim == nil {
		return nil, nil
	}
	if verr := q2AffClaimUsableForDecision(claimSubject, claim); verr != nil {
		return nil, verr
	}
	if claim.CopySubject != carriedCopy {
		// A mismatch is SUPERSEDED only when the carried copy is a copy of THIS assignment
		// under a different incarnation — same owner, same generation, same claim hash, so
		// the two subjects differ in their incarnation token and nothing else. That is the
		// only shape a reap-and-re-election produces, and it is the only one whose copy
		// nothing can ever authorize.
		//
		// A copy of a DIFFERENT assignment is not superseded by anything: it belongs to
		// another claim that may be perfectly live, and disposing of it on this evidence
		// destroys that claim's message — the very loss the pair exists to prevent,
		// committed by the remedy for it. So it is refused outright instead.
		if q2AffCopyIsSupersededIncarnationOf(claim, carriedCopy) {
			return claim, fmt.Errorf("claim on %q names copy %q, not the carried %q: %w",
				claimSubject, claim.CopySubject, carriedCopy, errQ2AffCarriedCopySuperseded)
		}
		return claim, fmt.Errorf("claim on %q names copy %q, and the carried %q is a copy of a DIFFERENT assignment",
			claimSubject, claim.CopySubject, carriedCopy)
	}
	return claim, nil
}

// A REFUSED affine copy is re-offered on an exponential backoff, and the reason is a
// measurement rather than a preference: a plain NAK is re-offered immediately, so a refusal
// state that PERSISTS becomes a request storm. One copy under an absent claim was measured
// at 9214 deliveries in ten seconds — a tight load-and-refuse loop against JetStream, with
// one log line per turn.
//
// The states that reach it are not exotic. docs/CONFORMANCE.md states three windows in
// which a copy legitimately outlives the claim that would authorize it (a crash, a delete
// that could not be confirmed, an indeterminate final reading), and an unreadable claim
// record or an unavailable backend produces the same shape for as long as it lasts. So the
// storm is reachable in ordinary degraded operation, not only from a corrupt record.
//
// The delay is derived from JetStream's OWN delivery count for that message, which is why
// this needs no per-copy state to remember: no map to grow, nothing to evict, and nothing
// that a broker restart loses. The first refusal is paced at the base delay, which keeps
// the COMMON race fast — a copy delivered before its claim's phase advance is a normal
// interleaving, not an error, and it resolves in one retry — and a persistent refusal
// settles at the cap, which also paces its diagnostics.
const (
	mqttPacedRefusalBaseDelay = 100 * time.Millisecond
	mqttPacedRefusalMaxDelay  = 30 * time.Second
	mqttPacedRefusalMaxShift  = 12 // 100ms << 12 is already past the cap
)

// mqttPacedRefusalNak is a NAK carrying the backoff for a delivery count. Returned as a
// value rather than sent here so every route that hands a message back uses ONE rule:
// the affine gate, the retired settlement, and the shared-group subscribe deny in
// mqtt.go. That last one arrived with a fixed 50ms delay of its own and a comment
// arguing that a PERMANENT refusal has nothing to back off from. The argument was
// backwards -- permanence is the case the cap above is FOR. A denied sole member
// re-offered every 50ms with MaxDeliver -1 is the same request storm this backoff was
// built to stop, reachable by any client permitted to hold such a subscription.
func mqttPacedRefusalNak(deliveryCount uint64) []byte {
	delay := mqttPacedRefusalBaseDelay
	if deliveryCount > 1 {
		shift := deliveryCount - 1
		if shift > mqttPacedRefusalMaxShift {
			shift = mqttPacedRefusalMaxShift
		}
		delay = mqttPacedRefusalBaseDelay << shift
	}
	if delay > mqttPacedRefusalMaxDelay {
		delay = mqttPacedRefusalMaxDelay
	}
	return []byte(fmt.Sprintf("%s {\"delay\":%d}", AckNak, int64(delay)))
}

// refuseQ2AffCopy declines to act on a copy and hands it back for a LATER attempt, paced.
// Every refusal on the affine path goes through here rather than sending AckNak directly:
// the arms differ in what they could not prove, not in how soon it is worth asking again.
func (sess *mqttSession) refuseQ2AffCopy(reply string) {
	_, _, dc, _, _ := ackReplyInfo(reply)
	sess.jsa.sendMsg(reply, mqttPacedRefusalNak(dc))
}

// q2AffCopyIsSupersededIncarnationOf reports whether copySubject is a copy of the SAME
// assignment as claim under a DIFFERENT incarnation, which is the only copy/claim mismatch
// that licenses disposing of the copy.
//
// It exists as one predicate because two paths dispose on that mismatch — the delivery
// gate, which takes its claim subject from the copy's own header, and the retired
// settlement, which takes it from a carrier — and review found each of them in turn
// disposing on bare subject inequality. Bare inequality is not the same statement: a copy
// of a DIFFERENT assignment mismatches too, and the claim that owns it may be perfectly
// live, so acknowledging it away destroys that claim's message and leaves it stranded
// without AffineDone. Same owner, same generation, same claim hash, differing in the
// incarnation token alone is the only shape a reap-and-re-election produces, and it is the
// only shape nothing can ever authorize.
//
// A cross-assignment mismatch is a persisted shape this broker does not write. Per the
// compatibility boundary it is REFUSED rather than interpreted, which on this path means
// handing the copy back paced and leaving it for an actor with evidence.
func q2AffCopyIsSupersededIncarnationOf(claim *mqttQ2AffClaim, copySubject string) bool {
	if claim == nil || copySubject == _EMPTY_ || claim.CopySubject == _EMPTY_ ||
		copySubject == claim.CopySubject {
		return false
	}
	incarnation := mqttQ2AffCopyIncarnation(copySubject)
	if incarnation == _EMPTY_ {
		return false
	}
	return mqttQ2AffCopySubject(claim.Winner, claim.Gen, claim.ClaimHash, incarnation) == copySubject
}

// mqttQ2AffCopyIncarnation returns the claim incarnation a copy subject names, or
// _EMPTY_ if the subject is not one this builder produced. The subject IS the
// identity, so a delivered copy needs no extra header to say which incarnation it
// belongs to — and deriving it here rather than trusting a header means a copy
// cannot claim an incarnation its own subject contradicts. F-065.
func mqttQ2AffCopyIncarnation(copySubject string) string {
	if !strings.HasPrefix(copySubject, mqttQ2AffineSubjectPrefix) {
		return _EMPTY_
	}
	toks := strings.Split(copySubject[len(mqttQ2AffineSubjectPrefix):], ".")
	if len(toks) != 4 {
		return _EMPTY_ // idHash.gen.claimHash.nonce, exactly
	}
	if !mqttValidQ2AffGen(toks[3]) {
		return _EMPTY_
	}
	return toks[3]
}

// mqttValidBase62 reports whether s is exactly n characters of the base-62 alphabet
// getHashSize emits. Every identity token in the two subjects below is one of those,
// so this is the single shape test rather than four hand-rolled loops.
func mqttValidBase62(s string, n int) bool {
	if len(s) != n {
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

// mqttValidQ2AffCopySubject reports whether copySubject is EXACTLY one this broker's
// mqttQ2AffCopySubject produces: the affine prefix and then idHash.gen.claimHash.nonce
// at their generated widths and alphabets.
//
// Stricter than mqttQ2AffCopyIncarnation, which validates the prefix, the token count
// and the nonce alone. That is enough where the value is about to be matched against a
// claim record that re-derives it, and not enough where the value's own well-formedness
// is the question — a carrier read off a stored PUBREL, where a loose value would parse
// as a usable reference, resolve to an absent claim, and take the acknowledging arm.
func mqttValidQ2AffCopySubject(copySubject string) bool {
	rest, ok := strings.CutPrefix(copySubject, mqttQ2AffineSubjectPrefix)
	if !ok {
		return false
	}
	toks := strings.Split(rest, ".")
	if len(toks) != 4 {
		return false
	}
	return mqttValidBase62(toks[0], sysHashLen) &&
		mqttValidQ2AffGen(toks[1]) &&
		mqttValidBase62(toks[2], mqttQ2AffClaimHashLen) &&
		mqttValidQ2AffGen(toks[3])
}

// mqttValidQ2AffClaimSubject reports whether claimSubject is EXACTLY one this broker's
// mqttQ2AffClaimSubject produces: the assignment prefix, a shared group durable this
// broker generated, and a nonzero decimal source sequence.
//
// The group durable is checked through mqttSharedQueueFromGroupDurable, which is the
// existing recogniser for "$MQTT_SHARED_ plus sixteen generated characters", so there is
// one definition of that shape rather than a second copy of it here.
func mqttValidQ2AffClaimSubject(claimSubject string) bool {
	rest, ok := strings.CutPrefix(claimSubject, mqttQoS2AssignStreamSubjectPrefix)
	if !ok {
		return false
	}
	i := strings.LastIndexByte(rest, '.')
	if i <= 0 || i == len(rest)-1 {
		return false
	}
	if _, ok := mqttSharedQueueFromGroupDurable(rest[:i]); !ok {
		return false
	}
	seqTok := rest[i+1:]
	seq, err := strconv.ParseUint(seqTok, 10, 64)
	// CANONICAL, not merely parseable. ParseUint accepts "01" and "+1"; the builder
	// emits strconv.FormatUint, so a value that does not round-trip through it is a
	// subject this broker never wrote — and one that would then resolve as an absent
	// claim, which is an ACKNOWLEDGING outcome, rather than failing closed.
	return err == nil && seq != 0 && strconv.FormatUint(seq, 10) == seqTok
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

// mqttQ2AffMsgId returns the copy's deduplication key. Stable across every retry of
// one claim INCARNATION, and distinct per (group, source message, incarnation) so
// two groups moving the same $MQTT_msgs message never dedup each other and a
// replacement incarnation is never deduped against the one it replaced.
//
// The incarnation belongs in here because a dedupe entry OUTLIVES its message
// (measured, TestQ2AffCopyStoreWithNoInterestSequenceAndDedupe): with an
// incarnation-blind key a replacement's store onto its own fresh subject would be
// silently discarded for the whole Duplicates window, and that claim would never
// materialize. It is NOT the attempt-scoped key F-063 refuted — one durable
// incarnation has many delivery attempts and every one produces this same key,
// which is what keeps a retry idempotent — so the value comes from the PERSISTED
// claim, never from an election candidate that may have lost.
func mqttQ2AffMsgId(groupDurable string, sseq uint64, nonce string) string {
	return mqttQ2AffMsgIdPrefix + groupDurable + "-" + strconv.FormatUint(sseq, 10) + "-" + nonce
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
	maxDur := len(mqttSharedConsumerDurablePrefix) + mqttSharedGroupKeyLen + 1 + mqttSharedIncMaxDigits
	maxSeq := mqttQ2AffMaxSeqDigits
	claimSubject = len(mqttQoS2AssignStreamSubjectPrefix) + maxDur + 1 + maxSeq
	// +1+gen for the claim INCARNATION the dedup key now carries: the nonce is minted
	// by the same generator as Gen, so its width is fixed and this stays a bound
	// rather than an estimate (F-065).
	msgID = len(mqttQ2AffMsgIdPrefix) + maxDur + 1 + maxSeq + 1 + mqttQ2AffGenLen
	gen = mqttQ2AffGenLen
	return claimSubject, msgID, gen
}

// mqttQ2AffClaimRecordMax bounds the ASSIGNMENT CLAIM record — the other thing a
// shared QoS 2 delivery stores, on $MQTT_qos2assign — computed by running the real
// encoder over the largest record this owner can reach.
//
// It is budgeted for the same reason the affine copy is, and the failure is the same
// eternal wedge from the other side: a PUBLISH admitted at PUBREC time, delivered to
// a shared group, whose CLAIM cannot be stored means no election can ever be won,
// the group delivery is never acked, and AckWait retries it forever. Nothing checked
// this before; F-065 widened the record and made the omission worth closing.
//
// The construction is explicit rather than "maximal widths", because the record also
// carries variable delivery decisions: maximal identity widths, the maximum
// Subscription Identifier, and Retained true — the same treatment the affine bound
// already gives them, and for the same reason (both come from a SUBSCRIBE that has
// not happened at admission time). Both reachable terminal shapes are compared, not
// just the initial election form: a later transition must not be the thing that
// exceeds the limit. AffineDone and Lost are mutually exclusive, so taking the max
// over the two shapes over-budgets by nothing, while setting Discarded alongside
// AffineDone over-budgets by one boolean deliberately.
func mqttQ2AffClaimRecordMax() int {
	claimW, _, genW := mqttQ2AffMaxIdentityWidths()
	maxDur := len(mqttSharedConsumerDurablePrefix) + mqttSharedGroupKeyLen + 1 + mqttSharedIncMaxDigits
	pad := strings.Repeat("x", claimW)
	rec := mqttQ2AffClaim{
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
		Phase:        mqttQ2AffMaterialized,
		SourceAcked:  true,
		Discarded:    true,
	}
	size := func(r mqttQ2AffClaim) int {
		b, _ := json.Marshal(&r)
		// The create-only CAS header is not stripped before storage, so it counts.
		return len(hdrLine) + len(JSExpectedLastSubjSeq) + 1 +
			len(strconv.FormatUint(math.MaxUint64, 10)) + 2 + 2 + len(b)
	}
	done, lost, proven := rec, rec, rec
	done.AffineDone = true
	lost.Lost = true
	// The THIRD reachable terminal shape, and it is the largest: PubRecProven implies
	// AffineDone (mergeProven), so the proven form is the AffineDone form plus one more
	// member. Comparing only the first two would leave a record this broker writes
	// outside the bound the admission preflight enforces, and the failure that causes is
	// the eternal retry these bounds exist to prevent.
	proven.AffineDone, proven.PubRecProven = true, true
	max := size(done)
	if n := size(lost); n > max {
		max = n
	}
	if n := size(proven); n > max {
		max = n
	}
	return max
}

// mqttQ2AffPubRelFormMax is the upper bound on a session-affine shared QoS 2 PUBREL,
// computed by running the REAL builder at maximal identity widths.
//
// It is its own ALTERNATIVE form rather than an addition to mqttQ2AffineFormMax, and
// the distinction is load-bearing: mqttMaxQoS2FormSize takes the MAXIMUM over forms,
// the affine copy and the affine PUBREL never coexist in one stored message, and
// folding this into the copy's bound would sum two alternatives and reject PUBLISHes
// no real later form justifies.
//
// Payload-independent, like the terminal and claim forms, so for a small payload with a
// short topic it could dominate — which is the reason it is budgeted at all rather than
// argued to be small. Before this reference existed the PUBREL form was not budgeted;
// that gap is closed here because this is what enlarges it.
func mqttQ2AffPubRelFormMax() int {
	maxClaimSubject, _, genW := mqttQ2AffMaxIdentityWidths()
	maxDur := len(mqttSharedConsumerDurablePrefix) + mqttSharedGroupKeyLen + 1 + mqttSharedIncMaxDigits
	maxCopySubject := len(mqttQ2AffineSubjectPrefix) + sysHashLen + 1 + genW + 1 +
		mqttQ2AffClaimHashLen + 1 + genW
	// The source correlation, the deduplication identity and the mandatory owner identity
	// are budgeted alongside it: a DRAINED affine source reaches this builder carrying all
	// four at once. The owner nonce is a fixed-width NUID, so its contribution is exact
	// rather than an upper bound — but it MUST be threaded through, because this budget is
	// derived by running the real encoder and a missing argument would silently under-count
	// the form by the width of a whole header.
	msgId := mqttPubRelMsgId(strings.Repeat("x", sysHashLen), strings.Repeat("x", maxDur),
		math.MaxUint64, math.MaxUint64)
	_, n := mqttNewCorrelatedPubRel(math.MaxUint16, strings.Repeat("x", mqttInflightNonceLen),
		strings.Repeat("x", maxDur), math.MaxUint64,
		msgId, strings.Repeat("x", maxClaimSubject), strings.Repeat("x", maxCopySubject))
	return n
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
// account's max payload. It is a live backstop, not a formality: the PUBLISH
// admission preflight applies the affine bound above (mqttMaxQoS2FormSize), but
// QoS 1/2 Will stores bypass that preflight entirely — mqttHandleWill and
// deliverWillRecord call mqttInitiateMsgDelivery directly — so a QoS 2 Will
// reaching a QoS 2 shared member arrives here unbounded, and a copy that can
// never be stored would wedge the group durable in eternal retry. The caller's
// policy on failure is to NOT claim, ack the group delivery, and log an oversize
// discard — the same disposal the client Maximum Packet Size path already uses.
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

// mqttTestQ2AffineConfigBuiltHookV fires on every construction of an affine QoS 2
// consumer configuration, naming the function that asked for one. It exists to
// prove that both writers really do go through the single constructor: an
// identical literal copied to one of them produces an identical config, so no
// comparison of the RESULT can tell the two apart — only observing the entry can.
// The caller is resolved by reflection on the stack, and only while the hook is
// installed, so the constructor's signature carries no test-only parameter.
// Stored and cleared by the test directly, with no setter beside it. The other
// hooks in this package have one because they are installed from untagged tests;
// this one is reachable only from the mqttport_fulltests suite, so a setter would
// be dead code in every build the linter sees.
var mqttTestQ2AffineConfigBuiltHookV atomic.Pointer[func(caller string)]

// buildQ2AffineConsumerConfig is the SOLE constructor of an affine QoS 2
// consumer's configuration, and that exclusivity is load-bearing rather than
// tidiness. Two writers build one: the session provisioning its own path, and a
// contender restoring a destroyed durable on behalf of an absent winner. The
// contender decides whether a persisted config is replayable by comparing it
// WHOLE against what this function returns, so a second literal anywhere would be
// a second canonical shape — and a record written through one would be refused by
// the other.
//
// Comparing the whole struct, rather than a list of interesting fields, is what
// makes the check closed against a dependency that adds a field: a field neither
// side sets is zero on both and passes, and any writer that sets one deviates and
// is refused. An enumerated list cannot close a struct upstream can extend, and
// MaxDeliver alone — outside every list an earlier revision of this design
// wrote — reopens the stall the feature exists to close.
//
// The three variable fields belong to the caller. The owner passes its local
// options. A contender passes the record's persisted values, except for
// MemoryStorage where a durable already exists: that type is immutable, so a
// contender must never impose its own local option on a durable the owner will
// later meet.
func buildQ2AffineConsumerConfig(idHash, gen string, ackWait time.Duration, maxAckPending int, memStorage bool) ConsumerConfig {
	if h := mqttTestQ2AffineConfigBuiltHookV.Load(); h != nil {
		(*h)(mqttCallerName(2))
	}
	filter, deliver, durable := mqttQ2AffineNames(idHash, gen)
	return ConsumerConfig{
		DeliverSubject: deliver,
		Durable:        durable,
		AckPolicy:      AckExplicit,
		DeliverPolicy:  DeliverNew,
		FilterSubject:  filter,
		AckWait:        ackWait,
		MaxAckPending:  maxAckPending,
		MemoryStorage:  memStorage,
		// InactiveThreshold is deliberately left at zero, unconditionally (the
		// broker has no per-consumer inactivity option): an inactivity delete
		// would reap this durable while the client is offline, and with it the
		// interest that keeps its pending copies alive. That is precisely the
		// resumable-session state this feature exists to preserve.
		//
		// Sending zero does not MAKE the effective threshold zero: nats-server
		// substitutes the stream's ConsumerLimits.InactiveThreshold whenever the
		// request field is zero (consumer.go:658-669). validateOutStream refuses a
		// nonzero limit at startup, and a contender re-checks it live because the
		// limit can be imposed afterwards.
	}
}

// errQ2AffStorageImmutable is nats-server's flattened cause for a create-or-update
// that would change a durable's storage type (server/consumer.go:2475, reached
// through checkNewConsumerConfig and wrapped in the generic JSConsumerCreateErrF).
// Measured, not recalled: the API error carries ErrCode 10012 and this exact
// Description.
var errQ2AffStorageImmutable = errors.New("storage type can not be updated")

// recoverQ2AffineStorageConflict makes ONE bounded attempt to complete a create
// that JetStream refused because the durable already exists with the other
// storage type. MemoryStorage is immutable (server/consumer.go:2474-2475), so the
// only way forward is to re-send the request built with the storage the durable
// ACTUALLY has.
//
// It is a recovery, not a preflight, and that distinction is the whole reason the
// owner's path may have it. F-064's decision 4 forbids adding a REFUSAL here,
// because a refusal during CONNECT-time restoration of a saved shared filter is
// silently skipped — there is no SUBACK on that path — and the only remedy that
// removes the silence closes the connection, publishing the client's Will and, at
// a zero Session Expiry Interval, deleting the session. This function is entered
// only AFTER a create that has already failed, and only on one exact cause, so it
// can turn a current failure into a success and can never produce one. A
// speculative lookup before the create could not say the same.
//
// It also closes a defect that exists with no contender anywhere: an operator who
// reloads MQTT.ConsumerMemoryStorage breaks affine resume for every session whose
// durable was created under the old value, permanently, because the owner rebuilds
// its request from the CURRENT local option while the durable keeps the old type.
//
// On a failed lookup, an absent or unchanged durable, or a failed retry, it
// reports false and the caller keeps TODAY's outcome: the original create error is
// returned and the caller's rollback runs. The lookup's own error is never
// substituted for it — the caller is entitled to be told why the create failed,
// not why the recovery could not run. That is deliberately narrower than "leaves
// identical state": a retry whose response was lost may have landed.
func (sess *mqttSession) recoverQ2AffineStorageConflict(ccr *CreateConsumerRequest, createErr error) (*JSApiConsumerCreateResponse, bool) {
	// Exactly this cause. Every other immutable mismatch — an AckPolicy or a
	// DeliverPolicy, which JetStream equally forbids changing — means the durable
	// is not one this broker can converge, and retrying would only repeat the
	// refusal with a different field.
	if sess.asm == nil || !isJSErrCausedBy(createErr, JSConsumerCreateErrF, errQ2AffStorageImmutable) {
		return nil, false
	}
	// The refused create mutated NOTHING: checkNewConsumerConfig runs before
	// updateConfig applies anything, so an immutable mismatch is rejected with the
	// durable untouched. The retry, by contrast, is accepted — and a create-or-
	// update carrying a zero InactiveThreshold has the stream's limit substituted
	// into it, which for an existing durable ARMS the server's deletion timer. A
	// retry whose response is then lost leaves this function reporting failure
	// while the durable it just made reapable keeps the session's interest-retained
	// copies until the threshold expires.
	//
	// Today that combination cannot occur: the mismatch fails inertly and the
	// durable survives. So retrying under a nonzero limit would convert an
	// unresumable session into a LOSING one, which is the one thing the monotonicity
	// this recovery is allowed under does not permit. Read the limit and decline.
	//
	// Checking it here costs nothing the caller did not already lose: this path is
	// entered only after a create that has already failed, and every exit from it
	// preserves that original error. `validateOutStream` refuses a nonzero limit at
	// startup, so this can only be a stream mutated afterwards.
	if si, serr := sess.asm.lookupQ2AffOutStream(); serr != nil || si == nil ||
		si.Config.ConsumerLimits.InactiveThreshold != 0 {
		return nil, false
	}
	ci, lerr := sess.asm.lookupQ2AffineConsumer(ccr.Config.Durable)
	if lerr != nil || ci == nil || ci.Config == nil {
		return nil, false
	}
	if ci.Config.MemoryStorage == ccr.Config.MemoryStorage {
		// The durable no longer differs in the field the refusal named, so this
		// request has nothing to adopt and re-sending it would repeat the refusal.
		// One attempt is a bounded fail-closed policy, not a proof that the
		// conflict cannot recur: nothing makes the refusal and this lookup atomic.
		return nil, false
	}
	retry := ccr.Config
	retry.MemoryStorage = ci.Config.MemoryStorage
	resp, rerr := sess.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: ccr.Stream, Config: retry})
	if rerr != nil {
		return nil, false
	}
	// The caller stores &ccr.Config as the session's view and persists it, so the
	// record must name what was actually created, not what was first requested.
	ccr.Config = retry
	return resp, true
}

// retryQ2AffineStorageConflict makes ONE bounded attempt to complete a
// contender's create that JetStream refused because the durable exists with the
// other storage type. Same rule as the owner's recovery, and the same single
// cause: any other immutable mismatch means the durable is not one this broker
// can converge, and retrying would repeat the refusal with a different field.
//
// persisted is the winner record's own MemoryStorage, used when the relookup
// finds the durable gone again — the only value a contender may impose when
// there is nothing to adopt.
//
// One attempt is a bounded fail-closed policy, not a proof that the conflict
// cannot recur: nothing makes the refusal and the relookup atomic. What is
// guaranteed is narrower and sufficient — at this point no copy has been stored
// and no group source acknowledged, both of which happen strictly later.
func (as *mqttAccountSessionManager) retryQ2AffineStorageConflict(want *ConsumerConfig, createErr error, persisted bool) (*JSApiConsumerCreateResponse, bool) {
	if !isJSErrCausedBy(createErr, JSConsumerCreateErrF, errQ2AffStorageImmutable) {
		return nil, false
	}
	next := *want
	var gone bool
	ci, lerr := as.lookupQ2AffineConsumer(want.Durable)
	switch {
	case lerr != nil:
		if !IsNatsErr(lerr, JSConsumerNotFoundErr) {
			return nil, false
		}
		// The durable that caused the conflict is authoritatively GONE. The record's
		// value is the only one a contender may impose with nothing to adopt.
		gone, next.MemoryStorage = true, persisted
	case ci == nil || ci.Config == nil:
		return nil, false
	default:
		next.MemoryStorage = ci.Config.MemoryStorage
	}
	// Equality is only a reason to stop when the conflicting durable is still
	// THERE: then the field the refusal named no longer differs, so re-sending
	// would repeat the refusal. When it is gone, equality says nothing — the
	// obstacle has been removed, and the identical request is exactly the retry
	// that now succeeds. Refusing there would defer restoration to another AckWait
	// redelivery for no reason, holding a MaxAckPending slot meanwhile.
	if !gone && next.MemoryStorage == want.MemoryStorage {
		return nil, false
	}
	resp, rerr := as.createQ2AffineConsumer(&next)
	if rerr != nil {
		return nil, false
	}
	*want = next
	return resp, true
}

// q2AffConfigMatchesCanonical reports whether cfg is EXACTLY the affine QoS 2
// configuration this broker provisions for (idHash, gen), and returns the reason
// when it is not.
//
// A contender replays a persisted config to restore a durable somebody else owns,
// so "replayable" has to mean "identical to what we would have built", not
// "plausible". The comparison is therefore whole-struct against the single
// constructor, with cfg's own three variable fields substituted in so that a
// legitimate difference in AckWait, MaxAckPending or MemoryStorage is not read as
// a deviation — those are bounded separately, by q2AffVariableFieldsInBounds.
//
// Why not a list of fields worth checking: a list cannot close a struct that a
// dependency bump can extend, and every list an earlier revision of this design
// wrote turned out to be missing one. MaxDeliver alone, left unchecked, reopens
// the indefinite stall the feature exists to close. Whole-struct equality is
// closed by construction instead — a field neither side sets is zero on both and
// passes, and any writer that sets one deviates and is refused.
//
// It is DeepEqual rather than ==, and that is not stylistic: ConsumerConfig
// carries slices, maps and time pointers, so == does not compile and a shallow
// comparison would miss a PauseUntil or a BackOff.
//
// The comparison is against the persisted REQUEST, which is what the record
// holds: ensureQ2AffineConsumerSubscription stores &ccr.Config, the request it
// sent, not the server's normalized response. A record built from the response
// would carry server-filled fields and be refused here, so that is a property
// this check depends on rather than an accident.
func q2AffConfigMatchesCanonical(cfg *ConsumerConfig, idHash, gen string) error {
	if cfg == nil {
		// Reachable for real, not defensive: mqttValidateRecordAffinePair permits a
		// record carrying Q2AffGen with a nil Q2Aff, and restoreQ2AffineUnwind
		// produces a names-only record that buildPersistedSessionLocked persists
		// verbatim. Nothing can be replayed from it.
		return fmt.Errorf("the record names an affine QoS 2 generation but carries no consumer configuration to replay")
	}
	want := buildQ2AffineConsumerConfig(idHash, gen, cfg.AckWait, cfg.MaxAckPending, cfg.MemoryStorage)
	if !reflect.DeepEqual(*cfg, want) {
		return fmt.Errorf("the persisted affine QoS 2 consumer configuration is not the one this broker provisions: got %+v, want %+v", *cfg, want)
	}
	return nil
}

// q2AffVariableFieldsInBounds bounds the three fields q2AffConfigMatchesCanonical
// deliberately accepts from the record, at the domain a create could actually be
// provisioned in rather than merely at "non-zero".
//
// streamMaxAckPending is the live $MQTT_out ConsumerLimits.MaxAckPending; zero
// means the stream imposes none. A negative one reads as unlimited, because the
// broker option it would have to exceed is a uint16.
//
// MemoryStorage is deliberately unbounded: both of its values are ones this
// broker produces.
func q2AffVariableFieldsInBounds(cfg *ConsumerConfig, streamMaxAckPending int) error {
	if cfg.AckWait <= 0 {
		// A zero AckWait is not "the default" on this path: the owner substitutes
		// mqttDefaultAckWait before building, so a record carrying zero was not
		// written by a provisioning this broker would recognize.
		return fmt.Errorf("the persisted affine QoS 2 consumer has a non-positive ack_wait (%v)", cfg.AckWait)
	}
	if cfg.MaxAckPending <= 0 || cfg.MaxAckPending > mqttMaxAckTotalLimit {
		return fmt.Errorf("the persisted affine QoS 2 consumer has a max_ack_pending of %d, outside the provisionable range 1..%d", cfg.MaxAckPending, mqttMaxAckTotalLimit)
	}
	if streamMaxAckPending > 0 && cfg.MaxAckPending > streamMaxAckPending {
		return fmt.Errorf("the persisted affine QoS 2 consumer has a max_ack_pending of %d, above the stream's consumer limit of %d: JetStream would reject the create",
			cfg.MaxAckPending, streamMaxAckPending)
	}
	return nil
}

// mqttCallerName names the function skip frames up the stack. Test-seam support
// only; it is never called while no hook is installed.
func mqttCallerName(skip int) string {
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		return _EMPTY_
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return _EMPTY_
	}
	name := fn.Name()
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	return name
}

// lookupQ2AffineConsumer reads one affine durable's ConsumerInfo, honoring an
// instance-scoped test seam. EVERY affine consumer lookup goes through here, so
// that a test can force one to fail and thereby assert where lookups are and are
// not performed. A JSConsumerNotFoundErr is an authoritative absence; every other
// error, and a successful response carrying nil info or a nil Config, is
// indeterminate and its callers fail closed.
func (as *mqttAccountSessionManager) lookupQ2AffineConsumer(durable string) (*ConsumerInfo, error) {
	as.mu.RLock()
	hook := as.testQ2AffConsumerLookup
	as.mu.RUnlock()
	if hook != nil {
		return hook(durable)
	}
	return as.jsa.lookupConsumer(mqttOutStreamName, durable)
}

// lookupQ2AffOutStream reads $MQTT_out's live configuration for the contender's
// limit checks, honoring an instance-scoped test seam.
func (as *mqttAccountSessionManager) lookupQ2AffOutStream() (*StreamInfo, error) {
	as.mu.RLock()
	hook := as.testQ2AffOutStreamInfo
	as.mu.RUnlock()
	if hook != nil {
		return hook()
	}
	return as.jsa.lookupStream(mqttOutStreamName)
}

// createQ2AffineConsumer issues the create-or-update that restores an affine
// durable on behalf of an absent winner, honoring an instance-scoped test seam.
// Only the CONTENDER's create is routed through here: the owner's own
// provisioning is deliberately left on the direct path, so a seam installed to
// probe D1 can never perturb it.
func (as *mqttAccountSessionManager) createQ2AffineConsumer(cfg *ConsumerConfig) (*JSApiConsumerCreateResponse, error) {
	as.mu.RLock()
	hook := as.testQ2AffConsumerCreate
	as.mu.RUnlock()
	if hook != nil {
		return hook(cfg)
	}
	return as.jsa.createDurableConsumer(&CreateConsumerRequest{Stream: mqttOutStreamName, Config: *cfg})
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
func validateQoS2AssignStream(si *StreamInfo, wantReplicas int, bundleEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttQoS2AssignStreamSubjectPrefix + ">"}, wantReplicas); err != nil {
		return err
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
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a claim's CAS slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject a claim transition independently of any broker preflight", cfg.MaxMsgSize)
	}
	if cfg.DenyDelete {
		return fmt.Errorf("deny_delete is set: it blocks the claim reap")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages on claim subjects would be parsed as records", cfg.SubjectDeleteMarkerTTL)
	}
	// The bundle stamp: this stream carries the account's recovery epoch, which is
	// what makes the nine broker-owned streams one recreation unit rather than nine
	// independently recoverable stores (F-066).
	if err := validateBundleEpochStamp(&cfg, bundleEpoch); err != nil {
		return err
	}
	return nil
}

// validateOutStream verifies $MQTT_out can carry the affine copies. It is a
// DIFFERENT list from validateQoS2AssignStream's, derived from what this owner
// does to $MQTT_out: append-only stores (one of them a create-only CAS), consumer
// delivery, interest-based drop when a session's durable goes away, and — since
// F-065 — the DELETE of an exact sequence by which a materialization attempt
// withdraws a copy no claim of its incarnation can authorize. DenyDelete is
// therefore fatal; DenyPurge remains an audited NON-prerequisite, because nothing
// here purges by subject.
//
// InterestPolicy is load-bearing in BOTH directions: it is how a discarded
// session's undelivered copies disappear when its durable is deleted, and it is
// why a copy must never be stored before its consumer exists — a store with no
// interest returns success and keeps nothing.
//
// maxAckPending is the broker's effective per-consumer cap
// (opts.MQTT.MaxAckPending or mqttDefaultMaxAckPending). A stream
// ConsumerLimits.MaxAckPending below it makes JetStream reject the PUBREL and
// affine durable creates outright, so every QoS 2 resume fails; a limit at or
// above the cap changes nothing, so only a lower one is fatal. A reload that
// raises the broker cap past the stream limit is not covered by this startup
// check; the consumer-create rejection remains the backstop (stated residual
// in docs/CONFORMANCE.md).
func validateOutStream(si *StreamInfo, wantReplicas, maxAckPending int, bundleEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttOutSubjectPrefix + ">"}, wantReplicas); err != nil {
		return err
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
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy a copy subject's create-only CAS slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject an affine copy the broker max-payload preflight already admitted", cfg.MaxMsgSize)
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages would be delivered to a session's affine consumer as if they were copies", cfg.SubjectDeleteMarkerTTL)
	}
	if cfg.MaxConsumers > 0 {
		// One PUBREL durable and one affine durable per applicable session, so a
		// finite cap eventually rejects a legitimate session's resume.
		return fmt.Errorf("max_consumers is %d (want unlimited): the broker creates per-session PUBREL and affine durables, so a cap eventually rejects a legitimate session", cfg.MaxConsumers)
	}
	if cfg.ConsumerLimits.InactiveThreshold != 0 {
		// A stream-forced inactivity threshold would give every session's affine
		// durable a delete timer, reaping resumable-session state while the client
		// is offline — exactly what this feature exists to preserve.
		return fmt.Errorf("stream imposes a consumer inactivity limit (%v): it would reap a disconnected session's affine QoS 2 consumer", cfg.ConsumerLimits.InactiveThreshold)
	}
	if cfg.DenyDelete {
		// A materialization attempt withdraws the copy it proved present once nothing
		// of that copy's incarnation can authorize it. Denied, that copy stays on an
		// interest-retained stream behind the fail-closed delivery gate, NAK-looping and
		// holding a MaxAckPending slot for the session's whole life — which is F-065's
		// own harm, reintroduced by configuration.
		return fmt.Errorf("deny_delete is set: a materialization attempt could not withdraw a disowned session-affine QoS 2 copy, which would then NAK-loop behind the delivery gate")
	}
	if l := cfg.ConsumerLimits.MaxAckPending; l != 0 && l < maxAckPending {
		return fmt.Errorf("stream consumer max_ack_pending limit is %d, below the broker's effective cap %d: JetStream would reject the PUBREL and affine durable creates, failing every QoS 2 resume", l, maxAckPending)
	}
	// The bundle stamp: this stream carries the account's recovery epoch, which is
	// what makes the nine broker-owned streams one recreation unit rather than nine
	// independently recoverable stores (F-066).
	if err := validateBundleEpochStamp(&cfg, bundleEpoch); err != nil {
		return err
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
//
// Every condition below is LOCAL session state — a readiness flag, a generation and
// a consumer POINTER. None of them observes the durable consumer, so admitting a
// move here establishes nothing about the copy: since $MQTT_out is interest-retained,
// a copy stored after that consumer was deleted succeeds and keeps nothing. That is
// why admission is not evidence and materializeQ2AffCopy proves retention with a
// presence check instead (F-063).
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
// The bool result reports whether THIS call's own session-record save was rejected by its
// per-subject CAS. Its caller in a live SUBSCRIBE needs that specific fact: a CAS rejection
// is positive evidence the record MOVED, while the bare error below also covers consumer
// creation, the delivery subscription, validation and a test seam — none of which write the
// record at all. See mqttSubPacketFacts.recordCASRejected.
func (sess *mqttSession) ensureQ2AffineConsumerSubscription(c *client, tok recordTx) (bool, error) {
	// casRejected is this call's own record-write verdict; see the doc comment.
	var casRejected bool

	sess.mu.Lock()
	if sess.q2AffClosing {
		sess.mu.Unlock()
		return casRejected, fmt.Errorf("session is terminating")
	}
	idHash, id := sess.idHash, sess.id
	gen := sess.q2AffGen
	existing := sess.q2AffConsumer
	subscribed := sess.q2AffSubscribed
	ready := sess.q2AffReady
	tmaxack := sess.tmaxack
	sess.mu.Unlock()

	if ready && subscribed && existing != nil {
		return casRejected, nil
	}

	// A persisted consumer with no generation is unaccountable state: we can
	// neither derive the names it owns nor safely replace it. Refuse.
	if existing != nil && gen == _EMPTY_ {
		return casRejected, fmt.Errorf("session %q has a persisted affine QoS 2 consumer %q with no generation; refusing to resume it", id, existing.Durable)
	}
	minted := gen == _EMPTY_
	if minted {
		gen = mqttNewQ2AffGen()
	} else if existing != nil {
		if err := mqttValidateQ2AffineNames(existing, idHash, gen); err != nil {
			return casRejected, fmt.Errorf("session %q has an unaccountable affine QoS 2 consumer: %w", id, err)
		}
	}
	_, deliver, durable := mqttQ2AffineNames(idHash, gen)

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
			return casRejected, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v", after, mqttMaxAckTotalLimit)
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
			// No RECORD ever referenced this generation, so dropping the in-memory
			// identity strands nothing a reader could find; keeping it would pin the
			// session to a half-built identity. The CONSUMER is a different matter —
			// see the delete below.

			sess.q2AffGen = _EMPTY_
			sess.q2AffConsumer = nil
			sess.q2AffDeliverySubject = _EMPTY_
			sess.q2AffDeliverySubjectB = nil
		}
		sess.mu.Unlock()
		if minted {
			// The in-memory generation has just been cleared, so this is the ONLY
			// attempt and no later route can name this consumer: since F-036 course E
			// the sweep does not collect it either. A failure here is permanent, and is
			// the no-retry-anchor residual in docs/CONFORMANCE.md.
			if _, err := sess.jsa.deleteConsumer(mqttOutStreamName, durable, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
				c.Warnf("Unable to roll back affine QoS 2 consumer %q, and nothing will retry it; "+
					"it is retained and must be removed by hand: %v", durable, err)
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
			return casRejected, err
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
		Config: buildQ2AffineConsumerConfig(idHash, gen, ackWait, maxAckPending, opts.MQTT.ConsumerMemoryStorage),
	}
	resp, err := sess.jsa.createDurableConsumer(ccr)
	if err != nil {
		// One bounded recovery, from a cause that is ALREADY fatal today. See
		// recoverQ2AffineStorageConflict: it can convert this failure into a
		// success and can never manufacture one, which is what distinguishes it
		// from the preflight this path is not allowed to grow.
		if rresp, ok := sess.recoverQ2AffineStorageConflict(ccr, err); ok {
			c.Warnf("The affine QoS 2 consumer for client %q already existed with the other storage type; adopted it rather than failing the resume", id)
			resp, err = rresp, nil
		}
	}
	if err != nil {
		c.Errorf("Unable to add the affine QoS 2 JetStream consumer for client %q: %v", id, err)
		rollback()
		return casRejected, err
	}
	if resp != nil && resp.ConsumerInfo != nil {
		if verr := mqttValidateQ2AffineNames(resp.Config, idHash, gen); verr != nil {
			c.Errorf("The affine QoS 2 consumer for client %q does not match its expected identity: %v", id, verr)
			rollback()
			return casRejected, verr
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
		outcome, sdetail, err := sess.saveClassifiedDetail(tok)
		// The one failure here that carries ownership evidence.
		if mqttIsRecordCASRejection(sdetail.rawErr) {
			casRejected = true
		}
		if err != nil {
			c.Errorf("Unable to persist the affine QoS 2 consumer for client %q: %v", id, err)
			// Undo only when the write DEFINITELY did not land. An ambiguous one may
			// have stored a record that names this consumer, and detaching it in
			// memory would leave that record pointing at something the session no
			// longer knows about. See sess_uncertain.go.
			switch outcome {
			case saveDefinitelyFailed:
				rollback()
			case saveMaybeLanded:
				// The record may name the consumer, so keep the view: the resolver
				// settles the write forward by re-attempting exactly these bytes, and the
				// record then names what the session knows about.
				//
				// The SUBSCRIBE caller marks this filter failed and carries on, which is
				// truthful: its shared membership is persisted in a LATER loop, and that
				// save is refused while the session is unresolved — so the client is told
				// the filter failed and no membership for it becomes durable. What the
				// settlement DOES make durable is the affine path itself, and the packet's
				// own unused-path unwind and rollback cannot reconcile that: their
				// confirming saves are refused for the same reason. So the reclaim is owed
				// to the settlement, and registered here.
				//
				// ONLY when this call MINTED the generation. A resume that re-establishes
				// a path the record already named reaches this same save with
				// existing == nil but minted == false — the record can carry a generation
				// without a consumer config — and that generation may owe assignments to a
				// delivery already started to it, which an unwind would discard.
				if minted {
					sess.retainOnLanded(func(rtok recordTx) { sess.unwindProvisionedQ2AffineOnLanded(c, rtok) })
				}
			}
			return casRejected, err
		}
	}
	sess.mu.Lock()
	sess.q2AffReady = true
	sess.mu.Unlock()
	return casRejected, nil
}

// ---------------------------------------------------------------------------
// Claim transitions
// ---------------------------------------------------------------------------

// errQ2AffUnresolved marks an outcome that could not be PROVEN either way. It is
// never a reason to guess: a caller that sees it writes nothing, acks nothing and
// lets AckWait re-offer the delivery.
var errQ2AffUnresolved = errors.New("affine QoS 2 state could not be resolved")

// errQ2AffPrecheckRefused marks a transition or reap a caller-supplied PRECHECK
// vetoed against the freshly loaded record. It is deliberately distinct from
// errQ2AffUnresolved: "I refused this record" and "I could not tell" license
// different things, and collapsing them is what made an earlier design unable to
// tell an already-settled claim (still reap it) from a vetoed one (do not).
var errQ2AffPrecheckRefused = errors.New("affine QoS 2 record refused by the caller's precheck")

// q2AffShapePrecheck refuses a record whose SHAPE this broker does not write, for
// every checked transition and reap the orphan sweep and the owner's sourceless
// recovery attach it to.
//
// Three checks, and each is here because the other two do not make it:
//
//   - validateOrphanQ2AffClaim is the only one that rejects an unknown Phase and
//     the AffineDone∧Lost contradiction.
//   - mqttValidQ2AffGen is the account-wide generation floor, which
//     validateOrphanQ2AffClaim omits only because its own callers had already
//     passed it.
//   - q2AffClaimUsableForDecision is the only one that validates the INCARNATION,
//     without which a record valid at a checked reap's first load can be rewritten
//     with a malformed nonce and a recomputed, internally consistent CopySubject and
//     still receive SourceAcked.
//
// It is NOT "every shape this broker does not write": these reject the enumerated
// structural defects, not the ORDER in which this broker's writers may set these
// fields. A self-consistent record violating that order passes. Under the
// compatibility boundary no such record exists, because no release produced one.
// docs/CONFORMANCE.md states that residual in the direction it actually runs —
// false-negative acceptance.
func q2AffShapePrecheck(subject string) func(*mqttQ2AffClaim) error {
	return func(c *mqttQ2AffClaim) error {
		if verr := validateOrphanQ2AffClaim(subject, c); verr != nil {
			return fmt.Errorf("%w: %w", errQ2AffPrecheckRefused, verr)
		}
		if !mqttValidQ2AffGen(c.Gen) {
			return fmt.Errorf("%w: generation %q is not a valid identity", errQ2AffPrecheckRefused, c.Gen)
		}
		if verr := q2AffClaimUsableForDecision(subject, c); verr != nil {
			return fmt.Errorf("%w: %w", errQ2AffPrecheckRefused, verr)
		}
		return nil
	}
}

// advanceQ2AffClaim applies mutate to the claim on subject and commits it with a
// CAS at the sequence the claim was loaded at, retrying against the record's
// latest state. Facts already proven by the loaded record are preserved
// (mergeProven), so a transition can only ever ADD to what is known.
//
// wantWinner/wantGen, when set, assert whose claim this must be; a mismatch
// returns the loaded claim with errQ2AffUnresolved and mutates nothing, in the
// same spirit as finalizeQoS2Move's Superseded. (The claim subject is keyed by
// group durable and SOURCE SEQUENCE, and a stream sequence is never reused WITHIN
// ONE INCARNATION of $MQTT_msgs, so the subject already identifies one source
// message for as long as that stream is not recreated (F-066); the identity check
// guards against acting on a winner that is not the one the caller reasoned about.)
//
// A missing claim returns (nil, 0, nil): there is nothing to advance, and absence
// is never manufactured into a new claim here.
func (as *mqttAccountSessionManager) advanceQ2AffClaim(subject, wantWinner, wantGen, wantNonce, wantCopy string,
	mutate func(*mqttQ2AffClaim)) (*mqttQ2AffClaim, uint64, error) {

	return as.advanceQ2AffClaimChecked(subject, wantWinner, wantGen, wantNonce, wantCopy, nil, mutate)
}

// advanceQ2AffClaimChecked is advanceQ2AffClaim with a caller-supplied VETO, and the
// placement of that veto is the whole correctness property.
//
// precheck runs on EVERY attempt, against the record this attempt will actually CAS,
// after the load and after the existing winner/nonce/copy fences — and BEFORE
// `next := *cur`, before mutate, and before mergeProven. A refusal returns
// errQ2AffPrecheckRefused immediately and neither mutate nor mergeProven runs.
//
// Nothing weaker works, and this is measured rather than assumed. Carrying the same
// condition inside `mutate` cannot veto at all: mergeProven runs after it
// unconditionally, and on an AffineDone∧Lost record mergeProven clears Lost by itself,
// so `next != cur` and the CAS lands with the closure having mutated nothing —
// normalising the very shape the condition existed to refuse. The signature
// `mutate func(*mqttQ2AffClaim)` simply cannot express an abort, so no placement
// inside it could have been right.
//
// A nil precheck makes this exactly advanceQ2AffClaim, which is what lets the ten
// existing callers keep their behaviour without an edit.
func (as *mqttAccountSessionManager) advanceQ2AffClaimChecked(subject, wantWinner, wantGen, wantNonce, wantCopy string,
	precheck func(*mqttQ2AffClaim) error, mutate func(*mqttQ2AffClaim)) (*mqttQ2AffClaim, uint64, error) {

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
		// The INCARNATION gate, and it is here — before mutate, and before the
		// no-op-success return below — rather than a check on the outcome. An
		// incarnation-specific fact (the phase, AffineDone, Lost, and the source-gone
		// discard, each an inference about THIS attempt's own copy) must not be written
		// onto a claim that a later election put on this subject, because Winner+Gen
		// alone cannot tell two incarnations apart. Deciding after the write is too
		// late: the write has already moved the successor's record. F-065.
		//
		// _EMPTY_ opts out, independently of the winner check, because the two are
		// different questions: SourceAcked is a fact about the source MESSAGE and the
		// teardown discards are facts about the SESSION, so each applies to whichever
		// incarnation holds the subject.
		if wantNonce != _EMPTY_ && cur.Nonce != wantNonce {
			return cur, seq, errQ2AffUnresolved
		}
		// The COPY fence, for a caller that reached here holding one. It is inside this
		// loop, not at the caller, because the caller's validation was performed against a
		// DIFFERENT load: this function reads the record again, and between the two reads
		// the record can be deleted or rewritten. A same-nonce record whose other identity
		// fields no longer follow from its subject would otherwise be mutated on the
		// strength of a snapshot that is gone, and the caller would then treat the write as
		// proof and acknowledge away the copy.
		//
		// Attributability is asserted here rather than everywhere because it belongs to the
		// same question: a caller that names a copy is acting on THAT copy's claim, so a
		// record that cannot be attributed to its own subject cannot be the one. The
		// account-wide floor (mqttQ2AffClaimIdentityOK) is deliberately left alone for the
		// callers that carry no copy — SourceAcked and the teardown discards — for the
		// reason q2AffClaimUsableForDecision's own comment gives.
		if wantCopy != _EMPTY_ {
			if verr := q2AffClaimUsableForDecision(subject, cur); verr != nil {
				return cur, seq, errQ2AffUnresolved
			}
			if cur.CopySubject != wantCopy {
				return cur, seq, errQ2AffUnresolved
			}
		}
		// THE VETO, and its placement is the correctness property: after the load and
		// after every fence above, but before `next := *cur`, before mutate and before
		// mergeProven. Refusing here is the only thing that stops a write; mergeProven
		// runs unconditionally below and can itself change the record (it clears Lost on
		// an AffineDone∧Lost pair), so a condition evaluated inside mutate cannot veto.
		if precheck != nil {
			if verr := precheck(cur); verr != nil {
				return cur, seq, verr
			}
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
// The re-read is not enough on its own, which is what F-065 was: the copy state must
// be read AFTER the SourceAcked transition this function may perform, because that
// transition is a full JetStream round trip with retries and reapable() would
// otherwise be evaluated on a reading older than this function's own last write — so
// a copy stored during it would be invisible, and the claim deleted out from under a
// copy that exists. The order below is load, transition, read the copy, decide,
// delete, and NOTHING is added between the copy read and the delete.
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
	_ = as.reapQ2AffClaimChecked(s, subject, nil)
}

// mqttQ2AffReapOutcome is what a reap ATTEMPT established, and it exists because a
// caller that must act on the result — deleting an in-flight register only once the
// claim it references is provably gone — cannot distinguish "reaped", "deliberately
// retained" and "I could not tell" from a function that returns nothing.
//
// Every silent return of the reap body maps to exactly one of these; the mapping is
// the contract, not an implementation detail, so it is stated here and pinned by its
// own mutations.
type mqttQ2AffReapOutcome int

const (
	// mqttQ2AffReaped: the claim record is GONE — deleted by this call, already
	// absent when it loaded, or reported not-found by the delete. All three are the
	// same fact for a caller that needs the record not to exist.
	mqttQ2AffReaped mqttQ2AffReapOutcome = iota
	// mqttQ2AffRetained: the record is readable, attributable and DELIBERATELY kept,
	// because reapable() refuses it. A positive answer, not a failure.
	mqttQ2AffRetained
	// mqttQ2AffReapIndeterminate: nothing was established. A load failure, a refused
	// validation or precheck, a failed source transition, an unreadable copy, or a
	// delete that failed for any other reason. It must never be read as either of the
	// others: treating it as reaped would license deleting a register whose claim may
	// still be live.
	mqttQ2AffReapIndeterminate
)

// reapQ2AffClaimChecked is reapQ2AffClaim with a caller-supplied VETO and a typed
// outcome. A nil precheck makes it exactly reapQ2AffClaim, which is what lets the
// twelve existing callers keep their behaviour without an edit — they invoke it as a
// statement and discard the outcome.
//
// The precheck runs at TWO points and deliberately not a third: on this function's own
// initial load, before it interprets or deletes anything; and inside the conditional
// SourceAcked advance, on every retry's freshly loaded record. NOT again after that
// advance returns — the precheck is an invariant that transition PRESERVES, since only
// SourceAcked is added and a prechecked record cannot trigger mergeProven's
// AffineDone∧Lost normalisation, and a post-return call is not a fresh load so it could
// not retroactively veto a write that already happened.
func (as *mqttAccountSessionManager) reapQ2AffClaimChecked(s *Server, subject string,
	precheck func(*mqttQ2AffClaim) error) mqttQ2AffReapOutcome {

	claim, seq, err := as.loadQ2AffClaim(subject)
	if err != nil {
		return mqttQ2AffReapIndeterminate
	}
	if claim == nil {
		// Authoritatively absent: somebody else reaped it. That IS the fact a caller
		// gating on "the claim is gone" needs, so it is reaped rather than indeterminate.
		return mqttQ2AffReaped
	}
	if verr := q2AffClaimUsableForDecision(subject, claim); verr != nil {
		// This function DELETES a claim on what its copy subject says, so a record
		// whose subject does not follow from its own identity is refused rather than
		// interpreted: it would otherwise inspect a copy the record does not own and
		// delete the claim over a live one.
		s.Warnf("MQTT: refusing to reap shared QoS 2 assignment %q: %v", subject, verr)
		return mqttQ2AffReapIndeterminate
	}
	if precheck != nil {
		if verr := precheck(claim); verr != nil {
			// Refused on THIS function's own load, before it interprets or deletes
			// anything. Indeterminate, never retained: a refusal proves nothing about the
			// record, so it must not license a caller's dependent deletion.
			s.Warnf("MQTT: refusing to reap shared QoS 2 assignment %q: %v", subject, verr)
			return mqttQ2AffReapIndeterminate
		}
	}

	if !claim.SourceAcked && as.q2AffSourceUnofferable(claim) {
		// No identity of any kind: an unofferable source is a fact about the source
		// MESSAGE, true of whichever incarnation holds this subject. The precheck is
		// threaded in, because this advance is the one write this function issues and it
		// must not land on a record the caller would refuse.
		claim, seq, err = as.advanceQ2AffClaimChecked(subject, _EMPTY_, _EMPTY_, _EMPTY_, _EMPTY_,
			precheck, func(c *mqttQ2AffClaim) { c.SourceAcked = true })
		if err != nil || claim == nil {
			return mqttQ2AffReapIndeterminate
		}
		// Re-validated, because that transition is identity-unfenced BY DESIGN — a
		// SourceAcked write is a fact about the source message, so it applies to whichever
		// incarnation holds the subject — and it therefore returns whatever record it
		// loaded, which may be a concurrently rewritten occupant. The copy subject below
		// comes from that record and decides whether this claim is DELETED, so trusting an
		// unvalidated one would let a malformed rewrite redirect the absence check and
		// strand the original copy with no claim.
		// Re-validated, because that transition is identity-unfenced BY DESIGN — a
		// SourceAcked write is a fact about the source message, so it applies to whichever
		// incarnation holds the subject — and it therefore returns whatever record it
		// loaded, which may be a concurrently rewritten occupant. The copy subject below
		// comes from that record and decides whether this claim is DELETED, so trusting an
		// unvalidated one would let a malformed rewrite redirect the absence check and
		// strand the original copy with no claim.
		if verr := q2AffClaimUsableForDecision(subject, claim); verr != nil {
			s.Warnf("MQTT: refusing to reap shared QoS 2 assignment %q after its source transition: %v", subject, verr)
			return mqttQ2AffReapIndeterminate
		}
	}

	// AFTER the transition above, and with nothing but the delete after it. This is
	// R1: the reading reapable() consumes must be no older than the last write this
	// function issues. F-065.
	copyState, _ := as.loadQ2AffCopy(claim.CopySubject)
	if copyState == mqttQ2AffCopyUnknown {
		return mqttQ2AffReapIndeterminate // fail closed: an unreadable copy is not an absent copy
	}
	if !claim.reapable(copyState == mqttQ2AffCopyAbsent) {
		// Readable, attributable and deliberately kept. A POSITIVE answer.
		return mqttQ2AffRetained
	}
	// Delete the EXACT sequence loaded (qos2_move.go invariant 2): deleting a
	// sequence cannot reach a record written after it.
	if derr := as.jsa.deleteMsg(mqttQoS2AssignStreamName, seq, true); derr != nil {
		if isErrorOtherThan(derr, JSSequenceNotFoundErrF) &&
			!isJSErrCausedBy(derr, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
			s.Warnf("MQTT: unable to reap the completed shared QoS 2 assignment %q: %v", subject, derr)
			return mqttQ2AffReapIndeterminate
		}
		// Either authoritative not-found form: the record is gone, which is the
		// ordinary-success reading this delete has always applied.
		return mqttQ2AffReaped
	}
	return mqttQ2AffReaped
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
//
// Through the positive source-ACK gate (outbound_source_ack_gate.go): the group delivery is a
// $MQTT_msgs source on a shared durable, so a HandBack this session holds for it refuses the
// acknowledgement, and SourceAcked is recorded ONLY on a confirmed acknowledgement — never on a
// hand-back, which is not a fact about the message.
func (as *mqttAccountSessionManager) confirmSourceAckAndRecord(sess *mqttSession, claimSubject, reply string) error {
	r, err := sess.submitSourceAck(reply, true, nil)
	if err != nil {
		return err
	}
	if r != mqttSourceAckConfirmed {
		return fmt.Errorf("the group source acknowledgement of %q was not confirmed (result %d)", claimSubject, r)
	}
	// No identity: a confirmed group ack is a fact about the source MESSAGE.
	_, _, err = as.advanceQ2AffClaim(claimSubject, _EMPTY_, _EMPTY_, _EMPTY_, _EMPTY_, func(c *mqttQ2AffClaim) { c.SourceAcked = true })
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
	// This attempt's CANDIDATE incarnation. It becomes the claim's Nonce only if the
	// create-only election below wins; a losing candidate's value is discarded, and
	// materializeQ2AffCopy re-derives both identities from the PERSISTED claim so a
	// loser's nonce can never reach a stored copy. The width is fixed, so the size
	// preflight below is valid whichever value ends up persisted. F-065.
	nonce := mqttNewQ2AffGen()
	copySubject := mqttQ2AffCopySubject(idHash, gen, claimHash, nonce)
	_, _, affDurable := mqttQ2AffineNames(idHash, gen)

	// A message whose Message Expiry Interval has elapsed was never STARTED to
	// anyone, so no assignment exists to protect: drop it exactly as the ordinary
	// delivery path does. Spec5 [3.3.2.3.3].
	props := in.props
	if len(props) > 0 {
		var expired bool
		if props, expired = mqttForwardExpiry(props, ts); expired {
			_, _ = sess.submitSourceAck(in.reply, false, nil)
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
		msgID:        mqttQ2AffMsgId(in.groupDurable, sseq, nonce),
	}
	// Backstop for a message that never met the affine admission bound: a
	// PUBLISH is bounded at admission (mqttMaxQoS2FormSize), but a QoS 1/2 Will
	// store bypasses that preflight, so a QoS 2 Will can reach a QoS 2 shared
	// member unbounded. A copy that can never be stored would wedge the group
	// durable in eternal retry; drop it with a logged error instead — the same
	// disposal an undeliverable oversize PUBLISH already gets.
	// The CLAIM record is checked here too, and not only in the admission preflight,
	// because a QoS 1/2 Will store bypasses that preflight entirely: an election whose
	// record cannot be stored would leave the group delivery retrying forever with no
	// claim able to exist. Both forms are checked before either is written.
	if in.maxPayload > 0 && mqttQ2AffClaimRecordMax() > int(in.maxPayload) {
		s.Errorf("MQTT: discarding a shared QoS 2 message on %q: its assignment claim record (%d bytes) exceeds the account max payload (%d)",
			in.natsSubject, mqttQ2AffClaimRecordMax(), in.maxPayload)
		_, _ = sess.submitSourceAck(in.reply, false, nil)
		res = mqttQ2AffMoveDropped
		return res
	}
	if !mqttQ2AffineRecordFits(env, in.maxPayload) {
		s.Errorf("MQTT: discarding a shared QoS 2 message on %q: its session-affine form (%d bytes) exceeds the account max payload (%d); it was stored without the affine admission bound, which a QoS 1/2 Will bypasses",
			in.natsSubject, mqttComputeQ2AffineSize(env), in.maxPayload)
		_, _ = sess.submitSourceAck(in.reply, false, nil)
		res = mqttQ2AffMoveDropped
		return res
	}

	// 1. Elect. A create-only CAS: exactly one session can win, forever.
	claim := &mqttQ2AffClaim{
		Winner:       idHash,
		Gen:          gen,
		Nonce:        nonce,
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

	if verr := q2AffClaimUsableForDecision(claimSubject, cur); verr != nil {
		// Refused, not interpreted: every arm below acts on this record's derived copy
		// identity, and the source delivery is simply left for AckWait.
		s.Warnf("MQTT: refusing to act on shared QoS 2 assignment %q: %v", claimSubject, verr)
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
			_, _ = sess.submitSourceAck(in.reply, false, nil)
		} else if aerr := as.confirmSourceAckAndRecord(sess, claimSubject, in.reply); aerr != nil {
			return res
		}
		as.reapQ2AffClaim(s, claimSubject)
		res = mqttQ2AffMoveTerminal

	case cur.Phase == mqttQ2AffMaterialized:
		// A foreign session owns it and a copy was proven present at some earlier
		// instant. Whether one is there NOW is a different fact — interest retention
		// drops a copy with its consumer — and the source ack is irreversible, so the
		// reading is taken HERE, by the decision that acts on it, and the release is
		// authorized from the claim's own facts (F-063). An unreadable copy answers
		// "not present", which leaves only an already-recorded SourceAcked; the claim
		// is nonterminal by the case above. Nothing authorized means prove nothing,
		// write nothing, ack nothing — F-064 owns what that strands.
		st, _ := as.loadQ2AffCopy(cur.CopySubject)
		if !cur.sourceAckAuthorized(st == mqttQ2AffCopyPresent) {
			return res
		}
		if aerr := as.confirmSourceAckAndRecord(sess, claimSubject, in.reply); aerr != nil {
			return res
		}
		as.reapQ2AffClaim(s, claimSubject)
		res = mqttQ2AffMoveForeign

	default:
		// A foreign session owns it and no copy has been PROVEN yet — which does not
		// say the subject is empty: the store completes before the materialized CAS,
		// so a copy may already be there, and the create-only CAS is what makes
		// storing again safe. Waiting for that
		// session to come back is not bounded by anything: a perfectly valid
		// persistent winner may simply never reconnect, and the group delivery
		// would stall forever. Materialize the copy ON ITS BEHALF, onto ITS
		// durable, and then release the group delivery.
		if as.materializeQ2AffOnBehalf(sess, s, cur, claimSubject, curSeq, env, in) {
			res = mqttQ2AffMoveOnBehalf
		}
	}
	return res
}

// completeQ2AffOwnClaim finishes a move for a claim this session owns: prove the
// copy present, commit the materialized phase, then release the group delivery. Each
// step is idempotent, so a retry after any partial failure converges.
//
// A failed materialization does NOT end the attempt, because a terminal fact
// authorizes the source ack on its own (sourceAckAuthorized): returning here would
// make Discarded, AffineDone, Lost and an already-recorded SourceAcked unreachable
// for exactly the claims that need them, and the group delivery would be re-offered
// forever with nothing left able to release it. So the record is re-read — our
// snapshot predates the JetStream round trips above — and the ack proceeds only if
// that fresh record authorizes it.
//
// Returns false when nothing could be proven — the caller then acks nothing.
func (sess *mqttSession) completeQ2AffOwnClaim(s *Server, as *mqttAccountSessionManager,
	claim *mqttQ2AffClaim, claimSubject string, claimSeq uint64, env *mqttQ2AffineEnvelope, reply string) bool {

	if !as.materializeQ2AffCopy(s, claim, claimSubject, claimSeq, env) {
		cur, _, err := as.loadQ2AffClaim(claimSubject)
		// An unattributable record authorizes nothing either: releasing the source on one
		// would be irreversible.
		if err != nil || cur == nil || q2AffClaimUsableForDecision(claimSubject, cur) != nil ||
			!cur.sourceAckAuthorized(false) {
			// Nothing proven and nothing terminal: prove nothing, write nothing, ack
			// nothing. The source delivery is the only recoverable asset, so it is left
			// for AckWait to re-offer.
			return false
		}
		claim = cur
	}
	if claim.SourceAcked {
		_, _ = sess.submitSourceAck(reply, false, nil)
		return true
	}
	if err := as.confirmSourceAckAndRecord(sess, claimSubject, reply); err != nil {
		// Releasing the group delivery is the only thing outstanding: either the copy
		// was proven present on our own durable, or the claim is terminal. The unacked
		// group delivery just redelivers, and the next attempt re-proves the same
		// authorization.
		s.Warnf("MQTT: the source ack of shared QoS 2 assignment %q is unconfirmed: %v", claimSubject, err)
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

// materializeQ2AffCopy proves the affine copy is PRESENT — storing it first when it
// is not there and may be stored — and commits the claim's claimed->materialized
// transition. Returns true only when the copy was proven present by a fresh lookup
// and the claim is proven to say so, which is what licenses the caller's
// irreversible source ack.
//
// The copy is stored before the phase advances, never after: the phase is what
// authorizes delivery, and a materialized claim with no copy would let the delivery
// gate open on nothing.
//
// A store RESPONSE is not that proof, in either direction, and both outcomes
// therefore converge on ONE presence check (F-063):
//
//   - A SUCCESS proves nothing. $MQTT_out is interest-retained, so a store onto a
//     subject no consumer filters succeeds and keeps nothing — see "JetStream
//     behaviors this design depends on" in docs/ARCHITECTURE.md. That is the whole
//     failure: a copy stored after the winner's consumer was deleted returns an
//     ordinary success carrying a sequence that holds nothing.
//   - A REJECTED create-only CAS means "the subject is not empty", which is not "a
//     copy is there now": it can have been consumed and acked away in between.
//
// An ALREADY-materialized claim is re-verified, and never re-stored. Re-verified
// because the phase is a record of the past and its copy may have gone with its
// consumer since. Never re-stored because an absent copy under it is ambiguous —
// consumed by its own delivery, or dropped — and a replacement would be a copy no
// delivery may publish (the gate disposes of it under AffineDone) or, once its claim
// is reaped, one that NAKs forever.
func (as *mqttAccountSessionManager) materializeQ2AffCopy(s *Server,
	claim *mqttQ2AffClaim, claimSubject string, claimSeq uint64, env *mqttQ2AffineEnvelope) bool {

	if verr := q2AffClaimUsableForDecision(claimSubject, claim); verr != nil {
		s.Warnf("MQTT: refusing to materialize shared QoS 2 assignment %q: %v", claimSubject, verr)
		return false
	}
	if claim.Discarded {
		return false // the winner's session ended; nothing may be materialized for it
	}
	// Both derived identities come from the PERSISTED claim, never from the envelope
	// the mover built before the election. A candidate that LOST the election still
	// carries its own nonce in env.msgID, and storing under it would give one durable
	// incarnation two dedup keys — which is the attempt-scoped identity F-063 refuted.
	// The width is identical, so the mover's size preflight still holds. F-065.
	env.msgID = mqttQ2AffMsgId(claim.GroupDurable, claim.SourceSeq, claim.Nonce)

	state, smsg := as.loadQ2AffCopy(claim.CopySubject)
	if state == mqttQ2AffCopyUnknown {
		return false // fail closed: unreadable is not absent
	}
	// provenSeq is the sequence of a copy this call PROVED present, taken from the
	// proof and never from a store response: the response is absent in exactly the
	// landed-but-unreported case that most needs it, and on an interest-retained
	// stream it can name a store that kept nothing (F-063).
	var provenSeq uint64
	if smsg != nil {
		provenSeq = smsg.Sequence
	}
	if state == mqttQ2AffCopyAbsent {
		if claim.Phase == mqttQ2AffMaterialized {
			// Proven present once, absent now. Nothing here can tell a delivered copy
			// from a dropped one, and storing a replacement is wrong either way. Prove
			// nothing; the caller consults the claim's own terminal facts for the
			// source ack, and what a refusal strands is F-064's to resolve.
			return false
		}
		// Absent under a claim that is not materialized: the delivery gate refuses to
		// publish such a copy, so nothing consumed it, and a copy that WAS stored and
		// then dropped with its consumer is owed a replacement just as much as one
		// that never landed. Either way this store is the right action — it is the
		// proof afterwards, not the store, that decides anything.
		//
		// EVERY store outcome now converges on that proof, including an error: a
		// response is not evidence in either direction, and "the write did not land"
		// is not a verdict a failed response establishes (F-063,
		// project_ambiguous_write_reattempt). The old early return on a non-CAS error
		// abandoned exactly the copy that had landed with its response lost.
		msg, hdrLen := mqttNewQ2AffineRecord(env)
		_, _ = as.storeQ2AffCopy(claim.CopySubject, hdrLen, msg)
		st, sm := as.loadQ2AffCopy(claim.CopySubject)
		if st != mqttQ2AffCopyPresent {
			return false
		}
		provenSeq = sm.Sequence
	}

	as.mu.RLock()
	pre := as.testQ2AffPreMaterialize
	as.mu.RUnlock()
	if pre != nil {
		if err := pre(claimSubject, claimSeq); err != nil {
			return false
		}
	}

	updated, _, err := as.advanceQ2AffClaim(claimSubject, claim.Winner, claim.Gen, claim.Nonce, _EMPTY_,
		func(c *mqttQ2AffClaim) { c.Phase = mqttQ2AffMaterialized })
	if err == nil && updated != nil {
		// The transition reloads the record and matches identity fields only, so a
		// concurrent rewrite that preserves winner, generation and incarnation while
		// making CopySubject inconsistent lands `materialized` on a record nothing can
		// attribute. Reporting success then licenses the caller's IRREVERSIBLE source
		// acknowledgement, and the delivery gate — which does validate — would NAK that
		// copy forever. Same post-transition trust window the reaper closes, closed the
		// same way: prove nothing rather than act on an unattributable record.
		if verr := q2AffClaimUsableForDecision(claimSubject, updated); verr != nil {
			s.Warnf("MQTT: refusing to complete shared QoS 2 assignment %q: its record changed to one that cannot be attributed: %v", claimSubject, verr)
			return false
		}
	}
	if as.q2AffCopyIsDisowned(claimSubject, claim, updated, err) {
		// Nothing can ever authorize this copy again, so withdraw the exact sequence
		// this call proved present. Compensation, not arbitration: the claim and the
		// copy are on different streams and no write can commit to both, so the honest
		// bound is "the attempt that proved the copy resolves it" — see F-065's
		// re-scoped guarantee in docs/CONFORMANCE.md for what a lost attempt leaves.
		as.withdrawQ2AffCopy(s, claim.CopySubject, provenSeq)
		return false
	}
	if err != nil || updated == nil {
		return false
	}
	// A claim that turned discarded while we were storing keeps its copy dropped
	// by the delivery gate; the copy itself dies with the session's consumer.
	return updated.Phase == mqttQ2AffMaterialized
}

// q2AffCopyIsDisowned reports whether the copy this call proved present can never
// be delivered by anybody, which is what licenses withdrawing it. cur/err are the
// claimed->materialized transition's own outcome; claim is the snapshot this call
// acted on.
//
// The reading must be AUTHORITATIVE and it must be the most recent one available:
// an advance that reached the record supplies it directly, and only an
// indeterminate advance costs an extra load. Anything unreadable RETAINS the copy —
// "I could not tell" must never destroy an accepted message.
//
// Three positive readings disown a copy, and each is monotone, so none can be
// undone by a later writer:
//
//   - no claim on the subject. Nothing authorizes the copy, and with the
//     incarnation nonce nothing later will: a replacement election mints its own
//     and reads a different subject.
//   - a claim naming a different winner, generation or INCARNATION. The copy
//     subject is derived from all three, so such a claim can never adopt this copy.
//   - a TERMINAL claim of this very incarnation. The gate publishes only under
//     materialized-and-nonterminal, so the copy is either a duplicate of a PUBLISH
//     the client already answered (AffineDone) or a delivery that died with its
//     session (Discarded, [MQTT-4.8.2-5]); the gate's own terminal branch disposes
//     of it for that reason, and withdrawing reaches the same state without
//     depending on the consumer being live.
func (as *mqttAccountSessionManager) q2AffCopyIsDisowned(claimSubject string,
	claim, cur *mqttQ2AffClaim, err error) bool {

	if err != nil && !errors.Is(err, errQ2AffUnresolved) {
		// Indeterminate: one fresh, authoritative reading, and a load error retains.
		var lerr error
		cur, _, lerr = as.loadQ2AffClaim(claimSubject)
		if lerr != nil {
			return false
		}
	}
	if cur == nil {
		return true
	}
	if q2AffClaimUsableForDecision(claimSubject, cur) != nil {
		// Unattributable: it cannot establish that the copy is disowned any more than it
		// could authorize a delivery. Retain, and leave it to an actor with evidence.
		return false
	}
	if !cur.ownedBy(claim.Winner, claim.Gen) || cur.Nonce != claim.Nonce {
		return true
	}
	return cur.terminal()
}

// withdrawQ2AffCopy deletes the EXACT sequence a materialization attempt proved
// present. The exactness is the safety property: a sequence cannot name a copy
// stored after it, so no later incarnation's copy can be destroyed by a stale
// withdrawal (qos2_move.go invariant 2, applied to the copy stream).
//
// A no-interest store consumes a sequence that holds nothing, so "not found" is an
// ordinary success here rather than a failure.
func (as *mqttAccountSessionManager) withdrawQ2AffCopy(s *Server, copySubject string, seq uint64) {
	if seq == 0 {
		return
	}
	if derr := as.jsa.deleteMsg(mqttOutStreamName, seq, true); derr != nil &&
		isErrorOtherThan(derr, JSSequenceNotFoundErrF) &&
		!isJSErrCausedBy(derr, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
		// Stated residual: nothing else collects this copy. See docs/CONFORMANCE.md.
		s.Warnf("MQTT: unable to withdraw the disowned shared QoS 2 copy %q at sequence %d: %v",
			copySubject, seq, derr)
	}
}

// materializeQ2AffOnBehalf proves a copy onto a foreign winner's durable when the
// winner is not here to do it itself, and then releases the group delivery.
//
// Step (2) below looks the winner's consumer up, but that lookup is not what
// authorizes the release: it precedes the store, and the consumer can be deleted in
// between. The proof is materializeQ2AffCopy's presence check, which is why this
// path and the own-claim path converge on that one function (F-063).
//
// The verification order is load-bearing. The winner's SESSION RECORD is checked
// FIRST, and for the exact generation: an orphaned consumer can outlive the
// record that named it, so "the consumer exists" would happily materialize onto a
// durable belonging to a session that is gone. Only once the record shows the
// generation live is the consumer itself verified — without it the copy would
// be stored with no interest and silently discarded.
//
// Note the asymmetry, because the two directions are not equally supported. A record
// PRESENT for this generation is positive evidence and licenses materializing. NEITHER
// negative reading is its converse:
//
//   - ABSENT does not establish the winner stopped serving. A resume preserves the
//     generation, so it may still be serving on another broker after its record was
//     removed — F-036's false-absence inference.
//   - A valid record for a DIFFERENT generation establishes that ownership MOVED, which is
//     not the same fact. F-036's shape is precisely a previous owner continuing to serve
//     after ownership moved, and it begins BEFORE the record goes absent.
//
// The destructive branch below fires on both, so it acts on a stronger conclusion than
// either supports. F-033's consequence audit has since answered what that costs, and the
// answer is that this branch is not where the defect is: discarding on an ABSENT record
// rests on F-036's own falsified premise and is recorded as F-036 population, while the
// DIFFERENT-generation reading is settled on the clean-start route — teardown marks every
// assignment of the retiring generation Discarded before deleting its consumer, so the
// session really has ended and [MQTT-4.8.2-5] permits the delivery to die with it. The
// residue in both directions is a previous owner still serving after ownership moved,
// which is F-036's shape and not this function's to fix. See F-033, "What the audit
// established", in docs/findings/ha-arbitration.md; that section is authoritative.
//
// Anything indeterminate leaves the claim untouched and acks nothing.
func (as *mqttAccountSessionManager) materializeQ2AffOnBehalf(sess *mqttSession, s *Server, claim *mqttQ2AffClaim,
	claimSubject string, claimSeq uint64, env *mqttQ2AffineEnvelope, in *mqttQ2AffMoveInput) bool {

	if verr := q2AffClaimUsableForDecision(claimSubject, claim); verr != nil {
		// Every step below acts on this record — it discards, restores a consumer from
		// it, and materializes onto its derived copy subject — so an unattributable
		// record is refused before any of them. Contender-side, so the refusal is free.
		s.Warnf("MQTT: refusing to act on behalf of shared QoS 2 assignment %q: %v", claimSubject, verr)
		return false
	}

	// (1) Is the winning generation still a live session?
	ps, _, err := as.loadFenceSession(claim.Winner)
	if err != nil {
		return false // indeterminate: fail closed, a later contender resolves it
	}
	// A record that exists is USED here in both directions — a mismatch discards the
	// assignment, a match materializes a copy and acknowledges the group source — so
	// it is validated before either, not inside one arm. Both readings are wrong on
	// an uninterpretable record: the mismatch arm would turn a malformed shape into
	// proof of absence (an empty affine generation mismatches EVERY claim), and the
	// match arm would ACK the source irreversibly for a copy owed to a session that
	// can never resume, since the same record is refused at CONNECT and its affine
	// reconciliation is suspended in the sweep. Indeterminate leaves the claim
	// untouched and acknowledges nothing, which is this function's contract.
	// discardAbsentWinnerClaim validates before the same decision, for the same
	// reason.
	if ps != nil {
		if verr := mqttValidateSessionRecord(ps, claim.Winner); verr != nil {
			s.Warnf("MQTT: refusing to act on shared QoS 2 assignment %q: the winner's record is uninterpretable: %v", claimSubject, verr)
			return false
		}
	}
	if ps == nil || ps.Q2AffGen != claim.Gen {
		// No record for this generation in the evidence available here, or a later
		// generation of the same client ID holds it. The delivery dies with the session it
		// was assigned to, which [MQTT-4.8.2-5] permits; it is never released back to the
		// group. Neither condition proves the winner stopped serving — absence is bounded
		// evidence and a different generation means ownership moved, which F-036 says the
		// previous owner can outlive. See this function's doc: F-033's audit sorted that
		// cost onto F-036's premise rather than onto this stream's.
		//
		// No nonce: the winning GENERATION ended or moved, a session fact true of
		// every incarnation of this subject.
		if _, _, derr := as.advanceQ2AffClaim(claimSubject, claim.Winner, claim.Gen, _EMPTY_, _EMPTY_,
			func(c *mqttQ2AffClaim) { c.Discarded = true }); derr != nil {
			return false
		}
		if aerr := as.confirmSourceAckAndRecord(sess, claimSubject, in.reply); aerr != nil {
			return false
		}
		as.reapQ2AffClaim(s, claimSubject)
		return true
	}

	// (2) Bring the winner's affine consumer to the configuration its record
	// names, and refuse anything this broker cannot replay EXACTLY.
	//
	// $MQTT_out is interest-retained, so the consumer is not a detail of delivery:
	// without it the copy store returns success and keeps nothing. And
	// CONSUMER.DELETE carries no precondition, so the durable can be destroyed out
	// of band while its owner is away — which is F-064's premise. Restoring it here
	// is what makes the stall resolvable without the owner.
	//
	// Every refusal below is CONTENDER-side and costs nothing: nothing has been
	// stored, no source has been acked, and no client is connected to be told. The
	// same check on the owner's path would cost a live connection, so it is not
	// made there. See F-064 decision 4 and docs/ARCHITECTURE.md.
	if ps.Teardown != nil {
		// The winner's session is being torn down, and its consumer is deleted
		// before the record is released. Recreating it here would race that
		// deletion. This NARROWS the window and does not close it: consumer
		// creation has no precondition on the record's sequence, so a marker
		// written after this read is not seen. See docs/CONFORMANCE.md.
		return false
	}
	if verr := q2AffConfigMatchesCanonical(ps.Q2Aff, claim.Winner, claim.Gen); verr != nil {
		s.Warnf("MQTT: refusing to restore the affine QoS 2 consumer for assignment %q: %v", claimSubject, verr)
		return false
	}
	si, serr := as.lookupQ2AffOutStream()
	if serr != nil || si == nil {
		return false // indeterminate: the limits below are not knowable
	}
	// Before the create, not after, and the difference matters. nats-server
	// substitutes the stream's limit into any create whose own InactiveThreshold is
	// zero, and this config deliberately sends zero; for an ALREADY-PRESENT durable
	// the create-or-update applies that substitution and restarts the deletion
	// timer before returning. Refusing afterwards would refuse a durable the
	// refusal itself had just made reapable.
	//
	// The predicate is != 0, not > 0. InactiveThreshold is a signed Duration, the
	// server normalizes a negative REQUEST field but substitutes the stream limit
	// unconditionally, and its own limit rejection fires only for a positive limit
	// — so a negative stream limit would slip past > 0, reach create-or-update and
	// clear an existing durable's timer before the returned-config arm refused it.
	// validateOutStream refuses any nonzero value at startup for the same reason;
	// this is that posture one level down.
	if si.Config.ConsumerLimits.InactiveThreshold != 0 {
		s.Warnf("MQTT: refusing to restore the affine QoS 2 consumer for assignment %q: %s imposes a consumer inactivity limit (%v), which would reap it",
			claimSubject, mqttOutStreamName, si.Config.ConsumerLimits.InactiveThreshold)
		return false
	}
	if berr := q2AffVariableFieldsInBounds(ps.Q2Aff, si.Config.ConsumerLimits.MaxAckPending); berr != nil {
		s.Warnf("MQTT: refusing to restore the affine QoS 2 consumer for assignment %q: %v", claimSubject, berr)
		return false
	}

	// The lookup has exactly two jobs, and gating on PRESENCE is not among them: it
	// supplies the actual durable's immutable storage type for adoption, and it
	// separates an authoritative absence from an indeterminate answer.
	//
	// A successful response can still be unusable, and that is indeterminate rather
	// than absent: lookupConsumer returns cir.ConsumerInfo unchecked, so a
	// malformed-but-successful reply yields nil info, and a non-nil ConsumerInfo
	// may carry a nil Config. Reading either as "absent" would create a durable
	// against an unknown one.
	memStorage := ps.Q2Aff.MemoryStorage
	ci, lerr := as.lookupQ2AffineConsumer(claim.AffDurable)
	switch {
	case lerr != nil:
		if !IsNatsErr(lerr, JSConsumerNotFoundErr) {
			return false // indeterminate: fail closed, no create attempted
		}
		// AUTHORITATIVELY absent, which is F-064's own premise: $MQTT_out is
		// interest-retained and CONSUMER.DELETE carries no precondition, so the
		// winner's durable can be destroyed out of band while it is away, taking
		// every copy it held. Nothing owner-independent could resolve such an
		// assignment before; restoring the durable from the winner's record is what
		// makes the group delivery releasable without waiting for a session that
		// may never return.
		//
		// Absence and presence are deliberately NOT two paths from here: the same
		// create-or-update runs either way, with the only difference being which
		// MemoryStorage it carries — the durable's where one exists, the record's
		// where none does. A contender must never impose its own local option,
		// because the type is immutable and the owner will meet this durable later.
	case ci == nil || ci.Config == nil:
		return false // a successful request that told us nothing
	default:
		if verr := mqttValidateQ2AffineNames(ci.Config, claim.Winner, claim.Gen); verr != nil {
			s.Warnf("MQTT: refusing to materialize shared QoS 2 assignment %q on behalf of %q: %v", claimSubject, claim.AffDurable, verr)
			return false
		}
		// A storage type is immutable, so a contender must never impose its OWN
		// local option on a durable the owner will later meet. Adopt what is there.
		memStorage = ci.Config.MemoryStorage
	}

	want := buildQ2AffineConsumerConfig(claim.Winner, claim.Gen, ps.Q2Aff.AckWait, ps.Q2Aff.MaxAckPending, memStorage)
	resp, cerr := as.createQ2AffineConsumer(&want)
	if cerr != nil {
		if rresp, ok := as.retryQ2AffineStorageConflict(&want, cerr, ps.Q2Aff.MemoryStorage); ok {
			resp, cerr = rresp, nil
		}
	}
	if cerr != nil {
		s.Warnf("MQTT: unable to restore the affine QoS 2 consumer %q for assignment %q: %v", claim.AffDurable, claimSubject, cerr)
		return false
	}
	// Unconditionally, unlike the owner's own conditional check: a contender is
	// about to store a copy onto a consumer it did not previously have, so "the
	// response told us nothing" is not a reason to proceed.
	if resp == nil || resp.ConsumerInfo == nil || resp.Config == nil {
		return false
	}
	if verr := mqttValidateQ2AffineNames(resp.Config, claim.Winner, claim.Gen); verr != nil {
		s.Warnf("MQTT: refusing to materialize shared QoS 2 assignment %q: the restored consumer is not the one the claim names: %v", claimSubject, verr)
		return false
	}
	// The pre-check above is not atomic with the create: a stream update landing
	// between them yields a nonzero EFFECTIVE threshold of either sign, and this is
	// the arm that catches it. What it leaves behind differs by sign — a positive
	// limit arms a deletion timer that later reaps the durable, a negative one
	// clears the timer and leaves a drifted config nothing reaps. Both are refused;
	// docs/CONFORMANCE.md states the debris.
	if resp.Config.InactiveThreshold != 0 {
		s.Warnf("MQTT: refusing to materialize shared QoS 2 assignment %q: the restored consumer carries an inactivity threshold of %v",
			claimSubject, resp.Config.InactiveThreshold)
		return false
	}

	// (3) Write the WINNER's copy: its subject, its generation, and its delivery
	// decisions — not ours.
	onBehalf := *env
	onBehalf.gen = claim.Gen
	onBehalf.subID = claim.SubID
	onBehalf.retained = claim.Retained
	if !as.materializeQ2AffCopy(s, claim, claimSubject, claimSeq, &onBehalf) {
		// Same reasoning as completeQ2AffOwnClaim: a failed materialization is not the
		// end of the attempt, because a terminal fact authorizes the source ack by
		// itself. Returning here would make those facts unreachable on this path — the
		// winner's own teardown can commit Discarded while this store is in flight —
		// and the group delivery would be re-offered with nothing able to release it.
		// The claim is re-read because this snapshot predates every round trip above.
		cur, _, lerr := as.loadQ2AffClaim(claimSubject)
		if lerr != nil || cur == nil || q2AffClaimUsableForDecision(claimSubject, cur) != nil ||
			!cur.sourceAckAuthorized(false) {
			return false
		}
	}
	if aerr := as.confirmSourceAckAndRecord(sess, claimSubject, in.reply); aerr != nil {
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
func mqttDeliverQ2AffineCb(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
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

	// A resuming CONNECT provisions this affine consumer before its CONNACK, and
	// [MQTT-3.2.0-1] makes the CONNACK the first packet the client may see. Wait
	// rather than skip, and on a withheld verdict return WITHOUT acknowledging so
	// JetStream redelivers. No withheld-marker bookkeeping: the affine path derives
	// DUP from the JetStream delivery count, which is F-072's open defect, and a
	// copy refused here before trackPublish is exactly that finding's
	// offered-but-never-transmitted population. Waiting is what keeps the ordinary
	// case out of it. See sess_restore_gate.go.
	switch sess.awaitTransmissionGates(cc) {
	case mqttTransmissionWithheld:
		mqttTestRestoreGateBlockedHook("affine")
		return
	case mqttTransmissionTimedOut:
		// NAK rather than simply returning: an affine copy carries no withheld
		// marker, so nothing else would re-drive it. See mqttTransmissionTimedOut.
		mqttTestRestoreGateBlockedHook("affine")
		sess.jsa.sendMsg(reply, AckNak)
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

	// Everything downstream fences on the DELIVERED COPY SUBJECT rather than on an
	// incarnation lifted out of it, and never on a header: the subject is the
	// identity, so a copy cannot assert an incarnation its subject contradicts, and
	// naming the whole subject also refuses a reference to a different claim that
	// happens to carry the right incarnation.
	sseq, _, _, _, _ := ackReplyInfo(reply)

	// The RETIREMENT check comes before the claim gate's own branches, and before
	// trackPublish, because four of those branches acknowledge and return: taking one
	// of them first would dispose of the copy while leaving the retirement and its
	// register behind, and taking trackPublish first yields identifier zero (the
	// restored PUBREL already reserves this source) and returns without acknowledging
	// anything at all. Either way the affine side is never discharged. F-065.
	sess.mu.Lock()
	retired := sess.sourceRetiredLocked(jsDur, sseq)
	sess.mu.Unlock()
	if retired {
		// The CARRIED pair comes from the retirement record itself; this call supplies
		// only the copy actually in hand, so the two can be compared.
		sess.settleRetiredAffineSource(cc, jsDur, sseq, reply, subject, claimSubject, _EMPTY_)
		return
	}

	claim, _, err := as.loadQ2AffClaim(claimSubject)
	switch {
	case err != nil:
		sess.refuseQ2AffCopy(reply) // indeterminate: ask again later, never guess
		return
	case claim == nil:
		// FAIL CLOSED on an ABSENCE — which is all this arm is. It is no longer the
		// claim that "nothing may delete a claim whose affine side is unproven": F-065
		// was exactly such a deletion. A missing record is simply not a fact
		// (feedback_absence_is_not_a_fact), so the copy is NAK'd and left for an actor
		// with positive evidence; the superseded-incarnation arm below is what disposes
		// of a copy on proof rather than on silence. Paced, because this absence can last:
		// an immediate NAK here is what made one copy re-offer 9214 times in ten seconds.
		sess.refuseQ2AffCopy(reply)
		return
	case q2AffClaimUsableForDecision(claimSubject, claim) != nil:
		// Unattributable: refuse to deliver against it, and do not dispose of the copy
		// either — nothing here can prove what it belongs to.
		cc.Errorf("MQTT: refusing to deliver an affine QoS 2 copy on %q: %v", claimSubject, q2AffClaimUsableForDecision(claimSubject, claim))
		sess.refuseQ2AffCopy(reply)
		return
	case q2AffCopyIsSupersededIncarnationOf(claim, subject):
		// A copy of a SUPERSEDED INCARNATION of this claim subject. The live claim
		// names a different copy subject, so nothing will ever authorize this one —
		// and that is a POSITIVE fact about a record we just read, not an inference
		// from an absence, which is what separates it from the claim == nil arm above
		// (feedback_absence_is_not_a_fact). Dispose of it rather than NAK it forever.
		//
		// Reachable because the claim subject's create-only election can be won again
		// after a reap, by a group delivery callback dispatched before that reap. F-065.
		cc.Errorf("MQTT: dropping an affine QoS 2 copy on %q: it belongs to a superseded incarnation of that assignment (copy %q, claim names %q)",
			claimSubject, subject, claim.CopySubject)
		_, _ = sess.submitSourceAck(reply, false, nil)
		return
	case subject != _EMPTY_ && claim.CopySubject != _EMPTY_ && subject != claim.CopySubject:
		// The claim this copy's HEADER names is attributable, and it is about a DIFFERENT
		// ASSIGNMENT: not the same claim hash, or not the same owner or generation. So this
		// copy is nothing that claim can authorize, and it is equally nothing this branch
		// may destroy — the assignment whose copy subject this actually is may be perfectly
		// live, and acknowledging it away would lose that message and strand its claim
		// without AffineDone.
		//
		// It is a persisted shape this broker does not write, so it is REFUSED rather than
		// interpreted (the compatibility boundary in docs/ARCHITECTURE.md), paced like
		// every other refusal here.
		cc.Errorf("MQTT: refusing an affine QoS 2 copy on %q: the copy subject %q belongs to a DIFFERENT assignment than the claim its header names (that claim's copy is %q)",
			claimSubject, subject, claim.CopySubject)
		sess.refuseQ2AffCopy(reply)
		return
	case !claim.ownedBy(idHash, gen):
		// The claim says this message belongs to another session. Our copy is an
		// artifact that must not be delivered; dropping it is right, because the
		// real owner has (or will have) its own.
		cc.Errorf("MQTT: dropping an affine QoS 2 copy on %q: its claim names a different owner", claimSubject)
		_, _ = sess.submitSourceAck(reply, false, nil)
		return
	case claim.Discarded:
		// The session ended between the claim and the delivery. The message dies
		// with it [MQTT-4.8.2-5]; record the affine side complete so the claim can
		// be reaped once its source ack is proven.
		cc.mqttFinishQ2AffineDelivery(claimSubject, subject, reply)
		return
	case claim.AffineDone || claim.Lost:
		// The affine side is already recorded complete, so this is a redelivery of
		// a copy whose exchange has moved on — either into the PUBREL phase, or
		// into a recorded loss. Publishing it again would duplicate a PUBLISH the
		// client has already answered. Dispose of it and let the reap proceed now
		// that the copy is actually going away.
		cc.mqttFinishQ2AffineDelivery(claimSubject, subject, reply)
		return
	case claim.Phase != mqttQ2AffMaterialized:
		// The copy exists but its claim has not recorded the phase yet, which is an
		// ORDINARY interleaving: materializeQ2AffCopy stores the copy before it advances
		// the record, and the interest-based push can outrun that advance. So the first
		// retry is the fast one, and only a phase that never advances — a materializer
		// that died in the window — pays the backoff.
		sess.refuseQ2AffCopy(reply)
		return
	}

	// The THIRD subscribe-deny guard, and the one the F-081 dossier does not name.
	// This copy reaches the client through the SERVICE connection like the other
	// two service-owned routes, so NATS filters nothing here either.
	//
	// It needs its own site, and its own queue, for a reason that is easy to get
	// wrong: the affine subscription is created with a NIL queue
	// (processQOS12Sub(..., nil, ...) above), so by the time delivery arrives
	// sub.queue no longer names the shared group this copy came from. Deriving the
	// group from the validated claim is the only source that still knows -- the durable
	// is "<prefix><qkey>_<inc>" and the queue is the LOGICAL identity built from <qkey>
	// alone, so the recovery parses the tail and drops the incarnation.
	//
	// Placed after the claim has been validated (so GroupDurable is trustworthy)
	// and before trackPublish (so a refusal costs no packet identifier and no
	// in-flight register).
	switch queue, ok := mqttSharedQueueFromGroupDurable(claim.GroupDurable); {
	case !ok:
		// Provenance we cannot derive is NOT a denial. Acknowledging would consume a
		// copy we never judged; the paced refusal keeps it and retries, so a
		// permanently malformed claim is a bounded-rate poison record rather than a
		// hot loop. Stated in docs/CONFORMANCE.md.
		cc.Warnf("Affine QoS 2 copy on %q carries an underivable group durable %q; withholding without acknowledging",
			subject, claim.GroupDurable)
		sess.refuseQ2AffCopy(reply)
		return
	default:
		// Test seam. The indeterminate arm below cannot be reached from outside:
		// a resumed client's snapshot is fresh, and every external way of
		// invalidating it also closes the connection the copy would arrive on.
		// Nil in production.
		if gate := mqttTestAffineGateV.Load(); gate != nil {
			(*gate)(cc)
		}
		switch cc.checkDenySubVerdict(string(h.subject), queue) {
		case mqttSubDeliverDenied:
			// A proven deny is terminal for this copy: the client may never receive it,
			// so the affine side is recorded complete and the copy is reaped, exactly as
			// an ended session's would be. [MQTT-4.8.2-5].
			//
			// Terminal means the IDENTIFIER goes too, and it has to go first, in the
			// same order the expiry and Maximum-Packet-Size disposals below use: the
			// register is released with the identifier (R5), before the confirmed copy
			// ack. This copy may well have been transmitted before a disconnect, in
			// which case the resume restored its identifier and register before any
			// guard ran, and disposing of the message while keeping them wedges the
			// session's Receive Maximum for good.
			sess.releaseTransmittedIdentifier(cc, jsDur, reply)
			cc.mqttFinishQ2AffineDelivery(claimSubject, subject, reply)
			return
		case mqttSubDeliverIndeterminate:
			// No valid snapshot: withhold, but record NOTHING. AffineDone would assert a
			// delivery outcome we have not established.
			sess.refuseQ2AffCopy(reply)
			return
		}
	}

	sess.mu.Lock()
	if !mqttInflightPreSendAdmits(jsDur, reply) {
		sess.mu.Unlock()
		return // a test-staged pre-tracking refusal; AckWait re-offers the copy
	}
	pi, isNew := sess.trackPublish(jsDur, reply)
	// The allocation's nonce, read under the allocating lock (see mqttDeliverMsgCbQoS12),
	// and whether the entry was transmitted, which gates the expiry discard below.
	var regNonce string
	var expirable bool
	if pi != 0 {
		if p := sess.pendingPublish[pi]; p != nil {
			// PROVEN-UNSENT and releasable, exactly as the plain path gates it: a
			// possibly-attempted or unresolved-intent entry may have reached the client
			// and is never dropped on expiry (provenUnsentReleasableLocked).
			expirable = sess.provenUnsentReleasableLocked(p)
			if isNew {
				regNonce = p.regNonce
			}
			// Carry the claim AND its incarnation into the pending entry: the terminal
			// paths (PUBREC, expiry, oversize discard) see a packet identifier and
			// nothing else, and the incarnation is what fences their write to the claim
			// this copy actually belongs to (F-065).
			p.q2AffClaim = claimSubject
			p.q2AffCopy = subject
		}
	}
	sess.mu.Unlock()
	if pi == 0 {
		return
	}

	props := h.props
	if len(props) > 0 && expirable {
		_, _, _, ts, _ := ackReplyInfo(reply)
		var expired bool
		if props, expired = mqttForwardExpiry(props, ts); expired {
			// The AFFINE twin of the plain expiry drop, through the same removal: the
			// identifier is released and its register goes with it (R5), before the
			// confirmed copy ack below, and the exclusion is handed off first because this
			// delete can be refused or fail (F-098, holistic review round 3, swept round 4).
			// Normally a no-op, since expiry is decided before the register is written.
			sess.dropProvenUnsentPublishEntry(pi, mqttLicenceOf(cc))
			cc.mqttFinishQ2AffineDelivery(claimSubject, subject, reply)
			return
		}
	}

	// A newly allocated identifier is not sendable until its in-flight register is
	// confirmed (R1, inflight_register.go), and no earlier than it has to be: a
	// message discarded on expiry above never pays for one. The register carries the
	// claim subject AND its incarnation, which is not decoration here — after a
	// restart the recovered pending entry (or, if the PUBREL won the race, the
	// recovered RETIREMENT) is the only route by which the claim reaches the terminal
	// paths above, so a register without them would leave a post-restart PUBREC
	// unable to record AffineDone, or able to record it against the wrong
	// incarnation. F-065.
	if isNew && !sess.storeInflightRegister(cc, pi, 2, jsDur, sseq, regNonce, claimSubject, subject) {
		return
	}
	// The SEND SECTION, exactly as the plain QoS 1/2 path: the identifier is held by the
	// session registry until this write returns, so no removal path can free it and let
	// bumpPI re-mint it mid-write (F-087 WP5). Armed with defer before the enqueue.
	sess.mu.Lock()
	sendTok, sendOK := sess.beginSendLocked(cc, pi, jsDur, sseq)
	sess.mu.Unlock()
	if !sendOK {
		return // AckWait re-offers it, the same cost a regPending decline accepts
	}
	// ADMISSION, then durable INTENT, then the WRITE — exactly as mqttDeliverMsgCbQoS12,
	// and for the same reasons (see there).
	//
	// THE SHUTDOWN BARRIER on the same terms as the plain path: registered before the
	// section's own end so it unwinds after it, and a refusal releases the section without
	// taking anything durable (F-098).
	if !cc.srv.admitPublishSend() {
		sess.endPublishSend(cc, pi, sendTok, mqttIntentNone, false)
		return
	}
	defer cc.srv.publishSendDone()
	intent, wrote := mqttIntentNone, false
	defer func() { sess.endPublishSend(cc, pi, sendTok, intent, wrote) }()
	// The durable half of the exclusion, first and for the same reason as on the plain
	// path: a process-memory hold fences no peer.
	if !sess.acquirePublishAction(cc, pi, &sendTok) {
		return // a peer holds the action, or the slot moved; AckWait re-offers
	}
	if !sess.markerSendWindowHook(pi, mqttMarkerSendReserved) {
		return // a test-staged crash after the pair confirmed and BEFORE intent: proven-unsent
	}
	topic := natsSubjectStrToMQTTTopic(string(h.subject))
	// DUP from the section's token, captured under the lock (see mqttDeliverMsgCbQoS12).
	prep, admitted := pc.mqttPreparePublishMsgTo(cc, pi, 2, sendTok.dup, h.retained, subID, topic, msg, props)
	if !admitted {
		// Exceeds the client's MQTT 5.0 Maximum Packet Size at every framing. Spec5
		// [3.1.2.11.4].
		if !sess.oversizeDisposalAllowed(cc, pi, sendTok) {
			return // POSSIBLY-ATTEMPTED: retained whole, nothing acknowledged
		}
		// PROVEN-UNSENT: discard it and complete the delivery. It dies with this
		// session, never redistributed. Before the deferred endPublishSend, as on the
		// plain path — and after draining the durable action, because no destructive
		// disposition may act under a live token.
		sess.drainPublishAction(cc, sendTok)
		sendTok.actionHeld = false
		// The AFFINE twin of the plain oversize disposal, through the same removal.
		sess.dropProvenUnsentPublishEntry(pi, mqttLicenceOf(cc))
		cc.mqttFinishQ2AffineDelivery(claimSubject, subject, reply)
		return
	}
	intent = sess.recordSendIntent(cc, pi, sendTok)
	if intent == mqttIntentDefiniteFailure || intent == mqttIntentAmbiguous {
		return // nothing written; AckWait re-offers
	}
	if !sess.markerSendWindowHook(pi, mqttMarkerSendIntent) {
		return // a test-staged crash AFTER intent and before the write: possibly-attempted
	}
	prep.bindSection(sendTok)
	wrote = pc.mqttEnqueuePreparedPublishMsgTo(cc, sub, prep)
	sess.markerSendWindowHook(pi, mqttMarkerSendWrote)
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
// copy under a claim that never reached AffineDone was never acked by the
// JETSTREAM delivery — and every broker reads that the same way.
//
// That is a fact about the copy acknowledgement, and it is NOT a proof that the
// client missed the message. mqttProcessPublishReceived stores the correlated
// PUBREL before it reaches this function, so a client can have answered PUBREC
// and still leave an absent copy under a claim with no AffineDone. What the
// ordering buys is a reading every broker agrees on; what it does not buy is
// certainty about the client. See the Lost field's comment and
// docs/CONFORMANCE.md.
//
// The inverted window is benign and self-correcting: AffineDone recorded but the
// copy not yet acked leaves the copy live, the claim un-reapable (see reapable),
// and the redelivered copy disposed of by the delivery gate's terminal branch.
// carriedCopy fences the write to the INCARNATION the delivery gate attributed, by
// naming the copy subject the delivery actually carried and requiring the record now
// on claimSubject to name that same copy. Re-reading whatever claim occupies the
// subject now is exactly the mistake F-065 closes: a replacement election would
// otherwise be told a delivery it never made is complete.
// Winner and generation are deliberately NOT asserted — AffineDone is owner
// independent, a statement about a copy the gate already attributed — but the
// incarnation is, because the copy belongs to one.
// It reports TWO facts, because its two callers are licensed by different ones and
// collapsing them would give one of them the wrong answer:
//
//   - recorded: AffineDone is durable. That is what the PUBREC path needs, because the
//     in-flight register exists to let recovery reconstruct this exchange, and once the
//     fact is on the claim the register has nothing left to carry.
//   - acked: the copy's acknowledgement is CONFIRMED. That is what the retired-source
//     path needs, because a retirement exists to suppress redelivery and clearing it on
//     an unproven ack would leave the copy re-offerable with the state that governs it
//     already gone — the same reasoning settleRetiredSource applies to its own entry.
//
// The first return is load-bearing on the PUBREC path. The correlated PUBREL is stored before this runs
// and the pending entry is already gone, so the in-flight register is the ONLY
// remaining carrier of the claim and its incarnation: deleting it after a failed
// transition destroys the one thing a restart could reconstruct the retirement from,
// leaving the copy stalled with no route to completion. False therefore means "keep
// the register" — the identifier is spent either way, so retaining it costs a record
// the next resume resolves, while dropping it costs the delivery.
// proveQ2AffCopyDelivered records AffineDone for the claim that owns carriedCopy and
// returns that claim ONLY when the fact is durably on it. nil means "not proven", and
// no caller may acknowledge anything on a nil.
//
// Extracted so there is one definition of this transition and its proof rather than a
// second one for the sweep's register arm. Every rule is the original's:
//
//   - The carried pair becomes a claim through the single helper
//     q2AffClaimForCarriedCopy, which requires the record to be attributable AND to name
//     the copy this delivery carried. Fencing on an incarnation alone would accept a
//     reference to a DIFFERENT claim carrying its own correct incarnation.
//   - The advance asserts the claim's Nonce and the carried copy, and deliberately NOT
//     Winner+Gen: AffineDone is owner-independent, a statement about a copy the caller
//     has already attributed. Owner-independent is not the same as unfenced.
//   - A NIL ERROR IS NOT THE PROOF. advanceQ2AffClaim reports (nil, 0, nil) when the
//     record disappeared between the validation and its own load, and reports success
//     for a no-op write. Treating either as recorded would let a caller take its
//     irreversible step with the completion durable NOWHERE. So the fact has to be ON
//     the record this call left behind, and that record still has to be the one this
//     copy belongs to.
//
// precheck is threaded into the CAS loop, so a caller needing a retry-time veto gets one
// without a second transition site. warnf is passed in so each caller keeps its own
// logging: the delivery path logs through its *client, the sweep through its *Server,
// and neither's output changes.
// pubRecProven is the CALLER'S positive fact and is never inferred here. This function
// is a transition funnel, not evidence: only a caller holding a resolved (claim subject,
// copy subject) carrier read off a stored PUBREL may pass true, because that record is
// what proves the client answered PUBREC. Every other caller passes false and records a
// terminal copy DISPOSITION, which does not outrank a recorded loss.
func (as *mqttAccountSessionManager) proveQ2AffCopyDelivered(claimSubject, carriedCopy string,
	precheck func(*mqttQ2AffClaim) error, warnf func(string, ...any), pubRecProven bool) *mqttQ2AffClaim {

	claim, cerr := as.q2AffClaimForCarriedCopy(claimSubject, carriedCopy)
	if cerr != nil {
		warnf("Refusing to record the completion of affine QoS 2 assignment %q: %v", claimSubject, cerr)
		return nil
	}
	if claim == nil {
		return nil
	}
	updated, _, err := as.advanceQ2AffClaimChecked(claimSubject, _EMPTY_, _EMPTY_, claim.Nonce, carriedCopy,
		precheck, func(cl *mqttQ2AffClaim) {
			cl.AffineDone = true
			if pubRecProven {
				cl.PubRecProven = true
			}
		})
	if err != nil {
		// Not recorded: the caller must not ack. AckWait re-offers the copy, the gate
		// re-runs, and nothing has been made ambiguous.
		warnf("Unable to record the completion of affine QoS 2 assignment %q: %v", claimSubject, err)
		return nil
	}
	if updated == nil || !updated.AffineDone || updated.CopySubject != carriedCopy ||
		q2AffClaimUsableForDecision(claimSubject, updated) != nil {
		warnf("Refusing to acknowledge an affine QoS 2 delivery for %q: the transition reported success but the "+
			"record it left does not carry the completion of this copy (%+v)", claimSubject, updated)
		return nil
	}
	// A PROOF caller needs its own fact on the record, for the same reason the line
	// above needs AffineDone there: mergeProven can normalise, the CAS can be a no-op,
	// and a caller that is about to stop protecting the PUBREL must see the proof
	// landed rather than assume it. A nil error is not the proof here either.
	if pubRecProven && !updated.PubRecProven {
		warnf("Refusing to release the PUBREC proof of affine QoS 2 assignment %q: the transition reported "+
			"success but the record it left does not carry it (%+v)", claimSubject, updated)
		return nil
	}
	if pubRecProven && claim.Lost {
		// The CORRECTION, and it is owed to whoever read the loss report. A superseded
		// broker's PUBREL store is not CAS-conditioned, so it can land after this
		// incarnation's scan has already completed and its loss verdict has been written
		// and LOGGED. The proof then clears Lost and the claim is reaped — leaving the
		// operator with an erroneous loss report and no durable record contradicting it.
		// Retracting it in the log is the only place left to say so.
		warnf("MQTT: retracting the LOST report for shared QoS 2 assignment %q: a stored PUBREL proves the "+
			"client answered PUBREC for it, so that loss report was wrong. It may be read BEFORE the "+
			"report it retracts — the two are emitted by different goroutines, and the record, not "+
			"the log, is the authority", claimSubject)
	}
	return updated
}

func (c *client) mqttFinishQ2AffineDelivery(claimSubject, carriedCopy, ackSubject string) (recorded, acked bool) {
	as := c.mqtt.asm
	if as == nil || c.mqtt.sess == nil {
		return false, false
	}
	as.mu.RLock()
	ackHook := as.testQ2AffCopyAck
	as.mu.RUnlock()
	sess := c.mqtt.sess
	confirm := func(reply string) error {
		_, err := sess.submitSourceAck(reply, true, nil)
		return err
	}
	if ackHook != nil {
		confirm = ackHook
	}
	// Nil precheck: this caller's behaviour must not change, which is the property the
	// extraction exists to preserve. The sweep's register arm passes a real one.
	//
	// pubRecProven FALSE, and this is the caller the distinction exists for. This
	// function is reached from seven routes — including the delivery gate's own
	// `AffineDone || Lost` arm — and none of them holds a stored PUBREL naming this
	// assignment. What they record is a terminal copy DISPOSITION, which must never
	// outrank a recorded loss, or "this claim is already Lost" becomes the reason to
	// clear Lost. See the PubRecProven field.
	if as.proveQ2AffCopyDelivered(claimSubject, carriedCopy, nil, c.Warnf, false) == nil {
		return false, false
	}
	if err := confirm(ackSubject); err != nil {
		c.Warnf("Unable to confirm the acknowledgement of an affine QoS 2 delivery for %q: %v", claimSubject, err)
		// The affine side IS recorded, which is the fact recovery needs; only the
		// copy's own acknowledgement is unproven, and the gate's terminal branch
		// disposes of the redelivery. Discharged as far as the REGISTER is concerned,
		// and deliberately not as far as a retirement is.
		return true, false
	}
	as.reapQ2AffClaim(c.srv, claimSubject)
	return true, true
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

// q2AffClaimUsableForDecision reports whether a claim record can safely be ACTED on
// by a direct decision path — the mover, the delivery gate, either materialization
// path, the reaper, and the owner's recovery. All of them consume the record's
// derived copy identity, and none of them consults a validator today.
//
// It is deliberately NOT mqttQ2AffClaimIdentityOK, and the split is the point.
// That predicate is the shared attribution FLOOR for the account-wide scans, where
// one failure aborts the whole walk and, from clear(), fails the discard and can
// lock an unrelated client ID out of its next CONNECT. Winner+Gen still attributes a
// claim to its session even when its nonce is malformed, so nonce-format and
// derivation checks must not be able to do that to unrelated sessions. Here, by
// contrast, refusing costs nothing: prove nothing, write nothing, ack nothing.
//
// What it refuses, and why each matters:
//
//   - a Nonce that is not the exact minted form. The incarnation is what fences
//     every incarnation-specific transition, and a malformed one would be passed
//     into that fence and match only the equally malformed record it came from.
//   - a CopySubject that does not follow from this record's own fields. The reaper
//     reads it to decide whether to DELETE the claim, and recovery reads it before
//     writing Lost or the source-gone disposition; a subject that does not follow
//     from the record makes both inspect a copy the record does not own.
func q2AffClaimUsableForDecision(subject string, c *mqttQ2AffClaim) error {
	if c == nil {
		return fmt.Errorf("no claim")
	}
	if !mqttValidQ2AffGen(c.Nonce) {
		return fmt.Errorf("claim incarnation %q is not a valid identity", c.Nonce)
	}
	if c.GroupDurable == _EMPTY_ || c.SourceSeq == 0 {
		return fmt.Errorf("claim names group %q and source sequence %d, which cannot identify a source", c.GroupDurable, c.SourceSeq)
	}
	// BOUND TO THE SUBJECT IT WAS STORED ON, and to its own source, which internal
	// self-consistency does NOT give. Every derived name below follows from ClaimHash, so a
	// rewrite that changes ClaimHash and recomputes CopySubject from it is internally
	// consistent and passes every identity comparison — including the incarnation fence,
	// which compares winner, generation and nonce only. Acting on such a record
	// materializes and then IRREVERSIBLY releases a source under a claim hash that belongs
	// to some other (group, sequence), and the delivery gate then reads the real copy's
	// subject as a superseded incarnation and acknowledges it away without publishing:
	// the message is lost. Same discipline F-058 applies to Will records — a record is
	// refused unless it agrees with the subject it was found on.
	if want := mqttQ2AffClaimHash(c.GroupDurable, c.SourceSeq); c.ClaimHash != want {
		return fmt.Errorf("claim hash is %q, which does not follow from group %q and source sequence %d (want %q)", c.ClaimHash, c.GroupDurable, c.SourceSeq, want)
	}
	if subject != _EMPTY_ {
		if want := mqttQ2AffClaimSubject(c.GroupDurable, c.SourceSeq); subject != want {
			return fmt.Errorf("record on %q describes group %q and source sequence %d, whose subject is %q", subject, c.GroupDurable, c.SourceSeq, want)
		}
	}
	if want := mqttQ2AffCopySubject(c.Winner, c.Gen, c.ClaimHash, c.Nonce); c.CopySubject != want {
		return fmt.Errorf("copy subject is %q, which does not follow from the record's own identity (want %q)", c.CopySubject, want)
	}
	return nil
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
	return as.scanQ2AffClaimRecordsCharged(fn, nil)
}

// scanQ2AffClaimRecordsCharged is scanQ2AffClaimRecords with a per-record CHARGE, for the
// recovery pass and its bounded budget.
//
// Exhausting the charge returns an ERROR, so a caller cannot mistake a truncated walk for a
// complete one — and for the discard that is exactly right: "no claim of this generation is left
// undiscarded" is a statement about the WHOLE set, and half of it establishes nothing. nil means
// unbounded, which is every production caller.
func (as *mqttAccountSessionManager) scanQ2AffClaimRecordsCharged(
	fn func(claim *mqttQ2AffClaim, smsg *StoredMsg) error, charge func() bool) error {

	filter := mqttQoS2AssignStreamSubjectPrefix + ">"
	for seq := uint64(1); ; {
		if charge != nil && !charge() {
			return fmt.Errorf("the assignment walk exhausted its budget before reaching the end, " +
				"so nothing it did not visit is established")
		}
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

// mqttRecoveryClaimScanExtensions is K: how many times the recovery pass's assignment walk may
// extend its horizon before it gives up on this attempt.
//
// Two is a POLICY choice, not a correctness threshold. What correctness this walk HAS comes from
// the final verification read rather than from the number of tries — and that read is not
// authoritative today (F-108), so the number is not what is load-bearing either way: one extension absorbs the walk's OWN discard
// writes (which move the tail like any other), and the second absorbs one further moving segment.
// A tail still moving after that is a stream under sustained write load, and this pass withholds
// its assertion rather than chasing it.
const mqttRecoveryClaimScanExtensions = 2

// mqttAssignTail is one AUTHORITATIVE observation of the assignment stream's end.
//
// The incarnation travels with the sequence because a sequence means nothing across a recreation:
// a stream destroyed and rebuilt restarts its numbering, so two equal observations either side of
// that are not evidence of a still tail, they are evidence of nothing.
type mqttAssignTail struct {
	last uint64
	born time.Time
}

// assignStreamTail reads the assignment stream's last sequence and its incarnation.
func (as *mqttAccountSessionManager) assignStreamTail(charge func() bool) (mqttAssignTail, error) {
	// THE HORIZON READ IS A STORE OPERATION LIKE ANY OTHER, so it is cancellable too. A walk that
	// could be stopped between records but not before its verification read would hold the sweep
	// goroutine for one more round trip than the contract states.
	if charge != nil && !charge() {
		return mqttAssignTail{}, fmt.Errorf("the assignment walk was interrupted before it could " +
			"observe the stream's end, so nothing about its completeness is established")
	}
	// FIRED BEFORE THE READ, so a test can move the tail in exactly the interval the contract
	// verifies — between a segment finishing and the observation that says it was the last. That
	// interval is not reachable from outside: everything else a fixture can drive happens before
	// the first horizon is taken, and a write there needs no extension at all.
	if h := mqttTestAssignTailHookV.Load(); h != nil {
		(*h)()
	}
	si, err := as.jsa.lookupStream(mqttQoS2AssignStreamName)
	if err != nil {
		return mqttAssignTail{}, fmt.Errorf("reading the end of %q: %w", mqttQoS2AssignStreamName, err)
	}
	if si == nil {
		return mqttAssignTail{}, fmt.Errorf("stream info for %q is unavailable, so the walk has no "+
			"horizon to bound itself by", mqttQoS2AssignStreamName)
	}
	// A SUCCESSFUL ANSWER IS NOT AN AUTHORITATIVE ONE. When the stream is clustered and has no
	// leader, a FOLLOWER answers from its own local state — and a lagging follower can report the
	// same stale end TWICE, which this walk would read as a still tail and take as proof it saw
	// every claim. It would then have enumerated nothing and published readiness, licensing the
	// seal and the consumer's deletion while undiscarded claims stood on the leader. Two equal
	// observations are a completion proof only if each was authoritative when it was made.
	if si.Cluster != nil && si.Cluster.Leader == _EMPTY_ {
		return mqttAssignTail{}, fmt.Errorf("the end of %q was reported by a peer of a LEADERLESS "+
			"stream, so it is that peer's local state rather than the stream's",
			mqttQoS2AssignStreamName)
	}
	// AN INCARNATION IS REQUIRED, not merely compared. A zero creation time cannot distinguish
	// two incarnations, so a pair of observations carrying it proves nothing about either.
	if si.Created.IsZero() {
		return mqttAssignTail{}, fmt.Errorf("stream info for %q names no creation time, so two "+
			"observations of its end cannot be shown to describe the same stream",
			mqttQoS2AssignStreamName)
	}
	return mqttAssignTail{last: si.State.LastSeq, born: si.Created}, nil
}

// scanQ2AffClaimRecordsBounded is scanQ2AffClaimRecordsCharged with a FINITE HORIZON, for the
// recovery pass.
//
// # Why the unbounded form cannot be used here
//
// The unbounded walk reads from sequence 1 until a read reports no next message. Nothing stops
// concurrent writers from supplying records ahead of the reader, so under sustained assignment
// traffic one call never returns — and the pass's one-attempt cap and its end-after-the-spender
// rule are both powerless, because neither is reached again until this function RETURNS. The
// recovery pass shares the maintenance goroutine with session expiry and the orphan sweeps, so
// "never returns" is not a slow pass, it is a stopped broker's worth of maintenance.
//
// # Why a horizon alone would be WRONG
//
// The stream is MaxMsgsPer:1, so advancing a claim REWRITES it at a higher sequence, and
// `confirmSourceAckAndRecord` advances a claim with no owner scoping at all. A claim of the
// generation being discarded can therefore be moved ABOVE a fixed horizon by an unrelated
// contender, and a walk that stopped there would report completion having never seen it —
// licensing the seal, the consumer's deletion, and a started delivery being redistributed instead
// of dying with its session. [MQTT-4.8.2-5].
//
// # What the bounded walk establishes, and what it does NOT
//
// THE ARGUMENT BELOW IS CONDITIONAL ON TWO PREMISES, NEITHER OF WHICH HOLDS TODAY, so this is not
// a claim that the walk is sound. (i) The closed generation must admit no fresh undiscarded claim:
// it can, because `beginQ2AffOp` admits against LOCAL session state and the claim CAS never
// contends with the closure (F-107). (ii) The tail observation must be AUTHORITATIVE for the state
// it describes: it is not, because `STREAM.INFO` samples `State` and `Cluster.Leader` separately,
// so an election between the samples returns a lagging follower's state under the new leader's
// name (F-108). What is demonstrated in the tests is execution over the populations they exercise,
// conditional on both.
//
// With that said, completion is asserted only when a segment finishes AND a subsequent read finds
// the tail exactly where that segment ended, in the same stream incarnation. A rewrite lands at a
// HIGHER sequence, never behind the cursor, so an owned claim rewritten during segment i landed at
// or below the horizon read AFTER segment i, and segment i+1 visits it. When the final read shows
// no movement, every surviving owned claim was either visited by this walk or carries a discard a
// previous visit wrote — the `SourceAcked` update preserves `Discarded`, and a claim already
// reaped need not still exist. What that reaches for — conditionally, per the two premises above —
// is the MONOTONE DISCARD PREDICATE, not an atomic inventory snapshot, and it rests on the closed
// generation admitting no fresh undiscarded claim.
//
// Reaching the horizon does not establish completion on its own: it leads to the verification
// read, an initially empty stream included. "No next message" does not lead there at all — it is
// INDETERMINATE and fails the segment, because the pinned server answers a failed store read the
// same way (F-108), and a verification read that asks whether the tail MOVED cannot tell an
// enumeration that finished from one that gave up.
func (as *mqttAccountSessionManager) scanQ2AffClaimRecordsBounded(
	fn func(claim *mqttQ2AffClaim, smsg *StoredMsg) error, charge func() bool) error {

	tail, terr := as.assignStreamTail(charge)
	if terr != nil {
		return terr
	}
	start := uint64(1)
	for ext := 0; ; ext++ {
		if serr := as.walkQ2AffClaimSegment(start, tail.last, fn, charge); serr != nil {
			return serr
		}
		next, nerr := as.assignStreamTail(charge)
		if nerr != nil {
			return nerr
		}
		if !next.born.Equal(tail.born) {
			return fmt.Errorf("the assignment stream was recreated during the walk, so its "+
				"sequences before and after name different populations (%s then %s)",
				tail.born, next.born)
		}
		if next.last == tail.last {
			return nil // COMPLETE: the segment ended where the stream still ends.
		}
		if next.last < tail.last {
			return fmt.Errorf("the end of the assignment stream went BACKWARDS during the walk, "+
				"from %d to %d, so no observation of it is usable", tail.last, next.last)
		}
		if ext == mqttRecoveryClaimScanExtensions {
			return fmt.Errorf("the assignment stream was still growing after %d horizon "+
				"extensions (now %d), so this attempt cannot say it saw every claim",
				mqttRecoveryClaimScanExtensions, next.last)
		}
		if tail.last == ^uint64(0) {
			return fmt.Errorf("the assignment stream's sequence space is exhausted")
		}
		start, tail = tail.last+1, next
	}
}

// walkQ2AffClaimSegment visits the records in [from, to], and NOTHING above to.
func (as *mqttAccountSessionManager) walkQ2AffClaimSegment(from, to uint64,
	fn func(claim *mqttQ2AffClaim, smsg *StoredMsg) error, charge func() bool) error {

	filter := mqttQoS2AssignStreamSubjectPrefix + ">"
	for seq := from; seq <= to; {
		if charge != nil && !charge() {
			return fmt.Errorf("the assignment walk was interrupted before reaching the end of " +
				"its segment, so nothing it did not visit is established")
		}
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttQoS2AssignStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				return err
			}
			// NO-MESSAGE IS NOT AN ABSENCE CERTIFICATE. The pinned server maps every error
			// returned by LoadMsg, LoadNextMsg and LoadLastMsg to JSNoMessageFoundErr, so a
			// FAILED READ and a genuinely empty suffix arrive as the same response — and a
			// segment that reads the failure as its own end reports success having skipped
			// whatever lay beyond it. The verification read cannot catch that: it asks whether
			// the TAIL MOVED, not whether the enumeration succeeded.
			//
			// So it is indeterminate, and indeterminate withholds. That costs availability on a
			// sparse or empty suffix, where the same response is the honest end — the repair
			// that tells the two apart needs positive completion evidence this walk does not
			// have yet, and it is the integrated change's (docs/ROADMAP.md, F-108).
			return fmt.Errorf("the assignment walk got no message at or after sequence %d of "+
				"segment [%d, %d], which this API cannot distinguish from a failed store read, "+
				"so the segment is not established", seq, from, to)
		}
		if smsg == nil {
			return fmt.Errorf("the assignment walk got an empty response at sequence %d of "+
				"segment [%d, %d], which establishes nothing about what lies there", seq, from, to)
		}
		if smsg.Sequence > to {
			// ABOVE THE BOUND. A next-message read is not confined to the segment, so the record
			// it returns may sit past it. It is neither processed nor advanced past: the next
			// segment begins at this segment's horizon plus one and will reach it.
			return nil
		}
		if smsg.Sequence == ^uint64(0) {
			return fmt.Errorf("the assignment stream's sequence space is exhausted at %d", smsg.Sequence)
		}
		seq = smsg.Sequence + 1
		claim := &mqttQ2AffClaim{}
		if uerr := json.Unmarshal(smsg.Data, claim); uerr != nil {
			return fmt.Errorf("assignment claim on %q at sequence %d is unreadable: %w",
				smsg.Subject, smsg.Sequence, uerr)
		}
		if !mqttQ2AffClaimIdentityOK(claim) {
			return fmt.Errorf("assignment claim on %q at sequence %d carries no usable identity "+
				"(winner %q, generation %q, group %q, source sequence %d)",
				smsg.Subject, smsg.Sequence, claim.Winner, claim.Gen, claim.GroupDurable, claim.SourceSeq)
		}
		if ferr := fn(claim, smsg); ferr != nil {
			return ferr
		}
	}
	return nil
}

// discardQ2AffClaimsFor marks every nonterminal assignment owned by one session
// generation as discarded, so its started deliveries die with the session rather
// than being released back to the group. [MQTT-4.8.2-5] permits the first and
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
	return as.discardQ2AffClaimsForCharged(idHash, gen, nil)
}

// discardQ2AffClaimsForCharged is discardQ2AffClaimsFor under a per-record charge, for the
// recovery pass. An exhausted budget fails the act, so a partial discard never licenses readiness.
func (as *mqttAccountSessionManager) discardQ2AffClaimsForCharged(idHash, gen string,
	charge func() bool) error {

	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return nil
	}
	s := as.server()
	visit := func(claim *mqttQ2AffClaim, smsg *StoredMsg) error {
		subject := smsg.Subject
		if !claim.ownedBy(idHash, gen) || claim.Discarded {
			return nil
		}
		// CANCELLABLE BEFORE THE MUTATION TOO, and this is where a UNIT begins. The CAS below
		// and the reap after it are store operations of their own, and a check that covered only
		// the enumeration would let a shutdown wait for every claim of the account to be
		// advanced. Once past this check the unit RUNS TO ITS END — advanceQ2AffClaim retries up
		// to mqttQ2AffClaimAttempts times and reapQ2AffClaim re-reads and may advance and delete
		// again — so the guarantee is a bounded unit, not a write barrier.
		if charge != nil && !charge() {
			return fmt.Errorf("discarding the assignments of %q/%q was interrupted, so the claims "+
				"it did not reach are not established", idHash, gen)
		}
		// No nonce: teardown of the winning generation is a session fact.
		updated, _, err := as.advanceQ2AffClaim(subject, idHash, gen, _EMPTY_, _EMPTY_, func(c *mqttQ2AffClaim) { c.Discarded = true })
		if err != nil {
			return fmt.Errorf("discard assignment %q: %w", subject, err)
		}
		if updated != nil && updated.reapable(true) {
			// Only reachable when both sides were already proven; reapQ2AffClaim
			// re-reads and re-proves before deleting anything.
			as.reapQ2AffClaim(s, subject)
		}
		return nil
	}
	if charge == nil {
		// THE UNCHARGED CALLERS ARE UNCHANGED. A teardown running with the fence closed is not
		// the population this bound exists for, and an incomplete result means something
		// different on that path — it fails the clear() rather than deferring a readiness
		// assertion to a later pass. Consult 6: pass-only.
		return as.scanQ2AffClaimRecordsCharged(visit, nil)
	}
	return as.scanQ2AffClaimRecordsBounded(visit, charge)
}

// mqttQ2AffProofEvidence says whether a resume managed to gather the PUBREL proofs
// that would REFUTE a loss inference. It is one flag rather than a per-claim set on
// purpose: a scan that failed cannot even name which assignments have proofs, and a
// per-record failure already means this incarnation's evidence is partial.
type mqttQ2AffProofEvidence int

const (
	// mqttQ2AffProofComplete: every stored PUBREL of this incarnation was read and
	// every reference it carried was resolved. An absent copy under a nonterminal
	// claim may now be read as a loss, because the thing that would have contradicted
	// it was looked for and is not there.
	mqttQ2AffProofComplete mqttQ2AffProofEvidence = iota
	// mqttQ2AffProofIncomplete: the walk failed, a reference could not be resolved, or
	// a reference was a shape this broker does not write. No loss may be inferred for
	// this incarnation — an inference drawn from evidence that was never gathered is
	// not fail-closed, it is a guess.
	mqttQ2AffProofIncomplete
)

// reconcileQ2AffPubRelProofs advances every assignment this incarnation's stored
// PUBRELs prove the client answered PUBREC for, and reports whether that evidence is
// complete.
//
// It is the correction for a false loss report. mqttProcessPublishReceived stores the
// correlated PUBREL BEFORE mqttFinishQ2AffineDelivery records AffineDone, so a broker
// that stops in between — with the affine consumer then destroyed out of band, taking
// its copies with it off the interest-retained $MQTT_out — leaves exactly the shape
// recoverQ2AffClaims reads as LOST, for a message the client has already answered and
// whose handshake will complete from that very PUBREL on this resume.
//
// Ordering, and both halves are constraints rather than preferences:
//
//   - BEFORE recoverQ2AffClaims, so the proof is on the record before the inference is
//     attempted. Not the correctness property — PubRecProven outranks a Lost that lands
//     first — but it is what keeps the common case from emitting a loss report at all.
//   - BEFORE ensureQ2AffineConsumerSubscription, because the reap is THIS function's to
//     perform: the recovery-and-reap route is skipped entirely when affine restoration
//     fails, so discharging the reap by ordering is unavailable. Nothing here needs the
//     consumer — it walks $MQTT_out by subject and writes to $MQTT_qos2assign.
//
// It is NOT serialized against affine deliveries: openRestoreGate runs before
// resendPendingRedelivery, so a restored callback can be proving the same claim
// concurrently. That is a fenced race, not an absence of one, and the fence is
// proveQ2AffCopyDelivered's nonce-and-copy CAS, which both parties take. Read-loop
// serialization prevents a SAME-CONNECTION PUBCOMP during CONNECT and nothing on
// another broker.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reconcileQ2AffPubRelProofs(c *client) mqttQ2AffProofEvidence {
	as := sess.asm
	sess.mu.Lock()
	pubRelSubject := sess.pubRelSubject
	// A stored PUBREL cannot exist for an incarnation that has no PUBREL consumer:
	// $MQTT_out is interest-retained, so a store with no consumer succeeds and keeps
	// nothing (I12). The crash-window population always has one, because the PUBREC that
	// stores the record goes through trackAsPubRel, which refuses without it, and the
	// consumer is persisted in the session record. So this is the exact precondition, not
	// an optimisation — and skipping on it yields COMPLETE evidence, because "there are
	// no records" is what the walk would have found.
	hasPubRelConsumer := sess.pubRelConsumer != nil
	sess.mu.Unlock()
	if as == nil || pubRelSubject == _EMPTY_ {
		// Nothing could be read, so nothing was ruled out.
		return mqttQ2AffProofIncomplete
	}
	if !hasPubRelConsumer {
		return mqttQ2AffProofComplete
	}
	s := as.server()
	records, _, ok := sess.scanPubRelRecords(pubRelSubject)
	if !ok {
		// A partial walk is never evidence of absence (scanPubRelRecords says so), and
		// here the absence in question is "no PUBREL proves this delivery".
		return mqttQ2AffProofIncomplete
	}
	evidence := mqttQ2AffProofComplete
	for _, r := range records {
		if r.shapeErr != nil {
			// UNREADABLE, so no field of it authorizes anything — including its affine
			// reference, which may be perfectly well formed on a record whose identifier,
			// owner or source correlation is not.
			//
			// The scan RETURNS such a record deliberately, so this walk can account for
			// what a teardown would destroy; accounting for it is not the same as acting
			// on it. Reading its reference would let an unattributable record record
			// PubRecProven and AffineDone and reap the assignment claim — suppressing or
			// retracting a real loss on evidence nothing can attribute.
			//
			// INCOMPLETE rather than skipped: the record may be the very proof this walk
			// was looking for, so its unreadability is exactly the state where a loss must
			// not be inferred either way.
			c.Warnf("MQTT: refusing a stored PUBREL at sequence %v as affine QoS 2 evidence: %v",
				r.seq, r.shapeErr)
			evidence = mqttQ2AffProofIncomplete
			continue
		}
		if r.affRef.refused() {
			// A shape this broker does not write. Refused, not ignored: it names an
			// assignment that cannot be resolved, so neither the proof nor the loss
			// inference may proceed around it.
			c.Warnf("MQTT: refusing the affine QoS 2 reference on a stored PUBREL at sequence %v: "+
				"claim %q copy %q is not a shape this broker writes", r.seq, r.affRef.Claim, r.affRef.Copy)
			evidence = mqttQ2AffProofIncomplete
			continue
		}
		if !r.affRef.valid() {
			continue // no affine reference: an ordinary QoS 2, retained, drained or uncorrelated PUBREL
		}
		claim, cerr := as.q2AffClaimForCarriedCopy(r.affRef.Claim, r.affRef.Copy)
		if cerr != nil {
			if errors.Is(cerr, errQ2AffCarriedCopySuperseded) {
				// POSITIVELY a superseded incarnation of this assignment: the record on
				// the subject is attributable and simply names a different copy, so
				// nothing will ever authorize the one this PUBREL was created for. There
				// is no proof to record and nothing is left unresolved.
				continue
			}
			c.Warnf("MQTT: unable to resolve the affine QoS 2 assignment %q named by a stored PUBREL: %v",
				r.affRef.Claim, cerr)
			evidence = mqttQ2AffProofIncomplete
			continue
		}
		if claim == nil {
			// The claim is gone. Nothing to record, and nothing left for a loss verdict
			// to be untruthful about either — recoverQ2AffClaims will not see it.
			continue
		}
		// The shape check runs BEFORE the already-proven short circuit, not only inside
		// the transition. q2AffClaimForCarriedCopy validates attribution and the copy
		// binding; only q2AffShapePrecheck rejects an unknown phase and the contradictory
		// terminal shapes. Checking it only on the writing path would let a record this
		// broker does not write skip straight to the reap on the strength of a
		// PubRecProven it should never have been allowed to carry.
		if verr := q2AffShapePrecheck(r.affRef.Claim)(claim); verr != nil {
			c.Warnf("MQTT: refusing the affine QoS 2 assignment %q named by a stored PUBREL: %v",
				r.affRef.Claim, verr)
			evidence = mqttQ2AffProofIncomplete
			continue
		}
		if !claim.PubRecProven {
			// The same precheck threaded into the CAS loop, because the record can be
			// rewritten between the reading above and the write.
			if as.proveQ2AffCopyDelivered(r.affRef.Claim, r.affRef.Copy,
				q2AffShapePrecheck(r.affRef.Claim), c.Warnf, true) == nil {
				evidence = mqttQ2AffProofIncomplete
				continue
			}
		}
		// The reap is performed HERE and not left to the caller. See the ordering note
		// above: the route that would otherwise do it is skipped whenever affine
		// consumer restoration fails, which is exactly the population this pass serves.
		as.reapQ2AffClaim(s, r.affRef.Claim)
	}
	return evidence
}

// recoverQ2AffClaims reconciles the assignments a resuming session still owns.
// Its job is to leave no claim in a state that stalls something:
//
//   - materialized, not discarded, AffineDone never recorded, and the copy gone:
//     recorded as LOST. The affine side is always recorded BEFORE the copy is acked
//     (mqttFinishQ2AffineDelivery), so a copy that vanished without that record was
//     never acked BY THE JETSTREAM DELIVERY — the only other thing that removes a
//     message from $MQTT_out is deleting the consumer that held it, which is
//     exactly the loss. This reads the same on every broker and across a handoff,
//     which a "did I see the consumer disappear" flag could not: the broker that
//     observed it may be gone by the time anyone reconciles.
//
//     That inference does NOT extend to the client's PUBREC, and the difference is
//     a known reporting defect rather than a subtlety. mqttProcessPublishReceived
//     stores the correlated PUBREL before it reaches mqttFinishQ2AffineDelivery, so
//     PUBREC then a crash then an out-of-band consumer delete leaves exactly this
//     shape for a message the client already answered. Lost is therefore sometimes
//     a false loss report. It is not a conformance defect — Lost releases the group
//     delivery without re-offering the message, so nothing is redistributed — and
//     correcting it is its own roadmap item. See F-064's residual 1.
//
//   - claimed with the SOURCE MESSAGE authoritatively gone and no copy: no PUBLISH
//     can ever have passed the materialization gate, and no group delivery can
//     ever be re-offered. Mark it `Discarded ∧ SourceAcked` and reap. Without this the
//     claim would sit at `claimed` forever, and contenders would keep honoring a
//     winner for a message that no longer exists. That rule is
//     settleSourcelessQ2AffClaim, extracted so there is ONE definition of it, and it
//     is the arm this comment used to describe as marking the claim "discarded and
//     affine-complete" and letting the reap prove the source side — which was never
//     what the code did: AffineDone would assert a delivery that never happened, and
//     the source side is asserted here rather than inferred later.
//
//   - claimed with the source still present: leave it. The group's AckWait
//     re-offers the source, and whichever member receives it (this one included)
//     materializes from the payload it carries — which recovery does not have.
//
// Best effort: everything here is re-derivable, so a failure just leaves the work
// for the next contender or the next resume.
//
// evidence says whether this resume's PUBREL proofs were fully gathered
// (reconcileQ2AffPubRelProofs). On anything less the LOST arm below writes nothing: an
// absent copy is only evidence of a loss once the thing that would refute it — a stored
// PUBREL naming this assignment — has actually been looked for.
func (as *mqttAccountSessionManager) recoverQ2AffClaims(idHash, gen string, evidence mqttQ2AffProofEvidence) {
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return
	}
	s := as.server()
	err := as.scanQ2AffClaims(func(subject string, claim *mqttQ2AffClaim, _ uint64) error {
		if verr := q2AffClaimUsableForDecision(subject, claim); verr != nil {
			// SKIPPED, not failed: this walk is account-wide and its error aborts the
			// caller, so one unattributable record must not stop an unrelated session's
			// reconciliation. Every arm below writes an incarnation-specific fact or
			// reads this record's copy subject, so neither is safe here.
			if s != nil {
				s.Warnf("MQTT: skipping shared QoS 2 assignment %q during recovery: %v", subject, verr)
			}
			return nil
		}
		if !claim.ownedBy(idHash, gen) || claim.Lost {
			// Lost is terminal, never reapable and mutually exclusive with
			// AffineDone: revisiting it on a later resume — when the consumer has
			// since been recreated and so looks intact — would only risk asserting
			// both that the delivery was proven and that it could not be proven.
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
			// The copy is gone and no delivery ever recorded disposing of it. Record
			// it as LOST: terminal, so no contender waits on it and the group
			// delivery is released, but never reapable, so the evidence survives
			// instead of being erased by a clean reap.
			//
			// LOST is an INFERENCE from an absent copy, never a proof — the field's own
			// comment says "could not prove", not "never delivered". A client that
			// answered PUBREC before the broker stopped leaves this same shape, because
			// the PUBREL is stored before the affine side is recorded. What refutes the
			// inference is that PUBREL, and reconcileQ2AffPubRelProofs is what looks for
			// it; PubRecProven then outranks a Lost that lands first anyway.
			//
			// So on incomplete evidence this writes NOTHING. Concluding a loss from a
			// search that did not happen is a guess, not a fail-closed reading, and the
			// next resume simply tries again.
			if evidence != mqttQ2AffProofComplete {
				if s != nil {
					s.Warnf("MQTT: deferring the disposition of shared QoS 2 assignment %q for session %q: its copy is gone, but this resume could not gather the PUBREL proofs that would show the client answered PUBREC",
						subject, idHash)
				}
				return nil
			}
			// Nonce-fenced: Lost is an inference from THIS claim's copy being absent,
			// so it must not be written onto a later incarnation of the subject that
			// has its own copy.
			updated, _, aerr := as.advanceQ2AffClaim(subject, idHash, gen, claim.Nonce, _EMPTY_,
				func(c *mqttQ2AffClaim) { c.Lost = true })
			if aerr != nil {
				return nil
			}
			// A NIL ERROR IS NOT THE VERDICT, exactly as it is not the proof on the
			// other side of this file. advanceQ2AffClaim reports (nil, 0, nil) when the
			// record vanished between the reading above and its own load, and reports
			// success for a write mergeProven turned into a NO-OP — which is precisely
			// what a concurrent PubRecProven does, since it clears Lost. Announcing a
			// loss on either would report the very thing this item exists to stop: a
			// delivered message called LOST, here without even a record saying so.
			//
			// So the diagnostic is emitted only for a loss that is ON the record this
			// call left behind, and it is worded to assert only what a log CAN assert.
			//
			// There is deliberately NO ordering rule between it and the retraction a
			// proof emits, because the code could not keep one: the record is the
			// authority and the log is a derived view of it, the two cannot be made
			// transactional, and a concurrent proof may clear this verdict and reap the
			// claim between the CAS above and this line. So the report is explicitly
			// PROVISIONAL instead, which stays truthful whichever way the two interleave.
			if updated == nil || !updated.Lost {
				return nil
			}
			if s != nil {
				s.Errorf("MQTT: shared QoS 2 assignment %q for session %q was materialized, but its copy is gone with no record of it being delivered; recording it as a LOST QoS 2 delivery. This verdict is PROVISIONAL: a stored PUBREL proving the client answered PUBREC retracts it, possibly before this line is read, and the assignment record is the authority",
					subject, idHash)
			}
		default:
			// The sourceless rule, and it REAPS INTERNALLY — so this arm returns before
			// the common tail reap below rather than falling through to it. Leaving it
			// to fall through would reap twice.
			as.settleSourcelessQ2AffClaim(s, subject, claim)
			return nil
		}
		as.reapQ2AffClaim(s, subject)
		return nil
	})
	if err != nil && s != nil {
		s.Warnf("MQTT: unable to reconcile the shared QoS 2 assignments of session %q: %v", idHash, err)
	}
}

// settleSourcelessQ2AffClaim is the rule for a NONTERMINAL claim whose SOURCE message
// is authoritatively gone: nothing can ever have passed the materialization gate for
// it and no group delivery can ever be re-offered, so it is marked
// `Discarded ∧ SourceAcked` and reaped.
//
// ONE definition, and exactly ONE caller: recoverQ2AffClaims, on the owning session's
// own resume. **The orphan sweep deliberately does NOT call this**, and that is a
// decision rather than an omission. A sweep-side caller would be acting on a claim
// whose session record is LIVE, and the write can be lost:
//
//  1. it proves the source gone and the copy absent;
//  2. a materializer dispatched earlier, holding the payload, STORES the copy — the
//     store precedes its own phase CAS, so the record is still `claimed`;
//  3. this transition therefore passes its precheck and commits;
//  4. the materializer's advance merges Discarded in, and q2AffCopyIsDisowned
//     withdraws the copy UNPUBLISHED.
//
// The session never ended, so that disposal falsifies the [MQTT-4.8.2-5] premise
// q2AffCopyIsDisowned itself states, and it is the write F-064's dossier refutes as
// non-conformant under [MQTT-4.8.2-4]. No positive fact licenses it: source absence
// does not revoke an already-dispatched payload, copy absence is instantaneous, a
// `claimed` phase does not prove no store is in flight, and there is neither a
// cross-stream transaction nor a bound on how long a materializer may be in flight.
//
// On the OWNER's resume the same window exists and this arm predates it; removing the
// arm would remove existing convergence, so it is HARDENED rather than presented as
// fixed. Three properties do that:
//
//   - The reads run source FIRST and copy LAST, so the copy reading is one round trip
//     old at the CAS rather than two (F-065's R1 discipline: the reading a decision
//     consumes must be no older than the decision).
//   - `Phase != materialized` is a PRECHECK, evaluated against the record the CAS will
//     be applied to. It cannot live in `mutate`, which cannot veto at all.
//   - The shape precheck runs with it, so an unsupported record met mid-window is
//     refused rather than mutated.
//
// The residual is the final copy read to the CAS, which is bounded RETRIES rather than
// one round trip (advanceQ2AffClaimChecked may repeat load-and-CAS up to
// mqttQ2AffClaimAttempts times). It is stated in docs/CONFORMANCE.md, narrowed by the
// fact that moveStartedSharedQoS2 drops a message whose Message Expiry Interval has
// elapsed BEFORE it elects or stores anything — so the actor that could store the copy
// refuses to when the source expired.
func (as *mqttAccountSessionManager) settleSourcelessQ2AffClaim(s *Server, subject string, claim *mqttQ2AffClaim) {
	if !as.q2AffSourceGone(claim) {
		return
	}
	// LAST read before the write.
	if copyState, _ := as.loadQ2AffCopy(claim.CopySubject); copyState != mqttQ2AffCopyAbsent {
		return // present, or unreadable: fail closed either way
	}
	shape := q2AffShapePrecheck(subject)
	_, _, err := as.advanceQ2AffClaimChecked(subject, claim.Winner, claim.Gen, claim.Nonce, claim.CopySubject,
		func(c *mqttQ2AffClaim) error {
			if verr := shape(c); verr != nil {
				return verr
			}
			// A materializer holding the payload can have completed since our reads, and
			// it stores the copy BEFORE its own phase CAS — so this is checked where a
			// refusal stops the write.
			if c.Phase == mqttQ2AffMaterialized {
				return fmt.Errorf("%w: the assignment materialized since this pass read it", errQ2AffPrecheckRefused)
			}
			return nil
		},
		func(c *mqttQ2AffClaim) {
			// Discarded, NOT AffineDone, which would assert a delivery that never
			// happened; and SourceAcked, because a source message that no longer exists
			// cannot be re-offered to anybody, which is exactly what it asserts. Without
			// that the claim could never become reapable.
			c.Discarded = true
			c.SourceAcked = true
		})
	if err != nil {
		// Refused or unresolved: nothing written, and NOTHING REAPED — reaping a claim a
		// materializer just advanced is F-065's harm re-entered.
		return
	}
	// A no-op success still reaps: an already `Discarded ∧ SourceAcked` claim is exactly
	// the shape this arm exists to clear.
	as.reapQ2AffClaimChecked(s, subject, shape)
}

// q2AffSourceGone reports whether the claim's SOURCE message is authoritatively
// absent from $MQTT_msgs. Only a confirmed no-message answer counts; an
// unreadable stream is not an empty one.
func (as *mqttAccountSessionManager) q2AffSourceGone(claim *mqttQ2AffClaim) bool {
	as.mu.RLock()
	probe := as.testQ2AffSourceProbe
	as.mu.RUnlock()
	if probe != nil {
		// Pure observation, before any decision: it cannot change the answer, and it is
		// what lets a test prove this read happens BEFORE the copy read.
		probe(claim)
	}
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

// prepareQ2AffineUnwind fences the affine path and clears it from the session —
// but deletes nothing yet.
//
// disposeNow selects WHEN this generation's assignments are terminated, and the
// two answers are not interchangeable:
//
//   - true: terminate them here, before the caller's confirming save. A failure
//     aborts the whole unwind with the path left intact and usable, because
//     clearing the session's view of a consumer that is still alive would orphan
//     it — the record would stop naming it, nothing would delete it, and the
//     assignments still pointing at it would have no owner able to act. Leaving a
//     working path provisioned after a failed packet is the strictly better
//     outcome: it is session-level, idempotent, and the next SUBSCRIBE reuses it.
//   - false: terminate nothing, and leave the disposal to the caller once its
//     record write is CONFIRMED (see discardQ2AffineUnwindClaims). The discard is
//     MONOTONIC, so a caller whose save is then rejected by a remote winner that
//     PRESERVED this generation would have terminated that winner's started
//     delivery with nothing able to put it back. Session teardown already makes
//     this choice — claimTeardown wins a durable ownership marker before
//     deleteTeardownTargets disposes of anything — and disposing LATE is
//     recoverable rather than stalling: materializeQ2AffOnBehalf discards, acks
//     and reaps a claim whose record no longer names its generation.
//
// Either way the assignments are marked DISCARDED, never released: a delivery
// already started to this session dies with the subscription that was never
// established, which [MQTT-4.8.2-5] permits, rather than being handed to another
// member.
//
// Runs from the client's readLoop, or from the resolver. sess.mu not held on entry.
func (sess *mqttSession) prepareQ2AffineUnwind(c *client, disposeNow bool) mqttQ2AffUnwind {
	gen, durable := sess.closeQ2AffAndDrain()
	if gen == _EMPTY_ {
		sess.mu.Lock()
		sess.q2AffClosing = false
		sess.mu.Unlock()
		return mqttQ2AffUnwind{}
	}

	if disposeNow && sess.asm != nil {
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

// q2AffineGenerationOwnsClaims reports whether this (client, generation) still
// owes a delivery — an assignment that is NOT terminal.
//
// The predicate is `terminal()`, not "not discarded". All three terminal states
// mean no further MQTT delivery can come of the claim, and two of them PERSIST:
// an `AffineDone` claim outlives a failed ack or reap, and a `Lost` claim is
// deliberately never reaped, because it is the only durable evidence that a
// message the broker accepted could not be proven delivered. Treating either as an
// outstanding obligation would retain this path forever — for a never-expiring
// session, the broker's lifetime — which is the exact leak the reclaim exists to
// close.
//
// FAILS CLOSED: the scan is account-wide and aborts on a claim it cannot read or
// attribute, and an aborted scan is reported as "yes, it owes something" — the
// only safe answer for a caller about to remove the path a delivery would use.
//
// sess.mu not held on entry.
func (sess *mqttSession) q2AffineGenerationOwnsClaims(gen string) (bool, error) {
	if sess.asm == nil || gen == _EMPTY_ {
		return false, nil
	}
	var owes bool
	if err := sess.asm.scanQ2AffClaims(func(_ string, claim *mqttQ2AffClaim, _ uint64) error {
		if claim.ownedBy(sess.idHash, gen) && !claim.terminal() {
			owes = true
		}
		return nil
	}); err != nil {
		return true, err
	}
	return owes, nil
}

// discardQ2AffineUnwindClaims terminates the assignments of a generation the
// record has been CONFIRMED to no longer authorize, for an unwind prepared with
// disposeNow false.
//
// Safe to run late, and safe to fail: a claim whose winning record no longer
// names its generation is discarded, source-acked and reaped by
// materializeQ2AffOnBehalf on the next source redelivery. What is NOT safe is
// running it early — see prepareQ2AffineUnwind.
//
// sess.mu not held on entry.
func (sess *mqttSession) discardQ2AffineUnwindClaims(u mqttQ2AffUnwind) error {
	if !u.prepared || sess.asm == nil {
		return nil
	}
	return sess.asm.discardQ2AffClaimsFor(sess.idHash, u.gen)
}

// releaseQ2AffineUnwindLocally drops the LOCAL half of a detached path: its
// delivery subscription and its share of the session's MaxAckPending budget.
//
// Split out from finishQ2AffineUnwind because the two halves are gated on
// different facts. Deleting the consumer needs the generation's assignments to
// have been terminated first — $MQTT_out is interest-retained, so the delete
// takes any copies with it — while the subscription and the charge belong to a
// path this session has already stopped owning, and nothing else can ever reach
// them: they are not on the record, and no sweep reaches another broker's in-memory
// accounting — nor, since F-036 course E, the consumer either. Stranding them leaks a
// slice of mqttMaxAckTotalLimit for the life of the session.
//
// sess.mu not held on entry.
func (sess *mqttSession) releaseQ2AffineUnwindLocally(c *client, u mqttQ2AffUnwind) {
	if !u.prepared {
		return
	}
	if u.subscribed && u.deliver != _EMPTY_ {
		if err := c.processUnsub([]byte(u.deliver)); err != nil {
			c.Warnf("Unable to remove the affine QoS 2 delivery subscription on %q: %v", u.deliver, err)
		}
	}
	// A path restored from a record was never charged against THIS process's
	// budget, so refunding it would credit a charge that was never made.
	if !u.charged {
		return
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

// finishQ2AffineUnwind deletes what prepareQ2AffineUnwind detached, once the
// record that no longer names it is confirmed.
func (sess *mqttSession) finishQ2AffineUnwind(c *client, u mqttQ2AffUnwind) {
	if !u.prepared {
		return
	}
	sess.releaseQ2AffineUnwindLocally(c, u)
	if !u.charged {
		return
	}
	// A failure here is TERMINAL for that consumer: this is the only attempt, the
	// record no longer names it, and since F-036 course E no sweep collects it. See
	// docs/CONFORMANCE.md's no-retry-anchor residual.
	if _, err := sess.jsa.deleteConsumer(mqttOutStreamName, u.durable, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
		c.Errorf("Unable to delete the affine QoS 2 consumer %q of a rolled-back SUBSCRIBE, and nothing will retry it; "+
			"it is retained with any undelivered copies and must be removed by hand: %v", u.durable, err)
	}
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
	for _, m := range sess.sharedSubs {
		if m.QoS == 2 {
			return true
		}
	}
	return false
}

// mqttRecordHasSharedQoS2 is hasSharedQoS2Sub over a PERSISTED record: does it
// claim any shared subscription at QoS 2, the only kind the affine path is
// provisioned for. See mqttValidateRecordAffinePair for the invariant it feeds.
func mqttRecordHasSharedQoS2(ps *mqttPersistedSession) bool {
	for _, m := range ps.SharedSubs {
		if m.QoS == 2 {
			return true
		}
	}
	return false
}

// unwindProvisionedQ2AffineOnLanded reclaims an affine path whose PROVISIONING
// save was ambiguous and has since been PROVEN durable.
//
// An ambiguity is settled by re-attempting the attempt's exact bytes, so what
// becomes durable is the record as the FIRST filter loop serialized it: a session
// naming this consumer and its generation. No membership this PACKET tried to add
// can accompany it — shared memberships are persisted in a later loop, and every
// save is refused while the session is unresolved — and no sweep collects the
// consumer in any case, since F-036 course E removed that route. Neither the
// packet's unused-path unwind nor its rollback can reconcile it either: their own
// confirming saves are refused for the same reason. Without this continuation the
// path, its MaxAckPending charge and its consumer survive until the session is
// torn down — for a session that never expires, the broker's lifetime.
//
// What licenses the removal is that the PACKET FAILED, NOT that the generation is
// provably unreachable or claim-free. It is neither in general: a packet carrying
// a SECOND shared QoS 2 filter re-enters this function with the consumer already
// present, skips the save and opens the readiness gate below on a session that is
// still unresolved, so a move can transiently claim and copy under this generation
// before the membership rollback. A membership the record already CARRIED is a
// different matter again, and is what the guard in the continuation is for.
//
// Registered only when the provisioning call MINTED the generation, which is what
// makes the path this call's to unwind rather than one it merely re-established.
//
// The claims this discards are the ones the packet's own unwind would have
// discarded had the write been confirmed — a delivery started to a session whose
// subscription never became durable dies with it, which [MQTT-4.8.2-5] permits.
// Deferred to the settlement rather than done at failure time so it is one
// arbitrated CAS on a record we have proof of, not a guess against one we do not.
//
// OWNERSHIP IS ARBITRATED BEFORE ANYTHING IRREVERSIBLE. The unwind is prepared
// with disposeNow FALSE, so the assignments survive until the cleanup record is
// confirmed: a remote winner that took the session over and PRESERVED this
// generation wins the CAS instead, and the definite arm then restores a path
// whose claims were never touched. The other two unwind sites still dispose
// first; converting them has its own failure semantics and is filed in
// docs/ROADMAP.md.
//
// Runs from the resolver, holding its record transaction and its ownership gate.
// Safe against the move fence: closeQ2AffAndDrain waits only for in-flight move
// operations, and a move touches $MQTT_qos2assign and $MQTT_out — never the
// session record — so it cannot be waiting on the very transaction we hold.
//
// sess.mu not held on entry.
func (sess *mqttSession) unwindProvisionedQ2AffineOnLanded(c *client, tok recordTx) {
	// A real membership outranks this: an affine path a live shared QoS 2
	// subscription is owed is not ours to reclaim.
	//
	// Kept as a FAIL-CLOSED BACKSTOP, not for a shape a valid record can produce.
	// The continuation registers only when minting was needed, meaning q2AffGen was
	// empty on entry — and a record whose SharedSubs carries a qos-2 entry without a
	// generation is now refused at CONNECT, in the sweep and at expiry
	// (mqttValidateRecordAffinePair), so no interpretable record reaches here with a
	// membership to find. It stays because the two do not state the same fact: the
	// refusal rejects a record shape at load, this refuses an IRREVERSIBLE disposal
	// given the in-memory state, and any hole in the refusal would otherwise become
	// assignments discarded out from under a started delivery ([MQTT-4.8.2-5]).
	if sess.hasSharedQoS2Sub() {
		return
	}
	u := sess.prepareQ2AffineUnwind(c, false)
	if !u.prepared {
		// There was nothing to unwind. Not ours to delete.
		return
	}
	// A STARTED DELIVERY outranks the reclaim. The packet failed; it did not end
	// the session — and `Discarded` means the owning session ENDED. An assignment
	// this generation still owns therefore belongs to a session that is still
	// live, which [MQTT-4.8.2-5] requires to complete the delivery it started. So
	// the path is put back and session teardown becomes what ends it, which is the
	// one event that makes the discard truthful.
	//
	// This is NOT check-then-act. prepareQ2AffineUnwind fenced and DRAINED the path
	// first, and it returns with the generation detached — and beginQ2AffOp admits
	// nothing without a generation and readiness — so no move can add an assignment
	// between the scan and the decision. A remote winner can still create one after
	// it, which is what the CAS-first ordering below is for.
	if owns, err := sess.q2AffineGenerationOwnsClaims(u.gen); owns {
		sess.restoreQ2AffineUnwind(u)
		if err != nil {
			c.Errorf("MQTT: cannot prove the unused affine QoS 2 path of %q owns no assignment, so it is retained: %v", sess.id, err)
		} else {
			c.Warnf("MQTT: keeping the unused affine QoS 2 path of %q: its generation still owns a started delivery, which only the end of the session may discard", sess.id)
		}
		return
	}
	// dispose runs only once the record that no longer authorizes this generation
	// is CONFIRMED. A failure to terminate the assignments stops short of the
	// CONSUMER DELETE and of nothing else: $MQTT_out is interest-retained, so
	// deleting it would take any copies with it.
	//
	// That consumer is then PERMANENTLY UNCOLLECTED. This comment used to hand it to
	// the orphan sweep; F-036 course E removed that route, and nothing replaced it —
	// InactiveThreshold is deliberately zero and $MQTT_out refuses
	// ConsumerLimits.InactiveThreshold. It is a stated residual in
	// docs/CONFORMANCE.md: every authorized cleanup route that fails after dropping
	// the reference has no retry anchor. The LOCAL half is released either way: the
	// delivery subscription and the MaxAckPending charge belong to a path this session
	// no longer owns, and they are reachable from nothing else once the record stops
	// naming it.
	dispose := func() {
		if err := sess.discardQ2AffineUnwindClaims(u); err != nil {
			sess.releaseQ2AffineUnwindLocally(c, u)
			c.Errorf("MQTT: the unused affine QoS 2 path of %q is removed from its record but its assignments could not be terminated; releasing its subscription and budget, and RETAINING the consumer %q with its undelivered copies — nothing collects it, so it must be removed by hand once no session serves that affine generation: %v", sess.id, u.durable, err)
			return
		}
		sess.finishQ2AffineUnwind(c, u)
	}
	// Detach, persist a record that no longer names the path, and only then
	// dispose of anything.
	outcome, err := sess.saveClassified(tok)
	switch {
	case err == nil:
		dispose()
	case outcome == saveDefinitelyFailed:
		// Nothing stored, so the session must go back to describing the resources
		// that are still live, and the path reverts to being the teardown residual.
		// Nothing was disposed of, so the restore is complete rather than partial —
		// which is the whole reason the claims were not touched above. A remote
		// winner that preserved this generation keeps its assignments.
		sess.restoreQ2AffineUnwind(u)
		c.Errorf("MQTT: unable to persist the removal of a settled but unused affine QoS 2 path for %q: %v", sess.id, err)
	default:
		// Ambiguous again: the record may already omit the path. Keep the detached
		// view (it matches what may have landed) but dispose of nothing until the
		// NEXT settlement proves the record no longer names it. Deliberately NOT
		// resolved from here — the resolver holds the fence, so an attempt from
		// inside it would be refused; resolutionAfterContinuations reports the
		// session unresolved and the periodic pass retries.
		sess.retainOnLanded(func(recordTx) { dispose() })
		c.Errorf("MQTT: the removal of a settled but unused affine QoS 2 path for %q is unresolved: %v", sess.id, err)
	}
}

// mqttTestAssignTailHookV fires before each authoritative read of the assignment stream's end.
var mqttTestAssignTailHookV atomic.Pointer[func()]

func setMQTTTestAssignTailHook(h func()) func() {
	prev := mqttTestAssignTailHookV.Load()
	if h == nil {
		mqttTestAssignTailHookV.Store(nil)
	} else {
		mqttTestAssignTailHookV.Store(&h)
	}
	return func() { mqttTestAssignTailHookV.Store(prev) }
}
