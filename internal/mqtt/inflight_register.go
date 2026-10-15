// Durable identity for an outbound QoS 1/2 in-flight delivery.
//
// pendingPublish, pendingPubRel, cpending and last_pi are memory-only. A
// same-process reconnect therefore reused a started delivery's packet identifier,
// but a broker restart, a failover, or a resume on another broker did not: the
// tracking maps came back empty, trackPublish reported the delivery as not started,
// and the redelivery went out with DUP set under a FRESHLY ALLOCATED identifier. A
// client is entitled to read that as a new message. [MQTT-4.4.0-1] requires both
// sides to re-send unacknowledged PUBLISH and PUBREL packets under their ORIGINAL
// identifiers, so this bound the whole broker's exactly-once guarantee to one
// process lifetime — for QoS 1 as much as for QoS 2, and on every outbound path
// rather than only the shared-subscription one that first surfaced it.
//
// An IN-FLIGHT REGISTER is the missing durable fact. One tiny record per started
// PUBLISH-phase delivery, on $MQTT.inflight.<idHash>.<gen>.<PI> (MaxMsgsPer:1, so
// each identifier is a single-slot register), naming the source durable and stream
// sequence the delivery came from. On resume the registers are read back before any
// source interest exists, so the redelivery that follows finds cpending already
// populated and goes out under the identifier the client is holding.
//
// # Why a stream of its own
//
// $MQTT_out cannot carry it: that stream is interest-retained, so a store with no
// consumer succeeds and keeps nothing, and a register has no consumer by design (it
// is read by direct subject lookup and by a bounded walk, never delivered). The
// session record cannot carry it either — a record write per delivery would put
// every outbound QoS 1/2 message behind the record CAS and the record fence, which
// is the one lock the delivery path must not contend on.
//
// # Invariants
//
// R1 PERSIST BEFORE SEND. A PUBLISH under a NEW identifier goes on the wire only
// after its register is CONFIRMED at a sequence. The register is created as a PAIR with
// a sibling UNSENT MARKER in one atomic batch (inflight_marker.go): the register records
// the allocation, the marker records that nothing has been transmitted yet, and the
// outgoing DUP flag is derived from the marker's absence and nothing else (F-072). From
// allocation until confirmation the pending entry carries regPending, and a regPending
// entry is not sendable by anyone: a concurrent redelivery of the same sequence returns
// identifier 0 (AckWait re-offers it), resendPendingRedelivery does not NAK it, and
// stageDrain does not enrol it. A definite store failure untracks the identifier and
// sends nothing, which is safe precisely because nothing else could have sent it. An
// ambiguous store is resolved by RE-READING the subject and matching the nonce,
// never by the shape of the response.
//
//	R1 is also the takeover fence. An identifier the client can be holding implies
//	a completed send, which implies a confirmed register, which implies the next
//	broker's scan sees it. A superseded broker's late register write therefore
//	concerns an identifier that never reached the client; it is inert. If recovery
//	cannot distinguish it from a second transmitted claimant for one source, the
//	duplicate-(Dur,Seq) rule reserves both and fails closed rather than guessing.
//
// R2 ONE DURABLE AUTHORITY FOR THE WHOLE EXCHANGE. The slot owns the identifier from
// PUBLISH until terminal cleanup. PUBREC re-phases that same slot to pubrel_ready only
// after an exact read proves the correlated PUBREL record exists; the record is evidence
// and payload, not a second identifier claimant. Admission then CAS-acquires an action on
// the slot before any slot-backed PUBREL write, and PUBCOMP advances it to terminal before
// disposal. (A live retained exchange has no register and so no slot, R6; it is outside R2
// and is authorized instead by the live in-memory owner identity of its incarnation.)
// Recovery completes a publish-state handoff only from a PRESENT record matching the
// slot's PI, nonce, source and affine identity. Source-only correlation proves no exchange
// and is quarantined. A complete source does not finish an exchange whose PUBREL still
// exists—the client may still owe PUBCOMP.
//
// R3 RESERVE BEFORE INTEREST. On resume every identifier named by a register or by a
// PUBREL record is reserved in the pending maps BEFORE any source interest exists.
// This also closes a pre-existing hazard: pendingPubRel was empty until the PUBREL
// callback replayed a record, so a fresh bumpPI could mint an identifier an
// unreplayed PUBREL still owned.
//
//	An incomplete identifier space does NOT deny the CONNECT. It sets
//	piSpaceIncomplete, which stops bumpPI ALLOCATING anything new while leaving
//	everything else working: QoS 0 flows, exchanges already reserved complete, and the
//	drain barrier's PUBREL recreation still runs under its RECORDED identifiers.
//	Withholding allocation is the precise remedy, because allocation is the only
//	operation an unknown identifier space makes unsafe — a withheld QoS 1/2 delivery is
//	redelivered by AckWait, exactly as it is at the in-flight cap. Denying the CONNECT
//	was considered and rejected: it would take a whole session offline over a transient
//	scan failure, and it contradicts the drain barrier's contract, which withholds a
//	durable's interest rather than the connection (recoverDrainPubRels).
//
//	The conditions that set it are all the same condition wearing different clothes:
//	the identifier space cannot be fully accounted for. They include:
//
//	 1. A scan that could not complete — identifiers may be in use that we never saw.
//	 2. A register whose body is UNREADABLE — we see the identifier (its subject names
//	    it) but not the delivery that owns it, so its source is still unacked and its
//	    redelivery would find no cpending entry and take a fresh identifier.
//	 3. TWO registers naming ONE source on different identifiers — we see both
//	    identifiers but cannot say which one the client holds, because a stream
//	    sequence orders the writes and not the sends.
//	 4. A PUBREL and PUBLISH slot that name one source but not one whole exchange, or
//	    an exact handoff whose phase CAS cannot be proved.
//
//	In each case reserving identifiers is necessary but NOT sufficient: what remains
//	dangerous is issuing a new one, or re-sending a source under a guess. Only
//	withholding allocation covers both. The flag is cleared by the next resume that
//	completes its scans and finds nothing unaccountable, and by clear(), which retires
//	the whole identifier space along with the incarnation.
//
// R4 SEQUENCES ARE NOT IDENTITIES. A register subject is reused by later exchanges
// within one incarnation, so every delete targets the EXACT sequence read, and only
// a nonce match proves a record is ours. Same rule, same reason, as scope rule 2 of
// qos2_release_claim.go.
//
// R5 NO SILENT SURVIVORS. Every terminal path that releases an identifier also settles
// its slot: PUBACK, a failed PUBREC, Message Expiry and Maximum Packet Size delete a
// PUBLISH slot; PUBCOMP moves a QoS 2 slot to terminal, disposes every matching PUBREL
// record, then exact-deletes it. A failed or uncertain terminal delete keeps an in-memory
// reservation and retries—it may delay reuse, but it never silently frees an identifier
// whose durable owner may survive.
//
// R6 REGISTERED IS NOT TRANSMITTED; THE MARKER DECIDES. Since F-072 a confirmed register
// does NOT imply a transmitted PUBLISH — the register is created before its send, so the
// two are distinct durable facts. The PUBLISH-phase pair carries both: the register
// (allocated) and the unsent marker (present ⇔ not yet transmitted). DUP is
// mqttPending.transmitted, set by the send section that wrote and by a PUBREC rollback,
// seeded on resume from the marker's presence, and derived from the JetStream delivery
// count NOWHERE. A delivery withheld by the in-flight cap is still never registered
// (trackPublish returns before allocating). A retained-message send has no JetStream
// backing at all, so it has no register, no marker and no transmitted bit (R2's slot-less
// admission), and no post-restart redelivery.
//
// # Cost
//
// One atomic-batch round trip per FIRST transmission of every outbound QoS 1/2 message
// (the register and its unsent marker together), plus a marker delete at the transmission
// and a register delete at completion, plus (QoS 2 only) the phase/action CASes and a
// confirmed source ack. The marker delete is fire-and-forget: nothing waits on it, a lost
// one is retried by the next transmission or terminal pass and reaped by the sweep.
// Recovery custody refresh and a route-B handoff also rewrite selected QoS 2 slots;
// ordinary redelivery does not create a second register. The pair is one batch by
// necessity, not preference: the marker must be as durable as the register and share its
// fate exactly, and the earlier note that "batching would delay first transmissions" was
// measured against a different design — this batch IS the first transmission's store. The
// obvious alternative — register after sending — remains refused: it reintroduces the
// deviation as a narrower window (F-072's refused design C).

package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nuid"
)

const (
	// mqttInflightNonceLen is the length of a register's exchange nonce, a
	// nuid.Next() like every other identity token here.
	mqttInflightNonceLen = 22

	// Bounded budget for the create-only CAS. Every failure is resolved by an
	// authoritative re-read rather than a blind retry, so a small budget suffices;
	// exhaustion is reported, never guessed.
	mqttInflightStoreAttempts = 3

	// mqttInflightLoadBatch bounds one pipelined batch of register or source loads on the
	// resume path. A resumed session may hold up to 65_535 identifiers, so recovery reads
	// in batches rather than one synchronous request per record — that difference is
	// whether a restart's first reconnect completes inside a client's timeout. Bounded
	// rather than unbounded so one enormous session cannot build a single request set of
	// arbitrary size.
	mqttInflightLoadBatch = 1024

	// mqttInflightClaimWindow bounds the concurrent CAS writes by which a resume
	// takes ownership of shared-delivery registers before installing them in memory.
	mqttInflightClaimWindow = 32

	// mqttInflightClaimContestAttempts bounds how many OLDER claims one contest may
	// take over in sequence. Contenders are ordered and each stops permanently at its
	// first sight of a newer claim, so a chain this long needs as many distinct
	// accepted resumers racing one register inside one barrier window; exhaustion is
	// classified DISPLACED — the resume is refused and the retry meets a settled
	// contest — never as an opaque reservation on the rightful newest owner.
	mqttInflightClaimContestAttempts = 64
)

// mqttInflightRecord is the durable in-flight register.
//
// PI is the identifier the PUBLISH went out under. Dur and Seq name the source
// delivery, which is what lets recovery decide whether the exchange is still live
// and reattach the identifier to the redelivery of that exact sequence. Nonce is the
// record's immutable identity: the subject is reused by later exchanges on the same
// identifier, so a sequence proves nothing (R4) and only the nonce proves a record is
// the one this session wrote.
//
// Q2AffClaim carries the session-affine assignment claim for a shared QoS 2 copy, and
// is not optional decoration: after a restart the recovered pending entry — or the
// recovered RETIREMENT, where the stored PUBREL won that race — is the ONLY route by
// which the claim reaches the terminal paths that must record the affine side as
// complete (see the mqttPending comment on q2AffClaim). Empty for every ordinary
// delivery.
//
// Q2AffCopy is the SUBJECT of the copy this delivery carried, and it is what binds the
// pair together. It replaces an earlier incarnation field for two reasons: the copy
// subject already CONTAINS the incarnation, so a separate nonce was redundant with it;
// and a nonce alone cannot say WHICH copy the claim reference belongs to, so a register
// naming another claim's valid subject and incarnation would credit that claim with this
// delivery and let its own copy be acknowledged away unpublished — losing that message.
// Every user therefore requires the loaded claim's CopySubject to EQUAL this value, and
// recovery additionally requires the stored message at this register's own Dur/Seq to be
// on it. A record carrying a Q2AffClaim without a valid Q2AffCopy is REFUSED rather than
// interpreted — there are no releases, so that shape does not exist (F-065, and the
// compatibility boundary in docs/ARCHITECTURE.md).
// Retired is the durable half of an in-memory SOURCE retirement: the client answered
// PUBREC, but acknowledgement of the source could not be proven, so a re-offer must be
// acknowledged away rather than transmitted again. It is not a slot phase. In particular,
// a crash can leave State at publish even though Retired landed; only a present exact
// PUBREL lets recovery advance that slot to pubrel_ready. See retireSourceLocked and
// mqttSession.srcAckOwed.
type mqttInflightRecord struct {
	PI    uint16 `json:"pi"`
	Dur   string `json:"dur"`
	Seq   uint64 `json:"seq"`
	QoS   byte   `json:"qos"`
	Nonce string `json:"nonce"`
	// SourceAckReply is the offered source reply, independent of abandonment evidence.
	SourceAckReply string `json:"source_ack_reply,omitempty"`
	// Lin is the exchange's LINEAGE: the identity of the CLIENT-FACING obligation, and it is
	// MANDATORY. It is not the nonce, and the distinction is the whole reason it exists.
	//
	// The NONCE identifies the ALLOCATION, and the same-delivery replacement arm deliberately
	// re-mints it ("Take the slot over at its exact sequence so the record carries OUR
	// nonce") so that the previous writer's cleanup is fenced off the new record. But that
	// same arm INHERITS the client's obligation when the occupant's marker is
	// authoritatively absent: it rewrites the register alone, "with no marker minted for an
	// exchange the client may already hold", and preserves the occupant's QoS because that is
	// "evidence about what the CLIENT may already be holding". So after a replacement the
	// record is a new allocation for an OLD obligation, and no field said which obligation.
	//
	// The lineage says it. It is minted with a fresh allocation and carried across exactly
	// the replacement that inherits, so a durable fact recorded about the obligation still
	// names it afterwards. Two identities, two questions: "whose write is this" and "whose
	// message is this".
	//
	// Which value each write puts here is decided by THAT ATTEMPT's write mode, never once
	// per call: the store loop resets to a create-only pair after any failed attempt without
	// rebuilding the record, so a lineage chosen for a replacement and then carried into the
	// retry would stamp a genuinely new exchange with an old obligation. See
	// storeInflightRegister.
	//
	// mqttLineageForWrite is the one definition of that per-mode value. The admission ladder
	// applies it, and custody's charge, the ambiguous-attempt record and the landed settlement
	// all read the value the ladder froze; the replacement composer and opAdmitPublish refuse a
	// register that disagrees with it. It is also the identity F-097's completion evidence is
	// keyed on.
	Lin string `json:"lin"`
	// Acq is the exchange's TRANSMISSION IDENTITY counter, and it is MANDATORY.
	//
	// It is incremented by every action-acquisition CAS and is NEVER reset — not by a
	// drain, not by a phase advance, not by any other same-allocation write. Overflow is
	// REFUSED rather than wrapped.
	//
	// An exchange's authority identity is the triple (slot subject, Nonce, Acq), and every
	// release, deletion and capacity free CASes on the whole triple. Nothing else in the
	// record can play this role: Nonce is per-ALLOCATION, Lin is per-OBLIGATION,
	// beginSendLocked's send nonce is process-local, ActionToken is erased by a drain, and
	// the slot's stream sequence also moves for custody writes and retirement marks, so
	// evidence keyed on it goes stale spuriously.
	//
	// THE BINDING RULE IS WHAT MAKES IT SOUND, and omitting it reopens the hole: an
	// acknowledgement is stamped with the transmission identity ALREADY BOUND to that
	// handler's exchange, never by fetching the slot's current Acq after the packet
	// arrives — otherwise A receives an old acknowledgement, pauses, and stamps it with B's
	// newer counter. A retry may refresh the CAS SEQUENCE; it may never refresh the
	// evidence identity.
	//
	// This is a conservative AUTHORIZATION rule. It is not a claim that MQTT
	// acknowledgements identify individual retransmissions: an acknowledgement that cannot
	// be attributed to a live transmission does not discharge one.
	//
	// Mandatory means PRESENCE-validated, by the decoder's key walk, not "non-zero": a
	// fresh allocation legitimately carries zero, and silently reading a missing field as
	// zero would give a record from an unsupported shape the identity of a brand-new one.
	Acq uint64 `json:"acq"`
	// Claim identifies the session OBJECT entitled to reconcile this shared-delivery
	// register — the identity a resume takes and gen deliberately keeps. Empty on
	// records that need no arbitration; ignored on non-shared durables. ClaimSeq is
	// the claimant's session-record sequence at its accept save, which ORDERS rival
	// claims: record CAS makes every accept sequence unique, so a claim contest has
	// exactly one newer side, and that side takes the register while the older one
	// reads displacement.
	Claim      string `json:"claim,omitempty"`
	ClaimSeq   uint64 `json:"claimseq,omitempty"`
	Q2AffClaim string `json:"q2aff,omitempty"`
	Q2AffCopy  string `json:"q2affcopy,omitempty"`
	Retired    bool   `json:"retired,omitempty"`
	// Cancelled records a DECISION to stop delivering this allocation: it was withdrawn — by a
	// packet staging a drain (an UNSUBSCRIBE, or a QoS 0 downgrade), a discard, a takeover —
	// typically while its store was still in flight.
	//
	// IT IS NOT EVIDENCE OF NON-TRANSMISSION (F-088). The writer cannot see a PUBLISH another
	// broker, or an inherited occupant, already put on the wire, so the bit never frees an
	// identifier, never exempts it from Receive Maximum and never licenses destroying the
	// register a late acknowledgement would complete. The allocation's unsent marker is the
	// only proof the PUBLISH never went out: with it standing the cancellation is genuine and
	// its withdrawal ends the obligation; without it the register is possibly-attempted and is
	// preserved (a client's answer clears a known-false bit, mqttRefutedCancellation). The one
	// cancellation disposed of on its own say-so is a PENDING FENCE, by its written provenance
	// (CancelOrigin below), never by this bit.
	Cancelled bool `json:"cancelled,omitempty"`
	// CancelOrigin is the WRITTEN provenance of a cancellation, present iff Cancelled (F-088,
	// consult C4). The body alone cannot say which of two very different records a cancelled
	// register is: a fresh admitted QoS 1 register carries the same fields as the sparse
	// tombstone written before any admission, and copying it and setting Cancelled produces
	// that shape exactly. So the provenance is written by the writer that knows, never
	// inferred from sparsity, lineage equality or an absent exchange:
	//
	//   - mqttCancelOriginPendingCreate: a create-only cancellation FENCE installed on an
	//     empty subject before any admission landed. It is not an exchange's identity carrier,
	//     and it is the one cancellation that may be disposed by its provenance alone.
	//   - mqttCancelOriginOccupant: a cancellation written over an occupant by CAS. Whatever
	//     that occupant was, it stays it: its cleanup rests on marker or exchange-ending
	//     evidence like any register.
	//
	// A Cancelled register without it, or with an unknown value, is an UNSUPPORTED shape:
	// refused at decode, never interpreted and never migrated ("Compatibility boundary").
	CancelOrigin mqttCancelOrigin `json:"cancel_origin,omitempty"`
	// AbandonReason and AbandonTenure are set ONLY on a delivery_abandoned register: why the
	// broker abandoned delivery (a closed set, mqttValidAbandonReason) and the tenure of the
	// connection whose authorization decided it — the durable record of the authorizing
	// binding (the exchange's own Tenure stays its admission tenure). See
	// outbound_abandon_delivery.go.
	AbandonReason string `json:"abandon,omitempty"`
	AbandonTenure string `json:"abandontenure,omitempty"`

	// State is the exchange's phase, and it is MANDATORY. One record owns the identifier
	// for the exchange's whole life rather than only its PUBLISH phase, and the phase says
	// who may act. An absent or unknown state is an unsupported
	// persisted shape and is REFUSED — never read as PUBLISH, which would license a
	// PUBLISH under an identifier already past PUBREC. See inflight_slot_state.go.
	State mqttInflightState `json:"state"`

	// ActionToken is the live PUBREL action's licence: the durable half of the exclusion
	// that lets one broker transmit a PUBREL while every other writer refuses.
	//
	// A successful CAS installing it establishes an invariant that REMAINS TRUE THROUGH
	// the action, which is what a re-read cannot do — see inflight_slot_state.go's
	// "Why a re-read is not a fence". Valid ONLY in the two active states, and the
	// validator refuses it anywhere else.
	//
	// There is no TTL, no expiry and no steal. An abandoned action holds its identifier
	// until its own holder drains it, the generation is discarded, or an operator
	// intervenes: a lease would not make this correct, because a broker can pause after
	// its own expiry check and resume into the same stale write, and an MQTT client
	// cannot validate a fencing token so the external effect is not fenceable
	// downstream. That cost is stated in docs/CONFORMANCE.md rather than disguised.
	ActionToken string `json:"action,omitempty"`
	// ActionClaim and ActionClaimSeq are the custodian that acquired the action. They
	// MUST equal Claim/ClaimSeq: an action belongs to the custody that took it, and a
	// mismatch means one of the two was rewritten underneath the other.
	//
	// ActionClaimSeq is an ORDERING sequence, exactly as ClaimSeq is, and NOT an
	// acceptance sequence. Defining it as one is unimplementable: ClaimSeq is sess.seq
	// snapshotted at store time and sess.seq is replaced by every confirmed save, so
	// comparing it to the current sequence invalidates refreshed slots while comparing
	// it to the original acceptance rejects newly allocated ones. Current ownership is
	// proved separately, by Claim equality against the session object's constructor-minted
	// regClaim plus a live $MQTT_sess acceptance read.
	ActionClaim    string `json:"actclaim,omitempty"`
	ActionClaimSeq uint64 `json:"actclaimseq,omitempty"`
	// PubRelDurable and PubRelStreamSeq name the CANONICAL PUBREL record: the consumer
	// durable and the exact $MQTT_out sequence.
	//
	// Decided ONCE — on the live path from the confirmed store readback, and in recovery
	// from the lowest matching sequence over a COMPLETE candidate scan — and never
	// replaced merely because another record was delivered first. The sequence comes from
	// the actual message or an exact readback, never from Nats-Msg-Id, a drain attempt
	// counter or the source sequence.
	//
	// Mandatory in every past-PUBREC state and forbidden in `publish`, where no record
	// exists yet.
	PubRelDurable   string `json:"preldur,omitempty"`
	PubRelStreamSeq uint64 `json:"prelseq,omitempty"`

	// Payload and Digest are the RETAINED arm of the register's source (F-105 7c): a retained
	// delivery has no source consumer and no sequence, so it names the materialization it
	// replays from instead. The two arms are EXCLUSIVE and exactly one is present — Dur/Seq for
	// an ordinary delivery, Payload/Digest for a retained one — and the decoder refuses a record
	// carrying both or neither. A retained register that carried a fabricated Dur would be
	// believed by every reader that asks a consumer about a sequence (outbound_source_union.go).
	Payload string `json:"payload,omitempty"`
	Digest  string `json:"digest,omitempty"`
	// RelSubj and RelID are a retained QoS 2 exchange's CANONICAL PUBREL: the in-flight arm of
	// the canonical reference, an exchange-specific record on $MQTT_inflight written in the same
	// batch as the PUBREC advance. The identity is carried beside the subject because a subject
	// locates an address and only the identity says the record there is this exchange's. They
	// take the place PubRelDurable/PubRelStreamSeq hold for an ordinary exchange, and the two
	// canonical arms follow the source arms.
	RelSubj string `json:"rel_subj,omitempty"`
	RelID   string `json:"rel_id,omitempty"`
}

// retainedSource reports whether the register's source is the RETAINED arm. The arm is decided
// by which members the record carries — the decoder admits exactly one arm — so this reads a
// validated fact rather than guessing from an absence.
func (r *mqttInflightRecord) retainedSource() bool {
	return r != nil && (r.Payload != _EMPTY_ || r.Digest != _EMPTY_)
}

// source is the register's source as the union this design reasons in. Both arms carry what
// they carry and nothing of the other's, so a reader branching on Kind cannot be handed a
// fabricated coordinate.
func (r *mqttInflightRecord) source() *mqttObligationSource {
	if r == nil {
		return nil
	}
	if r.retainedSource() {
		return &mqttObligationSource{Kind: mqttSourceRetained, Payload: r.Payload, Digest: r.Digest}
	}
	return &mqttObligationSource{Kind: mqttSourceOrdinary, Stream: mqttInflightSourceStream(r.Dur),
		Dur: r.Dur, Seq: r.Seq}
}

// canonical is the register's canonical PUBREL reference, or nil while it names none.
func (r *mqttInflightRecord) canonical() *mqttCanonicalRef {
	switch {
	case r == nil:
		return nil
	case r.RelSubj != _EMPTY_ || r.RelID != _EMPTY_:
		return &mqttCanonicalRef{Kind: mqttCanonicalInflight, Subject: r.RelSubj, Identity: r.RelID}
	case r.PubRelDurable != _EMPTY_ || r.PubRelStreamSeq != 0:
		return &mqttCanonicalRef{Kind: mqttCanonicalConsumer, Consumer: r.PubRelDurable, Seq: r.PubRelStreamSeq}
	}
	return nil
}

// mqttRecoveredInflightReg is one readable register and the exact stream sequence
// at which recovery observed it. foreign marks a member whose claim arbitration was
// LOST: seq is then the rival's sequence, kept for the reservation it becomes but
// never a sequence this object may settle.
type mqttRecoveredInflightReg struct {
	rec     *mqttInflightRecord
	seq     uint64
	foreign bool
	// send is the entry's send state as recovery's marker phase derived it from the pair
	// (rows 4-9, 12, 14). Undecided for a record no marker phase reached — a past-PUBREC
	// or Retired slot needs none, and a live PUBLISH-phase one installs fail-closed.
	send mqttRecoveredSend
	// srcRouted is this register's source classification AS ROUTED for PUBLISH-phase
	// cleanup — a routing decision, NOT a restatement of what the source cache says.
	//
	// The distinction is the whole point and it is load-bearing. The classify loop assigns
	// this only at a site every exclusion has already had its chance to divert the record:
	// a pure cancellation, an affine register its own source does not prove, a
	// drain-AMBIGUOUS source, a drain-OWNED record and a retirement each continue BEFORE
	// the assignment. So the zero value means "not routed to source cleanup", and a
	// drain-owned register carries it even over a source the cache calls complete. Reading
	// the cache directly at the consume arm instead would delete every one of those
	// exclusions silently, which is exactly the defect this field was reshaped to prevent
	// (F-102 plan review).
	//
	// Which of the two routed values it holds still matters downstream. Source
	// disappearance is not receiver completion, so the marker phase decides the outcome:
	// proven-unsent (marker present) is DROPPED as the lost-delete register it is, and
	// possibly-attempted (marker absent) is RESERVED with its identifier held, because the
	// receiver may hold that identifier from an attempt whose PUBREC or PUBACK was lost.
	// Only mqttSourceRoutedComplete drops the source mapping; a Lost source keeps it,
	// because a mapping is what stops a dispatched callback minting a second identifier
	// for one source.
	srcRouted mqttRoutedSourceCleanup
	// traversed is every sequence this operation authoritatively held the record at,
	// in order: the walk sequence, each reread a continuing contest arm re-targeted
	// through, and the landed sequence. An in-memory regSeq is advanced to the landed
	// sequence only when it names a link in this chain — an operation may legitimately
	// pass through S2 on its way from S1 to S3 while memory sits at S2, and a check
	// admitting only the base would leave that entry aimed at dead S2. A sequence
	// outside the chain belongs to a writer this operation never saw, and its fact
	// stands (F-087 WP4).
	traversed []uint64
}

// traversedContains reports whether seq is a link in this operation's chain.
// mqttRecoveredSend is what the marker phase decided for one live PUBLISH-phase register.
//
// transmitted and markerSeq are INDEPENDENT facts (F-088). transmitted is the classification —
// POSSIBLY ATTEMPTED, so the identifier is held, the entry is never released as unsent and its
// next send carries DUP=1 — and markerSeq is a DELETION DEBT: a standing unsent marker the next
// sender must confirm deleted before it writes. Proven-unsent is markerSeq != 0 with transmitted
// false (mqttRecoveredProvenUnsent); a recovered output token makes a marker-standing register
// possibly attempted while its marker stays owed.
type mqttRecoveredSend struct {
	decided     bool
	transmitted bool   // possibly attempted: InstallTransmitted, or a recovered output token; DUP=1 on the next send
	markerSeq   uint64 // the standing marker the next transmission must confirm deleted first (0: none stands)
	opaque      bool   // Withhold: the marker-opaque reservation
}

// mqttRecoveredProvenUnsent reports whether recovery PROVED the register never transmitted: its
// decided send state carries a standing marker and no possible attempt. It is the one statement
// of that test for every routing decision that may drop a register as never sent, so a
// token-bearing register (possibly attempted with its marker still owed) stays in the restore
// population at each of them.
func mqttRecoveredProvenUnsent(rv mqttRecoveredInflightReg) bool {
	return rv.send.decided && !rv.send.opaque && rv.send.markerSeq != 0 && !rv.send.transmitted
}

// mqttRecoveredOutputAuthority reports whether a recovered ORDINARY PUBLISH-phase register carries
// an output token: an acquisition whose holder may still record intent and write. Scoped to the
// authority-backed acquisition, which commits the token atomically under the acquirer's accepted
// entitlement; the slot-only class (chapter P) takes its token by a standalone CAS and is not
// covered.
func mqttRecoveredOutputAuthority(rec *mqttInflightRecord) bool {
	return rec != nil && rec.State == mqttInflightPublish && !rec.Retired && rec.ActionToken != _EMPTY_ &&
		!mqttDurIsSlotOnlyClass(rec.Dur)
}

// errInflightReclassify is the resume pass asking to be RE-RUN: an authoritative reread
// contradicted the walk snapshot (rows 6-8, or a pair committed between the two walks),
// so the classification this pass built is stale and the next pass re-enters it from the
// stream. Bounded by the caller.
var errInflightReclassify = errors.New("mqtt: the in-flight walk snapshot is stale; reclassify")

func (rv mqttRecoveredInflightReg) traversedContains(seq uint64) bool {
	for _, s := range rv.traversed {
		if s == seq {
			return true
		}
	}
	return false
}

// mqttNewInflightNonce mints an exchange nonce.
func mqttNewInflightNonce() string { return nuid.Next() }

// mqttValidInflightNonce reports whether s is a well-formed nonce: exactly
// mqttInflightNonceLen characters of the NUID base-62 alphabet. Same rule, and same
// reasoning, as mqttValidSessGen — a token of the right length from a foreign
// alphabet is not one of ours, and an empty one must not collapse "no identity" into
// the identity space.
func mqttValidInflightNonce(s string) bool {
	if len(s) != mqttInflightNonceLen {
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

// mqttInflightRecordIdentityOK reports whether a decoded register carries a usable
// identity. Every field it names is load-bearing, so a record missing any of them is
// unreadable rather than partially usable: without Dur/Seq recovery cannot decide
// whether the exchange is live, without PI it names no identifier, and without the
// nonce nothing can prove ownership.
// It is the IDENTITY clause of the legal-shape matrix, not a rival to it:
// mqttValidateInflightShape calls this rather than restating PI, Dur/Seq, QoS and the
// nonce, so there is exactly one definition of a usable identity and a reader and a writer
// cannot drift apart on it. The clause below that the matrix would otherwise lose — an
// affine claim subject must name a valid incarnation — is the reason the delegation runs in
// this direction rather than the other.
//
// Two rounds shaped that. The first found the matrix governing only WRITES, so an
// unsupported shape read back was classified as readable; delegating the whole decision to
// the matrix was tried at that point and reverted, because production writers on the
// shared-claim, verdict and takeover paths still emitted shapes it refused and a resumed
// session would have lost live registers. Those writers were then fixed — every production
// store validates before writing, and the tombstone writers carry State and custody — and
// the second round found the READER half still missing. It is now closed the only way that
// converges: one decoder, mqttDecodeInflightRecord, which every persisted read goes through.
func mqttInflightRecordIdentityOK(r *mqttInflightRecord) bool {
	if r == nil {
		return false
	}
	if r.PI == 0 {
		return false
	}
	// EXACTLY ONE SOURCE ARM (F-105 7c). An ordinary register names its delivery (Dur/Seq); a
	// retained one names its materialization (Payload/Digest) and NO delivery coordinate. A record
	// carrying both describes a source no reader can act on; one carrying neither names nothing.
	if r.retainedSource() {
		if r.Dur != _EMPTY_ || r.Seq != 0 {
			return false
		}
		if mqttValidateObligationSource(r.source()) != nil {
			return false
		}
	} else if r.Dur == _EMPTY_ || r.Seq == 0 {
		return false
	}
	if r.QoS != 1 && r.QoS != 2 {
		return false
	}
	// A record naming an affine assignment must also name its INCARNATION, and this
	// is a refusal rather than a tolerated gap: _EMPTY_ is the deliberate OPT-OUT
	// value at the fence in advanceQ2AffClaim, so a restored register carrying a claim
	// subject with no valid incarnation would silently disable the fence and let a
	// stale exchange record AffineDone against a replacement claim — the exact write
	// F-065 exists to prevent. There are no releases, so a record of that shape does
	// not exist; meeting one means the state is not interpretable and it is refused.
	if r.Q2AffClaim != _EMPTY_ && mqttQ2AffCopyIncarnation(r.Q2AffCopy) == _EMPTY_ {
		return false
	}
	return mqttValidInflightNonce(r.Nonce)
}

// mqttInflightSourceStream returns the stream a register's source durable consumes.
//
// Derived from the durable's own prefix rather than stored in the record: the affine
// QoS 2 path delivers copies from $MQTT_out through a $MQTT_Q2AFF_* durable, while
// every plain and drained subscription delivers from $MQTT_msgs. Deriving it keeps
// the two sequence spaces apart without a field that could disagree with the name —
// the same reason resendPendingRedelivery carries jsDur alongside each sequence.
func mqttInflightSourceStream(dur string) string {
	if strings.HasPrefix(dur, mqttQ2AffineConsumerDurablePrefix) {
		return mqttOutStreamName
	}
	return mqttStreamName
}

// buildInflightRecord encodes a register with its expected-sequence CAS header. Sole
// writer of the register's wire form.
func buildInflightRecord(expectSeq uint64, r *mqttInflightRecord) (msg []byte, hdrLen int) {
	b, _ := json.Marshal(r)
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(expectSeq, 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdrLen = bb.Len()
	bb.Write(b)
	return bb.Bytes(), hdrLen
}

// storeInflightCAS writes a register to subject expecting expectSeq as the subject's
// current last sequence (0 = create-only) and returns the sequence it landed on.
func (jsa *mqttJSA) storeInflightCAS(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
	// THE WRITE BOUNDARY VALIDATES, and it is here rather than at each caller because a
	// per-caller rule is a promise and this is a property.
	//
	// A holistic review disproved the per-caller version: I had validated at the store path
	// and the mark helpers and then claimed "every production register write is validated",
	// which was false — claimRecoveredSharedInflight copies a loaded record, rewrites its
	// custody and writes it directly, so a resume actively re-wrote an unsupported shape at
	// a new sequence. Every writer funnels through here, so this is the only place the
	// claim can be true.
	//
	// Refusing is the right failure: an unsupported record would be refused by the phase
	// gate on the way out again, which can leave the identifier unrecoverable. Better never
	// to create it. The error is deliberately distinguishable so a caller can classify the
	// input as opaque rather than retrying a write that cannot succeed.
	if verr := mqttValidateInflightShape(r); verr != nil {
		return 0, fmt.Errorf("refusing to store an unsupported in-flight shape on %q: %w", subject, verr)
	}
	msg, hdrLen := buildInflightRecord(expectSeq, r)
	resp, err := jsa.storeMsg(subject, hdrLen, msg)
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// loadInflightRecord reads the current register on subject. Its four outcomes are
// distinct for the same reason loadQoS2RelClaim's are, and none may be collapsed:
//
//	(nil, nil, nil)    the subject is empty
//	(nil, smsg, nil)   a record is present but UNREADABLE (the caller needs its
//	                   sequence to reap it, and its metadata to log)
//	(rec, smsg, nil)   readable
//	(nil, nil, err)    INDETERMINATE, never to be guessed into an absence: absence
//	                   authorizes reusing an identifier, which is the one decision
//	                   that can put a duplicate identifier on the wire.
func (jsa *mqttJSA) loadInflightRecord(subject string) (*mqttInflightRecord, *StoredMsg, error) {
	smsg, err := jsa.loadLastMsgFor(mqttInflightStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	if smsg == nil {
		return nil, nil, nil
	}
	r, derr := mqttDecodeInflightRecord(smsg.Data)
	if derr != nil {
		// Unreadable is a DEFINITE fact about the record, not an indeterminate read, and
		// an ILLEGAL SHAPE is one of the ways a record is unreadable: a body the schema
		// declares uninterpretable may not authorize a mutation or a deletion any more
		// than malformed JSON may. See mqttDecodeInflightRecord.
		return nil, smsg, nil
	}
	return r, smsg, nil
}

// storeInflightCAS performs a register write, honoring an instance-scoped test seam.
// Only register writes are routed through here, so an injected fault never perturbs
// the source store, the PUBREL store or unrelated loads.
//
// IT NEVER WRITES A PENDING FENCE (F-088, consult C4). This is a GENERIC sink: any caller may
// hand it any body at any expectation, so it cannot know the write is a create-only fence on an
// empty subject, which is the only thing `pending-create` may assert. The one slot-only fence
// writer goes through storePendingFenceCAS, which checks exactly that.
func (as *mqttAccountSessionManager) storeInflightCAS(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
	if err := mqttRefuseGenericFenceWrite(subject, r); err != nil {
		return 0, err
	}
	as.mu.RLock()
	hook := as.testInflightStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, r)
	}
	return as.jsa.storeInflightCAS(subject, expectSeq, r)
}

// storePendingFenceCAS is the ONE slot-only (experimental affine) entry that may write a pending
// cancellation fence (F-088, consult C4, plan revision 12). The ordinary classes write theirs as a
// create-only member of a tombstone-register transition; the slot-only class has no authority
// transition, so its fence is checked here instead: the slot-only class, the sparse fence form
// naming this subject, and expectation ZERO — JetStream's create-only "expected last subject
// sequence 0" is what proves the subject held no occupant, which is the whole content of the
// provenance.
func (as *mqttAccountSessionManager) storePendingFenceCAS(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
	switch {
	case r == nil || !mqttDurIsSlotOnlyClass(r.Dur):
		return 0, fmt.Errorf("a pending cancellation fence on %q through the slot-only entry must be slot-only", subject)
	case !mqttValidPendingFence(subject, r):
		return 0, fmt.Errorf("a pending cancellation fence on %q is not the sparse fence form for that subject", subject)
	case expectSeq != 0:
		return 0, fmt.Errorf("a pending cancellation fence on %q is create-only, not written at sequence %d", subject, expectSeq)
	}
	// The same instance-scoped register-store seam the generic sink honours.
	as.mu.RLock()
	hook := as.testInflightStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, 0, r)
	}
	return as.jsa.storeInflightCAS(subject, 0, r)
}

// mqttRefuseGenericFenceWrite is the generic sinks' refusal of a pending-fence body: only the
// fence's own create-only writers may mint that provenance.
func mqttRefuseGenericFenceWrite(subject string, r *mqttInflightRecord) error {
	if r != nil && r.CancelOrigin == mqttCancelOriginPendingCreate {
		return fmt.Errorf("refusing to write a pending cancellation fence on %q through a generic register "+
			"sink: a fence is written only create-only by its own writer", subject)
	}
	return nil
}

// enumerateInflightSubjects lists one incarnation's register subjects, honoring an
// instance-scoped test seam.
//
// The stream's last sequence is discarded: this walk removes nothing by filter, so it
// has no purge to bound. Every register delete it leads to already targets an exact
// sequence (R4).
func (as *mqttAccountSessionManager) enumerateInflightSubjects(filter string) (map[string]uint64, error) {
	as.mu.RLock()
	hook := as.testInflightEnumerate
	as.mu.RUnlock()
	if hook != nil {
		return hook(filter)
	}
	subjects, _, err := as.jsa.streamSubjects(mqttInflightStreamName, filter)
	return subjects, err
}

// enumerateInflightSubjectsBounded lists one incarnation's register subjects TOGETHER
// with the stream watermark that bounds what a destructive pass is entitled to remove,
// honoring an instance-scoped test seam.
//
// The watermark is used in the one direction streamSubjects supports: as a bound on
// destruction. Its contract is explicit that State and State.Subjects are not one
// snapshot, so a returned subject may sit ABOVE the watermark and is then SPARED — and
// that reading everything returned as at-or-below would be false.
func (as *mqttAccountSessionManager) enumerateInflightSubjectsBounded(filter string) (map[string]uint64, uint64, error) {
	as.mu.RLock()
	hook := as.testInflightEnumerateBounded
	as.mu.RUnlock()
	if hook != nil {
		return hook(filter)
	}
	return as.jsa.streamSubjects(mqttInflightStreamName, filter)
}

// loadInflightRecordsMulti is the batched register read, honoring an instance-scoped
// test seam. Both teardown and resume batch through this boundary; sharing the transport
// does not share their disposition rules.
func (as *mqttAccountSessionManager) loadInflightRecordsMulti(subjects []string) ([]*JSApiMsgGetResponse, error) {
	as.mu.RLock()
	hook := as.testInflightLoadMulti
	as.mu.RUnlock()
	if hook != nil {
		return hook(subjects)
	}
	return as.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
}

// mqttSlotMutate is applied to a LOADED slot record to produce the record to CAS back. It
// must mutate only the fields its caller owns: everything else is carried forward, which is
// the whole reason this exists.
type mqttSlotMutate func(cur *mqttInflightRecord)

// casInflightSlot is the ONE way an existing slot is rewritten: load it, prove it is still
// the exchange the caller means and that the caller's disposition is permitted from the
// state it is actually in, apply the mutation, and CAS at the sequence the load returned.
//
// It replaces the construct-a-fresh-record pattern the mark helpers used, and that pattern
// was a latent defect the moment the slot grew a phase: rebuilding from a few remembered
// arguments silently dropped every field the rebuilder did not know about — the state
// itself, the canonical PUBREL identity, an action token. A record so rebuilt would be
// refused by the shape validator at best, and at worst would erase a live action's claim.
//
// Carrying the record forward also means a caller cannot accidentally widen its own
// authority: it mutates its fields and the rest of the exchange's identity is whatever the
// stream says it is, not whatever the caller remembered.
//
// Returns the landed sequence and true only on a CAS this call performed. A mismatched
// occupant, a state the disposition does not permit, an unreadable record or an
// indeterminate read all return false — fail closed, because every one of them means the
// caller's premise about this identifier is no longer true.
//
// Session lock NOT held on entry.
func (sess *mqttSession) casInflightSlot(c *client, ref mqttInflightSlotRef,
	from mqttInflightState, d mqttInflightDisposition, mut mqttSlotMutate) (uint64, bool) {

	as := sess.asm
	if as == nil || !ref.slotOK() || ref.name.subject == _EMPTY_ || mut == nil {
		return 0, false
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, lerr := as.loadInflightRecord(ref.name.subject)
		switch {
		case lerr != nil:
			// Indeterminate, which is not an absence and not a licence to write.
			return 0, false
		case smsg == nil, cur == nil:
			// Gone, or present but unattributable. Either way this exchange's premise is
			// false and nothing may be written over it.
			return 0, false
		case !mqttInflightRecordMatchesSubject(cur, ref.name.subject):
			return 0, false
		case !ref.matchesRecord(cur):
			// Another exchange holds this identifier now. PRESERVE it.
			return 0, false
		}
		if verr := mqttValidateInflightShape(cur); verr != nil {
			// An unsupported persisted shape is refused rather than rewritten into a
			// supported one — that would be migration, which the compatibility boundary
			// forbids outright.
			mqttWarnf(c, "MQTT: refusing to rewrite the in-flight slot on %q: %v",
				ref.name.subject, verr)
			return 0, false
		}
		if !mqttInflightTransitionAllowed(d, cur.State) {
			// The phase gate. This is what stops every path written when a register's life
			// ended at PUBREC from vacating a slot that is now mid-exchange.
			return 0, false
		}
		if mqttSlotDestructiveDisposition(d) && cur.ActionToken != _EMPTY_ {
			// C-IV ARM 2 for the destructive dispositions that go through a CAS rather than
			// a delete — a same-source REPLACEMENT destroys the previous occupant as surely
			// as a delete does. Any live action withholds, ours included; see
			// reapRegisterGatedOutcome for the regression this guards.
			return 0, false
		}
		if d == mqttSlotAdvanceAcknowledgedTerminal && (ref.acq != cur.Acq || cur.ActionToken != _EMPTY_) {
			// C-I AND C-IV ARM 2 FOR THE ACKNOWLEDGED TERMINAL ARROW, checked HERE rather
			// than only at the caller's outer read, because this helper refreshes its CAS
			// base on every iteration: the caller reads transmission k, a peer acquires
			// k+1, and the internal load then returns that successor — whose nonce, source
			// and identifier all match, since matchesRecord deliberately omits the counter.
			// Writing terminal authority there records that the client answered a
			// transmission it never saw (holistic review round 3, finding 1).
			return 0, false
		}
		if mqttSlotDestructiveDisposition(d) && ref.acq != cur.Acq {
			// C-I AT THE DESTRUCTIVE EDGE. The caller is acting for a transmission whose
			// identity the slot no longer carries: B holds evidence naming Acq=k while
			// the occupant is at k+1, because A installed, transmitted and drained in
			// between. Nothing moved between B's read and its write, so a fresher CAS
			// base would not catch this — a sequence is not an identity.
			//
			// A retry may refresh the sequence; it may never re-aim at the newer counter.
			return 0, false
		}
		switch d {
		case mqttSlotAcquireAction:
			if !mqttSlotAcquisitionBaseHolds(ref, cur) {
				return 0, false
			}
		case mqttSlotTransferCustody:
			// The transfer must observe the EXACT custody it means to replace, so a
			// mid-session successor reconcile (Decision 7) and a concurrent rival cannot
			// both win: whichever CAS lands first moves ClaimSeq, and the other sees a
			// mismatch and loses. Recovery's own transfer passes a reference built from the
			// slot it just read, so its predicate is trivially satisfied unless the slot
			// moved under it — exactly when it SHOULD lose.
			if ref.claim == _EMPTY_ || cur.Claim != ref.claim || cur.ClaimSeq != ref.claimSeq {
				return 0, false
			}
		case mqttSlotDrainAction:
			// Only the holder of this exact non-revocable token may mutate an active slot.
			// A check inside the mutator is too late: a no-op still rewrites the record and
			// moves its freshness sequence under the real holder.
			if ref.actionToken == _EMPTY_ || cur.ActionToken != ref.actionToken {
				return 0, false
			}
		}
		if from != _EMPTY_ && cur.State != mqttInflightState(from) {
			// AN EXACT PREDECESSOR, which every named phase arrow needs and which
			// ref.matchesRecord deliberately does not supply: a reference is often taken
			// in one state and acted on in another, so identity is compared there and the
			// permitted transition is asked of the gate. But `ready -> sending` must
			// observe `ready` — not merely "a state from which advancing is allowed" — or
			// a slot another broker already moved would be advanced a second time.
			//
			// Checked against the record THIS load returned, never the reference's
			// remembered state, so a slot that moved between observation and write loses
			// the CAS rather than being overwritten. An empty `from` opts out, for the
			// state-agnostic marks whose legality is entirely the gate's business.
			return 0, false
		}
		next := *cur
		mut(&next)
		if next.SourceAckReply != cur.SourceAckReply {
			return 0, false
		}
		// A SLOT REWRITE NEVER CARRIES OR MINTS A PENDING FENCE (F-088, consult C4): a fence is
		// not an exchange, so no mark, custody write or phase arrow acts on one, and a CAS cannot
		// assert the create-only provenance.
		if cur.CancelOrigin == mqttCancelOriginPendingCreate || mqttRefuseGenericFenceWrite(ref.name.subject, &next) != nil {
			return 0, false
		}
		// C-I's invariant on the transmission counter, enforced HERE because this is the
		// one door every slot rewrite passes through: a rewrite either leaves the counter
		// alone or advances it by EXACTLY ONE, and at the ceiling it may not move at all.
		//
		// A DECREASE — a drain resetting it, say — would hand a later transmission an
		// identity an earlier one's evidence already names, which is the discharge C-I
		// exists to refuse. A JUMP means a mutator counted an acquisition nobody made.
		//
		// THE CEILING NEEDS ITS OWN TERM, and a test is what proved it: `cur.Acq+1` WRAPS
		// too, so at MaxUint64 the "exactly one" comparison compares zero against zero
		// and a wrapped counter sails through the very guard meant to stop it. Overflow
		// is REFUSED rather than wrapped because wrapping produces the identity collision
		// by another route and there is no correct value to write; the rewrite fails and
		// the identifier stays reserved.
		if next.Acq != cur.Acq && (cur.Acq == math.MaxUint64 || next.Acq != cur.Acq+1) {
			mqttWarnf(c, "MQTT: refusing an in-flight slot rewrite on %q: the transmission "+
				"counter would move from %d to %d, and only an advance of exactly one is "+
				"a rewrite this broker writes", ref.name.subject, cur.Acq, next.Acq)
			return 0, false
		}
		if verr := mqttValidateInflightShape(&next); verr != nil {
			// Refuse to WRITE an illegal shape, for the same reason the store path does:
			// every reader validates, so it would only become residue an operator removes.
			mqttWarnf(c, "MQTT: refusing an in-flight slot rewrite on %q that would be "+
				"unsupported: %v", ref.name.subject, verr)
			return 0, false
		}
		// The CAS base is the sequence THIS load returned, never the reference's
		// remembered one: a rewrite between the two must lose the CAS rather than be
		// overwritten.
		seq, err := as.storeInflightCAS(ref.name.subject, smsg.Sequence, &next)
		if err == nil {
			return seq, true
		}
		// A rejected CAS means the record moved under us. Loop: the reread re-proves
		// identity and the gate before trying again, so a rewrite by another writer is
		// re-classified rather than overwritten.
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// C-IV: the shared evidence loader
// ---------------------------------------------------------------------------

// mqttSlotEvidenceFor assembles, from ONE observation, everything
// mqttSlotReleaseAuthority needs about a slot, and it is the only way a destructive
// caller obtains release authority.
//
// It exists because C-IV is about ADMISSIBLE PROOFS, and a proof assembled piecemeal at
// each site is how this subsystem came to have several authoritative opinions of what a
// slot means. Every field is resolved here, from this observation, or it is
// indeterminate — there is deliberately no arm that carries a cached answer forward.
//
// `bound` is the transmission identity the CALLER's own exchange is bound to, captured at
// that exchange and never refreshed from the slot afterwards (C-I's binding rule). A
// caller that cannot supply one has not proven which transmission it is acting for, and
// the authority function refuses it at arm 1.
//
// THE MARKER IS PROBED, not assumed. An authoritative absence is a determinate answer,
// and it resolves arm 1 — but it proves only that the exchange is POSSIBLY-ATTEMPTED, so
// it never by itself reaches arm 4. An unreadable, mismatching or failed observation is
// indeterminate and withholds.
//
// NO CANCELLATION VERDICT IS EVIDENCE, staged or persisted (F-088): a cancellation is a
// decision to stop delivering, not a proof the PUBLISH never went out.
//
// Returns the evidence, the loaded record and its observed sequence. A caller CASes at
// that exact sequence and nowhere else.
//
// Session lock NOT held on entry.
func (sess *mqttSession) mqttSlotEvidenceFor(subject string, bound mqttSlotIdentity) (
	mqttSlotEvidence, *mqttInflightRecord, uint64) {

	var ev mqttSlotEvidence
	ev.bound = bound
	as := sess.asm
	if as == nil || subject == _EMPTY_ {
		return ev, nil, 0
	}
	cur, smsg, lerr := as.loadInflightRecord(subject)
	if lerr != nil || smsg == nil || cur == nil {
		// Indeterminate, gone, or present but unattributable. `readable` stays false and
		// arm 1 withholds; a GONE record is the caller's to interpret, because "there is
		// nothing to delete" is not the same answer as "you may delete".
		return ev, nil, 0
	}
	if !mqttInflightRecordMatchesSubject(cur, subject) {
		return ev, nil, smsg.Sequence
	}
	// THROUGH THE ONE EVIDENCE CONSTRUCTOR, so the loading path and the record-in-hand path
	// cannot drift about what evidence means.
	ev = sess.mqttSlotEvidenceFromRecord(subject, cur, bound)
	return ev, cur, smsg.Sequence
}

// mintSlotRelease computes the release authority for a record the caller has ALREADY
// loaded, and binds it to that exact observation.
//
// The evidence comes from ONE constructor — mqttSlotEvidenceFromRecord, shared with the
// reference-holding path — so there is a single definition of what counts as evidence. The
// authority carries the subject, the exchange identity and the sequence it was computed
// against, so it cannot be spent against a different observation.
//
// A MARKER PROBE IS A READ, and this does not pretend otherwise: "no extra RECORD read" is
// the accurate claim, because re-loading the record here would replace one TOCTOU with two.
// The marker is a separate observation from the record, which is precisely why the
// exact-sequence delete and the post-delete confirmation are both still required — neither
// this token nor the evidence behind it closes the window between deciding and acting.
//
// Session lock NOT held on entry.
func (sess *mqttSession) mintSlotRelease(subject string, cur *mqttInflightRecord, seq uint64) mqttSlotRelease {
	if cur == nil || subject == _EMPTY_ || sess.asm == nil {
		return mqttSlotRelease{} // the zero value withholds
	}
	bound := mqttSlotIdentityOf(subject, cur)
	return mqttSlotRelease{
		auth:     mqttSlotReleaseAuthority(sess.mqttSlotEvidenceFromRecord(subject, cur, bound)),
		bound:    bound,
		seq:      seq,
		source:   sess.terminalSourceProof(subject, seq, cur),
		handBack: sess.handBackTerminalProof(subject, seq, cur),
	}
}

// mqttSlotEvidenceFromRecord assembles the evidence for a record the caller already holds:
// the record itself and the marker probed for THIS allocation. It performs no RECORD read.
//
// It is the shared half of mqttSlotEvidenceFor, which differs only in loading the record
// first — so the two cannot drift about what evidence means.
//
// Session lock NOT held on entry.
func (sess *mqttSession) mqttSlotEvidenceFromRecord(subject string, cur *mqttInflightRecord,
	bound mqttSlotIdentity) mqttSlotEvidence {

	ev := mqttSlotEvidence{readable: true, rec: cur, bound: bound}
	if sess.asm == nil || cur == nil {
		return mqttSlotEvidence{bound: bound}
	}
	// THE EVIDENCE BACKEND, WHICHEVER IT IS. An injected marker loader is a backend in its
	// own right, so the question is whether one EXISTS, not whether it is the real store —
	// guarding on the internal client alone would stop a seam-based fixture supplying
	// evidence it has correctly configured.
	sess.asm.mu.RLock()
	seamed := sess.asm.testInflightMarkerLoad != nil
	sess.asm.mu.RUnlock()
	if !seamed && sess.asm.jsa.c == nil {
		// No backend at all: the marker cannot be probed, so the evidence is INDETERMINATE
		// and the authority withholds. Fail-closed, never an assumed absence.
		ev.marker = mqttSlotMarkerIndeterminate
		return ev
	}
	// THE MARKER, for THIS allocation. The nonce comparison is what makes it this
	// allocation's: a predecessor's marker on the same subject names a different exchange,
	// and treating it as ours would be a transferable capability arm 4 forbids.
	//
	// Tagged as EVIDENCE, not as the caller's own disposition. A probe that borrowed the
	// disposing caller's tag would be counted as one of ITS marker reloads, which conflates
	// two different operations: this read decides whether a release is licensed, while the
	// caller's later reap decides what happens to the marker itself.
	load := sess.asm.loadInflightMarker(mqttMarkerCallerEvidence, subject)
	switch {
	case load.kind == mqttInflightResponseAbsent:
		ev.marker = mqttSlotMarkerAbsent
	case load.kind == mqttInflightResponsePresent && load.decodeErr == nil &&
		load.marker != nil && load.marker.Nonce == cur.Nonce:
		ev.marker = mqttSlotMarkerPresent
	default:
		// Present but unreadable, naming another exchange, or an indeterminate read. An
		// unavailable evidence backend lands here too, which is the fail-closed direction.
		ev.marker = mqttSlotMarkerIndeterminate
	}
	// NO CANCELLATION VERDICT IS READ (F-088). A verdict, staged or durably covered, records a
	// decision to stop delivering; it proves nothing about whether the PUBLISH already went out,
	// so it is not release evidence. The marker above is the only proof of non-transmission.
	return ev
}

// confirmSlotReleased is the POST-DELETE subject read that says what a delete actually
// established, and it is the one definition of that question.
//
// It exists because deleteInflightRegisterConfirmed reads an already-absent sequence as
// success: a same-identity rewrite between the caller's load and its delete is
// indistinguishable from the delete having removed the record. recoverPublishTerminalSlot
// has always done this confirmation inline; this is that code, shared.
//
// `rec` is the record the caller observed — its Nonce, Dur and Seq are the comparison,
// which is deliberately NOT the release token's (subject, Nonce, Acq): a same-allocation
// Acq advance still refutes this confirmation, and calling both "identity" hides that.
//
// It OBSERVES; it does not lock the subject against a later creation. A caller lifting a
// local reservation on this answer must still re-check identity under sess.mu.
//
// Session lock NOT held on entry.
func (sess *mqttSession) confirmSlotReleased(subject string, rec *mqttInflightRecord) mqttReleaseConfirmed {
	if sess.asm == nil || rec == nil || subject == _EMPTY_ {
		return mqttReleaseUnresolved
	}
	cur, smsg, lerr := sess.asm.loadInflightRecord(subject)
	switch {
	case lerr != nil:
		return mqttReleaseUnresolved // indeterminate: not a confirmed release
	case smsg == nil:
		return mqttReleaseEstablishedAbsent // the subject holds nothing
	case cur == nil:
		// PRESENT BUT UNREADABLE, which the loader reports as a record it cannot interpret.
		// It is not an absence, and reading it as one released an identifier hold over a
		// record a rewrite may have left racing this delete.
		return mqttReleaseUnresolved
	case cur.Nonce == rec.Nonce && cur.Dur == rec.Dur && cur.Seq == rec.Seq:
		return mqttReleaseRefuted // our own record, rewritten: the release is unconfirmed
	}
	return mqttReleaseEstablishedOther // a successor holds the subject; ours is gone
}

// mqttRecoveredPending builds the in-memory entry for a recovered register, and it is the
// ONE constructor for that shape.
//
// It exists because a holistic review found two recovery branches installing an actionable
// entry with regSeq and regNonce but NO captured slot reference. deleteInflightRegisterFor
// refuses an entry with no reference — correctly, since without one it cannot address the
// subject — so those entries' later terminal cleanup became a silent no-op that left durable
// registers behind, producing repeated store conflicts and stranded identifiers. Every
// recovered entry now gets the complete reference from the same place, so a new branch
// cannot forget it.
func mqttRecoveredPending(idHash, gen string, pi uint16, rv mqttRecoveredInflightReg) *mqttPending {
	if rv.rec == nil {
		return nil
	}
	if mqttInflightTerminalPhase(rv.rec.State) {
		// A TERMINAL SLOT IS NEVER AN ACTIONABLE ENTRY, and this is a BOUNDARY rather
		// than a classification: `beginSendLocked` does not inspect slot state, so an
		// entry built here could put an already-acknowledged payload back on the wire.
		// Marking it transmitted would not help — a transmitted entry is redelivered with
		// DUP, which is the same duplicate one flag later.
		//
		// The routing above sends terminal records to their own recovery path, so
		// reaching here means a new branch forgot. Refusing costs the identifier for this
		// incarnation; admitting costs a duplicate delivery of a completed exchange.
		return nil
	}
	e := &mqttPending{
		sourceAckReply: rv.rec.SourceAckReply,
		jsDur:          rv.rec.Dur,
		sseq:           rv.rec.Seq,
		q2AffClaim:     rv.rec.Q2AffClaim,
		q2AffCopy:      rv.rec.Q2AffCopy,
		regSeq:         rv.seq,
		regNonce:       rv.rec.Nonce,
		ev:             mqttEvidenceFromRecord(gen, rv.rec),
		// The record's QoS is the ONLY per-exchange evidence a recovery has, and
		// dropping it here was the defect: the delivery callback then re-derived the
		// wire QoS from whatever the subscription said at re-offer time, so a
		// re-SUBSCRIBE at a lower maximum retransmitted a started QoS 2 exchange at
		// QoS 1 and its handshake was abandoned. Mandatory and validated to 1 or 2 on
		// every stored record, so this never seeds a zero.
		qos: rv.rec.QoS,
		// The send state the marker phase derived (rows 4-9, 12, 14). CLASS DISPATCH
		// comes first: a slot past PUBREC, or a Retired one, was answered by the client and
		// is transmitted whatever a marker says; a live PUBLISH-phase register takes what
		// the marker phase decided, and one it never reached is installed marker-opaque —
		// fail closed, never "registered therefore transmitted". The two fields are copied
		// INDEPENDENTLY: a possibly-attempted register recovered over an outstanding output
		// token keeps its standing marker as the deletion debt its next send confirms first
		// (F-088), and only the placeholder arms below — which never send — clear it.
		transmitted:  rv.send.transmitted,
		markerSeq:    rv.send.markerSeq,
		markerOpaque: rv.send.opaque,
	}
	if rv.rec.Retired || mqttInflightPastPubRec(rv.rec.State) {
		e.regRecoveryPlaceholder = true
		e.transmitted, e.markerSeq, e.markerOpaque = true, 0, false
	} else if !rv.send.decided {
		e.transmitted, e.markerSeq, e.markerOpaque = false, 0, true
	}
	// THE KIND FOLLOWS THE RECORD'S CLASS. A PUBLISH-phase register whose send state was
	// decided restores an acting entry — Affine for the slot-only class, Ordinary otherwise,
	// which setPendingLocked installs only while custody holds the lineage. A past-PUBREC or
	// Retired one is the PUBREL-side reservation an acquired action later promotes, and a
	// marker-opaque one reserves the identifier: both act for nothing, and both keep the
	// record's evidence so the lineage's release takes them.
	if !e.regRecoveryPlaceholder && !e.markerOpaque {
		e.kind = mqttPendingOrdinary
		if mqttDurIsSlotOnlyClass(rv.rec.Dur) {
			e.kind = mqttPendingAffine
		}
	}
	if name, err := mqttParseInflightSubject(mqttInflightSubject(idHash, gen, pi)); err == nil {
		e.slot = mqttInflightSlotRef{
			name: *name, seq: rv.seq, nonce: rv.rec.Nonce, acq: rv.rec.Acq,
			state:      rv.rec.State,
			actionLive: rv.rec.ActionToken != _EMPTY_,
			dur:        rv.rec.Dur, sseq: rv.rec.Seq,
			claim: rv.rec.Claim, claimSeq: rv.rec.ClaimSeq,
		}
	}
	return e
}

// loadInflightRecord reads a register, honoring an instance-scoped test seam.
func (as *mqttAccountSessionManager) loadInflightRecord(subject string) (*mqttInflightRecord, *StoredMsg, error) {
	as.mu.RLock()
	hook := as.testInflightLoad
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject)
	}
	return as.jsa.loadInflightRecord(subject)
}

// deleteInflightRegister removes one register at its EXACT sequence (R4), honoring
// an instance-scoped test seam. Asynchronous: a failed delete leaves a stale record
// that the store path's reconcile, the next resume scan or the orphan sweep removes,
// and never wedges an identifier.
//
// Zero is a no-op, which lets pre-store cancellation and retained sends call this
// unconditionally. Slot-backed exchanges recovered from PUBREL records are different:
// their whole-life owner slot is finalized through the gated terminal path.
func (as *mqttAccountSessionManager) deleteInflightRegister(seq uint64) {
	if seq == 0 {
		return
	}
	as.mu.RLock()
	hook := as.testInflightDelete
	as.mu.RUnlock()
	if hook != nil {
		_ = hook(seq)
		return
	}
	_ = as.jsa.deleteMsg(mqttInflightStreamName, seq, false)
}

// recomputeDrainRegisterStateLocked re-derives every claimed drain entry's register hint
// and its AMBIGUITY from one complete recovery scan, and returns which keys came out owned
// and which ambiguous.
//
// It resets first, over every claimed entry, and that ordering is the whole point. An
// earlier revision only cleared the flag for groups the scan still found, so when BOTH
// duplicates of a pair disappeared the loop visited nothing for that key and the old flag
// stood — leaving the drain unfinalizable, and outbound allocation withheld, while no
// ambiguous register remained. A flag that only ever sets is a latch, not an observation.
// Review of F-034 caught it.
//
// sess.mu must be held.
func (sess *mqttSession) recomputeDrainRegisterStateLocked(
	drainRegs map[string][]mqttRecoveredInflightReg) (owned, ambiguous map[string]bool) {

	owned, ambiguous, obs := sess.computeDrainRegisterStateLocked(drainRegs)
	sess.applyDrainRegisterStateLocked(obs)
	return owned, ambiguous
}

// mqttDrainRegObservation is one claimed drain entry's register hint as a scan found it: the
// entry it is about (by identity), and the register sequence or the ambiguity.
type mqttDrainRegObservation struct {
	dur       string
	seq       uint64
	st        *drainSeq
	regSeq    uint64
	ambiguous bool
}

// computeDrainRegisterStateLocked is the recomputation's READ half: which keys come out owned and
// which ambiguous, and the observations applyDrainRegisterStateLocked would install. It writes
// NOTHING (F-105 7b): the resume walk classifies with it before apply and installs only once
// apply has proven the walk still serves, because regSeq and regAmbiguous gate drain finalization
// and are serialized in the session record.
//
// sess.mu must be held.
func (sess *mqttSession) computeDrainRegisterStateLocked(
	drainRegs map[string][]mqttRecoveredInflightReg) (owned, ambiguous map[string]bool, obs []mqttDrainRegObservation) {

	owned, ambiguous = make(map[string]bool), make(map[string]bool)
	for key, group := range drainRegs {
		fr := group[0]
		if mqttRefuseSourcelessRecordAtOrdinarySink(fr.rec) {
			continue
		}
		d := sess.drainByDur[fr.rec.Dur]
		if d == nil {
			continue
		}
		st := d.seqs[fr.rec.Seq]
		if st == nil || !st.claimed {
			continue
		}
		o := mqttDrainRegObservation{dur: fr.rec.Dur, seq: fr.rec.Seq, st: st}
		if len(group) > 1 {
			// More than one register names this delivery, on different identifiers. Record
			// the AMBIGUITY explicitly: zeroing the hint alone used to be the whole signal,
			// and the drain's release then read a zero hint as "nothing to do" and
			// finalized anyway.
			o.ambiguous = true
			ambiguous[key] = true
			obs = append(obs, o)
			continue
		}
		// A SINGLETON is only unambiguous if it is the identifier this drain entry
		// addresses. Review of F-034 found the gap: with duplicates on PI A and PI B, if A
		// vanishes the next scan sees a singleton B and would clear ambiguity — while the
		// release still addresses A, finds it absent, reads AlreadyGone, and lets the claim
		// and the consumer go while B survives as a phantom holding a Receive Maximum slot.
		//
		// So a singleton on a DIFFERENT identifier stays ambiguous. It is not that the
		// register is unknown; it is that this entry cannot say which identifier the client
		// holds, which is the same fact the duplicate case records.
		if fr.rec.PI != st.pi {
			o.ambiguous = true
			ambiguous[key] = true
			obs = append(obs, o)
			continue
		}
		o.regSeq = fr.seq
		owned[key] = true
		obs = append(obs, o)
	}
	return owned, ambiguous, obs
}

// applyDrainRegisterStateLocked is the recomputation's WRITE half. It resets first, over every
// claimed entry, and that ordering is the whole point: an earlier revision only cleared the flag
// for groups the scan still found, so when BOTH duplicates of a pair disappeared the loop visited
// nothing for that key and the old flag stood — leaving the drain unfinalizable, and outbound
// allocation withheld, while no ambiguous register remained. A flag that only ever sets is a
// latch, not an observation. Review of F-034 caught it. An observation whose entry was replaced
// since it was computed is not installed.
//
// sess.mu must be held.
func (sess *mqttSession) applyDrainRegisterStateLocked(obs []mqttDrainRegObservation) {
	for _, d := range sess.drains {
		for _, st := range d.seqs {
			st.regAmbiguous = false
		}
	}
	for _, o := range obs {
		d := sess.drainByDur[o.dur]
		if d == nil || d.seqs[o.seq] != o.st {
			continue
		}
		if o.ambiguous {
			o.st.regSeq, o.st.regAmbiguous = 0, true
			continue
		}
		o.st.regSeq = o.regSeq
	}
}

// mqttInflightRecordMatchesSubject reports whether a decoded register's body agrees with
// the subject it was addressed by — specifically that its PI is the identifier that
// subject names.
//
// The reader's own rule is in recoverInflightRegisters, in the scan's `rec.PI != name.pi`
// term: a record whose body names another identifier is UNREADABLE, its identifier is
// RESERVED and the record is KEPT. Cited rather than paraphrased so a future reader can
// check the claim, and so that if the reader's rule ever loosens this mirror is findable.
//
// A DESTRUCTIVE path must be at least as strict. Review of F-034's first implementation
// found the release and the reap both matching only on (Dur, Seq), so an inconsistent
// record the reader would have refused could be deleted and the caller allowed to progress.
// Being more permissive than the reader is how a destructive path removes evidence for an
// identifier the client still holds.
//
// Destructive covers OVERWRITES, not just deletes — a later review round found the audit
// scoped to deletes and the read-after-CAS reconciliations replacing a record they had
// classified, which turns "reserved, unreadable" into "cancelled, freeable" by exactly the
// route a delete would.
//
// The two dispositions deliberately DIFFER in one respect. The reader reserves AND keeps;
// the destructive paths refuse AND keep. Both preserve the record; only the reader also
// claims the identifier, because only it is in a position to.
func mqttInflightRecordMatchesSubject(rec *mqttInflightRecord, subject string) bool {
	if rec == nil {
		return false
	}
	name, err := mqttParseInflightSubject(subject)
	if err != nil {
		return false
	}
	return rec.PI == name.pi
}

// mqttInflightIdentityOutcome is how an identity-conditioned release ended. The three
// values are distinct because their CALLERS differ: two authorize the caller to record
// its own progress, and the third must withhold it.
type mqttInflightIdentityOutcome int

const (
	// mqttInflightReleased: our register was found at this subject and deleted at the
	// exact sequence it was found at.
	mqttInflightReleased mqttInflightIdentityOutcome = iota
	// mqttInflightAlreadyGone: the subject is empty, or it holds a register that is
	// NOT ours. Either way this caller has nothing to release, so it may record its
	// progress — but nothing was destroyed.
	mqttInflightAlreadyGone
	// mqttInflightUndecided: the occupant could not be read, or the read itself was
	// indeterminate. Nothing was deleted and the caller must WITHHOLD its progress.
	mqttInflightUndecided
)

// releaseInflightRegisterByIdentity releases a register the caller can NAME, addressing
// it by SUBJECT and deleting only the exact sequence the subject currently holds — and
// only when the occupant's identity matches.
//
// # Why a subject and not a sequence
//
// R4 says sequences are not identities, and every delete in this file already targets an
// exact sequence. What R4 does not say, and what F-034 found, is that WHICH sequence to
// read is itself a question with a right answer. A caller holding a sequence it recorded
// EARLIER is holding a guess: the store path's replacement branch rewrites a register for
// the same (Dur, Seq) at a new sequence, and after $MQTT_inflight is recreated the
// recorded sequence names an unrelated register entirely. Deleting there destroys another
// session's register — the defect F-034 owns, which F-066's table had already assigned to
// "that mechanism's owner".
//
// A subject is an identity. Addressing by subject and matching the occupant makes the two
// failure modes asymmetric in the safe direction: a stale subject finds nothing or finds
// someone else and skips, where a stale sequence finds a victim and deletes it.
//
// # The contract
//
// Six outcomes, and each one is a caller obligation rather than an implementation detail:
//
//	absent                  -> AlreadyGone. Our record is not here; nothing to do.
//	readable, identity match -> Released, at the sequence the load returned.
//	readable, mismatch       -> AlreadyGone. The occupant belongs to another exchange and
//	                            is PRESERVED. This is the common legitimate case: a later
//	                            exchange on the same identifier.
//	unreadable               -> Undecided. Fail closed: an unattributable record may not be
//	                            deleted, and may not be assumed absent either.
//	indeterminate read       -> Undecided. Never guessed into an absence.
//	ambiguous delete         -> resolved inside the confirmed delete, which retries and
//	                            reads an already-absent sequence as success.
//
// The exact-sequence delete is the linearization point. A rewrite between the load and
// the delete moves the record to a new sequence, so the delete removes nothing and the
// next pass sees the replacement — which is why no fence is needed here beyond the
// identity match. A concurrent LIVE stream recreation between the two is outside the
// supported all-brokers-stopped recovery procedure.
//
// wantDur and wantSeq are the caller's authority. For a drain that is
// (Cons.Durable, Started.Seq), the register's own source-delivery key, which the record
// already persists — so this needs no new persisted field. It is deliberately not
// interchangeable with a nonce: recreating the SOURCE stream that issues wantSeq is
// F-066's coupling problem, not this contract's.
// sess is threaded so both deletes below can route through the verdict property: this
// path reads a full record, so it HOLDS the exchange nonce, and a fire-and-forget or
// verdict-blind delete here would strand a cancellation verdict naming the register it
// removes. Its own unconditional-confirmation semantics are preserved (F-087 WP3).
func (as *mqttAccountSessionManager) releaseInflightRegisterByIdentity(c *client, sess *mqttSession,
	subject, wantDur string, wantSeq uint64) mqttInflightIdentityOutcome {

	// THE RELEASE IS BOUND TO ONE IDENTITY, ONCE, AND EVERY RETRY IS COMPARED TO IT (F-098 L4,
	// holistic review round 1, finding 1).
	//
	// The authority is the persisted Rel when one exists — the request an earlier attempt of
	// THIS finalize confirmed durably, possibly before a restart — and otherwise the triple
	// this first load captured. It is NEVER rebuilt from whatever occupies the subject later:
	// a retry that loaded the current record and deleted it would discharge a successor
	// transmission on evidence about ours, which is exactly the retargeting C-I refuses. The
	// client's acknowledgement is evidence about OUR transmission and nothing else.
	rel := sess.drainReleaseRequestFor(wantDur, wantSeq)
	if rel != nil && rel.valid() && rel.Subject != subject {
		// A VALID REQUEST FOR A DIFFERENT TARGET — the drained delivery was re-offered under
		// a new identifier after an earlier attempt recorded it. It is an obligation to that
		// OTHER register: never ignored (its target could survive un-released) and never
		// re-aimed at the current subject (that is the retargeting the request exists to
		// forbid).
		//
		// SETTLED BEFORE ANYTHING AT THE CURRENT SUBJECT IS READ (holistic review round 2,
		// finding 1). Every answer the current subject gives — empty, another exchange, a
		// successor — is evidence about THAT subject only, and returning on it let the
		// caller consume this request while its own target survived. And settled through
		// the SAME bound release, rewrite loop included (finding 2): an exact-sequence
		// delete that finds its sequence already gone is exactly what a same-identity
		// rewrite looks like from here.
		if as.releaseBoundInflightRegister(c, sess, rel.Subject, wantDur, wantSeq, rel) == mqttInflightUndecided {
			mqttWarnf(c, "MQTT: withholding the release of the in-flight register on %q: an "+
				"earlier release request for %q is not yet settled", subject, rel.Subject)
			return mqttInflightUndecided
		}
		sess.retireDrainReleaseRequest(wantDur, wantSeq, rel)
		rel = nil
	}
	return as.releaseBoundInflightRegister(c, sess, subject, wantDur, wantSeq, rel)
}

// releaseBoundInflightRegister is the release itself, bound to rel when one is given — rel
// then names subject — and otherwise to the triple its first load captures. It serves both
// the current target and the settlement of a request for another one, so the two cannot
// disagree about what releases a register.
//
// Session lock NOT held on entry.
func (as *mqttAccountSessionManager) releaseBoundInflightRegister(c *client, sess *mqttSession,
	subject, wantDur string, wantSeq uint64, rel *mqttPersistedDrainRelease) mqttInflightIdentityOutcome {

	cur, smsg, err := as.loadInflightRecord(subject)
	switch {
	case err != nil:
		// Indeterminate. Not an absence, and not a licence to delete.
		mqttWarnf(c, "MQTT: cannot release the in-flight register on %q: the read was indeterminate: %v",
			subject, err)
		return mqttInflightUndecided
	case smsg == nil:
		// Empty subject: our register is already gone.
		return mqttInflightAlreadyGone
	case cur == nil:
		// Present but unattributable. Fail closed: it cannot be proven ours, so it
		// cannot be deleted, and it cannot be proven foreign either.
		mqttWarnf(c, "MQTT: cannot release the in-flight register on %q at sequence %d: "+
			"the record is unreadable", subject, smsg.Sequence)
		return mqttInflightUndecided
	case !mqttInflightRecordMatchesSubject(cur, subject):
		// The body disagrees with the subject that addressed it. Recovery already treats
		// rec.PI != subject.PI as UNREADABLE, and a destructive path must not be more
		// permissive than the reader: deleting here would remove evidence for an
		// identifier the client may still hold, on a record the rest of the tree refuses
		// to interpret.
		mqttWarnf(c, "MQTT: cannot release the in-flight register on %q at sequence %d: its body "+
			"names packet identifier %d, which that subject does not", subject, smsg.Sequence, cur.PI)
		return mqttInflightUndecided
	case cur.Dur != wantDur || cur.Seq != wantSeq:
		// Someone else's exchange holds this identifier. PRESERVE it. Under sequence
		// addressing this is the case that destroyed a victim.
		if c != nil {
			c.Debugf("MQTT: leaving the in-flight register on %q at sequence %d alone: it names "+
				"delivery %s/%d, not %s/%d", subject, smsg.Sequence, cur.Dur, cur.Seq, wantDur, wantSeq)
		}
		return mqttInflightAlreadyGone
	}
	var id mqttInflightIdentity
	switch {
	case rel == nil:
		id = mqttInflightIdentity{subject: subject, nonce: cur.Nonce, acq: cur.Acq}
	case !rel.valid():
		// A persisted request this broker cannot interpret. It must not fall back to the
		// occupant: that fallback is the retargeting the request exists to forbid.
		mqttWarnf(c, "MQTT: cannot release the in-flight register on %q: its persisted release "+
			"request is not one this broker can apply", subject)
		return mqttInflightUndecided
	default:
		id = mqttInflightIdentity{subject: rel.Subject, nonce: rel.Nonce, acq: rel.Acq}
	}
	// IDENTITY BEFORE EVERY SHORTCUT. A successor — a new allocation for the same source, or
	// the same allocation transmitted again — is preserved whatever phase it is in, and only
	// the exact old request retires.
	if cur.Nonce != id.nonce || cur.Acq != id.acq {
		// PRESERVED, and the request is NOT retired here (holistic review round 4, finding 1).
		// Its consumption belongs to the same save that removes the claim: retired now, a
		// caller whose claim removal is then refused by the fence — or rolled back by a
		// definitely-failed save — retries with no request at all, binds to whatever occupies
		// the subject, and deletes the very successor this arm just preserved.
		return mqttInflightAlreadyGone
	}
	if mqttInflightPastPubRec(cur.State) || mqttInflightTerminalPhase(cur.State) || cur.Retired {
		// NOT THIS PATH'S TO RELEASE, and NOT undecided — reporting UNDECIDED here stalls
		// the drain for the life of the session.
		//
		// Three shapes, one reading: the slot has an owner that will release it, so this
		// drain's obligation is discharged. Past PUBREC it is PUBCOMP's terminal path.
		// `publish_terminal` is the same statement for the PUBLISH side (F-098): the
		// acknowledgement recorded the exchange terminal and owes only the delete, which
		// its own continuation performs. Retired is the same source-side proof in the crash
		// cut where State is still PUBLISH; it is not itself a phase transition.
		return mqttInflightReleased
	}
	if !mqttSlotAdmitsDisposition(cur, mqttSlotRefFromRecord(subject, smsg.Sequence, cur),
		mqttSlotDeleteComplete) {
		// The phase gate, on the record this function has ALREADY loaded and matched.
		mqttWarnf(c, "MQTT: leaving the in-flight slot on %q at sequence %d alone: its state does "+
			"not permit this release", subject, smsg.Sequence)
		return mqttInflightUndecided
	}
	// D3, AND IT IS THIS ITEM'S HEADLINE BEHAVIOUR. The authority is interpreted HERE, at the
	// decision, not only at the door: a RESERVED slot is possibly-attempted — the marker is
	// gone, nothing cancels it, no terminal phase was written — so the client may hold this
	// identifier and the register is the only durable evidence of that.
	//
	// PRESERVED, and the drain's obligation is nonetheless DISCHARGED. That is the reading
	// this function already gives past-PUBREC, terminal and Retired slots directly above:
	// the slot has an owner that will release it, so the drain's source-side progress —
	// claim removal, source acknowledgement, consumer delete — proceeds. Reporting Undecided
	// instead would stall this drain for the life of the session (see the comment above),
	// which is the fail-closed placeholder attempt 2 recorded as NOT strictly safer.
	//
	// BEFORE recordDrainReleaseRequest, deliberately: there is no release to request, so a
	// first reserved pass records no `Rel` at all. An existing request from an earlier
	// attempt is left untouched and is consumed by the finalize's own transaction.
	//
	// WITHHOLD is different and keeps its meaning: the evidence is unusable, which is a
	// transient fact, where Reserved is a durable one.
	switch sess.mintSlotRelease(subject, cur, smsg.Sequence).auth {
	case mqttSlotReservedAuthority:
		if c != nil {
			c.Debugf("MQTT: preserving the in-flight register on %q: its exchange is "+
				"possibly-attempted, so the identifier stays with the client while this "+
				"drain completes", subject)
		}
		return mqttInflightReleased
	case mqttSlotWithhold:
		mqttWarnf(c, "MQTT: withholding the release of the in-flight register on %q: its "+
			"release authority could not be established", subject)
		return mqttInflightUndecided
	}
	// THE POSITIVE RELEASE REQUEST, PERSISTED AND CONFIRMED BEFORE THE DELETE (L4), captured
	// from the record this function has ALREADY loaded and matched — so there is no window
	// between naming the target and acting on it. When a request already exists this is a
	// no-op, and the identity above was taken from it rather than from this load.
	if !sess.recordDrainReleaseRequest(c, wantDur, wantSeq, subject, cur) {
		mqttWarnf(c, "MQTT: withholding the release of the in-flight register on %q: its "+
			"release request could not be recorded, so nothing is deleted", subject)
		return mqttInflightUndecided
	}
	// THROUGH THE ONE ADMISSION RULE, so a live action on OUR allocation — our own token
	// included — refuses the delete, at the exact sequence this load returned.
	deleted := false
	if sess.reapLoadedRegister(subject, smsg.Sequence, cur, mqttSlotDeleteComplete, mqttLicenceOf(c), nil) != mqttReapDeleted {
		mqttWarnf(c, "MQTT: unable to release the in-flight register on %q at sequence %d",
			subject, smsg.Sequence)
		return mqttInflightUndecided
	}
	deleted = true
	// "Deleted" or "already absent" is exactly what a REWRITE of the same delivery looks like
	// from here: S vanishes and S2 appears. Returning Released on that reading would let the
	// caller clear its progress, persist its claim removal, acknowledge the source and delete
	// the consumer while S2 survives. So the loop re-reads and re-classifies through the SAME
	// states — and against the SAME bound identity — rather than trusting one round. It
	// terminates: each iteration either deletes a strictly newer sequence of that identity
	// or reaches absent / a successor / undecided, and the attempt bound stops a pathological
	// rewrite-per-iteration from spinning.
	for attempt := 1; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, err := as.loadInflightRecord(subject)
		switch {
		case err != nil:
			mqttWarnf(c, "MQTT: released the in-flight register on %q but could not confirm the "+
				"subject is clear: %v", subject, err)
			return mqttInflightUndecided
		case smsg == nil:
			if deleted {
				return mqttInflightReleased // genuinely clear, and ours went
			}
			return mqttInflightAlreadyGone
		case cur == nil:
			mqttWarnf(c, "MQTT: an unreadable in-flight register occupies %q at sequence %d after "+
				"the release", subject, smsg.Sequence)
			return mqttInflightUndecided
		case !mqttInflightRecordMatchesSubject(cur, subject):
			mqttWarnf(c, "MQTT: an in-flight register whose body names packet identifier %d occupies "+
				"%q at sequence %d after the release", cur.PI, subject, smsg.Sequence)
			return mqttInflightUndecided
		case cur.Dur != wantDur || cur.Seq != wantSeq:
			// A different exchange took the subject. Not ours.
			if deleted {
				return mqttInflightReleased
			}
			return mqttInflightAlreadyGone
		case cur.Nonce != id.nonce || cur.Acq != id.acq:
			// A SUCCESSOR for the same source — even after sequence 100 was ours and went.
			// Preserved. AlreadyGone completes THIS request's source-side progress and says
			// nothing about the successor's slot, token or identifier — and the request
			// itself stands until the claim removal that consumes it is durable (round 4,
			// finding 1).
			return mqttInflightAlreadyGone
		}
		// The SAME identity at a new sequence: a rewrite of our own record — a mark, a custody
		// write. Follow it through the same property.
		if mqttInflightPastPubRec(cur.State) || mqttInflightTerminalPhase(cur.State) || cur.Retired {
			// NOT THIS PATH'S TO RELEASE, for the reason given on the first edge above. A
			// pre-check at the caller cannot substitute: the slot can be advanced by the
			// PUBREC handler between that check and this reread.
			return mqttInflightReleased
		}
		if !mqttSlotAdmitsDisposition(cur, mqttSlotRefFromRecord(subject, smsg.Sequence, cur),
			mqttSlotDeleteComplete) {
			mqttWarnf(c, "MQTT: leaving the rewritten in-flight slot on %q at sequence %d alone: "+
				"its state does not permit this release", subject, smsg.Sequence)
			return mqttInflightUndecided
		}
		// D3 ON THE REWRITE TOO. A rewrite can have made the exchange possibly-attempted
		// since the first observation — a consumed marker is exactly that — so the decision
		// is re-taken against what is there now rather than inherited from the earlier loop
		// iteration.
		switch sess.mintSlotRelease(subject, cur, smsg.Sequence).auth {
		case mqttSlotReservedAuthority:
			if deleted {
				return mqttInflightReleased // ours went; the successor is not ours to free
			}
			return mqttInflightReleased
		case mqttSlotWithhold:
			mqttWarnf(c, "MQTT: withholding the release of the rewritten in-flight register "+
				"on %q: its release authority could not be established", subject)
			return mqttInflightUndecided
		}
		if sess.reapLoadedRegister(subject, smsg.Sequence, cur, mqttSlotDeleteComplete, mqttLicenceOf(c), nil) != mqttReapDeleted {
			mqttWarnf(c, "MQTT: unable to release a rewritten in-flight register on %q at sequence %d",
				subject, smsg.Sequence)
			return mqttInflightUndecided
		}
		deleted = true
	}
	// Rewritten faster than it could be released. Not a licence to record progress.
	mqttWarnf(c, "MQTT: the in-flight register on %q is being rewritten faster than it can be "+
		"released; withholding", subject)
	return mqttInflightUndecided
}

// mqttInflightIdentity is the transmission identity a destructive continuation is bound to:
// the slot's subject, the allocation's nonce and the transmission counter (C-I).
type mqttInflightIdentity struct {
	subject string
	nonce   string
	acq     uint64
}

// reapIncarnationRegisters removes one retiring incarnation's registers by ENUMERATING
// them, loading each, and deleting at the exact sequence the load returned — bounded by
// the watermark the enumeration itself took.
//
// It replaces an unbounded `purgeStream(..., filter, 0, ...)` that read nothing. That
// purge had two problems and they need two different mechanisms:
//
//  1. IT COULD NOT NAME WHAT IT DESTROYED. A peer may still be serving this incarnation —
//     a resume preserves the generation — so "no second party" was a statement about this
//     broker, not about the cluster. Enumerating and loading is what gives every deletion
//     a subject and a record to check, which is R4 applied to the purge rather than only
//     to the singleton deletes.
//
//  2. IT HAD NOTHING TO BOUND ITSELF BY, because it enumerated nothing. That is fixed
//     here by keeping the watermark `streamSubjects` already returns and SKIPPING any
//     record above it. A register created after the enumeration is not this pass's to
//     destroy, and a replacement landing between the enumeration and the load is exactly
//     the case the identity check cannot see — the pass learns both the nonce and the
//     sequence from the same load, so it holds no prior identity to call it a mismatch.
//     Only the watermark distinguishes them.
//
// That second point is why the identity check ALONE is insufficient and why F-062's
// cutoff mechanism, which an earlier draft of this design rejected outright, is half the
// answer. The two mechanisms close two different windows.
//
// The watermark is used in the ONE direction `streamSubjects` supports: as a bound on
// destruction. Its contract is explicit that `State` and `State.Subjects` are not one
// snapshot, so a returned subject may sit ABOVE the watermark — "that direction is benign
// for every caller here — such a record is SPARED by a bounded purge" — and that reading
// everything returned as at-or-below is FALSE. This spares; it never infers.
//
// A zero watermark is not a licence: with no bound available the pass deletes nothing and
// reports failure. What that buys depends on the caller: a record-backed teardown keeps its
// marked record, so a later attempt re-enumerates, while clear()'s no-record branch has
// neither record nor marker and is not re-driven at all — there the orphan sweep is the only
// cleanup. docs/CONFORMANCE.md states the split per call site.
//
// Failure semantics are fail-closed, and the caller's contract is the important half: an
// unreadable or indeterminate occupant aborts with an error, so the teardown does not
// release the record while a register of unknown status remains. That is a deliberate
// wedge — a permanently unreadable record blocks this client's teardown until an operator
// removes it — and it is the correct trade against releasing a record on a read that
// failed. The diagnostic names the stream, the subject and the sequence for exactly that
// reason.
//
// spareRetained spares RETAINED registers (F-105 7d).
// A DISCARDED generation's retained register is its exchange's discard cleanup's, not this
// subject walk's: it carries the identity proof (RelID) of the exchange's canonical PUBREL, and the
// executor deletes that rel first, then the register (outbound_discard_exec.go). Taking the
// register here would leave the rel unprovable and so undeletable. The spared register keeps every
// guard below — it is skipped only after it was read, attributed and found to agree with its
// subject — so the walk's bound and its refusals are unchanged; its marker is still reaped, since
// nothing in a discarded generation reads one.
func (as *mqttAccountSessionManager) reapIncarnationRegisters(c *client, idHash, gen string, spareRetained bool) error {
	filter := mqttInflightWalkFilter(idHash, gen)
	// RESAMPLE for a usable watermark rather than refusing on the first zero, which is
	// the rule the inbound hold walk already applies and for the same reason: STREAM.INFO
	// builds State before filling State.Subjects from a separate store read, so a walk
	// that started against an empty stream can legitimately be handed a non-empty subject
	// map with a zero last sequence. Nothing is written between samples, which is what
	// makes retrying safe, and the watermark never rewinds, so a later sample resolves it.
	//
	// Exhaustion is an ERROR, not an unbounded pass: this walk has found real
	// generation-scoped registers and merely cannot bound its entitlement to them.
	var subjects map[string]uint64
	var watermark uint64
	var lastSeen uint64
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		var err error
		subjects, watermark, err = as.enumerateInflightSubjectsBounded(filter)
		if err != nil {
			if isErrorOtherThan(err, JSStreamNotFoundErr) {
				return fmt.Errorf("enumerating the outbound in-flight registers: %w", err)
			}
			return nil // no stream, nothing to reap
		}
		if len(subjects) == 0 {
			return nil
		}
		if watermark != 0 {
			break
		}
		lastSeen = watermark
	}
	if watermark == 0 {
		return fmt.Errorf("outbound in-flight registers match %q but the stream reported an "+
			"unusable last sequence %d after %d attempts, so this pass has no proven bound",
			filter, lastSeen, mqttInflightStoreAttempts)
	}

	doomed := make([]uint64, 0, len(subjects))
	names := make([]string, 0, len(subjects))
	for subject := range subjects {
		names = append(names, subject)
	}
	// Deterministic, so a log line or a test does not depend on map iteration.
	sort.Strings(names)

	// Batched, for the reason the resume walk states: one incarnation may legitimately
	// hold up to 65_535 identifiers, and a per-record round trip would put tens of
	// thousands of serial requests on a teardown.
	for start := 0; start < len(names); start += mqttInflightLoadBatch {
		end := min(start+mqttInflightLoadBatch, len(names))
		batch := names[start:end]
		resps, berr := as.loadInflightRecordsMulti(batch)
		if berr != nil {
			return fmt.Errorf("loading outbound in-flight registers for %q/%q: %w", idHash, gen, berr)
		}
		for i, resp := range resps {
			subject := batch[i]
			switch mqttClassifyMsgGetResponse(resp) {
			case mqttInflightResponseAbsent:
				// Enumerated and now gone. Benign HERE, unlike on the resume path,
				// because this pass is destroying rather than reserving: an absent
				// register is one less thing to delete.
				continue
			case mqttInflightResponsePresent:
			default:
				return fmt.Errorf("in-flight register on %q for %q/%q could not be read, so this "+
					"teardown cannot prove what it would destroy: remove or repair that record",
					subject, idHash, gen)
			}
			smsg := resp.Message
			if smsg.Sequence > watermark {
				// Above this pass's entitlement: written after the enumeration took its
				// watermark, so it is not ours to destroy. Left to the next teardown
				// pass, the store path's reconcile, a later resume, or the orphan sweep.
				continue
			}
			rec, derr := mqttDecodeInflightRecord(smsg.Data)
			if derr != nil {
				return fmt.Errorf("in-flight register on %q at sequence %d for %q/%q is unreadable "+
					"(%v), so this teardown cannot prove what it would destroy: remove or repair "+
					"that record", subject, smsg.Sequence, idHash, gen, derr)
			}
			// The identity that authorizes this deletion is the incarnation the subject
			// names, which is what the enumeration filtered on — so a record whose own
			// body disagrees with its subject is refused rather than deleted.
			name, perr := mqttParseInflightSubject(subject)
			if perr != nil || name.gen != gen || name.idHash != idHash {
				return fmt.Errorf("in-flight register subject %q does not name incarnation %q/%q: %v",
					subject, idHash, gen, perr)
			}
			// And the body must agree with the subject, at least as strictly as recovery
			// requires. A destructive path may not be more permissive than the reader.
			if rec.PI != name.pi {
				return fmt.Errorf("in-flight register on %q at sequence %d names packet identifier "+
					"%d, which that subject does not: remove or repair that record",
					subject, smsg.Sequence, rec.PI)
			}
			if spareRetained && rec.retainedSource() {
				continue // its exchange's discard cleanup deletes it, after its canonical PUBREL
			}
			doomed = append(doomed, smsg.Sequence)
		}
	}
	if err := as.deleteInflightRegistersPipelined(doomed); err != nil {
		return err
	}
	// THE MARKER PASS, after the register pass and under the SAME proven watermark
	// (N-5.11): a marker above it belongs to a pair committed after this pass took its
	// bound — as does its register, which the pass above spared for the same reason — so
	// the pair survives whole and no bare register is ever left. Each marker is met under
	// "reap", capture first: its body is in hand from the load, and its sibling is read
	// afresh before the boundary decides (rows 24-28).
	return as.reapIncarnationMarkers(c, idHash, gen, watermark)
}

// reapIncarnationMarkers is the generation teardown's marker pass. See
// reapIncarnationRegisters, whose watermark it inherits rather than re-sampling: a fresh
// sample could only be HIGHER, and a higher bound would admit markers whose registers the
// register pass had already spared.
func (as *mqttAccountSessionManager) reapIncarnationMarkers(c *client, idHash, gen string, watermark uint64) error {
	filter := mqttInflightMarkerWalkFilter(idHash, gen)
	subjects, _, err := as.enumerateInflightSubjectsBounded(filter)
	if err != nil {
		if isErrorOtherThan(err, JSStreamNotFoundErr) {
			return fmt.Errorf("enumerating the unsent markers: %w", err)
		}
		return nil
	}
	if len(subjects) == 0 {
		return nil
	}
	names := make([]string, 0, len(subjects))
	for subject := range subjects {
		names = append(names, subject)
	}
	sort.Strings(names)
	var failed int
	var first error
	for start := 0; start < len(names); start += mqttInflightLoadBatch {
		end := min(start+mqttInflightLoadBatch, len(names))
		batch := names[start:end]
		resps, berr := as.loadInflightRecordsMulti(batch)
		if berr != nil {
			return fmt.Errorf("loading the unsent markers for %q/%q: %w", idHash, gen, berr)
		}
		for i, resp := range resps {
			regName, perr := mqttParseInflightMarkerSubject(batch[i])
			if perr != nil {
				// The marker filter cannot return a subject that is not a marker; a test
				// seam answering both walks from one list can. Nothing here acts on it.
				continue
			}
			if regName.gen != gen || regName.idHash != idHash {
				return fmt.Errorf("unsent marker subject %q does not name incarnation %q/%q", batch[i], idHash, gen)
			}
			load := mqttMarkerLoadFromResponse(resp)
			switch load.kind {
			case mqttInflightResponseAbsent:
				continue // enumerated and now gone: one less thing to delete
			case mqttInflightResponsePresent:
			default:
				return fmt.Errorf("unsent marker on %q for %q/%q could not be read, so this teardown cannot prove what it would destroy", batch[i], idHash, gen)
			}
			if load.seq > watermark {
				continue // above this pass's entitlement, with its register: a pair the next pass meets
			}
			// CAPTURED by the load above; the sibling is read now, after it.
			mk := load.observe(mqttMarkerCaptured, _EMPTY_)
			sibling := as.siblingObservation(regName.subject, load.nonceOf())
			switch as.executeReap(c, mqttMarkerCallerClear, regName.subject, sibling, mk, true) {
			case mqttMarkerDeleteMarker, mqttMarkerPreserveMarker:
			default:
				failed++
				if first == nil {
					first = fmt.Errorf("the unsent marker on %q at sequence %d could not be reaped", batch[i], load.seq)
				}
			}
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d unsent marker deletion(s) failed; first: %w", failed, first)
	}
	return nil
}

// mqttInflightReapConcurrency bounds the reap's in-flight confirmed deletes.
//
// One incarnation may hold up to 65_535 registers, and a confirmed delete is a round trip
// with retries. Issuing them serially is the shape inflight_register.go's own resume walk
// forbids for exactly this reason — "a per-record round trip would put tens of thousands of
// serial requests in front of every reconnect" — and the teardown path is no more able to
// absorb that than the CONNECT path is. Named rather than inline so the cost is a decision
// with a place to argue about it, and kept beside mqttInflightLoadBatch, which bounds the
// same pass's reads.
const mqttInflightReapConcurrency = 16

// deleteInflightRegistersPipelined issues confirmed deletes under a bounded concurrency
// cap and AGGREGATES failures rather than stopping at the first.
//
// Aggregation matters for the caller's contract: the teardown must not release the record
// while ANY register of unknown status remains, so a partial pass has to report that it was
// partial. Progress is still real — every delete that landed stays landed. Whether anything
// re-enumerates the survivors is the CALLER's property, not this function's: a marked record
// anchors a retry, and clear()'s no-record branch has no marker to anchor one.
func (as *mqttAccountSessionManager) deleteInflightRegistersPipelined(seqs []uint64) error {
	if len(seqs) == 0 {
		return nil
	}
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		failed int
		first  error
	)
	sem := make(chan struct{}, mqttInflightReapConcurrency)
	for _, seq := range seqs {
		seq := seq
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := as.deleteInflightRegisterConfirmed(seq, nil); err != nil {
				mu.Lock()
				failed++
				if first == nil {
					first = fmt.Errorf("deleting the in-flight register at sequence %d: %w", seq, err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if failed > 0 {
		return fmt.Errorf("%d of %d outbound in-flight register deletions failed; first: %w",
			failed, len(seqs), first)
	}
	return nil
}

// errMQTTDeleteNotAttempted is a gated confirmed delete that stopped before an attempt because
// its caller's gate closed (shutdown): nothing more was issued, and the outcome of any earlier
// attempt is unknown.
var errMQTTDeleteNotAttempted = errors.New("the confirmed register delete stopped before an attempt: its gate closed")

// deleteInflightRegisterConfirmed removes a register at its exact sequence and WAITS for
// the outcome, retrying a bounded number of times.
//
// Used where the ordinary asynchronous delete would not be reconcilable: a register whose
// delivery was cancelled names a source whose consumer is about to be deleted, and a
// consumer-gone register is deliberately not read as complete, so a lost delete reserves
// an identifier that only a marker-proven recovery drop can now resolve — and a CANCELLED
// allocation's register is precisely the shape that cannot rely on one, because its marker
// was consumed or was never the evidence. Treat it as unresolvable here. A register
// whose source stays live has no such problem, which is why the ordinary path is allowed
// to be fire-and-forget.
//
// proceed, when non-nil, is asked before EACH attempt (F-088 WP3 rule 6 (c)): the
// exclusion-reclaim pass holds its worker token across these requests, so it must issue none
// after shutdown closes dispatch, and it charges each attempt it issues to its budget (rule 6
// (a)). ONE primitive for both forms, so every register delete stays visible under this name to
// the structural censuses that route deletes through the verdict property.
func (as *mqttAccountSessionManager) deleteInflightRegisterConfirmed(seq uint64, proceed func() bool) error {
	if seq == 0 {
		return nil
	}
	var err error
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		if proceed != nil && !proceed() {
			return errMQTTDeleteNotAttempted
		}
		// The seam is INSIDE the loop deliberately: a hook that short-circuited the retry
		// would make every test of this path exercise control flow production never runs.
		as.mu.RLock()
		hook := as.testInflightDelete
		as.mu.RUnlock()
		if hook != nil {
			err = hook(seq)
		} else {
			err = as.jsa.deleteMsg(mqttInflightStreamName, seq, true)
		}
		if err == nil {
			return nil
		}
		// A sequence that is already GONE is a success, not a failure to retry — and it has
		// two authoritative shapes, both of which must be accepted. The server reports a
		// delete of a missing sequence either as JSSequenceNotFoundErrF or as
		// JSStreamMsgDeleteFailedF wrapping ErrStoreMsgNotFound (the same pair
		// reapQ2AffClaim and releaseTeardown tolerate). Accepting only the first is how a
		// drained exchange whose register was already released gets its completion withheld
		// forever.
		if !isErrorOtherThan(err, JSSequenceNotFoundErrF) ||
			isJSErrCausedBy(err, JSStreamMsgDeleteFailedF, ErrStoreMsgNotFound) {
			return nil
		}
	}
	return err
}

// cancelRegPendingLocked records, durably, that an allocation whose register store is still
// in flight has been withdrawn — a staged drain, or a consumer teardown that purges its
// pending deliveries.
//
// The in-memory handoff already covers the ordinary case: confirmInflightRegister finds no
// entry, refuses the send and removes the register it just wrote. What it cannot cover is a
// crash. The cancelling packet can persist its removal and delete the consumer while the CAS
// is still on the wire, and if that CAS lands and this broker dies before the confirmation
// runs, no tombstone and no owed-delete entry was ever written — so recovery finds an
// ordinary register whose consumer is gone, which it deliberately does not read as
// complete. Recovery now releases that shape when the marker proves the PUBLISH never went
// out (F-102) — but this window cannot count on that: the crash it describes lands the CAS
// with the confirmation never run, so which side of the marker the pair ends on is exactly
// what is unknown. A possibly-attempted one still holds the identifier and its Receive
// Maximum slot for the life of the session.
//
// Recording the nonce is what closes that window, and it is durable at the right moment for
// free on the path that matters: stageDrain runs inside its packet's own record
// transaction — the UNSUBSCRIBE's, whose save is confirmed before commitUnsub deletes the
// consumer, or a SUBSCRIBE's, where the QoS 0 downgrade's conversion runs at the
// final-update boundary and the same save carries both facts.
//
// sess.mu must be held.
//
// It reports false ONLY when the staging was REFUSED (the licence is not the object's accepted
// one): an entry with nothing to stage (no store in flight, no nonce) is not a decision this
// connection was refused, and its caller proceeds exactly as before.
func (sess *mqttSession) cancelRegPendingLocked(ack *mqttPending, lic mqttDestroyLicence) bool {
	if ack == nil || !ack.regPending || ack.regNonce == _EMPTY_ {
		return true
	}
	return sess.stageCancellationVerdictLocked(lic, ack.regNonce)
}

// stageCancellationVerdictLocked is the ONE way a NEW cancellation decision enters the verdict
// map (F-105 7b). The map belongs to the session OBJECT and every later save marshals it, so the
// deciding connection must still be the object's accepted one: its licence's entitlement is the
// object's boundEnt. After a same-object successor accepted, boundEnt is the successor's and a
// stale connection's staging is REFUSED — reported, so the caller takes its no-verdict arm and
// does not treat the cancellation as established. A normal close stages after detaching (sess.c
// is already nil) under the entitlement it was accepted with, which is still boundEnt until a
// successor accepts. Replay of a verdict that is already durable (record hydration, a fork merge)
// is not a decision and does not come here.
//
// sess.mu must be held.
func (sess *mqttSession) stageCancellationVerdictLocked(lic mqttDestroyLicence, nonce string) bool {
	if !lic.entOK || !sess.boundEntOK || lic.ent != sess.boundEnt {
		return false
	}
	sess.voidRegNonceLocked(nonce)
	return true
}

// voidRegNonceLocked records that one register is void, whatever the reason.
//
// sess.mu must be held.
func (sess *mqttSession) voidRegNonceLocked(nonce string) {
	if nonce == _EMPTY_ {
		return
	}
	if _, already := sess.cancelledRegNonces[nonce]; already {
		return
	}
	if sess.cancelledRegNonces == nil {
		sess.cancelledRegNonces = make(map[string]uint64, 1)
	}
	sess.voidedRegsRev++
	// Stamped with the revision of ITS OWN insertion: a save captures the revision before
	// it marshals, so a landed save with captured revision r carried every nonce stamped
	// <= r. That stamp, against voidedRegsSavedRev, is what proves THIS verdict durable
	// rather than merely present beside one that is.
	sess.cancelledRegNonces[nonce] = sess.voidedRegsRev
}

// releaseCancelledReservation frees an identifier a consumer teardown withdrew, for the one
// caller that can prove THIS ATTEMPT never wrote a register for its nonce: the admission
// ladder's entry check, which runs BEFORE any CAS is issued. (The cancellation's own pending
// FENCE may carry the nonce; it is disposed of under its provenance rule and is no
// admission's register — F-088. Whether an earlier, paused attempt can still land after the
// fence is gone is F-119's residual.) With no write attempted here there is nothing this
// attempt could resurrect, so neither rung of the ladder is owed and the identifier is free —
// and the VERDICT is retired on the same proof, or a nonce no register can ever carry would
// sit in the record forever (recovery keeps verdicts whose register it has not seen, because
// scan-time absence cannot rule out a late CAS; this is the one place absence is provable).
//
// Every other settle point goes through confirmInflightRegister/abandonInflightRegister,
// which release only against a proven durable rung (see releaseSettledVoidReservationsLocked).
//
// nonce is the CALLER'S allocation identity, and the release happens only when the
// reservation carries it: after a stall the identifier may have been freed, reused and
// cancelled again, and the reservation then belongs to a newer allocation whose CAS is
// still out — precisely the write this proof does not cover.
//
// Session lock NOT held on entry.
func (sess *mqttSession) releaseCancelledReservation(pi uint16, nonce string) {
	if nonce == _EMPTY_ {
		return
	}
	sess.mu.Lock()
	if ack := sess.pendingPublish[pi]; ack != nil && ack.regCancelled && ack.regNonce == nonce {
		if _, held := sess.cancelledRegNonces[nonce]; held {
			delete(sess.cancelledRegNonces, nonce)
			sess.voidedRegsRev++
		}
		delete(sess.pendingPublish, pi)
	}
	sess.mu.Unlock()
}

// releaseSettledVoidReservationsLocked frees the identifiers of cancelled reservations whose
// store has SETTLED (regPending dropped at the settle point) now that their verdicts are
// provably on the session record. Until both held — store settled, verdict durable — the
// identifier stayed reserved: released early, a new allocation could take the subject while
// the old CAS was still in flight, or a crash could resurrect the register as an ordinary
// delivery with nothing anywhere calling it void.
//
// sess.mu must be held, with voidedRegsSavedRev already advanced: every nonce in the set is
// durable at that moment, so membership is the proof.
func (sess *mqttSession) releaseSettledVoidReservationsLocked() {
	for nonce, pi := range sess.settledVoidReservations {
		insertRev, held := sess.cancelledRegNonces[nonce]
		if held && insertRev > sess.voidedRegsSavedRev {
			// Only a verdict the landed save provably SERIALIZED: one recorded after
			// that save's marshal is durable nowhere yet, however recently a different
			// save landed, and releasing on membership alone would free its identifier
			// with a crash still able to resurrect the register.
			continue
		}
		if ack := sess.pendingPublish[pi]; ack != nil && ack.regCancelled && ack.regNonce == nonce {
			// The entry goes on the durable verdict exactly as before. What the verdict does
			// NOT establish is that the REGISTER is gone: a consolidated delete can be
			// withheld under a peer's live action while the tombstone still lands, and the
			// peer may be transmitting under that surviving slot. So the identifier's
			// exclusion is handed off here and released only against a confirmed absence
			// (F-098).
			sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), nonce)
			delete(sess.pendingPublish, pi)
		}
		delete(sess.settledVoidReservations, nonce)
	}
}

// creditVoidedRegsLocked records that a landed record write provably carried every
// void-register verdict up to rev, and releases the reservations that proof frees.
//
// It is the single credit point, and every proof of durability comes through it: a
// CONFIRMED save (saveWithDetail), a readback that finds those exact bytes at a newer
// sequence (resolveUncertainWriteInTx), and the contention classifier's landed verdicts
// (resolveContendedRecordSave). Never an unlanded, unresolved, definitely-failed or
// FOREIGN outcome — a record that is not ours carries nothing of ours.
//
// rev is the revision the write MARSHALLED, never the set's current revision: a nonce
// staged after the marshal is durable nowhere, and crediting it would free an identifier
// while a crash could still resurrect its register as an ordinary delivery.
//
// sess.mu must be held.
func (sess *mqttSession) creditVoidedRegsLocked(rev uint64) {
	// The gap is the common case's whole cost: one comparison on every save.
	if rev <= sess.voidedRegsSavedRev {
		return
	}
	sess.voidedRegsSavedRev = rev
	sess.releaseSettledVoidReservationsLocked()
}

// forgetCancelledReg drops a cancellation intent whose register is provably dealt with:
// deleted, tombstoned, or now carried as an owed delete against its exact sequence.
//
// Session lock NOT held on entry.
func (sess *mqttSession) forgetCancelledReg(nonce string) {
	if nonce == _EMPTY_ {
		return
	}
	sess.mu.Lock()
	if _, ok := sess.cancelledRegNonces[nonce]; ok {
		delete(sess.cancelledRegNonces, nonce)
		sess.voidedRegsRev++
	}
	// Retirement is itself a release edge. A reservation that SETTLED while the verdict
	// was still unproven parks waiting for the verdict's save — and a stream-rung proof
	// arriving after the settle (recovery deleting the register, a securing retry landing
	// its tombstone) removes the very membership that save-keyed release requires. Nothing
	// later re-examines the reservation, so the retirement must. Through the index, not a
	// pendingPublish walk: this runs once per voided register in a teardown, and walking a
	// map that can hold tens of thousands of live deliveries per nonce is quadratic.
	if pi, parked := sess.settledVoidReservations[nonce]; parked {
		if ack := sess.pendingPublish[pi]; ack != nil && ack.regCancelled && ack.regNonce == nonce {
			// Same handoff as the save-keyed release above: a stream rung retiring the
			// verdict says nothing about a register a peer may still be acting on (F-098).
			sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), nonce)
			delete(sess.pendingPublish, pi)
		}
		delete(sess.settledVoidReservations, nonce)
	}
	sess.mu.Unlock()
}

// retireSupersededVerdictLocked retires one allocation's cancellation verdict because recovery
// has just RESTORED that same allocation as a possibly-attempted acting entry (F-088). The verdict
// said "never transmitted"; the restoration rests on authoritative evidence that the PUBLISH may
// have gone out (its marker absent, or a recovered output token beside a standing marker), so the
// verdict is superseded — and kept, it would fence every retransmission in beginSendLocked and
// strand an exchange the client may hold.
//
// A RESTORATION IS NOT A RELEASE. Unlike forgetCancelledReg it frees no parked reservation and
// hands off no exclusion: the restored entry owns the identifier. It reports whether membership
// changed, and moves voidedRegsRev only then, so the revision-keyed persistence (persistVoidedRegs)
// writes the omission. A crash, or a fork repair's union resurrecting the nonce, before that write
// lands makes the next recovery repeat the same handoff.
//
// sess.mu must be held.
func (sess *mqttSession) retireSupersededVerdictLocked(nonce string) bool {
	if nonce == _EMPTY_ {
		return false
	}
	if _, held := sess.cancelledRegNonces[nonce]; !held {
		return false
	}
	delete(sess.cancelledRegNonces, nonce)
	sess.voidedRegsRev++
	return true
}

// recoveredResidentEligibleLocked reports whether a resident entry recovery leaves in place is
// THIS recovered allocation's ordinary acting entry, held under custody for a lineage that has not
// ended: the only resident a recovered classification may update. An opaque, demoted, foreign or
// cancelled resident, another allocation's, and an ended lineage's are refused.
//
// sess.mu must be held.
func (sess *mqttSession) recoveredResidentEligibleLocked(ack *mqttPending, rv mqttRecoveredInflightReg) bool {
	return ack != nil && rv.rec != nil && ack.regNonce == rv.rec.Nonce && ack.kind == mqttPendingOrdinary &&
		!ack.markerOpaque && !ack.regRecoveryDemoted && !ack.regForeign && !ack.regCancelled &&
		!mqttDurIsSlotOnlyClass(rv.rec.Dur) && sess.ordinaryProjectionHeldLocked(sess.gen, rv.rec) &&
		!sess.custody.lineageEnded(sess.gen, rv.rec.Lin)
}

// mqttSplitAdoptedPastPubRec separates the results a contest returned whose ADOPTED body
// is already past PUBREC from the ones that are still PUBLISH-phase deliveries.
//
// This is the phase half of the rule mqttSplitNewlyRetired states for the retirement flag,
// and leaving it unimplemented was a live defect. The contest's moved-and-ours arm adopts
// the reread WHOLE, and the withheld arm intercepts only action states and terminal — so a
// slot a peer re-phased `publish -> pubrel_ready` between the walk and the landed CAS comes
// back OWNED, carrying a past-PUBREC body. Routed on the pre-contest bucket it became an
// ordinary PUBLISH register: the apply install has no state check, so the exchange got a
// pendingPublish entry, and with a source still re-offerable its cpending mapping was
// restored too — re-sending the payload of an exchange the client had already answered
// with PUBREC.
//
// A caller that hands its owned set to PUBLISH grouping, send-state derivation or
// installation must route what this returns to the past-PUBREC disposition instead. The
// two questions are asked separately because they are separate facts: a Retired body may
// still be PUBLISH-phase, and a past-PUBREC body need not be Retired.
func mqttSplitAdoptedPastPubRec(in []mqttRecoveredInflightReg) (publishPhase, pastPubRec []mqttRecoveredInflightReg) {
	for _, fr := range in {
		if fr.rec != nil && mqttInflightPastPubRec(fr.rec.State) {
			pastPubRec = append(pastPubRec, fr)
			continue
		}
		publishPhase = append(publishPhase, fr)
	}
	return publishPhase, pastPubRec
}

// mqttSplitNewlyRetired separates the results a contest returned carrying Retired from
// the ones that are still PUBLISH-phase deliveries.
//
// Judged on the RETURNED body, never on the bucket the walk sorted the record into
// before the contest ran: between the two, another writer may have proved the exchange
// past PUBREC, and a rebuild carries that fact forward (F-087 WP3).
func mqttSplitNewlyRetired(in []mqttRecoveredInflightReg) (live, newlyRetired []mqttRecoveredInflightReg) {
	for _, fr := range in {
		if fr.rec != nil && fr.rec.Retired {
			newlyRetired = append(newlyRetired, fr)
			continue
		}
		live = append(live, fr)
	}
	return live, newlyRetired
}

// reapRegisterWithVerdict deletes a register at its exact sequence and, where a
// cancellation verdict names its nonce, releases that verdict against the PROOF of the
// deletion. It reports whether the register is gone.
//
// This is a PROPERTY, not a site list: every exact-sequence register delete whose caller
// HOLDS OR CAN RECOVER the exchange nonce routes through here. Three separate reviews
// each found one more delete site that stranded a verdict — the affine sweep, the
// store's stale-record reap, recovery's second retirement drop — which is the signal
// that enumerating sites is itself the defect. The boundary is a decision procedure: a
// caller that can name the nonce owes the verdict a proof, and an AST guard over every
// production file in the package fails on any delete outside the allow-list.
//
// ONE qualification, because the property is not universal and saying so would be false:
// naming the nonce is not enough where the VERDICT cannot be reached. The verdict set
// lives on a session, so a caller with no identity-fenced session resident — the orphan
// sweep's affine arm — can consult nothing. It retains a register that CARRIES a
// cancellation, but a verdict held only in a non-resident session's persisted
// CancelledRegs is invisible to it and that register is still deleted. That is F-090,
// filed under docs/ROADMAP.md §P: the arm that strands a verdict needs a register carrying
// both Q2AffClaim and Q2AffCopy, which the supported default never creates, so the
// population is experimental-only rather than an alpha blocker.
//
// Two things force a confirmed delete, and either is enough:
//
//   - a verdict names the nonce. Forgetting it after a fire-and-forget delete would
//     leave a register nothing anywhere explains, which is the very state the verdict
//     exists to prevent; a failed or ambiguous delete therefore keeps BOTH, and the next
//     scan or settle retries the whole sequence against a verdict that is already
//     durable;
//   - the caller's own semantics require it (mustConfirm) — the store's reap, whose
//     retried create fails forever if the reap did not land.
//
// The forget's persistence is deliberately opportunistic, exactly as the committed
// recovery path: the revision gap rides the existing voidedRegs machinery and lands with
// the next save. The window that leaves — a proven delete, then process death before any
// save carries the forgotten nonce — parks one nonce as stale metadata under the late-CAS
// rule, with no payload, identifier or register consequence. That is the standard the
// tree already accepts here, not a new one (F-087, DECISIONS.md D2).
//
// Session lock NOT held on entry: the verdict lookup takes it briefly, and the confirmed
// delete's round trip must not be paid under it.
// THE PHASE GATE IS THE CALLER'S, and deliberately so. Two shapes of caller exist and they
// must not be conflated:
//
//   - a caller that has ALREADY loaded the record and proven the occupant is this exchange
//     asks mqttSlotAdmitsDisposition with the record in hand and then calls this. Making
//     this function load again would be a second round trip AND a second decision point,
//     where the record could have moved between the two — replacing one TOCTOU with two.
//   - a caller with no record — deleteInflightRegisterFor is the only one — uses
//     reapRegisterWithVerdictGated below, which loads once and gates on what it finds.
//
// The census test is what keeps that second set from growing silently.
//
// gate is the caller's dispatch gate (mqttDoorGate); nil for every caller but the
// exclusion-reclaim continuation.
func (sess *mqttSession) reapRegisterWithVerdict(seq uint64, nonce string, mustConfirm bool,
	observed *mqttInflightRecord, subject string, gate mqttDoorGate) bool {

	if sess == nil || seq == 0 {
		return false
	}
	as := sess.asm
	if as == nil {
		return false
	}
	verdicted := false
	if nonce != _EMPTY_ {
		sess.mu.Lock()
		_, verdicted = sess.cancelledRegNonces[nonce]
		sess.mu.Unlock()
	}
	if !verdicted && !mustConfirm && gate == nil {
		as.deleteInflightRegister(seq)
		return true
	}
	// A GATED caller's delete asks the gate before EACH attempt (F-088 WP3 rule 6 (c)); a nil
	// gate is the ungated form.
	if err := as.deleteInflightRegisterConfirmed(seq, gate); err != nil {
		return false
	}
	if verdicted {
		// THE VERDICT IS RETIRED ONLY AGAINST A CONFIRMED ABSENCE (F-104). The delete above
		// reports success for an ALREADY-ABSENT sequence, so a same-identity rewrite landing
		// between the caller's load and this delete looks identical to a removal — and
		// forgetting there strands the surviving register with neither its verdict nor
		// necessarily a marker, which leaves it permanently reserved.
		//
		// A retained verdict is re-derivable evidence; a forgotten one is unrecoverable, so
		// anything short of a proven absence keeps it. An observation-less caller cannot
		// confirm, and therefore cannot retire.
		// A closed gate issues no confirmation, so the verdict is kept: retained is re-derivable.
		if observed != nil && subject != _EMPTY_ && gate.open() &&
			sess.confirmSlotReleased(subject, observed) == mqttReleaseEstablishedAbsent {
			sess.forgetCancelledReg(nonce)
		}
	}
	return true
}

// mqttDoorGate is a door caller's DISPATCH GATE (F-088 WP3 rule 6 (c)): asked before every
// store request the door issues on that caller's behalf, it answers whether the request may be
// issued — and a caller that holds a worker token across them (the exclusion-reclaim
// continuation) charges each one it allows to its request budget. A nil gate is the ungated door
// every other caller uses.
type mqttDoorGate func() bool

// open asks the gate; a nil gate is always open.
func (g mqttDoorGate) open() bool { return g == nil || g() }

// recoverPublishTerminalSlot performs the register delete a `publish_terminal` slot is
// still owed, and reports whether the subject is now provably clear.
//
// IDENTITY-BOUND AND CONFIRMED, through the same gated reap every terminal cleanup uses:
// the occupant must still be this exchange (subject, nonce, source, and the transmission
// counter at the destructive edge), the disposition must be permitted from the phase
// found, and the delete must be proven rather than issued. A live action token REFUSES —
// a holder that may still write to this slot is not one whose record another party may
// remove, and C-IV arm 2 says so for the release; the same reasoning binds the delete.
//
// A TERMINAL PUBLISH HOLDS NO MARKER. Its intent delete landed before the write, so the
// marker reap here is the "none held" form: it removes a stray rather than a captured
// one, and it runs only after the register half is proven gone, which is the precondition
// that reap's classification depends on.
//
// Session lock NOT held on entry.
func (sess *mqttSession) recoverPublishTerminalSlot(c *client, subject string, seq uint64,
	rec *mqttInflightRecord) (released bool) {

	defer func() {
		if released && mqttTerminalSourceCarrier(rec) {
			if ref := mqttSlotRefFromRecord(subject, seq, rec); ref != nil {
				// released means the identity-bound delete was CONFIRMED.
				released = sess.finishTerminalSource(*ref, false, true)
			}
		}
	}()

	if sess.asm == nil || rec == nil || subject == _EMPTY_ || seq == 0 {
		return false
	}
	if rec.ActionToken != _EMPTY_ {
		// A live action. Its holder may still write, so the record is not this pass's to
		// remove; the drain that ends it is the event that clears the subject.
		return false
	}
	ref := mqttSlotRefFromRecord(subject, seq, rec)
	if ref == nil {
		return false
	}
	if mqttTerminalSourceCarrier(rec) {
		// The terminal pair owns the slot, so the drain finalizer preserves it.
		// Persist claim removal BEFORE settling the source; a failed or unknown
		// save must leave both the source and its suppression carrier intact.
		if !sess.terminalSourcePair(*ref, rec) {
			return false
		}
		sess.mu.Lock()
		w := sess.holdTerminalSourceLocked(*ref)
		if w == nil || (w.record.Nonce != _EMPTY_ &&
			(w.record.Lin != rec.Lin || w.record.QoS != rec.QoS ||
				w.record.SourceAckReply != rec.SourceAckReply)) {
			sess.mu.Unlock()
			return false
		}
		w.record = *rec
		if d := sess.drainForDurableLocked(rec.Dur); d != nil && d.staged {
			sess.mu.Unlock()
			return false
		}
		sess.mu.Unlock()
		sess.finalizeDrainDelivery(c, rec.Dur, rec.Seq, rec.SourceAckReply)
		sess.mu.Lock()
		drainPending := false
		if d := sess.drainForDurableLocked(rec.Dur); d != nil {
			st := d.seqs[rec.Seq]
			drainPending = d.staged || (st != nil &&
				(st.claimed || st.unclaimUnconfirmed || st.rel.valid()))
		}
		sess.mu.Unlock()
		if drainPending || !sess.settleTerminalSource(subject, seq, rec) {
			return false
		}
	}
	// No licence: a terminal register is completion evidence and never an unsent obligation.
	switch sess.reapRegisterGatedOutcome(seq, rec.Nonce, true, ref, mqttSlotDeletePublishTerminal, mqttDestroyLicence{}) {
	case mqttReapDeleted:
		// Deleted at the sequence this caller named. The subject is confirmed below, because
		// an exact-sequence delete reads an ALREADY-ABSENT sequence as success, and a
		// same-identity rewrite between the load and the delete looks exactly like that.
	case mqttReapAbsent:
		return true // nothing there: the obligation is over
	case mqttReapPreservedOther:
		// Another exchange holds the subject: OURS is gone, so this obligation owes nothing
		// and the answer stays true. But the caller releases the identifier hold on true,
		// and a successor's identifier is not ours to free — so the exclusion is handed off
		// first, nonce-less because the occupant is not ours to name (F-098).
		sess.mu.Lock()
		sess.excludeIdentifierLocked(rec.PI, subject, _EMPTY_)
		sess.mu.Unlock()
		return true
	default:
		// Refused, or PRESERVED-MOVED: a same-identity rewrite landed between the caller's
		// observation and the gate, so the terminal record is still there (holistic review
		// round 6, finding 4). Reporting success here released the identifier hold and
		// scheduled nothing, and recovery skipped its reservation and its enrolment.
		return false
	}
	// THE INSTANCE IS CAPTURED BEFORE THE READ THAT PRODUCES THE ABSENCE, and that ordering
	// is the whole point of the instance. Capturing it afterwards makes the comparison
	// ineffective: between the read and the lock a successor can occupy the subject and
	// another continuation can install its exclusion, and a lift that then captured THAT
	// instance would clear a restriction the old absence never observed.
	sess.mu.Lock()
	inst := sess.slotExclusionInstLocked(rec.PI)
	sess.mu.Unlock()
	// THROUGH THE ONE DEFINITION of the post-delete subject confirmation, which this
	// function used to carry inline. Behaviour is unchanged: Unresolved and Refuted still
	// report false, and both established forms still discharge this obligation — the
	// difference between them matters to the identifier's exclusion, not to this answer.
	switch sess.confirmSlotReleased(subject, rec) {
	case mqttReleaseEstablishedAbsent:
		// PROVEN CLEAR: the subject holds nothing — which is the ONE fact that releases an
		// identifier's exclusion, so the exclusion OBSERVED BEFORE THE READ is lifted here.
		if inst != 0 {
			sess.mu.Lock()
			sess.liftIdentifierExclusionLocked(rec.PI, inst, map[string]bool{subject: true})
			sess.mu.Unlock()
		}
	case mqttReleaseEstablishedOther:
		// A successor took the subject. Ours is gone, so the obligation is discharged —
		// and its identifier stays excluded, for the reason the PreservedOther arm gives.
		sess.mu.Lock()
		sess.excludeIdentifierLocked(rec.PI, subject, _EMPTY_)
		sess.mu.Unlock()
		return true
	default:
		// Refuted (our own record, rewritten) or Unresolved (indeterminate, or present but
		// unreadable, which is not an absence). Reporting success released the identifier
		// hold and scheduled nothing.
		return false
	}
	sess.asm.reapMarkerNoneHeld(c, mqttMarkerCallerCleanup, subject)
	return true
}

// resolveAckAttempt settles this section's acquisition attempt against the exchange's
// resolution cell and runs the acknowledgement's cleanup if that completes the pair.
//
// `ours` is true only where the caller PROVED the identity belongs to its own token. Every
// other outcome — an absence, a foreign token, an unreadable read — resolves the attempt
// without publishing, because none of them says which acquisition we owned.
//
// Session lock NOT held on entry.
func (sess *mqttSession) resolveAckAttempt(c *client, tok mqttSendToken, ref mqttInflightSlotRef,
	ours bool) {

	sess.mu.Lock()
	run, pi, bound := sess.publishAckResolutionLocked(tok.res, ref, ours, tok.token)
	sess.mu.Unlock()
	if run {
		sess.handOffAcknowledgedPublish(c, pi, &mqttPending{slot: bound})
	}
}

// holdTerminalCleanupLocked reserves pi's IDENTIFIER for the acknowledged exchange whose
// terminal cleanup is still owed. Called in the SAME critical section that untracks the
// entry, so there is no instant at which the identifier is free while the slot still names
// the exchange.
//
// Session lock held on entry.
func (sess *mqttSession) holdTerminalCleanupLocked(pi uint16, nonce string) {
	if pi == 0 || nonce == _EMPTY_ {
		return
	}
	if sess.sendsInFlight == nil {
		sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
	}
	h := sess.sendsInFlight[pi]
	if h == nil {
		h = &mqttSendHold{}
		sess.sendsInFlight[pi] = h
	}
	h.terminalOwed = nonce
}

// releaseTerminalCleanupHold releases the identifier reserved for nonce's terminal cleanup —
// and only that one: a hold that has since been re-aimed at another exchange is left alone.
//
// Session lock NOT held on entry.
func (sess *mqttSession) releaseTerminalCleanupHold(pi uint16, nonce string) {
	if pi == 0 || nonce == _EMPTY_ {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.releaseTerminalCleanupHoldLocked(pi, nonce)
}

// sess.mu is held; independent output holds and successor exclusions remain intact.
func (sess *mqttSession) releaseTerminalCleanupHoldLocked(pi uint16, nonce string) {
	h := sess.sendsInFlight[pi]
	if h == nil || h.terminalOwed != nonce {
		return
	}
	h.terminalOwed = _EMPTY_
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
}

// completeAcknowledgedPublish performs an acknowledgement's durable half — `publish ->
// publish_terminal -> delete` — and, if any step cannot be confirmed now, hands it to the
// terminal continuation rather than dropping it (holistic review round 1, finding 5).
//
// INLINE, so it is for the READ LOOP's acknowledgement handler only. A send section
// reaches the same obligation through handOffAcknowledgedPublish: both run one attempt
// function under the same ACK authority, so the two cannot disagree about what an
// acknowledgement owes — they differ only in which goroutine pays for it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) completeAcknowledgedPublish(c *client, pi uint16, ack *mqttPending) {
	if ack == nil || !ack.slot.slotOK() || ack.slot.name.subject == _EMPTY_ {
		// No slot reference, so no register this path could terminalize. (An AckRoute's
		// snapshot always carries one: it is built only from an entry with a valid slot,
		// never for a retained or slot-only entry, outbound_ack_route.go.)
		return
	}
	if !sess.attemptTerminalRegisterRelease(c, pi, ack.slot, true) {
		sess.scheduleTerminalRegisterRelease(c, pi, ack.slot, true)
	}
}

// handOffAcknowledgedPublish is completeAcknowledgedPublish for an acknowledgement whose
// durable half falls to a SEND SECTION — parked on the section (the client answered while
// it was still draining) or bound when the section's action resolved. The attempt runs on
// the terminal-retry dispatcher, promptly, and NEVER on the section's own goroutine.
//
// That goroutine is the subscription's delivery callback. A terminal release is several
// JetStream round trips ending in a confirmed source acknowledgement, and JetStream's
// re-offers of the same source queue behind it on that one goroutine. Run inline, the
// release settles and lifts the source's suppression first, and the queued re-offer is then
// admitted as a NEW delivery of a message the client already acknowledged: a fresh
// identifier, DUP=0. Off the goroutine, the re-offer is processed while the release is still
// running and meets the suppression the acknowledgement installed (terminalSourceHeldLocked).
//
// Nothing else moves: the identifier stays held (holdTerminalCleanupLocked) and the source
// suppressed until the attempt confirms cleanup or hands off to a successor, exactly as on
// the path a failed inline attempt always took; shutdown is the dispatcher's
// (admitWillWorker, then recordOwedTerminal for Phase 3b). The identifier argument is the
// slot's own (ack.slot.name.pi); it is taken for parity with completeAcknowledgedPublish.
//
// Session lock NOT held on entry.
func (sess *mqttSession) handOffAcknowledgedPublish(c *client, _ uint16, ack *mqttPending) {
	if ack == nil || !ack.slot.slotOK() || ack.slot.name.subject == _EMPTY_ {
		return // As completeAcknowledgedPublish: no slot, no register to terminalize.
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.armTerminalRetryPromptLocked(c, ack.slot, true)
}

// attemptTerminalRegisterRelease makes ONE attempt at the terminal cleanup a slot is owed, and
// reports whether the obligation is DISCHARGED — the slot deleted, gone, or proven to belong
// to someone else — in which case the identifier hold is released too.
//
// It is BOUND TO ONE IDENTITY, ref's (subject, nonce, source, transmission counter), and it
// never re-aims: a successor transmission or allocation is not this obligation's, so finding
// one discharges the obligation without touching it.
//
// `publish -> publish_terminal` is written ONLY when acknowledged is true, because only the
// client's acknowledgement of THIS transmission licenses it: a generic retry finding an
// ordinary `publish` slot has no authority to terminalize it. A slot already
// `publish_terminal` carries its own authority, which is what recovery's enrolment relies on.
//
// A LIVE ACTION WAITS: its holder may still act on the record, and its own drain clears the
// token; the gated delete refuses under it anyway.
//
// Session lock NOT held on entry.
func (sess *mqttSession) attemptTerminalRegisterRelease(c *client, pi uint16, ref mqttInflightSlotRef,
	acknowledged bool) bool {
	// A FOREGROUND authority operation for its whole span: the PUBACK's advance outlasts the
	// retained background writers (outbound_foreground_priority.go).
	release := sess.announceForegroundAuthority()
	defer release()

	// Includes the shutdown final pass: a deferred recovery owner is reachable,
	// but cannot dispatch cleanup while the recovery walk still owns its apply.
	sess.mu.Lock()
	deferred := sess.terminalRecoveryDepth != 0
	sess.mu.Unlock()
	if deferred {
		return false
	}
	done := func(handedOff bool) bool {
		return sess.finishTerminalSource(ref, handedOff, false)
	}
	// doneOverSurvivor is done() for the exits where THIS obligation is discharged but the
	// SUBJECT is not known empty: a successor or another exchange holds it (F-098, holistic
	// review round 7; plan revision 16's D11, which ruled this and which the implementation
	// had not carried out).
	//
	// The obligation really is over — the record is not ours, and terminalizing a successor
	// would be acting on evidence about a different transmission — but the caller releases
	// the identifier's terminal hold on `true`, and a successor's identifier is not ours to
	// free. Preserving the successor RECORD, which this function already does, protects the
	// record and not the allocator: `bumpPI` and `trackPublishRetained` consult the send
	// registry, so without a handoff the identifier goes back into circulation while a peer
	// may be transmitting under it.
	doneOverSurvivor := func() bool {
		sess.mu.Lock()
		// Nonce-less: the occupant is NOT ours to name, which is the same shape the
		// post-delete confirmation's EstablishedOther arm installs.
		sess.excludeIdentifierLocked(pi, ref.name.subject, _EMPTY_)
		sess.mu.Unlock()
		return done(true)
	}
	if sess.asm == nil || ref.name.subject == _EMPTY_ {
		return false
	}
	sess.mu.Lock()
	stale := ref.name.gen != sess.gen
	sess.mu.Unlock()
	if stale {
		// A proven generation discard: the next incarnation uses different subjects, so this
		// is teardown material rather than retry state.
		return done(false)
	}
	sess.mu.Lock()
	w := sess.terminalSources[mqttTerminalSourceKeyOf(ref)]
	wrongEpoch := (ref.terminalEpoch != _EMPTY_ && ref.terminalEpoch != sess.asm.sessStreamEpoch) ||
		(w != nil && w.epoch != sess.asm.sessStreamEpoch)
	// Creation's source continuation cannot race an unresolved acquisition or
	// consume an ACK handler's hold merely because its source is already complete.
	awaitingACK := !acknowledged && w != nil && ref.state != mqttInflightPublishTerminal &&
		w.record.State != mqttInflightPublishTerminal
	sess.mu.Unlock()
	if wrongEpoch || awaitingACK {
		return false // Keep the captured epoch and the live ACK owner's authority.
	}
	cur, smsg, lerr := sess.asm.loadInflightRecord(ref.name.subject)
	switch {
	case lerr != nil:
		return false // indeterminate: retry
	case smsg == nil:
		return sess.finishAbsentTerminal(ref, acknowledged)
	case cur == nil:
		return false // present but unreadable: not an absence, retry
	case !ref.matchesRecord(cur) || cur.Acq != ref.acq ||
		(ref.abandonedAck != nil && !ref.abandonedAck.belongsTo(sess)):
		// A successor, or another exchange: not this obligation's to touch — and not this
		// obligation's identifier to free either, so the exclusion is handed off with it.
		// The Acq mismatch is included deliberately: a same-allocation RETRANSMISSION is a
		// different transmission under C-I, and the one that is live is not ours.
		return doneOverSurvivor()
	case cur.ActionToken != _EMPTY_:
		return false // a holder may still act; its drain clears the token
	}
	switch cur.State {
	case mqttInflightPublish, mqttInflightDeliveryAbandoned:
		if !acknowledged {
			return done(false) // No authority to consume an ACK owner's restriction.
		}
		// DOES THIS ACKNOWLEDGEMENT END AN F-105 EXCHANGE, OR ONLY ITS SLOT? One gate answers it
		// for every acknowledgement arrow (ackExchangeFor). A refusal writes nothing: the caller
		// keeps its scheduled continuation, so it can never overwrite a transition that landed.
		class, _ := sess.ackExchangeFor(cur, ref.name.gen)
		if class == mqttAckRefused {
			return false
		}
		if class == mqttAckExchange {
			// THE F-105 OBLIGATION: advance the exchange AND re-phase its slot in ONE atomic batch.
			// QoS 1 ends at Terminal (a PUBACK); the only way a QoS 2 exchange reaches an ended
			// PUBLISH phase is a failed PUBREC, which ends it at Rejected. The arm's guards above
			// already established this arrow's predicates — cur.Acq == ref.acq (the answered
			// transmission) and no live action token. A failed advance returns false and the caller
			// schedules the continuation, exactly as a failed slot write does.
			to := mqttExchangeTerminal
			if cur.QoS == 2 {
				to = mqttExchangeRejected
			}
			if _, aerr := sess.advanceExchangeAck(&mqttAdvanceExchangeAckRequest{
				ID: sess.id, Epoch: sess.asm.sessStreamEpoch, Txn: mqttNewInflightNonce(), Op: "op-ack-advance",
				Gen: ref.name.gen, Nonce: cur.Lin, To: to, Acq: ref.acq,
				Register: cur, RegisterSeq: smsg.Sequence,
			}); aerr != nil {
				return false
			}
			// The slot is now publish_terminal; complete its cleanup at the register sequence the
			// atomic batch left it (mqttOpOutcome.Seq is the AUTHORITY sequence, not the
			// register's), so re-read.
			cur2, smsg2, lerr := sess.asm.loadInflightRecord(ref.name.subject)
			if lerr != nil || smsg2 == nil || cur2 == nil {
				return false
			}
			if !ref.matchesRecord(cur2) || cur2.Acq != ref.acq {
				return doneOverSurvivor()
			}
			if !sess.recoverPublishTerminalSlot(c, ref.name.subject, smsg2.Sequence, cur2) {
				return false
			}
			return done(false)
		}
		// SLOT-ONLY (experimental affine: no exchange exists for it; chapter P). THROUGH THE
		// ONE AUDITED WRITER OF THIS ARROW, so `publish -> publish_terminal` has a single
		// definition however the acknowledgement reaches it.
		live := mqttSlotRefFromRecord(ref.name.subject, smsg.Sequence, cur)
		if live == nil {
			return false
		}
		carrier := &mqttPending{slot: *live}
		if !sess.advancePublishTerminal(c, carrier) {
			return false
		}
		terminal := *cur
		terminal.State = mqttInflightPublishTerminal
		if !sess.recoverPublishTerminalSlot(c, ref.name.subject, carrier.slot.seq, &terminal) {
			return false
		}
		return done(false)
	case mqttInflightPublishTerminal:
		if !sess.recoverPublishTerminalSlot(c, ref.name.subject, smsg.Sequence, cur) {
			return false
		}
		return done(false)
	}
	// Past PUBREC or PUBREL-terminal: a different continuation owns this slot now.
	return done(true)
}

// scheduleTerminalRegisterRelease is the terminal continuation: it re-attempts
// attemptTerminalRegisterRelease on the session's own shutdown-gated lifecycle until the
// obligation is discharged. It serves BOTH the live acknowledgement paths (acknowledged
// true) and recovery / store-time discovery of a `publish_terminal` record (false), because
// two mechanisms stating one obligation is how they drift.
//
// UNLIMITED ATTEMPTS WITH A CAPPED DELAY, which is only correct because this work CAN
// succeed.
//
// WHEN WORKER ADMISSION HAS CLOSED the obligation is handed to the server's shutdown
// registry and finished in Phase 3b, while JetStream still answers. Recovery could not
// reconstruct it afterwards: an acknowledgement whose terminal write never landed leaves an
// ordinary `publish` record, which carries no evidence that the client acknowledged it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) scheduleTerminalRegisterRelease(c *client, pi uint16, ref mqttInflightSlotRef,
	acknowledged bool) {

	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.armTerminalRetryLocked(c, ref, acknowledged)
}

// mqttSlotAdmitsDisposition is the phase gate for a caller that HOLDS the record: it has
// loaded the slot, and asks whether the occupant is still this exchange and whether the
// disposition it intends is permitted from the state the record is actually in.
//
// Separate from mqttInflightTransitionAllowed because the identity half matters as much as
// the state half: a permitted disposition against ANOTHER exchange's record is not
// permitted at all.
func mqttSlotAdmitsDisposition(cur *mqttInflightRecord, ref *mqttInflightSlotRef,
	d mqttInflightDisposition) bool {

	if cur == nil || ref == nil {
		return false
	}
	if !ref.matchesRecord(cur) {
		return false
	}
	return mqttInflightTransitionAllowed(d, cur.State)
}

// reapRegisterWithVerdictGated is reapRegisterWithVerdict for the one caller that has no
// record in hand: it loads the slot once, applies the phase gate, and deletes only if the
// occupant is still this exchange and the disposition is permitted from the state found.
//
// An absent subject reports success — there is nothing to delete and nothing can read an
// absent register as an owed delivery. A mismatched occupant also reports success and is
// PRESERVED, the same reading releaseInflightRegisterByIdentity gives. An indeterminate
// read fails closed: it is not an absence and not a licence to delete.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reapRegisterGatedOutcome(seq uint64, nonce string, mustConfirm bool,
	ref *mqttInflightSlotRef, d mqttInflightDisposition, lic mqttDestroyLicence) mqttReapOutcome {

	if sess == nil || seq == 0 {
		return mqttReapRefused
	}
	as := sess.asm
	if as == nil {
		return mqttReapRefused
	}
	if ref == nil || ref.name.subject == _EMPTY_ {
		// No identity to gate on. Fail closed rather than fall back to an ungated delete:
		// a caller that cannot name the slot cannot prove the delete is safe.
		return mqttReapRefused
	}
	cur, smsg, lerr := as.loadInflightRecord(ref.name.subject)
	switch {
	case lerr != nil:
		return mqttReapRefused
	case smsg == nil:
		return mqttReapAbsent // already gone
	case cur == nil:
		return mqttReapRefused // present but unattributable: fail closed
	case !ref.matchesRecord(cur):
		return mqttReapPreservedOther // another exchange's, preserved
	}
	// A never-transmitted delete that finds this allocation's PENDING FENCE disposes of it under
	// the fence's own rule (F-088, consult C4 item 3).
	d = mqttDoorDisposition(ref.name.subject, cur, d)
	if !mqttInflightTransitionAllowed(d, cur.State) {
		// The phase half of mqttSlotDestructionAdmitted, checked FIRST here only so a
		// forbidden phase never reaches the freshness fence's "preserved" answer below.
		return mqttReapRefused
	}
	// C-I AT THE DESTRUCTIVE EDGE, and it has to be here as well as in casInflightSlot
	// because this helper does not go through that door: it deletes the MESSAGE rather
	// than rewriting the record, so the counter comparison the CAS makes never runs.
	//
	// `matchesRecord` above deliberately does NOT compare the counter — identity-as-an-
	// exchange and identity-as-a-transmission are different questions, and a mark or a
	// custody write legitimately acts on an exchange that has since transmitted again.
	// A DELETE is the other kind: B holds evidence naming Acq=k while A installed,
	// transmitted and drained k+1, and nothing moved between B's read and its write, so
	// the freshness fence below cannot catch it either. A sequence is not an identity.
	if mqttSlotDestructiveDisposition(d) && ref.acq != cur.Acq {
		return mqttReapRefused
	}
	// THE FRESHNESS FENCE, and it is a third requirement distinct from the two above.
	// Addressing is by subject; authorization is by exchange identity and legal phase; and
	// this is a CAS-VERSION check — has the record moved since the reference observed it?
	//
	// It is not sequence AUTHORITY and does not reinstate it: the sequence selects nothing
	// and names nothing here. It answers only "is this destructive continuation still
	// operating on the state it decided against". A rewrite means a concurrent operation
	// has re-targeted this exchange's record and reinstalled memory around it, so a stale
	// terminal snapshot must PRESERVE the record rather than delete it — deleting would
	// remove the only durable evidence needed to reconcile that newly installed memory once
	// the source completes. F-034 mutation-tested the opposite behaviour as M26, and the
	// "a moved register is preserved" case of TestMQTTInflightTerminalCleanupRemovesRegisterThenMarker
	// is the fixture that catches it.
	if ref.seq != 0 && smsg.Sequence != ref.seq {
		// A MOVED SEQUENCE IS NEVER RELEASE AUTHORITY FOR THE NEWER TRANSMISSION, and
		// the `true` returns below say only "this continuation is not blocked", never
		// "the identifier may be freed". Consult C2 named this as the trap: a caller
		// reading the success as permission to free its capacity would discharge an
		// obligation nothing proved was over. Nothing is deleted on this branch.
		if d == mqttSlotDeleteNeverTransmitted {
			// A never-transmitted delete must prove that THIS allocation is gone before
			// its reservation is released, and a same-identity rewrite is not that proof.
			// Terminal and already-complete continuations may treat a rewrite as another
			// owner taking responsibility, which is why their existing success result
			// remains.
			//
			// TWO callers, and they do different things with the refusal. A cancellation
			// falls back to tombstoning the live sequence. RECOVERY's marker-proven drop
			// does neither: it leaves the pair for the next pass, and the false return is
			// also what stops it reaping the marker of a register it just declined to
			// delete. Do not read this comment as saying every caller retries.
			//
			// The wrapper maps this to refusal for a never-transmitted delete; the
			// classified value is the same either way: nothing was deleted.
			return mqttReapPreservedMoved
		}
		return mqttReapPreservedMoved // moved on: preserved, and nothing was deleted
	}
	// C-IV ARM 2 AT THE ONE DESTRUCTIVE DOOR: ANY LIVE ACTION WITHHOLDS, OUR OWN INCLUDED.
	//
	// This is what the deleted `pubrel_terminal_draining` phase used to provide, and removing
	// the phase without moving its guard here was a regression the holistic review caught:
	// PUBCOMP advances `pubrel_ready -> pubrel_terminal` while a PUBREL write is still out,
	// preserving the token, and terminal cleanup then deleted the slot under the holder's
	// feet. A holder whose write has not returned may still act on the record, so nothing
	// may remove it; the holder's own drain completes the cleanup from the phase it
	// observes.
	//
	// Checked on the record THIS load returned, immediately before the delete, so a token
	// installed after the caller decided is caught too. A refusal keeps the slot AND the
	// caller's reservation: the classified value says nothing was deleted.
	// THE AUTHORITY IS MINTED FROM THIS LOAD, which is the strongest form available: the
	// record the authority is computed from is the record this delete acts on, so there is
	// no window between the evidence and the act. A caller's own stale token is never
	// accepted here — it would be evidence about a different observation.
	if sess.disposalHoldsSource(cur.Dur, cur.Seq) {
		return mqttReapRefused
	}
	rel := sess.mintSlotRelease(ref.name.subject, cur, smsg.Sequence)
	if !mqttSlotDestructionAdmitted(cur, ref, d, ref.name.subject, smsg.Sequence, rel) {
		return mqttReapRefused
	}
	// The same ending step as reapLoadedRegister: an unsent obligation is ended before its
	// register is destroyed, and the delete acts on the tombstone.
	seq, cur, ok := sess.endUnsentBeforeDestroy(ref.name.subject, smsg.Sequence, cur, d, lic)
	if !ok {
		return mqttReapRefused
	}
	if d == mqttSlotDeletePendingFence {
		// A fence's disposal is always CONFIRMED (F-088, consult C4 item 3): an unproven delete
		// would leave a create-only retry failing identically on a fence still standing.
		mustConfirm = true
		// And it happens only behind a confirmed authority advance, or not at all (F-119).
		if !sess.fenceBeforePendingFenceDisposal(ref.name.subject, cur, lic, nil) {
			return mqttReapRefused
		}
	}
	if !sess.reapRegisterWithVerdict(seq, cur.Nonce, mustConfirm, cur, ref.name.subject, nil) {
		return mqttReapRefused
	}
	return mqttReapDeleted
}

// mqttSlotAcquisitionBaseHolds is the acquisition's premise on the base it is about to CAS: the
// custody, the transmission counter and the absence of any live token, all as the reference
// observed them. One statement for both routes that install a token — the slot machine's own CAS
// and the F-105 authority batch — because each refreshes its base between attempts and each must
// arbitrate, not observe.
func mqttSlotAcquisitionBaseHolds(ref mqttInflightSlotRef, cur *mqttInflightRecord) bool {
	if cur == nil {
		return false
	}
	// SLOT-ONLY (experimental affine) custody only. Admission checked current custody before
	// entering this CAS, but a successor can transfer the quiescent slot between that check and
	// this load; following the rewrite would bind a stale broker's new action to the successor's
	// claim. An ORDINARY slot's ownership is its exchange's (F-105 7b): the acquisition that
	// follows is opAcquireAction, which compares the connection's accepted entitlement, and the
	// register's claim stamp decides nothing.
	if mqttDurIsSlotOnlyClass(cur.Dur) &&
		(ref.claim == _EMPTY_ || cur.Claim != ref.claim || cur.ClaimSeq != ref.claimSeq) {
		return false
	}
	// C-I ON THE ACQUISITION'S OWN BASE, checked here for the same reason the token
	// absence is: this helper REFRESHES its base on every internal attempt. A peer that
	// acquired and drained in between leaves custody unchanged and the token clear, so
	// both checks above pass while the COUNTER has moved — and the caller, which derived
	// its expected counter and the entry's recorded one from its OUTER read, would record
	// an identity the slot never carried. Its own acknowledgement would then read as a
	// successor's, and the terminal cleanup it owes would be dropped (holistic review
	// round 4, finding 2).
	if ref.acq != cur.Acq {
		return false
	}
	// TOKEN ABSENCE IS CHECKED HERE, ON EVERY INTERNAL LOAD AND RETRY, and a
	// caller's own pre-CAS check is NOT a substitute — that check is an
	// observation, and this is arbitration.
	//
	// The schedule it closes needs no failed CAS at all, because this helper
	// refreshes its CAS base on every iteration: A outer-loads an empty token; B
	// acquires and installs token B; A enters here; A's internal load SEES token
	// B; the custody comparison above passes, because B's acquisition did not
	// move custody; and A's mutator then overwrites B's token with its own. Two
	// brokers would each believe they held the exchange, which is the exclusion
	// this whole mechanism exists to provide.
	//
	// The phase gate cannot catch it either, now that a token is legal in every
	// phase (F-098).
	if cur.ActionToken != _EMPTY_ {
		return false
	}
	return true
}

// mqttSlotDestructionAdmitted is the ONE admission rule for removing a slot, shared by the
// loading gate above and by reapLoadedRegister for callers that already hold the record, so
// the two doors can never disagree about what licenses a delete:
//
//   - the phase must permit the disposition;
//   - a destructive disposition must name the transmission the record carries (C-I);
//   - and ANY live action withholds it, ours included (C-IV arm 2).
//   - and a RELEASE must additionally spend an authority bound to the observation it was
//     computed from (F-098's C-IV consolidation). The phase, the counter and the
//     live-action veto together answer "this delete is not blocked"; they never answer
//     "a release is licensed", and the second question is the one the authority settles.
//
// `rel` is that authority. The zero value carries mqttSlotWithhold, so a caller that
// obtained none refuses rather than inherits a licence, and the token re-checks its own
// binding against the record and sequence in hand — an authority computed for a different
// observation cannot be spent here.
//
// `subject` and `seq` are the observation the caller holds. A non-release disposition — a
// mark, a custody write, a phase advance, a replacement — is unaffected: those are separate
// permissions and are deliberately not gated on a release authority.
// mqttSlotGuardsAdmitted is mqttSlotDestructionAdmitted WITHOUT the release-authority
// clause: the phase, the transmission counter and the live-action veto, and nothing else.
//
// It exists for ONE caller, the experimental affine disposer, which F-098's D8 leaves
// deciding its own release (consult D-2, Option A). That exception must **mint no release
// token** — fabricating a proven-unsent authority for a retired record would be a false
// assertion dressed as an exception, not an exception — so it spends none and calls this
// instead. Every other guard still applies.
//
// A supported caller must not reach this. The disposer's population is unreachable without
// the experimental shared-QoS-2 gate, which the production command fixes off.
func mqttSlotGuardsAdmitted(cur *mqttInflightRecord, ref *mqttInflightSlotRef,
	d mqttInflightDisposition) bool {

	if cur == nil || ref == nil || !mqttInflightTransitionAllowed(d, cur.State) {
		return false
	}
	if mqttSlotDestructiveDisposition(d) && (ref.acq != cur.Acq || cur.ActionToken != _EMPTY_) {
		return false
	}
	return true
}

func mqttSlotDestructionAdmitted(cur *mqttInflightRecord, ref *mqttInflightSlotRef,
	d mqttInflightDisposition, subject string, seq uint64, rel mqttSlotRelease) bool {

	if !mqttSlotGuardsAdmitted(cur, ref, d) {
		return false
	}
	if mqttTerminalSourceCarrier(cur) && !rel.source.permits(subject, seq, cur) &&
		!rel.handBack.permits(subject, seq, cur) {
		return false
	}
	if mqttSlotReleaseDisposition(d) && !rel.permits(subject, cur, seq, d) {
		return false
	}
	return true
}

// reapLoadedRegister removes a register the caller has JUST loaded and matched, deleting at
// the exact sequence that load returned.
//
// It does NOT re-read, and that is safe rather than an omission: every rewrite of the record
// — a mark, a custody write, a newly acquired action — lands at a NEW sequence, so a delete
// addressed to the loaded one cannot remove whatever replaced it; it finds the sequence
// already absent. The loading gate (reapRegisterGatedOutcome) exists for callers holding only
// a remembered reference; a caller holding the record would pay an extra round trip there for
// nothing, and a fixture that models the store read by read showed the cost: the gate's own
// read consumed the rewrite the caller's loop was about to follow.
//
// The admission rule is mqttSlotDestructionAdmitted, the same one the gate applies.
//
// gate is the caller's dispatch gate (mqttDoorGate), asked before the evidence probe and before
// every delete request; nil for every caller but the exclusion-reclaim continuation, which
// disposes of pending fences through this door rather than beside it (F-088 WP3 rule 6).
//
// Session lock NOT held on entry.
func (sess *mqttSession) reapLoadedRegister(subject string, seq uint64, cur *mqttInflightRecord,
	d mqttInflightDisposition, lic mqttDestroyLicence, gate mqttDoorGate) mqttReapOutcome {

	if seq == 0 || cur == nil {
		return mqttReapRefused
	}
	if sess.disposalHoldsSource(cur.Dur, cur.Seq) {
		return mqttReapRefused
	}
	d = mqttDoorDisposition(subject, cur, d) // a pending fence is disposed of under its own rule (F-088)
	ref := mqttSlotRefFromRecord(subject, seq, cur)
	if !gate.open() {
		return mqttReapRefused
	}
	rel := sess.mintSlotRelease(subject, cur, seq)
	if !mqttSlotDestructionAdmitted(cur, ref, d, subject, seq, rel) {
		return mqttReapRefused
	}
	// AN UNSENT OBLIGATION IS ENDED BEFORE ITS REGISTER IS DESTROYED, at both entrances of this
	// door: the delete then acts on the tombstone the ending wrote.
	seq, cur, ok := sess.endUnsentBeforeDestroy(subject, seq, cur, d, lic)
	if !ok {
		return mqttReapRefused
	}
	// A pending fence goes only behind a confirmed authority advance, or not at all (F-119).
	if d == mqttSlotDeletePendingFence && !sess.fenceBeforePendingFenceDisposal(subject, cur, lic, gate) {
		return mqttReapRefused
	}
	if !sess.reapRegisterWithVerdict(seq, cur.Nonce, true, cur, subject, gate) {
		return mqttReapRefused
	}
	return mqttReapDeleted
}

// reapObservedRegister deletes the register a store-time arm has JUST loaded, and reports
// whether it was deleted so the caller's retried create finds the subject clear.
//
// It exists so those arms stop pairing a phase check with the UNGATED delete: that pair
// checked the phase and nothing else, so a live action or a foreign counter would have been
// deleted over. It applies the one admission rule through reapLoadedRegister.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reapObservedRegister(c *client, subject string, seq uint64,
	cur *mqttInflightRecord, d mqttInflightDisposition) bool {

	return sess.reapLoadedRegister(subject, seq, cur, d, mqttLicenceOf(c), nil) == mqttReapDeleted
}

// mqttReapOutcome is what the one gated destructive door did, CLASSIFIED, because the
// boolean it replaced conflated three different facts: this call deleted the occupant, the
// subject was already empty, and a DIFFERENT occupant — another exchange, or this exchange
// at a moved sequence — was preserved. A caller that read "true" as "the identifier is free"
// would discharge an obligation nothing proved was over.
type mqttReapOutcome uint8

const (
	// mqttReapRefused: nothing was deleted and nothing is proven — an indeterminate or
	// unreadable read, a phase or counter the disposition may not act on, a live action,
	// or a delete that could not be confirmed.
	mqttReapRefused mqttReapOutcome = iota
	// mqttReapDeleted: this call deleted exactly the occupant it observed.
	mqttReapDeleted
	// mqttReapAbsent: the subject was empty.
	mqttReapAbsent
	// mqttReapPreservedOther: another exchange occupies the subject and was left alone.
	mqttReapPreservedOther
	// mqttReapPreservedMoved: this exchange's record moved since the reference observed it,
	// and was left alone.
	mqttReapPreservedMoved
)

// reapRegisterWithVerdictGated is the boolean form of the one gated destructive door, kept
// for the callers whose continuation only needs "was this blocked". It reports true for
// every outcome that does not block the caller's progress — deleted, absent, or preserved
// under a different owner or sequence — and never means "the identifier may be freed".
// A never-transmitted delete is the exception: a same-identity rewrite does not prove THIS
// allocation gone, so a moved sequence refuses it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reapRegisterWithVerdictGated(seq uint64, nonce string, mustConfirm bool,
	ref *mqttInflightSlotRef, d mqttInflightDisposition, lic mqttDestroyLicence) bool {

	switch sess.reapRegisterGatedOutcome(seq, nonce, mustConfirm, ref, d, lic) {
	case mqttReapRefused:
		return false
	case mqttReapPreservedMoved:
		// A fence disposal, like a never-transmitted delete, must prove THIS record gone; a
		// moved sequence is not that proof (F-088).
		return d != mqttSlotDeleteNeverTransmitted && d != mqttSlotDeletePendingFence
	default:
		return true
	}
}

// parkSettledVoidReservationLocked records a settled-but-unproven reservation in the index
// the proof edges release from.
//
// sess.mu must be held.
func (sess *mqttSession) parkSettledVoidReservationLocked(nonce string, pi uint16) {
	if nonce == _EMPTY_ {
		return
	}
	if sess.settledVoidReservations == nil {
		sess.settledVoidReservations = make(map[string]uint16, 1)
	}
	sess.settledVoidReservations[nonce] = pi
}

// voidVerdictProvenLocked reports whether a reservation's verdict is durably proven — by a
// register-stream rung (the nonce was retired: a landed tombstone or a confirmed delete) or
// by the session record (its insertion revision is at or below the revision a landed save is
// known to carry). It is the ONE predicate every release edge shares, because the two proofs
// and the store's settlement race freely: a verdict can be durable before the settle (no
// later save will come round to release), and a rung can retire the nonce after it (removing
// the membership the save-keyed release would need).
//
// sess.mu must be held.
func (sess *mqttSession) voidVerdictProvenLocked(nonce string) bool {
	insertRev, held := sess.cancelledRegNonces[nonce]
	return !held || insertRev <= sess.voidedRegsSavedRev
}

// mqttNonceSet and mqttSortedNonces convert between the in-memory set and the record's
// list form. The list is sorted so an unchanged set marshals identically, which keeps a
// record write from looking like a change when it is not.
// Restored entries carry insertion revision 0: they came OUT of the record, so they are
// durable by construction and every savedRev comparison passes.
func mqttNonceSet(list []string) map[string]uint64 {
	if len(list) == 0 {
		return nil
	}
	set := make(map[string]uint64, len(list))
	for _, n := range list {
		if n != _EMPTY_ {
			set[n] = 0
		}
	}
	return set
}

func mqttSortedNonces(set map[string]uint64) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// The deferred-cleanup helpers below are also reached from paths with NO bound client — a
// surviving disconnect's shared purge passes nil — so every log site in them goes through
// these. A bare c.Warnf there is a panic on exactly the failure it was trying to report.
func mqttWarnf(c *client, format string, args ...any) {
	if c != nil {
		c.Warnf(format, args...)
	}
}

func mqttErrorf(c *client, format string, args ...any) {
	if c != nil {
		c.Errorf(format, args...)
	}
}

// persistVoidedRegs writes the void-register verdicts to the session record when the record
// is not already known to carry them, and reports whether it now provably does.
//
// Two things about the ordering are deliberate. The nonce set is mutated BEFORE the fence is
// taken (voidRegNonceLocked), which inverts the usual discipline, and it is safe here for a
// reason that does not generalise: the mutation is purely additive on this path, and a
// concurrent fence holder observing the addition and persisting it early is the desired
// result, not a half-applied transaction — the verdict is a fact about the register that
// exists whether or not the record mentions it yet.
//
// And the addition can lose a race it cannot see: a readLoop transaction that has already
// marshalled its record commits without the new entry, while this off-readLoop writer was
// refused the fence. So durability is tracked by a revision counter rather than assumed from
// "I just tried to save". The revision captured before saving is the one that may later be
// declared durable, never the set's current revision: an addition that lands after the
// marshal must stay unsaved, and one that lands before it is merely re-saved later, which
// costs nothing. Only saveLanded licenses the claim — an ambiguous write is resolved by a
// readback.
//
// Session lock NOT held on entry.
// voidVerdictsDurable reports whether every void verdict this session has staged is durably
// credited — that is, whether the revision counter and the credited-revision watermark agree.
//
// It exists because "the record is still ours" and "everything I staged is persisted" are
// DIFFERENT questions, and conflating them is one defect stated three ways. They come apart
// exactly when one fallback write goes ambiguous: `persistRecord` then refuses every later write
// at entry while the session is unresolved, so a second target's cancellation is never sent —
// and resolving the FIRST write credits only the revision that write marshalled. The resolution
// legitimately reports "ours", and the second verdict is still owed.
//
// A caller about to do something irreversible therefore needs both: an ownership verdict AND
// this. Naming the requirement is what makes it complete, where enumerating the writes was not.
//
// No lock held on entry.
func (sess *mqttSession) voidVerdictsDurable() bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	// Over the OUTSTANDING insertions, never over the global mutation counter.
	//
	// voidedRegsRev counts removals as well as insertions: forgetCancelledReg increments it when
	// a register-stream proof RETIRES an already-durable verdict, and that path runs no
	// session-record save because none is owed. Comparing the counter to the saved watermark
	// therefore reports "not durable" for a session whose cleanup entirely SUCCEEDED — which on
	// the direct path would close an already-acknowledged connection, recreating the exact defect
	// this change exists to remove. A holistic round caught that, and it was a new defect rather
	// than another variant of the ordering seam.
	//
	// This is the same test releaseSettledVoidReservationsLocked already applies per nonce: a
	// verdict is durable when the save that landed provably SERIALIZED it, i.e. its insertion
	// revision is at or below the credited watermark.
	for _, insertRev := range sess.cancelledRegNonces {
		if insertRev > sess.voidedRegsSavedRev {
			return false
		}
	}
	return true
}

func (sess *mqttSession) persistVoidedRegs(c *client) bool {
	sess.mu.Lock()
	rev, saved := sess.voidedRegsRev, sess.voidedRegsSavedRev
	sess.mu.Unlock()
	if rev == saved {
		return true
	}
	// tryBeginRecordTx, never the blocking form: this runs on a delivery goroutine, and a
	// refused fence must leave the verdict pending rather than block. The revision gap
	// stays open, so the next convergence point retries — including the one at the end of
	// the UNSUBSCRIBE that is usually the holder here. Refusal does not mean a transaction
	// is open: a fence reserved for an announced blocking writer refuses too, and that
	// waiter's own release is then the retry edge.
	tok, ok := sess.tryBeginRecordTx()
	if !ok {
		return false
	}
	outcome, err := sess.saveClassified(tok)
	switch outcome {
	case saveLanded:
		// Nothing to do: the save credited the revision it marshalled, which is at
		// least rev. See creditVoidedRegsLocked.
	case saveDefinitelyFailed:
		// Nothing landed. The in-memory set still drives this connection's behaviour, and
		// the record is not credited with a verdict it never recorded.
		mqttWarnf(c, "MQTT: could not persist the void-register verdicts for client %q: %v",
			sess.id, err)
	case saveMaybeLanded:
		// The verdict exists either way, so nothing is undone. The REVISION is a different
		// matter: it may be claimed only on proof — and the resolver is what produces it,
		// re-attempting these exact bytes and crediting the revision they marshalled.
		// Nothing further is owed here.
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		mqttErrorf(c, "MQTT: record transaction not released after recording void-register verdicts for client %q: %v", sess.id, terr)
	}
	if outcome == saveMaybeLanded {
		// AFTER the fence is released, never inside it: resolution takes the fence for the
		// whole decide-clear-reconcile transition, so an attempt from within this
		// transaction is refused outright — and the periodic pass would then run the
		// continuations far later, with every save on the session refused meanwhile.
		sess.resolveUncertainNow(c)
	}
	return outcome == saveLanded
}

// The release handoff: who converges a void verdict staged while somebody else held the
// lock it needed.
//
// A verdict is staged in memory by whichever goroutine gives the register up, and it becomes
// durable only on a session-record save. Those two are not the same thread of control, and
// the gap is not a rare interleaving: an off-readLoop register store settling
// (confirmInflightRegister / abandonInflightRegister) stages its nonce and then finds
// persistVoidedRegs REFUSED, because at that instant the record transaction fence — or the
// ownership gate a resolver needs — is either held by somebody else OR, free but RESERVED for
// an announced blocking writer under waiter priority (see sess_recordtx.go). The second cause
// is production-reachable with nobody holding either lock: the fence is free, a blocker
// announces itself and stalls before sess.mu, a register settlement stages a verdict, and
// persistVoidedRegs is refused at the fence's pre-lock fast check. The refusal leaves the
// revision gap open, which is correct, but nothing is left owing the retry.
//
// For a BOUND session the connection itself is the convergence point: ordinary traffic,
// mqttResolveAfterPacket, and the UNSUBSCRIBE tail's flush all save again, and every landed
// save credits what it marshalled. An UNBOUND session has none of that, and a never-expiring
// one has no timer either — so the identifier and its Receive Maximum slot could stay
// reserved for the life of the session, and a crash would restore the surviving register as
// an ordinary pending delivery.
//
// So the rule is at the LOCK RELEASES, which is where the reason for the refusal ends —
// whether that reason was an actual holder or a fence reserved for a waiter, since the waiter
// releases too: every release of the record fence and of the ownership gate re-checks whether
// a verdict was staged while it was held, and if so attempts the save the stager could not.
// Two sites, one predicate, one attempt — deliberately not a third mechanism per staging path.
//
// voidVerdictHandoffDueLocked is that predicate. Each conjunct earns its place:
//
//   - !seqUncertain: an unresolved session refuses every write, and the uncertainty
//     machinery already owns the retry (its resolver's own releases re-check afterwards).
//   - sess.c == nil: a bound session converges through its packet tails, and firing there
//     would inject a fresh ambiguity into a tail that has no resolver — CONNECT's
//     endAcceptTx, right after a success CONNACK, with the ownership gate held.
//   - a revision gap: the record does not carry everything staged.
//   - rev above the handoff watermark: one attempt per newly staged revision, which is what
//     makes the sequence of attempts finite.
//   - the fence closed AND no blocking writer announced: an open transaction means a writer
//     is mid-record, and ITS release re-checks. An ANNOUNCED waiter is the same situation one
//     step earlier — the fence is reserved for it, so taking it here would be exactly the
//     barge waiter priority exists to refuse — and the waiter's own release re-checks in turn.
//     The deferral is a handoff, not a strand: provided blocking arrivals are finite and each
//     holder releases, the chain reaches a release with no waiter announced and the attempt
//     fires. If something intervenes, the fate follows the session's state — a definite store
//     failure on a resident-unbound session leaves the gap open for the periodic pass, a
//     rebind removes the session from this predicate (sess.c == nil) so the bound session's
//     own convergence events carry the verdicts, and an eviction ends local retry by design.
//
// sess.mu must be held.
func (sess *mqttSession) voidVerdictHandoffPendingLocked() bool {
	return !sess.seqUncertain && sess.c == nil && !sess.recordTxOpen &&
		sess.recordTxWaiters.Load() == 0 &&
		sess.voidedRegsRev > sess.voidedRegsSavedRev &&
		sess.voidedRegsRev > sess.voidedRegsHandoffRev
}

// voidVerdictHandoffDueLocked is what a lock RELEASE asks: the same pending condition, plus
// "nobody is already running the loop". An attempt's own releases — the fence it holds and
// the ownership gate it took — would otherwise re-enter this decision inside their own
// frames, recursing once per late staging and building a stack proportional to the session's
// outstanding register stores, each frame holding JetStream round trips. The loop in
// attemptVoidVerdictHandoff is the trampoline instead, and this conjunct is what routes work
// to it: it clears the flag only in the same critical section in which it finds nothing
// pending, so a staging that races the exit is either seen by the loop or fires its own
// trigger — never dropped between the two.
//
// sess.mu must be held.
func (sess *mqttSession) voidVerdictHandoffDueLocked() bool {
	return !sess.voidHandoffActive && sess.voidVerdictHandoffPendingLocked()
}

// attemptVoidVerdictHandoff persists the staged void verdicts of an unbound session, for a
// lock release that observed the gap.
//
// It runs UNDER THE OWNERSHIP GATE, taken fail-fast, and skipped when that fails. Both
// halves are load-bearing. Gated, any uncertainty this attempt creates exists only while the
// gate is held, so a CONNECT that ever holds the gate SEES it at its own uncertainty check
// and resolves it; ungated, an attempt whose store went ambiguous just after that check let
// the CONNECT bind, have its accept save refused, and evict the very session object holding
// the continuation. Skipping on contention loses nothing: the holder is an ownership-gated
// operation whose own release re-checks, and a CONNECT that wins resolves what it finds and
// then carries — and credits — the verdicts in its accept save.
//
// The gate is claimed as NOBODY (lockSession with a nil client), which is also what makes it
// refuse for a session that has since rebound, so the unbound scoping cannot go stale
// between the decision and the attempt.
//
// Successive revisions are ridden by a LOOP, not by recursion. Each iteration takes the gate,
// attempts, releases it, and only then asks whether more was staged meanwhile — so one frame
// serves however many late stagings arrive, and the releases inside it are suppressed by the
// active flag rather than re-entering here. Every property of a per-attempt handoff is kept:
// the gate is acquired and released per attempt, the watermark still advances only on fence
// acquisition, and no revision is coalesced away, because the loop re-reads the pending
// condition after each attempt.
//
// No lock held on entry, and asm.mu must NOT be held: this takes the ownership gate.
func (sess *mqttSession) attemptVoidVerdictHandoff(c *client) {
	asm := sess.asm
	if asm == nil {
		return
	}
	sess.mu.Lock()
	if sess.voidHandoffActive {
		// A loop is already running on another frame; it re-checks before it exits.
		sess.mu.Unlock()
		return
	}
	sess.voidHandoffActive = true
	sess.mu.Unlock()

	for {
		// The pending condition above is a PRE-GATE sample, and a CONNECT can bind (and
		// release the gate again) before the claim below — which is why the claim is made
		// as NOBODY rather than as the session's current owner: that is what refuses for a
		// session which has since rebound, and a bound session's tails own its convergence.
		sess.voidHandoffWindowHook(mqttHandoffWindowPreGate)
		// Contention ends the loop rather than spinning against the holder, whose own
		// release re-checks — and the ownership is given up INSIDE the gate's critical
		// section, which is what stops that release from suppressing itself against a loop
		// that is already leaving. See lockSessionForVoidHandoff.
		if err := asm.lockSessionForVoidHandoff(sess); err != nil {
			// Fired where a two-step hand-back would still be holding the ownership flag:
			// the fixed form has already given it back inside the gate's critical section,
			// so a gate release driven from here observes it CLEAR and fires its own loop.
			sess.voidHandoffWindowHook(mqttHandoffWindowGateRefused)
			return
		}
		sess.persistVoidedRegsFromRelease(c)
		asm.unlockSession(sess)
		sess.mu.Lock()
		if !sess.voidVerdictHandoffPendingLocked() {
			// Cleared in the SAME critical section that finds nothing pending, which is
			// what makes the handoff lossless: staging takes sess.mu, so it either landed
			// before this decision (and the loop continues) or after the flag is clear
			// (and its own release triggers a new loop).
			sess.voidHandoffActive = false
			sess.mu.Unlock()
			return
		}
		sess.mu.Unlock()
	}
}

// persistVoidedRegsFromRelease is persistVoidedRegs for the release handoff, differing in
// exactly one thing: the ATTEMPT is what advances the handoff watermark, and it advances it
// in the same critical section that acquires the fence.
//
// persistVoidedRegs cannot express this — it reports only "is the record known to carry the
// verdicts", so a refused fence and a failed store are the same answer — and the difference
// decides whether the handoff terminates or strands. A releaser that decided to attempt, and
// burned the watermark on that decision, can lose the fence to a waiter it just signalled;
// that waiter may make no relevant save, and the refused trigger stored nothing, so the
// nonce is never ridden by anything. Advancing only on acquisition makes the refused trigger
// a non-event, and the fence winner's own release the next re-check.
//
// Waiter priority adds one more way to be refused without acquiring: an ANNOUNCED blocking
// writer makes the pending predicate false, so this attempt returns without taking the fence
// and without burning the watermark — the same non-event arm, deferred to that waiter's own
// release. There is deliberately no pre-lock fast check here, unlike tryBeginRecordTx: this
// site runs one attempt per lock release rather than in a storm, and it must take sess.mu
// anyway for the predicate's non-atomic fields, so a pre-lock load would add a path without
// removing any class of mutex pressure. The cost of the deferral is that the staged verdict
// stays memory-only for longer, so a crash in the added interval loses it — the consequence
// already documented above for an unpersisted verdict. It is usually converged by the
// prioritized transaction itself, since any landed save marshals current memory, staged
// verdicts included.
//
// No lock held on entry.
func (sess *mqttSession) persistVoidedRegsFromRelease(c *client) {
	// Test seam, fired in the window the acquired-only watermark exists for: the releaser has
	// unlocked and signalled its waiters, and this attempt has not taken the fence yet. A
	// waiter stealing it here is the interleaving that strands a nonce if the watermark was
	// burned at the decision instead, and nothing outside can produce that window.
	if h := mqttTestVoidHandoffPreAttemptHookV.Load(); h != nil {
		(*h)(sess)
	}
	sess.mu.Lock()
	if !sess.voidVerdictHandoffPendingLocked() {
		// Re-validated under the gate: the state that justified the trigger is a
		// pre-gate sample, and a rebind, a resolution or another attempt may have
		// overtaken it. The PENDING form, because the loop that called us holds the
		// active flag by construction.
		sess.mu.Unlock()
		return
	}
	tok := sess.openRecordTxLocked()
	sess.voidedRegsHandoffRev = sess.voidedRegsRev
	sess.mu.Unlock()

	outcome, err := sess.saveClassified(tok)
	switch outcome {
	case saveLanded:
		// Credited by the save itself, along with everything else it marshalled.
	case saveMaybeLanded:
		// Ambiguity is not this mechanism's to retry: seqUncertain excludes the session
		// from the predicate, and the periodic pass resolves it (it walks every resident
		// uncertain session, expiry or none) by re-attempting these exact bytes, which
		// credits the revision they marshalled.
	case saveDefinitelyFailed:
		// The gap stays open with the watermark at it, so no self-retrigger: this class
		// keeps its existing posture — the next activity on the session persists it.
		mqttWarnf(c, "MQTT: could not persist the void-register verdicts for client %q at a lock release: %v",
			sess.id, err)
	}
	if terr := sess.endRecordTx(tok); terr != nil {
		mqttErrorf(c, "MQTT: record transaction not released after a void-verdict handoff for client %q: %v", sess.id, terr)
	}
}

// mqttTestVoidHandoffWindowHookV fires at the two windows of a handoff attempt that another
// goroutine can act in, named by the window: PRE-GATE, after the pending condition was
// sampled and before the gate is claimed — where a rebind makes that sample stale, which is
// why the claim is made as NOBODY — and GATE-REFUSED, where a two-step ownership hand-back
// would not yet have run. See attemptVoidVerdictHandoff.
var mqttTestVoidHandoffWindowHookV atomic.Pointer[func(sess *mqttSession, window string)]

const (
	mqttHandoffWindowPreGate     = "pre-gate"
	mqttHandoffWindowGateRefused = "gate-refused"
)

func setMQTTTestVoidHandoffWindowHook(h func(sess *mqttSession, window string)) func() {
	prev := mqttTestVoidHandoffWindowHookV.Load()
	if h == nil {
		mqttTestVoidHandoffWindowHookV.Store(nil)
	} else {
		mqttTestVoidHandoffWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestVoidHandoffWindowHookV.Store(prev) })
}

func (sess *mqttSession) voidHandoffWindowHook(window string) {
	if h := mqttTestVoidHandoffWindowHookV.Load(); h != nil {
		(*h)(sess, window)
	}
}

// mqttTestVoidHandoffPreAttemptHookV fires after a lock release decided a void-verdict handoff
// is owed and before the attempt takes the record fence. See persistVoidedRegsFromRelease.
var mqttTestVoidHandoffPreAttemptHookV atomic.Pointer[func(sess *mqttSession)]

func setMQTTTestVoidHandoffPreAttemptHook(h func(sess *mqttSession)) func() {
	prev := mqttTestVoidHandoffPreAttemptHookV.Load()
	if h == nil {
		mqttTestVoidHandoffPreAttemptHookV.Store(nil)
	} else {
		mqttTestVoidHandoffPreAttemptHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestVoidHandoffPreAttemptHookV.Store(prev) })
}

// deleteInflightRegisterFor removes the register of a pending entry a terminal path
// just took out of the tracking maps. Nil-safe, so every such path can call it
// without first asking whether there was an entry at all.
//
// A FOREIGN reservation is the one entry whose regSeq must never be settled here: it is a
// recovery reservation over a record this object may not interpret, and this delete
// carries no nonce or claim check — settling it through the completion path would destroy
// the evidence on the strength of the state recovery just refused.
//
// Session lock NOT held on entry: this issues a JetStream request.
// The slot reference is built from the entry, so this carries the phase gate. Past PUBREC
// the exchange's slot is its only durable claimant, and deleting it here would free an
// identifier the client still holds; only PUBCOMP's terminal path releases it.
//
// THE PAIR, IN ORDER: the register first and the unsent marker second (N-5.8), so no
// moment leaves a bare register that a resume would read as "registered, marker absent,
// therefore transmitted". The marker half runs only when the register half was proven —
// gone, preserved as another exchange's, or moved on — and never from the register
// delete's success result: it is operation "reap", with the marker sequence the entry
// CAPTURED (reapMarkerCaptured) or, for an entry that holds none, a mandatory RELOAD
// (reapMarkerNoneHeld). A marker-opaque reservation is refused like a foreign one: nothing
// here may settle an exchange whose transmission state is unknown.
//
// Reports whether the register half was proven; a false return issued no marker delete.
func (sess *mqttSession) deleteInflightRegisterFor(ack *mqttPending, d mqttInflightDisposition, lic mqttDestroyLicence) bool {
	if ack == nil || ack.regSeq == 0 || ack.regForeign || ack.markerOpaque || sess.asm == nil {
		return false
	}
	// The reference is the one CAPTURED on the entry, never rebuilt from the session's
	// current identity: rebuilding would address whatever the session is now rather than the
	// exchange this entry was about, losing the snapshot boundary that makes a stale
	// continuation detectable. The identifier comes from the reference's own name.
	var slot *mqttInflightSlotRef
	if ack.slot.slotOK() && ack.slot.name.subject != _EMPTY_ {
		ref := ack.slot
		slot = &ref
	}
	if slot == nil {
		// No captured identity, so nothing can be proven about the occupant. Fail closed
		// rather than fall back to a sequence-addressed delete — that is the addressing
		// F-034 forbids, and it is what a missing snapshot would silently reintroduce.
		return false
	}
	// Through the verdict property (F-087 WP3): this entry carries BOTH the sequence and
	// the exchange nonce, so a cancellation verdict naming the register it deletes is
	// released against the proof of that deletion. "Readable body" was never the
	// boundary — this caller reads no body and still holds the identity — and a
	// fire-and-forget delete here strands a verdict with no crash at all: a teardown
	// records the nonce, both durable rungs fail, and a parked acknowledgement's later
	// delete lands once the storage fault clears.
	// CONFIRMED, not fire-and-forget, because the marker reap below READS this register's
	// subject: an unconfirmed delete leaves the record still present at the sibling read,
	// which classifies it as a live same-exchange sibling and PRESERVES the marker (row 26)
	// — stranding a transmitted exchange's marker and re-offering it as unsent. The reap's
	// precondition is that the register half is already GONE, so it has to be proven gone
	// here. A delete that cannot be confirmed reaps no marker: both halves are left to the
	// sweep rather than acted on without proof.
	if !sess.reapRegisterWithVerdictGated(ack.regSeq, ack.regNonce, true, slot, d, lic) {
		return false
	}
	var c *client
	if sess.jsa != nil {
		c = sess.jsa.c
	}
	switch {
	case ack.markerSeq != 0:
		sess.asm.reapMarkerCaptured(c, mqttMarkerCallerCleanup, slot.name.subject, ack.regNonce, ack.markerSeq)
	case ack.transmitted:
		sess.asm.reapMarkerNoneHeld(c, mqttMarkerCallerCleanup, slot.name.subject)
	default:
		// Neither transmitted nor holding a marker: a shape beginSendLocked never
		// authorizes. Nothing is inferred from it; the sweep meets whatever is there.
		mqttWarnf(c, "MQTT: the entry for packet identifier %d of client %q held no marker and was never transmitted; its marker, if any, is left to the sweep",
			slot.name.pi, sess.id)
	}
	return true
}

// releaseTransmittedIdentifier installs ordinary disposal ownership before returning
// to the callback. Store I/O, retries and source completion belong to that owner.
// Shared/affine and drain-owned deliveries retain their existing disposal.
func (sess *mqttSession) releaseTransmittedIdentifier(c *client, jsDur, reply string) mqttAbandonResult {
	if jsDur == _EMPTY_ || reply == _EMPTY_ {
		return mqttAbandonNone
	}
	sseq, _, _, _, _ := ackReplyInfo(reply)
	sess.mu.Lock()
	if sess.offerDisposalLocked(jsDur, reply, sseq) {
		var owner *mqttDisposal
		for w := range sess.disposalsBySource[mqttDeferredKey{jsDur, sseq}] {
			owner = w
			break
		}
		sess.mu.Unlock()
		if owner != nil {
			return owner.runNow()
		}
		return mqttAbandonPending
	}
	var removed *mqttPending
	pi := sess.cpending[jsDur][sseq]
	if cur := sess.pendingPublish[pi]; pi != 0 && cur != nil && sess.pendingPubRel[pi] == nil &&
		!cur.regPending && !cur.regForeign && !cur.markerOpaque {
		st := sess.drainSeqLocked(jsDur, sseq)
		drainOwned := st != nil && (st.claimed || st.unclaimUnconfirmed)
		if !drainOwned && !mqttDurIsSlotOnlyClass(jsDur) && cur.ev.lin != _EMPTY_ {
			if c == nil {
				sess.mu.Unlock()
				return mqttAbandonPending
			}
			ent, ok := c.outboundEntitlement()
			if !ok {
				sess.mu.Unlock()
				return mqttAbandonPending
			}
			w := sess.installDisposalLocked(c, mqttDenyEvidence{
				Gen: sess.gen, Tenure: ent.Tenure, Subject: sess.inflightSubjectLocked(pi),
				Dur: jsDur, Seq: sseq, Nonce: cur.regNonce, Lin: cur.ev.lin, PI: pi,
				Reason: mqttAbandonReasonACLDeny, Reply: reply,
			})
			sess.mu.Unlock()
			if w == nil {
				return mqttAbandonPending
			}
			return w.runNow()
		}
		if !sess.condemnSendLocked(pi, mqttSendSettleDenyRelease) {
			sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), cur.regNonce)
			// UNANSWERED (F-088 WP2): a TRANSMITTED drain-owned entry gets its AckRoute here, so the
			// client's late acknowledgement completes the exchange; the door below preserves its
			// register (it is possibly-attempted, so no destruction is licensed).
			removed = sess.untrackPublishEntry(pi, mqttUntrackUnanswered)
		}
	}
	sess.mu.Unlock()
	sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttLicenceOf(c))
	return mqttAbandonNone
}

// settleAbandonedDeliveryLocked applies a durable abandonment to memory: the EXACT resident
// entry and mapping go (same identifier AND same allocation — a successor's entry is left
// untouched), and the source is suppressed so every re-offer of it is acknowledged away rather
// than delivered, whatever a later authorization says. The suppression carries no register
// sequence: source settlement cannot reap an unanswered abandoned register. A later bound
// MQTT ACK can advance it; C4 owns source settlement and terminal suppression retention.
//
// Lock held on entry.
func (sess *mqttSession) settleAbandonedDeliveryLocked(ev mqttDenyEvidence, rec *mqttInflightRecord) bool {
	if sess.gen != ev.Gen || sess.asm == nil || rec == nil ||
		rec.PI != ev.PI || rec.Nonce != ev.Nonce || rec.Lin != ev.Lin ||
		rec.Dur != ev.Dur || rec.Seq != ev.Seq || rec.Payload != ev.Payload {
		return false
	}
	if cur := sess.pendingPublish[ev.PI]; cur != nil &&
		(cur.regNonce != ev.Nonce || cur.ev.lin != ev.Lin || cur.jsDur != ev.Dur ||
			cur.sseq != ev.Seq || cur.qos != rec.QoS) {
		return false
	}
	subject := mqttInflightSubject(sess.idHash, ev.Gen, ev.PI)
	binding := newMQTTAbandonedAckBinding(sess, sess.asm.sessStreamEpoch, subject, rec)
	if !sess.installAbandonedAckLocked(binding) {
		return false
	}
	// The ACK route exists before either scheduling link disappears, under the same lock.
	if sess.testAbandonedAckInstalled != nil {
		sess.testAbandonedAckInstalled()
	}
	if cur := sess.pendingPublish[ev.PI]; cur != nil && cur.regNonce == ev.Nonce &&
		cur.ev.lin == ev.Lin && cur.jsDur == ev.Dur && cur.sseq == ev.Seq && cur.qos == rec.QoS {
		delete(sess.pendingPublish, ev.PI)
		if sess.cpending[ev.Dur][ev.Seq] == ev.PI {
			delete(sess.cpending[ev.Dur], ev.Seq)
		}
	}
	// A SHARED source is handed back, never retired (7a): retirement is the suppression that
	// ACKs every re-offer away, which for a group message consumes it on behalf of the members who
	// are allowed it. The held binding is what stands in its place (outbound_source_ack_gate.go).
	// A RETAINED delivery has no source to retire (F-105 7c).
	if ev.Payload == _EMPTY_ && !mqttIsSharedDurable(ev.Dur) {
		sess.retireSourceLocked(ev.Dur, ev.Seq, &mqttRetiredSource{pi: ev.PI, nonce: ev.Nonce, durable: true})
	}
	return true
}

// markInflightRegisterRetired rewrites a register as RETIRED, in place, by a CAS at its
// exact sequence, and reports the sequence it now occupies.
//
// This is what makes a retirement survive a restart. The in-memory entry covers the
// current process; without this record the next resume sees an ordinary PUBLISH-phase
// register over a live source and re-sends a payload the client has already completed.
// Same nonce, so it stays the same record; the CAS is at the exact sequence, so a
// register a peer has since replaced is left alone (R4).
//
// Bounded and reconciled like every other CAS here rather than attempted once: a
// rejection under clustered contention (10164) is transient and says nothing about the
// subject, so it is resolved by re-reading. A record that ALREADY carries our nonce and
// the retirement is treated as ours — that is the ambiguous-write case, and its response
// being lost must not be read as a failure.
//
// A false return is not the end of the matter. Its caller does not shrug: the exchange's
// PUBREL record is still durable evidence that the source is past PUBREC, so the PUBREL
// is not acknowledged away until this has succeeded (see mqttProcessPubComp).
//
// Session lock NOT held on entry.
func (sess *mqttSession) markInflightRegisterRetired(c *client, pi uint16, dur string, sseq, regSeq uint64,
	nonce, q2AffClaim, q2AffCopy string) (uint64, bool) {
	as := sess.asm
	if as == nil || pi == 0 || regSeq == 0 || nonce == _EMPTY_ {
		return 0, false
	}
	sess.mu.Lock()
	idHash, gen, regClaim, regClaimSeq := sess.idHash, sess.gen, sess.regClaim, sess.seq
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return 0, false
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	// The mark is applied by LOADING the slot and setting one field, never by rebuilding
	// the record from these arguments. Rebuilding was safe only while a register held
	// nothing but the delivery's identity; now it would drop the exchange's phase, its
	// canonical PUBREL identity and any live action token — erasing a claim this caller
	// does not own. casInflightSlot also applies the phase gate, which permits this mark
	// from any supported state — "retired" is a fact about the SOURCE, not about the slot's
	// phase, and the tree records it on a PUBLISH slot today.
	name, perr := mqttParseInflightSubject(subject)
	if perr != nil {
		return 0, false
	}
	// The reference names the state the slot is in NOW, which is `publish`: the retired
	// mark is a fact about the SOURCE and the pre-existing design records it while the
	// register is still in its PUBLISH shape. An earlier revision named a past-PUBREC state
	// here, describing where the design is going rather than where the slot is, which the
	// gate would now refuse outright.
	ref := mqttInflightSlotRef{
		name: *name, seq: regSeq, nonce: nonce,
		state: mqttInflightPublish, dur: dur, sseq: sseq,
		claim: regClaim, claimSeq: regClaimSeq,
	}
	// ONE path, and no fallback. The mark is applied by loading the slot and setting one
	// field; there is deliberately no rebuild-from-arguments arm any more.
	//
	// The removed fallback was actively harmful once the slot carried a phase: it wrote a
	// record reconstructed from these few arguments, which for a past-PUBREC state means a
	// PUBREL phase with NO canonical record identity — a shape the validator refuses, so
	// the mark appeared to land and then read back as unreadable.
	//
	// A refusal here is not a failure to handle: the caller's contract keeps the exchange's
	// PUBREL, which is durable evidence that the source is past PUBREC, so nothing is lost
	// and the next pass retries. The phase gate itself permits this source fact from every
	// supported state; identity, shape or CAS movement can still refuse the rewrite.
	// No expected predecessor: the retirement mark is legal from any known state, because
	// it records a fact about the SOURCE rather than the slot's phase.
	seq, ok := sess.casInflightSlot(c, ref, _EMPTY_, mqttSlotMarkRetired, func(next *mqttInflightRecord) {
		next.Retired = true
	})
	if !ok {
		return 0, false
	}
	return seq, true
}

// resolveRetiredSource drops a retirement whose source is PROVABLY complete, and reports
// whether it did.
//
// A retirement is created because a source acknowledgement could not be CONFIRMED — but
// an unproven acknowledgement is ambiguous, and the likeliest case is that it landed and
// only its confirmation was lost. Then nothing will ever re-offer the source, so nothing
// will ever settle the retirement: it and its register would persist for the life of the
// session, be re-established from the register by every later resume, and accumulate one
// entry per such failure. Bounded by failures rather than traffic, but never converging,
// which is not good enough.
//
// So the ambiguity is resolved the same way an ambiguous register write is — by READING
// the authoritative state. A source that is acknowledged, or authoritatively absent while
// its consumer still holds interest, has nothing left to suppress. Unknown or live keeps
// the retirement, which is the safe direction.
//
// Session lock NOT held on entry.
func (sess *mqttSession) resolveRetiredSource(c *client, key mqttDeferredKey) bool {
	as := sess.asm
	if as == nil {
		return false
	}
	sess.mu.Lock()
	rs := sess.srcAckOwed[key]
	sess.mu.Unlock()
	if rs == nil {
		return true // already settled
	}
	if as.inflightSourceState(&mqttInflightRecord{Dur: key.jsDur, Seq: key.sseq}) != mqttInflightSourceComplete {
		return false
	}
	sess.mu.Lock()
	cur := sess.srcAckOwed[key]
	if cur != rs {
		sess.mu.Unlock()
		return false
	}
	sess.wakeRetainedTerminalCleanupLocked(cur.nonce, key.jsDur, key.sseq) // before the owner leaves
	delete(sess.srcAckOwed, key)
	sess.mu.Unlock()
	if cur != nil && cur.regSeq != 0 {
		// A retirement settle owes any attached cancellation verdict its proof (F-087
		// WP3): the Retired-dominates precedence routes a record carrying both here.
		//
		// PHASE-GATED (census C05b). Settling the source is this path's business; the
		// exchange's slot is not, and past PUBREC it is the identifier's only claimant.
		// The reference is built from the retirement itself, which carries the identifier
		// and the exchange nonce for exactly this kind of exact addressing.
		var slot *mqttInflightSlotRef
		sess.mu.Lock()
		idHash, gen := sess.idHash, sess.gen
		sess.mu.Unlock()
		if idHash != _EMPTY_ && gen != _EMPTY_ && cur.pi != 0 {
			if nm, perr := mqttParseInflightSubject(mqttInflightSubject(idHash, gen, cur.pi)); perr == nil {
				slot = &mqttInflightSlotRef{
					name: *nm, seq: cur.regSeq, nonce: cur.nonce,
					state: mqttInflightPublish, dur: key.jsDur, sseq: key.sseq,
				}
			}
		}
		// No licence: a retired register is past PUBREC, never an unsent obligation.
		sess.reapRegisterWithVerdictGated(cur.regSeq, cur.nonce, false, slot, mqttSlotDeleteComplete, mqttDestroyLicence{})
	}
	c.Debugf("MQTT: retirement of QoS 2 source %s/%d for client %q resolved: the acknowledgement had landed after all",
		key.jsDur, key.sseq, sess.id)
	return true
}

// mqttRegCancelOutcome names WHICH FACT licensed a cancellation attempt's result.
//
// It exists because the bare error this replaced collapsed five different endings into one
// `nil`, and both callers then answered that nil with the same comment — "the tombstone is
// on the register itself, which is strictly stronger than a nonce recorded beside it". That
// is true of exactly two of them. For the other three there is no tombstone anywhere, and
// whatever licenses retiring the nonce verdict is a different fact each time. A guard's
// claim must match its reach, so the reach is named here rather than asserted there.
//
// The two questions the outcomes answer are deliberately SEPARATE, because they diverge: is
// this nonce's verdict retirable, and is the IDENTIFIER releasable. A foreign successor
// proves our exchange is gone while its identifier is not ours to free, so one answer may
// never stand in for the other. NO outcome releases an identifier: that is unit A's, and it
// takes a captured exclusion instance and an all-bind absence proof this function has
// neither of.
type mqttRegCancelOutcome uint8

const (
	// mqttRegCancelRefused is the ZERO VALUE, so a new arm that forgets to classify itself
	// fails closed rather than inheriting a licence.
	mqttRegCancelRefused mqttRegCancelOutcome = iota
	// mqttRegCancelStored: the CAS landed. The tombstone IS on the record, and it survives
	// a restart, which is what makes it strictly stronger than the nonce verdict.
	mqttRegCancelStored
	// mqttRegCancelAlreadySet: cur.Cancelled, ours. The same tombstone, written by an
	// earlier attempt or by the store's own fallback.
	mqttRegCancelAlreadySet
	// mqttRegCancelAbsent: the subject read came back empty. The register is GONE, so the
	// verdict describes nothing and retiring it loses no evidence. It does NOT license a
	// lift here — this absence was observed without a captured instance, so unit A's pass
	// must observe it again under the protocol before the identifier can be freed.
	mqttRegCancelAbsent
	// mqttRegCancelSuperseded: a readable foreign exchange holds the subject. matchesRecord
	// compares packet identifier, exchange nonce and source delivery, so a mismatch is a
	// DIFFERENT exchange and ours is provably gone. The successor's identifier is not ours.
	mqttRegCancelSuperseded
)

// mqttRegCancelRetiresVerdict reports whether an outcome licenses RETIRING the nonce
// verdict — the first of the two questions the outcomes keep apart.
//
// Refused is absent because it is the zero value: an arm added without classifying itself
// retires nothing.
func mqttRegCancelRetiresVerdict(o mqttRegCancelOutcome) bool {
	switch o {
	case mqttRegCancelStored, mqttRegCancelAlreadySet, mqttRegCancelAbsent, mqttRegCancelSuperseded:
		return true
	}
	return false
}

// markInflightRegisterCancelled rewrites a PUBLISH-phase register as CANCELLED in place.
//
// Cancellation is the fact THIS path relies on to let a later resume free the identifier,
// so a cached sequence is never sufficient authority for the write. It is not the only
// such fact — an unsent MARKER still naming a register whose source can never be
// re-offered proves the same thing by different evidence (F-102) — but a cancelled
// allocation is exactly the shape where the marker has been consumed, which is why the
// tombstone is written rather than relied upon. Each attempt first loads the
// subject, proves that its readable occupant is this exact exchange and that it is still in
// PUBLISH, carries the whole record forward, and CASes at the sequence that load returned.
// A record that crossed PUBREC between teardown and this call is therefore preserved even
// when a stale pendingPublish entry still remembers its old sequence.
//
// Session lock NOT held on entry.
func (sess *mqttSession) markInflightRegisterCancelled(c *client, pi uint16, jsDur string, sseq, regSeq uint64, nonce string,
	lic mqttDestroyLicence) (mqttRegCancelOutcome, error) {
	as := sess.asm
	if as == nil || pi == 0 || regSeq == 0 || nonce == _EMPTY_ {
		return mqttRegCancelRefused, fmt.Errorf("no register identity to mark cancelled")
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return mqttRegCancelRefused, fmt.Errorf("session has no incarnation identity")
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	nm, err := mqttParseInflightSubject(subject)
	if err != nil {
		return mqttRegCancelRefused, fmt.Errorf("invalid in-flight register subject %q: %w", subject, err)
	}
	ref := mqttInflightSlotRef{
		name: *nm, seq: regSeq, nonce: nonce, state: mqttInflightPublish,
		dur: jsDur, sseq: sseq,
	}
	var lastErr error
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		switch {
		case smsg == nil:
			// GONE: the delete landed after all, which is the better outcome. The verdict
			// describes a register that no longer exists, so retiring it loses nothing —
			// but the identifier is NOT freed on this read (see the outcome's comment).
			return mqttRegCancelAbsent, nil
		case cur == nil:
			// PRESENT BUT UNREADABLE — loadInflightRecord's second outcome, which is a
			// definite fact about the record and not an indeterminate read.
			//
			// The `default` arm below concludes "our register is gone" and reports
			// SUCCESS. For a FOREIGN readable record that is sound: something else
			// demonstrably occupies the subject. For an unreadable one it is a positive
			// claim drawn from a read that failed, and its cost is real — the caller
			// forgets the nonce verdict and goes on to destroy the consumer, so nothing
			// anywhere records that this delivery was never transmitted, while the record
			// still occupies the identifier. Review of F-034 found this arm reporting
			// success; the release and the store already fail closed here.
			return mqttRegCancelRefused, fmt.Errorf("an unreadable record occupies %q at sequence %d", subject, smsg.Sequence)
		case !mqttInflightRecordMatchesSubject(cur, subject):
			// Unreadable by recovery's own rule, and the arms below would either claim
			// this cancellation resolved or overwrite the record to make it so.
			//
			// An ERROR rather than the `default` arm's nil, and deliberately BEFORE the
			// nonce is consulted. `default` concludes "our register is gone" from a
			// foreign record, which is sound only for a record that can be READ — and a
			// body naming another identifier is exactly what the reader refuses to
			// interpret, whosever nonce it carries. Concluding anything positive from it
			// would be a claim built on state this broker cannot interpret.
			//
			// Failing closed costs nothing here: both callers answer an error by
			// recording the nonce verdict on the session record, which is the same fact
			// this write was for, on a rung this subject cannot corrupt.
			return mqttRegCancelRefused, fmt.Errorf("packet identifier %d is held by a record naming another identifier on %q", pi, subject)
		case !ref.matchesRecord(cur):
			// A readable foreign exchange now holds this identifier. The remembered
			// register is gone, and the current occupant must be preserved.
			return mqttRegCancelSuperseded, nil
		case cur.Cancelled:
			return mqttRegCancelAlreadySet, nil // ours, already marked
		case !mqttInflightTransitionAllowed(mqttSlotMarkCancelled, cur.State):
			// The exchange crossed PUBREC. "Never transmitted" is now false, so retain
			// the whole-life owner and make the caller persist its weaker nonce verdict.
			return mqttRegCancelRefused, fmt.Errorf("in-flight register %q is in state %q and cannot be marked never-transmitted", subject, cur.State)
		}

		// AN ORDINARY OBLIGATION WITH WHOLE-EXCHANGE EVIDENCE IS ENDED, NOT ONLY TOMBSTONED: the
		// withdrawal writes this same tombstone and moves the exchange to WithdrawnUnsent in one
		// batch. Without the evidence the standalone tombstone below is written as before, and
		// the exchange is left where it is.
		switch sess.withdrawIfEvidenced(gen, subject, cur, lic) {
		case mqttWithdrawLanded:
			return mqttRegCancelStored, nil
		case mqttWithdrawUnlicensed:
			// Not this connection's decision any more (F-105 7b): no fallback tombstone.
			return mqttRegCancelRefused, fmt.Errorf("cancelling the obligation on %q: %w", subject, errMQTTEntitlementEnded)
		case mqttWithdrawUnresolved:
			// THE WITHDRAWAL IS OWED (F-088, WP3 "withdrawOwed lifecycle"): its landing is unknown,
			// so nothing is concluded here, and the custody dispatcher retries exactly this
			// obligation's withdrawal under the deciding licence. Every caller of this function —
			// the confirmation's cancelled arm, the teardown's register ladder, the securing pass —
			// reaches this exit, so it is recorded once, here.
			sess.recordWithdrawOwed(gen, subject, cur.Nonce, cur.Lin, smsg.Sequence, lic)
			lastErr = fmt.Errorf("the withdrawal of the obligation on %q may or may not have landed", subject)
			return mqttRegCancelRefused, lastErr
		case mqttWithdrawNoEvidence, mqttWithdrawNeverLanded:
			continue // re-read: the register moved, or the evidence changed under the attempt
		}

		// Carry every field the current owner wrote and mutate only this writer's fact.
		// In particular, Retired and custody survive a concurrent source settlement.
		next := *cur
		// A CAS over the occupant: the cancellation is an OCCUPANT cancellation (F-088, consult
		// C4), and can never mint a pending fence, whatever the occupant's body looks like.
		next.Cancelled, next.CancelOrigin = true, mqttCancelOriginOccupant
		serr := sess.storeCancellationTombstone(subject, smsg.Sequence, &next, lic)
		if serr == nil {
			return mqttRegCancelStored, nil
		}
		if mqttTombstoneRefusedTerminally(serr) {
			return mqttRegCancelRefused, serr
		}
		lastErr = serr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("could not mark in-flight register %q cancelled", subject)
	}
	return mqttRegCancelRefused, lastErr
}

// secureRetiredSource makes a retirement durable, and reports whether it now is.
//
// Called both where the retirement is created and again at PUBCOMP, because PUBCOMP is
// about to destroy the other evidence. While the whole-life slot is durably past PUBREC,
// recovery can infer source suppression from its phase; once terminal cleanup removes
// that slot and its PUBREL records, an unresolved retirement needs its own durable mark.
//
// Session lock NOT held on entry.
func (sess *mqttSession) secureRetiredSource(c *client, key mqttDeferredKey) bool {
	sess.mu.Lock()
	rs := sess.srcAckOwed[key]
	sess.mu.Unlock()
	if rs == nil || rs.durable {
		return true // nothing owed, or already durable
	}
	seq, ok := sess.markInflightRegisterRetired(c, rs.pi, key.jsDur, key.sseq, rs.regSeq, rs.nonce, rs.q2AffClaim, rs.q2AffCopy)
	if !ok {
		return false
	}
	sess.mu.Lock()
	// Only while the retirement is still outstanding: a redelivery may have settled it
	// meanwhile, and reviving it would resurrect a completed retirement.
	if cur := sess.srcAckOwed[key]; cur != nil {
		cur.durable = true
		if seq != 0 {
			cur.regSeq = seq
		}
	}
	sess.mu.Unlock()
	return true
}

// confirmInflightSourceAck acknowledges the source delivery of a QoS 2 exchange that
// has moved into its PUBREL phase, WITH confirmation, honoring an instance-scoped test
// seam. Routed separately from every other ack so a test can fail this one alone.
func (sess *mqttSession) confirmInflightSourceAck(ackSubject string) error {
	return sess.confirmInflightSourceAckFor(ackSubject, nil)
}

// confirmInflightSourceAckFor is confirmInflightSourceAck for a continuation whose ownership the
// gate revalidates at submission (owner nil for a callback).
func (sess *mqttSession) confirmInflightSourceAckFor(ackSubject string, owner mqttSourceAckOwner) error {
	if sess.disposalBlocksReply(ackSubject) {
		return fmt.Errorf("source settlement awaits disposal classification")
	}
	as := sess.asm
	var hook func(string) error
	if as != nil {
		as.mu.RLock()
		hook = as.testInflightSourceAck
		as.mu.RUnlock()
	}
	if hook != nil {
		// The seam stands in for JetStream's confirmation only: the HandBack check still runs
		// first, so a test cannot observe a positive acknowledgement the gate would refuse.
		sess.mu.Lock()
		stale := owner != nil && !owner()
		held := !stale && sess.handBackRefusesLocked(ackSubject)
		sess.mu.Unlock()
		if stale {
			return fmt.Errorf("the continuation acknowledging %q is no longer the installed owner", ackSubject)
		}
		if held {
			return errMQTTSourceHandedBack
		}
		mqttObserveSourceAck(ackSubject, true)
		return hook(ackSubject)
	}
	r, err := sess.submitSourceAck(ackSubject, true, owner)
	if err == nil && !r.positive() {
		err = fmt.Errorf("the acknowledgement of %q was not submitted (result %d)", ackSubject, r)
	}
	return err
}

// sharedRecordUnsupported reports whether a recovered shared register is a shape this broker does
// not write: not QoS 1, retired, or not beside an exchange that states exactly its dependency (a
// fresh point read; an unreadable exchange is not a supported pair either).
//
// Session lock NOT held on entry.
func (sess *mqttSession) sharedRecordUnsupported(as *mqttAccountSessionManager, gen string, rec *mqttInflightRecord) bool {
	if rec == nil || rec.QoS != 1 || rec.Retired {
		return true
	}
	if as == nil {
		return true
	}
	as.mu.RLock()
	hook := as.testSharedExchangeLoad
	as.mu.RUnlock()
	var x *mqttOutboundExchange
	var xerr error
	switch {
	case hook != nil:
		x, xerr = hook(gen, rec.Lin)
	case sess.jsa != nil:
		x, _, xerr = sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, gen, rec.Lin)
	default:
		return true
	}
	return xerr != nil || x == nil || mqttCheckSharedAcqPair(rec, x) != nil
}

// recoveredPublishPairRefused applies the pair rule to one recovered PUBLISH-phase register about to
// be installed as a delivery. A shared register's exchange is read fresh; any other register is
// judged against the census observation, if one was taken.
//
// Session lock NOT held on entry.
func (sess *mqttSession) recoveredPublishPairRefused(as *mqttAccountSessionManager, gen string,
	rec *mqttInflightRecord, exchanges map[string]*mqttOutboundExchange) bool {

	if rec == nil || rec.State != mqttInflightPublish {
		return false
	}
	if mqttIsSharedDurable(rec.Dur) {
		// Refused before the walk interprets it (sharedRecordUnsupported): a shared register that
		// reaches here was already read beside its exact exchange.
		return false
	}
	x := exchanges[rec.Lin]
	return x != nil && mqttCheckSharedAcqPair(rec, x) != nil
}

// mqttVoidedReg is one register a consumer teardown gave up, in the form the cleanup ladder
// needs: the exact sequence to delete, the nonce that names the verdict, and the identity
// fields a tombstone rewrite requires.
type mqttVoidedReg struct {
	seq   uint64
	nonce string
	pi    uint16
	dur   string
	sseq  uint64
}

// deleteInflightRegisters removes a teardown's registers at their exact sequences, walking
// the same durable ladder everywhere: (1) confirmed delete, (2) Cancelled tombstone at the
// exact sequence, (3) the nonce verdict persisted with the session record.
//
// Confirmed rather than fire-and-forget, unlike the ordinary terminal deletes, and the
// difference is what happens next: a terminal path leaves behind a record whose source stays
// live and classifiable, so the store's reconcile, the next resume or the orphan sweep will
// remove it. These registers come from a consumer TEARDOWN, and the consumer that would
// classify their sources is about to disappear — after which a surviving register is
// deliberately not read as complete, so a single lost delete would reserve an identifier for
// the life of the session. Under a Receive Maximum of 1 that is the session.
//
// A proven rung ends the entry's story: a deleted register needs no verdict, a tombstoned
// one IS its own verdict, and either way the nonce is forgotten so the record's verdict set
// stays bounded across a client that subscribes and unsubscribes forever. Only when both
// register-stream rungs fail — one storage domain, so they usually fail together — does the
// session record carry the verdict, kept until a later scan proves the register gone.
//
// tok is the caller's open transaction (zero where there is none). Callers run this BEFORE
// the consumer delete.
//
// Session lock NOT held on entry: each of these issues JetStream requests.
func (sess *mqttSession) deleteInflightRegisters(c *client, tok recordTx, voided []mqttVoidedReg, lic mqttDestroyLicence) {
	_ = sess.deleteInflightRegistersClassified(c, tok, voided, lic)
}

// deleteInflightRegistersClassified is deleteInflightRegisters reporting whether its record
// write was rejected by the per-subject CAS. Same reason as
// secureRegCancellationsClassified: this is the SECOND chain that reaches proveVoidedRegs,
// and a rule that covered only the first would leave the hole open through this one.
func (sess *mqttSession) deleteInflightRegistersClassified(c *client, tok recordTx, voided []mqttVoidedReg, lic mqttDestroyLicence) bool {
	if !sess.voidRegistersOnStream(c, voided, lic) {
		return false
	}
	proven, casRejected := sess.proveVoidedRegs(c, tok)
	if !proven {
		mqttErrorf(c, "MQTT: the void-register verdict(s) of client %q are not durably recorded yet; "+
			"the next convergence point retries", sess.id)
	}
	return casRejected
}

// voidRegistersOnStream is deleteInflightRegisters' ladder over the REGISTER STREAM
// alone — the confirmed delete and the tombstone — reporting whether any register got
// neither, so its nonce is staged in memory and the SESSION RECORD still owes the
// verdict.
//
// Split out from the wrapper because who writes the record is the caller's business,
// and one caller must write it itself: the deferred close folds the verdict into the
// same classified save that persists its disconnect time, and a save of our own there
// would leave that one refused. See applyDeferredDisconnect.
//
// Session lock NOT held on entry: each of these issues JetStream requests.
func (sess *mqttSession) voidRegistersOnStream(c *client, voided []mqttVoidedReg, lic mqttDestroyLicence) bool {
	if len(voided) == 0 || sess.asm == nil {
		return false
	}
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	unproven := false
	for _, v := range voided {
		// The first rung is phase-gated too. A recovered PUBLISH entry can coexist with
		// a slot that already crossed PUBREC until F-093's ordered handoff runs; deleting
		// its remembered sequence directly would erase the whole-life owner underneath a
		// live PUBREL action. The reference carries the teardown's exact exchange identity,
		// and the helper reloads the subject before deciding whether PUBLISH completion is
		// still a permitted disposition.
		var slot *mqttInflightSlotRef
		if idHash != _EMPTY_ && gen != _EMPTY_ && v.pi != 0 {
			if nm, perr := mqttParseInflightSubject(mqttInflightSubject(idHash, gen, v.pi)); perr == nil {
				slot = &mqttInflightSlotRef{
					name: *nm, seq: v.seq, nonce: v.nonce, state: mqttInflightPublish,
					dur: v.dur, sseq: v.sseq,
				}
			}
		}
		if sess.reapRegisterWithVerdictGated(v.seq, v.nonce, true, slot, mqttSlotDeleteNeverTransmitted, lic) {
			// The register half is proven; the marker half follows (N-5.8), capture first:
			// a never-transmitted pair's marker is present by construction, and it is
			// deleted only if its sibling is now authoritatively absent (row 24).
			if slot != nil {
				sess.asm.reapMarkerCaptureFirst(c, mqttMarkerCallerCleanup, slot.name.subject, false)
			}
			continue
		}
		if v.nonce != _EMPTY_ && v.pi != 0 {
			if outcome, merr := sess.markInflightRegisterCancelled(c, v.pi, v.dur, v.sseq, v.seq, v.nonce, lic); merr == nil {
				// RETIRED ON THE OUTCOME'S OWN LICENCE, not on a blanket "the tombstone
				// is on the register itself" — that is true of Stored and AlreadySet and
				// of nothing else here. Absent retires because the register is gone and
				// the verdict describes nothing; Superseded because a readable foreign
				// exchange proves ours is gone.
				if mqttRegCancelRetiresVerdict(outcome) {
					sess.forgetCancelledReg(v.nonce)
				}
				continue
			}
		}
		mqttWarnf(c, "MQTT: deferring the removal of in-flight register %d for client %q", v.seq, sess.id)
		sess.mu.Lock()
		staged := sess.stageCancellationVerdictLocked(lic, v.nonce)
		sess.mu.Unlock()
		if staged {
			unproven = true
		}
	}
	return unproven
}

// voidRegistrationsLocked marks every in-flight register of one consumer durable VOID, because
// this session is abandoning that consumer, and reports the withdrawn allocations for securing.
//
// ABANDONING is the precondition, and it is why a QoS 0 downgrade that will DRAIN its
// consumer instead takes cancelPendingRegistrationsLocked alone: those exchanges are not
// abandoned, they are still owed.
//
// This is the STAGING half of a consumer teardown, and where it is called from is the whole
// point: inside the transaction that removes the consumer's membership from the record, so
// that the write saying "this subscription is gone" is the same write saying "this session
// has abandoned these exchanges and will never complete them". The verdict is an
// ABANDONMENT, not a transmission claim: whether a PUBLISH ever reached the client is the
// durable pair's to say — the register's unsent marker (inflight_marker.go) — and a verdict
// on a transmitted exchange is exactly the known-false "never transmitted" reading F-088
// owns. Nothing else can close the membership gap. Keeping the consumer
// when the cancellation cannot be recorded is not equivalent — the membership has already
// gone, so there is no subscription left to redeliver through, and the identifier is stuck
// whether or not the durable survives.
//
// Only a PROVEN-UNSENT register is voided here. That is the whole of F-101's change to this
// function: the second arm used to void every confirmed allocation of the durable, including
// the possibly-attempted ones its own comment called transmitted, and a void verdict is a
// durable "never transmitted" claim that recovery acts on by freeing the identifier.
//
// Two kinds of register are voided here and the difference is only in what else has to
// happen. An allocation whose store is still IN FLIGHT is withdrawn exactly as stageDrain
// withdraws one — the tracking goes, a reservation holds the identifier until the store
// settles, and a withheld marker keeps the eventual first transmission at DUP=0 if this packet
// rolls back ([MQTT-3.3.1-1]) — and its cancellation is returned so a tombstone can be written
// promptly. A TRANSMITTED delivery's register needs none of that: its entry is left for the
// teardown's REAP to remove, and only the verdict is recorded. Since F-034 that removal is
// no longer unconditional — the reap spares records above its enumeration watermark and
// refuses outright on an unreadable one — so what is guaranteed here is that nothing is
// destroyed by leaving it, not that the teardown will certainly free it.
//
// The verdict is keyed by the register's NONCE, and that is the whole point. A durable-scoped
// verdict cannot work: a shared group durable's name recurs for as long as its incarnation
// stays live, so a rejoin can return the same name, and no amount of withdrawing-and-restoring makes "every register on
// this durable is void" mean the right thing across that boundary — it either resurrects an
// abandoned register or destroys a genuine new one. A nonce is minted with its allocation and
// never reused, so the verdict cannot be misapplied however often the durable returns.
//
// The verdicts it records are returned so a rollback can withdraw them, and that asymmetry
// is deliberate: a rolled-back packet keeps its subscription, so those deliveries are still
// live and still owe their identifiers. Withdrawing the verdict is also what restores their
// send admission, since beginSendLocked refuses a register under one (F-101).
//
// A CANCELLED allocation is never reinstated (the source is simply re-offered under a fresh
// one), so its verdict stands either way.
//
// It is the CANCEL arm plus the VOID arm, in that order, and the two are separable:
// cancelPendingRegistrationsLocked below is the cancel arm on its own, for the one caller
// that must withdraw the not-yet-transmitted allocations WITHOUT abandoning the confirmed
// ones, which a drain still owes. Nothing else may take that half alone.
//
// sess.mu must be held.
func (sess *mqttSession) voidRegistrationsLocked(durable string, lic mqttDestroyLicence) ([]mqttRegCancellation, []string) {
	out := sess.cancelPendingRegistrationsLocked(durable, lic)
	// The registers this teardown ABANDONS as never transmitted. Named for what it holds:
	// before F-101 it was called `transmitted` and carried exactly the entries whose
	// verdict it had no right to record.
	var abandoned []string
	// RETIRED QoS 2 sources (srcAckOwed) are deliberately NOT voided here. Retired proves
	// the source crossed PUBREC, so a void verdict would falsely say the delivery was never
	// transmitted and license identifier reuse. purgeConsumerPendingLocked removes the
	// source-side suppression when this consumer goes, but deliberately leaves the slot for
	// phase-aware recovery or terminal cleanup. A crash before the consumer delete is safe
	// in the other direction too: recovery sees the surviving Retired fact, suppresses the
	// payload, and never re-sends it.
	//
	// The cancel arm above already removed every regPending entry from cpending, so what is
	// left here is every CONFIRMED allocation of this durable — proven-unsent and
	// possibly-attempted alike, which is why the arm has to classify rather than assume.
	for _, pi := range sess.cpending[durable] {
		ack := sess.pendingPublish[pi]
		if ack == nil || ack.regCancelled || ack.regNonce == _EMPTY_ || ack.regPending {
			continue
		}
		// ONLY A PROVEN-UNSENT REGISTER MAY BE VOIDED. A void verdict is a durable claim
		// that the delivery was never transmitted — recovery reads a voided nonce as
		// "never transmitted, whatever else it looks like" and releases its identifier and
		// its Receive Maximum slot — so recording one over a POSSIBLY-ATTEMPTED exchange
		// is a known-false claim that frees an identifier the client may be holding
		// ([MQTT-2.3.1-4]). This arm used to record one for every entry left in cpending,
		// which is exactly the set its own comment called transmitted (F-101).
		//
		// The gate is the same predicate the purge below asks, and asking it HERE as well
		// is not redundant: this staging runs earlier, and its verdict can already have
		// entered the membership-removal save by the time the purge runs, so a gate only
		// at the purge could not retract it.
		if !sess.provenUnsentReleasableLocked(ack) {
			continue
		}
		// Proven unsent: the register exists (or is being deleted) and its identifier is
		// the teardown reap's to free — which since F-034 it may decline to do, sparing
		// an above-watermark record or refusing on an unreadable one. Record the verdict
		// and leave the rest alone; the verdict is what a later reader needs, and it does
		// not depend on the removal having happened.
		if sess.stageCancellationVerdictLocked(lic, ack.regNonce) {
			abandoned = append(abandoned, ack.regNonce)
		}
	}
	return out, abandoned
}

// cancelPendingRegistrationsLocked withdraws the allocations of one durable that are not yet
// transmitted, and NOTHING else: it records no verdict on a transmitted register.
//
// It exists because a QoS 0 downgrade that will convert its consumer into a drain owes
// exactly this half. It must cancel now, because the delivery barrier does not stop a
// callback that is already past it: beginSendLocked tests regPending/regCancelled/regForeign
// and never sub.mqtt.closed, so an allocation whose register store is still in flight would
// otherwise confirm and transmit under a membership the packet has removed. And it must NOT
// void, because the transmitted deliveries are about to be DRAINED — a never-transmitted
// verdict on one of them is a lie that any save in this packet would make durable, and
// unvoiding afterwards cannot retract what a save already stored.
//
// The withheld marker each cancellation leaves in sess.deferred is what keeps DUP honest if
// the staging is rolled back: nothing was transmitted, so the eventual first transmission
// must carry DUP=0 ([MQTT-3.3.1-1]). Those markers are the caller's to preserve across a
// conversion — see convertDowngradeCandidatesLocked, which transfers the durable's LIVE
// marker set around stageDrain because stageDrain owns that map and drops what it cannot
// itself discover.
//
// sess.mu must be held.
func (sess *mqttSession) cancelPendingRegistrationsLocked(durable string, lic mqttDestroyLicence) []mqttRegCancellation {
	var out []mqttRegCancellation
	seqPis := sess.cpending[durable]
	for sseq, pi := range seqPis {
		ack := sess.pendingPublish[pi]
		if ack == nil || ack.regCancelled || ack.regNonce == _EMPTY_ || !ack.regPending {
			continue
		}
		if !sess.cancelRegPendingLocked(ack, lic) {
			continue // not this connection's decision any more (F-105 7b): the entry stands
		}
		out = append(out, mqttRegCancellation{pi: pi, nonce: ack.regNonce, durable: durable, sseq: sseq, reply: ack.registerSourceAckReply()})
		if ack.jsAckSubject != _EMPTY_ {
			if sess.deferred == nil {
				sess.deferred = make(map[mqttDeferredKey]string)
			}
			sess.deferred[mqttDeferredKey{durable, sseq}] = ack.jsAckSubject
		}
		delete(seqPis, sseq)
		sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectExactly(ack), mqttNewCancelledHold(ack.regNonce))
	}
	if len(seqPis) == 0 {
		delete(sess.cpending, durable)
	}
	return out
}

// unvoidRegNonces withdraws the verdicts a rolled-back packet had recorded on TRANSMITTED
// registers: its subscription survived, so those deliveries are live and their identifiers are
// still owed.
//
// Session lock NOT held on entry.
func (sess *mqttSession) unvoidRegNonces(nonces []string) {
	if len(nonces) == 0 {
		return
	}
	sess.mu.Lock()
	sess.unvoidRegNoncesLocked(nonces)
	sess.mu.Unlock()
}

// unvoidRegNoncesLocked is unvoidRegNonces for a caller that already holds the lock,
// because it is withdrawing the verdicts inside a critical section that must not be split:
// a revoked QoS downgrade reopens the delivery barrier, installs the new terms and
// withdraws these verdicts as one state change. See revokeLocked.
//
// sess.mu must be held.
func (sess *mqttSession) unvoidRegNoncesLocked(nonces []string) {
	for _, n := range nonces {
		if _, ok := sess.cancelledRegNonces[n]; ok {
			delete(sess.cancelledRegNonces, n)
			sess.voidedRegsRev++
		}
	}
}

// closeTeardownDeliveryLocked marks a doomed consumer's delivery subscription CLOSED, so no
// new delivery can begin tracking, and returns it so a rollback can reopen it.
//
// It has to run BEFORE the registrations are snapshotted. Otherwise the snapshot is only a
// moment in time: the callback stays live until the packet commits, so a delivery arriving
// after it allocates an identifier whose cancellation misses the membership-removal write —
// exactly the window that write exists to close. mqttDeliverMsgCbQoS12 tests this flag under
// sess.mu, the same lock the snapshot is taken under, so closing it here is what makes the
// snapshot complete rather than merely current.
//
// sess.mu must be held.
func (sess *mqttSession) closeTeardownDeliveryLocked(c *client, cc *ConsumerConfig) *subscription {
	if cc == nil {
		return nil
	}
	return sess.closeSharedDeliveryLocked(c, cc.DeliverSubject)
}

// closeSharedDeliveryLocked is closeTeardownDeliveryLocked keyed on the deliver SUBJECT.
//
// A shared filter's deliver subject is derived from its membership's acquisition references
// (mqttSharedGroupNamesFor), so the abandoned join of a FAILED filter can be barriered by name — which the config form cannot
// do, because that join has no sess.cons entry until the whole filter succeeds. F-082.
//
// sess.mu must be held.
func (sess *mqttSession) closeSharedDeliveryLocked(c *client, deliverSubject string) *subscription {
	if c == nil || deliverSubject == _EMPTY_ {
		return nil
	}
	c.mu.Lock()
	sub := c.subs[deliverSubject]
	c.mu.Unlock()
	if sub == nil || sub.mqtt == nil || sub.mqtt.closed {
		return nil
	}
	sess.subsMu.Lock()
	sub.mqtt.closed = true
	sess.subsMu.Unlock()
	return sub
}

// reopenTeardownDelivery undoes closeTeardownDeliveryLocked for a packet whose record write
// did NOT land: the subscription is still the client's, so it must go back to delivering.
//
// Session lock NOT held on entry.
func (sess *mqttSession) reopenTeardownDelivery(subs []*subscription) {
	if len(subs) == 0 {
		return
	}
	sess.mu.Lock()
	sess.subsMu.Lock()
	for _, sub := range subs {
		if sub != nil && sub.mqtt != nil {
			sub.mqtt.closed = false
		}
	}
	sess.subsMu.Unlock()
	sess.mu.Unlock()
}

// stageDeferredDeleteLocked prepares a consumer whose deletion a SUBSCRIBE packet has
// deferred to its confirmed save: the delivery barrier goes down and the pending
// registrations are withdrawn, both inside that packet's transaction.
//
// One helper for BOTH halves of a multi-level filter. The level-up companion of a "foo/#"
// filter is a consumer in its own right — its own durable, its own deliveries — and it owes
// exactly what the main one does; the two once differed here, and the companion's
// registrations could miss the membership-removal write entirely.
//
// sess.mu must be held.
// convert says the caller will try to turn this consumer into a DRAIN at its final-update
// boundary rather than destroy it. Then only the cancel arm runs: the transmitted registers
// keep their identifiers, because their exchanges are the ones being preserved, and a
// never-transmitted verdict recorded here would be made durable by any save this packet
// still issues. See cancelPendingRegistrationsLocked.
func (sess *mqttSession) stageDeferredDeleteLocked(c *client, cc *ConsumerConfig, convert bool) ([]mqttRegCancellation, *subscription, []string) {
	if cc == nil {
		return nil, nil, nil
	}
	sub := sess.closeTeardownDeliveryLocked(c, cc)
	if convert {
		return sess.cancelPendingRegistrationsLocked(cc.Durable, mqttLicenceOf(c)), sub, nil
	}
	cancels, transmitted := sess.voidRegistrationsLocked(cc.Durable, mqttLicenceOf(c))
	return cancels, sub, transmitted
}

// stageUnsubCancellationsLocked withdraws the in-flight registrations of every consumer an
// UNSUBSCRIBE is about to tear down, so the packet's own record write carries them.
//
// The delivery subscription of each such consumer is CLOSED first, in this same critical
// section, so nothing can allocate a further identifier behind the snapshot; the closed subs
// are returned for a rollback to reopen.
//
// A DRAINED sid is skipped: it keeps its consumer, and stageDrain has already withdrawn its
// pending registrations. What is left is the plain consumers this packet destroys (including
// the case where draining is not permitted at all) and the shared group durables, whose
// membership the same write removes. A shared filter's durables are derived from the session
// key rather than from cons, exactly as removeJSCons derives them: a persisted membership can
// exist with no local join.
//
// sess.mu must be held.
func (sess *mqttSession) stageUnsubCancellationsLocked(c *client, filters []*mqttFilter,
	drainedSids map[string]bool) ([]mqttRegCancellation, []*subscription, []string) {
	var out []mqttRegCancellation
	var closed []*subscription
	var transmitted []string
	void := func(dur string) {
		cancels, sent := sess.voidRegistrationsLocked(dur, mqttLicenceOf(c))
		out = append(out, cancels...)
		transmitted = append(transmitted, sent...)
	}
	// Every shared key this packet leaves. The removals have NOT been applied yet, so the
	// membership that survives is the live map minus this whole set — not minus the one key
	// in hand: a single packet can leave BOTH overlapping filters, and excluding only the
	// current one would let each doomed key protect the other and suppress a release nothing
	// will then owe. F-083.
	doomed := make(map[string]bool, len(filters))
	// The doomed MEMBERSHIPS, read off the live map before the removals are applied: their
	// acquisition references name the incarnations whose deliver subjects and durables this
	// staging half lowers the barrier on and voids.
	sharedKeys := make(map[string]mqttSharedMembership, len(filters))
	for _, f := range filters {
		if !f.shared {
			continue
		}
		key := f.sessionKey()
		if !doomed[key] {
			doomed[key] = true
			if m, held := sess.sharedSubs[key]; held {
				sharedKeys[key] = m
			}
		}
	}
	for _, f := range filters {
		if f.shared {
			continue // handled by the fenced, name-keyed arm below
		}
		for _, sid := range mqttUnsubSids(f.sessionKey()) {
			if drainedSids[sid] {
				continue // the drain KEEPS this consumer: it is not abandoned at all
			}
			cc, ok := sess.cons[sid]
			if !ok {
				continue
			}
			if sub := sess.closeTeardownDeliveryLocked(c, cc); sub != nil {
				closed = append(closed, sub)
			}
			void(cc.Durable)
		}
	}
	// A shared filter's targets come from its KEY, and the barrier is lowered by DELIVER
	// SUBJECT rather than through sess.cons. The two used to be keyed differently — the
	// barrier through cons, the void by durable — so for a persisted membership with no live
	// join the barrier was never lowered at all, leaving the void snapshot "only a moment in
	// time" (closeTeardownDeliveryLocked). A shared filter never drains, so nothing here is
	// owed to the drain arm above. F-083.
	if len(sharedKeys) > 0 {
		surv := sess.survivingSharedLocked(doomed)
		for _, t := range surv.targetsOwed(sharedKeys) {
			if sub := sess.closeSharedDeliveryLocked(c, t.deliver); sub != nil {
				closed = append(closed, sub)
			}
			void(t.durable)
		}
	}
	return out, closed, transmitted
}

// mqttRegCancellation is one allocation a consumer teardown withdrew while its register
// store was still in flight, in the form the securing pass needs: the subject is derived
// from pi, and (durable, sseq) complete the tombstone so the record it writes describes a
// real delivery rather than a bare marker.
type mqttRegCancellation struct {
	pi      uint16
	nonce   string
	durable string
	sseq    uint64
	reply   string
}

// purgeConsumerPendingLocked drops every PUBLISH-phase tracking entry belonging to
// one consumer durable, and returns the in-flight register sequences the caller must
// delete once it has released the session lock (R5), plus the cancellations it must
// SECURE before destroying the consumer (secureRegCancellations).
//
// The five callers all destroy or abandon a consumer without acking its deliveries —
// a 3.1.1 forced shared leave, a deferred consumer delete, a shared-consumer release,
// a surviving disconnect's shared purge, and a non-drain UNSUBSCRIBE teardown. Each
// one frees identifiers, so each one owes
// the registers naming them: a register left behind would have the next resume
// reserve an identifier for a delivery whose consumer no longer exists.
//
// It also releases this durable's in-memory SOURCE retirements, which are not in cpending
// at all. Their whole-life owner slots are deliberately NOT returned for deletion: losing
// the source consumer makes the suppression unnecessary, but it does not prove PUBCOMP.
// The client can still be waiting for (or answering) PUBREL, so the slot must remain until
// terminal cleanup observes completion. Once the consumer is gone, terminal cleanup reads
// the source as Lost and can safely remove the slot after PUBCOMP.
//
// Lock held on entry.
func (sess *mqttSession) purgeConsumerPendingLocked(durable string, lic mqttDestroyLicence) ([]mqttVoidedReg, []mqttRegCancellation) {
	return sess.purgeConsumerPendingModeLocked(durable, mqttPurgeDeliveryTargetTeardown, lic)
}

// mqttPendingPurgeMode is what the caller of the pending purge is doing to the delivery target.
type mqttPendingPurgeMode uint8

const (
	// mqttPurgeDeliveryTargetTeardown: the session is giving up the delivery target (a forced
	// leave, a deferred delete, a shared release, an UNSUBSCRIBE). This session will not be
	// re-offered the source through it, so every source mapping goes.
	mqttPurgeDeliveryTargetTeardown mqttPendingPurgeMode = iota + 1
	// mqttPurgeSurvivingDisconnect: a surviving session's disconnect gives up nothing but its
	// connection. The group durable survives and DOES re-offer the source to this member on its
	// resume, so a possibly-attempted entry the purge retains keeps its source mapping — the
	// redelivery then retransmits the SAME identifier with DUP set ([MQTT-4.4.0-1]) instead of
	// allocating a second one beside it (7a, consult 85). A dropped entry's mapping still goes.
	mqttPurgeSurvivingDisconnect
)

// purgeConsumerPendingModeLocked is purgeConsumerPendingLocked for an explicit caller mode.
//
// Lock held on entry.
func (sess *mqttSession) purgeConsumerPendingModeLocked(durable string, mode mqttPendingPurgeMode, lic mqttDestroyLicence) ([]mqttVoidedReg, []mqttRegCancellation) {
	seen := make(map[uint64]struct{})
	var regs []mqttVoidedReg
	var cancels []mqttRegCancellation
	// The sequence→nonce association is kept through the purge, because it is what lets a
	// PROVEN delete forget its verdict (deleteInflightRegisters): without it the verdict set
	// would grow without bound across a client that subscribes and unsubscribes forever.
	// The identity fields ride along for the tombstone rung.
	addReg := func(v mqttVoidedReg) {
		if v.seq == 0 {
			return
		}
		if _, dup := seen[v.seq]; dup {
			return
		}
		seen[v.seq] = struct{}{}
		regs = append(regs, v)
	}

	if seqPis, ok := sess.cpending[durable]; ok {
		surviving := mode == mqttPurgeSurvivingDisconnect
		// The mappings a surviving disconnect keeps: exactly those of the possibly-attempted
		// entries it retains. Every other mapping goes, and the bucket with it once empty.
		retained := make(map[uint64]struct{})
		if !surviving {
			delete(sess.cpending, durable)
		}
		defer func() {
			if !surviving {
				return
			}
			for sseq := range seqPis {
				if _, keep := retained[sseq]; !keep {
					delete(seqPis, sseq)
				}
			}
			if len(seqPis) == 0 {
				delete(sess.cpending, durable)
			}
		}()
		for sseq, pi := range seqPis {
			if ack := sess.pendingPublish[pi]; ack != nil {
				if ack.regForeign {
					// A RECOVERY-RESERVED (regForeign) entry contributes NO register
					// sequence to the teardown's ladder — the record is not this object's to
					// settle, and settling it violates the standing regForeign rule — and the
					// RESERVATION is
					// kept: only the cpending mapping goes with the consumer, exactly as
					// the recovery demote's shape (F-087 WP2).
					continue
				}
				if !sess.provenUnsentReleasableLocked(ack) {
					// POSSIBLY ATTEMPTED. RESERVED, not released, and this `continue` is
					// the whole arm: the entry stays, so bumpPI will not re-mint the
					// identifier and it still holds its Receive Maximum slot; no register
					// sequence is collected, so the teardown's ladder cannot destroy the
					// durable owner slot the exchange needs to complete; and no settle is
					// parked, because every parked continuation frees the identifier
					// (endSendWith). Only the cpending mapping goes, with the consumer that
					// is being destroyed — the regForeign arm's shape, and right here for
					// the same reason: nothing can re-offer that source again. Deliberately
					// UNLIKE F-100's resume reap, which keeps its mapping because a live
					// callback may still hold the payload.
					//
					// A teardown's own ladder establishes what the TEARDOWN owes. It never
					// establishes that the client completed: having no way to redeliver is a
					// fact about this broker (F-101). The client's own acknowledgement is
					// what releases this — PUBACK, or PUBREC storing a PUBREL against the
					// surviving slot and then PUBCOMP — and the acknowledgement handler
					// admits a reserved entry, declining only regForeign, regPending,
					// regCancelled and markerOpaque.
					//
					// An entry whose SEND SECTION is still open is reserved by this same
					// arm, because the predicate refuses an open section: the teardown
					// cannot decide an outcome that has not happened yet, and it schedules
					// no later release either. That leaves one identifier held where the
					// section afterwards proves a definite pre-write refusal — accepted,
					// stated in docs/CONFORMANCE.md, and the reason condemnSendLocked is
					// gone from this site rather than kept beside the gate (F-101, D4).
					//
					// A SURVIVING DISCONNECT keeps this entry's mapping, though: the group
					// durable survives and re-offers the source to this member on its resume,
					// and the mapping is what makes that redelivery a retransmission of this
					// exchange rather than a second admission (mqttPurgeSurvivingDisconnect).
					if surviving {
						retained[sseq] = struct{}{}
					}
					continue
				}
				addReg(mqttVoidedReg{seq: ack.regSeq, nonce: ack.regNonce, pi: pi, dur: durable, sseq: sseq})
				if ack.regPending {
					// An allocation whose register store is still in flight has no sequence
					// to collect, so the register it may be about to write would survive
					// this teardown unnamed. Two things make it nameable instead: the
					// cancellation nonce, recorded here, and a tombstone the caller writes
					// at its subject before the consumer goes — which is why the identifier
					// stays RESERVED until the store settles. See secureRegCancellations.
					if !sess.cancelRegPendingLocked(ack, lic) {
						continue // not this connection's decision any more (F-105 7b)
					}
					cancels = append(cancels, mqttRegCancellation{
						pi: pi, nonce: ack.regNonce, durable: durable, sseq: sseq, reply: ack.registerSourceAckReply(),
					})
					sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectExactly(ack), mqttNewCancelledHold(ack.regNonce))
					continue
				}
			}
			// PROVEN-UNSENT BY A LOCAL CLASSIFICATION, which is an observation and not
			// exclusion against the next acquisition: the durable slot can survive this
			// teardown, and the ladder that would remove it runs later and can be refused.
			// The entry's removal is unchanged; the identifier is excluded until a confirmed
			// absence settles it (F-098).
			var purgeNonce string
			if cur := sess.pendingPublish[pi]; cur != nil {
				purgeNonce = cur.regNonce
			}
			sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), purgeNonce)
			delete(sess.pendingPublish, pi)
		}
	}
	// Withheld markers for this durable go too. This is the COMMITTED-teardown half of the
	// rule stageDrain's cancellation relies on: a marker exists so a rolled-back
	// UNSUBSCRIBE keeps DUP=0 for a never-transmitted delivery, and once the consumer is
	// actually gone nothing will re-offer the sequence, so the marker has no first
	// transmission left to protect.
	for k := range sess.deferred {
		if k.jsDur == durable {
			delete(sess.deferred, k)
		}
	}
	for key := range sess.srcAckOwed {
		if key.jsDur != durable {
			continue
		}
		// Only the source-side suppression goes with its consumer. The slot remains the
		// packet identifier's durable owner until PUBCOMP; queuing rs.regSeq into the raw
		// teardown delete would bypass the phase gate and strand a stored PUBREL with no
		// admission authority.
		delete(sess.srcAckOwed, key)
	}
	// last_pi stays monotonic (see untrackPublishEntry); do not reset here.
	return regs, cancels
}

// secureRegCancellations makes each withdrawn allocation's cancellation durable ON THE
// REGISTER STREAM, and must be called before the consumer that owns them is destroyed.
//
// The session record carries the same fact, but not always in time: these callers run
// after their packet's save (a deferred consumer delete, a forced leave, a shared release),
// so an intent staged here may reach the record only at some later convergence point —
// and a crash before that leaves a landed register with nothing anywhere to explain it.
// A tombstone at the register's own subject needs no fence and no packet, so it is durable
// exactly when it has to be.
//
// The write is create-only first and then a replacement at the exact sequence its own
// nonce is found at. A FOREIGN occupant is a failure, never a success: with no sequence of
// our own to name, a foreign record is either a predecessor whose delete was lost or a
// successor on the same reused identifier, and overwriting the second would free an
// identifier the client is holding. Failing keeps the record-borne intent, which is the
// weaker but still durable tier.
//
// It is CLEANUP, not the guarantee: on every path that removes a membership the cancellation
// is already in that packet's own record — staged by voidRegistrationsLocked, or by its
// cancel arm alone where the consumer is being converted into a drain rather than
// abandoned. What
// this adds is promptness — a tombstone lets the register go now rather than at the next
// resume — plus a durable home for a cancellation that arrived too late to be staged.
//
// It reports whether every cancellation is durable, which callers log. They do NOT withhold
// the consumer delete on a false answer: for a late cancellation the membership is gone
// either way, so keeping the durable makes no identifier recoverable and leaves an
// unreferenced consumer nothing reaps.
//
// tok is the caller's open transaction, and it is what makes the record-borne fallback
// PROVABLE: these paths hold their packet's fence, so a transaction of our own would simply
// be refused. A zero token means there is none.
//
// Session lock NOT held on entry: this issues JetStream requests.
func (sess *mqttSession) secureRegCancellations(c *client, tok recordTx, cancels []mqttRegCancellation, lic mqttDestroyLicence) bool {
	secured, _ := sess.secureRegCancellationsClassified(c, tok, cancels, lic)
	return secured
}

// secureRegCancellationsClassified is secureRegCancellations with the record write's CAS
// verdict preserved, for the live SUBSCRIBE path.
//
// A packet that arms the operation gate, downgrades a plain consumer successfully, and then
// meets a remote takeover during this late record write would otherwise reach the tail with
// no trigger and no visible failure — and open the gate while the session's SURVIVING shared
// subscriptions serve under a membership another broker now owns. That is the same
// destructive shape as the early membership save's.
func (sess *mqttSession) secureRegCancellationsClassified(c *client, tok recordTx, cancels []mqttRegCancellation, lic mqttDestroyLicence) (bool, bool) {
	if !sess.tombstoneCancellationsOnStream(c, cancels, lic) {
		return true, false
	}
	return sess.proveVoidedRegs(c, tok)
}

// tombstoneCancellationsOnStream is secureRegCancellations' tombstone pass over the
// REGISTER STREAM alone, reporting whether any cancellation failed to get one, so its
// nonce stays staged in memory and the SESSION RECORD still owes the verdict.
//
// Split from the wrapper for the same reason voidRegistersOnStream is: the deferred
// close owns its own record write and folds the verdict into it.
//
// Session lock NOT held on entry: this issues JetStream requests.
func (sess *mqttSession) tombstoneCancellationsOnStream(c *client, cancels []mqttRegCancellation, lic mqttDestroyLicence) bool {
	owed := false
	for _, rc := range cancels {
		if err := sess.markInflightRegisterCancelledPending(c, rc, lic); err != nil {
			mqttWarnf(c, "MQTT: could not mark the cancelled in-flight registration of packet identifier %d "+
				"for client %q: %v", rc.pi, sess.id, err)
			owed = true
			continue
		}
		// The tombstone says everything the nonce did, on the record itself.
		sess.forgetCancelledReg(rc.nonce)
	}
	return owed
}

// proveVoidedRegs reports whether the session record is KNOWN to carry the void-register
// verdicts, saving it if it does not.
//
// Session lock NOT held on entry.
// The second result reports whether the record-borne fallback below was rejected by its
// PER-SUBJECT CAS. That is the one failure here carrying ownership evidence — the record
// demonstrably moved — and a live SUBSCRIBE must not open its transmission gate as "quiet"
// after it. It cannot be recovered from the bool: proven-or-not merges a CAS rejection with
// a local failure and an ambiguity. See mqttSubPacketFacts.recordCASRejected.
func (sess *mqttSession) proveVoidedRegs(c *client, tok recordTx) (bool, bool) {
	sess.mu.Lock()
	rev, saved := sess.voidedRegsRev, sess.voidedRegsSavedRev
	sess.mu.Unlock()
	if rev == saved {
		return true, false
	}
	if tok == (recordTx{}) {
		return sess.persistVoidedRegs(c), false
	}
	// The caller's transaction, since these paths hold their packet's fence and a
	// transaction of our own would simply be refused. An ambiguous outcome counts as not
	// proven: the caller withholds its irreversible step and the readback settles it.
	//
	// saveWithDetail rather than save: the outward error cannot answer the CAS question,
	// because persistRecord formats the store error with %v and destroys its NATS identity.
	if detail, err := sess.saveWithDetail(tok); err != nil {
		mqttWarnf(c, "MQTT: could not persist the void-register verdicts of client %q: %v", sess.id, err)
		return false, mqttIsRecordCASRejection(detail.rawErr)
	}
	// The save credited the revision it marshalled — at least rev, since the set can only
	// have grown since the sample above. See creditVoidedRegsLocked.
	return true, false
}

// markInflightRegisterCancelledPending writes the cancellation tombstone of an allocation
// whose register store is still in flight, so no sequence is known for it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) markInflightRegisterCancelledPending(c *client, rc mqttRegCancellation,
	lic mqttDestroyLicence) error {
	as := sess.asm
	if as == nil || rc.pi == 0 || rc.nonce == _EMPTY_ {
		return fmt.Errorf("no register identity to mark cancelled")
	}
	sess.mu.Lock()
	idHash, gen, regClaim, regClaimSeq := sess.idHash, sess.gen, sess.regClaim, sess.seq
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return fmt.Errorf("session has no incarnation identity")
	}
	subject := mqttInflightSubject(idHash, gen, rc.pi)
	// See the sibling writer: State is mandatory and a tombstone is PUBLISH-phase.
	// Lin: this body is written CREATE-ONLY, so it lands only on a subject the CAS proved
	// empty — there is no occupant in hand and no historical lineage to recover, and the
	// record's own lineage is its allocation's. What that does NOT prove is that a
	// predecessor's client obligation went away: a record may have landed with an inherited
	// lineage and then been removed, and a tombstone written afterwards cannot see it. The
	// conflict arm below rebuilds from the landed record instead, which preserves whatever
	// lineage it actually carries.
	sparse := mqttInflightRecord{
		PI: rc.pi, Dur: rc.durable, Seq: rc.sseq, QoS: 1, State: mqttInflightPublish,
		Nonce: rc.nonce, Lin: rc.nonce, Claim: regClaim, ClaimSeq: regClaimSeq, Cancelled: true,
		// THE ONE WRITER OF A PENDING FENCE (F-088, consult C4): this body is only ever written
		// create-only, and the create is what proves the subject held no occupant.
		CancelOrigin: mqttCancelOriginPendingCreate,
	}
	if mqttOrdinarySourceRegister(&sparse) {
		sparse.SourceAckReply = rc.reply
	}
	rec := &sparse
	var lastErr error
	expectSeq := uint64(0) // create-only: the store's own write may not have landed yet
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		err := sess.storeCancellationTombstone(subject, expectSeq, rec, lic)
		if err == nil {
			return nil
		}
		if mqttTombstoneRefusedTerminally(err) {
			return err
		}
		lastErr = err
		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		switch {
		case smsg == nil:
			// Raced away: retry the CREATE, and with the sparse body — a create-only CAS
			// is the first-CAS exemption at its strongest (there is no record to carry
			// forward, and the create itself proves the subject was empty).
			rec = &sparse
			expectSeq = 0
			continue
		case cur != nil && !mqttInflightRecordMatchesSubject(cur, subject):
			// The arms below would read a mismatched body as an authoritative tombstone,
			// or select it for replacement — and on the success side the caller then
			// FORGETS the nonce verdict, so the only remaining record of "never
			// transmitted" would be discarded on the strength of a record recovery
			// refuses to read. Fail closed; the caller leaves the cancellation owed.
			return fmt.Errorf("packet identifier %d is held by a record naming another identifier on %q", rc.pi, subject)
		case cur != nil && cur.Nonce == rc.nonce && cur.Cancelled:
			return nil // already marked, by us or by the store's own fallback
		case cur != nil && cur.Nonce == rc.nonce:
			if !mqttInflightTransitionAllowed(mqttSlotMarkCancelled, cur.State) {
				// The pending store settled and this exchange crossed PUBREC before the
				// create-only cancellation could win. Never add a fresh "not transmitted"
				// fact to that phase; its whole-life owner must remain authoritative.
				return fmt.Errorf("in-flight register %q is in state %q and cannot be marked never-transmitted", subject, cur.State)
			}
			// The store landed. REBUILD from it and set only Cancelled (F-087 WP1):
			// re-proposing the sparse create body would erase whatever the store wrote
			// and whatever a concurrent writer has since proved — a Retired above all,
			// whose loss re-sends a completed payload.
			rebuilt := *cur
			// Over an occupant, by CAS: never a fence, whatever the occupant looks like.
			rebuilt.Cancelled, rebuilt.CancelOrigin = true, mqttCancelOriginOccupant
			rec = &rebuilt
			expectSeq = smsg.Sequence
		default:
			// Foreign or unreadable, and nothing here can say which of the two it is —
			// which costs nothing, because this arm concludes nothing positive. Fail
			// closed rather than overwrite a record that may belong to a delivery the
			// client is holding, and leave the cancellation owed.
			return fmt.Errorf("packet identifier %d is held by another record on %q", rc.pi, subject)
		}
	}
	return lastErr
}

// mqttInflightSourceState is what can be proven about the source delivery a register
// names. The three values are distinct because they authorize different things, and
// collapsing Unknown into Complete would let an identifier the client is still
// holding be handed to a different message.
type mqttInflightSourceState int

const (
	// mqttInflightSourceLive: the source message is still there, unacked. The
	// register describes a delivery that can still be redelivered.
	mqttInflightSourceLive mqttInflightSourceState = iota
	// mqttInflightSourceComplete: the source was acknowledged or is authoritatively
	// absent while its consumer still holds interest.
	//
	// The SOURCE side of the exchange is over. That is not the same as the exchange being
	// over, and the register is not thereby garbage: a PUBLISH may have reached a client
	// whose PUBREC or PUBACK was lost, and the identifier is then still held on the wire.
	// The marker decides which — see srcRouted — and an earlier version of this sentence
	// asserted the stronger reading the code has never acted on.
	mqttInflightSourceComplete
	// mqttInflightSourceUnknown: a lookup or read FAILED. Nothing may be concluded, so the
	// identifier is reserved and counted as ordinary in-flight state — the delivery may
	// well still be live.
	mqttInflightSourceUnknown
	// mqttInflightSourceLost: the source's consumer is GONE. Deliberately not "complete" —
	// a delivery whose consumer was destroyed was lost rather than finished, and the client
	// may still be holding its identifier — but not "unknown" either, because this one is
	// KNOWN to be unrecoverable: nothing will ever re-offer it and no acknowledgement can
	// resolve it.
	//
	// Whether its identifier is held is CONDITIONAL on the marker, and that condition is
	// the whole of F-102. A POSSIBLY-ATTEMPTED exchange (marker absent) holds it: the
	// client may be holding it too, and a consumer disappearing does not prove the client
	// completed anything. A PROVEN-UNSENT one (marker present) does not: the PUBLISH
	// demonstrably never went out, so recovery consumes the pair and frees the identifier
	// exactly as it does over a Complete source. Before F-102 neither was released and the
	// reservation had no release edge at all.
	//
	// A held identifier is NOT exempt from Receive Maximum: that limit bounds what the
	// client holds, and the same reasoning applies.
	mqttInflightSourceLost
)

// mqttRoutedSourceCleanup is a source classification AS ROUTED for PUBLISH-phase cleanup:
// what the classify loop decided about a register that survived every exclusion, not what
// the source cache would answer if asked again.
//
// The ZERO value is what makes the type worth having. "Not routed" covers both a record
// diverted before the routing site — cancelled, unproven affine, drain-ambiguous,
// drain-OWNED, retired — and one whose source can still be re-offered, and neither may be
// consumed. mqttInflightSourceState is deliberately NOT reused here: its own zero is
// mqttInflightSourceLive, which would read an EXCLUDED record as a classified live source
// and make the two indistinguishable. The excluded case has to be representable, and
// distinct, or a later reader repeats the F-102 plan review's P1.
type mqttRoutedSourceCleanup uint8

const (
	// mqttSourceNotRouted: excluded before the routing site, or a source that may still be
	// re-offered. Never consumed here.
	mqttSourceNotRouted mqttRoutedSourceCleanup = iota
	// mqttSourceRoutedComplete: the source is acknowledged or authoritatively absent while
	// its consumer still holds interest. Nothing will re-offer it, and no cpending mapping
	// is installed for it.
	mqttSourceRoutedComplete
	// mqttSourceRoutedLost: the source's consumer is authoritatively gone, so nothing will
	// re-offer it either — but the mapping IS kept, because Lost is per-lookup and a
	// dispatched callback can still reach trackPublish for this source. See srcRouted.
	mqttSourceRoutedLost
)

// mqttInflightSourceMayReoffer reports whether a delivery can still come round again.
// Lost is deliberately false: unlike Complete it does not prove that a PUBLISH-phase
// exchange finished, but the classifier's positive meaning is that the consumer is gone
// and nothing can re-offer this delivery.
//
// TWO callers rest on that, and keeping them one predicate is the point. Past-PUBREC
// suppression asks it to decide whether durable source suppression is still owed, which
// lets a terminal PUBCOMP release its identifier. PUBLISH-phase recovery asks it at the
// routing site (see srcRouted) to decide whether a register is a candidate for source
// cleanup at all — and there it is only half the test: the MARKER decides the outcome, so
// the stricter completion question is not weakened, it is answered by different evidence.
func mqttInflightSourceMayReoffer(st mqttInflightSourceState) bool {
	return st == mqttInflightSourceLive || st == mqttInflightSourceUnknown
}

// mqttInflightResponseKind is what a single batched message-get response proves.
//
// Three values, because two would lose the distinction the whole file rests on: a read
// that FAILED and a message that is ABSENT look similar in a response and mean opposite
// things. Absence proves an exchange finished; a failed read proves nothing, and treating
// it as absence frees an identifier the client may still be holding and deletes the
// register that records it.
type mqttInflightResponseKind int

const (
	// mqttInflightResponseIndeterminate: no response at all, or an API error that is not
	// "no message found". Nothing may be concluded.
	mqttInflightResponseIndeterminate mqttInflightResponseKind = iota
	// mqttInflightResponseAbsent: the server answered, authoritatively, that there is no
	// such message.
	mqttInflightResponseAbsent
	mqttInflightResponsePresent
)

// mqttClassifyMsgGetResponse applies that rule. It is the single definition shared by the
// batched register load and the batched source classification, so neither can drift into
// reading a failed read as an absence — which is exactly what the first batched
// implementation of both did.
func mqttClassifyMsgGetResponse(resp *JSApiMsgGetResponse) mqttInflightResponseKind {
	if resp == nil {
		return mqttInflightResponseIndeterminate
	}
	if rerr := resp.ToError(); rerr != nil {
		if isErrorOtherThan(rerr, JSNoMessageFoundErr) {
			return mqttInflightResponseIndeterminate
		}
		return mqttInflightResponseAbsent
	}
	if resp.Message == nil {
		return mqttInflightResponseIndeterminate
	}
	return mqttInflightResponsePresent
}

// applyConsumerLookup records ONE batched consumer lookup outcome.
//
// Extracted so the production mapping from (info, settled) to the cache's two maps can be
// driven by a test, rather than a test pre-seeding those maps and proving only its own
// fixture. The distinction this carries is terminal: a SETTLED absence resolves to Lost —
// knowably unrecoverable — while an unsettled lookup stays Unknown. Encoding a reply that
// established nothing as the former is the defect mqttClassifyConsumerInfoResponse fixes,
// and this is the seam through which that fix is observable end to end.
func (sc *mqttInflightSourceCache) applyConsumerLookup(durable string, info *ConsumerInfo, settled bool) {
	if !settled {
		return // nothing established: no entry, which resolves to Unknown
	}
	sc.consumers[durable], sc.resolved[durable] = info, true
}

// mqttInflightSourceCache resolves the state of many source deliveries with a bounded
// number of JetStream requests instead of two per record: one consumer lookup per
// DURABLE, and one batched message load per stream.
//
// It exists for the resume path, which runs before the CONNACK. Classifying 65_535
// registers one at a time is upwards of 100_000 serial round trips; the same work here is
// a handful of consumer lookups plus a few batches.
//
// Every uncertainty is preserved exactly as the single-record path preserves it: a
// consumer that cannot be READ and a message load with no response are Unknown, never
// Complete.
//
// A consumer authoritatively GONE is not an uncertainty and is deliberately not in that
// list — it resolves to Lost, which is terminal. An earlier version of this comment said
// gone was Unknown too, which the state branch below has never done. The distinction is
// the whole reason the cache keeps `known` and `resolved` apart, and it is why the batched
// lookup must never encode "the reply said nothing" the same way as "the consumer is not
// there": see mqttClassifyConsumerInfoResponse.
type mqttInflightSourceCache struct {
	as        *mqttAccountSessionManager
	consumers map[string]*ConsumerInfo // durable -> info; nil means absent or unreadable
	known     map[string]bool          // durable -> a lookup has been ATTEMPTED
	resolved  map[string]bool          // durable -> that lookup completed, so nil means absent
	states    map[string]mqttInflightSourceState
	// subjects holds the SUBJECT of every source the load found LIVE, keyed the same way
	// as states. It costs nothing — the message was read to classify the source anyway —
	// and it is what lets an affine register be checked against the message it actually
	// names rather than against its own two fields (F-065).
	subjects map[string]string
}

func (as *mqttAccountSessionManager) newInflightSourceCache() *mqttInflightSourceCache {
	return &mqttInflightSourceCache{
		as:        as,
		consumers: make(map[string]*ConsumerInfo),
		known:     make(map[string]bool),
		resolved:  make(map[string]bool),
		states:    make(map[string]mqttInflightSourceState),
		subjects:  make(map[string]string),
	}
}

// resolve classifies every source in recs, filling the cache. Safe to call more than once;
// sources already resolved are not re-read.
func (sc *mqttInflightSourceCache) resolve(recs []*mqttInflightRecord) {
	// A record naming no source delivery — a retained register, or empty coordinates — is refused
	// here rather than keyed by those coordinates, where it would collide with every other one.
	kept := recs[:0:0]
	for _, r := range recs {
		if !mqttRefuseSourcelessRecordAtOrdinarySink(r) {
			kept = append(kept, r)
		}
	}
	recs = kept
	sc.as.mu.RLock()
	hook := sc.as.testInflightSourceState
	sc.as.mu.RUnlock()
	if hook != nil {
		for _, r := range recs {
			if r != nil {
				sc.states[mqttPubRelSourceKey(r.Dur, r.Seq)] = hook(r)
			}
		}
		return
	}
	// One consumer lookup per distinct durable, and those lookups BATCHED. Per-durable
	// caching alone is not enough: a session with a few thousand subscriptions, each
	// holding one in-flight delivery, would still pay a few thousand serial round trips
	// before the CONNACK. Grouped by stream, since the affine path's durables live on
	// $MQTT_out and every other on $MQTT_msgs.
	pending := make(map[string][]string)
	for _, r := range recs {
		if sc.known[r.Dur] {
			continue
		}
		sc.known[r.Dur] = true
		stream := mqttInflightSourceStream(r.Dur)
		pending[stream] = append(pending[stream], r.Dur)
	}
	for stream, durs := range pending {
		for start := 0; start < len(durs); start += mqttInflightLoadBatch {
			end := min(start+mqttInflightLoadBatch, len(durs))
			batch := durs[start:end]
			infos, ok, err := sc.as.jsa.lookupConsumersMulti(stream, batch)
			if err != nil {
				// A whole batch that could not be completed leaves its durables unresolved,
				// which resolves to Unknown — never to "the consumer is gone".
				continue
			}
			for i, dur := range batch {
				var info *ConsumerInfo
				var settled bool
				if i < len(infos) {
					info = infos[i]
				}
				if i < len(ok) {
					settled = ok[i]
				}
				sc.applyConsumerLookup(dur, info, settled)
			}
		}
	}

	// Everything the ack floor cannot settle needs its message read. Grouped by stream and
	// batched; per-message expiry leaves holes ABOVE the floor, so the floor alone is not
	// enough (the same reason the single-record path pays for a lookup).
	type want struct {
		key string
		seq uint64
	}
	byStream := make(map[string][]want)
	for _, r := range recs {
		key := mqttPubRelSourceKey(r.Dur, r.Seq)
		if _, done := sc.states[key]; done {
			continue
		}
		ci, looked := sc.consumers[r.Dur], sc.resolved[r.Dur]
		if ci == nil {
			// Absent where the lookup COMPLETED is Lost — knowably unrecoverable. Absent
			// because the lookup could not be read is merely Unknown. Neither is complete.
			if looked {
				sc.states[key] = mqttInflightSourceLost
			} else {
				sc.states[key] = mqttInflightSourceUnknown
			}
			continue
		}
		if ci.AckFloor.Stream >= r.Seq {
			sc.states[key] = mqttInflightSourceComplete
			continue
		}
		stream := mqttInflightSourceStream(r.Dur)
		byStream[stream] = append(byStream[stream], want{key, r.Seq})
		sc.states[key] = mqttInflightSourceUnknown // until the read says otherwise
	}

	for stream, wants := range byStream {
		for start := 0; start < len(wants); start += mqttInflightLoadBatch {
			end := min(start+mqttInflightLoadBatch, len(wants))
			batch := wants[start:end]
			seqs := make([]uint64, 0, len(batch))
			for _, w := range batch {
				seqs = append(seqs, w.seq)
			}
			resps, err := sc.as.jsa.loadMsgsMulti(stream, seqs)
			if err != nil {
				continue // every unresolved entry stays Unknown
			}
			for i, resp := range resps {
				if i >= len(batch) {
					continue
				}
				switch mqttClassifyMsgGetResponse(resp) {
				case mqttInflightResponseAbsent:
					// Authoritatively absent while its consumer still holds interest, which
					// is what proves the delivery was acknowledged.
					sc.states[batch[i].key] = mqttInflightSourceComplete
				case mqttInflightResponsePresent:
					sc.states[batch[i].key] = mqttInflightSourceLive
					sc.subjects[batch[i].key] = resp.Message.Subject
				default:
					// Indeterminate: stays Unknown, never Complete.
				}
			}
		}
	}
}

// affinePairProvenBySource reports whether a register's carried affine pair is PROVEN by
// the source that register names, and — when it is not — what was observed instead.
//
// This is the third leg of the pair's binding, and it is the one neither of the others can
// supply. The record's own shape is checked by mqttInflightRecordIdentityOK, and the claim
// it names is required to name the carried copy back (q2AffClaimForCarriedCopy) — but both
// of those are satisfied by a register that names some OTHER claim's copy while its own
// delivery is a different message entirely. Such a register would credit that claim with a
// delivery it never made and let its copy be acknowledged away unpublished, which is the
// loss F-065 exists to prevent. The source is what says which copy this register was
// written for, so it is the arbiter: for an affine register the source IS the copy.
//
// The predicate is deliberately POSITIVE, and that is a reversal of the first form built
// here. That one refused only a live source naming a DIFFERENT subject, reasoning that an
// absent or unreadable source is not evidence of a forgery — which is true, and beside the
// point: nothing licenses installing an authoritative pair on the strength of a source
// that was never read. Under the contradiction-only form an indeterminate consumer lookup
// or message read, or a consumer that is authoritatively gone, admitted the forgery whole.
// So only a live source whose subject IS the carried copy proves the pair; every other
// state leaves it unproven and the record is reserved rather than interpreted.
//
// A COMPLETE source is not this predicate's business: no pair is installed for one, since
// the register is dropped as a delivery whose exchange finished. Callers check that first.
func (sc *mqttInflightSourceCache) affinePairProvenBySource(r *mqttInflightRecord) (observed string, proven bool) {
	if r == nil || r.Q2AffClaim == _EMPTY_ {
		return _EMPTY_, true // not an affine register: nothing for the source to bind
	}
	key := mqttPubRelSourceKey(r.Dur, r.Seq)
	if sc.states[key] != mqttInflightSourceLive {
		return _EMPTY_, false // never read, unreadable, or knowably gone: unproven
	}
	subject, ok := sc.subjects[key]
	if !ok {
		return _EMPTY_, false
	}
	return subject, subject == r.Q2AffCopy
}

// state returns a resolved source's classification, defaulting to Unknown for anything
// resolve could not settle.
func (sc *mqttInflightSourceCache) state(r *mqttInflightRecord) mqttInflightSourceState {
	if st, ok := sc.states[mqttPubRelSourceKey(r.Dur, r.Seq)]; ok {
		return st
	}
	return mqttInflightSourceUnknown
}

// inflightSourceState classifies the source delivery a register names.
//
// The consumer is consulted FIRST and its absence is never read as completion, for
// the reason recoverDrains gives about the same lookup: on the interest-retained
// streams, deleting a consumer deletes the messages it still held, so "the message
// is gone" proves the exchange finished only while the consumer that would have
// delivered it still exists.
func (as *mqttAccountSessionManager) inflightSourceState(r *mqttInflightRecord) mqttInflightSourceState {
	// UNKNOWN, never Lost: empty coordinates — a retained register's, or a record synthesized from
	// a sourceless slot — would read as a consumer that is gone, and Lost licenses cleanup.
	if mqttRefuseSourcelessRecordAtOrdinarySink(r) {
		return mqttInflightSourceUnknown
	}
	as.mu.RLock()
	hook := as.testInflightSourceState
	as.mu.RUnlock()
	if hook != nil {
		return hook(r)
	}
	stream := mqttInflightSourceStream(r.Dur)
	ci, err := as.jsa.lookupConsumer(stream, r.Dur)
	if err != nil && isErrorOtherThan(err, JSConsumerNotFoundErr) {
		return mqttInflightSourceUnknown
	}
	if ci == nil {
		// The lookup completed and found nothing: unrecoverable, but knowably so.
		return mqttInflightSourceLost
	}
	if ci.AckFloor.Stream >= r.Seq {
		return mqttInflightSourceComplete
	}
	// Per-message expiry leaves holes ABOVE the ack floor, so the floor is not the
	// only way a message can be gone. Only an exact lookup that CONFIRMS absence
	// resolves it; an indeterminate one is Unknown, never Complete.
	if _, lerr := as.jsa.loadMsg(stream, r.Seq); lerr != nil {
		if isErrorOtherThan(lerr, JSNoMessageFoundErr) {
			return mqttInflightSourceUnknown
		}
		return mqttInflightSourceComplete
	}
	return mqttInflightSourceLive
}

// storeInflightRegister makes a freshly allocated identifier durable, and is the
// gate every first transmission passes through (R1).
//
// Returns true only once a register naming (jsDur, sseq) under pi is confirmed at a
// sequence, at which point the entry's regPending is cleared and the caller may send.
// A false return has already removed the identifier from the tracking maps, so the
// caller must send nothing and simply return: JetStream re-offers the sequence after
// AckWait, and the next attempt allocates afresh.
//
// Reconciliation on a failed CAS — every outcome resolved by an authoritative
// re-read, because an ambiguous write may have landed:
//
//   - absent                       -> contention; retry the create
//   - our nonce                    -> our own write landed and its response was
//     lost; adopt its sequence and proceed
//   - a foreign record naming the SAME (Dur, Seq) -> the same fact recorded by a
//     superseded broker; replace it by a CAS at its exact sequence so ownership is
//     unambiguous, rather than adopting a record another party may delete
//   - a foreign record naming ANOTHER delivery whose source is provably complete ->
//     a stale register left by a lost delete; reap it at its exact sequence,
//     CONFIRMED, and retry the create
//   - a foreign record naming a live or unknown delivery -> FAIL CLOSED. This
//     identifier is not ours to use, and no send may happen under it. Deliberately
//     not reaped: on another broker that record may be a LIVE delivery's identity,
//     and destroying it would strand the identifier the client is holding.
//
// Session lock NOT held on entry.
func (sess *mqttSession) runInflightAdmission(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim, q2AffCopy string, write mqttInflightWrite) bool {
	as := sess.asm
	// Opportunistic, and free when nothing is pending: one of the convergence points that
	// is guaranteed to come round again on a live session, so a void verdict whose save was
	// refused earlier still reaches the record.
	sess.persistVoidedRegs(c)
	sess.mu.Lock()
	idHash, gen, regClaim, regClaimSeq := sess.idHash, sess.gen, sess.regClaim, sess.seq
	ack := sess.pendingPublish[pi]
	// The caller's nonce — read under the lock that ALLOCATED the entry — is the identity
	// every check here keys on. The identifier alone cannot be: this goroutine can stall
	// (the persist above is a round trip) across a cancellation, a settle, a reuse and a
	// second cancellation, and by then pendingPublish[pi] describes someone else's
	// allocation, whose reservation a stale store must not touch.
	stillOurs := ack != nil && ack.regPending && !ack.regCancelled &&
		ack.jsDur == jsDur && ack.sseq == sseq && ack.regNonce == nonce
	sourceReply := _EMPTY_
	if stillOurs && !mqttDurIsSlotOnlyClass(jsDur) {
		sourceReply = ack.registerSourceAckReply()
	}
	sess.mu.Unlock()
	if as == nil || idHash == _EMPTY_ || gen == _EMPTY_ {
		// Unreachable for a real session: every session carries an incarnation
		// generation (mqttSessGenFromRecord rejects a record without one) and an
		// account manager. Fail closed rather than sending under an identifier nothing
		// records.
		sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
		return false
	}
	if !stillOurs {
		// The entry went away under us (a discard, or a terminal path) or is no longer
		// the allocation we were asked to persist. Nothing to confirm and nothing to
		// send — but OUR cancelled reservation still has to be released, because this is
		// the same "the store is over" moment confirmInflightRegister represents, reached
		// when the teardown won the race to the entry before the first read. Keyed by the
		// nonce: a reservation carrying a DIFFERENT one belongs to a newer allocation
		// whose own store is still out, and releasing it would free that identifier with
		// its CAS still on the wire.
		sess.releaseCancelledReservation(pi, nonce)
		return false
	}

	if nonce == _EMPTY_ {
		// Unreachable: trackPublish mints the nonce with every allocation that owes a
		// register. Fail closed rather than write a record with no identity — a nonce is
		// what proves a landed write is ours and what a cancellation names it by.
		sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
		return false
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	// A fresh allocation STARTS its own lineage, and the first write mode is the create-only
	// pair. Every later attempt re-derives this from its own mode; see the reset below and the
	// two replacement arms. Only the Lin is applied: see mqttLineageForWrite.
	createLin, _ := mqttLineageForWrite(mqttLineageWriteCreate, nil, nonce)
	rec := &mqttInflightRecord{
		PI:             pi,
		Dur:            jsDur,
		Seq:            sseq,
		QoS:            qos,
		SourceAckReply: sourceReply,
		// Every slot starts in PUBLISH. A successful PUBREC handoff or direct PUBREL
		// admission CAS-transitions the same record through the later phases; it remains
		// the identifier's only durable claimant until terminal cleanup.
		State:      mqttInflightPublish,
		Nonce:      nonce,
		Lin:        createLin,
		Claim:      regClaim,
		ClaimSeq:   regClaimSeq,
		Q2AffClaim: q2AffClaim,
		Q2AffCopy:  q2AffCopy,
	}
	// Every newly written QoS 2 slot is created with this session object's claim, recovery
	// refreshes eligible quiescent slots, and PUBREL admission requires equality before
	// acquiring an action. The structural matrix still reads an unclaimed publish slot so
	// recovery can reserve or refresh the pre-transition shape; custody is mandatory once
	// the slot moves past PUBREC.
	//
	// Nothing is derived here. regClaim/regClaimSeq were captured from the session UNDER
	// THE LOCK above, and re-reading sess.regClaim at this point would be a data race
	// against a concurrent rebind — a session's claim is exactly the kind of field a
	// takeover rewrites. An absent claim is therefore a genuine precondition failure and
	// fails closed below rather than being patched from unsynchronised state.
	if verr := mqttValidateInflightShape(rec); verr != nil {
		// Refuse to WRITE a shape no reader may interpret. Every reader validates, so
		// storing one would only create residue an operator has to remove — and the
		// identifier would be reserved by a record nothing can act on.
		mqttWarnf(c, "MQTT: refusing to store an in-flight register for packet identifier %d of "+
			"client %q: %v", pi, sess.id, verr)
		sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
		return false
	}

	var lastErr error
	// The next attempt's write. A fresh allocation creates the PAIR — the unsent marker
	// staged first, the register committing the batch, both create-only. The replacement
	// arm below chooses between a pair CAS at both observed sequences and a register-only
	// CAS, on the boundary's word (N-6.4). Whatever mode an attempt used is remembered
	// across its reread, because "our own write, response lost" has to know whether a
	// marker was part of it.
	regExpect, markerExpect := uint64(0), uint64(0)
	registerOnly := false
	// The occupant the previous attempt loaded, carried forward so the replacement write can
	// rewrite it; nil on the first attempt, whose write is always a create.
	var lastCur *mqttInflightRecord
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		// THE WRITE IS THE ONLY THING THAT DIFFERS between the standalone register/marker pair
		// this ladder was born with and the F-105 admission that carries the authority, exchange
		// and resource edges beside it. Everything below — the mode/lineage reset, the classifier
		// and its dispositions — is identical, so the write is a parameter and the ladder is one.
		seq, err := write(regExpect, markerExpect, registerOnly, lastCur, rec, subject)
		if err == nil {
			// rec.QoS is what LANDED: the replacement arm above may have set it from the
			// occupant, so the caller's qos argument is not authoritative here.
			return sess.settleLandedRegister(c, mqttLandedRegister{gen: gen, pi: pi, jsDur: jsDur, sseq: sseq,
				regSeq: seq, nonce: rec.Nonce, lin: rec.Lin, qos: rec.QoS}, subject, registerOnly)
		}
		lastErr = err
		landedRegisterOnly := registerOnly
		regExpect, markerExpect, registerOnly = 0, 0, false
		// THE LINEAGE FOLLOWS THE WRITE MODE, so it is reset with the mode and not once per
		// call. The line above returns this attempt to a create-only pair, and a create-only
		// CAS proves the subject was empty — there is nothing to inherit from, so the record
		// about to be written is a FRESH exchange and its lineage is its own nonce.
		//
		// Without this, an attempt that took the inheriting replacement arm below and then
		// failed would carry the occupant's lineage into a create on an empty subject,
		// durably stamping a genuinely new exchange with an obligation the client already
		// discharged. rec is built once before this loop and the arms below mutate it in
		// place, which is exactly why the reset has to be here beside the mode.
		//
		// The occupant is passed as nil, not lastCur: lastCur is the PREVIOUS attempt's read,
		// stale here because the reread is below, and a create inherits from nothing anyway.
		rec.Lin, _ = mqttLineageForWrite(mqttLineageWriteCreate, nil, rec.Nonce)
		rec.SourceAckReply = sourceReply

		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		lastCur = cur
		// THE CLASSIFIER IS THE DECISION (F-105, row 1). What stood here was eleven inline
		// predicates; the ladder below now only CARRIES OUT a verdict, and every question
		// about which disposition a slot takes is answered in inflight_slot_classify.go.
		//
		// It was earned rather than asserted: an audit hook ran the classifier beside the live
		// predicates over the corpus that already exercises this function and failed the run on
		// any disagreement, because a unit table proves a classifier self-consistent and not
		// faithful. Once the predicates were gone the audit compared the verdict to itself, so
		// hook and harness were deleted as a test that cannot fail — that file's closing comment
		// records it. Nothing runs beside this ladder now.
		//
		// TWO CLASSIFICATIONS. The proofs an other-delivery occupant needs — its source
		// complete AND proven-unsent release authority — are round trips this ladder only ever
		// pays for in that one arm, so the first pass runs without them and the second is
		// asked only when the first says abandon.
		slotObs := mqttSlotObservation{
			occupant:             cur,
			subjectNamesOccupant: mqttInflightRecordMatchesSubject(cur, subject),
		}
		if smsg == nil {
			slotObs.resp = mqttSlotSubjectAbsent
		} else {
			slotObs.resp, slotObs.seq = mqttSlotSubjectPresent, smsg.Sequence
		}
		// A CANCELLED OCCUPANT IS DECIDED BY ITS MARKER (F-088, WP3 edit 7): the classifier needs
		// that evidence BEFORE it classifies, so the occupant's marker is probed here — the
		// replacement arm's own probe, MOVED earlier for this one shape and reused there, so it is
		// read once (cancelledOccupantMarker). Every other occupant keeps the probe where it was.
		var cancelledOccupantMarker *mqttMarkerLoad
		// A PENDING FENCE takes no probe: it has no marker and is classified by its provenance.
		if cur != nil && smsg != nil && cur.State == mqttInflightPublish && cur.Cancelled && !cur.Retired &&
			cur.CancelOrigin != mqttCancelOriginPendingCreate && cur.Nonce != rec.Nonce {
			load := as.loadInflightMarker(mqttMarkerCallerReplace, subject)
			cancelledOccupantMarker = &load
			switch {
			case load.kind == mqttInflightResponseAbsent:
				slotObs.cancelMarker = mqttSlotMarkerAbsent
			case mqttMarkerNamesAllocation(load, cur.Nonce):
				slotObs.cancelMarker = mqttSlotMarkerPresent
			default:
				slotObs.cancelMarker = mqttSlotMarkerIndeterminate
			}
		}
		slotIntent := mqttAllocationIntent{pi: pi, dur: jsDur, sseq: sseq, nonce: rec.Nonce}
		slotVerdict := mqttClassifyInflightSlot(slotObs, slotIntent)

		switch slotVerdict.disposition {
		case mqttSlotDispCreate:
			// Raced away, or never landed. Before the create is retried, a marker with no
			// register — a stale pair's leftover — would make the marker's create-only CAS
			// reject the whole batch identically forever. Operation "reap", CAPTURE FIRST:
			// the marker's sequence and body are taken, then the sibling is read again, and
			// only a marker whose sibling is authoritatively absent is deleted (row 24), at
			// exactly the captured sequence, confirmed so the retry can succeed (T6b).
			as.reapMarkerCaptureFirst(c, mqttMarkerCallerStore, subject, true)
			continue
		case mqttSlotDispAbandonedHeld:
			c.Warnf("MQTT: refusing packet identifier %d for client %q: %q holds an abandoned "+
				"delivery's disposal record, kept until the generation is discarded", pi, sess.id, subject)
			sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
			return false
		case mqttSlotDispRefuse:
			// THREE SHAPES, ONE OUTCOME: withdraw this allocation and touch nothing. They
			// are one disposition because they agree on what may be DONE — nothing — and
			// they keep separate log lines because they do not agree on what was OBSERVED,
			// and an operator reading a refusal needs to know which.
			switch {
			case cur == nil:
				// Unreadable. It cannot be attributed to any delivery, so it cannot be
				// proven stale either — and it occupies an identifier we must not use.
				c.Warnf("MQTT: refusing packet identifier %d for client %q: an unreadable in-flight register occupies %q at sequence %d",
					pi, sess.id, subject, smsg.Sequence)
			case !slotObs.subjectNamesOccupant:
				// The occupant's body names a DIFFERENT packet identifier than this
				// subject. Recovery classifies exactly this shape as unreadable and
				// RESERVES the identifier; this path must not be more permissive, because
				// every branch below either replaces or deletes on the strength of the body
				// it just read. Erasing it here and then transmitting under this identifier
				// is how a corrupted record becomes a reused identifier the client may
				// still hold. Review of F-034 found this was the third such path.
				c.Warnf("MQTT: refusing packet identifier %d for client %q: the record on %q at "+
					"sequence %d names packet identifier %d, which that subject does not",
					pi, sess.id, subject, smsg.Sequence, cur.PI)
			default:
				// A past-PUBREC slot carrying a retirement. Not a stale PUBLISH retirement
				// to adopt: the slot is already the authoritative owner of a PUBREL
				// exchange, so neither rewriting it nor installing a PUBLISH-side
				// retirement is legal. Withdraw and leave it untouched.
			}
			sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
			return false
		case mqttSlotDispAdoptLandedWrite:
			// OUR OWN WRITE, SINCE ENDED BY SOMEONE ELSE. A tombstone carrying our nonce while our
			// own allocation is NOT cancelled is not our cancel-pending sparse record (that one is
			// adopted below, and the confirmation's cancelled arm settles it): another
			// continuation — a withdrawal of the obligation this write admitted — ended it after
			// our response was lost. Adopting it would confirm an entry for an exchange no
			// acquisition may ever serve. The tombstone is reaped through the gated door (which
			// finds the exchange ended, or withdraws it first), and this allocation is abandoned
			// whether or not the reap succeeds: the identifier stays excluded until a confirmed
			// absence, and the source re-offers under a FRESH lineage.
			if cur.Cancelled && !sess.ownAllocationCancelled(pi, nonce) {
				if !sess.reapObservedRegister(c, subject, smsg.Sequence, cur, mqttSlotDeleteNeverTransmitted) {
					c.Warnf("MQTT: the ended in-flight register on %q for client %q could not be reaped now; "+
						"its identifier stays excluded until recovery removes it", subject, sess.id)
					// The door refused: nothing proves the obligation ended (a cancellation is a
					// decision, not an ending), so an un-ended exchange of this allocation may stand
					// and the abandonment keeps its source under the admitting licence (F-097 WP5).
					sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
					return false
				}
				// The door destroyed our register, which it does for this Publish-state tombstone
				// only once endUnsentBeforeDestroy read its exchange ENDED or ended it itself
				// (WithdrawnUnsent). Nothing is left for a continuity record to refuse or
				// withdraw (WP5's "ended -> remove"), so the abandonment carries the zero licence,
				// which authorizes nothing and installs none: the source is deferred and re-offered
				// under a fresh lineage.
				sess.abandonInflightRegister(mqttDestroyLicence{}, pi, jsDur, sseq, nonce)
				return false
			}
			// Our own write, whose response we lost. This is the whole reason the nonce
			// exists: the sequence cannot tell us this, since the subject is reused.
			// cur.QoS, not rec.QoS: the nonce identifies this ALLOCATION, never an
			// immutable record body. markInflightRegisterCancelledPending can write a
			// sparse record carrying this same nonce with a hardcoded QoS 1, and this arm
			// is ordered before the Cancelled classification below, so the record that
			// actually exists is the only thing that may be threaded back. (Where it is
			// such a tombstone the confirmation refuses anyway, so this costs nothing --
			// but asserting the two are equal would be a false premise in the code.)
			//
			// The lost attempt's MODE decides what landed beside it: a pair, whose marker
			// step 3 now loads; or a register alone, which records a transmitted exchange.
			return sess.settleLandedRegister(c, mqttLandedRegister{gen: gen, pi: pi, jsDur: jsDur, sseq: sseq,
				regSeq: smsg.Sequence, nonce: rec.Nonce, lin: cur.Lin, qos: cur.QoS}, subject, landedRegisterOnly)
		case mqttSlotDispReapCancelledThenRetry:
			// A PURE cancellation tombstone. It records a delivery that was NEVER
			// transmitted, so the identifier it names is free by construction and this
			// record is pure residue — its writer wanted it gone and only wrote it
			// because the delete could not be proven. Reaping it is therefore always
			// safe, and it is what keeps a securing pass whose store never followed (the
			// teardown won the race to the entry) from wedging this identifier until the
			// next resume. Confirmed and at the exact sequence observed (R4): an unproven
			// reap makes the retried create fail identically forever.
			//
			// Ordered ABOVE the same-delivery arm deliberately (F-087 WP2): when the
			// tombstoned delivery happens to name OUR source, our allocation IS the fresh
			// first transmission the tombstone's freeing licensed, so reap-and-retry is
			// exactly right — where the takeover arm would instead OVERWRITE the
			// tombstone, destroying the same evidence without the confirmed delete.
			//
			// Retired excluded by the precedence rule (F-087 WP3): a record carrying both
			// was transmitted, and reaping it would free an identifier the client holds.
			// It falls to the arms below.
			c.Debugf("MQTT: reaping a cancelled in-flight register on %q at sequence %d", subject, smsg.Sequence)
			// Census C01: gated, and the gate is what makes the precedence rule above
			// mechanical rather than a comment. A pure cancellation tombstone is
			// PUBLISH-state by construction, so mqttSlotDeleteNeverTransmitted admits it
			// and refuses anything past PUBREC — where the identifier is one the client
			// holds.
			if !sess.reapObservedRegister(c, subject, smsg.Sequence, cur, mqttSlotDeleteNeverTransmitted) {
				c.Errorf("MQTT: unable to reap a cancelled in-flight register on %q for client %q",
					subject, sess.id)
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				return false
			}
			// The register is gone; a marker the tombstone's own pair left behind is met
			// under "reap", capture first, so the retried create finds the subject clear.
			as.reapMarkerCaptureFirst(c, mqttMarkerCallerStore, subject, true)
		case mqttSlotDispReapPendingFenceThenRetry:
			// ANOTHER ALLOCATION'S PENDING CANCELLATION FENCE (F-088, consult C4 item 3). Disposed
			// of by its provenance alone, through the one door, at exactly the observed sequence
			// and confirmed; a moved or different occupant is preserved and this allocation is
			// withdrawn. Nothing is ended or released by it: the fence carries no exchange and the
			// ledger holds no charge for it.
			c.Debugf("MQTT: disposing of a pending cancellation fence on %q at sequence %d", subject, smsg.Sequence)
			if !sess.reapObservedRegister(c, subject, smsg.Sequence, cur, mqttSlotDeletePendingFence) {
				c.Warnf("MQTT: unable to dispose of a pending cancellation fence on %q for client %q",
					subject, sess.id)
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				return false
			}
			// A fence has no marker of its own; a stray left by an earlier pair is met under
			// "reap", capture first, so the retried create finds the subject clear.
			as.reapMarkerCaptureFirst(c, mqttMarkerCallerStore, subject, true)
		case mqttSlotDispTerminalCleanupWithdraw:
			// A TERMINAL PUBLISH FOR THIS VERY SOURCE still occupies the subject: its
			// acknowledgement recorded the phase and its delete has not landed, either
			// because it is racing this allocation or because it failed. Reached without
			// a crash at all, so recovery's own branch does not make it unreachable.
			//
			// ORDERED ABOVE THE RETIREMENT ARMS and tested INDEPENDENTLY of `Retired`: a
			// terminal record is not a PUBLISH-side retirement to adopt — adopting it
			// would install a source suppression for an exchange that is already
			// complete — and it is not a live occupant to replace either.
			//
			// The cleanup is attempted through the terminal disposition, identity-bound
			// and confirmed, and REPLACEMENT IS WITHHELD until it is proven: this
			// allocation is withdrawn either way, and the source's next offer meets a
			// clear subject once the delete lands. Writing over an unresolved terminal
			// record would destroy the only durable evidence that the exchange completed.
			if sess.recoverPublishTerminalSlot(c, subject, smsg.Sequence, cur) {
				c.Debugf("MQTT: released the terminal in-flight register on %q before re-allocating",
					subject)
			} else {
				c.Warnf("MQTT: refusing packet identifier %d for client %q: a terminal in-flight "+
					"register on %q at sequence %d could not be released", pi, sess.id,
					subject, smsg.Sequence)
				if tref := mqttSlotRefFromRecord(subject, smsg.Sequence, cur); tref != nil {
					sess.scheduleTerminalRegisterRelease(c, cur.PI, *tref, false)
				}
			}
			sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
			return false
		case mqttSlotDispAdoptRetirement:
			// The occupant proves THIS identifier's exchange for THIS very source is past
			// PUBREC — the client answered under this packet identifier. Replacing it
			// would erase that proof and re-send a completed payload, and abandoning our
			// allocation would let the source's next offer allocate a second identifier.
			// So the retirement is ADOPTED: this allocation never transmits, and the
			// source's suppression answers every later offer (F-087 WP2).
			ok := func() bool {
				sess.mu.Lock()
				defer sess.mu.Unlock()
				if sess.gen != gen {
					// A clear() ran while this reconcile was outside the lock: the maps
					// are already gone and the generation is new. Installing here would
					// write the RETIRING generation's suppression into the reused object.
					// Nothing is installed and nothing is ours to abandon — the retired
					// generation's register belongs to the generation-retirement reap.
					return false
				}
				// Installed FIRST, so the moment the mapping goes a re-offer meets
				// sourceRetiredLocked rather than a fresh allocation.
				sess.retireSourceLocked(jsDur, sseq, &mqttRetiredSource{
					pi:         pi,
					regSeq:     smsg.Sequence,
					nonce:      cur.Nonce,
					q2AffClaim: cur.Q2AffClaim,
					q2AffCopy:  cur.Q2AffCopy,
					durable:    true,
				})
				return true
			}()
			if ok {
				c.Debugf("MQTT: adopting the retirement recorded on %q for delivery %s/%d rather than re-sending it",
					subject, jsDur, sseq)
				// The post-CAS settle ladder, NOT releaseCancelledReservation, whose
				// documented proof ("no CAS was ever issued") this loop has already
				// spent. The ladder covers every residue an ordinary abandon does.
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				// The retirement is the client's PUBREC: protocol evidence that the
				// occupant's PUBLISH was received. Its marker, if one survived, is cleaned
				// under that operation — reload-acquired, so only a body naming the
				// occupant's own exchange is deleted (row 22); anything else is left.
				as.cleanMarkerAfterEvidenceReloaded(c, subject, cur.Nonce,
					as.loadInflightMarker(mqttMarkerCallerEvidence, subject))
			}
			return false
		case mqttSlotDispReplace:
			// Someone else recorded exactly what we are recording, for this very delivery.
			// No claim is compared (F-105 7b): on the authority-backed route the replacement
			// batch compares this connection's accepted entitlement, and the experimental
			// affine route writes with the standalone CAS. Take the slot over at its exact
			// sequence so the record carries OUR
			// nonce, rather than adopting a record whose writer may still delete it. The
			// CAS at the exact sequence read is what makes this sound: if the occupant
			// gains Retired or Cancelled after our read, it rejects and the loop rereads
			// into the arms above.
			gatePassed := mqttSlotAdmitsDisposition(cur,
				mqttSlotRefFromRecord(subject, smsg.Sequence, cur), mqttSlotReplacePublish)
			if !gatePassed {
				// A past-PUBREC slot is this identifier's continuing authority. Replacing
				// it with the new allocation's PUBLISH record would destroy its canonical
				// PUBREL identity and make the answered payload eligible for retransmission.
				// Row 18, which precedes every marker row: the refused gate refuses whatever
				// the occupant's marker would have said, and no marker is read for it.
				if v, verr := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpReplaceOccupant, caller: mqttMarkerCallerReplace},
					mqttRegisterObservation{kind: mqttRegisterOurs, seq: smsg.Sequence}, mqttMarkerObservation{}); verr != nil || v.disposition != mqttMarkerRefuse {
					c.Errorf("MQTT: the marker boundary did not refuse a phase-gated occupant on %q: %v %v", subject, v.disposition, verr)
				}
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				return false
			}
			// The occupant's MARKER decides the shape of the replacement (rows 15-17): a
			// marker naming the occupant is replaced WITH the register in one batch, both
			// CAS'd at their observed sequences, so no interval shows a bare register or a
			// marker naming a record that is gone; an authoritatively absent marker means
			// the occupant was transmitted, and the register alone is rewritten, with no
			// marker minted for an exchange the client may already hold; anything else
			// reserves the identifier as marker-opaque and writes nothing.
			// The probe already taken for a cancelled occupant (above) IS this probe, moved: it is
			// reused, never repeated, so the cancelled shape costs one marker read, as before.
			var occupantMarker mqttMarkerLoad
			if cancelledOccupantMarker != nil {
				occupantMarker = *cancelledOccupantMarker
			} else {
				occupantMarker = as.loadInflightMarker(mqttMarkerCallerReplace, subject)
			}
			v, verr := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpReplaceOccupant, caller: mqttMarkerCallerReplace, gatePassed: true},
				mqttRegisterObservation{kind: mqttRegisterOurs, seq: smsg.Sequence},
				occupantMarker.observe(mqttMarkerReloaded, cur.Nonce))
			if verr != nil {
				c.Errorf("MQTT: replacing the in-flight register on %q for client %q: %v", subject, sess.id, verr)
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				return false
			}
			// THE TWO REPLACEMENT MODES DECIDE THE LINEAGE, and they decide it OPPOSITE ways,
			// because the boundary has just partitioned the occupant into the two states that
			// matter to a client obligation.
			//
			// A marker that NAMES the occupant is proof it was PROVEN-UNSENT: nothing was
			// ever put on the socket under it, so there is no client obligation to inherit
			// and no durable fact can have been recorded about one. That is a fresh exchange,
			// and its lineage is its own.
			//
			// An authoritatively ABSENT marker is the other side, and it is deliberately NOT
			// read as "transmitted": the send intent deletes the marker BEFORE the bytes
			// leave, so absence means POSSIBLY-ATTEMPTED. That is precisely the state this
			// arm already treats as an exchange the client may be holding — it mints no
			// marker for it and preserves its QoS below for that reason — so the obligation
			// continues and its lineage continues with it.
			rec.SourceAckReply = cur.SourceAckReply
			switch v.disposition {
			case mqttMarkerReplacePair:
				rec.Lin, _ = mqttLineageForWrite(mqttLineageWritePair, cur, rec.Nonce)
				regExpect, markerExpect, registerOnly = smsg.Sequence, occupantMarker.seq, false
			case mqttMarkerReplaceRegisterOnly:
				// INHERITED, for the reason the QoS preservation below states: the occupant
				// is evidence about what the client may already be holding. Lin only: the
				// replacement composer applies the occupant's Acq itself, and the affine
				// writer's raw CAS stores this record verbatim (mqttLineageForWrite).
				rec.Lin, _ = mqttLineageForWrite(mqttLineageWriteRegisterOnly, cur, rec.Nonce)
				regExpect, markerExpect, registerOnly = smsg.Sequence, 0, true
			default:
				// Withhold (row 17): the occupant stands, our allocation becomes the
				// marker-opaque reservation over it, and allocation is withheld until a
				// resume reads the pair whole.
				c.Warnf("MQTT: withholding packet identifier %d for client %q: the unsent marker beside %q could not be read as the occupant's or as absent",
					pi, sess.id, subject)
				// cur.Lin, not rec.Lin: this reservation stands over the OCCUPANT, so it
				// must name the occupant's obligation. The failed proposal's lineage would
				// identify an exchange this entry is not reserving.
				sess.installMarkerOpaqueReservation(c, pi, jsDur, sseq, nonce, mqttEvidenceFromRecord(gen, cur), cur.QoS, smsg.Sequence, subject,
					"the unsent marker beside a same-delivery occupant could not be read", true)
				return false
			}
			c.Debugf("MQTT: replacing an in-flight register for the same delivery on %q written elsewhere", subject)
			// The occupant's QoS is PRESERVED, not overwritten with ours. This writer
			// cannot prove the occupant was never transmitted -- Cancelled is not
			// exhaustive evidence, since a cancellation may live only as a nonce in the
			// session record's CancelledRegs -- so a live PUBLISH-phase occupant for this
			// exact delivery is evidence about what the CLIENT may already be holding,
			// and its QoS is the only durable trace of it. Taking ours instead destroys
			// that trace, and for 2 -> 1 it licenses the slot deletion that abandons the
			// exactly-once handshake.
			//
			// The cost, accepted deliberately: if the occupant genuinely was never
			// transmitted, this first transmission carries a QoS the current grant no
			// longer matches, deviating from [MQTT-3.8.4-6]. A graded-QoS inaccuracy on
			// one message is preferred to an abandoned handshake, i.e. duplication or
			// loss. Recorded in docs/CONFORMANCE.md.
			//
			// Assigned HERE rather than at the qos argument because rec was built once
			// before this retry loop: mutating the argument would not change the record
			// that is about to be written at the occupant's exact sequence.
			//
			// What this does NOT do is keep the inherited exchange alive. A concurrent
			// cancellation or a failed store still destroys or abandons it -- those are
			// pre-existing lifecycle defects of this arm, filed as their own roadmap
			// item, and no claim here may say otherwise.
			rec.QoS = cur.QoS
		case mqttSlotDispAbandon, mqttSlotDispReapCompleteThenRetry:
			// AN OCCUPANT NAMING ANOTHER DELIVERY, and the two proofs this arm — and only
			// this arm — pays for. Source completion authorizes NOTHING on its own: the
			// identifier belongs to the CLIENT exchange, and the two part company exactly
			// when a PUBLISH went out and its acknowledgement did not come back.
			//
			// The first classification could only say ABANDON, because it was not given
			// those proofs; this is where they are established and the verdict re-taken.
			// Both dispositions share the case for that reason: which of them applies is
			// not decidable until the reads below have happened.
			if as.inflightSourceState(cur) != mqttInflightSourceComplete {
				// A live or unprovable delivery owns this identifier. Never reaped: on
				// another broker this record is that delivery's only durable identity.
				c.Warnf("MQTT: refusing packet identifier %d for client %q: %q still names delivery %s/%d",
					pi, sess.id, subject, cur.Dur, cur.Seq)
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				return false
			}
			// Provably finished: a register whose delete was lost. Reaping must be
			// CONFIRMED — an unconfirmed reap makes the retried create fail identically
			// forever — and must target the exact sequence just observed (R4). No claim
			// gate here, deliberately: a provably COMPLETE source means the exchange
			// terminated, and identifier continuity protects only incomplete exchanges —
			// the claim holder's own terminal path is issuing this same delete.
			c.Debugf("MQTT: reaping a stale in-flight register on %q at sequence %d (delivery %s/%d is complete)",
				subject, smsg.Sequence, cur.Dur, cur.Seq)
			//
			// Through the verdict property, with confirmation FORCED: this arm's
			// unconditional confirmation is its own existing semantics, and with the
			// Retired-dominates precedence in place a different-source record carrying
			// both bits now reaches this reap — a delete that would otherwise strand an
			// attached cancellation verdict systematically, no crash required (F-087).
			// Census C04: gated. A record for a DIFFERENT source whose own source is
			// provably complete is reapable — but only from a state where reaping frees
			// nothing the client holds, which is what the disposition asserts.
			//
			// C-II, AND THIS IS THE EDGE THE CONTRACT EXISTS FOR: the source being
			// complete authorizes NOTHING here. Source completion is a fact about the
			// DELIVERY; the identifier belongs to the CLIENT exchange, and the two part
			// company exactly when a PUBLISH went out and its acknowledgement did not
			// come back. Reaping on source completion alone frees an identifier the
			// receiver may still be holding, which is the release recovery's own rule
			// has always refused (`possibly-attempted is RESERVED`) and which this arm
			// was still performing.
			//
			// So the authority is asked for, not assumed. Only PROVEN-UNSENT releases:
			// the marker is present for this exact allocation, or a cancellation names
			// it, with no contradicting retirement and no live action. A TERMINAL
			// occupant is deliberately NOT reaped here either — terminal cleanup owes a
			// duplicate scan, a canonical acknowledgement and affine disposal that this
			// arm cannot discharge — and everything else RESERVES.
			ev, evRec, evSeq := sess.mqttSlotEvidenceFor(subject,
				mqttSlotIdentityOf(subject, cur))
			auth := mqttSlotReleaseAuthority(ev)
			slotObs.sourceComplete = true
			slotObs.provenUnsent = evRec != nil && evSeq == smsg.Sequence &&
				auth == mqttSlotProvenUnsentAuthority
			slotVerdict = mqttClassifyInflightSlot(slotObs, slotIntent)
			if slotVerdict.disposition != mqttSlotDispReapCompleteThenRetry {
				c.Warnf("MQTT: refusing packet identifier %d for client %q: the register on %q "+
					"names a complete delivery %s/%d but cannot be proven never transmitted, "+
					"so the identifier it holds is reserved rather than reused",
					pi, sess.id, subject, cur.Dur, cur.Seq)
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				return false
			}
			if !sess.reapObservedRegister(c, subject, smsg.Sequence, cur, mqttSlotDeleteComplete) {
				c.Errorf("MQTT: unable to reap a stale in-flight register on %q for client %q",
					subject, sess.id)
				sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
				return false
			}
			// The register is gone; its marker, if the lost delete left one, is met under
			// "reap", capture first.
			as.reapMarkerCaptureFirst(c, mqttMarkerCallerStore, subject, true)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("in-flight register not resolved after %d attempts", mqttInflightStoreAttempts)
	}
	c.Errorf("MQTT: unable to record the in-flight register for packet identifier %d of client %q: %v",
		pi, sess.id, lastErr)
	sess.abandonInflightRegister(mqttLicenceOf(c), pi, jsDur, sseq, nonce)
	return false
}

// mqttInflightWrite performs one attempt's durable write for an in-flight admission, in the mode
// the ladder chose: a create-only pair (regExpect 0 and not registerOnly), a replacement pair at
// both observed sequences, or a register-only replacement. cur is the occupant a replacement
// rewrites (nil on a create), rec the record to write, and subject its address. It returns the
// authority/register sequence the batch landed at. The pre-F-105 caller writes the register and
// its marker alone; the F-105 admission writes them beside the authority, exchange and resource
// edges (opAdmitPublish / the replacement composer) — the ONLY difference between the two routes.
type mqttInflightWrite func(regExpect, markerExpect uint64, registerOnly bool, cur, rec *mqttInflightRecord, subject string) (uint64, error)

// storeInflightRegister is the pre-F-105 admission: it makes a freshly allocated identifier
// durable through the standalone register-and-marker write, with no authority, exchange or
// resource edges beside it. It runs the shared ladder (runInflightAdmission) with the pair/CAS
// writer. Its sole remaining production caller is the experimental affine route (chapter P); the ordinary and shared QoS 1/2 routes are
// converted onto admitInflightPublish, which runs the SAME ladder with the authority-carrying
// write.
func (sess *mqttSession) storeInflightRegister(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim, q2AffCopy string) bool {
	return sess.runInflightAdmission(c, pi, qos, jsDur, sseq, nonce, q2AffClaim, q2AffCopy,
		func(regExpect, markerExpect uint64, registerOnly bool, _, rec *mqttInflightRecord, subject string) (uint64, error) {
			if registerOnly {
				return sess.asm.storeInflightCAS(subject, regExpect, rec)
			}
			return sess.asm.storeInflightPair(subject, regExpect, markerExpect, rec)
		})
}

// confirmInflightRegister stamps a confirmed register onto its pending entry and
// clears regPending, releasing the entry for transmission.
//
// If the entry has gone (a discard, a takeover, or a terminal path that ran while the
// store was in flight) the register is deleted immediately: it names an identifier
// nothing is tracking, and leaving it would make the next resume reserve an
// identifier for a delivery that has no pending entry.
//
// markerSeq and transmitted are the entry's send state as the boundary derived it from
// the landed pair (rows 1 and 16): a marker captured at step 3 and transmitted=false, or
// none held and transmitted=true for a register-only replacement.
func (sess *mqttSession) confirmInflightRegister(c *client, l mqttLandedRegister, markerSeq uint64, transmitted bool) bool {
	pi, jsDur, sseq, regSeq, nonce := l.pi, l.jsDur, l.sseq, l.regSeq, l.nonce
	sess.mu.Lock()
	ack := sess.pendingPublish[pi]
	idHash, gen := sess.idHash, sess.gen
	if ack == nil || !ack.regPending || ack.jsDur != jsDur || ack.sseq != sseq {
		sess.mu.Unlock()
		// The allocation was CANCELLED under us — a packet staging a drain, a
		// discard, a takeover — so this register describes a delivery that will never be
		// sent. Removing it is CONFIRMED and retried, unlike the ordinary terminal delete
		// which may be fire-and-forget: there the record is reconcilable, because its
		// source is still live and classifiable. Here the consumer is usually being deleted
		// in the same breath, and a register whose consumer is gone is deliberately NOT read
		// as complete — so a lost delete leaves an identifier reserved with no later pass
		// able to resolve it. Recovery's marker-proven drop (F-102) is not that pass for
		// this shape: it releases a register whose MARKER still proves non-transmission,
		// and a cancelled allocation is exactly the case where that evidence is gone.
		//
		// proven means SOME durable rung of the ladder holds: the register is gone, or it
		// is tombstoned, or its nonce verdict landed on the session record. The
		// reservation the teardown left on this identifier is released only then — the
		// store has settled here, which is the other half of the release condition, but a
		// PI freed with no durable verdict anywhere would let a crash resurrect this
		// register as an ordinary delivery holding a Receive Maximum slot forever.
		proven := false
		// What actually stands may be our OWN pending fence rather than the register-only
		// replacement this attempt meant (the cancellation's create-only tombstone carries our
		// nonce): a fence describes no transmission, so it takes the disposal path below, where
		// the door disposes of it under its own rule (F-088, consult C4 item 3).
		if transmitted && !sess.subjectHoldsOwnPendingFence(idHash, gen, pi, nonce) {
			// A REGISTER-ONLY REPLACEMENT LANDED (F-088, WP3 edit 5): it inherited an occupant
			// whose PUBLISH may already be on the wire — that is why no marker was minted. The
			// cancellation was decided against OUR allocation and says nothing about that
			// inherited transmission, so nothing here destroys or tombstones the register: it is
			// the only durable carrier of what the client may be holding, a late acknowledgement
			// completes it, and the custody ledger holds its identifier and charge until then
			// (or a resume restores it as a possibly-attempted delivery). The store is settled,
			// so our reservation goes, keeping the identifier excluded.
			sess.mu.Lock()
			if cur := sess.pendingPublish[pi]; cur != nil && cur.regCancelled && cur.regNonce == nonce {
				sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), nonce)
				delete(sess.pendingPublish, pi)
			}
			sess.mu.Unlock()
			return false
		}
		if sess.asm != nil {
			var slot *mqttInflightSlotRef
			if name, perr := mqttParseInflightSubject(mqttInflightSubject(idHash, gen, pi)); perr == nil {
				slot = &mqttInflightSlotRef{
					name: *name, seq: regSeq, nonce: nonce, state: mqttInflightPublish,
					dur: jsDur, sseq: sseq,
				}
			}
			if deleted := sess.reapRegisterWithVerdictGated(regSeq, nonce, true, slot,
				mqttSlotDeleteNeverTransmitted, mqttLicenceOf(c)); !deleted {
				derr := fmt.Errorf("the landed register could not be identity- and phase-confirmed as never transmitted")
				// The delete is what this path wants, and it could not be proven. So the fact
				// is made DURABLE instead: a cancellation tombstone on the record itself,
				// which for THIS path is the only thing that can tell a later resume the
				// delivery was never transmitted — the marker proof F-102 uses is not
				// available here, the cancellation having consumed it. Without the tombstone
				// the surviving register is indistinguishable from a started delivery, and —
				// since the consumer that would classify its source is deleted in the same
				// breath — its identifier would be reserved,
				// and its Receive Maximum slot consumed, for the life of the session.
				outcome, merr := sess.markInflightRegisterCancelled(c, pi, jsDur, sseq, regSeq, nonce, mqttLicenceOf(c))
				if merr != nil {
					// Neither register-stream rung could be proven — one storage domain, so
					// they usually fail together. The NONCE verdict is the remaining home:
					// kept in memory for this connection and persisted with the session
					// record, so a restart still knows this register describes a delivery
					// that was never transmitted.
					c.Warnf("MQTT: deferring cleanup of the in-flight register of cancelled packet identifier %d for client %q: %v (%v)",
						pi, sess.id, derr, merr)
					sess.mu.Lock()
					staged := sess.stageCancellationVerdictLocked(mqttLicenceOf(c), nonce)
					sess.mu.Unlock()
					proven = staged && sess.persistVoidedRegs(c)
				} else {
					// Same per-outcome licence as the sibling caller: only the arms that
					// actually establish the fact retire the verdict. `proven` is set
					// either way — this path's DURABLE obligation is settled by any
					// non-refusing outcome, and what the classification gates is the
					// evidence, not the settlement.
					if mqttRegCancelRetiresVerdict(outcome) {
						sess.forgetCancelledReg(nonce)
					}
					proven = true
				}
			} else {
				// Provably gone — but "the delete returned nil" is NOT that proof (F-104,
				// the OUTER bypass). deleteInflightRegisterConfirmed succeeds for an
				// already-absent sequence, so a same-identity rewrite landing between the
				// load and the delete reaches here looking exactly like a removal. The
				// verdict is therefore retired only against a CONFIRMED ABSENCE of the
				// subject; anything else keeps it, which is the recoverable direction.
				//
				// `proven` is unaffected: the delete DID settle this path's durable
				// obligation, and the identifier's own reservation is released on that
				// footing exactly as before. What the confirmation gates is the verdict,
				// which is evidence a later pass may still need.
				// The comparison identity is built from what this path KNOWS — the nonce and
				// the source delivery it just wrote — which is exactly the triple
				// confirmSlotReleased compares. No record was loaded here and none needs to
				// be: a successor on this subject differs in at least one of the three.
				if slot != nil {
					observed := &mqttInflightRecord{Nonce: nonce, Dur: jsDur, Seq: sseq}
					if sess.confirmSlotReleased(slot.name.subject, observed) == mqttReleaseEstablishedAbsent {
						sess.forgetCancelledReg(nonce)
					}
				}
				proven = true
				// The register half is proven, so the marker half follows (N-5.8): at the
				// sequence step 3 captured, or — a register-only replacement, whose marker
				// was authoritatively absent — by capturing whatever is there first.
				if slot != nil {
					if markerSeq != 0 {
						sess.asm.reapMarkerCaptured(c, mqttMarkerCallerCleanup, slot.name.subject, nonce, markerSeq)
					} else {
						sess.asm.reapMarkerCaptureFirst(c, mqttMarkerCallerCleanup, slot.name.subject, false)
					}
				}
			}
		}
		sess.mu.Lock()
		// Only OUR reservation: after a stall the identifier may already belong to a
		// newer allocation's cancellation, whose own settle point owes this decision.
		if cur := sess.pendingPublish[pi]; cur != nil && cur.regCancelled && cur.regNonce == nonce {
			if proven || sess.voidVerdictProvenLocked(nonce) {
				// The identifier's exclusion outlives the entry unless the subject was
				// CONFIRMED absent above: `proven` also covers a tombstoned register that is
				// still there, and voidVerdictProvenLocked is true for an ABSENT verdict,
				// neither of which says a peer cannot be transmitting (F-098).
				sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), nonce)
				// Proven by this ladder, or by a rung an earlier pass ran (a securing
				// tombstone retires the nonce, and the save-keyed release can never
				// fire for a nonce that is no longer in the set).
				delete(sess.pendingPublish, pi)
			} else {
				// SETTLED but unproven: the store is over, so regPending drops, but the
				// identifier stays held until the verdict's save lands —
				// persistVoidedRegs releases these on proof. See
				// releaseSettledVoidReservationsLocked.
				cur.regPending = false
				sess.parkSettledVoidReservationLocked(nonce, pi)
			}
		}
		sess.mu.Unlock()
		return false
	}
	// THE LANDING PROMOTES THE ALLOCATION, by replacement: the promoted entry carries what
	// LANDED (the QoS and lineage the replacement arm may have taken from the occupant, so the
	// values this attempt proposed are not authoritative), the send state the pair licenses —
	// the ONLY place a fresh allocation acquires it — and the slot's durable identity CAPTURED
	// at the moment it becomes known, which every later destructive path addresses the slot
	// through rather than rebuilding a subject from whatever the session looks like then.
	var slot mqttInflightSlotRef
	name, perr := mqttParseInflightSubject(mqttInflightSubject(sess.idHash, sess.gen, pi))
	if perr == nil {
		slot = mqttInflightSlotRef{
			name: *name, seq: regSeq, nonce: nonce, state: mqttInflightPublish,
			dur: ack.jsDur, sseq: ack.sseq, claim: sess.regClaim, claimSeq: sess.seq,
		}
	}
	next := mqttPromoteLanded(ack, l, markerSeq, transmitted, slot, perr == nil)
	if !sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectExactly(ack), next) {
		// Custody no longer holds the landed lineage: its ending was accepted while the store
		// was in flight, so this exchange is over and its projection may not exist. The
		// allocation and its source mapping go; the ended exchange's own records are its
		// cleanup's.
		sess.untrackPublishEntry(pi, mqttUntrackEnded)
		sess.mu.Unlock()
		return false
	}
	sess.mu.Unlock()
	return true
}

// mqttTestClaimRecoveredHook observes every batch the affine claim pass receives, so a fixture can
// witness that the recovery partition diverted a pending fence BEFORE it (F-088 T33). Nil in
// production.
var mqttTestClaimRecoveredHook func(in []mqttRecoveredInflightReg)

// subjectHoldsOwnPendingFence reports whether pi's subject in generation gen holds the pending
// cancellation fence of allocation nonce. A read that does not resolve answers false, which
// keeps the caller on its conservative (preserving) path.
//
// Session lock NOT held on entry.
func (sess *mqttSession) subjectHoldsOwnPendingFence(idHash, gen string, pi uint16, nonce string) bool {
	if sess.asm == nil || idHash == _EMPTY_ || gen == _EMPTY_ || pi == 0 || nonce == _EMPTY_ {
		return false
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	cur, smsg, lerr := sess.asm.loadInflightRecord(subject)
	if lerr != nil || smsg == nil || cur == nil {
		return false
	}
	return cur.Nonce == nonce && mqttValidPendingFence(subject, cur)
}

// settleLandedRegister is the store path's step 3 and the derivation of the entry's send
// state from what LANDED (rows 1-3, and row 16's consequence).
//
// A register that landed alone — a register-only replacement — records an exchange the
// client may already hold: none held, transmitted, DUP=1 on the next send. A register
// that landed as a pair has its marker loaded AUTHORITATIVELY and cached before any send
// is authorized; the marker's sequence is never derived from the register's
// acknowledgement (T22b). A marker that reads as ours installs the unsent entry; one that
// reads absent, as another exchange's, unreadable, or not at all installs the
// marker-opaque reservation — "the register is there and its marker is not" is exactly
// the shape a later resume reads as transmitted, and this process cannot tell whether
// that reading would be true, so nothing may go out under the identifier until a resume
// reads the pair whole (N-7.2). Never repaired by creating the missing side.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleLandedRegister(c *client, l mqttLandedRegister, subject string, registerOnly bool) bool {
	pi, regSeq, nonce := l.pi, l.regSeq, l.nonce
	if registerOnly {
		return sess.confirmInflightRegister(c, l, 0, true)
	}
	as := sess.asm
	load := as.loadInflightMarker(mqttMarkerCallerStore, subject)
	v, verr := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpDeriveSendState, caller: mqttMarkerCallerStore},
		mqttRegisterObservation{kind: mqttRegisterOurs, seq: regSeq}, load.observe(mqttMarkerReloaded, nonce))
	if verr != nil {
		// A programming error at this caller, not a stream state. Fail closed the same
		// way an unreadable marker does.
		c.Errorf("MQTT: deriving the send state of packet identifier %d for client %q: %v", pi, sess.id, verr)
		v.disposition = mqttMarkerWithhold
	}
	switch v.disposition {
	case mqttMarkerInstallUnsent:
		return sess.confirmInflightRegister(c, l, load.seq, false)
	default:
		why := "its unsent marker could not be read"
		switch load.kind {
		case mqttInflightResponseAbsent:
			why = "its unsent marker is absent although the pair was just committed"
		case mqttInflightResponsePresent:
			why = "its unsent marker does not name this exchange"
		}
		c.Warnf("MQTT: withholding packet identifier %d for client %q: the register on %q landed at sequence %d but %s",
			pi, sess.id, subject, regSeq, why)
		if installed, refused := sess.installMarkerOpaqueReservation(c, pi, l.jsDur, l.sseq, nonce,
			mqttEvidenceFromLanding(l), l.qos, regSeq, subject, why, false); installed || refused {
			return false
		}
		// The allocation is no longer ours to reserve — cancelled under us — so the landed
		// register is settled by the confirmation's own ladder, which finds the entry gone.
		return sess.confirmInflightRegister(c, l, load.seq, false)
	}
}

// installMarkerOpaqueReservation converts our unconfirmed allocation into the
// MARKER-OPAQUE reservation (N-7.2): the identifier stays reserved, nothing may be sent or
// settled under it, the source mapping is dropped, and the session's identifier space is
// marked incomplete so no NEW identifier is minted until a resume reads the pair whole.
// recNonce is the exchange the durable record actually carries — ours for a landed pair,
// the occupant's when a same-delivery replacement was withheld — so the reservation names
// the record a resume will find.
//
// Only while the entry is still the allocation this attempt owns; reports whether it was.
//
// Session lock NOT held on entry.
func (sess *mqttSession) installMarkerOpaqueReservation(c *client, pi uint16, jsDur string, sseq uint64,
	ourNonce string, ev mqttPendingEvidence, qos byte, regSeq uint64, subject, why string, occupant bool) (installed, refused bool) {
	mqttRunTestSeam(&mqttTestBeforeMarkerOpaqueConversionV)
	sess.mu.Lock()
	ack := sess.pendingPublish[pi]
	if ack == nil || !ack.regPending || ack.regCancelled || ack.jsDur != jsDur || ack.sseq != sseq || ack.regNonce != ourNonce {
		sess.mu.Unlock()
		return false, false
	}
	if !sess.markerOpaqueEligibleLocked(ack, ev, occupant) {
		// The obligation this reservation would stand over is not one the session may reserve
		// for: the landed exchange's ending was accepted, or custody history moved under a
		// withheld replacement. LOCAL cleanup only — this attempt's allocation and its own
		// mapping; nothing landed for it, so there is no register of ours to reap, and the
		// occupant's record is not ours to touch. The source's redelivery re-admits against
		// whatever the ledger then says. (A pending store: Unknown evidence, no binding.)
		sess.untrackPublishEntry(pi, mqttUntrackUnanswered)
		sess.mu.Unlock()
		return false, true
	}
	// The reservation names the obligation it stands over, or it would be the one entry that
	// cannot say which exchange it is holding an identifier for. Its two callers supply
	// different evidence deliberately: the landed record's after a commit, the OCCUPANT's
	// when the replacement withheld. The durable obligation's QoS is kept even though
	// confirmation was withheld.
	var slot mqttInflightSlotRef
	name, perr := mqttParseInflightSubject(subject)
	if perr == nil {
		slot = mqttInflightSlotRef{
			name: *name, seq: regSeq, nonce: ev.nonce, state: mqttInflightPublish,
			dur: jsDur, sseq: sseq, claim: sess.regClaim, claimSeq: sess.seq,
		}
	}
	next := mqttNewMarkerOpaque(ack, ev, regSeq, qos, slot, perr == nil)
	if !sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectExactly(ack), next) {
		sess.mu.Unlock()
		return false, false
	}
	// No source mapping: the reservation holds the identifier, not the delivery. A
	// re-offer of the source meets the incomplete identifier space and is declined.
	if m := sess.cpending[jsDur]; m != nil && m[sseq] == pi {
		delete(m, sseq)
		if len(m) == 0 {
			delete(sess.cpending, jsDur)
		}
	}
	// The incomplete identifier space is committed in THIS section, with the reservation: a
	// mark set after the unlock could land after a resume that already read the pair whole
	// and cleared it, withholding allocation for an identifier space that is known.
	first := sess.markPISpaceIncompleteLocked()
	sess.mu.Unlock()
	mqttRunTestSeam(&mqttTestAfterMarkerOpaqueConversionV)
	if first {
		sess.warnPISpaceIncomplete(c, fmt.Sprintf("packet identifier %d is reserved with an unreadable transmission state: %s", pi, why))
	}
	return true, false
}

// markerOpaqueEligibleLocked decides whether a marker-opaque reservation may be installed over
// the obligation ev names. The slot-only class has no exchange custody at all, so it always may.
// For an ordinary exchange the two routes stand on different facts:
//
//   - LANDED: the exchange is our own admission's, which the ledger holds from landing until an
//     accepted ending; a released lineage reserves nothing.
//   - OCCUPANT (a withheld same-delivery replacement): the occupant's lineage was not a holder at
//     allocation — the allocator never hands out an identifier the ledger holds — and that fact
//     is carried to here by the allocation's fence: same generation, connection and ledger, and
//     no holder created since. With the fence intact the lineage was never held in between, so
//     no ending of it can have been accepted. Anything else refuses.
//
// Lock held on entry.
func (sess *mqttSession) markerOpaqueEligibleLocked(alloc *mqttPending, ev mqttPendingEvidence, occupant bool) bool {
	if mqttDurIsSlotOnlyClass(alloc.jsDur) {
		return true
	}
	if !occupant {
		return sess.ordinaryHeldLocked(ev)
	}
	f := alloc.allocFence
	return f.valid && f.gen == sess.gen && f.c == sess.c && f.ledger == sess.custody &&
		sess.custody != nil && f.holderEpoch == sess.custody.holderEpoch &&
		!sess.custody.hasExchange(sess.gen, ev.lin)
}

// ownAllocationCancelled reports whether the allocation this admission owns — the entry at pi
// carrying nonce — has been cancelled while its store was in flight.
//
// Session lock NOT held on entry.
func (sess *mqttSession) ownAllocationCancelled(pi uint16, nonce string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	ack := sess.pendingPublish[pi]
	return ack != nil && ack.regNonce == nonce && ack.regCancelled
}

// abandonInflightRegister releases an identifier whose register could not be made
// durable, but ONLY while it is still the unconfirmed allocation this attempt owns.
//
// The guard matters: while the store was in flight the entry may have been replaced
// (a redelivery cannot take it — regPending declines that — but a discard or a
// takeover can), and removing whatever now holds the identifier would take out state
// this attempt has no claim on.
//
// The sequence is then recorded as DEFERRED, which is not bookkeeping tidiness: it is
// what keeps the DUP flag honest. Nothing was transmitted here, so the eventual first
// transmission must carry DUP=0 ([MQTT-3.3.1-1]) — but its AckWait redelivery arrives
// with a JetStream delivery count above one, and trackPublish derives dup from that
// count unless the sequence is known to have been withheld. This is the same state, for
// the same reason, as a delivery withheld at the in-flight cap, so it uses the same
// marker; the NAK it may attract simply retries the register sooner.
//
// lic is the licence of the admitting connection whose ladder gives up (its callers'
// mqttLicenceOf(c), read before the session lock): a continuity record's withdrawal carries it.
func (sess *mqttSession) abandonInflightRegister(lic mqttDestroyLicence, pi uint16, jsDur string, sseq uint64, nonce string) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	ack := sess.pendingPublish[pi]
	if ack == nil || !ack.regPending || ack.regCancelled || ack.jsDur != jsDur || ack.sseq != sseq || ack.regNonce != nonce {
		// A cancelled reservation fails this check by design. The store this attempt
		// owned is over — but one of its failed attempts may have LANDED ambiguously, so
		// a register can exist with no proven rung. Mark the reservation settled and let
		// the verdict's save release it (releaseSettledVoidReservationsLocked); freeing
		// the identifier here would let a crash resurrect that register as an ordinary
		// delivery.
		//
		// UNLESS the verdict is already retired: a nonce absent from the set was proven
		// by a register-stream rung (a landed tombstone, or a confirmed delete) while the
		// store was still out. The save-keyed release can never fire for it — there is
		// nothing left to save — so withholding here would park the reservation, and its
		// Receive Maximum slot, for the life of the session.
		//
		// And only OUR reservation, by nonce: after a stall this identifier may already
		// carry a newer allocation's cancellation, whose settle point owes this decision.
		if ack != nil && ack.regCancelled && ack.regNonce == nonce {
			if sess.voidVerdictProvenLocked(nonce) {
				// As the ladders above: verdict durability is not proof the register is gone.
				sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), nonce)
				delete(sess.pendingPublish, pi)
			} else {
				ack.regPending = false
				sess.parkSettledVoidReservationLocked(nonce, pi)
			}
		}
		return
	}
	ackSubject := ack.jsAckSubject
	// The ordinary abandonment: the entry goes, and with it any local record of what this
	// identifier was doing. Whether a register landed is exactly what an abandonment cannot
	// say — the CAS may have been in flight — so the identifier is excluded until a
	// confirmed absence settles it (F-098).
	sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), ack.regNonce)
	sess.untrackPublishEntry(pi, mqttUntrackUnanswered)
	// AN ADMISSION THAT MAY HAVE LANDED KEEPS ITS SOURCE (F-097 WP5, witnessed by W5b): with the
	// ledger reporting the attempt landed or unsettled, the source's next offer is refused until
	// the continuity record decides the first admission; it is then re-offered, never doubled.
	if sess.installAdmissionContinuityLocked(pi, jsDur, sseq, nonce, ack.ev.lin, ack.kind == mqttPendingRetained,
		ackSubject, lic) {
		return
	}
	if ackSubject == _EMPTY_ {
		return
	}
	if sess.deferred == nil {
		sess.deferred = make(map[mqttDeferredKey]string)
	}
	sess.deferred[mqttDeferredKey{jsDur, sseq}] = ackSubject
}

// mqttStagedRelease is a recovery release the resume decided on, held until the resume has
// actually succeeded.
//
// The decision is made inside the pre-CONNACK barrier, where scan absence is the durable
// decision because the complete recovery walk and the duplicate-source reap follow it.
// The APPLICATION waits, because the walk runs long before the success CONNACK and every
// refusal after it preserves the session object and its maps — releasing at the walk
// would free an identifier on a resume that never succeeded (F-087 WP2).
//
// Frame-local by construction: the CONNECT frame holds the slice and every abort path
// drops it. Session-resident staging would let an aborted resume's decision be committed
// by a LATER resume whose own walk never observed the absence.
type mqttStagedRelease struct {
	recovery *mqttRecoveryRelease
}

// commitStagedReleases applies the recovery-reservation releases a resume's walk staged,
// and is called only once that resume has SUCCEEDED — past the success CONNACK, past the
// forced leave and its refusals, immediately before the restore verdict opens.
//
// Each release is re-fenced on what its capture recorded (recoveryReleaseCurrentLocked):
// the same client binding, generation, stream epoch and walk, completed-apply provenance,
// a complete identifier space, a custody ledger not withholding allocation, no custody or
// disposal hold on the identifier, and the same reservation entries at the same captured
// register sequences with no send section. A release hands the identifier to exclusion
// rather than freeing it outright.
//
// A fence MISS keeps the reservation; a later resume can reconsider it and stage an
// eligible absence.
//
// Session lock NOT held on entry.
func (sess *mqttSession) commitStagedReleases(c *client, staged []mqttStagedRelease) {
	if len(staged) == 0 {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, sr := range staged {
		if sr.recovery != nil {
			sess.commitRecoveryReleaseLocked(c, sr.recovery)
		}
	}
}

// oversizeDisposalAllowed decides what a Maximum Packet Size refusal may do with the entry,
// from the section's token — the entry's send state as captured under the lock. Refusing
// THIS attempt proves only that THIS attempt wrote nothing. A PROVEN-UNSENT entry (marker
// held, no intent unresolved) has never been attempted at all, so discarding it and
// completing the delivery is safe and is today's behaviour (Spec5 [3.1.2.11.4]). A
// POSSIBLY-ATTEMPTED entry — transmitted, marker none held, or its intent unresolved — may
// have reached the receiver under this identifier on an earlier attempt, so it is RETAINED
// whole: entry, register, source, identifier. Nothing is acknowledged, so AckWait
// re-offers it and each offer is refused again; the exchange ends only by the client's own
// acknowledgement of that earlier attempt, or with the session. That is the cost the
// conservative reading accepts: a stuck identifier, never a re-minted one.
func (sess *mqttSession) oversizeDisposalAllowed(c *client, pi uint16, tok mqttSendToken) bool {
	if !tok.dup && tok.markerSeq != 0 && !tok.intentUnresolved {
		return true
	}
	if c != nil {
		c.Debugf("MQTT: PUBLISH for packet identifier %d of client %q exceeds the client's Maximum Packet Size but may already have been attempted; retaining it and its identifier",
			pi, sess.id)
	}
	return false
}

// unresolvedReservationLocked reports whether this entry's disposition is UNRESOLVED —
// recovery could not say what it is — as opposed to one of the positively classified
// phases.
//
// The distinction is the whole of the restriction rule below: a KNOWN phase, including a
// recovered PUBREL reserving its source, restricts nothing and must be allowed to complete
// on its own evidence. Only an entry recovery could not classify can hide an obligation the
// claim floor therefore failed to reconstruct.
//
// sess.mu must be held.
func unresolvedReservationLocked(ack *mqttPending) bool {
	if ack == nil {
		return false
	}
	// The unresolved shape is identified POSITIVELY: an entry that names a durable REGISTER
	// but no SOURCE. That is exactly what recovery installs for an unreadable or
	// illegal-shape register — a bare identifier reservation, regSeq set with no jsDur and
	// no sseq, and neither flag set, because it is not another owner's and its marker was
	// never reached. Nothing else in the tree has that shape.
	//
	// It is stated this way round after two holistic findings in a row on the same class.
	// The first said the flags alone missed the reservation; testing "no source identity"
	// instead then swept in TWO positively classified shapes that also lack identity — a
	// CANCELLED allocation, whose durable and sequence the untrack clears, and a RETAINED
	// replay, which never had a source because it is not a stream delivery at all. Both
	// were read as session-wide uncertainty, so unrelated drains stopped retiring and an
	// ordinary teardown staged an empty drain instead of destroying its consumer.
	//
	// Enumerating a third exception would have invited a fourth. Requiring the register is
	// what closes the class: a reservation exists because a register was found, so the
	// register is the thing that is actually unaccounted for, and no entry created by an
	// ordinary send path carries one without a source.
	if ack.regSeq != 0 && (ack.jsDur == _EMPTY_ || ack.sseq == 0) {
		return true
	}
	return ack.markerOpaque || ack.regForeign
}

// applyProvenanceCompleteLocked reports whether the LATEST register-recovery attempt on this
// session object ran its apply phase to completion.
//
// Equality means either the incarnation's initial baseline or a completed latest apply, and
// both authorize attributing uncertainty per source. Inequality means an attempt did not
// apply, so an unknown population may have been missed entirely and no per-source
// attribution can be trusted.
//
// sess.mu must be held.
func (sess *mqttSession) applyProvenanceCompleteLocked() bool {
	return sess.inflightApplyCompletedWalk == sess.inflightWalks
}

// sourceRestrictedLocked reports whether an unresolved reservation may be hiding an
// obligation for THIS delivery, so a reader that would act on the claim's absence must
// decline.
//
// It is deliberately per source. An opaque reservation loses its cpending mapping, so the
// claim floor cannot enrol its sequence — but it keeps its jsDur and sseq, so the sequence
// at risk is exactly its own. One durable's opacity therefore says nothing about a different
// durable, or about a different sequence on the same one, and treating it as if it did
// withheld unrelated messages and blocked unrelated retirement.
//
// Two things do fall back to the whole session: an attempt that did not apply, and an
// unresolved entry carrying no source identity to attribute it to.
//
// sess.mu must be held.
func (sess *mqttSession) sourceRestrictedLocked(dur string, sseq uint64) bool {
	if sess.disposalSourceLocked(dur, sseq) {
		return true
	}
	if !sess.applyProvenanceCompleteLocked() {
		return true
	}
	for _, ack := range sess.pendingPublish {
		if !unresolvedReservationLocked(ack) {
			continue
		}
		if ack.regRecoveryDemoted || ack.jsDur == _EMPTY_ || ack.sseq == 0 {
			return true // nothing to attribute it to
		}
		if ack.jsDur == dur && ack.sseq == sseq {
			return true
		}
	}
	return false
}

// durableRestrictedLocked is sourceRestrictedLocked aggregated over one durable, for the
// readers whose question is about the durable rather than about one delivery: "does this
// drain still owe anything?"
//
// sess.mu must be held.
func (sess *mqttSession) durableRestrictedLocked(dur string) bool {
	if sess.disposalDurableLocked(dur) {
		return true
	}
	if !sess.applyProvenanceCompleteLocked() {
		return true
	}
	for _, ack := range sess.pendingPublish {
		if !unresolvedReservationLocked(ack) {
			continue
		}
		if ack.regRecoveryDemoted || ack.jsDur == _EMPTY_ || ack.sseq == 0 {
			return true
		}
		if ack.jsDur == dur {
			return true
		}
	}
	return false
}

// provenUnsentReleasableLocked reports whether an entry may be DISCARDED as never
// transmitted — on Message Expiry, or by an UNSUBSCRIBE drain's withdrawal. Only a
// PROVEN-UNSENT entry may be: one whose durable pair still holds the unsent marker (or
// whose register is not stored at all yet), with no intent unresolved and no section
// writing. A POSSIBLY-ATTEMPTED entry — marker gone, or its intent delete ambiguous — may
// have reached the client, who may then hold this identifier: [MQTT-4.4.0-1] lets only a
// PUBLISH the client has never seen be dropped, and freeing the identifier would let
// bumpPI re-mint it under an exchange the receiver is still in. The durable state decides,
// not any in-memory history: it is what a restart reads too.
func (sess *mqttSession) provenUnsentReleasableLocked(ack *mqttPending) bool {
	if ack == nil || ack.transmitted || ack.intentUnresolved || ack.regForeign ||
		ack.markerOpaque || ack.regCancelled || ack.sendNonce != _EMPTY_ {
		return false
	}
	return ack.regPending || ack.markerSeq != 0
}

// beginSendLocked reserves an entry for ONE transmission, and reports the token that
// ends it.
//
// The identifier is what is being reserved, and the reservation lives in the session's
// sendsInFlight registry rather than on the entry: a successful PUBREC REPLACES the
// PUBLISH entry with a PUBREL one while a blocked write may still be in flight, so
// per-entry state alone would strand the identifier and let bumpPI re-mint it.
//
// The identity is taken FROM THE ENTRY under the lock — its nonce, durable and sequence
// — so a redelivery and a recovered entry are fenced by the entry's own identity without
// the caller having had to capture it at allocation time.
//
// The token is a VALUE carrying, besides the section's identity, what the section needs
// at its end and must not re-read then: the DUP value this transmission carries, and the
// unsent marker's subject and exact sequence, captured under this lock. The entry can be
// replaced, condemned or removed while the write is out, and the delete that records the
// transmission must address the marker this section observed, never whatever the entry
// or the session names afterwards (a clear() re-mints the generation).
//
// Session lock held on entry.
func (sess *mqttSession) beginSendLocked(cc *client, pi uint16, jsDur string, sseq uint64) (mqttSendToken, bool) {
	var none mqttSendToken
	if cc == nil || pi == 0 || sess.c != cc {
		return none, false
	}
	ack := sess.pendingPublish[pi]
	if ack == nil || ack.jsDur != jsDur || ack.sseq != sseq {
		return none, false
	}
	if sess.disposalPILocked(pi) {
		return none, false
	}
	if !ack.acts() || ack.regPending || ack.regCancelled || ack.regForeign || ack.markerOpaque {
		return none, false // nothing may be transmitted under a reservation (R1, WP2, the opaque marker)
	}
	// A REGISTER UNDER AN ABANDONMENT VERDICT MAY NOT TRANSMIT, and this is what makes that
	// verdict true rather than merely hoped-for. A teardown's staging records "this register
	// was never transmitted" and unlocks; without this refusal a delivery callback already
	// past the barrier could then open a section, delete the marker and put the PUBLISH on
	// the wire, leaving the membership-removal save carrying a known-false verdict for an
	// exchange the client now holds — and recovery reads such a verdict as licence to free
	// the identifier. The staging gate alone cannot prevent that, because it decides before
	// this send rather than after it (F-101).
	//
	// Keyed on the NONCE, so it fences this allocation and nothing else: nonces are minted
	// per allocation and never reused, so a verdict restored from the session record cannot
	// fence a fresh allocation, and a replacement that re-mints the nonce is not fenced by
	// its predecessor's verdict.
	//
	// A rolled-back packet needs nothing added here: unvoidRegNonces already withdraws the
	// verdicts its staging recorded, and withdrawing one restores admission in the same
	// step. That is why the fence is keyed on the VERDICT and not on the subscription's
	// closure — a rollback keeps its subscription and still owes these deliveries.
	//
	// A refusal is not a loss. The caller returns without acknowledging and AckWait re-offers
	// the message, exactly as it does for a regPending decline; where the teardown really did
	// abandon the consumer there is nothing left to re-offer, which is the intended end.
	if ack.regNonce != _EMPTY_ {
		if _, voided := sess.cancelledRegNonces[ack.regNonce]; voided {
			return none, false
		}
	}
	if !ack.transmitted && ack.markerSeq == 0 {
		// The entry-shape invariant for an entry with send authority: it is transmitted,
		// or it holds the marker whose confirmed delete will record send intent before the
		// write. Neither means
		// no first transmission can be recorded, so none is authorized.
		return none, false
	}
	if ack.sendNonce != _EMPTY_ || ack.sendSettle.kind != mqttSendSettleNone {
		return none, false // a section is already active, or one parked a settle
	}
	// LIVENESS, not presence. An inert hold — its write returned, its acknowledgement
	// debt owed to a connection that is no longer ours — reserves nothing, and bumpPI
	// will hand this identifier out. Refusing here on presence is how that identifier,
	// its register and a Receive Maximum slot get stranded for the session's life:
	// nothing ever transmits, so nothing ever removes the hold, and every redelivery
	// meets the same refusal (F-089). An inert hold is TAKEN OVER instead.
	if sess.sendReservedLocked(pi) {
		return none, false
	}
	// A per-send token, not a counter: recovery can REPLACE the entry, and a
	// per-entry counter would restart with it and let a stale endSend match.
	token := mqttNewInflightNonce()
	ack.sendNonce = token
	if sess.sendsInFlight == nil {
		sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
	}
	if ack.res == nil {
		// SEEDED AT THE STANDING IDENTITY, which is what an attempt that definitely did not
		// acquire keeps. Created here — before any acquisition can resolve — so a resolver
		// can never publish into a cell that does not exist yet.
		ack.res = &mqttAckResolution{ref: ack.slot}
	}
	tok := mqttSendToken{token: token, dup: ack.transmitted, markerSeq: ack.markerSeq,
		intentUnresolved: ack.intentUnresolved, slot: ack.slot, res: ack.res,
		// THE SECTION'S OUTPUT IDENTITY, shared by every copy of this token; its pre-submit tier
		// fixed now, on the connection that will write (outbound_output_holder.go).
		out: &mqttPublishOutput{exec: mqttNewExecCell(cc), gen: ack.slot.name.gen, pi: pi, token: token}}
	if ack.markerSeq != 0 && ack.slot.name.subject != _EMPTY_ {
		// The marker's subject from the entry's CAPTURED slot name, not from the session's
		// current identity (class F2: a clear() between here and endSend re-mints gen).
		tok.regSubject = ack.slot.name.subject
	}
	// The takeover is a REPLACEMENT, so an inert debt cannot be inherited by this
	// transmission. Nothing is lost with it: an inert hold reserves nothing for
	// anybody, so keeping it and dropping it are indistinguishable to every reader.
	//
	// A live PUBREL action makes sendReservedLocked true above, so it can never reach this
	// replacement. That property is load-bearing: dropping the action term would free an
	// identifier whose durable slot still names a capability nobody else may drain.
	// pubIdent NAMES THE PUBLISH SECTION, and it is installed here rather than derived
	// later because the fact it carries — "a PUBLISH is on the wire under this identifier"
	// — is not recoverable from the other terms. `token` is set by a PUBREL opener too, so
	// a reader asking it would refuse a stored PUBREL because a PUBREL was going out.
	//
	// It is NOT a second reservation. sendReservedLocked remains the one answer to whether
	// the identifier is held; this answers which KIND of transmission holds it, which is
	// what PUBREL admission needs and what it had been reading from a field nothing ever
	// assigned (F-098): the check was inert from the commit that introduced it, and a wire
	// probe in the post-drain window is what proved it.
	sess.sendsInFlight[pi] = &mqttSendHold{token: token, pubIdent: token, pubOut: tok.out}
	return tok, true
}

// mqttAckResolution is the ONE place an acknowledgement's cleanup identity is settled
// (F-098, holistic review round 7). It is shared by the send section and the pending
// exchange, and it outlives both: the section ends, the entry is untracked, and the cell is
// still the thing that says which transmission the acknowledgement may terminalize.
//
// It exists because three rounds of PROPAGATING that identity — into the entry's captured
// slot, then into a token-keyed map — each left a window: an acknowledgement arriving after
// the section closed, a resolver finishing before the acknowledgement registered, a foreign
// token's counter adopted as our own. A cell cannot have those windows, because both facts
// are published to the SAME state under sess.mu and whichever completes the pair claims the
// continuation exactly once.
//
// Its identity is MONOTONE:
//
//   - It starts at the identity the section opened on, which is the exchange's standing
//     binding and is what a definitely-unacquired attempt keeps.
//   - An acquisition ATTEMPT marks it pending: the record may have moved past the standing
//     binding, so nothing may be bound until that is resolved.
//   - Only our EXACT token's proof publishes a new identity — a confirmed acquisition, an
//     adopted one, or our own drain's CAS. A foreign token, an absence or an unreadable read
//     publishes nothing: none of them says which acquisition we owned.
//
// Every field is guarded by sess.mu.
type mqttAckResolution struct {
	// ref is the identity a cleanup bound to this exchange must use.
	ref mqttInflightSlotRef
	// pending records that an acquisition of ours may have advanced the record past ref, and
	// pendingTok names the ATTEMPT that set it. A publication settles the cell only for its
	// OWN attempt: an older drain resuming after a NEWER section marked the cell would
	// otherwise clear a pending state it knows nothing about, and the acknowledgement would
	// then bind to an identity the newer acquisition has already moved past (holistic review
	// round 9, finding 1).
	pending    bool
	pendingTok string
	// ackPI and ackOwed record an acknowledgement whose cleanup is owed.
	ackPI   uint16
	ackOwed bool
	// claimed makes the continuation exactly-once across both publishers.
	claimed bool
}

// markAckResolutionPendingLocked records that an acquisition attempt may advance the
// identity. Until it resolves, no cleanup may be bound.
//
// sess.mu must be held.
func (sess *mqttSession) markAckResolutionPendingLocked(res *mqttAckResolution, token string) {
	if res != nil {
		res.pending, res.pendingTok = true, token
	}
}

// publishAckResolutionLocked settles the pending acquisition. A ref is published ONLY when
// our exact token proved the identity; `ours` false resolves the attempt while keeping the
// standing binding, which is what a definitely-unacquired attempt owes.
//
// It reports whether the caller must now run the acknowledgement's cleanup, and with what.
//
// sess.mu must be held.
func (sess *mqttSession) publishAckResolutionLocked(res *mqttAckResolution, ref mqttInflightSlotRef,
	ours bool, token string) (run bool, pi uint16, bound mqttInflightSlotRef) {

	if res == nil {
		return false, 0, bound
	}
	if ours && ref.name.subject != _EMPTY_ &&
		ref.name.subject == res.ref.name.subject && ref.nonce == res.ref.nonce &&
		ref.dur == res.ref.dur && ref.sseq == res.ref.sseq && ref.acq >= res.ref.acq {
		// SUBJECT, NONCE AND SOURCE, not the nonce alone: they are what say this is the same
		// exchange, and the counter is what says which transmission of it. Monotone, so a
		// delayed publisher cannot put an earlier transmission back.
		res.ref = ref
	}
	if !res.pending || res.pendingTok == token {
		// ONLY OUR OWN ATTEMPT. A stale resolver may still refine the identity above — that
		// is monotone and safe — but it may not declare a NEWER attempt settled.
		res.pending, res.pendingTok = false, _EMPTY_
	}
	return sess.claimAckResolutionLocked(res)
}

// registerAckCleanupLocked records that an acknowledgement owes this exchange's terminal
// cleanup, and reports whether the caller must run it now.
//
// sess.mu must be held.
func (sess *mqttSession) registerAckCleanupLocked(res *mqttAckResolution, pi uint16) (run bool,
	bound mqttInflightSlotRef) {

	if res == nil {
		return false, bound
	}
	res.ackOwed, res.ackPI = true, pi
	run, _, bound = sess.claimAckResolutionLocked(res)
	return run, bound
}

// claimAckResolutionLocked hands the continuation to exactly one caller, once both facts are
// in: an acknowledgement owes a cleanup, and the identity it must use is settled.
//
// sess.mu must be held.
func (sess *mqttSession) claimAckResolutionLocked(res *mqttAckResolution) (run bool, pi uint16,
	bound mqttInflightSlotRef) {

	if res == nil || res.claimed || res.pending || !res.ackOwed {
		return false, 0, bound
	}
	res.claimed = true
	return true, res.ackPI, res.ref
}

// mqttSendToken is what beginSendLocked hands a PUBLISH send section: its identity, and
// the facts captured under the lock that the section consumes without re-reading.
type mqttSendToken struct {
	token string
	// admit, when set, admits each of the section's authority-transition attempts: a BACKGROUND
	// section (the retained driver's) yields every acquisition attempt to announced foreground
	// operations (outbound_foreground_priority.go). Nil for a foreground section.
	admit func() bool
	// dup is the DUP flag this transmission carries: the entry's transmitted bit as it
	// stood when the section opened.
	dup bool
	// regSubject and markerSeq name the unsent marker the section CONSUMES as its intent
	// record before it writes — the register subject the marker is a sibling of, and the
	// marker's exact sequence. Zero markerSeq means none held: the entry is already
	// possibly-attempted or transmitted and the section records no intent.
	regSubject string
	markerSeq  uint64
	// intentUnresolved: a previous section's intent delete at markerSeq was ambiguous, so
	// this section RE-ATTEMPTS that exact delete before it may write.
	intentUnresolved bool
	// slot is the entry's CAPTURED durable slot, and it is what the section's durable
	// PUBLISH action is acquired on and drained from. Captured under the lock with
	// everything else here, never re-read at write time: a clear() between the two
	// re-mints the generation, and the subject would then name a different incarnation's
	// slot (class F2).
	//
	// Empty subject means the exchange has no durable slot at all. acquirePublishAction
	// refuses it, so no section with one transmits; and bindSection requires an action of
	// any QoS 1/2 frame outside the slot-only class, so it could not go out raw either.
	slot mqttInflightSlotRef
	// res is the exchange's acknowledgement-resolution cell, shared with the pending entry.
	// The section publishes what its own token proves; the entry's acknowledgement registers
	// against the same cell, so neither can miss the other.
	res *mqttAckResolution
	// actionHeld records that this section acquired the durable action, so the deferred
	// end drains exactly what it took and nothing else.
	actionHeld bool
	// out is the section's output identity and closure, one object shared by every value copy of
	// this token (outbound_output_holder.go). Its action is set only when an ORDINARY acquisition
	// landed cleanly through the authority, and it is what the authorized write consumes.
	out *mqttPublishOutput
}

// acquirePublishAction takes the DURABLE half of a PUBLISH transmission's exclusion.
//
// Before this, a PUBLISH's hold was PROCESS MEMORY (`sendsInFlight`) while a PUBREL's was
// a durable token, and that asymmetry is one of F-098's four unfenced routes: two brokers
// may legitimately serve one incarnation, so a hold no peer can observe excludes nobody.
// `recordSendIntent` returns `mqttIntentNone` whenever `markerSeq == 0`, which is EVERY
// retransmission, so the marker did not fence those either.
//
// The token is the section's own send nonce, deliberately: one transmission, one identity,
// in memory and on the record. The CAS also advances `Acq` (C-I), which is what makes an
// acknowledgement bound to THIS transmission undischargeable by a later one.
//
// ORDERED BEFORE ADMISSION AND BEFORE INTENT. The action is authority, not evidence, so
// taking it first fences the whole section — including the size refusal, whose destructive
// disposal must therefore wait for the drain (see mqttSlotReleaseAuthority arm 2).
//
// Session lock NOT held on entry.
func (sess *mqttSession) acquirePublishAction(c *client, pi uint16, tok *mqttSendToken) bool {
	if tok == nil || tok.token == _EMPTY_ {
		return false
	}
	if tok.slot.name.subject == _EMPTY_ || !tok.slot.slotOK() {
		// NO DURABLE SLOT, NO OUTPUT (F-105 7c). The registerless retained send this escape
		// existed for is gone: a retained delivery is register-backed, so every acting entry
		// that reaches a send section carries a slot, and one that does not is refused rather
		// than transmitted unfenced.
		return false
	}
	as := sess.asm
	if as == nil {
		return false
	}
	// THE REFERENCE IS REBUILT FROM A FRESH READ, not taken from the entry's capture, and
	// a full-suite regression is what proved it necessary.
	//
	// The entry's captured slot carries the CUSTODY AS IT STOOD AT ALLOCATION. Recovery
	// refreshes custody on every eligible slot at resume, so after a reconnect the
	// capture's claim is stale — and the acquisition CAS compares custody exactly. Every
	// post-resume transmission then failed to acquire, transmitted nothing, and was
	// re-offered by AckWait: an endless redelivery of a message the client should have
	// had, which is how the regression surfaced (an old topic arriving ahead of a control
	// message that was published after it).
	//
	// Rebuilding here is not a weakening. The exchange IDENTITY is still checked against
	// the entry's own nonce below, the CAS still fences on the exact observed sequence,
	// and custody is compared against what this read actually found rather than against a
	// belief formed before a takeover.
	cur, smsg, lerr := as.loadInflightRecord(tok.slot.name.subject)
	if lerr != nil || smsg == nil || cur == nil {
		// Indeterminate, gone, or unattributable. Nothing is transmitted and nothing is
		// acknowledged; AckWait re-offers.
		return false
	}
	if cur.Nonce != tok.slot.nonce || cur.State != mqttInflightPublish {
		// A DIFFERENT allocation owns the identifier now, or the exchange has moved past
		// the PUBLISH phase. Either way this send's premise is false.
		return false
	}
	if cur.ActionToken != _EMPTY_ {
		// A LIVE ACTION, ours or a peer's, and this MUST be refused explicitly rather
		// than left to the CAS: the mutator below sets the token unconditionally, so
		// without this check an acquisition would overwrite a live holder's capability
		// and two brokers would believe they held the same exchange. The phase gate
		// cannot catch it either, now that a token is legal in every phase.
		//
		// UNLESS THE HOLDER'S CONNECTION IS DURABLY CERTIFIED (F-105 8c). A successor that resumed
		// before its predecessor certified meets the token here, on every redelivery; this is its
		// next chance, under the same licence the resume's pre-pass checks. The register is then
		// read again and every premise above re-proven: nothing from the pre-clearance read is used.
		if !sess.clearCertifiedBeforeAcquire(c, tok.slot.name.subject) {
			return false
		}
		cur, smsg, lerr = as.loadInflightRecord(tok.slot.name.subject)
		if lerr != nil || smsg == nil || cur == nil || cur.Nonce != tok.slot.nonce ||
			cur.State != mqttInflightPublish || cur.ActionToken != _EMPTY_ {
			return false
		}
	}
	if mqttSlotRefFromRecord(tok.slot.name.subject, smsg.Sequence, cur) == nil {
		return false
	}
	// TENTATIVE OWNERSHIP, ARMED BEFORE THE CAS (holistic review round 1, finding 2).
	//
	// An acquisition CAS can LAND and lose its response. casInflightSlot then rereads, finds
	// a token — ours — and refuses on it, so this function saw only "not acquired". With the
	// ownership armed only on success, nothing was left holding that token: the section's
	// end skipped its drain, no owed-drain entry was recorded, and every later acquisition
	// refused the survivor for the life of the session, across a healthy graceful shutdown
	// too. So the section is marked as the token's holder FOR CLEANUP first — exactly as the
	// PUBREL path arms its tentative action — and the mark is withdrawn only when a reread
	// PROVES the token is not ours. The mark authorizes no transmission: the send still
	// requires the acquisition to have landed and been seen.
	tok.actionHeld = true
	sess.mu.Lock()
	sess.markAckResolutionPendingLocked(tok.res, tok.token)
	sess.mu.Unlock()
	expectAcq := cur.Acq + 1
	var seq uint64
	// THE ROUTE IS THE CLASS OF THE SLOT JUST READ, through the one statement of it every
	// acknowledgement arrow also asks (mqttDurIsSlotOnlyClass). An affine slot has no
	// exchange (its conversion is chapter P's) and keeps the standalone CAS below — a scope
	// boundary, not a fallback: an ORDINARY slot never takes it, whatever the authority says.
	if tok.out != nil {
		tok.out.lin = cur.Lin
	}
	if !mqttDurIsSlotOnlyClass(cur.Dur) {
		// THE ACQUISITION IS ONE AUTHORITY BATCH (F-105 row 2, §4.3 point 1): the output-custody
		// token is installed on the register BESIDE the durable action record, arbitrated by the
		// serving authority, in place of the standalone slot CAS. An acquisition whose response
		// is lost is still resolved by rereading the register — resolveUnacquiredPublishAction
		// checks the STORE for the landed token and does not depend on which writer produced it.
		epoch := _EMPTY_
		if sess.asm != nil {
			epoch = sess.asm.sessStreamEpoch
		}
		// RE-ATTEMPT UNDER THE PINNED ENTITLEMENT (holistic review rounds 2-3). The
		// acknowledgement advance (opAdvanceExchangeAck) rewrites the authority on every ending,
		// so this acquisition — a frequent authority writer on the send path — can lose its CAS to
		// a concurrent acknowledgement even though no peer owns its slot. Any such contention wrote
		// nothing, so it is settled the way the admission ladder settles every write: a bounded
		// re-attempt on ANY error after opAcquireAction re-reads the authority itself and the
		// register is re-read here, rather than a predicate that must enumerate each pre-submit
		// check concurrency can trip. Without it the delivery returns unsent and waits out AckWait.
		//
		// OWNERSHIP IS THE CONNECTION'S OWN, not whatever the store serves when this asks. The
		// entitlement is the generation and tenure this connection was ACCEPTED under
		// (c.outboundEntitlement), passed on every attempt: after a takeover, opAcquireAction's
		// gen/tenure guard refuses every attempt this connection makes — on this delivery, every
		// redelivery and every retry — because the pair names a tenure that no longer serves.
		// Reading the serving authority here instead would hand the displaced connection its
		// successor's tenure. A connection with no accepted entitlement acquires nothing. The slot
		// premise is re-validated on every refreshed read: a slot that moved past PUBLISH or that a
		// peer acquired is not ours. An AMBIGUOUS outcome (a lost response) lands its token on the
		// register, so the premise re-read sees ActionToken != empty and routes it to
		// resolveUnacquiredPublishAction, whose job is exactly that uncertainty.
		ent, entOK := c.outboundEntitlement()
		if !entOK {
			sess.resolveUnacquiredPublishAction(c, tok, expectAcq, cur.Claim, cur.ClaimSeq)
			return false
		}
		var acquired *mqttOutboundAction
		for attempt := 0; ; attempt++ {
			if tok.admit != nil && !tok.admit() {
				sess.resolveUnacquiredPublishAction(c, tok, expectAcq, cur.Claim, cur.ClaimSeq)
				return false
			}
			_, act, aqerr := sess.jsa.opAcquireAction(&mqttAcquireActionRequest{
				ID: sess.id, Epoch: epoch, Txn: mqttNewInflightNonce(), Op: "op-acquire-publish",
				Gen: ent.Gen, Tenure: ent.Tenure,
				Nonce: cur.Lin, Acq: expectAcq, Kind: mqttActionPublish,
				// The connection that will emit the bytes, its ORIGINAL binding — which a captured
				// completion handler later resolves against. Outstanding is FALSE: this path only
				// reaches here on a slot carrying NO live token (checked above), so no earlier action
				// of this exchange may still emit and race it.
				Binding: c.outBind, Outstanding: false,
				Register: cur, RegisterSeq: smsg.Sequence, Token: tok.token,
			})
			if aqerr == nil {
				acquired = act
				break
			}
			// AN ENDED ENTITLEMENT IS TERMINAL: re-attempting can only ask again under a tenure
			// that is not this connection's. Whatever an ambiguous earlier attempt left is the
			// store's to say.
			if errors.Is(aqerr, errMQTTEntitlementEnded) {
				sess.resolveUnacquiredPublishAction(c, tok, expectAcq, cur.Claim, cur.ClaimSeq)
				return false
			}
			if attempt+1 < mqttInflightStoreAttempts {
				rcur, rsmsg, rlerr := as.loadInflightRecord(tok.slot.name.subject)
				if rlerr != nil || rsmsg == nil || rcur == nil ||
					rcur.Nonce != tok.slot.nonce || rcur.State != mqttInflightPublish ||
					rcur.ActionToken != _EMPTY_ {
					// The premise is no longer true: the slot moved past PUBLISH, a peer acquired
					// it (or our own late-landing acquisition did), or it is gone. Not a contention
					// to retry; resolveUnacquiredPublishAction settles the store's truth.
					sess.resolveUnacquiredPublishAction(c, tok, expectAcq, cur.Claim, cur.ClaimSeq)
					return false
				}
				cur, smsg = rcur, rsmsg
				expectAcq = cur.Acq + 1
				continue
			}
			sess.resolveUnacquiredPublishAction(c, tok, expectAcq, cur.Claim, cur.ClaimSeq)
			return false
		}
		// CLEANLY LANDED. Reread the register for its new sequence — the batch's outcome carries
		// the AUTHORITY sequence, not the register's — then adopt the acquisition and transmit.
		_, seqMsg, slerr := as.loadInflightRecord(tok.slot.name.subject)
		if slerr != nil || seqMsg == nil {
			sess.resolveUnacquiredPublishAction(c, tok, expectAcq, cur.Claim, cur.ClaimSeq)
			return false
		}
		// THE ACQUISITION THE FRAME WILL CONSUME, published only now: a landing whose readback
		// failed above resolves like a lost response and never emits.
		if tok.out != nil {
			tok.out.action = acquired
			tok.out.acq = expectAcq
		}
		seq = seqMsg.Sequence
	} else {
		// THE PRE-F-105 STANDALONE SLOT CAS. The shared-group and affine send paths are not
		// converted (chapter P), and a register they stored carries no outbound exchange, so their
		// acquisition installs the token by a direct register CAS, exactly as before.
		ref := mqttSlotRefFromRecord(tok.slot.name.subject, smsg.Sequence, cur)
		if ref == nil {
			return false
		}
		var ok bool
		seq, ok = sess.casInflightSlot(c, *ref, mqttInflightPublish, mqttSlotAcquireAction,
			func(next *mqttInflightRecord) {
				next.ActionToken = tok.token
				// Bound to the custody the slot ALREADY carries: the validator requires the two
				// to be equal, so deriving them from this session's current belief would write a
				// record the next reader refuses.
				next.ActionClaim = next.Claim
				next.ActionClaimSeq = next.ClaimSeq
				next.Acq++ // C-I
			})
		if !ok {
			// NOT ACQUIRED AS FAR AS THIS CALL COULD SEE, which is not "not acquired". Resolve
			// by rereading; nothing is transmitted either way, and AckWait re-offers.
			sess.resolveUnacquiredPublishAction(c, tok, expectAcq, cur.Claim, cur.ClaimSeq)
			return false
		}
	}
	tok.slot.acq = cur.Acq
	tok.slot.claim, tok.slot.claimSeq = cur.Claim, cur.ClaimSeq
	tok.slot.seq = seq
	tok.slot.acq++
	tok.slot.actionLive = true
	tok.slot.actionToken = tok.token
	tok.actionHeld = true
	// THE ENTRY'S CAPTURED SLOT LEARNS THE NEW COUNTER, and this is C-I's binding rule
	// rather than bookkeeping. The acknowledgement handler stamps its completion evidence
	// with the identity bound to ITS OWN exchange — which is this entry — so if the entry
	// kept the pre-acquisition counter, every later release would be bound to a
	// transmission identity the slot no longer carries and would refuse itself forever.
	//
	// Guarded on the NONCE: a replacement re-mints it, and writing our counter onto a
	// successor's slot would hand it an identity it never acquired.
	sess.mu.Lock()
	if ack := sess.pendingPublish[pi]; ack != nil && ack.slot.nonce == tok.slot.nonce &&
		ack.slot.name.subject == tok.slot.name.subject {
		ack.slot.acq = tok.slot.acq
		ack.slot.seq = seq
	}
	run, ackPI, bound := sess.publishAckResolutionLocked(tok.res, tok.slot, true, tok.token)
	sess.mu.Unlock()
	if run {
		sess.handOffAcknowledgedPublish(c, ackPI, &mqttPending{slot: bound})
	}
	return true
}

// advancePublishTerminal records, DURABLY, that a PUBLISH phase ended without a PUBREL
// exchange: a PUBACK, or a FAILED PUBREC.
//
// It exists because terminal authority has to be a FACT ON THE RECORD rather than an
// inference from a missing PUBREL. Before this, the acknowledgement lived only in the
// executing control flow: a peer, or this broker after a restart, saw an ordinary
// `publish` slot and had nothing positive to release it on — which is why C-IV's arm 3
// had no phase to recognise and an acknowledged QoS 1 exchange would otherwise reserve
// its identifier forever.
//
// A FAILED WRITE LEAVES THE PREVIOUS PHASE, not terminal authority. The caller keeps its
// captured acknowledgement identity and withholds freeing until the advance lands; there
// is deliberately no arm here that treats "I could not write it" as "it is done".
//
// The action token is NOT required to be absent. The acknowledgement and the holder's
// drain are independent events and either may land first, so the phase advance runs
// regardless and the RELEASE is what waits for the token (C-IV arm 2).
//
// Session lock NOT held on entry.
func (sess *mqttSession) advancePublishTerminal(c *client, ack *mqttPending) bool {
	if ack == nil || ack.slot.name.subject == _EMPTY_ || !ack.slot.slotOK() {
		// No durable slot: a retained send (R6) has none, and there is nothing to
		// advance. The disclosed residual, removed with the source-less retained slot.
		return true
	}
	if ack.slot.state == mqttInflightPublishTerminal {
		return true // already advanced by an earlier attempt of this same acknowledgement
	}
	seq, ok := sess.casInflightSlot(c, ack.slot, mqttInflightPublish, mqttSlotAdvanceAcknowledgedTerminal,
		func(next *mqttInflightRecord) {
			next.State = mqttInflightPublishTerminal
		})
	if !ok {
		return false
	}
	ack.slot.seq = seq
	ack.slot.state = mqttInflightPublishTerminal
	return true
}

// resolveUnacquiredPublishAction decides who holds the token an unconfirmed acquisition may
// have installed, and keeps or withdraws the section's cleanup ownership accordingly.
//
// Only the EXACT identity is adopted — this section's token, at the counter the acquisition
// would have written, under the custody it was bound to. Anything else is another holder's or
// nobody's, and the section must not drain it: a drain clears only its exact token anyway,
// but withdrawing the mark keeps the section from recording an obligation it does not have.
//
// An UNRESOLVED read keeps the mark. The section's end then attempts the drain, which rereads
// and clears only an exact match; if that cannot resolve either, the drain enrols the
// shutdown-visible owed-drain registry and the reconciler. So an indeterminate outcome always
// has an owner, which is the whole of the finding.
//
// Adoption is for CLEANUP only: the caller has already decided not to transmit.
//
// Session lock NOT held on entry.
func (sess *mqttSession) resolveUnacquiredPublishAction(c *client, tok *mqttSendToken,
	expectAcq uint64, claim string, claimSeq uint64) {

	as := sess.asm
	if as == nil || tok == nil {
		return
	}
	cur, smsg, lerr := as.loadInflightRecord(tok.slot.name.subject)
	switch {
	case lerr != nil:
		return // indeterminate: the mark stays, and the section's end owns the resolution
	case smsg == nil:
		// PROVEN ABSENT: nothing of ours can be on it, and that IS resolution evidence, so the
		// cell is settled here rather than abandoned. Before, this arm cleared the mark and
		// returned — which also stopped endPublishSend from draining, since the drain refuses
		// an unheld token — so no owed drain and no continuation existed at all, and an
		// acknowledgement registering against the still-pending cell held its identifier
		// indefinitely (holistic review round 9, finding 2).
		tok.actionHeld = false
		sess.mu.Lock()
		run, ackPI, bound := sess.publishAckResolutionLocked(tok.res, mqttInflightSlotRef{}, false,
			tok.token)
		sess.mu.Unlock()
		if run {
			sess.handOffAcknowledgedPublish(c, ackPI, &mqttPending{slot: bound})
		}
		return
	case cur == nil:
		return // present but unreadable: not an absence, the mark stays
	}
	if cur.ActionToken == tok.token && cur.Nonce == tok.slot.nonce && cur.Acq == expectAcq &&
		cur.ActionClaim == claim && cur.ActionClaimSeq == claimSeq {
		// THE ACQUISITION LANDED. Ours, exactly: keep the mark so the section's end drains
		// it, and say so, because the log is the only trace of a lost response.
		mqttWarnf(c, "MQTT: the durable PUBLISH action on %q landed but its response was lost; "+
			"it is adopted for cleanup and drained at the end of this section, and nothing is "+
			"transmitted under it", tok.slot.name.subject)
		// AND IT CARRIES C-I's BINDING RULE, exactly as the SEEN acquisition does (holistic
		// review round 5, finding 1). The counter advanced on the record; an entry still
		// holding the pre-acquisition one stamps its acknowledgement with an identity the
		// slot no longer carries, and the terminal continuation then reads its OWN exchange
		// as a successor, releases the identifier hold and drops the cleanup it owed.
		tok.slot.acq = cur.Acq
		tok.slot.seq = smsg.Sequence
		tok.slot.claim, tok.slot.claimSeq = cur.Claim, cur.ClaimSeq
		tok.slot.actionLive = true
		tok.slot.actionToken = tok.token
		sess.mu.Lock()
		if ack := sess.pendingPublish[tok.slot.name.pi]; ack != nil &&
			ack.slot.nonce == tok.slot.nonce && ack.slot.name.subject == tok.slot.name.subject &&
			ack.slot.acq <= tok.slot.acq {
			// Monotonic, for the reason the drain's own write-back is (round 6, finding 2).
			ack.slot.acq = tok.slot.acq
			ack.slot.seq = tok.slot.seq
		}
		run, ackPI, bound := sess.publishAckResolutionLocked(tok.res, tok.slot, true, tok.token)
		sess.mu.Unlock()
		if run {
			sess.handOffAcknowledgedPublish(c, ackPI, &mqttPending{slot: bound})
		}
		return
	}
	// A different token, or none: this section holds nothing on the record. The attempt is
	// RESOLVED — it did not acquire — so the exchange's standing binding stands; publishing an
	// identity from a record we did not prove ours is exactly the adoption round 7 refused.
	tok.actionHeld = false
	sess.mu.Lock()
	run, ackPI, bound := sess.publishAckResolutionLocked(tok.res, mqttInflightSlotRef{}, false, tok.token)
	sess.mu.Unlock()
	if run {
		sess.handOffAcknowledgedPublish(c, ackPI, &mqttPending{slot: bound})
	}
}

// drainPublishAction ends the durable half, and it PRESERVES THE PHASE.
//
// The phase may legitimately have moved under this write — a PUBREC handler advances the
// slot past PUBLISH while the bytes are still out — so the drain re-reads, clears its own
// exact token in whatever phase it finds, and derives nothing from the phase at all. That
// is the same correction `startActionDrain` carries: the outcome comes from the state
// OBSERVED, never the state at acquisition.
//
// `Acq` is NOT reset. A drain ends a transmission's authority; it does not un-count it,
// and a reset would hand a later transmission an identity an earlier one's evidence
// already names.
//
// IT REPORTS WHETHER THIS CAPTURED ACTION IS PROVEN OVER — drained by this call, or the
// record proves it is no longer ours — and that is deliberately NOT "the identifier is
// unblocked": a DIFFERENT live token ends our action and remains a blocker, and so does a
// local send section this call knows nothing about. The deferred path asks both questions
// separately. Every caller on the synchronous path ignores the result: there the wake
// belongs after the LOCAL half too, which only endPublishSend can order.
//
// Session lock NOT held on entry.
func (sess *mqttSession) drainPublishAction(c *client, tok mqttSendToken) bool {
	if !tok.actionHeld || tok.token == _EMPTY_ || tok.slot.name.subject == _EMPTY_ {
		return false
	}
	as := sess.asm
	if as == nil {
		return false
	}
	// THE LEASE IS BEING GIVEN UP, so no execution may begin under it from here — set before the
	// first store call, on the output object every copy of this token shares.
	if tok.out != nil {
		tok.out.closed.Store(true)
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, lerr := as.loadInflightRecord(tok.slot.name.subject)
		// THE ORDER IS INDETERMINATE, PROVEN ABSENCE, UNREADABLE PRESENCE, IDENTITY, and
		// it is not interchangeable: the loader reports a present-but-unattributable
		// record and a genuinely absent one with the same nil record, so testing the
		// record before `smsg` folds an absence proof into an unproven read.
		switch {
		case lerr != nil:
			// INDETERMINATE. The token may still be ours, so nothing is proven and the
			// identifier stays reserved. Retry.
			continue
		case smsg == nil:
			sess.clearOwedDrain(c, tok)
			// The slot is gone, so this attempt can publish no identity — but it IS resolved,
			// and the exchange's standing binding is what a cleanup uses. That cleanup then
			// finds the register absent and discharges by releasing its hold.
			sess.resolveAckAttempt(c, tok, mqttInflightSlotRef{}, false)
			return true // PROVEN absent: the slot is gone and nothing of ours remains
		case cur == nil:
			// PRESENT but unattributable. Not an absence, and not a licence to conclude
			// this action is over. Retry.
			continue
		case cur.ActionToken != tok.token:
			sess.clearOwedDrain(c, tok)
			// A DIFFERENT TOKEN, or none. Neither says which acquisition WE owned, so nothing
			// is published: adopting the counter we happen to observe is how a predecessor's
			// acknowledgement came to terminalize a successor (holistic review round 7,
			// finding 3). The attempt is resolved; the standing binding stands.
			sess.resolveAckAttempt(c, tok, mqttInflightSlotRef{}, false)
			return true // a different token, or none: this action is already over
		}
		// OUR OWN TOKEN, OBSERVED: that alone establishes which acquisition we owned, and it
		// is published HERE rather than after the CAS (holistic review round 8). A CAS that
		// lands and loses its response leaves the next pass looking at a cleared token, which
		// is indistinguishable from a foreign one — so a publication that waited for the CAS
		// would discard proof this read already had, and the acknowledged cleanup would bind
		// to a standing identity the record has moved past.
		//
		// Publishing under a LIVE token is safe: the continuation it may start refuses to
		// terminalize or delete while any action is live, and retries.
		observed := tok.slot
		observed.acq, observed.seq = cur.Acq, smsg.Sequence
		sess.resolveAckAttempt(c, tok, observed, true)
		// AND THE ACQUISITION IT OWNED IS ABANDONED HERE unless it executed, recorded BEFORE the
		// clearing CAS while this read still proves the counter (F-105 row 2): the key is the
		// counter READ beside our exact token, never a candidate a failed attempt computed. A
		// consumed acquisition is left alone; an ordinary one whose section never wrote — an
		// oversize disposal, a refused intent, an adopted lost response — is recorded as never to
		// execute on this connection, the evidence its closure certificate is built from (the
		// discharge oracle that once read it is deleted, F-105 8c). A slot-only register carries no
		// action record to abandon.
		if !mqttDurIsSlotOnlyClass(cur.Dur) {
			tok.out.abandonProven(cur.Acq)
		}
		ref := tok.slot
		ref.seq = smsg.Sequence
		ref.state = cur.State
		ref.acq = cur.Acq
		// THE DRAIN'S AUTHORITY IS THE EXACT TOKEN IT HOLDS, just proven present above — not
		// whatever the section's captured slot happens to record. That copy is filled only
		// when an acquisition is SEEN to succeed, so a token adopted after a lost response,
		// or one whose acquisition could not be resolved at all, carried an empty authority
		// here, the CAS refused it, and the orphan the adoption exists to clear survived.
		ref.actionToken = tok.token
		if seq, ok := sess.casInflightSlot(c, ref, cur.State, mqttSlotDrainAction,
			func(next *mqttInflightRecord) {
				if next.ActionToken != tok.token {
					return
				}
				next.ActionToken = _EMPTY_
				next.ActionClaim = _EMPTY_
				next.ActionClaimSeq = 0
			}); ok {
			// THE ENTRY'S CAPTURED SLOT LEARNS THE NEW SEQUENCE, and leaving it stale is
			// a silent lost-completion bug rather than untidiness — a full-suite
			// regression is what proved it.
			//
			// This CAS is a REWRITE, so it moves the record's stream sequence. The
			// acknowledgement path then addresses the slot through the entry's captured
			// reference, and the gated reap's freshness fence compares that remembered
			// sequence against what it finds: a stale one reads as "another operation
			// re-targeted this exchange", so the delete is DECLINED, the register
			// survives, the identifier stays reserved and the source is never
			// acknowledged — and AckWait then redelivers a message the client already
			// has.
			//
			// Guarded on the NONCE, for the same reason the acquisition's write-back is:
			// a replacement re-mints it, and writing our sequence onto a successor's slot
			// would point its cleanup at a record it does not own.
			sess.mu.Lock()
			if ack := sess.pendingPublish[tok.slot.name.pi]; ack != nil &&
				ack.slot.nonce == tok.slot.nonce &&
				ack.slot.name.subject == tok.slot.name.subject &&
				ack.slot.acq <= cur.Acq {
				// MONOTONIC, and that is not tidiness (holistic review round 6, finding 2).
				// Successive transmissions of one allocation deliberately SHARE nonce and
				// subject, so those two do not separate an older drain from a newer one: this
				// drain can have cleared its token, paused, and resumed after an AckWait
				// delivery acquired and drained the same allocation at a higher counter. A
				// blind write-back then puts the OLDER counter on the entry, and the next
				// acknowledgement is bound to a transmission the slot has moved past.
				ack.slot.seq = seq
				// The COUNTER too: this drain may be the first reader of a landed acquisition
				// whose response was lost, and the entry's evidence identity must name the
				// transmission the slot actually carries (round 5, finding 1).
				ack.slot.acq = cur.Acq
				if ack.slot.actionToken == tok.token || ack.slot.actionToken == _EMPTY_ {
					// Only OUR action's state. A newer transmission's token is its own.
					ack.slot.actionLive = false
					ack.slot.actionToken = _EMPTY_
				}
			}
			sess.mu.Unlock()
			sess.clearOwedDrain(c, tok)
			// OUR EXACT TOKEN, just proven present and cleared by this CAS: this is the one
			// observation that establishes which transmission we owned, so it publishes.
			proven := tok.slot
			proven.acq, proven.seq = cur.Acq, seq
			sess.resolveAckAttempt(c, tok, proven, true)
			return true
		}
	}
	// UNRESOLVED after the bounded attempts, and this is NOT the end of the obligation.
	//
	// Returning here was the defect: a FINITE storage failure would convert into a
	// permanently live token, because `endPublishSend` goes on to release the in-memory
	// section regardless and nothing else ever retries this drain. The identifier, its
	// Receive Maximum capacity and the exchange would then be held for the life of the
	// session on a fault that cleared seconds later — the same successful-path leak the
	// PUBREL action's own lifecycle exists to prevent.
	//
	// So it is ENROLLED, on that same shutdown-gated lifecycle, with unlimited attempts
	// and a capped delay. Only a bounded attempt COUNT would recreate the defect.
	//
	// While the drain is pending a redelivery's acquisition REFUSES, and that is the
	// exclusion working rather than a stall: the offer is preserved and AckWait re-offers
	// it once the token is gone.
	mqttWarnf(c, "MQTT: the durable PUBLISH action on %q could not be drained; its packet "+
		"identifier stays reserved and the drain is retried until it lands",
		tok.slot.name.subject)
	// RECORDED WHERE SHUTDOWN CAN SEE IT, beside the timer rather than inside it. The
	// timer is refused once the dispatch gate closes, and an entry can change phase or
	// vanish under either; the obligation is to a durable record and outlives both, so it
	// is held by identity on the server until some attempt discharges it.
	sess.noteOwedDrain(c, tok)
	sess.schedulePublishDrainReconcile(c, tok)
	return false
}

// noteOwedDrain and clearOwedDrain reach the server's shutdown-visible registry. The
// session hop exists because a drain may run with no client (the shutdown pass itself),
// and the session is the one object that always knows its server.
func (sess *mqttSession) noteOwedDrain(c *client, tok mqttSendToken) {
	if srv := sess.serverFor(c); srv != nil {
		srv.noteOwedDrain(sess, tok)
	}
}

func (sess *mqttSession) clearOwedDrain(c *client, tok mqttSendToken) {
	if srv := sess.serverFor(c); srv != nil {
		srv.clearOwedDrain(tok)
	}
}

// serverFor returns the server this session belongs to, preferring the caller's client and
// falling back to the session's own JSA client so a drain with no caller still reaches it.
func (sess *mqttSession) serverFor(c *client) *Server {
	if c != nil && c.srv != nil {
		return c.srv
	}
	if sess.jsa != nil && sess.jsa.c != nil {
		return sess.jsa.c.srv
	}
	return nil
}

// schedulePublishDrainReconcile retries an unresolved PUBLISH-action drain on the session's
// own reconciliation lifecycle, which is the same mechanism `scheduleActionReconcile`
// gives an unresolved PUBREL action and for the same reason: after the send section ends,
// NO FUTURE EVENT IS GUARANTEED to retry it.
//
// SHUTDOWN-AWARE on the shared gate: the timer is armed only while the gate admits it, so
// an unlimited retry can never hold shutdown open or touch torn-down JetStream state. A
// refused arming leaves the durable token, which the next resume reclassifies — that is
// the disclosed availability residual, reached only when the process is going away rather
// than on every transient fault.
//
// Session lock NOT held on entry.
func (sess *mqttSession) schedulePublishDrainReconcile(c *client, tok mqttSendToken) {
	if !tok.actionHeld || tok.token == _EMPTY_ {
		return
	}
	sess.mu.Lock()
	// The GENERATION is part of the identity: a proven discard means the next incarnation
	// uses different subjects entirely, so an old-generation token is teardown material
	// rather than retry state.
	stale := tok.slot.name.gen != sess.gen
	sess.mu.Unlock()
	if stale {
		return
	}
	var srv *Server
	if c != nil {
		srv = c.srv
	}
	if srv == nil || !srv.admitWillWorker() {
		return
	}
	time.AfterFunc(mqttActionReconcileDelay, func() {
		defer srv.willWorkerWG.Done()
		sess.mu.Lock()
		stop := tok.slot.name.gen != sess.gen
		sess.mu.Unlock()
		if stop {
			return
		}
		// drainPublishAction re-reads and re-proves the token before acting, and
		// re-enrols itself if it is still unresolved, so this is one attempt rather than
		// a loop.
		//
		// WAKING BELONGS HERE TOO, and its absence was a hole: on this path the local
		// section closed long ago — endPublishSend's own wake ran while the token was
		// still standing and found nothing to release — so a stored PUBREL refused on the
		// durable half has no other event left. Only a RESOLVED drain wakes: NAKing on an
		// attempt that changed nothing buys a re-offer, a fresh refusal and a fresh
		// registration once per reconcile delay, for as long as the fault lasts.
		// The wake is conditioned on BOTH facts. A resolved drain says only that THIS
		// captured action is over — a replacement action, or a local section this timer
		// outran, is still a blocker — so the combined check is what decides whether a
		// waiter may be consumed.
		if sess.drainPublishAction(c, tok) {
			sess.wakePubRelRedriveIfClear(tok.slot.name.pi, tok.slot.name.subject)
		}
	})
}

// mqttIntentOutcome is what recordSendIntent reports about the durable intent record.
type mqttIntentOutcome uint8

const (
	// mqttIntentNone: the entry held no marker, so no intent was owed (already
	// possibly-attempted or transmitted).
	mqttIntentNone mqttIntentOutcome = iota
	// mqttIntentConfirmed: the marker is durably gone — including "already gone", a
	// re-attempt whose earlier issue landed. The write may proceed.
	mqttIntentConfirmed
	// mqttIntentDefiniteFailure: the server answered and refused. Nothing changed durably;
	// the entry stays proven-unsent, nothing is written, AckWait re-offers.
	mqttIntentDefiniteFailure
	// mqttIntentAmbiguous: no authoritative answer (timeout, no responders, connection
	// loss). The marker MAY be gone. Nothing is written, nothing is released, and the entry
	// is held unresolved until a re-attempt of the same delete settles it.
	mqttIntentAmbiguous
)

// recordSendIntent makes the exchange durably POSSIBLY-ATTEMPTED before the socket write,
// by deleting its unsent marker WITH CONFIRMATION. It is the linearization point of the
// DUP derivation: a crash anywhere after this returns Confirmed leaves marker-absent, which
// recovery reads as "may have been written" and answers with DUP=1 and a held identifier.
// A crash before it leaves the pair intact, which recovery reads as proven-unsent, DUP=0.
//
// It is never issued for an entry holding no marker (intentNone): the confirmed operation
// is once per exchange. The result is CLASSIFIED, never read back: a definite refusal
// (the server answered) leaves the entry exactly as it was; an ambiguous one holds the
// entry unresolved so the next section re-attempts these exact bytes
// (feedback_ambiguous_write_reattempt). Session lock NOT held: this is a JetStream request.
func (sess *mqttSession) recordSendIntent(c *client, pi uint16, tok mqttSendToken) mqttIntentOutcome {
	if tok.markerSeq == 0 {
		return mqttIntentNone
	}
	mk := mqttMarkerObservation{prov: mqttMarkerCaptured, outcome: mqttMarkerMatch, seq: tok.markerSeq}
	v, err := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpRecordIntent, caller: mqttMarkerCallerSender},
		mqttRegisterObservation{kind: mqttRegisterUnobserved}, mk)
	if err != nil {
		mqttErrorf(c, "MQTT: recording send intent for packet identifier %d of client %q: %v", pi, sess.id, err)
		return mqttIntentDefiniteFailure
	}
	if v.disposition != mqttMarkerDeleteMarker || sess.asm == nil {
		return mqttIntentDefiniteFailure
	}
	derr := sess.asm.deleteInflightMarker(mqttMarkerCallerSender, tok.regSubject, tok.markerSeq, true)
	switch {
	case derr == nil:
		return mqttIntentConfirmed
	case mqttDeleteRefusedBeforeMutation(derr):
		// The server REFUSED before touching the store: nothing landed. (Already-gone is
		// a success inside deleteInflightMarker, so it never reaches here; a generic
		// delete failure is NOT definite — see mqttDeleteRefusedBeforeMutation.)
		mqttWarnf(c, "MQTT: send intent for packet identifier %d of client %q refused, nothing transmitted: %v", pi, sess.id, derr)
		return mqttIntentDefiniteFailure
	default:
		mqttWarnf(c, "MQTT: send intent for packet identifier %d of client %q is unresolved, nothing transmitted until it is re-attempted: %v", pi, sess.id, derr)
		return mqttIntentAmbiguous
	}
}

// beginPubRelSendLocked reserves an identifier for ONE PUBREL transmission and reports
// the token that ends it.
//
// The reservation is taken here rather than after the write, because the packet is
// queued under the connection lock and can reach the client before the enqueue call
// returns — counting the owed PUBCOMP afterwards would leave a window in which one is
// owed and unrecorded.
//
// Session lock held on entry.
func (sess *mqttSession) beginPubRelSendLocked(cc *client, pi uint16) (string, bool) {
	// A ZERO cid is refused rather than recorded. Every accepted connection gets its
	// identity from srv.gcid.Add(1), so zero is not a connection identity — it is an
	// uninitialized one, and recording it would make two such connections
	// indistinguishable: a foreign PUBCOMP could consume this debt, and the debt could
	// stay live across a rebind. Refusing here is what keeps that case out of the
	// predicate entirely, so no hold can ever carry a zero owner.
	if cc == nil || pi == 0 || cc.cid == 0 || sess.c != cc || sess.recoveryPIHeldLocked(pi) {
		return _EMPTY_, false
	}
	h := sess.sendsInFlight[pi]
	if h != nil && h.token != _EMPTY_ {
		// A transmission is already going out under this identifier — an earlier PUBREL
		// copy, or the PUBLISH whose successful PUBREC replaced the entry while its
		// write was still blocked. The caller returns WITHOUT acknowledging, so AckWait
		// re-offers the record once that write has returned.
		return _EMPTY_, false
	}
	// NO retained-term refusal here, and NO fence on a live pendingPublish entry. Durable
	// SLOT-BACKED PUBREL admission has already acquired the exact owner-slot action before
	// reaching this primitive; a live retained exchange has no slot (R6) and reaches this
	// opener with no such action, authorized instead by its in-memory owner — which is
	// exactly the overlap the passage below turns on. Reintroducing a broader, map-shaped
	// refusal here would contradict that authority and break the legitimate retained and
	// drain overlaps below.
	//
	// A retained-only hold may therefore coexist with the PUBREL token and PUBCOMP debt this
	// call is about to add. That is coherent: the owner matches so nothing is reset, and
	// endSend and consumePubRelAckLocked both ask the shared liveness predicate, so each term
	// is removed only by the event that discharges it.
	//
	// Three refusals were tried here and all three were REFUTED — do not re-propose any:
	//
	//   - refusing on a live RETAINED term refuses the retained QoS 2 exchange's OWN PUBREL,
	//     because the delivery-side flusher keeps the hold live until after it writes the
	//     triggering live PUBLISH; the record then waits out AckWait where HEAD transmitted at
	//     once. The compensating "retire the term at PUBREC" needed two further fixes and
	//     still lost custody across a concurrent clear() in one ordering or the other;
	//   - refusing on ANY live pendingPublish entry breaks a drain, which
	//     legitimately recreates a CORRELATED PUBREL under a recovered entry's identifier
	//     (measured: three drain tests timed out waiting for a PUBREL the client was owed);
	//   - refusing on a SOURCE-LESS entry starves an abandoned frame's record for the life of
	//     the connection, since that empty entry survives until the next resume.
	//
	// The stale-PUBREL multiplexing hazard is owned by F-092's owner-slot action, acquired
	// by the delivery callback before this opener. F-091's reservation still stops bumpPI
	// and any PUBLISH send section from reusing the identifier, which is what it was
	// chartered to do.
	// A standing debt is only ours to add to when this connection incurred it. One owed by a
	// predecessor can never be answered — mqttProcessPubComp refuses a PUBCOMP from any
	// connection that is not sess.c — so it is RESET rather than accumulated onto. Adding to
	// it instead is what would leave a resumed session's identifier held forever: the client
	// owes exactly one PUBCOMP on this connection, and a count of two would never reach zero.
	if h == nil || h.owner != cc.cid {
		// An action is NOT connection-owned. A successor resets the predecessor's inert
		// PUBCOMP debt but must carry an action it just acquired (or is still draining)
		// across that replacement. Dropping it here makes the durable token invisible to
		// bumpPI as soon as the new debt is consumed.
		var action *mqttSendAction
		var slotOwed *mqttSlotExclusion
		if h != nil {
			action = h.action
			// CARRIED FOR THE SAME REASON AS THE ACTION, and omitting it was a hole: a
			// legitimate recovered PUBREL replaces a hold whose owner differs, and a
			// placeholder starts at owner zero, so the replacement would silently erase an
			// exclusion protecting an exchange a peer may still be transmitting. Carrying it
			// blocks no PUBREL custody — the opener does not refuse on this term.
			slotOwed = h.slotOwed
		}
		h = &mqttSendHold{owner: cc.cid, action: action, slotOwed: slotOwed}
		if sess.sendsInFlight == nil {
			sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
		}
		sess.sendsInFlight[pi] = h
	}
	token := mqttNewInflightNonce()
	h.token = token
	h.owner = cc.cid
	h.pubCompOwed++
	h.relEntered = false
	return token, true
}

// beginRetainedSendLocked reserves pi for ONE buffered retained PUBLISH, from this
// allocation until the frame has been written or is provably unwritable. "Written" ends
// the reservation only while the allocating incarnation is still current: past a clear()
// this hold is the identifier's ONLY reservation and outlives the write. See
// releaseRetainedSendWrittenLocked.
//
// Taken at ALLOCATION rather than at the flush, in the same sess.mu acquisition that runs
// trackPublishRetained, so no goroutine ever observes the identifier allocated and
// unreserved. That is also why the caller's own gate matters: serializeRetainedMsgsForSub
// runs holding asm.sessLocked[sess.id], so no successor can rebind between the two.
//
// Session lock held on entry.
func (sess *mqttSession) beginRetainedSendLocked(cc *client, pi uint16) bool {
	// A ZERO cid is refused rather than recorded, exactly as beginPubRelSendLocked
	// refuses it: zero is an uninitialized connection identity, not a connection
	// identity, and two of them would be indistinguishable to every ownership
	// comparison. sess.c != cc keeps a reservation from being opened on behalf of a
	// connection this session no longer serves.
	if cc == nil || pi == 0 || cc.cid == 0 || sess.c != cc {
		return false
	}
	h := sess.sendsInFlight[pi]
	// A hold that is LIVE for ANY reason refuses. Including an already-live retained
	// reservation: one identifier is allocated per retained message, so a second live
	// one is an invariant violation rather than a case to serve. bumpPI cannot in fact
	// hand out a live identifier — it asks sendReservedLocked — so this is a defensive
	// agreement with the allocator, stated as an invariant rather than relied on.
	if sess.sendHoldLiveLocked(h) {
		return false
	}
	// An INERT hold is REPLACED, and the replacement takes THIS connection's owner.
	// Presence is not a reservation (F-089's rule 2), so an inert hold reserves nothing
	// for anybody and taking it over is what keeps the identifier from being stranded
	// for the session's life. Inheriting the predecessor's owner instead would leave the
	// new reservation inert on arrival.
	if h == nil {
		if sess.sendsInFlight == nil {
			sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
		}
		h = &mqttSendHold{}
		sess.sendsInFlight[pi] = h
	}
	h.owner = cc.cid
	h.retainedGen = sess.gen
	return true
}

// releaseRetainedSendAbandonedLocked ends the reservation of a retained frame that is
// provably never going to be written — its subscription has left c.subs, or the frame was
// discarded before it was ever buffered.
//
// Unconditional, and the generation fence deliberately does NOT apply: no bytes reached
// the client, so there is nothing for a re-minted identifier to collide with. Holding on
// here would reserve the identifier against reallocation for the session's life while
// nothing will ever write the frame (F-091).
//
// IT ALSO RELEASES THE PENDING ENTRY, and this is the ONE site in the tree that may. A
// retained send has no register and no source (R6), so no durable pair can ever say whether
// its PUBLISH was transmitted, and abandonment is the only fact that proves it was not.
// F-091's departure property is what makes that proof total — every frame is WRITTEN by a
// flusher, or DETACHED and then TRANSFERRED to a same-sid replacement or ABANDONED here,
// never both and never neither — so a retained entry that has NOT been abandoned is
// possibly-attempted and goes on reserving its identifier. Deciding it anywhere else means
// inferring "never sent" from something that is not the proof, which is what
// dropUnredeliverableRetained did on resume until F-101 deleted it: having no way to
// redeliver is a fact about this broker, never about what the client received.
//
// Keyed by the ALLOCATION, not the identifier. A disposition can arrive after a clear() has
// removed the entry and reset last_pi, so an identifier-keyed untrack would remove whichever
// exchange holds it by then. A nonce is minted with its allocation and never reused, so a
// match proves this entry IS that retained exchange and a mismatch leaves it alone. Nothing
// is checked beside the nonce, because nothing else would say what the nonce does not.
//
// Session lock held on entry.
func (sess *mqttSession) releaseRetainedSendAbandonedLocked(cc *client, pi uint16, nonce string) {
	h := sess.sendsInFlight[pi]
	if h == nil || cc == nil || cc.cid == 0 || h.owner != cc.cid || h.retainedGen == _EMPTY_ {
		return
	}
	if ack := sess.pendingPublish[pi]; ack != nil && nonce != _EMPTY_ && ack.regNonce == nonce {
		sess.untrackPublish(pi, mqttUntrackUnanswered) // retained: no AckRoute (outbound_ack_route.go)
	}
	sess.dropRetainedSendLocked(pi, h)
}

// dropRetainedSendLocked clears the retained term and removes the hold once it reserves
// nothing more. The removal asks the shared predicate rather than re-spelling it, which
// is what keeps a co-resident write token or PUBCOMP debt from being discarded with it.
//
// Session lock held on entry.
// handRetainedHoldToSectionLocked ends a snapshot's retained hold on pi so a send section can open
// over the same identifier (F-105 7c): the retained hold would make the section refuse, and the
// caller opens the section in this same critical section, so the identifier is never unreserved
// in between.
//
// Lock held on entry.
func (sess *mqttSession) handRetainedHoldToSectionLocked(pi uint16) {
	if h := sess.sendsInFlight[pi]; h != nil && h.retainedGen != _EMPTY_ {
		sess.dropRetainedSendLocked(pi, h)
	}
}

func (sess *mqttSession) dropRetainedSendLocked(pi uint16, h *mqttSendHold) {
	h.retainedGen = _EMPTY_
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
}

// abandonRetainedSends is endRetainedSends' counterpart for frames that will never be
// written, and it is the reason the two verbs are separate: this one is not fenced by the
// generation, and it is the one that releases the pending entry as well as the hold.
//
// It takes the frames' RESERVATIONS rather than their bare identifiers, because the entry it
// releases is named by the allocation and not by the identifier — see
// releaseRetainedSendAbandonedLocked.
//
// Session lock NOT held on entry.
func (sess *mqttSession) abandonRetainedSends(cc *client, refs []mqttRetainedRef) {
	if len(refs) == 0 {
		return
	}
	sess.mu.Lock()
	for _, r := range refs {
		sess.releaseRetainedSendAbandonedLocked(cc, r.pi, r.nonce)
	}
	sess.mu.Unlock()
}

// consumePubRelAckLocked records that the client answered one transmitted PUBREL under
// pi, which is the event [MQTT-2.3.1-4] frees the identifier at.
//
// A debt owned by another connection is left alone: it is not this connection's to
// consume, and consuming it would release an identifier on the strength of an
// acknowledgement for a transmission this connection never made. It already reserves
// nothing, so leaving it costs nothing.
//
// Session lock held on entry.
func (sess *mqttSession) consumePubRelAckLocked(c *client, pi uint16) {
	h := sess.sendsInFlight[pi]
	if h == nil || c == nil || c.cid == 0 || h.owner != c.cid || h.pubCompOwed == 0 {
		return
	}
	h.pubCompOwed--
	// The shared predicate, NOT a hand-written "token empty and no debt". Re-spelling the
	// condition is what made this mutator blind to the retained term: a PUBCOMP would
	// have deleted the whole hold and with it a buffered retained PUBLISH's only
	// reservation (F-091). endSend already asks the predicate for the same reason.
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
}

// pubCompHeldLocked reports whether this hold still reserves its identifier because a
// PUBCOMP is owed on THIS connection.
//
// Session lock held on entry.
func (sess *mqttSession) pubCompHeldLocked(h *mqttSendHold) bool {
	return h != nil && h.pubCompOwed > 0 && sess.holdOwnerLiveLocked(h.owner)
}

// pubCompEmittableOwnerLocked is the connection that owes the client's PUBCOMP for a PUBREL
// POSSIBLY EMITTED under pi; zero when none is. It is the live debt of pubCompHeldLocked less
// the open attempt's unit while that attempt has not entered its write (F-116).
//
// Exact for the one reader that needs it, the lineage release: release and write entry both
// run under sess.mu, and once the release has converted or untracked the entry, write entry
// refuses it — so an attempt not entered at the release never emits, and a released delivery
// built on its unit would await a PUBCOMP nobody will send. Liveness is NOT this: the open
// unit still reserves the identifier until the section ends.
//
// Session lock held on entry.
func (sess *mqttSession) pubCompEmittableOwnerLocked(pi uint16) uint64 {
	h := sess.sendsInFlight[pi]
	if !sess.pubCompHeldLocked(h) {
		return 0
	}
	owed := h.pubCompOwed
	if h.token != _EMPTY_ && h.pubIdent == _EMPTY_ && !h.relEntered {
		owed--
	}
	if owed <= 0 {
		return 0
	}
	return h.owner
}

// markPubRelEnteredLocked records that the PUBREL attempt holding token has entered its write.
// Called in the critical section in which write entry succeeded; a token that is no longer the
// hold's marks nothing, so the fact can never describe another attempt.
//
// Session lock held on entry.
func (sess *mqttSession) markPubRelEnteredLocked(pi uint16, token string) {
	if h := sess.sendsInFlight[pi]; h != nil && token != _EMPTY_ && h.token == token {
		h.relEntered = true
	}
}

// retainedHeldLocked reports whether this hold still reserves its identifier because a
// buffered retained PUBLISH under it is unwritten, or was written past a clear() that
// destroyed the pendingPublish entry which would otherwise have carried custody on.
//
// Owner-scoped like pubCompHeldLocked, and sound for a different reason: once the
// connection is no longer the session's, no ADDITIONAL identifier-bearing PUBLISH under
// this identifier can reach that socket, so the orphaned frame is one PUBLISH under one
// identifier and reserves nothing. See mqttSendHold.retainedGen (F-091).
//
// Session lock held on entry.
func (sess *mqttSession) retainedHeldLocked(h *mqttSendHold) bool {
	return h != nil && h.retainedGen != _EMPTY_ && sess.holdOwnerLiveLocked(h.owner)
}

// sendHoldLiveLocked reports whether a hold reserves its identifier at all.
//
// Session lock held on entry.
func (sess *mqttSession) sendHoldLiveLocked(h *mqttSendHold) bool {
	return h != nil && (h.token != _EMPTY_ || sess.pubCompHeldLocked(h) ||
		sess.retainedHeldLocked(h) || actionHeldLocked(h) || h.terminalOwed != _EMPTY_ ||
		slotExclusionHeldLocked(h))
}

// inflightSubjectLocked is this session's slot subject for pi, in the CURRENT generation.
// Empty when the session cannot name one, which is what makes an exclusion install a no-op
// rather than a guess.
//
// Session lock held on entry.
func (sess *mqttSession) inflightSubjectLocked(pi uint16) string {
	if pi == 0 || sess.idHash == _EMPTY_ || sess.gen == _EMPTY_ {
		return _EMPTY_
	}
	return mqttInflightSubject(sess.idHash, sess.gen, pi)
}

// dropProvenUnsentPublishEntry removes an entry this broker has PROVEN never transmitted and
// hands off the identifier's exclusion while doing it (F-098, holistic review round 3).
//
// FOUR call sites share it, and they are the whole class: Message Expiry and a Maximum
// Packet Size refusal, each on the plain delivery path and on the affine one. Every one had
// open-coded the same untrack-then-delete, and the review found them one round at a time —
// which is why this is a named function over a population rather than a patch per site.
//
// Only an exchange PROVEN never transmitted reaches any of them — a possibly-sent redelivery
// is resent, not dropped ([MQTT-4.4.0-1]) — so the DELETE is licensed. What is not licensed
// is treating the untrack as the moment the identifier becomes free: the delete can be refused
// by the freshness fence when a peer acquired or rewrote the slot, or it can simply fail,
// and this path deliberately ignores its return because the source acknowledgement must not
// wait on it. `bumpPI` consults the send registry rather than the durable record, so without
// the exclusion a surviving register would sit under a reusable identifier.
//
// REGISTER-BACKED ONLY, which is the whole population at risk. Expiry is decided BEFORE the
// pair is written, so a freshly allocated entry dropped here never paid for a register; the
// one shape that arrives holding one is a RECOVERED never-transmitted entry, restored as a
// live source and expired on a later delivery. Recovery's own exclusions cannot cover it,
// because at recovery time nothing was being removed. A registerless retained send keeps its
// existing behaviour untouched.
//
// It is ONE function rather than a sequence open-coded at each call site so the property has
// something to test: the allocator-level regression drives exactly what production runs.
//
// Session lock NOT held on entry. Returns the removed entry for the caller's own
// continuation, exactly as untrackPublishEntry did.
func (sess *mqttSession) dropProvenUnsentPublishEntry(pi uint16, lic mqttDestroyLicence) *mqttPending {
	sess.mu.Lock()
	if subject := sess.inflightSubjectLocked(pi); subject != _EMPTY_ {
		if ack := sess.pendingPublish[pi]; ack != nil && ack.regNonce != _EMPTY_ &&
			ack.jsDur != _EMPTY_ && ack.sseq != 0 {
			sess.excludeIdentifierLocked(pi, subject, ack.regNonce)
		}
	}
	removed := sess.untrackPublishEntry(pi, mqttUntrackUnanswered)
	sess.mu.Unlock()
	// The identifier is released, so its register must go with it (R5), and its marker with
	// the register. Ordered before the source ack, so a crash in between leaves a pair over
	// a source that is still live rather than one over a completed delivery.
	sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, lic)
	// ...and the exclusion is discharged only by a CONFIRMED ABSENCE of the subject, on the
	// same terms as every other lift: an exact-sequence delete reads an already-absent
	// sequence as success, so its return cannot tell a removal from a same-identity rewrite
	// that landed first.
	if removed != nil && removed.slot.slotOK() {
		slot := removed.slot // a copy: no pointer into a pending entry escapes
		sess.settleRecoveryDropExclusion(&slot, removed.regNonce, removed.jsDur, removed.sseq)
	}
	return removed
}

// settleRecoveryDropExclusion lifts the exclusion a RECOVERY DROP installed, and only
// against a confirmed absence of that register's subject.
//
// Recovery installs an exclusion for every drop candidate before allocation opens, because a
// staged drop is not a completed one: the delete can be refused by the freshness fence, can
// fail, or can be skipped when an earlier unresolved terminal advance ends the pass. This is
// the other half of that bargain — the proof that discharges it.
//
// It is NOT the delete's return value that discharges it. An exact-sequence delete reports
// success for an already-absent sequence, so "deleted" cannot distinguish a removal from a
// same-identity rewrite that landed first; only a subject read can, and only
// mqttReleaseEstablishedAbsent counts. mqttReleaseEstablishedOther deliberately does not
// lift: a successor occupying the subject is exactly what the exclusion is for.
//
// The instance is captured BEFORE the confirming read and re-checked under the lock after
// it, so a restriction installed or strengthened while the read was out survives this stale
// answer.
//
// Session lock NOT held on entry.
func (sess *mqttSession) settleRecoveryDropExclusion(ref *mqttInflightSlotRef, nonce, dur string, sseq uint64) {
	if ref == nil || ref.name.pi == 0 || ref.name.subject == _EMPTY_ || nonce == _EMPTY_ {
		return
	}
	sess.mu.Lock()
	inst := sess.slotExclusionInstLocked(ref.name.pi)
	sess.mu.Unlock()
	if inst == 0 {
		return // nothing excluded here
	}
	// The comparison record carries the identity the confirmation partitions on, and
	// nothing else: this is the exchange this drop was classified against.
	if sess.confirmSlotReleased(ref.name.subject,
		&mqttInflightRecord{Nonce: nonce, Dur: dur, Seq: sseq}) != mqttReleaseEstablishedAbsent {
		return
	}
	sess.mu.Lock()
	sess.liftIdentifierExclusionLocked(ref.name.pi, inst, map[string]bool{ref.name.subject: true})
	sess.mu.Unlock()
}

// excludeIdentifierLocked hands an identifier's EXCLUSION to the hold, which is what a
// removal site owes when it cannot confirm the durable slot released.
//
// The site still removes exactly what it removed before. This adds a restriction and
// never a capacity charge, so no capacity semantics change anywhere.
//
// `nonce` is EMPTY where the site cannot name the occupant — a staged recovery release
// (commitRecoveryReleaseLocked) creates exactly that shape — and an empty bind is a real
// restriction, not a refusal.
//
// Session lock held on entry.
func (sess *mqttSession) excludeIdentifierLocked(pi uint16, subject, nonce string) {
	if pi == 0 || subject == _EMPTY_ {
		return
	}
	h := sess.sendsInFlight[pi]
	if h == nil {
		if sess.sendsInFlight == nil {
			sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
		}
		h = &mqttSendHold{}
		sess.sendsInFlight[pi] = h
	}
	if h.slotOwed == nil {
		h.slotOwed = &mqttSlotExclusion{}
	}
	// A SET, not a list: an equivalent restriction adds no protection and would accumulate
	// without bound, since an excluded identifier still admits legitimate PUBREL custody.
	//
	// BUT THE INSTANCE STILL ADVANCES (holistic review round 2, finding 1). Deduplicating
	// the STORAGE and deduplicating the EVENT are different things, and only the first is
	// safe. A reassertion is a fresh statement that this identifier is restricted NOW, and
	// any absence observation taken before it is stale by construction:
	//
	//	1. a release continuation captures instance N and reads the subject as absent;
	//	2. a peer installs a successor on that subject;
	//	3. finalizeTerminalSlot's mismatch arm reasserts the same (subject, EMPTY) bind —
	//	   the unnamed shape, because the occupant is not ours to name;
	//	4. if that reassertion returned without advancing N, the continuation from step 1
	//	   could still lift on its stale absence, and bumpPI would hand the identifier out
	//	   while the successor's register stands.
	//
	// Capturing the instance before the read does not fence that schedule; only advancing
	// it on every reassertion does. The counter is the invalidation token, not a bind count.
	//
	// ONE EXIT, so the arming below cannot be reached by one path and missed by the other.
	// The two-exit shape this replaced also stated the saturation rule twice and applied it
	// once: only the duplicate path refused at the limit, while the append path incremented
	// unguarded and could wrap. Storing the bind once and advancing once, under one guard,
	// is what makes both facts true everywhere rather than per branch.
	found := false
	for _, b := range h.slotOwed.binds {
		if b.subject == subject && b.nonce == nonce {
			found = true
			break
		}
	}
	if !found {
		h.slotOwed.binds = append(h.slotOwed.binds, mqttSlotExclusionBind{subject: subject, nonce: nonce})
	}
	// SATURATION REFUSES rather than wraps, as it does in the lift. At the limit no lift
	// can succeed anyway, so leaving the instance is safe in that direction — and the bind
	// above still stands, so the restriction itself is never lost to saturation.
	if sess.slotExclusionInst != math.MaxUint64 {
		sess.slotExclusionInst++
		h.slotOwed.inst = sess.slotExclusionInst
	}
	// THE INSTALL IS THE ARM. Every site that excludes an identifier reaches this one
	// statement, so the reclamation continuation covers all of them by construction — the
	// same reason slotExclusionHeldLocked makes the exclusion effective at all six
	// hold-deletion sites without a per-site guard. A per-site arm over this function's 27
	// callers is the shape F-098's own review history rejected one instance at a time.
	sess.armExclusionReclaimLocked()
}

// slotExclusionInstLocked is the instance a caller must carry with an absence observation
// taken outside the lock, so a reservation installed or strengthened afterwards survives
// that stale result. Zero when nothing is excluded.
//
// Session lock held on entry.
func (sess *mqttSession) slotExclusionInstLocked(pi uint16) uint64 {
	if h := sess.sendsInFlight[pi]; h != nil && h.slotOwed != nil {
		return h.slotOwed.inst
	}
	return 0
}

// liftIdentifierExclusionLocked discharges an exclusion against a CONFIRMED ABSENCE of
// every subject it binds, at the instance the observation was taken.
//
// `absent` is the set of subjects proven absent by the caller's own confirmation. The lift
// requires all of them, because one constituent's absence never discharges a conjunction.
//
// Session lock held on entry.
func (sess *mqttSession) liftIdentifierExclusionLocked(pi uint16, inst uint64, absent map[string]bool) {
	h := sess.sendsInFlight[pi]
	if h == nil || h.slotOwed == nil || inst == 0 {
		return
	}
	// SATURATION REFUSES rather than wraps: continuing to accept matching lifts at the
	// counter's limit is the same unsafety as wrapping it.
	if sess.slotExclusionInst == math.MaxUint64 {
		return
	}
	if h.slotOwed.inst != inst {
		return // installed or strengthened after the observation: it is not this one
	}
	for _, b := range h.slotOwed.binds {
		if !absent[b.subject] {
			return
		}
	}
	h.slotOwed = nil
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
}

// adoptIdentifierExclusionLocked lets a RECOVERED exchange reclaim its own exclusion, so a
// restored entry is never blocked from transmitting by a reservation it owns.
//
// It requires a UNANIMOUS, non-empty, matching identity: every bind must name this exact
// subject and nonce. Any empty bind, or a second differing nonce, makes the exclusion
// permanently un-adoptable — that is what stops a named restriction arriving over an
// unidentified one from making the unidentified one adoptable.
//
// Nonce equality transfers LOCAL ALLOCATION CUSTODY only. It is not authority to transmit:
// acquirePublishAction keeps its own fresh nonce, phase and action checks and its CAS
// fence, none of which this touches.
//
// Session lock held on entry.
func (sess *mqttSession) adoptIdentifierExclusionLocked(pi uint16, subject, nonce string) bool {
	h := sess.sendsInFlight[pi]
	if h == nil || h.slotOwed == nil || nonce == _EMPTY_ || subject == _EMPTY_ {
		return false
	}
	for _, b := range h.slotOwed.binds {
		if b.nonce != nonce || b.subject != subject {
			return false
		}
	}
	h.slotOwed = nil
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
	return true
}

// slotExclusionHeldLocked is the SIXTH liveness term, and adding it here is what makes the
// exclusion effective everywhere at once rather than site by site.
//
// Every one of the six places that deletes a hold asks this predicate first —
// releaseTerminalCleanupHold, dropRetainedSendLocked, endPubCompDebtLocked,
// sweepDeadSendHoldsLocked, endPublishSend and retireActionLocked_ — so an exclusion makes
// the hold undeletable at all of them BY CONSTRUCTION, with no per-site guard. The comment
// on endPubCompDebtLocked says why the predicate was built that way: a PUBCOMP would
// otherwise delete the whole hold and with it a buffered retained PUBLISH's only
// reservation (F-091). Only the INSTALL side needs a per-site audit.
//
// NOT owner-scoped, for the same reason actionHeldLocked is not: the obligation belongs to
// an exchange a peer may still be transmitting, not to whoever owns this connection now.
//
// Session lock held on entry.
func slotExclusionHeldLocked(h *mqttSendHold) bool {
	return h != nil && h.slotOwed != nil && len(h.slotOwed.binds) > 0
}

// actionHeldLocked is the FOURTH liveness term, and the only one that is NOT owner-scoped.
//
// The other three ask whether the connection that incurred them is still ours, because a
// debt owed to a departed connection can never be answered and a retained frame nobody
// will write reserves nothing. A durable action is different in kind: it is a claim on the
// SLOT, and it outlives whoever owns the connection now. A holder that acquired an action
// must be able to drain it after a takeover — refusing that would abandon the action and
// manufacture an unbounded identifier reservation out of an ordinary, fully successful
// takeover, which is the residual this whole mechanism exists to bound rather than create.
//
// So the term is live from the moment the tentative holder is armed until a reread proves
// the exact token is GONE — resolved forward, replaced by a different valid token, or the
// slot absent. `released` alone does not end it: a locally released but durably unresolved
// action still holds its identifier, because nothing else knows the write will not happen.
//
// Session lock held on entry.
func actionHeldLocked(h *mqttSendHold) bool {
	return h != nil && h.action != nil && !h.action.drained
}

// sendReservedLocked is the question every reservation reader asks: may pi be used?
// Lookup and liveness together, so no caller has to reach into the registry to find
// out — which is what makes "reserved" one testable API rather than a rule five call
// sites have to remember.
//
// Session lock held on entry.
func (sess *mqttSession) sendReservedLocked(pi uint16) bool {
	return sess.sendHoldLiveLocked(sess.sendsInFlight[pi])
}

// sendTransmissionOpenLocked reports whether a PUBLISH transmission of pi is still being
// written — the one question an abandonment's classification must wait for. It is deliberately
// NOT the reservation question (sendReservedLocked): an acknowledged exchange's terminal
// identifier hold also reserves the identifier, and must not block the handoff that settles it.
//
// Session lock held on entry.
func (sess *mqttSession) sendTransmissionOpenLocked(pi uint16) bool {
	h := sess.sendsInFlight[pi]
	return h != nil && h.token != _EMPTY_
}

// sweepDeadSendHoldsLocked drops holds that reserve nothing: no write out, and no
// acknowledgement owed by the connection that is ours now.
//
// This is BOUNDED-MEMORY HYGIENE and nothing rests on it. An inert hold is invisible to
// sendReservedLocked, so one this never reaches blocks no allocation and no
// transmission. It is emphatically NOT the drain for the acknowledgement debt: clear()
// never assigns sess.c, a persistent-session disconnect never calls clear() at all, and
// on the clean-start path clear() runs while the displaced client is still bound — so a
// debt dropped here would leak in the first case and free an identifier the client is
// still holding in the third (F-089).
//
// Session lock held on entry.
func (sess *mqttSession) sweepDeadSendHoldsLocked() {
	for pi, h := range sess.sendsInFlight {
		if !sess.sendHoldLiveLocked(h) {
			delete(sess.sendsInFlight, pi)
		}
	}
}

// boundOwnerCIDLocked is the cid of the connection currently bound, or 0 for none.
func (sess *mqttSession) boundOwnerCIDLocked() uint64 {
	if sess.c == nil {
		return 0
	}
	return sess.c.cid
}

// holdOwnerLiveLocked reports whether ownerCID still holds identifiers on this
// session.
//
// The bound connection does. So does one a two-phase retirement DETACHED, but only
// while NOTHING is bound in its place (F-036 unit 1): the displaced connection is
// closed on another goroutine, so between the detach and the next bind its write can
// still be in the socket and its PUBCOMP debt still owed — freeing the identifier
// there is what F-089 forbids. THE REBIND IS THE EVENT that retires it: once
// ownership has moved, no further PUBLISH under that identifier can reach the
// predecessor's socket, so the hold reserves nothing and the sweep at the rebind
// drops it. Keying on "nothing bound" rather than on clearing a field at each
// ownership move is what keeps that true for every path that binds, not just the
// one that remembered to reset it.
func (sess *mqttSession) holdOwnerLiveLocked(ownerCID uint64) bool {
	if ownerCID == 0 {
		return false
	}
	if bound := sess.boundOwnerCIDLocked(); bound != 0 {
		return ownerCID == bound
	}
	return ownerCID == sess.retiringOwnerCID
}

// endSend ends a send section and executes whatever continuation a removal path parked
// while the write was out.
//
// Guarded by the TOKEN, not by map residency (a condemned entry's disposition is
// completed here) and not by register identity (a rollback restores the same pointer).
//
// Session lock NOT held on entry: the parked continuation is extracted under the lock,
// and the durable work it owes runs after the unlock.
func (sess *mqttSession) endSend(c *client, pi uint16, token string) {
	sess.endSendWith(c, pi, token, nil)
}

// endPublishSend releases a QoS 1/2 send section and records, IN MEMORY ONLY, what the
// section established. The durable record was made BEFORE the write by recordSendIntent;
// nothing durable happens here, because a record made after the write is exactly the one a
// crash loses (holistic Finding 1).
//
//   - intent Confirmed: the marker is gone, so the exchange is durably possibly-attempted
//     whether or not the write then happened. The entry drops its marker, and is marked
//     transmitted: its next send carries DUP=1, which is the conservative answer the
//     durable state already gives recovery. A live abort after intent could honestly say
//     DUP=0, but that would be a second state for the same durable fact.
//   - intent Ambiguous: the entry keeps its marker sequence and is held UNRESOLVED (no
//     write happened; nothing may release it) until a section re-attempts the delete.
//   - intent None or DefiniteFailure: only a write that possibly happened marks the entry
//     transmitted (None is the already-transmitted case, where this is a no-op); a definite
//     refusal leaves a proven-unsent entry exactly as it was.
func (sess *mqttSession) endPublishSend(c *client, pi uint16, tok mqttSendToken, intent mqttIntentOutcome, wrote bool) {
	if pi == 0 || tok.token == _EMPTY_ {
		return
	}
	// THE DURABLE ACTION IS DRAINED BEFORE the in-memory section is released, and the
	// order is the mechanism: releasing the memory half first would let another section
	// open and acquire while this one's token is still on the record, and that
	// acquisition would lose its CAS and re-offer for no reason.
	//
	// A drain that cannot be confirmed leaves the token standing, which reserves the
	// identifier. That is the stated availability cost, not a leak to be patched with a
	// lease: a broker can pause after its own expiry check and resume into the same stale
	// write, and an MQTT client cannot validate a fencing token.
	sess.drainPublishAction(c, tok)
	if h := mqttTestPublishDrainedPauseV.Load(); h != nil {
		(*h)(pi)
	}
	// AFTER BOTH HALVES, wake any stored PUBREL that admission refused while this
	// transmission was live. Deferred so it runs after endSendWith has released the local
	// hold too: waking between the durable drain and the local release just earns a second
	// refusal from beginPubRelSendLocked.
	//
	// CONDITIONED, not unconditional. This section ending is not proof that the identifier
	// is clear — an unresolved drain leaves the durable token standing, and a replacement
	// transmission may already have opened — and a waiter is one-shot, so consuming it here
	// on a blocked identifier spends the very wake the refusal was promised.
	defer sess.wakePubRelRedriveIfClear(pi, tok.slot.name.subject)
	sess.endSendWith(c, pi, tok.token, func(ack *mqttPending) {
		switch intent {
		case mqttIntentConfirmed:
			ack.markerSeq, ack.intentUnresolved, ack.transmitted = 0, false, true
		case mqttIntentAmbiguous:
			ack.intentUnresolved = true
		default:
			if wrote {
				ack.transmitted = true
			}
		}
	})
}

// endPubRelSend ends a PUBREL send section. possiblyEmitted is false only when the attempt's
// frame was PROVABLY not emitted — control never reached a writer, or the writer reported
// mqttOutputPrevented — and then the unit beginPubRelSendLocked took for this attempt is given
// back (F-116). Any frame that may have reached the client keeps its unit until its PUBCOMP.
//
// Session lock NOT held on entry.
func (sess *mqttSession) endPubRelSend(c *client, pi uint16, token string, possiblyEmitted bool) {
	sess.endSendCore(c, pi, token, nil, !possiblyEmitted)
}

// endSendWith is the shared core of endSend and endPublishSend. onEntry, when non-nil,
// runs under the lock on the entry whose section this token names, before the section is
// released and before any parked continuation is extracted.
func (sess *mqttSession) endSendWith(c *client, pi uint16, token string,
	onEntry func(ack *mqttPending)) {
	sess.endSendCore(c, pi, token, onEntry, false)
}

// endSendCore is endSendWith with one more fact: unemitted says this section was a PUBREL
// attempt whose frame provably never left, so its PUBCOMP unit is cancelled.
func (sess *mqttSession) endSendCore(c *client, pi uint16, token string,
	onEntry func(ack *mqttPending), unemitted bool) {
	if pi == 0 || token == _EMPTY_ {
		return
	}
	sess.mu.Lock()
	h := sess.sendsInFlight[pi]
	if h == nil || h.token != token {
		sess.mu.Unlock()
		return
	}
	h.token = _EMPTY_
	// CLEARED WITH THE TOKEN AND ONLY BEHIND THE EXACT-TOKEN MATCH ABOVE: a stale closer
	// clears neither, or it would tell PUBREL admission that a PUBLISH still on the wire
	// had finished. A PUBREL opener never sets it, so this is always this section's own.
	h.pubIdent, h.pubOut = _EMPTY_, nil
	h.relEntered = false
	// THE UNEMITTED ATTEMPT'S UNIT, cancelled ONCE and only behind the exact-token match: the
	// token was cleared just above, so a second closer for it stops at the match. Only this
	// attempt's unit goes — a debt still standing for an earlier written copy is the PUBCOMP
	// the client really owes. While the token was out no opener could replace the hold, so
	// the owner is still this attempt's connection; the comparison is a defensive agreement.
	if unemitted && c != nil && h.owner == c.cid && h.pubCompOwed > 0 {
		h.pubCompOwed--
		// A lineage release while this attempt had ENTERED its write converted its entry into
		// a released delivery awaiting this owner's PUBCOMP. With nothing left owed, no PUBCOMP
		// will ever answer it, and owner liveness — the bulk retirement's predicate — would keep
		// it for the connection's life. Retired here, by the same disposition, and only at zero.
		if h.pubCompOwed == 0 {
			if p := sess.pendingPubRel[pi]; p != nil && p.awaitingPubCompOwner == h.owner {
				sess.retireReleasedDeliveryLocked(pi, p)
			}
		}
	}
	// The hold goes only when it reserves nothing more. A PUBREL transmitted under this
	// identifier and not yet answered keeps it, because the write returning is not the
	// event that frees a PUBREL's identifier — its PUBCOMP is (F-089).
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
	var settle mqttSendSettle
	var removed *mqttPending
	ackRegistered, ackRun := false, false
	var ackPI uint16
	var ackBound mqttInflightSlotRef
	if ack := sess.pendingPublish[pi]; ack != nil && ack.sendNonce == token {
		if onEntry != nil {
			onEntry(ack)
		}
		ack.sendNonce = _EMPTY_
		settle, ack.sendSettle = ack.sendSettle, mqttSendSettle{}
		switch settle.kind {
		case mqttSendSettleNone:
		default:
			// THE EXCLUSION IS HANDED OFF BEFORE THE ENTRY GOES (F-098). For the
			// settlements whose durable delete may be refused, fail, or never have been
			// issued, the memory untrack below must not be the moment the identifier
			// becomes allocatable — nothing has confirmed the slot gone. See
			// mqttSendSettleOwesExclusion for why the acknowledgement kinds are not here.
			//
			// REGISTERLESS RETAINED SENDS ARE EXEMPT, and the test is the entry's own
			// JetStream backing rather than its nonce: a retained send mints a nonce with
			// no register behind it (R6), so binding an exclusion to that nonce would
			// reserve an identifier against a durable slot that never existed.
			//
			// STRUCTURALLY UNREACHABLE TODAY, and stated as a defensive agreement rather
			// than as a live case: a settlement is parked by condemnSendLocked, which
			// requires a `sendNonce`, which only beginSendLocked installs, and that call
			// matches the entry against a non-empty source durable. A retained send opens
			// its section through beginRetainedSendLocked, which records `retainedGen` on
			// the hold and no nonce on the entry, so it can never be condemned. The guard
			// keeps the two from drifting; it is not evidence that a retained frame
			// reaches here. TestMQTTDeferredSettlementExcludesOnlyARegisterBackedIdentifier
			// pins that unreachability directly.
			// Keyed on the entry's VALIDATED SLOT, not on source coordinates (F-105 7c): a retained
			// exchange is register-backed with no source durable, and its identifier is exactly as
			// durable as an ordinary one's.
			if mqttSendSettleOwesExclusion(settle.kind) && ((ack.jsDur != _EMPTY_ && ack.sseq != 0) || ack.slot.slotOK()) {
				sess.excludeIdentifierLocked(pi, sess.inflightSubjectLocked(pi), ack.regNonce)
			}
			// Every parked continuation frees the identifier; what differs is the durable
			// half below. The client's acknowledgement settlements are ANSWERED; a parked teardown,
			// deny or recovery drop is UNANSWERED, so a section that did transmit gets its
			// AckRoute (F-088 WP2).
			why := mqttUntrackUnanswered
			if settle.kind == mqttSendSettleAckComplete || settle.kind == mqttSendSettlePubRecFailed {
				why = mqttUntrackAnswered
			}
			removed = sess.untrackPublishEntry(pi, why)
			if removed != nil && removed.slot.slotOK() &&
				(settle.kind == mqttSendSettleAckComplete || settle.kind == mqttSendSettlePubRecFailed) {
				// ...except the IDENTIFIER of an acknowledged exchange, which stays held in
				// this same critical section until its terminal cleanup is confirmed.
				sess.holdTerminalCleanupLocked(pi, removed.slot.nonce)
				// AND THE BINDING COMES FROM THE CELL, never from this entry's captured
				// counter: an acquisition still unresolved may already have advanced the
				// record past it (round 7). Registration and the resolver's publication are
				// both made here, under sess.mu, so whichever completes the pair claims the
				// continuation — exactly once.
				if removed.res != nil {
					ackRegistered = true
					ackPI = pi
					ackRun, ackBound = sess.registerAckCleanupLocked(removed.res, pi)
				}
			}
		}
	}
	sess.mu.Unlock()
	if removed == nil {
		return
	}
	switch settle.kind {
	case mqttSendSettleAckComplete, mqttSendSettlePubRecFailed:
		// The acknowledgement's identifier-freeing half, deferred: the protocol answer
		// already went out, and this is the register delete it owed.
		//
		// IT MUST TERMINALIZE FIRST, exactly as the undeferred path does. When a PUBACK
		// or a failed PUBREC lands while this transmission's section is still open,
		// `releaseRegister` skips its work entirely and parks it here — so if this arm
		// deleted straight from `publish`, an acknowledged exchange would be released
		// with terminal authority existing only in the goroutine that observed it, which
		// is precisely the inference `publish_terminal` replaced. Two paths to one
		// completion must not disagree about what licenses it.
		//
		// A failed advance WITHHOLDS the delete, on the same terms as the undeferred
		// path: the slot, the identifier and its capacity stay held, and recovery
		// finishes the exchange from the phase it finds.
		//
		// Through the same attempt as the immediate path, and with the identifier HELD from
		// the untrack above until the delete is confirmed — but HANDED OFF, never run here:
		// this is the delivery callback's goroutine, and a release run on it holds back the
		// source's own re-offers until its suppression has lifted, so they are admitted as
		// new deliveries (see handOffAcknowledgedPublish).
		switch {
		case !ackRegistered:
			// No cell: nothing ever transmitted this exchange under a section, so the entry's
			// own captured identity is the binding.
			sess.handOffAcknowledgedPublish(c, pi, removed)
		case ackRun:
			sess.handOffAcknowledgedPublish(c, ackPI, &mqttPending{slot: ackBound})
		}
		// AND THE DRAIN'S FINALIZE, which the PUBACK path parked with this settlement for a
		// drained source: now that the section has closed, the drain may complete its source.
		// A no-op for a source no drain owns — the handler acknowledged that one already.
		if removed == nil || !removed.slot.slotOK() || mqttDurIsSlotOnlyClass(removed.jsDur) {
			sess.finalizeDrainSource(c, removed)
		}
	case mqttSendSettleDenyRelease:
		// A DENIAL, not an acknowledgement: nothing was accepted by the client, so there
		// is no terminal phase to record and the ordinary complete-delete is right.
		sess.deleteInflightRegisterFor(removed, mqttSlotDeleteComplete, mqttLicenceOf(c))
	case mqttSendSettlePurgeTeardown, mqttSendSettleRecoveryDrop:
		// The teardown's ladder and the recovery pass have ISSUED, or deliberately
		// declined, every durable delete they owed by this point; only the memory untrack
		// was deferred. "Declined" is not pedantry: recovery's drop can be refused by the
		// freshness fence, can fail, or can be skipped entirely when an earlier unresolved
		// terminal advance ends the pass, and in each case the durable pair outlives this
		// settlement and the next resume reconciles it.
		if c != nil {
			c.Debugf("MQTT: released packet identifier %d of client %q after its transmission settled", pi, sess.id)
		}
	}
}

// condemnSendLocked parks a removal path's continuation on an entry whose transmission
// has not returned, and reports whether it did.
//
// The FIRST parked settle wins, with ONE exception: the client's answer replaces a parked
// non-answer (below). A later remover still performs its own durable work —
// verdicts, tombstone rungs, register deletes at sequences that no longer move — but it
// neither replaces this disposition nor executes it early, which is sound because every
// continuation is idempotent against another remover's durable work: the untrack is keyed
// by the entry that is still resident, and an exact-sequence delete's "already gone" is
// the existing success reading (R4/R5).
//
// Session lock held on entry.
func (sess *mqttSession) condemnSendLocked(pi uint16, kind mqttSendSettleKind) bool {
	ack := sess.pendingPublish[pi]
	if ack == nil || ack.sendNonce == _EMPTY_ {
		return false
	}
	// THE CLIENT'S ANSWER REPLACES A PARKED NON-ANSWER (F-097 WP6, D4, witnessed by W6b): a
	// teardown, deny or recovery drop parked first would otherwise take the section's end, and
	// the acknowledgement that arrived meanwhile - already consumed from the wire, never re-sent
	// by the client - would complete nothing. The reverse never happens: once an answer is
	// parked, no later non-answer replaces it.
	if ack.sendSettle.kind == mqttSendSettleNone ||
		(mqttSendSettleIsAnswer(kind) && !mqttSendSettleIsAnswer(ack.sendSettle.kind)) {
		ack.sendSettle = mqttSendSettle{kind: kind, regSeq: ack.regSeq, nonce: ack.regNonce}
	}
	return true
}

// answerOpenPublishLocked records, on the output of the PUBLISH section open for pi, that the
// client has acknowledged the PUBLISH it is transmitting, so no execution of it begins after this
// point (F-115). Without it the read loop processes the acknowledgement while the section is still
// spending its round trips, and the section's write then puts an acknowledged PUBLISH on the wire:
// past a PUBREC that is a PUBLISH after the exchange moved to its PUBREL.
//
// Only the RESIDENT entry's own section is marked — the hold's pubIdent must be that entry's
// sendNonce — and it must be called before the entry is replaced or untracked, in the critical
// section that consumes the acknowledgement: afterwards nothing names which section it answered.
//
// Session lock held on entry.
func (sess *mqttSession) answerOpenPublishLocked(pi uint16) {
	ack := sess.pendingPublish[pi]
	h := sess.sendsInFlight[pi]
	if ack == nil || h == nil || h.pubIdent == _EMPTY_ || h.pubIdent != ack.sendNonce || h.pubOut == nil {
		return
	}
	h.pubOut.answered.Store(true)
}

// mqttSendSettleIsAnswer reports whether a settle kind carries the CLIENT'S acknowledgement.
func mqttSendSettleIsAnswer(k mqttSendSettleKind) bool {
	return k == mqttSendSettleAckComplete || k == mqttSendSettlePubRecFailed
}

// ---------------------------------------------------------------------------
// Subject parsing
// ---------------------------------------------------------------------------

// mqttInflightName is a parsed $MQTT_inflight subject.
type mqttInflightName struct {
	idHash  string
	gen     string
	pi      uint16
	subject string
}

// mqttParseInflightSubject parses a subject on $MQTT_inflight strictly, in the exact
// shape this broker writes and no other.
//
// Strictness for the reason mqttParseQoS2InSubject is strict: a subject that cannot
// be attributed to a specific (idHash, generation) is retained rather than swept, so
// a lenient parse would be guessing which session owns an identifier. The packet
// identifier must be CANONICAL decimal (no leading zeros, 1..65535) because
// mqttInflightSubject formats it with strconv.FormatUint — "01" is a subject this
// broker can never have written, so it is foreign rather than ours oddly spelled.
func mqttParseInflightSubject(subject string) (*mqttInflightName, error) {
	if !strings.HasPrefix(subject, mqttInflightStreamSubjectPrefix) {
		return nil, fmt.Errorf("subject is not on the outbound in-flight prefix")
	}
	rest := subject[len(mqttInflightStreamSubjectPrefix):]
	tokens := strings.Split(rest, ".")
	if len(tokens) != 3 {
		return nil, fmt.Errorf("subject has %d tokens past the prefix (want 3)", len(tokens))
	}
	if !mqttValidIDHashToken(tokens[0]) {
		return nil, fmt.Errorf("malformed client-ID hash token %q", tokens[0])
	}
	if !mqttValidSessGen(tokens[1]) {
		return nil, fmt.Errorf("malformed incarnation generation token %q", tokens[1])
	}
	pi, err := strconv.ParseUint(tokens[2], 10, 16)
	if err != nil || pi == 0 || strconv.FormatUint(pi, 10) != tokens[2] {
		return nil, fmt.Errorf("malformed packet identifier token %q", tokens[2])
	}
	return &mqttInflightName{
		idHash:  tokens[0],
		gen:     tokens[1],
		pi:      uint16(pi),
		subject: mqttInflightSubject(tokens[0], tokens[1], uint16(pi)),
	}, nil
}

// ---------------------------------------------------------------------------
// The resume barrier
// ---------------------------------------------------------------------------

// setPISpaceIncomplete records that the durable identifier space could not be fully
// reconstructed, so no NEW outbound identifier may be allocated (R3).
//
// This is the whole degradation, and it is deliberately narrow. Allocation is the only
// operation an unknown identifier space makes unsafe: reserving too little would let
// bumpPI hand out an identifier a register or an unreplayed PUBREL still owns, which
// the client is entitled to read as a second message on an identifier it is already
// using ([MQTT-2.3.1-4]). Everything else is untouched — QoS 0 flows, the exchanges
// that WERE reserved complete, and the drain barrier still recreates PUBRELs under
// their recorded identifiers. A withheld QoS 1/2 delivery is redelivered by AckWait,
// exactly as one withheld at the in-flight cap is.
//
// Session lock NOT held on entry.
func (sess *mqttSession) setPISpaceIncomplete(c *client, why string) {
	sess.mu.Lock()
	first := sess.markPISpaceIncompleteLocked()
	sess.mu.Unlock()
	if first {
		sess.warnPISpaceIncomplete(c, why)
	}
}

// markPISpaceIncompleteLocked is setPISpaceIncomplete's state change, for a caller that must
// commit the mark in the same critical section as the state that requires it. Reports whether
// this call set it; the caller warns after the unlock.
//
// Lock held on entry.
func (sess *mqttSession) markPISpaceIncompleteLocked() bool {
	already := sess.piSpaceIncomplete
	sess.piSpaceIncomplete = true
	return !already
}

func (sess *mqttSession) warnPISpaceIncomplete(c *client, why string) {
	c.Warnf("MQTT: withholding new outbound QoS 1/2 deliveries for client %q: %s, so the packet identifier space is not fully known. "+
		"Deliveries already in flight are unaffected and allocation resumes on a resume that completes the scan.", sess.id, why)
}

type mqttInflightClaimResult int

const (
	mqttInflightClaimOwned mqttInflightClaimResult = iota
	mqttInflightClaimClosed
	mqttInflightClaimOpaque
	// mqttInflightClaimDisplaced: the re-read found a READABLE live record with this
	// allocation's nonce under another object's claim. That is durable proof this
	// session has been superseded — a rival claims only after its own accept save, so
	// its record write is both durable and later than ours — and it can only be
	// observed against an ACTIVELY racing rival: a dead rival's standing claim is
	// re-claimed by the ordinary CAS at the loaded sequence and never reaches the
	// re-read. The barrier aborts on it rather than installing foreign reservations
	// on a connection about to be (or already) displaced.
	mqttInflightClaimDisplaced
	// mqttInflightClaimWithheld means the selected quiescent slot moved into an
	// action or terminal state before the custody CAS landed. Those states are
	// deliberately not transferable: an action belongs to its holder and terminal
	// cleanup is custody-agnostic. Recovery still reserves the exact slot, but it
	// neither refuses the whole CONNECT nor rewrites through the phase change.
	mqttInflightClaimWithheld
)

// claimRecoveredTransferableInflight transfers custody for the exact positive QoS 2
// population selected by recovery: live, unpoisoned, non-retired and non-cancelled slots
// in publish or pubrel_ready. It deliberately shares the ordered claim contest with the
// older shared-delivery path so a refresh can never overwrite a newer accepted owner.
func (sess *mqttSession) claimRecoveredTransferableInflight(c *client, in []mqttRecoveredInflightReg) (
	owned, closed, opaque, displaced, withheld []mqttRecoveredInflightReg,
) {
	return sess.claimRecoveredInflight(c, in, mqttInflightClaimTransferable)
}

// claimRecoveredRetiredPubRelInflight is the deliberately narrow retired exception to
// the positive transferable set. Its caller supplies only pubrel_ready slots for which a
// present, structurally valid PUBREL record names the exact source, identifier and nonce.
// Such a slot is still actionable—the client is waiting for PUBREL—so admission will read
// its custody. A retired PUBLISH slot or a flag without that external evidence remains
// excluded.
func (sess *mqttSession) claimRecoveredRetiredPubRelInflight(c *client, in []mqttRecoveredInflightReg) (
	owned, closed, opaque, displaced, withheld []mqttRecoveredInflightReg,
) {
	return sess.claimRecoveredInflight(c, in, mqttInflightClaimRetiredPubRel)
}

type mqttInflightClaimScope uint8

const (
	mqttInflightClaimTransferable mqttInflightClaimScope = iota
	mqttInflightClaimRetiredPubRel
)

// claimRecoveredInflight makes the recovery-versus-close decision durable
// at each selected register's own subject sequence. A successful rewrite invalidates
// every sequence held by an older session object; a close that deleted or
// tombstoned first is consumed as closed. Any state that cannot prove either fact
// becomes opaque, so the identifier is withheld rather than guessed free.
//
// The writes are independent by subject and run in a bounded window. This remains
// part of the pre-CONNACK barrier without turning a large resume into one serial
// round trip per register.
func (sess *mqttSession) claimRecoveredInflight(c *client, in []mqttRecoveredInflightReg, scope mqttInflightClaimScope) (
	owned, closed, opaque, displaced, withheld []mqttRecoveredInflightReg,
) {
	if hook := mqttTestClaimRecoveredHook; hook != nil {
		hook(in)
	}
	if len(in) == 0 {
		return nil, nil, nil, nil, nil
	}
	type result struct {
		reg     mqttRecoveredInflightReg
		outcome mqttInflightClaimResult
		closeAt uint64
	}
	results := make([]result, len(in))
	jobs := make(chan int)
	workers := min(len(in), mqttInflightClaimWindow)

	sess.mu.Lock()
	idHash, gen, claim, claimSeq := sess.idHash, sess.gen, sess.regClaim, sess.seq
	sess.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				fr := in[i]
				results[i].reg = fr
				results[i].outcome = mqttInflightClaimOwned
				if fr.rec == nil {
					continue
				}
				// ONLY THE SLOT-ONLY (experimental affine) CLASS IS CONTESTED (F-105 7b). An ordinary
				// register's ownership is its exchange's, arbitrated by the connection's accepted
				// entitlement inside every owner-dependent authority batch; it passes through as the
				// walk observed it and no ordinary decision reads its claim stamp.
				if !mqttDurIsSlotOnlyClass(fr.rec.Dur) {
					continue
				}
				transferableQoS2 := scope == mqttInflightClaimTransferable && fr.rec.QoS == 2 &&
					(fr.rec.State == mqttInflightPublish || fr.rec.State == mqttInflightPubRelReady ||
						fr.rec.State == mqttInflightPubRelIntended) &&
					!fr.rec.Retired && !fr.rec.Cancelled
				retiredPubRel := scope == mqttInflightClaimRetiredPubRel && fr.rec.QoS == 2 &&
					fr.rec.State == mqttInflightPubRelReady && fr.rec.Retired
				if !transferableQoS2 && !retiredPubRel {
					continue
				}
				// Deliberately no own-claim fast path: a record already carrying this
				// object's claim is still REWRITTEN, because moving the sequence is
				// itself load-bearing. A terminal path that untracked its entry and
				// paused before its durable half holds the OLD sequence; a same-object
				// rebind's barrier would otherwise reinstall the entry, and the paused
				// delete-and-ack would then complete the exchange invisibly — a
				// permanently consumed Receive Maximum slot. Rewritten, the paused
				// delete no-ops and the surviving register's completed source is
				// consumed at the next barrier run.

				subject := mqttInflightSubject(idHash, gen, fr.rec.PI)
				// ORDERING BEFORE THE FIRST WRITE, not only on a contested reread.
				//
				// The arms below re-target and re-compare when a CAS FAILS, which covers
				// two writers racing one tuple. It does not cover two writers arriving in
				// sequence: a rival accepted LATER can transfer this slot to itself
				// (admission's custody repair does exactly that), and a resumer whose own
				// acceptance is OLDER would then read that settled tuple and take it with
				// a first CAS that succeeds — no conflict, no failed attempt, no contest.
				// The rightful newer owner is silently displaced by a broker it already
				// superseded.
				//
				// STRICTLY NEWER, not at-or-newer, and the difference is the invariant
				// this rule rests on. Accept saves are what order two owners: the record
				// CAS makes every accept sequence unique, so of two DIFFERENT accepted
				// owners exactly one is newer and equality between two different claims is
				// a state acceptance cannot produce. Refusing on equality would abort
				// resumes on no evidence, while the contest below acts on an observed
				// conflicting write, which is strictly better evidence.
				//
				// A claimless record is untouched (nothing to be behind); an OWN claim is
				// still rewritten at its unmoved sequence, because moving the sequence is
				// what fences a paused terminal delete; and a foreign claim carrying
				// ClaimSeq ZERO is not evidence of a newer acceptance, since JetStream
				// sequences start at 1 and zero means that writer had no confirmed record.
				if fr.rec.Claim != _EMPTY_ && fr.rec.Claim != claim &&
					fr.rec.ClaimSeq != 0 && fr.rec.ClaimSeq > claimSeq {
					results[i].outcome = mqttInflightClaimDisplaced
					continue
				}
				claimed := *fr.rec
				claimed.Claim = claim
				claimed.ClaimSeq = claimSeq
				// The chain starts at the walk sequence, and every arm that CONTINUES
				// the contest on our own record extends it (F-087 WP4).
				traversed := sess.seedResumeLineage(fr.rec.Nonce, fr.seq)
				// Bounded contest, not one shot: an OLDER accepted owner's claim is
				// re-targeted and taken over, and only a NEWER one displaces. Two
				// simultaneous resumers otherwise split a pair of registers and refuse
				// each other forever — the claim nonce alone is unordered, so nothing
				// would let either side win deterministically. Accept saves give the
				// order: record CAS makes every accept sequence unique, so exactly one
				// of any two racing resumers is newer, and that one takes every
				// contested register while the other aborts.
				expectSeq := fr.seq
				for attempt := 0; attempt < mqttInflightClaimContestAttempts; attempt++ {
					seq, err := sess.asm.storeInflightCAS(subject, expectSeq, &claimed)
					if err == nil {
						// Owned is reasserted, not assumed: a re-targeted contest
						// reaches here after a failed attempt already parked the
						// outcome at fail-closed.
						landed := claimed
						results[i].outcome = mqttInflightClaimOwned
						results[i].reg = mqttRecoveredInflightReg{
							srcRouted: fr.srcRouted,
							rec:       &landed, seq: seq, traversed: append(traversed, seq),
						}
						break
					}
					results[i].outcome = mqttInflightClaimOpaque // until proven otherwise

					cur, smsg, lerr := sess.asm.loadInflightRecord(subject)
					switch {
					case lerr != nil:
						// An INDETERMINATE re-read after a failed CAS: sequence movement
						// cannot be proven either way, so a reservation here could sit
						// at exactly the unfenced sequence a paused terminal delete
						// holds. Refuse the resume — the same fail-safe as every other
						// unprovable fence; the retry meets a working store.
						results[i].outcome = mqttInflightClaimDisplaced
					case smsg == nil:
						// The close's exact-sequence delete won. Absence is the durable
						// decision and this identifier is free.
						results[i].outcome = mqttInflightClaimClosed
					case cur == nil || cur.Nonce != fr.rec.Nonce ||
						!mqttInflightRecordMatchesSubject(cur, subject):
						// A body naming another identifier belongs in THIS arm, beside
						// `cur == nil`, because that is what the reader makes of it: the
						// scan at recoverInflightRegisters treats a subject/body PI
						// disagreement as unreadable. Left to the arms below it would be
						// taken for a close decision and DELETED, or for our own claim
						// having landed. Opaque here reserves the identifier and destroys
						// nothing, which is the same disposition the reader reaches.
						results[i].reg.seq = smsg.Sequence
					case cur.Cancelled && !cur.Retired:
						// A matching-nonce tombstone is the close decision. Delete only
						// the exact generation just loaded; a later rewrite is untouched.
						//
						// PURE cancellation only: Retired dominates Cancelled (F-087
						// WP3). A record carrying both was transmitted — the client
						// answered it with a PUBREC — so consuming it as a close would
						// free an identifier the client still holds. It falls through to
						// the claim arms below and is claimed like any retired record,
						// which the post-contest routing sends to the suppression.
						results[i].outcome = mqttInflightClaimClosed
						results[i].closeAt = smsg.Sequence
					case cur.ActionToken != _EMPTY_ || cur.State == mqttInflightPubRelTerminal:
						// The candidate was quiescent when selected but crossed a phase
						// boundary before our CAS. Never rebuild a custody write over that
						// observation: active custody is frozen into the action, and terminal
						// finalization does not need custody. Preserve the exact reread so the
						// caller can route it as a localized past-PUBREC reservation (or
						// terminal cleanup) without making the whole identifier space opaque.
						results[i].outcome = mqttInflightClaimWithheld
						results[i].reg = mqttRecoveredInflightReg{
							srcRouted: fr.srcRouted,
							rec:       cur, seq: smsg.Sequence,
							traversed: append(traversed, smsg.Sequence),
						}
					case cur.Claim == claim:
						if smsg.Sequence == expectSeq {
							// Own claim at the UNMOVED sequence: with no own-claim fast
							// path, the loaded record was own-claimed to begin with, so
							// this proves nothing about our write — the CAS failed
							// without landing, and accepting the sequence would hand a
							// paused terminal delete exactly the target the rewrite
							// exists to move. The MOVE is the fence; retry — and
							// EXHAUSTION refuses the resume rather than falling to an
							// opaque reservation, which would sit at exactly that
							// unfenced sequence: a persistent store failure means no
							// fence landed, and fail-safe here is the refused CONNECT
							// whose retry meets a working store.
							if attempt == mqttInflightClaimContestAttempts-1 {
								results[i].outcome = mqttInflightClaimDisplaced
								results[i].reg.seq = smsg.Sequence
								break
							}
							continue
						}
						// Moved and ours: our claim landed and only its response was
						// lost. The body is ADOPTED from the reread, which is the rewrite
						// rule's own shape — whatever a concurrent writer proved rides
						// forward untouched.
						results[i].outcome = mqttInflightClaimOwned
						results[i].reg = mqttRecoveredInflightReg{
							srcRouted: fr.srcRouted,
							rec:       cur, seq: smsg.Sequence,
							traversed: append(traversed, smsg.Sequence),
						}
					case cur.ClaimSeq < claimSeq:
						// An OLDER accepted owner's claim. Our accept save superseded
						// its record, so the register is ours to take: re-target the
						// contest at the sequence its claim occupies. Contenders are
						// unbounded while a fixed retry budget is not, so EXHAUSTION
						// here must read as displacement (refuse the resume; the retry
						// meets a settled contest) — an opaque reservation would strand
						// the rightful NEWEST owner behind a chain of older claims,
						// with its allocation withheld.
						if attempt == mqttInflightClaimContestAttempts-1 {
							results[i].outcome = mqttInflightClaimDisplaced
							results[i].reg.seq = smsg.Sequence
							break
						}
						// REBUILD from the record just read, mutating only the fields the
						// contest owns (F-087 WP1). Re-proposing the pre-reread snapshot
						// erases whatever the round's winner proved — a Retired written
						// between our build and this re-target would be silently cleared,
						// and the payload re-sent after the client's PUBCOMP.
						claimed = *cur
						claimed.Claim = claim
						claimed.ClaimSeq = claimSeq
						traversed = append(traversed, smsg.Sequence)
						expectSeq = smsg.Sequence
						continue
					default:
						// A readable live record with our nonce under a claim whose
						// accept is NOT older than ours: durable displacement proof —
						// the rival's accept save precedes its claims — not an
						// ambiguity to reserve around.
						results[i].outcome = mqttInflightClaimDisplaced
						results[i].reg.seq = smsg.Sequence
					}
					break
				}
			}
		}()
	}
	for i := range in {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	// Every chain this pass built is remembered for a possible re-run (recoverInflightRegisters):
	// a Reclassify discards the pass but not the rewrites it landed. Written here, after the
	// workers have joined, because the map is read by them without a lock.
	if sess.resumeLineage != nil {
		for _, res := range results {
			if res.reg.rec != nil && len(res.reg.traversed) > 0 {
				sess.resumeLineage[res.reg.rec.Nonce] = res.reg.traversed
			}
		}
	}

	for _, res := range results {
		switch res.outcome {
		case mqttInflightClaimOwned:
			owned = append(owned, res.reg)
		case mqttInflightClaimClosed:
			// The record's identity rides along, because consuming a close decision is
			// not only about the stream: a same-object barrier re-run may still hold the
			// LIVE in-memory entry this record backed, and the apply phase must retire
			// it too or the reused object redelivers what the close durably ended. The
			// seq carries the tombstone's re-read sequence (0 when absence was the
			// decision), which is what the caller's confirmed delete targets.
			closed = append(closed, mqttRecoveredInflightReg{rec: res.reg.rec, seq: res.closeAt})
		case mqttInflightClaimOpaque:
			mqttWarnf(c, "MQTT: reserving packet identifier %d for client %q after ownership of shared in-flight register %s/%d could not be claimed",
				res.reg.rec.PI, sess.id, res.reg.rec.Dur, res.reg.rec.Seq)
			// The identity is KEPT, not stripped to a bodyless reservation here: an
			// unclaimed member still names its source, and the caller's grouping needs
			// that to fail the source's whole group closed. A record this pass could not
			// claim may be owned by a concurrent resumer, and installing its sibling as
			// an authoritative singleton would resolve a duplicate by exactly the guess
			// the group refuses to make. The caller converts these to bodyless
			// reservations only after the grouping has seen them — FOREIGN ones, whose
			// sequence may be the rival's and is never this object's to settle.
			res.reg.foreign = true
			opaque = append(opaque, res.reg)
		case mqttInflightClaimDisplaced:
			displaced = append(displaced, res.reg)
		case mqttInflightClaimWithheld:
			withheld = append(withheld, res.reg)
		}
	}
	return owned, closed, opaque, displaced, withheld
}

// ordinaryExchangeEnded reports whether an ORDINARY slot's exchange durably reads Terminal for
// exactly this obligation — the session's epoch, the slot's generation, its lineage, identifier
// and QoS. Anything else — pending drain, PUBREC-observed, an unreadable, absent or mismatched
// exchange — is false: the slot keeps its reservation.
//
// Session lock NOT held on entry.
func (sess *mqttSession) ordinaryExchangeEnded(rec *mqttInflightRecord) bool {
	if rec == nil || sess.asm == nil {
		return false
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	x, _, err := sess.jsa.loadOutboundExchange(sess.id, sess.asm.sessStreamEpoch, gen, rec.Lin)
	if err != nil || x == nil || x.Nonce != rec.Lin || x.PI != rec.PI || x.QoS != rec.QoS {
		return false
	}
	return x.Phase == mqttExchangeTerminal
}

// recoverInflightRegisters rebuilds this session's outbound in-flight identifier
// space from durable state, and MUST run before any source interest exists (R3).
//
// It is the whole point of the feature: after it returns, cpending names every
// started delivery, so the redelivery that follows finds its ORIGINAL identifier
// instead of allocating a fresh one, and bumpPI cannot hand out an identifier that a
// register or an unreplayed PUBREL record still owns.
//
// Two durable sources, with one authority and one phase-evidence stream (R2):
//
//   - a PUBREL record on this incarnation's PUBREL subject is evidence that an exchange
//     is PAST PUBREC. Its identifier is reserved in pendingPubRel exactly as
//     trackAsPubRel(pi, _EMPTY_) would, and — where the record names its source — the
//     source is entered in cpending too, so the usedForPubRel guard suppresses a
//     re-PUBLISH of a payload the client has already answered.
//   - a register is the whole-life owner slot. A PUBLISH-phase slot is source-classified:
//     a provably complete source means that phase finished, while a live or unprovable
//     one is reserved in pendingPublish with no ack subject, waiting for redelivery.
//     A past-PUBREC slot is never restored as PUBLISH; it is reserved in pendingPubRel,
//     custody-refreshed when actionable, or terminalized only from positive completion
//     evidence.
//
// It fails closed WITHOUT denying the CONNECT (R3): an incomplete scan leaves the
// identifier space unknown, so the session is marked piSpaceIncomplete and bumpPI
// stops allocating new identifiers, while everything already reserved — and every
// QoS 0 subscription — keeps working. That is the same shape as the drain barrier's
// contract, which withholds a durable's source interest rather than the connection.
//
// In-memory state normally wins on a same-process reconnect, but a durable close,
// quarantine or poisoned-source disposition demotes a matching resident entry first.
// Those negative outcomes are the durable evidence that the old in-memory view is no
// longer authoritative; every such demotion is nonce-fenced.
//
// Session lock NOT held on entry.
func (sess *mqttSession) recoverInflightRegisters(c *client) (staged []mqttStagedRelease, err error) {
	defer sess.deferTerminalRetries()()
	// Bounded RE-ENTRY. The marker phase may find the register walk's snapshot stale —
	// a same-nonce rewrite at a new sequence, a different exchange, a proven absence, or
	// a pair committed between the register walk and the marker walk (rows 6-8, N-5.6) —
	// and the contract's answer is Reclassify: re-enter classification from the
	// authoritative observation, under the claim path's ownership and phase checks and
	// the duplicate-source grouping. The one place all of those live is this pass, so
	// re-entering means re-running it; everything it did before its apply phase is
	// idempotent against a re-run (claims and re-phasings are CAS writes that a second
	// pass reads back as its own). Exhaustion fails the identifier space closed.
	//
	// What is NOT idempotent by itself is the custody rewrite's LINEAGE: a discarded pass
	// has landed A→B, the re-run lands B→C, and the live entry still names A. The chain
	// each pass builds is therefore seeded from every earlier pass of THIS invocation
	// (resumeLineage), so the apply phase recognizes A as a link it moved through.
	sess.resumeLineage = make(map[string][]uint64)
	defer func() { sess.resumeLineage = nil }()
	// THE CUSTODY LEDGER IS REBUILT FROM THE EXCHANGES FIRST (F-105 row 5), before any
	// register is routed and before delivery or allocation opens: an outstanding exchange
	// whose register is gone — a destruction without whole-exchange evidence, a
	// cancellation, an occupancy-only retirement — still holds its charge and its
	// identifier, and a census of REGISTERS would not see it. An incomplete census leaves
	// the ledger withholding. The pending entries this walk installs for a charged lineage
	// are marked as the ledger's once it returns, whatever it returns.
	sess.mu.Lock()
	custodyGen := sess.gen
	// The veto window opens BEFORE the census and the walk read anything (consult 93): an
	// ending that lands after the walk captured a live register re-phases or tombstones that
	// register in its own batch, so every ending this resume could have missed is accepted
	// inside the window and vetoes a provisional holder for it.
	ledger := sess.custody
	ledger.beginResume()
	sess.mu.Unlock()
	defer func() {
		sess.mu.Lock()
		ledger.endResume()
		sess.mu.Unlock()
	}()
	census := sess.rebuildCustodyForResume(custodyGen)
	if census != nil {
		// Handed to the post-CONNACK discovery of retained copies (F-105 7c).
		sess.mu.Lock()
		sess.retainedDiscovery = mqttRetainedDiscovery{gen: custodyGen, incs: census.resources}
		sess.mu.Unlock()
	}
	// DISPLACEMENT IS OBSERVED DURABLY (F-105 7b, replacing the register claim contest's
	// "displaced" outcome): the resume walks under THIS connection's accepted entitlement, and a
	// takeover on another broker changes the authority without necessarily reaching this broker
	// yet — so the authority is read, here and again immediately before apply.
	if derr := sess.resumeEntitlementServes(c); derr != nil {
		return nil, derr
	}
	defer func() {
		sess.mu.Lock()
		sess.markRecoveredCustodyLocked(custodyGen)
		sess.mu.Unlock()
	}()
	// A CERTIFIED CONNECTION'S TOKENS END BEFORE THE WALK (F-105 8c). A token whose holder's
	// connection is durably certified unable to emit is cleared here, under that licence, so the
	// walk below classifies the register as the holder's own drain would have left it.
	sess.clearCertifiedTokensForResume(c)
	if as := sess.asm; as != nil {
		as.mu.RLock()
		hook := as.testResumeWalkEntry
		as.mu.RUnlock()
		if hook != nil {
			hook(sess.id)
		}
	}
	// EVERY SUCCESS THAT INSTALLED NOTHING IS CHECKED, not each exit that produces one: the walk's
	// conservative successes (the identifier space withheld, an exhausted reclassification) apply
	// nothing, which is sound for a source-backed delivery its consumer re-offers and leaves a
	// RETAINED exchange owned by nothing on an accepted connection (F-105 7c, consult 106).
	installed := func(staged []mqttStagedRelease, err error) ([]mqttStagedRelease, error) {
		if err == nil && len(sess.resumeRetainedUnowned) > 0 {
			return nil, fmt.Errorf("refusing the resume of %q: the retained exchange(s) on %v could not be given an "+
				"owner", sess.id, sess.resumeRetainedUnowned)
		}
		if err == nil && !sess.resumeRetainedInstalled {
			if rerr := sess.retainedObligationsUnexcluded(census, "the resume walk installed nothing"); rerr != nil {
				return nil, rerr
			}
		}
		return staged, err
	}
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		sess.resumeRetainedInstalled, sess.resumeRetainedUnowned = false, nil
		staged, err = sess.recoverInflightRegistersOnce(c, census)
		if !errors.Is(err, errInflightReclassify) {
			return installed(staged, err)
		}
	}
	sess.resumeRetainedInstalled, sess.resumeRetainedUnowned = false, nil
	// DISPLACEMENT IS RE-READ BEFORE THE CONSERVATIVE SUCCESS (F-105 7c, consult 101). A pass that
	// reclassifies every time never reaches the pre-apply entitlement read, so a takeover that landed
	// during the loop would otherwise be answered with a successful resume. A gap at HEAD for both
	// arms; the CONNECT caller refuses before CONNACK on this error.
	if derr := sess.resumeEntitlementServes(c); derr != nil {
		return nil, derr
	}
	sess.setPISpaceIncomplete(c, "the in-flight registers kept changing under the resume walk")
	return installed(nil, nil)
}

// retainedObligationsUnexcluded is the walk's answer to a success that installed nothing: the
// conservative success (the identifier space withheld, an exhausted reclassification) is sound
// for source-backed deliveries, whose consumer re-offers them, but installs nothing — so if a
// RETAINED exchange may be outstanding, nothing would own it on an accepted connection (F-105 7c,
// consult 106), and the resume is refused before CONNACK instead. One may be outstanding unless this resume's inventory
// is complete and certain, no live exchange carries the retained abandonment fact (an ACK-only
// exchange whose copy is reclaimed), and every resource it names is readable and not a retained
// copy still standing — an admitted copy closes only once its exchange ended or was abandoned.
//
// Session lock NOT held on entry.
func (sess *mqttSession) retainedObligationsUnexcluded(census *mqttCustodyCensus, why string) error {
	refuse := fmt.Errorf("refusing the resume of %q (%s) while a retained exchange may be outstanding",
		sess.id, why)
	if sess.jsa == nil || sess.asm == nil {
		return nil // no store, so no durable exchange of any kind: nothing outstanding to own
	}
	if census == nil || len(census.uncertain) > 0 {
		return refuse
	}
	for _, x := range census.live {
		if x != nil && x.Abandoned.retained() {
			return refuse
		}
	}
	sess.mu.Lock()
	gen := sess.gen
	sess.mu.Unlock()
	for _, inc := range census.resources {
		r, _, err := sess.jsa.loadOutboundResource(sess.id, sess.asm.sessStreamEpoch, gen, inc)
		if err != nil || r == nil {
			return refuse
		}
		if r.Kind == mqttResourceRetainedPayload && r.State != mqttResourceCleanupComplete {
			return refuse
		}
	}
	return nil
}

// resumeEntitlementServes reports whether the resuming connection's accepted entitlement is still
// the SERVING authority's generation and tenure: the durable displacement observation that
// replaced the register claim contest's "displaced" outcome (F-105 7b). A newer foreign claim
// stamp on a register means nothing; this does.
//
// Session lock NOT held on entry.
func (sess *mqttSession) resumeEntitlementServes(c *client) error {
	ent, ok := c.outboundEntitlement()
	if !ok {
		return fmt.Errorf("the resume of %q runs under no accepted entitlement", sess.id)
	}
	if sess.asm == nil {
		return fmt.Errorf("session %q has no store to read its authority from", sess.id)
	}
	sess.asm.mu.RLock()
	hook := sess.asm.testResumeAuthorityLoad
	sess.asm.mu.RUnlock()
	var load mqttAuthorityLoad
	switch {
	case hook != nil:
		load = hook(sess.id)
	case sess.jsa != nil:
		load = sess.jsa.loadOutboundAuthority(sess.id, sess.asm.sessStreamEpoch)
	default:
		return fmt.Errorf("session %q has no store to read its authority from", sess.id)
	}
	if load.status != mqttOutboundValid || load.rec == nil || load.rec.State != mqttAuthorityServing ||
		load.rec.Gen != ent.Gen || load.rec.Tenure != ent.Tenure {
		return fmt.Errorf("the resume of %q under %q/%q was displaced: the outbound authority reads %v "+
			"and serves another tenure", sess.id, ent.Gen, ent.Tenure, load.status)
	}
	return nil
}

// seedResumeLineage starts a custody rewrite's chain at the walk sequence, prefixed with
// every link an earlier pass of the same recoverInflightRegisters invocation already moved
// this exchange through. The walk sequence is normally that pass's landed sequence, so it
// is appended only when it is not already the chain's last link.
func (sess *mqttSession) seedResumeLineage(nonce string, walkSeq uint64) []uint64 {
	prior := sess.resumeLineage[nonce]
	chain := make([]uint64, 0, len(prior)+1)
	chain = append(chain, prior...)
	if len(chain) == 0 || chain[len(chain)-1] != walkSeq {
		chain = append(chain, walkSeq)
	}
	return chain
}

func (sess *mqttSession) recoverInflightRegistersOnce(c *client, census *mqttCustodyCensus) (staged []mqttStagedRelease, err error) {
	defer sess.deferTerminalRetries()()
	as := sess.asm
	sess.mu.Lock()
	// This walk's identity. A recovery release captured below is current only while no
	// later walk has started (recoveryReleaseCurrentLocked), disposal recovery is prepared
	// under it, and a completed apply records it as the provenance watermark.
	sess.inflightWalks++
	walkID := sess.inflightWalks
	idHash, gen, pubRelSubject := sess.idHash, sess.gen, sess.pubRelSubject
	recoveryReservations := sess.captureRecoveryReservationsLocked()
	// The cancellation intents this incarnation carries, restored from the record. A
	// register whose nonce is here was never transmitted, whatever else it looks like.
	cancelledNonces := make(map[string]bool, len(sess.cancelledRegNonces))
	for n := range sess.cancelledRegNonces {
		cancelledNonces[n] = true
	}
	sess.mu.Unlock()
	if as == nil || idHash == _EMPTY_ || gen == _EMPTY_ {
		// Unreachable for a real session: every session carries an incarnation
		// generation and an account manager. Withhold allocation rather than deny, on
		// the same reasoning as a failed scan.
		sess.setPISpaceIncomplete(c, "the session carries no incarnation identity")
		return nil, nil
	}

	// (1) The registers. One bounded filter over this incarnation alone, which a
	// 16-bit identifier caps at 65_535 subjects.
	regs := make([]mqttRecoveredInflightReg, 0, 8)
	// opaque holds registers whose body is unusable but whose subject names an
	// identifier this incarnation wrote. They reserve that identifier and nothing else:
	// there is no source to classify, and nothing may be deleted.
	var opaque []mqttRecoveredInflightReg
	// unsupportedShared are shared registers refused as shapes this broker does not write (7a): each
	// joins its source's grouping as a member this pass may not install.
	var unsupportedShared []mqttRecoveredInflightReg
	// Retained registers, dispatched before every ordinary-source route (F-105 7c); the ended ones'
	// cleanup continues once this pass has succeeded.
	var retainedRecovered []mqttRecoveredRetained
	// The positive-store-answer quarantines, with their evidence (consult 107).
	var retainedQuarantined []mqttRetainedQuarantined
	defer func() {
		if err == nil {
			sess.continueRecoveredRetainedTerminals(c, retainedRecovered)
		}
	}()
	// quarantinedPublishConflicts names PUBLISH slots that recovery cannot safely route
	// because their exact route-B handoff failed or a PUBREL names the same source but a
	// different exchange. These identities are needed during apply to demote a same-object
	// resident PUBLISH entry; an opaque reservation
	// alone would lose to the existing map entry and leave the refused payload sendable.
	var quarantinedPublishConflicts []mqttRecoveredInflightReg
	// pastPubRec holds re-phased slots: readable, legal, and owning an exchange the
	// client has already answered with PUBREC. They are neither ordinary PUBLISH
	// registers nor opaque — reserving them as opaque would withhold the session's whole
	// outbound flow, and restoring them as PUBLISH entries could retransmit an answered
	// payload — so they get their own disposition (see settlePastPubRecSlots).
	var pastPubRec []mqttRecoveredInflightReg
	// routeB is the cross-stream crash cut in which the exact PUBREL record landed but
	// the owner slot did not yet move out of PUBLISH. It is paired with the record now,
	// while the completed scan is available, and re-phased before source classification
	// can mistake source completion for permission to delete it.
	type mqttRecoveryRouteB struct {
		reg mqttRecoveredInflightReg
		rel mqttStoredPubRel
	}
	var routeB []mqttRecoveryRouteB
	// An unusable pair cannot borrow authority from a separate PUBREL scan.
	quarantinedPairs := make(map[uint16]bool)
	// The inventory census is MANDATORY (F-105 7b): nil is a failed census, never an empty one.
	censusIncomplete := census == nil || len(census.uncertain) != 0
	// Census uncertainty belongs to custody. Only an unresolved disposal handoff
	// can additionally prevent this register walk from completing its apply.
	applyIncomplete := false
	exchanges := make(map[string]*mqttOutboundExchange)
	factPIs := make(map[uint16]bool)
	if census != nil {
		for _, x := range census.live {
			exchanges[x.Nonce] = x
			if x.Abandoned != nil {
				factPIs[x.PI] = true
			}
		}
	}
	// Enumerated by SUBJECT and loaded in batches, not walked one synchronous request at
	// a time. A resumed session may legitimately hold up to 65_535 in-flight identifiers,
	// and this runs before the CONNACK: a per-record round trip would put tens of
	// thousands of serial requests in front of every reconnect and put restart recovery
	// past ordinary client timeouts.
	//
	// jsa.streamSubjects is safe HERE although the orphan sweep refuses it, and the
	// difference is the filter: a 16-bit identifier bounds one incarnation at 65_535
	// subjects, well under the JSMaxSubjectDetails truncation point, so this response is
	// complete by construction. The sweep's account-wide walk has no such bound.
	filter := mqttInflightWalkFilter(idHash, gen)
	subjectCounts, serr := as.enumerateInflightSubjects(filter)
	if serr != nil {
		// EVERY failure, including a missing stream. Elsewhere — the orphan sweep, for
		// instance — a stream-not-found is tolerated because it means "nothing to clean
		// up", which is safe. Here it would mean "nothing is in flight", which is not: the
		// stream is created and validated at account startup, so its absence is an
		// unexpected state and not proof that this session holds no identifiers. Reading it
		// as emptiness reserves nothing and lets allocation hand out an identifier the
		// client may be holding.
		sess.setPISpaceIncomplete(c, fmt.Sprintf("the in-flight registers could not be enumerated: %v", serr))
		return nil, nil
	}
	// The identifiers whose subjects the enumeration RETURNED. The enumeration is
	// complete, so an identifier it did not return is authoritatively absent; presence
	// here establishes nothing about absence. The staged recovery release and the marker
	// walk's sibling check both rest on that absence.
	observedPI := make(map[uint16]bool, len(subjectCounts))
	subjects := make([]string, 0, len(subjectCounts))
	for subject := range subjectCounts {
		subjects = append(subjects, subject)
		if name, perr := mqttParseInflightSubject(subject); perr == nil {
			observedPI[name.pi] = true
		}
	}
	// Deterministic order, so a log line or a test does not depend on map iteration.
	sort.Strings(subjects)

	loaded := make([]*StoredMsg, 0, len(subjects))
	for start := 0; start < len(subjects); start += mqttInflightLoadBatch {
		end := min(start+mqttInflightLoadBatch, len(subjects))
		batch := subjects[start:end]
		resps, berr := as.loadInflightRecordsMulti(batch)
		if berr != nil {
			sess.setPISpaceIncomplete(c, fmt.Sprintf("the in-flight registers could not be loaded: %v", berr))
			return nil, nil
		}
		for i, resp := range resps {
			switch mqttClassifyMsgGetResponse(resp) {
			case mqttInflightResponsePresent:
				loaded = append(loaded, resp.Message)
			case mqttInflightResponseAbsent:
				// Enumerated and now genuinely gone: a concurrent delete, which is ordinary.
				// Nothing can read an absent register as an owed delivery.
			default:
				// A failed READ, not an absence. Treating it as "this register is gone" would
				// leave its identifier unreserved and free for reuse while the client may
				// still be holding it — the same class of mistake as reading an unreadable
				// record as "nothing in flight". Some identifier may be in use that this pass
				// never saw, so the identifier space is incomplete.
				sess.setPISpaceIncomplete(c,
					fmt.Sprintf("the in-flight register on %q could not be read", batch[i]))
				return nil, nil
			}
		}
	}

	// Only fact-bearing exchanges belong to C5's missing-slot check.
	// Other exchanges retain the pre-C5 register route and custody accounting.
	if census != nil {
		present := make(map[string]bool, len(loaded))
		for _, msg := range loaded {
			present[msg.Subject] = true
		}
		for _, x := range census.live {
			if x.Abandoned != nil && !mqttExchangePhaseEnded(x.Phase) &&
				!present[mqttInflightSubject(idHash, gen, x.PI)] {
				opaque = append(opaque, mqttRecoveredInflightReg{rec: &mqttInflightRecord{PI: x.PI}})
				quarantinedPairs[x.PI] = true
			}
		}
	}

	// (2) The PUBREL records: identifiers already past PUBREC.
	//
	// SCANNED AFTER EVERY REGISTER BATCH ABOVE, never before, and the ordering is the
	// protocol fact rather than a detail of this function. A record is evidence ABOUT a
	// register, so an absence of one may only be read against a register observed EARLIER
	// than the scan. Read first, the scan answers "is there a record for this exchange"
	// from a moment before the exchange's own register was looked at — and a peer that
	// stored the PUBREL and re-phased the slot in between then had its record reasoned
	// away as absent, the slot declared finished, and the client's PUBREL deleted with no
	// PUBCOMP ever sent (F-099). This is the same rule the marker phase states below: the
	// register enumeration runs FIRST and the per-exchange evidence second, never the
	// other way round (N-5.6).
	//
	// What the ordering does NOT give, because a stronger reading of it would be false.
	// The scan is a forward cursor over PRESENTLY RETAINED records, not history, so a
	// record created and deleted before the cursor reached it is absent from it. And it
	// excludes no FUTURE store: a body this pass adopts later — from a custody contest's
	// authoritative reread — is newer than this scan, so it gets no verdict from it and is
	// routed on its own adopted phase instead.
	//
	// UNSUPPORTED RECORDS ARE ALREADY GONE, refused by mqttDecodeStoredPubRel inside the
	// scan, and a holistic review found this was the gap that made the rest of the
	// identity work decorative.
	//
	// A record whose shape this broker does not write cannot be attributed to any exchange,
	// so it must not decide anything on one's behalf. Left in, it fed source evidence —
	// suppressing a matching PUBLISH source as "past PUBREC" — and became a pendingPubRel
	// reservation, holding a packet identifier that the delivery callback then refuses to
	// transmit or acknowledge. With a Receive Maximum of 1 that stalls the session's QoS
	// 1/2 delivery entirely, and it contradicted the documented claim that such a record
	// costs stream space rather than an identifier.
	//
	// Filtered here, at the single point every later consumer of THIS function reads from,
	// rather than at each consumer. The scan itself no longer filters: the affine-proof walk
	// and the generation teardown must see every record, because their question is what
	// evidence is about to be destroyed. The predicate widened from the owner alone to the
	// whole shape when the one decoder landed.
	rels, refused, ok := sess.scanPubRelRecords(pubRelSubject)
	if !ok {
		// Fails closed BEFORE any classification or apply, exactly as it did when this ran
		// ahead of the enumeration. The enumeration and the batch reads have now already
		// happened, so a register-read failure reports first and this one is reached only
		// when the registers were readable; the identifier space is withheld either way.
		sess.setPISpaceIncomplete(c, "the PUBREL records could not be scanned")
		return nil, nil
	}
	if refused > 0 {
		c.Warnf("MQTT: ignoring %d stored PUBREL record(s) of client %q whose shape this broker "+
			"does not write: they are never transmitted, never acknowledged and reserve no "+
			"identifier; remove them from the %s stream", refused, sess.id, mqttOutStreamName)
	}
	supported := make([]mqttStoredPubRel, 0, len(rels))
	for _, r := range rels {
		if r.shapeErr != nil {
			continue
		}
		supported = append(supported, r)
	}
	rels = supported
	// canonicalPubRelPresent is whether the slot's NAMED canonical record — the exact stream
	// sequence its tuple carries — is among the records the scan found. An identity match at
	// another sequence (a non-canonical duplicate, such as a re-materialisation candidate that
	// never became canonical) is not the canonical record (holistic review H7b-2).
	canonicalPubRelPresent := func(rec *mqttInflightRecord) bool {
		for _, rel := range rels {
			if rel.seq == rec.PubRelStreamSeq && mqttStoredPubRelMatchesSlot(rel, rec) {
				return true
			}
		}
		return false
	}
	findExactPubRel := func(rec *mqttInflightRecord) (mqttStoredPubRel, bool) {
		for _, rel := range rels {
			if mqttStoredPubRelMatchesSlot(rel, rec) {
				return rel, true // scan order: the lowest matching stream sequence
			}
		}
		return mqttStoredPubRel{}, false
	}

	// The pending cancellation fences the walk diverts before any classification (F-088).
	var pendingFences []mqttRecoveredInflightReg
	// (3) Classification, from registers observed before the scan above.
	for _, smsg := range loaded {
		name, perr := mqttParseInflightSubject(smsg.Subject)
		if perr != nil {
			// The filter cannot match a subject this broker did not write, so this is
			// unreachable. Retained rather than deleted, and skipped rather than fatal:
			// it names no identifier we could be holding.
			c.Warnf("MQTT: ignoring an unattributable in-flight register on %q at sequence %d: %v",
				smsg.Subject, smsg.Sequence, perr)
			continue
		}
		rec, derr := mqttDecodeInflightRecord(smsg.Data)
		if derr != nil || rec.PI != name.pi {
			// A RESIDENT RETAINED entry on the identifier is demoted too (F-105 7c, consult 107): the
			// quarantine authorizes nothing, and a retained entry left acting over a register this
			// broker cannot read would be replayed, owed and, exhausted, close the connection.
			sess.mu.Lock()
			residentRetained := (sess.pendingPublish[name.pi] != nil && sess.pendingPublish[name.pi].kind == mqttPendingRetained) ||
				(sess.pendingPubRel[name.pi] != nil && sess.pendingPubRel[name.pi].kind == mqttPendingRetained)
			sess.mu.Unlock()
			quarantinedPairs[name.pi] = factPIs[name.pi] || residentRetained
			// Unreadable, or naming an identifier other than its own subject's. The BODY
			// is unusable, but the SUBJECT is not: it parsed, so this incarnation wrote a
			// register for that exact identifier — and by R1 a register exists only for a
			// delivery that was about to go on the wire, so the client may be holding it.
			//
			// So the identifier is RESERVED and the record is KEPT. Deleting it and
			// reserving nothing would free an identifier that may be in use, which is the
			// same mistake the consumer-gone case exists to avoid, and the next resume
			// would not even know to reserve it. The cost is one identifier and one
			// Receive Maximum slot held until the session ends, which is the safe
			// direction; the delivery itself is unrecoverable either way, because its
			// source is exactly what the unreadable body would have named.
			c.Warnf("MQTT: reserving packet identifier %d for client %q from an UNREADABLE in-flight register on %q at sequence %d: "+
				"the identifier is held so it is never reused, and because the delivery that owns it cannot be identified, "+
				"new outbound QoS 1/2 allocation is withheld for this session",
				name.pi, sess.id, smsg.Subject, smsg.Sequence)
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: name.pi},
				seq: smsg.Sequence,
			})
			continue
		}
		// THE LEGAL-SHAPE MATRIX IS APPLIED HERE, NOT JUST THE STATE, because the matrix is
		// this subsystem's ONE definition of a record it wrote and no reader may form its
		// own opinion of legality. A holistic round found the read side checking the state
		// alone, which admitted every illegal `publish` shape the matrix exists to name — a
		// half-written action, a half-written PUBREL identity, half an affine pair, a QoS 1
		// record carrying QoS 2 fields — straight into source classification, where a
		// partial affine pair becomes an ordinary exchange and loses its completion
		// obligation. The write boundary refuses all of them, so any that reaches a read is
		// from elsewhere and is exactly what the quarantine below is for.
		if verr := mqttValidateInflightShape(rec); verr != nil {
			quarantinedPairs[name.pi] = factPIs[name.pi] ||
				mqttRecoveryOwnsExchangeRoute(rec, exchanges[rec.Lin])
			c.Warnf("MQTT: reserving packet identifier %d for client %q from an in-flight register on %q "+
				"at sequence %d whose shape this broker does not write (%v): the identifier is held so "+
				"it is never reused, and nothing acts on the record",
				name.pi, sess.id, smsg.Subject, smsg.Sequence, verr)
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: name.pi}, seq: smsg.Sequence,
			})
			continue
		}
		// A PENDING CANCELLATION FENCE IS IDENTIFIED FIRST, by its written provenance, and goes
		// straight to the drop list under its own disposal rule (F-088, consult C4 item 3). It
		// is diverted HERE — before the shared-record exchange validation, the inventory check,
		// the marker phase and the affine claim pass — so none of those paths ever receives a
		// fence, and neither they nor their writers can establish or rewrite its provenance. It
		// is never restored and no acting entry is installed for it.
		if mqttValidPendingFence(smsg.Subject, rec) {
			pendingFences = append(pendingFences, mqttRecoveredInflightReg{rec: rec, seq: smsg.Sequence})
			continue
		}
		// AN UNSUPPORTED SHARED SHAPE IS REFUSED BEFORE ANYTHING INTERPRETS IT (7a). A shared
		// register exists only as a QoS 1 delivery admitted beside an exchange naming its exact
		// acquisition; a shared register at another QoS, a retired one (retirement is a PUBREC fact),
		// or one whose exchange is missing, unreadable or names another dependency is a shape this
		// broker does not write. It is refused HERE — before the custody claim, the retirement
		// classification and every source-settlement route — so no reading of it installs source-ACK
		// authority: its identifier is reserved and nothing acts on the record.
		// It still names its source, so it joins the source grouping as a member this pass may not
		// install: a duplicate of it fails closed exactly as an unclaimed member's does.
		if mqttIsSharedDurable(rec.Dur) && sess.sharedRecordUnsupported(as, gen, rec) {
			c.Warnf("MQTT: reserving packet identifier %d for client %q: the shared in-flight register on %q at "+
				"sequence %d is a shape this broker does not write, so nothing acts on the record",
				name.pi, sess.id, smsg.Subject, smsg.Sequence)
			unsupportedShared = append(unsupportedShared, mqttRecoveredInflightReg{rec: rec, seq: smsg.Sequence})
			quarantinedPairs[name.pi] = true
			continue
		}
		// AN ORDINARY REGISTER IS RESTORED ONLY BESIDE THE EXCHANGE THE INVENTORY REACHED (F-105 7b).
		// The register, its exchange and its duty ordinal are written in ONE admission batch, so
		// with a COMPLETE inventory census an ordinary register whose exchange the traversal did
		// not reach (absent, or unreadable and so uncertain) is not an obligation this resume may
		// continue: it reserves its identifier and nothing acts on it — restoration never
		// manufactures admission from a register. NO CANCELLATION IS EXEMPT (F-088): the one
		// cancellation written with no exchange by design, the pending fence, was diverted above
		// by its provenance; an OCCUPANT cancellation without its exchange is an unattributed
		// shape (P-d2), refused here like any register, never restored on the strength of its
		// bit. An incomplete census decides nothing here: the ledger withholds new work and
		// allocation until it completes.
		if !censusIncomplete && !mqttDurIsSlotOnlyClass(rec.Dur) && exchanges[rec.Lin] == nil {
			c.Warnf("MQTT: reserving packet identifier %d for client %q: the in-flight register on %q at "+
				"sequence %d names obligation %q, which this generation's inventory does not reach, so "+
				"nothing acts on the record", name.pi, sess.id, smsg.Subject, smsg.Sequence, rec.Lin)
			opaque = append(opaque, mqttRecoveredInflightReg{rec: &mqttInflightRecord{PI: name.pi}, seq: smsg.Sequence})
			quarantinedPairs[name.pi] = true
			continue
		}
		// A RETAINED REGISTER LEAVES HERE, before every ordinary-source route (F-105 7c): the
		// source cache, the source and drain groupings and the source mappings never see it.
		if rec.retainedSource() {
			rr, rerr := sess.classifyRecoveredRetained(as, gen, smsg.Subject, smsg.Sequence, rec)
			var q *mqttRetainedQuarantineError
			if errors.As(rerr, &q) {
				retainedQuarantined = append(retainedQuarantined,
					mqttRetainedQuarantined{subject: smsg.Subject, nonce: rec.Nonce, reason: q.reason})
				rr = nil
			} else if rerr != nil {
				return nil, rerr
			}
			if rr == nil {
				c.Warnf("MQTT: reserving packet identifier %d for client %q: the retained in-flight register on %q at "+
					"sequence %d cannot be restored from its copy or canonical, so nothing acts on it",
					name.pi, sess.id, smsg.Subject, smsg.Sequence)
				opaque = append(opaque, mqttRecoveredInflightReg{rec: &mqttInflightRecord{PI: name.pi}, seq: smsg.Sequence})
				quarantinedPairs[name.pi] = true
				continue
			}
			retainedRecovered = append(retainedRecovered, *rr)
			continue
		}
		// C5 routing non-interference: the census selects exchange-only facts;
		// register metadata and terminal carriers select themselves. Selection
		// grants no authority: validate an owned pair with a fresh point read.
		// All other records keep exactly their pre-C5 route and read sequence.
		if mqttRecoveryOwnsExchangeRoute(rec, exchanges[rec.Lin]) {
			paired := false
			if sess.jsa != nil {
				x, _, xerr := sess.jsa.loadOutboundExchange(sess.id, as.sessStreamEpoch, gen, rec.Lin)
				sess.mu.Lock()
				paired = xerr == nil && sess.recoverExchangeRouteLocked(c, smsg.Subject, smsg.Sequence, rec, x)
				sess.mu.Unlock()
			}
			if !paired {
				opaque = append(opaque, mqttRecoveredInflightReg{rec: &mqttInflightRecord{PI: name.pi}, seq: smsg.Sequence})
				quarantinedPairs[name.pi] = true
				continue
			}
			if rec.State == mqttInflightDeliveryAbandoned {
				continue // Only this row is ACK-only; successors still owe PUBREL/cleanup.
			}
		}
		// Opaque occupancy gains an identity, not send authority. The final
		// classifier and marker decision still govern its replacement below.
		sess.mu.Lock()
		sess.bindRecoveryOpaqueLocked(rec.PI, smsg.Sequence, rec)
		sess.mu.Unlock()
		// ONLY A KNOWN, LEGAL SHAPE IS ROUTED. PUBLISH slots enter ordinary source
		// classification; past-PUBREC slots enter their dedicated recovery path below; an
		// absent or unknown state is quarantined so its identifier is reserved and nothing
		// acts on the record.
		//
		// Two reviews shaped this routing. First: with identity-only validation, a record in a
		// PUBREL state flowed into the live-source bucket and became an actionable
		// `pendingPublish` entry, and `beginSendLocked` does not inspect slot state — so it
		// could RETRANSMIT the payload of an exchange already past PUBREC. Second, and the
		// reason an absent state is refused too: tolerating it as "what earlier writers
		// produced" was itself rolling-upgrade machinery, and it CONTRADICTED the write
		// boundary — every shared register is claim-rewritten through storeInflightCAS,
		// whose validator refuses an absent state, so an admitted record became an
		// unresumable session a moment later.
		//
		// There is no legacy population to serve: pre-alpha, no releases anywhere, so an
		// unsupported persisted shape is REFUSED rather than migrated or tolerated. A
		// developer stream carrying older records resets, which is the documented boundary
		// (the nine broker-owned streams are one recreation unit, F-066).
		if !mqttInflightStateKnown(rec.State) {
			// A state this broker genuinely does not write. Opaque: the identifier is
			// reserved, nothing acts on the record, and the identifier space is
			// incomplete — which withholds new outbound QoS 1/2 allocation until an
			// operator removes it.
			//
			// ONE PREDICATE, and that matters. This used to read `rec.State !=
			// mqttInflightPublish`, written when `publish` was the only state, and it was
			// a SECOND enumeration of "states this broker writes" beside
			// mqttInflightStateKnown. When the four past-PUBREC states arrived, that copy
			// classified every re-phased slot as unreadable and withheld the session's
			// entire outbound QoS 1/2 flow — two mechanisms stating one fact, disagreeing
			// the moment one of them changed.
			c.Warnf("MQTT: reserving packet identifier %d for client %q from an in-flight register on %q "+
				"at sequence %d whose state %q this broker does not write: the identifier is held so it "+
				"is never reused, and nothing acts on the record",
				name.pi, sess.id, smsg.Subject, smsg.Sequence, rec.State)
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: name.pi}, seq: smsg.Sequence,
			})
			continue
		}
		if rec.State == mqttInflightPublishTerminal {
			// A PUBLISH THIS BROKER RECORDED TERMINAL AND DID NOT LIVE TO DELETE.
			//
			// The record is itself the proof the client-facing exchange ended: the
			// acknowledgement was processed, the phase was written, and only the register
			// delete was still owed when the process died. So this is the CONTINUATION of
			// that delete, not the recovery of a live delivery — and it must not join
			// `regs`, because the classification chain there installs a pendingPublish
			// entry and beginSendLocked does not inspect slot state, so an acknowledged
			// payload would go back on the wire with nothing to stop it. That is the same
			// hazard the past-PUBREC routing below exists for, reached by a phase that did
			// not exist when that routing was written.
			//
			// Source completion is separate from the MQTT ending. C4 retains the
			// source-bearing register and its owner while the source is Live/Unknown.
			// Reserve and arm together, including slot-only records and records
			// whose pair/action cannot yet be settled. Dispatch waits for the walk.
			tref := mqttSlotRefFromRecord(smsg.Subject, smsg.Sequence, rec)
			if tref != nil {
				sess.mu.Lock()
				sess.armTerminalRetryLocked(c, *tref, false)
				sess.holdTerminalCleanupLocked(name.pi, tref.nonce)
				sess.handOffRecoveryPublishLocked(*tref)
				sess.mu.Unlock()
			}
			if sess.recoverPublishTerminalSlot(c, smsg.Subject, smsg.Sequence, rec) {
				continue
			}
			// UNRESOLVED. Not an absence and not a licence to install anything. The
			// identifier is reserved by the IDENTITY-BOUND terminal hold — the term an
			// acknowledgement installs — and the continuation that confirms the delete
			// releases it (holistic review round 2, finding 3). Not an opaque entry: that
			// made the whole identifier space incomplete, and the confirmed delete had no
			// reservation of its own to discharge, so allocation stayed withheld after the
			// obligation was met. Both hold and continuation already exist, so any
			// later scan failure can return without orphaning either one.
			if tref != nil {
				c.Warnf("MQTT: reserving packet identifier %d for client %q: the terminal in-flight "+
					"register on %q at sequence %d could not be released, and the release is retried",
					name.pi, sess.id, smsg.Subject, smsg.Sequence)
				continue
			}
			// No identity a continuation could be bound to: reserved opaque, fail closed.
			c.Warnf("MQTT: reserving packet identifier %d for client %q: the terminal in-flight "+
				"register on %q at sequence %d names no identity its release could be bound to",
				name.pi, sess.id, smsg.Subject, smsg.Sequence)
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: name.pi}, seq: smsg.Sequence,
			})
			continue
		}
		if mqttInflightPastPubRec(rec.State) {
			// PAST PUBREC, and it must NOT join `regs`. The classification chain below
			// installs a `pendingPublish` entry, and beginSendLocked does not inspect slot
			// state — so a past-PUBREC slot restored as an ordinary PUBLISH entry could
			// RETRANSMIT the payload of an exchange the client has already answered. That
			// hazard is the reason the single-state check existed, and it is real; what was
			// wrong was refusing the record rather than routing it.
			pastPubRec = append(pastPubRec, mqttRecoveredInflightReg{rec: rec, seq: smsg.Sequence})
			continue
		}
		fr := mqttRecoveredInflightReg{rec: rec, seq: smsg.Sequence}
		// PUBLISH plus a PRESENT exact PUBREL is recovery route B, not ordinary
		// source-completion evidence. Pick the lowest matching stream sequence from the
		// complete, ordered scan; later exact records are duplicates of that canonical
		// choice. A record naming only the same source is not enough—the PI, nonce and
		// affine identity are part of the exchange.
		sourceNamed := false
		exact, exactFound := findExactPubRel(rec)
		for i := range rels {
			rel := &rels[i]
			if rel.srcDur != rec.Dur || rel.srcSeq != rec.Seq {
				continue
			}
			sourceNamed = true
		}
		switch {
		case exactFound:
			routeB = append(routeB, mqttRecoveryRouteB{reg: fr, rel: exact})
			continue
		case sourceNamed:
			// Source-only evidence used to flow into relBySource and could delete this
			// slot as a completed handoff even though the PUBREL belonged to another
			// identifier or exchange. Reserve and keep it; the conflict needs an
			// operator, not an inferred winner.
			c.Warnf("MQTT: reserving packet identifier %d for client %q: a stored PUBREL "+
				"names its source %s/%d but not the same exchange", rec.PI, sess.id, rec.Dur, rec.Seq)
			quarantinedPublishConflicts = append(quarantinedPublishConflicts, fr)
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: rec.PI}, seq: smsg.Sequence,
			})
			continue
		default:
			regs = append(regs, fr)
		}
	}

	// An independent PUBREL scan cannot authorize an inconsistent pair.
	keptRels := rels[:0]
	for _, rel := range rels {
		if !quarantinedPairs[rel.pi] {
			keptRels = append(keptRels, rel)
		}
	}
	rels = keptRels

	// Complete route B before asking what happened to the source. The slot transition
	// preserves its current custody; the ordinary past-PUBREC classifier below then
	// performs the same ordered custody contest as every recovered actionable PUBREL.
	for _, handoff := range routeB {
		phased, ok := sess.phaseRecoveredPublishSlotToReady(c, handoff.reg, handoff.rel)
		if !ok {
			// A failed or raced CAS proves no state. Keep the identifier and the slot, and
			// withhold new allocation rather than letting source classification free it.
			c.Warnf("MQTT: reserving packet identifier %d for client %q: recovery could not "+
				"complete its stored PUBREL handoff", handoff.reg.rec.PI, sess.id)
			quarantinedPublishConflicts = append(quarantinedPublishConflicts, handoff.reg)
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: handoff.reg.rec.PI}, seq: handoff.reg.seq,
			})
			continue
		}
		pastPubRec = append(pastPubRec, phased)
	}

	// (3) Classify each register, mutating no session state yet: a JetStream lookup
	// must not run under the session lock, and the decisions have to be taken against
	// one consistent view.
	//
	// Sources are resolved ONCE, in bulk: one consumer lookup per durable and batched
	// message reads, rather than two round trips per record before the CONNACK. Duplicate
	// registers naming one source share the result.
	srcCache := as.newInflightSourceCache()
	if len(regs)+len(pastPubRec) > 0 {
		all := make([]*mqttInflightRecord, 0, len(regs)+len(pastPubRec))
		for _, fr := range regs {
			all = append(all, fr.rec)
		}
		for _, fr := range pastPubRec {
			all = append(all, fr.rec)
		}
		srcCache.resolve(all)
	}

	var reserve []mqttRecoveredInflightReg
	// A custody contest can observe the exact slot deleted after the PUBREL scan. Keep
	// that outcome through classification so the earlier scan cannot install a slot-less
	// placeholder which a later callback might mistake for retained in-memory authority.
	closedPubRelOwners := make(map[uint16]string)
	noteCustodyClosed := func(in []mqttRecoveredInflightReg) {
		for _, fr := range in {
			if fr.rec != nil && fr.rec.PI != 0 && fr.rec.Nonce != _EMPTY_ {
				closedPubRelOwners[fr.rec.PI] = fr.rec.Nonce
			}
		}
	}
	// Every drop carries its NONCE alongside its sequence: the delete routes through
	// reapRegisterWithVerdict, so a cancellation verdict attached to the register is
	// released against the proof of its deletion instead of accumulating for the life of
	// the session (F-087 WP3).
	type regDrop struct {
		seq   uint64
		nonce string
		// dur and sseq are the SOURCE identity of the exchange this drop names, carried so
		// the post-delete confirmation can compare the whole triple. confirmSlotReleased
		// distinguishes "our own record, rewritten" from "a successor took the subject" on
		// (Nonce, Dur, Seq), and a drop that could not supply them could only ever lift its
		// exclusion on a weaker comparison than every other release edge uses.
		dur  string
		sseq uint64
		// ref is the slot identity this drop was classified against, so the deletion is
		// subject-addressed, identity-authorized and freshness-fenced instead of aiming a
		// remembered global sequence at whatever occupies it later.
		ref *mqttInflightSlotRef
		// disp is the LICENSE this particular drop rests on, and it is not decoration: the
		// gate treats the two values differently at the freshness fence. On a same-exchange
		// rewrite mqttSlotDeleteNeverTransmitted refuses — a marker-proven drop must not
		// act on a record that has moved under it, and must not initiate cleanup of that
		// register's marker either — while mqttSlotDeleteComplete treats the rewrite as
		// another owner taking responsibility and reports success. Marker-proven drops take
		// the former; a close decision and a proven-complete retirement take the latter.
		disp mqttInflightDisposition
	}
	var drop []regDrop
	// The diverted fences join the drop list under their own disposition: the door deletes
	// exactly the observed fence, confirmed, preserves a moved or different occupant, ends no
	// exchange and releases no charge; the identifier's exclusion is installed in the apply
	// phase and lifted only on the subject's confirmed absence, like every drop (F-088).
	for _, pf := range pendingFences {
		drop = append(drop, regDrop{
			seq: pf.seq, nonce: pf.rec.Nonce,
			ref:  mqttSlotRefFromRecord(mqttInflightSubject(idHash, gen, pf.rec.PI), pf.seq, pf.rec),
			disp: mqttSlotDeletePendingFence,
			dur:  pf.rec.Dur, sseq: pf.rec.Seq,
		})
	}
	// sourceConsumed carries every register this pass dropped on the strength of its
	// SOURCE into the apply phase: a same-object barrier re-run may still hold the live
	// entry that exchange backed, and it must be retired with the register or it counts
	// against Receive Maximum forever.
	//
	// TWO member classes, and the name is deliberately not "complete" because only one of
	// them is: a RETIREMENT whose source is proven complete, added before the marker phase
	// runs at all; and a PUBLISH-phase register whose source can never be re-offered AND
	// whose marker proves the PUBLISH never went out. The second class is the one that may
	// carry a merely LOST source, so no member's presence here asserts the exchange
	// completed.
	var sourceConsumed []mqttRecoveredInflightReg
	// Registers dropped on the strength of a cancellation NONCE, kept apart because their
	// removal has to be proven before the intent that named them is released.
	type cancelledDrop struct {
		seq   uint64
		nonce string
		ref   *mqttInflightSlotRef // see regDrop.ref
		dur   string               // see regDrop.dur
		sseq  uint64
	}
	var dropByNonce []cancelledDrop

	// A register a DRAIN claims is the drain's, and recovery must not touch the record.
	// The reason is I2b, NOT the drain's release addressing: finalizeDrainDelivery
	// (unsub_drain.go) ignores the persisted RegSeq and releases by IDENTITY through
	// releaseInflightRegisterByIdentity, following a rewrite, so a moved sequence would not
	// by itself strand it (N-11). What retiring the record here WOULD do is duplicate the
	// drain's own suppression, which I2b governs: only a CONFIRMED PUBREL may release a
	// drained claim, so recovery re-phasing or dropping the record would let a source be
	// re-offered or acked that the drain still owes a PUBREL for.
	//
	// So the register is left alone here and its sequence is REPAIRED into the drain state
	// instead, which also brings records written before RegSeq existed up to date. The
	// identifier is still reserved: past PUBREC by the PUBREL loop below, and otherwise by
	// the ordinary live-register path.
	// A drain holds ONE register sequence per claimed delivery, so more than one register
	// naming a drained source cannot be represented: assigning each in turn would leave the
	// drain releasing only the last, and the others would survive its consumer as phantoms.
	// That state means a superseded broker wrote a second register, and it is the same
	// ambiguity the duplicate-source rule already fails closed on — so these are routed
	// there instead: every identifier reserved, every record kept, nothing deleted, and
	// allocation withheld. The drain's own sequence is cleared so it cannot release one of
	// them and orphan the rest.
	drainRegs := make(map[string][]mqttRecoveredInflightReg)
	for _, fr := range regs {
		drainRegs[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)] = append(
			drainRegs[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)], fr)
	}
	// Route B removes its PUBLISH-shaped slot from regs before source classification,
	// but a drain still needs the original observed register sequence repaired into its
	// non-authoritative hint. Omitting these records reset an old-format drain's RegSeq
	// to zero even though recovery had just found and re-phased its exact slot.
	for _, handoff := range routeB {
		fr := handoff.reg
		drainRegs[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)] = append(
			drainRegs[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)], fr)
	}
	sess.mu.Lock()
	// READ ONLY here (F-105 7b): the observations are installed by apply, after the walk has
	// proven it still serves, so a refused walk leaves the drains exactly as they stood.
	drainOwned, drainAmbiguous, drainObs := sess.computeDrainRegisterStateLocked(drainRegs)
	sess.mu.Unlock()
	for key := range drainAmbiguous {
		group := drainRegs[key]
		c.Errorf("MQTT: %d in-flight registers of client %q name one DRAINED delivery %s/%d on different packet identifiers; "+
			"all are reserved, none is released, and new outbound QoS 1/2 allocation is withheld for this session",
			len(group), sess.id, group[0].rec.Dur, group[0].rec.Seq)
		for _, fr := range group {
			opaque = append(opaque, mqttRecoveredInflightReg{rec: &mqttInflightRecord{PI: fr.rec.PI}, seq: fr.seq})
		}
	}

	// Source retirements suppress a payload whose exchange is known past PUBREC while its
	// source may still be re-offered. The Retired bit is not itself a slot phase: a crash
	// may leave State at publish, and only a present exact PUBREL licenses route B's phase
	// transition. Such a publish-shaped retirement still reserves its known identifier as
	// a non-actionable PUBREL-side entry in the apply phase; it is not made deliverable and
	// it does not make the otherwise reconstructed identifier space opaque.
	var retired []mqttRecoveredInflightReg
	// occupancyOwed names the registers this pass PRESERVES while consuming their source:
	// the durable record stays, and its identifier must be excluded from allocation in the
	// apply phase whether or not an in-memory entry is resident to carry the obligation. A
	// cold resume has no such entry, which is exactly the case the exclusion exists for.
	var occupancyOwed []mqttRecoveredInflightReg
	// Affine registers whose carried pair its own source does not prove. Kept apart only
	// until the claim pass has run, then folded in as unowned members of their source's
	// group: nothing here may be claimed, installed or settled.
	var unprovenAffine []mqttRecoveredInflightReg
	live := make([]mqttRecoveredInflightReg, 0, len(regs))
	for _, fr := range regs {
		// A CANCELLATION IS NOT DECIDED HERE (F-088, WP3 edit 4). A cancellation — the tombstone
		// bit on the record, or the record's nonce in the session's cancelled set — is a decision
		// to stop delivering, made by a writer that cannot see a PUBLISH already on the wire
		// (an inherited occupant, another broker). It proves nothing about transmission. So a
		// pure cancellation goes through the SAME classification as any other PUBLISH-phase
		// register, and its marker decides it in the marker phase below: a standing marker for
		// this allocation is a genuine never-transmitted cancellation and takes the cancellation
		// disposition there (the GENUINE CANCELLATION arm after the marker phase); an absent marker makes it
		// possibly-attempted, served exactly like an uncancelled marker-absent register; an
		// unreadable marker reserves it opaque. Retired still dominates: such a record routes
		// to the retirement arms, suppression and nothing freed.
		if fr.rec.Q2AffClaim != _EMPTY_ && srcCache.state(fr.rec) != mqttInflightSourceComplete {
			// An affine register's carried pair is installed only where the register's OWN
			// SOURCE proves it, because for an affine delivery the source IS the copy.
			// Anything else — a live source naming a DIFFERENT copy, a source that could
			// not be read, a consumer that is knowably gone — leaves the pair unproven, and
			// interpreting it anyway is what would credit another claim with this delivery
			// and lose that claim's message. A COMPLETE source is excluded because no pair
			// is installed for one: the arms below drop that register outright.
			if observed, proven := srcCache.affinePairProvenBySource(fr.rec); !proven {
				if observed != _EMPTY_ {
					c.Errorf("MQTT: the in-flight register on packet identifier %d of client %q carries affine QoS 2 copy %q "+
						"but its own delivery %s/%d is %q: the register is reserved, nothing is written to the claim it names, "+
						"and new outbound QoS 1/2 allocation is withheld for this session",
						fr.rec.PI, sess.id, fr.rec.Q2AffCopy, fr.rec.Dur, fr.rec.Seq, observed)
				} else {
					c.Errorf("MQTT: the in-flight register on packet identifier %d of client %q carries affine QoS 2 copy %q "+
						"but its own delivery %s/%d could not be proven to be that copy: the register is reserved, nothing is "+
						"written to the claim it names, and new outbound QoS 1/2 allocation is withheld for this session",
						fr.rec.PI, sess.id, fr.rec.Q2AffCopy, fr.rec.Dur, fr.rec.Seq)
				}
				// It joins the duplicate-source grouping as an UNOWNED member rather than
				// being set aside on its own, and that is the difference between reserving
				// this identifier and actually failing closed. An unproven pair does not
				// prove the identifier was never transmitted — the client may hold THIS one
				// rather than a sibling's — so a sibling naming the same delivery must not
				// be installed as an authoritative singleton with a cpending mapping, or the
				// source's redelivery follows that mapping and re-sends the payload under a
				// second identifier. As an unowned member it poisons every sibling of its
				// source, exactly as a register whose claim could not be established does.
				unprovenAffine = append(unprovenAffine, fr)
				continue
			}
		}
		if drainAmbiguous[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)] {
			continue // already routed to opaque above: reserved, kept, never released
		}
		if drainOwned[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)] {
			// The drain owns this record (see above): neither rewritten nor deleted here.
			// Its identifier is reserved by the PUBREL loop when the exchange is past PUBREC,
			// and by the live path otherwise.
			// An exact PUBREL was removed into route B before this loop, while a
			// same-source mismatch was quarantined. What remains is still PUBLISH-side.
			live = append(live, fr)
			continue
		}
		if fr.rec.Retired {
			// A retirement CONVERGES here: its source may have been acknowledged since (the
			// unproven acknowledgement that created it may simply have landed), and a
			// retirement whose source is provably complete has nothing left to suppress. Without
			// this every resume would re-establish it from its own record, forever.
			if srcCache.state(fr.rec) == mqttInflightSourceComplete {
				// Proven COMPLETE, not merely non-reofferable: a retirement is past-PUBREC
				// evidence, so Lost never licenses dropping one and the test stays exact.
				//
				// THE SOURCE IS CONSUMED; THE REGISTER IS NOT DROPPED (F-098, consult D-7).
				// This arm used to stage a complete-delete, and under C-IV that is a release
				// this record can never obtain authority for: `Retired` is backed by the
				// client's own PUBREC, so arm 4 refuses it whatever the marker or a
				// cancellation says, and `publish` is not a terminal phase, so arm 3 does not
				// apply either. Staging it anyway produced the worst of both — the delete was
				// refused, the durable record survived, and nothing reserved its identifier,
				// so a COLD resume (no resident entry to carry the obligation) left the
				// register standing with its packet identifier allocatable.
				//
				// What the source completion actually settles is the SOURCE. The client's
				// half of the exchange is untouched by it — that is C-II — so the identifier
				// stays OCCUPIED and the occupancy obligation is recorded here, to be
				// installed in the apply phase independently of any resident entry.
				sourceConsumed = append(sourceConsumed, fr)
				occupancyOwed = append(occupancyOwed, fr)
				continue
			}
			retired = append(retired, fr)
			continue
		}
		if st := srcCache.state(fr.rec); !mqttInflightSourceMayReoffer(st) {
			// THE SOURCE CAN NEVER BE RE-OFFERED, and this is the one site that says so.
			// Two states qualify and mqttInflightSourceMayReoffer is their one definition:
			// COMPLETE — acknowledged, or authoritatively absent while its consumer still
			// holds interest, including a message reaped by its own Message Expiry TTL; and
			// LOST — the consumer is authoritatively gone, which took the messages it held
			// with it on an interest-retained stream.
			//
			// Neither is proof the RECEIVER's exchange finished: a PUBLISH may have reached
			// a persistent client whose PUBREC or PUBACK was lost, and its identifier is
			// then still held on the wire. So the register is not dropped here; it is routed
			// through the marker phase, which drops it only when the marker proves it never
			// transmitted and otherwise RESERVES it. The identity is kept for the apply
			// phase either way: a same-object rebind may hold a LIVE entry this register
			// backed.
			//
			// The assignment is HERE, after every exclusion above has continued, and that
			// position is the mechanism rather than an accident of layout — see srcRouted.
			if st == mqttInflightSourceComplete {
				fr.srcRouted = mqttSourceRoutedComplete
			} else {
				fr.srcRouted = mqttSourceRoutedLost
			}
		}
		live = append(live, fr)
	}

	// A shared register becomes this session object's responsibility only after its
	// claim rewrite has invalidated every sequence a stale close can know. Conversely,
	// absence or a matching tombstone means that close won and is consumed here. The
	// claim runs BEFORE the duplicate-source grouping below, deliberately: a duplicate
	// group fails closed as reservations, and a reservation protects nothing if the
	// registers behind it still sit at sequences a stale close knows — the close would
	// delete one, and the next resume would see a single register and install it as
	// authoritative, resolving by deletion the very ambiguity the group refused to
	// guess about. Claimed first, every member of the group moves, and the ambiguity
	// survives any stale close until an operator resolves it.
	// Every OWNED result whose ADOPTED body is past PUBREC, from any contest below. They
	// are held here rather than routed at their call site because the past-PUBREC sink is
	// built further down, and because two things must NOT happen to them: they may not
	// re-enter a custody contest — custody is already established and the contest has no
	// own-claim fast path, so a second pass would rewrite for nothing — and they may not be
	// judged by the PUBREL scan, which was taken before their body was adopted. See
	// mqttSplitAdoptedPastPubRec.
	var adoptedPastPubRec []mqttRecoveredInflightReg
	// No custody contest over the live PUBLISH population (F-105 7b): it held only shared
	// registers, which are ordinary since 7a, and an ordinary register's ownership is its
	// exchange's. Its outcomes are the walk's own observation: an absent register is simply not
	// loaded, a pending fence takes its provenance drop route above, an unreadable or
	// foreign-nonce record is already opaque, and a displaced CONNECT is refused by the
	// entitlement checks in recoverInflightRegisters.
	var claimClosed, claimFailed, displaced, claimWithheld []mqttRecoveredInflightReg
	// A quiescent PUBLISH slot may have crossed into an action or terminal phase while
	// the custody CAS was in flight. It is no longer a publish candidate; route the
	// authoritative reread through the past-PUBREC classifier below.
	pastPubRec = append(pastPubRec, claimWithheld...)
	// And the same question for the phase the OWNED bodies came back carrying, which the
	// withheld arm does not answer: it intercepts action states and terminal, so a
	// `publish -> pubrel_ready` re-phase lands here instead.
	live, adopted := mqttSplitAdoptedPastPubRec(live)
	adoptedPastPubRec = append(adoptedPastPubRec, adopted...)
	// claimConsumed carries every consumed close decision into the apply phase, so a
	// same-object barrier re-run retires the LIVE in-memory entries those records
	// backed; the non-zero seqs are the tombstones the confirmed delete targets.
	claimConsumed := claimClosed
	for _, fr := range claimClosed {
		if fr.seq != 0 {
			drop = append(drop, regDrop{
				seq: fr.seq, nonce: fr.rec.Nonce,
				ref:  mqttSlotRefFromRecord(mqttInflightSubject(sess.idHash, sess.gen, fr.rec.PI), fr.seq, fr.rec),
				disp: mqttSlotDeleteComplete,
				dur:  fr.rec.Dur, sseq: fr.rec.Seq,
			})
		}
	}
	// The contest returns a BODY, and the body it returns may not be the body the walk
	// sorted into this bucket: a rebuild-from-reread carries forward a Retired a
	// concurrent writer proved between the walk and the landed CAS, and the
	// moved-and-ours arm adopts the record whole. Routing on the pre-contest bucket
	// would install such a record as an ordinary PUBLISH-phase delivery and re-send a
	// payload the client has already completed — the finding itself. So every OWNED
	// result is re-judged on what it now carries, and the newly retired ones leave the
	// live path for the retirement handling below at their post-contest sequence. They
	// are never re-contested (already claimed, at that sequence) and never left in the
	// duplicate-source grouping (they are not PUBLISH-phase deliveries at all).
	live, newlyRetired := mqttSplitNewlyRetired(live)
	// Never claimed, because a claim rewrite would move a record this pass refuses to
	// interpret; grouped with the members whose claim FAILED, whose treatment is exactly
	// what an unproven pair needs.
	claimFailed = append(claimFailed, unprovenAffine...)
	claimFailed = append(claimFailed, unsupportedShared...)
	var retiredFailed, retiredDisplaced, retiredWithheld []mqttRecoveredInflightReg
	// Nor over the retired population (it too held only shared registers).
	claimClosed = nil
	pastPubRec = append(pastPubRec, retiredWithheld...)
	// A RETIRED body whose phase also advanced during this contest. Retirement is
	// source-side suppression and the phase is the slot's own state, so this is not the
	// question mqttSplitNewlyRetired asks and a retired body is not exempt from it: routed
	// as an ordinary retirement it would still reach the PUBLISH-phase install below.
	// Suppression is re-established for it at the past-PUBREC sink, on the same
	// mqttInflightSourceMayReoffer predicate every other past-PUBREC slot uses.
	retired, adopted = mqttSplitAdoptedPastPubRec(retired)
	adoptedPastPubRec = append(adoptedPastPubRec, adopted...)
	displaced = append(displaced, retiredDisplaced...)
	// Retired is source-side suppression, not a substitute slot phase. A record that
	// acquires the bit during the claim contest therefore follows the same retirement
	// path as one the initial scan observed with it; only an exact PUBREL can route the
	// slot through route B.
	retired = append(retired, newlyRetired...)
	claimConsumed = append(claimConsumed, claimClosed...)
	for _, fr := range claimClosed {
		if fr.seq != 0 {
			drop = append(drop, regDrop{
				seq: fr.seq, nonce: fr.rec.Nonce,
				ref:  mqttSlotRefFromRecord(mqttInflightSubject(sess.idHash, sess.gen, fr.rec.PI), fr.seq, fr.rec),
				disp: mqttSlotDeleteComplete,
				dur:  fr.rec.Dur, sseq: fr.rec.Seq,
			})
		}
	}
	if len(displaced) > 0 {
		// Durable proof of displacement, observed mid-barrier: a rival object claimed
		// this allocation between our load and our CAS, and its accept save precedes
		// its claims. Completing this resume would bind a client to a superseded
		// session holding foreign reservations — QoS 2 exchanges declined forever and
		// allocation withheld, with only a missable takeover notification left to
		// evict it. Refuse the resume instead: nothing is installed, the CONNECT is
		// aborted, and the client's retry binds to (or re-establishes) the
		// authoritative owner. A rival that has since crashed does not trip this — its
		// standing claim is re-claimed by the ordinary CAS and never reaches the
		// re-read.
		return nil, fmt.Errorf("could not take ownership of %d in-flight register(s) during resume — "+
			"claimed by another session owner, or the claim fence could not be established (first: pi %d, %s/%d)",
			len(displaced), displaced[0].rec.PI, displaced[0].rec.Dur, displaced[0].rec.Seq)
	}
	for _, fr := range retiredFailed {
		// A retirement this pass could not claim reserves its identifier and nothing
		// else: it enters no source mapping, so there is no sibling install for it to
		// poison the way an unclaimed LIVE member must (below).
		opaque = append(opaque, mqttRecoveredInflightReg{
			rec: &mqttInflightRecord{PI: fr.rec.PI}, seq: fr.seq, foreign: true,
		})
	}

	// Group what survives by the source delivery it names. One register per source is
	// the ordinary case; more than one means a superseded broker wrote a second, on a
	// different identifier, for the same delivery. Live members whose CLAIM FAILED
	// (foreign, unreadable, indeterminate) are grouped too, deliberately: an unclaimed
	// member may be owned by a CONCURRENT resumer, and if its sibling were installed as
	// an authoritative singleton, a duplicate would be resolved by exactly the guess
	// the group below refuses to make — the split-claim shape of the same hazard.
	type mqttSourceGroupMember struct {
		fr      mqttRecoveredInflightReg
		unowned bool
	}
	var poisoned []mqttRecoveredInflightReg
	poisoned = append(poisoned, quarantinedPublishConflicts...)
	bySource := make(map[string][]mqttSourceGroupMember, len(live)+len(claimFailed))
	order := make([]string, 0, len(live))
	addMember := func(fr mqttRecoveredInflightReg, unowned bool) {
		key := mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)
		if _, seen := bySource[key]; !seen {
			order = append(order, key)
		}
		bySource[key] = append(bySource[key], mqttSourceGroupMember{fr: fr, unowned: unowned})
	}
	for _, fr := range live {
		addMember(fr, false)
	}
	for _, fr := range claimFailed {
		addMember(fr, true)
	}
	for _, key := range order {
		group := bySource[key]
		unowned := 0
		for _, m := range group {
			if m.unowned {
				unowned++
			}
		}
		if unowned > 0 {
			// At least one member of this source's group is not this session's to install —
			// another owner's claim, or an affine pair its own source does not prove — so
			// nothing about the group is this session's to decide: every member is
			// reserved, none is installed, and the source gets no cpending mapping.
			// The members keep their identity in poisoned, because a same-object
			// barrier re-run may hold a LIVE entry for one of them — the apply phase
			// demotes it to the same reservation a fresh object would get.
			if len(group) > unowned {
				c.Errorf("MQTT: %d in-flight register(s) of client %q for delivery %s/%d share the source with %d "+
					"register(s) this pass may not install: all are reserved, none is installed, and new outbound "+
					"QoS 1/2 allocation is withheld for this session",
					len(group)-unowned, sess.id, group[0].fr.rec.Dur, group[0].fr.rec.Seq, unowned)
			}
			for _, m := range group {
				member := m.fr
				member.foreign = m.unowned
				poisoned = append(poisoned, member)
				opaque = append(opaque, mqttRecoveredInflightReg{
					rec: &mqttInflightRecord{PI: m.fr.rec.PI}, seq: m.fr.seq, foreign: m.unowned,
				})
			}
			continue
		}
		if len(group) == 1 {
			// THE PAIR RULE ON THE ONE REGISTER ABOUT TO BE INSTALLED AS A DELIVERY (7a). A shared
			// register is admitted only beside an exchange naming its exact acquisition, so one
			// whose exchange is missing, unreadable, or names another dependency is a shape this
			// broker does not write: its identifier is reserved and nothing acts on it — never
			// re-admitted, never given output. A non-shared register whose census exchange names an
			// acquisition is the same contradiction. Duplicate and unowned groups are already
			// reserved above, whatever their pairs say.
			if fr := group[0].fr; sess.recoveredPublishPairRefused(as, gen, fr.rec, exchanges) {
				c.Warnf("MQTT: reserving packet identifier %d for client %q: the in-flight register at sequence %d "+
					"and its exchange do not state one shared dependency, so nothing acts on the record",
					fr.rec.PI, sess.id, fr.seq)
				opaque = append(opaque, mqttRecoveredInflightReg{rec: &mqttInflightRecord{PI: fr.rec.PI}, seq: fr.seq})
				quarantinedPairs[fr.rec.PI] = true
				continue
			}
			reserve = append(reserve, group[0].fr)
			continue
		}
		// Two or more identifiers for one delivery, and NOTHING here can say which one
		// the client actually holds. A stream sequence orders the WRITES, not the sends:
		// by R1 each writer confirmed its register before transmitting, so the broker
		// whose record landed later may well have sent first. Picking the newest — as
		// this once did — can therefore restore the identifier the client never saw, and
		// then the redelivery looks like a brand new exchange. That is precisely the
		// deviation this file exists to remove, so guessing is not an option.
		//
		// So it fails CLOSED, exactly as an unreadable register does: every identifier in
		// the group is reserved so none is reused, every record is kept, no source is
		// entered in cpending, and the identifier space is marked incomplete so nothing
		// new is allocated. With no cpending entry the source's redelivery finds no
		// identifier and allocation is withheld, so it is not re-sent under a guess. The
		// state is operator-visible and is resolved by removing the superseded record.
		c.Errorf("MQTT: %d in-flight registers of client %q name the same delivery %s/%d on different packet identifiers, "+
			"so the identifier the client holds cannot be determined: all of them are reserved, none is reused, and new "+
			"outbound QoS 1/2 allocation is withheld for this session",
			len(group), sess.id, group[0].fr.rec.Dur, group[0].fr.rec.Seq)
		for _, m := range group {
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: m.fr.rec.PI},
				seq: m.fr.seq,
			})
		}
	}

	// UNIVERSAL QOS 2 CUSTODY, selected from the classifier's final OWNED set rather
	// than from the raw scan. This is the publish half of §11's positive eligibility
	// rule: cancelled, retired, consumed, poisoned and duplicate-source records never
	// reach this pass. Shared records were already moved by the claim contest above;
	// plain QoS 2 records use that same contest here so a refresh cannot overwrite a
	// newer accepted owner.
	claimedReserve := make([]mqttRecoveredInflightReg, 0, len(reserve))
	var custodyCandidates []mqttRecoveredInflightReg
	for _, rv := range reserve {
		if rv.rec != nil && rv.rec.QoS == 2 && rv.rec.State == mqttInflightPublish &&
			!rv.rec.Retired && !rv.rec.Cancelled &&
			!strings.HasPrefix(rv.rec.Dur, mqttSharedConsumerDurablePrefix) {
			custodyCandidates = append(custodyCandidates, rv)
			continue
		}
		claimedReserve = append(claimedReserve, rv)
	}
	if len(custodyCandidates) > 0 {
		var custodyClosed, custodyFailed, custodyDisplaced, custodyWithheld []mqttRecoveredInflightReg
		custodyCandidates, custodyClosed, custodyFailed, custodyDisplaced, custodyWithheld =
			sess.claimRecoveredTransferableInflight(c, custodyCandidates)
		if len(custodyDisplaced) > 0 {
			return nil, fmt.Errorf("could not take custody of %d QoS 2 in-flight slot(s) during resume — "+
				"claimed by another session owner, or the claim fence could not be established (first: pi %d, %s/%d)",
				len(custodyDisplaced), custodyDisplaced[0].rec.PI,
				custodyDisplaced[0].rec.Dur, custodyDisplaced[0].rec.Seq)
		}
		claimConsumed = append(claimConsumed, custodyClosed...)
		noteCustodyClosed(custodyClosed)
		pastPubRec = append(pastPubRec, custodyWithheld...)
		for _, fr := range custodyFailed {
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: fr.rec.PI}, seq: fr.seq, foreign: true,
			})
		}
		// Every OWNED contest result is re-judged on the body it NOW carries, and it is BOTH
		// questions, not one. This site selected its candidates on `publish` AND not-Retired AND
		// not-Cancelled, so either fact can have changed under the contest:
		//
		//   - the PHASE moved to pubrel_ready, and the body must take the past-PUBREC
		//     disposition rather than the PUBLISH one;
		//   - the RETIREMENT bit was set while the body stayed `publish`, and the body must
		//     take the retirement path, which reserves the identifier non-actionably and
		//     suppresses the source. Left in the PUBLISH population it gets a pendingPublish
		//     entry and its cpending mapping back, and a live source then re-sends a payload
		//     the client has already answered — the same retransmission by the other route.
		//
		// Retirement is NOT a substitute slot phase, so a retired `publish` body is never
		// given an invented pubrel_ready transition; it goes where a record the initial walk
		// observed retired goes. Retired-AND-Cancelled belongs here too: retirement
		// dominates cancellation (F-087 WP3), so the contest's pure-cancellation branch does
		// not consume it and it arrives as an ordinary owned body.
		custodyCandidates, adopted = mqttSplitAdoptedPastPubRec(custodyCandidates)
		adoptedPastPubRec = append(adoptedPastPubRec, adopted...)
		var custodyNewlyRetired []mqttRecoveredInflightReg
		custodyCandidates, custodyNewlyRetired = mqttSplitNewlyRetired(custodyCandidates)
		// Never re-contested: custody is already established on them at the sequence this
		// pass landed.
		retired = append(retired, custodyNewlyRetired...)
		claimedReserve = append(claimedReserve, custodyCandidates...)
	}
	reserve = claimedReserve

	// Classify every past-PUBREC slot BEFORE the in-memory apply. In particular, a
	// pubrel_ready slot is custody-contested even when a pendingPubRel entry already
	// exists: admission reads the durable slot, so its refresh set must be a provable
	// superset of the states admission can acquire from. Active actions remain localized
	// reservations, terminal/finished slots bypass custody, and excluded flag shapes get
	// no custody CAS.
	var pastPubRecFinished []mqttInflightSlotRef
	pastReservations := make([]mqttRecoveredInflightReg, 0, len(pastPubRec))
	var pastCustodyCandidates []mqttRecoveredInflightReg
	var retiredPubRelCustodyCandidates []mqttRecoveredInflightReg
	refreshRetirement := func(updated []mqttRecoveredInflightReg) {
		for _, next := range updated {
			if next.rec == nil {
				continue
			}
			for i := range retired {
				if retired[i].rec != nil && retired[i].rec.Nonce == next.rec.Nonce {
					retired[i] = next
				}
			}
		}
	}
	for _, rv := range pastPubRec {
		if rv.rec == nil {
			continue
		}
		ref := mqttSlotRefFromRecord(
			mqttInflightSubject(sess.idHash, sess.gen, rv.rec.PI), rv.seq, rv.rec)
		if ref == nil {
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: rv.rec.PI}, seq: rv.seq,
			})
			continue
		}
		sourceState := srcCache.state(rv.rec)
		sourceComplete := sourceState == mqttInflightSourceComplete
		if mqttInflightSourceMayReoffer(sourceState) {
			// The PHASE is durable evidence that the source is already past PUBREC, even
			// when a crash preceded the explicit Retired mark. Install suppression before
			// source interest opens; otherwise the source can be re-sent after PUBCOMP
			// removes its pendingPubRel guard.
			retired = append(retired, rv)
		}
		finished := rv.rec.State == mqttInflightPubRelTerminal
		if !finished && rv.rec.State == mqttInflightPubRelReady && mqttDurIsSlotOnlyClass(rv.rec.Dur) {
			// ONLY FOR A SLOT-ONLY CLASS. "No canonical PUBREL and the source acked" is an
			// ABSENCE — the source is acked at PUBREC, so it reduces to "no record" — and for an
			// ORDINARY exchange it is no proof of PUBCOMP: the PUBCOMP batch records the
			// completion with the exchange, and the canonical record's ack is withheld until it
			// does, so this broker never produces "ready slot, no record" from a PUBCOMP. A
			// record missing for another reason leaves a release owed (the exchange says so), and
			// completing it here would record a PUBCOMP the client never sent.
			_, exact := findExactPubRel(rv.rec)
			finished = !exact && sourceComplete
		}
		if !finished && rv.rec.State == mqttInflightPubRelReady && !mqttDurIsSlotOnlyClass(rv.rec.Dur) {
			// AN ORDINARY SLOT IS FINISHED ONLY ON ITS EXCHANGE'S WORD. A durably Terminal
			// exchange is positive evidence the client completed this obligation — PUBCOMP
			// records it there in the same batch that moves the slot — so a pubrel_ready slot
			// beside one is a slot whose completion this broker already recorded: the shape a
			// store that revives a limit-evicted register version produces (MAXMSGSPER-REVIVAL,
			// docs/known-upstream-issues.md). Reflecting that recorded fact into the slot writes
			// no exchange and infers nothing from an absence. PUBREC-observed and
			// terminal-pending-drain are NOT completion: a missing token proves no drain, and the
			// slot may predate the acquisition (consult 26).
			finished = sess.ordinaryExchangeEnded(rv.rec)
		}
		if finished {
			pastPubRecFinished = append(pastPubRecFinished, *ref)
			continue
		}
		if rv.rec.State == mqttInflightPubRelIntended && !rv.rec.Retired && !rv.rec.Cancelled {
			// AN OWED RECORD, and recovery is the continuation that owes it (F-098 L1,
			// holistic review round 1, finding 3). The intent landed and the store did not —
			// or the process died between them — so no PUBREL record exists for admission
			// to act on, and nothing else will ever rebuild one. Custody is contested here
			// exactly as for a ready slot, and the installed reservation is enrolled in
			// materialization below, once the apply section's lock is released.
			pastCustodyCandidates = append(pastCustodyCandidates, rv)
			continue
		}
		if rv.rec.State == mqttInflightPubRelReady {
			switch {
			case rv.rec.Retired:
				_, exact := findExactPubRel(rv.rec)
				if !exact {
					break
				}
				// Retired is normally excluded from custody transfer. A PRESENT exact
				// PUBREL is different: the client is still waiting for that protocol
				// step, admission will inspect the slot, and leaving predecessor custody
				// here would refuse it forever after restart.
				retiredPubRelCustodyCandidates = append(retiredPubRelCustodyCandidates, rv)
				continue
			case !rv.rec.Retired && !rv.rec.Cancelled:
				pastCustodyCandidates = append(pastCustodyCandidates, rv)
				continue
			}
		}
		pastReservations = append(pastReservations, rv)
	}
	// THE ADOPTED PAST-PUBREC BODIES, joined here and not through the loop above.
	//
	// Custody is already established on every one of them — this object's own claim, at the
	// sequence the contest landed — so they are not custody candidates and re-contesting
	// would rewrite the slot and move that sequence for nothing.
	//
	// They are also not eligible for the completion verdict the loop above computes. That
	// verdict reads `rels`, and `rels` was scanned before the contest adopted these bodies:
	// an absence in it predates the phase it would be used to rule on, which is the
	// ordering F-099 exists to stop. A body whose record this pass cannot ask about is OWED.
	// The next resume observes it as pubrel_ready in its own INITIAL register load, where
	// the scan is newer than the observation and the verdict is legitimate — so a genuinely
	// finished exchange is reaped there, provided its source proves complete.
	for _, rv := range adoptedPastPubRec {
		if rv.rec == nil {
			continue
		}
		if mqttInflightSourceMayReoffer(srcCache.state(rv.rec)) {
			// The PHASE proves the source is past PUBREC, so suppression is owed exactly as
			// it is for a slot the initial walk observed past PUBREC. Predicated on
			// may-re-offer and not on !complete: a LOST source can never be re-offered and
			// suppressing it would claim a fact about a delivery that cannot recur.
			retired = append(retired, rv)
		}
		if rv.rec.State == mqttInflightPubRelTerminal {
			// Terminal is positive authority this broker durably wrote, so it needs no
			// evidence from the scan and finalization is already legal from here.
			if ref := mqttSlotRefFromRecord(
				mqttInflightSubject(sess.idHash, sess.gen, rv.rec.PI), rv.seq, rv.rec); ref != nil {
				pastPubRecFinished = append(pastPubRecFinished, *ref)
				continue
			}
		}
		pastReservations = append(pastReservations, rv)
	}
	if len(retiredPubRelCustodyCandidates) > 0 {
		var custodyClosed, custodyFailed, custodyDisplaced, custodyWithheld []mqttRecoveredInflightReg
		retiredPubRelCustodyCandidates, custodyClosed, custodyFailed, custodyDisplaced, custodyWithheld =
			sess.claimRecoveredRetiredPubRelInflight(c, retiredPubRelCustodyCandidates)
		if len(custodyDisplaced) > 0 {
			return nil, fmt.Errorf("could not take custody of %d retired past-PUBREC slot(s) during resume — "+
				"claimed by another session owner, or the claim fence could not be established (first: pi %d, %s/%d)",
				len(custodyDisplaced), custodyDisplaced[0].rec.PI,
				custodyDisplaced[0].rec.Dur, custodyDisplaced[0].rec.Seq)
		}
		claimConsumed = append(claimConsumed, custodyClosed...)
		noteCustodyClosed(custodyClosed)
		for _, fr := range custodyFailed {
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: fr.rec.PI}, seq: fr.seq, foreign: true,
			})
		}
		refreshRetirement(retiredPubRelCustodyCandidates)
		for _, fr := range retiredPubRelCustodyCandidates {
			if fr.rec == nil || !fr.rec.Retired {
				pastReservations = append(pastReservations, fr)
			}
		}
		refreshRetirement(custodyWithheld)
		for _, fr := range custodyWithheld {
			if fr.rec != nil && fr.rec.State == mqttInflightPubRelTerminal {
				if ref := mqttSlotRefFromRecord(
					mqttInflightSubject(sess.idHash, sess.gen, fr.rec.PI), fr.seq, fr.rec); ref != nil {
					pastPubRecFinished = append(pastPubRecFinished, *ref)
				}
				continue
			}
			pastReservations = append(pastReservations, fr)
		}
	}
	if len(pastCustodyCandidates) > 0 {
		var custodyClosed, custodyFailed, custodyDisplaced, custodyWithheld []mqttRecoveredInflightReg
		pastCustodyCandidates, custodyClosed, custodyFailed, custodyDisplaced, custodyWithheld =
			sess.claimRecoveredTransferableInflight(c, pastCustodyCandidates)
		if len(custodyDisplaced) > 0 {
			return nil, fmt.Errorf("could not take custody of %d past-PUBREC slot(s) during resume — "+
				"claimed by another session owner, or the claim fence could not be established (first: pi %d, %s/%d)",
				len(custodyDisplaced), custodyDisplaced[0].rec.PI,
				custodyDisplaced[0].rec.Dur, custodyDisplaced[0].rec.Seq)
		}
		for _, fr := range custodyFailed {
			opaque = append(opaque, mqttRecoveredInflightReg{
				rec: &mqttInflightRecord{PI: fr.rec.PI}, seq: fr.seq, foreign: true,
			})
		}
		refreshRetirement(pastCustodyCandidates)
		refreshRetirement(custodyWithheld)
		// A concurrent exact delete is a completed exchange; it needs no reservation.
		noteCustodyClosed(custodyClosed)
		for _, fr := range custodyWithheld {
			if fr.rec != nil && fr.rec.State == mqttInflightPubRelTerminal {
				if ref := mqttSlotRefFromRecord(
					mqttInflightSubject(sess.idHash, sess.gen, fr.rec.PI), fr.seq, fr.rec); ref != nil {
					pastPubRecFinished = append(pastPubRecFinished, *ref)
				}
				continue
			}
			pastReservations = append(pastReservations, fr)
		}
		pastReservations = append(pastReservations, pastCustodyCandidates...)
	}
	pastPubRec = pastReservations
	if len(closedPubRelOwners) > 0 {
		kept := rels[:0]
		for _, rel := range rels {
			if nonce, closed := closedPubRelOwners[rel.pi]; closed &&
				rel.owner.State == mqttPubRelOwnerValid && rel.owner.Nonce == nonce {
				continue
			}
			kept = append(kept, rel)
		}
		rels = kept
	}

	// (3b) THE MARKER PHASE: derive every live PUBLISH-phase register's send state from
	// its unsent marker (inflight_marker.go). The register enumeration ran FIRST and this
	// runs second, never the other way round: a marker read before its register would
	// let a pair committed in between look like a transmitted register (N-5.6).
	//
	// One bounded enumeration of this incarnation's markers, loaded in batches like the
	// registers. A marker the walk did not return is authoritatively absent by the same
	// standard the register walk's absence is settled on (observedPI): the filter is
	// bounded by the 16-bit identifier, so the response is complete by construction.
	markers := make(map[string]mqttMarkerLoad)
	markerSubjects, merr := as.enumerateInflightSubjects(mqttInflightMarkerWalkFilter(idHash, gen))
	if merr != nil {
		sess.setPISpaceIncomplete(c, fmt.Sprintf("the unsent markers could not be enumerated: %v", merr))
		return nil, nil
	}
	markerNames := make([]string, 0, len(markerSubjects))
	for subject := range markerSubjects {
		markerNames = append(markerNames, subject)
	}
	sort.Strings(markerNames)
	for start := 0; start < len(markerNames); start += mqttInflightLoadBatch {
		end := min(start+mqttInflightLoadBatch, len(markerNames))
		batch := markerNames[start:end]
		resps, berr := as.loadInflightRecordsMulti(batch)
		if berr != nil || len(resps) != len(batch) {
			sess.setPISpaceIncomplete(c, fmt.Sprintf("the unsent markers could not be loaded: %v (%d of %d answered)", berr, len(resps), len(batch)))
			return nil, nil
		}
		for i, resp := range resps {
			regName, perr := mqttParseInflightMarkerSubject(batch[i])
			if perr != nil {
				continue // the filter cannot return this; nothing it could name is ours
			}
			load := mqttMarkerLoadFromResponse(resp)
			if load.kind == mqttInflightResponseIndeterminate {
				// One marker that cannot be read is one identifier whose transmission state
				// is unknown; the whole space is withheld rather than guessed (row 4a for
				// the record, but the pass cannot even tell WHICH record).
				sess.setPISpaceIncomplete(c, fmt.Sprintf("the unsent marker %q could not be read", batch[i]))
				return nil, nil
			}
			markers[regName.subject] = load
		}
	}
	markerFor := func(subject string) mqttMarkerLoad {
		if l, ok := markers[subject]; ok {
			return l
		}
		return mqttMarkerLoad{kind: mqttInflightResponseAbsent}
	}
	// THE TWO-WALK CASE (N-5.6, T21a): a marker whose register the snapshot did not see.
	// The sibling is loaded authoritatively: present means a pair committed between the
	// walks, and the snapshot is stale — reclassify; absent means an orphan marker, which
	// reserves nothing and is left to the sweep (N-9.6); unresolved fails closed.
	for subject := range markers {
		name, perr := mqttParseInflightSubject(subject)
		if perr != nil || observedPI[name.pi] {
			continue
		}
		_, smsg, lerr := as.loadInflightRecord(subject)
		switch {
		case lerr != nil:
			sess.setPISpaceIncomplete(c, fmt.Sprintf("the register beside the unsent marker on %q could not be read", subject))
			return nil, nil
		case smsg != nil:
			return nil, errInflightReclassify
		}
	}
	// Rows 4 and 4a against the walk snapshot for a present marker; for an absent one the
	// register's OWN subject is reread, batched, and the reread REPLACES the snapshot as
	// the register observation rows 5-9 decide on. Marker absence is never interpreted
	// against the cached snapshot (M46).
	rereadIdx := make([]int, 0, len(reserve))
	rereadSubjects := make([]string, 0, len(reserve))
	withheld := make([]mqttRecoveredInflightReg, 0)
	for i := range reserve {
		rv := &reserve[i]
		subject := mqttInflightSubject(idHash, gen, rv.rec.PI)
		mk := markerFor(subject).observe(mqttMarkerReloaded, rv.rec.Nonce)
		if mk.outcome == mqttMarkerAbsent {
			rereadIdx = append(rereadIdx, i)
			rereadSubjects = append(rereadSubjects, subject)
			continue
		}
		v, verr := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpDeriveSendState, caller: mqttMarkerCallerResume, resume: true},
			mqttRegisterObservation{kind: mqttRegisterSnapshot, seq: rv.seq}, mk)
		switch {
		case verr != nil:
			c.Errorf("MQTT: deriving the send state of packet identifier %d for client %q: %v", rv.rec.PI, sess.id, verr)
			rv.send = mqttRecoveredSend{decided: true, opaque: true}
		case v.disposition == mqttMarkerInstallUnsent && mqttRecoveredOutputAuthority(rv.rec):
			// A STANDING MARKER BESIDE OUTSTANDING OUTPUT AUTHORITY IS NOT PROOF OF NON-TRANSMISSION
			// (F-088). An ordinary acquisition commits under the acquiring connection's accepted
			// entitlement, atomically with its register token, so every token under which another
			// holder — a displaced tenure, or this object's own live section — may still record
			// intent and write is visible here as ActionToken. Such a holder can delete this marker
			// and put the PUBLISH on the wire after this read. The register is therefore POSSIBLY
			// ATTEMPTED; and the marker is KEPT as deletion debt, because a holder that drains
			// without intent leaves it standing, and a later transmission of ours that did not
			// confirm its delete first would leave a standing marker over a real PUBLISH.
			rv.send = mqttRecoveredSend{decided: true, transmitted: true, markerSeq: mk.seq}
		case v.disposition == mqttMarkerInstallUnsent:
			rv.send = mqttRecoveredSend{decided: true, markerSeq: mk.seq}
		default:
			c.Warnf("MQTT: reserving packet identifier %d for client %q marker-opaque: the unsent marker on %q does not name its register",
				rv.rec.PI, sess.id, subject)
			rv.send = mqttRecoveredSend{decided: true, opaque: true}
		}
	}
	for start := 0; start < len(rereadSubjects); start += mqttInflightLoadBatch {
		end := min(start+mqttInflightLoadBatch, len(rereadSubjects))
		resps, berr := as.loadInflightRecordsMulti(rereadSubjects[start:end])
		if berr != nil || len(resps) != end-start {
			sess.setPISpaceIncomplete(c, fmt.Sprintf("the in-flight registers could not be reread: %v (%d of %d answered)", berr, len(resps), end-start))
			return nil, nil
		}
		for j, resp := range resps {
			rv := &reserve[rereadIdx[start+j]]
			subject := rereadSubjects[start+j]
			var reg mqttRegisterObservation
			switch mqttClassifyMsgGetResponse(resp) {
			case mqttInflightResponseAbsent:
				reg = mqttRegisterObservation{kind: mqttRegisterAbsent}
			case mqttInflightResponseIndeterminate:
				reg = mqttRegisterObservation{kind: mqttRegisterIndeterminate}
			default:
				cur, derr := mqttDecodeInflightRecord(resp.Message.Data)
				switch {
				case derr != nil || !mqttInflightRecordMatchesSubject(cur, subject):
					reg = mqttRegisterObservation{kind: mqttRegisterUnreadable, seq: resp.Message.Sequence}
				case cur.Nonce == rv.rec.Nonce:
					reg = mqttRegisterObservation{kind: mqttRegisterSameExchange, seq: resp.Message.Sequence, snapshotSeq: rv.seq}
				default:
					reg = mqttRegisterObservation{kind: mqttRegisterDifferentExchange, seq: resp.Message.Sequence}
				}
			}
			v, verr := mqttDecideMarker(mqttMarkerContext{op: mqttMarkerOpDeriveSendState, caller: mqttMarkerCallerResume, resume: true},
				reg, mqttMarkerObservation{prov: mqttMarkerReloaded, outcome: mqttMarkerAbsent})
			switch {
			case verr != nil:
				c.Errorf("MQTT: deriving the send state of packet identifier %d for client %q: %v", rv.rec.PI, sess.id, verr)
				rv.send = mqttRecoveredSend{decided: true, opaque: true}
			case v.disposition == mqttMarkerInstallTransmitted:
				rv.send = mqttRecoveredSend{decided: true, transmitted: true}
			case v.disposition == mqttMarkerReclassify:
				return nil, errInflightReclassify
			default:
				c.Warnf("MQTT: reserving packet identifier %d for client %q marker-opaque: its unsent marker is absent and the register on %q could not be reread",
					rv.rec.PI, sess.id, subject)
				rv.send = mqttRecoveredSend{decided: true, opaque: true}
			}
		}
	}
	// A Withhold leaves the ordinary install: the identifier is reserved marker-opaque
	// with no source mapping, counted with the unreadable registers so the identifier
	// space is marked incomplete (fail closed, N-9.5).
	kept := reserve[:0]
	as.mu.RLock()
	cancellationSeam := as.testRecoverCancellation
	recoveryDropSeam := as.testRecoverDropCondemn
	as.mu.RUnlock()
	for _, rv := range reserve {
		if rv.send.opaque {
			withheld = append(withheld, rv)
			continue
		}
		if cancellationSeam != nil && rv.send.decided && !rv.rec.Retired &&
			(rv.rec.Cancelled || cancelledNonces[rv.rec.Nonce]) {
			cancellationSeam(rv.rec.PI, rv.rec.Nonce, rv.rec.Cancelled, cancelledNonces[rv.rec.Nonce], rv.send.markerSeq)
		}
		// A GENUINE CANCELLATION: a cancelled register whose unsent marker for THIS allocation
		// stands was never transmitted, so the cancellation's own disposition applies (the door
		// it is routed through withdraws the obligation on that same marker). A cancelled
		// register whose marker is absent is NOT here: it is possibly-attempted and continues as
		// any such register does (F-088). Nor is one whose marker stands beside a recovered output
		// token: a holder may yet write under it, so it is possibly attempted too, and the
		// cancellation decided over it cannot have proved otherwise.
		if mqttRecoveredProvenUnsent(rv) && !rv.rec.Retired &&
			(rv.rec.Cancelled || cancelledNonces[rv.rec.Nonce]) {
			subject := mqttInflightSubject(idHash, gen, rv.rec.PI)
			ref := mqttSlotRefFromRecord(subject, rv.seq, rv.rec)
			if cancelledNonces[rv.rec.Nonce] {
				// The verdict set names this register, so its delete must be PROVEN before
				// the nonce is retired (the drop-by-nonce list retires it only against that
				// confirmed deletion).
				dropByNonce = append(dropByNonce, cancelledDrop{rv.seq, rv.rec.Nonce, ref, rv.rec.Dur, rv.rec.Seq})
			} else {
				drop = append(drop, regDrop{
					seq: rv.seq, nonce: rv.rec.Nonce, ref: ref,
					disp: mqttSlotDeleteComplete,
					dur:  rv.rec.Dur, sseq: rv.rec.Seq,
				})
			}
			continue
		}
		if rv.srcRouted != mqttSourceNotRouted {
			if mqttRecoveredProvenUnsent(rv) {
				// PROVEN-UNSENT over a source nothing can re-offer: the marker is the
				// license here, and it is positive evidence this pass read rather than an
				// inference from absence. A confirmed marker delete strictly precedes the
				// write, so a marker still naming this register means the PUBLISH had not
				// gone out; the client holds nothing under this identifier, and identifier
				// continuity is owed only to an exchange the client may be holding. A
				// marker standing beside a recovered output token licenses nothing: its
				// holder may still write, so that register falls through to the
				// possibly-attempted reservation below with its marker kept as debt.
				//
				// The classification answers the weaker question — will anything ever
				// re-offer this delivery — and a wrong answer to it costs nothing HERE for
				// an exchange that stays never-transmitted: a later re-offer allocates a
				// fresh identifier with DUP=0, which is what a message that never went out
				// is owed. It does NOT confer exclusive authority over a concurrent
				// sender; that fence does not exist within a generation (F-098), and this
				// arm neither claims nor needs it.
				//
				// So the pair is consumed as the lost-delete register it is, dropped below
				// and its live entry reconciled in the apply phase. NEVER-TRANSMITTED is
				// the disposition rather than complete: the license is the marker, the
				// source may merely be gone rather than finished, and a same-exchange
				// rewrite must therefore leave this register AND its marker alone.
				drop = append(drop, regDrop{
					seq: rv.seq, nonce: rv.rec.Nonce,
					ref:  mqttSlotRefFromRecord(mqttInflightSubject(idHash, gen, rv.rec.PI), rv.seq, rv.rec),
					disp: mqttSlotDeleteNeverTransmitted,
					dur:  rv.rec.Dur, sseq: rv.rec.Seq,
				})
				sourceConsumed = append(sourceConsumed, rv)
				continue
			}
			// POSSIBLY-ATTEMPTED over a source nothing can re-offer: the payload can never
			// come round again, and the receiver may hold this identifier. It is reserved,
			// installed transmitted, until the client's own acknowledgement or the
			// session's end releases it. Whether it also keeps a source mapping depends on
			// WHICH routed state this is, and the apply phase decides that: a Complete
			// source gets none, a Lost one keeps it.
			c.Warnf("MQTT: reserving packet identifier %d for client %q: its PUBLISH may have been transmitted and its source %s/%d is gone, so it is held until the client acknowledges it or the session ends",
				rv.rec.PI, sess.id, rv.rec.Dur, rv.rec.Seq)
		}
		kept = append(kept, rv)
	}
	reserve = kept
	opaque = append(opaque, withheld...)

	// Lease only displaced ordinary owners in this completed classification.
	// A busy/unreadable candidate makes this apply incomplete, not a local
	// non-landing inference. No callback or timer enters this preparation.
	var disposalHandoffs []*mqttDisposal
	if !censusIncomplete && len(opaque) == 0 {
		for _, rv := range reserve {
			w, needed := sess.prepareDisposalRecovery(c, rv, walkID)
			if w != nil {
				disposalHandoffs = append(disposalHandoffs, w)
			} else if needed {
				applyIncomplete = true
			}
		}
	}
	defer sess.releaseDisposalRecoveryLeases(disposalHandoffs)

	// The second durable read, as late as the walk can take it (F-105 7b). A read is not a lock:
	// a takeover after it cannot be kept from racing the local install, but it cannot make any
	// owner-dependent write this walk performs land (each carries this connection's entitlement),
	// and the takeover displaces this connection, whose teardown discards what it installed.
	if derr := sess.resumeEntitlementServes(c); derr != nil {
		return nil, derr
	}

	// (4) Apply. Everything below is memory-only and takes the session lock once.
	// Counters for the log line only. Nothing here governs a decision — in particular
	// the piSpaceIncomplete condition below reads len(opaque), what the walk FOUND,
	// never installedOpaque, what this pass happened to install.
	var restoredPubs, restoredRels, installedOpaque, skipped int
	sess.mu.Lock()
	// THE LOCAL BINDING, under the lock apply holds (F-105 7b): a same-object rebind replaces
	// sess.c before its own accept and boundEnt at its accept, so a walk whose connection is no
	// longer the bound one, or whose entitlement is no longer the object's accepted one, installs
	// nothing.
	if ent, _ := c.outboundEntitlement(); c != nil &&
		((sess.c != nil && sess.c != c) || (sess.boundEntOK && ent != sess.boundEnt)) {
		sess.mu.Unlock()
		return nil, fmt.Errorf("the resume of %q was displaced by a newer connection on the same session "+
			"object before it could install its in-flight state", sess.id)
	}
	sess.applyDrainRegisterStateLocked(drainObs)
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.pendingPubRel == nil {
		sess.pendingPubRel = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	// Quarantine also demotes a reused object's resident scheduling entries.
	// The opaque PUBLISH-side reservation is intentional: the existing single
	// source-restriction API must see it even if a PUBREL entry is resident.
	for _, rv := range opaque {
		pi := rv.rec.PI
		if !quarantinedPairs[pi] {
			continue
		}
		if cur := sess.pendingPublish[pi]; cur != nil {
			sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectExactly(cur), mqttNewDemoted(cur))
		} else {
			sess.setPendingLocked(mqttPendingPublishMap, pi, mqttExpectEmpty(), mqttRecoveryOpaquePending(rv.seq))
		}
		if cur := sess.pendingPubRel[pi]; cur != nil {
			sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectExactly(cur), mqttNewDemoted(cur))
		}
	}
	// Negative claim dispositions reconcile a REUSED object's memory FIRST. "In-memory
	// state wins" (the installs below) is the right rule for records this pass would
	// re-install — but a consumed close decision and a poisoned group are the opposite
	// case: durable facts saying the in-memory entry must NOT stand. A same-object
	// barrier re-run that skipped them would keep delivering under a mapping the
	// arbitration just refused to trust — redelivering a source the close durably ended,
	// or sending under one identifier of a pair whose owner cannot be determined. Every
	// touch is guarded by the record's own nonce, so an entry a newer allocation owns is
	// never disturbed.
	// THE OCCUPANCY OBLIGATION, CARRIED INDEPENDENTLY OF ANY RESIDENT ENTRY (F-098).
	//
	// These registers are PRESERVED with their source consumed, so nothing below will
	// delete them and nothing else reserves their identifiers. The install is here, above
	// the reconciliation loop, precisely because that loop can only act through a matching
	// `pendingPublish` entry — and a cold resume has none, which is how the durable record
	// came to survive with its identifier allocatable. Recovery runs before restored source
	// interest and before a successful CONNACK, so this lands before allocation opens.
	//
	// Bound by NONCE, so the owning exchange can adopt it back if it is recovered, and so a
	// later allocation on this identifier cannot inherit it. Idempotent: the bind set
	// deduplicates, so a repeated recovery pass re-asserts the same restriction rather than
	// accumulating one per pass.
	for _, fr := range occupancyOwed {
		sess.excludeIdentifierLocked(fr.rec.PI,
			mqttInflightSubject(sess.idHash, sess.gen, fr.rec.PI), fr.rec.Nonce)
	}
	// AND EVERY DROP CANDIDATE, on the same terms (holistic review round 1, finding 1).
	//
	// A staged drop is not a completed one. The drop loops run AFTER this apply phase and
	// each delete can be REFUSED by the freshness fence — a peer rewrote the slot or
	// acquired an action on it — or can simply fail. Recovery then finishes with the
	// durable register still standing, and on a COLD resume there is no resident entry
	// anywhere to carry the obligation, so `bumpPI` and `trackPublishRetained` would hand
	// that identifier straight out while a peer may still be transmitting under it.
	//
	// The exclusion is therefore installed for the WHOLE candidate population before
	// allocation opens, not only for the retired records above, and it is the drop loop's
	// CONFIRMED ABSENCE that lifts it again. Installing optimistically and lifting on proof
	// is the only ordering that is safe in both directions: the opposite — installing only
	// where the delete failed — would have to observe the failure before it could react,
	// and allocation does not wait for that observation.
	for _, rd := range drop {
		if rd.ref != nil {
			sess.excludeIdentifierLocked(rd.ref.name.pi, rd.ref.name.subject, rd.nonce)
			sess.handOffRecoveryPublishLocked(*rd.ref)
		}
	}
	for _, cd := range dropByNonce {
		if cd.ref != nil {
			sess.excludeIdentifierLocked(cd.ref.name.pi, cd.ref.name.subject, cd.nonce)
			sess.handOffRecoveryPublishLocked(*cd.ref)
		}
	}
	for _, fr := range append(claimConsumed, sourceConsumed...) {
		// A CANCELLED reservation is exempt: it is held until its store settles AND a
		// rung is proven, and its nonce deliberately matches the tombstone that names
		// it — releasing it here would free an identifier whose CAS may still be on
		// the wire.
		if ack := sess.pendingPublish[fr.rec.PI]; ack != nil && ack.regNonce == fr.rec.Nonce && !ack.regCancelled {
			// An entry whose transmission has not returned is CONDEMNED rather than
			// deleted (F-087 WP5, class F): every durable delete this pass owes is issued
			// or declined below, so endSend's continuation is the memory untrack alone.
			// Below, not already — the drop loop runs after this apply — and a condemned
			// entry keeps its Receive Maximum charge until endSendWith settles it, which
			// is why a release here is not the same as capacity coming back.
			sendNonce := ack.sendNonce
			parked := sess.condemnSendLocked(fr.rec.PI, mqttSendSettleRecoveryDrop)
			if recoveryDropSeam != nil {
				recoveryDropSeam(fr.rec.PI, sendNonce, parked)
			}
			if !parked {
				// The staged durable drop runs AFTER this apply phase and can be refused by
				// the freshness fence, fail, or be skipped when an earlier unresolved
				// terminal advance ends the pass. The entry goes either way, so the
				// identifier's exclusion is handed off here (F-098).
				sess.excludeIdentifierLocked(fr.rec.PI,
					mqttInflightSubject(sess.idHash, sess.gen, fr.rec.PI), fr.rec.Nonce)
				delete(sess.pendingPublish, fr.rec.PI)
			}
		}
		if m := sess.cpending[fr.rec.Dur]; m != nil && m[fr.rec.Seq] == fr.rec.PI {
			delete(m, fr.rec.Seq)
		}
		key := mqttDeferredKey{fr.rec.Dur, fr.rec.Seq}
		if rs := sess.srcAckOwed[key]; rs != nil && rs.nonce == fr.rec.Nonce {
			delete(sess.srcAckOwed, key)
		}
	}
	for _, fr := range poisoned {
		if m := sess.cpending[fr.rec.Dur]; m != nil && m[fr.rec.Seq] == fr.rec.PI {
			delete(m, fr.rec.Seq)
		}
		if ack := sess.pendingPublish[fr.rec.PI]; ack != nil && ack.regNonce == fr.rec.Nonce {
			// Demoted, not deleted: the identifier stays RESERVED (the client may hold
			// it), it just no longer names a source this session may deliver under. For
			// a member whose claim was LOST, the sequence is the rival's — regForeign
			// keeps every terminal path off it.
			//
			// The replacement CARRIES FORWARD an active send section and any settle it
			// parked, so the section's edge survives the demote (F-087 WP5, class F):
			// endSend is token-guarded and would otherwise find an entry it cannot match,
			// leaving the identifier held until the write returned and then dropped
			// without its continuation.
			sess.setPendingLocked(mqttPendingPublishMap, fr.rec.PI, mqttExpectExactly(ack), mqttNewPoisoned(fr.seq, ack))
		}
	}
	for _, fr := range retired {
		// Idempotent, and it must be: a same-broker reconnect re-runs this while the
		// in-memory retirement is still in place. durable reflects the RECORD: a register
		// already marked needs no further write, while one inferred from a surviving PUBREL
		// still owes its mark, which secureRetiredSource performs below.
		sess.retireSourceLocked(fr.rec.Dur, fr.rec.Seq, &mqttRetiredSource{
			pi:         fr.rec.PI,
			regSeq:     fr.seq,
			nonce:      fr.rec.Nonce,
			q2AffClaim: fr.rec.Q2AffClaim,
			q2AffCopy:  fr.rec.Q2AffCopy,
			durable:    fr.rec.Retired,
		})
	}
	reserveSource := func(dur string, sseq uint64, pi uint16) {
		if mqttRefuseSourcelessAtOrdinarySink(dur, sseq) {
			return
		}
		sseqToPi := sess.cpending[dur]
		if sseqToPi == nil {
			sseqToPi = make(map[uint64]uint16)
			sess.cpending[dur] = sseqToPi
		}
		sseqToPi[sseq] = pi
	}
	// The install body for a recovered PUBREL, called by the ordinary restore loop below.
	// The correlated cpending restore is what
	// keeps a real source re-offer from allocating a second identifier (trackPublish
	// reaches pendingPubRel only through the mapping), and a filled in-memory entry must
	// be PRESERVED rather than overwritten by the placeholder.
	installPubRelLocked := func(r mqttStoredPubRel) {
		cur := sess.pendingPubRel[r.pi]
		if cur != nil && cur.jsAckSubject != _EMPTY_ {
			return // a filled entry is the live authority; the placeholder is not
		}
		// The placeholder shape trackAsPubRel(pi, _EMPTY_) writes: the ack subject is
		// filled in by the PUBREL delivery, and mqttDeliverPubRelCb's duplicate guard
		// already tolerates an empty one.
		//
		// regNonce carries the exchange identity even before delivery fills the ack subject.
		// Without it a finished-slot cleanup could delete the durable slot while this
		// nonce-less placeholder lingered forever — installTerminalReservationLocked and
		// releaseTerminalReservation are both nonce-keyed, so neither could adopt or remove
		// it, leaking the identifier and its Receive Maximum charge (the F-093 crash cut a
		// holistic round caught). With the nonce, the terminal path adopts and releases it.
		if !sess.setPendingLocked(mqttPendingPubRelMap, r.pi, mqttExpectCaptured(cur),
			mqttNewPubRelPlaceholder(r.srcDur, r.srcSeq, r.owner.Nonce)) {
			return
		}
		if r.srcDur != _EMPTY_ {
			// Reserve the SOURCE too, so a redelivery of it hits the usedForPubRel
			// guard instead of re-sending a payload the client has already PUBREC'd.
			reserveSource(r.srcDur, r.srcSeq, r.pi)
		}
	}
	for _, r := range rels {
		if r.pi == 0 {
			continue
		}
		if _, live := sess.pendingPubRel[r.pi]; live {
			skipped++
			continue // in-memory state wins
		}
		if _, asPub := sess.pendingPublish[r.pi]; asPub {
			// Can only happen on a same-process reconnect, where the entry is the live
			// authority: this record is then the one the PUBREC path stored moments ago
			// and the callback will track it itself.
			skipped++
			continue
		}
		installPubRelLocked(r)
		restoredRels++
	}
	// Terminal cleanup runs after dropping the lock and performs several storage round
	// trips. Reserve those identifiers now so an outbound callback cannot re-mint one in
	// the interval before its confirmed slot deletion.
	for i := range pastPubRecFinished {
		ref := pastPubRecFinished[i]
		if sess.installTerminalReservationLocked(ref) {
			sess.handOffRecoveryPublishLocked(ref)
		}
	}
	for _, rv := range reserve {
		if ack, live := sess.pendingPublish[rv.rec.PI]; live &&
			!(ack.markerOpaque && ack.regNonce == rv.rec.Nonce) &&
			!mqttRecoveryReplaceable(ack, rv.rec) {
			// In-memory state wins except for this exchange's marker-opaque
			// reservation (the N-7.2 licence, nonce-only and never narrowed) or a
			// validated recovery reservation, which this pass's disposition exists to
			// replace: it falls through to the install below, and its source
			// mapping is rebuilt with it. Nonce-guarded, so a reservation a newer
			// allocation owns is untouched.
			//
			// The register's SEQUENCE is the stream's to
			// say: the claim rewrite moved it, and a live entry left holding the old
			// one would aim every later exact-sequence operation at a message that no
			// longer exists. Nonce-guarded, so a newer allocation is untouched.
			// LINEAGE-aware, not base-conditioned (F-087 WP4): the operation may have
			// re-targeted through several sequences, so an entry sitting at any link in
			// the chain is at-or-behind a state this pass moved through and is advanced
			// to its end. A sequence OUTSIDE the chain belongs to a writer this pass
			// never saw, and its fact stands. A record that was never contested carries a
			// one-element chain, which makes this exactly "unchanged since the walk".
			if ack != nil && ack.regNonce == rv.rec.Nonce && !ack.regCancelled && ack.regSeq != 0 &&
				rv.traversedContains(ack.regSeq) {
				ack.regSeq = rv.seq
				// The WHOLE reference advances, not just the sequence. Advancing regSeq
				// alone left ack.slot.seq at the pre-rewrite value, and the deletion's
				// freshness fence then read that as "someone else rewrote this" and
				// preserved the record forever — the identifier would never be released.
				// This branch IS the rewrite, so its own snapshot moves with it.
				ack.slot.seq = rv.seq
				ack.slot.state = rv.rec.State
				ack.slot.claim, ack.slot.claimSeq = rv.rec.Claim, rv.rec.ClaimSeq
			}
			// THE RESIDENT KEEPS ITS PLACE, NOT AN OBSOLETE CLASSIFICATION (F-088). Three
			// independent steps, none nested under the sequence advance above, because neither
			// depends on the register having moved:
			//   (i)   only this allocation's own ordinary acting entry, held under custody for a
			//         lineage that has not ended, is eligible; every other resident is untouched;
			//   (ii)  a possibly-attempted classification makes it possibly attempted — monotone,
			//         the marker debt and any live section's captured DUP and marker kept;
			//   (iii) the restoration supersedes this allocation's cancellation verdict, even when
			//         the entry already read transmitted (a verdict a fork repair resurrected, or
			//         one an earlier handoff left).
			// Safe when the token is this object's OWN live section's: that section keeps the DUP
			// and marker it captured, so the bit changes no write, skips no intent delete and
			// releases no reservation; a section that then aborts without intent leaves a
			// conservative possible attempt, the accepted cost.
			if sess.recoveredResidentEligibleLocked(ack, rv) && rv.send.decided && rv.send.transmitted && !rv.send.opaque {
				if !ack.transmitted {
					ack.transmitted = true
				}
				sess.retireSupersededVerdictLocked(rv.rec.Nonce)
			}
			skipped++
			continue
		}
		if _, asRel := sess.pendingPubRel[rv.rec.PI]; asRel {
			// The identifier is owned by a PUBREL record for a DIFFERENT source. The
			// PUBREL wins (R2) and this register is stale; it cannot be dropped from
			// here, so it is left for a later pass rather than reserved twice.
			skipped++
			continue
		}
		// The resident that fell through the test above — this exchange's marker-opaque or
		// validated recovery reservation, which this pass replaces — or none. Captured HERE,
		// where eligibility was decided, and the install below replaces exactly it.
		resident := sess.pendingPublish[rv.rec.PI]
		// Through the one constructor (mqttRecoveredPending), which installs the captured
		// slot reference a later terminal path needs to address the subject.
		restored := mqttRecoveredPending(sess.idHash, sess.gen, rv.rec.PI, rv)
		if restored == nil {
			// Refused by the constructor's boundary (a terminal slot). Reserve the identifier
			// rather than install an entry that could retransmit.
			sess.setPendingLocked(mqttPendingPublishMap, rv.rec.PI, mqttExpectCaptured(resident), mqttRecoveryOpaquePending(rv.seq))
			skipped++
			continue
		}
		if sess.recoveryRestoreRefusedLocked(census, rv.rec, restored) {
			// Refused by the custody gate (F-105 7b): the ledger does not hold this lineage and
			// the inventory decided it (an unreached register was already reserved by the
			// unpaired refusal; an undecided one was held provisionally). Nothing is installed:
			// the identifier is the ledger's if it holds it and nobody's if not, and an ended
			// projection never becomes an opaque reservation.
			skipped++
			continue
		}
		// ADOPT THIS EXCHANGE'S OWN EXCLUSION, atomically with the install and under this
		// same lock. A withdrawal or an ambiguous abandonment excluded the identifier while
		// its durable PUBLISH survived; without this the restored entry would be refused its
		// own transmission by beginSendLocked, since sendReservedLocked reports the
		// exclusion — and for an unsent exchange no client acknowledgement can ever unblock
		// it.
		//
		// Only a UNANIMOUS, non-empty, matching identity adopts. A different nonce, or any
		// nonce-less bind, is somebody else's reservation: it is kept, and this entry lives
		// behind it rather than transmitting through a stranger's exclusion. Nonce equality
		// transfers LOCAL ALLOCATION CUSTODY only — acquirePublishAction still applies its
		// own nonce, phase and action checks and its CAS fence.
		if !sess.setPendingLocked(mqttPendingPublishMap, rv.rec.PI, mqttExpectCaptured(resident), restored) {
			skipped++
			continue
		}
		// HANDOFF (F-088 WP2): the restored ACTING entry now owns this allocation's
		// acknowledgement, so an AckRoute naming the same allocation retires — the two never both
		// route it.
		if b := sess.abandonedAckOfAllocationLocked(restored.slot); b != nil && b.role == mqttAckRoleAckRoute {
			sess.removeAbandonedAckBindingLocked(b, mqttBindingReleaseHandoff)
		}
		// THE RESTORATION SUPERSEDES THE ALLOCATION'S VERDICT (F-088), in the section that
		// installed it and only after the custody-gated install succeeded: an ordinary acting
		// entry restored on an authoritative possible attempt is owed its retransmission, and a
		// verdict left standing would fence every one of them in beginSendLocked. A genuine
		// cancellation (marker standing, no output token) is proven-unsent and never reaches here.
		if restored.kind == mqttPendingOrdinary && rv.send.decided && rv.send.transmitted && !rv.send.opaque {
			sess.retireSupersededVerdictLocked(rv.rec.Nonce)
		}
		sess.adoptIdentifierExclusionLocked(rv.rec.PI,
			mqttInflightSubject(sess.idHash, sess.gen, rv.rec.PI), rv.rec.Nonce)
		if rv.srcRouted != mqttSourceRoutedComplete {
			// Keyed on COMPLETE alone, deliberately, and not on the same "cannot be
			// re-offered" test the consume arm uses. The two answer different questions.
			//
			// A finished source is never re-offered by anything, so a mapping for it would
			// only mislead the drain and finalization paths that read cpending. A LOST one
			// is a fact about one completed consumer lookup: a payload already dispatched
			// to a callback is not retracted by the consumer's deletion, and if that
			// callback reaches trackPublish with no mapping here it takes the new-delivery
			// branch and mints a SECOND identifier for one source. So Lost keeps its
			// mapping, at the stated cost that drain enrolment can see it.
			reserveSource(rv.rec.Dur, rv.rec.Seq, rv.rec.PI)
		}
		restoredPubs++
	}
	// THE RETAINED EXCHANGES, custody-gated like every restoration (F-105 7c).
	sess.resumeRetainedUnowned = sess.installRecoveredRetainedLocked(census, retainedRecovered)
	sess.resumeRetainedInstalled = true
	sess.retainedQuarantined = retainedQuarantined
	// PAST-PUBREC SLOTS, plus the narrow Retired/publish crash cut. The latter is source
	// evidence, not permission to manufacture a slot phase, so it receives only the same
	// non-actionable in-memory reservation; a present exact PUBREL is still required for
	// route B's durable publish -> pubrel_ready transition.
	//
	// Three outcomes, and the middle one is the disclosed residual rather than a bug:
	//
	//   - the exchange is PROVABLY FINISHED — terminal, or ready with a complete source
	//     and no PUBREL record naming it — so the slot is advanced to terminal, deleted,
	//     and its identifier freed. This is the lost-delete case, and without it a
	//     re-phased slot holds its identifier for the life of the session.
	//   - an ACTION IS LIVE. Observing another holder's token proves phase progress, not
	//     authority, so nothing is advanced, deleted or transmitted: the identifier stays
	//     reserved and that one exchange is non-actionable until its own holder drains it.
	//     It does NOT make the identifier space incomplete — the rest of the session
	//     resumes normally, which is what distinguishes this from opaque.
	//   - otherwise the exchange is genuinely outstanding: the identifier is reserved as a
	//     NON-ACTIONABLE pendingPubRel entry, so nothing transmits from it and bumpPI
	//     cannot re-mint it.
	// The owed-record reservations installed below, enrolled in materialization once this
	// section's lock is released: the enrolment arms a timer and must not run under sess.mu.
	var owedIntended []mqttInflightSlotRef
	// Ordinary pubrel_ready slots whose canonical record the (forward, present-only) scan did not
	// find: CANDIDATES for re-materialisation (F-105 7b). The scan's absence selects them and proves
	// nothing; the continuation's own point read decides.
	var rematerialize []*mqttRematerialization
	pubRelReservations := make([]mqttRecoveredInflightReg, 0, len(pastPubRec)+len(retired))
	pubRelReservations = append(pubRelReservations, pastPubRec...)
	for _, rv := range retired {
		if rv.rec != nil && rv.rec.State == mqttInflightPublish {
			pubRelReservations = append(pubRelReservations, rv)
		}
	}
	for _, rv := range pubRelReservations {
		if rv.rec == nil {
			continue
		}
		pi := rv.rec.PI
		pub := sess.pendingPublish[pi]
		if pub != nil && (pub.regNonce != rv.rec.Nonce || pub.jsDur != rv.rec.Dur ||
			pub.sseq != rv.rec.Seq) {
			skipped++
			continue // a different in-memory exchange owns this identifier
		}
		// RE-MATERIALISATION IS ENROLLED FOR THE SLOT, NOT FOR THE ENTRY (holistic review H7b-2):
		// an ordinary pubrel_ready slot whose named canonical record is not present is a
		// candidate whether this pass installs its PUBREL entry, finds one resident (a
		// same-object reconnect), or finds the scan's placeholder (a crash after a candidate
		// was published and before its rebind). Enrolling installs nothing; the continuation
		// re-derives every fact from the store and rebinds only an exchange still PubRecObserved.
		if rv.rec.State == mqttInflightPubRelReady && !mqttDurIsSlotOnlyClass(rv.rec.Dur) &&
			rv.rec.PubRelStreamSeq != 0 && !canonicalPubRelPresent(rv.rec) {
			if ref := mqttSlotRefFromRecord(mqttInflightSubject(sess.idHash, sess.gen, pi), rv.seq, rv.rec); ref != nil {
				rematerialize = append(rematerialize, &mqttRematerialization{
					slot: *ref, oldSeq: rv.rec.PubRelStreamSeq, lin: rv.rec.Lin, lic: mqttLicenceOf(c),
				})
			}
		}
		if rel := sess.pendingPubRel[pi]; rel != nil {
			if rel.regRecoveryDemoted && mqttRecoveryPendingMatches(rel, rv.rec, true) &&
				!sess.recoveryInstallRefusedLocked(rv.rec, rel) {
				// Reactivation is installation: gated on the lineage the ledger holds (F-105 7b),
				// and a replacement of exactly this resident.
				sess.reactivateDemotedLocked(mqttPendingPubRelMap, pi, rel, rv.rec)
			}
			if rv.rec.State == mqttInflightPubRelIntended && rel.regNonce == rv.rec.Nonce {
				if ref := mqttSlotRefFromRecord(
					mqttInflightSubject(sess.idHash, sess.gen, pi), rv.seq, rv.rec); ref != nil {
					owedIntended = append(owedIntended, *ref)
				}
			}
			if pub != nil && rel.regNonce == rv.rec.Nonce {
				// The PUBREL-side owner is already installed. Complete only the exact
				// PUBLISH-side half of the same exchange below.
			} else {
				skipped++
				continue
			}
		} else {
			// OWNER FIRST. F-093 requires the new reservation to exist before the old
			// PUBLISH entry or source mapping is removed. mqttRecoveredPending also
			// carries the authoritative slot reference that terminal cleanup needs.
			e := mqttRecoveredPending(sess.idHash, sess.gen, pi, rv)
			if e == nil {
				continue
			}
			if sess.recoveryRestoreRefusedLocked(census, rv.rec, e) {
				// The custody gate refused (F-105 7b): the ledger does not hold this lineage.
				// Nothing is installed, as at the PUBLISH site.
				skipped++
				continue
			}
			if !sess.setPendingLocked(mqttPendingPubRelMap, pi, mqttExpectEmpty(), e) {
				skipped++
				continue
			}
			restoredRels++
			if rv.rec.State == mqttInflightPubRelIntended {
				// Carried with the reference as it stands AFTER custody, so the first
				// attempt addresses the slot this pass just claimed. Materialization
				// revalidates custody, identity and any live token on every attempt, so a
				// withheld or live-token slot is retried rather than acted on, and its
				// reservation stays either way.
				owedIntended = append(owedIntended, e.slot)
			}
		}
		if pub != nil {
			// Source suppression was installed by the retirement loop above whenever
			// the source may still be offered. With the PUBREL-side owner now resident,
			// compare-delete the stale PUBLISH half and only its exact mapping. A send
			// already in progress remains independently reserved by sendsInFlight.
			delete(sess.pendingPublish, pi)
			if m := sess.cpending[pub.jsDur]; m != nil && m[pub.sseq] == pi {
				delete(sess.cpending[pub.jsDur], pub.sseq)
			}
			continue
		}
	}

	// The opaque ones reserve their identifier and nothing else: there is no source to
	// enter in cpending, because the unreadable body is exactly what would have named
	// it. The entry still carries its register sequence, which is what tells a later
	// reader this reservation stands on a register rather than being the register-less
	// shape a retained send has (R6).

	for _, rv := range opaque {
		if _, live := sess.pendingPublish[rv.rec.PI]; live {
			skipped++
			continue // in-memory state wins
		}
		if _, asRel := sess.pendingPubRel[rv.rec.PI]; asRel {
			skipped++
			continue // a PUBREL record proves it is past PUBREC (R2)
		}
		if rv.send.opaque && rv.rec.Nonce != _EMPTY_ {
			// The MARKER-OPAQUE reservation (N-7.2): a readable register whose marker
			// could not be read as its own or as absent. Installed with its identity, so
			// the next resume's disposition can replace it nonce-guarded, and with NO
			// source mapping — the reservation holds the identifier, not the delivery.
			e := mqttRecoveredPending(sess.idHash, sess.gen, rv.rec.PI, rv)
			switch {
			case e != nil && !sess.recoveryRestoreRefusedLocked(census, rv.rec, e):
				sess.setPendingLocked(mqttPendingPublishMap, rv.rec.PI, mqttExpectEmpty(), e)
			case e != nil && rv.rec.Lin != _EMPTY_:
				// The custody gate refused a lineage the ledger does not hold (F-105 7b): that
				// exchange ended, and nothing is installed for it.
				skipped++
				continue
			default:
				// No constructible entry, or a LINEAGE-LESS occupant (an unreadable record, or an
				// ordinary register the inventory did not reach): reserved, conferring nothing.
				sess.setPendingLocked(mqttPendingPublishMap, rv.rec.PI, mqttExpectEmpty(), mqttRecoveryOpaquePending(rv.seq))
			}
			installedOpaque++
			continue
		}
		sess.setPendingLocked(mqttPendingPublishMap, rv.rec.PI, mqttExpectEmpty(), mqttRecoveryOpaquePending(rv.seq))
		installedOpaque++
	}

	// An opaque register leaves the identifier space INCOMPLETE, and reserving the
	// identifier is not sufficient on its own. The register's source is still on its
	// stream, unacked, so JetStream will redeliver it — and with no cpending entry to
	// find (the unreadable body is what named the source) that redelivery would take a
	// FRESH identifier, putting the same message in flight under two of them. For QoS 2
	// that is two exchanges for one message, which is the exact deviation this file
	// exists to remove.
	//
	// So allocation stops, which is the same remedy a failed scan gets and for the same
	// reason: we know an identifier is in use but cannot say which delivery owns it, so
	// no new identifier can be issued safely. The session completes what it already has
	// and accepts nothing new; QoS 0 is unaffected. It is loud rather than silent, and
	// an operator removing the corrupt record lifts it on the next resume. Recreating
	// $MQTT_inflight is NOT an alternative: the nine broker-owned streams are one
	// recreation unit, so the only other route is a whole-bundle reset (F-066, and
	// the compatibility boundary in docs/ARCHITECTURE.md).
	//
	// The condition is what the walk FOUND (len(opaque)), never what this pass newly
	// reserved. On a same-broker reconnect the session object survives, so the
	// reservation from the previous resume is already in pendingPublish and this pass
	// installs nothing — counting installations would read that as "no opaque registers"
	// and silently re-enable allocation on the second resume, with the unreadable record
	// still there and its source still unidentifiable.
	//
	// Cleared only when nothing is opaque and both scans completed — never on entry, so
	// a resume that fails part way through leaves the previous restriction standing.
	switch {
	case len(opaque) > 0 || applyIncomplete:
		sess.piSpaceIncomplete = true
	case sess.piSpaceIncomplete:
		sess.piSpaceIncomplete = false
		defer c.Warnf("MQTT: outbound packet identifier allocation resumed for client %q: the durable identifier space was reconstructed", sess.id)
	}
	// Completing the register apply does not complete the custody census.
	// Preserve the C5b barrier only when a needed handoff could not be prepared.
	if !applyIncomplete {
		sess.inflightApplyCompletedWalk = walkID
	}
	if census != nil && !censusIncomplete && !sess.piSpaceIncomplete {
		relPIs := make(map[uint16]bool, len(rels))
		for _, rel := range rels {
			relPIs[rel.pi] = true
		}
		for i := range recoveryReservations {
			rr := &recoveryReservations[i]
			present := observedPI[rr.pi] || relPIs[rr.pi]
			if !present && sess.recoveryReleaseCurrentLocked(rr) {
				staged = append(staged, mqttStagedRelease{recovery: rr})
			}
		}
	}
	// The claim FLOOR, applied here because this is the first point at which the installed
	// source mappings are final and the last point before any drain can deliver: a
	// record-restored drain has no delivery subscription until restoreDrains installs one.
	// It runs before recoverDrains too, so a claim it adds is visible to that pass's
	// pruning rather than being pruned behind its back. See unsub_drain.go.
	for _, w := range disposalHandoffs {
		sess.applyDisposalRecoveryLocked(w)
	}
	sess.enrolFloorClaimsLocked(disposalHandoffs...)
	sess.mu.Unlock()

	// RECOVERY'S HALF OF L1'S LIVENESS: every owed-record reservation this pass installed is
	// enrolled in materialization. The live PUBREC path enrols at the transition that creates
	// the obligation; a restart loses that timer, and without this the obligation would wait
	// for traffic that never has to come.
	for i := range owedIntended {
		sess.scheduleOwedMaterialization(c, &mqttOwedMaterialization{slot: owedIntended[i]})
	}
	// And every candidate for canonical re-materialisation, for the same liveness reason: nothing
	// else will ever deliver a PUBREL whose record vanished.
	for _, rm := range rematerialize {
		sess.scheduleCanonicalRematerialization(c, rm)
	}

	// The finished past-PUBREC slots, off the lock because each is a CAS and a delete.
	//
	// Both steps are needed and in this order: the phase gate refuses a delete from
	// `pubrel_ready`, deliberately, because a ready slot is the identifier's live durable
	// claimant. Recovery's completeness proof is what licenses the advance to terminal,
	// and the advance is what licenses the delete. Neither step alone is sound — an
	// advance with no delete leaks the slot, and a delete with no advance is the
	// vacate-a-live-exchange hazard F-092 exists to close.
	for i := range pastPubRecFinished {
		ref := pastPubRecFinished[i]
		switch sess.advanceFinishedSlotToTerminal(c, &ref) {
		case mqttSlotAdvanceNotOurs:
			// An action is live, or the slot moved. Either way it is not ours to finish:
			// the identifier stays reserved and its own holder converges it.
			continue
		case mqttSlotAdvanceUnresolved:
			// REQUIRED WORK WHOSE OUTCOME IS UNKNOWN, so the resume is refused rather than
			// completed around it.
			//
			// This slot was PROVEN finished, and its identifier is reserved by a terminal
			// reservation installed moments ago. Continuing here accepted the connection
			// with that reservation standing and nothing left to retry the advance — a
			// finite storage blip permanently consuming a Receive Maximum slot on a
			// session that looks healthy. There is no later event: the source is already
			// settled, so nothing re-offers it, and no delivery addresses this slot.
			//
			// Refusing costs a CONNECT and is self-correcting: the reservation and the
			// durable slot are preserved, and the next resume re-derives the completion
			// proof and retries the advance against a working store. The cost is stated in
			// docs/CONFORMANCE.md — while the store keeps failing this refusal repeats,
			// which is a storage-progress dependency and not a bounded number of retries.
			return nil, fmt.Errorf("the terminal advance of a finished past-PUBREC slot for packet "+
				"identifier %d could not be resolved, so its identifier would stay reserved with "+
				"nothing left to retry it", ref.name.pi)
		}
		switch sess.finalizeTerminalSlot(c, ref) {
		case mqttTerminalDone:
			sess.releaseTerminalReservation(ref)
		case mqttTerminalRetainedReoffer:
			// Retained for a re-offerable source: free the reservation and reconcile the
			// slot independently once the source settles (consult 7). The CAPACITY is
			// released; the IDENTIFIER is not, because the register is deliberately kept
			// and nothing has confirmed it gone (F-098, round 5) — and after a cold resume
			// there is no send hold left to carry that obligation.
			sess.releaseRetainedTerminalReservation(c, ref)
		default:
			// Unsettled cleanup of a recorded completion: the resume CONTINUES (the refusal above
			// is the ADVANCE's), and the cleanup goes to its class's owner — for an ordinary
			// exchange the completion worker, which a delivered entry cannot displace.
			if mqttDurIsSlotOnlyClass(ref.dur) {
				sess.retainAndReconcileTerminal(c, ref)
			} else {
				sess.ownTerminalCleanup(c, ref)
			}
		}
	}

	if len(opaque) > 0 {
		c.Errorf("MQTT: withholding new outbound QoS 1/2 deliveries for client %q: %d in-flight register(s) are unreadable, "+
			"so their identifiers are reserved but their deliveries cannot be identified. Deliveries already in flight complete "+
			"normally and QoS 0 is unaffected; remove the unreadable record(s) to restore allocation.", sess.id, len(opaque))
	}

	// Make every retirement inferred from a surviving PUBREL durable now, rather than
	// leaving it to depend on that PUBREL: the client's next PUBCOMP would otherwise
	// destroy the evidence, and the PUBCOMP path can only withhold its acknowledgement
	// for a retirement this broker created in the first place.
	for _, fr := range retired {
		if fr.rec.Retired {
			continue
		}
		sess.secureRetiredSource(c, mqttDeferredKey{fr.rec.Dur, fr.rec.Seq})
	}

	// Then re-classify every retirement this pass installed, against the source as it is
	// NOW rather than as the earlier snapshot saw it — and do it in ONE BATCHED pass.
	//
	// Why the re-classification is needed: the entries above were installed from a
	// snapshot taken earlier in this recovery, and this session is registered in
	// sessByHash BEFORE recovery runs, so the orphan sweep's affine register arm can
	// settle the very same exchange in between — deleting the copy, reaping the claim,
	// removing the register and clearing the resident retirement while this map is still
	// EMPTY. The install would then put a stale retirement back with its copy already
	// gone, and nothing revisits one afterwards: both other resolveRetiredSource call
	// sites run only where a retirement is CREATED, and a redelivery cannot happen
	// without the copy. One permanent heap entry per interleaving.
	//
	// Why it is BATCHED, and not a per-retirement resolveRetiredSource call: this runs
	// before the CONNACK, and this function supports 65_535 records. resolveRetiredSource
	// performs a synchronous consumer lookup and an exact message load per call, which is
	// exactly the "upwards of 100_000 serial round trips" mqttInflightSourceCache exists
	// to avoid — and the accumulation-heavy population this item targets is precisely
	// where the count would be highest, so a per-record loop would make the largest
	// sessions unable to resume at all. One cache, one lookup per distinct durable, a few
	// batched message reads.
	if len(retired) > 0 {
		fresh := as.newInflightSourceCache()
		recs := make([]*mqttInflightRecord, 0, len(retired))
		for _, fr := range retired {
			recs = append(recs, fr.rec)
		}
		fresh.resolve(recs)
		for _, fr := range retired {
			if fresh.state(fr.rec) != mqttInflightSourceComplete {
				continue // live or unknown: the retirement still has something to suppress
			}
			// Identity-fenced on the register's own nonce, so a retirement belonging to a
			// different exchange on the same key is never dropped.
			key := mqttDeferredKey{fr.rec.Dur, fr.rec.Seq}
			sess.mu.Lock()
			cur := sess.srcAckOwed[key]
			if cur != nil && cur.nonce == fr.rec.Nonce {
				sess.wakeRetainedTerminalCleanupLocked(cur.nonce, key.jsDur, key.sseq) // before the owner leaves
				delete(sess.srcAckOwed, key)
			} else {
				cur = nil
			}
			sess.mu.Unlock()
			if cur != nil {
				// The SECOND retirement drop, and it owes the verdict the same proof as
				// the first: a Retired-and-Cancelled record routes to the retirement
				// arms by the precedence rule, so a cancellation verdict can name the
				// very register this drop deletes (F-087 WP3).
				// Census C28: gated through the same helper. Recovery proved the source
				// complete, which licenses dropping the RETIREMENT; the exchange's slot
				// is a separate question and the gate is what keeps the two apart.
				sess.reapRegisterWithVerdictGated(cur.regSeq, cur.nonce, false,
					mqttSlotRefFromRecord(mqttInflightSubject(sess.idHash, sess.gen, cur.pi), cur.regSeq,
						&mqttInflightRecord{PI: cur.pi, Dur: fr.rec.Dur, Seq: fr.rec.Seq,
							QoS: 2, State: fr.rec.State, Nonce: cur.nonce,
							Claim: fr.rec.Claim, ClaimSeq: fr.rec.ClaimSeq}),
					mqttSlotDeleteComplete, mqttLicenceOf(c))
				c.Debugf("MQTT: dropping the recovered retirement of QoS 2 source %s/%d for client %q: its "+
					"source is provably complete", fr.rec.Dur, fr.rec.Seq, sess.id)
			}
		}
	}

	for _, rd := range drop {
		// Through the verdict property: an unverdicted nonce keeps today's
		// fire-and-forget delete, and a verdicted one is deleted with proof and its
		// verdict released against that proof (F-087 WP3).
		//
		// Census C26: GATED. An earlier revision argued this loop was safe ungated because
		// the scan had just classified these records; a holistic review refuted that — the
		// loop runs AFTER the scan, so a remembered global sequence can by then name an
		// unrelated record under stream-sequence reuse. The reference carried on the drop
		// supplies the subject, the occupant identity and the freshness fence.
		// If the slot has since crossed PUBREC, this PUBLISH-side cleanup is stale and
		// refuses it. Past-PUBREC completion is classified separately above and goes
		// through advanceFinishedSlotToTerminal plus finalizeTerminalSlot, including the
		// complete duplicate scan that an ordinary source-completion reap must not bypass.
		//
		// The register is dropped, THEN its marker is met under "reap" (N-9.2), capture
		// first. Two licenses reach here and they owe different things. A source provably
		// complete owes no marker, and one left by a lost delete is reaped only if its
		// sibling is now authoritatively absent (row 24). A MARKER-PROVEN drop is the
		// opposite: its marker is present by construction — that presence is what proved
		// the PUBLISH never went out — and it is met by this same call with no extra code.
		// The disposition carried on the drop is what keeps the two apart at the fence:
		// mqttSlotDeleteNeverTransmitted refuses a same-exchange rewrite and so does not
		// reach this cleanup at all, which is correct, because a register this pass
		// declined to delete must not have its marker reaped out from beside it.
		if sess.reapRegisterWithVerdictGated(rd.seq, rd.nonce, false, rd.ref, rd.disp, mqttLicenceOf(c)) && rd.ref != nil {
			as.reapMarkerCaptureFirst(c, mqttMarkerCallerResume, rd.ref.name.subject, false)
		}
		// The exclusion installed for this candidate in the apply phase is lifted HERE and
		// only against a CONFIRMED ABSENCE of its subject — never against the delete's own
		// return, which reads an already-absent sequence as success and so cannot tell a
		// removal from a same-identity rewrite that landed first.
		sess.settleRecoveryDropExclusion(rd.ref, rd.nonce, rd.dur, rd.sseq)
	}
	// PROTOCOL EVIDENCE consumers (N-8.3 items 4-6): a Retired register, a slot past
	// PUBREC, and a PUBLISH-shaped slot superseded by its exact PUBREL record were all
	// answered by the client. Their markers are cleaned under that operation with the
	// walk's reload-acquired observation: only a body naming the exchange the evidence
	// names is deleted (row 22); an unattributed one is left to the sweep (row 23).
	for _, groups := range [][]mqttRecoveredInflightReg{retired, pastPubRec} {
		for _, rv := range groups {
			if rv.rec == nil || rv.rec.PI == 0 {
				continue
			}
			subject := mqttInflightSubject(idHash, gen, rv.rec.PI)
			as.cleanMarkerAfterEvidenceReloaded(c, subject, rv.rec.Nonce, markerFor(subject))
		}
	}
	for _, handoff := range routeB {
		if handoff.reg.rec == nil || handoff.reg.rec.PI == 0 {
			continue
		}
		subject := mqttInflightSubject(idHash, gen, handoff.reg.rec.PI)
		as.cleanMarkerAfterEvidenceReloaded(c, subject, handoff.reg.rec.Nonce, markerFor(subject))
	}
	// A register the verdict set named is deleted with PROOF, and its verdict released only
	// against that proof. Forgetting it after a fire-and-forget delete would leave a register
	// nothing anywhere explains — an ordinary-looking one whose consumer is gone, which is
	// the exact state the verdict exists to prevent. A failed delete simply KEEPS the
	// verdict: it is already durable (this pass read it from the record), and the next scan
	// retries the delete against it.
	for _, cd := range dropByNonce {
		// The same property as the loop above, with confirmation forced: the verdict set
		// names this register by construction, so the delete must be proven whether or
		// not the map lookup is racing a concurrent forget.
		// Census C27: GATED, same correction as C26. Confirmation stays forced — the verdict
		// names this register and the delete must be proven — but proof of deletion is not
		// proof that the right record was addressed.
		if !sess.reapRegisterWithVerdictGated(cd.seq, cd.nonce, true, cd.ref, mqttSlotDeleteComplete, mqttLicenceOf(c)) {
			c.Warnf("MQTT: keeping the void-register verdict for sequence %d of client %q: "+
				"its register could not be deleted with proof", cd.seq, sess.id)
		}
		sess.settleRecoveryDropExclusion(cd.ref, cd.nonce, cd.dur, cd.sseq)
	}
	// A verdict whose register was NOT found is deliberately kept, not retired. Absence at
	// this scan is not proof that no register will ever carry the nonce: R1 permits a
	// superseded broker's create-only CAS to land LATE, after a takeover's resume has
	// already run, and retiring the verdict on this scan's emptiness would leave that
	// late register ordinary — an abandoned consumer's register nothing anywhere calls
	// void, holding its identifier at every later resume. Kept, the verdict is
	// self-healing instead: the scan that finally SEES the late register deletes it with
	// proof (the loop above) and retires the verdict then. The residue is a nonce whose
	// register never lands at all, which is bounded stale metadata: the no-CAS-issued
	// path retires its own verdict (releaseCancelledReservation), so what remains is one
	// entry per store whose every attempt definitively failed.
	//
	// (This replaced an absence-based cleanup that a review showed to be exactly the
	// late-CAS hazard above.)
	// Whatever the loops above changed should reach the record. Refused while the
	// CONNECT holds the fence, which costs nothing: the resume's own save carries it.
	sess.persistVoidedRegs(c)
	if restoredPubs+restoredRels+len(opaque)+len(retired)+len(drop)+len(dropByNonce)+skipped > 0 {
		c.Debugf("MQTT: restored %d in-flight PUBLISH and %d PUBREL identifier(s) for client %q, "+
			"re-retired %d source(s), found %d unreadable register(s) (%d newly reserved), "+
			"dropped %d stale (%d cancelled), kept %d in-memory",
			restoredPubs, restoredRels, sess.id, len(retired), len(opaque), installedOpaque,
			len(drop)+len(dropByNonce), len(dropByNonce), skipped)
	}
	return staged, nil
}

// ---------------------------------------------------------------------------
// Stream prerequisites
// ---------------------------------------------------------------------------

// validateInflightStream verifies $MQTT_inflight can carry the registers.
//
// The list is derived from what THIS owner does, which is the doctrine every
// validator here follows and the reason no two of these lists are the same: a
// create-only CAS per identifier, a per-subject singleton, deletes at an exact
// sequence, and a bounded forward walk. Since F-034 there is NO filter purge: the
// teardown's per-incarnation purge was replaced by reapIncarnationRegisters, which
// enumerates, loads, classifies and deletes at exact sequences — which is why
// DenyPurge is a waiver here and not a rejection. It
// therefore requires the CAS and acknowledgement semantics, and — unlike
// $MQTT_teardown, which never removes anything — it requires DELETION to be
// permitted. Purging is not required and stopped being required with F-034: the
// reap is the only remover left, and it deletes at exact sequences.
//
// It deliberately does NOT check ConsumerLimits.InactiveThreshold: nothing ever
// creates a consumer on this stream, so a stream-forced consumer limit cannot affect
// it. A validator that listed prerequisites its owner does not have would fail
// startups for no reason.
func validateInflightStream(si *StreamInfo, wantReplicas int, bundleEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttInflightStreamSubjectPrefix + ">"}, wantReplicas); err != nil {
		return err
	}
	if !cfg.AllowAtomicPublish {
		// The PUBLISH-phase register and its unsent marker are committed as one atomic
		// batch, which the stream would reject outright without this. A stream that
		// merely LOOKED right would fail every first QoS 1/2 delivery. An existing stream
		// lacking it is refused here, never patched: the nine broker-owned streams are one
		// recreation unit (F-066).
		return fmt.Errorf("allow_atomic is not set: a PUBLISH-phase in-flight register and its unsent marker are committed in one atomic batch, which the stream would reject")
	}
	if cfg.Retention != LimitsPolicy {
		// Nothing consumes a register — they are read by direct lookup and by a
		// bounded walk — so interest or work-queue retention would drop every one of
		// them at the moment it was written.
		return fmt.Errorf("retention is %v (want limits): nothing consumes a register, so any other retention drops it immediately", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		// DiscardNew turns a replacement into a rejection. Registers are written
		// create-only today, but a full subject must be replaceable for the store
		// path's reconcile-then-retry to converge rather than fail identically forever.
		return fmt.Errorf("discard is %v (want old)", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		// With more than one record per subject a write APPENDS: loadLastMsgFor
		// returns the newest while an older register survives underneath, and
		// deleting the exact sequence then re-exposes a stale identifier claim.
		return fmt.Errorf("max_msgs_per_subject is %d (want 1)", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		// Age eviction cannot honour "delete only once the exchange is complete", and
		// a count or size cap would silently drop the identity of a delivery the
		// client is still holding. Cleanup here is explicit reconciliation.
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict the identity of a live in-flight delivery",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy an identifier's CAS slot")
	}
	if cfg.MaxMsgSize > 0 {
		// A register is small and fixed-shape, but a cap is not this owner's
		// semantics: it would reject the write that must precede a send, stalling the
		// delivery rather than degrading anything.
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject the register write that must precede a send", cfg.MaxMsgSize)
	}
	if cfg.DenyDelete {
		return fmt.Errorf("deny_delete is set: it blocks the exact-sequence delete every terminal path performs")
	}
	// DenyPurge is deliberately NOT a prerequisite, and it stopped being one with F-034.
	// The teardown's per-incarnation FILTER PURGE was replaced by reapIncarnationRegisters,
	// which enumerates, loads, classifies and deletes at exact sequences — so DenyDelete
	// above is the whole of what this owner needs. Refusing DenyPurge would reject a
	// hardened stream this broker can now use, on the strength of a purge that no longer
	// exists: the audit that removed the purge is the same one that must remove its
	// prerequisite (feedback_delete_redundant_mechanisms).
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages on register subjects would be parsed as records", cfg.SubjectDeleteMarkerTTL)
	}
	// The bundle stamp: this stream carries the account's recovery epoch, which is
	// what makes the nine broker-owned streams one recreation unit rather than nine
	// independently recoverable stores (F-066).
	if err := validateBundleEpochStamp(&cfg, bundleEpoch); err != nil {
		return err
	}
	return nil
}
