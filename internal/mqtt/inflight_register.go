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
// after its register is CONFIRMED at a sequence. From allocation until that
// confirmation the pending entry carries regPending, and a regPending entry is not
// sendable by anyone: a concurrent redelivery of the same sequence returns
// identifier 0 (AckWait re-offers it), resendPendingRedelivery does not NAK it, and
// stageDrain does not enrol it. A definite store failure untracks the identifier and
// sends nothing, which is safe precisely because nothing else could have sent it. An
// ambiguous store is resolved by RE-READING the subject and matching the nonce,
// never by the shape of the response.
//
//	R1 is also the takeover fence. An identifier the client can be holding implies
//	a completed send, which implies a confirmed register, which implies the next
//	broker's scan sees it. A superseded broker's late register write therefore
//	concerns an identifier that never reached the client; it is inert, and the
//	duplicate-(Dur,Seq) rule in the recovery scan reaps it.
//
// R2 ONE DURABLE AUTHORITY PER PHASE, WITH A PROVEN HANDOFF. The PUBLISH phase is
// owned by the register; past PUBREC the correlated PUBREL record on $MQTT_out owns
// the identifier (pubrel_correlation.go), and mqttDeliverPubRelCb already re-sends
// under it. The handoff is the PUBREL store, and on that path the source ack is
// CONFIRMED (jsa.confirmAck) before the register is deleted, because that is the
// codebase's standing rule wherever a durable record is deleted on the strength of an
// ack. What it buys here is precise and worth stating exactly: it keeps the register
// from being deleted over a source that was never actually released, which would cost
// the redelivery its ORIGINAL identifier. It does not, by itself, prevent a duplicate
// — a source ack that never lands means JetStream redelivers whatever we do, which was
// true before this file existed. QoS 1 keeps the fire-and-forget ack: there the worst
// case is one duplicate after PUBACK, which QoS 1 permits.
// Where a register and a PUBREL record name the same source, the PUBREL record wins.
//
//	What actually stops a COMPLETED exchange being resurrected is R5's proof, not
//	this ordering: recovery classifies the source and drops a register whose delivery
//	is provably finished. The two are complementary, and only the second one is load-
//	bearing for correctness.
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
//	THREE conditions set it, and they are all the same condition wearing different
//	clothes: the identifier space cannot be fully accounted for.
//
//	 1. A scan that could not complete — identifiers may be in use that we never saw.
//	 2. A register whose body is UNREADABLE — we see the identifier (its subject names
//	    it) but not the delivery that owns it, so its source is still unacked and its
//	    redelivery would find no cpending entry and take a fresh identifier.
//	 3. TWO registers naming ONE source on different identifiers — we see both
//	    identifiers but cannot say which one the client holds, because a stream
//	    sequence orders the writes and not the sends.
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
// R5 NO SILENT SURVIVORS. Every terminal path that releases an identifier — PUBACK,
// a failed PUBREC, the PUBREL store, Message Expiry, Maximum Packet Size — also
// deletes the register. A failed delete costs a stale record that the store path's
// reconcile, the next resume scan, or the orphan sweep removes; it never wedges an
// identifier and never duplicates a message.
//
// R6 REGISTERED IFF TRANSMITTED. A delivery withheld by the in-flight cap was never
// sent, so it is never registered (trackPublish returns before allocating). A
// retained-message send has no JetStream backing at all, so it has no register and
// no post-restart redelivery — unchanged, and now stated.
//
// # Cost
//
// One create-only CAS round trip per FIRST transmission of every outbound QoS 1/2
// message, plus one delete per completion, plus (QoS 2 only) turning the source ack
// into a confirmed round trip. Redeliveries and recovered deliveries pay nothing.
// Batching was rejected: it would delay first transmissions behind a batch, and the
// obvious alternative — register after sending — is not a cheaper version of this
// design but a reintroduction of the deviation as a narrower window.

package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

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
// Retired marks the one state a register can reach that is NOT a PUBLISH-phase
// delivery: the exchange is past PUBREC and its source's acknowledgement could not be
// proven, so the payload must never be sent again but the source may still be re-offered
// and must be acknowledged away. Without it that state is indistinguishable from a
// genuine PUBLISH-phase register — same subject, same source, same live source message —
// so a restart would read it as "still owed" and re-send a payload the client has
// already completed. It is the durable half of the in-memory retirement; see
// retireSourceLocked and mqttSession.srcAckOwed.
type mqttInflightRecord struct {
	PI    uint16 `json:"pi"`
	Dur   string `json:"dur"`
	Seq   uint64 `json:"seq"`
	QoS   byte   `json:"qos"`
	Nonce string `json:"nonce"`
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
	// Cancelled marks a register whose delivery was NEVER TRANSMITTED: the allocation was
	// withdrawn — by an UNSUBSCRIBE staging a drain, a discard, a takeover — while its
	// store was still in flight, so the confirmation found no entry to release and the
	// PUBLISH was never enqueued.
	//
	// It is the durable evidence that distinguishes "the client may be holding this
	// identifier" from "the client never saw it", and only the second may free an
	// identifier or exempt it from Receive Maximum. Written only where the record could not
	// be DELETED, which is the outcome the cancellation actually wants; recovery then frees
	// the identifier and removes the record.
	Cancelled bool `json:"cancelled,omitempty"`
}

// mqttRecoveredInflightReg is one readable register and the exact stream sequence
// at which recovery observed it. foreign marks a member whose claim arbitration was
// LOST: seq is then the rival's sequence, kept for the reservation it becomes but
// never a sequence this object may settle.
type mqttRecoveredInflightReg struct {
	rec     *mqttInflightRecord
	seq     uint64
	foreign bool
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
func mqttInflightRecordIdentityOK(r *mqttInflightRecord) bool {
	if r == nil {
		return false
	}
	if r.PI == 0 || r.Dur == _EMPTY_ || r.Seq == 0 {
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

// ownsSharedInflightRegister reports whether a nonce match is also owned by this
// session object. Plain registers are intentionally outside this arbitration: they
// are not claimed during resume, so applying the gate to them would turn the
// current owner's own restored records into foreign state.
func (sess *mqttSession) ownsSharedInflightRegister(durable, claim string) bool {
	return !strings.HasPrefix(durable, mqttSharedConsumerDurablePrefix) || claim == sess.regClaim
}

// abandonForeignSharedInflightRegister records the lost-arbitration outcome. The
// peer's claimed register is the durable decision, so this object must neither
// rewrite it nor persist a competing nonce verdict.
func (sess *mqttSession) abandonForeignSharedInflightRegister(c *client, durable, nonce, subject string) {
	sess.forgetCancelledReg(nonce)
	mqttWarnf(c, "MQTT: abandoning the in-flight register on %q for client %q: shared durable %q is claimed by another session owner",
		subject, sess.id, durable)
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
	r := &mqttInflightRecord{}
	if uerr := json.Unmarshal(smsg.Data, r); uerr != nil || !mqttInflightRecordIdentityOK(r) {
		// Unreadable is a DEFINITE fact about the record, not an indeterminate read.
		return nil, smsg, nil
	}
	return r, smsg, nil
}

// storeInflightCAS performs a register write, honoring an instance-scoped test seam.
// Only register writes are routed through here, so an injected fault never perturbs
// the source store, the PUBREL store or unrelated loads.
func (as *mqttAccountSessionManager) storeInflightCAS(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error) {
	as.mu.RLock()
	hook := as.testInflightStore
	as.mu.RUnlock()
	if hook != nil {
		return hook(subject, expectSeq, r)
	}
	return as.jsa.storeInflightCAS(subject, expectSeq, r)
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
// test seam. Used by the teardown reap; the resume walk has its own batching.
func (as *mqttAccountSessionManager) loadInflightRecordsMulti(subjects []string) ([]*JSApiMsgGetResponse, error) {
	as.mu.RLock()
	hook := as.testInflightLoadMulti
	as.mu.RUnlock()
	if hook != nil {
		return hook(subjects)
	}
	return as.jsa.loadLastMsgForMulti(mqttInflightStreamName, subjects)
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
// Zero is a no-op, which is what makes every terminal path able to call this
// unconditionally on whatever entry it removed — a retained send and a delivery
// recovered from a PUBREL record both carry no register.
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

	owned, ambiguous = make(map[string]bool), make(map[string]bool)
	for _, d := range sess.drains {
		for _, st := range d.seqs {
			st.regAmbiguous = false
		}
	}
	for key, group := range drainRegs {
		fr := group[0]
		d := sess.drainByDur[fr.rec.Dur]
		if d == nil {
			continue
		}
		st := d.seqs[fr.rec.Seq]
		if st == nil || !st.claimed {
			continue
		}
		if len(group) > 1 {
			// More than one register names this delivery, on different identifiers. Record
			// the AMBIGUITY explicitly: zeroing the hint alone used to be the whole signal,
			// and the drain's release then read a zero hint as "nothing to do" and
			// finalized anyway.
			st.regSeq = 0
			st.regAmbiguous = true
			ambiguous[key] = true
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
			st.regSeq = 0
			st.regAmbiguous = true
			ambiguous[key] = true
			continue
		}
		st.regSeq = fr.seq
		owned[key] = true
	}
	return owned, ambiguous
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
// wantDur and wantSeq are the caller's authority. For the UNSUBSCRIBE drain that is
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
	if !sess.reapRegisterWithVerdict(smsg.Sequence, cur.Nonce, true) {
		mqttWarnf(c, "MQTT: unable to release the in-flight register on %q at sequence %d",
			subject, smsg.Sequence)
		return mqttInflightUndecided
	}
	// The delete landed, OR the sequence was already absent — deleteInflightRegisterConfirmed
	// reads the second as success, which it must, because a crash between a landed delete
	// and the caller's own progress would otherwise withhold the exchange forever.
	//
	// But "already absent" is exactly what a REWRITE of the same delivery looks like from
	// here: the store path's replacement branch moves a register for the same (Dur, Seq)
	// to a new sequence, so S vanishes and S2 appears. Returning Released on that reading
	// would let the caller clear its progress, persist its claim removal, acknowledge the
	// source and delete the consumer while S2 survives — the same phantom subject
	// addressing exists to prevent, reached one step later.
	//
	// So the loop re-reads and re-classifies through the SAME states rather than trusting
	// one round. It terminates: each iteration either deletes a strictly newer sequence or
	// reaches absent/mismatch/undecided, and the attempt bound stops a pathological
	// rewrite-per-iteration from spinning.
	for attempt := 1; attempt < mqttInflightStoreAttempts; attempt++ {
		cur, smsg, err := as.loadInflightRecord(subject)
		switch {
		case err != nil:
			// The delete landed but the confirming re-read did not. The caller must not
			// record progress on an unverified state.
			mqttWarnf(c, "MQTT: released the in-flight register on %q but could not confirm the "+
				"subject is clear: %v", subject, err)
			return mqttInflightUndecided
		case smsg == nil:
			return mqttInflightReleased // genuinely clear
		case cur == nil:
			mqttWarnf(c, "MQTT: an unreadable in-flight register occupies %q at sequence %d after "+
				"the release", subject, smsg.Sequence)
			return mqttInflightUndecided
		case !mqttInflightRecordMatchesSubject(cur, subject):
			mqttWarnf(c, "MQTT: an in-flight register whose body names packet identifier %d occupies "+
				"%q at sequence %d after the release", cur.PI, subject, smsg.Sequence)
			return mqttInflightUndecided
		case cur.Dur != wantDur || cur.Seq != wantSeq:
			// A different exchange took the subject. Ours is gone and this is not ours.
			return mqttInflightReleased
		}
		// Still our delivery, at a new sequence: a rewrite. Delete that one too, through
		// the same property — the rewrite carries the exchange's nonce as much as the
		// first record did.
		if !sess.reapRegisterWithVerdict(smsg.Sequence, cur.Nonce, true) {
			mqttWarnf(c, "MQTT: unable to release a rewritten in-flight register on %q at sequence %d",
				subject, smsg.Sequence)
			return mqttInflightUndecided
		}
	}
	// Rewritten faster than it could be released. Not a licence to record progress.
	mqttWarnf(c, "MQTT: the in-flight register on %q is being rewritten faster than it can be "+
		"released; withholding", subject)
	return mqttInflightUndecided
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
func (as *mqttAccountSessionManager) reapIncarnationRegisters(c *client, idHash, gen string) error {
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
			rec := &mqttInflightRecord{}
			if uerr := json.Unmarshal(smsg.Data, rec); uerr != nil || !mqttInflightRecordIdentityOK(rec) {
				return fmt.Errorf("in-flight register on %q at sequence %d for %q/%q is unreadable, "+
					"so this teardown cannot prove what it would destroy: remove or repair that record",
					subject, smsg.Sequence, idHash, gen)
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
			doomed = append(doomed, smsg.Sequence)
		}
	}
	return as.deleteInflightRegistersPipelined(doomed)
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
			if err := as.deleteInflightRegisterConfirmed(seq); err != nil {
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

// deleteInflightRegisterConfirmed removes a register at its exact sequence and WAITS for
// the outcome, retrying a bounded number of times.
//
// Used where the ordinary asynchronous delete would not be reconcilable: a register whose
// delivery was cancelled names a source whose consumer is about to be deleted, and a
// consumer-gone register is deliberately not read as complete, so a lost delete reserves
// an identifier for the life of the session with nothing able to resolve it. A register
// whose source stays live has no such problem, which is why the ordinary path is allowed
// to be fire-and-forget.
func (as *mqttAccountSessionManager) deleteInflightRegisterConfirmed(seq uint64) error {
	if seq == 0 {
		return nil
	}
	var err error
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
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
// ordinary register whose consumer is gone, which it deliberately does not read as complete,
// and holds the identifier and its Receive Maximum slot for the life of the session.
//
// Recording the nonce is what closes that window, and it is durable at the right moment for
// free on the path that matters: stageDrain runs inside the UNSUBSCRIBE's own record
// transaction, whose save is confirmed before commitUnsub deletes the consumer.
//
// sess.mu must be held.
func (sess *mqttSession) cancelRegPendingLocked(ack *mqttPending) {
	if ack == nil || !ack.regPending || ack.regNonce == _EMPTY_ {
		return
	}
	sess.voidRegNonceLocked(ack.regNonce)
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
// caller that can prove no register will ever carry its nonce: storeInflightRegister's entry
// check, which runs BEFORE any CAS is issued. With no write ever attempted there is nothing a
// crash could resurrect, so neither rung of the ladder is owed and the identifier is free —
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
			delete(sess.pendingPublish, pi)
		}
		delete(sess.settledVoidReservations, nonce)
	}
	sess.mu.Unlock()
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
// filed and not an alpha blocker.
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
func (sess *mqttSession) reapRegisterWithVerdict(seq uint64, nonce string, mustConfirm bool) bool {
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
	if !verdicted && !mustConfirm {
		as.deleteInflightRegister(seq)
		return true
	}
	if err := as.deleteInflightRegisterConfirmed(seq); err != nil {
		return false
	}
	if verdicted {
		sess.forgetCancelledReg(nonce)
	}
	return true
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
	return func() { mqttTestVoidHandoffWindowHookV.Store(prev) }
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
	return func() { mqttTestVoidHandoffPreAttemptHookV.Store(prev) }
}

// deleteInflightRegisterFor removes the register of a pending entry a terminal path
// just took out of the tracking maps. Nil-safe, so every such path can call it
// without first asking whether there was an entry at all.
//
// A FOREIGN reservation is the one entry whose regSeq must never be settled here: it
// names another session object's claimed register (a lost shared-claim arbitration),
// and this delete carries no nonce or claim check — a stale object acknowledging its
// old delivery would destroy the rival's only durable identity through the completion
// path.
//
// Session lock NOT held on entry: this issues a JetStream request.
func (sess *mqttSession) deleteInflightRegisterFor(ack *mqttPending) {
	if ack == nil || ack.regSeq == 0 || ack.regForeign || sess.asm == nil {
		return
	}
	// Through the verdict property (F-087 WP3): this entry carries BOTH the sequence and
	// the exchange nonce, so a cancellation verdict naming the register it deletes is
	// released against the proof of that deletion. "Readable body" was never the
	// boundary — this caller reads no body and still holds the identity — and a
	// fire-and-forget delete here strands a verdict with no crash at all: a teardown
	// records the nonce, both durable rungs fail, and a parked acknowledgement's later
	// delete lands once the storage fault clears.
	sess.reapRegisterWithVerdict(ack.regSeq, ack.regNonce, false)
}

// releaseTransmittedIdentifier releases the packet identifier that a PREVIOUS
// transmission of this source left behind, for a terminal path that has decided the
// client will never be sent it.
//
// It exists because the subscribe-deny guards are deliberately placed BEFORE
// trackPublish, so that a refusal costs no NEW identifier and no new register. That
// placement says nothing about an identifier the delivery did not allocate: a copy
// transmitted before a disconnect leaves a durable in-flight register, and the resume
// restores it into pendingPublish and cpending before any guard runs. A terminal
// denial that only disposed of the message would leave that entry standing forever --
// counted by qos12InflightForCap, so with a Receive Maximum of 1 the session's whole
// quota is consumed and every later ALLOWED delivery is withheld silently, for the
// life of the session. Nothing on the wire says so, and non-delivery of the DENIED
// message looks identical.
//
// Only for TERMINAL dispositions. A withheld-but-still-owed message keeps its
// identifier: it will be offered again, and reusing the identifier the register
// already names is what keeps one message in flight under one identity.
//
// Session lock NOT held on entry: deleteInflightRegisterFor issues a JetStream request.
func (sess *mqttSession) releaseTransmittedIdentifier(jsDur, reply string) {
	if jsDur == _EMPTY_ || reply == _EMPTY_ {
		return
	}
	sseq, _, _, _, _ := ackReplyInfo(reply)
	sess.mu.Lock()
	var removed *mqttPending
	if sseqToPi := sess.cpending[jsDur]; sseqToPi != nil {
		if pi := sseqToPi[sseq]; pi != 0 {
			// Two identifiers this must not take. One the PUBREL phase owns: the
			// PUBLISH side of that exchange is already complete and its identifier is
			// not ours to free -- the same precedence trackPublish applies. And one
			// whose register store is still in flight (R1), which cannot be a restored
			// entry and therefore was not ours to begin with.
			// A THIRD identifier this must not take: a QUARANTINED one. Its register is
			// another writer's, so neither the entry nor the mapping is ours to remove —
			// untracking here would free an identifier whose durable custody is
			// unsettled, and the source's next offer would allocate a second one (F-087
			// WP2). The register delete was already excluded by deleteInflightRegisterFor's
			// own regForeign guard; the ENTRY untrack was the hole.
			_, asRel := sess.pendingPubRel[pi]
			if cur := sess.pendingPublish[pi]; !asRel && cur != nil && !cur.regPending && !cur.regForeign {
				// An active send section CONDEMNS instead (F-087 WP5, class B): the
				// denial's own continuation — the untrack and this register's delete —
				// runs at endSend, so the identifier is not re-minted under a write that
				// has not returned.
				if !sess.condemnSendLocked(pi, mqttSendSettleDenyRelease) {
					removed = sess.untrackPublishEntry(pi)
				}
			}
		}
	}
	sess.mu.Unlock()
	sess.deleteInflightRegisterFor(removed)
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
	rec := &mqttInflightRecord{
		PI: pi, Dur: dur, Seq: sseq, QoS: 2,
		Nonce: nonce, Claim: regClaim, ClaimSeq: regClaimSeq, Q2AffClaim: q2AffClaim, Q2AffCopy: q2AffCopy, Retired: true,
	}

	var lastErr error
	expectSeq := regSeq
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		seq, err := as.storeInflightCAS(subject, expectSeq, rec)
		if err == nil {
			return seq, true
		}
		lastErr = err
		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		switch {
		case smsg == nil:
			// The subject is empty. Nothing left to mark, and nothing can read an absent
			// register as an owed delivery either, so the window is closed by absence.
			return 0, true
		case cur != nil && !mqttInflightRecordMatchesSubject(cur, subject):
			// Before the nonce is consulted at all: a body naming another identifier is
			// what recovery calls unreadable, and every arm below is destructive of it —
			// the retired fast path answers for it, and the replace arm OVERWRITES it.
			// Fail closed, which here means the caller keeps its PUBREL.
			c.Warnf("MQTT: cannot retire the in-flight register on %q for client %q: the record there "+
				"names a different packet identifier", subject, sess.id)
			return 0, false
		case cur != nil && cur.Nonce == nonce && !sess.ownsSharedInflightRegister(dur, cur.Claim):
			sess.abandonForeignSharedInflightRegister(c, dur, nonce, subject)
			return 0, false
		case cur != nil && cur.Nonce == nonce && cur.Retired:
			// Ours, already retired: an earlier attempt landed and lost its response.
			return smsg.Sequence, true
		case cur != nil && cur.Nonce == nonce:
			// Our record, moved: REBUILD the proposal from what is there now and set
			// only the fact this writer owns (F-087 WP1). The constructed body above
			// describes the record as it was before the round we just lost; re-proposing
			// it would clear a Cancelled a concurrent teardown proved, and would refresh
			// Claim/ClaimSeq, which belong to the contest and to no other writer. A
			// record ending Retired AND Cancelled is a supported shape, resolved by the
			// precedence rule at every reader.
			rebuilt := *cur
			rebuilt.Retired = true
			rec = &rebuilt
			expectSeq = smsg.Sequence
		default:
			// A FOREIGN record (readable, another exchange's) or an UNREADABLE one
			// (loadInflightRecord's second outcome) holds the subject. The two are
			// different facts and this arm deliberately does not distinguish them,
			// because the disposition is the same and fail-closed for both: no
			// retirement is claimed, so the caller keeps its PUBREL.
			//
			// Stated explicitly because the equivalent arm in markInflightRegisterCancelled
			// returns SUCCESS, and review found that unsound for the unreadable half —
			// "our register is gone" is a positive conclusion, and a failed read licenses
			// no positive conclusion. Here nothing positive is concluded, so the two
			// outcomes may share an arm.
			c.Warnf("MQTT: cannot retire the in-flight register on %q for client %q: it no longer holds our record",
				subject, sess.id)
			return 0, false
		}
	}
	c.Warnf("MQTT: unable to record the retirement of QoS 2 source %s/%d for client %q durably: %v",
		dur, sseq, sess.id, lastErr)
	return 0, false
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
	delete(sess.srcAckOwed, key)
	sess.mu.Unlock()
	if cur != nil && cur.regSeq != 0 {
		// A retirement settle owes any attached cancellation verdict its proof (F-087
		// WP3): the Retired-dominates precedence routes a record carrying both here.
		sess.reapRegisterWithVerdict(cur.regSeq, cur.nonce, false)
	}
	c.Debugf("MQTT: retirement of QoS 2 source %s/%d for client %q resolved: the acknowledgement had landed after all",
		key.jsDur, key.sseq, sess.id)
	return true
}

// markInflightRegisterCancelled rewrites a register as CANCELLED, in place, by a CAS at
// its exact sequence.
//
// Same shape and same reasoning as the retirement mark: same nonce so it stays the same
// record, exact sequence so a register a peer has replaced is left alone (R4), bounded and
// reconciled rather than attempted once. What differs is what it proves — not "past
// PUBREC" but "never transmitted", which is the one fact that lets a later resume free the
// identifier instead of holding it.
//
// Session lock NOT held on entry.
func (sess *mqttSession) markInflightRegisterCancelled(c *client, pi uint16, jsDur string, sseq, regSeq uint64, nonce string) error {
	as := sess.asm
	if as == nil || pi == 0 || regSeq == 0 || nonce == _EMPTY_ {
		return fmt.Errorf("no register identity to mark cancelled")
	}
	sess.mu.Lock()
	idHash, gen, regClaim := sess.idHash, sess.gen, sess.regClaim
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return fmt.Errorf("session has no incarnation identity")
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := &mqttInflightRecord{
		PI: pi, Dur: jsDur, Seq: sseq, QoS: 1, Nonce: nonce, Claim: regClaim, Cancelled: true,
	}
	var lastErr error
	expectSeq := regSeq
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		if _, err := as.storeInflightCAS(subject, expectSeq, rec); err == nil {
			return nil
		} else {
			lastErr = err
		}
		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		switch {
		case smsg == nil:
			return nil // gone: the delete landed after all, which is the better outcome
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
			return fmt.Errorf("an unreadable record occupies %q at sequence %d", subject, smsg.Sequence)
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
			return fmt.Errorf("packet identifier %d is held by a record naming another identifier on %q", pi, subject)
		case cur != nil && cur.Nonce == nonce && !sess.ownsSharedInflightRegister(jsDur, cur.Claim):
			sess.abandonForeignSharedInflightRegister(c, jsDur, nonce, subject)
			return nil
		case cur != nil && cur.Nonce == nonce && cur.Cancelled:
			return nil // ours, already marked
		case cur != nil && cur.Nonce == nonce:
			// REBUILD from the reread and set only Cancelled (F-087 WP1). The sparse
			// body above is the first-CAS proposal and stays sound there — the CAS at
			// our own cached sequence IS the proof that nothing intervened — but
			// re-proposing it after a reread would erase a Retired the retirement writer
			// just proved, along with Claim/ClaimSeq the contest owns.
			rebuilt := *cur
			rebuilt.Cancelled = true
			rec = &rebuilt
			expectSeq = smsg.Sequence
		default:
			// A FOREIGN record holds the subject: readable, and naming another exchange.
			// Our register is gone, and this arm is reached only for that case — the
			// UNREADABLE outcome is answered above, because "our register is gone" is a
			// positive conclusion and a failed read licenses none.
			return nil
		}
	}
	return lastErr
}

// secureRetiredSource makes a retirement durable, and reports whether it now is.
//
// Called both where the retirement is created and again at PUBCOMP, because PUBCOMP is
// about to destroy the other evidence: while the exchange's PUBREL record exists a
// restart is safe regardless (recovery reads register-plus-PUBREL as "past PUBREC"), and
// acknowledging that PUBREL away removes it. A retirement that is not durable by then
// must therefore keep its PUBREL.
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
	as := sess.asm
	if as == nil {
		return sess.jsa.confirmAck(ackSubject)
	}
	as.mu.RLock()
	hook := as.testInflightSourceAck
	as.mu.RUnlock()
	if hook != nil {
		return hook(ackSubject)
	}
	return as.jsa.confirmAck(ackSubject)
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
	// retired marks a register that is a QoS 2 retirement rather than a PUBLISH-phase
	// claim. Its cleanup is delete-or-leave: a Cancelled tombstone would overwrite the
	// suppression it carries, and recovery would then drop it without reinstating the
	// retirement — resurrecting a completed payload if the source is still re-offerable.
	retired bool
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
func (sess *mqttSession) deleteInflightRegisters(c *client, tok recordTx, voided []mqttVoidedReg) {
	_ = sess.deleteInflightRegistersClassified(c, tok, voided)
}

// deleteInflightRegistersClassified is deleteInflightRegisters reporting whether its record
// write was rejected by the per-subject CAS. Same reason as
// secureRegCancellationsClassified: this is the SECOND chain that reaches proveVoidedRegs,
// and a rule that covered only the first would leave the hole open through this one.
func (sess *mqttSession) deleteInflightRegistersClassified(c *client, tok recordTx, voided []mqttVoidedReg) bool {
	if !sess.voidRegistersOnStream(c, voided) {
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
func (sess *mqttSession) voidRegistersOnStream(c *client, voided []mqttVoidedReg) bool {
	if len(voided) == 0 || sess.asm == nil {
		return false
	}
	unproven := false
	for _, v := range voided {
		if err := sess.asm.deleteInflightRegisterConfirmed(v.seq); err == nil {
			sess.forgetCancelledReg(v.nonce)
			continue
		}
		if v.retired {
			// Delete-or-leave, never tombstone and never a verdict: this register IS the
			// durable suppression of a completed exchange, and both alternatives would
			// have recovery drop it without reinstating the retirement — resurrecting the
			// payload if the source is still re-offerable. Left in place, recovery
			// re-reads it as a retirement; the residue when the consumer is also gone is
			// a parked retirement, which reserves no identifier and no Receive Maximum
			// slot.
			mqttWarnf(c, "MQTT: leaving the retired-source register at sequence %d for client %q to a later scan", v.seq, sess.id)
			continue
		}
		if v.nonce != _EMPTY_ && v.pi != 0 {
			if merr := sess.markInflightRegisterCancelled(c, v.pi, v.dur, v.sseq, v.seq, v.nonce); merr == nil {
				// The tombstone is on the register itself, which is strictly stronger
				// than a nonce recorded beside it.
				sess.forgetCancelledReg(v.nonce)
				continue
			}
		}
		mqttWarnf(c, "MQTT: deferring the removal of in-flight register %d for client %q", v.seq, sess.id)
		sess.mu.Lock()
		sess.voidRegNonceLocked(v.nonce)
		sess.mu.Unlock()
		unproven = true
	}
	return unproven
}

// voidRegistrationsLocked marks every in-flight register of one consumer durable VOID, because
// this session is abandoning that consumer, and reports the withdrawn allocations for securing.
//
// This is the STAGING half of a consumer teardown, and where it is called from is the whole
// point: inside the transaction that removes the consumer's membership from the record, so
// that the write saying "this subscription is gone" is the same write saying "these
// allocations were never transmitted". Nothing else can close that gap. Keeping the consumer
// when the cancellation cannot be recorded is not equivalent — the membership has already
// gone, so there is no subscription left to redeliver through, and the identifier is stuck
// whether or not the durable survives.
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
// verdict cannot work: shared group durables are named deterministically, so the same name
// comes back on a rejoin, and no amount of withdrawing-and-restoring makes "every register on
// this durable is void" mean the right thing across that boundary — it either resurrects an
// abandoned register or destroys a genuine new one. A nonce is minted with its allocation and
// never reused, so the verdict cannot be misapplied however often the durable returns.
//
// The verdicts on TRANSMITTED registers are returned so a rollback can withdraw them, and
// that asymmetry is deliberate: a rolled-back packet keeps its subscription, so those
// deliveries are still live and still owe their identifiers — voiding them would free
// identifiers the client is holding. A CANCELLED allocation is never reinstated (the source is
// simply re-offered under a fresh one), so its verdict stands either way.
//
// sess.mu must be held.
func (sess *mqttSession) voidRegistrationsLocked(durable string) ([]mqttRegCancellation, []string) {
	var out []mqttRegCancellation
	var transmitted []string
	// RETIRED QoS 2 sources (srcAckOwed) are deliberately NOT voided here, although the
	// teardown will delete their registers too. A retirement's register is not a claim on
	// an identifier — it is the durable SUPPRESSION of a completed-but-unproven exchange —
	// and a void verdict would have recovery drop it without reinstating that suppression.
	// A crash between this packet's save and the consumer delete then leaves the source
	// live and re-offerable, and the completed payload goes out again: a QoS 2 duplicate,
	// which is the worse trade. Left unvoided, the failure residue is instead a register
	// recovery re-reads as a retirement (reinstated, still suppressing) — at worst a
	// permanently parked retirement when the consumer is also gone, which is bounded stale
	// metadata, not a wedged identifier: a retirement reserves no packet identifier and
	// consumes no Receive Maximum slot. deleteInflightRegisters skips the tombstone rung
	// for these for the same reason.
	seqPis := sess.cpending[durable]
	for sseq, pi := range seqPis {
		ack := sess.pendingPublish[pi]
		if ack == nil || ack.regCancelled || ack.regNonce == _EMPTY_ {
			continue
		}
		if !ack.regPending {
			// Transmitted: the register exists (or is being deleted) and its identifier is
			// the teardown reap's to free — which since F-034 it may decline to do, sparing
			// an above-watermark record or refusing on an unreadable one. Record the verdict
			// and leave the rest alone; the verdict is what a later reader needs, and it does
			// not depend on the removal having happened.
			sess.voidRegNonceLocked(ack.regNonce)
			transmitted = append(transmitted, ack.regNonce)
			continue
		}
		out = append(out, mqttRegCancellation{pi: pi, nonce: ack.regNonce, durable: durable, sseq: sseq})
		sess.cancelRegPendingLocked(ack)
		if ack.jsAckSubject != _EMPTY_ {
			if sess.deferred == nil {
				sess.deferred = make(map[mqttDeferredKey]string)
			}
			sess.deferred[mqttDeferredKey{durable, sseq}] = ack.jsAckSubject
		}
		delete(seqPis, sseq)
		sess.pendingPublish[pi] = &mqttPending{regPending: true, regCancelled: true, regNonce: ack.regNonce}
	}
	if len(seqPis) == 0 {
		delete(sess.cpending, durable)
	}
	return out, transmitted
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
	for _, n := range nonces {
		if _, ok := sess.cancelledRegNonces[n]; ok {
			delete(sess.cancelledRegNonces, n)
			sess.voidedRegsRev++
		}
	}
	sess.mu.Unlock()
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
// A shared filter's deliver subject is deterministic (mqttSharedGroupNamesForKey), so the
// abandoned join of a FAILED filter can be barriered by name — which the config form cannot
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
func (sess *mqttSession) stageDeferredDeleteLocked(c *client, cc *ConsumerConfig) ([]mqttRegCancellation, *subscription, []string) {
	if cc == nil {
		return nil, nil, nil
	}
	sub := sess.closeTeardownDeliveryLocked(c, cc)
	cancels, transmitted := sess.voidRegistrationsLocked(cc.Durable)
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
		cancels, sent := sess.voidRegistrationsLocked(dur)
		out = append(out, cancels...)
		transmitted = append(transmitted, sent...)
	}
	// Every shared key this packet leaves. The removals have NOT been applied yet, so the
	// membership that survives is the live map minus this whole set — not minus the one key
	// in hand: a single packet can leave BOTH overlapping filters, and excluding only the
	// current one would let each doomed key protect the other and suppress a release nothing
	// will then owe. F-083.
	doomed := make(map[string]bool, len(filters))
	var sharedKeys []string
	for _, f := range filters {
		if !f.shared {
			continue
		}
		key := f.sessionKey()
		if !doomed[key] {
			doomed[key] = true
			sharedKeys = append(sharedKeys, key)
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
// It also releases this durable's RETIRED sources, which are not in cpending at all: a
// retirement is created after untrackPublishEntry has removed the source's tracking, so a
// teardown that only walked cpending left the retirement and its register behind while
// deleting the consumer that could have settled them. Nothing could then ever re-offer
// that source, and the next resume — seeing a register whose consumer is gone — would
// reserve its identifier indefinitely.
//
// Lock held on entry.
func (sess *mqttSession) purgeConsumerPendingLocked(durable string) ([]mqttVoidedReg, []mqttRegCancellation) {
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
		delete(sess.cpending, durable)
		for sseq, pi := range seqPis {
			if ack := sess.pendingPublish[pi]; ack != nil {
				if ack.regForeign {
					// A QUARANTINED entry contributes NO register sequence to the
					// teardown's ladder — the sequence is the rival's, and settling it
					// violates the standing regForeign rule — and the RESERVATION is
					// kept: only the cpending mapping goes with the consumer, exactly as
					// the recovery demote's shape. Its source-keyed foreignContested
					// entry stays, which is what the pre-allocation decline consults once
					// the mapping is gone (F-087 WP2).
					continue
				}
				addReg(mqttVoidedReg{seq: ack.regSeq, nonce: ack.regNonce, pi: pi, dur: durable, sseq: sseq})
				if sess.condemnSendLocked(pi, mqttSendSettlePurgeTeardown) {
					// CONDEMNED, not deleted (F-087 WP5, class B): this entry's PUBLISH is
					// still being written, and freeing the identifier now would let bumpPI
					// re-mint it under the blocked write. The teardown's durable ladder is
					// unchanged — the sequence above is collected and its rungs run — and
					// endSend performs the memory untrack. The cpending mapping is unlinked
					// as today, below.
					continue
				}
				if ack.regPending {
					// An allocation whose register store is still in flight has no sequence
					// to collect, so the register it may be about to write would survive
					// this teardown unnamed. Two things make it nameable instead: the
					// cancellation nonce, recorded here, and a tombstone the caller writes
					// at its subject before the consumer goes — which is why the identifier
					// stays RESERVED until the store settles. See secureRegCancellations.
					sess.cancelRegPendingLocked(ack)
					cancels = append(cancels, mqttRegCancellation{
						pi: pi, nonce: ack.regNonce, durable: durable, sseq: sseq,
					})
					sess.pendingPublish[pi] = &mqttPending{regPending: true, regCancelled: true, regNonce: ack.regNonce}
					continue
				}
			}
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
	for key, rs := range sess.srcAckOwed {
		if key.jsDur != durable {
			continue
		}
		if rs != nil {
			addReg(mqttVoidedReg{seq: rs.regSeq, nonce: rs.nonce, pi: rs.pi, dur: key.jsDur, sseq: key.sseq, retired: true})
		}
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
// is already in that packet's own record, staged by voidRegistrationsLocked. What
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
func (sess *mqttSession) secureRegCancellations(c *client, tok recordTx, cancels []mqttRegCancellation) bool {
	secured, _ := sess.secureRegCancellationsClassified(c, tok, cancels)
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
func (sess *mqttSession) secureRegCancellationsClassified(c *client, tok recordTx, cancels []mqttRegCancellation) (bool, bool) {
	if !sess.tombstoneCancellationsOnStream(c, cancels) {
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
func (sess *mqttSession) tombstoneCancellationsOnStream(c *client, cancels []mqttRegCancellation) bool {
	owed := false
	for _, rc := range cancels {
		if err := sess.markInflightRegisterCancelledPending(c, rc); err != nil {
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
func (sess *mqttSession) markInflightRegisterCancelledPending(c *client, rc mqttRegCancellation) error {
	as := sess.asm
	if as == nil || rc.pi == 0 || rc.nonce == _EMPTY_ {
		return fmt.Errorf("no register identity to mark cancelled")
	}
	sess.mu.Lock()
	idHash, gen, regClaim := sess.idHash, sess.gen, sess.regClaim
	sess.mu.Unlock()
	if idHash == _EMPTY_ || gen == _EMPTY_ {
		return fmt.Errorf("session has no incarnation identity")
	}
	subject := mqttInflightSubject(idHash, gen, rc.pi)
	sparse := mqttInflightRecord{
		PI: rc.pi, Dur: rc.durable, Seq: rc.sseq, QoS: 1, Nonce: rc.nonce, Claim: regClaim, Cancelled: true,
	}
	rec := &sparse
	var lastErr error
	expectSeq := uint64(0) // create-only: the store's own write may not have landed yet
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		if _, err := as.storeInflightCAS(subject, expectSeq, rec); err == nil {
			return nil
		} else {
			lastErr = err
		}
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
		case cur != nil && cur.Nonce == rc.nonce && !sess.ownsSharedInflightRegister(rc.durable, cur.Claim):
			sess.abandonForeignSharedInflightRegister(c, rc.durable, rc.nonce, subject)
			return nil
		case cur != nil && cur.Nonce == rc.nonce && cur.Cancelled:
			return nil // already marked, by us or by the store's own fallback
		case cur != nil && cur.Nonce == rc.nonce:
			// The store landed. REBUILD from it and set only Cancelled (F-087 WP1):
			// re-proposing the sparse create body would erase whatever the store wrote
			// and whatever a concurrent writer has since proved — a Retired above all,
			// whose loss re-sends a completed payload.
			rebuilt := *cur
			rebuilt.Cancelled = true
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
	// absent while its consumer still holds interest. The exchange is over and the
	// register is garbage.
	mqttInflightSourceComplete
	// mqttInflightSourceUnknown: a lookup or read FAILED. Nothing may be concluded, so the
	// identifier is reserved and counted as ordinary in-flight state — the delivery may
	// well still be live.
	mqttInflightSourceUnknown
	// mqttInflightSourceLost: the source's consumer is GONE. Deliberately not "complete" —
	// a delivery whose consumer was destroyed was lost rather than finished, and the client
	// may still be holding its identifier — but not "unknown" either, because this one is
	// KNOWN to be unrecoverable: nothing will ever re-offer it and no acknowledgement can
	// resolve it. Its identifier is held so it is never reused, while consuming no
	// in-flight quota. NOT exempt from Receive Maximum, though: that limit bounds what
	// the client holds, and a consumer disappearing does not prove the client completed
	// anything (only a cancellation tombstone proves never-transmitted).
	mqttInflightSourceLost
)

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
func (sess *mqttSession) storeInflightRegister(c *client, pi uint16, qos byte, jsDur string, sseq uint64, nonce, q2AffClaim, q2AffCopy string) bool {
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
	sess.mu.Unlock()
	if as == nil || idHash == _EMPTY_ || gen == _EMPTY_ {
		// Unreachable for a real session: every session carries an incarnation
		// generation (mqttSessGenFromRecord rejects a record without one) and an
		// account manager. Fail closed rather than sending under an identifier nothing
		// records.
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
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
		sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		return false
	}
	subject := mqttInflightSubject(idHash, gen, pi)
	rec := &mqttInflightRecord{
		PI:         pi,
		Dur:        jsDur,
		Seq:        sseq,
		QoS:        qos,
		Nonce:      nonce,
		Claim:      regClaim,
		ClaimSeq:   regClaimSeq,
		Q2AffClaim: q2AffClaim,
		Q2AffCopy:  q2AffCopy,
	}

	var lastErr error
	expectSeq := uint64(0)
	for attempt := 0; attempt < mqttInflightStoreAttempts; attempt++ {
		seq, err := as.storeInflightCAS(subject, expectSeq, rec)
		if err == nil {
			return sess.confirmInflightRegister(c, pi, jsDur, sseq, seq, rec.Nonce)
		}
		lastErr = err
		expectSeq = 0

		cur, smsg, lerr := as.loadInflightRecord(subject)
		if lerr != nil {
			lastErr = lerr
			continue
		}
		switch {
		case smsg == nil:
			continue // raced away; retry the create
		case cur != nil && !mqttInflightRecordMatchesSubject(cur, subject):
			// The occupant's body names a DIFFERENT packet identifier than this subject.
			// Recovery classifies exactly this shape as unreadable and RESERVES the
			// identifier; this path must not be more permissive, because every branch
			// below either replaces or deletes on the strength of the body it just read.
			// Erasing it here and then transmitting under this identifier is how a
			// corrupted record becomes a reused identifier the client may still hold.
			// Review of F-034 found this was the third such path.
			c.Warnf("MQTT: refusing packet identifier %d for client %q: the record on %q at "+
				"sequence %d names packet identifier %d, which that subject does not",
				pi, sess.id, subject, smsg.Sequence, cur.PI)
			sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
			return false
		case cur != nil && cur.Nonce == rec.Nonce && !sess.ownsSharedInflightRegister(jsDur, cur.Claim):
			// The write belongs to this allocation, but a resumed peer has since
			// claimed it. Learning that peer's sequence would let this stale object
			// delete the durable identity after the peer recovered it.
			sess.abandonForeignSharedInflightRegister(c, jsDur, nonce, subject)
			sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
			return false
		case cur != nil && cur.Nonce == rec.Nonce:
			// Our own write, whose response we lost. This is the whole reason the nonce
			// exists: the sequence cannot tell us this, since the subject is reused.
			return sess.confirmInflightRegister(c, pi, jsDur, sseq, smsg.Sequence, rec.Nonce)
		case cur != nil && cur.Cancelled && !cur.Retired:
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
			if !sess.reapRegisterWithVerdict(smsg.Sequence, cur.Nonce, true) {
				c.Errorf("MQTT: unable to reap a cancelled in-flight register on %q for client %q",
					subject, sess.id)
				sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
				return false
			}
		case cur != nil && cur.Dur == jsDur && cur.Seq == sseq && cur.Retired:
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
				sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
			}
			return false
		case cur != nil && cur.Dur == jsDur && cur.Seq == sseq &&
			sess.ownsSharedInflightRegister(jsDur, cur.Claim):
			// Someone else recorded exactly what we are recording, on a register that is
			// OURS to rewrite — any plain durable, or a shared one whose claim equals
			// ours. Take the slot over at its exact sequence so the record carries OUR
			// nonce, rather than adopting a record whose writer may still delete it. The
			// CAS at the exact sequence read is what makes this sound: if the occupant
			// gains Retired or Cancelled after our read, it rejects and the loop rereads
			// into the arms above.
			c.Debugf("MQTT: replacing an in-flight register for the same delivery on %q written elsewhere", subject)
			expectSeq = smsg.Sequence
		case cur != nil && cur.Dur == jsDur && cur.Seq == sseq:
			// The occupant is live and NOT ours — foreign, or an UNCLAIMED shared
			// register, which this test deliberately does not treat as ours (unclaimed
			// replacement is the three-owner resurrection the current code names, and
			// this item does not reopen it).
			//
			// QUARANTINE IN PLACE rather than abandon (F-087 WP2): abandoning untracks
			// the entry AND the cpending mapping, so the next AckWait offer allocates a
			// DIFFERENT identifier and transmits the same source while the foreign
			// register stands — two identifiers for one message, on a live connection
			// that may never resume.
			// The QUARANTINE goes in FIRST, and the durable bookkeeping after. These are
			// two lock-taking operations, and a consumer teardown running between them
			// would leave the allocation withdrawn with no contest recorded: a
			// re-subscribe then re-offers the source, trackPublish finds neither a
			// mapping nor a contest, and one message goes out under a second identifier
			// while the rival register still owns the first.
			sess.quarantineForeignInflight(c, pi, jsDur, sseq, nonce, subject, smsg.Sequence)
			sess.abandonForeignSharedInflightRegister(c, jsDur, nonce, subject)
			return false
		case cur == nil:
			// Unreadable. It cannot be attributed to any delivery, so it cannot be
			// proven stale either — and it occupies an identifier we must not use.
			c.Warnf("MQTT: refusing packet identifier %d for client %q: an unreadable in-flight register occupies %q at sequence %d",
				pi, sess.id, subject, smsg.Sequence)
			sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
			return false
		default:
			if as.inflightSourceState(cur) != mqttInflightSourceComplete {
				// A live or unprovable delivery owns this identifier. Never reaped: on
				// another broker this record is that delivery's only durable identity.
				c.Warnf("MQTT: refusing packet identifier %d for client %q: %q still names delivery %s/%d",
					pi, sess.id, subject, cur.Dur, cur.Seq)
				sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
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
			if !sess.reapRegisterWithVerdict(smsg.Sequence, cur.Nonce, true) {
				c.Errorf("MQTT: unable to reap a stale in-flight register on %q for client %q",
					subject, sess.id)
				sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
				return false
			}
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("in-flight register not resolved after %d attempts", mqttInflightStoreAttempts)
	}
	c.Errorf("MQTT: unable to record the in-flight register for packet identifier %d of client %q: %v",
		pi, sess.id, lastErr)
	sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
	return false
}

// confirmInflightRegister stamps a confirmed register onto its pending entry and
// clears regPending, releasing the entry for transmission.
//
// If the entry has gone (a discard, a takeover, or a terminal path that ran while the
// store was in flight) the register is deleted immediately: it names an identifier
// nothing is tracking, and leaving it would make the next resume reserve an
// identifier for a delivery that has no pending entry.
func (sess *mqttSession) confirmInflightRegister(c *client, pi uint16, jsDur string, sseq, regSeq uint64, nonce string) bool {
	sess.mu.Lock()
	ack := sess.pendingPublish[pi]
	if ack == nil || !ack.regPending || ack.jsDur != jsDur || ack.sseq != sseq {
		sess.mu.Unlock()
		// The allocation was CANCELLED under us — an UNSUBSCRIBE staging a drain, a
		// discard, a takeover — so this register describes a delivery that will never be
		// sent. Removing it is CONFIRMED and retried, unlike the ordinary terminal delete
		// which may be fire-and-forget: there the record is reconcilable, because its
		// source is still live and classifiable. Here the consumer is usually being deleted
		// in the same breath, and a register whose consumer is gone is deliberately NOT read
		// as complete — so a lost delete leaves an identifier reserved for the life of the
		// session, with no later pass able to resolve it.
		//
		// proven means SOME durable rung of the ladder holds: the register is gone, or it
		// is tombstoned, or its nonce verdict landed on the session record. The
		// reservation the teardown left on this identifier is released only then — the
		// store has settled here, which is the other half of the release condition, but a
		// PI freed with no durable verdict anywhere would let a crash resurrect this
		// register as an ordinary delivery holding a Receive Maximum slot forever.
		proven := false
		if sess.asm != nil {
			if derr := sess.asm.deleteInflightRegisterConfirmed(regSeq); derr != nil {
				// The delete is what this path wants, and it could not be proven. So the fact
				// is made DURABLE instead: a cancellation tombstone on the record itself,
				// which is the only thing that can tell a later resume that this delivery was
				// never transmitted. Without it the surviving register is indistinguishable
				// from a started delivery, and — since the consumer that would classify its
				// source is deleted in the same breath — its identifier would be reserved,
				// and its Receive Maximum slot consumed, for the life of the session.
				if merr := sess.markInflightRegisterCancelled(c, pi, jsDur, sseq, regSeq, nonce); merr != nil {
					// Neither register-stream rung could be proven — one storage domain, so
					// they usually fail together. The NONCE verdict is the remaining home:
					// kept in memory for this connection and persisted with the session
					// record, so a restart still knows this register describes a delivery
					// that was never transmitted.
					c.Warnf("MQTT: deferring cleanup of the in-flight register of cancelled packet identifier %d for client %q: %v (%v)",
						pi, sess.id, derr, merr)
					sess.mu.Lock()
					sess.voidRegNonceLocked(nonce)
					sess.mu.Unlock()
					proven = sess.persistVoidedRegs(c)
				} else {
					// The tombstone is on the register itself, which is strictly stronger than
					// a nonce recorded beside it.
					sess.forgetCancelledReg(nonce)
					proven = true
				}
			} else {
				// Provably gone. There is nothing left for recovery to misread.
				sess.forgetCancelledReg(nonce)
				proven = true
			}
		}
		sess.mu.Lock()
		// Only OUR reservation: after a stall the identifier may already belong to a
		// newer allocation's cancellation, whose own settle point owes this decision.
		if cur := sess.pendingPublish[pi]; cur != nil && cur.regCancelled && cur.regNonce == nonce {
			if proven || sess.voidVerdictProvenLocked(nonce) {
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
	ack.regSeq, ack.regNonce, ack.regPending = regSeq, nonce, false
	sess.mu.Unlock()
	return true
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
func (sess *mqttSession) abandonInflightRegister(pi uint16, jsDur string, sseq uint64, nonce string) {
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
				delete(sess.pendingPublish, pi)
			} else {
				ack.regPending = false
				sess.parkSettledVoidReservationLocked(nonce, pi)
			}
		}
		return
	}
	ackSubject := ack.jsAckSubject
	sess.untrackPublishEntry(pi)
	if ackSubject == _EMPTY_ {
		return
	}
	if sess.deferred == nil {
		sess.deferred = make(map[mqttDeferredKey]string)
	}
	sess.deferred[mqttDeferredKey{jsDur, sseq}] = ackSubject
}

// mqttContestClass is the FINAL disposition a resume's register walk reached for one
// source delivery — final in the sense of post-routing: after the contest, the
// duplicate-source grouping, the poisoned demote and the retirement installs. A raw
// claim success is deliberately not one of these, because it is too early a
// classification: an owned record may be newly retired, and an owned-live one may still
// be poisoned by a sibling naming the same delivery (F-087 WP2).
//
// Ordered by PRECEDENCE, lowest first: a source observed in two classes takes the
// highest.
type mqttContestClass uint8

const (
	// mqttContestUnobserved is the zero value: the walk reached no routed result for
	// this source. It is deliberately unnamed at any use site — a map lookup that misses
	// yields it, which is exactly the "nothing was decided" reading — and it is FIRST so
	// every named class outranks it in the precedence order below.
	mqttContestUnobserved mqttContestClass = iota //nolint:unused // the zero value, reached by a map miss
	// mqttContestClosed: a consumed close decision, or a register whose source is
	// provably complete. Nothing durable reserves the identifier any more.
	mqttContestClosed
	// mqttContestOwnedLive: one owned, live, unpoisoned register — the ordinary
	// PUBLISH-phase delivery this session may serve again.
	mqttContestOwnedLive
	// mqttContestPoisoned: the group could not be resolved, a claim failed, or the
	// record could not be interpreted. Fail closed.
	mqttContestPoisoned
	// mqttContestRetired: the exchange is past PUBREC; a suppression answers the
	// source's offers and no identifier is reserved for it.
	mqttContestRetired
	// mqttContestPubRelOwned: a surviving PUBREL record owns the identifier (R2). The
	// HIGHEST precedence: the PUBREL is the exchange's identity once the PUBLISH phase
	// is over, and it outranks whatever the register walk made of the register.
	mqttContestPubRelOwned
)

// mqttContestDisposition is one final disposition plus the evidence behind it.
type mqttContestDisposition struct {
	class mqttContestClass
	reg   mqttRecoveredInflightReg
	rel   mqttStoredPubRel
}

// mqttDispositionNamesPI reports whether a routed disposition is evidence about THIS
// packet identifier, rather than about another one that happens to name the same source.
//
// The distinction is not pedantic: a source and an identifier are different identities,
// and one delivery can legitimately have been re-offered under a second identifier after
// the first was consumed. A disposition that names another identifier says nothing about
// this one (F-087 WP2).
func mqttDispositionNamesPI(d mqttContestDisposition, pi uint16) bool {
	switch d.class {
	case mqttContestUnobserved:
		return false
	case mqttContestPubRelOwned:
		return d.rel.pi == pi
	default:
		return d.reg.rec != nil && d.reg.rec.PI == pi
	}
}

// mqttStagedRelease is a quarantine the resume's reconciliation decided to RELEASE,
// held until the resume has actually succeeded.
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
	key    mqttDeferredKey // the contested source
	pi     uint16          // the quarantined identifier
	regSeq uint64          // the rival record's sequence as the quarantine recorded it
}

// reconcileForeignContestsLocked disposes every quarantine by the walk's FINAL result
// class for its source, and returns the releases to be committed once the resume
// succeeds.
//
// Session lock held on entry.
func (sess *mqttSession) reconcileForeignContestsLocked(c *client, final map[mqttDeferredKey]mqttContestDisposition,
	observedPI map[uint16]bool, walkID uint64, installPubRel func(mqttStoredPubRel),
) []mqttStagedRelease {
	if len(sess.foreignContested) == 0 {
		return nil
	}
	var staged []mqttStagedRelease
	remove := func(key mqttDeferredKey, fc *mqttForeignContest) {
		if ack := sess.pendingPublish[fc.pi]; ack != nil && ack.regForeign &&
			ack.jsDur == key.jsDur && ack.sseq == key.sseq {
			delete(sess.pendingPublish, fc.pi)
		}
		if m := sess.cpending[key.jsDur]; m != nil && m[key.sseq] == fc.pi {
			delete(m, key.sseq)
		}
		delete(sess.foreignContested, key)
	}
	for key, fc := range sess.foreignContested {
		// ELIGIBILITY FIRST, before any disposition is consulted. A quarantine installed
		// while this walk was running was never covered by it, and the walk's result for
		// that SOURCE describes an EARLIER register — the one the walk read — not the
		// rival register this quarantine is contesting. Applying it would dispose of a
		// contest on evidence that predates the contest: a Closed result would stage a
		// release, freeing the identifier while the new foreign register still stands,
		// and a later offer for the same source would then transmit under a second
		// identifier. Keeping is fail-closed and converges — the NEXT resume's walk
		// covers this quarantine and decides it on evidence of its own.
		if fc.walk >= walkID {
			c.Debugf("MQTT: keeping the quarantined packet identifier %d of client %q: it was installed while "+
				"this resume's walk was running, so nothing this walk observed describes it", fc.pi, sess.id)
			continue
		}
		// The disposition index is keyed by the SOURCE, and a source is not an
		// identifier: the walk may have found this delivery's register on a DIFFERENT
		// packet identifier — its rival consumed the quarantined one through the
		// cancellation lifecycle and the never-transmitted source was re-offered under
		// another. Applying that record here would install its sequence and nonce into
		// OUR identifier's entry and remap the source to it, so a redelivery transmits
		// under this identifier while the durable register and its reservation sit on the
		// other one. Evidence that does not name THIS identifier decides nothing about
		// it; it falls to the subject test below, which is about this identifier alone.
		d := final[key]
		if !mqttDispositionNamesPI(d, fc.pi) {
			d = mqttContestDisposition{}
		}
		// A SOURCE-ONLY contest owns no reservation, so none of the entry-shaped arms
		// applies to it: the identifier is not this contest's to install into, replace or
		// free. What it still owns is the DECLINE, so the only question is whether the
		// contest is settled — and the same classes answer it, reduced to keep or drop.
		if !fc.reserved {
			switch d.class {
			case mqttContestPoisoned, mqttContestUnobserved:
				if d.class == mqttContestPoisoned || observedPI[fc.pi] {
					continue // unresolved, or a register still stands there: keep declining
				}
				staged = append(staged, mqttStagedRelease{key: key, pi: fc.pi, regSeq: fc.regSeq})
			case mqttContestOwnedLive, mqttContestPubRelOwned:
				// The source is this session's to serve again, and recovery's own loops
				// installed the entry (nothing of ours was resident to make them skip it).
				// The decline must go with the contest or it would suppress a delivery
				// this pass just restored.
				delete(sess.foreignContested, key)
			default:
				staged = append(staged, mqttStagedRelease{key: key, pi: fc.pi, regSeq: fc.regSeq})
			}
			continue
		}
		// The rule the arms below obey: an arm that leaves the identifier HELD — as a
		// live entry, or as a PUBREL reservation on the same identifier — applies now,
		// because recovery's other installs are pre-CONNACK too and a refused resume
		// leaves them harmlessly in memory. An arm that would FREE the identifier stages
		// instead, and the commit runs only once the resume has succeeded.
		switch d.class {
		case mqttContestPubRelOwned:
			// The rival progressed the contested exchange past PUBREC and its PUBREL
			// survives, so the identifier is the PUBREL's (R2). The ordinary restore loop
			// SKIPPED it — that loop passes over an identifier whose pendingPublish entry
			// is resident, and a quarantine is exactly that — so removing the quarantine
			// without installing the reservation here would lose it outright.
			remove(key, fc)
			installPubRel(d.rel)
		case mqttContestOwnedLive:
			// Reclaimed: the recovered entry REPLACES the quarantine. This is the one
			// place a regForeign entry is replaced, keyed by the contested tuple rather
			// than by a nonce — the ordinary reserve install's nonce guard deliberately
			// skips a reservation, whose nonce is empty.
			sess.pendingPublish[fc.pi] = &mqttPending{
				jsDur:      d.reg.rec.Dur,
				sseq:       d.reg.rec.Seq,
				q2AffClaim: d.reg.rec.Q2AffClaim,
				q2AffCopy:  d.reg.rec.Q2AffCopy,
				regSeq:     d.reg.seq,
				regNonce:   d.reg.rec.Nonce,
			}
			if m := sess.cpending[key.jsDur]; m != nil {
				m[key.sseq] = fc.pi
			} else {
				sess.cpending[key.jsDur] = map[uint64]uint16{key.sseq: fc.pi}
			}
			delete(sess.foreignContested, key)
		case mqttContestRetired:
			// A retirement reserves no identifier — the client completed the exchange
			// under it — so this quarantine's identifier is free, and the suppression the
			// retired apply installed is what answers the source's offers from here.
			//
			// But FREEING is exactly what may not happen before the resume succeeds
			// (a refusal after this point preserves the session and its maps), so it is
			// STAGED like every other release. The suppression stands either way, which
			// is what keeps the source from taking a second identifier in the meantime.
			staged = append(staged, mqttStagedRelease{key: key, pi: fc.pi, regSeq: fc.regSeq})
		case mqttContestPoisoned:
			// Fail closed, agreeing with the group's own fail-closed state: the
			// quarantine is refreshed at the newly observed sequence and KEPT.
			fc.regSeq = d.reg.seq
			if ack := sess.pendingPublish[fc.pi]; ack != nil && ack.regForeign {
				ack.regSeq = d.reg.seq
			}
		case mqttContestClosed:
			// STAGED, not released (F-087 WP2). The absence or the consumed close is a
			// sound decision here — inside the barrier, with the walk and the
			// duplicate-source reap behind it — but applying it now would free the
			// identifier even if this resume is refused after the walk, which every
			// post-recovery refusal path allows (the session object and its maps survive
			// mqttAbortAcceptedConnect). The commit runs past every refusal return.
			staged = append(staged, mqttStagedRelease{key: key, pi: fc.pi, regSeq: fc.regSeq})
		default:
			// No routed result for this source. Two states share that, and they must not
			// share a disposition:
			//
			//   - the walk ENUMERATED this identifier's subject and found no register
			//     there. The contest is settled: the rival completed and its terminal
			//     path removed the record. That is a sound decision HERE and only here —
			//     inside the barrier, with the complete walk and the duplicate-source reap
			//     behind it — so it is STAGED for release;
			//   - the walk never covered that identifier's subject at all. Nothing was
			//     decided about it, so it is KEPT and waits for the next reconciliation.
			//     (A quarantine installed DURING this walk never reaches here: the
			//     eligibility fence at the top of the loop keeps it, whatever class its
			//     source was routed to.)
			if !observedPI[fc.pi] {
				staged = append(staged, mqttStagedRelease{key: key, pi: fc.pi, regSeq: fc.regSeq})
				continue
			}
			c.Debugf("MQTT: keeping the quarantined packet identifier %d of client %q: this resume's walk decided "+
				"nothing about delivery %s/%d", fc.pi, sess.id, key.jsDur, key.sseq)
		}
	}
	return staged
}

// commitStagedReleases frees the identifiers a resume's reconciliation staged, and is
// called only once that resume has SUCCEEDED — past the success CONNACK, past the forced
// leave and its refusals, immediately before the restore verdict opens.
//
// Each release is re-fenced on the EXACT tuple its walk recorded: the same source-keyed
// quarantine still installed, the same regForeign reservation for the same (durable,
// sequence, register sequence), and the cpending mapping — where one exists — still
// naming this identifier. A post-purge absence is not a mismatch, because the purge
// deliberately keeps the reservation and drops only the mapping.
//
// A fence MISS keeps the quarantine, and that converges: the next resume's walk
// re-observes the same state and stages it again.
//
// Session lock NOT held on entry.
func (sess *mqttSession) commitStagedReleases(c *client, staged []mqttStagedRelease) {
	if len(staged) == 0 {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, sr := range staged {
		fc := sess.foreignContested[sr.key]
		if fc == nil || fc.pi != sr.pi || fc.regSeq != sr.regSeq {
			continue
		}
		if !fc.reserved {
			// A contest that never owned a reservation — a teardown withdrew the
			// allocation before the quarantine could be installed. There is no entry to
			// fence against and none to remove; the contest key IS the whole of it, and
			// demanding a reservation here would reject the release at every resume and
			// leave the source declined until the generation retired.
			delete(sess.foreignContested, sr.key)
			c.Debugf("MQTT: releasing the source-only quarantine of delivery %s/%d for client %q: it is settled",
				sr.key.jsDur, sr.key.sseq, sess.id)
			continue
		}
		ack := sess.pendingPublish[sr.pi]
		if ack == nil || !ack.regForeign || ack.jsDur != sr.key.jsDur ||
			ack.sseq != sr.key.sseq || ack.regSeq != sr.regSeq {
			continue
		}
		// Only OUR OWN mapping is ours to remove, and a mapping naming ANOTHER identifier
		// must not veto this release. That source may legitimately have been re-offered
		// under a second identifier after the rival consumed this one, and recovery
		// installs that newer entry before this commit runs — so treating the difference
		// as a fence mismatch would refuse the release at every resume, forever, leaving
		// this identifier reserved and counted while nothing on the live connection ever
		// retries it. The reservation itself is already fenced by the exact-tuple check
		// above, which is what proves the entry being removed is the quarantine's.
		if m := sess.cpending[sr.key.jsDur]; m != nil {
			if pi, mapped := m[sr.key.sseq]; mapped && pi == sr.pi {
				delete(m, sr.key.sseq)
			}
		}
		delete(sess.pendingPublish, sr.pi)
		delete(sess.foreignContested, sr.key)
		c.Debugf("MQTT: releasing the quarantined packet identifier %d of client %q: delivery %s/%d is settled",
			sr.pi, sess.id, sr.key.jsDur, sr.key.sseq)
	}
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
// Session lock held on entry.
func (sess *mqttSession) beginSendLocked(cc *client, pi uint16, jsDur string, sseq uint64) (string, bool) {
	if cc == nil || pi == 0 || sess.c != cc {
		return _EMPTY_, false
	}
	ack := sess.pendingPublish[pi]
	if ack == nil || ack.jsDur != jsDur || ack.sseq != sseq {
		return _EMPTY_, false
	}
	if ack.regPending || ack.regCancelled || ack.regForeign {
		return _EMPTY_, false // nothing may be transmitted under any of these (R1, WP2)
	}
	if ack.sendNonce != _EMPTY_ || ack.sendSettle.kind != mqttSendSettleNone {
		return _EMPTY_, false // a section is already active, or one parked a settle
	}
	// LIVENESS, not presence. An inert hold — its write returned, its acknowledgement
	// debt owed to a connection that is no longer ours — reserves nothing, and bumpPI
	// will hand this identifier out. Refusing here on presence is how that identifier,
	// its register and a Receive Maximum slot get stranded for the session's life:
	// nothing ever transmits, so nothing ever removes the hold, and every redelivery
	// meets the same refusal (F-089). An inert hold is TAKEN OVER instead.
	if sess.sendReservedLocked(pi) {
		return _EMPTY_, false
	}
	// A per-send token, not a counter: recovery can REPLACE the entry, and a
	// per-entry counter would restart with it and let a stale endSend match.
	token := mqttNewInflightNonce()
	ack.sendNonce = token
	if sess.sendsInFlight == nil {
		sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
	}
	// The takeover is a REPLACEMENT, so an inert debt cannot be inherited by this
	// transmission. Nothing is lost with it: an inert hold reserves nothing for
	// anybody, so keeping it and dropping it are indistinguishable to every reader.
	sess.sendsInFlight[pi] = &mqttSendHold{token: token}
	return token, true
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
	if cc == nil || pi == 0 || cc.cid == 0 || sess.c != cc {
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
	// NO retained-term refusal here, and NO fence on a live pendingPublish entry. PUBREL
	// ADMISSION IS DELIBERATELY UNCHANGED FROM BEFORE F-091, and that is a reviewed scope
	// decision (lane DECISIONS.md, D4) rather than an omission.
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
	//   - refusing on ANY live pendingPublish entry breaks the UNSUBSCRIBE drain, which
	//     legitimately recreates a CORRELATED PUBREL under a recovered entry's identifier
	//     (measured: three drain tests timed out waiting for a PUBREL the client was owed);
	//   - refusing on a SOURCE-LESS entry starves an abandoned frame's record for the life of
	//     the connection, since that empty entry survives until the next resume.
	//
	// The stale-PUBREL multiplexing hazard is real and is owned WHOLLY by F-092, whose fence
	// correlates the record's (durable, source sequence) against the entry holding the
	// identifier — a decision only the delivery callback can make, because only it parses the
	// correlation. F-091's reservation still stops bumpPI and any PUBLISH send section from
	// reusing the identifier, which is what it was chartered to do.
	// A standing debt is only ours to add to when this connection incurred it. One owed by a
	// predecessor can never be answered — mqttProcessPubComp refuses a PUBCOMP from any
	// connection that is not sess.c — so it is RESET rather than accumulated onto. Adding to
	// it instead is what would leave a resumed session's identifier held forever: the client
	// owes exactly one PUBCOMP on this connection, and a count of two would never reach zero.
	if h == nil || h.owner != cc.cid {
		h = &mqttSendHold{owner: cc.cid}
		if sess.sendsInFlight == nil {
			sess.sendsInFlight = make(map[uint16]*mqttSendHold, 1)
		}
		sess.sendsInFlight[pi] = h
	}
	token := mqttNewInflightNonce()
	h.token = token
	h.owner = cc.cid
	h.pubCompOwed++
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

// releaseRetainedSendWrittenLocked ends the reservation of a retained frame whose bytes
// have gone out — but only while the incarnation that allocated it is still current.
//
// Past a clear() this hold is the identifier's ONLY reservation: the map that would
// otherwise carry custody on to PUBACK/PUBREC was nilled and last_pi was reset, so the
// write returning does not free the identifier and releasing here would put a second
// PUBLISH under it on the same socket. Held instead, it goes inert when ownership moves
// and is swept there (F-091).
//
// Session lock held on entry.
func (sess *mqttSession) releaseRetainedSendWrittenLocked(cc *client, pi uint16) {
	h := sess.sendsInFlight[pi]
	// Another connection's reservation is not ours to release: it already reserves
	// nothing, and leaving it costs nothing. Same rule as consumePubRelAckLocked.
	if h == nil || cc == nil || cc.cid == 0 || h.owner != cc.cid || h.retainedGen == _EMPTY_ {
		return
	}
	if h.retainedGen != sess.gen {
		return
	}
	sess.dropRetainedSendLocked(pi, h)
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
// Session lock held on entry.
func (sess *mqttSession) releaseRetainedSendAbandonedLocked(cc *client, pi uint16) {
	h := sess.sendsInFlight[pi]
	if h == nil || cc == nil || cc.cid == 0 || h.owner != cc.cid || h.retainedGen == _EMPTY_ {
		return
	}
	sess.dropRetainedSendLocked(pi, h)
}

// dropRetainedSendLocked clears the retained term and removes the hold once it reserves
// nothing more. The removal asks the shared predicate rather than re-spelling it, which
// is what keeps a co-resident write token or PUBCOMP debt from being discarded with it.
//
// Session lock held on entry.
func (sess *mqttSession) dropRetainedSendLocked(pi uint16, h *mqttSendHold) {
	h.retainedGen = _EMPTY_
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
}

// endRetainedSends records that these buffered retained frames have been WRITTEN, which
// is the event that MAY end their reservation — see releaseRetainedSendWrittenLocked for
// when it does not.
//
// Its ONLY callers are the two flushers. A departure never comes here: an abandoned frame
// was not written, so it takes the UNFENCED verb through releaseDetachedRetainedFrames →
// abandonRetainedSends instead, and conflating the two would apply the generation fence to a
// frame no client ever saw.
//
// Returns immediately on an empty list WITHOUT taking the session lock.
//
// Session lock NOT held on entry: both flushers hold the CONNECTION lock while they take the
// frames and must release it before reaching for sess.mu.
func (sess *mqttSession) endRetainedSends(cc *client, pis []uint16) {
	if len(pis) == 0 {
		return
	}
	sess.mu.Lock()
	for _, pi := range pis {
		sess.releaseRetainedSendWrittenLocked(cc, pi)
	}
	sess.mu.Unlock()
}

// abandonRetainedSends is endRetainedSends' counterpart for frames that will never be
// written, and it is the reason the two verbs are separate: this one is not fenced by the
// generation.
//
// Session lock NOT held on entry.
func (sess *mqttSession) abandonRetainedSends(cc *client, pis []uint16) {
	if len(pis) == 0 {
		return
	}
	sess.mu.Lock()
	for _, pi := range pis {
		sess.releaseRetainedSendAbandonedLocked(cc, pi)
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
	return h != nil && (h.token != _EMPTY_ || sess.pubCompHeldLocked(h) || sess.retainedHeldLocked(h))
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
	// The hold goes only when it reserves nothing more. A PUBREL transmitted under this
	// identifier and not yet answered keeps it, because the write returning is not the
	// event that frees a PUBREL's identifier — its PUBCOMP is (F-089).
	if !sess.sendHoldLiveLocked(h) {
		delete(sess.sendsInFlight, pi)
	}
	var settle mqttSendSettle
	var removed *mqttPending
	if ack := sess.pendingPublish[pi]; ack != nil && ack.sendNonce == token {
		ack.sendNonce = _EMPTY_
		settle, ack.sendSettle = ack.sendSettle, mqttSendSettle{}
		switch settle.kind {
		case mqttSendSettleNone:
		default:
			// Every parked continuation frees the identifier; what differs is the durable
			// half below.
			removed = sess.untrackPublishEntry(pi)
		}
	}
	sess.mu.Unlock()
	if removed == nil {
		return
	}
	switch settle.kind {
	case mqttSendSettleAckComplete, mqttSendSettlePubRecFailed, mqttSendSettleDenyRelease:
		// The acknowledgement's (or the denial's) identifier-freeing half, deferred: the
		// protocol answer already went out, and this is the register delete it owed.
		sess.deleteInflightRegisterFor(removed)
	case mqttSendSettlePurgeTeardown, mqttSendSettleRecoveryDrop:
		// The teardown's ladder and the recovery pass already issued every durable delete
		// they owed; only the memory untrack was deferred.
		if c != nil {
			c.Debugf("MQTT: released packet identifier %d of client %q after its transmission settled", pi, sess.id)
		}
	}
}

// condemnSendLocked parks a removal path's continuation on an entry whose transmission
// has not returned, and reports whether it did.
//
// The FIRST parked settle wins. A later remover still performs its own durable work —
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
	if ack.sendSettle.kind == mqttSendSettleNone {
		ack.sendSettle = mqttSendSettle{kind: kind, regSeq: ack.regSeq, nonce: ack.regNonce}
	}
	return true
}

// quarantineForeignInflight converts our own pending allocation into a FOREIGN
// reservation, keeping the identifier reserved and the source's cpending mapping
// intact, and records the contest.
//
// The identifier is held rather than freed because freeing it is what mints a second
// identifier for one message: abandonInflightRegister removes the cpending mapping, and
// the source's next AckWait offer then allocates afresh while the rival's register
// stands. Held, the source is declined at every later offer and one message keeps one
// identity.
//
// Keyed by the SOURCE, not the identifier: a consumer purge unlinks the mapping, and a
// re-subscribe re-offers the same delivery, so a pi-keyed record would be invisible to
// trackPublish at exactly the moment it must decline.
//
// There is deliberately no live release. The quarantine's only removers are the next
// SUCCESSFUL resume's final-disposition reconciliation and session-generation
// retirement (F-087 WP2).
//
// Session lock NOT held on entry.
func (sess *mqttSession) quarantineForeignInflight(c *client, pi uint16, jsDur string, sseq uint64,
	nonce, subject string, regSeq uint64,
) {
	sess.mu.Lock()
	ack := sess.pendingPublish[pi]
	if ack == nil || !ack.regPending || ack.jsDur != jsDur || ack.sseq != sseq ||
		ack.regNonce != nonce || ack.regCancelled {
		// Our allocation is no longer the thing in this slot. A teardown that WITHDREW it
		// is the case that matters, and it is why the contest is still recorded here: the
		// consumer is gone, so nothing re-offers the source now — but a re-subscribe
		// will, and with neither a cpending mapping nor a contest to consult,
		// trackPublish would allocate a SECOND identifier for a delivery whose first one
		// the rival's register still names.
		//
		// The ENTRY is left exactly as it is. A cancelled reservation carries the
		// settled/proof bookkeeping the cancellation owes and installing a foreign
		// reservation over it would erase that; the store's own settle edge runs instead
		// (below), and the foreign register simply stands for the next resume.
		// SOURCE-ONLY: the entry is the teardown's, so this contest owns no reservation
		// and a release must remove it alone.
		ours := ack != nil && ack.regNonce == nonce
		if ours {
			sess.recordForeignContestLocked(jsDur, sseq, pi, subject, regSeq, false)
		}
		cancelled := ours && ack.regCancelled
		sess.mu.Unlock()
		if cancelled {
			sess.abandonInflightRegister(pi, jsDur, sseq, nonce)
		}
		return
	}
	defer sess.mu.Unlock()
	sess.pendingPublish[pi] = &mqttPending{
		jsDur:      jsDur,
		sseq:       sseq,
		regSeq:     regSeq,
		regForeign: true,
	}
	sess.recordForeignContestLocked(jsDur, sseq, pi, subject, regSeq, true)
	c.Warnf("MQTT: quarantining packet identifier %d for client %q: delivery %s/%d is recorded on %q by another "+
		"writer, so the identifier is reserved until this session resumes", pi, sess.id, jsDur, sseq, subject)
}

// recordForeignContestLocked records a lost same-delivery contest against the SOURCE.
//
// Separated from the reservation install because the two do not always happen together:
// a teardown that withdrew our allocation before this ran leaves no reservation of ours
// to convert, but the contest must still stand — a re-subscribe re-offers that source,
// and the contest is the only thing left that can decline it.
//
// Session lock held on entry.
func (sess *mqttSession) recordForeignContestLocked(jsDur string, sseq uint64, pi uint16,
	subject string, regSeq uint64, reserved bool,
) {
	if sess.foreignContested == nil {
		sess.foreignContested = make(map[mqttDeferredKey]*mqttForeignContest, 1)
	}
	sess.foreignContested[mqttDeferredKey{jsDur, sseq}] = &mqttForeignContest{
		pi: pi, subject: subject, regSeq: regSeq, walk: sess.inflightWalks, reserved: reserved,
	}
}

// sourceForeignContestedLocked reports whether a source delivery is quarantined.
//
// Session lock held on entry.
func (sess *mqttSession) sourceForeignContestedLocked(jsDur string, sseq uint64) bool {
	if len(sess.foreignContested) == 0 {
		return false
	}
	_, contested := sess.foreignContested[mqttDeferredKey{jsDur, sseq}]
	return contested
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
	already := sess.piSpaceIncomplete
	sess.piSpaceIncomplete = true
	sess.mu.Unlock()
	if !already {
		c.Warnf("MQTT: withholding new outbound QoS 1/2 deliveries for client %q: %s, so the packet identifier space is not fully known. "+
			"Deliveries already in flight are unaffected and allocation resumes on a resume that completes the scan.", sess.id, why)
	}
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
)

// claimRecoveredSharedInflight makes the recovery-versus-close decision durable
// at each shared register's own subject sequence. A successful rewrite invalidates
// every sequence held by an older session object; a close that deleted or
// tombstoned first is consumed as closed. Any state that cannot prove either fact
// becomes opaque, so the identifier is withheld rather than guessed free.
//
// The writes are independent by subject and run in a bounded window. This remains
// part of the pre-CONNACK barrier without turning a large resume into one serial
// round trip per register.
func (sess *mqttSession) claimRecoveredSharedInflight(c *client, in []mqttRecoveredInflightReg) (
	owned, closed, opaque, displaced []mqttRecoveredInflightReg,
) {
	if len(in) == 0 {
		return nil, nil, nil, nil
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
				if fr.rec == nil || !strings.HasPrefix(fr.rec.Dur, mqttSharedConsumerDurablePrefix) {
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
				claimed := *fr.rec
				claimed.Claim = claim
				claimed.ClaimSeq = claimSeq
				// The chain starts at the walk sequence, and every arm that CONTINUES
				// the contest on our own record extends it (F-087 WP4).
				traversed := []uint64{fr.seq}
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
							rec: &landed, seq: seq, traversed: append(traversed, seq),
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
							rec: cur, seq: smsg.Sequence,
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
		}
	}
	return owned, closed, opaque, displaced
}

// recoverInflightRegisters rebuilds this session's outbound in-flight identifier
// space from durable state, and MUST run before any source interest exists (R3).
//
// It is the whole point of the feature: after it returns, cpending names every
// started delivery, so the redelivery that follows finds its ORIGINAL identifier
// instead of allocating a fresh one, and bumpPI cannot hand out an identifier that a
// register or an unreplayed PUBREL record still owns.
//
// Two durable sources, and where they disagree the PUBREL record wins (R2):
//
//   - a PUBREL record on this incarnation's PUBREL subject is an exchange PAST
//     PUBREC. Its identifier is reserved in pendingPubRel exactly as
//     trackAsPubRel(pi, _EMPTY_) would, and — where the record names its source — the
//     source is entered in cpending too, so the usedForPubRel guard suppresses a
//     re-PUBLISH of a payload the client has already answered.
//   - a register is an exchange in the PUBLISH phase. Its source is classified
//     first: a provably complete source means the exchange finished and the register
//     is deleted; a live or unprovable one is reserved in pendingPublish with no ack
//     subject, waiting for the redelivery that fills it in.
//
// It fails closed WITHOUT denying the CONNECT (R3): an incomplete scan leaves the
// identifier space unknown, so the session is marked piSpaceIncomplete and bumpPI
// stops allocating new identifiers, while everything already reserved — and every
// QoS 0 subscription — keeps working. That is the same shape as the drain barrier's
// contract, which withholds a durable's source interest rather than the connection.
//
// In-memory state wins every conflict: on a same-process reconnect the tracking maps
// are the live authority and already hold everything, so this only fills gaps.
//
// Session lock NOT held on entry.
func (sess *mqttSession) recoverInflightRegisters(c *client) (staged []mqttStagedRelease, err error) {
	as := sess.asm
	sess.mu.Lock()
	// This walk's identity, so the reconciliation can tell a quarantine that PREDATES
	// the enumeration from one installed while it ran: only the first is a contest this
	// pass is entitled to decide (F-087 WP2).
	sess.inflightWalks++
	walkID := sess.inflightWalks
	idHash, gen, pubRelSubject := sess.idHash, sess.gen, sess.pubRelSubject
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

	// (1) The PUBREL records: identifiers already past PUBREC.
	rels, ok := sess.scanPubRelRecords(pubRelSubject)
	if !ok {
		sess.setPISpaceIncomplete(c, "the PUBREL records could not be scanned")
		return nil, nil
	}
	relBySource := make(map[string]struct{}, len(rels))
	for _, r := range rels {
		if r.srcDur != _EMPTY_ {
			relBySource[mqttPubRelSourceKey(r.srcDur, r.srcSeq)] = struct{}{}
		}
	}

	// (2) The registers. One bounded filter over this incarnation alone, which a
	// 16-bit identifier caps at 65_535 subjects.
	regs := make([]mqttRecoveredInflightReg, 0, 8)
	// opaque holds registers whose body is unusable but whose subject names an
	// identifier this incarnation wrote. They reserve that identifier and nothing else:
	// there is no source to classify, and nothing may be deleted.
	var opaque []mqttRecoveredInflightReg
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
	// Which identifiers this walk actually LOOKED at, which is what tells an absence
	// apart from a subject the walk never covered. The quarantine reconciliation needs
	// exactly that distinction: an enumerated subject with no register is a settled
	// contest, while an unenumerated one says nothing (F-087 WP2).
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
		resps, berr := as.jsa.loadLastMsgForMulti(mqttInflightStreamName, batch)
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
		rec := &mqttInflightRecord{}
		if uerr := json.Unmarshal(smsg.Data, rec); uerr != nil || !mqttInflightRecordIdentityOK(rec) || rec.PI != name.pi {
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
		regs = append(regs, mqttRecoveredInflightReg{rec: rec, seq: smsg.Sequence})
	}

	// (3) Classify each register, mutating no session state yet: a JetStream lookup
	// must not run under the session lock, and the decisions have to be taken against
	// one consistent view.
	//
	// Sources are resolved ONCE, in bulk: one consumer lookup per durable and batched
	// message reads, rather than two round trips per record before the CONNACK. Duplicate
	// registers naming one source share the result.
	srcCache := as.newInflightSourceCache()
	if len(regs) > 0 {
		all := make([]*mqttInflightRecord, 0, len(regs))
		for _, fr := range regs {
			all = append(all, fr.rec)
		}
		srcCache.resolve(all)
	}

	var reserve []mqttRecoveredInflightReg
	// Every drop carries its NONCE alongside its sequence: the delete routes through
	// reapRegisterWithVerdict, so a cancellation verdict attached to the register is
	// released against the proof of its deletion instead of accumulating for the life of
	// the session (F-087 WP3).
	type regDrop struct {
		seq   uint64
		nonce string
	}
	var drop []regDrop
	// completeConsumed carries every register dropped on PROOF OF COMPLETION into the
	// apply phase: a same-object barrier re-run may still hold the live entry the
	// finished exchange backed, and it must be retired with the register or it counts
	// against Receive Maximum forever.
	var completeConsumed []mqttRecoveredInflightReg
	// Registers dropped on the strength of a cancellation NONCE, kept apart because their
	// removal has to be proven before the intent that named them is released.
	type cancelledDrop struct {
		seq   uint64
		nonce string
	}
	var dropByNonce []cancelledDrop

	// A register a DRAIN claims is the drain's, and recovery must not touch the record.
	// Retiring it would rewrite it — a CAS moves the record to a new sequence — while the
	// drain's persisted RegSeq still names the old one, so the drain's later release would
	// target a sequence that no longer exists and the real record would survive as exactly
	// the phantom this ownership exists to prevent. It would also duplicate the drain's own
	// suppression, which is governed by I2b: only a CONFIRMED PUBREL may release a drained
	// claim.
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
	sess.mu.Lock()
	drainOwned, drainAmbiguous := sess.recomputeDrainRegisterStateLocked(drainRegs)
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

	// Retired registers first: these are not PUBLISH-phase deliveries at all. Their
	// exchange is past PUBREC with an unproven source acknowledgement, so the payload must
	// never be sent again while the source may still be re-offered. They reserve NO
	// identifier — the client completed the exchange, which freed it — and instead
	// reinstate the in-memory retirement, so the next attempt at that source is
	// acknowledged and discarded rather than delivered. This is the half a memory-only
	// retirement loses across a restart.
	var retired []mqttRecoveredInflightReg
	// Affine registers whose carried pair its own source does not prove. Kept apart only
	// until the claim pass has run, then folded in as unowned members of their source's
	// group: nothing here may be claimed, installed or settled.
	var unprovenAffine []mqttRecoveredInflightReg
	live := make([]mqttRecoveredInflightReg, 0, len(regs))
	for _, fr := range regs {
		if (fr.rec.Cancelled || cancelledNonces[fr.rec.Nonce]) && !fr.rec.Retired {
			// PURE cancellation. Retired dominates Cancelled (F-087 WP3): a retirement is
			// backed by the client's own PUBREC, so the transmission provably happened,
			// while a cancellation over a transmitted delivery is the known-false write
			// F-088 owns. Freeing an identifier the client holds is the catastrophic
			// direction, so a record carrying both routes to the retirement arms below —
			// suppression, nothing freed — and a verdict naming it parks, which is the
			// existing bounded-stale-metadata residual for a verdict whose register never
			// surfaces as free.
			// Provably NEVER TRANSMITTED, which is the one thing that licenses freeing an
			// identifier rather than holding it: the client never saw this PUBLISH, so the
			// identifier is not in use and its Receive Maximum slot is not consumed. Every
			// other surviving register is treated as possibly client-held, because it might
			// be. The record is removed; the cancellation wanted that all along and only
			// failed to prove it.
			//
			// Two durable forms say this, and the second exists because the first cannot
			// always be written in time: a tombstone ON the record, and the record's NONCE
			// in the session's cancelled set, written by the cancelling packet itself while
			// the store was still in flight. A nonce is safe to match on where an identifier
			// or a source would not be — the cancellation frees the identifier for reuse and
			// the source is re-offered, so either could name a genuine later delivery.
			if cancelledNonces[fr.rec.Nonce] {
				// The verdict set names this register — whether or not it is ALSO
				// tombstoned — so its delete must be PROVEN before the nonce is retired.
				// The tombstoned-and-in-set case matters: it is what a crash between the
				// teardown's tombstone rung and the forget's save leaves behind, and
				// routing it to the fire-and-forget list would keep the verdict on the
				// record forever. The register is right here, so retiring against its
				// confirmed deletion needs no absence reasoning at all.
				dropByNonce = append(dropByNonce, cancelledDrop{fr.seq, fr.rec.Nonce})
				continue
			}
			drop = append(drop, regDrop{fr.seq, fr.rec.Nonce})
			continue
		}
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
			if _, past := relBySource[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)]; !past {
				live = append(live, fr)
			}
			continue
		}
		if fr.rec.Retired {
			// A retirement CONVERGES here: its source may have been acknowledged since (the
			// unproven acknowledgement that created it may simply have landed), and a
			// retirement whose source is provably complete has nothing left to suppress. Without
			// this every resume would re-establish it from its own record, forever.
			if srcCache.state(fr.rec) == mqttInflightSourceComplete {
				drop = append(drop, regDrop{fr.seq, fr.rec.Nonce})
				completeConsumed = append(completeConsumed, fr)
				continue
			}
			retired = append(retired, fr)
			continue
		}
		if _, past := relBySource[mqttPubRelSourceKey(fr.rec.Dur, fr.rec.Seq)]; past {
			// A PUBREL record names the same source, so this exchange is provably past
			// PUBREC (R2) and the identifier is reserved by the PUBREL loop below, not
			// here. What becomes of the register depends on the SOURCE, and the difference
			// matters:
			//
			//   - source provably complete -> the handoff finished and only the register's
			//     delete was lost. Nothing can redeliver it; drop it.
			//   - source still live -> the handoff's source acknowledgement was never
			//     proven, so the source WILL be re-offered. Deleting the register here
			//     would destroy the very record that has to carry the retirement, and
			//     once the client's PUBCOMP removes this PUBREL nothing would be left to
			//     say the payload was already answered. So it is retired instead.
			if srcCache.state(fr.rec) == mqttInflightSourceComplete {
				drop = append(drop, regDrop{fr.seq, fr.rec.Nonce})
			} else {
				retired = append(retired, fr)
			}
			continue
		}
		if srcCache.state(fr.rec) == mqttInflightSourceComplete {
			// Proven finished: the source was acknowledged, or is authoritatively absent
			// while its consumer still holds interest. A register whose delete was lost.
			// The identity is kept for the apply phase: a same-object rebind may hold a
			// LIVE entry this register backed — the terminal-race resurrection — and a
			// finished exchange's entry must not keep consuming a Receive Maximum slot.
			drop = append(drop, regDrop{fr.seq, fr.rec.Nonce})
			completeConsumed = append(completeConsumed, fr)
			continue
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
	var claimClosed, claimFailed, displaced []mqttRecoveredInflightReg
	live, claimClosed, claimFailed, displaced = sess.claimRecoveredSharedInflight(c, live)
	// claimConsumed carries every consumed close decision into the apply phase, so a
	// same-object barrier re-run retires the LIVE in-memory entries those records
	// backed; the non-zero seqs are the tombstones the confirmed delete targets.
	claimConsumed := claimClosed
	for _, fr := range claimClosed {
		if fr.seq != 0 {
			drop = append(drop, regDrop{fr.seq, fr.rec.Nonce})
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
	var retiredFailed, retiredDisplaced []mqttRecoveredInflightReg
	retired, claimClosed, retiredFailed, retiredDisplaced = sess.claimRecoveredSharedInflight(c, retired)
	displaced = append(displaced, retiredDisplaced...)
	// The live bucket's newly retired members join the retired bucket AFTER its own
	// contest: they are already claimed at their landed sequence, and re-contesting them
	// would rewrite a record this pass has just taken ownership of.
	retired = append(retired, newlyRetired...)
	claimConsumed = append(claimConsumed, claimClosed...)
	for _, fr := range claimClosed {
		if fr.seq != 0 {
			drop = append(drop, regDrop{fr.seq, fr.rec.Nonce})
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

	// The final disposition of every source this pass observed, which is what the
	// quarantine reconciliation consumes (F-087 WP2). Built here, from the routed
	// results, so the reconciliation reads one classification rather than re-deriving it
	// from five slices.
	// Sized from the walk, NOT from sess.foreignContested: that map is written by the
	// delivery path under sess.mu, and this classify phase deliberately holds no lock.
	final := make(map[mqttDeferredKey]mqttContestDisposition, len(regs))
	note := func(dur string, sseq uint64, d mqttContestDisposition, rv mqttRecoveredInflightReg, rel mqttStoredPubRel) {
		if dur == _EMPTY_ || sseq == 0 {
			return
		}
		key := mqttDeferredKey{dur, sseq}
		if cur, seen := final[key]; seen && cur.class >= d.class {
			return // higher precedence already recorded
		}
		d.reg, d.rel = rv, rel
		final[key] = d
	}
	for _, r := range rels {
		if r.pi != 0 && r.srcDur != _EMPTY_ {
			note(r.srcDur, r.srcSeq, mqttContestDisposition{class: mqttContestPubRelOwned}, mqttRecoveredInflightReg{}, r)
		}
	}
	for _, fr := range append(claimConsumed, completeConsumed...) {
		note(fr.rec.Dur, fr.rec.Seq, mqttContestDisposition{class: mqttContestClosed}, fr, mqttStoredPubRel{})
	}
	for _, fr := range poisoned {
		note(fr.rec.Dur, fr.rec.Seq, mqttContestDisposition{class: mqttContestPoisoned}, fr, mqttStoredPubRel{})
	}
	for _, fr := range retired {
		note(fr.rec.Dur, fr.rec.Seq, mqttContestDisposition{class: mqttContestRetired}, fr, mqttStoredPubRel{})
	}
	for _, fr := range retiredFailed {
		note(fr.rec.Dur, fr.rec.Seq, mqttContestDisposition{class: mqttContestPoisoned}, fr, mqttStoredPubRel{})
	}
	for _, rv := range reserve {
		note(rv.rec.Dur, rv.rec.Seq, mqttContestDisposition{class: mqttContestOwnedLive}, rv, mqttStoredPubRel{})
	}

	// (4) Apply. Everything below is memory-only and takes the session lock once.
	// Counters for the log line only. Nothing here governs a decision — in particular
	// the piSpaceIncomplete condition below reads len(opaque), what the walk FOUND,
	// never installedOpaque, what this pass happened to install.
	var restoredPubs, restoredRels, installedOpaque, skipped int
	sess.mu.Lock()
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.pendingPubRel == nil {
		sess.pendingPubRel = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
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
	for _, fr := range append(claimConsumed, completeConsumed...) {
		// A CANCELLED reservation is exempt: it is held until its store settles AND a
		// rung is proven, and its nonce deliberately matches the tombstone that names
		// it — releasing it here would free an identifier whose CAS may still be on
		// the wire.
		if ack := sess.pendingPublish[fr.rec.PI]; ack != nil && ack.regNonce == fr.rec.Nonce && !ack.regCancelled {
			// An entry whose transmission has not returned is CONDEMNED rather than
			// deleted (F-087 WP5, class F): this pass has already issued every durable
			// delete it owes, so endSend's continuation is the memory untrack alone.
			if !sess.condemnSendLocked(fr.rec.PI, mqttSendSettleRecoveryDrop) {
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
			sess.pendingPublish[fr.rec.PI] = &mqttPending{
				regSeq:     fr.seq,
				regForeign: fr.foreign,
				sendNonce:  ack.sendNonce,
				sendSettle: ack.sendSettle,
			}
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
		sseqToPi := sess.cpending[dur]
		if sseqToPi == nil {
			sseqToPi = make(map[uint64]uint16)
			sess.cpending[dur] = sseqToPi
		}
		sseqToPi[sseq] = pi
	}
	// ONE install body for a recovered PUBREL, called by the ordinary restore loop below
	// and by the quarantine reconciliation — which needs it because that loop SKIPS an
	// identifier whose pendingPublish entry is resident, and a quarantine is exactly
	// that. Extracted rather than duplicated: the correlated cpending restore is what
	// keeps a real source re-offer from allocating a second identifier (trackPublish
	// reaches pendingPubRel only through the mapping), and a filled in-memory entry must
	// be PRESERVED rather than overwritten by the placeholder.
	installPubRelLocked := func(r mqttStoredPubRel) {
		if cur := sess.pendingPubRel[r.pi]; cur != nil {
			if cur.jsAckSubject != _EMPTY_ {
				return // a filled entry is the live authority; the placeholder is not
			}
		}
		// The placeholder shape trackAsPubRel(pi, _EMPTY_) writes: the ack subject is
		// filled in by the PUBREL delivery, and mqttDeliverPubRelCb's duplicate guard
		// already tolerates an empty one.
		sess.pendingPubRel[r.pi] = &mqttPending{jsDur: r.srcDur, sseq: r.srcSeq}
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
	for _, rv := range reserve {
		if ack, live := sess.pendingPublish[rv.rec.PI]; live {
			// In-memory state wins, but the register's SEQUENCE is the stream's to
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
		sess.pendingPublish[rv.rec.PI] = &mqttPending{
			jsDur:      rv.rec.Dur,
			sseq:       rv.rec.Seq,
			q2AffClaim: rv.rec.Q2AffClaim,
			q2AffCopy:  rv.rec.Q2AffCopy,
			regSeq:     rv.seq,
			regNonce:   rv.rec.Nonce,
		}
		reserveSource(rv.rec.Dur, rv.rec.Seq, rv.rec.PI)
		restoredPubs++
	}
	// The opaque ones reserve their identifier and nothing else: there is no source to
	// enter in cpending, because the unreadable body is exactly what would have named
	// it. The entry carries its register sequence, which is what keeps
	// dropUnredeliverableRetained from mistaking it for a retained send and freeing the
	// identifier on the next reconnect.
	for _, rv := range opaque {
		if _, live := sess.pendingPublish[rv.rec.PI]; live {
			skipped++
			continue // in-memory state wins
		}
		if _, asRel := sess.pendingPubRel[rv.rec.PI]; asRel {
			skipped++
			continue // a PUBREL record owns it (R2)
		}
		sess.pendingPublish[rv.rec.PI] = &mqttPending{regSeq: rv.seq, regForeign: rv.foreign}
		installedOpaque++
	}

	// The quarantine's ONE reconciliation, and its only removers (F-087 WP2). It runs at
	// the END of the apply — after the routing, the grouping, the poisoned demote, the
	// retirement installs and the reserve install — because what disposes a quarantine is
	// the walk's FINAL result class for its source, not the raw fact that a claim
	// succeeded: an owned record may be newly retired (routed to suppression, never
	// deliverable) and an owned-live one may still be poisoned by the duplicate-source
	// grouping.
	//
	// Without this, a reconnect that successfully RECLAIMS the rival register would leave
	// the quarantine standing forever — recovery's in-memory-wins arms are nonce-guarded
	// and a reservation's nonce is empty, so nothing else ever reclassifies it.
	staged = sess.reconcileForeignContestsLocked(c, final, observedPI, walkID, installPubRelLocked)
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
	case len(opaque) > 0:
		sess.piSpaceIncomplete = true
	case sess.piSpaceIncomplete:
		sess.piSpaceIncomplete = false
		defer c.Warnf("MQTT: outbound packet identifier allocation resumed for client %q: the durable identifier space was reconstructed", sess.id)
	}
	sess.mu.Unlock()

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
				sess.reapRegisterWithVerdict(cur.regSeq, cur.nonce, false)
				c.Debugf("MQTT: dropping the recovered retirement of QoS 2 source %s/%d for client %q: its "+
					"source is provably complete", fr.rec.Dur, fr.rec.Seq, sess.id)
			}
		}
	}

	for _, rd := range drop {
		// Through the verdict property: an unverdicted nonce keeps today's
		// fire-and-forget delete, and a verdicted one is deleted with proof and its
		// verdict released against that proof (F-087 WP3).
		sess.reapRegisterWithVerdict(rd.seq, rd.nonce, false)
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
		if !sess.reapRegisterWithVerdict(cd.seq, cd.nonce, true) {
			c.Warnf("MQTT: keeping the void-register verdict for sequence %d of client %q: "+
				"its register could not be deleted with proof", cd.seq, sess.id)
		}
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
